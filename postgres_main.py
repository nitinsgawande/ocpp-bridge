"""
main.py
-------
ChargeFlow FastAPI bridge service.

Responsibilities:
  - Receive CPO session event webhooks (StartTransaction, MeterValues, StopTransaction)
  - Route only CHARGEFLOW_ prefixed sessions — all prepaid sessions ignored
  - Trigger UPI collect after session ends
  - Receive Juspay payment confirmation webhook
  - Publish cable unlock event via Redis pub/sub after payment
  - Startup recovery — resume in-flight sessions from Postgres after crash

Run with:
  uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request

import session_store as store
from cable_lock import publish_unlock
from tariff_calculator import compute_cost, format_receipt, get_tariff
from upi_collect import trigger_collect

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

IDTAG_PREFIX    = os.getenv("CHARGEFLOW_IDTAG_PREFIX", "CHARGEFLOW_")
WEBHOOK_SECRET  = os.getenv("CPO_WEBHOOK_SECRET", "")
CPO_API_BASE    = os.getenv("CPO_API_BASE_URL", "")
CPO_API_KEY     = os.getenv("CPO_API_KEY", "")


# ─── Startup / shutdown ──────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("ChargeFlow bridge starting...")
    asyncio.create_task(startup_recovery())
    asyncio.create_task(cable_lock_subscriber())
    yield
    log.info("ChargeFlow bridge shutting down.")

app = FastAPI(
    title="ChargeFlow Bridge",
    description="Postpaid EV charging payment bridge — UEI BPP",
    version="1.0.0",
    lifespan=lifespan
)

# Register Beckn BPP endpoints
from beckn_bpp import router as beckn_router
app.include_router(beckn_router)

# ─── Signature verification ──────────────────────────────────────────────────

def verify_signature(body: bytes, sig: str) -> bool:
    """
    Verify CPO webhook HMAC-SHA256 signature.
    Skipped in dev when WEBHOOK_SECRET is not set.
    """
    if not WEBHOOK_SECRET:
        return True
    expected = hmac.new(
        WEBHOOK_SECRET.encode(), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, sig or "")


# ─── CPO session webhook ─────────────────────────────────────────────────────

@app.post("/webhook/cpo/session")
async def cpo_session_webhook(
    request: Request,
    x_signature: str = Header(default="")
):
    """
    Receives OCPP session events from the CPO CSMS.
    Only processes sessions whose idTag starts with CHARGEFLOW_.
    All prepaid / wallet sessions are ignored — returned immediately.

    CPO must configure their CSMS to POST to this endpoint for
    StartTransaction, MeterValues, and StopTransaction events.
    """
    body  = await request.body()

    if not verify_signature(body, x_signature):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    event  = json.loads(body)
    action = event.get("action", "")   # StartTransaction | MeterValues | StopTransaction
    idtag  = event.get("idTag", "")

    # ── Route guard: ignore all non-ChargeFlow sessions ──────────────────────
    if not idtag.startswith(IDTAG_PREFIX):
        return {"status": "ignored", "reason": "not a ChargeFlow session"}

    charger_id     = event.get("charger_id", "")
    transaction_id = int(event.get("transaction_id", 0))

    log.info(f"CPO webhook | action={action} | charger={charger_id} | txn={transaction_id}")

    if action == "StartTransaction":
        await handle_start(charger_id, transaction_id, event, idtag)

    elif action == "MeterValues":
        await handle_meter(charger_id, transaction_id, event)

    elif action == "StopTransaction":
        await handle_stop(charger_id, transaction_id, event)

    else:
        log.warning(f"Unknown action in CPO webhook: {action}")

    return {"status": "ok"}


# ─── Session event handlers ──────────────────────────────────────────────────

async def handle_start(charger_id: str, transaction_id: int,
                        event: dict, idtag: str):
    """
    Handle StartTransaction — create Redis session and audit log row.
    Extracts driver phone from idTag: CHARGEFLOW_9876543210 → 9876543210
    """
    driver_phone = idtag.replace(IDTAG_PREFIX, "")
    meter_start  = event.get("meter_start", 0)
    kwh_start    = meter_start / 1000
    cpo_id       = event.get("cpo_id", "default")
    tariff_rate  = get_tariff(cpo_id)["base_rate"]

    # Create Redis session
    store.create_session(
        charger_id=charger_id,
        transaction_id=transaction_id,
        kwh_start=kwh_start,
        tariff=tariff_rate,
        id_tag=idtag
    )
    store.transition(charger_id, transaction_id, "CHARGING")

    # Write-through to Postgres
    await store.write_audit(
        charger_id=charger_id,
        transaction_id=transaction_id,
        idtag=idtag,
        driver_phone=driver_phone,
        status="CHARGING",
        kwh_start=kwh_start,
        tariff=tariff_rate
    )

    log.info(f"[{charger_id}] Session started | "
             f"driver={driver_phone} | "
             f"meter_start={kwh_start:.3f} kWh | "
             f"tariff=₹{tariff_rate}/kWh")


async def handle_meter(charger_id: str, transaction_id: int, event: dict):
    """
    Handle MeterValues — update Redis live cost and append to Postgres meter_log.
    Updates last_meter_ts which the watchdog uses to detect power failure.
    """
    for mv in event.get("meter_value", []):
        for sv in mv.get("sampled_value", []):
            measurand = sv.get("measurand", "Energy.Active.Import.Register")
            if measurand != "Energy.Active.Import.Register":
                continue

            kwh_live  = float(sv.get("value", 0)) / 1000   # Wh → kWh
            cost_live = store.update_live_meter(
                charger_id, transaction_id, kwh_live
            )

            session   = store.get_session(charger_id, transaction_id)
            if session:
                delta = kwh_live - float(session.get("kwh_start", 0))
                log.info(
                    f"[{charger_id}] LIVE METER | "
                    f"consumed={delta:.3f} kWh | "
                    f"cost=₹{cost_live:.2f}"
                )

            # Append to Postgres meter_log for billing dispute audit trail
            await store.log_meter(
                charger_id=charger_id,
                transaction_id=transaction_id,
                kwh=kwh_live,
                cost=f"{cost_live:.2f}"
            )


async def handle_stop(charger_id: str, transaction_id: int, event: dict):
    """
    Handle StopTransaction — finalise session, compute exact cost, trigger UPI collect.
    Handles orphan case: StopTransaction with no matching Redis session.
    """
    meter_stop = event.get("meter_stop", 0)
    kwh_stop   = meter_stop / 1000

    # ── Orphan guard: no matching session in Redis ────────────────────────────
    existing = store.get_session(charger_id, transaction_id)
    if not existing:
        kwh_consumed = kwh_stop  # meter_stop is absolute, no baseline known
        if kwh_consumed < 0.1:
            log.warning(f"[{charger_id}] Orphan StopTransaction — "
                        f"near-zero kWh ({kwh_consumed:.3f}), skipping billing")
            return
        else:
            log.warning(f"[{charger_id}] Orphan StopTransaction — "
                        f"{kwh_consumed:.3f} kWh, creating recovery session")
            idtag = event.get("idTag", IDTAG_PREFIX + "unknown")
            store.create_session(charger_id, transaction_id,
                                  0, 18.0, idtag)
            store.transition(charger_id, transaction_id, "CHARGING")

    # Finalise: compute exact cost, move to PENDING_PAYMENT
    session = store.finalise_session(charger_id, transaction_id, kwh_stop)
    if not session:
        log.error(f"[{charger_id}] finalise_session returned None — txn={transaction_id}")
        return

    # Compute itemised cost using tariff calculator
    result = compute_cost(
        cpo_id="pulse-energy",
        kwh=float(session["kwh_total"]),
        started_at=datetime.now()
    )
    print(format_receipt(result))

    # Update Postgres with final cost
    await store.write_audit(
        charger_id=charger_id,
        transaction_id=transaction_id,
        status="PENDING_PAYMENT",
        kwh_total=float(session["kwh_total"]),
        cost_final=result["total_payable"]
    )

    # Extract driver phone from idTag
    driver_phone = session.get("id_tag", "").replace(IDTAG_PREFIX, "")
    if not driver_phone or driver_phone == "unknown":
        log.error(f"[{charger_id}] No driver phone for txn={transaction_id} — cannot collect")
        return

    # Trigger UPI payment request for exact amount
    trigger_collect(
        charger_id=charger_id,
        transaction_id=transaction_id,
        amount_inr=result["total_payable"],
        driver_phone=driver_phone,
        driver_name="EV Driver"
    )


# ─── Juspay payment confirmation webhook ─────────────────────────────────────

@app.post("/webhook/juspay/payment")
async def juspay_payment_webhook(request: Request):
    """
    Receives payment confirmation from Juspay after driver approves UPI collect.

    Juspay order_id format: CHARGEFLOW_{charger_id}_{transaction_id}
    e.g. CHARGEFLOW_CHARGER-001_1001

    On CHARGED status:
      1. Mark session COMPLETE in Redis
      2. Write to Postgres audit log
      3. Publish unlock event → cable_lock subscriber → CPO API UnlockConnector
    """
    body  = await request.body()
    event = json.loads(body)

    payment_status = event.get("status", "")
    order_id       = event.get("order_id", "")
    upi_txn_id     = event.get("txn_id", "")
    amount_inr     = event.get("amount", "0.00")

    log.info(f"Juspay webhook | order={order_id} | status={payment_status}")

    if payment_status != "CHARGED":
        log.info(f"Juspay webhook ignored — status={payment_status}")
        return {"status": "ignored"}

    # Parse charger_id and transaction_id from order_id
    # Format: CHARGEFLOW_{charger_id}_{transaction_id}
    # Example: CHARGEFLOW_CHARGER-001_1001
    parts = order_id.replace(IDTAG_PREFIX, "", 1).rsplit("_", 1)
    if len(parts) != 2:
        log.error(f"Cannot parse order_id: {order_id}")
        raise HTTPException(status_code=400, detail="Invalid order_id format")

    charger_id     = parts[0]
    transaction_id = int(parts[1])

    # Mark session COMPLETE in Redis
    store.mark_paid(charger_id, transaction_id, upi_txn_id)

    # Write to Postgres
    await store.write_audit(
        charger_id=charger_id,
        transaction_id=transaction_id,
        status="COMPLETE",
        upi_txn_id=upi_txn_id
    )

    log.info(f"[{charger_id}] Payment CONFIRMED | "
             f"txn={transaction_id} | "
             f"upi={upi_txn_id} | "
             f"amount=₹{amount_inr}")

    # Publish unlock event — cable_lock subscriber sends UnlockConnector to CPO
    publish_unlock(
        charger_id=charger_id,
        transaction_id=transaction_id,
        upi_txn_id=upi_txn_id
    )

    return {"status": "ok"}


# ─── Cable lock Redis subscriber ─────────────────────────────────────────────

async def cable_lock_subscriber():
    """
    Background task — subscribes to Redis unlock:* pub/sub channel.
    When payment confirms, upi_collect publishes to unlock:{charger_id}.
    This subscriber calls the CPO REST API to send UnlockConnector.
    Runs for the lifetime of the process.
    """
    import redis.asyncio as aioredis

    redis_url    = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis_client = aioredis.Redis.from_url(redis_url, decode_responses=True)
    pubsub       = redis_client.pubsub()
    await pubsub.psubscribe("unlock:*")
    log.info("Cable lock subscriber listening on unlock:*")

    async for message in pubsub.listen():
        if message["type"] != "pmessage":
            continue
        try:
            data           = json.loads(message["data"])
            charger_id     = data["charger_id"]
            transaction_id = data["transaction_id"]
            upi_txn_id     = data["upi_txn_id"]

            log.info(f"[{charger_id}] Unlock event received | txn={transaction_id}")
            success = await send_unlock_to_cpo(charger_id, transaction_id)

            if not success:
                log.error(
                    f"[{charger_id}] CPO unlock FAILED after payment | "
                    f"txn={transaction_id} | "
                    f"OPS ALERT: manual cable release needed"
                )
        except Exception as e:
            log.error(f"Cable lock subscriber error: {e}")


async def send_unlock_to_cpo(charger_id: str,
                              transaction_id: int,
                              connector_id: int = 1) -> bool:
    """
    Call CPO REST API to send UnlockConnector to the charger.
    Only ever called after payment is confirmed — this is the cable release.
    """
    if not CPO_API_BASE:
        log.warning(f"[{charger_id}] CPO_API_BASE_URL not set — "
                    f"simulating unlock for local testing")
        log.info(f"[{charger_id}] ✅ CABLE RELEASED (simulated) | txn={transaction_id}")
        return True

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{CPO_API_BASE}/chargers/{charger_id}/unlock",
                headers={"Authorization": f"Bearer {CPO_API_KEY}"},
                json={
                    "transaction_id": transaction_id,
                    "connector_id":   connector_id
                },
                timeout=10.0
            )
            if resp.status_code == 200:
                log.info(f"[{charger_id}] ✅ CABLE RELEASED via CPO API | "
                         f"txn={transaction_id}")
                return True
            else:
                log.error(f"[{charger_id}] CPO unlock API error: "
                          f"{resp.status_code} {resp.text}")
                return False
    except Exception as e:
        log.error(f"[{charger_id}] CPO unlock exception: {e}")
        return False


# ─── Startup recovery ─────────────────────────────────────────────────────────

async def startup_recovery():
    """
    On service restart: query Postgres for sessions that were
    CHARGING or PENDING_PAYMENT at time of crash / restart.
    Re-seeds Redis from Postgres so in-flight sessions resume correctly.
    Watchdog and payment_poller will pick them up immediately.
    """
    await asyncio.sleep(2)   # wait for DB connection to settle

    try:
        from sqlalchemy.ext.asyncio import create_async_engine
        from sqlmodel import select
        from sqlmodel.ext.asyncio.session import AsyncSession
        from db import SessionAudit
        import redis as sync_redis

        database_url = os.getenv("DATABASE_URL",
                                  "postgresql+asyncpg://localhost/chargeflow")
        engine = create_async_engine(database_url)

        async with AsyncSession(engine) as db:
            result = await db.exec(
                select(SessionAudit).where(
                    SessionAudit.status.in_(["CHARGING", "PENDING_PAYMENT"])
                )
            )
            sessions = result.all()

        if not sessions:
            log.info("Startup recovery: no in-flight sessions found")
            return

        r = sync_redis.Redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True
        )

        recovered = 0
        for s in sessions:
            key = f"session:{s.charger_id}:{s.transaction_id}"
            if not r.exists(key):
                r.hset(key, mapping={
                    "charger_id":     s.charger_id,
                    "transaction_id": str(s.transaction_id),
                    "id_tag":         s.idtag,
                    "status":         s.status,
                    "kwh_start":      str(s.kwh_start),
                    "kwh_live":       str(s.kwh_live),
                    "kwh_total":      str(s.kwh_total),
                    "tariff":         str(s.tariff),
                    "gst_rate":       "0.18",
                    "cost_final":     s.cost_final,
                    "cost_live":      "0.0",
                    "upi_txn_id":     s.upi_txn_id or "",
                    "retry_count":    "0",
                    "last_meter_ts":  datetime.utcnow().isoformat(),
                    "updated_at":     datetime.utcnow().isoformat(),
                })
                r.expire(key, 7200)
                recovered += 1
                log.warning(f"[{s.charger_id}] Recovered {s.status} session "
                             f"txn={s.transaction_id} from Postgres")

        log.info(f"Startup recovery complete — {recovered} sessions re-seeded in Redis")

    except Exception as e:
        log.error(f"Startup recovery failed: {e}")


# ─── Health check ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """ALB health check endpoint."""
    return {
        "status":    "ok",
        "service":   "chargeflow-bridge",
        "timestamp": datetime.utcnow().isoformat()
    }


# ─── Session status API (ops dashboard) ──────────────────────────────────────

@app.get("/session/{charger_id}/{transaction_id}")
async def get_session_status(charger_id: str, transaction_id: int):
    """
    Returns current session state.
    Used by: ops dashboard, driver self-service payment confirmation.
    """
    session = store.get_session(charger_id, transaction_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session
