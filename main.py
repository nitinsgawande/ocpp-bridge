"""
main.py
-------
ChargeFlow FastAPI bridge service.

Responsibilities:
  - Receive CPO session event webhooks (StartTransaction, MeterValues, StopTransaction)
  - Route only CHARGEFLOW_ prefixed sessions — all prepaid sessions ignored
  - BLOCK-DEBIT: capture exact amount from pre-blocked funds at session end
  - POSTPAID:    trigger UPI collect after session ends (fallback model)
  - Receive Juspay payment confirmation webhook
  - Publish cable unlock event via Redis pub/sub after payment
  - Startup recovery — resume in-flight sessions from MongoDB after crash

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
from upi_collect import trigger_collect, capture_block, release_block

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
    from db import create_indexes
    await create_indexes()
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
    """Verify CPO webhook HMAC-SHA256 signature. Skipped in dev when no secret."""
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
    """
    body = await request.body()

    if not verify_signature(body, x_signature):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    event  = json.loads(body)
    action = event.get("action", "")
    idtag  = event.get("idTag", "")

    # Route guard: ignore all non-ChargeFlow sessions
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
    Attaches block-debit mandate from the pending_confirm entry if present.
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

    # ── Attach block-debit mandate from pending_confirm (if Beckn confirm ran) ──
    pending_key = f"pending_confirm:{charger_id}:{idtag}"
    pending     = store.r.hgetall(pending_key)
    if pending and pending.get("mandate_id"):
        key = store.session_key(charger_id, transaction_id)
        store.r.hset(key, mapping={
            "mandate_id":     pending["mandate_id"],
            "reserve_amount": pending.get("reserve_amount", "0.00"),
            "bap_uri":        pending.get("bap_uri", ""),
            "order_id":       pending.get("order_id", ""),
            "payment_mode":   "BLOCK_DEBIT",
        })
        store.r.delete(pending_key)
        log.info(f"[{charger_id}] Attached mandate {pending['mandate_id']} "
                 f"(BLOCK_DEBIT) to txn={transaction_id}")
    else:
        # No mandate — pure postpaid session
        store.r.hset(store.session_key(charger_id, transaction_id),
                     "payment_mode", "POSTPAID")

    # Write-through to MongoDB audit log
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
    Handle MeterValues — update Redis live cost and append to MongoDB meter_log.
    Updates last_meter_ts which the watchdog uses to detect power failure.
    """
    for mv in event.get("meter_value", []):
        for sv in mv.get("sampled_value", []):
            measurand = sv.get("measurand", "Energy.Active.Import.Register")
            if measurand != "Energy.Active.Import.Register":
                continue

            kwh_live  = float(sv.get("value", 0)) / 1000
            cost_live = store.update_live_meter(
                charger_id, transaction_id, kwh_live
            )

            session = store.get_session(charger_id, transaction_id)
            if session:
                delta = kwh_live - float(session.get("kwh_start", 0))
                log.info(
                    f"[{charger_id}] LIVE METER | "
                    f"consumed={delta:.3f} kWh | cost=₹{cost_live:.2f}"
                )

            await store.log_meter(
                charger_id=charger_id,
                transaction_id=transaction_id,
                kwh=kwh_live,
                cost=f"{cost_live:.2f}"
            )


async def handle_stop(charger_id: str, transaction_id: int, event: dict):
    """
    Handle StopTransaction — finalise session, compute exact cost.
    BLOCK_DEBIT: capture exact amount from blocked funds, unlock immediately.
    POSTPAID:    trigger UPI collect, wait for payment, then unlock.
    """
    meter_stop = event.get("meter_stop", 0)
    kwh_stop   = meter_stop / 1000

    # ── Orphan guard: no matching session in Redis ────────────────────────────
    existing = store.get_session(charger_id, transaction_id)
    if not existing:
        kwh_consumed = kwh_stop
        if kwh_consumed < 0.1:
            log.warning(f"[{charger_id}] Orphan StopTransaction — "
                        f"near-zero kWh ({kwh_consumed:.3f}), skipping billing")
            return
        else:
            log.warning(f"[{charger_id}] Orphan StopTransaction — "
                        f"{kwh_consumed:.3f} kWh, creating recovery session")
            idtag = event.get("idTag", IDTAG_PREFIX + "unknown")
            store.create_session(charger_id, transaction_id, 0, 18.0, idtag)
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

    # Update MongoDB with final cost
    await store.write_audit(
        charger_id=charger_id,
        transaction_id=transaction_id,
        status="PENDING_PAYMENT",
        kwh_total=float(session["kwh_total"]),
        cost_final=result["total_payable"]
    )

    driver_phone = session.get("id_tag", "").replace(IDTAG_PREFIX, "")
    if not driver_phone or driver_phone == "unknown":
        log.error(f"[{charger_id}] No driver phone for txn={transaction_id} — cannot collect")
        return

    payment_mode = session.get("payment_mode", "POSTPAID")
    mandate_id   = session.get("mandate_id", "")
    exact_amount = result["total_payable"]

    # ── BLOCK-DEBIT path: capture exact amount from blocked funds ─────────────
    if payment_mode == "BLOCK_DEBIT" and mandate_id:
        kwh_total = float(session.get("kwh_total", 0))

        if kwh_total < 0.05:
            # Near-zero consumption — release the full block, charge nothing
            release_block(charger_id, transaction_id, mandate_id)
            store.mark_failed(charger_id, transaction_id,
                              "Near-zero kWh — block released")
            log.info(f"[{charger_id}] Block released, no charge | txn={transaction_id}")
            return

        capture = capture_block(
            charger_id=charger_id,
            transaction_id=transaction_id,
            mandate_id=mandate_id,
            exact_amount=exact_amount
        )

        if capture and capture.get("status") == "CAPTURED":
            # Payment secured immediately — mark COMPLETE and unlock cable
            store.mark_paid(charger_id, transaction_id, capture["upi_txn_id"])
            await store.write_audit(
                charger_id=charger_id,
                transaction_id=transaction_id,
                status="COMPLETE",
                upi_txn_id=capture["upi_txn_id"]
            )
            publish_unlock(charger_id, transaction_id, capture["upi_txn_id"])
            log.info(f"[{charger_id}] CAPTURED ₹{exact_amount} from block | "
                     f"cable unlocking | txn={transaction_id}")
        else:
            # Capture failed — fall back to postpaid collect as safety net
            log.error(f"[{charger_id}] Capture failed, falling back to collect")
            trigger_collect(charger_id, transaction_id,
                            exact_amount, driver_phone, "EV Driver")
        return

    # ── POSTPAID path: trigger UPI collect (driver pays, then unlock) ─────────
    trigger_collect(
        charger_id=charger_id,
        transaction_id=transaction_id,
        amount_inr=exact_amount,
        driver_phone=driver_phone,
        driver_name="EV Driver"
    )


# ─── Juspay payment confirmation webhook ─────────────────────────────────────

@app.post("/webhook/juspay/payment")
async def juspay_payment_webhook(request: Request):
    """
    Receives payment confirmation from Juspay/Razorpay (postpaid path).
    order_id format: CHARGEFLOW_{charger_id}_{transaction_id}
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

    parts = order_id.replace(IDTAG_PREFIX, "", 1).rsplit("_", 1)
    if len(parts) != 2:
        log.error(f"Cannot parse order_id: {order_id}")
        raise HTTPException(status_code=400, detail="Invalid order_id format")

    charger_id     = parts[0]
    transaction_id = int(parts[1])

    store.mark_paid(charger_id, transaction_id, upi_txn_id)

    await store.write_audit(
        charger_id=charger_id,
        transaction_id=transaction_id,
        status="COMPLETE",
        upi_txn_id=upi_txn_id
    )

    log.info(f"[{charger_id}] Payment CONFIRMED | "
             f"txn={transaction_id} | upi={upi_txn_id} | amount=₹{amount_inr}")

    publish_unlock(
        charger_id=charger_id,
        transaction_id=transaction_id,
        upi_txn_id=upi_txn_id
    )

    return {"status": "ok"}


# ─── Cable lock Redis subscriber ─────────────────────────────────────────────

async def cable_lock_subscriber():
    """
    Background task — consumes unlock events from a Redis LIST queue.

    Reliability: uses BLPOP (blocking pop) on the 'unlock_queue' list.
    Unlike pub/sub (at-most-once, lost if subscriber is disconnected),
    a list queue persists each event until it is consumed — at-least-once
    delivery that survives subscriber restarts and dropped connections.

    If the connection drops, the loop reconnects and any events enqueued
    in the meantime are still waiting in the list. Nothing is lost.
    """
    import redis.asyncio as aioredis

    redis_url        = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    UNLOCK_QUEUE_KEY = "unlock_queue"
    log.info("Cable lock consumer listening on unlock_queue (BLPOP)")

    while True:
        try:
            redis_client = aioredis.Redis.from_url(redis_url, decode_responses=True)
            while True:
                # BLPOP blocks until an item is available (timeout 5s, then loop).
                # Returns (queue_key, message) tuple, or None on timeout.
                result = await redis_client.blpop(UNLOCK_QUEUE_KEY, timeout=5)
                if result is None:
                    continue   # timeout — loop and keep waiting

                _, raw = result
                try:
                    data           = json.loads(raw)
                    charger_id     = data["charger_id"]
                    transaction_id = data["transaction_id"]
                    upi_txn_id     = data["upi_txn_id"]

                    log.info(f"[{charger_id}] Unlock event dequeued | txn={transaction_id}")
                    success = await send_unlock_to_cpo(charger_id, transaction_id)

                    if not success:
                        # Re-enqueue for retry — do not lose the unlock
                        await redis_client.rpush(UNLOCK_QUEUE_KEY, raw)
                        log.error(
                            f"[{charger_id}] CPO unlock FAILED — re-queued | "
                            f"txn={transaction_id} | OPS ALERT if repeated"
                        )
                        await asyncio.sleep(5)   # backoff before retry
                except Exception as e:
                    log.error(f"Cable lock consumer message error: {e}")

        except Exception as e:
            # Connection dropped — reconnect after a short delay.
            # Events remain safely in the Redis list while we are away.
            log.error(f"Cable lock consumer connection lost, reconnecting: {e}")
            await asyncio.sleep(3)


async def send_unlock_to_cpo(charger_id: str,
                              transaction_id: int,
                              connector_id: int = 1) -> bool:
    """Call CPO REST API to send UnlockConnector. Only after payment confirmed."""
    if not CPO_API_BASE:
        log.warning(f"[{charger_id}] CPO_API_BASE_URL not set — simulating unlock")
        log.info(f"[{charger_id}] ✅ CABLE RELEASED (simulated) | txn={transaction_id}")
        return True

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{CPO_API_BASE}/chargers/{charger_id}/unlock",
                headers={"Authorization": f"Bearer {CPO_API_KEY}"},
                json={"transaction_id": transaction_id, "connector_id": connector_id},
                timeout=10.0
            )
            if resp.status_code == 200:
                log.info(f"[{charger_id}] ✅ CABLE RELEASED via CPO API | txn={transaction_id}")
                return True
            else:
                log.error(f"[{charger_id}] CPO unlock API error: {resp.status_code} {resp.text}")
                return False
    except Exception as e:
        log.error(f"[{charger_id}] CPO unlock exception: {e}")
        return False


# ─── Startup recovery (MongoDB) ──────────────────────────────────────────────

async def startup_recovery():
    """
    On service restart: query MongoDB for sessions that were CHARGING or
    PENDING_PAYMENT at time of crash. Re-seeds Redis so they resume correctly.
    """
    await asyncio.sleep(2)

    try:
        from db import session_audit_col
        import redis as sync_redis

        cursor   = session_audit_col().find(
            {"status": {"$in": ["CHARGING", "PENDING_PAYMENT"]}}
        )
        sessions = await cursor.to_list(length=None)

        if not sessions:
            log.info("Startup recovery: no in-flight sessions found")
            return

        r = sync_redis.Redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True
        )

        recovered = 0
        for s in sessions:
            charger_id     = s.get("charger_id", "")
            transaction_id = s.get("transaction_id", 0)
            key            = f"session:{charger_id}:{transaction_id}"
            if not r.exists(key):
                r.hset(key, mapping={
                    "charger_id":     charger_id,
                    "transaction_id": str(transaction_id),
                    "id_tag":         s.get("idtag", ""),
                    "status":         s.get("status", ""),
                    "kwh_start":      str(s.get("kwh_start", 0.0)),
                    "kwh_live":       str(s.get("kwh_live", 0.0)),
                    "kwh_total":      str(s.get("kwh_total", 0.0)),
                    "tariff":         str(s.get("tariff", 18.0)),
                    "gst_rate":       "0.18",
                    "cost_final":     s.get("cost_final", "0.00"),
                    "cost_live":      "0.0",
                    "upi_txn_id":     s.get("upi_txn_id", ""),
                    "retry_count":    "0",
                    "last_meter_ts":  datetime.utcnow().isoformat(),
                    "updated_at":     datetime.utcnow().isoformat(),
                })
                r.expire(key, 7200)
                recovered += 1
                log.warning(f"[{charger_id}] Recovered {s.get('status')} session "
                             f"txn={transaction_id} from MongoDB")

        log.info(f"Startup recovery complete — {recovered} sessions re-seeded in Redis")

    except Exception as e:
        log.error(f"Startup recovery failed: {e}")


# ─── Health + session status ─────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status":    "ok",
        "service":   "chargeflow-bridge",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/session/{charger_id}/{transaction_id}")
async def get_session_status(charger_id: str, transaction_id: int):
    session = store.get_session(charger_id, transaction_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session
