"""
session_store.py
----------------
Session state machine backed by Redis (fast operational store)
with write-through to PostgreSQL (durable audit log).

Redis  — source of truth for live session state during a session.
Postgres — source of truth for recovery after crash / Redis eviction.

State machine:
  AWAITING → CHARGING → PENDING_PAYMENT → COMPLETE
                                         → FAILED
"""

import logging
import os
from datetime import datetime

import redis

log = logging.getLogger(__name__)

# Redis connection — single instance reused across the app
r = redis.Redis.from_url(
    os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    decode_responses=True
)

# Session TTL — auto-expire abandoned sessions after 2 hours
SESSION_TTL_SECONDS = 7200

# Valid state transitions — guards against illegal moves
VALID_TRANSITIONS = {
    "AWAITING":        ["CHARGING", "FAILED"],
    "CHARGING":        ["PENDING_PAYMENT", "FAILED"],
    "PENDING_PAYMENT": ["COMPLETE", "FAILED"],
    "COMPLETE":        [],   # terminal state
    "FAILED":          [],   # terminal state
}


# ─── Helpers ────────────────────────────────────────────────────────────────

def session_key(charger_id: str, transaction_id: int) -> str:
    """Canonical Redis key for a session."""
    return f"session:{charger_id}:{transaction_id}"


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


# ─── MongoDB write-through ─────────────────────────────────────────────────
# The watchdog / Celery tasks use the sync Redis functions directly.

async def write_audit(charger_id: str, transaction_id: int, **kwargs) -> None:
    """
    Upsert a document in MongoDB session_audit for every state transition.
    Called from FastAPI async handlers (handle_start, handle_stop, etc).
    Silently logs on error — never blocks the main session flow.

    Document _id convention: "{charger_id}:{transaction_id}"
    Uses update_one with upsert=True — creates on first call,
    updates on subsequent calls. Matches original PostgreSQL upsert behaviour.
    """
    try:
        from db import session_audit_col
        from datetime import datetime

        doc_id = f"{charger_id}:{transaction_id}"
        now    = datetime.utcnow()

        # Build the $set payload from kwargs + always-updated fields
        update_fields = {
            "charger_id":     charger_id,
            "transaction_id": transaction_id,
            "updated_at":     now,
        }
        update_fields.update(kwargs)

        await session_audit_col().update_one(
            {"_id": doc_id},
            {
                "$set":         update_fields,
                "$setOnInsert": {"created_at": now}
            },
            upsert=True
        )

    except Exception as e:
        log.error(f"[{charger_id}] MongoDB write_audit failed "
                  f"txn={transaction_id}: {e}")


async def log_meter(charger_id: str, transaction_id: int,
                    kwh: float, cost: str) -> None:
    """
    Append one document to MongoDB meter_log for every MeterValues reading.
    This is the tamper-evident audit trail for billing disputes.

    Each document gets an auto-generated MongoDB ObjectId (_id).
    Query pattern: find all readings for a session →
      db.meter_log.find({"charger_id": X, "transaction_id": Y})
    """
    try:
        from db import meter_log_col
        from datetime import datetime

        await meter_log_col().insert_one({
            "charger_id":     charger_id,
            "transaction_id": transaction_id,
            "kwh_value":      kwh,
            "cost_live":      cost,
            "recorded_at":    datetime.utcnow()
        })

    except Exception as e:
        log.error(f"[{charger_id}] MongoDB log_meter failed "
                  f"txn={transaction_id}: {e}")


# ─── Redis session operations (sync) ────────────────────────────────────────

def create_session(charger_id: str, transaction_id: int,
                   kwh_start: float, tariff: float,
                   id_tag: str) -> dict:
    """
    Create a new session in AWAITING state.
    Called on StartTransaction / Beckn confirm.
    """
    key  = session_key(charger_id, transaction_id)
    now  = _now_iso()
    data = {
        "charger_id":     charger_id,
        "transaction_id": str(transaction_id),
        "id_tag":         id_tag,
        "status":         "AWAITING",
        "kwh_start":      str(kwh_start),
        "kwh_live":       str(kwh_start),
        "kwh_total":      "0.0",
        "tariff":         str(tariff),
        "gst_rate":       "0.18",
        "cost_live":      "0.0",
        "cost_final":     "0.0",
        "upi_vpa":        "",
        "upi_txn_id":     "",
        "bap_uri":        "",        # populated by Beckn confirm
        "order_id":       "",        # populated by Beckn confirm
        "retry_count":    "0",       # UPI collect retry counter
        "last_meter_ts":  now,       # watchdog timestamp — updated on every MeterValues
        "updated_at":     now,
        "created_at":     now,
    }
    r.hset(key, mapping=data)
    r.expire(key, SESSION_TTL_SECONDS)
    log.info(f"[{charger_id}] Session CREATED | "
             f"txn={transaction_id} | status=AWAITING")
    return data


def transition(charger_id: str, transaction_id: int,
               new_status: str) -> bool:
    """
    Move session to new_status if transition is valid.
    Returns True on success, False if transition is illegal.
    """
    key     = session_key(charger_id, transaction_id)
    current = r.hget(key, "status")

    if current is None:
        log.error(f"[{charger_id}] Session not found: txn={transaction_id}")
        return False

    allowed = VALID_TRANSITIONS.get(current, [])
    if new_status not in allowed:
        log.error(f"[{charger_id}] Illegal transition "
                  f"{current} → {new_status} | txn={transaction_id}")
        return False

    r.hset(key, mapping={
        "status":     new_status,
        "updated_at": _now_iso()
    })
    r.expire(key, SESSION_TTL_SECONDS)
    log.info(f"[{charger_id}] State {current} → {new_status} | "
             f"txn={transaction_id}")
    return True


def update_live_meter(charger_id: str, transaction_id: int,
                      kwh_live: float) -> float:
    """
    Update live kWh reading and recompute running cost.
    Called on every MeterValues message.
    Updates last_meter_ts — watchdog uses this to detect power failure.
    Returns current cost_live.
    """
    key       = session_key(charger_id, transaction_id)
    kwh_start = float(r.hget(key, "kwh_start") or 0)
    tariff    = float(r.hget(key, "tariff")    or 18.0)
    gst_rate  = float(r.hget(key, "gst_rate")  or 0.18)

    delta_kwh = kwh_live - kwh_start
    cost_live = delta_kwh * tariff * (1 + gst_rate)

    r.hset(key, mapping={
        "kwh_live":      str(kwh_live),
        "cost_live":     f"{cost_live:.2f}",
        "last_meter_ts": _now_iso(),   # ← watchdog timestamp
        "updated_at":    _now_iso(),
    })
    r.expire(key, SESSION_TTL_SECONDS)
    return cost_live


def finalise_session(charger_id: str, transaction_id: int,
                     kwh_stop: float) -> dict:
    """
    Compute final cost and move to PENDING_PAYMENT.
    Called on StopTransaction or watchdog stale-session recovery.
    Returns session dict with cost_final set.
    """
    key       = session_key(charger_id, transaction_id)
    kwh_start = float(r.hget(key, "kwh_start") or 0)
    tariff    = float(r.hget(key, "tariff")    or 18.0)
    gst_rate  = float(r.hget(key, "gst_rate")  or 0.18)

    kwh_total  = kwh_stop - kwh_start
    cost_final = kwh_total * tariff * (1 + gst_rate)

    r.hset(key, mapping={
        "kwh_total":  f"{kwh_total:.3f}",
        "kwh_live":   str(kwh_stop),
        "cost_final": f"{cost_final:.2f}",
        "status":     "PENDING_PAYMENT",
        "updated_at": _now_iso(),
    })
    r.expire(key, SESSION_TTL_SECONDS)

    log.info(f"[{charger_id}] Session FINALISED | "
             f"txn={transaction_id} | "
             f"kWh={kwh_total:.3f} | "
             f"cost=₹{cost_final:.2f} | "
             f"status=PENDING_PAYMENT")

    return get_session(charger_id, transaction_id)


def mark_paid(charger_id: str, transaction_id: int,
              upi_txn_id: str) -> bool:
    """
    Mark session COMPLETE after UPI collect confirmed.
    Called from Juspay webhook handler and payment_poller.
    """
    key = session_key(charger_id, transaction_id)
    r.hset(key, mapping={
        "upi_txn_id": upi_txn_id,
        "status":     "COMPLETE",
        "updated_at": _now_iso(),
    })
    log.info(f"[{charger_id}] Session COMPLETE | "
             f"txn={transaction_id} | upi_txn={upi_txn_id}")
    return True


def mark_failed(charger_id: str, transaction_id: int,
                reason: str) -> bool:
    """
    Mark session FAILED.
    Called by watchdog (stale), escalation_worker (bad debt), or error handler.
    """
    key = session_key(charger_id, transaction_id)
    r.hset(key, mapping={
        "status":      "FAILED",
        "fail_reason": reason,
        "updated_at":  _now_iso(),
    })
    log.warning(f"[{charger_id}] Session FAILED | "
                f"txn={transaction_id} | reason={reason}")
    return True


def get_session(charger_id: str, transaction_id: int) -> dict | None:
    """Fetch full session dict from Redis. Returns None if not found."""
    key  = session_key(charger_id, transaction_id)
    data = r.hgetall(key)
    return data if data else None


def get_sessions_by_status(status: str) -> list[dict]:
    """
    Return all sessions in a given status.
    Used by watchdog and escalation_worker to find stale sessions.
    """
    results = []
    for key in r.keys("session:*"):
        session = r.hgetall(key)
        if session.get("status") == status:
            results.append(session)
    return results


def increment_retry(charger_id: str, transaction_id: int) -> int:
    """
    Increment and return the UPI collect retry counter.
    Used by escalation_worker.
    """
    key   = session_key(charger_id, transaction_id)
    count = r.hincrby(key, "retry_count", 1)
    r.hset(key, "updated_at", _now_iso())
    return count
