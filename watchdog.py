"""
watchdog.py
-----------
Celery Beat task that scans Redis every 5 minutes for stale sessions.

Handles two critical negative scenarios:

1. POWER FAILURE / CHARGER DISCONNECT
   Session stuck in CHARGING with no MeterValues for 5+ minutes.
   Watchdog finalises the session using the last known kWh reading
   and triggers UPI collect. Driver pays even if StopTransaction
   was never received.

2. STUCK PENDING_PAYMENT
   Session stuck in PENDING_PAYMENT for 30+ minutes — means
   UPI collect was never triggered (e.g. bridge restarted between
   StopTransaction and trigger_collect). Watchdog re-triggers collect.

Run with:
  celery -A watchdog worker --beat --loglevel=info
  (combines worker + beat scheduler in one process for simplicity)
"""

import logging
import os
from datetime import datetime

import redis as sync_redis
from celery import Celery
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

REDIS_URL               = os.getenv("REDIS_URL", "redis://localhost:6379/0")
IDTAG_PREFIX            = os.getenv("CHARGEFLOW_IDTAG_PREFIX", "CHARGEFLOW_")
STALE_THRESHOLD_MINUTES = 5     # fire after 5 min silence on CHARGING session
MIN_BILLABLE_KWH        = 0.05  # sessions below this are not billed

app = Celery("watchdog", broker=REDIS_URL)

app.conf.beat_schedule = {
    "scan-stale-sessions": {
        "task":     "watchdog.scan_stale_sessions",
        "schedule": 300.0,   # every 5 minutes
    }
}
app.conf.timezone = "UTC"


@app.task(name="watchdog.scan_stale_sessions")
def scan_stale_sessions():
    """
    Scans all Redis session keys and handles:
      - CHARGING sessions with stale last_meter_ts (power failure)
      - PENDING_PAYMENT sessions older than 30 min (missed UPI trigger)
    """
    r   = sync_redis.Redis.from_url(REDIS_URL, decode_responses=True)
    now = datetime.utcnow()

    keys     = r.keys("session:*")
    total    = len(keys)
    handled  = 0

    log.info(f"Watchdog scan started — checking {total} session(s)")

    for key in keys:
        session = r.hgetall(key)
        if not session:
            continue

        charger_id     = session.get("charger_id", "")
        transaction_id = int(session.get("transaction_id", 0))
        status         = session.get("status", "")

        if status == "CHARGING":
            if _handle_stale_charging(session, charger_id,
                                       transaction_id, now, r):
                handled += 1

        elif status == "PENDING_PAYMENT":
            if _handle_stale_pending(session, charger_id,
                                      transaction_id, now):
                handled += 1

    log.info(f"Watchdog scan complete — {handled}/{total} session(s) acted on")


def _handle_stale_charging(session: dict, charger_id: str,
                             transaction_id: int,
                             now: datetime,
                             r: sync_redis.Redis) -> bool:
    """
    Checks if a CHARGING session has gone silent.
    If last_meter_ts is older than STALE_THRESHOLD_MINUTES:
      - Finalises session with last known kWh
      - Triggers UPI collect
    Returns True if action was taken.
    """
    last_meter_ts = session.get("last_meter_ts")
    if not last_meter_ts:
        log.warning(f"[{charger_id}] CHARGING session has no last_meter_ts "
                    f"— skipping txn={transaction_id}")
        return False

    try:
        last_seen      = datetime.fromisoformat(last_meter_ts)
        minutes_silent = (now - last_seen).total_seconds() / 60
    except ValueError:
        log.error(f"[{charger_id}] Invalid last_meter_ts format: {last_meter_ts}")
        return False

    if minutes_silent < STALE_THRESHOLD_MINUTES:
        return False   # session is still active

    kwh_live  = float(session.get("kwh_live",  0))
    kwh_start = float(session.get("kwh_start", 0))
    kwh_consumed = kwh_live - kwh_start

    log.warning(
        f"[{charger_id}] STALE CHARGING SESSION detected | "
        f"txn={transaction_id} | "
        f"silent={minutes_silent:.1f} min | "
        f"kwh_consumed={kwh_consumed:.3f}"
    )

    # Near-zero consumption — likely a failed session start, not a real charge
    if kwh_consumed < MIN_BILLABLE_KWH:
        log.info(f"[{charger_id}] Stale session — near-zero kWh, marking FAILED")
        import session_store as store
        store.mark_failed(
            charger_id, transaction_id,
            reason=f"Stale session: {kwh_consumed:.3f} kWh below billing threshold"
        )
        return True

    # Finalise with last known kWh — slightly underbills rather than overbills
    # This is the correct behaviour: always favour the driver on uncertain data
    log.info(
        f"[{charger_id}] Finalising stale session | "
        f"txn={transaction_id} | "
        f"using last known meter: {kwh_live:.3f} kWh"
    )

    import session_store as store
    session_data = store.finalise_session(charger_id, transaction_id, kwh_live)

    if not session_data:
        log.error(f"[{charger_id}] finalise_session returned None — txn={transaction_id}")
        return False

    # Trigger UPI collect for last known amount
    driver_phone = session.get("id_tag", "").replace(IDTAG_PREFIX, "")
    if not driver_phone:
        log.error(f"[{charger_id}] No driver phone — cannot trigger collect")
        return False

    from upi_collect import trigger_collect
    trigger_collect(
        charger_id=charger_id,
        transaction_id=transaction_id,
        amount_inr=session_data.get("cost_final", "0.00"),
        driver_phone=driver_phone,
        driver_name="EV Driver"
    )
    log.info(
        f"[{charger_id}] Watchdog UPI collect triggered | "
        f"txn={transaction_id} | "
        f"amount=₹{session_data.get('cost_final')}"
    )
    return True


def _handle_stale_pending(session: dict, charger_id: str,
                            transaction_id: int,
                            now: datetime) -> bool:
    """
    Checks if a PENDING_PAYMENT session has been waiting too long.
    If updated_at is older than 30 minutes, re-triggers UPI collect.
    This handles: bridge restarted between StopTransaction and collect trigger.
    Returns True if action was taken.
    """
    updated_at = session.get("updated_at")
    if not updated_at:
        return False

    try:
        age_minutes = (now - datetime.fromisoformat(updated_at)).total_seconds() / 60
    except ValueError:
        return False

    if age_minutes < 30:
        return False   # still within normal window

    retry_count = int(session.get("retry_count", 0))

    log.warning(
        f"[{charger_id}] PENDING_PAYMENT stuck {age_minutes:.0f} min | "
        f"txn={transaction_id} | "
        f"retry_count={retry_count}"
    )

    driver_phone = session.get("id_tag", "").replace(IDTAG_PREFIX, "")
    cost_final   = session.get("cost_final", "0.00")

    if not driver_phone or cost_final == "0.00":
        log.error(f"[{charger_id}] Cannot re-trigger — missing phone or cost")
        return False

    from upi_collect import trigger_collect
    trigger_collect(
        charger_id=charger_id,
        transaction_id=transaction_id,
        amount_inr=cost_final,
        driver_phone=driver_phone,
        driver_name="EV Driver"
    )
    log.info(
        f"[{charger_id}] Watchdog re-triggered UPI collect | "
        f"txn={transaction_id} | amount=₹{cost_final}"
    )
    return True
