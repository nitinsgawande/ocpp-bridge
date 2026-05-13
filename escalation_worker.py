"""
escalation_worker.py
--------------------
Celery task that runs every 30 minutes to handle unpaid sessions.

3-tier escalation ladder for sessions stuck in PENDING_PAYMENT:

  Tier 1 (30 min, 60 min, 90 min) — Resend payment link
    Driver missed the notification. Resend up to 3 times.

  Tier 2 (2–24 hours) — SMS direct payment link
    Driver phone notification may have been missed. Send SMS directly.
    (SMS integration via Twilio / AWS SNS — stubbed for now)

  Tier 3 (24 hours) — Bad debt + cable release
    Energy is a sunk cost. Holding the cable longer serves no one.
    Release cable, mark session BAD_DEBT, add driver to watchlist.
    Next session from this driver requires ₹50 activation deposit.

This mirrors Uber's model: you cannot book the next ride until you
clear outstanding dues. The watchlist prevents future free-riding.
"""

import logging
import os
from datetime import datetime

import redis as sync_redis
from celery import Celery
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

REDIS_URL    = os.getenv("REDIS_URL", "redis://localhost:6379/0")
IDTAG_PREFIX = os.getenv("CHARGEFLOW_IDTAG_PREFIX", "CHARGEFLOW_")

# Escalation thresholds
MAX_RETRIES        = 3     # maximum UPI collect resend attempts
RETRY_INTERVAL_MIN = 30    # resend every 30 minutes
BAD_DEBT_HOURS     = 24    # release cable and mark bad debt after 24 hours

app = Celery("escalation_worker", broker=REDIS_URL)

app.conf.beat_schedule = {
    "escalate-unpaid-sessions": {
        "task":     "escalation_worker.escalate_unpaid_sessions",
        "schedule": 1800.0,   # every 30 minutes
    }
}
app.conf.timezone = "UTC"


@app.task(name="escalation_worker.escalate_unpaid_sessions")
def escalate_unpaid_sessions():
    """
    Scans all PENDING_PAYMENT sessions and applies the escalation ladder.
    """
    r   = sync_redis.Redis.from_url(REDIS_URL, decode_responses=True)
    now = datetime.utcnow()

    keys      = r.keys("session:*")
    escalated = 0

    for key in keys:
        session = r.hgetall(key)
        if not session:
            continue

        if session.get("status") != "PENDING_PAYMENT":
            continue

        charger_id     = session.get("charger_id", "")
        transaction_id = int(session.get("transaction_id", 0))
        retry_count    = int(session.get("retry_count", 0))

        updated_at = session.get("updated_at")
        if not updated_at:
            continue

        try:
            age_hours = (
                now - datetime.fromisoformat(updated_at)
            ).total_seconds() / 3600
        except ValueError:
            continue

        driver_phone = session.get("id_tag", "").replace(IDTAG_PREFIX, "")
        cost_final   = session.get("cost_final", "0.00")

        if not driver_phone or cost_final == "0.00":
            log.error(f"[{charger_id}] Cannot escalate — missing phone or cost")
            continue

        # ── Tier 3: Bad debt after 24 hours ──────────────────────────────────
        if age_hours >= BAD_DEBT_HOURS:
            _mark_bad_debt(charger_id, transaction_id,
                           driver_phone, cost_final, r)
            escalated += 1

        # ── Tier 1 & 2: Retry up to MAX_RETRIES times ────────────────────────
        elif retry_count < MAX_RETRIES:
            min_age_for_retry = (retry_count + 1) * RETRY_INTERVAL_MIN / 60
            if age_hours >= min_age_for_retry:
                _retry_collect(charger_id, transaction_id,
                               driver_phone, cost_final, retry_count, r)
                escalated += 1

    if escalated:
        log.info(f"Escalation worker: acted on {escalated} session(s)")


def _retry_collect(charger_id: str, transaction_id: int,
                    driver_phone: str, cost_final: str,
                    current_retry: int,
                    r: sync_redis.Redis) -> None:
    """
    Resend UPI collect request and increment retry counter.
    """
    from upi_collect import trigger_collect
    import session_store as store

    new_count = store.increment_retry(charger_id, transaction_id)

    log.info(
        f"[{charger_id}] Escalation retry {new_count}/{MAX_RETRIES} | "
        f"txn={transaction_id} | "
        f"driver={driver_phone} | "
        f"amount=₹{cost_final}"
    )

    result = trigger_collect(
        charger_id=charger_id,
        transaction_id=transaction_id,
        amount_inr=cost_final,
        driver_phone=driver_phone,
        driver_name="EV Driver"
    )

    if result:
        log.info(
            f"[{charger_id}] Retry payment link sent | "
            f"link={result.get('short_url')}"
        )
    else:
        log.error(f"[{charger_id}] Retry trigger_collect failed")

    # Tier 2 — also send direct SMS if on retry 2 or 3
    if new_count >= 2:
        _send_sms_reminder(driver_phone, cost_final, charger_id, transaction_id)


def _send_sms_reminder(driver_phone: str, amount: str,
                        charger_id: str, transaction_id: int) -> None:
    """
    Send direct SMS reminder with payment link.
    Stubbed for now — integrate Twilio / AWS SNS / MSG91 in production.
    """
    message = (
        f"Reminder: Your EV charging session at {charger_id} "
        f"is unpaid. Amount due: ₹{amount}. "
        f"Pay now to avoid your account being blocked: "
        f"https://pay.chargeflow.in/{charger_id}/{transaction_id}"
    )
    # TODO: integrate SMS provider
    # import boto3
    # sns = boto3.client("sns", region_name="ap-south-1")
    # sns.publish(PhoneNumber=f"+91{driver_phone}", Message=message)
    log.info(f"SMS reminder (stub): to={driver_phone} | msg={message}")


def _mark_bad_debt(charger_id: str, transaction_id: int,
                    driver_phone: str, cost_final: str,
                    r: sync_redis.Redis) -> None:
    """
    After 24 hours of non-payment:
      1. Release the cable — sunk cost, no value in keeping locked
      2. Mark session as BAD_DEBT in Redis
      3. Add driver phone to watchlist set in Redis
      4. Log for reconciliation with CPO

    Next time this driver scans a ChargeFlow QR, your BPP checks
    the watchlist and requires a ₹50 activation deposit before
    starting the session.
    """
    import session_store as store
    from cable_lock import publish_unlock

    log.warning(
        f"[{charger_id}] BAD DEBT after 24hr | "
        f"txn={transaction_id} | "
        f"driver={driver_phone} | "
        f"amount=₹{cost_final}"
    )

    # Release cable — energy is sunk cost
    store.mark_failed(
        charger_id, transaction_id,
        reason=f"BAD_DEBT_24HR — ₹{cost_final} unpaid"
    )
    publish_unlock(
        charger_id=charger_id,
        transaction_id=transaction_id,
        upi_txn_id="BAD_DEBT_RELEASE"
    )
    log.info(f"[{charger_id}] Cable released after bad debt | txn={transaction_id}")

    # Add driver to watchlist — requires deposit on next session
    r.sadd("watchlist:drivers", driver_phone)
    log.warning(
        f"Driver {driver_phone} added to watchlist — "
        f"next session requires ₹50 activation deposit"
    )

    # Log for CPO reconciliation report
    r.rpush("reconciliation:bad_debt", str({
        "charger_id":     charger_id,
        "transaction_id": transaction_id,
        "driver_phone":   driver_phone,
        "amount":         cost_final,
        "timestamp":      datetime.utcnow().isoformat()
    }))


def is_driver_on_watchlist(driver_phone: str) -> bool:
    """
    Check if a driver is on the bad debt watchlist.
    Called by the Beckn BPP confirm handler before starting a session.
    Returns True if driver requires an activation deposit.
    """
    r = sync_redis.Redis.from_url(REDIS_URL, decode_responses=True)
    return r.sismember("watchlist:drivers", driver_phone)
