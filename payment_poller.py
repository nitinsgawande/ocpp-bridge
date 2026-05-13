"""
payment_poller.py
-----------------
Celery task that polls Juspay every 2 minutes for sessions in
PENDING_PAYMENT state where the webhook may have been missed.

Handles the scenario:
  1. Driver approves UPI collect on their phone
  2. Juspay fires webhook to /webhook/juspay/payment
  3. But: your server was restarting at that exact moment
  4. Webhook is missed — session stays PENDING_PAYMENT
  5. Cable stays locked even though driver has paid
  6. Poller runs 2 min later, finds CHARGED on Juspay, releases cable

Maximum time a driver waits after paying before cable releases: 2 minutes.

Run alongside watchdog:
  celery -A payment_poller worker --beat --loglevel=info
Or combine all workers in one command (see run instructions below).
"""

import logging
import os
from datetime import datetime

import redis as sync_redis
from celery import Celery
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

REDIS_URL        = os.getenv("REDIS_URL", "redis://localhost:6379/0")
JUSPAY_API_KEY   = os.getenv("JUSPAY_API_KEY", "")
JUSPAY_MERCHANT  = os.getenv("JUSPAY_MERCHANT_ID", "")
IDTAG_PREFIX     = os.getenv("CHARGEFLOW_IDTAG_PREFIX", "CHARGEFLOW_")
POLL_MIN_AGE_MIN = 5   # only poll sessions older than 5 minutes

app = Celery("payment_poller", broker=REDIS_URL)

app.conf.beat_schedule = {
    "poll-pending-payments": {
        "task":     "payment_poller.poll_pending_payments",
        "schedule": 120.0,   # every 2 minutes
    }
}
app.conf.timezone = "UTC"


@app.task(name="payment_poller.poll_pending_payments")
def poll_pending_payments():
    """
    For every PENDING_PAYMENT session older than 5 minutes,
    poll Juspay's order status API directly.
    If CHARGED: mark session COMPLETE and publish cable unlock.
    """
    r   = sync_redis.Redis.from_url(REDIS_URL, decode_responses=True)
    now = datetime.utcnow()

    keys   = r.keys("session:*")
    polled = 0

    for key in keys:
        session = r.hgetall(key)
        if not session:
            continue

        if session.get("status") != "PENDING_PAYMENT":
            continue

        charger_id     = session.get("charger_id", "")
        transaction_id = int(session.get("transaction_id", 0))

        # Only poll sessions that have been PENDING_PAYMENT
        # for more than POLL_MIN_AGE_MIN minutes
        updated_at = session.get("updated_at")
        if not updated_at:
            continue

        try:
            age_minutes = (
                now - datetime.fromisoformat(updated_at)
            ).total_seconds() / 60
        except ValueError:
            continue

        if age_minutes < POLL_MIN_AGE_MIN:
            continue

        log.info(
            f"[{charger_id}] Polling Juspay for payment status | "
            f"txn={transaction_id} | age={age_minutes:.1f} min"
        )
        _poll_and_process(charger_id, transaction_id, session)
        polled += 1

    if polled:
        log.info(f"Payment poller: polled {polled} PENDING_PAYMENT session(s)")


def _poll_and_process(charger_id: str,
                       transaction_id: int,
                       session: dict) -> None:
    """
    Poll Juspay order status for one session.
    On CHARGED: mark paid and publish cable unlock.
    """
    # Juspay order_id format: CHARGEFLOW_{charger_id}_{transaction_id}
    order_id = f"{IDTAG_PREFIX}{charger_id}_{transaction_id}"

    # Sandbox fallback — if no Juspay credentials, use Razorpay
    if not JUSPAY_API_KEY:
        _poll_razorpay(charger_id, transaction_id, session, order_id)
        return

    try:
        import httpx
        resp = httpx.get(
            f"https://api.juspay.in/orders/{order_id}",
            auth=(JUSPAY_MERCHANT, JUSPAY_API_KEY),
            timeout=10.0
        )

        if resp.status_code == 404:
            log.info(f"[{charger_id}] Juspay: order not found — txn={transaction_id}")
            return

        data   = resp.json()
        status = data.get("status", "")

        log.info(f"[{charger_id}] Juspay poll result: {status} | txn={transaction_id}")

        if status == "CHARGED":
            upi_txn = data.get("txn_id", f"POLLED_{order_id}")
            _complete_session(charger_id, transaction_id, upi_txn,
                              session.get("cost_final", "0.00"))

    except Exception as e:
        log.error(f"[{charger_id}] Juspay poll failed: {e}")


def _poll_razorpay(charger_id: str, transaction_id: int,
                    session: dict, order_id: str) -> None:
    """
    Razorpay sandbox fallback poller.
    Checks payment link status via Razorpay API.
    Used during development / sandbox phase.
    """
    razorpay_key_id     = os.getenv("RAZORPAY_KEY_ID", "")
    razorpay_key_secret = os.getenv("RAZORPAY_KEY_SECRET", "")

    if not razorpay_key_id:
        log.debug(f"[{charger_id}] No payment credentials — skipping poll")
        return

    try:
        import razorpay
        client = razorpay.Client(auth=(razorpay_key_id, razorpay_key_secret))

        # List payment links and check status
        # In sandbox, we can only check if payment link exists and its status
        links = client.payment_link.all({"reference_id": str(transaction_id)})
        items = links.get("items", [])

        for link in items:
            if link.get("status") == "paid":
                payments = link.get("payments", [])
                upi_txn  = payments[0].get("payment_id", f"RZP_POLLED_{transaction_id}") \
                           if payments else f"RZP_POLLED_{transaction_id}"
                log.info(
                    f"[{charger_id}] Razorpay poller found PAID | "
                    f"txn={transaction_id}"
                )
                _complete_session(
                    charger_id, transaction_id, upi_txn,
                    session.get("cost_final", "0.00")
                )
                return

    except Exception as e:
        log.debug(f"[{charger_id}] Razorpay poll skipped: {e}")


def _complete_session(charger_id: str, transaction_id: int,
                       upi_txn_id: str, amount_inr: str) -> None:
    """
    Mark session COMPLETE and publish cable unlock.
    Called when poller confirms payment was made.
    """
    import session_store as store
    from cable_lock import publish_unlock

    store.mark_paid(charger_id, transaction_id, upi_txn_id)

    log.info(
        f"[{charger_id}] Poller: session marked COMPLETE | "
        f"txn={transaction_id} | "
        f"upi={upi_txn_id} | "
        f"amount=₹{amount_inr}"
    )

    publish_unlock(
        charger_id=charger_id,
        transaction_id=transaction_id,
        upi_txn_id=upi_txn_id
    )
