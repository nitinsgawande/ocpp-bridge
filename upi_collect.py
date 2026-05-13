"""
upi_collect.py
--------------
Triggers a payment request after an EV charging session ends.

Sandbox:    Razorpay Payment Links (driver gets a link to pay via UPI)
Production: Swap to Juspay UPI collect (direct VPA debit request)

Flow:
  1. StopTransaction fires in csms.py
  2. csms.py calls trigger_collect()
  3. Razorpay creates a payment link for the exact amount
  4. Link sent to driver's phone via Razorpay SMS
  5. Driver pays → Razorpay fires webhook → on_payment_success() called
  6. on_payment_success() calls session_store.mark_paid()
  7. on_payment_success() publishes to Redis unlock channel
  8. csms.py subscriber receives unlock event → sends OCPP UnlockConnector
  9. Cable releases
"""

import os
import logging
import razorpay
from dotenv import load_dotenv
import session_store as store
from cable_lock import publish_unlock

load_dotenv()
log = logging.getLogger(__name__)

# Razorpay client — initialised once at module load
client = razorpay.Client(
    auth=(
        os.getenv("RAZORPAY_KEY_ID"),
        os.getenv("RAZORPAY_KEY_SECRET")
    )
)

# Retry config
MAX_RETRIES   = 3
RETRY_DELAY_S = 30   # seconds between retries in production


def trigger_collect(charger_id: str, transaction_id: int,
                    amount_inr: str, driver_phone: str,
                    driver_name: str = "EV Driver") -> dict | None:
    """
    Create a Razorpay Payment Link for the exact session amount.
    In production (Juspay): sends UPI collect request directly to driver VPA.

    Args:
        charger_id    : e.g. 'CHARGER-001'
        transaction_id: OCPP transaction ID
        amount_inr    : Final amount as string e.g. '206.67'
        driver_phone  : Driver's mobile number e.g. '9876543210'
        driver_name   : Driver's name for the payment link

    Returns:
        dict with payment_link_id and short_url, or None on failure
    """
    amount_paise = int(float(amount_inr) * 100)

    payload = {
        "amount":         amount_paise,
        "currency":       "INR",
        "accept_partial": False,
        "description":    f"EV Charging — Session {transaction_id} "
                          f"at {charger_id}",
        "customer": {
            "name":    driver_name,
            "contact": f"+91{driver_phone}",
        },
        "notify": {
            "sms":   True,
            "email": False
        },
        "reminder_enable": True,
        "notes": {
            "charger_id":     charger_id,
            "transaction_id": str(transaction_id),
            "amount_inr":     amount_inr,
            "source":         "ev-charging-bridge"
        },
        "callback_url":    "https://your-domain.com/webhook/razorpay",
        "callback_method": "get"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.payment_link.create(payload)
            log.info(
                f"[{charger_id}] Payment link CREATED | "
                f"txn={transaction_id} | "
                f"amount=₹{amount_inr} | "
                f"link={response.get('short_url')} | "
                f"link_id={response.get('id')}"
            )
            return {
                "payment_link_id": response.get("id"),
                "short_url":       response.get("short_url"),
                "amount_inr":      amount_inr,
                "status":          response.get("status")
            }
        except Exception as e:
            log.error(
                f"[{charger_id}] Payment link attempt {attempt}/{MAX_RETRIES} "
                f"FAILED | txn={transaction_id} | error={e}"
            )
            if attempt == MAX_RETRIES:
                log.error(
                    f"[{charger_id}] All retries exhausted. "
                    f"Marking session FAILED. Cable stays locked."
                )
                store.mark_failed(
                    charger_id, transaction_id,
                    reason=f"Payment link creation failed: {e}"
                )
                return None
            import time
            time.sleep(2)

    return None


def on_payment_success(payload: dict) -> bool:
    """
    Called when Razorpay webhook confirms payment.
    In production: called from your FastAPI/Flask webhook endpoint.
    In local test: called manually to simulate webhook.

    1. Marks session COMPLETE in Redis
    2. Publishes unlock event to Redis pub/sub
    3. csms.py subscriber receives event and sends OCPP UnlockConnector
    """
    notes          = payload.get("notes", {})
    charger_id     = notes.get("charger_id")
    transaction_id = int(notes.get("transaction_id", 0))
    razorpay_txn   = payload.get("id", "TEST-TXN-001")
    amount_inr     = notes.get("amount_inr", "0.00")

    if not charger_id or not transaction_id:
        log.error("Webhook missing charger_id or transaction_id in notes")
        return False

    # Step 1 — Move session PENDING_PAYMENT → COMPLETE
    store.mark_paid(charger_id, transaction_id, upi_txn_id=razorpay_txn)

    log.info(
        f"[{charger_id}] PAYMENT CONFIRMED | "
        f"txn={transaction_id} | "
        f"razorpay_id={razorpay_txn} | "
        f"amount=₹{amount_inr}"
    )

    # Step 2 — Publish unlock event → csms.py sends OCPP UnlockConnector
    publish_unlock(
        charger_id=charger_id,
        transaction_id=transaction_id,
        upi_txn_id=razorpay_txn
    )

    return True
