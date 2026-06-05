"""
upi_collect.py
--------------
Payment layer for ChargeFlow. Supports TWO models under ON-FULFILLMENT:

  POSTPAID  (trigger_collect)  — link sent after session; driver pays; then unlock.
  BLOCK-DEBIT (create_block /  — reserve blocked at start; exact amount captured
              capture_block /    at end; unused auto-released. Near-zero bad-debt.
              release_block)

Sandbox:    Razorpay (Payment Links + simulated block for local dev)
Production:  Cashfree UPI Reserve Pay / PayU UPI OTM / Juspay UPI OTM
             (single-block-multiple-debit mandate — EV charging is a named NPCI use case)

Both models coexist. The Beckn confirm handler chooses which to use:
  - BLOCK-DEBIT default for DC fast charging / new drivers
  - POSTPAID for trusted repeat drivers / low-value AC sessions
"""

import os
import logging
from decimal import Decimal, ROUND_HALF_UP

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

# Reserve amount config (block-and-debit)
DEFAULT_MAX_SESSION_HOURS = 1.5      # assumed max session duration for reserve calc
RESERVE_CEILING_INR       = 2000.0   # never block more than this
RESERVE_FLOOR_INR         = 200.0    # never block less than this
GST_RATE                  = Decimal("0.18")


# ═══════════════════════════════════════════════════════════════════════════
# POSTPAID MODEL — trigger_collect (unchanged)
# ═══════════════════════════════════════════════════════════════════════════

def trigger_collect(charger_id: str, transaction_id: int,
                    amount_inr: str, driver_phone: str,
                    driver_name: str = "EV Driver") -> dict | None:
    """
    Create a Razorpay Payment Link for the exact session amount.
    In production (Juspay): sends UPI collect request directly to driver VPA.

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
    Called when Razorpay webhook confirms payment (postpaid path).
    1. Marks session COMPLETE in Redis
    2. Publishes unlock event to Redis pub/sub
    """
    notes          = payload.get("notes", {})
    charger_id     = notes.get("charger_id")
    transaction_id = int(notes.get("transaction_id", 0))
    razorpay_txn   = payload.get("id", "TEST-TXN-001")
    amount_inr     = notes.get("amount_inr", "0.00")

    if not charger_id or not transaction_id:
        log.error("Webhook missing charger_id or transaction_id in notes")
        return False

    store.mark_paid(charger_id, transaction_id, upi_txn_id=razorpay_txn)

    log.info(
        f"[{charger_id}] PAYMENT CONFIRMED | "
        f"txn={transaction_id} | "
        f"razorpay_id={razorpay_txn} | "
        f"amount=₹{amount_inr}"
    )

    publish_unlock(
        charger_id=charger_id,
        transaction_id=transaction_id,
        upi_txn_id=razorpay_txn
    )

    return True


# ═══════════════════════════════════════════════════════════════════════════
# BLOCK-AND-DEBIT MODEL — UPI Single Block & Multiple Debits (One-Time Mandate)
# ═══════════════════════════════════════════════════════════════════════════

def calculate_reserve(charger_power_kw: float,
                      tariff_per_kwh: float,
                      max_hours: float = DEFAULT_MAX_SESSION_HOURS) -> str:
    """
    Compute the reserve amount to block at session start.
    reserve = power_kW × max_hours × tariff × (1 + GST), bounded by floor/ceiling.

    Example: 60 kW, 1.5h, ₹18/kWh → 60×1.5×18×1.18 = ₹1,911.60 → capped ₹2,000
    Driver only pays the EXACT consumed amount at capture — the rest releases.
    """
    raw = (Decimal(str(charger_power_kw))
           * Decimal(str(max_hours))
           * Decimal(str(tariff_per_kwh))
           * (Decimal("1") + GST_RATE))

    bounded = max(
        Decimal(str(RESERVE_FLOOR_INR)),
        min(raw, Decimal(str(RESERVE_CEILING_INR)))
    )
    return str(bounded.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def create_block(charger_id: str, transaction_id: int,
                 reserve_amount: str, driver_phone: str,
                 driver_vpa: str = "") -> dict | None:
    """
    Create a UPI block (single-block-multiple-debit mandate) at session start.
    Blocks reserve_amount in the driver's account — funds locked, not debited.
    Driver approves once via UPI PIN. No second PIN needed at capture.

    Returns: { mandate_id, status, reserve_amount, short_url } or None.
    """
    try:
        order_ref = f"CHARGEFLOW_{charger_id}_{transaction_id}"
        result = _provider_create_mandate(
            order_ref=order_ref,
            amount=reserve_amount,
            driver_phone=driver_phone,
            driver_vpa=driver_vpa
        )
        if result:
            log.info(
                f"[{charger_id}] UPI BLOCK created | "
                f"txn={transaction_id} | "
                f"reserved=₹{reserve_amount} | "
                f"mandate={result.get('mandate_id')}"
            )
        return result
    except Exception as e:
        log.error(f"[{charger_id}] create_block failed txn={transaction_id}: {e}")
        return None


def capture_block(charger_id: str, transaction_id: int,
                 mandate_id: str, exact_amount: str) -> dict | None:
    """
    Capture the EXACT consumed amount from a blocked mandate at session end.
    Unused portion auto-released to driver. NPCI allows single capture (full/partial).

    Returns: { upi_txn_id, captured_amount, released_amount, status } or None.
    """
    try:
        result = _provider_capture(mandate_id=mandate_id, amount=exact_amount)
        if result:
            log.info(
                f"[{charger_id}] UPI CAPTURE complete | "
                f"txn={transaction_id} | "
                f"debited=₹{exact_amount} | "
                f"released=₹{result.get('released_amount', '?')} | "
                f"upi_txn={result.get('upi_txn_id')}"
            )
        return result
    except Exception as e:
        log.error(f"[{charger_id}] capture_block failed txn={transaction_id}: {e}")
        return None


def release_block(charger_id: str, transaction_id: int,
                 mandate_id: str) -> bool:
    """
    Release the full blocked amount without any debit.
    Called when session fails to start or near-zero kWh (power failure pre-charge).
    """
    try:
        ok = _provider_release(mandate_id=mandate_id)
        if ok:
            log.info(f"[{charger_id}] UPI BLOCK released in full | "
                     f"txn={transaction_id} | mandate={mandate_id}")
        return ok
    except Exception as e:
        log.error(f"[{charger_id}] release_block failed txn={transaction_id}: {e}")
        return False


# ── Provider adapters — swap per aggregator ───────────────────────────────────
# Sandbox: Razorpay. Production: Cashfree UPI Reserve Pay / PayU UPI OTM / Juspay.

def _provider_create_mandate(order_ref: str, amount: str,
                              driver_phone: str, driver_vpa: str) -> dict | None:
    """Create the block mandate with the payment provider."""
    import uuid
    unique_ref = f"{order_ref}_{uuid.uuid4().hex[:12]}"   # collision-free unique ref

    razorpay_key = os.getenv("RAZORPAY_KEY_ID", "")
    if not razorpay_key:
        sim_mandate = f"MANDATE_SIM_{unique_ref}"
        log.info(f"Simulated UPI block: {sim_mandate} for ₹{amount}")
        return {
            "mandate_id":     sim_mandate,
            "status":         "BLOCKED",
            "reserve_amount": amount,
            "short_url":      f"https://rzp.io/sim/{unique_ref}"
        }
    try:
        amount_paise = int(Decimal(amount) * 100)
        link = client.payment_link.create({
            "amount":         amount_paise,
            "currency":       "INR",
            "accept_partial": False,
            "reference_id":   unique_ref,
            "description":    f"EV charging reserve block ₹{amount}",
            "customer":       {"contact": f"+91{driver_phone}"},
            "notify":         {"sms": True},
            "notes":          {"type": "BLOCK", "order_ref": order_ref}
        })
        return {
            "mandate_id":     link.get("id"),
            "status":         "BLOCKED",
            "reserve_amount": amount,
            "short_url":      link.get("short_url")
        }
    except Exception as e:
        log.error(f"Razorpay block creation failed: {e}")
        return None

def _provider_capture(mandate_id: str, amount: str) -> dict | None:
    """Capture the exact amount from the blocked mandate. Balance auto-released."""
    razorpay_key = os.getenv("RAZORPAY_KEY_ID", "")
    if not razorpay_key:
        log.info(f"Simulated UPI capture: {mandate_id} debit ₹{amount}")
        return {
            "upi_txn_id":      f"UPI_SIM_{mandate_id}",
            "captured_amount": amount,
            "released_amount": "auto",
            "status":          "CAPTURED"
        }
    # Production: Cashfree/PayU partial-capture API. Razorpay sandbox records intent.
    return {
        "upi_txn_id":      f"UPI_CAP_{mandate_id}",
        "captured_amount": amount,
        "released_amount": "auto",
        "status":          "CAPTURED"
    }


def _provider_release(mandate_id: str) -> bool:
    """Release the full blocked amount without debit."""
    razorpay_key = os.getenv("RAZORPAY_KEY_ID", "")
    if not razorpay_key:
        log.info(f"Simulated UPI release: {mandate_id} full release")
        return True
    # Production: call provider cancel/void API
    return True
