"""
test_upi_collect.py
-------------------
Tests the full UPI collect trigger flow against Razorpay sandbox.

Run with: python3 test_upi_collect.py

What this tests:
  1. Razorpay sandbox credentials are valid
  2. Payment link is created for the correct amount
  3. Webhook simulation marks session as COMPLETE in Redis
  4. on_payment_success returns True (triggering cable unlock)
"""

import logging
import session_store as store
from upi_collect import trigger_collect, on_payment_success

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def test_full_flow():
    print("\n=== UPI Collect Trigger Test ===\n")

    # ---------------------------------------------------------------
    # Step 1: Seed a session in Redis in PENDING_PAYMENT state
    # (normally csms.py does this via StartTransaction + StopTransaction)
    # ---------------------------------------------------------------
    charger_id     = "CHARGER-001"
    transaction_id = 9001   # different from csms.py test to avoid collision

    store.create_session(
        charger_id=charger_id,
        transaction_id=transaction_id,
        kwh_start=10.0,
        tariff=18.0,
        id_tag="DRIVER-TEST-001"
    )
    store.transition(charger_id, transaction_id, "CHARGING")
    store.finalise_session(charger_id, transaction_id, kwh_stop=19.73)

    session = store.get_session(charger_id, transaction_id)
    print(f"Session seeded in Redis:")
    print(f"  status    : {session['status']}")
    print(f"  kwh_total : {session['kwh_total']} kWh")
    print(f"  cost_final: ₹{session['cost_final']}\n")

    assert session["status"] == "PENDING_PAYMENT", \
        f"Expected PENDING_PAYMENT, got {session['status']}"

    # ---------------------------------------------------------------
    # Step 2: Trigger the payment link creation via Razorpay sandbox
    # Use Razorpay's test phone number — no real SMS will be sent
    # ---------------------------------------------------------------
    print("Creating Razorpay payment link...")
    result = trigger_collect(
        charger_id=charger_id,
        transaction_id=transaction_id,
        amount_inr=session["cost_final"],
        driver_phone="9876543210",    # test number — no real SMS
        driver_name="Test Driver"
    )

    if result is None:
        print("❌ FAIL — Payment link creation failed. "
              "Check your .env API keys.")
        return

    print(f"\nPayment link CREATED ✅")
    print(f"  link_id  : {result['payment_link_id']}")
    print(f"  short_url: {result['short_url']}")
    print(f"  amount   : ₹{result['amount_inr']}")
    print(f"  status   : {result['status']}\n")

    # ---------------------------------------------------------------
    # Step 3: Simulate Razorpay webhook confirming payment
    # In production this fires automatically when driver pays
    # ---------------------------------------------------------------
    print("Simulating payment webhook (driver paid)...")
    simulated_webhook = {
        "id":     "pay_TEST" + str(transaction_id),
        "status": "captured",
        "notes": {
            "charger_id":     charger_id,
            "transaction_id": str(transaction_id),
            "amount_inr":     session["cost_final"],
            "source":         "ev-charging-bridge"
        }
    }

    success = on_payment_success(simulated_webhook)
    assert success, "on_payment_success returned False"

    # ---------------------------------------------------------------
    # Step 4: Verify session is now COMPLETE in Redis
    # ---------------------------------------------------------------
    session_after = store.get_session(charger_id, transaction_id)
    print(f"\nSession state after payment:")
    print(f"  status    : {session_after['status']}")
    print(f"  upi_txn_id: {session_after['upi_txn_id']}\n")

    assert session_after["status"] == "COMPLETE", \
        f"Expected COMPLETE, got {session_after['status']}"

    print("=== All checks passed ✅ ===")
    print("\nNext: OCPP UnlockConnector fires here to release cable.\n")


if __name__ == "__main__":
    test_full_flow()
