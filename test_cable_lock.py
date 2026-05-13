"""
test_cable_lock.py
------------------
Simulates the webhook arriving after a session ends.
Run this in Terminal 3 WHILE csms.py and charger_simulator.py are running.

What this tests:
  1. on_payment_success() marks session COMPLETE
  2. publish_unlock() publishes to Redis channel unlock:CHARGER-001
  3. csms.py subscriber receives the message
  4. csms.py sends OCPP UnlockConnector to charger_simulator.py
  5. charger_simulator.py logs "CABLE RELEASED"

Run order:
  Terminal 1: python3 csms.py
  Terminal 2: python3 charger_simulator.py
  Terminal 3: python3 test_cable_lock.py   ← after session ends
"""

import logging
import time
import session_store as store
from upi_collect import on_payment_success

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def simulate_webhook():
    print("\n=== Cable Lock Test — Simulating Payment Webhook ===\n")

    charger_id     = "CHARGER-001"
    transaction_id = 1001   # must match transaction_id in csms.py

    # Check session is in PENDING_PAYMENT state
    session = store.get_session(charger_id, transaction_id)

    if session is None:
        print("❌ Session not found in Redis.")
        print("   Make sure csms.py is running and charger_simulator.py")
        print("   has completed StopTransaction first.")
        return

    print(f"Session found in Redis:")
    print(f"  status    : {session['status']}")
    print(f"  kwh_total : {session['kwh_total']} kWh")
    print(f"  cost_final: ₹{session['cost_final']}\n")

    if session["status"] != "PENDING_PAYMENT":
        print(f"❌ Expected status PENDING_PAYMENT, "
              f"got {session['status']}")
        print("   Run charger_simulator.py first to complete a session.")
        return

    # Simulate Razorpay webhook payload
    print("Firing simulated payment webhook...")
    simulated_webhook = {
        "id":     "pay_CABLETEST001",
        "status": "captured",
        "notes": {
            "charger_id":     charger_id,
            "transaction_id": str(transaction_id),
            "amount_inr":     session["cost_final"],
            "source":         "ev-charging-bridge"
        }
    }

    success = on_payment_success(simulated_webhook)

    if not success:
        print("❌ on_payment_success returned False")
        return

    # Brief wait for pub/sub delivery and OCPP round-trip
    time.sleep(2)

    # Verify session is COMPLETE in Redis
    session_after = store.get_session(charger_id, transaction_id)
    print(f"\nSession after webhook:")
    print(f"  status    : {session_after['status']}")
    print(f"  upi_txn_id: {session_after['upi_txn_id']}\n")

    if session_after["status"] == "COMPLETE":
        print("✅ Session COMPLETE in Redis")
        print("✅ Check Terminal 1 (csms.py) for: "
              "'OCPP UnlockConnector sent'")
        print("✅ Check Terminal 2 (charger_simulator.py) for: "
              "'CABLE RELEASED'")
    else:
        print(f"❌ Expected COMPLETE, got {session_after['status']}")


if __name__ == "__main__":
    simulate_webhook()
