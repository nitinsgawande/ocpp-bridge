"""
test_tariff.py
--------------
Run with: python3 test_tariff.py
No external dependencies — uses only tariff_calculator.py
"""

from datetime import datetime
from tariff_calculator import compute_cost, format_receipt, get_applicable_rate


def run_test(name, cpo_id, kwh, started_at,
             expected_total, expected_rate_type):
    result = compute_cost(cpo_id, kwh, started_at)
    passed_total = result["total_payable"] == expected_total
    passed_type  = result["rate_type"]     == expected_rate_type

    status = "✅ PASS" if (passed_total and passed_type) else "❌ FAIL"
    print(f"{status} | {name}")

    if not passed_total:
        print(f"       total: expected ₹{expected_total} "
              f"got ₹{result['total_payable']}")
    if not passed_type:
        print(f"       rate_type: expected '{expected_rate_type}' "
              f"got '{result['rate_type']}'")
    return passed_total and passed_type


def main():
    print("\n=== Tariff Calculator Tests ===\n")

    all_passed = True

    # -----------------------------------------------------------------------
    # Test 1 — Standard session, base rate
    # Pulse Energy, 9.73 kWh, 3pm IST (not peak, not solar → base ₹18)
    # base = 9.73 × 18 = 175.14
    # gst  = 175.14 × 0.18 = 31.5252 → rounds to 31.53
    # total = 175.14 + 31.53 = 206.67
    # -----------------------------------------------------------------------
    all_passed &= run_test(
        name="Base rate — Pulse Energy 9.73 kWh at 3pm",
        cpo_id="pulse-energy",
        kwh=9.73,
        started_at=datetime(2025, 4, 14, 15, 0),   # 3pm IST — base hour
        expected_total="206.67",
        expected_rate_type="base"
    )

    # -----------------------------------------------------------------------
    # Test 2 — Peak rate
    # Pulse Energy, 5.0 kWh, 8pm IST (peak hours 18–23 → ₹22/kWh)
    # base = 5.0 × 22 = 110.00
    # gst  = 110.00 × 0.18 = 19.80
    # total = 110.00 + 19.80 = 129.80
    # -----------------------------------------------------------------------
    all_passed &= run_test(
        name="Peak rate — Pulse Energy 5.0 kWh at 8pm",
        cpo_id="pulse-energy",
        kwh=5.0,
        started_at=datetime(2025, 4, 14, 20, 0),   # 8pm IST — peak
        expected_total="129.80",
        expected_rate_type="peak"
    )

    # -----------------------------------------------------------------------
    # Test 3 — Solar rate
    # Pulse Energy, 10.0 kWh, 12pm IST (solar hours 11–15 → ₹14/kWh)
    # base = 10.0 × 14 = 140.00
    # gst  = 140.00 × 0.18 = 25.20
    # total = 140.00 + 25.20 = 165.20
    # -----------------------------------------------------------------------
    all_passed &= run_test(
        name="Solar rate — Pulse Energy 10.0 kWh at 12pm",
        cpo_id="pulse-energy",
        kwh=10.0,
        started_at=datetime(2025, 4, 14, 12, 0),   # 12pm IST — solar
        expected_total="165.20",
        expected_rate_type="solar"
    )

    # -----------------------------------------------------------------------
    # Test 4 — Different CPO (Kazam)
    # Kazam, 3.5 kWh, 6am (base hours → ₹17/kWh)
    # base = 3.5 × 17 = 59.50
    # gst  = 59.50 × 0.18 = 10.71
    # total = 59.50 + 10.71 = 70.21
    # -----------------------------------------------------------------------
    all_passed &= run_test(
        name="Kazam base rate — 3.5 kWh at 6am",
        cpo_id="kazam",
        kwh=3.5,
        started_at=datetime(2025, 4, 14, 6, 0),    # 6am IST — base
        expected_total="70.21",
        expected_rate_type="base"
    )

    # -----------------------------------------------------------------------
    # Test 5 — Unknown CPO falls back to default
    # Default ₹18/kWh base, 2.0 kWh
    # base = 2.0 × 18 = 36.00
    # gst  = 36.00 × 0.18 = 6.48
    # total = 36.00 + 6.48 = 42.48
    # -----------------------------------------------------------------------
    all_passed &= run_test(
        name="Unknown CPO falls back to default",
        cpo_id="unknown-cpo-xyz",
        kwh=2.0,
        started_at=datetime(2025, 4, 14, 10, 0),
        expected_total="42.48",
        expected_rate_type="base"
    )

    # -----------------------------------------------------------------------
    # Test 6 — Very small session (rounding check)
    # Pulse Energy, 0.5 kWh, base rate ₹18
    # base = 0.5 × 18 = 9.00
    # gst  = 9.00 × 0.18 = 1.62
    # total = 9.00 + 1.62 = 10.62
    # -----------------------------------------------------------------------
    all_passed &= run_test(
        name="Small session rounding — 0.5 kWh",
        cpo_id="pulse-energy",
        kwh=0.5,
        started_at=datetime(2025, 4, 14, 16, 0),   # 4pm — base
        expected_total="10.62",
        expected_rate_type="base"
    )

    # -----------------------------------------------------------------------
    # Print a sample receipt
    # -----------------------------------------------------------------------
    print("\n=== Sample Receipt ===\n")
    result = compute_cost(
        cpo_id="pulse-energy",
        kwh=9.73,
        started_at=datetime(2025, 4, 14, 20, 0)   # peak
    )
    print(format_receipt(result))

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print(f"\n{'All tests passed ✅' if all_passed else 'Some tests FAILED ❌'}\n")


if __name__ == "__main__":
    main()
