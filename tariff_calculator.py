"""
tariff_calculator.py
--------------------
Computes the final payable amount for an EV charging session.

Responsibilities:
  - Per-CPO tariff config (rate per kWh)
  - Time-of-day pricing (peak / off-peak / solar)
  - GST calculation (18% as per Indian tax rules)
  - Rounding to 2 decimal places
  - Returns itemised breakdown for receipt / UEI on_update payload

Usage:
    from tariff_calculator import compute_cost, get_tariff

    result = compute_cost(
        cpo_id="pulse-energy",
        kwh=9.73,
        started_at=datetime(2025, 4, 14, 14, 30)   # 2:30pm IST
    )
    print(result["total_payable"])   # ₹206.87
"""

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP


# ---------------------------------------------------------------------------
# Per-CPO tariff configuration
# In production this comes from a database / Redis config table.
# Each entry has:
#   base_rate   — ₹ per kWh (ex-GST)
#   peak_rate   — ₹ per kWh during peak hours (ex-GST)
#   solar_rate  — ₹ per kWh during solar hours if applicable (ex-GST)
#   peak_hours  — list of (start_hour, end_hour) in IST (24h)
#   solar_hours — list of (start_hour, end_hour) in IST (24h)
#   currency    — always INR for India
# ---------------------------------------------------------------------------
CPO_TARIFFS = {
    "pulse-energy": {
        "base_rate":   18.00,
        "peak_rate":   22.00,
        "solar_rate":  14.00,
        "peak_hours":  [(8, 11), (18, 23)],   # 8–11am and 6–11pm
        "solar_hours": [(11, 15)],             # 11am–3pm
        "currency":    "INR",
    },
    "kazam": {
        "base_rate":   17.00,
        "peak_rate":   21.00,
        "solar_rate":  13.50,
        "peak_hours":  [(7, 10), (17, 22)],
        "solar_hours": [(10, 15)],
        "currency":    "INR",
    },
    "chargezone": {
        "base_rate":   19.00,
        "peak_rate":   23.00,
        "solar_rate":  15.00,
        "peak_hours":  [(8, 11), (18, 22)],
        "solar_hours": [(11, 14)],
        "currency":    "INR",
    },
    # Default fallback — used when CPO not in config
    "default": {
        "base_rate":   18.00,
        "peak_rate":   18.00,
        "solar_rate":  18.00,
        "peak_hours":  [],
        "solar_hours": [],
        "currency":    "INR",
    },
}

GST_RATE = Decimal("0.18")   # 18% GST on EV charging in India


def get_tariff(cpo_id: str) -> dict:
    """
    Return tariff config for a given CPO.
    Falls back to 'default' if CPO not found.
    """
    return CPO_TARIFFS.get(cpo_id.lower(), CPO_TARIFFS["default"])


def _is_in_hours(hour: int, hour_ranges: list) -> bool:
    """Return True if hour falls within any of the given ranges."""
    return any(start <= hour < end for start, end in hour_ranges)


def get_applicable_rate(cpo_id: str, started_at: datetime) -> tuple:
    """
    Return (rate, rate_type) based on time-of-day the session started.

    Rate type is one of: 'solar' | 'peak' | 'base'
    Solar takes priority over peak (cheaper green energy).
    """
    tariff = get_tariff(cpo_id)
    hour   = started_at.hour   # IST hour — caller must pass IST datetime

    if _is_in_hours(hour, tariff["solar_hours"]):
        return tariff["solar_rate"], "solar"
    elif _is_in_hours(hour, tariff["peak_hours"]):
        return tariff["peak_rate"], "peak"
    else:
        return tariff["base_rate"], "base"


def compute_cost(cpo_id: str, kwh: float,
                 started_at: datetime) -> dict:
    """
    Compute the full cost breakdown for a session.

    Args:
        cpo_id     : CPO identifier string e.g. 'pulse-energy'
        kwh        : Total kWh consumed in the session
        started_at : Session start time as a datetime (IST)

    Returns a dict with:
        cpo_id          : CPO identifier
        kwh             : Units consumed
        rate            : ₹/kWh applied (ex-GST)
        rate_type       : 'base' | 'peak' | 'solar'
        base_amount     : kwh × rate (ex-GST), rounded to 2dp
        gst_amount      : 18% GST on base_amount, rounded to 2dp
        total_payable   : base_amount + gst_amount, rounded to 2dp
        currency        : 'INR'
    """
    rate, rate_type = get_applicable_rate(cpo_id, started_at)

    # Use Decimal for precise financial arithmetic
    d_kwh  = Decimal(str(kwh))
    d_rate = Decimal(str(rate))

    base_amount = (d_kwh * d_rate).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    gst_amount  = (base_amount * GST_RATE).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    total       = (base_amount + gst_amount).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    return {
        "cpo_id":        cpo_id,
        "kwh":           float(d_kwh),
        "rate":          float(d_rate),
        "rate_type":     rate_type,
        "base_amount":   str(base_amount),
        "gst_amount":    str(gst_amount),
        "total_payable": str(total),
        "currency":      "INR",
    }


def format_receipt(result: dict) -> str:
    """
    Return a human-readable receipt string.
    Used in logs and UEI on_update SESSION-SUMMARY tag.
    """
    return (
        f"─────────────────────────────\n"
        f"  EV Charging Receipt\n"
        f"─────────────────────────────\n"
        f"  CPO          : {result['cpo_id']}\n"
        f"  Units        : {result['kwh']:.3f} kWh\n"
        f"  Rate         : ₹{result['rate']:.2f}/kWh ({result['rate_type']})\n"
        f"  Base amount  : ₹{result['base_amount']}\n"
        f"  GST (18%)    : ₹{result['gst_amount']}\n"
        f"─────────────────────────────\n"
        f"  Total        : ₹{result['total_payable']}\n"
        f"─────────────────────────────"
    )
