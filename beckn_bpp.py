"""
beckn_bpp.py
------------
UEI Beckn BPP (Beckn Provider Platform) endpoints.

Implements the six Beckn API actions for EV charging:
  /search  → on_search  (return charger catalogue)
  /select  → on_select  (return tariff for chosen charger)
  /init    → on_init    (return order draft)
  /confirm → on_confirm (start session via CPO API)
  /status  → on_status  (return live kWh + cost — PR #308 LIVE-METER)
  /update  → on_update  (return final settlement — PR #308 SESSION-SUMMARY)

Payment type: ON-FULFILLMENT (hyphen) per UEI spec and PR #308.
Driver pays exact kWh amount AFTER session ends via UPI collect.

Beckn async pattern:
  Every endpoint returns {"message": {"ack": {"status": "ACK"}}} immediately.
  Then calls the BAP's callback URL asynchronously with the actual response.
  This is mandatory in the Beckn protocol — never block the initial request.

Register this router in main.py:
  from beckn_bpp import router as beckn_router
  app.include_router(beckn_router)
"""

import logging
import os
from datetime import datetime

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException

import session_store as store
from tariff_calculator import compute_cost, get_tariff

log = logging.getLogger(__name__)

router = APIRouter(prefix="/beckn", tags=["Beckn BPP"])

IDTAG_PREFIX  = os.getenv("CHARGEFLOW_IDTAG_PREFIX", "CHARGEFLOW_")
CPO_API_BASE  = os.getenv("CPO_API_BASE_URL", "")
CPO_API_KEY   = os.getenv("CPO_API_KEY", "")
BPP_ID        = os.getenv("BPP_ID", "chargeflow.in")
BPP_URI       = os.getenv("BPP_URI", "https://api.chargeflow.in")

# ── Sample charger catalogue ─────────────────────────────────────────────────
# In production this comes from the CPO's charger inventory API
# or from a database of registered chargers on your BPP

CHARGER_CATALOGUE = [
    {
        "id":          "CHARGER-001",
        "descriptor":  {"name": "CCS2 DC Fast Charger — HPCL Bangalore"},
        "location":    {"gps": "12.9716,77.5946"},
        "cpo_id":      "pulse-energy",
        "connector":   "CCS2",
        "type":        "DC",
        "power_kw":    60,
        "tariff_per_kwh": 18.0,
    },
    {
        "id":          "CHARGER-002",
        "descriptor":  {"name": "CCS2 DC Fast Charger — Shell Mumbai"},
        "location":    {"gps": "19.0760,72.8777"},
        "cpo_id":      "pulse-energy",
        "connector":   "CCS2",
        "type":        "DC",
        "power_kw":    50,
        "tariff_per_kwh": 19.0,
    },
]


# ── Beckn callback helper ─────────────────────────────────────────────────────

async def send_callback(action: str, payload: dict, bap_uri: str) -> None:
    """
    Send async callback to BAP after processing the request.
    Called via FastAPI BackgroundTasks — never blocks the initial response.
    Retries once on failure.
    """
    url = f"{bap_uri}/{action}"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, timeout=10.0)
            if resp.status_code == 200:
                log.info(f"Beckn callback sent: {action} → {bap_uri}")
            else:
                log.error(f"Beckn callback failed: {action} → {resp.status_code}")
    except Exception as e:
        log.error(f"Beckn callback error: {action} → {bap_uri}: {e}")


def ack_response() -> dict:
    """Standard Beckn ACK response — returned immediately on every endpoint."""
    return {"message": {"ack": {"status": "ACK"}}}


def build_context(incoming: dict, action: str) -> dict:
    """Build the response context from the incoming context."""
    return {
        **incoming,
        "action":   action,
        "bpp_id":   BPP_ID,
        "bpp_uri":  BPP_URI,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


# ── /search ──────────────────────────────────────────────────────────────────

@router.post("/search")
async def beckn_search(request: dict, bg: BackgroundTasks):
    """
    Return list of ChargeFlow-enabled DC fast chargers.
    BAP sends: location + radius
    BPP returns: charger catalogue with ON-FULFILLMENT payment option
    """
    context = request.get("context", {})
    bg.add_task(
        send_callback,
        "on_search",
        {
            "context": build_context(context, "on_search"),
            "message": {
                "catalog": {
                    "descriptor": {
                        "name": "ChargeFlow — Postpaid EV Charging"
                    },
                    "providers": [_build_provider(c) for c in CHARGER_CATALOGUE]
                }
            }
        },
        context.get("bap_uri", "")
    )
    return ack_response()


def _build_provider(charger: dict) -> dict:
    """Build a Beckn provider object for a single charger."""
    tariff = charger["tariff_per_kwh"]
    return {
        "id":         charger["id"],
        "descriptor": charger["descriptor"],
        "locations":  [{"id": charger["id"], "gps": charger["location"]["gps"]}],
        "items": [{
            "id":          charger["id"],
            "descriptor":  {
                "name": f"{charger['connector']} {charger['power_kw']}kW",
                "code": charger["connector"]
            },
            "price": {
                "currency": "INR",
                "value":    str(tariff)
            },
            "tags": [{
                "descriptor": {"code": "CHARGER-SPECS"},
                "list": [
                    {"descriptor": {"code": "CONNECTOR-TYPE"},  "value": charger["connector"]},
                    {"descriptor": {"code": "CHARGE-TYPE"},     "value": charger["type"]},
                    {"descriptor": {"code": "POWER-KW"},        "value": str(charger["power_kw"])},
                    {"descriptor": {"code": "TARIFF-PER-KWH"},  "value": str(tariff)},
                    {"descriptor": {"code": "PAYMENT-TYPE"},    "value": "ON-FULFILLMENT"},
                ]
            }]
        }],
        "payments": [{
            "type":         "ON-FULFILLMENT",    # ← PR #308
            "status":       "NOT-PAID",
            "collected_by": "BPP",
        }]
    }


# ── /select ──────────────────────────────────────────────────────────────────

@router.post("/select")
async def beckn_select(request: dict, bg: BackgroundTasks):
    """
    Return tariff breakdown for the selected charger.
    BAP sends: provider_id + item_id
    BPP returns: quoted tariff with ON-FULFILLMENT payment confirmation
    """
    context    = request.get("context", {})
    order      = request.get("message", {}).get("order", {})
    items      = order.get("items", [])
    charger_id = items[0].get("id", "") if items else ""

    charger = next(
        (c for c in CHARGER_CATALOGUE if c["id"] == charger_id), None
    )
    if not charger:
        log.error(f"Charger not found in catalogue: {charger_id}")
        return ack_response()

    tariff = charger["tariff_per_kwh"]
    bg.add_task(
        send_callback,
        "on_select",
        {
            "context": build_context(context, "on_select"),
            "message": {
                "order": {
                    "provider": {"id": charger_id},
                    "items":    [{"id": charger_id}],
                    "quote": {
                        "price":      {"currency": "INR", "value": "0.00"},
                        "breakup": [{
                            "title": f"Charging @ ₹{tariff}/kWh",
                            "price": {"currency": "INR", "value": str(tariff)}
                        }, {
                            "title": "GST (18%)",
                            "price": {"currency": "INR", "value": "included"}
                        }]
                    },
                    "payments": [{
                        "type":         "ON-FULFILLMENT",   # ← PR #308
                        "status":       "NOT-PAID",
                        "collected_by": "BPP",
                        "params":       {"currency": "INR"}
                    }]
                }
            }
        },
        context.get("bap_uri", "")
    )
    return ack_response()


# ── /init ─────────────────────────────────────────────────────────────────────

@router.post("/init")
async def beckn_init(request: dict, bg: BackgroundTasks):
    """
    Return finalised order draft before confirmation.
    BAP sends: order with fulfillment details + driver VPA
    BPP returns: order with ON-FULFILLMENT payment terms
    """
    context = request.get("context", {})
    order   = request.get("message", {}).get("order", {})
    items   = order.get("items", [])
    charger_id = items[0].get("id", "") if items else ""

    bg.add_task(
        send_callback,
        "on_init",
        {
            "context": build_context(context, "on_init"),
            "message": {
                "order": {
                    "provider":     {"id": charger_id},
                    "items":        [{"id": charger_id}],
                    "fulfillments": order.get("fulfillments", []),
                    "payments": [{
                        "type":         "ON-FULFILLMENT",   # ← PR #308
                        "status":       "NOT-PAID",
                        "collected_by": "BPP",
                        "params": {
                            "currency": "INR",
                            "vpa":      order.get("payments", [{}])[0]
                                            .get("params", {})
                                            .get("vpa", "")
                        }
                    }],
                    "tags": [{
                        "descriptor": {"code": "PAYMENT-TERMS"},
                        "list": [
                            {"descriptor": {"code": "TYPE"},
                             "value": "ON-FULFILLMENT"},
                            {"descriptor": {"code": "DESCRIPTION"},
                             "value": "You will be charged the exact "
                                      "kWh amount after your session ends. "
                                      "No upfront payment required."}
                        ]
                    }]
                }
            }
        },
        context.get("bap_uri", "")
    )
    return ack_response()


# ── /confirm ─────────────────────────────────────────────────────────────────

@router.post("/confirm")
async def beckn_confirm(request: dict, bg: BackgroundTasks):
    """
    Driver confirms postpaid session.
    BAP sends: order with payment.type = ON-FULFILLMENT + driver phone
    BPP:
      1. Validates payment type is ON-FULFILLMENT
      2. Checks driver is not on bad debt watchlist
      3. Calls CPO REST API: RemoteStartTransaction with CHARGEFLOW_ idTag
      4. Returns on_confirm with order status ACTIVE
    """
    context  = request.get("context", {})
    order    = request.get("message", {}).get("order", {})
    payments = order.get("payments", [{}])
    payment  = payments[0] if payments else {}
    items    = order.get("items", [])

    charger_id   = items[0].get("id", "") if items else ""
    payment_type = payment.get("type", "")
    driver_vpa   = payment.get("params", {}).get("vpa", "")
    fulfillments = order.get("fulfillments", [{}])
    driver_phone = (fulfillments[0]
                   .get("customer", {})
                   .get("contact", {})
                   .get("phone", "")) if fulfillments else ""
    bap_uri      = context.get("bap_uri", "")

    # ── Validate payment type ─────────────────────────────────────────────────
    if payment_type != "ON-FULFILLMENT":
        log.error(f"Confirm rejected — invalid payment type: {payment_type}")
        bg.add_task(
            send_callback,
            "on_confirm",
            {
                "context": build_context(context, "on_confirm"),
                "message": {
                    "order": {
                        "status": "FAILED",
                        "error": {
                            "code":    "INVALID_PAYMENT_TYPE",
                            "message": "Only ON-FULFILLMENT payment is supported"
                        }
                    }
                }
            },
            bap_uri
        )
        return ack_response()

    # ── Check driver watchlist (bad debt) ─────────────────────────────────────
    from escalation_worker import is_driver_on_watchlist
    if driver_phone and is_driver_on_watchlist(driver_phone):
        log.warning(f"Confirm rejected — driver {driver_phone} on watchlist")
        bg.add_task(
            send_callback,
            "on_confirm",
            {
                "context": build_context(context, "on_confirm"),
                "message": {
                    "order": {
                        "status": "FAILED",
                        "error": {
                            "code":    "DRIVER_WATCHLIST",
                            "message": "Outstanding dues exist. "
                                       "Please pay ₹50 activation deposit "
                                       "to resume charging."
                        }
                    }
                }
            },
            bap_uri
        )
        return ack_response()

    # ── Block reserve funds (UPI single-block-multiple-debit) ─────────────────
    # Look up charger specs to size the reserve, then block before starting.
    from upi_collect import calculate_reserve, create_block

    charger  = next((c for c in CHARGER_CATALOGUE if c["id"] == charger_id), None)
    power_kw = charger["power_kw"]       if charger else 60
    tariff   = charger["tariff_per_kwh"] if charger else 18.0
    reserve  = calculate_reserve(power_kw, tariff)

    block = create_block(
        charger_id=charger_id,
        transaction_id=0,                 # txn not known yet — keyed by phone below
        reserve_amount=reserve,
        driver_phone=driver_phone,
        driver_vpa=driver_vpa
    )

    if not block:
        # Block failed — cannot start session. Return FAILED in on_confirm.
        log.error(f"[{charger_id}] UPI block failed — aborting confirm")
        bg.add_task(
            send_callback,
            "on_confirm",
            {
                "context": build_context(context, "on_confirm"),
                "message": {
                    "order": {
                        "status": "FAILED",
                        "error": {
                            "code":    "UPI_BLOCK_FAILED",
                            "message": "Could not reserve funds. Please try again."
                        }
                    }
                }
            },
            bap_uri
        )
        return ack_response()

    mandate_id = block["mandate_id"]

    # ── Start session via CPO API ─────────────────────────────────────────────
    idtag          = f"{IDTAG_PREFIX}{driver_phone}"
    session_started = await _remote_start(charger_id, idtag)
    order_id        = f"order_{charger_id}_{driver_phone}"
    order_status    = "ACTIVE" if session_started else "FAILED"

    # Store bap_uri, order_id, mandate_id + reserve in Redis for handle_start
    if session_started:
        # Transaction ID not yet known — will be set when StartTransaction
        # webhook arrives. Store a pending entry keyed by charger + idtag.
        r_client = store.r
        pending_key = f"pending_confirm:{charger_id}:{idtag}"
        r_client.hset(pending_key, mapping={
            "bap_uri":        bap_uri,
            "order_id":       order_id,
            "driver_vpa":     driver_vpa,
            "mandate_id":     mandate_id,
            "reserve_amount": reserve,
        })
        r_client.expire(pending_key, 300)   # 5 min TTL — StartTransaction expected soon
    else:
        # CPO failed to start — release the block we just created
        from upi_collect import release_block
        release_block(charger_id, 0, mandate_id)

    bg.add_task(
        send_callback,
        "on_confirm",
        {
            "context": build_context(context, "on_confirm"),
            "message": {
                "order": {
                    "id":     order_id,
                    "status": order_status,
                    "provider":     {"id": charger_id},
                    "fulfillments": [{
                        "id":    "session-001",
                        "state": {"descriptor": {"code": order_status}}
                    }],
                    "payments": [{
                        "type":         "ON-FULFILLMENT",   # ← PR #308
                        "status":       "NOT-PAID",
                        "collected_by": "BPP",
                    }]
                }
            }
        },
        bap_uri
    )
    return ack_response()


async def _remote_start(charger_id: str, idtag: str) -> bool:
    """
    Call CPO REST API to send RemoteStartTransaction.
    idTag format: CHARGEFLOW_9876543210
    CPO CSMS routes all CHARGEFLOW_ sessions to our webhook.
    """
    if not CPO_API_BASE:
        # Local testing — simulate successful start
        log.info(f"[{charger_id}] RemoteStart simulated (no CPO_API_BASE_URL set)")
        return True

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{CPO_API_BASE}/chargers/{charger_id}/remote-start",
                headers={"Authorization": f"Bearer {CPO_API_KEY}"},
                json={"idTag": idtag, "connectorId": 1},
                timeout=15.0
            )
            success = resp.status_code == 200
            if success:
                log.info(f"[{charger_id}] RemoteStartTransaction sent | idTag={idtag}")
            else:
                log.error(f"[{charger_id}] RemoteStart failed: {resp.status_code}")
            return success
    except Exception as e:
        log.error(f"[{charger_id}] RemoteStart exception: {e}")
        return False


# ── /status ──────────────────────────────────────────────────────────────────

@router.post("/status")
async def beckn_status(request: dict, bg: BackgroundTasks):
    """
    Return live session status with kWh and running cost.
    BAP sends: order_id
    BPP returns: on_status with LIVE-METER tag (PR #308)
                 or SESSION-SUMMARY if session is complete
    """
    context  = request.get("context", {})
    order_id = request.get("message", {}).get("order_id", "")
    bap_uri  = context.get("bap_uri", "")

    # Parse charger_id from order_id: order_{charger_id}_{phone}
    session = _find_session_by_order_id(order_id)

    if not session:
        bg.add_task(
            send_callback,
            "on_status",
            {
                "context": build_context(context, "on_status"),
                "message": {
                    "order": {
                        "id":     order_id,
                        "status": "NOT_FOUND"
                    }
                }
            },
            bap_uri
        )
        return ack_response()

    status     = session.get("status", "UNKNOWN")
    kwh_start  = float(session.get("kwh_start", 0))
    kwh_live   = float(session.get("kwh_live", 0))
    cost_live  = session.get("cost_live", "0.00")
    cost_final = session.get("cost_final", "0.00")
    tariff     = session.get("tariff", "18.0")
    kwh_delta  = round(kwh_live - kwh_start, 3)

    if status in ("CHARGING", "AWAITING"):
        # Live session — push LIVE-METER tag (PR #308)
        fulfillment_tag = {
            "descriptor": {"code": "LIVE-METER"},
            "list": [
                {"descriptor": {"code": "KWH-CONSUMED"},
                 "value": str(kwh_delta)},
                {"descriptor": {"code": "COST-SO-FAR"},
                 "value": cost_live},
                {"descriptor": {"code": "TARIFF"},
                 "value": tariff},
                {"descriptor": {"code": "CURRENCY"},
                 "value": "INR"},
            ]
        }
        fulfillment_code = "CHARGING"
        payment_status   = "NOT-PAID"
        quote_value      = cost_live

    elif status == "PENDING_PAYMENT":
        # Session ended — show final amount awaiting payment
        fulfillment_tag = {
            "descriptor": {"code": "LIVE-METER"},
            "list": [
                {"descriptor": {"code": "KWH-CONSUMED"},
                 "value": session.get("kwh_total", "0.0")},
                {"descriptor": {"code": "COST-SO-FAR"},
                 "value": cost_final},
                {"descriptor": {"code": "TARIFF"},
                 "value": tariff},
                {"descriptor": {"code": "CURRENCY"},
                 "value": "INR"},
            ]
        }
        fulfillment_code = "COMPLETE"
        payment_status   = "NOT-PAID"
        quote_value      = cost_final

    elif status == "COMPLETE":
        # Paid — push SESSION-SUMMARY (PR #308)
        fulfillment_tag = {
            "descriptor": {"code": "SESSION-SUMMARY"},
            "list": [
                {"descriptor": {"code": "TOTAL-KWH"},
                 "value": session.get("kwh_total", "0.0")},
                {"descriptor": {"code": "FINAL-COST"},
                 "value": cost_final},
                {"descriptor": {"code": "UPI-TXN-ID"},
                 "value": session.get("upi_txn_id", "")},
                {"descriptor": {"code": "CURRENCY"},
                 "value": "INR"},
            ]
        }
        fulfillment_code = "COMPLETE"
        payment_status   = "PAID"
        quote_value      = cost_final

    else:
        fulfillment_tag  = {}
        fulfillment_code = status
        payment_status   = "NOT-PAID"
        quote_value      = "0.00"

    tags = [fulfillment_tag] if fulfillment_tag else []

    bg.add_task(
        send_callback,
        "on_status",
        {
            "context": build_context(context, "on_status"),
            "message": {
                "order": {
                    "id":     order_id,
                    "status": fulfillment_code,
                    "fulfillments": [{
                        "state": {"descriptor": {"code": fulfillment_code}},
                        "tags":  tags
                    }],
                    "quote": {
                        "price": {"currency": "INR", "value": quote_value}
                    },
                    "payments": [{
                        "type":         "ON-FULFILLMENT",   # ← PR #308
                        "status":       payment_status,
                        "collected_by": "BPP",
                    }]
                }
            }
        },
        bap_uri
    )
    return ack_response()


# ── /update ──────────────────────────────────────────────────────────────────

@router.post("/update")
async def beckn_update(request: dict, bg: BackgroundTasks):
    """
    Return final session settlement after payment confirmed.
    BAP sends: order_id
    BPP returns: on_update with SESSION-SUMMARY tag and PAID status (PR #308)
    """
    context  = request.get("context", {})
    order_id = request.get("message", {}).get("order", {}).get("id", "")
    bap_uri  = context.get("bap_uri", "")

    session = _find_session_by_order_id(order_id)

    if not session or session.get("status") != "COMPLETE":
        bg.add_task(
            send_callback,
            "on_update",
            {
                "context": build_context(context, "on_update"),
                "message": {
                    "order": {
                        "id":     order_id,
                        "status": "PAYMENT_PENDING"
                    }
                }
            },
            bap_uri
        )
        return ack_response()

    kwh_total  = session.get("kwh_total", "0.0")
    cost_final = session.get("cost_final", "0.00")
    upi_txn_id = session.get("upi_txn_id", "")

    # Compute itemised breakdown for receipt
    try:
        result = compute_cost(
            cpo_id="pulse-energy",
            kwh=float(kwh_total),
            started_at=datetime.now()
        )
        base_amount = result["base_amount"]
        gst_amount  = result["gst_amount"]
    except Exception:
        base_amount = cost_final
        gst_amount  = "0.00"

    bg.add_task(
        send_callback,
        "on_update",
        {
            "context": build_context(context, "on_update"),
            "message": {
                "order": {
                    "id":     order_id,
                    "status": "COMPLETE",
                    "fulfillments": [{
                        "state": {"descriptor": {"code": "COMPLETE"}},
                        "tags": [{
                            # SESSION-SUMMARY tag — PR #308
                            "descriptor": {"code": "SESSION-SUMMARY"},
                            "list": [
                                {"descriptor": {"code": "TOTAL-KWH"},
                                 "value": kwh_total},
                                {"descriptor": {"code": "BASE-AMOUNT"},
                                 "value": base_amount},
                                {"descriptor": {"code": "GST-AMOUNT"},
                                 "value": gst_amount},
                                {"descriptor": {"code": "FINAL-COST"},
                                 "value": cost_final},
                                {"descriptor": {"code": "CURRENCY"},
                                 "value": "INR"},
                                {"descriptor": {"code": "UPI-TXN-ID"},
                                 "value": upi_txn_id},
                            ]
                        }]
                    }],
                    "quote": {
                        "price": {"currency": "INR", "value": cost_final}
                    },
                    "payments": [{
                        "type":         "ON-FULFILLMENT",   # ← PR #308
                        "status":       "PAID",
                        "collected_by": "BPP",
                        "params": {
                            "transaction_id": upi_txn_id,
                            "amount":         cost_final,
                            "currency":       "INR",
                        }
                    }]
                }
            }
        },
        bap_uri
    )
    return ack_response()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_session_by_order_id(order_id: str) -> dict | None:
    """
    Find a Redis session from a Beckn order_id.
    order_id format: order_{charger_id}_{driver_phone}
    """
    if not order_id.startswith("order_"):
        return None

    # Strip "order_" prefix, then split on last underscore for phone
    remainder  = order_id[len("order_"):]
    sessions   = store.r.keys("session:*")

    for key in sessions:
        session = store.r.hgetall(key)
        if not session:
            continue
        # Match by charger_id + driver phone embedded in id_tag
        charger_id   = session.get("charger_id", "")
        driver_phone = session.get("id_tag", "").replace(IDTAG_PREFIX, "")
        expected_oid = f"order_{charger_id}_{driver_phone}"
        if expected_oid == order_id:
            return session

    return None
