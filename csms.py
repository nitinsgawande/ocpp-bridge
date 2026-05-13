"""
csms.py
-------
OCPP 1.6J Central System (CSMS) bridge service.

Responsibilities:
  - Accept WebSocket connections from OCPP chargers
  - Handle BootNotification, StartTransaction, MeterValues, StopTransaction
  - Maintain session state via session_store (Redis)
  - Compute tariff via tariff_calculator
  - Trigger UPI payment via upi_collect after session ends
  - Subscribe to Redis unlock channel and send OCPP UnlockConnector
    after payment is confirmed (cable lock controller)
"""

import asyncio
import json
import logging
from datetime import datetime
from ocpp.routing import on
from ocpp.v16 import ChargePoint as cp
from ocpp.v16 import call, call_result
from ocpp.v16.enums import RegistrationStatus, Action
import redis.asyncio as aioredis
import websockets

import session_store as store
from tariff_calculator import compute_cost, format_receipt, get_tariff
from cable_lock import unlock_channel
from upi_collect import trigger_collect

logging.basicConfig(level=logging.INFO)

# Registry of active ChargePoint connections keyed by charger_id
# Used by the unlock subscriber to find the right WebSocket connection
active_chargers: dict = {}


class ChargePoint(cp):

    @on(Action.boot_notification)
    async def on_boot_notification(self, charge_point_vendor,
                                   charge_point_model, **kwargs):
        logging.info(f"[{self.id}] BootNotification from "
                     f"{charge_point_vendor} {charge_point_model}")
        return call_result.BootNotification(
            current_time=datetime.utcnow().isoformat(),
            interval=30,
            status=RegistrationStatus.accepted
        )

    @on(Action.start_transaction)
    async def on_start_transaction(self, connector_id, id_tag,
                                   meter_start, timestamp, **kwargs):
        transaction_id = 1001
        kwh_start      = meter_start / 1000
        cpo_id         = "pulse-energy"
        tariff_rate    = get_tariff(cpo_id)["base_rate"]

        store.create_session(
            charger_id=self.id,
            transaction_id=transaction_id,
            kwh_start=kwh_start,
            tariff=tariff_rate,
            id_tag=id_tag
        )
        store.transition(self.id, transaction_id, "CHARGING")

        logging.info(f"[{self.id}] StartTransaction | "
                     f"idTag: {id_tag} | "
                     f"Start meter: {kwh_start:.3f} kWh | "
                     f"txn={transaction_id}")

        return call_result.StartTransaction(
            transaction_id=transaction_id,
            id_tag_info={"status": "Accepted"}
        )

    @on(Action.meter_values)
    async def on_meter_values(self, connector_id,
                              meter_value, transaction_id=None, **kwargs):
        for mv in meter_value:
            for sv in mv.get("sampled_value", []):
                measurand = sv.get("measurand",
                                   "Energy.Active.Import.Register")
                value_str = sv.get("value", "0")

                if measurand != "Energy.Active.Import.Register":
                    continue

                kwh_live  = float(value_str) / 1000
                cost_live = store.update_live_meter(
                    self.id, transaction_id, kwh_live
                )
                session   = store.get_session(self.id, transaction_id)
                delta_kwh = kwh_live - float(session["kwh_start"])

                logging.info(
                    f"[{self.id}] LIVE METER | "
                    f"Consumed: {delta_kwh:.3f} kWh | "
                    f"Cost (incl GST): ₹{cost_live:.2f} | "
                    f"status={session['status']}"
                )

        return call_result.MeterValues()

    @on(Action.stop_transaction)
    async def on_stop_transaction(self, transaction_id,
                                  meter_stop, timestamp,
                                  reason=None, **kwargs):
        kwh_stop = meter_stop / 1000
        session  = store.finalise_session(self.id, transaction_id, kwh_stop)

        if session:
            result = compute_cost(
                cpo_id="pulse-energy",
                kwh=float(session["kwh_total"]),
                started_at=datetime.now()
            )
            print(format_receipt(result))

            # Trigger Razorpay payment link — driver receives SMS
            trigger_collect(
                charger_id=self.id,
                transaction_id=transaction_id,
                amount_inr=result["total_payable"],
                driver_phone="9876543210",
                driver_name="EV Driver"
            )
            logging.info(
                f"[{self.id}] Waiting for payment confirmation. "
                f"Cable locked."
            )

        return call_result.StopTransaction(
            id_tag_info={"status": "Accepted"}
        )

    async def unlock_connector(self, transaction_id: int,
                               upi_txn_id: str) -> bool:
        """
        Send OCPP UnlockConnector to the physical charger.
        Called by the Redis unlock subscriber after payment confirms.
        Only fires after payment is confirmed — cable stays locked until then.
        """
        try:
            request  = call.UnlockConnector(connector_id=1)
            response = await self.call(request)
            status   = response.status

            logging.info(
                f"[{self.id}] OCPP UnlockConnector sent | "
                f"txn={transaction_id} | "
                f"upi_txn={upi_txn_id} | "
                f"result={status}"
            )

            if status == "Unlocked":
                logging.info(
                    f"[{self.id}] ✅ CABLE RELEASED | "
                    f"Driver can remove connector"
                )
                return True
            else:
                logging.warning(
                    f"[{self.id}] ⚠️  UnlockConnector returned: {status}"
                )
                return False

        except Exception as e:
            logging.error(
                f"[{self.id}] UnlockConnector FAILED | "
                f"txn={transaction_id} | error={e}"
            )
            return False


async def unlock_subscriber():
    """
    Background task — subscribes to all unlock:{charger_id} channels.
    When a message arrives (published by upi_collect.on_payment_success),
    finds the active ChargePoint connection and sends UnlockConnector.

    Uses Redis pattern subscribe to catch all charger unlock channels
    with a single subscriber: unlock:*
    """
    redis_client = aioredis.Redis(
        host="localhost", port=6379, db=0, decode_responses=True
    )
    pubsub = redis_client.pubsub()

    # Subscribe to all unlock channels using pattern matching
    await pubsub.psubscribe("unlock:*")
    logging.info("Unlock subscriber listening on channel pattern: unlock:*")

    async for message in pubsub.listen():
        if message["type"] != "pmessage":
            continue

        try:
            data       = json.loads(message["data"])
            charger_id = data.get("charger_id")
            txn_id     = data.get("transaction_id")
            upi_txn    = data.get("upi_txn_id")

            logging.info(
                f"Unlock event received | "
                f"charger={charger_id} | txn={txn_id}"
            )

            # Look up the active ChargePoint WebSocket connection
            charge_point = active_chargers.get(charger_id)

            if charge_point is None:
                logging.error(
                    f"No active connection for charger: {charger_id}. "
                    f"Cannot send UnlockConnector."
                )
                continue

            # Send OCPP UnlockConnector — cable releases here
            await charge_point.unlock_connector(txn_id, upi_txn)

        except Exception as e:
            logging.error(f"Unlock subscriber error: {e}")


async def on_connect(websocket):
    charge_point_id = websocket.request.path.strip("/")
    logging.info(f"Charger connected: {charge_point_id}")

    charge_point = ChargePoint(charge_point_id, websocket)

    # Register in active_chargers so unlock_subscriber can find it
    active_chargers[charge_point_id] = charge_point
    logging.info(f"Registered {charge_point_id} in active_chargers")

    try:
        await charge_point.start()
    finally:
        # Clean up when charger disconnects
        active_chargers.pop(charge_point_id, None)
        logging.info(f"Charger disconnected: {charge_point_id}")


async def main():
    # Start OCPP WebSocket server
    server = await websockets.serve(
        on_connect,
        "0.0.0.0",
        9000,
        subprotocols=["ocpp1.6"]
    )
    logging.info("CSMS listening on ws://0.0.0.0:9000")

    # Run WebSocket server and Redis subscriber concurrently
    await asyncio.gather(
        server.wait_closed(),
        unlock_subscriber()
    )


if __name__ == "__main__":
    asyncio.run(main())
