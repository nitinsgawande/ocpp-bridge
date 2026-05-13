"""
charger_simulator.py
--------------------
Simulates an OCPP 1.6J EV charger for local testing.

Handles UnlockConnector request from CSMS — confirming the cable
releases after payment is confirmed via the Redis pub/sub flow.
"""

import asyncio
import logging
from datetime import datetime, timezone
from ocpp.routing import on
from ocpp.v16 import call, call_result
from ocpp.v16 import ChargePoint as cp
from ocpp.v16.enums import Action, UnlockStatus
import websockets

logging.basicConfig(level=logging.INFO)

CSMS_URL = "ws://localhost:9000/CHARGER-001"
TARIFF   = 18.0


class ChargePoint(cp):

    @on(Action.unlock_connector)
    async def on_unlock_connector(self, connector_id, **kwargs):
        """
        CSMS sends this after payment is confirmed.
        In real hardware: electromechanical lock releases here.
        In simulator: log the event and return Unlocked status.
        """
        logging.info(
            f"[{self.id}] ⚡ UnlockConnector received | "
            f"connector_id={connector_id}"
        )
        logging.info(
            f"[{self.id}] 🔓 CABLE RELEASED — "
            f"Driver can now remove the connector"
        )
        return call_result.UnlockConnector(
            status=UnlockStatus.unlocked
        )

    async def send_boot_notification(self):
        request  = call.BootNotification(
            charge_point_model="FastCharger-60kW",
            charge_point_vendor="TestVendor"
        )
        response = await self.call(request)
        logging.info(f"BootNotification response: {response.status}")

    async def simulate_session(self):
        """
        Simulates a full charging session:
        1. StartTransaction at 10.000 kWh
        2. MeterValues every 3s — +0.5 kWh each interval (30s in production)
        3. StopTransaction at 19.730 kWh (9.73 kWh consumed)
        4. Waits for UnlockConnector from CSMS after payment confirmed
        """
        meter_start = 10000   # Wh baseline

        # --- Start transaction ---
        start_resp = await self.call(call.StartTransaction(
            connector_id=1,
            id_tag="DRIVER-TEST-001",
            meter_start=meter_start,
            timestamp=datetime.now(timezone.utc).isoformat()
        ))
        transaction_id = start_resp.transaction_id
        logging.info(f"Transaction started: ID {transaction_id}")

        # --- Send MeterValues every 3 seconds ---
        for i in range(1, 6):
            await asyncio.sleep(3)
            kwh_consumed = i * 0.5
            meter_now_wh = meter_start + int(kwh_consumed * 1000)

            await self.call(call.MeterValues(
                connector_id=1,
                transaction_id=transaction_id,
                meter_value=[{
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "sampled_value": [{
                        "value":     str(meter_now_wh),
                        "context":   "Sample.Periodic",
                        "measurand": "Energy.Active.Import.Register",
                        "unit":      "Wh"
                    }]
                }]
            ))
            logging.info(
                f"  Sent MeterValues: {kwh_consumed:.1f} kWh"
            )

        # --- Stop transaction ---
        meter_stop = meter_start + 9730   # 9.73 kWh consumed

        await self.call(call.StopTransaction(
            transaction_id=transaction_id,
            meter_stop=meter_stop,
            timestamp=datetime.now(timezone.utc).isoformat(),
            reason="EVDisconnected"
        ))

        logging.info(
            f"StopTransaction sent. "
            f"Session ended — waiting for payment + cable unlock..."
        )

        # Keep connection alive so CSMS can send UnlockConnector
        # In production the charger stays connected indefinitely
        await asyncio.sleep(30)
        logging.info("Simulator done. Closing connection.")


async def run_session(charge_point):
    await asyncio.sleep(1)
    await charge_point.send_boot_notification()
    await asyncio.sleep(1)
    await charge_point.simulate_session()


async def main():
    async with websockets.connect(
        CSMS_URL,
        subprotocols=["ocpp1.6"]
    ) as ws:
        charge_point = ChargePoint("CHARGER-001", ws)
        await asyncio.gather(
            charge_point.start(),
            run_session(charge_point)
        )


if __name__ == "__main__":
    asyncio.run(main())
