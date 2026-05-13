"""
cable_lock.py
-------------
Publisher side of the cable lock controller.

Publishes an unlock event to Redis pub/sub channel unlock:{charger_id}
after payment is confirmed.

The subscriber lives in main.py (cable_lock_subscriber async task).
On receiving the event, main.py calls the CPO REST API to send
UnlockConnector — releasing the cable.

Channel naming : unlock:{charger_id}
Message payload: JSON with transaction_id and upi_txn_id
"""

import json
import logging
import os

import redis

log = logging.getLogger(__name__)

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True
)


def unlock_channel(charger_id: str) -> str:
    """Canonical Redis pub/sub channel name for a charger."""
    return f"unlock:{charger_id}"


def publish_unlock(charger_id: str,
                   transaction_id: int,
                   upi_txn_id: str) -> bool:
    """
    Publish an unlock event after payment is confirmed.
    main.py cable_lock_subscriber receives this and calls CPO API.

    Returns True if at least one subscriber received the message.
    """
    channel = unlock_channel(charger_id)
    message = json.dumps({
        "charger_id":     charger_id,
        "transaction_id": transaction_id,
        "upi_txn_id":     upi_txn_id,
        "action":         "UNLOCK_CONNECTOR"
    })

    receivers = r.publish(channel, message)

    if receivers > 0:
        log.info(
            f"[{charger_id}] Unlock event PUBLISHED | "
            f"txn={transaction_id} | "
            f"channel={channel} | "
            f"subscribers={receivers}"
        )
        return True
    else:
        log.warning(
            f"[{charger_id}] Unlock event published but NO subscribers | "
            f"txn={transaction_id} — main.py may not be running"
        )
        return False
