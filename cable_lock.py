"""
cable_lock.py
-------------
Publisher side of the cable unlock controller.

RELIABILITY MODEL — Redis LIST queue (not pub/sub).

Why a list, not pub/sub:
  Redis pub/sub is at-most-once: if the subscriber is momentarily
  disconnected when the event is published, the message is lost forever.
  Cable unlock is payment-critical — it must never be lost. A Redis list
  (RPUSH to enqueue, BLPOP to consume) persists the event until a consumer
  reads it, giving at-least-once delivery that survives subscriber restarts
  and dropped connections.

Queue key : unlock_queue
Enqueue   : publish_unlock()  → RPUSH
Consume   : main.py cable_lock_subscriber() → BLPOP

The consumer in main.py calls the CPO REST API to send UnlockConnector.
"""

import json
import logging
import os

import redis

log = logging.getLogger(__name__)

r = redis.Redis.from_url(
    os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    decode_responses=True
)

UNLOCK_QUEUE_KEY = "unlock_queue"


def publish_unlock(charger_id: str,
                   transaction_id: int,
                   upi_txn_id: str) -> bool:
    """
    Enqueue an unlock event after payment is confirmed.
    Uses RPUSH onto a Redis list — the event persists until the
    consumer (main.py) reads it via BLPOP. Survives subscriber restarts.

    Returns True if the event was enqueued.
    """
    message = json.dumps({
        "charger_id":     charger_id,
        "transaction_id": transaction_id,
        "upi_txn_id":     upi_txn_id,
        "action":         "UNLOCK_CONNECTOR"
    })

    try:
        queue_len = r.rpush(UNLOCK_QUEUE_KEY, message)
        log.info(
            f"[{charger_id}] Unlock event ENQUEUED | "
            f"txn={transaction_id} | "
            f"queue_depth={queue_len}"
        )
        return True
    except Exception as e:
        log.error(
            f"[{charger_id}] Failed to enqueue unlock event | "
            f"txn={transaction_id} | error={e}"
        )
        return False
