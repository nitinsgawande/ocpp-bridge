"""
REPLACEMENT FUNCTIONS FOR session_store.py
-------------------------------------------
Replace ONLY these two functions in your existing session_store.py:
  - write_audit()
  - log_meter()

The import block at the top of session_store.py stays as-is:
  import logging
  import os
  from datetime import datetime
  import redis

No other changes needed in session_store.py.
"""

# ── write_audit ───────────────────────────────────────────────────────────────

async def write_audit(charger_id: str, transaction_id: int, **kwargs) -> None:
    """
    Upsert a document in MongoDB session_audit for every state transition.
    Called from FastAPI async handlers (handle_start, handle_stop, etc).
    Silently logs on error — never blocks the main session flow.

    Document _id convention: "{charger_id}:{transaction_id}"
    Uses update_one with upsert=True — creates on first call,
    updates on subsequent calls. Matches original PostgreSQL upsert behaviour.
    """
    try:
        from db import session_audit_col
        from datetime import datetime

        doc_id = f"{charger_id}:{transaction_id}"
        now    = datetime.utcnow()

        # Build the $set payload from kwargs + always-updated fields
        update_fields = {
            "charger_id":     charger_id,
            "transaction_id": transaction_id,
            "updated_at":     now,
        }
        update_fields.update(kwargs)

        await session_audit_col().update_one(
            {"_id": doc_id},
            {
                "$set":         update_fields,
                "$setOnInsert": {"created_at": now}
            },
            upsert=True
        )

    except Exception as e:
        log.error(f"[{charger_id}] MongoDB write_audit failed "
                  f"txn={transaction_id}: {e}")


# ── log_meter ─────────────────────────────────────────────────────────────────

async def log_meter(charger_id: str, transaction_id: int,
                    kwh: float, cost: str) -> None:
    """
    Append one document to MongoDB meter_log for every MeterValues reading.
    This is the tamper-evident audit trail for billing disputes.

    Each document gets an auto-generated MongoDB ObjectId (_id).
    Query pattern: find all readings for a session →
      db.meter_log.find({"charger_id": X, "transaction_id": Y})
    """
    try:
        from db import meter_log_col
        from datetime import datetime

        await meter_log_col().insert_one({
            "charger_id":     charger_id,
            "transaction_id": transaction_id,
            "kwh_value":      kwh,
            "cost_live":      cost,
            "recorded_at":    datetime.utcnow()
        })

    except Exception as e:
        log.error(f"[{charger_id}] MongoDB log_meter failed "
                  f"txn={transaction_id}: {e}")
