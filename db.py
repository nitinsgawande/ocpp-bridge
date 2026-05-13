"""
db.py
-----
MongoDB Motor async client — replaces SQLModel + PostgreSQL.

Two collections:
  session_audit — one document per charging session (upsert on state change)
  meter_log     — one document per MeterValues reading (append-only)

Document ID convention:
  session_audit : "{charger_id}:{transaction_id}"
  meter_log     : auto-generated ObjectId

Indexes created on startup:
  session_audit : status (for startup_recovery query)
  meter_log     : charger_id + transaction_id (for dispute queries)

Connection is a module-level singleton — Motor manages the connection pool.
"""

import os
from motor.motor_asyncio import AsyncIOMotorClient

# ── Connection ────────────────────────────────────────────────────────────────

MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGODB_DB",  "chargeflow")

_client: AsyncIOMotorClient | None = None


def get_client() -> AsyncIOMotorClient:
    """Return the module-level Motor client, creating it if needed."""
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(MONGO_URL)
    return _client


def get_db():
    """Return the chargeflow database handle."""
    return get_client()[DB_NAME]


def session_audit_col():
    """Return the session_audit collection."""
    return get_db()["session_audit"]


def meter_log_col():
    """Return the meter_log collection."""
    return get_db()["meter_log"]


async def create_indexes() -> None:
    """
    Create indexes on both collections.
    Called once at FastAPI startup via lifespan.
    Motor create_index is idempotent — safe to call on every restart.
    """
    await session_audit_col().create_index("status")
    await session_audit_col().create_index("charger_id")
    await session_audit_col().create_index("transaction_id")
    await meter_log_col().create_index(
        [("charger_id", 1), ("transaction_id", 1)]
    )
