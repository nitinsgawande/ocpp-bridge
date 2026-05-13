"""
REPLACEMENT FUNCTION FOR main.py
----------------------------------
Replace ONLY the startup_recovery() function in your existing main.py.

Also add ONE line to the lifespan() function to create MongoDB indexes.
Instructions for both changes are below.
"""

# ── startup_recovery (replaces the existing function) ────────────────────────

async def startup_recovery():
    """
    On service restart: query MongoDB for sessions that were
    CHARGING or PENDING_PAYMENT at time of crash / restart.
    Re-seeds Redis from MongoDB so in-flight sessions resume correctly.
    Watchdog and payment_poller will pick them up immediately.
    """
    await asyncio.sleep(2)   # wait for DB connection to settle

    try:
        from db import session_audit_col
        import redis as sync_redis

        # Query MongoDB for all in-flight sessions
        cursor   = session_audit_col().find(
            {"status": {"$in": ["CHARGING", "PENDING_PAYMENT"]}}
        )
        sessions = await cursor.to_list(length=None)

        if not sessions:
            log.info("Startup recovery: no in-flight sessions found")
            return

        r = sync_redis.Redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True
        )

        recovered = 0
        for s in sessions:
            charger_id     = s.get("charger_id", "")
            transaction_id = s.get("transaction_id", 0)
            key            = f"session:{charger_id}:{transaction_id}"

            if not r.exists(key):
                r.hset(key, mapping={
                    "charger_id":     charger_id,
                    "transaction_id": str(transaction_id),
                    "id_tag":         s.get("idtag", ""),
                    "status":         s.get("status", ""),
                    "kwh_start":      str(s.get("kwh_start", 0.0)),
                    "kwh_live":       str(s.get("kwh_live", 0.0)),
                    "kwh_total":      str(s.get("kwh_total", 0.0)),
                    "tariff":         str(s.get("tariff", 18.0)),
                    "gst_rate":       "0.18",
                    "cost_final":     s.get("cost_final", "0.00"),
                    "cost_live":      "0.0",
                    "upi_txn_id":     s.get("upi_txn_id", ""),
                    "retry_count":    "0",
                    "last_meter_ts":  datetime.utcnow().isoformat(),
                    "updated_at":     datetime.utcnow().isoformat(),
                })
                r.expire(key, 7200)
                recovered += 1
                log.warning(f"[{charger_id}] Recovered {s.get('status')} session "
                             f"txn={transaction_id} from MongoDB")

        log.info(
            f"Startup recovery complete — {recovered} sessions re-seeded in Redis"
        )

    except Exception as e:
        log.error(f"Startup recovery failed: {e}")


# ── lifespan update ────────────────────────────────────────────────────────────
#
# In main.py, find your existing lifespan() function:
#
#   @asynccontextmanager
#   async def lifespan(app: FastAPI):
#       log.info("ChargeFlow bridge starting...")
#       asyncio.create_task(startup_recovery())
#       asyncio.create_task(cable_lock_subscriber())
#       yield
#       log.info("ChargeFlow bridge shutting down.")
#
# Add ONE line — await db.create_indexes() — so it becomes:
#
#   @asynccontextmanager
#   async def lifespan(app: FastAPI):
#       log.info("ChargeFlow bridge starting...")
#       from db import create_indexes
#       await create_indexes()                      ← add this line
#       asyncio.create_task(startup_recovery())
#       asyncio.create_task(cable_lock_subscriber())
#       yield
#       log.info("ChargeFlow bridge shutting down.")
