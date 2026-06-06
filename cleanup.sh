#!/usr/bin/env bash
#
# cleanup.sh — Full wipe of ChargeFlow test data (Redis + MongoDB)
# ----------------------------------------------------------------
# Clears ALL session state from Redis and ALL audit/meter documents
# from MongoDB, then restarts the services for a clean slate.
#
# USE ONLY IN TESTING. This deletes everything — do not run in production
# without changing FLUSHDB to targeted deletion (it also wipes the
# bad-debt watchlist and any other Redis keys).
#
# Usage:
#   cd ~/ocpp-bridge
#   chmod +x cleanup.sh        # first time only
#   ./cleanup.sh
#
# The script reads REDIS_URL and MONGODB settings from your .env so you
# never hardcode credentials here.

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

# ── Load .env ────────────────────────────────────────────────────────────────
if [[ ! -f .env ]]; then
    echo "❌ .env not found in $PROJECT_DIR — aborting."
    exit 1
fi

# Export all .env vars so this script and Python can read them
set -a
# shellcheck disable=SC1091
source .env
set +a

echo "═══════════════════════════════════════════════"
echo "  ChargeFlow FULL WIPE — Redis + MongoDB"
echo "═══════════════════════════════════════════════"
echo ""
echo "This will DELETE ALL test data:"
echo "  • All Redis keys (sessions, unlock_queue, watchlist)"
echo "  • All MongoDB session_audit documents"
echo "  • All MongoDB meter_log documents"
echo ""
read -r -p "Type 'WIPE' to confirm: " CONFIRM
if [[ "$CONFIRM" != "WIPE" ]]; then
    echo "Aborted — nothing was deleted."
    exit 0
fi

echo ""
echo "── Step 1: Flushing Redis ──────────────────────"

# Parse REDIS_URL into redis-cli arguments.
# Format: rediss://default:PASSWORD@HOST:PORT
#   rediss:// → TLS (--tls)
#   redis://  → no TLS
if [[ -z "${REDIS_URL:-}" ]]; then
    echo "❌ REDIS_URL not set in .env — skipping Redis flush."
else
    # Strip scheme
    SCHEME="${REDIS_URL%%://*}"
    REST="${REDIS_URL#*://}"

    # Extract credentials (everything before the last @) and host:port (after)
    CREDS="${REST%@*}"
    HOSTPORT="${REST##*@}"

    REDIS_PASS="${CREDS#*:}"          # after the colon (default:PASSWORD → PASSWORD)
    REDIS_HOST="${HOSTPORT%%:*}"
    REDIS_PORT="${HOSTPORT##*:}"

    TLS_FLAG=""
    if [[ "$SCHEME" == "rediss" ]]; then
        TLS_FLAG="--tls"
    fi

    redis-cli $TLS_FLAG -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASS" --no-auth-warning FLUSHDB
    echo "✅ Redis flushed ($REDIS_HOST:$REDIS_PORT)"
fi

echo ""
echo "── Step 2: Dropping MongoDB collections ────────"

# Use the project's venv Python + Motor so we reuse db.py connection config
if [[ -d venv ]]; then
    # shellcheck disable=SC1091
    source venv/bin/activate
fi

python3 - <<'PYEOF'
import asyncio
from db import session_audit_col, meter_log_col

async def wipe():
    r1 = await session_audit_col().delete_many({})
    r2 = await meter_log_col().delete_many({})
    print(f"✅ Deleted {r1.deleted_count} session_audit documents")
    print(f"✅ Deleted {r2.deleted_count} meter_log documents")

asyncio.run(wipe())
PYEOF

echo ""
echo "── Step 3: Restarting services ─────────────────"

sudo systemctl restart chargeflow
sudo systemctl restart chargeflow-celery
echo "✅ Restarted chargeflow + chargeflow-celery"

echo ""
echo "── Step 4: Verifying clean state ───────────────"
sleep 3
SESSION_KEYS=$(redis-cli $TLS_FLAG -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASS" --no-auth-warning KEYS 'session:*' | grep -c 'session:' || true)
echo "Redis session keys remaining: $SESSION_KEYS"
sudo journalctl -u chargeflow --no-pager -n 15 | grep -i "startup recovery" || true

echo ""
echo "═══════════════════════════════════════════════"
echo "  ✅ Full wipe complete — clean slate ready"
echo "═══════════════════════════════════════════════"
