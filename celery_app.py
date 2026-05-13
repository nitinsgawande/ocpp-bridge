"""
celery_app.py
-------------
Single Celery app combining all three beat schedules:
  - watchdog          (every 5 min)  — stale session / power failure recovery
  - payment_poller    (every 2 min)  — webhook delivery fallback
  - escalation_worker (every 30 min) — unpaid session escalation ladder

Run all workers and beat scheduler with one command:
  celery -A celery_app worker --beat --loglevel=info

Or separately (recommended for production):
  celery -A celery_app worker --loglevel=info
  celery -A celery_app beat   --loglevel=info
"""

import os
import sys
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

# Add project directory to path so Celery workers find local modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

app = Celery("chargeflow", broker=REDIS_URL, include=[
    "watchdog",
    "payment_poller",
    "escalation_worker"
])

app.conf.beat_schedule = {
    "scan-stale-sessions": {
        "task":     "watchdog.scan_stale_sessions",
        "schedule": 300.0,    # every 5 minutes
    },
    "poll-pending-payments": {
        "task":     "payment_poller.poll_pending_payments",
        "schedule": 120.0,    # every 2 minutes
    },
    "escalate-unpaid-sessions": {
        "task":     "escalation_worker.escalate_unpaid_sessions",
        "schedule": 1800.0,   # every 30 minutes
    },
}
app.conf.timezone = "UTC"
