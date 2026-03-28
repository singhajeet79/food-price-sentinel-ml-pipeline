"""
detection/scheduler.py
-----------------------
Lightweight in-process scheduler for Phase 3 MLOps jobs.

Runs two recurring jobs in background threads:
  - Retraining   — every RETRAIN_INTERVAL_HOURS (default: 24h)
  - Drift check  — every DRIFT_CHECK_INTERVAL_HOURS (default: 6h)

Designed to be imported and started by the consumer process OR run
standalone as a separate process.

Usage — standalone:
    PYTHONPATH=. python detection/scheduler.py

Usage — embedded in consumer (add to processing/consumer.py startup):
    from detection.scheduler import MLOpsScheduler
    scheduler = MLOpsScheduler()
    scheduler.start()
    # ... consumer runs ...
    scheduler.stop()

The scheduler uses threading rather than asyncio so it can be embedded
in the synchronous Kafka consumer loop without refactoring.

Cron alternative (production):
    # /etc/cron.d/food-price-sentinel
    0 2  * * * cd /app && PYTHONPATH=. .venv/bin/python detection/retrain.py --days 90
    0 */6 * * * cd /app && PYTHONPATH=. .venv/bin/python detection/drift.py --window-hours 6 --write-valkey
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("scheduler")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

RETRAIN_INTERVAL_HOURS    = int(os.getenv("RETRAIN_INTERVAL_HOURS", "24"))
DRIFT_CHECK_INTERVAL_HOURS = int(os.getenv("DRIFT_CHECK_INTERVAL_HOURS", "6"))
RETRAIN_DAYS              = int(os.getenv("RETRAIN_DAYS", "90"))
MODELS_DIR                = os.getenv("MODELS_DIR", "detection/models")


# ---------------------------------------------------------------------------
# Job runner
# ---------------------------------------------------------------------------

def run_job(name: str, cmd: list[str]) -> int:
    """
    Run a job as a subprocess. Returns exit code.
    Captures stdout/stderr and logs them.
    """
    log.info("Starting job: %s", name)
    log.info("Command: %s", " ".join(cmd))
    start = time.monotonic()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,   # 10 minute timeout
        )
        duration = time.monotonic() - start

        if result.returncode == 0:
            log.info("Job %s completed successfully in %.1fs", name, duration)
        else:
            log.warning("Job %s exited with code %d in %.1fs", name, result.returncode, duration)

        if result.stdout:
            for line in result.stdout.strip().splitlines():
                log.info("[%s] %s", name, line)
        if result.stderr:
            for line in result.stderr.strip().splitlines():
                log.info("[%s:stderr] %s", name, line)

        return result.returncode

    except subprocess.TimeoutExpired:
        log.error("Job %s timed out after 600s", name)
        return -1
    except Exception as exc:
        log.error("Job %s failed with exception: %s", name, exc)
        return -1


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

class MLOpsScheduler:
    """
    Background thread scheduler for retraining and drift detection.

    Uses threading.Event for clean shutdown — calling stop() will
    interrupt any sleep and allow threads to exit gracefully.
    """

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._python = sys.executable

        # Track last run times
        self._last_retrain:    datetime | None = None
        self._last_drift_check: datetime | None = None

    def start(self) -> None:
        """Start background scheduler threads."""
        retrain_thread = threading.Thread(
            target=self._retrain_loop,
            name="retrain-scheduler",
            daemon=True,
        )
        drift_thread = threading.Thread(
            target=self._drift_loop,
            name="drift-scheduler",
            daemon=True,
        )
        retrain_thread.start()
        drift_thread.start()
        self._threads = [retrain_thread, drift_thread]
        log.info(
            "MLOps scheduler started. Retrain every %dh, drift check every %dh.",
            RETRAIN_INTERVAL_HOURS, DRIFT_CHECK_INTERVAL_HOURS,
        )

    def stop(self) -> None:
        """Signal all threads to stop and wait for them."""
        log.info("Stopping MLOps scheduler...")
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=10)
        log.info("MLOps scheduler stopped.")

    def _retrain_loop(self) -> None:
        """Retraining loop — runs every RETRAIN_INTERVAL_HOURS."""
        # Stagger initial run: wait half the interval before first retrain
        # so the scheduler doesn't immediately fire on startup
        stagger = (RETRAIN_INTERVAL_HOURS * 3600) // 2
        log.info("Retrain scheduler: first run in %.1fh", stagger / 3600)
        self._stop_event.wait(timeout=stagger)

        while not self._stop_event.is_set():
            self._run_retrain()
            self._stop_event.wait(timeout=RETRAIN_INTERVAL_HOURS * 3600)

    def _drift_loop(self) -> None:
        """Drift check loop — runs every DRIFT_CHECK_INTERVAL_HOURS."""
        # First drift check after 1 hour (let some data accumulate)
        self._stop_event.wait(timeout=3600)

        while not self._stop_event.is_set():
            self._run_drift_check()
            self._stop_event.wait(timeout=DRIFT_CHECK_INTERVAL_HOURS * 3600)

    def _run_retrain(self) -> None:
        now = datetime.now(tz=timezone.utc)
        log.info("Scheduled retrain starting at %s", now.isoformat())
        self._last_retrain = now

        rc = run_job(
            name="retrain",
            cmd=[
                self._python, "detection/retrain.py",
                "--days", str(RETRAIN_DAYS),
                "--version", "auto",
                "--models-dir", MODELS_DIR,
            ],
        )

        if rc == 0:
            log.info("Retrain succeeded — new model promoted")
            # Immediately run drift check after successful retrain
            self._run_drift_check()
        else:
            log.warning("Retrain did not promote new model (rc=%d)", rc)

    def _run_drift_check(self) -> None:
        now = datetime.now(tz=timezone.utc)
        log.info("Scheduled drift check starting at %s", now.isoformat())
        self._last_drift_check = now

        run_job(
            name="drift-check",
            cmd=[
                self._python, "detection/drift.py",
                "--window-hours", str(DRIFT_CHECK_INTERVAL_HOURS),
                "--write-valkey",
                "--models-dir", MODELS_DIR,
            ],
        )

    def status(self) -> dict:
        return {
            "retrain_interval_hours":    RETRAIN_INTERVAL_HOURS,
            "drift_check_interval_hours": DRIFT_CHECK_INTERVAL_HOURS,
            "last_retrain":              self._last_retrain.isoformat() if self._last_retrain else None,
            "last_drift_check":          self._last_drift_check.isoformat() if self._last_drift_check else None,
            "threads_alive":             [t.is_alive() for t in self._threads],
        }


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

def main() -> None:
    scheduler = MLOpsScheduler()
    scheduler.start()

    running = True

    def _shutdown(sig, frame):
        nonlocal running
        log.info("Shutdown signal received.")
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("Scheduler running standalone. Press Ctrl+C to stop.")
    log.info("Status: %s", scheduler.status())

    while running:
        time.sleep(60)
        log.info("Scheduler heartbeat: %s", scheduler.status())

    scheduler.stop()


if __name__ == "__main__":
    main()
