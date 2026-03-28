"""
detection/drift.py
------------------
Score distribution drift detector for the SentinelModel.

Compares the current model's score distribution over a recent window
against the distribution seen during training (stored in model metadata).
Flags drift when the distributions diverge significantly.

Two drift signals:

  1. Score median drift  — p50 of recent scores has shifted relative to
     the training-time p50. A rising median means the model is seeing
     increasingly anomalous data OR the model has gone stale and is
     scoring everything higher. Either warrants investigation.

  2. Anomaly rate drift  — % of recent events above ANOMALY_THRESHOLD
     has diverged significantly from the configured contamination rate.
     Large upward drift = genuine market stress OR model false-positive storm.
     Large downward drift = model no longer detecting (stale / distribution shift).

Output:
  - Structured dict with drift metrics and a DRIFT / STABLE / STALE signal
  - Optionally writes a drift alert to Valkey (key: drift:{model_version})
  - Logs WARNING when drift is detected so log aggregators can alert

Usage:
    # Run manually
    python detection/drift.py --window-hours 24

    # Run from cron after retraining
    python detection/drift.py --window-hours 24 --write-valkey
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from detection.model import SentinelModel, MODELS_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("drift")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ANOMALY_THRESHOLD          = float(os.getenv("ANOMALY_THRESHOLD", "0.55"))
MEDIAN_DRIFT_THRESHOLD     = 0.08   # p50 shift > this triggers DRIFT
RATE_DRIFT_THRESHOLD       = 0.20   # anomaly rate shift > this triggers DRIFT
STALE_RATE_THRESHOLD       = 0.02   # anomaly rate < this triggers STALE
VALKEY_DRIFT_TTL           = 3600 * 6  # 6h TTL on drift alerts


# ---------------------------------------------------------------------------
# Load recent scores from PostgreSQL
# ---------------------------------------------------------------------------

def load_recent_scores(window_hours: int) -> tuple[np.ndarray, int]:
    """
    Load anomaly scores from anomaly_log for the last `window_hours`.

    Returns (scores_array, total_events_in_window).
    `total_events_in_window` includes non-anomalous events (is_anomaly=False)
    so we can compute the true anomaly rate.
    """
    from dotenv import load_dotenv
    load_dotenv()

    from storage.db import get_session
    from storage.models import AnomalyLog

    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=window_hours)

    with get_session() as session:
        all_rows = (
            session.query(AnomalyLog)
            .filter(AnomalyLog.detected_at >= cutoff)
            .all()
        )

    if not all_rows:
        return np.array([]), 0

    scores = np.array([float(r.anomaly_score or 0) for r in all_rows])
    return scores, len(all_rows)


# ---------------------------------------------------------------------------
# Drift computation
# ---------------------------------------------------------------------------

def compute_drift(
    scores: np.ndarray,
    total_events: int,
    model: SentinelModel,
) -> dict:
    """
    Compare recent score distribution against training-time baseline.

    Training baseline is stored in model metadata. If not present
    (older model format), uses configured contamination as proxy.
    """
    if len(scores) == 0:
        return {
            "signal":        "UNKNOWN",
            "reason":        "No recent scores available",
            "window_events": total_events,
            "n_scores":      0,
        }

    # Recent distribution
    recent_p50   = float(np.percentile(scores, 50))
    recent_p75   = float(np.percentile(scores, 75))
    recent_p25   = float(np.percentile(scores, 25))
    recent_rate  = float((scores >= ANOMALY_THRESHOLD).mean())
    recent_max   = float(scores.max())

    # Training baseline from metadata
    # We use contamination as proxy for expected anomaly rate
    # and assume training p50 was around 0.35-0.45 (normal regime)
    baseline_rate = model.metadata.contamination
    baseline_p50  = 0.40   # approximate for IsolationForest on normal data

    # Parse actual validation metrics from model metadata notes
    notes = model.metadata.notes or ""
    if "anomaly_rate=" in notes:
        try:
            baseline_rate = float(notes.split("anomaly_rate=")[1].split(" |")[0].strip())
        except (ValueError, IndexError):
            pass
    if "val_p50=" in notes:
        try:
            baseline_p50 = float(notes.split("val_p50=")[1].split(" |")[0].strip())
        except (ValueError, IndexError):
            pass

    # Compute drift signals
    median_drift = abs(recent_p50 - baseline_p50)
    rate_drift   = abs(recent_rate - baseline_rate)
    rate_delta   = recent_rate - baseline_rate

    # Determine signal
    if recent_rate < STALE_RATE_THRESHOLD and len(scores) > 20:
        signal = "STALE"
        reason = (
            f"Anomaly rate {recent_rate:.4f} below stale threshold {STALE_RATE_THRESHOLD}. "
            "Model may no longer be detecting anomalies — retrain recommended."
        )
    elif median_drift > MEDIAN_DRIFT_THRESHOLD or rate_drift > RATE_DRIFT_THRESHOLD:
        signal = "DRIFT"
        reason = (
            f"Score distribution has shifted. "
            f"Median drift: {median_drift:.4f} (threshold {MEDIAN_DRIFT_THRESHOLD}). "
            f"Rate drift: {rate_drift:.4f} (threshold {RATE_DRIFT_THRESHOLD}). "
            f"Rate delta: {rate_delta:+.4f} ({'↑ more anomalies' if rate_delta > 0 else '↓ fewer anomalies'}). "
            "Retraining recommended."
        )
    else:
        signal = "STABLE"
        reason = (
            f"Score distribution within expected range. "
            f"Median drift: {median_drift:.4f}, Rate drift: {rate_drift:.4f}"
        )

    result = {
        "signal":          signal,
        "reason":          reason,
        "model_version":   model.metadata.model_version,
        "window_events":   total_events,
        "n_scores":        len(scores),
        "recent_p25":      round(recent_p25, 4),
        "recent_p50":      round(recent_p50, 4),
        "recent_p75":      round(recent_p75, 4),
        "recent_max":      round(recent_max, 4),
        "recent_rate":     round(recent_rate, 4),
        "baseline_p50":    round(baseline_p50, 4),
        "baseline_rate":   round(baseline_rate, 4),
        "median_drift":    round(median_drift, 4),
        "rate_drift":      round(rate_drift, 4),
        "rate_delta":      round(rate_delta, 4),
        "checked_at":      datetime.now(tz=timezone.utc).isoformat(),
    }

    return result


# ---------------------------------------------------------------------------
# Valkey writer
# ---------------------------------------------------------------------------

def write_drift_to_valkey(drift_result: dict) -> None:
    """Write drift signal to Valkey so the API and dashboard can surface it."""
    from dotenv import load_dotenv
    load_dotenv()

    try:
        import redis
        client = redis.Redis(
            host=os.getenv("VALKEY_HOST", "localhost"),
            port=int(os.getenv("VALKEY_PORT", "6379")),
            password=os.getenv("VALKEY_PASSWORD"),
            ssl=os.getenv("VALKEY_TLS", "false").lower() == "true",
            decode_responses=True,
            socket_connect_timeout=5,
        )
        key = f"drift:{drift_result['model_version']}"
        client.setex(key, VALKEY_DRIFT_TTL, json.dumps(drift_result))
        log.info("Drift result written to Valkey: key=%s signal=%s", key, drift_result["signal"])
    except Exception as exc:
        log.error("Failed to write drift to Valkey: %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(args: argparse.Namespace) -> int:
    """Returns 0 for STABLE, 1 for DRIFT or STALE."""
    log.info("=== Food Price Sentinel — Drift Detection ===")

    # Load model
    try:
        model = SentinelModel.load(models_dir=Path(args.models_dir))
        log.info("Model loaded: version=%s", model.metadata.model_version)
    except FileNotFoundError as exc:
        log.error("No model found: %s", exc)
        return 1

    # Load recent scores
    scores, total_events = load_recent_scores(args.window_hours)
    log.info(
        "Loaded %d scores from %d events in last %dh",
        len(scores), total_events, args.window_hours,
    )

    if len(scores) < 10:
        log.warning(
            "Only %d scores in window — insufficient for drift detection. "
            "Need at least 10 events.",
            len(scores),
        )
        return 0

    # Compute drift
    result = compute_drift(scores, total_events, model)

    # Log result
    signal = result["signal"]
    if signal == "STABLE":
        log.info("STABLE — %s", result["reason"])
    elif signal == "DRIFT":
        log.warning("DRIFT DETECTED — %s", result["reason"])
        log.warning("Recent: p50=%.4f rate=%.4f | Baseline: p50=%.4f rate=%.4f",
                    result["recent_p50"], result["recent_rate"],
                    result["baseline_p50"], result["baseline_rate"])
    elif signal == "STALE":
        log.warning("MODEL STALE — %s", result["reason"])

    # Full metrics
    log.info("Drift metrics: %s", json.dumps({
        k: v for k, v in result.items()
        if k not in ("signal", "reason", "checked_at")
    }, indent=2))

    # Write to Valkey if requested
    if args.write_valkey:
        write_drift_to_valkey(result)

    # Print summary
    print(json.dumps(result, indent=2))

    return 0 if signal == "STABLE" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect drift in Food Price Sentinel model")
    parser.add_argument("--window-hours", type=int, default=24,
                        help="Lookback window in hours (default: 24)")
    parser.add_argument("--models-dir",   default=str(MODELS_DIR))
    parser.add_argument("--write-valkey", action="store_true",
                        help="Write drift result to Valkey for API/dashboard consumption")
    args = parser.parse_args()
    sys.exit(run(args))
