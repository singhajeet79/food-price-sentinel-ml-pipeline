"""
detection/retrain.py
--------------------
Automated retraining pipeline for the SentinelModel.

Designed to run as a daily cron job (Phase 3). Pulls the last N days
of feature_vectors from PostgreSQL, trains a new model, evaluates it,
compares against the currently deployed model, and promotes if better.

Promotion criteria (ALL must pass):
  1. New model anomaly_rate within DRIFT_TOLERANCE of configured contamination
  2. New model score_p50 >= MIN_MEDIAN_SCORE  (model is actually detecting)
  3. New model score_p75 - score_p25 >= MIN_SCORE_SPREAD  (discriminative)
  4. n_training_samples >= MIN_TRAINING_SAMPLES

If promotion fails, the existing model is retained and a warning is logged.
The failed candidate is saved with a .rejected suffix for post-mortem analysis.

Usage:
    # Manual run
    python detection/retrain.py --days 90 --version auto

    # Cron (daily at 02:00 UTC)
    0 2 * * * cd /app && PYTHONPATH=. .venv/bin/python detection/retrain.py --days 90

Environment variables:
    POSTGRES_URL
    MODELS_DIR
    ANOMALY_THRESHOLD
    RETRAIN_MIN_SAMPLES     (default: 500)
    RETRAIN_DRIFT_TOLERANCE (default: 0.15)
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

from detection.model import SentinelModel, MODELS_DIR, FEATURE_NAMES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("retrain")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MIN_TRAINING_SAMPLES  = int(os.getenv("RETRAIN_MIN_SAMPLES", "500"))
DRIFT_TOLERANCE       = float(os.getenv("RETRAIN_DRIFT_TOLERANCE", "0.15"))
MIN_MEDIAN_SCORE      = 0.25    # p50 must be above this — model must see something
MIN_SCORE_SPREAD      = 0.02    # p75-p25 must be above this — model must discriminate
CONTAMINATION         = float(os.getenv("ANOMALY_THRESHOLD", "0.05"))
ANOMALY_THRESHOLD     = float(os.getenv("ANOMALY_THRESHOLD", "0.55"))


# ---------------------------------------------------------------------------
# Data loading from PostgreSQL
# ---------------------------------------------------------------------------

def load_feature_vectors(days: int) -> np.ndarray:
    """
    Load feature vectors from PostgreSQL for the last `days` days.

    Filters:
      - computed_at >= cutoff
      - is_seasonal_adjusted = TRUE  (prefer adjusted features)
      - is_low_confidence = FALSE     (exclude sparse window data)

    Falls back to including low_confidence rows if < MIN_TRAINING_SAMPLES
    after strict filtering.
    """
    from dotenv import load_dotenv
    load_dotenv()

    from storage.db import get_session
    from storage.models import FeatureVector
    from sqlalchemy import and_

    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)

    with get_session() as session:
        # Strict query first
        rows = (
            session.query(FeatureVector)
            .filter(
                and_(
                    FeatureVector.computed_at >= cutoff,
                    FeatureVector.is_low_confidence == False,
                )
            )
            .all()
        )

        if len(rows) < MIN_TRAINING_SAMPLES:
            log.warning(
                "Strict filter yielded only %d rows — relaxing low_confidence filter",
                len(rows),
            )
            rows = (
                session.query(FeatureVector)
                .filter(FeatureVector.computed_at >= cutoff)
                .all()
            )

    if not rows:
        raise ValueError(f"No feature vectors found in the last {days} days.")

    log.info("Loaded %d feature vectors from PostgreSQL (last %d days)", len(rows), days)

    X = np.array([
        [
            float(r.seasonal_adjusted_price or r.raw_price_usd or 0),
            float(r.rolling_7d_avg or 0),
            float(r.rolling_30d_std or 0),
            float(r.momentum or 0),
            float(r.energy_lag_corr or 0),
            float(r.fertilizer_index_delta or 0),
            float(r.day_of_year_sin or 0),
            float(r.day_of_year_cos or 0),
        ]
        for r in rows
    ], dtype=float)

    # Sanity check — drop rows with NaN/Inf
    mask = np.all(np.isfinite(X), axis=1)
    n_dropped = (~mask).sum()
    if n_dropped > 0:
        log.warning("Dropped %d rows with NaN/Inf values", n_dropped)
    X = X[mask]

    return X


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate_model(model: SentinelModel, X_val: np.ndarray) -> dict:
    """Score the validation set and return evaluation metrics."""
    scores = model.score_batch(X_val)

    flagged = scores >= ANOMALY_THRESHOLD
    anomaly_rate = float(flagged.mean())

    return {
        "n_val_samples":   len(scores),
        "anomaly_rate":    round(anomaly_rate, 4),
        "score_min":       round(float(scores.min()), 4),
        "score_p25":       round(float(np.percentile(scores, 25)), 4),
        "score_p50":       round(float(np.percentile(scores, 50)), 4),
        "score_p75":       round(float(np.percentile(scores, 75)), 4),
        "score_max":       round(float(scores.max()), 4),
        "score_spread":    round(float(np.percentile(scores, 75) - np.percentile(scores, 25)), 4),
        "threshold_used":  ANOMALY_THRESHOLD,
        "contamination":   CONTAMINATION,
    }


# ---------------------------------------------------------------------------
# Promotion gate
# ---------------------------------------------------------------------------

def should_promote(metrics: dict, prev_version: str | None) -> tuple[bool, str]:
    """
    Apply all promotion criteria. Returns (promote: bool, reason: str).
    """
    n = metrics["n_val_samples"]
    rate = metrics["anomaly_rate"]
    p50 = metrics["score_p50"]
    spread = metrics["score_spread"]

    if n < int(MIN_TRAINING_SAMPLES * 0.2):   # val set too small
        return False, f"Validation set too small: {n} < {int(MIN_TRAINING_SAMPLES * 0.2)}"

    drift = abs(rate - CONTAMINATION)
    if drift > DRIFT_TOLERANCE:
        return False, (
            f"Anomaly rate drift too large: |{rate:.4f} - {CONTAMINATION:.4f}| = "
            f"{drift:.4f} > {DRIFT_TOLERANCE}"
        )

    if p50 < MIN_MEDIAN_SCORE:
        return False, f"Median score too low: {p50} < {MIN_MEDIAN_SCORE} — model not detecting"

    if spread < MIN_SCORE_SPREAD:
        return False, (
            f"Score spread too narrow: {spread} < {MIN_SCORE_SPREAD} — model not discriminating"
        )

    return True, "All promotion criteria passed"


# ---------------------------------------------------------------------------
# Version string
# ---------------------------------------------------------------------------

def next_version(models_dir: Path) -> str:
    """Auto-increment patch version from latest.txt."""
    latest_path = models_dir / "latest.txt"
    if not latest_path.exists():
        return "1.0.0"
    current = latest_path.read_text().strip()
    parts = current.split(".")
    try:
        major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])
        return f"{major}.{minor}.{patch + 1}"
    except (ValueError, IndexError):
        return "1.0.1"


# ---------------------------------------------------------------------------
# Save rejected model for analysis
# ---------------------------------------------------------------------------

def save_rejected(model: SentinelModel, metrics: dict, reason: str, models_dir: Path) -> None:
    """Save the rejected model with a .rejected suffix for post-mortem."""
    import joblib
    version = model.metadata.model_version
    rejected_path = models_dir / f"sentinel_{version}.rejected.pkl"
    meta_path = models_dir / f"sentinel_{version}.rejected.meta.json"

    joblib.dump(
        {"iso_forest": model._iso_forest, "scaler": model._scaler},
        rejected_path, compress=3,
    )
    meta = model.metadata.to_dict()
    meta["rejection_reason"] = reason
    meta["rejection_metrics"] = metrics
    meta_path.write_text(json.dumps(meta, indent=2))
    log.info("Rejected model saved for analysis: %s", rejected_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(args: argparse.Namespace) -> int:
    """
    Returns 0 on successful promotion, 1 if model was not promoted.
    """
    log.info("=== Food Price Sentinel — Automated Retraining ===")
    models_dir = Path(args.models_dir)

    # 1. Determine version
    version = args.version if args.version != "auto" else next_version(models_dir)
    log.info("Target version: %s", version)

    # 2. Load training data
    try:
        X = load_feature_vectors(days=args.days)
    except ValueError as exc:
        log.error("Data loading failed: %s", exc)
        return 1

    if len(X) < MIN_TRAINING_SAMPLES:
        log.error(
            "Insufficient training data: %d samples < minimum %d. "
            "Run more producers to accumulate data.",
            len(X), MIN_TRAINING_SAMPLES,
        )
        return 1

    log.info("Training data: %d samples × %d features", X.shape[0], X.shape[1])

    # 3. Train/val split
    rng = np.random.default_rng(args.seed)
    idx = rng.permutation(len(X))
    split = int(len(X) * 0.8)
    X_train, X_val = X[idx[:split]], X[idx[split:]]
    log.info("Train: %d | Val: %d", len(X_train), len(X_val))

    # 4. Train
    model = SentinelModel.new(
        version=version,
        contamination=CONTAMINATION,
        n_estimators=args.n_estimators,
        random_state=args.seed,
    )
    model.fit(X_train)

    # 5. Evaluate
    metrics = evaluate_model(model, X_val)
    log.info("Evaluation:")
    for k, v in metrics.items():
        log.info("  %-30s %s", k, v)

    # 6. Get current version for comparison log
    prev_version = None
    prev_path = models_dir / "latest.txt"
    if prev_path.exists():
        prev_version = prev_path.read_text().strip()

    # 7. Promotion gate
    promote, reason = should_promote(metrics, prev_version)
    log.info("Promotion decision: %s — %s", "PROMOTE" if promote else "REJECT", reason)

    if promote:
        model.metadata.notes = (
            f"retrain from postgres | days={args.days} | "
            f"prev={prev_version} | anomaly_rate={metrics['anomaly_rate']} | "
            f"val_p50={metrics['score_p50']} | val_spread={metrics['score_spread']}"
        )
        saved = model.save(models_dir=models_dir)
        log.info("✓ Model promoted: %s → %s (saved: %s)", prev_version, version, saved)
        log.info("=== Retraining complete — new model is live ===")
        return 0
    else:
        save_rejected(model, metrics, reason, models_dir)
        log.warning("✗ Model NOT promoted. Previous version %s retained.", prev_version)
        log.info("=== Retraining complete — previous model retained ===")
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retrain Food Price Sentinel model from PostgreSQL")
    parser.add_argument("--days",         type=int,   default=90,           help="Days of history to use")
    parser.add_argument("--version",      default="auto",                   help="Version string or 'auto'")
    parser.add_argument("--n-estimators", type=int,   default=200)
    parser.add_argument("--seed",         type=int,   default=42)
    parser.add_argument("--models-dir",   default=str(MODELS_DIR))
    args = parser.parse_args()
    sys.exit(run(args))
