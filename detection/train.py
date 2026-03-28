"""
detection/train.py
------------------
Offline training script for the SentinelModel.

Two modes:
  1. --source simulate   Generate synthetic training data from the
                         SimulatedFoodFeed and run a full training cycle.
                         Use this for initial bootstrap before real data
                         accumulates in PostgreSQL.

  2. --source postgres   Pull the last N days of feature_vectors from
                         PostgreSQL and retrain. This is what the daily
                         cron job runs (Phase 3).

Evaluation:
    After training, the script evaluates the model against a held-out
    validation set (20% of training data). Reports:
      - Anomaly rate (% of validation samples flagged)
      - Score distribution (min, p25, p50, p75, max)
      - Contamination alignment (actual rate vs configured rate)

    If --compare is passed and a previous model exists, the new model's
    anomaly rate is compared against the old one. If the new model's
    rate deviates more than DRIFT_TOLERANCE from the old, a warning is
    logged and the old model is kept.

Usage:
    # Bootstrap with synthetic data (no DB needed)
    python -m detection.train --source simulate --n-samples 5000 --version 1.0.0

    # Retrain from PostgreSQL (production cron)
    python -m detection.train --source postgres --days 90 --compare

Environment variables:
    POSTGRES_URL
    MODELS_DIR
    ANOMALY_THRESHOLD
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from detection.model import MODELS_DIR, SentinelModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("train")

DRIFT_TOLERANCE = 0.15  # max allowed shift in anomaly rate vs previous model


# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------


def generate_synthetic_features(
    n_samples: int = 5000,
    anomaly_fraction: float = 0.05,
    random_seed: int = 42,
) -> np.ndarray:
    """
    Generate a synthetic feature matrix for bootstrapping the model.

    Structure:
      - (1 - anomaly_fraction) of samples are drawn from a "normal" distribution
        anchored around realistic food price baselines.
      - anomaly_fraction of samples are injected shocks: price spikes or crashes
        with elevated momentum and fertilizer delta values.

    This gives the Isolation Forest enough contrast to learn the normal
    manifold before real data is available.

    Returns X: np.ndarray of shape (n_samples, N_FEATURES)
    """
    rng = np.random.default_rng(random_seed)
    n_normal = int(n_samples * (1 - anomaly_fraction))
    n_anomaly = n_samples - n_normal

    # --- Normal samples ---
    # seasonal_adjusted_price: mean 300, std 25 (wheat-like)
    normal_sap = rng.normal(300.0, 25.0, n_normal)
    # rolling_7d_avg: close to seasonal_adjusted_price
    normal_7d_avg = normal_sap + rng.normal(0, 5.0, n_normal)
    # rolling_30d_std: low during normal periods
    normal_30d_std = rng.gamma(shape=2.0, scale=4.0, size=n_normal)
    # momentum: near zero, mild positive drift
    normal_momentum = rng.normal(0.001, 0.012, n_normal)
    # energy_lag_corr: moderate positive correlation
    normal_energy = rng.normal(0.35, 0.20, n_normal)
    # fertilizer_delta: small changes
    normal_fert = rng.normal(0.002, 0.015, n_normal)
    # cyclical encodings: uniformly distributed through the year
    doys_normal = rng.integers(1, 366, n_normal)
    normal_sin = np.sin(2 * np.pi * doys_normal / 365)
    normal_cos = np.cos(2 * np.pi * doys_normal / 365)

    X_normal = np.column_stack(
        [
            normal_sap,
            normal_7d_avg,
            normal_30d_std,
            normal_momentum,
            normal_energy,
            normal_fert,
            normal_sin,
            normal_cos,
        ]
    )

    # --- Anomalous samples ---
    # Price spikes: seasonal_adjusted_price >> rolling_7d_avg
    anomaly_sap = rng.normal(300.0, 25.0, n_anomaly) * rng.uniform(1.2, 2.0, n_anomaly)
    anomaly_7d_avg = rng.normal(300.0, 25.0, n_anomaly)  # avg hasn't caught up
    anomaly_30d_std = rng.gamma(
        shape=3.0, scale=15.0, size=n_anomaly
    )  # high volatility
    anomaly_momentum = rng.uniform(0.08, 0.45, n_anomaly)  # sharp upward momentum
    anomaly_energy = rng.normal(0.70, 0.15, n_anomaly)  # high energy correlation
    anomaly_fert = rng.uniform(0.05, 0.30, n_anomaly)  # large fertilizer delta
    doys_anomaly = rng.integers(1, 366, n_anomaly)
    anomaly_sin = np.sin(2 * np.pi * doys_anomaly / 365)
    anomaly_cos = np.cos(2 * np.pi * doys_anomaly / 365)

    X_anomaly = np.column_stack(
        [
            anomaly_sap,
            anomaly_7d_avg,
            anomaly_30d_std,
            anomaly_momentum,
            anomaly_energy,
            anomaly_fert,
            anomaly_sin,
            anomaly_cos,
        ]
    )

    X = np.vstack([X_normal, X_anomaly])

    # Shuffle
    idx = rng.permutation(len(X))
    return X[idx]


# ---------------------------------------------------------------------------
# PostgreSQL data loading (stub — wired up in Phase 3)
# ---------------------------------------------------------------------------


def load_features_from_postgres(days: int = 90) -> np.ndarray:
    """
    Load feature vectors from PostgreSQL for the last `days` days.

    Stub — replace with real SQLAlchemy query once storage/ is wired up:

        from storage.db import get_session
        from storage.models import FeatureVectorORM
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        with get_session() as session:
            rows = session.query(FeatureVectorORM).filter(
                FeatureVectorORM.computed_at >= cutoff,
                FeatureVectorORM.is_seasonal_adjusted == True,
            ).all()
        return np.array([[
            r.seasonal_adjusted_price, r.rolling_7d_avg, r.rolling_30d_std,
            r.momentum, r.energy_lag_corr, r.fertilizer_index_delta,
            r.day_of_year_sin, r.day_of_year_cos,
        ] for r in rows])
    """
    raise NotImplementedError(
        "PostgreSQL training data loading is not yet implemented. "
        "Run with --source simulate for bootstrap training."
    )


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


def evaluate(
    model: SentinelModel,
    X_val: np.ndarray,
    contamination: float,
) -> dict:
    """
    Evaluate the model on a held-out validation set.

    Returns a dict of evaluation metrics.
    """
    scores = model.score_batch(X_val)
    threshold = float(os.getenv("ANOMALY_THRESHOLD", "0.55"))

    flagged = scores >= threshold
    anomaly_rate = float(flagged.mean())

    metrics = {
        "n_val_samples": len(scores),
        "anomaly_rate": round(anomaly_rate, 4),
        "configured_contamination": contamination,
        "score_min": round(float(scores.min()), 4),
        "score_p25": round(float(np.percentile(scores, 25)), 4),
        "score_p50": round(float(np.percentile(scores, 50)), 4),
        "score_p75": round(float(np.percentile(scores, 75)), 4),
        "score_max": round(float(scores.max()), 4),
        "threshold_used": threshold,
    }

    log.info("Evaluation results:")
    for k, v in metrics.items():
        log.info("  %-35s %s", k, v)

    return metrics


# ---------------------------------------------------------------------------
# Comparison with previous model
# ---------------------------------------------------------------------------


def compare_with_previous(
    new_metrics: dict,
    models_dir: Path,
) -> bool:
    """
    Compare new model anomaly rate against the currently deployed model.

    Returns True if new model should be promoted, False if old model should be kept.
    """
    try:
        old_model = SentinelModel.load(models_dir=models_dir)
    except FileNotFoundError:
        log.info("No existing model found — promoting new model automatically.")
        return True

    log.info(
        "Comparing with existing model version=%s",
        old_model.metadata.model_version,
    )

    new_rate = new_metrics["anomaly_rate"]
    old_contamination = old_model.metadata.contamination

    drift = abs(new_rate - old_contamination)
    log.info(
        "Anomaly rate drift: new_rate=%.4f old_contamination=%.4f drift=%.4f tolerance=%.4f",
        new_rate,
        old_contamination,
        drift,
        DRIFT_TOLERANCE,
    )

    if drift > DRIFT_TOLERANCE:
        log.warning(
            "Drift %.4f exceeds tolerance %.4f — keeping existing model %s",
            drift,
            DRIFT_TOLERANCE,
            old_model.metadata.model_version,
        )
        return False

    log.info("Drift within tolerance — promoting new model.")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run(args: argparse.Namespace) -> None:
    log.info("=== Food Price Sentinel — Model Training ===")
    log.info("Source: %s | Version: %s", args.source, args.version)

    # 1. Load training data
    if args.source == "simulate":
        log.info(
            "Generating %d synthetic samples (anomaly_fraction=%.2f)",
            args.n_samples,
            args.contamination,
        )
        X = generate_synthetic_features(
            n_samples=args.n_samples,
            anomaly_fraction=args.contamination,
            random_seed=args.seed,
        )
    elif args.source == "postgres":
        log.info("Loading feature vectors from PostgreSQL (last %d days)", args.days)
        X = load_features_from_postgres(days=args.days)
    else:
        raise ValueError(f"Unknown source: {args.source}")

    log.info("Training data shape: %s", X.shape)

    # 2. Train/validation split (80/20)
    rng = np.random.default_rng(args.seed)
    idx = rng.permutation(len(X))
    split = int(len(X) * 0.8)
    X_train, X_val = X[idx[:split]], X[idx[split:]]
    log.info("Train: %d samples | Val: %d samples", len(X_train), len(X_val))

    # 3. Build and train model
    model = SentinelModel.new(
        version=args.version,
        contamination=args.contamination,
        n_estimators=args.n_estimators,
        random_state=args.seed,
    )
    model.fit(X_train)

    # 4. Evaluate
    metrics = evaluate(model, X_val, args.contamination)
    model.metadata.notes = (
        f"source={args.source} val_anomaly_rate={metrics['anomaly_rate']}"
    )

    # 5. Compare with previous model (if requested)
    models_dir = Path(args.models_dir)
    if args.compare:
        should_promote = compare_with_previous(metrics, models_dir)
    else:
        should_promote = True

    # 6. Save if promoting
    if should_promote:
        saved_path = model.save(models_dir=models_dir)
        log.info("Model saved to: %s", saved_path)
        log.info("Model promoted as latest version: %s", args.version)
    else:
        log.warning("New model NOT saved — previous model retained.")

    log.info("=== Training complete ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train the Food Price Sentinel anomaly model"
    )
    parser.add_argument(
        "--source",
        choices=["simulate", "postgres"],
        default="simulate",
        help="Training data source (default: simulate)",
    )
    parser.add_argument("--version", default="1.0.0", help="Model version string")
    parser.add_argument(
        "--n-samples", type=int, default=5000, help="Samples for simulate mode"
    )
    parser.add_argument(
        "--contamination", type=float, default=0.05, help="Expected anomaly fraction"
    )
    parser.add_argument("--n-estimators", type=int, default=200, help="Number of trees")
    parser.add_argument(
        "--days", type=int, default=90, help="Days of history for postgres mode"
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--models-dir", default=str(MODELS_DIR), help="Model artifact directory"
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Compare with existing model before promoting",
    )
    args = parser.parse_args()
    run(args)
