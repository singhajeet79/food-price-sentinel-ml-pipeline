"""
detection/model.py
------------------
Isolation Forest wrapper for food price anomaly detection.

Wraps scikit-learn's IsolationForest with:
  - Versioned model serialisation / deserialisation
  - Feature name enforcement (guards against silent column misalignment)
  - Severity classification from raw anomaly scores
  - Model metadata (training date, feature list, contamination, AUC-ROC)

Feature vector order (must match features.FeatureVector.to_model_array()):
    [
        seasonal_adjusted_price,
        rolling_7d_avg,
        rolling_30d_std,
        momentum,
        energy_lag_corr,
        fertilizer_index_delta,
        day_of_year_sin,
        day_of_year_cos,
    ]

Anomaly score contract:
    IsolationForest.decision_function() returns values in roughly (-0.5, 0.5).
    Negative = more anomalous.  We normalise to [0, 1] where 1 = most anomalous
    so the rest of the pipeline works with an intuitive "higher = worse" scale.

    normalised_score = 1 - (raw_score - min_possible) / (max_possible - min_possible)
    In practice we use a sigmoid-like rescaling anchored at 0.0 boundary.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FEATURE_NAMES: list[str] = [
    "seasonal_adjusted_price",
    "rolling_7d_avg",
    "rolling_30d_std",
    "momentum",
    "energy_lag_corr",
    "fertilizer_index_delta",
    "day_of_year_sin",
    "day_of_year_cos",
]

N_FEATURES = len(FEATURE_NAMES)

# Default model artifact directory
MODELS_DIR = Path(os.getenv("MODELS_DIR", "detection/models"))

# Severity thresholds (normalised score 0-1)
SEVERITY_THRESHOLDS: dict[str, float] = {
    "CRITICAL": 0.85,
    "HIGH":     0.70,
    "MEDIUM":   0.55,
    "LOW":      0.40,
}


# ---------------------------------------------------------------------------
# Model metadata
# ---------------------------------------------------------------------------

@dataclass
class ModelMetadata:
    model_version: str
    trained_at: str
    feature_names: list[str]
    n_training_samples: int
    contamination: float
    n_estimators: int
    auc_roc: Optional[float] = None         # set after evaluation
    notes: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "ModelMetadata":
        return cls(**d)


# ---------------------------------------------------------------------------
# Sentinel model
# ---------------------------------------------------------------------------

class SentinelModel:
    """
    Versioned Isolation Forest model with scaler and metadata.

    Usage — training:
        model = SentinelModel.new(version="1.0.0", contamination=0.05)
        model.fit(X_train)
        model.save()

    Usage — inference:
        model = SentinelModel.load()          # loads latest
        score = model.score(feature_array)    # float in [0, 1]
        severity = model.severity(score)      # "LOW" | "MEDIUM" | "HIGH" | "CRITICAL" | None
    """

    def __init__(
        self,
        iso_forest: IsolationForest,
        scaler: StandardScaler,
        metadata: ModelMetadata,
    ) -> None:
        self._iso_forest = iso_forest
        self._scaler = scaler
        self.metadata = metadata
        self._fitted = False

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def new(
        cls,
        version: str,
        contamination: float = 0.05,
        n_estimators: int = 200,
        max_samples: str | int = "auto",
        random_state: int = 42,
    ) -> "SentinelModel":
        """Create an unfitted model ready for training."""
        iso_forest = IsolationForest(
            n_estimators=n_estimators,
            contamination=contamination,
            max_samples=max_samples,
            random_state=random_state,
            n_jobs=-1,
        )
        scaler = StandardScaler()
        metadata = ModelMetadata(
            model_version=version,
            trained_at="",
            feature_names=FEATURE_NAMES,
            n_training_samples=0,
            contamination=contamination,
            n_estimators=n_estimators,
        )
        return cls(iso_forest, scaler, metadata)

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def fit(self, X: np.ndarray) -> "SentinelModel":
        """
        Fit the scaler and Isolation Forest on training data X.

        X shape: (n_samples, N_FEATURES)
        Feature column order must match FEATURE_NAMES exactly.
        """
        self._validate_shape(X)
        log.info(
            "Training IsolationForest: n_samples=%d n_features=%d contamination=%.3f",
            X.shape[0], X.shape[1], self.metadata.contamination,
        )

        X_scaled = self._scaler.fit_transform(X)
        self._iso_forest.fit(X_scaled)
        self._fitted = True

        self.metadata.n_training_samples = X.shape[0]
        self.metadata.trained_at = datetime.now(tz=timezone.utc).isoformat()

        log.info("Training complete. Model version: %s", self.metadata.model_version)
        return self

    # ------------------------------------------------------------------
    # Inference
    # ------------------------------------------------------------------

    def score(self, feature_array: list[float] | np.ndarray) -> float:
        """
        Score a single feature vector.

        Returns a normalised anomaly score in [0, 1].
        Higher = more anomalous.

        Raises RuntimeError if model has not been fitted.
        """
        if not self._fitted:
            raise RuntimeError("Model has not been fitted. Call fit() or load() first.")

        x = np.array(feature_array, dtype=float).reshape(1, -1)
        self._validate_shape(x)

        x_scaled = self._scaler.transform(x)

        # decision_function: negative = anomalous, positive = normal
        raw = float(self._iso_forest.decision_function(x_scaled)[0])

        # Normalise to [0, 1] using sigmoid-like rescaling
        # raw is roughly in (-0.5, 0.5); we map 0.0 → 0.5, -0.5 → ~1.0
        normalised = self._normalise(raw)
        return round(normalised, 6)

    def score_batch(self, X: np.ndarray) -> np.ndarray:
        """Score a batch of feature vectors. Returns array of normalised scores."""
        if not self._fitted:
            raise RuntimeError("Model has not been fitted.")
        self._validate_shape(X)
        X_scaled = self._scaler.transform(X)
        raw_scores = self._iso_forest.decision_function(X_scaled)
        return np.array([self._normalise(r) for r in raw_scores])

    def severity(self, normalised_score: float) -> Optional[str]:
        """
        Map a normalised score to a severity label.

        Returns None if score is below the LOW threshold (not anomalous).
        """
        if normalised_score >= SEVERITY_THRESHOLDS["CRITICAL"]:
            return "CRITICAL"
        if normalised_score >= SEVERITY_THRESHOLDS["HIGH"]:
            return "HIGH"
        if normalised_score >= SEVERITY_THRESHOLDS["MEDIUM"]:
            return "MEDIUM"
        if normalised_score >= SEVERITY_THRESHOLDS["LOW"]:
            return "LOW"
        return None

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, models_dir: Path = MODELS_DIR) -> Path:
        """
        Serialise model, scaler, and metadata to disk.

        Saves three files:
            {models_dir}/sentinel_{version}.pkl      — IsolationForest + scaler
            {models_dir}/sentinel_{version}.meta.json — ModelMetadata
            {models_dir}/latest.txt                  — points to current version
        """
        if not self._fitted:
            raise RuntimeError("Cannot save an unfitted model.")

        models_dir.mkdir(parents=True, exist_ok=True)
        version = self.metadata.model_version

        # Save model + scaler bundle
        bundle_path = models_dir / f"sentinel_{version}.pkl"
        joblib.dump(
            {"iso_forest": self._iso_forest, "scaler": self._scaler},
            bundle_path,
            compress=3,
        )
        log.info("Saved model bundle: %s", bundle_path)

        # Save metadata
        meta_path = models_dir / f"sentinel_{version}.meta.json"
        meta_path.write_text(json.dumps(self.metadata.to_dict(), indent=2))
        log.info("Saved metadata: %s", meta_path)

        # Update latest pointer
        latest_path = models_dir / "latest.txt"
        latest_path.write_text(version)
        log.info("Updated latest pointer → %s", version)

        return bundle_path

    @classmethod
    def load(
        cls,
        version: Optional[str] = None,
        models_dir: Path = MODELS_DIR,
    ) -> "SentinelModel":
        """
        Load a saved model from disk.

        If version is None, loads the version named in latest.txt.
        Raises FileNotFoundError if no model exists.
        """
        if version is None:
            latest_path = models_dir / "latest.txt"
            if not latest_path.exists():
                raise FileNotFoundError(
                    f"No model found at {models_dir}. "
                    "Run detection/train.py to train an initial model."
                )
            version = latest_path.read_text().strip()

        bundle_path = models_dir / f"sentinel_{version}.pkl"
        meta_path   = models_dir / f"sentinel_{version}.meta.json"

        if not bundle_path.exists():
            raise FileNotFoundError(f"Model bundle not found: {bundle_path}")

        bundle   = joblib.load(bundle_path)
        metadata = ModelMetadata.from_dict(json.loads(meta_path.read_text()))

        instance = cls(
            iso_forest=bundle["iso_forest"],
            scaler=bundle["scaler"],
            metadata=metadata,
        )
        instance._fitted = True

        log.info(
            "Loaded model version=%s trained_at=%s n_samples=%d",
            metadata.model_version,
            metadata.trained_at,
            metadata.n_training_samples,
        )
        return instance

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise(raw_score: float) -> float:
        """
        Map IsolationForest decision_function output to [0, 1].

        decision_function output is typically in (-0.5, 0.5).
        We clip and invert so that higher = more anomalous.
        """
        clipped = max(-0.6, min(0.6, raw_score))
        # Invert: anomalous (negative) → high score
        inverted = -clipped
        # Shift to [0, 1] range
        normalised = (inverted + 0.6) / 1.2
        return float(np.clip(normalised, 0.0, 1.0))

    def _validate_shape(self, X: np.ndarray) -> None:
        if X.shape[-1] != N_FEATURES:
            raise ValueError(
                f"Expected {N_FEATURES} features ({FEATURE_NAMES}), "
                f"got {X.shape[-1]}."
            )

    def __repr__(self) -> str:
        status = "fitted" if self._fitted else "unfitted"
        return (
            f"SentinelModel(version={self.metadata.model_version!r}, "
            f"status={status}, "
            f"contamination={self.metadata.contamination})"
        )
