"""
detection/score.py
------------------
Online anomaly scorer — the live inference layer.

Used by processing/consumer.py to score every FeatureVector that passes
the data quality gates. Replaces the score_feature_vector() stub.

Key behaviours:
  - Loads the model once at startup (lazy singleton pattern)
  - Hot-reloads the model when the latest.txt pointer changes on disk
    (allows zero-downtime model upgrades during the daily retrain)
  - Returns a ScoredResult with both the raw score and severity label
  - Emits structured log lines for every score so anomalies are visible
    in log aggregators (Loki, CloudWatch, etc.) without a DB query

Integration — replace the stub in processing/consumer.py:

    # Old stub:
    def score_feature_vector(fv: FeatureVector) -> Optional[float]:
        return None

    # New (add to top of consumer.py):
    from detection.score import Scorer
    _scorer = Scorer()

    # Inside run():
    score_result = _scorer.score(fv)
    if score_result and score_result.severity:
        # → forward to alerting layer
        pass
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys_path_hack = True  # removed when package structure is finalised
if sys_path_hack:
    import sys

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from detection.model import SentinelModel, MODELS_DIR  # noqa: E402
from processing.features import FeatureVector  # noqa: E402

log = logging.getLogger(__name__)

ANOMALY_THRESHOLD: float = float(os.getenv("ANOMALY_THRESHOLD", "0.55"))


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class ScoredResult:
    """The output of scoring one FeatureVector."""

    commodity: str
    region: str
    data_as_of: datetime
    scored_at: datetime

    normalised_score: float  # [0, 1] — higher = more anomalous
    severity: Optional[str]  # None | LOW | MEDIUM | HIGH | CRITICAL
    is_anomaly: bool  # True if score >= ANOMALY_THRESHOLD
    is_low_confidence: bool  # propagated from FeatureVector

    model_version: str
    feature_snapshot: dict  # the feature values that produced this score

    def to_dict(self) -> dict:
        return {
            "commodity": self.commodity,
            "region": self.region,
            "data_as_of": self.data_as_of.isoformat(),
            "scored_at": self.scored_at.isoformat(),
            "normalised_score": self.normalised_score,
            "severity": self.severity,
            "is_anomaly": self.is_anomaly,
            "is_low_confidence": self.is_low_confidence,
            "model_version": self.model_version,
            "feature_snapshot": self.feature_snapshot,
        }


# ---------------------------------------------------------------------------
# Scorer
# ---------------------------------------------------------------------------


class Scorer:
    """
    Singleton-friendly online scorer.

    Instantiate once per consumer process. Automatically hot-reloads
    the model when the latest.txt pointer on disk changes — enabling
    zero-downtime model upgrades.

    Usage:
        scorer = Scorer()                          # loads model at init
        result = scorer.score(feature_vector)      # ScoredResult or None
    """

    def __init__(self, models_dir: Path = MODELS_DIR) -> None:
        self._models_dir = models_dir
        self._model: Optional[SentinelModel] = None
        self._loaded_version: Optional[str] = None
        self._load_model()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def score(self, fv: FeatureVector) -> Optional[ScoredResult]:
        """
        Score a FeatureVector.

        Returns None if:
          - No model is loaded (logs a warning)
          - The feature array contains NaN or Inf values

        Otherwise returns a ScoredResult.
        """
        # Hot-reload check
        self._maybe_reload()

        if self._model is None:
            log.warning(
                "No model loaded — cannot score %s/%s. "
                "Run detection/train.py to create an initial model.",
                fv.commodity,
                fv.region,
            )
            return None

        feature_array = fv.to_model_array()

        # Guard against invalid values
        import math

        if any(math.isnan(v) or math.isinf(v) for v in feature_array):
            log.warning(
                "Invalid feature values for %s/%s — skipping score. values=%s",
                fv.commodity,
                fv.region,
                feature_array,
            )
            return None

        try:
            normalised_score = self._model.score(feature_array)
            severity = self._model.severity(normalised_score)
            is_anomaly = normalised_score >= ANOMALY_THRESHOLD

            result = ScoredResult(
                commodity=fv.commodity,
                region=fv.region,
                data_as_of=fv.data_as_of,
                scored_at=datetime.now(tz=timezone.utc),
                normalised_score=normalised_score,
                severity=severity,
                is_anomaly=is_anomaly,
                is_low_confidence=fv.is_low_confidence,
                model_version=self._loaded_version or "unknown",
                feature_snapshot={
                    "seasonal_adjusted_price": fv.seasonal_adjusted_price,
                    "rolling_7d_avg": fv.rolling_7d_avg,
                    "rolling_30d_std": fv.rolling_30d_std,
                    "momentum": fv.momentum,
                    "energy_lag_corr": fv.energy_lag_corr,
                    "fertilizer_index_delta": fv.fertilizer_index_delta,
                    "day_of_year_sin": fv.day_of_year_sin,
                    "day_of_year_cos": fv.day_of_year_cos,
                },
            )

            self._log_result(result)
            return result

        except Exception as exc:
            log.error(
                "Scoring failed for %s/%s: %s",
                fv.commodity,
                fv.region,
                exc,
                exc_info=True,
            )
            return None

    # ------------------------------------------------------------------
    # Model loading / hot-reload
    # ------------------------------------------------------------------

    def _load_model(self) -> None:
        """Load the latest model from disk. Silently skips if none exists."""
        try:
            self._model = SentinelModel.load(models_dir=self._models_dir)
            self._loaded_version = self._model.metadata.model_version
            log.info(
                "Scorer loaded model version=%s trained_at=%s",
                self._loaded_version,
                self._model.metadata.trained_at,
            )
        except FileNotFoundError:
            log.warning(
                "No trained model found at %s. "
                "Scorer will operate in pass-through mode until a model is trained.",
                self._models_dir,
            )
            self._model = None
            self._loaded_version = None

    def _current_latest_version(self) -> Optional[str]:
        """Read the current version from latest.txt without loading the model."""
        latest_path = self._models_dir / "latest.txt"
        if not latest_path.exists():
            return None
        return latest_path.read_text().strip()

    def _maybe_reload(self) -> None:
        """
        Check if latest.txt has changed since last load.
        If so, reload the model (hot-reload for zero-downtime upgrades).
        """
        current = self._current_latest_version()
        if current != self._loaded_version:
            log.info(
                "Model version changed on disk: %s → %s. Hot-reloading.",
                self._loaded_version,
                current,
            )
            self._load_model()

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    @staticmethod
    def _log_result(result: ScoredResult) -> None:
        if result.is_anomaly:
            log.warning(
                "ANOMALY DETECTED | %s/%s | score=%.4f severity=%s "
                "low_conf=%s model=%s",
                result.commodity,
                result.region,
                result.normalised_score,
                result.severity,
                result.is_low_confidence,
                result.model_version,
            )
        else:
            log.debug(
                "Normal | %s/%s | score=%.4f model=%s",
                result.commodity,
                result.region,
                result.normalised_score,
                result.model_version,
            )
