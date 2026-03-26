"""
alerting/alert_schema.py
------------------------
Builder that constructs PriceAlertEvent payloads from a ScoredResult.

Keeps the alerting layer decoupled from the detection layer — the alert
writer only deals with PriceAlertEvent dicts, not ScoredResult objects.

Responsibilities:
  - Compute pct_deviation_usd from current vs baseline price
  - Derive contributing_factors from feature snapshot heuristics
  - Build the dedup_key and cooldown_until timestamp
  - Populate local currency fields when available
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import uuid4

ALERT_COOLDOWN_SECONDS: int = int(os.getenv("ALERT_COOLDOWN_SECONDS", "21600"))


def build_alert_payload(
    scored_result,                          # detection.score.ScoredResult
    current_price_usd: float,
    baseline_price_usd: float,
    model_version: str,
    current_price_local: Optional[float] = None,
    local_currency: Optional[str] = None,
    pct_deviation_local: Optional[float] = None,
    geopolitical_tags: Optional[list[str]] = None,
    is_seasonal_adjusted: bool = True,
) -> dict:
    """
    Build a PriceAlertEvent-compatible dict from a ScoredResult.

    Returns a plain dict (JSON-serialisable) ready for:
      - Kafka producer (price-alerts topic)
      - Valkey SET with TTL
      - PostgreSQL anomaly_log insert
    """
    now = datetime.now(tz=timezone.utc)
    cooldown_until = now + timedelta(seconds=ALERT_COOLDOWN_SECONDS)

    pct_deviation_usd = round(
        (current_price_usd - baseline_price_usd) / baseline_price_usd * 100, 2
    ) if baseline_price_usd > 0 else 0.0

    contributing_factors = _infer_contributing_factors(
        scored_result.feature_snapshot,
        pct_deviation_usd,
    )

    dedup_key = f"{scored_result.commodity}:{scored_result.region}:{scored_result.severity}"

    return {
        "alert_id":              str(uuid4()),
        "commodity":             scored_result.commodity,
        "region":                scored_result.region,
        "anomaly_score":         scored_result.normalised_score,
        "severity":              scored_result.severity,
        "current_price_usd":     current_price_usd,
        "baseline_price_usd":    baseline_price_usd,
        "pct_deviation_usd":     pct_deviation_usd,
        "current_price_local":   current_price_local,
        "local_currency":        local_currency,
        "pct_deviation_local":   pct_deviation_local,
        "contributing_factors":  contributing_factors,
        "geopolitical_tags":     geopolitical_tags or [],
        "is_seasonal_adjusted":  is_seasonal_adjusted,
        "model_version":         model_version,
        "dedup_key":             dedup_key,
        "cooldown_until":        cooldown_until.isoformat(),
        "triggered_at":          now.isoformat(),
        "ttl_seconds":           ALERT_COOLDOWN_SECONDS,
    }


def _infer_contributing_factors(
    feature_snapshot: dict,
    pct_deviation_usd: float,
) -> list[str]:
    """
    Heuristic inference of contributing factors from feature values.

    These are best-effort signals — not causal attribution. They give
    operators a starting point for investigation without requiring a
    separate causal model.

    Rules (thresholds tuned against synthetic training distribution):
      - energy_spike:          energy_lag_corr > 0.60
      - fertilizer_shortage:   fertilizer_index_delta > 0.08
      - demand_surge:          momentum > 0.12 AND energy_corr low
      - currency_depreciation: pct_deviation_local >> pct_deviation_usd (set upstream)
      - transport_disruption:  fertilizer_delta high BUT energy_corr low
    """
    factors = []
    snap = feature_snapshot

    energy_corr = snap.get("energy_lag_corr", 0.0)
    fert_delta   = snap.get("fertilizer_index_delta", 0.0)
    momentum     = snap.get("momentum", 0.0)
    vol          = snap.get("rolling_30d_std", 0.0)
    sap          = snap.get("seasonal_adjusted_price", 0.0)
    avg_7d       = snap.get("rolling_7d_avg", 0.0)

    if energy_corr > 0.60:
        factors.append("energy_spike")

    if fert_delta > 0.08:
        factors.append("fertilizer_shortage")

    if momentum > 0.12 and energy_corr < 0.40:
        factors.append("demand_surge")

    if fert_delta > 0.06 and energy_corr < 0.35:
        factors.append("transport_disruption")

    if vol > 30.0:
        factors.append("high_volatility")

    if pct_deviation_usd > 20.0 and not factors:
        factors.append("unknown")

    return factors
