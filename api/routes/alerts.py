"""
api/routes/alerts.py
--------------------
Alert endpoints — reads live alerts from Valkey.

Endpoints:
    GET /alerts/active              — all active alerts (all commodities)
    GET /alerts/{commodity}         — active alerts for one commodity
    GET /alerts/{commodity}/{region}— active alerts for commodity + region

Valkey key pattern: alert:{commodity}:{region}:{severity}
TTL is set by alert_writer.py (default: ALERT_COOLDOWN_SECONDS = 6h).

Alerts are served directly from Valkey — sub-millisecond reads,
no PostgreSQL query needed for the hot path.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.dependencies import get_valkey_client

router = APIRouter()
log = logging.getLogger(__name__)

VALKEY_KEY_PREFIX = "alert"


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class AlertResponse(BaseModel):
    alert_id: str
    commodity: str
    region: str
    severity: str
    anomaly_score: float
    current_price_usd: float
    baseline_price_usd: float
    pct_deviation_usd: float
    contributing_factors: list[str]
    geopolitical_tags: list[str]
    is_seasonal_adjusted: bool
    model_version: str
    dedup_key: str
    triggered_at: str
    ttl_seconds: int
    current_price_local: Optional[float] = None
    local_currency: Optional[str] = None
    pct_deviation_local: Optional[float] = None
    cooldown_until: Optional[str] = None


class AlertsListResponse(BaseModel):
    count: int
    alerts: list[AlertResponse]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_alerts_by_pattern(pattern: str) -> list[dict]:
    """
    Scan Valkey for keys matching pattern and return parsed alert payloads.
    Uses SCAN (non-blocking) instead of KEYS to avoid blocking Redis/Valkey.
    """
    client = get_valkey_client()
    alerts = []

    try:
        cursor = 0
        while True:
            cursor, keys = client.scan(cursor=cursor, match=pattern, count=100)
            for key in keys:
                raw = client.get(key)
                if raw:
                    try:
                        alerts.append(json.loads(raw))
                    except json.JSONDecodeError:
                        log.warning("Failed to parse alert JSON for key: %s", key)
            if cursor == 0:
                break
    except Exception as exc:
        log.error("Valkey scan failed for pattern '%s': %s", pattern, exc)
        raise HTTPException(status_code=503, detail="Alert cache unavailable")

    # Sort by anomaly_score descending — highest severity first
    alerts.sort(key=lambda a: a.get("anomaly_score", 0), reverse=True)
    return alerts


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/active", response_model=AlertsListResponse)
def get_active_alerts(
    severity: Optional[str] = Query(None, description="Filter by severity: LOW, MEDIUM, HIGH, CRITICAL"),
    limit: int = Query(50, ge=1, le=200, description="Max alerts to return"),
):
    """
    Fetch all currently active alerts from Valkey.

    Alerts expire automatically when their TTL elapses (default 6h).
    Use ?severity=HIGH to filter by minimum severity.
    """
    pattern = f"{VALKEY_KEY_PREFIX}:*"
    alerts = _fetch_alerts_by_pattern(pattern)

    if severity:
        severity_order = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
        min_rank = severity_order.get(severity.upper(), 0)
        alerts = [
            a for a in alerts
            if severity_order.get(a.get("severity", ""), 0) >= min_rank
        ]

    alerts = alerts[:limit]
    return AlertsListResponse(count=len(alerts), alerts=alerts)


@router.get("/{commodity}", response_model=AlertsListResponse)
def get_alerts_for_commodity(
    commodity: str,
    region: Optional[str] = Query(None, description="Filter by region"),
):
    """
    Fetch active alerts for a specific commodity.

    commodity examples: wheat, rice, maize, palm_oil
    """
    commodity = commodity.lower().strip()

    if region:
        pattern = f"{VALKEY_KEY_PREFIX}:{commodity}:{region.lower()}:*"
    else:
        pattern = f"{VALKEY_KEY_PREFIX}:{commodity}:*"

    alerts = _fetch_alerts_by_pattern(pattern)

    if not alerts:
        return AlertsListResponse(count=0, alerts=[])

    return AlertsListResponse(count=len(alerts), alerts=alerts)


@router.get("/{commodity}/{region}", response_model=AlertsListResponse)
def get_alerts_for_commodity_region(commodity: str, region: str):
    """
    Fetch active alerts for a specific commodity and region combination.
    """
    commodity = commodity.lower().strip()
    region = region.lower().strip()
    pattern = f"{VALKEY_KEY_PREFIX}:{commodity}:{region}:*"
    alerts = _fetch_alerts_by_pattern(pattern)
    return AlertsListResponse(count=len(alerts), alerts=alerts)
