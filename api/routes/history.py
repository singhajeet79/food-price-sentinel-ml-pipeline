"""
api/routes/history.py
---------------------
History endpoints — reads anomaly log from PostgreSQL.

Endpoints:
    GET /history                    — paginated anomaly log (all commodities)
    GET /history/{commodity}        — anomaly trend for one commodity
    GET /history/stats              — aggregate stats (counts by severity)

These endpoints serve the dashboard's historical trend charts and
are used by the retraining pipeline to understand model performance.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import desc, func
from sqlalchemy.orm import Session

from api.dependencies import get_db_session
from storage.models import AnomalyLog

router = APIRouter()
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class AnomalyRecord(BaseModel):
    id: int
    commodity: str
    region: Optional[str]
    anomaly_score: Optional[float]
    severity: Optional[str]
    model_version: Optional[str]
    is_anomaly: bool
    alerted: bool
    suppressed: bool
    detected_at: str

    class Config:
        from_attributes = True


class HistoryResponse(BaseModel):
    count: int
    total: int
    page: int
    page_size: int
    records: list[AnomalyRecord]


class SeverityStats(BaseModel):
    severity: str
    count: int
    avg_score: Optional[float]


class StatsResponse(BaseModel):
    period_days: int
    total_anomalies: int
    total_alerts_sent: int
    total_suppressed: int
    by_severity: list[SeverityStats]
    top_commodities: list[dict]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _orm_to_record(row: AnomalyLog) -> AnomalyRecord:
    return AnomalyRecord(
        id=row.id,
        commodity=row.commodity,
        region=row.region,
        anomaly_score=float(row.anomaly_score) if row.anomaly_score else None,
        severity=row.severity,
        model_version=row.model_version,
        is_anomaly=row.is_anomaly,
        alerted=row.alerted,
        suppressed=row.suppressed,
        detected_at=row.detected_at.isoformat() if row.detected_at else "",
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("", response_model=HistoryResponse)
def get_history(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    days: int = Query(7, ge=1, le=365, description="Lookback window in days"),
    anomalies_only: bool = Query(True, description="Only return anomalous events"),
    session: Session = Depends(get_db_session),
):
    """
    Paginated anomaly log for all commodities.

    Default: last 7 days, anomalies only, 50 per page.
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)

    query = session.query(AnomalyLog).filter(AnomalyLog.detected_at >= cutoff)

    if anomalies_only:
        query = query.filter(AnomalyLog.is_anomaly)

    total = query.count()
    records = (
        query
        .order_by(desc(AnomalyLog.detected_at))
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return HistoryResponse(
        count=len(records),
        total=total,
        page=page,
        page_size=page_size,
        records=[_orm_to_record(r) for r in records],
    )


@router.get("/stats", response_model=StatsResponse)
def get_stats(
    days: int = Query(30, ge=1, le=365),
    session: Session = Depends(get_db_session),
):
    """
    Aggregate anomaly statistics for the dashboard summary cards.
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)

    base_q = session.query(AnomalyLog).filter(
        AnomalyLog.detected_at >= cutoff,
        AnomalyLog.is_anomaly,
    )

    total_anomalies = base_q.count()
    total_alerts    = base_q.filter(AnomalyLog.alerted).count()
    total_suppressed = base_q.filter(AnomalyLog.suppressed).count()

    # By severity
    severity_rows = (
        session.query(
            AnomalyLog.severity,
            func.count(AnomalyLog.id).label("count"),
            func.avg(AnomalyLog.anomaly_score).label("avg_score"),
        )
        .filter(AnomalyLog.detected_at >= cutoff, AnomalyLog.is_anomaly)
        .group_by(AnomalyLog.severity)
        .all()
    )
    by_severity = [
        SeverityStats(
            severity=row.severity or "UNKNOWN",
            count=row.count,
            avg_score=round(float(row.avg_score), 4) if row.avg_score else None,
        )
        for row in severity_rows
    ]

    # Top commodities by anomaly count
    commodity_rows = (
        session.query(
            AnomalyLog.commodity,
            func.count(AnomalyLog.id).label("count"),
        )
        .filter(AnomalyLog.detected_at >= cutoff, AnomalyLog.is_anomaly)
        .group_by(AnomalyLog.commodity)
        .order_by(desc("count"))
        .limit(5)
        .all()
    )
    top_commodities = [
        {"commodity": row.commodity, "anomaly_count": row.count}
        for row in commodity_rows
    ]

    return StatsResponse(
        period_days=days,
        total_anomalies=total_anomalies,
        total_alerts_sent=total_alerts,
        total_suppressed=total_suppressed,
        by_severity=by_severity,
        top_commodities=top_commodities,
    )


@router.get("/{commodity}", response_model=HistoryResponse)
def get_commodity_history(
    commodity: str,
    days: int = Query(30, ge=1, le=365),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    session: Session = Depends(get_db_session),
):
    """
    Anomaly history for a specific commodity.
    Used by the dashboard price trend chart.
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)
    commodity = commodity.lower().strip()

    query = (
        session.query(AnomalyLog)
        .filter(
            AnomalyLog.commodity == commodity,
            AnomalyLog.detected_at >= cutoff,
        )
        .order_by(desc(AnomalyLog.detected_at))
    )

    total = query.count()
    records = query.offset((page - 1) * page_size).limit(page_size).all()

    return HistoryResponse(
        count=len(records),
        total=total,
        page=page,
        page_size=page_size,
        records=[_orm_to_record(r) for r in records],
    )
