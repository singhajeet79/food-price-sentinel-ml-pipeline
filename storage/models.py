"""
storage/models.py
-----------------
SQLAlchemy ORM models matching the PostgreSQL schema defined in CLAUDE.md.

Tables:
    price_events          — raw price observations from Kafka
    feature_vectors       — computed rolling-window features
    anomaly_log           — anomaly detection results
    geopolitical_events   — named geopolitical scenarios
    anomaly_geo_tags      — many-to-many: anomaly ↔ geopolitical event
    consumer_lag_log      — Kafka consumer group lag snapshots

All timestamps are stored as TIMESTAMPTZ (timezone-aware).
All numeric fields use Numeric(precision, scale) for exact arithmetic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Float,
    ForeignKey, Integer, Numeric, SmallInteger,
    String, Text, UniqueConstraint, Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from storage.db import Base


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# price_events
# ---------------------------------------------------------------------------

class PriceEvent(Base):
    """
    Raw price observation ingested from a Kafka topic.
    One row per message that passes the data quality gates.
    """
    __tablename__ = "price_events"

    id                     = Column(BigInteger, primary_key=True, autoincrement=True)
    commodity              = Column(Text, nullable=False, index=True)
    price_usd              = Column(Numeric(12, 4), nullable=False)
    price_local            = Column(Numeric(12, 4), nullable=True)
    local_currency         = Column(String(3), nullable=True)
    fx_rate_to_usd         = Column(Numeric(10, 6), nullable=True)
    unit_raw               = Column(Text, nullable=True)
    unit_normalized        = Column(Text, default="per_tonne")
    unit_conversion_factor = Column(Numeric(10, 6), default=1.0)
    source                 = Column(Text, nullable=True)
    source_priority        = Column(SmallInteger, default=99)
    region                 = Column(Text, nullable=True, index=True)
    season                 = Column(Text, nullable=True)
    day_of_year            = Column(SmallInteger, nullable=True)
    day_of_year_sin        = Column(Float, nullable=True)
    day_of_year_cos        = Column(Float, nullable=True)
    data_as_of             = Column(DateTime(timezone=True), nullable=True, index=True)
    ingested_at            = Column(DateTime(timezone=True), default=_utcnow, index=True)
    latency_minutes        = Column(Integer, nullable=True)
    is_stale               = Column(Boolean, default=False, index=True)

    # Relationship
    feature_vectors = relationship("FeatureVector", back_populates="price_event")

    __table_args__ = (
        Index("idx_price_events_commodity_time", "commodity", "data_as_of"),
        Index("idx_price_events_stale", "is_stale", postgresql_where=Column("is_stale")),
    )

    @classmethod
    def from_kafka_event(cls, event: dict) -> "PriceEvent":
        """Construct a PriceEvent from a deserialised Kafka message dict."""
        return cls(
            commodity=event.get("commodity"),
            price_usd=event.get("price_usd"),
            price_local=event.get("price_local"),
            local_currency=event.get("local_currency"),
            fx_rate_to_usd=event.get("fx_rate_to_usd"),
            unit_raw=event.get("unit_raw"),
            unit_normalized=event.get("unit_normalized", "per_tonne"),
            unit_conversion_factor=event.get("unit_conversion_factor", 1.0),
            source=event.get("source"),
            source_priority=event.get("source_priority", 99),
            region=event.get("region", "global"),
            season=event.get("season"),
            day_of_year=event.get("day_of_year"),
            day_of_year_sin=event.get("day_of_year_sin"),
            day_of_year_cos=event.get("day_of_year_cos"),
            data_as_of=event.get("data_as_of"),
            latency_minutes=event.get("latency_minutes"),
            is_stale=event.get("is_stale", False),
        )

    def __repr__(self) -> str:
        return f"<PriceEvent id={self.id} {self.commodity}@{self.price_usd} {self.data_as_of}>"


# ---------------------------------------------------------------------------
# feature_vectors
# ---------------------------------------------------------------------------

class FeatureVector(Base):
    """
    Computed rolling-window feature vector for one price event.
    Input to the anomaly detection model.
    """
    __tablename__ = "feature_vectors"

    id                      = Column(BigInteger, primary_key=True, autoincrement=True)
    price_event_id          = Column(BigInteger, ForeignKey("price_events.id"), nullable=True)
    commodity               = Column(Text, nullable=False, index=True)
    region                  = Column(Text, nullable=True)
    rolling_7d_avg          = Column(Numeric(12, 4), nullable=True)
    rolling_30d_std         = Column(Numeric(12, 4), nullable=True)
    momentum                = Column(Numeric(8, 4), nullable=True)
    energy_lag_corr         = Column(Numeric(6, 4), nullable=True)
    fertilizer_index_delta  = Column(Numeric(8, 4), nullable=True)
    seasonal_index          = Column(Numeric(6, 4), nullable=True)
    seasonal_adjusted_price = Column(Numeric(12, 4), nullable=True)
    is_seasonal_adjusted    = Column(Boolean, default=False)
    is_low_confidence       = Column(Boolean, default=False)
    raw_price_usd           = Column(Numeric(12, 4), nullable=True)
    day_of_year_sin         = Column(Float, nullable=True)
    day_of_year_cos         = Column(Float, nullable=True)
    data_as_of              = Column(DateTime(timezone=True), nullable=True)
    computed_at             = Column(DateTime(timezone=True), default=_utcnow, index=True)

    # Relationships
    price_event  = relationship("PriceEvent", back_populates="feature_vectors")
    anomaly_logs = relationship("AnomalyLog", back_populates="feature_vector")

    __table_args__ = (
        Index("idx_feature_vectors_commodity_time", "commodity", "computed_at"),
    )

    @classmethod
    def from_feature_vector(cls, fv, price_event_id: Optional[int] = None) -> "FeatureVector":
        """Construct from a processing.features.FeatureVector instance."""
        return cls(
            price_event_id=price_event_id,
            commodity=fv.commodity,
            region=fv.region,
            rolling_7d_avg=fv.rolling_7d_avg,
            rolling_30d_std=fv.rolling_30d_std,
            momentum=fv.momentum,
            energy_lag_corr=fv.energy_lag_corr,
            fertilizer_index_delta=fv.fertilizer_index_delta,
            seasonal_index=fv.seasonal_index,
            seasonal_adjusted_price=fv.seasonal_adjusted_price,
            is_seasonal_adjusted=fv.is_seasonal_adjusted,
            is_low_confidence=fv.is_low_confidence,
            raw_price_usd=fv.raw_price_usd,
            day_of_year_sin=fv.day_of_year_sin,
            day_of_year_cos=fv.day_of_year_cos,
            data_as_of=fv.data_as_of,
        )

    def __repr__(self) -> str:
        return f"<FeatureVector id={self.id} {self.commodity} score_adj={self.seasonal_adjusted_price}>"


# ---------------------------------------------------------------------------
# anomaly_log
# ---------------------------------------------------------------------------

class AnomalyLog(Base):
    """
    Anomaly detection result for one feature vector.
    Written by the detection layer for every scored event.
    """
    __tablename__ = "anomaly_log"

    id            = Column(BigInteger, primary_key=True, autoincrement=True)
    feature_id    = Column(BigInteger, ForeignKey("feature_vectors.id"), nullable=True)
    commodity     = Column(Text, nullable=False, index=True)
    region        = Column(Text, nullable=True)
    anomaly_score = Column(Numeric(6, 4), nullable=True)
    severity      = Column(Text, nullable=True)
    model_version = Column(Text, nullable=True)
    is_anomaly    = Column(Boolean, default=False)
    alerted       = Column(Boolean, default=False)
    suppressed    = Column(Boolean, default=False)
    alert_payload = Column(JSONB, nullable=True)    # full alert dict when alerted=True
    detected_at   = Column(DateTime(timezone=True), default=_utcnow, index=True)

    # Relationships
    feature_vector    = relationship("FeatureVector", back_populates="anomaly_logs")
    geopolitical_tags = relationship(
        "GeopoliticalEvent",
        secondary="anomaly_geo_tags",
        back_populates="anomaly_logs",
    )

    __table_args__ = (
        Index("idx_anomaly_log_detected", "detected_at"),
        Index("idx_anomaly_log_commodity", "commodity", "detected_at"),
    )

    @classmethod
    def from_scored_result(
        cls,
        scored_result,
        feature_vector_id: Optional[int] = None,
        alerted: bool = False,
        suppressed: bool = False,
        alert_payload: Optional[dict] = None,
    ) -> "AnomalyLog":
        """Construct from a detection.score.ScoredResult instance."""
        return cls(
            feature_id=feature_vector_id,
            commodity=scored_result.commodity,
            region=scored_result.region,
            anomaly_score=scored_result.normalised_score,
            severity=scored_result.severity,
            model_version=scored_result.model_version,
            is_anomaly=scored_result.is_anomaly,
            alerted=alerted,
            suppressed=suppressed,
            alert_payload=alert_payload,
        )

    def __repr__(self) -> str:
        return (
            f"<AnomalyLog id={self.id} {self.commodity} "
            f"score={self.anomaly_score} severity={self.severity}>"
        )


# ---------------------------------------------------------------------------
# geopolitical_events
# ---------------------------------------------------------------------------

class GeopoliticalEvent(Base):
    """
    Named geopolitical scenario that may influence price anomalies.
    Tags are applied post-hoc to AnomalyLog entries for v2 training data.
    """
    __tablename__ = "geopolitical_events"

    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    tag         = Column(Text, unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    started_at  = Column(DateTime(timezone=True), nullable=True)
    ended_at    = Column(DateTime(timezone=True), nullable=True)
    created_at  = Column(DateTime(timezone=True), default=_utcnow)

    anomaly_logs = relationship(
        "AnomalyLog",
        secondary="anomaly_geo_tags",
        back_populates="geopolitical_tags",
    )

    def __repr__(self) -> str:
        return f"<GeopoliticalEvent tag={self.tag!r}>"


# ---------------------------------------------------------------------------
# anomaly_geo_tags  (association table)
# ---------------------------------------------------------------------------

from sqlalchemy import Table

anomaly_geo_tags = Table(
    "anomaly_geo_tags",
    Base.metadata,
    Column("anomaly_id", BigInteger, ForeignKey("anomaly_log.id"), primary_key=True),
    Column("event_id",   BigInteger, ForeignKey("geopolitical_events.id"), primary_key=True),
)


# ---------------------------------------------------------------------------
# consumer_lag_log
# ---------------------------------------------------------------------------

class ConsumerLagLog(Base):
    """
    Kafka consumer group lag snapshot.
    Written every LAG_LOG_INTERVAL_S seconds by the consumer.
    High lag = pipeline is falling behind = alerts are delayed.
    """
    __tablename__ = "consumer_lag_log"

    id             = Column(BigInteger, primary_key=True, autoincrement=True)
    consumer_group = Column(Text, nullable=False)
    topic          = Column(Text, nullable=False)
    partition      = Column(SmallInteger, nullable=False)
    lag_messages   = Column(BigInteger, nullable=True)
    recorded_at    = Column(DateTime(timezone=True), default=_utcnow, index=True)

    __table_args__ = (
        Index("idx_consumer_lag_time", "recorded_at"),
        Index("idx_consumer_lag_topic", "topic", "partition"),
    )

    def __repr__(self) -> str:
        return (
            f"<ConsumerLagLog topic={self.topic} partition={self.partition} "
            f"lag={self.lag_messages}>"
        )
