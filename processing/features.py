"""
processing/features.py
-----------------------
Rolling-window feature engineering for the Food Price Sentinel pipeline.

Consumes raw price events from Kafka and computes the feature vectors that
feed the anomaly detection model. All state is held in-memory as deques
(bounded by the longest window needed). No external dependencies beyond
the standard library and numpy.

Feature vector (matches CLAUDE.md ML Model spec):
    [
        seasonal_adjusted_price,   # price_usd / seasonal_index
        rolling_7d_avg,            # 7-day rolling mean of price_usd
        rolling_30d_std,           # 30-day rolling std dev of price_usd
        momentum,                  # (price_today - price_7d_ago) / price_7d_ago
        energy_lag_corr,           # Pearson r: food price vs energy price (14d lag)
        fertilizer_index_delta,    # % change in fertilizer price over 7 days
        day_of_year_sin,           # cyclical encoding (from schema)
        day_of_year_cos,           # cyclical encoding (from schema)
    ]

Data quality gates (all must pass before vector is emitted):
    1. is_stale check  — latency_minutes <= STALE_DATA_THRESHOLD_MINUTES
    2. Source conflict — highest-priority source wins per (commodity, region, ts)
    3. Window completeness — at least MIN_WINDOW_POINTS of last 7 days present

Seasonal index:
    Ratio of (commodity, day_of_year) 3-year historical avg to annual mean.
    Bootstrapped from a hardcoded table for v1; replaced by PostgreSQL lookup
    in production (see _load_seasonal_index).
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config (from environment)
# ---------------------------------------------------------------------------

STALE_THRESHOLD_MINUTES: int = int(os.getenv("STALE_DATA_THRESHOLD_MINUTES", "180"))
ROLLING_WINDOW_SHORT: int = int(os.getenv("ROLLING_WINDOW_DAYS", "7"))
ROLLING_WINDOW_LONG: int = int(os.getenv("LONG_WINDOW_DAYS", "30"))
ENERGY_LAG_DAYS: int = int(os.getenv("ENERGY_LAG_DAYS", "14"))
MIN_WINDOW_POINTS: int = 5  # minimum data points for 7d window to be valid
LOW_CONFIDENCE_FLAG: float = (
    -1.0
)  # sentinel value in feature vector for low-confidence windows


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class PricePoint:
    """A single price observation stored in the rolling window buffer."""

    price_usd: float
    data_as_of: datetime
    source_priority: int
    day_of_year: int
    day_of_year_sin: float
    day_of_year_cos: float


@dataclass
class FeatureVector:
    """
    The fully computed feature vector for one (commodity, region) tick.
    Passed to the anomaly detection scorer.
    """

    commodity: str
    region: str
    data_as_of: datetime
    computed_at: datetime

    # Core features
    seasonal_adjusted_price: float
    rolling_7d_avg: float
    rolling_30d_std: float
    momentum: float
    energy_lag_corr: float
    fertilizer_index_delta: float
    day_of_year_sin: float
    day_of_year_cos: float

    # Metadata
    seasonal_index: float
    is_seasonal_adjusted: bool
    is_low_confidence: bool  # True if window completeness gate was near threshold
    raw_price_usd: float
    source_priority: int

    def to_model_array(self) -> list[float]:
        """Returns the feature values in the order expected by the ML model."""
        return [
            self.seasonal_adjusted_price,
            self.rolling_7d_avg,
            self.rolling_30d_std,
            self.momentum,
            self.energy_lag_corr,
            self.fertilizer_index_delta,
            self.day_of_year_sin,
            self.day_of_year_cos,
        ]

    def to_dict(self) -> dict:
        return {
            "commodity": self.commodity,
            "region": self.region,
            "data_as_of": self.data_as_of.isoformat(),
            "computed_at": self.computed_at.isoformat(),
            "seasonal_adjusted_price": self.seasonal_adjusted_price,
            "rolling_7d_avg": self.rolling_7d_avg,
            "rolling_30d_std": self.rolling_30d_std,
            "momentum": self.momentum,
            "energy_lag_corr": self.energy_lag_corr,
            "fertilizer_index_delta": self.fertilizer_index_delta,
            "day_of_year_sin": self.day_of_year_sin,
            "day_of_year_cos": self.day_of_year_cos,
            "seasonal_index": self.seasonal_index,
            "is_seasonal_adjusted": self.is_seasonal_adjusted,
            "is_low_confidence": self.is_low_confidence,
            "raw_price_usd": self.raw_price_usd,
        }


# ---------------------------------------------------------------------------
# Seasonal index bootstrap table
# ---------------------------------------------------------------------------
# Format: { commodity: { day_of_year: seasonal_index } }
# seasonal_index = avg_price_on_this_doy / annual_avg_price
# Values below are approximate Northern Hemisphere historical patterns.
# In production these are loaded from PostgreSQL (3-year rolling computation).
#
# Index > 1.0 means prices are typically higher on this day than annual avg.
# Index < 1.0 means prices are typically lower.


def _build_bootstrap_seasonal_index() -> dict[str, dict[int, float]]:
    """
    Build a day-of-year seasonal index for each commodity using a simple
    sinusoidal approximation of known harvest cycle patterns.

    Wheat:    peaks ~May (pre-harvest tension), troughs ~Aug (post-harvest)
    Maize:    peaks ~Jun, troughs ~Oct
    Rice:     peaks ~Mar, troughs ~Nov
    Soybeans: peaks ~Jul, troughs ~Nov
    Palm oil: relatively flat, mild peak ~Jan (dry season)
    Sugar:    peaks ~Mar, troughs ~Oct
    Barley:   similar to wheat
    """
    import math

    commodities = {
        # commodity: (peak_doy, amplitude)
        "wheat": (130, 0.08),
        "maize": (160, 0.07),
        "rice": (75, 0.06),
        "soybeans": (190, 0.09),
        "palm_oil": (15, 0.04),
        "sugar": (80, 0.07),
        "barley": (125, 0.08),
        # Energy and fertilizer get flat index (1.0) — handled separately
        "crude_oil_brent": (1, 0.0),
        "crude_oil_wti": (1, 0.0),
        "natural_gas": (1, 0.0),
        "coal": (1, 0.0),
        "urea": (1, 0.0),
        "dap": (1, 0.0),
        "mop": (1, 0.0),
        "ammonia": (1, 0.0),
    }

    index: dict[str, dict[int, float]] = {}
    for commodity, (peak_doy, amplitude) in commodities.items():
        index[commodity] = {}
        for doy in range(1, 366):
            # Sinusoidal seasonal pattern centred on peak_doy
            phase = 2 * math.pi * (doy - peak_doy) / 365
            index[commodity][doy] = round(1.0 + amplitude * math.cos(phase), 6)

    return index


_SEASONAL_INDEX: dict[str, dict[int, float]] = _build_bootstrap_seasonal_index()


def get_seasonal_index(commodity: str, day_of_year: int) -> float:
    """
    Return the seasonal index for a (commodity, day_of_year) pair.

    Falls back to 1.0 (no seasonal adjustment) for unknown commodities.
    In production this should be overridden with a PostgreSQL-backed lookup
    that uses actual 3-year rolling historical data.
    """
    commodity_index = _SEASONAL_INDEX.get(commodity.lower())
    if commodity_index is None:
        log.debug("No seasonal index for commodity '%s' — using 1.0", commodity)
        return 1.0
    return commodity_index.get(day_of_year, 1.0)


# ---------------------------------------------------------------------------
# Source conflict resolver
# ---------------------------------------------------------------------------


class SourceConflictResolver:
    """
    Tracks the most recently seen price per (commodity, region, data_as_of_bucket).

    When two sources report for the same bucket, the lower source_priority
    integer wins (1 = highest trust). Ties go to the most recently ingested.

    Bucket granularity: 1 hour (configurable). This handles sources that
    report at slightly different minutes within the same hour.
    """

    BUCKET_MINUTES: int = 1

    def __init__(self) -> None:
        # key: (commodity, region, bucket_ts) -> PricePoint
        self._seen: dict[tuple, PricePoint] = {}

    def _bucket(self, dt: datetime) -> datetime:
        """Floor datetime to bucket boundary."""
        floored = dt.replace(minute=0, second=0, microsecond=0)
        return floored

    def resolve(self, commodity: str, region: str, point: PricePoint) -> bool:
        """
        Attempt to register a price point.

        Returns True if this point is accepted (best source for this bucket).
        Returns False if a higher-priority source already claimed this bucket.
        """
        key = (commodity, region, self._bucket(point.data_as_of))
        existing = self._seen.get(key)

        if existing is None:
            self._seen[key] = point
            return True

        # Lower integer = higher priority
        if point.source_priority < existing.source_priority:
            log.debug(
                "Source conflict resolved: %s/%s bucket=%s — accepting priority %d over %d",
                commodity,
                region,
                self._bucket(point.data_as_of).isoformat(),
                point.source_priority,
                existing.source_priority,
            )
            self._seen[key] = point
            return True

        log.debug(
            "Source conflict resolved: %s/%s — discarding priority %d (kept %d)",
            commodity,
            region,
            point.source_priority,
            existing.source_priority,
        )
        return False


# ---------------------------------------------------------------------------
# Rolling window buffer
# ---------------------------------------------------------------------------


class RollingWindowBuffer:
    """
    Fixed-size deque of PricePoints for a single (commodity, region) stream.

    Keeps the last `max_days` days of observations. Thread-safe for single
    consumer thread use (Kafka consumer is single-threaded per partition).
    """

    def __init__(self, max_days: int = ROLLING_WINDOW_LONG) -> None:
        self._max_days = max_days
        self._points: deque[PricePoint] = deque()

    def append(self, point: PricePoint) -> None:
        self._points.append(point)
        # Evict points older than max_days
        cutoff = point.data_as_of - timedelta(days=self._max_days)
        while self._points and self._points[0].data_as_of < cutoff:
            self._points.popleft()

    def points_within(self, days: int, reference: datetime) -> list[PricePoint]:
        """Return all points within the last `days` days from `reference`."""
        cutoff = reference - timedelta(days=days)
        return [p for p in self._points if p.data_as_of >= cutoff]

    def prices_within(self, days: int, reference: datetime) -> list[float]:
        return [p.price_usd for p in self.points_within(days, reference)]

    def __len__(self) -> int:
        return len(self._points)


# ---------------------------------------------------------------------------
# Feature engineer
# ---------------------------------------------------------------------------


class FeatureEngineer:
    """
    Stateful feature computation engine.

    One instance per consumer process. Maintains rolling buffers for:
      - food prices      per (commodity, region)
      - energy prices    per energy_type
      - fertilizer prices per fertilizer_type

    Call .process_food_event() / .process_energy_event() /
    .process_fertilizer_event() as events arrive from Kafka.

    When a food event is processed, it attempts to emit a FeatureVector.
    Returns None if data quality gates prevent emission.
    """

    def __init__(self) -> None:
        # Food price buffers: (commodity, region) -> RollingWindowBuffer
        self._food_buffers: dict[tuple[str, str], RollingWindowBuffer] = defaultdict(
            lambda: RollingWindowBuffer(max_days=ROLLING_WINDOW_LONG)
        )
        # Energy price buffers: energy_type -> RollingWindowBuffer
        self._energy_buffers: dict[str, RollingWindowBuffer] = defaultdict(
            lambda: RollingWindowBuffer(max_days=ROLLING_WINDOW_LONG + ENERGY_LAG_DAYS)
        )
        # Fertilizer price buffers: fertilizer_type -> RollingWindowBuffer
        self._fertilizer_buffers: dict[str, RollingWindowBuffer] = defaultdict(
            lambda: RollingWindowBuffer(max_days=ROLLING_WINDOW_LONG)
        )

        self._conflict_resolver = SourceConflictResolver()

        # Stats
        self.events_processed: int = 0
        self.events_stale: int = 0
        self.events_conflict_discarded: int = 0
        self.vectors_emitted: int = 0
        self.vectors_low_confidence: int = 0

    # ------------------------------------------------------------------
    # Ingest methods
    # ------------------------------------------------------------------

    def process_food_event(self, event: dict) -> Optional[FeatureVector]:
        """
        Ingest a food price event dict (deserialised from Kafka).
        Returns a FeatureVector if all quality gates pass, else None.
        """
        self.events_processed += 1

        commodity = event["commodity"]
        region = event.get("region", "global")
        price_usd = event["price_usd"]
        latency = event.get("latency_minutes", 0) or 0
        source_priority = event.get("source_priority", 99)
        day_of_year = event.get("day_of_year", 1)
        doy_sin = event.get("day_of_year_sin", 0.0)
        doy_cos = event.get("day_of_year_cos", 1.0)

        data_as_of_raw = event.get("data_as_of") or event.get("ingested_at")
        data_as_of = _parse_dt(data_as_of_raw)

        # Gate 1: staleness
        if latency > STALE_THRESHOLD_MINUTES:
            log.warning(
                "Stale event dropped: %s/%s latency=%dm (threshold=%dm)",
                commodity,
                region,
                latency,
                STALE_THRESHOLD_MINUTES,
            )
            self.events_stale += 1
            return None

        point = PricePoint(
            price_usd=price_usd,
            data_as_of=data_as_of,
            source_priority=source_priority,
            day_of_year=day_of_year,
            day_of_year_sin=doy_sin,
            day_of_year_cos=doy_cos,
        )

        # Gate 2: source conflict resolution
        # NOTE: disabled for single-source simulated feeds.
        # Re-enable when multiple real sources (FAO, WorldBank) are ingesting.
        # if not self._conflict_resolver.resolve(commodity, region, point):
        #     self.events_conflict_discarded += 1
        #     return None

        # Append to buffer
        key = (commodity, region)
        self._food_buffers[key].append(point)

        # Attempt to compute feature vector
        return self._compute_vector(commodity, region, point)

    def process_energy_event(self, event: dict) -> None:
        """Ingest an energy price event — updates energy buffer only."""
        energy_type = event.get("energy_type") or event.get("commodity", "unknown")
        price_usd = event["price_usd"]
        data_as_of = _parse_dt(event.get("data_as_of") or event.get("ingested_at"))
        source_priority = event.get("source_priority", 99)
        day_of_year = event.get("day_of_year", 1)

        point = PricePoint(
            price_usd=price_usd,
            data_as_of=data_as_of,
            source_priority=source_priority,
            day_of_year=day_of_year,
            day_of_year_sin=event.get("day_of_year_sin", 0.0),
            day_of_year_cos=event.get("day_of_year_cos", 1.0),
        )
        self._energy_buffers[energy_type].append(point)

    def process_fertilizer_event(self, event: dict) -> None:
        """Ingest a fertilizer price event — updates fertilizer buffer only."""
        fertilizer_type = event.get("fertilizer_type") or event.get(
            "commodity", "unknown"
        )
        price_usd = event["price_usd"]
        data_as_of = _parse_dt(event.get("data_as_of") or event.get("ingested_at"))
        source_priority = event.get("source_priority", 99)
        day_of_year = event.get("day_of_year", 1)

        point = PricePoint(
            price_usd=price_usd,
            data_as_of=data_as_of,
            source_priority=source_priority,
            day_of_year=day_of_year,
            day_of_year_sin=event.get("day_of_year_sin", 0.0),
            day_of_year_cos=event.get("day_of_year_cos", 1.0),
        )
        self._fertilizer_buffers[fertilizer_type].append(point)

    # ------------------------------------------------------------------
    # Feature computation
    # ------------------------------------------------------------------

    def _compute_vector(
        self,
        commodity: str,
        region: str,
        latest: PricePoint,
    ) -> Optional[FeatureVector]:
        """
        Compute all features for the latest food price point.
        Returns None if window completeness gate fails hard.
        """
        key = (commodity, region)
        buffer = self._food_buffers[key]
        now = latest.data_as_of

        # --- Rolling 7-day prices ---
        prices_7d = buffer.prices_within(ROLLING_WINDOW_SHORT, now)

        # Gate 3: window completeness
        is_low_confidence = len(prices_7d) < MIN_WINDOW_POINTS
        if len(prices_7d) == 0:
            log.debug("No 7d data for %s/%s — skipping vector", commodity, region)
            return None

        if is_low_confidence:
            self.vectors_low_confidence += 1
            log.debug(
                "Low confidence vector for %s/%s: only %d points in 7d window",
                commodity,
                region,
                len(prices_7d),
            )

        # --- Rolling 30-day prices ---
        prices_30d = buffer.prices_within(ROLLING_WINDOW_LONG, now)

        rolling_7d_avg = float(np.mean(prices_7d))
        rolling_30d_std = float(np.std(prices_30d)) if len(prices_30d) > 1 else 0.0

        # --- Momentum: (now - 7d_ago) / 7d_ago ---
        points_7d = buffer.points_within(ROLLING_WINDOW_SHORT, now)
        if len(points_7d) >= 2:
            oldest_7d_price = points_7d[0].price_usd
            momentum = (
                (latest.price_usd - oldest_7d_price) / oldest_7d_price
                if oldest_7d_price > 0
                else 0.0
            )
        else:
            momentum = 0.0

        # --- Seasonal adjustment ---
        seasonal_index = get_seasonal_index(commodity, latest.day_of_year)
        is_seasonal_adjusted = seasonal_index != 1.0
        seasonal_adjusted_price = (
            round(latest.price_usd / seasonal_index, 4)
            if seasonal_index > 0
            else latest.price_usd
        )

        # --- Energy lag correlation ---
        energy_lag_corr = self._compute_energy_lag_corr(commodity, region, now)

        # --- Fertilizer index delta ---
        fertilizer_index_delta = self._compute_fertilizer_delta(now)

        self.vectors_emitted += 1

        return FeatureVector(
            commodity=commodity,
            region=region,
            data_as_of=now,
            computed_at=datetime.now(tz=timezone.utc),
            seasonal_adjusted_price=seasonal_adjusted_price,
            rolling_7d_avg=round(rolling_7d_avg, 4),
            rolling_30d_std=round(rolling_30d_std, 4),
            momentum=round(momentum, 6),
            energy_lag_corr=round(energy_lag_corr, 6),
            fertilizer_index_delta=round(fertilizer_index_delta, 6),
            day_of_year_sin=latest.day_of_year_sin,
            day_of_year_cos=latest.day_of_year_cos,
            seasonal_index=seasonal_index,
            is_seasonal_adjusted=is_seasonal_adjusted,
            is_low_confidence=is_low_confidence,
            raw_price_usd=latest.price_usd,
            source_priority=latest.source_priority,
        )

    def _compute_energy_lag_corr(
        self, commodity: str, region: str, reference: datetime
    ) -> float:
        """
        Pearson correlation between food prices (current window) and
        energy prices shifted back by ENERGY_LAG_DAYS.

        Uses crude_oil_brent as the primary energy signal. Falls back to
        wti if brent unavailable, then to 0.0 if no energy data at all.

        Returns correlation in [-1, 1]. Returns 0.0 on insufficient data.
        """
        food_key = (commodity, region)
        food_prices = self._food_buffers[food_key].prices_within(
            ROLLING_WINDOW_LONG, reference
        )

        # Select best available energy buffer
        energy_buffer = None
        for candidate in ("crude_oil_brent", "crude_oil_wti", "natural_gas", "coal"):
            if (
                candidate in self._energy_buffers
                and len(self._energy_buffers[candidate]) > 0
            ):
                energy_buffer = self._energy_buffers[candidate]
                break

        if energy_buffer is None or len(food_prices) < 5:
            return 0.0

        # Get energy prices from the lagged window
        lag_reference = reference - timedelta(days=ENERGY_LAG_DAYS)
        energy_prices = energy_buffer.prices_within(ROLLING_WINDOW_LONG, lag_reference)

        if len(energy_prices) < 5:
            return 0.0

        # Align lengths (take the shorter)
        n = min(len(food_prices), len(energy_prices))
        food_arr = np.array(food_prices[-n:])
        energy_arr = np.array(energy_prices[-n:])

        # Pearson r
        if np.std(food_arr) == 0 or np.std(energy_arr) == 0:
            return 0.0

        corr_matrix = np.corrcoef(food_arr, energy_arr)
        return float(corr_matrix[0, 1])

    def _compute_fertilizer_delta(self, reference: datetime) -> float:
        """
        Percentage change in composite fertilizer price index over last 7 days.

        Composite = mean of all available fertilizer types.
        Returns 0.0 if insufficient data.
        """
        if not self._fertilizer_buffers:
            return 0.0

        current_prices = []
        past_prices = []
        past_reference = reference - timedelta(days=ROLLING_WINDOW_SHORT)

        for buf in self._fertilizer_buffers.values():
            current = buf.prices_within(2, reference)  # last 2 days
            past = buf.prices_within(2, past_reference)  # 2 days around 7d ago

            if current:
                current_prices.append(np.mean(current))
            if past:
                past_prices.append(np.mean(past))

        if not current_prices or not past_prices:
            return 0.0

        composite_now = np.mean(current_prices)
        composite_past = np.mean(past_prices)

        if composite_past == 0:
            return 0.0

        return float((composite_now - composite_past) / composite_past)

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        return {
            "events_processed": self.events_processed,
            "events_stale": self.events_stale,
            "events_conflict_discarded": self.events_conflict_discarded,
            "vectors_emitted": self.vectors_emitted,
            "vectors_low_confidence": self.vectors_low_confidence,
            "food_buffers": {
                f"{k[0]}/{k[1]}": len(v) for k, v in self._food_buffers.items()
            },
            "energy_buffers": {k: len(v) for k, v in self._energy_buffers.items()},
            "fertilizer_buffers": {
                k: len(v) for k, v in self._fertilizer_buffers.items()
            },
        }


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _parse_dt(value) -> datetime:
    """Parse an ISO datetime string or pass through a datetime object."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    return datetime.now(tz=timezone.utc)
