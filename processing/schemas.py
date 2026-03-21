"""
processing/schemas.py
---------------------
Pydantic v2 data models for every Kafka topic payload in the Food Price Sentinel
pipeline. These are the single source of truth for serialisation/deserialisation
across producers, consumers, and the API layer.

Import convention:
    from processing.schemas import FoodPriceEvent, EnergyPriceEvent, ...
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class Season(str, Enum):
    SPRING = "spring"
    SUMMER = "summer"
    AUTUMN = "autumn"
    WINTER = "winter"


class Severity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class UnitNormalized(str, Enum):
    PER_TONNE = "per_tonne"


class FoodCommodity(str, Enum):
    WHEAT = "wheat"
    RICE = "rice"
    MAIZE = "maize"
    SOYBEANS = "soybeans"
    PALM_OIL = "palm_oil"
    SUGAR = "sugar"
    BARLEY = "barley"


class EnergyCommodity(str, Enum):
    CRUDE_OIL_BRENT = "crude_oil_brent"
    CRUDE_OIL_WTI = "crude_oil_wti"
    NATURAL_GAS = "natural_gas"
    COAL = "coal"


class FertilizerCommodity(str, Enum):
    UREA = "urea"
    DAP = "dap"
    MOP = "mop"
    AMMONIA = "ammonia"
    NATURAL_GAS_FEEDSTOCK = "natural_gas_feedstock"


class DataSource(str, Enum):
    FAO = "FAO"
    WORLD_BANK = "WORLDBANK"
    USDA = "USDA"
    IEA = "IEA"
    SYNTHETIC = "SYNTHETIC"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _season_from_doy(day_of_year: int) -> Season:
    """Derive meteorological season from day-of-year (Northern Hemisphere)."""
    if day_of_year < 60 or day_of_year >= 335:
        return Season.WINTER
    elif day_of_year < 152:
        return Season.SPRING
    elif day_of_year < 244:
        return Season.SUMMER
    else:
        return Season.AUTUMN


def _doy_sin(day_of_year: int) -> float:
    return math.sin(2 * math.pi * day_of_year / 365)


def _doy_cos(day_of_year: int) -> float:
    return math.cos(2 * math.pi * day_of_year / 365)


# ---------------------------------------------------------------------------
# Base event
# ---------------------------------------------------------------------------


class BasePriceEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    price_usd: float = Field(..., gt=0)
    price_local: Optional[float] = Field(None, gt=0)
    local_currency: Optional[str] = Field(None, min_length=3, max_length=3)
    fx_rate_to_usd: Optional[float] = Field(None, gt=0)
    unit_raw: str = Field(...)
    unit_normalized: UnitNormalized = Field(UnitNormalized.PER_TONNE)
    unit_conversion_factor: float = Field(1.0, gt=0)
    source: DataSource
    source_priority: int = Field(..., ge=1, le=99)
    region: str = Field(...)
    data_as_of: datetime = Field(...)
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    day_of_year: int = Field(..., ge=1, le=366)
    season: Season
    latency_minutes: Optional[int] = Field(None, ge=0)
    is_stale: bool = False
    day_of_year_sin: Optional[float] = None
    day_of_year_cos: Optional[float] = None

    @field_validator("local_currency")
    @classmethod
    def currency_uppercase(cls, v: Optional[str]) -> Optional[str]:
        return v.upper() if v else v

    @field_validator("data_as_of", "ingested_at", mode="before")
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        if isinstance(v, datetime) and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    @model_validator(mode="after")
    def derive_computed_fields(self) -> "BasePriceEvent":
        delta = self.ingested_at - self.data_as_of
        self.latency_minutes = int(delta.total_seconds() / 60)
        self.day_of_year_sin = round(_doy_sin(self.day_of_year), 6)
        self.day_of_year_cos = round(_doy_cos(self.day_of_year), 6)
        return self

    model_config = {"use_enum_values": True, "protected_namespaces": ()}


# ---------------------------------------------------------------------------
# Topic: food-prices
# ---------------------------------------------------------------------------


class FoodPriceEvent(BasePriceEvent):
    commodity: FoodCommodity

    @classmethod
    def build(
        cls,
        commodity: FoodCommodity,
        price_usd: float,
        unit_raw: str,
        unit_conversion_factor: float,
        source: DataSource,
        source_priority: int,
        region: str,
        data_as_of: datetime,
        price_local: Optional[float] = None,
        local_currency: Optional[str] = None,
        fx_rate_to_usd: Optional[float] = None,
    ) -> "FoodPriceEvent":
        doy = data_as_of.timetuple().tm_yday
        return cls(
            commodity=commodity,
            price_usd=price_usd,
            price_local=price_local,
            local_currency=local_currency,
            fx_rate_to_usd=fx_rate_to_usd,
            unit_raw=unit_raw,
            unit_conversion_factor=unit_conversion_factor,
            source=source,
            source_priority=source_priority,
            region=region,
            data_as_of=data_as_of,
            day_of_year=doy,
            season=_season_from_doy(doy),
        )


# ---------------------------------------------------------------------------
# Topic: energy-prices
# ---------------------------------------------------------------------------


class EnergyPriceEvent(BasePriceEvent):
    commodity: EnergyCommodity
    barrel_price_usd: Optional[float] = Field(None, gt=0)

    @classmethod
    def build(
        cls,
        commodity: EnergyCommodity,
        price_usd: float,
        unit_raw: str,
        unit_conversion_factor: float,
        source: DataSource,
        source_priority: int,
        region: str,
        data_as_of: datetime,
        barrel_price_usd: Optional[float] = None,
        price_local: Optional[float] = None,
        local_currency: Optional[str] = None,
        fx_rate_to_usd: Optional[float] = None,
    ) -> "EnergyPriceEvent":
        doy = data_as_of.timetuple().tm_yday
        return cls(
            commodity=commodity,
            price_usd=price_usd,
            barrel_price_usd=barrel_price_usd,
            price_local=price_local,
            local_currency=local_currency,
            fx_rate_to_usd=fx_rate_to_usd,
            unit_raw=unit_raw,
            unit_conversion_factor=unit_conversion_factor,
            source=source,
            source_priority=source_priority,
            region=region,
            data_as_of=data_as_of,
            day_of_year=doy,
            season=_season_from_doy(doy),
        )


# ---------------------------------------------------------------------------
# Topic: fertilizer-supply
# ---------------------------------------------------------------------------


class FertilizerSupplyEvent(BasePriceEvent):
    commodity: FertilizerCommodity
    supply_index: Optional[float] = Field(None, ge=0, le=200)
    natural_gas_price_usd: Optional[float] = Field(None, gt=0)

    @classmethod
    def build(
        cls,
        commodity: FertilizerCommodity,
        price_usd: float,
        unit_raw: str,
        unit_conversion_factor: float,
        source: DataSource,
        source_priority: int,
        region: str,
        data_as_of: datetime,
        supply_index: Optional[float] = None,
        natural_gas_price_usd: Optional[float] = None,
        price_local: Optional[float] = None,
        local_currency: Optional[str] = None,
        fx_rate_to_usd: Optional[float] = None,
    ) -> "FertilizerSupplyEvent":
        doy = data_as_of.timetuple().tm_yday
        return cls(
            commodity=commodity,
            price_usd=price_usd,
            supply_index=supply_index,
            natural_gas_price_usd=natural_gas_price_usd,
            price_local=price_local,
            local_currency=local_currency,
            fx_rate_to_usd=fx_rate_to_usd,
            unit_raw=unit_raw,
            unit_conversion_factor=unit_conversion_factor,
            source=source,
            source_priority=source_priority,
            region=region,
            data_as_of=data_as_of,
            day_of_year=doy,
            season=_season_from_doy(doy),
        )


# ---------------------------------------------------------------------------
# Topic: price-alerts
# ---------------------------------------------------------------------------


class ContributingFactor(str, Enum):
    ENERGY_SPIKE = "energy_spike"
    FERTILIZER_SHORTAGE = "fertilizer_shortage"
    EXPORT_BAN = "export_ban"
    DROUGHT = "drought"
    FLOOD = "flood"
    CURRENCY_DEPRECIATION = "currency_depreciation"
    LOGISTICS_DISRUPTION = "logistics_disruption"
    SPECULATION = "speculation"


class PriceAlertEvent(BaseModel):
    alert_id: UUID = Field(default_factory=uuid4)
    commodity: str
    region: str
    anomaly_score: float = Field(..., ge=0.0, le=1.0)
    severity: Severity
    current_price_usd: float
    current_price_local: Optional[float] = None
    local_currency: Optional[str] = None
    baseline_price_usd: float
    pct_deviation_usd: float
    pct_deviation_local: Optional[float] = None
    contributing_factors: list[ContributingFactor] = Field(default_factory=list)
    geopolitical_tags: list[str] = Field(default_factory=list)
    is_seasonal_adjusted: bool = True
    model_version: str = "unknown"
    dedup_key: str = ""
    cooldown_until: datetime
    ttl_seconds: int = Field(default=3600, gt=0)
    triggered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def derive_dedup_key(self) -> "PriceAlertEvent":
        if not self.dedup_key:
            self.dedup_key = f"{self.commodity}:{self.region}:{self.severity}"
        return self

    model_config = {"use_enum_values": True, "protected_namespaces": ()}
