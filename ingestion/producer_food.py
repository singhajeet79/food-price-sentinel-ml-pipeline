"""
ingestion/producer_food.py
--------------------------
Kafka producer for food commodity prices (topic: food-prices).

Simulates a realistic price feed for local dev using SYNTHETIC source.
In production, replace _fetch_prices() with real API calls to FAO, USDA, etc.

Usage:
    python -m ingestion.producer_food               # runs indefinitely
    python -m ingestion.producer_food --once        # single emit then exit
    python -m ingestion.producer_food --dry-run     # print payloads, no Kafka
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

from confluent_kafka import Producer
from dotenv import load_dotenv

from processing.schemas import DataSource, FoodCommodity, FoodPriceEvent

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("producer.food")


# ---------------------------------------------------------------------------
# Kafka configuration
# ---------------------------------------------------------------------------


def _build_kafka_config() -> dict:
    """Build Aiven-compatible Kafka producer config from environment."""
    cfg: dict = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL"),
        # Reliability settings
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "enable.idempotence": True,
        # Throughput / latency trade-off (tuned for low-volume price feeds)
        "linger.ms": 50,
        "batch.size": 16384,
        "compression.type": "snappy",
    }

    # SSL certificates — required for Aiven Kafka
    ssl_cafile = os.getenv("KAFKA_SSL_CAFILE")
    ssl_certfile = os.getenv("KAFKA_SSL_CERTFILE")
    ssl_keyfile = os.getenv("KAFKA_SSL_KEYFILE")

    if ssl_cafile:
        cfg["ssl.ca.location"] = ssl_cafile
    if ssl_certfile:
        cfg["ssl.certificate.location"] = ssl_certfile
    if ssl_keyfile:
        cfg["ssl.key.location"] = ssl_keyfile

    return cfg


# ---------------------------------------------------------------------------
# Price simulation (replace with live API calls in production)
# ---------------------------------------------------------------------------

# Baseline prices in USD per tonne — approximate real-world 2024 averages
_BASELINES: dict[FoodCommodity, float] = {
    FoodCommodity.WHEAT: 310.0,
    FoodCommodity.RICE: 560.0,
    FoodCommodity.MAIZE: 220.0,
    FoodCommodity.SOYBEANS: 480.0,
    FoodCommodity.PALM_OIL: 950.0,
    FoodCommodity.SUGAR: 430.0,
    FoodCommodity.BARLEY: 250.0,
}

# Units as reported by typical sources before normalization
_UNIT_RAW: dict[FoodCommodity, tuple[str, float]] = {
    # (unit_raw, conversion_factor_to_per_tonne)
    FoodCommodity.WHEAT: ("per_bushel", 36.7437),  # 1 tonne = 36.74 bushels
    FoodCommodity.RICE: ("per_cwt", 22.0462),  # 1 tonne = 22.05 cwt
    FoodCommodity.MAIZE: ("per_bushel", 39.3683),
    FoodCommodity.SOYBEANS: ("per_bushel", 36.7437),
    FoodCommodity.PALM_OIL: ("per_tonne", 1.0),
    FoodCommodity.SUGAR: ("per_lb", 2204.62),  # 1 tonne = 2204.62 lb
    FoodCommodity.BARLEY: ("per_tonne", 1.0),
}

# FX rate simulation: USD → local currency (INR as representative South Asia rate)
_FX_RATE_USD_INR = 83.5


def _simulate_price(commodity: FoodCommodity, as_of: datetime) -> dict:
    """
    Generate a realistic synthetic price for a commodity.

    Adds:
    - Gaussian noise (±3% daily volatility)
    - Mild seasonal trend (±8% across the year)
    - Occasional shock (5% chance of ±15% spike)
    """
    baseline = _BASELINES[commodity]
    doy = as_of.timetuple().tm_yday

    # Seasonal component: sine wave peaking in northern-hemisphere winter
    import math

    seasonal_factor = 1 + 0.08 * math.sin(2 * math.pi * (doy - 30) / 365)

    # Random daily noise
    noise = random.gauss(0, 0.03)

    # Rare shock
    shock = random.choice([0.0] * 19 + [random.uniform(-0.15, 0.20)])

    raw_price_per_unit = baseline * seasonal_factor * (1 + noise + shock)
    # Hard ceiling: price cannot exceed 3× baseline regardless of walk
    raw_price_per_unit = min(raw_price_per_unit, baseline * 3.0)
    raw_price_per_unit = max(raw_price_per_unit, baseline * 0.3)

    unit_raw, conversion_factor = _UNIT_RAW[commodity]

    # Baselines are always USD/tonne. conversion_factor converts the
    # raw source unit back to per-tonne for the schema's unit_raw field.
    # The simulation already works in per-tonne space so no conversion needed.
    price_usd = raw_price_per_unit

    price_local = price_usd * _FX_RATE_USD_INR

    return {
        "price_usd": round(price_usd, 4),
        "price_local": round(price_local, 2),
        "local_currency": "INR",
        "fx_rate_to_usd": _FX_RATE_USD_INR,
        "unit_raw": unit_raw,
        "unit_conversion_factor": conversion_factor,
    }


def _generate_events(as_of: datetime | None = None) -> Iterator[FoodPriceEvent]:
    """Yield one FoodPriceEvent per commodity for a given timestamp."""
    if as_of is None:
        as_of = datetime.now(timezone.utc) - timedelta(
            minutes=30
        )  # simulate ~30min lag

    source_priority = int(os.getenv("SOURCE_PRIORITY_FAO", "1"))

    for commodity in FoodCommodity:
        sim = _simulate_price(commodity, as_of)
        event = FoodPriceEvent.build(
            commodity=commodity,
            price_usd=sim["price_usd"],
            unit_raw=sim["unit_raw"],
            unit_conversion_factor=sim["unit_conversion_factor"],
            source=DataSource.SYNTHETIC,
            source_priority=source_priority,
            region="global",
            data_as_of=as_of,
            price_local=sim["price_local"],
            local_currency=sim["local_currency"],
            fx_rate_to_usd=sim["fx_rate_to_usd"],
        )

        # Apply staleness flag based on configured threshold
        stale_threshold = int(os.getenv("STALE_DATA_THRESHOLD_MINUTES", "180"))
        if event.latency_minutes and event.latency_minutes > stale_threshold:
            event.is_stale = True

        yield event


# ---------------------------------------------------------------------------
# Kafka delivery callback
# ---------------------------------------------------------------------------


def _on_delivery(err, msg) -> None:
    if err:
        log.error("Delivery failed | topic=%s error=%s", msg.topic(), err)
    else:
        log.debug(
            "Delivered | topic=%s partition=%d offset=%d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


# ---------------------------------------------------------------------------
# Main producer loop
# ---------------------------------------------------------------------------


def produce_once(producer: Producer | None, topic: str, dry_run: bool = False) -> int:
    """
    Emit one batch of food price events (one per commodity) to Kafka.
    Returns the number of events produced.
    """
    count = 0
    for event in _generate_events():
        payload = event.model_dump_json()

        if dry_run:
            log.info("[DRY RUN] %s → %s", topic, payload)
        else:
            assert producer is not None
            producer.produce(
                topic=topic,
                key=event.commodity,  # partition by commodity
                value=payload.encode("utf-8"),
                callback=_on_delivery,
            )

        stale_marker = " [STALE]" if event.is_stale else ""
        log.info(
            "%-20s | $%8.2f USD/t | %s%s",
            event.commodity,
            event.price_usd,
            event.season,
            stale_marker,
        )
        count += 1

    if not dry_run and producer:
        producer.flush(timeout=10)

    return count


def run(interval_seconds: int = 60, once: bool = False, dry_run: bool = False) -> None:
    topic = os.getenv("KAFKA_TOPIC_FOOD", "food-prices")

    if dry_run:
        log.info("=== DRY RUN MODE — no Kafka connection ===")
        produce_once(None, topic, dry_run=True)
        return

    producer = Producer(_build_kafka_config())
    log.info("Producer connected | topic=%s | interval=%ds", topic, interval_seconds)

    try:
        while True:
            n = produce_once(producer, topic)
            log.info("Batch complete | events=%d", n)
            if once:
                break
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        log.info("Shutting down producer")
    finally:
        producer.flush(timeout=15)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Food price Kafka producer")
    parser.add_argument("--once", action="store_true", help="Emit one batch and exit")
    parser.add_argument(
        "--dry-run", action="store_true", help="Print payloads without Kafka"
    )
    parser.add_argument(
        "--interval", type=int, default=60, help="Seconds between batches"
    )
    args = parser.parse_args()

    run(interval_seconds=args.interval, once=args.once, dry_run=args.dry_run)
