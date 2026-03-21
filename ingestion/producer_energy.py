"""
ingestion/producer_energy.py
----------------------------
Kafka producer for energy prices (topic: energy-prices).

Energy prices (crude oil, natural gas, coal) are upstream leading indicators
for food prices — typically with a 2–6 week lag before the shock propagates
through fertilizer production and transport costs.

Usage:
    python -m ingestion.producer_energy
    python -m ingestion.producer_energy --once
    python -m ingestion.producer_energy --dry-run
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

from processing.schemas import DataSource, EnergyCommodity, EnergyPriceEvent

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("producer.energy")


# ---------------------------------------------------------------------------
# Kafka config (identical pattern to producer_food — shared via env)
# ---------------------------------------------------------------------------


def _build_kafka_config() -> dict:
    cfg: dict = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL"),
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "enable.idempotence": True,
        "linger.ms": 50,
        "compression.type": "snappy",
    }
    if cafile := os.getenv("KAFKA_SSL_CAFILE"):
        cfg["ssl.ca.location"] = cafile
    if certfile := os.getenv("KAFKA_SSL_CERTFILE"):
        cfg["ssl.certificate.location"] = certfile
    if keyfile := os.getenv("KAFKA_SSL_KEYFILE"):
        cfg["ssl.key.location"] = keyfile
    return cfg


# ---------------------------------------------------------------------------
# Price simulation
# ---------------------------------------------------------------------------

# Baseline prices in USD per tonne (2024 averages)
_BASELINES: dict[EnergyCommodity, float] = {
    EnergyCommodity.CRUDE_OIL_BRENT: 550.0,   # ~$75/barrel × 7.33 barrels/tonne
    EnergyCommodity.CRUDE_OIL_WTI:   530.0,
    EnergyCommodity.NATURAL_GAS:      95.0,   # Henry Hub equivalent, per tonne LNG
    EnergyCommodity.COAL:            140.0,
}

# (unit_raw, conversion_factor → per_tonne, barrel_equiv or None)
# Crude oil: 1 tonne ≈ 7.33 barrels (API 35° gravity)
_UNIT_META: dict[EnergyCommodity, tuple[str, float, bool]] = {
    EnergyCommodity.CRUDE_OIL_BRENT: ("per_barrel", 7.33, True),
    EnergyCommodity.CRUDE_OIL_WTI:   ("per_barrel", 7.33, True),
    EnergyCommodity.NATURAL_GAS:     ("per_mmbtu",  55.0, False),  # ~55 MMBTU/tonne LNG
    EnergyCommodity.COAL:            ("per_tonne",   1.0, False),
}

# Energy prices are more volatile than food; use higher noise parameters
_VOLATILITY: dict[EnergyCommodity, float] = {
    EnergyCommodity.CRUDE_OIL_BRENT: 0.02,
    EnergyCommodity.CRUDE_OIL_WTI:   0.02,
    EnergyCommodity.NATURAL_GAS:     0.05,   # nat gas is extremely volatile
    EnergyCommodity.COAL:            0.015,
}

_FX_RATE_USD_INR = 83.5


def _simulate_energy_price(commodity: EnergyCommodity) -> dict:
    baseline = _BASELINES[commodity]
    unit_raw, conversion_factor, has_barrel = _UNIT_META[commodity]
    vol = _VOLATILITY[commodity]

    # Geopolitical shock: higher base rate for energy (15% chance vs 5% for food)
    shock = random.choice([0.0] * 17 + [random.uniform(-0.20, 0.30)] * 3)
    noise = random.gauss(0, vol)

    raw_unit_price = baseline / conversion_factor * (1 + noise + shock)
    price_usd = raw_unit_price * conversion_factor

    barrel_price = raw_unit_price if has_barrel else None
    price_local = price_usd * _FX_RATE_USD_INR

    return {
        "price_usd": round(price_usd, 4),
        "barrel_price_usd": round(barrel_price, 4) if barrel_price else None,
        "price_local": round(price_local, 2),
        "local_currency": "INR",
        "fx_rate_to_usd": _FX_RATE_USD_INR,
        "unit_raw": unit_raw,
        "unit_conversion_factor": conversion_factor,
    }


def _generate_events(as_of: datetime | None = None) -> Iterator[EnergyPriceEvent]:
    if as_of is None:
        # Energy prices update more frequently; simulate 15-min lag
        as_of = datetime.now(timezone.utc) - timedelta(minutes=15)

    source_priority = int(os.getenv("SOURCE_PRIORITY_IEA", "2"))

    for commodity in EnergyCommodity:
        sim = _simulate_energy_price(commodity)
        event = EnergyPriceEvent.build(
            commodity=commodity,
            price_usd=sim["price_usd"],
            unit_raw=sim["unit_raw"],
            unit_conversion_factor=sim["unit_conversion_factor"],
            source=DataSource.SYNTHETIC,
            source_priority=source_priority,
            region="global",
            data_as_of=as_of,
            barrel_price_usd=sim["barrel_price_usd"],
            price_local=sim["price_local"],
            local_currency=sim["local_currency"],
            fx_rate_to_usd=sim["fx_rate_to_usd"],
        )

        stale_threshold = int(os.getenv("STALE_DATA_THRESHOLD_MINUTES", "180"))
        if event.latency_minutes and event.latency_minutes > stale_threshold:
            event.is_stale = True

        yield event


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------


def _on_delivery(err, msg) -> None:
    if err:
        log.error("Delivery failed | topic=%s error=%s", msg.topic(), err)
    else:
        log.debug(
            "Delivered | topic=%s partition=%d offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ---------------------------------------------------------------------------
# Producer loop
# ---------------------------------------------------------------------------


def produce_once(producer: Producer | None, topic: str, dry_run: bool = False) -> int:
    count = 0
    for event in _generate_events():
        payload = event.model_dump_json()

        if dry_run:
            log.info("[DRY RUN] %s → %s", topic, payload)
        else:
            assert producer is not None
            producer.produce(
                topic=topic,
                key=event.commodity,
                value=payload.encode("utf-8"),
                callback=_on_delivery,
            )

        barrel_str = (
            f" (${event.barrel_price_usd:.2f}/bbl)"
            if event.barrel_price_usd else ""
        )
        stale_marker = " [STALE]" if event.is_stale else ""
        log.info(
            "%-25s | $%8.2f USD/t%s%s",
            event.commodity,
            event.price_usd,
            barrel_str,
            stale_marker,
        )
        count += 1

    if not dry_run and producer:
        producer.flush(timeout=10)

    return count


def run(interval_seconds: int = 30, once: bool = False, dry_run: bool = False) -> None:
    """
    Energy prices update more frequently than food prices.
    Default interval: 30s (vs 60s for food).
    """
    topic = os.getenv("KAFKA_TOPIC_ENERGY", "energy-prices")

    if dry_run:
        log.info("=== DRY RUN MODE ===")
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
        log.info("Shutting down energy producer")
    finally:
        producer.flush(timeout=15)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Energy price Kafka producer")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    args = parser.parse_args()

    run(interval_seconds=args.interval, once=args.once, dry_run=args.dry_run)
