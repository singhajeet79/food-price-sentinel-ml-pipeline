"""
ingestion/producer_fertilizer.py
---------------------------------
Kafka producer for fertilizer supply and prices (topic: fertilizer-supply).

Fertilizer prices are a lagged leading indicator — typically 4-8 weeks ahead
of food price movement. Natural gas is the dominant input cost for nitrogen
fertilizers (urea, ammonia), so natural gas shocks propagate with this lag.

Key behaviour:
- Emits supply_index alongside price — a value <80 signals supply constraint
  even when the headline price hasn't moved yet.
- Natural gas price is captured on each event as a production cost snapshot.

Usage:
    python -m ingestion.producer_fertilizer
    python -m ingestion.producer_fertilizer --once
    python -m ingestion.producer_fertilizer --dry-run
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

from processing.schemas import (DataSource, FertilizerCommodity,
                                FertilizerSupplyEvent)

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("producer.fertilizer")


# ---------------------------------------------------------------------------
# Kafka config
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
# Price & supply simulation
# ---------------------------------------------------------------------------

# Baseline prices USD per tonne (2024 averages)
_BASELINES: dict[FertilizerCommodity, float] = {
    FertilizerCommodity.UREA: 350.0,
    FertilizerCommodity.DAP: 600.0,
    FertilizerCommodity.MOP: 320.0,
    FertilizerCommodity.AMMONIA: 430.0,
    FertilizerCommodity.NATURAL_GAS_FEEDSTOCK: 95.0,  # per tonne LNG equivalent
}

# Which commodities are directly nat-gas-dependent (nitrogen-based)
_NAT_GAS_DEPENDENT = {
    FertilizerCommodity.UREA,
    FertilizerCommodity.AMMONIA,
    FertilizerCommodity.NATURAL_GAS_FEEDSTOCK,
}

# Baseline supply index (100 = normal)
_SUPPLY_INDEX_BASELINE = 100.0

# Natural gas baseline (USD per tonne LNG equivalent)
_NAT_GAS_BASELINE_USD = 95.0

# FX
_FX_RATE_USD_INR = 83.5


def _simulate_nat_gas_price() -> float:
    """Simulate current natural gas price — shared across fertilizer events in a batch."""
    shock = random.choice([0.0] * 15 + [random.uniform(-0.25, 0.40)] * 5)
    noise = random.gauss(0, 0.05)
    return round(_NAT_GAS_BASELINE_USD * (1 + noise + shock), 4)


def _simulate_supply_index(
    commodity: FertilizerCommodity, nat_gas_price: float
) -> float:
    """
    Supply index reflects availability constraints.
    Nat-gas-dependent fertilizers see supply drops when gas price spikes >30%
    (producers idle plants when margins collapse).
    """
    base = _SUPPLY_INDEX_BASELINE
    noise = random.gauss(0, 3)  # ±3 index points daily noise

    if commodity in _NAT_GAS_DEPENDENT:
        gas_pct_above_baseline = (
            nat_gas_price - _NAT_GAS_BASELINE_USD
        ) / _NAT_GAS_BASELINE_USD
        if gas_pct_above_baseline > 0.30:
            # Simulate plant idling: supply drops 10-30 index points
            constraint = random.uniform(10, 30)
            base -= constraint
            log.debug(
                "%-25s | Nat-gas spike (%.0f%% above baseline) → supply constraint −%.1f",
                commodity,
                gas_pct_above_baseline * 100,
                constraint,
            )

    return round(max(0.0, min(200.0, base + noise)), 2)


def _simulate_fertilizer_price(
    commodity: FertilizerCommodity, nat_gas_price: float
) -> dict:
    baseline = _BASELINES[commodity]

    # Nat-gas-dependent commodities co-move with gas price
    if commodity in _NAT_GAS_DEPENDENT:
        gas_factor = nat_gas_price / _NAT_GAS_BASELINE_USD
        # Gas is ~70% of urea/ammonia production cost
        price_factor = 0.30 + 0.70 * gas_factor
    else:
        price_factor = 1.0

    noise = random.gauss(0, 0.02)
    shock = random.choice([0.0] * 18 + [random.uniform(-0.12, 0.20)] * 2)
    price_usd = baseline * price_factor * (1 + noise + shock)

    supply_index = _simulate_supply_index(commodity, nat_gas_price)
    price_local = price_usd * _FX_RATE_USD_INR

    return {
        "price_usd": round(price_usd, 4),
        "price_local": round(price_local, 2),
        "local_currency": "INR",
        "fx_rate_to_usd": _FX_RATE_USD_INR,
        "supply_index": supply_index,
        "natural_gas_price_usd": nat_gas_price,
    }


def _generate_events(as_of: datetime | None = None) -> Iterator[FertilizerSupplyEvent]:
    if as_of is None:
        # Fertilizer data typically has a 60-120 min reporting lag
        as_of = datetime.now(timezone.utc) - timedelta(minutes=90)

    source_priority = int(os.getenv("SOURCE_PRIORITY_WORLDBANK", "2"))

    # Simulate a single nat-gas price for the whole batch (market snapshot)
    nat_gas_price = _simulate_nat_gas_price()
    log.info("Nat-gas price snapshot: $%.2f/t-LNG", nat_gas_price)

    for commodity in FertilizerCommodity:
        sim = _simulate_fertilizer_price(commodity, nat_gas_price)
        event = FertilizerSupplyEvent.build(
            commodity=commodity,
            price_usd=sim["price_usd"],
            unit_raw="per_tonne",
            unit_conversion_factor=1.0,
            source=DataSource.SYNTHETIC,
            source_priority=source_priority,
            region="global",
            data_as_of=as_of,
            supply_index=sim["supply_index"],
            natural_gas_price_usd=sim["natural_gas_price_usd"],
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
            msg.topic(),
            msg.partition(),
            msg.offset(),
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

        supply_str = (
            f" | supply_idx={event.supply_index:.1f}" if event.supply_index else ""
        )
        stale_marker = " [STALE]" if event.is_stale else ""
        log.info(
            "%-30s | $%8.2f USD/t%s%s",
            event.commodity,
            event.price_usd,
            supply_str,
            stale_marker,
        )
        count += 1

    if not dry_run and producer:
        producer.flush(timeout=10)

    return count


def run(interval_seconds: int = 120, once: bool = False, dry_run: bool = False) -> None:
    """
    Fertilizer data updates less frequently than energy.
    Default interval: 120s (2 min).
    """
    topic = os.getenv("KAFKA_TOPIC_FERTILIZER", "fertilizer-supply")

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
        log.info("Shutting down fertilizer producer")
    finally:
        producer.flush(timeout=15)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fertilizer supply Kafka producer")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--interval", type=int, default=120)
    args = parser.parse_args()

    run(interval_seconds=args.interval, once=args.once, dry_run=args.dry_run)
