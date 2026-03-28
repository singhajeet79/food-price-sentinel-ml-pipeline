"""
processing/consumer.py
-----------------------
Kafka consumer orchestrator for the Food Price Sentinel pipeline.

Subscribes to all three raw price topics simultaneously using a single
KafkaConsumer with topic assignment. Routes each message to the appropriate
FeatureEngineer ingest method. When a FeatureVector is produced, it is:

    1. Logged to PostgreSQL (storage layer)          — async, best-effort
    2. Forwarded to the anomaly scorer               — inline, blocking
    3. Consumer lag recorded to PostgreSQL            — every LAG_LOG_INTERVAL_S

Architecture note — single consumer, three topics:
    Kafka allows one consumer to subscribe to multiple topics. All three
    price topics (food, energy, fertilizer) are consumed in a single
    poll loop. This keeps the feature state (rolling buffers) in one
    process and avoids cross-process synchronisation.

    If throughput grows beyond what a single process can handle, split
    topics across consumer group members with separate FeatureEngineer
    instances — but then energy/fertilizer correlation must be handled
    via a shared state store (e.g. Redis/Valkey).

Usage:
    python -m processing.consumer

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS
    KAFKA_SECURITY_PROTOCOL
    KAFKA_SSL_CAFILE / KAFKA_SSL_CERTFILE / KAFKA_SSL_KEYFILE
    KAFKA_TOPIC_FOOD
    KAFKA_TOPIC_ENERGY
    KAFKA_TOPIC_FERTILIZER
    KAFKA_CONSUMER_GROUP
    KAFKA_AUTO_OFFSET_RESET       (earliest | latest, default: latest)
    STALE_DATA_THRESHOLD_MINUTES
    LAG_LOG_INTERVAL_S            (default: 60)
    DRY_RUN                       (1 = skip DB writes and scoring, just log)
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from typing import Optional

from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from processing.features import FeatureEngineer, FeatureVector

# Add after the existing imports
from detection.score import Scorer
from alerting.alert_writer import AlertWriter

from storage.models import FeatureVector as FeatureVectorORM, AnomalyLog
from storage.db import get_session

_scorer = Scorer()
_alert_writer = AlertWriter()

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("consumer")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
CONSUMER_GROUP    = os.getenv("KAFKA_CONSUMER_GROUP", "food-price-sentinel-processor")
AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

TOPIC_FOOD        = os.getenv("KAFKA_TOPIC_FOOD",        "food-prices")
TOPIC_ENERGY      = os.getenv("KAFKA_TOPIC_ENERGY",      "energy-prices")
TOPIC_FERTILIZER  = os.getenv("KAFKA_TOPIC_FERTILIZER",  "fertilizer-supply")

LAG_LOG_INTERVAL_S = int(os.getenv("LAG_LOG_INTERVAL_S", "60"))
DRY_RUN            = os.getenv("DRY_RUN", "0") == "1"

ALL_TOPICS = [TOPIC_FOOD, TOPIC_ENERGY, TOPIC_FERTILIZER]


# ---------------------------------------------------------------------------
# Consumer factory
# ---------------------------------------------------------------------------

def build_consumer() -> KafkaConsumer:
    kwargs: dict = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "group_id": CONSUMER_GROUP,
        "auto_offset_reset": AUTO_OFFSET_RESET,
        "enable_auto_commit": False,       # manual commit for at-least-once
        "value_deserializer": lambda b: json.loads(b.decode("utf-8")),
        "key_deserializer": lambda b: b.decode("utf-8") if b else None,
        "max_poll_records": 50,
        "session_timeout_ms": 30_000,
        "heartbeat_interval_ms": 10_000,
        "fetch_max_wait_ms": 500,
    }

    if SECURITY_PROTOCOL == "SSL":
        kwargs.update(
            {
                "security_protocol": "SSL",
                "ssl_cafile": os.getenv("KAFKA_SSL_CAFILE"),
                "ssl_certfile": os.getenv("KAFKA_SSL_CERTFILE"),
                "ssl_keyfile": os.getenv("KAFKA_SSL_KEYFILE"),
            }
        )
    else:
        kwargs["security_protocol"] = "PLAINTEXT"

    consumer = KafkaConsumer(**kwargs)
    consumer.subscribe(ALL_TOPICS)
    log.info(
        "Consumer subscribed to topics=%s group=%s offset_reset=%s",
        ALL_TOPICS, CONSUMER_GROUP, AUTO_OFFSET_RESET,
    )
    return consumer


# ---------------------------------------------------------------------------
# Storage stub (replaced by real SQLAlchemy calls in storage/db.py)
# ---------------------------------------------------------------------------

def persist_feature_vector(fv: FeatureVector) -> Optional[int]:
    """Write feature vector to PostgreSQL. Returns the inserted row ID."""
    try:
        with get_session() as session:
            row = FeatureVectorORM.from_feature_vector(fv)
            session.add(row)
            session.flush()
            return row.id
    except Exception as exc:
        log.error("Failed to persist feature vector: %s", exc)
        return None

# ---------------------------------------------------------------------------
# Scorer stub (replaced by detection/score.py)
# ---------------------------------------------------------------------------

def score_feature_vector(fv: FeatureVector) -> Optional[float]:
    result = _scorer.score(fv)
    if result is None:
        return None

    alerted = False
    suppressed = False
    alert_payload = None

    if result.is_anomaly:
        written = _alert_writer.write(scored_result=result, feature_vector=fv)
        alerted = written
        suppressed = not written

    try:
        with get_session() as session:
            log_row = AnomalyLog.from_scored_result(
                scored_result=result,
                alerted=alerted,
                suppressed=suppressed,
                alert_payload=alert_payload,
            )
            session.add(log_row)
    except Exception as exc:
        log.error("Failed to persist anomaly log: %s", exc)

    return result.normalised_score

# ---------------------------------------------------------------------------
# Consumer lag tracking
# ---------------------------------------------------------------------------

class LagTracker:
    """
    Polls Kafka for consumer group lag and logs it.

    Lag = (latest offset in partition) - (committed offset for consumer group)

    High lag means the consumer is falling behind and "real-time" alerts
    are actually delayed. This is the operational health signal.
    """

    def __init__(self, consumer: KafkaConsumer) -> None:
        self._consumer = consumer
        self._last_log_time = time.monotonic()

    def maybe_log(self) -> None:
        now = time.monotonic()
        if now - self._last_log_time < LAG_LOG_INTERVAL_S:
            return
        self._last_log_time = now
        self._log_lag()

    def _log_lag(self) -> None:
        try:
            partitions = self._consumer.assignment()
            if not partitions:
                return

            end_offsets = self._consumer.end_offsets(list(partitions))
            committed = {
                tp: self._consumer.committed(tp) or 0
                for tp in partitions
            }

            total_lag = 0
            for tp in partitions:
                end = end_offsets.get(tp, 0)
                committed_offset = committed.get(tp, 0)
                lag = max(0, end - committed_offset)
                total_lag += lag
                if lag > 1000:
                    log.warning(
                        "HIGH LAG: topic=%s partition=%d lag=%d",
                        tp.topic, tp.partition, lag,
                    )
                else:
                    log.info(
                        "Lag: topic=%s partition=%d lag=%d",
                        tp.topic, tp.partition, lag,
                    )

            log.info("Total consumer lag across all partitions: %d messages", total_lag)

            if not DRY_RUN:
                _persist_lag_snapshot(partitions, end_offsets, committed)

        except Exception as exc:
            log.warning("Failed to compute consumer lag: %s", exc)


def _persist_lag_snapshot(partitions, end_offsets, committed) -> None:
    """
    Write lag snapshot to PostgreSQL consumer_lag_log table.

    Stub — replace with real DB write once storage/ is wired up.
    """
    log.debug("_persist_lag_snapshot: stub — wire up storage layer")


# ---------------------------------------------------------------------------
# Message router
# ---------------------------------------------------------------------------

class MessageRouter:
    """
    Routes Kafka messages to the correct FeatureEngineer ingest method
    based on the topic name.
    """

    def __init__(self, engineer: FeatureEngineer) -> None:
        self._engineer = engineer
        self._topic_handlers = {
            TOPIC_FOOD:       self._handle_food,
            TOPIC_ENERGY:     self._handle_energy,
            TOPIC_FERTILIZER: self._handle_fertilizer,
        }

    def route(self, topic: str, key: Optional[str], value: dict) -> Optional[FeatureVector]:
        handler = self._topic_handlers.get(topic)
        if handler is None:
            log.warning("No handler for topic '%s' — skipping", topic)
            return None
        return handler(key, value)

    def _handle_food(self, key: Optional[str], value: dict) -> Optional[FeatureVector]:
        try:
            return self._engineer.process_food_event(value)
        except Exception as exc:
            log.error("Error processing food event (key=%s): %s", key, exc, exc_info=True)
            return None

    def _handle_energy(self, key: Optional[str], value: dict) -> None:
        try:
            self._engineer.process_energy_event(value)
        except Exception as exc:
            log.error("Error processing energy event (key=%s): %s", key, exc, exc_info=True)

    def _handle_fertilizer(self, key: Optional[str], value: dict) -> None:
        try:
            self._engineer.process_fertilizer_event(value)
        except Exception as exc:
            log.error("Error processing fertilizer event (key=%s): %s", key, exc, exc_info=True)


# ---------------------------------------------------------------------------
# Main poll loop
# ---------------------------------------------------------------------------

def run() -> None:
    consumer   = build_consumer()
    engineer   = FeatureEngineer()
    router     = MessageRouter(engineer)
    lag_tracker = LagTracker(consumer)

    running = True

    def _shutdown(sig, frame):
        nonlocal running
        log.info("Shutdown signal received — draining and exiting.")
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("Consumer poll loop started. DRY_RUN=%s", DRY_RUN)
    messages_total = 0
    vectors_total  = 0

    try:
        while running:
            # Poll with a 1-second timeout so we can check `running` flag
            records = consumer.poll(timeout_ms=1000)

            if not records:
                lag_tracker.maybe_log()
                continue

            offsets_to_commit: dict[TopicPartition, OffsetAndMetadata] = {}

            for tp, messages in records.items():
                for msg in messages:
                    messages_total += 1

                    fv = router.route(tp.topic, msg.key, msg.value)

                    if fv is not None:
                        vectors_total += 1

                        # 1. Persist feature vector
                        persist_feature_vector(fv)

                        # 2. Score for anomalies
                        score = score_feature_vector(fv)

                        log.info(
                            "Vector emitted: %s/%s price=%.2f seasonal_adj=%.2f "
                            "momentum=%.4f energy_corr=%.4f low_conf=%s score=%s",
                            fv.commodity, fv.region,
                            fv.raw_price_usd,
                            fv.seasonal_adjusted_price,
                            fv.momentum,
                            fv.energy_lag_corr,
                            fv.is_low_confidence,
                            f"{score:.4f}" if score is not None else "pending",
                        )

                    # Track offset for manual commit
                    offsets_to_commit[tp] = OffsetAndMetadata(msg.offset + 1, None, -1)

            # Commit after processing the full batch (at-least-once semantics)
            if offsets_to_commit:
                consumer.commit(offsets_to_commit)

            lag_tracker.maybe_log()

    except Exception as exc:
        log.critical("Unhandled exception in poll loop: %s", exc, exc_info=True)
        raise
    finally:
        log.info(
            "Shutting down. messages_total=%d vectors_emitted=%d",
            messages_total, vectors_total,
        )
        log.info("Feature engineer stats: %s", engineer.stats())
        consumer.close()
        log.info("Consumer closed cleanly.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()
