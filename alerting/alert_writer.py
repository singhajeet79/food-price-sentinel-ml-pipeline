"""
alerting/alert_writer.py
------------------------
Writes anomaly alerts to Valkey (dedup + TTL cache) and the
price-alerts Kafka topic.

Two responsibilities:
  1. Deduplication gate — checks Valkey for an existing alert with the
     same dedup_key. If one exists and the cooldown hasn't expired, the
     alert is suppressed (logged but not written anywhere).

  2. Dual write — if the alert passes dedup, it is written to:
       a. Valkey   → key=alert:{dedup_key}  TTL=ttl_seconds
                     (for instant API lookups without hitting Postgres)
       b. Kafka    → topic=price-alerts
                     (for downstream consumers and audit trail)

     PostgreSQL write is handled by the consumer's persist layer —
     the alert_writer only owns the hot path (Valkey + Kafka).

Usage (called from processing/consumer.py after scorer returns anomaly):

    from alerting.alert_writer import AlertWriter
    _alert_writer = AlertWriter()

    # Inside the consumer loop, after scoring:
    if score_result and score_result.is_anomaly:
        _alert_writer.write(
            scored_result=score_result,
            feature_vector=fv,
        )

Environment variables:
    VALKEY_HOST
    VALKEY_PORT
    VALKEY_PASSWORD
    VALKEY_TLS                 (true | false)
    KAFKA_BOOTSTRAP_SERVERS
    KAFKA_SECURITY_PROTOCOL
    KAFKA_SSL_CAFILE / CERTFILE / KEYFILE
    KAFKA_TOPIC_ALERTS
    ALERT_COOLDOWN_SECONDS
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Optional

from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

load_dotenv()
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

VALKEY_HOST = os.getenv("VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.getenv("VALKEY_PORT", "6379"))
VALKEY_PASSWORD = os.getenv("VALKEY_PASSWORD")
VALKEY_TLS = os.getenv("VALKEY_TLS", "false").lower() == "true"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_ALERTS", "price-alerts")

ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "21600"))

# Valkey key prefix
VALKEY_KEY_PREFIX = "alert"


# ---------------------------------------------------------------------------
# Valkey client factory
# ---------------------------------------------------------------------------


def _build_valkey_client():
    """
    Build a Redis-compatible client pointed at Valkey.

    Uses the `redis` library (Valkey is wire-compatible with Redis).
    Returns None if connection fails — alert_writer degrades gracefully
    to Kafka-only mode rather than crashing the consumer.
    """
    try:
        import redis

        client = redis.Redis(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            password=VALKEY_PASSWORD,
            ssl=VALKEY_TLS,
            ssl_cert_reqs="required" if VALKEY_TLS else None,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        client.ping()
        log.info(
            "Valkey connected at %s:%d (tls=%s)", VALKEY_HOST, VALKEY_PORT, VALKEY_TLS
        )
        return client
    except Exception as exc:
        log.warning(
            "Valkey connection failed (%s) — alerts will be Kafka-only until Valkey is available.",
            exc,
        )
        return None


# ---------------------------------------------------------------------------
# Kafka producer factory
# ---------------------------------------------------------------------------


def _build_kafka_producer():
    """Build Kafka producer for publishing alerts."""
    try:
        from kafka import KafkaProducer

        kwargs: dict = {
            "bootstrap_servers": KAFKA_BOOTSTRAP,
            "value_serializer": lambda v: json.dumps(v, default=str).encode("utf-8"),
            "key_serializer": lambda k: k.encode("utf-8") if k else None,
            "acks": "all",
            "retries": 5,
            "retry_backoff_ms": 500,
        }
        if KAFKA_PROTOCOL == "SSL":
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

        producer = KafkaProducer(**kwargs)
        log.info("Alert Kafka producer connected → topic=%s", KAFKA_TOPIC)
        return producer
    except Exception as exc:
        log.error("Failed to build alert Kafka producer: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Alert writer
# ---------------------------------------------------------------------------


class AlertWriter:
    """
    Writes anomaly alerts to Valkey and the price-alerts Kafka topic.

    Instantiate once per consumer process. Handles its own Valkey and
    Kafka connections with lazy reconnect on failure.
    """

    def __init__(self) -> None:
        self._valkey = _build_valkey_client()
        self._kafka = _build_kafka_producer()

        # Counters
        self.alerts_written: int = 0
        self.alerts_suppressed: int = 0
        self.valkey_failures: int = 0
        self.kafka_failures: int = 0

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def write(
        self,
        scored_result,  # detection.score.ScoredResult
        feature_vector,  # processing.features.FeatureVector
        baseline_price_usd: Optional[float] = None,
    ) -> bool:
        """
        Attempt to write an alert for a scored anomaly.

        Returns True if the alert was written, False if suppressed by dedup.

        baseline_price_usd defaults to rolling_7d_avg when not provided —
        a reasonable proxy for "normal" price before the spike.
        """
        from alerting.alert_schema import build_alert_payload

        if baseline_price_usd is None:
            baseline_price_usd = feature_vector.rolling_7d_avg

        payload = build_alert_payload(
            scored_result=scored_result,
            current_price_usd=feature_vector.raw_price_usd,
            baseline_price_usd=baseline_price_usd,
            model_version=scored_result.model_version,
            is_seasonal_adjusted=feature_vector.is_seasonal_adjusted,
        )

        dedup_key = payload["dedup_key"]

        # --- Deduplication gate ---
        if self._is_suppressed(dedup_key):
            self.alerts_suppressed += 1
            log.info(
                "Alert suppressed (cooldown active): %s score=%.4f",
                dedup_key,
                scored_result.normalised_score,
            )
            return False

        # --- Dual write ---
        valkey_ok = self._write_valkey(dedup_key, payload)
        kafka_ok = self._write_kafka(dedup_key, payload)

        if valkey_ok or kafka_ok:
            self.alerts_written += 1
            log.warning(
                "ALERT WRITTEN | %s | severity=%s score=%.4f "
                "pct_dev=%.1f%% factors=%s valkey=%s kafka=%s",
                dedup_key,
                payload["severity"],
                payload["anomaly_score"],
                payload["pct_deviation_usd"],
                payload["contributing_factors"],
                "ok" if valkey_ok else "failed",
                "ok" if kafka_ok else "failed",
            )
            return True

        log.error("Alert lost — both Valkey and Kafka writes failed for %s", dedup_key)
        return False

    def stats(self) -> dict:
        return {
            "alerts_written": self.alerts_written,
            "alerts_suppressed": self.alerts_suppressed,
            "valkey_failures": self.valkey_failures,
            "kafka_failures": self.kafka_failures,
            "valkey_connected": self._valkey is not None,
            "kafka_connected": self._kafka is not None,
        }

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def _is_suppressed(self, dedup_key: str) -> bool:
        """
        Returns True if a cooldown key exists in Valkey for this dedup_key.

        Falls back to False (allow the alert) if Valkey is unavailable —
        better to over-alert than to silently drop during an outage.
        """
        if self._valkey is None:
            self._try_reconnect_valkey()
            return False  # allow alert if Valkey unavailable

        valkey_key = f"{VALKEY_KEY_PREFIX}:{dedup_key}"
        try:
            return self._valkey.exists(valkey_key) == 1
        except Exception as exc:
            log.warning("Valkey dedup check failed (%s) — allowing alert", exc)
            self.valkey_failures += 1
            self._valkey = None  # mark for reconnect on next call
            return False

    # ------------------------------------------------------------------
    # Valkey write
    # ------------------------------------------------------------------

    def _write_valkey(self, dedup_key: str, payload: dict) -> bool:
        """
        Write alert payload to Valkey with TTL.

        Key:   alert:{dedup_key}
        Value: JSON-serialised payload
        TTL:   ALERT_COOLDOWN_SECONDS
        """
        if self._valkey is None:
            self._try_reconnect_valkey()

        if self._valkey is None:
            log.warning("Valkey unavailable — skipping Valkey write for %s", dedup_key)
            self.valkey_failures += 1
            return False

        valkey_key = f"{VALKEY_KEY_PREFIX}:{dedup_key}"
        try:
            self._valkey.setex(
                name=valkey_key,
                time=ALERT_COOLDOWN_SECONDS,
                value=json.dumps(payload, default=str),
            )
            log.debug(
                "Valkey write OK: key=%s ttl=%ds", valkey_key, ALERT_COOLDOWN_SECONDS
            )
            return True
        except Exception as exc:
            log.error("Valkey write failed for %s: %s", valkey_key, exc)
            self.valkey_failures += 1
            self._valkey = None
            return False

    # ------------------------------------------------------------------
    # Kafka write
    # ------------------------------------------------------------------

    def _write_kafka(self, dedup_key: str, payload: dict) -> bool:
        """Publish alert payload to the price-alerts Kafka topic."""
        if self._kafka is None:
            self._try_reconnect_kafka()

        if self._kafka is None:
            log.warning("Kafka unavailable — skipping Kafka write for %s", dedup_key)
            self.kafka_failures += 1
            return False

        try:
            future = self._kafka.send(
                KAFKA_TOPIC,
                key=dedup_key,
                value=payload,
            )
            future.get(timeout=10)
            log.debug("Kafka alert published: topic=%s key=%s", KAFKA_TOPIC, dedup_key)
            return True
        except Exception as exc:
            log.error("Kafka alert publish failed for %s: %s", dedup_key, exc)
            self.kafka_failures += 1
            self._kafka = None
            return False

    # ------------------------------------------------------------------
    # Reconnect helpers
    # ------------------------------------------------------------------

    def _try_reconnect_valkey(self) -> None:
        log.info("Attempting Valkey reconnect...")
        self._valkey = _build_valkey_client()

    def _try_reconnect_kafka(self) -> None:
        log.info("Attempting Kafka alert producer reconnect...")
        self._kafka = _build_kafka_producer()
