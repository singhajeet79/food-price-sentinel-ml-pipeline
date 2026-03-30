"""
api/routes/health.py
--------------------
GET /health — service health check.

Checks all three backends (Kafka, Valkey, PostgreSQL) and returns
a structured status response. Used by:
  - Load balancers / Kubernetes liveness probes
  - Dashboard "system status" indicator
  - Monitoring alerting (Prometheus, Grafana)

Response codes:
  200 — all backends healthy
  207 — partial degradation (some backends down, API still serving)
  503 — critical backends down (PostgreSQL + Valkey both unavailable)
"""

from __future__ import annotations

import os
import time
from typing import Optional

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

router = APIRouter()


class BackendStatus(BaseModel):
    status: str  # "ok" | "degraded" | "down"
    latency_ms: Optional[float] = None
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str  # "healthy" | "degraded" | "unhealthy"
    version: str = "1.0.0"
    backends: dict[str, BackendStatus]


def _check_valkey() -> BackendStatus:
    try:
        from api.dependencies import get_valkey_client

        client = get_valkey_client()
        start = time.monotonic()
        client.ping()
        latency_ms = (time.monotonic() - start) * 1000
        return BackendStatus(status="ok", latency_ms=round(latency_ms, 2))
    except Exception as exc:
        return BackendStatus(status="down", error=str(exc))


_pg_cache: dict = {"status": None, "ts": 0.0}
_PG_CACHE_TTL = 30.0  # seconds


def _check_postgres() -> BackendStatus:
    """PostgreSQL health check with 30s cache."""
    now = time.monotonic()
    if _pg_cache["status"] is not None and (now - _pg_cache["ts"]) < _PG_CACHE_TTL:
        return _pg_cache["status"]
    try:
        from sqlalchemy import text
        from api.dependencies import get_db_engine

        engine = get_db_engine()
        start = time.monotonic()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        latency_ms = (time.monotonic() - start) * 1000
        result = BackendStatus(status="ok", latency_ms=round(latency_ms, 2))
    except Exception as exc:
        result = BackendStatus(status="down", error=str(exc))
    _pg_cache["status"] = result
    _pg_cache["ts"] = time.monotonic()
    return result


# Kafka check cache — avoids 2-3s broker round-trip on every /health call
_kafka_cache: dict = {"status": None, "ts": 0.0}
_KAFKA_CACHE_TTL = 60.0  # seconds


def _check_kafka() -> BackendStatus:
    """
    Lightweight Kafka check with 60s result cache.

    The KafkaAdminClient round-trip to Aiven takes 2-3s — too slow to
    run on every /health poll. We cache the last result and only re-check
    after TTL expires.
    """
    now = time.monotonic()
    if (
        _kafka_cache["status"] is not None
        and (now - _kafka_cache["ts"]) < _KAFKA_CACHE_TTL
    ):
        return _kafka_cache["status"]

    try:
        from kafka import KafkaAdminClient

        start = time.monotonic()
        admin = KafkaAdminClient(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            ssl_cafile=os.getenv("KAFKA_SSL_CAFILE"),
            ssl_certfile=os.getenv("KAFKA_SSL_CERTFILE"),
            ssl_keyfile=os.getenv("KAFKA_SSL_KEYFILE"),
            request_timeout_ms=5000,
        )
        admin.list_topics()
        admin.close()
        latency_ms = (time.monotonic() - start) * 1000
        result = BackendStatus(status="ok", latency_ms=round(latency_ms, 2))
    except Exception as exc:
        result = BackendStatus(status="degraded", error=str(exc))

    _kafka_cache["status"] = result
    _kafka_cache["ts"] = time.monotonic()
    return result


@router.get("/health", response_model=HealthResponse)
def health_check():
    """
    Check liveness of all pipeline backends.

    Returns 200 if all healthy, 207 if partially degraded,
    503 if critically degraded (Valkey + PostgreSQL both down).
    """
    backends = {
        "valkey": _check_valkey(),
        "postgresql": _check_postgres(),
        "kafka": _check_kafka(),
    }

    down = [k for k, v in backends.items() if v.status == "down"]
    degraded = [k for k, v in backends.items() if v.status == "degraded"]

    if not down and not degraded:
        overall = "healthy"
        status_code = 200
    elif "valkey" in down and "postgresql" in down:
        overall = "unhealthy"
        status_code = 503
    else:
        overall = "degraded"
        status_code = 207

    response = HealthResponse(
        status=overall,
        backends=backends,
    )
    return JSONResponse(content=response.model_dump(), status_code=status_code)


@router.get("/drift")
def get_drift_status():
    """
    Return the latest model drift signal from Valkey.

    Written by detection/drift.py every DRIFT_CHECK_INTERVAL_HOURS.
    Returns the most recent drift result across all model versions.

    Signal values: STABLE | DRIFT | STALE | UNKNOWN
    """
    import json

    try:
        from api.dependencies import get_valkey_client

        client = get_valkey_client()

        # Scan for all drift keys — pick the most recently checked
        cursor = 0
        drift_keys = []
        while True:
            cursor, keys = client.scan(cursor=cursor, match="drift:*", count=50)
            drift_keys.extend(keys)
            if cursor == 0:
                break

        if not drift_keys:
            return {
                "signal": "UNKNOWN",
                "reason": "No drift data available. Run detection/drift.py to generate.",
                "model_version": None,
                "checked_at": None,
            }

        # Parse all and return the most recently checked
        results = []
        for key in drift_keys:
            raw = client.get(key)
            if raw:
                try:
                    results.append(json.loads(raw))
                except json.JSONDecodeError:
                    continue

        if not results:
            return {
                "signal": "UNKNOWN",
                "reason": "Drift keys exist but values are empty.",
            }

        latest = max(results, key=lambda r: r.get("checked_at", ""))
        return latest

    except Exception as exc:
        return {
            "signal": "UNKNOWN",
            "reason": f"Drift check unavailable: {exc}",
            "model_version": None,
            "checked_at": None,
        }
