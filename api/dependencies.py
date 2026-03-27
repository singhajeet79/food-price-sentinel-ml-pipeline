"""
api/dependencies.py
-------------------
Shared FastAPI dependencies — database session, Valkey client.

All route handlers receive these via FastAPI's dependency injection
rather than importing globals directly. This makes routes testable
(dependencies can be overridden in tests).
"""

from __future__ import annotations

import os
from typing import Generator

import redis
from dotenv import load_dotenv
from sqlalchemy.orm import Session

load_dotenv()

# ---------------------------------------------------------------------------
# Valkey (Redis-compatible)
# ---------------------------------------------------------------------------

_valkey_client = None


def get_valkey_client() -> redis.Redis:
    """Return a singleton Valkey client."""
    global _valkey_client
    if _valkey_client is None:
        _valkey_client = redis.Redis(
            host=os.getenv("VALKEY_HOST", "localhost"),
            port=int(os.getenv("VALKEY_PORT", "6379")),
            password=os.getenv("VALKEY_PASSWORD"),
            ssl=os.getenv("VALKEY_TLS", "false").lower() == "true",
            ssl_cert_reqs="required" if os.getenv("VALKEY_TLS", "false").lower() == "true" else None,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
    return _valkey_client


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

def get_db_engine():
    """Return the SQLAlchemy engine singleton."""
    from storage.db import get_engine
    return get_engine()


def get_db_session() -> Generator[Session, None, None]:
    """
    FastAPI dependency that yields a database session.

    Usage in route:
        @router.get("/something")
        def my_route(session: Session = Depends(get_db_session)):
            ...
    """
    from storage.db import get_session
    with get_session() as session:
        yield session
