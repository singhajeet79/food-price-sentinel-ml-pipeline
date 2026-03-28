"""
storage/db.py
-------------
SQLAlchemy engine and session factory for the Food Price Sentinel pipeline.

Provides:
  - get_engine()     — singleton engine (connection pool)
  - get_session()    — context manager yielding a scoped session
  - init_db()        — create all tables (dev/test only; prod uses Alembic)

Usage:
    from storage.db import get_session

    with get_session() as session:
        session.add(SomeORM(...))
        session.commit()

Environment variables:
    POSTGRES_URL   — full SQLAlchemy URL including sslmode=require
    DB_POOL_SIZE   — connection pool size (default: 5)
    DB_MAX_OVERFLOW — max overflow connections (default: 10)
    DB_ECHO        — set to '1' to log all SQL (default: off)
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Generator

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

load_dotenv()
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

POSTGRES_URL = os.getenv("POSTGRES_URL")
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "10"))
DB_ECHO = os.getenv("DB_ECHO", "0") == "1"


# ---------------------------------------------------------------------------
# Declarative base (shared by all ORM models)
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Engine singleton
# ---------------------------------------------------------------------------

_engine: Engine | None = None


def get_engine() -> Engine:
    """
    Return the singleton SQLAlchemy engine.
    Creates it on first call using POSTGRES_URL from environment.
    """
    global _engine
    if _engine is None:
        if not POSTGRES_URL:
            raise RuntimeError(
                "POSTGRES_URL environment variable is not set. "
                "Add it to your .env file."
            )
        _engine = create_engine(
            POSTGRES_URL,
            pool_size=DB_POOL_SIZE,
            max_overflow=DB_MAX_OVERFLOW,
            pool_pre_ping=True,  # test connection before checkout
            echo=DB_ECHO,
        )
        log.info(
            "SQLAlchemy engine created (pool_size=%d max_overflow=%d echo=%s)",
            DB_POOL_SIZE,
            DB_MAX_OVERFLOW,
            DB_ECHO,
        )
    return _engine


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

_SessionFactory: sessionmaker | None = None


def _get_session_factory() -> sessionmaker:
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(
            bind=get_engine(),
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
    return _SessionFactory


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Context manager that yields a SQLAlchemy Session.

    Commits on clean exit, rolls back on exception, always closes.

    Usage:
        with get_session() as session:
            session.add(row)
            session.commit()
    """
    factory = _get_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Table creation (dev / test only)
# ---------------------------------------------------------------------------


def init_db() -> None:
    """
    Create all tables defined in storage/models.py.

    For development and testing only. Production uses Alembic migrations:
        alembic upgrade head
    """
    # Import models so their metadata is registered on Base
    import storage.models  # noqa: F401

    engine = get_engine()
    Base.metadata.create_all(engine)
    log.info("All tables created (init_db).")


def health_check() -> bool:
    """Return True if the database is reachable."""
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        log.error("Database health check failed: %s", exc)
        return False
