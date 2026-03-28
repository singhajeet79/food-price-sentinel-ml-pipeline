"""
api/main.py
-----------
FastAPI application entry point for the Food Price Sentinel API.

Mounts all route modules and configures:
  - CORS (permissive for dev; tighten for production)
  - Startup / shutdown lifecycle hooks
  - Global exception handler
  - Request logging middleware

Usage:
    uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
"""

from __future__ import annotations

import logging
import os
import time

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("api")

from api.routes import alerts, health, history  # noqa: E402
from api.dependencies import get_valkey_client, get_db_engine  # noqa: E402

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Food Price Sentinel API",
    description=(
        "Real-time food price anomaly detection pipeline. "
        "Streams anomaly alerts from Valkey and historical data from PostgreSQL."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Request timing middleware
# ---------------------------------------------------------------------------

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    duration_ms = (time.monotonic() - start) * 1000
    log.info(
        "%s %s → %d (%.1fms)",
        request.method, request.url.path,
        response.status_code, duration_ms,
    )
    return response

# ---------------------------------------------------------------------------
# Global exception handler
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    log.error("Unhandled exception: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)},
    )

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup():
    log.info("Food Price Sentinel API starting up...")
    # Warm up connections
    try:
        client = get_valkey_client()
        client.ping()
        log.info("Valkey connection OK")
    except Exception as exc:
        log.warning("Valkey not available at startup: %s", exc)

    try:
        engine = get_db_engine()
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("PostgreSQL connection OK")
    except Exception as exc:
        log.warning("PostgreSQL not available at startup: %s", exc)


@app.on_event("shutdown")
async def shutdown():
    log.info("Food Price Sentinel API shutting down.")

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(health.router,  prefix="",         tags=["health"])
app.include_router(alerts.router,  prefix="/alerts",  tags=["alerts"])
app.include_router(history.router, prefix="/history", tags=["history"])
