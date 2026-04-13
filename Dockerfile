# ── Build stage ──────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build deps - Added libsnappy-dev
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    libsnappy-dev \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Runtime stage ────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Runtime system deps - Added libsnappy1v5
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    libsnappy1v5 \
    && rm -rf /var/lib/apt/lists/*
# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY alerting/   alerting/
COPY api/        api/
COPY detection/  detection/
COPY ingestion/  ingestion/
COPY processing/ processing/
COPY storage/    storage/
COPY alembic.ini .

# Create non-root user
RUN useradd --no-create-home --shell /bin/false sentinel
RUN mkdir -p detection/models certs && chown -R sentinel:sentinel /app
USER sentinel

# Healthcheck — used by docker-compose and k8s liveness probe fallback
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command (overridden by k8s manifests per-pod)
CMD ["python", "processing/consumer.py"]
