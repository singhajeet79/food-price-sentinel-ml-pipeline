# Food Price Sentinel — CLAUDE.md

## Project Overview

**Food Price Sentinel** is a real-time, proactive food inflation monitoring system. Traditional price reports are reactive—published weeks after a spike has already hit consumers. This project flips that model: a streaming MLOps pipeline detects anomalies and predicts short-term price shocks *as they form*, driven by upstream signals like energy prices and fertilizer supply chain disruptions.

---

## Problem Statement

- Food price reports are published with a 2–6 week lag.
- By the time analysts react, the damage to supply chains and consumer budgets has already occurred.
- No existing consumer-facing tool correlates **upstream commodity signals** (energy, fertilizer) with **downstream food price anomalies** in real time.

---

## Solution Architecture

An end-to-end **MLOps streaming pipeline** with four layers:

```
[Data Ingestion]        →  Aiven for Apache Kafka
[Feature Engineering]   →  Rolling-window processors (Python consumers)
[Anomaly Detection]     →  Isolation Forest / Z-score model
[Alerting & Storage]    →  Aiven for Valkey (cache) + Aiven for PostgreSQL (log)
```

### Data Flow

1. **Ingest**: Raw price feeds (food commodities, energy, fertilizer indices) are produced into Kafka topics at configurable intervals.
2. **Process**: Kafka consumers compute rolling-window features (7-day avg, 30-day std dev, momentum, correlation with energy lag).
3. **Detect**: Processed feature vectors are scored by an anomaly detection model. Scores above a threshold trigger an alert event.
4. **Alert**: Alert payloads are written to Valkey with a short TTL for instant user-facing lookups.
5. **Log**: Every event (raw + processed + score) is persisted to PostgreSQL for audit trails, dashboarding, and model retraining.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Message Broker | Aiven for Apache Kafka | Real-time data streaming |
| Cache / Alerts | Aiven for Valkey (Redis-compatible) | Sub-millisecond alert retrieval |
| Database | Aiven for PostgreSQL | Event logging, retraining data |
| ML Model | scikit-learn (Isolation Forest) | Anomaly detection |
| Pipeline Runtime | Python 3.11+ | Kafka consumers, feature engineering |
| API Layer | FastAPI | REST endpoints for dashboard / alerts |
| Frontend | React + Recharts | Real-time dashboard UI |
| Orchestration | Docker Compose | Local dev; Kubernetes for prod |
| CI/CD | GitHub Actions | Lint, test, deploy pipeline |

---

## Repository Structure

```
food-price-sentinel/
├── CLAUDE.md                  # ← You are here
├── README.md
├── docker-compose.yml
├── .env.example
│
├── ingestion/                 # Kafka producers
│   ├── producer_food.py       # Food commodity price feeds
│   ├── producer_energy.py     # Energy index feeds
│   └── producer_fertilizer.py # Fertilizer supply feeds
│
├── processing/                # Kafka consumers + feature engineering
│   ├── consumer.py            # Main consumer loop
│   ├── features.py            # Rolling-window feature computation
│   └── schemas.py             # Pydantic data models
│
├── detection/                 # Anomaly detection model
│   ├── model.py               # Isolation Forest wrapper
│   ├── train.py               # Offline training script
│   ├── score.py               # Online scoring
│   └── models/                # Serialized model artifacts (.pkl)
│
├── alerting/                  # Alert writing to Valkey
│   ├── alert_writer.py        # Writes alert payloads to Valkey
│   └── alert_schema.py        # Alert data structure
│
├── storage/                   # PostgreSQL persistence
│   ├── db.py                  # SQLAlchemy engine + session
│   ├── models.py              # ORM table definitions
│   └── migrations/            # Alembic migration files
│
├── api/                       # FastAPI backend
│   ├── main.py
│   ├── routes/
│   │   ├── alerts.py          # GET /alerts (reads Valkey)
│   │   ├── history.py         # GET /history (reads PostgreSQL)
│   │   └── health.py          # GET /health
│   └── dependencies.py
│
└── frontend/                  # React dashboard
    ├── src/
    │   ├── App.jsx
    │   ├── components/
    │   │   ├── PriceChart.jsx
    │   │   ├── AlertBanner.jsx
    │   │   └── CommodityCard.jsx
    │   └── hooks/
    │       └── useAlerts.js
    └── package.json
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your Aiven credentials:

```env
# Kafka
KAFKA_BOOTSTRAP_SERVERS=<aiven-kafka-host>:<port>
KAFKA_SECURITY_PROTOCOL=SSL
KAFKA_SSL_CAFILE=ca.pem
KAFKA_SSL_CERTFILE=service.cert
KAFKA_SSL_KEYFILE=service.key

# Topics
KAFKA_TOPIC_FOOD=food-prices
KAFKA_TOPIC_ENERGY=energy-prices
KAFKA_TOPIC_FERTILIZER=fertilizer-supply
KAFKA_TOPIC_ALERTS=price-alerts

# Valkey (Redis-compatible)
VALKEY_HOST=<aiven-valkey-host>
VALKEY_PORT=<port>
VALKEY_PASSWORD=<password>
VALKEY_TLS=true
VALKEY_ALERT_TTL=3600          # seconds

# PostgreSQL
POSTGRES_URL=postgresql://<user>:<password>@<host>:<port>/<db>?sslmode=require

# Model
ANOMALY_THRESHOLD=0.65             # Isolation Forest contamination score
ROLLING_WINDOW_DAYS=7
LONG_WINDOW_DAYS=30
STALE_DATA_THRESHOLD_MINUTES=180   # Events older than this are flagged is_stale=TRUE
ALERT_COOLDOWN_SECONDS=21600       # 6 hours — dedup window per commodity/region/severity
SEASONAL_LOOKBACK_YEARS=3          # Years of history used to compute seasonal index

# FX
FX_UPDATE_INTERVAL_MINUTES=60      # How often to refresh exchange rates
FX_PROVIDER=openexchangerates      # or 'ecb', 'fixer'
FX_API_KEY=<your-fx-api-key>

# Data Sources
SOURCE_PRIORITY_FAO=1
SOURCE_PRIORITY_WORLDBANK=2
SOURCE_PRIORITY_USDA=3
```

---

## Kafka Topic Schema

### `food-prices`
```json
{
  "commodity": "wheat",
  "price_usd": 312.50,
  "price_local": 25930.00,
  "local_currency": "INR",
  "fx_rate_to_usd": 83.01,
  "unit_raw": "per_cwt",
  "unit_normalized": "per_tonne",
  "unit_conversion_factor": 22.0462,
  "source": "FAO",
  "source_priority": 1,
  "data_as_of": "2026-03-20T08:00:00Z",
  "ingested_at": "2026-03-20T10:00:00Z",
  "latency_minutes": 120,
  "region": "global",
  "season": "spring",
  "day_of_year": 79
}
```

**Field notes:**
- `price_usd` — canonical price used for model features and cross-commodity correlation.
- `price_local` / `local_currency` / `fx_rate_to_usd` — enables region-specific anomaly detection on top of global USD signals.
- `unit_raw` / `unit_normalized` / `unit_conversion_factor` — all prices normalized to `per_tonne` before feature engineering; raw unit preserved for auditability.
- `source_priority` — integer rank (1 = highest trust). If two sources report conflicting prices for the same commodity/region/timestamp, the lower integer wins.
- `data_as_of` vs `ingested_at` — `data_as_of` is when the source measured the price; `ingested_at` is pipeline receipt time. `latency_minutes` is derived. Staleness threshold: if `latency_minutes > STALE_DATA_THRESHOLD_MINUTES`, event is flagged and excluded from model scoring.
- `season` / `day_of_year` — used for seasonal decomposition in feature engineering to reduce false positives from expected harvest-cycle price movements.

### `price-alerts`
```json
{
  "alert_id": "uuid-v4",
  "commodity": "wheat",
  "region": "global",
  "anomaly_score": 0.82,
  "severity": "HIGH",
  "current_price_usd": 412.50,
  "current_price_local": 34238.25,
  "local_currency": "INR",
  "baseline_price_usd": 312.50,
  "pct_deviation_usd": 31.9,
  "pct_deviation_local": 29.4,
  "contributing_factors": ["energy_spike", "fertilizer_shortage", "export_ban"],
  "geopolitical_tags": ["russia_ukraine", "india_export_restriction"],
  "is_seasonal_adjusted": true,
  "dedup_key": "wheat:global:HIGH",
  "cooldown_until": "2026-03-20T16:05:00Z",
  "triggered_at": "2026-03-20T10:05:00Z",
  "ttl_seconds": 3600
}
```

**Field notes:**
- `dedup_key` — composite of `commodity:region:severity`. Valkey checks this key before writing; if it exists and `cooldown_until` is in the future, the alert is suppressed. Prevents alert fatigue during sustained shocks.
- `geopolitical_tags` — freeform tags from an external events feed or manual annotation. Not used in v1 model features but logged for post-hoc analysis and v2 training data.
- `is_seasonal_adjusted` — boolean flag indicating whether seasonal decomposition was applied before anomaly scoring. Alerts where this is `false` should be treated with lower confidence.
- `pct_deviation_local` — deviation in local currency terms, which can differ significantly from USD deviation due to FX movements.

---

## PostgreSQL Schema

```sql
-- Raw price events
CREATE TABLE price_events (
    id                      BIGSERIAL PRIMARY KEY,
    commodity               TEXT NOT NULL,
    price_usd               NUMERIC(12, 4) NOT NULL,
    price_local             NUMERIC(12, 4),
    local_currency          CHAR(3),
    fx_rate_to_usd          NUMERIC(10, 6),
    unit_raw                TEXT,
    unit_normalized         TEXT DEFAULT 'per_tonne',
    unit_conversion_factor  NUMERIC(10, 6) DEFAULT 1.0,
    source                  TEXT,
    source_priority         SMALLINT DEFAULT 99,
    region                  TEXT,
    season                  TEXT,
    day_of_year             SMALLINT,
    data_as_of              TIMESTAMPTZ,
    ingested_at             TIMESTAMPTZ DEFAULT NOW(),
    latency_minutes         INTEGER,
    is_stale                BOOLEAN DEFAULT FALSE
);

-- Computed features per event
CREATE TABLE feature_vectors (
    id                      BIGSERIAL PRIMARY KEY,
    price_event_id          BIGINT REFERENCES price_events(id),
    rolling_7d_avg          NUMERIC(12, 4),
    rolling_30d_std         NUMERIC(12, 4),
    momentum                NUMERIC(8, 4),
    energy_lag_corr         NUMERIC(6, 4),
    fertilizer_index_delta  NUMERIC(8, 4),
    seasonal_index          NUMERIC(6, 4),   -- ratio of current price to expected seasonal price
    seasonal_adjusted_price NUMERIC(12, 4),  -- price_usd ÷ seasonal_index
    is_seasonal_adjusted    BOOLEAN DEFAULT FALSE,
    computed_at             TIMESTAMPTZ DEFAULT NOW()
);

-- Anomaly detections
CREATE TABLE anomaly_log (
    id              BIGSERIAL PRIMARY KEY,
    feature_id      BIGINT REFERENCES feature_vectors(id),
    anomaly_score   NUMERIC(6, 4),
    severity        TEXT,
    model_version   TEXT,
    alerted         BOOLEAN DEFAULT FALSE,
    suppressed      BOOLEAN DEFAULT FALSE,   -- TRUE if dedup cooldown prevented alert
    detected_at     TIMESTAMPTZ DEFAULT NOW()
);

-- Geopolitical event tags (many-to-many with anomaly_log)
CREATE TABLE geopolitical_events (
    id          BIGSERIAL PRIMARY KEY,
    tag         TEXT UNIQUE NOT NULL,        -- e.g. 'russia_ukraine', 'india_export_ban'
    description TEXT,
    started_at  TIMESTAMPTZ,
    ended_at    TIMESTAMPTZ
);

CREATE TABLE anomaly_geopolitical_tags (
    anomaly_id  BIGINT REFERENCES anomaly_log(id),
    event_id    BIGINT REFERENCES geopolitical_events(id),
    PRIMARY KEY (anomaly_id, event_id)
);

-- Operational metrics: Kafka consumer lag
CREATE TABLE consumer_lag_log (
    id              BIGSERIAL PRIMARY KEY,
    consumer_group  TEXT NOT NULL,
    topic           TEXT NOT NULL,
    partition       SMALLINT NOT NULL,
    lag_messages    BIGINT,
    recorded_at     TIMESTAMPTZ DEFAULT NOW()
);
```

**Index recommendations:**
```sql
CREATE INDEX idx_price_events_commodity_time ON price_events(commodity, data_as_of DESC);
CREATE INDEX idx_price_events_region ON price_events(region);
CREATE INDEX idx_price_events_stale ON price_events(is_stale) WHERE is_stale = TRUE;
CREATE INDEX idx_anomaly_log_detected ON anomaly_log(detected_at DESC);
CREATE INDEX idx_consumer_lag_time ON consumer_lag_log(recorded_at DESC);
```

---

## ML Model

### Algorithm: Isolation Forest
- **Why**: Unsupervised, no labeled anomaly data required. Works well for sparse, high-dimensional tabular data.
- **Input features**: `[seasonal_adjusted_price, rolling_7d_avg, rolling_30d_std, momentum, energy_lag_corr, fertilizer_index_delta, day_of_year_sin, day_of_year_cos]`
- **Threshold**: Configurable via `ANOMALY_THRESHOLD`. Default `0.65` (65th percentile of anomaly scores triggers alert).

### Seasonality Handling
Food prices have strong annual cycles (harvest, monsoon, planting windows). Without seasonal adjustment, the model generates high false-positive rates during predictable Q3/Q4 price movements.

- **Seasonal index**: For each `(commodity, day_of_year)` pair, compute the ratio of the 3-year historical average price to the annual mean. This index is stored in a lookup table and applied as a divisor to `price_usd` before feature engineering.
- **Cyclical encoding**: `day_of_year` is encoded as `sin(2π × doy/365)` and `cos(2π × doy/365)` to preserve cyclical continuity (day 365 → day 1) as model features.
- **Season tag**: Categorical `season` field (`spring`, `summer`, `autumn`, `winter`) for interpretability in alerts, not used directly as a model feature.

### Data Quality Gates (pre-scoring)
Before a feature vector is scored, it must pass all gates. Failed gates are logged but not scored:
1. `is_stale = FALSE` — data freshness check (configurable via `STALE_DATA_THRESHOLD_MINUTES`)
2. Source conflict resolution: if two sources report for the same `(commodity, region, data_as_of)`, the lower `source_priority` integer wins; the other is discarded
3. Unit normalization: `price_usd` must be in `per_tonne` equivalent before ingestion (conversion applied at producer level)
4. Rolling window completeness: at least 5 of the last 7 days must have valid data points; otherwise the 7-day feature is flagged as `low_confidence`

### Alert Deduplication (Valkey)
During a sustained price shock, the model will score anomalies on every new message. To prevent alert fatigue:
- Before writing an alert, check Valkey for key `alert_dedup:{commodity}:{region}:{severity}`
- If the key exists, suppress the alert and set `suppressed = TRUE` in `anomaly_log`
- If the key does not exist, write the alert and set the key with TTL = `ALERT_COOLDOWN_SECONDS` (default: 21600 = 6 hours)

### Retraining Loop
1. Every 24 hours, a cron job pulls the last 90 days of `feature_vectors` from PostgreSQL.
2. A new model is trained and evaluated (AUC-ROC on held-out historical spikes).
3. If the new model outperforms the current one, it replaces the serialized `.pkl` and the consumer is hot-reloaded.

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Service health check |
| `GET` | `/alerts/active` | Fetch current alerts from Valkey |
| `GET` | `/alerts/{commodity}` | Alerts for a specific commodity |
| `GET` | `/history` | Paginated anomaly log from PostgreSQL |
| `GET` | `/history/{commodity}` | Historical anomaly trend for a commodity |

---

## Development Quickstart

```bash
# 1. Clone and configure
git clone https://github.com/your-org/food-price-sentinel
cd food-price-sentinel
cp .env.example .env
# → Fill in Aiven credentials in .env

# 2. Install Python deps
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Run database migrations
alembic upgrade head

# 4. Train initial model on historical data
python detection/train.py --days 90

# 5. Start the full stack
docker-compose up

# Services started:
#   - Kafka producer(s)       → feeds raw price data
#   - Feature processor       → rolling-window consumer
#   - Anomaly scorer          → detection + alert writer
#   - FastAPI                 → http://localhost:8000
#   - React dashboard         → http://localhost:3000
```

---

## Roadmap

### Phase 1 — MVP (Weeks 1–4)
- [ ] Kafka producers for food, energy, fertilizer data
- [ ] Feature engineering consumer
- [ ] Isolation Forest model + offline training script
- [ ] Valkey alert writer
- [ ] PostgreSQL logging
- [ ] FastAPI `/alerts` and `/health` endpoints

### Phase 2 — Dashboard (Weeks 5–7)
- [ ] React real-time price chart (polling `/alerts/active`)
- [ ] Commodity cards with severity badges
- [ ] Historical trend view

### Phase 3 — MLOps (Weeks 8–10)
- [ ] Automated daily retraining cron job
- [ ] Model versioning and rollback
- [ ] Drift detection alerts
- [ ] A/B model evaluation harness

### Phase 4 — Production (Weeks 11–12)
- [ ] Kubernetes manifests
- [ ] GitHub Actions CI/CD
- [ ] Alerting integrations (email, Slack webhook)
- [ ] Multi-region Kafka topics

---

## Conventions

- **Python**: Black formatter, isort, type hints required on all public functions.
- **Commits**: Conventional Commits (`feat:`, `fix:`, `chore:`, `docs:`).
- **Branches**: `main` (prod), `develop` (integration), `feature/*`, `fix/*`.
- **Tests**: pytest, minimum 80% coverage on `processing/` and `detection/`.
- **Secrets**: Never commit `.env`. Use `.env.example` for documentation only.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Message broker | Kafka (Aiven managed) | Battle-tested for high-throughput streaming; Aiven removes ops burden |
| Cache layer | Valkey over Redis | Drop-in Redis-compatible, open-source, Aiven managed |
| Anomaly model | Isolation Forest | Unsupervised, no labeled training data needed for v1 |
| Feature window | 7-day + 30-day | Balances noise vs. signal; matches agricultural price cycle |
| API framework | FastAPI | Async-native, automatic OpenAPI docs, Pydantic integration |
| DB migrations | Alembic | Standard SQLAlchemy companion, version-controlled schema |
| Price denomination | USD (canonical) + local currency | USD matches all upstream commodity sources; local currency enables region-specific alerts; FX rate stored on every event |
| Unit normalization | Normalize to `per_tonne` at producer | Prevents silent cross-commodity correlation errors downstream; raw unit preserved for audit |
| Seasonality | Seasonal index + cyclical sin/cos encoding | Eliminates false positives from harvest-cycle price movements; sin/cos preserves day 365 → day 1 continuity |
| Data freshness | `data_as_of` vs `ingested_at` + staleness gate | Prevents stale data from being scored as live; latency tracked for operational visibility |
| Source conflicts | `source_priority` integer hierarchy | Deterministic resolution when multiple sources report conflicting prices for same commodity/region/timestamp |
| Alert deduplication | Valkey TTL key per `commodity:region:severity` | Prevents alert fatigue during sustained multi-day price shocks |
| Geopolitical signals | Event tag table + post-hoc annotation | Not enough labeled data for v1 model feature; preserved for v2 training data |
| Consumer lag | Dedicated `consumer_lag_log` table | Surfaces when "real-time" alerts are actually delayed; separates ML health from operational health |

---

*Last updated: 2026-03-20 (rev 2 — dual-currency, seasonality, unit normalization, data quality gates, alert dedup, geopolitical tags, consumer lag) | Maintainer: Food Price Sentinel Team*
