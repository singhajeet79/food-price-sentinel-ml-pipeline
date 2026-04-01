# 🛡️ Food-Price-Sentinel
**Real-time MLOps Pipeline for Global Commodity Price Governance**

Food-Price-Sentinel is an end-to-end streaming MLOps platform designed to detect price anomalies in global grain markets. It leverages a high-performance stack to ingest, analyze, and alert on food price deviations using unsupervised machine learning.

---

## 🚀 System Architecture

- **Ingestion Layer:** Aiven Kafka integration with managed topics.
- **Detection Layer:** **Isolation Forest** model (v1.2.0) for unsupervised anomaly detection.
- **Storage Layer:** PostgreSQL (SQLAlchemy + Alembic) for long-term price history.
- **Cache & Alerting:** Valkey (Redis-compatible) for deduplication gates and drift status tracking.
- **Dashboard:** React-based "Control Room" with Recharts for live price timelines and alert status.
- **Security:** Claude-powered AI security scanning on every Pull Request.

---

## ✨ Key Features

- **Live Anomaly Detection:** Real-time scoring using `IsolationForest` to identify market manipulation or supply chain shocks.
- **Drift Monitoring:** Integrated "Drift Signal Banner" showing `STABLE`, `DRIFT`, or `STALE` statuses based on Valkey-cached health checks.
- **Automated Governance:** Proactive AI security scans on PRs with idempotent commenting (powered by Claude).
- **Control Room Aesthetic:** A high-visibility React dashboard featuring:
  - Per-commodity colored timelines (7-color palette).
  - Dynamic Y-axis zooming for volatile commodities (e.g., Sugar price ceiling fixes).
  - Relative X-axis for real-time polling synchronization.
- **Cloud Native:** Fully containerized with Docker and ready for orchestration via Kubernetes (ConfigMaps, Kustomization, and Secret manifests included).

---

## 🛠️ Tech Stack

| Component | Technology |
| :--- | :--- |
| **Language** | Python (Functional-style), JavaScript (React) |
| **Streaming** | Aiven Kafka (Managed Service) |
| **ML Engine** | Scikit-learn (Isolation Forest), Joblib |
| **Database** | PostgreSQL, SQLAlchemy, Alembic |
| **Caching** | Valkey (via Aiven) |
| **API Framework** | FastAPI, Uvicorn |cp .env.example .env
| **DevOps/CI** | GitHub Actions, Ruff, Black, Isort, Pre-commit |
| **Infrastructure** | Docker, Kubernetes (K8s) |

---

## 📦 Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/singhajeet79/food-price-sentinel-ml-pipeline.git](https://github.com/singhajeet79/food-price-sentinel-ml-pipeline.git)
   cd food-price-sentinel
   ```
2. **Environment Configuration:**
   ```bash
   # Copy .env.example to .env and fill in your Aiven credentials:
   cp .env.example .env
   ```
3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
4. **Initialize Database:**
   ```bash
   alembic upgrade head
   ```
5. **Start the Pipeline:**
   ```bash
   # Start the consumer/detection engine
   python -m src.detection.consumer
   ```

---

## 🛡️ Security & Quality Gate

This project employs a rigorous CI/CD pipeline to ensure code quality and security:

- **Pre-commit Hooks:** Automatically runs ruff and ruff-format before every commit.
- **AI Security Scan:** Every Pull Request triggers a Claude-powered security auditor that scans for vulnerabilities and hardcoded secrets.
- **Automated Formatting:** Consistent import ordering via isort and style via black.

---

## 📈 Recent Milestones (Commit Highlights)
- **v1.2.0 Promotion:** Successful model promotion and conflict resolver fix.
- **Performance:** Cached Kafka health checks to reduce latency on polling.
- **Visualization:** Implemented /history/grouped endpoint for synchronized commodity charting.
- **Deployment:** Added full K8s manifest suite for production-grade scaling.

---

## 🤝 Contributing
We welcome contributions from the MLOps and Data Engineering community! Whether you are fixing a bug in the Isolation Forest scoring or enhancing the React dashboard.

---

Developed by Ajeet Singh — Industrializing AI for Global Food Security.

---
#### Check out [My GitHub Page](https://singhajeet79.github.io/) for more AI/MLOps/Data Engineering projects. 
**Happy Engineering!** 🚀
