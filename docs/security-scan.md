# AI Security Scan — Food Price Sentinel

## Overview

Every pull request targeting `main` or `develop` automatically triggers an
AI-powered security analysis using Claude. Results are posted as a PR comment
within ~60 seconds of opening or updating a PR.

---

## How It Works

```
PR opened / updated
        │
        ▼
Fetch changed files (GitHub API)
        │
        ▼  (skip binary, lockfiles, large diffs)
For each Python / config / YAML file
        │
        ▼
Claude analyzes the diff for vulnerabilities
        │
        ▼
Aggregate findings by severity
        │
        ▼
Post / update PR comment with report
        │
        ▼
Block merge if CRITICAL or HIGH found
```

---

## What Gets Scanned

### Files included
- All `.py`, `.yml`, `.yaml`, `.json`, `.env.example`, `.jsx` changes
- Kubernetes manifests (`k8s/`)
- GitHub Actions workflows (`.github/`)
- Dockerfile and docker-compose

### Files skipped automatically
- Binary files (images, fonts, model artifacts `.pkl`)
- Dependency lockfiles (`package-lock.json`, `poetry.lock`, `requirements.txt`)
- Files with no diff (renamed/moved only)
- Diffs exceeding 8,000 characters are truncated before analysis

---

## Project-Specific Security Checks

In addition to OWASP Top 10, the scanner is tuned for this codebase:

| Check | What it looks for |
|---|---|
| Aiven credential leaks | Connection strings, passwords in logs or exception messages |
| Kafka SSL weakening | Cleartext fallback, disabled cert validation |
| Valkey password exposure | `VALKEY_PASSWORD` in logs, hardcoded in source |
| PostgreSQL connection strings | Embedded credentials in `POSTGRES_URL` |
| Pydantic validation weakening | Removing `gt`/`ge`/`le` field bounds, removing validators |
| Anomaly threshold manipulation | Changes to `ANOMALY_THRESHOLD` or scoring logic that suppress alerts |
| Model path traversal | Unsafe file path construction in `detection/model.py` |
| API auth gaps | Unprotected FastAPI routes on sensitive endpoints |
| FX API key exposure | Hardcoded keys in `ingestion/producer_*.py` |

---

## Severity Levels

| Level | Meaning | Merge policy |
|---|---|---|
| 🔴 Critical | Immediate exploitability, credential exposure | **Block** |
| 🟠 High | Significant vulnerability, likely exploitable | **Block** |
| 🟡 Medium | Potential vulnerability, requires context | Review required |
| 🔵 Low | Minor issue, defence-in-depth | Notes only |
| ⚪ Info | Best practice suggestion | Informational |

---

## PR Comment Format

The scanner posts a comment structured as:

```
🔐 AI Security Scan Report

PR #42 · 12 files scanned · 3 skipped

Summary:
  🔴 Critical | 0
  🟠 High     | 1
  🟡 Medium   | 2
  🔵 Low      | 1
  ⚪ Info     | 0

Overall Recommendation:
  ⚠️ Needs Fixes — High severity issues should be addressed before merging.

Findings:
  [collapsible sections per file]
```

The comment is **updated in-place** on subsequent pushes to the same PR —
no duplicate comments are created.

---

## Setup Requirements

### Repository secrets

Two secrets must be configured in **Settings → Secrets and variables → Actions**:

| Secret | Value |
|---|---|
| `ANTHROPIC_API_KEY` | Your Anthropic API key from console.anthropic.com |
| `GITHUB_TOKEN` | Automatically provided by GitHub Actions — no manual setup needed |

### Permissions

The workflow requires:
- `contents: read` — to checkout the PR code
- `pull-requests: write` — to post the PR comment

These are declared in the workflow file and require no additional configuration.

---

## Workflow File

Location: `.github/workflows/security-scan.yml`

Key design decisions:

**Concurrency control** — uses `concurrency: group: security-scan-{PR_NUMBER}`
so rapid successive pushes cancel the in-progress scan and only run the latest.
This prevents redundant API calls and comment spam.

**Idempotency** — the script searches for an existing comment starting with
`<!-- food-price-sentinel-security-scan -->` before posting. If found, it
updates that comment. This means re-running the scan never creates duplicates.

**Non-blocking exit** — the Python script always exits 0. A separate step
(`check-scan-outcome`) reads the `block_merge` output and exits 1 if
CRITICAL or HIGH findings exist. This separation means the PR comment is
always posted even when blocking.

---

## Enhancements — E1 (Parked)

The following improvements are logged as **Enhancement-E1** for future work:

| ID | Enhancement |
|---|---|
| E1-SEC-01 | SARIF output upload to GitHub Security tab (`upload-sarif` action) |
| E1-SEC-02 | Dependency vulnerability scan via `pip-audit` before AI analysis |
| E1-SEC-03 | Secret detection pre-scan using `trufflehog` or `detect-secrets` |
| E1-SEC-04 | Scan only files in changed paths (skip unmodified modules) |
| E1-SEC-05 | Configurable severity threshold per branch (stricter for `main`) |
| E1-SEC-06 | Slack notification when CRITICAL findings block a PR |
| E1-SEC-07 | Weekly full-repo scan (not just PR diffs) on a schedule |
| E1-SEC-08 | False-positive suppression via `.security-ignore` config file |

---

## Other Project Enhancements — E1 (Parked)

| ID | Area | Description |
|---|---|---|
| E1-01 | Ingestion | Real FAO / World Bank / USDA API integrations (replace SYNTHETIC feed) |
| E1-02 | Ingestion | Live FX rate provider integration (openexchangerates / ECB) |
| E1-03 | Alerting | Slack webhook outbound notification on CRITICAL alerts |
| E1-04 | Alerting | Email notification via SendGrid / SES |
| E1-05 | MLOps | Wire `detection/scheduler.py` into consumer startup |
| E1-06 | MLOps | A/B live scoring harness — two models scoring in parallel |
| E1-07 | Storage | Persist raw `price_events` from Kafka consumer |
| E1-08 | Storage | Write `consumer_lag_log` from lag tracker (currently stub) |
| E1-09 | Dashboard | Commodity filter — click legend to show/hide timeline lines |
| E1-10 | Dashboard | Paginated recent events feed |
| E1-11 | Dashboard | Mobile-responsive layout |
| E1-12 | Testing | pytest suite — 80% coverage target for `processing/` and `detection/` |
| E1-13 | Infra | Multi-region Kafka topics (beyond `do-sgp`) |
| E1-14 | Infra | Aiven service auto-restart on inactivity (free tier) |
| E1-15 | Data | Geopolitical event tagging UI and API endpoints |

---


---

*Last updated: 2026-03-30 | Food Price Sentinel v1.3.0*
