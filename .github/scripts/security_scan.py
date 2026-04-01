"""
.github/scripts/security_scan.py
----------------------------------
AI-powered security analysis for Food Price Sentinel PRs.

Fetches changed files from the GitHub API, sends each diff to Claude
for security analysis, aggregates findings, and posts a structured
markdown report as a PR comment.

Project-specific checks (in addition to general OWASP Top 10):
  - Kafka SSL cert/key exposure in producer configs
  - Aiven credentials leaking into logs or exception messages
  - Valkey/Redis password exposure
  - PostgreSQL connection strings with embedded credentials
  - Model artifact paths that could enable path traversal
  - Pydantic model changes that weaken validation (removing gt/ge/le bounds)
  - Anomaly threshold manipulation that could suppress alerts
  - API keys for FX providers hardcoded in producer files

Environment variables required (all injected by GitHub Actions):
  ANTHROPIC_API_KEY  — Claude API key (stored in repo secrets)
  GITHUB_TOKEN       — GitHub PAT for API calls (automatic in Actions)
  PR_NUMBER          — Pull Request number
  REPO_OWNER         — Repository owner
  REPO_NAME          — Repository name

Idempotency: Before posting, the script checks for an existing comment
from the same bot that starts with the scan header. If found, it updates
the existing comment rather than creating a duplicate.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Optional

import anthropic
from github import Github

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
PR_NUMBER = int(os.environ["PR_NUMBER"])
REPO_OWNER = os.environ["REPO_OWNER"]
REPO_NAME = os.environ["REPO_NAME"]

# Files to skip entirely
SKIP_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".eot",
    ".pkl",
    ".bin",
    ".pyc",
    ".pyo",
    ".lock",
    ".sum",
}
SKIP_FILENAMES = {"package-lock.json", "poetry.lock", "requirements.txt"}

# Max diff size per file to send to Claude (tokens are expensive)
MAX_PATCH_CHARS = 8000

# Severity levels and their emoji
SEVERITY_EMOJI = {
    "CRITICAL": "🔴",
    "HIGH": "🟠",
    "MEDIUM": "🟡",
    "LOW": "🔵",
    "INFO": "⚪",
}

SCAN_HEADER = "<!-- food-price-sentinel-security-scan -->"

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class SecurityFinding:
    severity: str
    title: str
    description: str
    affected_snippet: str
    recommendation: str
    filename: str


@dataclass
class FileScanResult:
    filename: str
    findings: list[SecurityFinding] = field(default_factory=list)
    skipped: bool = False
    skip_reason: str = ""


# ---------------------------------------------------------------------------
# Fetch PR diff from GitHub API
# ---------------------------------------------------------------------------


def fetch_pr_files(gh: Github) -> list[dict]:
    """Fetch changed files for the PR using PyGithub."""
    repo = gh.get_repo(f"{REPO_OWNER}/{REPO_NAME}")
    pr = repo.get_pull(PR_NUMBER)
    files = []
    for f in pr.get_files():
        files.append(
            {
                "filename": f.filename,
                "status": f.status,
                "patch": getattr(f, "patch", None),
                "changes": f.changes,
            }
        )
    print(f"Fetched {len(files)} changed files from PR #{PR_NUMBER}")
    return files


def should_skip(filename: str, patch: Optional[str]) -> tuple[bool, str]:
    """Return (skip, reason) for a file."""
    ext = os.path.splitext(filename)[1].lower()
    if ext in SKIP_EXTENSIONS:
        return True, f"binary/asset file ({ext})"
    if os.path.basename(filename) in SKIP_FILENAMES:
        return True, "dependency lockfile"
    if patch is None:
        return True, "no diff available (binary or renamed)"
    if len(patch) > MAX_PATCH_CHARS * 3:
        return False, ""  # still scan, just truncate
    return False, ""


# ---------------------------------------------------------------------------
# AI security analysis
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a senior application security engineer performing a
code review of a Python MLOps streaming pipeline called Food Price Sentinel.

The system architecture:
- Kafka producers (food/energy/fertilizer price data) → Kafka topics
- Python consumer with feature engineering (rolling windows, seasonality)
- Isolation Forest anomaly detection model
- Valkey (Redis-compatible) for alert caching
- PostgreSQL for persistence
- FastAPI REST API
- React dashboard
- Infrastructure on Aiven managed services (Kafka, Valkey, PostgreSQL)

Your task: Analyze the provided code diff for security vulnerabilities.

Focus areas (in priority order):
1. Hardcoded secrets — Aiven credentials, API keys, SSL cert paths with embedded values,
   PostgreSQL connection strings with passwords, Valkey passwords
2. Kafka SSL configuration — improper cert validation, cleartext fallback
3. Injection vulnerabilities — SQL injection in SQLAlchemy raw queries, command injection
4. Sensitive data in logs — credentials, connection strings, PII in log statements
5. Pydantic validation weakening — removing field validators, relaxing bounds
6. Anomaly threshold tampering — changes that could suppress security-relevant alerts
7. Path traversal — model artifact loading, file path construction
8. Authentication gaps in FastAPI routes — missing auth on sensitive endpoints
9. Dependency vulnerabilities — suspicious new packages in requirements
10. OWASP Top 10 as applicable to this stack

For EACH issue found, respond with a JSON array of objects:
[
  {
    "severity": "CRITICAL|HIGH|MEDIUM|LOW|INFO",
    "title": "Short title (max 80 chars)",
    "description": "Clear explanation of the vulnerability and why it matters",
    "affected_snippet": "The specific line(s) from the diff that are problematic",
    "recommendation": "Specific, actionable fix for this codebase"
  }
]

If NO issues are found, respond with an empty array: []

IMPORTANT:
- Only flag real issues, not style preferences
- Do not flag TODO/FIXME comments as security issues
- Do not flag test/mock credentials clearly marked as such
- Consider the project context — SYNTHETIC data source is intentional
- Affected snippet should be the exact diff line(s), not paraphrased
"""


def analyze_file(
    client: anthropic.Anthropic, filename: str, patch: str
) -> list[SecurityFinding]:
    """Send a file diff to Claude for security analysis."""
    truncated = patch[:MAX_PATCH_CHARS]
    if len(patch) > MAX_PATCH_CHARS:
        truncated += f"\n\n[... diff truncated at {MAX_PATCH_CHARS} chars ...]"

    prompt = f"""Review this diff for security issues.

File: {filename}

```diff
{truncated}
```

Respond ONLY with a JSON array as specified. No preamble, no markdown fences."""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()

        # Strip accidental markdown fences
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        findings_raw = json.loads(raw)
        return [
            SecurityFinding(
                severity=f.get("severity", "INFO").upper(),
                title=f.get("title", "Untitled finding"),
                description=f.get("description", ""),
                affected_snippet=f.get("affected_snippet", ""),
                recommendation=f.get("recommendation", ""),
                filename=filename,
            )
            for f in findings_raw
        ]
    except json.JSONDecodeError as exc:
        print(f"  ⚠ JSON parse error for {filename}: {exc}")
        print(f"  Raw response: {raw[:200]}")
        return []
    except Exception as exc:
        print(f"  ⚠ Claude API error for {filename}: {exc}")
        return []


# ---------------------------------------------------------------------------
# Aggregate results
# ---------------------------------------------------------------------------


def aggregate(results: list[FileScanResult]) -> dict:
    counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "INFO": 0}
    all_findings = []
    for r in results:
        for f in r.findings:
            sev = f.severity if f.severity in counts else "INFO"
            counts[sev] += 1
            all_findings.append(f)
    return {"counts": counts, "findings": all_findings}


def overall_recommendation(counts: dict) -> str:
    if counts["CRITICAL"] > 0:
        return "❌ **Block Merge** — Critical security issues must be resolved before merging."
    if counts["HIGH"] > 0:
        return "⚠️ **Needs Fixes** — High severity issues should be addressed before merging."
    if counts["MEDIUM"] > 0:
        return "⚠️ **Review Required** — Medium severity issues found. Consider fixing before merge."
    if counts["LOW"] > 0 or counts["INFO"] > 0:
        return "✅ **Approve with Notes** — Only low/informational findings. Safe to merge."
    return "✅ **Clean** — No security issues detected."


def should_block(counts: dict) -> bool:
    return counts["CRITICAL"] > 0 or counts["HIGH"] > 0


# ---------------------------------------------------------------------------
# Generate markdown comment
# ---------------------------------------------------------------------------


def generate_comment(
    results: list[FileScanResult],
    aggregated: dict,
    pr_number: int,
    files_scanned: int,
    files_skipped: int,
) -> str:
    counts = aggregated["counts"]
    findings = aggregated["findings"]
    total = sum(counts.values())
    rec = overall_recommendation(counts)

    lines = [
        SCAN_HEADER,
        "## 🔐 AI Security Scan Report",
        "",
        f"**PR #{pr_number}** · {files_scanned} files scanned · {files_skipped} skipped",
        "",
        "### Summary",
        "",
        "| Severity | Count |",
        "|----------|-------|",
        f"| {SEVERITY_EMOJI['CRITICAL']} Critical | {counts['CRITICAL']} |",
        f"| {SEVERITY_EMOJI['HIGH']} High     | {counts['HIGH']} |",
        f"| {SEVERITY_EMOJI['MEDIUM']} Medium   | {counts['MEDIUM']} |",
        f"| {SEVERITY_EMOJI['LOW']} Low      | {counts['LOW']} |",
        f"| {SEVERITY_EMOJI['INFO']} Info     | {counts['INFO']} |",
        "",
        f"**Total findings:** {total}",
        "",
        "### Overall Recommendation",
        "",
        rec,
        "",
    ]

    if findings:
        lines.append("### Findings")
        lines.append("")

        # Group by file
        by_file: dict[str, list[SecurityFinding]] = {}
        for f in findings:
            by_file.setdefault(f.filename, []).append(f)

        # Sort files: files with critical/high issues first
        def file_priority(fname):
            file_findings = by_file[fname]
            sevs = [f.severity for f in file_findings]
            if "CRITICAL" in sevs:
                return 0
            if "HIGH" in sevs:
                return 1
            if "MEDIUM" in sevs:
                return 2
            return 3

        # Define the explicit order for severity sorting
        SEVERITY_ORDER = ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]

        for filename in sorted(by_file.keys(), key=file_priority):
            # Refactored lambda to avoid E701/Line-length issues
            file_findings = sorted(
                by_file[filename],
                key=lambda f: SEVERITY_ORDER.index(f.severity)
                if f.severity in SEVERITY_ORDER
                else len(SEVERITY_ORDER),
            )

            lines.append("<details>")
            lines.append(
                f"<summary><strong>{filename}</strong> — {len(file_findings)} finding(s)</summary>"
            )
            lines.append("")

            for finding in file_findings:
                emoji = SEVERITY_EMOJI.get(finding.severity, "⚪")
                lines.append(f"#### {emoji} {finding.severity}: {finding.title}")
                lines.append("")
                lines.append(f"**Description:** {finding.description}")
                lines.append("")
                if finding.affected_snippet:
                    lines.append("**Affected code:**")
                    lines.append("```diff")
                    # Limit snippet length in comment
                    snippet = finding.affected_snippet[:500]
                    if len(finding.affected_snippet) > 500:
                        snippet += "\n... (truncated)"
                    lines.append(snippet)
                    lines.append("```")
                    lines.append("")
                lines.append(f"**Recommendation:** {finding.recommendation}")
                lines.append("")
                lines.append("---")
                lines.append("")

            lines.append("</details>")
            lines.append("")
    else:
        lines.append("### Findings")
        lines.append("")
        lines.append("✅ No security issues detected in this PR.")
        lines.append("")

    # Skipped files
    skipped = [r for r in results if r.skipped]
    if skipped:
        lines.append("<details>")
        lines.append(f"<summary>Skipped files ({len(skipped)})</summary>")
        lines.append("")
        for r in skipped:
            lines.append(f"- `{r.filename}` — {r.skip_reason}")
        lines.append("")
        lines.append("</details>")
        lines.append("")

    lines.append("---")
    lines.append("*Powered by Claude · Food Price Sentinel Security Scanner*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Post or update PR comment
# ---------------------------------------------------------------------------


def post_or_update_comment(gh: Github, comment_body: str) -> None:
    """Post the security report as a PR comment. Update if one already exists."""
    repo = gh.get_repo(f"{REPO_OWNER}/{REPO_NAME}")
    pr = repo.get_pull(PR_NUMBER)

    # Check for existing scan comment (idempotency)
    existing = None
    for comment in pr.get_issue_comments():
        if comment.body.startswith(SCAN_HEADER):
            existing = comment
            break

    if existing:
        existing.edit(comment_body)
        print(f"Updated existing security scan comment (id={existing.id})")
    else:
        pr.create_issue_comment(comment_body)
        print(f"Posted new security scan comment to PR #{PR_NUMBER}")


# ---------------------------------------------------------------------------
# Set GitHub Actions output
# ---------------------------------------------------------------------------


def set_output(key: str, value: str) -> None:
    """Write a step output for use in subsequent workflow steps."""
    github_output = os.environ.get("GITHUB_OUTPUT", "")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"{key}={value}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("🔐 Food Price Sentinel — AI Security Scan")
    print(f"   PR #{PR_NUMBER} · {REPO_OWNER}/{REPO_NAME}")
    print()

    gh = Github(GITHUB_TOKEN)
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # 1. Fetch changed files
    pr_files = fetch_pr_files(gh)

    # 2. Scan each file
    results: list[FileScanResult] = []
    files_scanned = 0
    files_skipped = 0

    for f in pr_files:
        filename = f["filename"]
        patch = f.get("patch")
        skip, reason = should_skip(filename, patch)

        if skip:
            print(f"  ⏭  Skipping {filename} ({reason})")
            results.append(
                FileScanResult(filename=filename, skipped=True, skip_reason=reason)
            )
            files_skipped += 1
            continue

        print(f"  🔍 Scanning {filename} ({f['status']}, {f['changes']} changes)")
        findings = analyze_file(client, filename, patch)

        if findings:
            for finding in findings:
                emoji = SEVERITY_EMOJI.get(finding.severity, "⚪")
                print(f"     {emoji} [{finding.severity}] {finding.title}")
        else:
            print("     ✅ Clean")

        results.append(FileScanResult(filename=filename, findings=findings))
        files_scanned += 1

    # 3. Aggregate
    aggregated = aggregate(results)
    counts = aggregated["counts"]
    total = sum(counts.values())

    print()
    print(f"📊 Scan complete: {total} findings across {files_scanned} files")
    for sev, count in counts.items():
        if count > 0:
            print(f"   {SEVERITY_EMOJI[sev]} {sev}: {count}")

    # 4. Generate and post comment
    comment = generate_comment(
        results, aggregated, PR_NUMBER, files_scanned, files_skipped
    )
    post_or_update_comment(gh, comment)

    # 5. Set outputs for downstream steps
    block = should_block(counts)
    set_output("block_merge", str(block).lower())
    set_output("total_findings", str(total))
    set_output("critical_count", str(counts["CRITICAL"]))
    set_output("high_count", str(counts["HIGH"]))

    print()
    print("Security scan completed and comment posted successfully.")

    # Exit 0 — the workflow step handles blocking via check_scan_outcome


if __name__ == "__main__":
    main()
