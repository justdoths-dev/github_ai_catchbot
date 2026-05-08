from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RESULT_RECORD = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_db_redis_operator_provisioning_result_record.md"
)


REQUIRED_SECTIONS = (
    "# Dedicated VPS DB/Redis operator provisioning result record",
    "## Scope",
    "## Source-of-truth handling",
    "## Execution summary",
    "## Final result",
    "## Environment summary",
    "## Commands actually run by block",
    "## Deviations from approved runbook",
    "## Recovery actions performed",
    "## Final verification evidence",
    "## Security/redaction notes",
    "## Unauthorized actions not performed",
    "## Follow-up corrective actions",
    "## Next step",
)


def _text() -> str:
    return RESULT_RECORD.read_text(encoding="utf-8")


def test_result_record_has_required_sections_once() -> None:
    text = _text()

    for section in REQUIRED_SECTIONS:
        assert text.count(section) == 1, section


def test_result_record_captures_required_redacted_execution_facts() -> None:
    text = _text()

    required_phrases = {
        "recorded after 2026-05-08 operator execution",
        "github-ai-catchbot-prod-1",
        "deploy",
        "Ubuntu 24.04.4 LTS",
        "/home/deploy/workspace/bots/github_ai_catchbot",
        "a0d7e87",
        "PostgreSQL 16/main",
        "listen_addresses = '127.0.0.1'",
        "password_encryption = 'scram-sha-256'",
        "bind 127.0.0.1 ::1",
        "protected-mode yes",
        "supervised systemd",
        "postgresql active/enabled",
        "redis-server active/enabled",
        "github_ai_catchbot_app",
        "github_ai_catchbot",
        "password was set interactively",
        "password was not disclosed",
        "PostgreSQL local readiness: PASS",
        "Redis local PING: PASS",
        "no public 5432",
        "no public 6379",
        "UFW status active",
        "no UFW public 5432/6379 allow rule",
        "No `.env` was created",
        "No Alembic migration was run",
        "No app runtime was started",
        "No TDLib auth was performed",
        "No Telegram connection was performed",
        "No live collector was started",
        "No notifier transport was enabled",
        "No production rollout was performed",
    }

    for phrase in required_phrases:
        assert phrase in text, phrase


def test_result_record_redacts_public_operator_ips_and_secret_values() -> None:
    text = _text()

    ipv4_literals = set(re.findall(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", text))
    assert ipv4_literals <= {"127.0.0.1"}

    forbidden_patterns = {
        "database_url_assignment": r"DATABASE_URL\s*=",
        "redis_url_assignment": r"REDIS_URL\s*=",
        "password_assignment": r"(?i)\b(?:password|pgpassword|catchbot_db_password)\s*=\s*\S+",
        "postgresql_url": r"postgres(?:ql)?://",
        "redis_url": r"redis://",
        "ssh_private_key_path": r"\bssh\s+-i\b|(?:^|[`\s])(?:~|/[^`\s]*)/\.ssh/[^`\s]+",
    }

    for name, pattern in forbidden_patterns.items():
        assert re.search(pattern, text) is None, name
