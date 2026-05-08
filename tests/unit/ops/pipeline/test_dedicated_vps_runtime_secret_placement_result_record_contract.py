from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RESULT_RECORD = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_runtime_secret_placement_result_record.md"
)


REQUIRED_SECTIONS = (
    "# Dedicated VPS runtime secret placement result record",
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
        "recorded after 2026-05-08 runtime secret placement execution",
        "github-ai-catchbot-prod-1",
        "deploy",
        "/home/deploy/workspace/bots/github_ai_catchbot",
        "a660e52",
        "/etc/github-ai-catchbot/runtime.env",
        "root:deploy 0640",
        "root:deploy 0750",
        "Required keys present by name",
        "APP_ENV",
        "DATABASE_URL",
        "REDIS_URL",
        "ENABLE_NOTIFICATION_SEND",
        "NOTIFIER_TELEGRAM_DRY_RUN",
        "NOTIFIER_TELEGRAM_ALLOW_EDITS",
        "ENABLE_REPLAY_TO_PROD_DB",
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
        "Optional keys present: none",
        "Final redacted validation passed",
        "DATABASE_URL placeholder failure was caught and corrected",
        "secret_values_printed=false",
        "No `cat`, `source`, dot-source, or export command was run",
        "No repo `.env` was created",
        "No repo `env/*.env` was created",
        "No Alembic was run",
        "No app runtime was started",
        "No TDLib auth was performed",
        "No Telegram connection was performed",
        "No live collector was started",
        "No notifier transport was enabled",
        "No production rollout was performed",
    }

    for phrase in required_phrases:
        assert phrase in text, phrase


def test_result_record_redacts_ips_secret_values_and_private_key_paths() -> None:
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
