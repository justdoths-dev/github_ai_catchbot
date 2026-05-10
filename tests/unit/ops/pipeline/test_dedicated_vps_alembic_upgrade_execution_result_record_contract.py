from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RESULT_RECORD = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_alembic_upgrade_execution_result_record.md"
)


REQUIRED_SECTIONS = (
    "# Dedicated VPS Alembic upgrade execution result record",
    "## Scope",
    "## Source-of-truth handling",
    "## Execution summary",
    "## Final result",
    "## Environment summary",
    "## Commands actually run by block",
    "## Final verification evidence",
    "## Applied migration evidence",
    "## Non-blocking observations",
    "## Security/redaction notes",
    "## Unauthorized actions not performed",
    "## Follow-up actions",
    "## Next step",
)

REQUIRED_KEYS = (
    "APP_ENV",
    "DATABASE_URL",
    "REDIS_URL",
    "ENABLE_NOTIFICATION_SEND",
    "NOTIFIER_TELEGRAM_DRY_RUN",
    "NOTIFIER_TELEGRAM_ALLOW_EDITS",
    "ENABLE_REPLAY_TO_PROD_DB",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
)

MIGRATION_FILENAMES = (
    "0001_ingest_core.py",
    "0002_normalization_candidates.py",
    "0003_enrichment_bundles.py",
    "0004_judge_delivery_observability.py",
)

MIGRATION_CHAIN = (
    "Running upgrade  -> 0001_ingest_core, 0001_ingest_core",
    "Running upgrade 0001_ingest_core -> 0002_normalization_candidates, 0002_normalization_candidates",
    "Running upgrade 0002_normalization_candidates -> 0003_enrichment_bundles, 0003_enrichment_bundles",
    "Running upgrade 0003_enrichment_bundles -> 0004_judge_delivery_obs, 0004_judge_delivery_obs",
)


def _text() -> str:
    return RESULT_RECORD.read_text(encoding="utf-8")


def _normalized_text() -> str:
    return " ".join(_text().split())


def test_result_record_has_required_sections_once() -> None:
    text = _text()

    for section in REQUIRED_SECTIONS:
        assert text.count(section) == 1, section


def test_result_record_captures_environment_and_repo_execution_context() -> None:
    text = _text()

    required_phrases = {
        "recorded after 2026-05-09 Alembic upgrade execution",
        "github-ai-catchbot-prod-1",
        "deploy",
        "/home/deploy/workspace/bots/github_ai_catchbot",
        "11ec9ab",
        "1daa7ed",
    }

    for phrase in required_phrases:
        assert phrase in text, phrase

    ipv4_literals = set(re.findall(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", text))
    assert ipv4_literals <= {"127.0.0.1"}


def test_result_record_captures_alembic_asset_evidence() -> None:
    text = _text()

    required_phrases = {
        "`alembic.ini` present",
        "`migrations` directory present",
        "`migrations/env.py` present",
        "`migrations/versions` directory present",
        "`migration_file_count=4`",
    }

    for phrase in required_phrases:
        assert phrase in text, phrase
    for filename in MIGRATION_FILENAMES:
        assert filename in text, filename


def test_result_record_captures_redacted_runtime_env_validation_pass() -> None:
    text = _text()
    normalized = _normalized_text()

    assert "redacted runtime env shape and gate validation was run manually and passed" in normalized
    assert "`runtime_env_required_keys_present=PASS`" in text
    assert "required keys present by name only" in text
    for key in REQUIRED_KEYS:
        assert key in text, key
    assert "`runtime_env_optional_keys_policy=PASS`" in text
    assert "`database_url_shape_valid=PASS`" in text
    assert "`DATABASE_URL` exists and shape is valid, but the value was not printed" in text
    assert "`database_url_printed=false`" in text


def test_result_record_captures_pre_upgrade_current_and_approval_checkpoint() -> None:
    text = _text()

    required_phrases = {
        "`pre_upgrade_alembic_current_exit_code=0`",
        "`APPROVAL_CHECKPOINT: user explicitly approved Alembic upgrade execution now`",
        "`DB_SCHEMA_MUTATION_ACKNOWLEDGED=true`",
    }

    for phrase in required_phrases:
        assert phrase in text, phrase


def test_result_record_captures_upgrade_execution_and_applied_migration_chain() -> None:
    text = _text()

    required_phrases = {
        "`alembic_upgrade_exit_code=0`",
        "`alembic_upgrade_run=true`",
    }

    for phrase in required_phrases:
        assert phrase in text, phrase
    for migration in MIGRATION_CHAIN:
        assert migration in text, migration


def test_result_record_captures_post_upgrade_current_head_and_no_other_alembic_mutations() -> None:
    text = _text()

    required_phrases = {
        "`post_upgrade_alembic_current_exit_code=0`",
        "`0004_judge_delivery_obs (head)`",
        "`alembic_stamp_run=false`",
        "`alembic_revision_run=false`",
        "`alembic_downgrade_run=false`",
    }

    for phrase in required_phrases:
        assert phrase in text, phrase


def test_result_record_records_duplicate_alembic_logs_as_non_blocking() -> None:
    text = _text()

    required_phrases = {
        "Duplicate Alembic log lines observed",
        "No secret leakage was observed",
        "Exit codes remained 0",
        "Post-upgrade current reached head",
        "No logging correction is part of this slice",
    }

    for phrase in required_phrases:
        assert phrase in text, phrase


def test_result_record_captures_forbidden_runtime_env_and_runtime_actions_not_performed() -> None:
    normalized = _normalized_text()

    required_phrases = {
        "No `cat /etc/github-ai-catchbot/runtime.env` was performed",
        "No `source /etc/github-ai-catchbot/runtime.env` was performed",
        "No `. /etc/github-ai-catchbot/runtime.env` was performed",
        "No `export DATABASE_URL` was performed",
        "No `export REDIS_URL` was performed",
        "No repo `.env` was created",
        "No repo `env/*.env` was created",
        "No app runtime was started",
        "No TDLib auth was performed",
        "No Telegram connection was performed",
        "No live collector was started",
        "No notifier transport was enabled",
        "No production rollout was performed",
    }

    for phrase in required_phrases:
        assert phrase in normalized, phrase


def test_result_record_redacts_secret_values_and_private_key_paths() -> None:
    text = _text()

    forbidden_patterns = {
        "actual_database_url_assignment": r"DATABASE_URL\s*=\s*\S+",
        "db_password_assignment": r"(?i)\b(?:password|pgpassword|catchbot_db_password|db_password)\s*=\s*\S+",
        "postgresql_url": r"postgres(?:ql)?(?:\+psycopg)?://",
        "credential_bearing_redis_url": r"redis://[^`\s]*[:@][^`\s]*@",
        "ssh_private_key_path": r"\bssh\s+-i\b|(?:^|[`\s])(?:~|/[^`\s]*)/\.ssh/[^`\s]+",
    }

    for name, pattern in forbidden_patterns.items():
        assert re.search(pattern, text) is None, name


def test_result_record_does_not_authorize_runtime_rollout() -> None:
    normalized = _normalized_text()

    required_phrases = {
        "does not authorize app runtime, TDLib/Telegram, live collector startup, notifier transport, or production rollout by itself",
        "This result record does not authorize app runtime, TDLib/Telegram, live collector, notifier transport, or production rollout by itself",
        "production rollout remains unauthorized",
    }

    for phrase in required_phrases:
        assert phrase in normalized, phrase
