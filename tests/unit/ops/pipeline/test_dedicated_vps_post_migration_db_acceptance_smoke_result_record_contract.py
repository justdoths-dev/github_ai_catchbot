from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RESULT_RECORD = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_post_migration_db_acceptance_smoke_result_record.md"
)
PARENT_RUNBOOK = (
    ROOT
    / "ops"
    / "pipeline"
    / "runbooks"
    / "dedicated_vps_post_migration_db_acceptance_smoke.md"
)

KEY_TABLES = (
    "source_messages",
    "event_outbox",
    "artifact_registry",
    "candidate_group_proposals",
    "candidate_evidence_bundles",
    "judge_runs",
    "judge_outputs",
    "analyses",
    "notification_plans",
    "notification_delivery_records",
    "job_attempts",
    "state_transitions",
)

SIDE_EFFECT_FALSE_FLAGS = (
    "database_url_printed",
    "db_write_performed",
    "redis_connected",
    "redis_mutation_performed",
    "alembic_run",
    "alembic_upgrade_run",
    "alembic_downgrade_run",
    "alembic_stamp_run",
    "alembic_revision_run",
    "app_runtime_started",
    "tdlib_auth_performed",
    "telegram_connected",
    "live_collector_started",
    "notifier_transport_enabled",
    "production_rollout_performed",
    "docker_used",
    "systemd_modified",
    "migration_files_modified",
    "runtime_env_values_printed",
    "secret_values_printed",
)

REQUIRED_SECTIONS = (
    "# Dedicated VPS post-migration DB acceptance smoke result record",
    "## Purpose",
    "## Scope",
    "## Source-of-truth handling",
    "## Preconditions already satisfied",
    "## Redacted execution result summary",
    "## Explicit side-effect confirmation",
    "## Redaction and secret handling",
    "## Current next recommended slice",
    "## What remains unauthorized",
)


def _text() -> str:
    return RESULT_RECORD.read_text(encoding="utf-8")


def _normalized_text() -> str:
    return " ".join(_text().split())


def test_result_record_exists_and_has_required_sections_once() -> None:
    assert RESULT_RECORD.exists()
    text = _text()

    for section in REQUIRED_SECTIONS:
        assert text.count(section) == 1, section


def test_result_record_captures_pass_status_and_revision() -> None:
    text = _text()

    assert "contract_status: passed" in text
    assert "post-migration DB acceptance smoke passed" in text
    assert "expected_terminal_revision: 0004_judge_delivery_obs" in text
    assert "observed_alembic_versions:" in text
    assert "  - 0004_judge_delivery_obs" in text


def test_result_record_captures_counts_and_empty_missing_sets() -> None:
    text = _text()

    for phrase in (
        "expected_table_count: 33",
        "present_table_count: 33",
        "missing_tables: []",
        "index_check_summary:",
        "  expected: 55",
        "  present: 55",
        "  missing: []",
        "constraint_check_summary:",
        "  expected: 58",
        "  present: 58",
    ):
        assert phrase in text, phrase


def test_result_record_lists_all_key_tables_and_query_failures_empty() -> None:
    text = _text()

    assert "key_tables_queried:" in text
    for table in KEY_TABLES:
        assert f"  - {table}" in text, table
    assert "key_table_query_failures: []" in text


def test_result_record_captures_database_and_read_only_transaction_status() -> None:
    text = _text()

    assert "database_connected: true" in text
    assert "read_only_transaction_requested: true" in text
    assert "read_only_transaction_confirmed: true" in text


def test_result_record_captures_required_migration_sources() -> None:
    text = _text()

    for revision in (
        "0001_ingest_core",
        "0002_normalization_candidates",
        "0003_enrichment_bundles",
        "0004_judge_delivery_obs",
    ):
        assert revision in text, revision

    for filename in (
        "migrations/versions/0001_ingest_core.py",
        "migrations/versions/0002_normalization_candidates.py",
        "migrations/versions/0003_enrichment_bundles.py",
        "migrations/versions/0004_judge_delivery_observability.py",
    ):
        assert filename in text, filename


def test_result_record_states_side_effect_false_values() -> None:
    text = _text()

    for flag in SIDE_EFFECT_FALSE_FLAGS:
        assert f"{flag}: false" in text, flag


def test_result_record_states_runtime_env_read_was_approved_and_value_safe() -> None:
    text = _text()
    normalized = _normalized_text()

    assert "runtime_env_read: true" in text
    assert "approved Python runner read" in text
    assert "Values were not printed" in text
    assert "This repository result-record slice did not read `/etc/github-ai-catchbot/runtime.env`" in normalized
    assert "`runtime_env_metadata.keys_present` is key-name-only metadata and not secret values" in normalized


def test_result_record_includes_allowed_runtime_env_key_names_only() -> None:
    text = _text()

    for phrase in (
        "path: /etc/github-ai-catchbot/runtime.env",
        "database_url_present: true",
        "database_url_scheme: postgresql+psycopg",
        "database_url_has_credentials: true",
        "APP_ENV",
        "DATABASE_URL",
        "ENABLE_NOTIFICATION_SEND",
        "ENABLE_REPLAY_TO_PROD_DB",
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
        "NOTIFIER_TELEGRAM_ALLOW_EDITS",
        "NOTIFIER_TELEGRAM_DRY_RUN",
        "REDIS_URL",
    ):
        assert phrase in text, phrase


def test_result_record_redacts_secret_values_urls_credentials_and_ips() -> None:
    text = _text()

    forbidden_patterns = {
        "postgresql_psycopg_url": r"postgresql\+psycopg://",
        "postgresql_url": r"postgres(?:ql)?://",
        "database_url_assignment": r"DATABASE_URL\s*=",
        "redis_url_assignment": r"REDIS_URL\s*=",
        "raw_url_credentials": r"[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+:[^@\s]+@",
        "db_password_assignment": r"(?i)\b(?:password|pgpassword|catchbot_db_password|db_password)\s*=\s*\S+",
        "db_password_value": r"(?i)(?:password|secret|credential)[-_]?[A-Za-z0-9]{8,}",
        "raw_ipv4": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
    }

    for name, pattern in forbidden_patterns.items():
        assert re.search(pattern, text) is None, name

    for phrase in (
        "`DATABASE_URL` was not printed",
        "DB password was not printed",
        "`REDIS_URL` value was not printed",
        "Raw server IP was not recorded",
        "Raw operator IP was not recorded",
    ):
        assert phrase in text, phrase


def test_result_record_states_next_slice_and_non_authorization() -> None:
    normalized = _normalized_text()

    assert "`dedicated_vps_runtime_environment_consumer_preflight`" in normalized
    for phrase in (
        "does not authorize",
        "app runtime",
        "TDLib",
        "Telegram",
        "live collector",
        "notifier transport",
        "production rollout",
    ):
        assert phrase in normalized, phrase


def test_parent_runbook_points_to_result_record_without_authorizing_runtime() -> None:
    text = PARENT_RUNBOOK.read_text(encoding="utf-8")

    assert "## Completed redacted result record" in text
    assert "dedicated_vps_post_migration_db_acceptance_smoke_result_record.md" in text
    assert "`dedicated_vps_runtime_environment_consumer_preflight`" in text
    for phrase in (
        "does not authorize app runtime",
        "TDLib",
        "Telegram",
        "live collector",
        "notifier transport",
        "production rollout",
    ):
        assert phrase in text, phrase
