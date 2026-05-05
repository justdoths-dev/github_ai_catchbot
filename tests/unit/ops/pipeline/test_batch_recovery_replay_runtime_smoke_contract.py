from __future__ import annotations

import importlib
import inspect
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "batch_recovery_replay_runtime_smoke.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "batch_recovery_replay_runtime_smoke.md"


def _module():
    return importlib.import_module("scripts.ops.batch_recovery_replay_runtime_smoke")


def _fake_database_url() -> str:
    password = "super" + "secret"
    return f"postgresql+psycopg://user:{password}@localhost:5432/github_ai_catchbot_smoke"


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_report_contract_constants_are_locked() -> None:
    module = _module()

    assert module.REPORT_TYPE == "batch_recovery_replay_runtime_smoke_v1"
    assert module.SELECTED_SCENARIO == "replay_selected_minimal"
    assert module.DATABASE_URL_ENV == "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL"
    assert module.SMOKE_MARKER_PREFIX == "ops-smoke:batch-recovery-replay:"
    assert module.EXPECTED_RECOVERY_MODE == "replay-selected"
    assert module.EXPECTED_REPLAY_TYPE == "delivery"
    assert module.EXPECTED_ROOT_OBJECT_TYPE == "notification_plan"
    assert module.EXPECTED_REPLAY_STATUS == "requested"


def test_cli_requires_confirm_write() -> None:
    parser = _module().build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--database-url", _fake_database_url()])


def test_parser_accepts_explicit_smoke_database_url_without_redis() -> None:
    parser = _module().build_parser()

    args = parser.parse_args(["--database-url", _fake_database_url(), "--confirm", "write"])

    assert args.database_url == _fake_database_url()
    assert args.confirm == "write"


def test_safety_database_url_guards() -> None:
    module = _module()

    assert module._is_production_like_url("postgresql+psycopg://user@db-prod:5432/catchbot")
    assert module._is_production_like_url("postgresql+psycopg://user@remote.internal:5432/catchbot_smoke")
    assert module._is_expected_smoke_database_url(
        "postgresql+psycopg://user:secret@localhost:5432/github_ai_catchbot_smoke"
    )
    assert module._is_expected_smoke_database_url("postgresql://user@127.0.0.1:5432/catchbot_test")
    assert not module._is_expected_smoke_database_url("postgresql+psycopg://user@localhost:5432/catchbot")


def test_report_shape_and_mutation_safety_fields_are_stable() -> None:
    module = _module()
    report = module._new_report(module._build_seed_shape("abc123abc123abc123abc123abc123ab"))
    rendered = json.loads(module._render_json(report))

    assert set(rendered) == {
        "report_type",
        "selected_scenario",
        "smoke_id",
        "marker",
        "checks_run",
        "checks_passed",
        "checks_failed",
        "failures",
        "warnings",
        "database_url_redacted",
        "mutation_safety",
        "mutation_safety_fields",
        "seeded_ids",
        "db_precondition_counts",
        "db_postcondition_counts",
        "batch_recovery_result_summary",
        "replay_request_summary",
        "replay_request_observed",
        "notification_plan_snapshot_before",
        "notification_plan_snapshot_after",
    }
    assert rendered["report_type"] == "batch_recovery_replay_runtime_smoke_v1"
    assert rendered["selected_scenario"] == "replay_selected_minimal"
    assert rendered["checks_failed"] == []
    assert rendered["failures"] == []
    assert rendered["database_url_redacted"] is True
    assert rendered["mutation_safety_fields"]["redis_required"] is False
    assert rendered["mutation_safety_fields"]["redis_messages_published"] is False
    assert rendered["mutation_safety_fields"]["runtime_workers_started"] is False
    assert rendered["mutation_safety_fields"]["feature_flags_mutated"] is False
    assert rendered["mutation_safety_fields"]["environment_files_written"] is False
    assert rendered["mutation_safety_fields"]["notifier_worker_started"] is False
    assert rendered["mutation_safety_fields"]["telegram_bot_api_called"] is False
    assert rendered["mutation_safety_fields"]["openai_called"] is False
    assert rendered["mutation_safety_fields"]["github_or_x_api_called"] is False
    assert rendered["mutation_safety_fields"]["event_outbox_retry_intents_created"] is False
    assert rendered["mutation_safety_fields"]["notification_plans_mutated_after_seed"] is False
    assert rendered["mutation_safety_fields"]["notification_delivery_records_mutated_after_seed"] is False
    assert rendered["mutation_safety_fields"]["notification_renders_created"] is False
    assert rendered["mutation_safety_fields"]["extra_notification_delivery_records_created"] is False
    assert rendered["mutation_safety_fields"]["dead_letter_entries_created"] is False
    assert rendered["mutation_safety_fields"]["state_transitions_created"] is False


def test_script_references_existing_batch_recovery_tool_path() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "DeliveryBatchRecoveryTool" in text
    assert "replay_selected" in text
    assert "MaintenanceRepository" in text
    assert "replay_requests" in text
    assert "notification_plan" in text
    assert "requested" in text
    assert "delivery" in text
    assert "db.notification_plan_row_unchanged_after_replay" in text
    assert "db.notification_delivery_record_row_unchanged_after_replay" in text


def test_replay_request_loader_queries_expected_contract() -> None:
    module = _module()
    source = inspect.getsource(module._load_replay_request_rows)
    normalized_source = " ".join(source.lower().split())

    assert "from replay_requests" in normalized_source
    assert "replay_type = 'delivery'::replay_type_enum" in normalized_source
    assert "root_object_type = 'notification_plan'" in normalized_source
    assert "root_object_id = cast(:notification_plan_id as uuid)" in normalized_source


def test_notification_plan_snapshot_helper_queries_selected_plan_by_id() -> None:
    module = _module()
    source = inspect.getsource(module._load_notification_plan_snapshot)
    normalized_source = " ".join(source.lower().split())

    assert "from notification_plans" in normalized_source
    assert "where notification_plan_id = cast(:notification_plan_id as uuid)" in normalized_source


def test_delivery_record_snapshot_helper_queries_seeded_record_by_id() -> None:
    module = _module()
    source = inspect.getsource(module._load_notification_delivery_record_snapshot)
    normalized_source = " ".join(source.lower().split())

    assert "from notification_delivery_records" in normalized_source
    assert "where notification_delivery_record_id = cast(:notification_delivery_record_id as uuid)" in normalized_source


def test_notification_plan_snapshot_mutation_marks_safety_field_true() -> None:
    module = _module()
    report = module._new_report(module._build_seed_shape("abc123abc123abc123abc123abc123ab"))
    before = {field: None for field in module.NOTIFICATION_PLAN_SNAPSHOT_FIELDS}
    after = dict(before)
    before["notification_plan_id"] = "c5c71b8d-b83d-4e85-9437-2ff8ecdd01c6"
    after["notification_plan_id"] = before["notification_plan_id"]
    before["status"] = "failed_terminal"
    after["status"] = "pending"

    module._verify_notification_plan_row_unchanged_after_replay(
        report,
        before=before,
        after=after,
        database_url=_fake_database_url(),
    )

    assert report.mutation_safety_fields["notification_plans_mutated_after_seed"] is True
    assert "db.notification_plan_row_unchanged_after_replay" in report.checks_failed
    assert report.failures
    assert report.failures[0]["message"]


def test_delivery_record_snapshot_mutation_marks_safety_field_true() -> None:
    module = _module()
    report = module._new_report(module._build_seed_shape("abc123abc123abc123abc123abc123ab"))
    before = {field: None for field in module.NOTIFICATION_DELIVERY_RECORD_SNAPSHOT_FIELDS}
    after = dict(before)
    before["notification_delivery_record_id"] = "c5c71b8d-b83d-4e85-9437-2ff8ecdd01c6"
    after["notification_delivery_record_id"] = before["notification_delivery_record_id"]
    before["delivery_status"] = "failed_terminal"
    after["delivery_status"] = "sent"

    module._verify_notification_delivery_record_row_unchanged_after_replay(
        report,
        before=before,
        after=after,
        database_url=_fake_database_url(),
    )

    assert report.mutation_safety_fields["notification_delivery_records_mutated_after_seed"] is True
    assert "db.notification_delivery_record_row_unchanged_after_replay" in report.checks_failed
    assert report.failures
    assert report.failures[0]["message"]


def test_no_forbidden_worker_redis_external_transport_or_feature_flag_mutation_calls() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    forbidden_tokens = [
        "import openai",
        "from openai",
        "import redis",
        "from redis",
        "Redis.from_url",
        "RedisStreamsPublisher",
        ".xadd(",
        ".publish(",
        "requests.",
        "import requests",
        "httpx.",
        "import httpx",
        "aiohttp",
        "urllib.request",
        "socket.",
        "TelegramBotClient(",
        "send_message(",
        "edit_message_text(",
        "MaintenanceQueueWorker",
        "ReplayQueueWorker",
        "DueRetryPromotionWorker",
        "Collector",
        "OutboxRelay",
        "RouterNormalizer",
        "EvidenceAssembler",
        "AnalysisRouter",
        "JudgeOpenAI",
        "PolicyEngine",
        "NotifierTelegram",
        "run_forever(",
        "promote_due_retries_once(",
        "handle_maintenance_trigger_event(",
        "handle_replay_trigger_event(",
        "os.environ[",
        "dotenv",
        "ENABLE_NOTIFICATION_SEND=",
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=",
        "NOTIFIER_TELEGRAM_DRY_RUN=",
    ]
    for token in forbidden_tokens:
        assert token not in text


def test_script_does_not_create_forbidden_side_effect_rows() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "INSERT INTO notification_renders" not in text
    assert "INSERT INTO dead_letter_entries" not in text
    assert "INSERT INTO state_transitions" not in text
    assert "DELETE FROM" not in text
    assert "manual_selected_due_retry" not in text
    assert "notify:manual-retry-intent:" not in text
    assert "pending notification.plan.created.v1 manual retry-intent" not in text


def test_row_immutability_check_names_are_present() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "db.notification_plan_row_unchanged_after_replay" in text
    assert "db.notification_delivery_record_row_unchanged_after_replay" in text


def test_runbook_documents_required_safety_and_contract() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "replay-selected" in text
    assert "replay_selected_minimal" in text
    assert "batch_recovery_replay_runtime_smoke_v1" in text
    assert "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" in text
    assert "--confirm write" in text
    assert "replay_type = delivery" in text
    assert "root_object_type = notification_plan" in text
    assert "status = requested" in text
    assert "Redis is not required" in text
    assert "No Redis messages are published" in text
    assert "No notifier worker is started" in text
    assert "No Telegram Bot API call is made" in text
    assert "No OpenAI call is made" in text
    assert "No GitHub or X API call is made" in text
    assert "No feature flags or env files are mutated" in text
    assert "does not authorize production rollout" in text
    assert "This does not exercise `retry-selected-due`" in text
    assert "failed_retryable` rows belong to `retry-selected-due`, not `replay-selected`" in text
