from __future__ import annotations

import importlib
import inspect
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "delivery_gate_full_runtime_smoke.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "delivery_gate_full_runtime_smoke.md"


def _module():
    return importlib.import_module("scripts.ops.delivery_gate_full_runtime_smoke")


def _fake_database_url() -> str:
    password = "super" + "secret"
    return f"postgresql+psycopg://user:{password}@localhost:5432/github_ai_catchbot_smoke"


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_report_contract_constants_are_locked() -> None:
    module = _module()

    assert module.REPORT_TYPE == "delivery_gate_full_runtime_smoke_v1"
    assert module.SELECTED_SCENARIO == "full_pass_with_operator_review"
    assert module.DATABASE_URL_ENV == "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL"
    assert module.SMOKE_MARKER_PREFIX == "ops-smoke:delivery-gate-full:"
    assert module.EXPECTED_GATE_MODE == "full"
    assert module.EXPECTED_GATE_STATUS == "pass"


def test_parser_accepts_explicit_smoke_database_url_without_confirm_or_redis() -> None:
    parser = _module().build_parser()

    args = parser.parse_args(["--database-url", _fake_database_url()])

    assert args.database_url == _fake_database_url()


def test_safety_database_url_guards() -> None:
    module = _module()

    assert module._is_production_like_url("postgresql+psycopg://user@db-prod:5432/catchbot")
    assert module._is_production_like_url("postgresql+psycopg://user@remote.internal:5432/catchbot_smoke")
    assert module._is_expected_smoke_database_url(
        "postgresql+psycopg://user:secret@localhost:5432/github_ai_catchbot_smoke"
    )
    assert module._is_expected_smoke_database_url("postgresql://user@127.0.0.1:5432/catchbot_test")
    assert not module._is_expected_smoke_database_url("postgresql+psycopg://user@localhost:5432/catchbot")


def test_redaction_helper_removes_database_url_fragments() -> None:
    module = _module()
    database_url = _fake_database_url()
    message = f"failed db={database_url} password=supersecret"

    redacted = module._redact_sensitive_text(message, database_url=database_url)

    assert database_url not in redacted
    assert "postgresql+psycopg://" not in redacted
    assert "supersecret" not in redacted
    assert "<redacted" in redacted


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
        "gate_report_summary",
        "metric_order_expected",
        "metric_order_observed",
        "full_metric_names_observed",
        "recommended_flag_patch",
    }
    assert rendered["report_type"] == "delivery_gate_full_runtime_smoke_v1"
    assert rendered["selected_scenario"] == "full_pass_with_operator_review"
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
    assert rendered["mutation_safety_fields"]["recommended_flag_patch_applied"] is False
    assert rendered["mutation_safety_fields"]["event_outbox_rows_emitted_by_gate_runner"] is False
    assert rendered["mutation_safety_fields"]["notification_render_rows_created_by_gate_runner"] is False
    assert rendered["mutation_safety_fields"]["notification_delivery_rows_created_by_gate_runner"] is False
    assert rendered["mutation_safety_fields"]["dead_letter_rows_created_by_gate_runner"] is False
    assert rendered["mutation_safety_fields"]["replay_request_rows_created_by_gate_runner"] is False
    assert rendered["mutation_safety_fields"]["state_transition_rows_created_by_gate_runner"] is False


def test_script_uses_existing_delivery_gate_runner_full_operator_review_paths() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "DeliveryGateRunner" in text
    assert "MaintenanceRepository" in text
    assert 'runner.run(mode="full", operator_review_passed=True)' in text
    assert 'runner.run(mode="full", operator_review_passed=False)' in text
    assert "delivery_gate_operator_review_required" in text
    assert "full_warn_without_operator_review" in text


def test_full_mode_metric_names_are_locked() -> None:
    module = _module()

    assert module.FULL_METRIC_ORDER_EXPECTED == [
        "success_rate_1h",
        "high_source_to_delivery_p95_sec",
        "plan_to_transport_p95_sec",
        "due_retry_oldest_lag_sec",
        "open_delivery_dlq_count",
        "unexpected_send_disabled_count",
        "success_rate_24h",
        "replay_guard_reject_count_24h",
        "retry_ceiling_exceeded_count_24h",
        "oldest_delivery_dlq_age_sec",
        "duplicate_noop_ratio_1h",
    ]
    text = SCRIPT.read_text(encoding="utf-8")
    for metric_name in module.FULL_METRIC_ORDER_EXPECTED:
        assert metric_name in text


def test_recommended_flag_patch_expected_keys_are_locked() -> None:
    module = _module()

    assert module.EXPECTED_RECOMMENDED_FLAG_PATCH == {
        "ENABLE_NOTIFICATION_SEND": True,
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": True,
        "NOTIFIER_TELEGRAM_DRY_RUN": False,
    }


def test_precondition_queries_cover_full_mode_blockers() -> None:
    module = _module()
    source = inspect.getsource(module._load_precondition_counts)
    normalized_source = " ".join(source.lower().split())

    assert "from notification_delivery_records" in normalized_source
    assert "from dead_letter_entries" in normalized_source
    assert "from replay_requests" in normalized_source
    assert "rejected_by_env_guard" in normalized_source
    assert "max_notification_retry_attempts_exceeded" in normalized_source
    assert "failed_retryable" in normalized_source
    assert "send_disabled" in normalized_source


def test_no_batch_recovery_retry_replay_or_forbidden_worker_network_feature_flag_calls() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    forbidden_tokens = [
        "DeliveryBatchRecoveryTool",
        "batch_recovery_tool",
        "replay_selected",
        "retry_selected_due",
        "manual_selected_due_retry",
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


def test_script_does_not_create_forbidden_gate_side_effect_rows() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "INSERT INTO event_outbox" not in text
    assert "INSERT INTO notification_renders" not in text
    assert "INSERT INTO dead_letter_entries" not in text
    assert "INSERT INTO replay_requests" not in text
    assert "INSERT INTO state_transitions" not in text
    assert "DELETE FROM" not in text


def test_runbook_documents_required_safety_and_full_operator_review_contract() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "delivery_gate_full_runtime_smoke_v1" in text
    assert "full_pass_with_operator_review" in text
    assert "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" in text
    assert "Redis is not required" in text
    assert "No Redis messages are published" in text
    assert "No notifier worker is started" in text
    assert "No Telegram Bot API call is made" in text
    assert "No OpenAI call is made" in text
    assert "No GitHub or X API call is made" in text
    assert "No feature flags or env files are mutated" in text
    assert "recommended_flag_patch" in text
    assert "output-only" in text
    assert "operator review" in text
    assert "full_warn_without_operator_review" in text
    assert "does not authorize production rollout" in text
