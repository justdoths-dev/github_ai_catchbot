from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "delivery_gate_runtime_smoke.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "delivery_gate_runtime_smoke.md"


def _module():
    return importlib.import_module("scripts.ops.delivery_gate_runtime_smoke")


def _fake_database_url() -> str:
    password = "super" + "secret"
    return f"postgresql+psycopg://user:{password}@localhost:5432/github_ai_catchbot_smoke"


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


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


def test_redaction_helper_removes_database_url_fragments() -> None:
    module = _module()
    database_url = _fake_database_url()
    message = f"failed db={database_url} password=supersecret"

    redacted = module._redact_sensitive_text(message, database_url=database_url)

    assert database_url not in redacted
    assert "postgresql+psycopg://" not in redacted
    assert "supersecret" not in redacted
    assert "<redacted" in redacted


def test_report_contract_constants_are_locked() -> None:
    module = _module()

    assert module.REPORT_TYPE == "delivery_gate_runtime_smoke_v1"
    assert module.SELECTED_SCENARIO == "restricted_pass_minimal"
    assert module.DATABASE_URL_ENV == "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL"
    assert module.LOCKED_RESTRICTED_METRIC_ORDER == [
        "success_rate_1h",
        "high_source_to_delivery_p95_sec",
        "due_retry_oldest_lag_sec",
        "open_delivery_dlq_count",
        "unexpected_send_disabled_count",
    ]
    assert module.ALLOWED_RECOMMENDED_FLAG_PATCH_KEYS == [
        "ENABLE_NOTIFICATION_SEND",
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
        "NOTIFIER_TELEGRAM_DRY_RUN",
    ]
    assert "RESTRICTED_SCOPE_REVIEW_REQUIRED" in module.FORBIDDEN_RECOMMENDED_FLAG_PATCH_KEYS


def test_report_shape_is_stable() -> None:
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
        "metric_order_core_observed",
        "gate_status",
        "blocking_reason_codes",
        "warning_reason_codes",
        "recommended_flag_patch",
    }
    assert rendered["report_type"] == "delivery_gate_runtime_smoke_v1"
    assert rendered["selected_scenario"] == "restricted_pass_minimal"
    assert rendered["checks_failed"] == []
    assert rendered["failures"] == []
    assert rendered["database_url_redacted"] is True
    assert rendered["mutation_safety_fields"]["redis_required"] is False
    assert rendered["mutation_safety_fields"]["runtime_workers_started"] is False
    assert rendered["mutation_safety_fields"]["feature_flags_mutated"] is False
    assert rendered["mutation_safety_fields"]["recommended_flag_patch_applied"] is False


def test_core_metric_order_filter_allows_only_current_compatibility_metric() -> None:
    module = _module()

    observed = [
        "success_rate_1h",
        "high_source_to_delivery_p95_sec",
        "plan_to_transport_p95_sec",
        "due_retry_oldest_lag_sec",
        "open_delivery_dlq_count",
        "unexpected_send_disabled_count",
    ]

    assert module._restricted_core_metric_order(observed) == module.LOCKED_RESTRICTED_METRIC_ORDER
    assert module.CURRENT_COMPATIBILITY_METRICS == ["plan_to_transport_p95_sec"]


def test_no_openai_telegram_redis_or_direct_external_network_imports_or_calls() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "import openai" not in text
    assert "from openai" not in text
    assert "import redis" not in text
    assert "from redis" not in text
    assert "requests." not in text
    assert "import requests" not in text
    assert "httpx." not in text
    assert "import httpx" not in text
    assert "aiohttp" not in text
    assert "urllib.request" not in text
    assert "socket." not in text
    assert "TelegramBotClient(" not in text
    assert "send_message(" not in text
    assert "edit_message_text(" not in text


def test_no_runtime_worker_imports_or_calls() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    forbidden = [
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
    ]
    for token in forbidden:
        assert token not in text


def test_script_uses_existing_delivery_gate_runner_and_repository_only() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "DeliveryGateRunner" in text
    assert "MaintenanceRepository" in text
    assert "runner.run(mode=\"restricted\")" in text
    assert "DeliveryGateReportV1" not in text


def test_no_production_db_default_or_feature_flag_mutation_path_exists() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "os.getenv(\"DATABASE_URL\"" not in text
    assert "os.getenv('DATABASE_URL'" not in text
    assert "MaintenanceConfig.from_env" not in text
    assert "prod" not in _module()._new_report().mutation_safety.lower()
    assert "os.environ[" not in text
    assert "dotenv" not in text
    assert ".env" not in text
    assert "open(" not in text
    assert "Path(" not in text.replace("Path(__file__)", "")
    assert "ENABLE_NOTIFICATION_SEND" in text
    assert "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION" in text
    assert "NOTIFIER_TELEGRAM_DRY_RUN" in text
    assert "RESTRICTED_SCOPE_REVIEW_REQUIRED" not in _module().ALLOWED_RECOMMENDED_FLAG_PATCH_KEYS


def test_no_gate_runner_side_effect_insert_paths_exist() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "INSERT INTO event_outbox" not in text
    assert "INSERT INTO notification_renders" not in text
    assert "INSERT INTO dead_letter_entries" not in text
    assert "INSERT INTO replay_requests" not in text
    assert "INSERT INTO state_transitions" not in text
    assert "UPDATE notification_plans" not in text
    assert "DELETE FROM" not in text


def test_runbook_documents_one_shot_safety_and_gate_contract() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "one-shot DB acceptance harness" in text
    assert "not a runtime worker smoke" in text
    assert "restricted_pass_minimal" in text
    assert "Redis is not required" in text
    assert "does not call OpenAI" in text
    assert "does not start collector" in text
    assert "does not start" in text
    assert "does not mutate feature flags" in text
    assert "does not auto-apply" in text
    assert "recommended_flag_patch" in text
    assert "plan_to_transport_p95_sec" in text
