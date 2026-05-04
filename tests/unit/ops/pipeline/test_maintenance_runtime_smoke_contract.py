from __future__ import annotations

import importlib
import inspect
import json
from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "maintenance_runtime_smoke.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "maintenance_runtime_smoke.md"


def _module():
    return importlib.import_module("scripts.ops.maintenance_runtime_smoke")


def _fake_database_url() -> str:
    password = "super" + "secret"
    return f"postgresql+psycopg://user:{password}@localhost:5432/github_ai_catchbot_smoke"


def _fake_redis_url() -> str:
    password = "redis" + "secret"
    return f"redis://:{password}@localhost:6379/14"


def _valid_payload(module, *, event_id=None, notification_plan_id=None) -> dict[str, str]:
    event_id = event_id or uuid4()
    notification_plan_id = notification_plan_id or uuid4()
    return module._build_expected_redis_payload(
        event_id=event_id,
        notification_plan_id=notification_plan_id,
        marker=f"{module.SMOKE_MARKER_PREFIX}{event_id.hex}",
    )


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_cli_requires_confirm_write() -> None:
    parser = _module().build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--database-url", _fake_database_url(), "--redis-url", "redis://localhost:6379/14"])


def test_parser_accepts_explicit_smoke_urls() -> None:
    parser = _module().build_parser()

    args = parser.parse_args(
        [
            "--database-url",
            _fake_database_url(),
            "--redis-url",
            "redis://localhost:6379/14",
            "--confirm",
            "write",
        ]
    )

    assert args.database_url == _fake_database_url()
    assert args.redis_url == "redis://localhost:6379/14"
    assert args.confirm == "write"


def test_safety_url_guards() -> None:
    module = _module()

    assert module._is_production_like_url("postgresql+psycopg://user@db-prod:5432/catchbot")
    assert module._is_production_like_url("redis://production-redis:6379/14")
    assert module._is_production_like_url("rediss://localhost:6379/14")
    assert module._is_expected_smoke_database_url(
        "postgresql+psycopg://user:secret@localhost:5432/github_ai_catchbot_smoke"
    )
    assert module._is_expected_smoke_database_url("postgresql://user@127.0.0.1:5432/catchbot_test")
    assert not module._is_expected_smoke_database_url("postgresql+psycopg://user@localhost:5432/catchbot")
    assert module._is_expected_redis_db14("redis://localhost:6379/14")
    assert module._is_expected_redis_db14("redis://127.0.0.1:6379/14")
    assert not module._is_expected_redis_db14("redis://localhost:6379/0")


def test_redaction_helper_removes_database_and_redis_url_fragments() -> None:
    module = _module()
    database_url = _fake_database_url()
    redis_url = _fake_redis_url()
    message = f"failed db={database_url} redis={redis_url} password=supersecret"

    redacted = module._redact_sensitive_text(message, database_url=database_url, redis_url=redis_url)

    assert database_url not in redacted
    assert redis_url not in redacted
    assert "postgresql+psycopg://" not in redacted
    assert "redis://:redis" not in redacted
    assert "supersecret" not in redacted
    assert "<redacted" in redacted


def test_thin_redis_payload_validation_enforces_exact_field_set() -> None:
    module = _module()
    event_id = uuid4()
    notification_plan_id = uuid4()
    fields = _valid_payload(module, event_id=event_id, notification_plan_id=notification_plan_id)

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_notification_plan_id=notification_plan_id,
        database_url=_fake_database_url(),
        redis_url="redis://localhost:6379/14",
    )

    assert failures == []
    assert set(fields) == module.REQUIRED_REDIS_FIELDS


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("payload_json", "{}"),
        ("notification_plan_id", str(uuid4())),
        ("notification_delivery_record_id", str(uuid4())),
        ("delivery_status", "failed_retryable"),
        ("attempt_count", "1"),
        ("transport_error_code", "telegram_retryable_5xx"),
        ("telegram_response_json", "{}"),
        ("analysis_id", str(uuid4())),
        ("candidate_group_id", str(uuid4())),
        ("database_url", "postgresql+psycopg://user:secret@localhost:5432/db"),
        ("api_token", "secret-token"),
    ],
)
def test_thin_redis_payload_validation_rejects_business_and_secret_fields(field_name, field_value) -> None:
    module = _module()
    event_id = uuid4()
    notification_plan_id = uuid4()
    fields = _valid_payload(module, event_id=event_id, notification_plan_id=notification_plan_id)
    fields[field_name] = field_value

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_notification_plan_id=notification_plan_id,
        database_url=_fake_database_url(),
        redis_url="redis://localhost:6379/14",
    )

    assert any(field_name in failure or "URL or credential" in failure for failure in failures)


def test_event_queue_stage_and_output_names_are_locked() -> None:
    module = _module()

    assert module.REPORT_TYPE == "maintenance_runtime_smoke_v1"
    assert module.SELECTED_SCENARIO == "retryable_due_promotion"
    assert module.EXPECTED_EVENT_TYPE == "notification.delivery.result.v1"
    assert module.EXPECTED_QUEUE_NAME == "q.maintenance"
    assert module.EXPECTED_STAGE_NAME == "maintenance"
    assert module.EXPECTED_AGGREGATE_TYPE == "notification_plan"
    assert module.EXPECTED_RETRY_EVENT_TYPE == "notification.plan.created.v1"


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
        "redis_url_redacted",
        "mutation_safety",
        "queue_name",
        "redis_stream_message_id",
        "redis_message_ids",
        "seeded_ids",
        "db_postcondition_counts",
        "transport_safety",
        "mutation_booleans",
        "maintenance_output_outbox_ids",
        "maintenance_output_payloads",
        "worker_result",
        "due_retry_result",
    }
    assert rendered["report_type"] == "maintenance_runtime_smoke_v1"
    assert rendered["selected_scenario"] == "retryable_due_promotion"
    assert rendered["queue_name"] == "q.maintenance"
    assert rendered["transport_safety"]["maintenance_enable_notification_retry_promotion"] is True
    assert rendered["transport_safety"]["openai_key_required"] is False
    assert rendered["transport_safety"]["telegram_bot_token_required"] is False
    assert rendered["transport_safety"]["notifier_worker_started"] is False
    assert rendered["transport_safety"]["policy_engine_started"] is False
    assert rendered["mutation_booleans"] == {
        "notification_plan_mutated": False,
        "notification_delivery_record_mutated": False,
        "analysis_mutated": False,
        "judge_output_mutated": False,
        "candidate_group_mutated": False,
    }


def test_no_openai_telegram_transport_or_direct_external_network_imports_or_calls() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "import openai" not in text
    assert "from openai" not in text
    assert "requests." not in text
    assert "import requests" not in text
    assert "httpx." not in text
    assert "import httpx" not in text
    assert "aiohttp" not in text
    assert "urllib.request" not in text
    assert "socket." not in text
    assert "TelegramBotClient(" not in text
    assert "NotifierTelegramWorker" not in text
    assert "PolicyEngineService" not in text
    assert "send_message(" not in text
    assert "edit_message_text(" not in text


def test_script_uses_existing_route_worker_service_and_repository_paths() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "OutboxRouteResolver" in text
    assert "RedisStreamsPublisher" in text
    assert "OutboxRelayRepository" in text
    assert "RedisStreamConsumer" in text
    assert "MaintenanceQueueWorker" in text
    assert "DueRetryPromotionWorker" in text
    assert "MaintenanceService" in text
    assert "MaintenanceRepository" in text
    assert "SessionBackedMaintenanceService" in text
    assert "handle_maintenance_trigger_event(trigger_event_id)" in text


def test_script_uses_trigger_event_id_rehydration_and_thin_redis_contract() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    source = inspect.getsource(__import__("services.maintenance.service", fromlist=["MaintenanceService"]))
    repo_source = inspect.getsource(__import__("services.maintenance.repositories", fromlist=["MaintenanceRepository"]))

    assert '"trigger_event_id"' in text
    assert "payload_json" in text
    assert "FORBIDDEN_REDIS_FIELDS" in text
    assert "expected_notification_plan_id" in text
    assert "load_outbox_event(event_id)" in source
    assert "delivery_result_from_outbox(event)" in source
    assert "WHERE event_id = CAST(:event_id AS uuid)" in repo_source


def test_postcondition_and_mutation_checks_exist() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "db.seed_notification_delivery_result_outbox_published" in text
    assert "db.exactly_one_pending_notification_plan_created_retry_intent" in text
    assert "db.retry_intent_payload_matches_existing_contract" in text
    assert "db.retry_intent_dedupe_key_stable_and_plan_scoped" in text
    assert "db.notification_plans_not_mutated" in text
    assert "db.notification_delivery_records_not_mutated" in text
    assert "db.analyses_not_mutated" in text
    assert "db.judge_outputs_not_mutated" in text
    assert "db.candidate_group_proposals_not_mutated" in text
    assert "db.no_notification_render_created_by_maintenance" in text
    assert "db.no_second_notification_delivery_record_created_by_maintenance" in text


def test_selected_scenario_retryable_due_payload_contract() -> None:
    module = _module()
    seed_ids = module.SmokeSeedIds(
        source_message_id=uuid4(),
        artifact_id=uuid4(),
        snapshot_id=uuid4(),
        candidate_group_id=uuid4(),
        bundle_id=uuid4(),
        judge_run_id=uuid4(),
        judge_output_id=uuid4(),
        analysis_id=uuid4(),
        notification_plan_id=uuid4(),
        notification_delivery_record_id=uuid4(),
        delivery_result_event_id=uuid4(),
    )
    payload = module._build_delivery_result_payload(
        seed_ids=seed_ids,
        seed_shape=module._build_seed_shape("abc123abc123abc123abc123abc123ab"),
    )

    assert payload["notification_plan_id"] == str(seed_ids.notification_plan_id)
    assert payload["notification_delivery_record_id"] == str(seed_ids.notification_delivery_record_id)
    assert payload["delivery_status"] == "failed_retryable"
    assert payload["attempt_count"] == 1
    assert payload["transport_error_code"] == "telegram_retryable_5xx"
    assert payload["transport_error_class"] == "retryable_transport"


def test_runbook_documents_safety_and_execution_contract() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "notification.delivery.result.v1" in text
    assert "q.maintenance" in text
    assert "notification.plan.created.v1" in text
    assert "retryable_due_promotion" in text
    assert "Redis DB 14" in text
    assert "does not call OpenAI" in text
    assert "must not call the Telegram Bot API" in text
    assert "does not start notifier-telegram" in text
    assert "notification_plans" in text
    assert "notification_delivery_records" in text
    assert "notification_renders" in text
