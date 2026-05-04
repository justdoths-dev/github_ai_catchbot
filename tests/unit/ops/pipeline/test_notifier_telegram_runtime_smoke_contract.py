from __future__ import annotations

import importlib
import inspect
import json
from pathlib import Path
from uuid import uuid4

import pytest

from services.policy_engine.models import NotificationPlanIntent
from services.policy_engine.repositories import PolicyEngineRepository


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "notifier_telegram_runtime_smoke.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "notifier_telegram_runtime_smoke.md"


def _module():
    return importlib.import_module("scripts.ops.notifier_telegram_runtime_smoke")


def _fake_database_url() -> str:
    password = "super" + "secret"
    return f"postgresql+psycopg://user:{password}@localhost:5432/github_ai_catchbot_smoke"


def _fake_redis_url() -> str:
    password = "redis" + "secret"
    return f"redis://:{password}@localhost:6379/14"


def _valid_payload(module, *, event_id=None, analysis_id=None) -> dict[str, str]:
    event_id = event_id or uuid4()
    analysis_id = analysis_id or uuid4()
    return module._build_expected_redis_payload(
        event_id=event_id,
        analysis_id=analysis_id,
        marker=f"{module.SMOKE_MARKER_PREFIX}{event_id.hex}",
    )


class _CapturingPolicySession:
    def __init__(self) -> None:
        self.statement = ""
        self.params: dict[str, object] = {}

    def in_transaction(self) -> bool:
        return True

    def begin(self):  # pragma: no cover - not used by this focused repository method
        raise AssertionError("transaction context is not needed for this contract test")

    async def execute(self, statement, params=None):
        self.statement = str(statement)
        self.params = dict(params or {})
        return None


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
    analysis_id = uuid4()
    fields = _valid_payload(module, event_id=event_id, analysis_id=analysis_id)

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_analysis_id=analysis_id,
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
        ("analysis_id", str(uuid4())),
        ("candidate_group_id", str(uuid4())),
        ("delivery_decision", "send_now"),
        ("urgency_profile", "normal_silent"),
        ("target_chat_id", "12345"),
        ("material_change_hash", "abc"),
        ("database_url", "postgresql+psycopg://user:secret@localhost:5432/db"),
        ("api_token", "secret-token"),
    ],
)
def test_thin_redis_payload_validation_rejects_business_and_secret_fields(field_name, field_value) -> None:
    module = _module()
    event_id = uuid4()
    analysis_id = uuid4()
    fields = _valid_payload(module, event_id=event_id, analysis_id=analysis_id)
    fields[field_name] = field_value

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_analysis_id=analysis_id,
        database_url=_fake_database_url(),
        redis_url="redis://localhost:6379/14",
    )

    assert any(field_name in failure or "URL or credential" in failure for failure in failures)


def test_event_queue_stage_and_output_names_are_locked() -> None:
    module = _module()

    assert module.EXPECTED_EVENT_TYPE == "notification.plan.created.v1"
    assert module.EXPECTED_QUEUE_NAME == "q.notification.send"
    assert module.EXPECTED_STAGE_NAME == "notify"
    assert module.EXPECTED_AGGREGATE_TYPE == "analysis"
    assert module.EXPECTED_DOWNSTREAM_EVENT_TYPE == "notification.delivery.result.v1"
    assert module.EXPECTED_DOWNSTREAM_QUEUE_NAME == "q.maintenance"


@pytest.mark.asyncio
async def test_smoke_root_object_matches_policy_engine_notification_plan_event_contract() -> None:
    module = _module()
    session = _CapturingPolicySession()
    intent = NotificationPlanIntent(
        notification_plan_id=uuid4(),
        analysis_id=uuid4(),
        candidate_group_id=uuid4(),
        delivery_decision="send_now",
        urgency_profile="normal_silent",
        target_chat_id=12345,
        target_thread_id=None,
        render_profile="telegram_single_alert_normal_v1",
        dedupe_subject_key="subject",
        material_change_hash="material",
        send_after="2026-05-04T00:00:00+00:00",
        suppress_reason_code=None,
    )

    await PolicyEngineRepository(session).insert_notification_plan_created_outbox(intent)

    assert "'notification.plan.created.v1'" in session.statement
    assert "'analysis'" in session.statement
    assert "CAST(:analysis_id AS uuid)" in session.statement
    assert session.params["analysis_id"] == str(intent.analysis_id)
    assert module.EXPECTED_AGGREGATE_TYPE == "analysis"
    expected_payload = module._build_expected_redis_payload(
        event_id=uuid4(),
        analysis_id=intent.analysis_id,
        marker=f"{module.SMOKE_MARKER_PREFIX}contract",
    )
    assert expected_payload["root_object_type"] == "analysis"
    assert expected_payload["root_object_id"] == str(intent.analysis_id)


def test_notifier_rehydrates_business_data_from_trigger_event_payload_not_aggregate_fields() -> None:
    source = inspect.getsource(__import__("services.notifier_telegram.repositories", fromlist=["NotifierTelegramRepository"]))

    assert "WHERE event_id = CAST(:event_id AS uuid)" in source
    assert "payload_json" in source
    assert "notification_plan_id = _uuid_or_none(payload.get(\"notification_plan_id\"))" in source
    assert "analysis_id = _uuid_or_none(payload.get(\"analysis_id\"))" in source
    assert "aggregate_id" not in source.split("async def load_notification_plan", 1)[0]


def test_stringify_row_decodes_only_json_allowlisted_columns(monkeypatch) -> None:
    module = _module()
    calls: list[str] = []

    def fake_json_loads(value):
        calls.append(value)
        if value == '{"confidence": 55}':
            return {"confidence": 55}
        if value == '["runtime_smoke_fixture"]':
            return ["runtime_smoke_fixture"]
        if value == '{"dry_run": true}':
            return {"dry_run": True}
        raise AssertionError(f"unexpected json.loads call for {value!r}")

    monkeypatch.setattr(module.json, "loads", fake_json_loads)

    rendered = module._stringify_row(
        {
            "schema_version": "analysis_v1",
            "policy_version": "verdict_policy_v1",
            "prompt_version": "judge_github_primary_v1",
            "delivery_policy_version": "delivery_policy_v1",
            "verdict": "later",
            "delivery_decision": "send_now",
            "model_proposed_verdict": "later",
            "status": "pending",
            "delivery_status": "suppressed",
            "transport_error_code": "dry_run_skip_transport",
            "scores_json": '{"confidence": 55}',
            "reason_codes_json": '["runtime_smoke_fixture"]',
            "telegram_response_json": '{"dry_run": true}',
        }
    )

    assert rendered["schema_version"] == "analysis_v1"
    assert rendered["policy_version"] == "verdict_policy_v1"
    assert rendered["prompt_version"] == "judge_github_primary_v1"
    assert rendered["delivery_policy_version"] == "delivery_policy_v1"
    assert rendered["verdict"] == "later"
    assert rendered["delivery_decision"] == "send_now"
    assert rendered["model_proposed_verdict"] == "later"
    assert rendered["status"] == "pending"
    assert rendered["delivery_status"] == "suppressed"
    assert rendered["transport_error_code"] == "dry_run_skip_transport"
    assert rendered["scores_json"] == {"confidence": 55}
    assert rendered["reason_codes_json"] == ["runtime_smoke_fixture"]
    assert rendered["telegram_response_json"] == {"dry_run": True}
    assert calls == ['{"confidence": 55}', '["runtime_smoke_fixture"]', '{"dry_run": true}']


def test_notification_plan_payload_shape_matches_notifier_intent_contract() -> None:
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
        notification_plan_event_id=uuid4(),
    )
    payload = module._build_notification_plan_payload(
        seed_ids=seed_ids,
        seed_shape=module._build_seed_shape("abc123abc123abc123abc123abc123ab"),
    )

    assert payload["notification_plan_id"] == str(seed_ids.notification_plan_id)
    assert payload["analysis_id"] == str(seed_ids.analysis_id)
    assert payload["candidate_group_id"] == str(seed_ids.candidate_group_id)
    assert payload["delivery_decision"] == "send_now"
    assert payload["urgency_profile"] == "normal_silent"
    assert payload["target_chat_id"] == 12345
    assert payload["render_profile"] == "telegram_single_alert_normal_v1"
    assert payload["dedupe_subject_key"] == str(seed_ids.candidate_group_id)
    assert payload["material_change_hash"]
    assert payload["send_after"]


def test_report_shape_is_stable() -> None:
    module = _module()
    report = module._new_report(module._build_seed_shape("abc123abc123abc123abc123abc123ab"))
    rendered = json.loads(module._render_json(report))

    assert set(rendered) == {
        "report_type",
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
        "notification_ids",
        "notification_delivery_result_outbox_ids",
        "state_transitions",
    }
    assert rendered["report_type"] == "notifier_telegram_runtime_smoke_v1"
    assert rendered["queue_name"] == "q.notification.send"
    assert rendered["transport_safety"]["enable_notification_send"] is False
    assert rendered["transport_safety"]["notifier_telegram_dry_run"] is True
    assert rendered["transport_safety"]["notifier_telegram_allow_edits"] is False
    assert rendered["transport_safety"]["telegram_client_injected"] is False
    assert rendered["mutation_booleans"] == {
        "analysis_mutated": False,
        "judge_output_mutated": False,
        "candidate_group_mutated": False,
    }


def test_no_openai_or_direct_external_network_imports_or_calls() -> None:
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
    assert "send_message(" not in text
    assert "edit_message_text(" not in text


def test_script_uses_existing_route_worker_service_and_repository_paths() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "OutboxRouteResolver" in text
    assert "RedisStreamsPublisher" in text
    assert "OutboxRelayRepository" in text
    assert "NotifierTelegramWorker" in text
    assert "NotifierTelegramService" in text
    assert "NotifierTelegramRepository" in text
    assert "SessionBackedNotifierTelegramService" in text
    assert "handle_trigger_event(trigger_event_id)" in text


def test_script_uses_trigger_event_id_rehydration_and_thin_redis_contract() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert '"trigger_event_id"' in text
    assert "load_intent_job" not in text
    assert "payload_json" in text
    assert "FORBIDDEN_REDIS_FIELDS" in text
    assert "expected_analysis_id" in text


def test_postcondition_and_mutation_checks_exist() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "notification_plans" in text
    assert "notification_renders" in text
    assert "notification_delivery_records" in text
    assert "notification.delivery.result.v1" in text
    assert "db.analyses_not_mutated" in text
    assert "db.judge_outputs_not_mutated" in text
    assert "db.candidate_group_proposals_not_mutated" in text
    assert "dry_run_skip_transport" in text
    assert "transport_skipped" in text


def test_runbook_documents_safety_and_execution_contract() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "notification.plan.created.v1" in text
    assert "q.notification.send" in text
    assert "notification.delivery.result.v1" in text
    assert "q.maintenance" in text
    assert "Redis DB 14" in text
    assert "does not call OpenAI" in text
    assert "must not call the Telegram Bot API" in text
    assert "notification_renders" in text
    assert "notification_delivery_records" in text
