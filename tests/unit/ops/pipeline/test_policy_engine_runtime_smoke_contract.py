from __future__ import annotations

import importlib
import json
from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "policy_engine_runtime_smoke.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "policy_engine_runtime_smoke.md"


def _module():
    return importlib.import_module("scripts.ops.policy_engine_runtime_smoke")


def _fake_database_url() -> str:
    password = "super" + "secret"
    return f"postgresql+psycopg://user:{password}@localhost:5432/github_ai_catchbot_smoke"


def _fake_redis_url() -> str:
    password = "redis" + "secret"
    return f"redis://:{password}@localhost:6379/14"


def _valid_payload(module, *, event_id=None, judge_run_id=None) -> dict[str, str]:
    event_id = event_id or uuid4()
    judge_run_id = judge_run_id or uuid4()
    return module._build_expected_redis_payload(
        event_id=event_id,
        judge_run_id=judge_run_id,
        marker=f"{module.SMOKE_MARKER_PREFIX}{event_id.hex}",
    )


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_cli_requires_confirm_write() -> None:
    parser = _module().build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--database-url",
                _fake_database_url(),
                "--redis-url",
                "redis://localhost:6379/14",
            ]
        )


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


def test_production_like_execution_is_refused_by_default() -> None:
    module = _module()

    assert module._is_production_like_url("postgresql+psycopg://user@db-prod:5432/catchbot")
    assert module._is_production_like_url("redis://production-redis:6379/14")
    assert module._is_production_like_url("rediss://localhost:6379/14")
    assert not module._is_production_like_url("redis://localhost:6379/14")


def test_database_guard_requires_local_smoke_test_or_dev_database() -> None:
    module = _module()

    assert module._is_expected_smoke_database_url(
        "postgresql+psycopg://user:secret@localhost:5432/github_ai_catchbot_smoke"
    )
    assert module._is_expected_smoke_database_url("postgresql://user@127.0.0.1:5432/catchbot_test")
    assert not module._is_expected_smoke_database_url("postgresql+psycopg://user@db.internal:5432/catchbot_smoke")
    assert not module._is_expected_smoke_database_url("postgresql+psycopg://user@localhost:5432/catchbot")
    assert not module._is_expected_smoke_database_url("mysql://user@localhost:3306/catchbot_smoke")


def test_redis_db14_guard_requires_local_db14() -> None:
    module = _module()

    assert module._is_expected_redis_db14("redis://localhost:6379/14")
    assert module._is_expected_redis_db14("redis://127.0.0.1:6379/14")
    assert not module._is_expected_redis_db14("redis://localhost:6379/0")
    assert not module._is_expected_redis_db14("redis://example.com:6379/14")


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
    judge_run_id = uuid4()
    fields = _valid_payload(module, event_id=event_id, judge_run_id=judge_run_id)

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_judge_run_id=judge_run_id,
        database_url=_fake_database_url(),
        redis_url="redis://localhost:6379/14",
    )

    assert failures == []
    assert set(fields) == module.REQUIRED_REDIS_FIELDS


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("payload_json", "{}"),
        ("judge_run_id", str(uuid4())),
        ("judge_output_id", str(uuid4())),
        ("bundle_id", str(uuid4())),
        ("candidate_group_id", str(uuid4())),
        ("scores_json", "{}"),
        ("reason_codes_json", "[]"),
        ("model_proposed_verdict", "skip"),
        ("database_url", "postgresql+psycopg://user:secret@localhost:5432/db"),
        ("api_token", "secret-token"),
    ],
)
def test_thin_redis_payload_validation_rejects_business_and_secret_fields(field_name, field_value) -> None:
    module = _module()
    event_id = uuid4()
    judge_run_id = uuid4()
    fields = _valid_payload(module, event_id=event_id, judge_run_id=judge_run_id)
    fields[field_name] = field_value

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_judge_run_id=judge_run_id,
        database_url=_fake_database_url(),
        redis_url="redis://localhost:6379/14",
    )

    assert any(field_name in failure or "URL or credential" in failure for failure in failures)


def test_event_queue_stage_and_output_names_are_locked() -> None:
    module = _module()

    assert module.EXPECTED_EVENT_TYPE == "analysis.policy.apply.v1"
    assert module.EXPECTED_QUEUE_NAME == "q.analysis.policy"
    assert module.EXPECTED_STAGE_NAME == "analysis_policy"
    assert module.EXPECTED_DOWNSTREAM_EVENT_TYPE == "notification.plan.created.v1"


def test_policy_apply_payload_shape() -> None:
    module = _module()
    seed_ids = module.SmokeSeedIds(
        source_message_id=uuid4(),
        artifact_id=uuid4(),
        snapshot_id=uuid4(),
        candidate_group_id=uuid4(),
        bundle_id=uuid4(),
        judge_run_id=uuid4(),
        judge_output_id=uuid4(),
        policy_apply_event_id=uuid4(),
    )
    payload = module._build_policy_apply_payload(
        seed_ids=seed_ids,
        seed_shape=module._build_seed_shape("abc123abc123abc123abc123abc123ab"),
    )

    assert payload["judge_run_id"] == str(seed_ids.judge_run_id)
    assert payload["judge_output_id"] == str(seed_ids.judge_output_id)
    assert payload["candidate_group_id"] == str(seed_ids.candidate_group_id)
    assert payload["bundle_id"] == str(seed_ids.bundle_id)
    assert payload["smoke_marker"].startswith(module.SMOKE_MARKER_PREFIX)


def test_valid_judge_output_payload_recomputes_later_from_model_skip() -> None:
    module = _module()
    seed_ids = module.SmokeSeedIds(
        source_message_id=uuid4(),
        artifact_id=uuid4(),
        snapshot_id=uuid4(),
        candidate_group_id=uuid4(),
        bundle_id=uuid4(),
        judge_run_id=uuid4(),
        judge_output_id=uuid4(),
        policy_apply_event_id=uuid4(),
    )

    payload = module._build_valid_judge_output_payload(
        seed_ids=seed_ids,
        seed_shape=module._build_seed_shape("abc123abc123abc123abc123abc123ab"),
    )

    assert payload["judge_schema_version"] == "judge_output_v1"
    assert payload["candidate_group_id"] == str(seed_ids.candidate_group_id)
    assert payload["model_proposed_verdict"] == "skip"
    assert payload["model_confidence_band"] == "medium"
    assert payload["scores"]["practical_usefulness"] >= 45
    assert payload["scores"]["evidence_strength"] < 50
    assert payload["scores"]["confidence"] >= 35
    assert set(payload["scores"]) == {
        "novelty",
        "practical_usefulness",
        "evidence_strength",
        "hype_penalty",
        "confidence",
        "code_quality",
        "maintenance_signal",
        "specificity",
        "reproducibility_signal",
    }


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
        "seeded_ids",
        "redis_message_ids",
        "db_postcondition_counts",
        "forbidden_side_effect_counts",
        "mutation_booleans",
        "analysis_row",
        "notification_plan_created_outbox_ids",
        "external_network_calls_attempted",
    }
    assert rendered["report_type"] == "policy_engine_runtime_smoke_v1"
    assert rendered["database_url_redacted"] is True
    assert rendered["redis_url_redacted"] is True
    assert rendered["queue_name"] == "q.analysis.policy"
    assert rendered["mutation_booleans"] == {
        "judge_output_mutated": False,
        "candidate_evidence_bundle_mutated": False,
        "candidate_current_analysis_id_mutated": False,
    }
    assert rendered["external_network_calls_attempted"] is False


def test_no_openai_telegram_transport_or_external_network_imports_or_calls() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "import openai" not in text
    assert "from openai" not in text
    assert "telegram_transport" not in text
    assert "TelegramBotTransport" not in text
    assert "notifier_telegram" not in text
    assert "requests." not in text
    assert "import requests" not in text
    assert "httpx." not in text
    assert "import httpx" not in text
    assert "aiohttp" not in text
    assert "urllib.request" not in text
    assert "socket." not in text


def test_script_uses_existing_route_worker_service_and_repository_paths() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "OutboxRouteResolver" in text
    assert "RedisStreamsPublisher" in text
    assert "OutboxRelayRepository" in text
    assert "PolicyEngineWorker" in text
    assert "PolicyEngineService" in text
    assert "PolicyEngineRepository" in text
    assert "SessionBackedPolicyEngineService" in text
    assert "handle_trigger_event(trigger_event_id)" in text


def test_script_uses_trigger_event_id_rehydration_and_not_business_redis_payload() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert '"trigger_event_id"' in text
    assert "load_job_by_trigger_event_id" not in text
    assert "PolicyEngineService" in text
    assert "payload_json" in text
    assert "FORBIDDEN_REDIS_FIELDS" in text


def test_forbidden_notifier_side_effect_and_mutation_checks_exist() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "notification_plans" in text
    assert "notification_renders" in text
    assert "notification_delivery_records" in text
    assert "db.no_forbidden_notifier_side_effects" in text
    assert "db.judge_outputs_not_mutated" in text
    assert "db.candidate_evidence_bundles_not_mutated" in text
    assert "db.candidate_current_analysis_id_not_mutated" in text


def test_runbook_documents_safety_and_execution_contract() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "analysis.policy.apply.v1" in text
    assert "q.analysis.policy" in text
    assert "notification.plan.created.v1" in text
    assert "Redis DB 14" in text
    assert "does not call OpenAI" in text
    assert "does not start the notifier worker" in text
    assert "notification_renders" in text
    assert "notification_delivery_records" in text
