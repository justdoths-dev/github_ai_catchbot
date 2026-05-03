from __future__ import annotations

import importlib
import json
from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "analysis_validator_runtime_smoke.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "analysis_validator_runtime_smoke.md"


def _module():
    return importlib.import_module("scripts.ops.analysis_validator_runtime_smoke")


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
        ("finish_reason", "completed"),
        ("refusal_detected", "false"),
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


def test_event_queue_and_stage_names_are_locked() -> None:
    module = _module()

    assert module.EXPECTED_EVENT_TYPE == "judge.output.ready.v1"
    assert module.EXPECTED_QUEUE_NAME == "q.analysis.validate"
    assert module.EXPECTED_STAGE_NAME == "analysis_validate"
    assert module.EXPECTED_DOWNSTREAM_EVENT_TYPE == "analysis.policy.apply.v1"


def test_judge_output_ready_payload_shape() -> None:
    module = _module()
    seed_ids = module.SmokeSeedIds(
        source_message_id=uuid4(),
        artifact_id=uuid4(),
        snapshot_id=uuid4(),
        candidate_group_id=uuid4(),
        bundle_id=uuid4(),
        judge_run_id=uuid4(),
        judge_output_id=uuid4(),
        judge_output_ready_event_id=uuid4(),
    )
    payload = module._build_judge_output_ready_payload(
        seed_ids=seed_ids,
        seed_shape=module._build_seed_shape("abc123abc123abc123abc123abc123ab"),
    )

    assert payload["judge_run_id"] == str(seed_ids.judge_run_id)
    assert payload["judge_output_id"] == str(seed_ids.judge_output_id)
    assert payload["finish_reason"] == "completed"
    assert payload["refusal_detected"] is False
    assert payload["smoke_marker"].startswith(module.SMOKE_MARKER_PREFIX)


def test_valid_judge_output_payload_shape() -> None:
    module = _module()
    seed_ids = module.SmokeSeedIds(
        source_message_id=uuid4(),
        artifact_id=uuid4(),
        snapshot_id=uuid4(),
        candidate_group_id=uuid4(),
        bundle_id=uuid4(),
        judge_run_id=uuid4(),
        judge_output_id=uuid4(),
        judge_output_ready_event_id=uuid4(),
    )

    payload = module._build_valid_judge_output_payload(
        seed_ids=seed_ids,
        seed_shape=module._build_seed_shape("abc123abc123abc123abc123abc123ab"),
    )

    assert payload["judge_schema_version"] == "judge_output_v1"
    assert payload["candidate_group_id"] == str(seed_ids.candidate_group_id)
    assert payload["model_proposed_verdict"] == "later"
    assert payload["model_confidence_band"] == "medium"
    assert payload["comparables"]
    assert payload["reason_codes"]
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


def test_judge_output_snapshot_normalization_only_decodes_payload_json() -> None:
    module = _module()
    judge_output_id = uuid4()
    judge_run_id = uuid4()
    candidate_group_id = uuid4()
    payload = {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate_group_id),
        "model_proposed_verdict": "later",
        "model_confidence_band": "medium",
        "reason_codes": ["runtime_smoke_fixture"],
    }

    normalized = module._normalize_judge_output_snapshot_row(
        {
            "judge_output_id": judge_output_id,
            "judge_run_id": judge_run_id,
            "candidate_group_id": candidate_group_id,
            "judge_schema_version": "judge_output_v1",
            "payload_json": json.dumps(payload, sort_keys=True),
            "model_proposed_verdict": "later",
            "model_confidence_band": "medium",
            "created_at": "2026-05-03T00:00:00+00:00",
        }
    )

    assert normalized == {
        "judge_output_id": str(judge_output_id),
        "judge_run_id": str(judge_run_id),
        "candidate_group_id": str(candidate_group_id),
        "judge_schema_version": "judge_output_v1",
        "payload_json": payload,
        "model_proposed_verdict": "later",
        "model_confidence_band": "medium",
        "created_at": "2026-05-03T00:00:00+00:00",
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
        "downstream_outbox_ids",
        "redis_message_ids",
        "db_postcondition_counts",
        "forbidden_side_effect_counts",
        "judge_output_mutated",
        "external_network_calls_attempted",
    }
    assert rendered["report_type"] == "analysis_validator_runtime_smoke_v1"
    assert rendered["database_url_redacted"] is True
    assert rendered["redis_url_redacted"] is True
    assert rendered["queue_name"] == "q.analysis.validate"
    assert rendered["external_network_calls_attempted"] is False


def test_no_openai_or_external_network_imports_or_calls() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "import openai" not in text
    assert "from openai" not in text
    assert "OPENAI_API_KEY" not in text
    assert "OpenAIJudgeClient" not in text
    assert "import requests" not in text
    assert "import httpx" not in text
    assert "import aiohttp" not in text
    assert "urllib.request" not in text


def test_no_production_db_redis_defaults() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "postgresql://" not in text.replace("postgresql+psycopg://...@", "")
    assert "redis://localhost:6379/0" not in text
    assert "redis://localhost:6379/14" in text
    assert "DATABASE_URL_ENV" in text
    assert "REDIS_URL_ENV" in text


def test_no_writes_to_analyses_or_notification_tables_except_count_verification() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    forbidden_writes = [
        "INSERT INTO analyses",
        "UPDATE analyses",
        "DELETE FROM analyses",
        "INSERT INTO notification_plans",
        "UPDATE notification_plans",
        "DELETE FROM notification_plans",
        "INSERT INTO notification_renders",
        "UPDATE notification_renders",
        "DELETE FROM notification_renders",
        "INSERT INTO notification_delivery_records",
        "UPDATE notification_delivery_records",
        "DELETE FROM notification_delivery_records",
    ]
    for forbidden in forbidden_writes:
        assert forbidden not in text

    assert "FROM analyses" in text
    assert "FROM notification_plans" in text
    assert "FROM notification_renders" in text
    assert "FROM notification_delivery_records" in text


def test_smoke_does_not_mutate_judge_outputs_after_seed() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "UPDATE judge_outputs" not in text
    assert "DELETE FROM judge_outputs" not in text
    assert "INSERT INTO judge_outputs" in text
    assert "db.judge_outputs_not_mutated" in text


def test_smoke_uses_trigger_event_id_rehydration_path() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "trigger_event_id" in text
    assert "SessionBackedAnalysisValidatorService" in text
    assert "AnalysisValidatorRepository" in text
    assert "AnalysisValidatorService" in text
    assert "AnalysisValidatorWorker" in text
    assert "handle_trigger_event" in text


def test_forbidden_side_effect_counts_are_reported() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "forbidden_side_effect_counts" in text
    assert "\"analyses\"" in text
    assert "\"notification_plans\"" in text
    assert "\"notification_renders\"" in text
    assert "\"notification_delivery_records\"" in text


def test_runbook_documents_manual_env_and_boundaries() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" in text
    assert "REDIS_URL" in text
    assert "q.analysis.validate" in text
    assert "does not call OpenAI" in text
    assert "candidate_evidence_bundles" in text
    assert "checks_failed" in text
