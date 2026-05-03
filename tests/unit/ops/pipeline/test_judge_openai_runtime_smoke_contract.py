from __future__ import annotations

import importlib
import json
import sys
import time
from pathlib import Path
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.judge_openai.response_mapper import OpenAIResponseMapper


SCRIPT = ROOT / "scripts" / "ops" / "judge_openai_runtime_smoke.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "judge_openai_runtime_smoke.md"


def _module():
    return importlib.import_module("scripts.ops.judge_openai_runtime_smoke")


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


def _extract_insert_sql_block(table_name: str) -> str:
    text = SCRIPT.read_text(encoding="utf-8")
    marker = f"INSERT INTO {table_name}"
    marker_index = text.index(marker)
    block_start = text.rfind('"""', 0, marker_index)
    block_end = text.index('"""', marker_index)
    return text[block_start:block_end]


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_cli_requires_confirm_write() -> None:
    parser = _module().build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--redis-url", "redis://localhost:6379/14"])


def test_parser_accepts_explicit_smoke_redis_url() -> None:
    parser = _module().build_parser()

    args = parser.parse_args(["--redis-url", "redis://localhost:6379/14", "--confirm", "write"])

    assert args.redis_url == "redis://localhost:6379/14"
    assert args.confirm == "write"


def test_database_url_source_is_smoke_env_var_not_database_cli_arg() -> None:
    parser = _module().build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--database-url",
                _fake_database_url(),
                "--redis-url",
                "redis://localhost:6379/14",
                "--confirm",
                "write",
            ]
        )


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
        ("bundle_id", str(uuid4())),
        ("model", "gpt-5.4-mini"),
        ("reasoning_effort", "low"),
        ("prompt_cache_key", "judge:github_primary:v1"),
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


def test_seed_shape_preserves_locked_colon_based_github_repo_canonical_id() -> None:
    module = _module()
    seed_shape = module._build_seed_shape("abc123abc123abc123abc123abc123ab")

    assert seed_shape.canonical_id == "github:repo:octocat/judge-openai-smoke-abc123abc123"
    assert seed_shape.canonical_id.startswith("github:repo:")
    assert "github_repo:" not in seed_shape.canonical_id
    assert "github_repo:" not in SCRIPT.read_text(encoding="utf-8")


def test_judge_call_requested_payload_shape() -> None:
    module = _module()
    seed_ids = module.SmokeSeedIds(
        source_message_id=uuid4(),
        artifact_id=uuid4(),
        snapshot_id=uuid4(),
        candidate_group_id=uuid4(),
        bundle_id=uuid4(),
        judge_run_id=uuid4(),
        judge_event_id=uuid4(),
    )
    payload = module._build_judge_call_requested_payload(
        seed_ids=seed_ids,
        seed_shape=module._build_seed_shape("abc123abc123abc123abc123abc123ab"),
    )

    assert payload["judge_run_id"] == str(seed_ids.judge_run_id)
    assert payload["bundle_id"] == str(seed_ids.bundle_id)
    assert payload["model"] == "gpt-5.4-mini"
    assert payload["reasoning_effort"] == "low"
    assert payload["prompt_version"] == "judge_github_primary_v1"
    assert payload["prompt_cache_key"] == (
        "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1"
    )
    assert payload["smoke_marker"].startswith(module.SMOKE_MARKER_PREFIX)


def test_judge_runs_seed_insert_uses_canonical_timestamp_column() -> None:
    block = _extract_insert_sql_block("judge_runs")

    assert "created_at" not in block
    assert "started_at" in block


def test_fake_openai_client_structured_response_shape() -> None:
    module = _module()
    payload = module._build_fake_judge_output_payload(
        candidate_group_id=str(uuid4()),
        primary_summary={"repo_full_name": "octocat/judge-openai-smoke"},
    )

    assert payload["judge_schema_version"] == "judge_output_v1"
    assert payload["model_proposed_verdict"] == "later"
    assert payload["model_confidence_band"] == "medium"
    assert set(payload) >= {
        "headline",
        "scores",
        "reason_codes",
        "model_proposed_verdict",
        "model_confidence_band",
    }
    assert payload["scores"]["reproducibility_signal"] is None


@pytest.mark.asyncio
async def test_fake_openai_client_records_bundle_only_context_keys() -> None:
    module = _module()
    candidate_group_id = str(uuid4())
    client = module.DeterministicFakeOpenAIClient()

    response = await client.create_structured_response(
        model="gpt-5.4-mini",
        reasoning_effort="low",
        developer_prompt="Judge only from the provided evidence bundle.",
        user_context=json.dumps(
            {
                "candidate_group_id": candidate_group_id,
                "bundle_id": str(uuid4()),
                "current_primary_artifact_id": str(uuid4()),
                "primary_summary": {"repo_full_name": "octocat/judge-openai-smoke"},
                "supporting_summaries": [],
                "discovered_links_summary": [],
                "evidence_limitations": [],
                "token_budget_profile": "small",
                "reroot_count": 0,
            },
            sort_keys=True,
        ),
        json_schema={"properties": {"judge_schema_version": {"type": "string"}}},
        max_output_tokens=800,
        prompt_cache_key="judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1",
    )

    payload = json.loads(response.output_text)
    assert payload["candidate_group_id"] == candidate_group_id
    assert response.id == f"fake-response-{candidate_group_id}"
    assert response.status == module.EXPECTED_FINISH_REASON
    assert response.usage.input_tokens == 123
    assert response.usage.input_tokens_details.cached_tokens == 23
    assert response.usage.output_tokens == 45
    assert response.usage.output_tokens_details.reasoning_tokens == 7
    assert client.calls[0]["used_fake_client"] is True
    assert set(client.calls[0]["context_keys"]) == module.MODEL_CONTEXT_ALLOWED_KEYS


@pytest.mark.asyncio
async def test_fake_openai_response_maps_through_production_response_mapper() -> None:
    module = _module()
    candidate_group_id = str(uuid4())
    client = module.DeterministicFakeOpenAIClient()

    response = await client.create_structured_response(
        model="gpt-5.4-mini",
        reasoning_effort="low",
        developer_prompt="Judge only from the provided evidence bundle.",
        user_context=json.dumps(
            {
                "candidate_group_id": candidate_group_id,
                "bundle_id": str(uuid4()),
                "current_primary_artifact_id": str(uuid4()),
                "primary_summary": {"repo_full_name": "octocat/judge-openai-smoke"},
                "supporting_summaries": [],
                "discovered_links_summary": [],
                "evidence_limitations": [],
                "token_budget_profile": "small",
                "reroot_count": 0,
            },
            sort_keys=True,
        ),
        json_schema={"properties": {"judge_schema_version": {"type": "string"}}},
        max_output_tokens=800,
        prompt_cache_key="judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1",
    )

    result = OpenAIResponseMapper().parse(response, started_monotonic=time.monotonic())

    assert result.payload_json is not None
    assert result.payload_json["judge_schema_version"] == "judge_output_v1"
    assert result.finish_reason == module.EXPECTED_FINISH_REASON
    assert result.raw_response_id == f"fake-response-{candidate_group_id}"
    assert result.usage.input_tokens == 123
    assert result.usage.cached_input_tokens == 23
    assert result.usage.output_tokens == 45
    assert result.usage.reasoning_tokens == 7


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
        "resulting_judge_output_ids",
        "downstream_outbox_ids",
        "fake_openai_calls",
        "forbidden_side_effect_counts",
        "external_network_calls_attempted",
    }
    assert rendered["report_type"] == "judge_openai_runtime_smoke_v1"
    assert rendered["database_url_redacted"] is True
    assert rendered["redis_url_redacted"] is True
    assert rendered["queue_name"] == "q.analysis.judge"
    assert rendered["external_network_calls_attempted"] is False


def test_no_real_openai_client_or_api_key_requirement_in_smoke() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "import openai" not in text
    assert "from openai" not in text
    assert "OpenAIJudgeClient(" not in text
    assert "JudgeOpenAIConfig.from_env" not in text
    assert "OPENAI_API_KEY" not in text
    assert "os.getenv(DATABASE_URL_ENV" in text
    assert "os.getenv(REDIS_URL_ENV" in text


def test_forbidden_side_effect_checks_cover_analyses_and_notification_tables() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "outputs[\"analysis_count\"]" in text
    assert "FROM analyses a" in text
    assert "outputs[\"notification_plan_count\"]" in text
    assert "FROM notification_plans" in text
    assert "outputs[\"notification_render_count\"]" in text
    assert "FROM notification_renders" in text
    assert "outputs[\"notification_delivery_count\"]" in text
    assert "FROM notification_delivery_records" in text


def test_runbook_documents_manual_env_and_boundaries() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" in text
    assert "REDIS_URL" in text
    assert "q.analysis.judge" in text
    assert "does not call the real OpenAI API" in text
    assert "candidate_evidence_bundles" in text
    assert "checks_failed" in text
