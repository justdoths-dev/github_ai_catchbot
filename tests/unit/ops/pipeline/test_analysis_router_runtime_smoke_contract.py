from __future__ import annotations

import importlib
import json
from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "analysis_router_runtime_smoke.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "analysis_router_runtime_smoke.md"


def _fake_database_url() -> str:
    password = "super" + "secret"
    return f"postgresql+psycopg://user:{password}@localhost:5432/github_ai_catchbot_smoke"


def _fake_redis_url() -> str:
    password = "redis" + "secret"
    return f"redis://:{password}@localhost:6379/14"


def _valid_payload(module, *, event_id=None, candidate_group_id=None) -> dict[str, str]:
    event_id = event_id or uuid4()
    candidate_group_id = candidate_group_id or uuid4()
    return module._build_expected_redis_payload(
        event_id=event_id,
        candidate_group_id=candidate_group_id,
        marker=f"{module.SMOKE_MARKER_PREFIX}{event_id.hex}",
    )


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_cli_requires_confirm_write() -> None:
    module = importlib.import_module("scripts.ops.analysis_router_runtime_smoke")
    parser = module.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--redis-url", "redis://localhost:6379/14"])


def test_parser_accepts_explicit_smoke_redis_url() -> None:
    module = importlib.import_module("scripts.ops.analysis_router_runtime_smoke")
    parser = module.build_parser()

    args = parser.parse_args(["--redis-url", "redis://localhost:6379/14", "--confirm", "write"])

    assert args.redis_url == "redis://localhost:6379/14"
    assert args.confirm == "write"


def test_database_url_source_is_smoke_env_var_not_database_cli_arg() -> None:
    module = importlib.import_module("scripts.ops.analysis_router_runtime_smoke")
    parser = module.build_parser()

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
    module = importlib.import_module("scripts.ops.analysis_router_runtime_smoke")

    assert module._is_production_like_url("postgresql+psycopg://user@db-prod:5432/catchbot")
    assert module._is_production_like_url("redis://production-redis:6379/14")
    assert module._is_production_like_url("rediss://localhost:6379/14")
    assert not module._is_production_like_url("redis://localhost:6379/14")


def test_database_guard_requires_local_smoke_test_or_dev_database() -> None:
    module = importlib.import_module("scripts.ops.analysis_router_runtime_smoke")

    assert module._is_expected_smoke_database_url(
        "postgresql+psycopg://user:secret@localhost:5432/github_ai_catchbot_smoke"
    )
    assert module._is_expected_smoke_database_url("postgresql://user@127.0.0.1:5432/catchbot_test")
    assert not module._is_expected_smoke_database_url("postgresql+psycopg://user@db.internal:5432/catchbot_smoke")
    assert not module._is_expected_smoke_database_url("postgresql+psycopg://user@localhost:5432/catchbot")
    assert not module._is_expected_smoke_database_url("mysql://user@localhost:3306/catchbot_smoke")


def test_redis_db14_guard_requires_local_db14() -> None:
    module = importlib.import_module("scripts.ops.analysis_router_runtime_smoke")

    assert module._is_expected_redis_db14("redis://localhost:6379/14")
    assert module._is_expected_redis_db14("redis://127.0.0.1:6379/14")
    assert not module._is_expected_redis_db14("redis://localhost:6379/0")
    assert not module._is_expected_redis_db14("redis://example.com:6379/14")


def test_redaction_helper_removes_database_and_redis_url_fragments() -> None:
    module = importlib.import_module("scripts.ops.analysis_router_runtime_smoke")
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
    module = importlib.import_module("scripts.ops.analysis_router_runtime_smoke")
    event_id = uuid4()
    candidate_group_id = uuid4()
    fields = _valid_payload(module, event_id=event_id, candidate_group_id=candidate_group_id)

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_candidate_group_id=candidate_group_id,
        database_url=_fake_database_url(),
        redis_url="redis://localhost:6379/14",
    )

    assert failures == []
    assert set(fields) == module.REQUIRED_REDIS_FIELDS


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("payload_json", "{}"),
        ("candidate_group_id", str(uuid4())),
        ("bundle_id", str(uuid4())),
        ("judge_profile", "github_primary"),
        ("escalation_allowed", "true"),
        ("database_url", "postgresql+psycopg://user:secret@localhost:5432/db"),
        ("api_token", "secret-token"),
    ],
)
def test_thin_redis_payload_validation_rejects_business_and_secret_fields(field_name, field_value) -> None:
    module = importlib.import_module("scripts.ops.analysis_router_runtime_smoke")
    event_id = uuid4()
    candidate_group_id = uuid4()
    fields = _valid_payload(module, event_id=event_id, candidate_group_id=candidate_group_id)
    fields[field_name] = field_value

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_candidate_group_id=candidate_group_id,
        database_url=_fake_database_url(),
        redis_url="redis://localhost:6379/14",
    )

    assert any(field_name in failure or "URL or credential" in failure for failure in failures)


def test_seed_shape_preserves_locked_colon_based_github_repo_canonical_id() -> None:
    module = importlib.import_module("scripts.ops.analysis_router_runtime_smoke")
    seed_shape = module._build_seed_shape("abc123abc123abc123abc123abc123ab")

    assert seed_shape.canonical_id == "github:repo:octocat/analysis-router-smoke-abc123abc123"
    assert seed_shape.canonical_id.startswith("github:repo:")
    assert "github_repo:" not in seed_shape.canonical_id
    assert "github_repo:" not in SCRIPT.read_text(encoding="utf-8")


def test_analysis_requested_payload_shape() -> None:
    module = importlib.import_module("scripts.ops.analysis_router_runtime_smoke")
    seed_ids = module.SmokeSeedIds(
        source_message_id=uuid4(),
        artifact_id=uuid4(),
        snapshot_id=uuid4(),
        candidate_group_id=uuid4(),
        bundle_id=uuid4(),
        analysis_event_id=uuid4(),
    )
    payload = module._build_analysis_requested_payload(
        seed_ids=seed_ids,
        seed_shape=module._build_seed_shape("abc123abc123abc123abc123abc123ab"),
    )

    assert payload["event_id"] == str(seed_ids.analysis_event_id)
    assert payload["candidate_group_id"] == str(seed_ids.candidate_group_id)
    assert payload["bundle_id"] == str(seed_ids.bundle_id)
    assert payload["judge_profile"] == "github_primary"
    assert payload["escalation_allowed"] is True


def test_expected_judge_call_requested_payload_shape() -> None:
    module = importlib.import_module("scripts.ops.analysis_router_runtime_smoke")
    judge_run_id = uuid4()
    bundle_id = uuid4()

    payload = module._expected_judge_call_payload(judge_run_id=judge_run_id, bundle_id=bundle_id)

    assert payload == {
        "judge_run_id": str(judge_run_id),
        "bundle_id": str(bundle_id),
        "model": "gpt-5.4-mini",
        "reasoning_effort": "low",
        "prompt_version": "judge_github_primary_v1",
        "prompt_cache_key": "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1",
    }


def test_report_shape_is_stable() -> None:
    module = importlib.import_module("scripts.ops.analysis_router_runtime_smoke")
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
        "resulting_judge_run_ids",
        "downstream_outbox_ids",
        "forbidden_side_effect_counts",
        "external_network_calls_attempted",
    }
    assert rendered["report_type"] == "analysis_router_runtime_smoke_v1"
    assert rendered["database_url_redacted"] is True
    assert rendered["redis_url_redacted"] is True
    assert rendered["queue_name"] == "q.analysis.route"
    assert rendered["external_network_calls_attempted"] is False


def test_no_external_network_or_openai_requirement() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "import openai" not in text
    assert "from openai" not in text
    assert "import requests" not in text
    assert "import httpx" not in text
    assert "import aiohttp" not in text
    assert "urllib.request" not in text
    assert "GitHubClient" not in text
    assert "OpenAI" not in text.replace("It does not call OpenAI", "")


def test_analysis_count_query_uses_schema_valid_judge_output_join() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    analysis_query = text.split('outputs["analysis_count"] = int(', maxsplit=1)[1].split(
        'outputs["judge_output_count"] = int(', maxsplit=1
    )[0]

    assert "FROM analyses a" in analysis_query
    assert "LEFT JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id" in analysis_query
    assert "LEFT JOIN judge_runs jr ON jr.judge_run_id = jo.judge_run_id" in analysis_query
    assert "a.candidate_group_id = CAST(:candidate_group_id AS uuid)" in analysis_query
    assert "jr.bundle_id = CAST(:bundle_id AS uuid)" in analysis_query
    assert "FROM analyses\n            WHERE" not in analysis_query
    assert "judge_run_id IN" not in analysis_query


def test_runbook_documents_manual_env_and_boundaries() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" in text
    assert "REDIS_URL" in text
    assert "q.analysis.route" in text
    assert "does not call OpenAI" in text
    assert "does not call OpenAI, external network" in text
    assert "checks_failed" in text
