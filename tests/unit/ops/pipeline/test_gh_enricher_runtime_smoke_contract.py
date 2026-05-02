from __future__ import annotations

import importlib
import json
from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "gh_enricher_runtime_smoke.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "gh_enricher_runtime_smoke.md"


def _fake_database_url() -> str:
    password = "super" + "secret"
    return f"postgresql+psycopg://user:{password}@localhost:5432/github_ai_catchbot_smoke"


def _fake_redis_url() -> str:
    password = "redis" + "secret"
    return f"redis://:{password}@localhost:6379/14"


def _valid_payload(module, *, event_id=None, artifact_id=None) -> dict[str, str]:
    event_id = event_id or uuid4()
    artifact_id = artifact_id or uuid4()
    return module._build_expected_redis_payload(
        event_id=event_id,
        artifact_id=artifact_id,
        marker=f"{module.SMOKE_MARKER_PREFIX}{event_id.hex}",
    )


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_cli_requires_confirm_write() -> None:
    module = importlib.import_module("scripts.ops.gh_enricher_runtime_smoke")
    parser = module.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--redis-url", "redis://localhost:6379/14"])


def test_parser_accepts_explicit_smoke_redis_url() -> None:
    module = importlib.import_module("scripts.ops.gh_enricher_runtime_smoke")
    parser = module.build_parser()

    args = parser.parse_args(["--redis-url", "redis://localhost:6379/14", "--confirm", "write"])

    assert args.redis_url == "redis://localhost:6379/14"
    assert args.confirm == "write"


def test_database_url_source_is_smoke_env_var_not_database_cli_arg() -> None:
    module = importlib.import_module("scripts.ops.gh_enricher_runtime_smoke")
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
    module = importlib.import_module("scripts.ops.gh_enricher_runtime_smoke")

    assert module._is_production_like_url("postgresql+psycopg://user@db-prod:5432/catchbot")
    assert module._is_production_like_url("redis://production-redis:6379/14")
    assert module._is_production_like_url("rediss://localhost:6379/14")
    assert not module._is_production_like_url("redis://localhost:6379/14")


def test_database_guard_requires_local_smoke_test_or_dev_database() -> None:
    module = importlib.import_module("scripts.ops.gh_enricher_runtime_smoke")

    assert module._is_expected_smoke_database_url(
        "postgresql+psycopg://user:secret@localhost:5432/github_ai_catchbot_smoke"
    )
    assert module._is_expected_smoke_database_url("postgresql://user@127.0.0.1:5432/catchbot_test")
    assert not module._is_expected_smoke_database_url("postgresql+psycopg://user@db.internal:5432/catchbot_smoke")
    assert not module._is_expected_smoke_database_url("postgresql+psycopg://user@localhost:5432/catchbot")


def test_redis_db14_guard_requires_local_db14() -> None:
    module = importlib.import_module("scripts.ops.gh_enricher_runtime_smoke")

    assert module._is_expected_redis_db14("redis://localhost:6379/14")
    assert module._is_expected_redis_db14("redis://127.0.0.1:6379/14")
    assert not module._is_expected_redis_db14("redis://localhost:6379/0")
    assert not module._is_expected_redis_db14("redis://example.com:6379/14")


def test_redaction_helper_removes_database_and_redis_url_fragments() -> None:
    module = importlib.import_module("scripts.ops.gh_enricher_runtime_smoke")
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
    module = importlib.import_module("scripts.ops.gh_enricher_runtime_smoke")
    event_id = uuid4()
    artifact_id = uuid4()
    fields = _valid_payload(module, event_id=event_id, artifact_id=artifact_id)

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_artifact_id=artifact_id,
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
        ("artifact_type", "github_repo"),
        ("provider_route", "github"),
        ("database_url", "postgresql+psycopg://user:secret@localhost:5432/db"),
        ("api_token", "secret-token"),
    ],
)
def test_thin_redis_payload_validation_rejects_business_and_secret_fields(field_name, field_value) -> None:
    module = importlib.import_module("scripts.ops.gh_enricher_runtime_smoke")
    event_id = uuid4()
    artifact_id = uuid4()
    fields = _valid_payload(module, event_id=event_id, artifact_id=artifact_id)
    fields[field_name] = field_value

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_artifact_id=artifact_id,
        database_url=_fake_database_url(),
        redis_url="redis://localhost:6379/14",
    )

    assert any(field_name in failure or "URL or credential" in failure for failure in failures)


def test_seed_shape_preserves_locked_colon_based_github_repo_canonical_id() -> None:
    module = importlib.import_module("scripts.ops.gh_enricher_runtime_smoke")
    seed_shape = module._build_seed_shape("abc123abc123abc123abc123abc123ab")

    assert seed_shape.canonical_id == "github:repo:octocat/hello-world-smoke-abc123abc123"
    assert seed_shape.canonical_id.startswith("github:repo:")
    assert "github_repo:" not in seed_shape.canonical_id
    assert "github_repo:" not in SCRIPT.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_fake_github_client_fixture_shape_has_url_bearing_contents() -> None:
    module = importlib.import_module("scripts.ops.gh_enricher_runtime_smoke")
    client = module.FakeGitHubClient()

    repo = await client.get_repo("octocat", "hello-world-smoke-test", auth_mode="anonymous_degraded")
    head = await client.get_default_branch_head("octocat", "hello-world-smoke-test", "main", auth_mode="anonymous_degraded")
    tree = await client.get_tree("octocat", "hello-world-smoke-test", "main", recursive=True, auth_mode="anonymous_degraded")
    contents = await client.get_contents(
        "octocat",
        "hello-world-smoke-test",
        "README.md",
        ref="main",
        auth_mode="anonymous_degraded",
    )
    releases = await client.get_releases("octocat", "hello-world-smoke-test", auth_mode="anonymous_degraded")

    decoded = module.base64.b64decode(contents["content"]).decode("utf-8")
    assert repo["full_name"] == "octocat/hello-world-smoke-test"
    assert head["sha"] == "0123456789abcdef0123456789abcdef01234567"
    assert any(item["path"] == "README.md" for item in tree["tree"])
    assert "https://example.com/runtime-smoke/readme" in decoded
    assert releases[0]["assets"][0]["download_count"] == 5
    assert client.external_network_calls_attempted is False


def test_report_shape_is_stable() -> None:
    module = importlib.import_module("scripts.ops.gh_enricher_runtime_smoke")
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
        "resulting_snapshot_ids",
        "downstream_outbox_ids",
        "fake_github_calls",
    }
    assert rendered["report_type"] == "gh_enricher_runtime_smoke_v1"
    assert rendered["database_url_redacted"] is True
    assert rendered["redis_url_redacted"] is True
    assert rendered["queue_name"] == "q.artifact.enrich.github"


def test_no_external_github_client_or_credentials_required_by_script() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "from src.services.gh_enricher.github_client import GitHubClient" not in text
    assert "GITHUB_APP_ID" not in text
    assert "GITHUB_PRIVATE_KEY" not in text
    assert "FakeGitHubClient" in text
    assert "github_app_id=None" in text
    assert "github_private_key=None" in text


def test_runbook_documents_no_real_github_and_manual_env() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL" in text
    assert "REDIS_URL" in text
    assert "does not call the real GitHub API" in text
    assert "q.artifact.enrich.github" in text
    assert "checks_failed" in text
