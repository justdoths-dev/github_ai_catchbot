from __future__ import annotations

import importlib
import json
from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "router_normalizer_runtime_smoke.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "router_normalizer_runtime_smoke.md"


def _fake_database_url() -> str:
    password = "super" + "secret"
    return f"postgresql+psycopg://user:{password}@localhost:5432/catchbot_smoke"


def _fake_redis_url() -> str:
    password = "redis" + "secret"
    return f"redis://:{password}@localhost:6379/14"


def _valid_payload(module, *, event_id=None, source_message_id=None) -> dict[str, str]:
    event_id = event_id or uuid4()
    source_message_id = source_message_id or uuid4()
    return module._build_redis_payload(
        event_id=event_id,
        source_message_id=source_message_id,
        marker=f"{module.SMOKE_MARKER_PREFIX}{event_id}",
    )


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_cli_requires_explicit_database_url() -> None:
    module = importlib.import_module("scripts.ops.router_normalizer_runtime_smoke")
    parser = module.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--redis-url", "redis://localhost:6379/14", "--confirm", "write"])


def test_cli_requires_explicit_redis_url() -> None:
    module = importlib.import_module("scripts.ops.router_normalizer_runtime_smoke")
    parser = module.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--database-url", _fake_database_url(), "--confirm", "write"])


def test_cli_requires_confirm_write() -> None:
    module = importlib.import_module("scripts.ops.router_normalizer_runtime_smoke")
    parser = module.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--database-url",
                _fake_database_url(),
                "--redis-url",
                "redis://localhost:6379/14",
            ]
        )


def test_parser_accepts_required_write_smoke_options() -> None:
    module = importlib.import_module("scripts.ops.router_normalizer_runtime_smoke")
    parser = module.build_parser()

    args = parser.parse_args(
        [
            "--database-url",
            _fake_database_url(),
            "--redis-url",
            "redis://localhost:6379/14",
            "--confirm",
            "write",
            "--format",
            "human",
        ]
    )

    assert args.database_url.startswith("postgresql+psycopg://")
    assert args.redis_url == "redis://localhost:6379/14"
    assert args.confirm == "write"
    assert args.format == "human"


def test_production_like_execution_is_refused_by_default() -> None:
    module = importlib.import_module("scripts.ops.router_normalizer_runtime_smoke")

    assert module._is_production_like_url("postgresql+psycopg://user@db-prod:5432/catchbot")
    assert module._is_production_like_url("redis://production-redis:6379/0")
    assert module._is_production_like_url("rediss://localhost:6379/14")
    assert not module._is_production_like_url("redis://localhost:6379/14")


def test_database_guard_requires_local_smoke_test_or_dev_database() -> None:
    module = importlib.import_module("scripts.ops.router_normalizer_runtime_smoke")

    assert module._is_expected_smoke_database_url(
        "postgresql+psycopg://user:secret@localhost:5432/github_ai_catchbot_smoke"
    )
    assert module._is_expected_smoke_database_url("postgresql://user@127.0.0.1:5432/catchbot_test")
    assert not module._is_expected_smoke_database_url("postgresql+psycopg://user@db.internal:5432/catchbot_smoke")
    assert not module._is_expected_smoke_database_url("postgresql+psycopg://user@localhost:5432/catchbot")
    assert not module._is_expected_smoke_database_url("mysql://user@localhost:3306/catchbot_smoke")


def test_redis_db14_guard_requires_local_db14() -> None:
    module = importlib.import_module("scripts.ops.router_normalizer_runtime_smoke")

    assert module._is_expected_redis_db14("redis://localhost:6379/14")
    assert module._is_expected_redis_db14("redis://127.0.0.1:6379/14")
    assert not module._is_expected_redis_db14("redis://localhost:6379/15")
    assert not module._is_expected_redis_db14("redis://example.com:6379/14")


def test_redaction_helper_removes_database_and_redis_url_fragments() -> None:
    module = importlib.import_module("scripts.ops.router_normalizer_runtime_smoke")
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
    module = importlib.import_module("scripts.ops.router_normalizer_runtime_smoke")
    event_id = uuid4()
    source_message_id = uuid4()
    fields = _valid_payload(module, event_id=event_id, source_message_id=source_message_id)

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_root_object_id=source_message_id,
        database_url=_fake_database_url(),
        redis_url="redis://localhost:6379/14",
    )

    assert failures == []
    assert set(fields) == module.REQUIRED_REDIS_FIELDS


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("payload_json", "{}"),
        ("source_message_text", "Check this GitHub tool: https://github.com/octocat/Hello-World"),
        ("raw_message_json", "{}"),
        ("database_url", "postgresql+psycopg://user:secret@localhost:5432/db"),
        ("api_token", "secret-token"),
    ],
)
def test_thin_redis_payload_validation_rejects_business_and_secret_fields(field_name, field_value) -> None:
    module = importlib.import_module("scripts.ops.router_normalizer_runtime_smoke")
    event_id = uuid4()
    source_message_id = uuid4()
    fields = _valid_payload(module, event_id=event_id, source_message_id=source_message_id)
    fields[field_name] = field_value

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_root_object_id=source_message_id,
        database_url=_fake_database_url(),
        redis_url="redis://localhost:6379/14",
    )

    assert any(field_name in failure or "URL or credential" in failure for failure in failures)


def test_smoke_marker_prefix_is_used_for_payload_idempotency_key() -> None:
    module = importlib.import_module("scripts.ops.router_normalizer_runtime_smoke")
    fields = _valid_payload(module)

    assert fields["idempotency_key"].startswith("ops-smoke:router-normalizer-runtime:")
    assert module.SMOKE_MARKER_PREFIX == "ops-smoke:router-normalizer-runtime:"


def test_expected_github_repo_canonical_id_matches_router_normalizer_contract() -> None:
    module = importlib.import_module("scripts.ops.router_normalizer_runtime_smoke")

    assert module.EXPECTED_CANONICAL_ID == "github:repo:octocat/hello-world"
    assert "github_repo:octocat/hello-world" not in SCRIPT.read_text(encoding="utf-8")


def test_synthetic_first_version_uses_collector_new_reason_not_created() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    version_insert = text[text.index("INSERT INTO source_message_versions") : text.index("outbox_payload = {")]

    assert "'new'" in version_insert
    assert "'created'" not in version_insert


def test_synthetic_url_surface_uses_regex_source_kind_for_plain_text_url() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    url_surface_block = text[text.index("url_surface_json = [") : text.index("content_hash =")]

    assert '"source_kind": "regex"' in url_surface_block
    assert '"source_kind": "entity"' not in url_surface_block
    assert "entities_json\": json.dumps([], sort_keys=True)" in text


def test_pending_and_redis_queue_guards_are_present() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "safety.pending_outbox_empty_guard" in text
    assert "pending_before_count != 0" in text
    assert "status = 'pending'::outbox_status_enum" in text
    assert "safety.redis_known_queues_empty_guard" in text
    assert "non_empty_queues" in text
    assert "KNOWN_QUEUE_NAMES" in text


def test_report_shape_is_stable() -> None:
    module = importlib.import_module("scripts.ops.router_normalizer_runtime_smoke")
    report = module._new_report()
    rendered = json.loads(module._render_json(report))

    assert set(rendered) == {
        "report_type",
        "checks_run",
        "checks_passed",
        "checks_failed",
        "failures",
        "warnings",
        "database_url_redacted",
        "redis_url_redacted",
        "mutation_safety",
        "queue_name",
        "stream_message_id",
        "smoke_source_message_id",
        "smoke_event_id",
        "normalization_run_id",
        "candidate_group_id",
        "primary_artifact_id",
        "downstream_event_id",
    }
    assert rendered["report_type"] == "router_normalizer_runtime_smoke_v1"
    assert rendered["database_url_redacted"] is True
    assert rendered["redis_url_redacted"] is True


def test_no_import_time_network_or_database_connection(monkeypatch) -> None:
    import sqlalchemy.ext.asyncio as sqlalchemy_async

    calls: list[str] = []
    module_name = "scripts.ops.router_normalizer_runtime_smoke"

    def fail_create_async_engine(*args, **kwargs):
        calls.append("create_async_engine")
        raise AssertionError("import should not create a database engine")

    import sys

    sys.modules.pop(module_name, None)
    monkeypatch.setattr(sqlalchemy_async, "create_async_engine", fail_create_async_engine)
    try:
        importlib.import_module(module_name)
        assert calls == []
    finally:
        sys.modules.pop(module_name, None)


def test_script_uses_trigger_event_rehydration_not_redis_business_payload() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "trigger_event_id" in text
    assert "get_outbox_event" not in text
    assert "RouterNormalizerRepository(process_session)" in text
    assert "service.process_stream_message(message)" in text
    assert "SMOKE_SOURCE_TEXT" not in text[text.index("def _build_redis_payload") : text.index("async def _select_scalar")]


def test_runbook_documents_boundaries_expected_route_and_durable_rows() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "--confirm write" in text
    assert "redis://localhost:6379/14" in text
    assert "q.source.normalize" in text
    assert "source_message.created.v1" in text
    assert "artifact.enrich.requested.v1" in text
    assert "does not start live workers" in text
    assert "does not call Telegram, OpenAI, GitHub, X, or Web" in text
    assert "not Stage 45" in text
    assert "may leave controlled smoke DB rows" in text
