from __future__ import annotations

import importlib
import json
from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "outbox_redis_routing_smoke.py"
RUNBOOK = ROOT / "ops" / "delivery" / "runbooks" / "outbox_redis_routing_smoke.md"


def _fake_database_url() -> str:
    password = "super" + "secret"
    return f"postgresql+psycopg://user:{password}@localhost:5432/catchbot_smoke"


def _fake_redis_url() -> str:
    password = "redis" + "secret"
    return f"redis://:{password}@localhost:6379/15"


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_cli_requires_explicit_database_url() -> None:
    module = importlib.import_module("scripts.ops.outbox_redis_routing_smoke")
    parser = module.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--redis-url", "redis://localhost:6379/15", "--confirm", "write"])


def test_cli_requires_explicit_redis_url() -> None:
    module = importlib.import_module("scripts.ops.outbox_redis_routing_smoke")
    parser = module.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--database-url", _fake_database_url(), "--confirm", "write"])


def test_cli_requires_confirm_write() -> None:
    module = importlib.import_module("scripts.ops.outbox_redis_routing_smoke")
    parser = module.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--database-url",
                _fake_database_url(),
                "--redis-url",
                "redis://localhost:6379/15",
            ]
        )


def test_parser_accepts_required_write_smoke_options() -> None:
    module = importlib.import_module("scripts.ops.outbox_redis_routing_smoke")
    parser = module.build_parser()

    args = parser.parse_args(
        [
            "--database-url",
            _fake_database_url(),
            "--redis-url",
            "redis://localhost:6379/15",
            "--confirm",
            "write",
            "--format",
            "human",
        ]
    )

    assert args.database_url.startswith("postgresql+psycopg://")
    assert args.redis_url == "redis://localhost:6379/15"
    assert args.confirm == "write"
    assert args.format == "human"


def test_redaction_helper_removes_database_and_redis_url_fragments() -> None:
    module = importlib.import_module("scripts.ops.outbox_redis_routing_smoke")
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


def test_production_like_execution_is_refused_by_default() -> None:
    module = importlib.import_module("scripts.ops.outbox_redis_routing_smoke")

    assert module._is_production_like_url("postgresql+psycopg://user@db-prod:5432/catchbot")
    assert module._is_production_like_url("redis://production-redis:6379/0")
    assert not module._is_production_like_url("redis://localhost:6379/15")


def test_pending_guard_counts_and_rejects_any_pre_existing_pending_row() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "safety.pending_outbox_empty_guard" in text
    assert "SELECT COUNT(*)" in text
    assert "pending_before_count != 0" in text
    assert "status = 'pending'::outbox_status_enum" in text
    assert "non_smoke_pending" not in text


def test_smoke_insert_is_committed_before_relay_uses_new_session() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    insert_commit = text.index("await insert_session.commit()")
    relay_session = text.index("as relay_session")
    relay_repository = text.index("OutboxRelayRepository(relay_session)")
    relay_run = text.index("processed = await service.run_once()")

    assert insert_commit < relay_session < relay_repository < relay_run
    assert "pending_before_relay_count = await _count_pending_rows(relay_session)" in text
    assert "batch_size=1" in text


def test_report_shape_is_stable() -> None:
    module = importlib.import_module("scripts.ops.outbox_redis_routing_smoke")
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
        "smoke_event_id",
    }
    assert rendered["report_type"] == "outbox_redis_routing_smoke_v1"
    assert rendered["database_url_redacted"] is True
    assert rendered["redis_url_redacted"] is True


def test_thin_redis_payload_validation_rejects_payload_json() -> None:
    module = importlib.import_module("scripts.ops.outbox_redis_routing_smoke")
    event_id = uuid4()
    aggregate_id = uuid4()
    fields = {
        "job_id": str(event_id),
        "stage_name": "normalize",
        "root_object_type": "source_message",
        "root_object_id": str(aggregate_id),
        "idempotency_key": "ops-smoke:outbox-redis-routing:abc",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
        "payload_json": "{}",
    }

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_root_object_id=aggregate_id,
        database_url=_fake_database_url(),
        redis_url="redis://localhost:6379/15",
    )

    assert any("payload_json" in failure for failure in failures)


def test_thin_redis_payload_validation_rejects_unknown_extra_fields() -> None:
    module = importlib.import_module("scripts.ops.outbox_redis_routing_smoke")
    event_id = uuid4()
    aggregate_id = uuid4()
    fields = {
        "job_id": str(event_id),
        "stage_name": "normalize",
        "root_object_type": "source_message",
        "root_object_id": str(aggregate_id),
        "idempotency_key": "ops-smoke:outbox-redis-routing:abc",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
        "unexpected_hint": "extra",
    }

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_root_object_id=aggregate_id,
        database_url=_fake_database_url(),
        redis_url="redis://localhost:6379/15",
    )

    assert any("unexpected_hint" in failure for failure in failures)


def test_thin_redis_payload_validation_rejects_secret_like_fields() -> None:
    module = importlib.import_module("scripts.ops.outbox_redis_routing_smoke")
    event_id = uuid4()
    aggregate_id = uuid4()
    fields = {
        "job_id": str(event_id),
        "stage_name": "normalize",
        "root_object_type": "source_message",
        "root_object_id": str(aggregate_id),
        "idempotency_key": "ops-smoke:outbox-redis-routing:abc",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
        "api_token": "secret-token",
    }

    failures = module.validate_redis_payload(
        fields,
        expected_event_id=event_id,
        expected_root_object_id=aggregate_id,
        database_url=_fake_database_url(),
        redis_url="redis://localhost:6379/15",
    )

    assert any("api_token" in failure for failure in failures)


def test_no_import_time_network_or_database_connection(monkeypatch) -> None:
    import sqlalchemy.ext.asyncio as sqlalchemy_async

    calls: list[str] = []
    module_name = "scripts.ops.outbox_redis_routing_smoke"

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


def test_runbook_documents_boundaries_and_expected_route() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "--confirm write" in text
    assert "q.source.normalize" in text
    assert "source_message.created.v1" in text
    assert "does not start live workers" in text
    assert "does not call Telegram, OpenAI, GitHub, X, or Web" in text
    assert "not Stage 45" in text
