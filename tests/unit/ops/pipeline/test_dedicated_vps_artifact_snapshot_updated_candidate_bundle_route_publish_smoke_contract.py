from __future__ import annotations

import importlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_artifact_snapshot_updated_candidate_bundle_route_publish_smoke.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-snapshot-bundle@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-password-snapshot-bundle@127.0.0.1:6379/0"
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
FAKE_STREAM_ID = "1710000000000-0"
FAKE_DEDUPE_KEY = "private-snapshot-updated-dedupe-key"
FAKE_SOURCE_TEXT = "Sensitive source text must not be rendered"
FAKE_X_URL = "https://x.com/openai/status/1234567890123456789"
FAKE_SECRET = "fake-secret-value"


class FakeMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class FakeResult:
    def __init__(self, *, scalar: Any = None, rows: list[dict[str, Any]] | None = None) -> None:
        self._scalar = scalar
        self._rows = rows or []

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def scalar(self) -> Any:
        return self._scalar

    def mappings(self) -> FakeMappings:
        return FakeMappings(self._rows)

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class FakeSession:
    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        snapshot_pairs: set[tuple[str, str]] | None = None,
        x_post_snapshots: set[str] | None = None,
        read_only_value: str = "on",
        missing_tables: set[str] | None = None,
        raise_on_update: bool = False,
        raise_on_job_attempt: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self.rows = rows or []
        self.snapshot_pairs = snapshot_pairs or set()
        self.x_post_snapshots = x_post_snapshots or set()
        self.read_only_value = read_only_value
        self.missing_tables = missing_tables or set()
        self.raise_on_update = raise_on_update
        self.raise_on_job_attempt = raise_on_job_attempt
        self.order = order
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.published_event_ids: list[UUID] = []
        self.job_attempts: list[dict[str, Any]] = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(str(statement))
        self.statements.append(normalized)
        self.params.append(dict(params))
        module = _module()

        if normalized == _normalize(module.SET_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult()
        if normalized == _normalize(module.SHOW_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult(scalar=self.read_only_value)
        if normalized == _normalize(module.SELECT_ONE_QUERY):
            return FakeResult(scalar=1)
        if normalized == _normalize(module.TABLE_AVAILABLE_QUERY):
            table_name = str(params["qualified_table_name"]).removeprefix("public.")
            return FakeResult(scalar=table_name not in self.missing_tables)
        if normalized == _normalize(module.SELECT_PENDING_SNAPSHOT_UPDATED_EVENTS_QUERY):
            limit = int(params.get("limit", len(self.rows)))
            return FakeResult(rows=self.rows[:limit])
        if normalized == _normalize(module.COUNT_ARTIFACT_SNAPSHOT_QUERY):
            pair = (str(params["artifact_id"]), str(params["snapshot_id"]))
            return FakeResult(scalar=1 if pair in self.snapshot_pairs else 0)
        if normalized == _normalize(module.COUNT_X_POST_SNAPSHOT_QUERY):
            return FakeResult(
                scalar=1 if str(params["snapshot_id"]) in self.x_post_snapshots else 0
            )
        if normalized.startswith("UPDATE event_outbox"):
            if self.raise_on_update:
                raise RuntimeError(f"db update failed with {FAKE_SECRET}")
            if self.order is not None:
                self.order.append("db:update_published")
            self.published_event_ids.append(UUID(str(params["event_id"])))
            return FakeResult()
        if normalized.startswith("INSERT INTO job_attempts"):
            if self.raise_on_job_attempt:
                raise RuntimeError(f"job attempt failed with {FAKE_SECRET}")
            if self.order is not None:
                self.order.append("db:insert_job_attempt")
            self.job_attempts.append(dict(params))
            return FakeResult()

        raise AssertionError(f"unexpected SQL: {statement}")

    async def commit(self) -> None:
        self.committed = True
        if self.order is not None:
            self.order.append("db:commit")

    async def rollback(self) -> None:
        self.rolled_back = True
        if self.order is not None:
            self.order.append("db:rollback")

    async def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(
        self,
        *,
        fail_xadd: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self.fail_xadd = fail_xadd
        self.order = order
        self.ping_calls = 0
        self.xlen_calls: list[str] = []
        self.xadd_calls: list[tuple[str, dict[str, str], dict[str, Any]]] = []
        self.closed = False

    async def ping(self) -> bool:
        self.ping_calls += 1
        return True

    async def xlen(self, name: str) -> int:
        self.xlen_calls.append(name)
        return 0

    async def xadd(
        self,
        name: str,
        fields: dict[str, str],
        maxlen: int | None = None,
        approximate: bool | None = None,
    ) -> str:
        self.xadd_calls.append((name, dict(fields), {"maxlen": maxlen, "approximate": approximate}))
        if self.order is not None:
            self.order.append("redis:xadd")
        if self.fail_xadd:
            raise RuntimeError(f"redis publish failed with {FAKE_SECRET}")
        return FAKE_STREAM_ID

    async def close(self) -> None:
        self.closed = True


class MismatchedRouteResolver:
    def resolve(self, _row: Any) -> Any:
        module = _module()
        return module.QueueRoute(queue_name="q.analysis.route", stage_name="analysis_route")


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_artifact_snapshot_updated_candidate_bundle_route_publish_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "OUTBOX_RELAY_XADD_MAXLEN": "10000",
    }


def _all_approvals() -> Any:
    module = _module()
    return module.PublishApprovals(
        artifact_snapshot_updated_candidate_bundle_route_publish=True,
        redis_publish=True,
        event_outbox_update=True,
        job_attempt_write=True,
    )


def _approvals(**overrides: bool) -> Any:
    values = {
        "artifact_snapshot_updated_candidate_bundle_route_publish": False,
        "redis_publish": False,
        "event_outbox_update": False,
        "job_attempt_write": False,
    }
    values.update(overrides)
    return _module().PublishApprovals(**values)


def _fake_row(
    *,
    event_id: UUID | None = None,
    artifact_id: UUID | None = None,
    snapshot_id: UUID | None = None,
    provider: str = "x",
    provider_route: str | None = "x",
    aggregate_type: str = "artifact",
    include_artifact_id: bool = True,
    include_snapshot_id: bool = True,
    dedupe_key: str = FAKE_DEDUPE_KEY,
) -> dict[str, Any]:
    event_id = event_id or uuid4()
    artifact_id = artifact_id or uuid4()
    snapshot_id = snapshot_id or uuid4()
    payload: dict[str, Any] = {
        "provider": provider,
        "snapshot_type": "x_post",
        "status": "ready",
        "content_anchor": "private-content-anchor",
        "source_text": FAKE_SOURCE_TEXT,
        "canonical_url": FAKE_X_URL,
    }
    if provider_route is not None:
        payload["provider_route"] = provider_route
    if include_artifact_id:
        payload["artifact_id"] = str(artifact_id)
    if include_snapshot_id:
        payload["snapshot_id"] = str(snapshot_id)
    return {
        "event_id": event_id,
        "event_type": "artifact.snapshot.updated.v1",
        "aggregate_type": aggregate_type,
        "aggregate_id": artifact_id,
        "dedupe_key": dedupe_key,
        "payload_json": payload,
        "status": "pending",
        "fail_count": 0,
        "created_at": datetime.now(timezone.utc),
    }


def _snapshot_pairs(*rows: dict[str, Any]) -> set[tuple[str, str]]:
    return {
        (str(row["payload_json"]["artifact_id"]), str(row["payload_json"]["snapshot_id"]))
        for row in rows
        if row["payload_json"].get("artifact_id") and row["payload_json"].get("snapshot_id")
    }


def _x_post_snapshots(*rows: dict[str, Any]) -> set[str]:
    return {
        str(row["payload_json"]["snapshot_id"])
        for row in rows
        if row["payload_json"].get("snapshot_id")
    }


def _run_report(
    *,
    rows: list[dict[str, Any]] | None = None,
    approvals: Any | None = None,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    route_resolver: Any | None = None,
    side_effect_flags: dict[str, bool] | None = None,
) -> tuple[Any, FakeSession, FakeRedis]:
    module = _module()
    source_rows = rows or []
    fake_session = session or FakeSession(
        source_rows,
        snapshot_pairs=_snapshot_pairs(*source_rows),
        x_post_snapshots=_x_post_snapshots(*source_rows),
    )
    fake_redis = redis or FakeRedis()
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        approvals=approvals,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: fake_session,
        redis_client_factory=lambda _url: fake_redis,
        route_resolver=route_resolver,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_SECRET),
    )
    return result, fake_session, fake_redis


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_report_contains_required_sanitized_fields() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(rows=[row])

    required_fields = {
        "contract_status",
        "runtime_env_read",
        "database_connected",
        "redis_connected",
        "read_only_transaction",
        "snapshot_updated_pending_event_found_bucket",
        "snapshot_updated_event_valid_bucket",
        "provider_route_bucket",
        "redis_publish_planned_bucket",
        "redis_publish_attempted",
        "redis_publish_succeeded_bucket",
        "redis_stream_name_bucket",
        "redis_thin_payload_valid_bucket",
        "event_outbox_update_attempted",
        "event_outbox_published_bucket",
        "job_attempt_write_attempted",
        "job_attempt_succeeded_bucket",
        "evidence_assembler_started",
        "evidence_bundle_written_bucket",
        "analysis_requested_outbox_written_bucket",
        "judge_policy_notifier_started",
        "source_tables_mutation_performed",
        "telegram_raw_updates_mutation_performed",
        "candidate_mutation_performed",
        "artifact_registry_mutation_performed",
        "artifact_snapshot_mutation_performed",
        "docker_or_systemd_changed",
        "alembic_run",
        "raw_values_emitted",
        "checks_failed",
    }
    assert required_fields <= set(result.report)


def test_default_mode_is_read_only_and_does_not_publish_update_or_write() -> None:
    row = _fake_row()
    result, session, redis = _run_report(rows=[row])

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_READY
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["snapshot_updated_pending_event_found_bucket"] == "one"
    assert result.report["snapshot_updated_event_valid_bucket"] == "one"
    assert result.report["provider_route_bucket"] == "one"
    assert result.report["redis_publish_planned_bucket"] == "one"
    assert result.report["redis_publish_attempted"] is False
    assert result.report["event_outbox_update_attempted"] is False
    assert result.report["job_attempt_write_attempted"] is False
    assert redis.xadd_calls == []
    assert redis.xlen_calls == ["q.candidate.bundle"]
    assert session.published_event_ids == []
    assert session.job_attempts == []
    assert session.rolled_back is True


def test_partial_approvals_block_before_publish_write_and_connections() -> None:
    row = _fake_row()
    session = FakeSession([row])
    redis = FakeRedis()
    result, session, redis = _run_report(
        rows=[row],
        session=session,
        redis=redis,
        approvals=_approvals(
            artifact_snapshot_updated_candidate_bundle_route_publish=True,
            redis_publish=True,
            event_outbox_update=True,
        ),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_MISSING_APPROVAL
    assert "approval.job_attempt_write" in result.report["checks_failed"]
    assert result.report["database_connected"] is False
    assert result.report["redis_connected"] is False
    assert session.statements == []
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_approved_mode_publishes_exactly_one_redis_message_to_candidate_bundle_queue() -> None:
    row = _fake_row()
    redis = FakeRedis()
    result, session, redis = _run_report(
        rows=[row],
        approvals=_all_approvals(),
        redis=redis,
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_PUBLISHED
    assert len(redis.xadd_calls) == 1
    stream_name, fields, _options = redis.xadd_calls[0]
    assert stream_name == "q.candidate.bundle"
    assert fields["job_id"] == str(row["event_id"])
    assert fields["stage_name"] == "bundle"
    assert fields["root_object_type"] == "artifact"
    assert fields["root_object_id"] == str(row["aggregate_id"])
    assert fields["idempotency_key"] == row["dedupe_key"]
    assert fields["pipeline_run_id"] == ""
    assert fields["not_before"] == ""
    assert fields["trigger_event_id"] == str(row["event_id"])
    assert session.published_event_ids == [row["event_id"]]
    assert len(session.job_attempts) == 1


def test_redis_stream_message_is_thin_without_payload_source_text_url_or_raw_fields() -> None:
    row = _fake_row()
    redis = FakeRedis()
    result, _session, redis = _run_report(
        rows=[row],
        approvals=_all_approvals(),
        redis=redis,
    )

    assert result.exit_code == 0
    fields = redis.xadd_calls[0][1]
    assert set(fields) == _module().ALLOWED_REDIS_THIN_FIELDS
    rendered_fields = json.dumps(fields, sort_keys=True)
    forbidden_tokens = ("payload_json", "payload", "source_text", "canonical_url", FAKE_X_URL, FAKE_SOURCE_TEXT)
    for token in forbidden_tokens:
        assert token not in rendered_fields


def test_event_outbox_is_marked_published_only_after_redis_publish_succeeds() -> None:
    order: list[str] = []
    row = _fake_row()
    session = FakeSession(
        [row],
        snapshot_pairs=_snapshot_pairs(row),
        x_post_snapshots=_x_post_snapshots(row),
        order=order,
    )
    redis = FakeRedis(order=order)
    result, session, _redis = _run_report(
        rows=[row],
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert order[:4] == [
        "redis:xadd",
        "db:update_published",
        "db:insert_job_attempt",
        "db:commit",
    ]


def test_job_attempt_succeeded_row_is_written_for_bundle_stage() -> None:
    row = _fake_row()
    result, session, _redis = _run_report(rows=[row], approvals=_all_approvals())

    assert result.report["job_attempt_write_attempted"] is True
    assert result.report["job_attempt_succeeded_bucket"] == "one"
    assert len(session.job_attempts) == 1
    attempt = session.job_attempts[0]
    assert attempt["stage_name"] == "bundle"
    assert attempt["queue_name"] == "q.candidate.bundle"
    assert attempt["root_object_type"] == "artifact"
    assert attempt["root_object_id"] == str(row["aggregate_id"])
    assert attempt["attempt_status"] == "succeeded"
    assert attempt["error_code"] is None


def test_redis_publish_failure_rolls_back_db_and_does_not_mark_published() -> None:
    row = _fake_row()
    redis = FakeRedis(fail_xadd=True)
    result, session, redis = _run_report(
        rows=[row],
        approvals=_all_approvals(),
        redis=redis,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_REDIS_PUBLISH_FAILED
    assert result.report["redis_publish_attempted"] is True
    assert result.report["redis_publish_succeeded_bucket"] == "zero"
    assert result.report["event_outbox_update_attempted"] is False
    assert result.report["job_attempt_write_attempted"] is False
    assert len(redis.xadd_calls) == 1
    assert session.published_event_ids == []
    assert session.job_attempts == []
    assert session.rolled_back is True


def test_db_update_failure_after_redis_publish_is_sanitized_and_stops_downstream() -> None:
    row = _fake_row()
    session = FakeSession(
        [row],
        snapshot_pairs=_snapshot_pairs(row),
        x_post_snapshots=_x_post_snapshots(row),
        raise_on_update=True,
    )
    redis = FakeRedis()
    result, session, redis = _run_report(
        rows=[row],
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DB_UPDATE_FAILED
    assert result.report["redis_publish_succeeded_bucket"] == "one"
    assert len(redis.xadd_calls) == 1
    assert session.rolled_back is True
    assert result.report["evidence_assembler_started"] is False
    assert result.report["judge_policy_notifier_started"] is False
    assert FAKE_SECRET not in rendered
    assert "db update failed" not in rendered


def test_no_pending_event_blocks_without_publish_or_write() -> None:
    result, session, redis = _run_report(rows=[])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NO_PENDING
    assert result.report["snapshot_updated_pending_event_found_bucket"] == "zero"
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_invalid_event_blocks_before_publish_or_write() -> None:
    row = _fake_row(include_snapshot_id=False)
    result, session, redis = _run_report(rows=[row])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT
    assert "payload.snapshot_id" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_wrong_provider_route_blocks_before_publish_or_write() -> None:
    row = _fake_row(provider="github", provider_route="github")
    result, session, redis = _run_report(rows=[row])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT
    assert result.report["provider_route_bucket"] == "zero"
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_route_mismatch_blocks_before_publish_or_write() -> None:
    row = _fake_row()
    result, session, redis = _run_report(
        rows=[row],
        approvals=_all_approvals(),
        route_resolver=MismatchedRouteResolver(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT
    assert result.report["redis_stream_name_bucket"] == "zero"
    assert "route.queue" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_multiple_valid_snapshot_updated_events_block_as_ambiguous() -> None:
    first = _fake_row()
    second = _fake_row()
    result, session, redis = _run_report(rows=[first, second], approvals=_all_approvals())

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT
    assert result.report["snapshot_updated_event_valid_bucket"] == "multiple"
    assert "event_outbox.multiple_valid_events" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_no_downstream_consume_evidence_assembly_judge_policy_or_notifier_start() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(rows=[row])
    text = SCRIPT.read_text(encoding="utf-8")

    assert result.report["evidence_assembler_started"] is False
    assert result.report["evidence_bundle_written_bucket"] == "zero"
    assert result.report["analysis_requested_outbox_written_bucket"] == "zero"
    assert result.report["judge_policy_notifier_started"] is False
    assert "src.services.evidence_assembler" not in text
    assert "src.services.analysis_router" not in text
    assert "src.services.judge_openai" not in text
    assert "src.services.policy_engine" not in text
    assert "src.services.notifier_telegram" not in text


def test_forbidden_side_effect_flags_block() -> None:
    result, _session, _redis = _run_report(
        rows=[_fake_row()],
        side_effect_flags={"evidence_assembler_started": True},
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert "side_effect.forbidden" in result.report["checks_failed"]


def test_no_source_telegram_candidate_artifact_snapshot_or_runtime_mutation_is_reported() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(rows=[row])

    assert result.report["source_tables_mutation_performed"] is False
    assert result.report["telegram_raw_updates_mutation_performed"] is False
    assert result.report["candidate_mutation_performed"] is False
    assert result.report["artifact_registry_mutation_performed"] is False
    assert result.report["artifact_snapshot_mutation_performed"] is False
    assert result.report["docker_or_systemd_changed"] is False
    assert result.report["alembic_run"] is False


def test_no_raw_values_emitted_including_fake_ids_urls_secrets_or_stream_id() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    snapshot_id = uuid4()
    row = _fake_row(
        event_id=event_id,
        artifact_id=artifact_id,
        snapshot_id=snapshot_id,
        dedupe_key=FAKE_DEDUPE_KEY,
    )
    result, _session, _redis = _run_report(rows=[row], approvals=_all_approvals())
    rendered = json.dumps(result.report, sort_keys=True)

    forbidden_values = (
        str(event_id),
        str(artifact_id),
        str(snapshot_id),
        FAKE_DEDUPE_KEY,
        FAKE_SOURCE_TEXT,
        FAKE_X_URL,
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        "unit-db-password-snapshot-bundle",
        "unit-redis-password-snapshot-bundle",
        FAKE_RUNTIME_PATH,
        FAKE_STREAM_ID,
        FAKE_SECRET,
    )
    for value in forbidden_values:
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False
