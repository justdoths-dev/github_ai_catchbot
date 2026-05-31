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
    / "dedicated_vps_judge_call_requested_route_publish_smoke.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-judge-call-route"
FAKE_REDIS_CREDENTIAL = "unit" + "-redis" + "-credential" + "-judge-call-route"
FAKE_DATABASE_URL = (
    "postgresql+psycopg"
    + ":"
    + "/"
    + "/"
    + "github_ai_catchbot_app"
    + ":"
    + FAKE_DATABASE_CREDENTIAL
    + "@"
    + "127.0.0.1"
    + ":5432/"
    + "github_ai_catchbot"
)
FAKE_REDIS_URL = (
    "redis"
    + ":"
    + "/"
    + "/"
    + ":"
    + FAKE_REDIS_CREDENTIAL
    + "@"
    + "127.0.0.1"
    + ":6379/0"
)
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
FAKE_STREAM_ID = "1710000000000" + "-0"
FAKE_DEDUPE_KEY = "private-judge-call-dedupe-key"
FAKE_URL = "https" + ":" + "/" + "/" + "example.invalid" + "/private/judge-call"
FAKE_SOURCE_TEXT = " ".join(["private", "source", "text"])
FAKE_SENSITIVE_VALUE = "fake" + "-private-sensitive-value"


class FakeMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None


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
        judge_runs: dict[str, dict[str, Any]] | None = None,
        bundles: dict[str, dict[str, Any]] | None = None,
        read_only_value: str = "on",
        missing_tables: set[str] | None = None,
        raise_on_update: bool = False,
        raise_on_job_attempt: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self.rows = rows or []
        self.judge_runs = judge_runs or {}
        self.bundles = bundles or {}
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
        if normalized == _normalize(module.SELECT_PENDING_JUDGE_CALL_REQUESTED_EVENTS_QUERY):
            limit = int(params.get("limit", len(self.rows)))
            return FakeResult(rows=self.rows[:limit])
        if normalized == _normalize(module.SELECT_JUDGE_RUN_STATE_QUERY):
            row = self.judge_runs.get(str(params["judge_run_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(module.SELECT_BUNDLE_READY_QUERY):
            row = self.bundles.get(str(params["bundle_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("UPDATE event_outbox"):
            if self.raise_on_update:
                raise RuntimeError("db update failed " + FAKE_SENSITIVE_VALUE)
            if self.order is not None:
                self.order.append("db:update_published")
            self.published_event_ids.append(UUID(str(params["event_id"])))
            return FakeResult()
        if normalized.startswith("INSERT INTO job_attempts"):
            if self.raise_on_job_attempt:
                raise RuntimeError("job attempt failed " + FAKE_SENSITIVE_VALUE)
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
            raise RuntimeError("redis publish failed " + FAKE_SENSITIVE_VALUE)
        return FAKE_STREAM_ID

    async def close(self) -> None:
        self.closed = True


class MismatchedRouteResolver:
    def resolve(self, _row: Any) -> Any:
        module = _module()
        return module.QueueRoute(queue_name="q.analysis.route", stage_name="analysis_route")


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_judge_call_requested_route_publish_smoke"
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
        judge_call_route_publish=True,
        redis_publish=True,
        event_outbox_update=True,
        job_attempt_write=True,
    )


def _approvals(**overrides: bool) -> Any:
    values = {
        "judge_call_route_publish": False,
        "redis_publish": False,
        "event_outbox_update": False,
        "job_attempt_write": False,
    }
    values.update(overrides)
    return _module().PublishApprovals(**values)


def _fake_row(
    *,
    event_id: UUID | None = None,
    judge_run_id: UUID | None = None,
    bundle_id: UUID | None = None,
    aggregate_type: str = "judge_run",
    aggregate_id: UUID | None = None,
    event_type: str = "judge.call.requested.v1",
    status: str = "pending",
    payload_overrides: dict[str, Any] | None = None,
    omit_payload_fields: set[str] | None = None,
    dedupe_key: str = FAKE_DEDUPE_KEY,
) -> dict[str, Any]:
    event_id = event_id or uuid4()
    judge_run_id = judge_run_id or uuid4()
    bundle_id = bundle_id or uuid4()
    payload: dict[str, Any] = {
        "judge_run_id": str(judge_run_id),
        "bundle_id": str(bundle_id),
        "model": "gpt-5.4-mini",
        "reasoning_effort": "low",
        "prompt_version": "judge_github_primary_v1",
        "prompt_cache_key": "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1",
        "source_text": FAKE_SOURCE_TEXT,
        "canonical_url": FAKE_URL,
    }
    payload.update(payload_overrides or {})
    for field in omit_payload_fields or set():
        payload.pop(field, None)
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id or judge_run_id,
        "dedupe_key": dedupe_key,
        "payload_json": payload,
        "status": status,
        "fail_count": 0,
        "created_at": datetime.now(timezone.utc),
    }


def _judge_run_for_row(
    row: dict[str, Any],
    *,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = row["payload_json"]
    judge_run = {
        "judge_run_id": UUID(str(payload["judge_run_id"])),
        "bundle_id": UUID(str(payload["bundle_id"])),
        "status": "pending",
        "model": payload["model"],
        "reasoning_effort": payload["reasoning_effort"],
        "prompt_version": payload["prompt_version"],
        "prompt_cache_key": payload["prompt_cache_key"],
        "schema_version": "judge_output_v1",
        "policy_version": "verdict_policy_v1",
    }
    judge_run.update(overrides or {})
    return judge_run


def _bundle_for_row(row: dict[str, Any], *, ready: bool = True) -> dict[str, Any]:
    bundle_id = UUID(str(row["payload_json"]["bundle_id"]))
    return {"bundle_id": bundle_id, "ready_for_analysis": ready}


def _session_for_row(
    row: dict[str, Any],
    *,
    judge_run_overrides: dict[str, Any] | None = None,
    include_judge_run: bool = True,
    include_bundle: bool = True,
    bundle_ready: bool = True,
    **session_kwargs: Any,
) -> FakeSession:
    judge_runs = {}
    bundles = {}
    if include_judge_run:
        judge_run = _judge_run_for_row(row, overrides=judge_run_overrides)
        judge_runs[str(judge_run["judge_run_id"])] = judge_run
    if include_bundle:
        bundle = _bundle_for_row(row, ready=bundle_ready)
        bundles[str(bundle["bundle_id"])] = bundle
    return FakeSession([row], judge_runs=judge_runs, bundles=bundles, **session_kwargs)


def _run_report(
    *,
    row: dict[str, Any] | None = None,
    rows: list[dict[str, Any]] | None = None,
    approvals: Any | None = None,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    route_resolver: Any | None = None,
    side_effect_flags: dict[str, bool] | None = None,
) -> tuple[Any, FakeSession, FakeRedis]:
    module = _module()
    if rows is None and row is not None:
        rows = [row]
    if session is None:
        session = _session_for_row(row) if row is not None else FakeSession(rows or [])
    fake_redis = redis or FakeRedis()
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        approvals=approvals,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: session,
        redis_client_factory=lambda _url: fake_redis,
        route_resolver=route_resolver,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_SENSITIVE_VALUE),
    )
    return result, session, fake_redis


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_mode_is_read_only_and_does_not_publish_update_or_write() -> None:
    row = _fake_row()
    result, session, redis = _run_report(row=row)

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_READY
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["judge_call_requested_pending_event_found_bucket"] == "one"
    assert result.report["redis_publish_attempted"] is False
    assert result.report["event_outbox_update_attempted"] is False
    assert result.report["job_attempt_write_attempted"] is False
    assert redis.xadd_calls == []
    assert redis.xlen_calls == ["q.analysis.judge"]
    assert session.published_event_ids == []
    assert session.job_attempts == []
    assert session.rolled_back is True


def test_partial_approvals_fail_before_db_or_redis_connections() -> None:
    row = _fake_row()
    session = _session_for_row(row)
    redis = FakeRedis()
    result, session, redis = _run_report(
        row=row,
        session=session,
        redis=redis,
        approvals=_approvals(
            judge_call_route_publish=True,
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


def test_no_pending_judge_call_requested_event_fails_closed() -> None:
    result, session, redis = _run_report(rows=[])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NO_PENDING
    assert result.report["judge_call_requested_pending_event_found_bucket"] == "zero"
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_multiple_pending_events_fail_closed_before_publish() -> None:
    first = _fake_row()
    second = _fake_row()
    session = FakeSession(
        [first, second],
        judge_runs={str(first["aggregate_id"]): _judge_run_for_row(first)},
        bundles={str(first["payload_json"]["bundle_id"]): _bundle_for_row(first)},
    )
    result, session, redis = _run_report(rows=[first, second], session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT
    assert result.report["judge_call_requested_pending_event_found_bucket"] == "multiple"
    assert "event_outbox.pending_count_not_one" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []


def test_malformed_payload_fails_before_publish() -> None:
    row = _fake_row(omit_payload_fields={"prompt_cache_key"})
    session = FakeSession([row])
    result, session, redis = _run_report(row=row, session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT
    assert "payload.prompt_cache_key" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_invalid_aggregate_type_fails_before_publish() -> None:
    row = _fake_row(aggregate_type="candidate_group")
    result, session, redis = _run_report(row=row, session=FakeSession([row]))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT
    assert "event_outbox.aggregate_type" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []


def test_missing_linked_judge_run_fails_before_publish() -> None:
    row = _fake_row()
    result, session, redis = _run_report(row=row, session=_session_for_row(row, include_judge_run=False))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_JUDGE_RUN
    assert result.report["judge_run_linked_bucket"] == "zero"
    assert "judge_run.exists" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.job_attempts == []


def test_judge_run_payload_mismatch_fails_before_publish() -> None:
    row = _fake_row()
    result, session, redis = _run_report(
        row=row,
        session=_session_for_row(row, judge_run_overrides={"model": "gpt-5.4"}),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_JUDGE_RUN
    assert "judge_run.model" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []


def test_missing_or_not_ready_bundle_fails_before_publish() -> None:
    missing_bundle_row = _fake_row()
    missing_result, missing_session, missing_redis = _run_report(
        row=missing_bundle_row,
        session=_session_for_row(missing_bundle_row, include_bundle=False),
    )
    not_ready_row = _fake_row()
    not_ready_result, not_ready_session, not_ready_redis = _run_report(
        row=not_ready_row,
        session=_session_for_row(not_ready_row, bundle_ready=False),
    )

    assert missing_result.exit_code == 1
    assert missing_result.report["contract_status"] == _module().STATUS_INVALID_BUNDLE
    assert "bundle.exists" in missing_result.report["checks_failed"]
    assert missing_redis.xadd_calls == []
    assert missing_session.published_event_ids == []
    assert not_ready_result.exit_code == 1
    assert not_ready_result.report["contract_status"] == _module().STATUS_INVALID_BUNDLE
    assert "bundle.ready_for_analysis" in not_ready_result.report["checks_failed"]
    assert not_ready_redis.xadd_calls == []
    assert not_ready_session.published_event_ids == []


def test_route_uses_analysis_judge_queue_and_judge_stage() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(row=row)
    text = SCRIPT.read_text(encoding="utf-8")

    assert result.exit_code == 0
    assert "OutboxRouteResolver()" in text
    assert result.report["redis_stream_name_bucket"] == "one"
    assert result.report["redis_thin_payload_valid_bucket"] == "one"


def test_route_mismatch_blocks_before_publish_or_write() -> None:
    row = _fake_row()
    result, session, redis = _run_report(
        row=row,
        approvals=_all_approvals(),
        route_resolver=MismatchedRouteResolver(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_ROUTE_MISMATCH
    assert "route.queue" in result.report["checks_failed"]
    assert redis.xadd_calls == []
    assert session.published_event_ids == []
    assert session.job_attempts == []


def test_redis_payload_is_exactly_thin_field_set() -> None:
    row = _fake_row()
    redis = FakeRedis()
    result, _session, redis = _run_report(
        row=row,
        redis=redis,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    stream_name, fields, _options = redis.xadd_calls[0]
    assert stream_name == "q.analysis.judge"
    assert set(fields) == _module().ALLOWED_REDIS_THIN_FIELDS
    assert fields["job_id"] == str(row["event_id"])
    assert fields["stage_name"] == "judge"
    assert fields["root_object_type"] == "judge_run"
    assert fields["root_object_id"] == str(row["aggregate_id"])
    assert fields["idempotency_key"] == row["dedupe_key"]
    assert fields["pipeline_run_id"] == ""
    assert fields["not_before"] == ""
    assert fields["trigger_event_id"] == str(row["event_id"])
    rendered_fields = json.dumps(fields, sort_keys=True)
    assert "payload_json" not in rendered_fields
    assert FAKE_SOURCE_TEXT not in rendered_fields
    assert FAKE_URL not in rendered_fields


def test_approved_mode_publishes_then_marks_outbox_then_writes_job_attempt_then_commits() -> None:
    order: list[str] = []
    row = _fake_row()
    session = _session_for_row(row, order=order)
    redis = FakeRedis(order=order)
    result, session, redis = _run_report(
        row=row,
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_PUBLISHED
    assert result.report["redis_publish_succeeded_bucket"] == "one"
    assert session.published_event_ids == [row["event_id"]]
    assert len(session.job_attempts) == 1
    attempt = session.job_attempts[0]
    assert attempt["stage_name"] == "judge"
    assert attempt["queue_name"] == "q.analysis.judge"
    assert attempt["root_object_type"] == "judge_run"
    assert attempt["root_object_id"] == str(row["aggregate_id"])
    assert attempt["attempt_status"] == "succeeded"
    assert order[:4] == [
        "redis:xadd",
        "db:update_published",
        "db:insert_job_attempt",
        "db:commit",
    ]


def test_redis_publish_failure_rolls_back_and_does_not_mark_outbox_or_job_attempt() -> None:
    row = _fake_row()
    redis = FakeRedis(fail_xadd=True)
    result, session, redis = _run_report(
        row=row,
        redis=redis,
        approvals=_all_approvals(),
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


def test_db_write_failure_after_redis_publish_is_sanitized() -> None:
    row = _fake_row()
    session = _session_for_row(row, raise_on_job_attempt=True)
    redis = FakeRedis()
    result, session, redis = _run_report(
        row=row,
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
    assert FAKE_SENSITIVE_VALUE not in rendered
    assert "job attempt failed" not in rendered


def test_no_openai_judge_output_validator_policy_notifier_or_telegram_side_effects() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(row=row)
    text = SCRIPT.read_text(encoding="utf-8")

    assert result.report["judge_openai_started"] is False
    assert result.report["judge_output_written_bucket"] == "zero"
    assert result.report["analysis_validator_started"] is False
    assert result.report["analysis_written_bucket"] == "zero"
    assert result.report["policy_engine_started"] is False
    assert result.report["notification_plan_written_bucket"] == "zero"
    assert result.report["notifier_started"] is False
    assert result.report["telegram_send_attempted"] is False
    assert "src.services.judge_openai" not in text
    assert "src.services.analysis_validator" not in text
    assert "src.services.policy_engine" not in text
    assert "src.services.notifier_telegram" not in text


def test_no_forbidden_table_mutation_or_judge_output_ready_write_occurs() -> None:
    row = _fake_row()
    result, session, _redis = _run_report(row=row, approvals=_all_approvals())
    all_sql = "\n".join(session.statements).lower()

    assert result.exit_code == 0
    assert result.report["judge_output_ready_outbox_written_bucket"] == "zero"
    assert result.report["source_tables_mutation_performed"] is False
    assert result.report["telegram_raw_updates_mutation_performed"] is False
    assert result.report["candidate_mutation_performed"] is False
    assert result.report["artifact_registry_mutation_performed"] is False
    assert result.report["artifact_snapshot_mutation_performed"] is False
    assert result.report["evidence_bundle_mutation_performed"] is False
    assert "insert into judge_outputs" not in all_sql
    assert "judge.output.ready.v1" not in all_sql
    assert "insert into event_outbox" not in all_sql


def test_forbidden_side_effect_flags_block() -> None:
    result, _session, _redis = _run_report(
        row=_fake_row(),
        side_effect_flags={"judge_output_written_bucket": True},
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert "side_effect.forbidden" in result.report["checks_failed"]


def test_redaction_guard_blocks_raw_ids_urls_credentials_stream_id_payload_json_and_exception_body() -> None:
    event_id = uuid4()
    judge_run_id = uuid4()
    bundle_id = uuid4()
    row = _fake_row(
        event_id=event_id,
        judge_run_id=judge_run_id,
        bundle_id=bundle_id,
        dedupe_key=FAKE_DEDUPE_KEY,
    )
    result, _session, _redis = _run_report(row=row, approvals=_all_approvals())
    rendered = json.dumps(result.report, sort_keys=True)

    forbidden_values = (
        str(event_id),
        str(judge_run_id),
        str(bundle_id),
        FAKE_DEDUPE_KEY,
        FAKE_SOURCE_TEXT,
        FAKE_URL,
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_DATABASE_CREDENTIAL,
        FAKE_REDIS_CREDENTIAL,
        FAKE_RUNTIME_PATH,
        FAKE_STREAM_ID,
        FAKE_SENSITIVE_VALUE,
        json.dumps(row["payload_json"], sort_keys=True),
    )
    for value in forbidden_values:
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False
