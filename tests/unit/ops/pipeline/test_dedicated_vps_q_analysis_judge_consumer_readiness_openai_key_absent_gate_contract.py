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
    / "dedicated_vps_q_analysis_judge_consumer_readiness_openai_key_absent_gate.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-key-absent-gate"
FAKE_REDIS_CREDENTIAL = "unit" + "-redis" + "-credential" + "-key-absent-gate"
FAKE_OPENAI_KEY = "unit" + "-openai" + "-credential" + "-must-not-run"
FAKE_OPENAI_KEY_FILE = "/etc/github-ai-catchbot/private-openai-key-file"
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
        event_rows: dict[str, dict[str, Any]] | None = None,
        *,
        judge_runs: dict[str, dict[str, Any]] | None = None,
        bundles: dict[str, dict[str, Any]] | None = None,
        read_only_value: str = "on",
        missing_tables: set[str] | None = None,
        job_attempt_count: int = 1,
        judge_output_count: int = 0,
        judge_output_ready_count: int = 0,
        analysis_count: int = 0,
        policy_count: int = 0,
        notification_count: int = 0,
        raise_on_query: str | None = None,
    ) -> None:
        self.event_rows = event_rows or {}
        self.judge_runs = judge_runs or {}
        self.bundles = bundles or {}
        self.read_only_value = read_only_value
        self.missing_tables = missing_tables or set()
        self.job_attempt_count = job_attempt_count
        self.judge_output_count = judge_output_count
        self.judge_output_ready_count = judge_output_ready_count
        self.analysis_count = analysis_count
        self.policy_count = policy_count
        self.notification_count = notification_count
        self.raise_on_query = raise_on_query
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.rolled_back = False
        self.closed = False

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(str(statement))
        self.statements.append(normalized)
        self.params.append(dict(params))
        module = _module()

        if self.raise_on_query and self.raise_on_query in normalized:
            raise RuntimeError("query failed " + FAKE_SENSITIVE_VALUE)
        if normalized == _normalize(module.SET_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult()
        if normalized == _normalize(module.SHOW_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult(scalar=self.read_only_value)
        if normalized == _normalize(module.SELECT_ONE_QUERY):
            return FakeResult(scalar=1)
        if normalized == _normalize(module.TABLE_AVAILABLE_QUERY):
            table_name = str(params["qualified_table_name"]).removeprefix("public.")
            return FakeResult(scalar=table_name not in self.missing_tables)
        if normalized == _normalize(module.SELECT_EVENT_OUTBOX_BY_ID_QUERY):
            row = self.event_rows.get(str(params["event_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(module.SELECT_JUDGE_RUN_STATE_QUERY):
            row = self.judge_runs.get(str(params["judge_run_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(module.SELECT_BUNDLE_READY_QUERY):
            row = self.bundles.get(str(params["bundle_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(module.COUNT_SUCCEEDED_JUDGE_ROUTE_JOB_ATTEMPT_QUERY):
            return FakeResult(scalar=self.job_attempt_count)
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.judge_output_count)
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.judge_output_ready_count)
        if normalized == _normalize(module.COUNT_ANALYSES_FOR_RUN_QUERY):
            return FakeResult(scalar=self.analysis_count)
        if normalized == _normalize(module.COUNT_POLICY_SIDE_EFFECTS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.policy_count)
        if normalized == _normalize(module.COUNT_NOTIFICATION_PLANS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.notification_count)

        raise AssertionError(f"unexpected SQL: {statement}")

    async def rollback(self) -> None:
        self.rolled_back = True

    async def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(
        self,
        *,
        exists_count: int = 1,
        length: int = 1,
        entries: list[tuple[str, dict[str, str]]] | None = None,
        raise_on_ping: bool = False,
    ) -> None:
        self.exists_count = exists_count
        self.length = length
        self.entries = entries if entries is not None else []
        self.raise_on_ping = raise_on_ping
        self.ping_calls = 0
        self.exists_calls: list[str] = []
        self.xlen_calls: list[str] = []
        self.xrevrange_calls: list[tuple[str, int | None]] = []
        self.closed = False

    async def ping(self) -> bool:
        self.ping_calls += 1
        if self.raise_on_ping:
            raise RuntimeError("redis ping failed " + FAKE_SENSITIVE_VALUE)
        return True

    async def exists(self, name: str) -> int:
        self.exists_calls.append(name)
        return self.exists_count

    async def xlen(self, name: str) -> int:
        self.xlen_calls.append(name)
        return self.length

    async def xrevrange(self, name: str, count: int | None = None) -> list[tuple[str, dict[str, str]]]:
        self.xrevrange_calls.append((name, count))
        return self.entries

    async def close(self) -> None:
        self.closed = True


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_q_analysis_judge_consumer_readiness_openai_key_absent_gate"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(
    _path: str | Path,
    *,
    openai_key: bool = False,
    openai_key_file: bool = False,
) -> dict[str, str]:
    values = {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
    }
    if openai_key:
        values["OPENAI_API_KEY"] = FAKE_OPENAI_KEY
    if openai_key_file:
        values["OPENAI_API_KEY_FILE"] = FAKE_OPENAI_KEY_FILE
    return values


def _fake_row(
    *,
    event_id: UUID | None = None,
    judge_run_id: UUID | None = None,
    bundle_id: UUID | None = None,
    aggregate_type: str = "judge_run",
    aggregate_id: UUID | None = None,
    event_type: str = "judge.call.requested.v1",
    status: str = "published",
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
        "published_at": datetime.now(timezone.utc),
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
        "model": payload["model"],
        "reasoning_effort": payload["reasoning_effort"],
        "prompt_version": payload["prompt_version"],
        "prompt_cache_key": payload["prompt_cache_key"],
        "status": "pending",
    }
    judge_run.update(overrides or {})
    return judge_run


def _bundle_for_row(row: dict[str, Any], *, ready: bool = True) -> dict[str, Any]:
    bundle_id = UUID(str(row["payload_json"]["bundle_id"]))
    return {"bundle_id": bundle_id, "ready_for_analysis": ready}


def _redis_entry_for_row(
    row: dict[str, Any],
    *,
    overrides: dict[str, str] | None = None,
    extra: dict[str, str] | None = None,
    remove: set[str] | None = None,
) -> tuple[str, dict[str, str]]:
    fields = {
        "job_id": str(row["event_id"]),
        "stage_name": "judge",
        "root_object_type": "judge_run",
        "root_object_id": str(row["aggregate_id"]),
        "idempotency_key": row["dedupe_key"],
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(row["event_id"]),
    }
    fields.update(overrides or {})
    fields.update(extra or {})
    for field in remove or set():
        fields.pop(field, None)
    return FAKE_STREAM_ID, fields


def _session_for_row(
    row: dict[str, Any],
    *,
    judge_run_overrides: dict[str, Any] | None = None,
    include_event: bool = True,
    include_judge_run: bool = True,
    include_bundle: bool = True,
    bundle_ready: bool = True,
    **session_kwargs: Any,
) -> FakeSession:
    event_rows = {}
    judge_runs = {}
    bundles = {}
    if include_event:
        event_rows[str(row["event_id"])] = row
    if include_judge_run:
        judge_run = _judge_run_for_row(row, overrides=judge_run_overrides)
        judge_runs[str(judge_run["judge_run_id"])] = judge_run
    if include_bundle:
        bundle = _bundle_for_row(row, ready=bundle_ready)
        bundles[str(bundle["bundle_id"])] = bundle
    return FakeSession(event_rows, judge_runs=judge_runs, bundles=bundles, **session_kwargs)


def _run_report(
    *,
    row: dict[str, Any] | None = None,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    side_effect_flags: dict[str, bool] | None = None,
    runtime_openai_key: bool = False,
    runtime_openai_key_file: bool = False,
    forbidden_raw_values: tuple[str, ...] = (),
) -> tuple[Any, FakeSession, FakeRedis]:
    module = _module()
    if row is None:
        row = _fake_row()
    if session is None:
        session = _session_for_row(row)
    if redis is None:
        redis = FakeRedis(entries=[_redis_entry_for_row(row)])
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=lambda path: _runtime_env(
            path,
            openai_key=runtime_openai_key,
            openai_key_file=runtime_openai_key_file,
        ),
        database_session_factory=lambda _url: session,
        redis_client_factory=lambda _url: redis,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_SENSITIVE_VALUE, *forbidden_raw_values),
    )
    return result, session, redis


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_happy_path_passes_with_openai_key_absent_and_reports_bucketed_stop_condition() -> None:
    row = _fake_row()
    result, session, redis = _run_report(row=row)

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_PASSED
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["redis_connected"] is True
    assert result.report["q_analysis_judge_stream_exists"] is True
    assert result.report["q_analysis_judge_length_bucket"] == "one"
    assert result.report["redis_entry_shape_valid_bucket"] == "one"
    assert result.report["redis_entry_stage_judge_bucket"] == "one"
    assert result.report["redis_entry_root_judge_run_bucket"] == "one"
    assert result.report["event_outbox_rehydrated_bucket"] == "one"
    assert result.report["judge_call_requested_published_bucket"] == "one"
    assert result.report["judge_run_linked_bucket"] == "one"
    assert result.report["judge_run_pending_bucket"] == "one"
    assert result.report["bundle_ready_for_analysis_bucket"] == "one"
    assert result.report["route_publish_job_attempt_succeeded_bucket"] == "one"
    assert result.report["openai_api_key_configured"] is False
    assert result.report["openai_api_key_file_configured"] is False
    assert result.report["openai_execution_blocked_by_missing_key"] is True
    assert result.report["openai_call_attempted"] is False
    assert result.report["checks_failed"] == []
    assert session.rolled_back is True
    assert redis.exists_calls == ["q.analysis.judge"]
    assert redis.xlen_calls == ["q.analysis.judge"]
    assert redis.xrevrange_calls == [("q.analysis.judge", 1)]
    assert not _mutating_sql_seen(session.statements)


def test_read_only_transaction_is_required_and_verified() -> None:
    row = _fake_row()
    session = _session_for_row(row, read_only_value="off")
    result, session, _redis = _run_report(row=row, session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NOT_READY
    assert result.report["read_only_transaction"] is False
    assert "database.read_only_transaction" in result.report["checks_failed"]
    assert _normalize(_module().SET_TRANSACTION_READ_ONLY_QUERY) in session.statements
    assert _normalize(_module().SHOW_TRANSACTION_READ_ONLY_QUERY) in session.statements
    assert not _mutating_sql_seen(session.statements)


def test_redis_stream_missing_zero_or_multiple_entries_fail_closed() -> None:
    row = _fake_row()
    missing_result, _session, missing_redis = _run_report(
        row=row,
        redis=FakeRedis(exists_count=0, length=0, entries=[]),
    )
    zero_result, _session, zero_redis = _run_report(
        row=row,
        redis=FakeRedis(exists_count=1, length=0, entries=[]),
    )
    multiple_result, _session, multiple_redis = _run_report(
        row=row,
        redis=FakeRedis(
            exists_count=1,
            length=2,
            entries=[_redis_entry_for_row(row), _redis_entry_for_row(row)],
        ),
    )

    assert missing_result.exit_code == 1
    assert missing_result.report["contract_status"] == _module().STATUS_INVALID_REDIS_STREAM
    assert "redis.stream_missing" in missing_result.report["checks_failed"]
    assert missing_redis.xrevrange_calls == []

    assert zero_result.exit_code == 1
    assert zero_result.report["q_analysis_judge_length_bucket"] == "zero"
    assert "redis.stream_length_not_one" in zero_result.report["checks_failed"]
    assert zero_redis.xrevrange_calls == []

    assert multiple_result.exit_code == 1
    assert multiple_result.report["q_analysis_judge_length_bucket"] == "multiple"
    assert "redis.stream_length_not_one" in multiple_result.report["checks_failed"]
    assert multiple_redis.xrevrange_calls == []


def test_invalid_redis_field_set_fails_closed() -> None:
    row = _fake_row()
    redis = FakeRedis(entries=[_redis_entry_for_row(row, extra={"payload_json": "{}"})])
    result, _session, _redis = _run_report(row=row, redis=redis)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_REDIS_STREAM
    assert result.report["redis_entry_shape_valid_bucket"] == "zero"
    assert "redis.entry_shape" in result.report["checks_failed"]


def test_invalid_redis_stage_root_and_trigger_fail_closed() -> None:
    row = _fake_row()
    stage_result, _session, _redis = _run_report(
        row=row,
        redis=FakeRedis(entries=[_redis_entry_for_row(row, overrides={"stage_name": "analysis_route"})]),
    )
    root_result, _session, _redis = _run_report(
        row=row,
        redis=FakeRedis(entries=[_redis_entry_for_row(row, overrides={"root_object_type": "candidate_group"})]),
    )
    trigger_result, _session, _redis = _run_report(
        row=row,
        redis=FakeRedis(entries=[_redis_entry_for_row(row, overrides={"trigger_event_id": "not-a-uuid"})]),
    )

    assert stage_result.exit_code == 1
    assert stage_result.report["redis_entry_stage_judge_bucket"] == "zero"
    assert "redis.entry_stage" in stage_result.report["checks_failed"]
    assert root_result.exit_code == 1
    assert root_result.report["redis_entry_root_judge_run_bucket"] == "zero"
    assert "redis.entry_root_type" in root_result.report["checks_failed"]
    assert trigger_result.exit_code == 1
    assert trigger_result.report["redis_entry_trigger_event_id_valid_bucket"] == "zero"
    assert "redis.entry_trigger_event_id" in trigger_result.report["checks_failed"]


def test_event_outbox_missing_or_mismatch_fails_closed() -> None:
    row = _fake_row()
    missing_result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, include_event=False),
    )
    type_result, _session, _redis = _run_report(
        row=(wrong_type := _fake_row(event_type="analysis.requested.v1")),
        session=_session_for_row(wrong_type),
        redis=FakeRedis(entries=[_redis_entry_for_row(wrong_type)]),
    )
    status_result, _session, _redis = _run_report(
        row=(pending_row := _fake_row(status="pending")),
        session=_session_for_row(pending_row),
        redis=FakeRedis(entries=[_redis_entry_for_row(pending_row)]),
    )

    assert missing_result.exit_code == 1
    assert missing_result.report["contract_status"] == _module().STATUS_MISSING_EVENT
    assert "event_outbox.exists" in missing_result.report["checks_failed"]
    assert type_result.exit_code == 1
    assert type_result.report["contract_status"] == _module().STATUS_INVALID_EVENT
    assert "event_outbox.event_type" in type_result.report["checks_failed"]
    assert status_result.exit_code == 1
    assert "event_outbox.status" in status_result.report["checks_failed"]


def test_judge_run_missing_non_pending_or_already_executed_fails_closed() -> None:
    row = _fake_row()
    missing_result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, include_judge_run=False),
    )
    running_result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, judge_run_overrides={"status": "running"}),
    )
    executed_result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, judge_run_overrides={"status": "succeeded"}),
    )

    assert missing_result.exit_code == 1
    assert missing_result.report["contract_status"] == _module().STATUS_INVALID_JUDGE_RUN
    assert "judge_run.exists" in missing_result.report["checks_failed"]
    assert running_result.exit_code == 1
    assert running_result.report["judge_run_pending_bucket"] == "zero"
    assert "judge_run.status" in running_result.report["checks_failed"]
    assert executed_result.exit_code == 1
    assert "judge_run.status" in executed_result.report["checks_failed"]


def test_bundle_missing_or_not_ready_fails_closed() -> None:
    row = _fake_row()
    missing_result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, include_bundle=False),
    )
    not_ready_result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, bundle_ready=False),
    )

    assert missing_result.exit_code == 1
    assert missing_result.report["contract_status"] == _module().STATUS_INVALID_BUNDLE
    assert "bundle.exists" in missing_result.report["checks_failed"]
    assert not_ready_result.exit_code == 1
    assert not_ready_result.report["bundle_ready_for_analysis_bucket"] == "zero"
    assert "bundle.ready_for_analysis" in not_ready_result.report["checks_failed"]


def test_route_publish_job_attempt_succeeded_invariant_is_required() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, job_attempt_count=0),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_MISSING_JOB_ATTEMPT
    assert result.report["route_publish_job_attempt_succeeded_bucket"] == "zero"
    assert "job_attempts.succeeded_count_not_one" in result.report["checks_failed"]


def test_downstream_side_effects_fail_closed() -> None:
    row = _fake_row()
    judge_output_result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, judge_output_count=1),
    )
    ready_event_result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, judge_output_ready_count=1),
    )
    analysis_result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, analysis_count=1),
    )
    policy_result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, policy_count=1),
    )
    notification_result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, notification_count=1),
    )

    assert judge_output_result.exit_code == 1
    assert judge_output_result.report["judge_outputs_written_bucket"] == "one"
    assert "downstream.judge_outputs" in judge_output_result.report["checks_failed"]
    assert ready_event_result.exit_code == 1
    assert ready_event_result.report["judge_output_ready_outbox_written_bucket"] == "one"
    assert "downstream.judge_output_ready_outbox" in ready_event_result.report["checks_failed"]
    assert analysis_result.exit_code == 1
    assert analysis_result.report["analyses_written_bucket"] == "one"
    assert "downstream.analyses" in analysis_result.report["checks_failed"]
    assert policy_result.exit_code == 1
    assert policy_result.report["analysis_policy_apply_outbox_written_bucket"] == "one"
    assert "downstream.analysis_policy_apply_outbox" in policy_result.report["checks_failed"]
    assert notification_result.exit_code == 1
    assert notification_result.report["notification_plans_written_bucket"] == "one"
    assert "downstream.notification_plans" in notification_result.report["checks_failed"]


def test_openai_key_or_key_file_unexpectedly_configured_fails_closed_before_runtime_reads() -> None:
    key_result, key_session, key_redis = _run_report(runtime_openai_key=True)
    file_result, file_session, file_redis = _run_report(runtime_openai_key_file=True)

    assert key_result.exit_code == 1
    assert key_result.report["contract_status"] == _module().STATUS_OPENAI_KEY_CONFIGURED
    assert key_result.report["openai_api_key_configured"] is True
    assert key_result.report["openai_api_key_file_configured"] is False
    assert key_result.report["openai_execution_blocked_by_missing_key"] is False
    assert key_session.statements == []
    assert key_redis.ping_calls == 0

    assert file_result.exit_code == 1
    assert file_result.report["contract_status"] == _module().STATUS_OPENAI_KEY_CONFIGURED
    assert file_result.report["openai_api_key_configured"] is False
    assert file_result.report["openai_api_key_file_configured"] is True
    assert file_result.report["openai_execution_blocked_by_missing_key"] is False
    assert file_session.statements == []
    assert file_redis.ping_calls == 0


def test_no_openai_sdk_judge_service_validator_policy_notifier_or_telegram_call_path() -> None:
    result, _session, _redis = _run_report()
    text = SCRIPT.read_text(encoding="utf-8")

    assert result.report["openai_call_attempted"] is False
    assert result.report["judge_openai_started"] is False
    assert result.report["analysis_validator_started"] is False
    assert result.report["policy_engine_started"] is False
    assert result.report["notifier_started"] is False
    assert result.report["telegram_send_attempted"] is False
    assert "src.services.judge_openai" not in text
    assert "src.services.analysis_validator" not in text
    assert "src.services.policy_engine" not in text
    assert "src.services.notifier_telegram" not in text
    assert "from openai" not in text
    assert "import openai" not in text
    assert "AsyncOpenAI" not in text
    assert "responses.create" not in text


def test_no_redis_mutation_or_postgresql_mutating_sql_is_used() -> None:
    result, session, redis = _run_report()
    text = SCRIPT.read_text(encoding="utf-8").lower()

    assert result.exit_code == 0
    assert redis.exists_calls == ["q.analysis.judge"]
    assert redis.xlen_calls == ["q.analysis.judge"]
    assert redis.xrevrange_calls == [("q.analysis.judge", 1)]
    assert not _mutating_sql_seen(session.statements)
    assert "insert into" not in text
    assert "update " not in text
    assert "delete from" not in text
    assert "truncate " not in text
    assert "create table" not in text
    assert "drop table" not in text
    assert "alter table" not in text
    assert "xadd" not in text
    assert "xack" not in text
    assert "xdel" not in text
    assert "xgroup" not in text
    assert "xread" not in text


def test_forbidden_side_effect_flags_block_before_db_or_redis_reads() -> None:
    result, session, redis = _run_report(
        side_effect_flags={"judge_outputs_written_bucket": True},
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert "side_effect.forbidden" in result.report["checks_failed"]
    assert session.statements == []
    assert redis.xrevrange_calls == []


def test_redaction_guard_blocks_ids_urls_credentials_stream_ids_payload_json_and_exception_bodies() -> None:
    event_id = uuid4()
    judge_run_id = uuid4()
    bundle_id = uuid4()
    row = _fake_row(
        event_id=event_id,
        judge_run_id=judge_run_id,
        bundle_id=bundle_id,
        dedupe_key=FAKE_DEDUPE_KEY,
    )
    result, _session, _redis = _run_report(row=row)
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
        FAKE_OPENAI_KEY,
        FAKE_OPENAI_KEY_FILE,
        FAKE_RUNTIME_PATH,
        FAKE_STREAM_ID,
        FAKE_SENSITIVE_VALUE,
        json.dumps(row["payload_json"], sort_keys=True),
    )
    for value in forbidden_values:
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False


def test_exception_body_is_sanitized_when_audit_query_raises() -> None:
    row = _fake_row()
    session = _session_for_row(row, raise_on_query="FROM judge_outputs")
    result, _session, _redis = _run_report(row=row, session=session)
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NOT_READY
    assert "unexpected" in result.report["checks_failed"]
    assert FAKE_SENSITIVE_VALUE not in rendered
    assert "query failed" not in rendered


def _mutating_sql_seen(statements: list[str]) -> bool:
    forbidden = (
        "INSERT INTO",
        "UPDATE ",
        "DELETE FROM",
        "TRUNCATE ",
        "CREATE ",
        "DROP ",
        "ALTER ",
    )
    return any(any(token in statement.upper() for token in forbidden) for statement in statements)
