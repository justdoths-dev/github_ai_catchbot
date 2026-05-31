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
    / "dedicated_vps_judge_call_requested_route_publish_post_publish_read_only_audit.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-post-publish-audit"
FAKE_REDIS_CREDENTIAL = "unit" + "-redis" + "-credential" + "-post-publish-audit"
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
        pending_count: int = 0,
        job_attempt_count: int = 1,
        judge_output_count: int = 0,
        judge_output_ready_count: int = 0,
        analysis_count: int = 0,
        policy_count: int = 0,
        notification_count: int = 0,
        raise_on_query: str | None = None,
    ) -> None:
        self.rows = rows or []
        self.judge_runs = judge_runs or {}
        self.bundles = bundles or {}
        self.read_only_value = read_only_value
        self.missing_tables = missing_tables or set()
        self.pending_count = pending_count
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
        if normalized == _normalize(module.SELECT_RECENT_PUBLISHED_JUDGE_CALL_REQUESTED_QUERY):
            limit = int(params.get("limit", len(self.rows)))
            return FakeResult(rows=self.rows[:limit])
        if normalized == _normalize(module.COUNT_PENDING_JUDGE_CALL_REQUESTED_QUERY):
            return FakeResult(scalar=self.pending_count)
        if normalized == _normalize(module.COUNT_SUCCEEDED_JUDGE_ROUTE_JOB_ATTEMPT_QUERY):
            return FakeResult(scalar=self.job_attempt_count)
        if normalized == _normalize(module.SELECT_JUDGE_RUN_STATE_QUERY):
            row = self.judge_runs.get(str(params["judge_run_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(module.SELECT_BUNDLE_READY_QUERY):
            row = self.bundles.get(str(params["bundle_id"]))
            return FakeResult(rows=[row] if row else [])
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
        "scripts.ops.dedicated_vps_judge_call_requested_route_publish_post_publish_read_only_audit"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
    }


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
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    side_effect_flags: dict[str, bool] | None = None,
    forbidden_raw_values: tuple[str, ...] = (),
) -> tuple[Any, FakeSession, FakeRedis]:
    module = _module()
    if rows is None and row is not None:
        rows = [row]
    if session is None:
        session = _session_for_row(row) if row is not None else FakeSession(rows or [])
    if redis is None:
        if row is not None:
            redis = FakeRedis(entries=[_redis_entry_for_row(row)])
        else:
            redis = FakeRedis(entries=[])
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: session,
        redis_client_factory=lambda _url: redis,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_SENSITIVE_VALUE, *forbidden_raw_values),
    )
    return result, session, redis


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_audit_is_read_only_and_does_not_mutate_db_or_redis() -> None:
    row = _fake_row()
    result, session, redis = _run_report(row=row)

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_PASSED
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["redis_connected"] is True
    assert result.report["recent_judge_call_requested_published_bucket"] == "one"
    assert result.report["recent_judge_route_job_attempt_succeeded_bucket"] == "one"
    assert result.report["pending_judge_call_requested_bucket"] == "zero"
    assert result.report["q_analysis_judge_stream_exists"] is True
    assert result.report["q_analysis_judge_length_bucket"] == "one"
    assert result.report["checks_failed"] == []
    assert session.rolled_back is True
    assert redis.exists_calls == ["q.analysis.judge"]
    assert redis.xlen_calls == ["q.analysis.judge"]
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


def test_recent_published_judge_call_request_found_bucket_one() -> None:
    result, _session, _redis = _run_report(row=_fake_row())

    assert result.exit_code == 0
    assert result.report["recent_judge_call_requested_published_bucket"] == "one"


def test_pending_judge_call_requested_bucket_zero_is_required() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(row=row, session=_session_for_row(row, pending_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_PENDING_REMAINING
    assert result.report["pending_judge_call_requested_bucket"] == "one"
    assert "event_outbox.pending_judge_call_request" in result.report["checks_failed"]


def test_matching_succeeded_job_attempt_found_bucket_one() -> None:
    result, _session, _redis = _run_report(row=_fake_row())

    assert result.exit_code == 0
    assert result.report["recent_judge_route_job_attempt_succeeded_bucket"] == "one"


def test_redis_stream_exists_and_length_bucket_one() -> None:
    result, _session, _redis = _run_report(row=_fake_row())

    assert result.exit_code == 0
    assert result.report["q_analysis_judge_stream_exists"] is True
    assert result.report["q_analysis_judge_length_bucket"] == "one"


def test_redis_entry_thin_field_set_stage_and_root_validation() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(row=row)

    assert result.exit_code == 0
    assert result.report["latest_redis_entry_shape_valid_bucket"] == "one"
    assert result.report["latest_redis_entry_stage_judge_bucket"] == "one"
    assert result.report["latest_redis_entry_root_judge_run_bucket"] == "one"


def test_linked_judge_run_validation_and_ready_bundle_validation() -> None:
    result, _session, _redis = _run_report(row=_fake_row())

    assert result.exit_code == 0
    assert result.report["judge_run_linked_bucket"] == "one"
    assert result.report["bundle_ready_for_analysis_bucket"] == "one"


def test_missing_published_event_fails_closed() -> None:
    result, session, redis = _run_report(rows=[])

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_MISSING_PUBLISHED
    assert result.report["recent_judge_call_requested_published_bucket"] == "zero"
    assert "event_outbox.no_published_judge_call_request" in result.report["checks_failed"]
    assert session.rolled_back is True
    assert redis.xrevrange_calls == []


def test_multiple_published_events_fail_closed_as_ambiguous() -> None:
    first = _fake_row()
    second = _fake_row()
    session = FakeSession(
        [first, second],
        judge_runs={str(first["aggregate_id"]): _judge_run_for_row(first)},
        bundles={str(first["payload_json"]["bundle_id"]): _bundle_for_row(first)},
    )
    result, _session, redis = _run_report(rows=[first, second], session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_AMBIGUOUS_PUBLISHED
    assert result.report["recent_judge_call_requested_published_bucket"] == "multiple"
    assert "event_outbox.published_count_not_one" in result.report["checks_failed"]
    assert redis.xrevrange_calls == []


def test_missing_job_attempt_fails_closed() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(row=row, session=_session_for_row(row, job_attempt_count=0))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_MISSING_JOB_ATTEMPT
    assert result.report["recent_judge_route_job_attempt_succeeded_bucket"] == "zero"
    assert "job_attempts.succeeded_count_not_one" in result.report["checks_failed"]


def test_missing_redis_stream_fails_closed() -> None:
    row = _fake_row()
    redis = FakeRedis(exists_count=0, length=0, entries=[])
    result, _session, redis = _run_report(row=row, redis=redis)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_REDIS_STREAM
    assert result.report["q_analysis_judge_stream_exists"] is False
    assert "redis.stream_missing" in result.report["checks_failed"]
    assert redis.xrevrange_calls == []


def test_invalid_redis_shape_fails_closed() -> None:
    row = _fake_row()
    redis = FakeRedis(entries=[_redis_entry_for_row(row, extra={"payload_json": "{}"})])
    result, _session, _redis = _run_report(row=row, redis=redis)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_REDIS_STREAM
    assert result.report["latest_redis_entry_shape_valid_bucket"] == "zero"
    assert "redis.entry_shape" in result.report["checks_failed"]


def test_invalid_redis_stage_and_root_fail_closed() -> None:
    row = _fake_row()
    stage_result, _session, _redis = _run_report(
        row=row,
        redis=FakeRedis(entries=[_redis_entry_for_row(row, overrides={"stage_name": "analysis_route"})]),
    )
    root_result, _session, _redis = _run_report(
        row=row,
        redis=FakeRedis(entries=[_redis_entry_for_row(row, overrides={"root_object_type": "candidate_group"})]),
    )

    assert stage_result.exit_code == 1
    assert stage_result.report["latest_redis_entry_stage_judge_bucket"] == "zero"
    assert "redis.entry_stage" in stage_result.report["checks_failed"]
    assert root_result.exit_code == 1
    assert root_result.report["latest_redis_entry_root_judge_run_bucket"] == "zero"
    assert "redis.entry_root_type" in root_result.report["checks_failed"]


def test_missing_judge_run_fails_closed() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, include_judge_run=False),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_JUDGE_RUN
    assert result.report["judge_run_linked_bucket"] == "zero"
    assert "judge_run.exists" in result.report["checks_failed"]


def test_not_ready_bundle_fails_closed() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, bundle_ready=False),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_BUNDLE
    assert result.report["bundle_ready_for_analysis_bucket"] == "zero"
    assert "bundle.ready_for_analysis" in result.report["checks_failed"]


def test_downstream_judge_outputs_fail_closed() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(row=row, session=_session_for_row(row, judge_output_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DOWNSTREAM_SIDE_EFFECT
    assert result.report["recent_judge_output_written_bucket"] == "one"
    assert "downstream.judge_outputs" in result.report["checks_failed"]


def test_downstream_judge_output_ready_outbox_fails_closed() -> None:
    row = _fake_row()
    result, _session, _redis = _run_report(
        row=row,
        session=_session_for_row(row, judge_output_ready_count=1),
    )

    assert result.exit_code == 1
    assert result.report["recent_judge_output_ready_outbox_written_bucket"] == "one"
    assert "downstream.judge_output_ready_outbox" in result.report["checks_failed"]


def test_downstream_analysis_policy_and_notification_side_effects_fail_closed() -> None:
    row = _fake_row()
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

    assert analysis_result.exit_code == 1
    assert analysis_result.report["recent_analysis_written_bucket"] == "one"
    assert "downstream.analysis" in analysis_result.report["checks_failed"]
    assert policy_result.exit_code == 1
    assert policy_result.report["recent_policy_side_effect_bucket"] == "one"
    assert "downstream.policy" in policy_result.report["checks_failed"]
    assert notification_result.exit_code == 1
    assert notification_result.report["recent_notification_plan_written_bucket"] == "one"
    assert "downstream.notification_plan" in notification_result.report["checks_failed"]


def test_no_openai_validator_policy_notifier_or_telegram_side_effect_flags_remain_false() -> None:
    result, _session, _redis = _run_report(row=_fake_row())
    text = SCRIPT.read_text(encoding="utf-8")

    assert result.report["judge_openai_started"] is False
    assert result.report["analysis_validator_started"] is False
    assert result.report["policy_engine_started"] is False
    assert result.report["notifier_started"] is False
    assert result.report["telegram_send_attempted"] is False
    assert "src.services.judge_openai" not in text
    assert "src.services.analysis_validator" not in text
    assert "src.services.policy_engine" not in text
    assert "src.services.notifier_telegram" not in text


def test_source_candidate_artifact_snapshot_and_evidence_mutation_flags_remain_false() -> None:
    result, session, _redis = _run_report(row=_fake_row())

    assert result.report["source_tables_mutation_performed"] is False
    assert result.report["telegram_raw_updates_mutation_performed"] is False
    assert result.report["candidate_mutation_performed"] is False
    assert result.report["artifact_registry_mutation_performed"] is False
    assert result.report["artifact_snapshot_mutation_performed"] is False
    assert result.report["evidence_bundle_mutation_performed"] is False
    assert result.report["docker_or_systemd_changed"] is False
    assert result.report["alembic_run"] is False
    assert result.report["external_network_attempted"] is False
    assert not _mutating_sql_seen(session.statements)


def test_forbidden_side_effect_flags_block_before_audit_queries() -> None:
    result, session, redis = _run_report(
        row=_fake_row(),
        side_effect_flags={"recent_judge_output_written_bucket": True},
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert "side_effect.forbidden" in result.report["checks_failed"]
    assert session.statements == []
    assert redis.xrevrange_calls == []


def test_redaction_guard_blocks_raw_ids_urls_credentials_stream_ids_payload_json_and_exception_bodies() -> None:
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
