from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_judge_openai_live_call_result_audit.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-live-result-audit"
FAKE_REDIS_CREDENTIAL = "unit" + "-redis" + "-credential" + "-live-result-audit"
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
FAKE_PROMPT_CONTEXT = "private prompt context live result audit"
FAKE_SOURCE_TEXT = "private source text live result audit"
FAKE_URL = "https" + ":" + "/" + "/" + "example.invalid" + "/private/live-result"
FAKE_STDERR = "private stderr live result audit"
FAKE_OPENAI_KEY_FILE = "/run/secrets/openai-live-result-audit"
FAKE_OPENAI_KEY = "sk" + "-private-live-result-audit"


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

    def scalar_one(self) -> Any:
        return self._scalar

    def mappings(self) -> FakeMappings:
        return FakeMappings(self._rows)

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class FakeSession:
    def __init__(
        self,
        *,
        candidate_rows: list[dict[str, Any]] | None = None,
        output_count: int = 0,
        ready_count: int = 0,
        ready_published_count: int = 0,
        judge_call_count: int = 1,
        judge_call_status_rows: list[dict[str, Any]] | None = None,
        analysis_count: int = 0,
        policy_count: int = 0,
        policy_published_count: int = 0,
        notification_count: int = 0,
        notification_published_count: int = 0,
        event_summaries: dict[str, dict[str, Any]] | None = None,
        missing_tables: set[str] | None = None,
        read_only_value: str = "on",
    ) -> None:
        self.candidate_rows = candidate_rows if candidate_rows is not None else [_candidate_row()]
        self.output_count = output_count
        self.ready_count = ready_count
        self.ready_published_count = ready_published_count
        self.judge_call_count = judge_call_count
        self.judge_call_status_rows = judge_call_status_rows
        self.analysis_count = analysis_count
        self.policy_count = policy_count
        self.policy_published_count = policy_published_count
        self.notification_count = notification_count
        self.notification_published_count = notification_published_count
        self.event_summaries = event_summaries or {}
        self.missing_tables = missing_tables or set()
        self.read_only_value = read_only_value
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.commit_count = 0
        self.rollback_count = 0
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
        if normalized == _normalize(module.SELECT_REPLAY_LIVE_SMOKE_CANDIDATES_QUERY):
            return FakeResult(rows=self.candidate_rows[: int(params["limit"])])
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.output_count)
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.ready_count)
        if normalized == _normalize(module.COUNT_PUBLISHED_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.ready_published_count)
        if normalized == _normalize(module.COUNT_JUDGE_CALL_REQUESTED_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.judge_call_count)
        if normalized == _normalize(module.SELECT_JUDGE_CALL_REQUESTED_OUTBOX_STATUS_BUCKETS_QUERY):
            rows = self.judge_call_status_rows
            if rows is None:
                rows = [{"status": "pending", "status_count": self.judge_call_count}]
            return FakeResult(rows=rows)
        if normalized == _normalize(module.COUNT_ANALYSES_FOR_RUN_QUERY):
            return FakeResult(scalar=self.analysis_count)
        if normalized == _normalize(module.COUNT_POLICY_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.policy_count)
        if normalized == _normalize(module.COUNT_PUBLISHED_POLICY_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.policy_published_count)
        if normalized == _normalize(module.COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.notification_count)
        if normalized == _normalize(module.COUNT_PUBLISHED_NOTIFICATION_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.notification_published_count)
        if normalized == _normalize(module.SELECT_EVENT_SUMMARY_BY_ID_QUERY):
            row = self.event_summaries.get(str(params["event_id"]))
            return FakeResult(rows=[row] if row else [])

        raise AssertionError(f"unexpected SQL: {statement}")

    async def commit(self) -> None:
        self.commit_count += 1

    async def rollback(self) -> None:
        self.rollback_count += 1

    async def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(self, *, entries: list[tuple[str, dict[str, str]]] | None = None) -> None:
        self.entries = entries if entries is not None else []
        self.ping_calls = 0
        self.xrevrange_calls: list[tuple[str, int | None]] = []
        self.xadd_calls: list[Any] = []
        self.xack_calls: list[Any] = []
        self.xdel_calls: list[Any] = []
        self.xtrim_calls: list[Any] = []
        self.closed = False

    async def ping(self) -> bool:
        self.ping_calls += 1
        return True

    async def xrevrange(self, name: str, count: int | None = None) -> list[tuple[str, dict[str, str]]]:
        self.xrevrange_calls.append((name, count))
        return self.entries[: count or len(self.entries)]

    async def xadd(self, *args: Any, **kwargs: Any) -> None:
        self.xadd_calls.append((args, kwargs))
        raise AssertionError("redis write attempted")

    async def xack(self, *args: Any, **kwargs: Any) -> None:
        self.xack_calls.append((args, kwargs))
        raise AssertionError("redis ack attempted")

    async def xdel(self, *args: Any, **kwargs: Any) -> None:
        self.xdel_calls.append((args, kwargs))
        raise AssertionError("redis delete attempted")

    async def xtrim(self, *args: Any, **kwargs: Any) -> None:
        self.xtrim_calls.append((args, kwargs))
        raise AssertionError("redis trim attempted")

    async def close(self) -> None:
        self.closed = True


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_judge_openai_live_call_result_audit"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "OPENAI_API_KEY_FILE": FAKE_OPENAI_KEY_FILE,
        "OPENAI_API_KEY": FAKE_OPENAI_KEY,
    }


def _candidate_row(
    *,
    judge_run_id: UUID | None = None,
    bundle_id: UUID | None = None,
    event_id: UUID | None = None,
    status: str = "pending",
    schema_retry_count: int = 0,
    refusal_detected: bool = False,
    recency_at: datetime | None = None,
    created_at: datetime | None = None,
    outbox_status: str = "pending",
) -> dict[str, Any]:
    now = datetime(2026, 6, 1, 1, 2, 3, tzinfo=timezone.utc)
    return {
        "judge_run_id": judge_run_id or uuid4(),
        "bundle_id": bundle_id or uuid4(),
        "judge_run_status": status,
        "schema_retry_count": schema_retry_count,
        "refusal_detected": refusal_detected,
        "judge_call_requested_event_id": event_id or uuid4(),
        "judge_call_requested_status": outbox_status,
        "judge_call_requested_fail_count": 0,
        "recency_at": recency_at or now,
        "judge_call_requested_created_at": created_at or now,
    }


def _event_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": row["judge_call_requested_event_id"],
        "event_type": "judge.call.requested.v1",
        "aggregate_type": "judge_run",
        "aggregate_id": row["judge_run_id"],
        "status": "pending",
    }


def _redis_entry(row: dict[str, Any]) -> tuple[str, dict[str, str]]:
    return (
        FAKE_STREAM_ID,
        {
            "job_id": str(row["judge_call_requested_event_id"]),
            "stage_name": "judge",
            "root_object_type": "judge_run",
            "root_object_id": str(row["judge_run_id"]),
            "idempotency_key": "judge-call:" + str(row["judge_run_id"]),
            "pipeline_run_id": "",
            "not_before": "",
            "trigger_event_id": str(row["judge_call_requested_event_id"]),
        },
    )


def _run_report(
    *,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    forbidden_raw_values: tuple[str, ...] = (),
) -> tuple[Any, FakeSession, FakeRedis]:
    module = _module()
    session = session or FakeSession()
    redis = redis or FakeRedis()
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: session,
        redis_client_factory=lambda _url: redis,
        forbidden_raw_values=(
            FAKE_RUNTIME_PATH,
            FAKE_DATABASE_URL,
            FAKE_REDIS_URL,
            FAKE_OPENAI_KEY_FILE,
            FAKE_OPENAI_KEY,
            FAKE_PROMPT_CONTEXT,
            FAKE_SOURCE_TEXT,
            FAKE_URL,
            FAKE_STDERR,
            *forbidden_raw_values,
        ),
    )
    return result, session, redis


def _assert_no_write_or_mutation_attempts(
    result: Any,
    session: FakeSession,
    redis: FakeRedis,
) -> None:
    assert session.commit_count == 0
    assert session.rollback_count == 1
    assert redis.xadd_calls == []
    assert redis.xack_calls == []
    assert redis.xdel_calls == []
    assert redis.xtrim_calls == []
    assert result.report["database_write_attempted"] is False
    assert result.report["redis_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert result.report["redis_delete_or_trim_attempted"] is False
    for statement in session.statements:
        assert statement.split()[0].upper() in {"SET", "SHOW", "SELECT", "WITH"}


def _assert_no_raw_values(result: Any, *values: str) -> None:
    rendered = _module().render_json(result.report)
    for value in values:
        assert value not in rendered


def test_script_exists_and_direct_file_path_execution_from_repo_root_without_pythonpath(
    tmp_path: Path,
) -> None:
    assert SCRIPT.exists()
    missing_runtime_env = tmp_path / "missing-runtime.env"
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)

    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--runtime-env-path", str(missing_runtime_env)],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )

    combined_output = completed.stdout + completed.stderr
    assert "ModuleNotFoundError" not in combined_output
    report = json.loads(completed.stdout)
    assert completed.returncode == 1
    assert report["contract_status"] == _module().STATUS_NOT_READY
    assert report["runtime_env_read"] is False
    assert report["checks_failed"] == ["runtime_env.read"]


def test_default_run_is_read_only_no_db_write_no_redis_mutation_no_openai_no_key_read() -> None:
    result, session, redis = _run_report()

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_PASSED
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["openai_call_attempted"] is False
    assert result.report["openai_key_file_read_bucket"] == "zero"
    _assert_no_write_or_mutation_attempts(result, session, redis)


def test_no_replay_candidate_blocks() -> None:
    result, session, redis = _run_report(session=FakeSession(candidate_rows=[]))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NO_CANDIDATE
    assert result.report["replay_live_smoke_candidate_found_bucket"] == "zero"
    assert result.report["checks_failed"] == ["candidate.none"]
    _assert_no_write_or_mutation_attempts(result, session, redis)


def test_one_replay_candidate_reports_passed() -> None:
    row = _candidate_row(status="pending")
    result, session, redis = _run_report(
        session=FakeSession(candidate_rows=[row]),
        forbidden_raw_values=(str(row["judge_run_id"]), str(row["bundle_id"])),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_PASSED
    assert result.report["replay_live_smoke_candidate_found_bucket"] == "one"
    assert result.report["replay_judge_run_status_bucket"] == "pending"
    _assert_no_raw_values(result, str(row["judge_run_id"]), str(row["bundle_id"]))
    _assert_no_write_or_mutation_attempts(result, session, redis)


def test_same_recency_multiple_candidates_blocks_as_ambiguous() -> None:
    timestamp = datetime(2026, 6, 1, 1, 2, 3, tzinfo=timezone.utc)
    rows = [
        _candidate_row(recency_at=timestamp, created_at=timestamp),
        _candidate_row(recency_at=timestamp, created_at=timestamp),
    ]

    result, session, redis = _run_report(session=FakeSession(candidate_rows=rows))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_AMBIGUOUS_CANDIDATE
    assert result.report["replay_live_smoke_candidate_found_bucket"] == "multiple"
    assert result.report["checks_failed"] == ["candidate.ambiguous_recency"]
    _assert_no_write_or_mutation_attempts(result, session, redis)


def test_pending_judge_run_sets_pending_and_active_buckets() -> None:
    result, session, redis = _run_report(session=FakeSession(candidate_rows=[_candidate_row(status="pending")]))

    assert result.report["replay_judge_run_status_bucket"] == "pending"
    assert result.report["replay_judge_run_pending_bucket"] == "one"
    assert result.report["replay_judge_run_terminal_bucket"] == "zero"
    assert result.report["replay_judge_run_active_bucket"] == "one"
    _assert_no_write_or_mutation_attempts(result, session, redis)


def test_succeeded_and_failed_terminal_judge_runs_set_terminal_bucket() -> None:
    for status in ("succeeded", "failed_terminal"):
        result, session, redis = _run_report(
            session=FakeSession(candidate_rows=[_candidate_row(status=status)])
        )

        assert result.exit_code == 0
        assert result.report["replay_judge_run_status_bucket"] == status
        assert result.report["replay_judge_run_terminal_bucket"] == "one"
        assert result.report["replay_judge_run_pending_bucket"] == "zero"
        _assert_no_write_or_mutation_attempts(result, session, redis)


def test_failed_retryable_and_running_judge_runs_set_retryable_or_active_buckets() -> None:
    retryable, session, redis = _run_report(
        session=FakeSession(candidate_rows=[_candidate_row(status="failed_retryable")])
    )
    assert retryable.report["replay_judge_run_retryable_bucket"] == "one"
    assert retryable.report["replay_judge_run_active_bucket"] == "zero"
    _assert_no_write_or_mutation_attempts(retryable, session, redis)

    running, session, redis = _run_report(
        session=FakeSession(candidate_rows=[_candidate_row(status="running")])
    )
    assert running.report["replay_judge_run_retryable_bucket"] == "one"
    assert running.report["replay_judge_run_active_bucket"] == "one"
    _assert_no_write_or_mutation_attempts(running, session, redis)


def test_schema_retry_and_refusal_buckets_are_sanitized_counts() -> None:
    result, session, redis = _run_report(
        session=FakeSession(
            candidate_rows=[
                _candidate_row(
                    status="failed_retryable",
                    schema_retry_count=2,
                    refusal_detected=True,
                )
            ]
        )
    )

    assert result.report["schema_retry_count_bucket"] == "multiple"
    assert result.report["refusal_detected_bucket"] == "one"
    _assert_no_write_or_mutation_attempts(result, session, redis)


def test_judge_outputs_count_bucket() -> None:
    result, session, redis = _run_report(session=FakeSession(output_count=2))

    assert result.report["judge_outputs_for_run_bucket"] == "multiple"
    _assert_no_write_or_mutation_attempts(result, session, redis)


def test_judge_output_ready_outbox_count_and_validate_publish_flag() -> None:
    result, session, redis = _run_report(
        session=FakeSession(ready_count=1, ready_published_count=1)
    )

    assert result.report["judge_output_ready_outbox_for_run_bucket"] == "one"
    assert result.report["q_analysis_validate_published"] is True
    _assert_no_write_or_mutation_attempts(result, session, redis)


def test_judge_call_requested_outbox_count_and_status_bucket() -> None:
    result, session, redis = _run_report(
        session=FakeSession(
            judge_call_count=1,
            judge_call_status_rows=[{"status": "published", "status_count": 1}],
        )
    )

    assert result.report["judge_call_requested_outbox_for_run_bucket"] == "one"
    assert result.report["judge_call_requested_outbox_status_bucket"] == "published"
    _assert_no_write_or_mutation_attempts(result, session, redis)


def test_q_analysis_judge_active_candidate_bucket_from_redis_scan() -> None:
    row = _candidate_row(status="pending")
    session = FakeSession(
        candidate_rows=[row],
        event_summaries={str(row["judge_call_requested_event_id"]): _event_summary(row)},
    )
    redis = FakeRedis(entries=[_redis_entry(row)])

    result, session, redis = _run_report(
        session=session,
        redis=redis,
        forbidden_raw_values=(FAKE_STREAM_ID, str(row["judge_call_requested_event_id"])),
    )

    assert result.report["q_analysis_judge_scanned_bucket"] == "one"
    assert result.report["q_analysis_judge_active_candidate_bucket"] == "one"
    _assert_no_raw_values(result, FAKE_STREAM_ID, str(row["judge_call_requested_event_id"]))
    _assert_no_write_or_mutation_attempts(result, session, redis)


def test_downstream_analysis_policy_notification_buckets_and_publish_flags() -> None:
    result, session, redis = _run_report(
        session=FakeSession(
            analysis_count=1,
            policy_count=2,
            policy_published_count=1,
            notification_count=2,
            notification_published_count=1,
        )
    )

    assert result.report["analysis_rows_for_run_bucket"] == "one"
    assert result.report["policy_outbox_for_run_bucket"] == "multiple"
    assert result.report["notification_rows_for_run_bucket"] == "multiple"
    assert result.report["q_analysis_policy_published"] is True
    assert result.report["q_notification_send_published"] is True
    _assert_no_write_or_mutation_attempts(result, session, redis)


def test_raw_value_redaction_blocks_uuid_urls_runtime_path_source_prompt_stream_stderr() -> None:
    row = _candidate_row(status="pending")
    result, session, redis = _run_report(
        session=FakeSession(candidate_rows=[row]),
        redis=FakeRedis(entries=[_redis_entry(row)]),
        forbidden_raw_values=(
            str(row["judge_run_id"]),
            str(row["bundle_id"]),
            str(row["judge_call_requested_event_id"]),
            FAKE_STREAM_ID,
        ),
    )

    assert result.report["raw_values_emitted"] is False
    _assert_no_raw_values(
        result,
        str(row["judge_run_id"]),
        str(row["bundle_id"]),
        str(row["judge_call_requested_event_id"]),
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_RUNTIME_PATH,
        FAKE_OPENAI_KEY_FILE,
        FAKE_OPENAI_KEY,
        FAKE_PROMPT_CONTEXT,
        FAKE_SOURCE_TEXT,
        FAKE_URL,
        FAKE_STREAM_ID,
        FAKE_STDERR,
    )
    _assert_no_write_or_mutation_attempts(result, session, redis)


def test_source_contract_has_no_openai_service_imports_or_approval_flags() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "src.services.judge_openai" not in text
    assert "OpenAIJudgeClient" not in text
    assert "--approve" not in text
    assert "OPENAI_API_KEY_FILE" not in text
    assert "OPENAI_API_KEY" not in text


def test_side_effect_flags_remain_false() -> None:
    result, session, redis = _run_report()

    for key in (
        "openai_call_attempted",
        "database_write_attempted",
        "redis_write_attempted",
        "redis_ack_attempted",
        "redis_delete_or_trim_attempted",
        "analysis_validator_started",
        "policy_engine_started",
        "notifier_started",
        "telegram_send_attempted",
        "q_analysis_validate_published",
        "q_analysis_policy_published",
        "q_notification_send_published",
        "raw_values_emitted",
    ):
        assert result.report[key] is False
    _assert_no_write_or_mutation_attempts(result, session, redis)
