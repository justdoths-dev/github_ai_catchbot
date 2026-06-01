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
    / "dedicated_vps_judge_openai_fresh_candidate_readiness.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-fresh-candidate"
FAKE_REDIS_CREDENTIAL = "unit" + "-redis" + "-credential" + "-fresh-candidate"
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
FAKE_URL = "https" + ":" + "/" + "/" + "example.invalid" + "/private/fresh-candidate"
FAKE_SOURCE_TEXT = "private source text fresh candidate"
FAKE_PROMPT_CONTEXT = "private prompt context fresh candidate"
FAKE_SENSITIVE_VALUE = "fake" + "-private-sensitive-value"


class FakeMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None


class FakeResult:
    def __init__(
        self,
        *,
        scalar: Any = None,
        rows: list[dict[str, Any]] | None = None,
        rowcount: int | None = None,
    ) -> None:
        self._scalar = scalar
        self._rows = rows or []
        self.rowcount = rowcount

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
        *,
        analysis_rows: list[dict[str, Any]] | None = None,
        bundle_rows: list[dict[str, Any]] | None = None,
        bundles: dict[str, dict[str, Any]] | None = None,
        shape_stats: dict[str, dict[str, int]] | None = None,
        judge_runs: dict[str, dict[str, Any]] | None = None,
        events_by_id: dict[str, dict[str, Any]] | None = None,
        pending_judge_call_outbox: dict[str, list[dict[str, Any]]] | None = None,
        output_counts: dict[str, int] | None = None,
        ready_outbox_counts: dict[str, int] | None = None,
        analysis_counts: dict[str, int] | None = None,
        notification_counts: dict[str, int] | None = None,
        missing_tables: set[str] | None = None,
        read_only_value: str = "on",
        mark_published_rowcount: int | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.analysis_rows = analysis_rows or []
        self.bundle_rows = bundle_rows or []
        self.bundles = bundles or {}
        self.shape_stats = shape_stats or {}
        self.judge_runs = judge_runs or {}
        self.events_by_id = events_by_id or {}
        self.pending_judge_call_outbox = pending_judge_call_outbox or {}
        self.output_counts = output_counts or {}
        self.ready_outbox_counts = ready_outbox_counts or {}
        self.analysis_counts = analysis_counts or {}
        self.notification_counts = notification_counts or {}
        self.missing_tables = missing_tables or set()
        self.read_only_value = read_only_value
        self.mark_published_rowcount = mark_published_rowcount
        self.order = order
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.inserted_judge_runs: list[dict[str, Any]] = []
        self.inserted_outbox: list[dict[str, Any]] = []
        self.published_event_ids: list[UUID] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def in_transaction(self) -> bool:
        return False

    def begin(self) -> Any:
        raise AssertionError("transaction context is not used by this script")

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
        if normalized == _normalize(module.SELECT_PENDING_ANALYSIS_REQUESTED_EVENTS_QUERY):
            return FakeResult(rows=self.analysis_rows[: int(params["limit"])])
        if normalized == _normalize(module.SELECT_BUNDLE_BY_ID_QUERY):
            row = self.bundles.get(str(params["bundle_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(module.SELECT_BUNDLE_SHAPE_STATS_QUERY):
            row = self.shape_stats.get(
                str(params["bundle_id"]),
                {"member_count": 1, "supporting_count": 0},
            )
            return FakeResult(rows=[row])
        if normalized == _normalize(module.SELECT_JUDGE_RUNS_FOR_DECISION_QUERY):
            rows = [
                row
                for row in self.judge_runs.values()
                if str(row["bundle_id"]) == str(params["bundle_id"])
                and row["prompt_version"] == params["prompt_version"]
                and row["model"] == params["model"]
                and row["reasoning_effort"] == params["reasoning_effort"]
            ]
            return FakeResult(rows=rows)
        if normalized == _normalize(module.SELECT_CURRENT_READY_BUNDLES_QUERY):
            return FakeResult(rows=self.bundle_rows[: int(params["limit"])])
        if normalized == _normalize(module.SELECT_EVENT_OUTBOX_BY_ID_QUERY):
            row = self.events_by_id.get(str(params["event_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(module.SELECT_JUDGE_RUN_BY_ID_QUERY):
            row = self.judge_runs.get(str(params["judge_run_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.output_counts.get(str(params["judge_run_id"]), 0))
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.ready_outbox_counts.get(str(params["judge_run_id"]), 0))
        if normalized == _normalize(module.COUNT_ANALYSES_FOR_RUN_QUERY):
            return FakeResult(scalar=self.analysis_counts.get(str(params["judge_run_id"]), 0))
        if normalized == _normalize(module.COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.notification_counts.get(str(params["judge_run_id"]), 0))
        if normalized == _normalize(module.INSERT_JUDGE_RUN_QUERY):
            judge_run_id = uuid4()
            row = {
                "judge_run_id": judge_run_id,
                "bundle_id": UUID(str(params["bundle_id"])),
                "judge_profile": params["judge_profile"],
                "model": params["model"],
                "reasoning_effort": params["reasoning_effort"],
                "prompt_version": params["prompt_version"],
                "schema_version": params["schema_version"],
                "policy_version": params["policy_version"],
                "prompt_cache_key": params["prompt_cache_key"],
                "status": "pending",
            }
            self.judge_runs[str(judge_run_id)] = row
            self.inserted_judge_runs.append(row)
            if self.order is not None:
                self.order.append("db:insert_judge_run")
            return FakeResult(scalar=judge_run_id)
        if normalized == _normalize(module.SELECT_PENDING_JUDGE_CALL_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(rows=self.pending_judge_call_outbox.get(str(params["judge_run_id"]), []))
        if normalized == _normalize(module.INSERT_JUDGE_CALL_REQUESTED_OUTBOX_QUERY):
            event_id = uuid4()
            payload = json.loads(params["payload_json"])
            row = {
                "event_id": event_id,
                "event_type": "judge.call.requested.v1",
                "aggregate_type": "judge_run",
                "aggregate_id": UUID(str(params["judge_run_id"])),
                "dedupe_key": params["dedupe_key"],
                "payload_json": payload,
                "status": "pending",
                "fail_count": 0,
                "created_at": datetime.now(timezone.utc),
            }
            self.inserted_outbox.append(row)
            self.events_by_id[str(event_id)] = row
            self.pending_judge_call_outbox.setdefault(str(params["judge_run_id"]), []).append(row)
            if self.order is not None:
                self.order.append("db:insert_outbox")
            return FakeResult(rows=[row])
        if normalized == _normalize(module.SELECT_JUDGE_CALL_OUTBOX_BY_DEDUPE_KEY_QUERY):
            for rows in self.pending_judge_call_outbox.values():
                for row in rows:
                    if row["dedupe_key"] == params["dedupe_key"]:
                        return FakeResult(rows=[row])
            return FakeResult(rows=[])
        if normalized == _normalize(module.MARK_EVENT_OUTBOX_PUBLISHED_QUERY):
            row = self.events_by_id.get(str(params["event_id"]))
            rowcount = self.mark_published_rowcount
            if rowcount is None:
                rowcount = 1 if row is not None and row["status"] == "pending" else 0
            if rowcount == 1:
                if row is not None:
                    row["status"] = "published"
                self.published_event_ids.append(UUID(str(params["event_id"])))
            if self.order is not None:
                self.order.append("db:mark_published")
            return FakeResult(rowcount=rowcount)

        raise AssertionError(f"unexpected SQL: {statement}")

    async def commit(self) -> None:
        self.commit_count += 1
        self.committed = True
        if self.order is not None:
            self.order.append("db:commit")

    async def rollback(self) -> None:
        self.rollback_count += 1
        self.rolled_back = True
        if self.order is not None:
            self.order.append("db:rollback")

    async def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(
        self,
        *,
        entries: list[tuple[str, dict[str, str]]] | None = None,
        fail_xadd: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self.entries = entries if entries is not None else []
        self.fail_xadd = fail_xadd
        self.order = order
        self.ping_calls = 0
        self.xrevrange_calls: list[tuple[str, int | None]] = []
        self.xadd_calls: list[tuple[str, dict[str, str], dict[str, Any]]] = []
        self.closed = False

    async def ping(self) -> bool:
        self.ping_calls += 1
        return True

    async def xrevrange(self, name: str, count: int | None = None) -> list[tuple[str, dict[str, str]]]:
        self.xrevrange_calls.append((name, count))
        return self.entries[: count or len(self.entries)]

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


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_judge_openai_fresh_candidate_readiness"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "OUTBOX_RELAY_XADD_MAXLEN": "10000",
    }


def _approvals(**overrides: bool) -> Any:
    values = {"db_write": False, "redis_publish": False}
    values.update(overrides)
    return _module().FreshCandidateApprovals(**values)


def _all_approvals() -> Any:
    return _approvals(db_write=True, redis_publish=True)


def _bundle_row(
    *,
    bundle_id: UUID | None = None,
    candidate_group_id: UUID | None = None,
    current_bundle_id: UUID | None = None,
    ready_for_analysis: bool = True,
    primary_summary: dict[str, Any] | None = None,
    artifact_type: str = "github_repo",
    reroot_count: int = 0,
    token_budget_profile: str | None = None,
) -> dict[str, Any]:
    bundle_id = bundle_id or uuid4()
    candidate_group_id = candidate_group_id or uuid4()
    return {
        "bundle_id": bundle_id,
        "candidate_group_id": candidate_group_id,
        "ready_for_analysis": ready_for_analysis,
        "primary_summary": primary_summary or {"title": "kept out of report"},
        "current_bundle_id": current_bundle_id or bundle_id,
        "artifact_type": artifact_type,
        "reroot_count": reroot_count,
        "token_budget_profile": token_budget_profile,
    }


def _analysis_event(bundle: dict[str, Any], *, event_id: UUID | None = None) -> dict[str, Any]:
    event_id = event_id or uuid4()
    payload = {
        "candidate_group_id": str(bundle["candidate_group_id"]),
        "bundle_id": str(bundle["bundle_id"]),
        "judge_profile": "github_primary",
        "escalation_allowed": False,
        "source_text": FAKE_SOURCE_TEXT,
        "canonical_url": FAKE_URL,
        "prompt_context": FAKE_PROMPT_CONTEXT,
    }
    return {
        "event_id": event_id,
        "event_type": "analysis.requested.v1",
        "aggregate_type": "candidate_group",
        "aggregate_id": bundle["candidate_group_id"],
        "dedupe_key": "analysis-requested:" + str(event_id),
        "payload_json": payload,
        "status": "pending",
        "fail_count": 0,
        "created_at": datetime.now(timezone.utc),
    }


def _judge_run_row(
    bundle: dict[str, Any],
    *,
    judge_run_id: UUID | None = None,
    status: str = "pending",
) -> dict[str, Any]:
    judge_run_id = judge_run_id or uuid4()
    return {
        "judge_run_id": judge_run_id,
        "bundle_id": bundle["bundle_id"],
        "judge_profile": "github_primary",
        "model": "gpt-5.4-mini",
        "reasoning_effort": "low",
        "prompt_version": "judge_github_primary_v1",
        "schema_version": "judge_output_v1",
        "policy_version": "verdict_policy_v1",
        "prompt_cache_key": "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1",
        "status": status,
    }


def _judge_call_outbox(judge_run: dict[str, Any], *, event_id: UUID | None = None) -> dict[str, Any]:
    event_id = event_id or uuid4()
    payload = {
        "judge_run_id": str(judge_run["judge_run_id"]),
        "bundle_id": str(judge_run["bundle_id"]),
        "model": judge_run["model"],
        "reasoning_effort": judge_run["reasoning_effort"],
        "prompt_version": judge_run["prompt_version"],
        "prompt_cache_key": judge_run["prompt_cache_key"],
        "source_text": FAKE_SOURCE_TEXT,
        "canonical_url": FAKE_URL,
    }
    return {
        "event_id": event_id,
        "event_type": "judge.call.requested.v1",
        "aggregate_type": "judge_run",
        "aggregate_id": judge_run["judge_run_id"],
        "dedupe_key": "judge-call:" + str(judge_run["judge_run_id"]),
        "payload_json": payload,
        "status": "pending",
        "fail_count": 0,
        "created_at": datetime.now(timezone.utc),
    }


def _session_with_analysis_candidate(
    *,
    bundle: dict[str, Any] | None = None,
    analysis_rows: list[dict[str, Any]] | None = None,
    judge_runs: dict[str, dict[str, Any]] | None = None,
    pending_judge_call_outbox: dict[str, list[dict[str, Any]]] | None = None,
    **kwargs: Any,
) -> FakeSession:
    bundle = bundle or _bundle_row()
    rows = analysis_rows if analysis_rows is not None else [_analysis_event(bundle)]
    return FakeSession(
        analysis_rows=rows,
        bundles={str(bundle["bundle_id"]): bundle},
        shape_stats={str(bundle["bundle_id"]): {"member_count": 1, "supporting_count": 0}},
        judge_runs=judge_runs,
        pending_judge_call_outbox=pending_judge_call_outbox,
        **kwargs,
    )


def _run_report(
    *,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    approvals: Any | None = None,
    side_effect_flags: dict[str, bool] | None = None,
) -> tuple[Any, FakeSession, FakeRedis]:
    module = _module()
    session = session or _session_with_analysis_candidate()
    redis = redis or FakeRedis()
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        approvals=approvals,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: session,
        redis_client_factory=lambda _url: redis,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_SENSITIVE_VALUE),
    )
    return result, session, redis


def test_script_exists_and_file_path_execution_bootstraps_from_repo_root_without_pythonpath(
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


def test_default_mode_is_read_only_and_performs_no_db_write_redis_mutation_or_openai() -> None:
    result, session, redis = _run_report()

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_PREFLIGHT_PASSED
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["eligible_candidate_found_bucket"] == "one"
    assert session.inserted_judge_runs == []
    assert session.inserted_outbox == []
    assert session.published_event_ids == []
    assert redis.xadd_calls == []
    assert result.report["openai_call_attempted"] is False
    assert result.report["openai_key_file_read_bucket"] == "zero"
    assert session.rolled_back is True


def test_missing_approval_pair_blocks_before_connections() -> None:
    session = _session_with_analysis_candidate()
    redis = FakeRedis()
    result, session, redis = _run_report(
        session=session,
        redis=redis,
        approvals=_approvals(db_write=True),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_MISSING_APPROVAL
    assert "approval.redis_publish" in result.report["checks_failed"]
    assert result.report["database_connected"] is False
    assert result.report["redis_connected"] is False
    assert session.statements == []
    assert redis.xadd_calls == []


def test_zero_eligible_candidate_blocks() -> None:
    session = FakeSession(analysis_rows=[], bundle_rows=[])
    result, session, redis = _run_report(session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NO_CANDIDATE
    assert result.report["eligible_candidate_found_bucket"] == "zero"
    assert redis.xadd_calls == []
    assert session.inserted_judge_runs == []


def test_multiple_eligible_candidates_blocks_as_ambiguous() -> None:
    first_bundle = _bundle_row()
    second_bundle = _bundle_row()
    session = FakeSession(
        analysis_rows=[_analysis_event(first_bundle), _analysis_event(second_bundle)],
        bundles={
            str(first_bundle["bundle_id"]): first_bundle,
            str(second_bundle["bundle_id"]): second_bundle,
        },
        shape_stats={
            str(first_bundle["bundle_id"]): {"member_count": 1, "supporting_count": 0},
            str(second_bundle["bundle_id"]): {"member_count": 1, "supporting_count": 0},
        },
    )

    result, session, redis = _run_report(session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_AMBIGUOUS_CANDIDATE
    assert result.report["analysis_requested_event_found_bucket"] == "multiple"
    assert redis.xadd_calls == []
    assert session.inserted_judge_runs == []


def test_existing_active_q_analysis_judge_candidate_blocks_duplicate_target() -> None:
    bundle = _bundle_row()
    judge_run = _judge_run_row(bundle)
    outbox = _judge_call_outbox(judge_run)
    redis = FakeRedis(
        entries=[
            (
                FAKE_STREAM_ID,
                {
                    "job_id": str(outbox["event_id"]),
                    "stage_name": "judge",
                    "root_object_type": "judge_run",
                    "root_object_id": str(judge_run["judge_run_id"]),
                    "idempotency_key": outbox["dedupe_key"],
                    "pipeline_run_id": "",
                    "not_before": "",
                    "trigger_event_id": str(outbox["event_id"]),
                },
            )
        ]
    )
    session = _session_with_analysis_candidate(
        bundle=bundle,
        judge_runs={str(judge_run["judge_run_id"]): judge_run},
        events_by_id={str(outbox["event_id"]): outbox},
    )

    result, session, redis = _run_report(session=session, redis=redis)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EXISTING_Q_CANDIDATE
    assert result.report["existing_q_analysis_judge_candidate_bucket"] == "one"
    assert redis.xadd_calls == []
    assert session.inserted_judge_runs == []


def test_existing_running_judge_run_blocks_and_safe_pending_run_is_reused() -> None:
    bundle = _bundle_row()
    running = _judge_run_row(bundle, status="running")
    blocked, _blocked_session, _blocked_redis = _run_report(
        session=_session_with_analysis_candidate(
            bundle=bundle,
            judge_runs={str(running["judge_run_id"]): running},
        )
    )

    safe_pending = _judge_run_row(bundle, status="pending")
    prepared, session, redis = _run_report(
        session=_session_with_analysis_candidate(
            bundle=bundle,
            judge_runs={str(safe_pending["judge_run_id"]): safe_pending},
        ),
        approvals=_all_approvals(),
    )

    assert blocked.exit_code == 1
    assert blocked.report["contract_status"] == _module().STATUS_EXISTING_ACTIVE_JUDGE_RUN
    assert prepared.exit_code == 0
    assert prepared.report["contract_status"] == _module().STATUS_APPROVED_PREPARED
    assert prepared.report["judge_run_created_bucket"] == "zero"
    assert len(session.inserted_judge_runs) == 0
    assert len(session.inserted_outbox) == 1
    assert len(redis.xadd_calls) == 1


def test_existing_judge_output_blocks() -> None:
    bundle = _bundle_row()
    judge_run = _judge_run_row(bundle)
    result, session, redis = _run_report(
        session=_session_with_analysis_candidate(
            bundle=bundle,
            judge_runs={str(judge_run["judge_run_id"]): judge_run},
            output_counts={str(judge_run["judge_run_id"]): 1},
        )
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EXISTING_OUTPUT
    assert result.report["existing_judge_output_for_selected_run_bucket"] == "one"
    assert redis.xadd_calls == []
    assert session.inserted_outbox == []


def test_existing_judge_output_ready_outbox_blocks() -> None:
    bundle = _bundle_row()
    judge_run = _judge_run_row(bundle)
    result, session, redis = _run_report(
        session=_session_with_analysis_candidate(
            bundle=bundle,
            judge_runs={str(judge_run["judge_run_id"]): judge_run},
            ready_outbox_counts={str(judge_run["judge_run_id"]): 1},
        )
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EXISTING_READY_OUTBOX
    assert result.report["existing_ready_outbox_for_selected_run_bucket"] == "one"
    assert redis.xadd_calls == []
    assert session.inserted_outbox == []


def test_approved_mode_creates_one_pending_judge_run_and_one_judge_call_outbox() -> None:
    result, session, redis = _run_report(approvals=_all_approvals())

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_APPROVED_PREPARED
    assert result.report["judge_run_created_bucket"] == "one"
    assert result.report["judge_call_requested_outbox_created_bucket"] == "one"
    assert len(session.inserted_judge_runs) == 1
    assert session.inserted_judge_runs[0]["status"] == "pending"
    assert len(session.inserted_outbox) == 1
    assert session.inserted_outbox[0]["event_type"] == "judge.call.requested.v1"
    assert redis.xadd_calls


def test_approved_mode_publishes_exactly_one_thin_redis_message_to_q_analysis_judge() -> None:
    result, _session, redis = _run_report(approvals=_all_approvals())

    assert result.exit_code == 0
    assert result.report["q_analysis_judge_published_bucket"] == "one"
    assert len(redis.xadd_calls) == 1
    stream_name, fields, options = redis.xadd_calls[0]
    assert stream_name == "q.analysis.judge"
    assert set(fields) == _module().ALLOWED_REDIS_THIN_FIELDS
    assert fields["stage_name"] == "judge"
    assert fields["root_object_type"] == "judge_run"
    assert fields["pipeline_run_id"] == ""
    assert fields["not_before"] == ""
    assert options == {"maxlen": 10000, "approximate": True}
    rendered_fields = json.dumps(fields, sort_keys=True)
    assert "payload_json" not in rendered_fields
    assert FAKE_SOURCE_TEXT not in rendered_fields
    assert FAKE_URL not in rendered_fields


def test_event_outbox_is_marked_published_only_after_redis_xadd_success() -> None:
    order: list[str] = []
    result, session, redis = _run_report(
        session=_session_with_analysis_candidate(order=order),
        redis=FakeRedis(order=order),
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["event_outbox_marked_published_bucket"] == "one"
    assert order == [
        "db:insert_judge_run",
        "db:insert_outbox",
        "db:commit",
        "redis:xadd",
        "db:mark_published",
        "db:commit",
    ]
    assert session.commit_count == 2
    assert session.published_event_ids == [session.inserted_outbox[0]["event_id"]]
    assert len(redis.xadd_calls) == 1


def test_redis_publish_failure_after_db_commit_leaves_pending_candidate_for_retry() -> None:
    order: list[str] = []
    result, session, redis = _run_report(
        session=_session_with_analysis_candidate(order=order),
        redis=FakeRedis(fail_xadd=True, order=order),
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_REDIS_PUBLISH_FAILED
    assert result.report["q_analysis_judge_published_bucket"] == "zero"
    assert result.report["event_outbox_marked_published_bucket"] == "zero"
    assert len(redis.xadd_calls) == 1
    assert session.published_event_ids == []
    assert len(session.inserted_judge_runs) == 1
    assert len(session.inserted_outbox) == 1
    assert session.inserted_judge_runs[0]["status"] == "pending"
    assert session.inserted_outbox[0]["status"] == "pending"
    assert order == [
        "db:insert_judge_run",
        "db:insert_outbox",
        "db:commit",
        "redis:xadd",
    ]
    assert session.commit_count == 1
    assert session.rollback_count == 0


def test_mark_published_zero_row_update_fails_closed_after_redis_publish() -> None:
    result, session, redis = _run_report(
        session=_session_with_analysis_candidate(mark_published_rowcount=0),
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_WRITE_FAILED
    assert "event_outbox.mark_published" in result.report["checks_failed"]
    assert result.report["q_analysis_judge_published_bucket"] == "one"
    assert result.report["event_outbox_marked_published_bucket"] == "zero"
    assert len(redis.xadd_calls) == 1
    assert session.published_event_ids == []
    assert session.inserted_outbox[0]["status"] == "pending"
    assert session.commit_count == 1
    assert session.rollback_count == 1


def test_no_openai_key_read_validator_policy_notifier_telegram_or_downstream_side_effects() -> None:
    result, _session, _redis = _run_report()
    text = SCRIPT.read_text(encoding="utf-8")

    assert result.report["openai_call_attempted"] is False
    assert result.report["openai_key_file_read_bucket"] == "zero"
    assert result.report["judge_outputs_written_bucket"] == "zero"
    assert result.report["judge_output_ready_outbox_written_bucket"] == "zero"
    assert result.report["analysis_rows_written_bucket"] == "zero"
    assert result.report["notification_rows_written_bucket"] == "zero"
    assert result.report["analysis_validator_started"] is False
    assert result.report["policy_engine_started"] is False
    assert result.report["notifier_started"] is False
    assert result.report["telegram_send_attempted"] is False
    assert result.report["q_analysis_validate_published"] is False
    assert result.report["q_analysis_policy_published"] is False
    assert result.report["q_notification_send_published"] is False
    assert result.report["redis_ack_attempted"] is False
    assert result.report["redis_delete_or_trim_attempted"] is False
    assert "OPENAI_API_KEY" not in text
    assert "src.services.analysis_validator" not in text
    assert "src.services.policy_engine" not in text
    assert "src.services.notifier_telegram" not in text
    assert "src.services.collector" not in text
    assert "src.services.gh_enricher" not in text
    assert "src.services.x_enricher" not in text
    assert "src.services.web_enricher" not in text
    assert "openai import" not in text.lower()
    assert "telegram" not in "\n".join(
        line for line in text.lower().splitlines() if line.lstrip().startswith("import")
    )


def test_report_contains_no_raw_ids_urls_runtime_path_payload_context_stream_ids_or_secrets() -> None:
    bundle = _bundle_row()
    event = _analysis_event(bundle)
    result, session, _redis = _run_report(
        session=_session_with_analysis_candidate(
            bundle=bundle,
            analysis_rows=[event],
        ),
        approvals=_all_approvals(),
    )
    rendered = json.dumps(result.report, sort_keys=True)
    inserted_judge_run_id = str(session.inserted_judge_runs[0]["judge_run_id"])
    inserted_outbox = session.inserted_outbox[0]

    forbidden_values = (
        str(bundle["bundle_id"]),
        str(bundle["candidate_group_id"]),
        str(event["event_id"]),
        event["dedupe_key"],
        str(inserted_judge_run_id),
        str(inserted_outbox["event_id"]),
        inserted_outbox["dedupe_key"],
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_DATABASE_CREDENTIAL,
        FAKE_REDIS_CREDENTIAL,
        FAKE_RUNTIME_PATH,
        FAKE_STREAM_ID,
        FAKE_URL,
        FAKE_SOURCE_TEXT,
        FAKE_PROMPT_CONTEXT,
        FAKE_SENSITIVE_VALUE,
        json.dumps(event["payload_json"], sort_keys=True),
    )
    for value in forbidden_values:
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False


def test_forbidden_side_effect_flags_block_before_writes() -> None:
    result, session, redis = _run_report(
        side_effect_flags={"analysis_validator_started": True},
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert "side_effect.forbidden" in result.report["checks_failed"]
    assert session.statements == []
    assert redis.xadd_calls == []
