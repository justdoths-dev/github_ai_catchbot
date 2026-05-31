from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_judge_openai_fake_client_db_smoke.py"

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-judge-smoke"
FAKE_REDIS_CREDENTIAL = "unit" + "-redis" + "-credential" + "-judge-smoke"
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
FAKE_PROMPT_CONTEXT = "private prompt context should not be reported"
FAKE_SECRET_PATH = "/etc/github-ai-catchbot/secrets/openai-key"


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
        event_rows: dict[str, dict[str, Any]],
        judge_runs: dict[str, dict[str, Any]],
        bundles: dict[str, dict[str, Any]],
        existing_output_count: int = 0,
        existing_ready_count: int = 0,
        analysis_count: int = 0,
        notification_count: int = 0,
    ) -> None:
        self.event_rows = event_rows
        self.judge_runs = judge_runs
        self.bundles = bundles
        self.existing_output_count = existing_output_count
        self.existing_ready_count = existing_ready_count
        self.analysis_count = analysis_count
        self.notification_count = notification_count
        self.judge_outputs: list[dict[str, Any]] = []
        self.ready_outbox: list[dict[str, Any]] = []
        self.judge_run_updates: list[dict[str, Any]] = []
        self.statements: list[str] = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def in_transaction(self) -> bool:
        return False

    @asynccontextmanager
    async def _begin(self):
        yield self

    def begin(self):
        return self._begin()

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(str(statement))
        self.statements.append(normalized)

        if normalized == _normalize(_module().SET_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult()
        if normalized == _normalize(_module().SHOW_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult(scalar="on")
        if normalized == _normalize(_module().SELECT_ONE_QUERY):
            return FakeResult(scalar=1)
        if "SELECT event_id, event_type, payload_json FROM event_outbox" in normalized:
            row = self.event_rows.get(str(params["event_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT judge_run_id, bundle_id, judge_profile"):
            row = self.judge_runs.get(str(params["judge_run_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT bundle_id, candidate_group_id"):
            row = self.bundles.get(str(params["bundle_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(_module().COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.existing_output_count + len(self.judge_outputs))
        if normalized == _normalize(_module().COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.existing_ready_count + len(self.ready_outbox))
        if normalized == _normalize(_module().COUNT_ANALYSES_FOR_RUN_QUERY):
            return FakeResult(scalar=self.analysis_count)
        if normalized == _normalize(_module().COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.notification_count)
        if normalized == _normalize(_module().COUNT_POLICY_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=0)
        if normalized == _normalize(_module().SELECT_JUDGE_RUN_FINISH_STATE_QUERY):
            row = self.judge_runs.get(str(params["judge_run_id"]))
            if row is None:
                return FakeResult(rows=[])
            return FakeResult(
                rows=[
                    {
                        "status": row["status"],
                        "refusal_detected": row.get("refusal_detected", False),
                    }
                ]
            )
        if normalized.startswith("UPDATE judge_runs SET status = 'running'"):
            row = self.judge_runs[str(params["judge_run_id"])]
            row["status"] = "running"
            self.judge_run_updates.append({"status": "running"})
            return FakeResult()
        if normalized.startswith("UPDATE judge_runs SET status = :status"):
            row = self.judge_runs[str(params["judge_run_id"])]
            row["status"] = params["status"]
            row["refusal_detected"] = params["refusal_detected"]
            self.judge_run_updates.append(
                {
                    "status": params["status"],
                    "refusal_detected": params["refusal_detected"],
                    "finish_reason": params["finish_reason"],
                }
            )
            return FakeResult()
        if normalized.startswith("INSERT INTO judge_outputs"):
            judge_output_id = uuid4()
            payload_json = json.loads(params["payload_json"])
            self.judge_outputs.append(
                {
                    "judge_output_id": judge_output_id,
                    "judge_run_id": UUID(str(params["judge_run_id"])),
                    "candidate_group_id": UUID(str(params["candidate_group_id"])),
                    "payload_json": payload_json,
                    "model_proposed_verdict": params["model_proposed_verdict"],
                    "model_confidence_band": params["model_confidence_band"],
                }
            )
            return FakeResult(scalar=judge_output_id)
        if normalized.startswith("INSERT INTO event_outbox") and "judge.output.ready.v1" in normalized:
            self.ready_outbox.append(
                {
                    "event_type": "judge.output.ready.v1",
                    "judge_run_id": UUID(str(params["judge_run_id"])),
                    "payload_json": json.loads(params["payload_json"]),
                }
            )
            return FakeResult()

        raise AssertionError(f"unexpected SQL: {statement}")

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True

    async def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(
        self,
        *,
        exists_count: int = 1,
        entries: list[tuple[str, dict[str, str]]] | None = None,
    ) -> None:
        self.exists_count = exists_count
        self.entries = entries if entries is not None else []
        self.ping_calls = 0
        self.exists_calls: list[str] = []
        self.xrevrange_calls: list[tuple[str, int | None]] = []
        self.ack_calls: list[Any] = []
        self.closed = False

    async def ping(self) -> bool:
        self.ping_calls += 1
        return True

    async def exists(self, name: str) -> int:
        self.exists_calls.append(name)
        return self.exists_count

    async def xrevrange(self, name: str, count: int | None = None) -> list[tuple[str, dict[str, str]]]:
        self.xrevrange_calls.append((name, count))
        return self.entries[: count or len(self.entries)]

    async def xack(self, *args: Any) -> None:
        self.ack_calls.append(args)

    async def close(self) -> None:
        self.closed = True


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_judge_openai_fake_client_db_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "APP_ENV": "prod",
        "OPENAI_API_KEY_FILE": FAKE_SECRET_PATH,
    }


def _fixtures(*, judge_run_status: str = "pending", include_bundle: bool = True) -> tuple[
    UUID,
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    tuple[str, dict[str, str]],
]:
    trigger_event_id = uuid4()
    judge_run_id = uuid4()
    bundle_id = uuid4()
    candidate_group_id = uuid4()
    artifact_id = uuid4()
    payload = {
        "judge_run_id": str(judge_run_id),
        "bundle_id": str(bundle_id),
        "model": "gpt-5.4-mini",
        "reasoning_effort": "low",
        "prompt_version": "judge_prompt_v1",
        "prompt_cache_key": "judge:github_primary:judge_prompt_v1:judge_output_v1:policy_v1",
        "prompt_context": FAKE_PROMPT_CONTEXT,
    }
    event_rows = {
        str(trigger_event_id): {
            "event_id": trigger_event_id,
            "event_type": "judge.call.requested.v1",
            "payload_json": payload,
        }
    }
    judge_runs = {
        str(judge_run_id): {
            "judge_run_id": judge_run_id,
            "bundle_id": bundle_id,
            "judge_profile": "github_primary",
            "model": payload["model"],
            "reasoning_effort": payload["reasoning_effort"],
            "prompt_version": payload["prompt_version"],
            "schema_version": "judge_output_v1",
            "policy_version": "policy_v1",
            "prompt_cache_key": payload["prompt_cache_key"],
            "status": judge_run_status,
            "schema_retry_count": 0,
            "refusal_detected": False,
        }
    }
    bundles = {}
    if include_bundle:
        bundles[str(bundle_id)] = {
            "bundle_id": bundle_id,
            "candidate_group_id": candidate_group_id,
            "current_primary_artifact_id": artifact_id,
            "primary_summary": {"title": "repo", "summary": "evidence only"},
            "supporting_summaries_json": [{"kind": "repo"}],
            "discovered_links_summary_json": [],
            "evidence_limitations": ["unit fixture"],
            "token_budget_profile": "small",
            "reroot_count": 0,
            "created_at": datetime.now(timezone.utc),
        }
    redis_entry = (
        FAKE_STREAM_ID,
        {
            "job_id": str(trigger_event_id),
            "stage_name": "judge",
            "root_object_type": "judge_run",
            "root_object_id": str(judge_run_id),
            "idempotency_key": "private-dedupe-key",
            "pipeline_run_id": "",
            "not_before": "",
            "trigger_event_id": str(trigger_event_id),
        },
    )
    return trigger_event_id, event_rows, judge_runs, bundles, redis_entry


def _run_report(
    *,
    approve_db_write: bool = False,
    approve_fake_openai: bool = False,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    judge_run_status: str = "pending",
    include_bundle: bool = True,
    existing_output_count: int = 0,
    existing_ready_count: int = 0,
) -> tuple[Any, FakeSession, FakeRedis]:
    _trigger_event_id, event_rows, judge_runs, bundles, redis_entry = _fixtures(
        judge_run_status=judge_run_status,
        include_bundle=include_bundle,
    )
    if session is None:
        session = FakeSession(
            event_rows=event_rows,
            judge_runs=judge_runs,
            bundles=bundles,
            existing_output_count=existing_output_count,
            existing_ready_count=existing_ready_count,
        )
    if redis is None:
        redis = FakeRedis(entries=[redis_entry])
    result = _module().generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        approve_db_write=approve_db_write,
        approve_fake_openai=approve_fake_openai,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: session,
        redis_client_factory=lambda _url: redis,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_SECRET_PATH),
    )
    return result, session, redis


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_file_path_execution_from_repo_root_bootstraps_src_imports_without_pythonpath(
    tmp_path: Path,
) -> None:
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
    assert "No module named 'src'" not in combined_output

    report = json.loads(completed.stdout)
    assert completed.returncode == 1
    assert report["contract_status"] == _module().STATUS_NOT_READY
    assert report["runtime_env_read"] is False
    assert report["checks_failed"] == ["runtime_env.read"]


def test_default_mode_is_read_only_and_performs_no_db_write() -> None:
    result, session, redis = _run_report()

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_PREFLIGHT_PASSED
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["candidate_judge_message_found_bucket"] == "one"
    assert result.report["fake_openai_used"] is False
    assert session.judge_outputs == []
    assert session.ready_outbox == []
    assert session.committed is False
    assert session.rolled_back is True
    assert redis.ack_calls == []


def test_missing_approval_blocks_db_write() -> None:
    result, session, _redis = _run_report(approve_db_write=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_APPROVAL_MISSING
    assert "approval.required_pair" in result.report["checks_failed"]
    assert result.report["fake_openai_used"] is False
    assert session.judge_outputs == []
    assert session.ready_outbox == []
    assert session.committed is False


def test_approved_mode_with_one_valid_fake_candidate_writes_output_run_and_ready_outbox() -> None:
    result, session, redis = _run_report(approve_db_write=True, approve_fake_openai=True)

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_DB_WRITE_PASSED
    assert result.report["fake_openai_used"] is True
    assert result.report["judge_outputs_written_bucket"] == "one"
    assert result.report["judge_run_updated_bucket"] == "one"
    assert result.report["judge_output_ready_outbox_written_bucket"] == "one"
    assert len(session.judge_outputs) == 1
    assert session.judge_run_updates[-1]["status"] == "succeeded"
    assert session.judge_run_updates[-1]["refusal_detected"] is True
    assert len(session.ready_outbox) == 1
    assert session.committed is True
    assert redis.ack_calls == []


def test_fake_response_is_refusal_envelope_not_positive_verdict() -> None:
    result, session, _redis = _run_report(approve_db_write=True, approve_fake_openai=True)

    assert result.exit_code == 0
    payload = session.judge_outputs[0]["payload_json"]
    assert payload["judge_schema_version"] == "judge_output_v1"
    assert payload["output_kind"] == "refusal"
    assert payload["refusal_text"] == _module().FAKE_REFUSAL_TEXT
    assert "model_proposed_verdict" not in payload
    assert session.judge_outputs[0]["model_proposed_verdict"] is None
    assert session.judge_outputs[0]["model_confidence_band"] is None


def test_existing_judge_output_blocks_duplicate_db_write() -> None:
    result, session, _redis = _run_report(
        approve_db_write=True,
        approve_fake_openai=True,
        existing_output_count=1,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DUPLICATE_OUTPUT
    assert result.report["existing_judge_output_for_run_bucket"] == "one"
    assert result.report["fake_openai_used"] is False
    assert session.judge_outputs == []
    assert session.committed is False


def test_existing_ready_outbox_blocks_duplicate_outbox() -> None:
    result, session, _redis = _run_report(
        approve_db_write=True,
        approve_fake_openai=True,
        existing_ready_count=1,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DUPLICATE_READY_OUTBOX
    assert result.report["existing_judge_output_ready_outbox_for_run_bucket"] == "one"
    assert result.report["fake_openai_used"] is False
    assert session.ready_outbox == []


def test_non_pending_judge_run_noops() -> None:
    result, session, _redis = _run_report(
        approve_db_write=True,
        approve_fake_openai=True,
        judge_run_status="succeeded",
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NON_PENDING_RUN
    assert result.report["judge_run_pending_bucket"] == "zero"
    assert result.report["fake_openai_used"] is False
    assert session.judge_outputs == []


def test_missing_bundle_blocks_fake_call_and_write() -> None:
    result, session, _redis = _run_report(
        approve_db_write=True,
        approve_fake_openai=True,
        include_bundle=False,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_MISSING_BUNDLE
    assert result.report["bundle_ready_for_judge_bucket"] == "zero"
    assert result.report["fake_openai_used"] is False
    assert session.judge_outputs == []


def test_zero_redis_candidate_blocks_cleanly() -> None:
    result, _session, redis = _run_report(redis=FakeRedis(entries=[]))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NO_CANDIDATE
    assert result.report["candidate_judge_message_found_bucket"] == "zero"
    assert redis.xrevrange_calls == [("q.analysis.judge", 2)]


def test_multiple_redis_candidates_block_as_ambiguous() -> None:
    _trigger_event_id, event_rows, judge_runs, bundles, redis_entry = _fixtures()
    second = (
        "1710000000001-0",
        {**redis_entry[1], "trigger_event_id": str(uuid4()), "job_id": str(uuid4())},
    )
    session = FakeSession(event_rows=event_rows, judge_runs=judge_runs, bundles=bundles)
    result, session, _redis = _run_report(
        session=session,
        redis=FakeRedis(entries=[redis_entry, second]),
        approve_db_write=True,
        approve_fake_openai=True,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_AMBIGUOUS_CANDIDATE
    assert result.report["candidate_judge_message_found_bucket"] == "multiple"
    assert result.report["fake_openai_used"] is False
    assert session.judge_outputs == []


def test_report_contains_no_raw_ids_urls_runtime_env_prompt_context_or_secret_path() -> None:
    result, _session, _redis = _run_report(approve_db_write=True, approve_fake_openai=True)
    rendered = _module().render_json(result.report)
    _trigger_event_id, event_rows, judge_runs, bundles, redis_entry = _fixtures()
    forbidden_values = {
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_RUNTIME_PATH,
        FAKE_DATABASE_CREDENTIAL,
        FAKE_REDIS_CREDENTIAL,
        FAKE_PROMPT_CONTEXT,
        FAKE_SECRET_PATH,
        FAKE_STREAM_ID,
        redis_entry[1]["idempotency_key"],
    }
    for row in event_rows.values():
        forbidden_values.add(str(row["event_id"]))
        forbidden_values.add(str(row["payload_json"]["judge_run_id"]))
        forbidden_values.add(str(row["payload_json"]["bundle_id"]))
        forbidden_values.add(str(row["payload_json"]["prompt_context"]))
    for row in judge_runs.values():
        forbidden_values.add(str(row["judge_run_id"]))
        forbidden_values.add(str(row["bundle_id"]))
    for row in bundles.values():
        forbidden_values.update(str(value) for value in row.values() if isinstance(value, UUID))

    assert result.report["raw_values_emitted"] is False
    assert not any(value and value in rendered for value in forbidden_values)


def test_no_validator_policy_notifier_or_telegram_side_effect_flags_can_become_true() -> None:
    result, _session, _redis = _run_report(approve_db_write=True, approve_fake_openai=True)

    assert result.report["analysis_validator_started"] is False
    assert result.report["policy_engine_started"] is False
    assert result.report["notifier_started"] is False
    assert result.report["telegram_send_attempted"] is False
    assert result.report["analysis_rows_written_bucket"] == "zero"
    assert result.report["notification_rows_written_bucket"] == "zero"
    assert result.report["q_analysis_validate_published"] is False
    assert result.report["q_analysis_policy_published"] is False
    assert result.report["q_notification_send_published"] is False
    assert result.report["redis_ack_attempted"] is False
    assert result.report["redis_ack_skipped_by_contract"] is True


def test_script_source_has_no_forbidden_adjacent_service_imports_or_openai_sdk_requirement() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    forbidden = (
        "src.services.analysis_validator",
        "src.services.policy_engine",
        "src.services.notifier_telegram",
        "src.services.collector_telegram",
        "src.services.gh_enricher",
        "src.services.x_enricher",
        "src.services.web_enricher",
        "from openai",
        "import openai",
        "OPENAI_API_KEY_FILE",
    )
    for token in forbidden:
        assert token not in text
