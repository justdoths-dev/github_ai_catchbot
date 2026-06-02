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
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_judge_openai_real_context_service_path_persistence_smoke.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-service-path"
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
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-service-path-runtime.env"
FAKE_KEY = "file" + "-openai" + "-secret" + "-service-path"
FAKE_DIRECT_KEY = "direct" + "-openai" + "-secret" + "-service-path"
FAKE_PROJECT = "private" + "-openai" + "-project" + "-service-path"
FAKE_URL = "https" + ":" + "/" + "/" + "example.invalid/private/service-path"
FAKE_SOURCE_TEXT = "private source text service path must not leak"


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
        pending_rows: list[dict[str, Any]] | None = None,
        failed_rows: list[dict[str, Any]] | None = None,
        event_rows: dict[str, dict[str, Any]] | None = None,
        judge_runs: dict[str, dict[str, Any]] | None = None,
        bundles: dict[str, dict[str, Any]] | None = None,
        existing_output_count: int = 0,
        existing_ready_count: int = 0,
        analysis_count: int = 0,
        policy_count: int = 0,
        notification_count: int = 0,
        read_only_value: str = "on",
    ) -> None:
        self.pending_rows = pending_rows or []
        self.failed_rows = failed_rows or []
        self.event_rows = event_rows or {}
        self.judge_runs = judge_runs or {}
        self.bundles = bundles or {}
        self.existing_output_count = existing_output_count
        self.existing_ready_count = existing_ready_count
        self.analysis_count = analysis_count
        self.policy_count = policy_count
        self.notification_count = notification_count
        self.read_only_value = read_only_value
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
        module = _module()

        if normalized == _normalize(module.SET_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult()
        if normalized == _normalize(module.SHOW_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult(scalar=self.read_only_value)
        if normalized == _normalize(module.SELECT_ONE_QUERY):
            return FakeResult(scalar=1)
        if normalized == _normalize(module.SELECT_PENDING_SERVICE_PATH_CANDIDATES_QUERY):
            return FakeResult(rows=self.pending_rows[: int(params["limit"])])
        if normalized == _normalize(module.SELECT_FAILED_REPLAY_SERVICE_PATH_CANDIDATES_QUERY):
            return FakeResult(rows=self.failed_rows[: int(params["limit"])])
        if "SELECT event_id, event_type, payload_json FROM event_outbox" in normalized:
            row = self.event_rows.get(str(params["event_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT judge_run_id, bundle_id, judge_profile"):
            row = self.judge_runs.get(str(params["judge_run_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT bundle_id, candidate_group_id"):
            row = self.bundles.get(str(params["bundle_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(module.COUNT_JUDGE_CALL_REQUESTED_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=1)
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.existing_output_count + len(self.judge_outputs))
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.existing_ready_count + len(self.ready_outbox))
        if normalized == _normalize(module.COUNT_ANALYSES_FOR_RUN_QUERY):
            return FakeResult(scalar=self.analysis_count)
        if normalized == _normalize(module.COUNT_POLICY_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.policy_count)
        if normalized == _normalize(module.COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.notification_count)
        if normalized == _normalize(module.SELECT_JUDGE_RUN_FINISH_STATE_QUERY):
            row = self.judge_runs.get(str(params["judge_run_id"]))
            return FakeResult(rows=[] if row is None else [{"status": row["status"]}])
        if normalized.startswith("UPDATE judge_runs SET status = :status"):
            row = self.judge_runs[str(params["judge_run_id"])]
            row["status"] = params["status"]
            row["refusal_detected"] = params["refusal_detected"]
            self.judge_run_updates.append(
                {
                    "status": params["status"],
                    "finish_reason": params["finish_reason"],
                    "refusal_detected": params["refusal_detected"],
                }
            )
            return FakeResult()
        if normalized.startswith("INSERT INTO judge_outputs"):
            judge_output_id = uuid4()
            self.judge_outputs.append(
                {
                    "judge_output_id": judge_output_id,
                    "judge_run_id": UUID(str(params["judge_run_id"])),
                    "candidate_group_id": UUID(str(params["candidate_group_id"])),
                    "payload_json": json.loads(params["payload_json"]),
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


class FakeLiveOpenAIClient:
    def __init__(self, response: dict[str, Any] | None = None) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    async def create_structured_response(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return self.response or {"status": "completed", "output_text": "{}"}


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_judge_openai_real_context_service_path_persistence_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_reader(key_file: Path | None = None, *, direct_key: bool = False):
    def read(_path: str | Path, include_openai: bool) -> dict[str, str]:
        values = {"DATABASE_URL": FAKE_DATABASE_URL}
        if include_openai:
            if direct_key:
                values["OPENAI_API_KEY"] = FAKE_DIRECT_KEY
            if key_file is not None:
                values["OPENAI_API_KEY_FILE"] = str(key_file)
            values["OPENAI_PROJECT"] = FAKE_PROJECT
        return values

    return read


def _candidate_row(
    *,
    judge_run_id: UUID,
    bundle_id: UUID,
    event_id: UUID,
    status: str,
    finish_reason: str | None,
) -> dict[str, Any]:
    now = datetime(2026, 6, 2, 1, 2, 3, tzinfo=timezone.utc)
    return {
        "judge_run_id": judge_run_id,
        "bundle_id": bundle_id,
        "judge_run_status": status,
        "finish_reason": finish_reason,
        "judge_call_requested_event_id": event_id,
        "judge_call_requested_created_at": now,
        "recency_at": now,
    }


def _valid_payload(candidate_group_id: UUID) -> dict[str, Any]:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate_group_id),
        "headline": "Useful repo",
        "summary_one_line_ko": "summary",
        "skeptical_take_ko": "skeptical",
        "why_it_might_matter_ko": "why",
        "comparables": [],
        "scores": {
            "novelty": 60,
            "practical_usefulness": 70,
            "evidence_strength": 65,
            "hype_penalty": 20,
            "confidence": 55,
            "code_quality": 50,
            "maintenance_signal": 45,
            "specificity": 60,
            "reproducibility_signal": 40,
        },
        "reason_codes": ["has_repo"],
        "red_flags_ko": [],
        "evidence_limitations_ko": ["limited"],
        "recommended_action_ko": "inspect",
        "freshness_note_ko": "fresh",
        "model_proposed_verdict": "later",
        "model_confidence_band": "medium",
    }


def _structured_response(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "completed",
        "output_text": json.dumps(payload),
        "usage": {
            "input_tokens": 100,
            "input_tokens_details": {"cached_tokens": 80},
            "output_tokens": 25,
            "output_tokens_details": {"reasoning_tokens": 7},
        },
    }


def _fixtures(*, failed_replay: bool = False) -> tuple[FakeSession, UUID, UUID, UUID]:
    trigger_event_id = uuid4()
    judge_run_id = uuid4()
    bundle_id = uuid4()
    candidate_group_id = uuid4()
    artifact_id = uuid4()
    status = "failed_terminal" if failed_replay else "pending"
    prompt_version = "judge_prompt_v1"
    if failed_replay:
        prompt_version += "__replay_live_smoke_v1"
    prompt_cache_key = "judge:github_primary:" + prompt_version + ":judge_output_v1:policy_v1"
    payload = {
        "judge_run_id": str(judge_run_id),
        "bundle_id": str(bundle_id),
        "model": "gpt-5.4-mini",
        "reasoning_effort": "low",
        "prompt_version": prompt_version,
        "prompt_cache_key": prompt_cache_key,
        "replay_reason_code": "manual_live_smoke_replay",
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
            "prompt_version": prompt_version,
            "schema_version": "judge_output_v1",
            "policy_version": "policy_v1",
            "prompt_cache_key": prompt_cache_key,
            "status": status,
            "finish_reason": "openai_permanent_error" if failed_replay else None,
            "schema_retry_count": 0,
            "refusal_detected": False,
        }
    }
    bundles = {
        str(bundle_id): {
            "bundle_id": bundle_id,
            "candidate_group_id": candidate_group_id,
            "current_primary_artifact_id": artifact_id,
            "primary_summary": {
                "title": "repo",
                "summary": FAKE_SOURCE_TEXT,
                "private_url": FAKE_URL,
            },
            "supporting_summaries_json": [{"kind": "repo"}],
            "discovered_links_summary_json": [{"url": FAKE_URL}],
            "evidence_limitations": ["unit fixture"],
            "token_budget_profile": "small",
            "reroot_count": 0,
            "created_at": datetime.now(timezone.utc),
        }
    }
    row = _candidate_row(
        judge_run_id=judge_run_id,
        bundle_id=bundle_id,
        event_id=trigger_event_id,
        status=status,
        finish_reason="openai_permanent_error" if failed_replay else None,
    )
    session = FakeSession(
        pending_rows=[] if failed_replay else [row],
        failed_rows=[row] if failed_replay else [],
        event_rows=event_rows,
        judge_runs=judge_runs,
        bundles=bundles,
    )
    return session, judge_run_id, bundle_id, candidate_group_id


def _make_key_file(tmp_path: Path, *, content: str = FAKE_KEY) -> Path:
    key_file = tmp_path / "openai-key"
    key_file.write_text(content, encoding="utf-8")
    return key_file


def _run_preflight(session: FakeSession | None = None) -> tuple[Any, FakeSession]:
    if session is None:
        session, _judge_run_id, _bundle_id, _candidate_group_id = _fixtures(failed_replay=True)
    result = _module().generate_report(
        approve_db_read=True,
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=_runtime_reader(),
        database_session_factory=lambda _url: session,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_DATABASE_URL, FAKE_SOURCE_TEXT, FAKE_URL),
    )
    return result, session


def _run_full(
    tmp_path: Path,
    *,
    session: FakeSession | None = None,
    live_client: FakeLiveOpenAIClient | None = None,
    direct_key: bool = False,
    existing_output_count: int = 0,
    existing_ready_count: int = 0,
) -> tuple[Any, FakeSession, FakeLiveOpenAIClient]:
    if session is None:
        session, _judge_run_id, _bundle_id, candidate_group_id = _fixtures(failed_replay=True)
        session.existing_output_count = existing_output_count
        session.existing_ready_count = existing_ready_count
    else:
        candidate_group_id = next(iter(session.bundles.values()))["candidate_group_id"]
    key_file = _make_key_file(tmp_path)
    if live_client is None:
        live_client = FakeLiveOpenAIClient(_structured_response(_valid_payload(candidate_group_id)))
    result = _module().generate_report(
        approve_db_read=True,
        approve_db_write=True,
        approve_key_read=True,
        approve_live_openai=True,
        max_live_calls=1,
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=_runtime_reader(key_file, direct_key=direct_key),
        database_session_factory=lambda _url: session,
        openai_client_factory=lambda _api_key, _project, _timeout: live_client,
        forbidden_raw_values=(
            FAKE_RUNTIME_PATH,
            FAKE_DATABASE_URL,
            FAKE_KEY,
            str(key_file),
            FAKE_PROJECT,
            FAKE_SOURCE_TEXT,
            FAKE_URL,
        ),
    )
    return result, session, live_client


def _assert_no_writes(session: FakeSession) -> None:
    assert session.committed is False
    assert session.rolled_back is True
    assert session.closed is True
    assert session.judge_outputs == []
    assert session.ready_outbox == []
    for statement in session.statements:
        first = statement.split()[0].upper()
        assert first in {"SET", "SHOW", "SELECT", "WITH"}
        assert " INSERT " not in f" {statement.upper()} "
        assert " UPDATE " not in f" {statement.upper()} "
        assert " DELETE " not in f" {statement.upper()} "


def _assert_no_downstream(report: dict[str, Any]) -> None:
    assert report["analysis_rows_written_bucket"] == "zero"
    assert report["notification_rows_written_bucket"] == "zero"
    assert report["q_analysis_policy_published"] is False
    assert report["analysis_validator_started"] is False
    assert report["policy_engine_started"] is False
    assert report["notifier_started"] is False
    assert report["telegram_send_attempted"] is False
    assert report["redis_connected"] is False
    assert report["redis_write_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert report["redis_delete_or_trim_attempted"] is False


def _assert_no_raw_values(report: dict[str, Any], *values: str) -> None:
    rendered = _module().render_json(report)
    for value in values:
        assert value not in rendered


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_cli_default_mode_does_not_read_runtime_db_key_sdk_or_openai() -> None:
    env = os.environ.copy()
    env["OPENAI_API_KEY"] = FAKE_DIRECT_KEY
    env["DATABASE_URL"] = FAKE_DATABASE_URL
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=15,
        check=True,
    )

    report = json.loads(completed.stdout)
    assert report["contract_status"] == _module().STATUS_PREFLIGHT_PASSED
    assert report["runtime_env_read"] is False
    assert report["database_connected"] is False
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_key_file_read_bucket"] == "zero"
    assert report["openai_call_attempted"] is False
    assert report["request_shape_valid_bucket"] == "one"
    assert report["prompt_cache_key_presence_bucket"] == "zero"
    assert report["stored_prompt_cache_key_presence_bucket"] == "one"
    assert report["prompt_cache_key_transport_policy_bucket"] == "disabled"
    assert report["checks_failed"] == []
    assert FAKE_DIRECT_KEY not in completed.stdout
    assert FAKE_DATABASE_URL not in completed.stdout
    assert completed.stderr == ""
    _assert_no_downstream(report)


def test_partial_approval_blocks_before_runtime_db_key_or_live() -> None:
    result = _module().generate_report(
        approve_db_write=True,
        runtime_env_reader=_raising_runtime_reader,
        database_session_factory=_raising_database_factory,
        openai_client_factory=_raising_openai_factory,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_DATABASE_URL, FAKE_KEY),
    )

    report = result.report
    assert result.exit_code == 1
    assert report["contract_status"] == _module().STATUS_NOT_APPROVED
    assert report["checks_failed"] == ["approval.required_mode"]
    assert report["runtime_env_read"] is False
    assert report["database_connected"] is False
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_call_attempted"] is False
    _assert_no_downstream(report)


def test_db_read_preflight_uses_read_only_transaction_and_writes_nothing() -> None:
    result, session = _run_preflight()

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_DB_PREFLIGHT_PASSED
    assert report["runtime_env_read"] is True
    assert report["database_configured"] is True
    assert report["database_connected"] is True
    assert report["read_only_transaction"] is True
    assert report["target_candidate_found_bucket"] == "one"
    assert report["target_source_bucket"] == "failed_replay_real_context"
    assert report["target_judge_run_status_bucket"] == "failed_terminal"
    assert report["target_finish_reason_bucket"] == "openai_permanent_error"
    assert report["target_judge_call_requested_outbox_bucket"] == "one"
    assert report["existing_judge_outputs_for_run_bucket"] == "zero"
    assert report["existing_judge_output_ready_outbox_for_run_bucket"] == "zero"
    assert report["target_bundle_found_bucket"] == "one"
    assert report["bundle_structurally_usable_bucket"] == "one"
    assert report["prompt_rendered_bucket"] == "one"
    assert report["context_builder_bucket"] == "one"
    assert report["request_shape_valid_bucket"] == "one"
    assert report["request_shape_issue_count_bucket"] == "zero"
    assert report["prompt_cache_key_presence_bucket"] == "zero"
    assert report["stored_prompt_cache_key_presence_bucket"] == "one"
    assert report["prompt_cache_key_transport_policy_bucket"] == "disabled"
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_key_file_read_bucket"] == "zero"
    assert report["openai_call_attempted"] is False
    _assert_no_downstream(report)
    _assert_no_raw_values(report, FAKE_RUNTIME_PATH, FAKE_DATABASE_URL, FAKE_SOURCE_TEXT, FAKE_URL)
    _assert_no_writes(session)


def test_full_approved_fake_live_success_writes_output_run_and_ready_once(
    tmp_path: Path,
) -> None:
    result, session, live_client = _run_full(tmp_path)

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_DB_WRITE_PASSED
    assert report["openai_key_file_read_bucket"] == "one"
    assert report["openai_key_read_bucket"] == "one"
    assert report["live_openai_call_attempted_bucket"] == "one"
    assert report["live_openai_call_completed_bucket"] == "one"
    assert report["structured_output_observed_bucket"] == "one"
    assert report["judge_outputs_written_bucket"] == "one"
    assert report["judge_run_updated_bucket"] == "one"
    assert report["judge_output_ready_outbox_written_bucket"] == "one"
    assert len(live_client.calls) == 1
    assert report["prompt_cache_key_presence_bucket"] == "zero"
    assert report["stored_prompt_cache_key_presence_bucket"] == "one"
    assert report["prompt_cache_key_transport_policy_bucket"] == "disabled"
    assert len(session.judge_outputs) == 1
    assert session.judge_outputs[0]["model_proposed_verdict"] == "later"
    assert session.judge_run_updates == [
        {"status": "succeeded", "finish_reason": "completed", "refusal_detected": False}
    ]
    assert len(session.ready_outbox) == 1
    assert session.committed is True
    assert session.rolled_back is False
    _assert_no_downstream(report)
    _assert_no_raw_values(
        report,
        FAKE_RUNTIME_PATH,
        FAKE_DATABASE_URL,
        FAKE_KEY,
        FAKE_PROJECT,
        FAKE_SOURCE_TEXT,
        FAKE_URL,
    )


def test_existing_output_blocks_before_key_read_or_openai(tmp_path: Path) -> None:
    result, session, live_client = _run_full(tmp_path, existing_output_count=1)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DUPLICATE_OUTPUT
    assert result.report["checks_failed"] == ["judge_outputs.existing"]
    assert result.report["existing_judge_outputs_for_run_bucket"] == "one"
    assert result.report["openai_key_read_bucket"] == "zero"
    assert live_client.calls == []
    assert session.judge_outputs == []
    assert session.ready_outbox == []
    assert session.committed is False


def test_existing_ready_outbox_blocks_duplicate_ready_emit(tmp_path: Path) -> None:
    result, session, live_client = _run_full(tmp_path, existing_ready_count=1)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DUPLICATE_READY_OUTBOX
    assert result.report["checks_failed"] == ["event_outbox.ready_existing"]
    assert result.report["existing_judge_output_ready_outbox_for_run_bucket"] == "one"
    assert result.report["openai_key_read_bucket"] == "zero"
    assert live_client.calls == []
    assert session.ready_outbox == []


def test_openai_failure_writes_no_output_ready_or_run_update(tmp_path: Path) -> None:
    live_client = FakeLiveOpenAIClient(response={"status": "completed", "output_text": ""})
    result, session, live_client = _run_full(tmp_path, live_client=live_client)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_LIVE_CALL_FAILED
    assert result.report["checks_failed"] == ["openai.structured_output_missing"]
    assert result.report["live_openai_call_attempted_bucket"] == "one"
    assert len(live_client.calls) == 1
    assert session.judge_outputs == []
    assert session.ready_outbox == []
    assert session.judge_run_updates == []
    assert session.committed is False


def test_direct_openai_api_key_blocks_without_call_or_write(tmp_path: Path) -> None:
    result, session, live_client = _run_full(tmp_path, direct_key=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_KEY_NOT_READY
    assert result.report["checks_failed"] == ["openai_key.direct_env_unsupported"]
    assert result.report["direct_openai_api_key_present"] is True
    assert result.report["openai_key_read_bucket"] == "zero"
    assert live_client.calls == []
    assert session.judge_outputs == []


def test_pending_candidate_is_preferred_over_failed_replay_candidate() -> None:
    pending_session, _run_id, _bundle_id, _group_id = _fixtures(failed_replay=False)
    failed_session, _failed_run_id, _failed_bundle_id, _failed_group_id = _fixtures(failed_replay=True)
    pending_session.failed_rows = failed_session.failed_rows
    pending_session.event_rows.update(failed_session.event_rows)
    pending_session.judge_runs.update(failed_session.judge_runs)
    pending_session.bundles.update(failed_session.bundles)

    result, session = _run_preflight(session=pending_session)

    assert result.exit_code == 0
    assert result.report["target_source_bucket"] == "pending_candidate"
    assert result.report["target_judge_run_status_bucket"] == "pending"
    _assert_no_writes(session)


def test_downstream_rows_block_before_live() -> None:
    session, _run_id, _bundle_id, _group_id = _fixtures(failed_replay=True)
    session.analysis_count = 1

    result, session = _run_preflight(session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert result.report["checks_failed"] == ["downstream.side_effect"]
    _assert_no_writes(session)


def _raising_runtime_reader(_path: str | Path, _include_openai: bool) -> dict[str, str]:
    raise AssertionError("runtime env should not be read")


def _raising_database_factory(_database_url: str) -> Any:
    raise AssertionError("database should not be opened")


def _raising_openai_factory(_api_key: str, _project: str | None, _timeout: float) -> Any:
    raise AssertionError("openai client should not be created")
