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

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_analysis_validator_policy_apply_handoff_smoke.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-validator-handoff"
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
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-validator-handoff-runtime.env"
FAKE_DIRECT_KEY = "direct" + "-openai" + "-secret" + "-validator-handoff"
FAKE_KEY_FILE = "/etc/github-ai-catchbot/private-openai-key-validator-handoff"
FAKE_URL = "https" + ":" + "/" + "/" + "example.invalid/private/validator-handoff"
FAKE_SOURCE_TEXT = "private source text validator handoff must not leak"
FAKE_PROMPT_CONTEXT = "private prompt context validator handoff must not leak"


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
        include_ready_event: bool = True,
        judge_run_status: str = "succeeded",
        existing_policy_count: int = 0,
        analysis_count: int = 0,
        notification_count: int = 0,
        payload_override: dict[str, Any] | None = None,
        read_only_value: str = "on",
    ) -> None:
        self.trigger_event_id = uuid4()
        self.judge_run_id = uuid4()
        self.bundle_id = uuid4()
        self.candidate_group_id = uuid4()
        self.judge_output_id = uuid4()
        self.artifact_id = uuid4()
        self.include_ready_event = include_ready_event
        self.existing_policy_count = existing_policy_count
        self.analysis_count = analysis_count
        self.notification_count = notification_count
        self.read_only_value = read_only_value
        self.statements: list[str] = []
        self.state_transitions: list[dict[str, Any]] = []
        self.policy_outbox: list[dict[str, Any]] = []
        self.judge_output_mutations: list[dict[str, Any]] = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

        self.ready_event_row = {
            "event_id": self.trigger_event_id,
            "event_type": "judge.output.ready.v1",
            "payload_json": {
                "judge_run_id": str(self.judge_run_id),
                "judge_output_id": str(self.judge_output_id),
                "finish_reason": "completed",
                "refusal_detected": False,
                "private_prompt_context": FAKE_PROMPT_CONTEXT,
            },
        }
        self.target_row = {
            "trigger_event_id": self.trigger_event_id,
            "ready_event_created_at": datetime(2026, 6, 2, 1, 2, 3, tzinfo=timezone.utc),
            "judge_run_id": self.judge_run_id,
            "bundle_id": self.bundle_id,
            "judge_output_id": self.judge_output_id,
            "candidate_group_id": self.candidate_group_id,
            "judge_run_status": judge_run_status,
            "finish_reason": "completed",
            "refusal_detected": False,
            "prompt_version": "judge_prompt_v1__replay_live_smoke_v1",
            "target_source": "live_replay_persistence_smoke",
        }
        self.judge_run_row = {
            "judge_run_id": self.judge_run_id,
            "bundle_id": self.bundle_id,
            "judge_profile": "github_primary",
            "schema_version": "judge_output_v1",
            "policy_version": "verdict_policy_v1",
            "status": judge_run_status,
            "finish_reason": "completed",
            "refusal_detected": False,
        }
        payload = payload_override or _valid_payload(self.candidate_group_id)
        self.judge_output_row = {
            "judge_output_id": self.judge_output_id,
            "judge_run_id": self.judge_run_id,
            "candidate_group_id": self.candidate_group_id,
            "judge_schema_version": "judge_output_v1",
            "payload_json": payload,
            "model_proposed_verdict": payload.get("model_proposed_verdict"),
            "model_confidence_band": payload.get("model_confidence_band"),
            "created_at": datetime(2026, 6, 2, 1, 2, 4, tzinfo=timezone.utc),
        }
        self.bundle_row = {
            "bundle_id": self.bundle_id,
            "candidate_group_id": self.candidate_group_id,
            "current_primary_artifact_id": self.artifact_id,
            "current_primary_artifact_type": "github_repo",
            "created_at": datetime(2026, 6, 2, 1, 2, 2, tzinfo=timezone.utc),
        }

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
        if normalized == _normalize(module.SELECT_ELIGIBLE_READY_EVENT_QUERY):
            return FakeResult(rows=[self.target_row] if self.include_ready_event else [])
        if "SELECT event_id, event_type, payload_json FROM event_outbox" in normalized:
            row = (
                self.ready_event_row
                if self.include_ready_event and str(params["event_id"]) == str(self.trigger_event_id)
                else None
            )
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT judge_run_id, bundle_id, judge_profile"):
            row = (
                self.judge_run_row
                if str(params["judge_run_id"]) == str(self.judge_run_id)
                else None
            )
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT judge_output_id, judge_run_id, candidate_group_id"):
            row = (
                self.judge_output_row
                if str(params["judge_output_id"]) == str(self.judge_output_id)
                else None
            )
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT b.bundle_id, b.candidate_group_id"):
            row = self.bundle_row if str(params["bundle_id"]) == str(self.bundle_id) else None
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(module.COUNT_POLICY_OUTBOX_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.existing_policy_count + len(self.policy_outbox))
        if normalized == _normalize(module.COUNT_ANALYSES_FOR_RUN_QUERY):
            return FakeResult(scalar=self.analysis_count)
        if normalized == _normalize(module.COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.notification_count)
        if normalized == _normalize(module.COUNT_STATE_TRANSITIONS_FOR_RUN_QUERY):
            return FakeResult(scalar=len(self.state_transitions))
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY):
            return FakeResult(scalar=1 + len(self.judge_output_mutations))
        if normalized.startswith("INSERT INTO state_transitions"):
            self.state_transitions.append(params)
            return FakeResult()
        if normalized.startswith("INSERT INTO event_outbox") and "analysis.policy.apply.v1" in normalized:
            self.policy_outbox.append(
                {
                    "event_type": "analysis.policy.apply.v1",
                    "aggregate_type": "judge_run",
                    "aggregate_id": UUID(str(params["judge_run_id"])),
                    "payload_json": json.loads(params["payload_json"]),
                }
            )
            return FakeResult()
        if "judge_outputs" in normalized and normalized.split()[0].upper() in {"INSERT", "UPDATE", "DELETE"}:
            self.judge_output_mutations.append({"statement": normalized, "params": params})
            return FakeResult()

        raise AssertionError(f"unexpected SQL: {statement}")

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True

    async def close(self) -> None:
        self.closed = True


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_analysis_validator_policy_apply_handoff_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_reader(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "OPENAI_API_KEY": FAKE_DIRECT_KEY,
        "OPENAI_API_KEY_FILE": FAKE_KEY_FILE,
    }


def _raising_runtime_reader(_path: str | Path) -> dict[str, str]:
    raise AssertionError("runtime env should not be read")


def _raising_database_factory(_database_url: str) -> Any:
    raise AssertionError("database should not be opened")


def _valid_payload(candidate_group_id: UUID) -> dict[str, Any]:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate_group_id),
        "headline": "Useful repo",
        "summary_one_line_ko": FAKE_SOURCE_TEXT,
        "skeptical_take_ko": "skeptical",
        "why_it_might_matter_ko": FAKE_URL,
        "comparables": ["existing repo"],
        "scores": {
            "novelty": 60,
            "practical_usefulness": 70,
            "evidence_strength": 65,
            "hype_penalty": 20,
            "confidence": 65,
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


def _run_report(
    *,
    approve_db_read: bool = True,
    approve_db_write: bool = False,
    session: FakeSession | None = None,
    forbidden_raw_values: tuple[str, ...] = (),
) -> tuple[Any, FakeSession]:
    session = session or FakeSession()
    result = _module().generate_report(
        approve_db_read=approve_db_read,
        approve_db_write=approve_db_write,
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=_runtime_reader,
        database_session_factory=lambda _url: session,
        forbidden_raw_values=(
            FAKE_RUNTIME_PATH,
            FAKE_DATABASE_URL,
            FAKE_DIRECT_KEY,
            FAKE_KEY_FILE,
            FAKE_URL,
            FAKE_SOURCE_TEXT,
            FAKE_PROMPT_CONTEXT,
            str(session.trigger_event_id),
            str(session.judge_run_id),
            str(session.judge_output_id),
            str(session.bundle_id),
            str(session.candidate_group_id),
            *forbidden_raw_values,
        ),
    )
    return result, session


def _assert_no_writes(session: FakeSession) -> None:
    assert session.committed is False
    assert session.rolled_back is True
    assert session.closed is True
    assert session.state_transitions == []
    assert session.policy_outbox == []
    assert session.judge_output_mutations == []
    for statement in session.statements:
        first = statement.split()[0].upper()
        assert first in {"SET", "SHOW", "SELECT"}
        assert " INSERT " not in f" {statement.upper()} "
        assert " UPDATE " not in f" {statement.upper()} "
        assert " DELETE " not in f" {statement.upper()} "


def _assert_no_downstream(report: dict[str, Any]) -> None:
    assert report["analysis_rows_written_bucket"] == "zero"
    assert report["notification_rows_written_bucket"] == "zero"
    assert report["policy_engine_started"] is False
    assert report["notifier_started"] is False
    assert report["telegram_send_attempted"] is False
    assert report["redis_write_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert report["redis_delete_or_trim_attempted"] is False


def _assert_no_raw_values(report: dict[str, Any], *values: str) -> None:
    rendered = _module().render_json(report)
    for value in values:
        assert value not in rendered


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_parser_has_no_openai_or_key_approval_flags() -> None:
    parser = _module().build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--approve-key-read"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--approve-live-openai"])


def test_default_mode_does_not_read_env_db_redis_key_or_openai() -> None:
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
    assert report["database_write_attempted"] is False
    assert report["redis_write_attempted"] is False
    assert report["openai_call_attempted"] is False
    assert report["openai_key_read_bucket"] == "zero"
    assert report["analysis_policy_apply_outbox_written_bucket"] == "zero"
    assert report["analysis_rows_written_bucket"] == "zero"
    assert report["notification_rows_written_bucket"] == "zero"
    assert report["raw_values_emitted"] is False
    assert report["checks_failed"] == []
    assert FAKE_DIRECT_KEY not in completed.stdout
    assert FAKE_DATABASE_URL not in completed.stdout
    assert completed.stderr == ""
    _assert_no_downstream(report)


def test_partial_approval_blocks_before_runtime_or_database() -> None:
    result = _module().generate_report(
        approve_db_write=True,
        runtime_env_reader=_raising_runtime_reader,
        database_session_factory=_raising_database_factory,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_DATABASE_URL),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NOT_APPROVED
    assert result.report["checks_failed"] == ["approval.required_mode"]
    assert result.report["runtime_env_read"] is False
    assert result.report["database_connected"] is False
    assert result.report["openai_key_read_bucket"] == "zero"
    assert result.report["openai_call_attempted"] is False


def test_db_read_preflight_uses_read_only_transaction_and_writes_nothing() -> None:
    result, session = _run_report()

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_DB_PREFLIGHT_PASSED
    assert report["runtime_env_read"] is True
    assert report["database_configured"] is True
    assert report["database_connected"] is True
    assert report["read_only_transaction"] is True
    assert report["target_ready_event_found_bucket"] == "one"
    assert report["target_source_bucket"] == "live_replay_persistence_smoke"
    assert report["judge_run_found_bucket"] == "one"
    assert report["judge_run_succeeded_bucket"] == "one"
    assert report["judge_output_found_bucket"] == "one"
    assert report["bundle_found_bucket"] == "one"
    assert report["output_schema_valid_bucket"] == "one"
    assert report["business_rules_valid_bucket"] == "one"
    assert report["existing_analysis_policy_apply_outbox_for_run_bucket"] == "zero"
    assert report["existing_analysis_rows_for_run_bucket"] == "zero"
    assert report["existing_notification_rows_for_run_bucket"] == "zero"
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_call_attempted"] is False
    assert report["raw_values_emitted"] is False
    _assert_no_downstream(report)
    _assert_no_raw_values(
        report,
        FAKE_RUNTIME_PATH,
        FAKE_DATABASE_URL,
        FAKE_DIRECT_KEY,
        FAKE_KEY_FILE,
        FAKE_URL,
        FAKE_SOURCE_TEXT,
        FAKE_PROMPT_CONTEXT,
        str(session.trigger_event_id),
        str(session.judge_run_id),
        str(session.judge_output_id),
    )
    _assert_no_writes(session)


def test_db_read_preflight_rejects_missing_ready_event() -> None:
    result, session = _run_report(session=FakeSession(include_ready_event=False))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DB_READ_FAILED
    assert result.report["checks_failed"] == ["event_outbox.ready_missing"]
    assert result.report["target_ready_event_found_bucket"] == "zero"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_existing_policy_apply_outbox() -> None:
    result, session = _run_report(session=FakeSession(existing_policy_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DUPLICATE_POLICY_APPLY
    assert result.report["checks_failed"] == ["event_outbox.policy_apply_existing"]
    assert result.report["existing_analysis_policy_apply_outbox_for_run_bucket"] == "one"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_existing_analyses() -> None:
    result, session = _run_report(session=FakeSession(analysis_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert result.report["checks_failed"] == ["analyses.existing"]
    assert result.report["existing_analysis_rows_for_run_bucket"] == "one"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_existing_notification_rows() -> None:
    result, session = _run_report(session=FakeSession(notification_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert result.report["checks_failed"] == ["notifications.existing"]
    assert result.report["existing_notification_rows_for_run_bucket"] == "one"
    _assert_no_writes(session)


def test_db_write_fake_success_emits_one_transition_and_one_policy_apply() -> None:
    result, session = _run_report(approve_db_write=True)

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_DB_WRITE_PASSED
    assert report["database_write_attempted"] is True
    assert report["validator_state_transitions_written_bucket"] == "one"
    assert report["analysis_policy_apply_outbox_written_bucket"] == "one"
    assert len(session.state_transitions) == 1
    assert session.state_transitions[0]["object_type"] == "judge_run"
    assert session.state_transitions[0]["object_id"] == str(session.judge_run_id)
    assert session.state_transitions[0]["from_state"] == "succeeded"
    assert session.state_transitions[0]["to_state"] == "analysis_validated"
    assert session.state_transitions[0]["reason_code"] == "validator_passed"
    assert len(session.policy_outbox) == 1
    assert session.policy_outbox[0]["event_type"] == "analysis.policy.apply.v1"
    assert session.policy_outbox[0]["payload_json"] == {
        "judge_run_id": str(session.judge_run_id),
        "judge_output_id": str(session.judge_output_id),
        "candidate_group_id": str(session.candidate_group_id),
        "bundle_id": str(session.bundle_id),
    }
    assert session.committed is True
    assert session.rolled_back is False
    assert session.closed is True


def test_db_write_fake_success_writes_no_analyses_notifications_or_downstream() -> None:
    result, _session = _run_report(approve_db_write=True)

    report = result.report
    assert report["analysis_rows_written_bucket"] == "zero"
    assert report["notification_rows_written_bucket"] == "zero"
    assert report["policy_engine_started"] is False
    assert report["notifier_started"] is False
    assert report["telegram_send_attempted"] is False
    assert report["redis_write_attempted"] is False
    assert report["stops_at_event_type"] == "analysis.policy.apply.v1"


def test_db_write_fake_success_does_not_mutate_judge_outputs() -> None:
    result, session = _run_report(approve_db_write=True)

    assert result.report["judge_outputs_written_bucket"] == "zero"
    assert result.report["judge_outputs_mutation_attempted"] is False
    assert session.judge_output_mutations == []


def test_db_write_fake_success_has_no_openai_key_path_or_raw_output() -> None:
    result, session = _run_report(approve_db_write=True)

    report = result.report
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_call_attempted"] is False
    assert report["raw_values_emitted"] is False
    _assert_no_raw_values(
        report,
        FAKE_RUNTIME_PATH,
        FAKE_DATABASE_URL,
        FAKE_DIRECT_KEY,
        FAKE_KEY_FILE,
        FAKE_URL,
        FAKE_SOURCE_TEXT,
        FAKE_PROMPT_CONTEXT,
        str(session.trigger_event_id),
        str(session.judge_run_id),
        str(session.judge_output_id),
        str(session.bundle_id),
        str(session.candidate_group_id),
    )


def test_db_read_preflight_rejects_schema_invalid_output() -> None:
    payload = _valid_payload(uuid4())
    del payload["headline"]
    result, session = _run_report(session=FakeSession(payload_override=payload))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_VALIDATION_FAILED
    assert result.report["checks_failed"] == ["validator.schema"]
    assert result.report["output_schema_valid_bucket"] == "zero"
    _assert_no_writes(session)
