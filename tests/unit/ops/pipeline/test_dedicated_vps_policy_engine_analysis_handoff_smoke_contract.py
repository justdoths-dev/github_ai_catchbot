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
    / "dedicated_vps_policy_engine_analysis_handoff_smoke.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-policy-handoff"
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
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-policy-handoff-runtime.env"
FAKE_OPERATOR_CHAT_ID = "123456789"
FAKE_DIRECT_KEY = "direct" + "-openai" + "-secret" + "-policy-handoff"
FAKE_KEY_FILE = "/etc/github-ai-catchbot/private-openai-key-policy-handoff"
FAKE_URL = "https" + ":" + "/" + "/" + "example.invalid/private/policy-handoff"
FAKE_SOURCE_TEXT = "private source text policy handoff must not leak"
FAKE_PROMPT_CONTEXT = "private prompt context policy handoff must not leak"


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
        include_policy_event: bool = True,
        existing_analysis_count: int = 0,
        notification_plan_count: int = 0,
        notification_delivery_count: int = 0,
        notification_intent_count: int = 0,
        current_bundle_matches: bool = True,
        identity_mismatch: bool = False,
        payload_override: dict[str, Any] | None = None,
        primary_artifact_type: str = "github_repo",
        read_only_value: str = "on",
    ) -> None:
        self.trigger_event_id = uuid4()
        self.judge_run_id = uuid4()
        self.bundle_id = uuid4()
        self.current_bundle_id = self.bundle_id if current_bundle_matches else uuid4()
        self.candidate_group_id = uuid4()
        self.judge_output_id = uuid4()
        self.artifact_id = uuid4()
        self.analysis_id = uuid4()
        self.existing_analysis_id = uuid4()
        self.bundle_fingerprint = "bundle-fingerprint-private-policy-handoff"
        self.include_policy_event = include_policy_event
        self.existing_analysis_count = existing_analysis_count
        self.notification_plan_count = notification_plan_count
        self.notification_delivery_count = notification_delivery_count
        self.notification_intent_count = notification_intent_count
        self.read_only_value = read_only_value
        self.transaction_open = False
        self.statements: list[str] = []
        self.analyses: list[tuple[UUID, dict[str, Any]]] = []
        self.state_transitions: list[dict[str, Any]] = []
        self.notification_outbox: list[dict[str, Any]] = []
        self.notification_plan_rows: list[dict[str, Any]] = []
        self.notification_render_rows: list[dict[str, Any]] = []
        self.notification_delivery_rows: list[dict[str, Any]] = []
        self.judge_output_mutations: list[dict[str, Any]] = []
        self.candidate_bundle_mutations: list[dict[str, Any]] = []
        self.candidate_current_analysis_mutations: list[dict[str, Any]] = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

        payload = payload_override or _inspect_now_payload()
        self.event_judge_output_id = uuid4() if identity_mismatch else self.judge_output_id
        self.policy_event_row = {
            "event_id": self.trigger_event_id,
            "event_type": "analysis.policy.apply.v1",
            "payload_json": {
                "judge_run_id": str(self.judge_run_id),
                "judge_output_id": str(self.event_judge_output_id),
                "candidate_group_id": str(self.candidate_group_id),
                "bundle_id": str(self.bundle_id),
                "private_prompt_context": FAKE_PROMPT_CONTEXT,
            },
        }
        self.target_row = {
            "trigger_event_id": self.trigger_event_id,
            "policy_apply_event_created_at": datetime(2026, 6, 2, 2, 2, 3, tzinfo=timezone.utc),
            "aggregate_judge_run_id": self.judge_run_id,
            "judge_run_id": self.judge_run_id,
            "bundle_id": self.bundle_id,
            "prompt_version": "judge_prompt_v1__replay_live_smoke_v1",
            "judge_run_status": "succeeded",
            "judge_output_id": self.judge_output_id,
            "candidate_group_id": self.candidate_group_id,
            "bundle_row_id": self.bundle_id,
            "bundle_candidate_group_id": self.candidate_group_id,
            "current_bundle_id": self.current_bundle_id,
            "target_source": "live_replay_persistence_smoke",
        }
        self.candidate_row = {
            "candidate_group_id": self.candidate_group_id,
            "current_bundle_id": self.current_bundle_id,
            "current_analysis_id": None,
        }
        self.judge_run_row = {
            "judge_run_id": self.judge_run_id,
            "bundle_id": self.bundle_id,
            "prompt_version": "judge_prompt_v1__replay_live_smoke_v1",
            "policy_version": "verdict_policy_v1",
            "status": "succeeded",
        }
        self.judge_output_row = {
            "judge_output_id": self.judge_output_id,
            "judge_run_id": self.judge_run_id,
            "candidate_group_id": self.candidate_group_id,
            "payload_json": payload,
            "model_proposed_verdict": payload.get("model_proposed_verdict"),
            "model_confidence_band": payload.get("model_confidence_band"),
            "created_at": datetime(2026, 6, 2, 2, 2, 4, tzinfo=timezone.utc),
        }
        self.bundle_row = {
            "bundle_id": self.bundle_id,
            "candidate_group_id": self.candidate_group_id,
            "current_primary_artifact_id": self.artifact_id,
            "current_primary_artifact_type": primary_artifact_type,
            "created_at": datetime(2026, 6, 2, 2, 2, 2, tzinfo=timezone.utc),
        }

    def in_transaction(self) -> bool:
        return self.transaction_open

    @asynccontextmanager
    async def _begin(self):
        self.transaction_open = True
        try:
            yield self
        except Exception:
            self.rolled_back = True
            raise
        else:
            self.committed = True
        finally:
            self.transaction_open = False

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
        if normalized == _normalize(module.SELECT_ELIGIBLE_POLICY_APPLY_EVENT_QUERY):
            return FakeResult(rows=[self.target_row] if self.include_policy_event else [])
        if "SELECT event_id, event_type, payload_json FROM event_outbox" in normalized:
            row = (
                self.policy_event_row
                if self.include_policy_event and str(params["event_id"]) == str(self.trigger_event_id)
                else None
            )
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT candidate_group_id, current_bundle_id, current_analysis_id"):
            row = self.candidate_row if str(params["candidate_group_id"]) == str(self.candidate_group_id) else None
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT judge_run_id, bundle_id, prompt_version"):
            row = self.judge_run_row if str(params["judge_run_id"]) == str(self.judge_run_id) else None
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT judge_output_id, judge_run_id, candidate_group_id"):
            if str(params["judge_output_id"]) == str(self.judge_output_id):
                row = self.judge_output_row
            elif str(params["judge_output_id"]) == str(self.event_judge_output_id):
                row = {**self.judge_output_row, "judge_output_id": self.event_judge_output_id}
            else:
                row = None
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT b.bundle_id, b.candidate_group_id"):
            row = self.bundle_row if str(params["bundle_id"]) == str(self.bundle_id) else None
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT analysis_id, judge_output_id, policy_version"):
            if self.existing_analysis_count or self.analyses:
                analysis_id = self.analyses[0][0] if self.analyses else self.existing_analysis_id
                return FakeResult(
                    rows=[
                        {
                            "analysis_id": analysis_id,
                            "judge_output_id": self.judge_output_id,
                            "policy_version": "verdict_policy_v1",
                            "delivery_policy_version": "delivery_policy_v1",
                        }
                    ]
                )
            return FakeResult(rows=[])
        if normalized == _normalize(module.COUNT_ANALYSES_FOR_OUTPUT_QUERY):
            return FakeResult(scalar=self.existing_analysis_count + len(self.analyses))
        if normalized == _normalize(module.COUNT_POLICY_STATE_TRANSITIONS_FOR_OUTPUT_QUERY):
            return FakeResult(scalar=len(self.state_transitions))
        if normalized == _normalize(module.COUNT_NOTIFICATION_PLAN_INTENT_OUTBOX_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.notification_intent_count + len(self.notification_outbox))
        if normalized == _normalize(module.COUNT_NOTIFICATION_PLAN_ROWS_FOR_OUTPUT_QUERY):
            return FakeResult(scalar=self.notification_plan_count + len(self.notification_plan_rows))
        if normalized == _normalize(module.COUNT_NOTIFICATION_RENDER_ROWS_FOR_OUTPUT_QUERY):
            return FakeResult(scalar=len(self.notification_render_rows))
        if normalized == _normalize(module.COUNT_NOTIFICATION_DELIVERY_ROWS_FOR_OUTPUT_QUERY):
            return FakeResult(scalar=self.notification_delivery_count + len(self.notification_delivery_rows))
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY):
            return FakeResult(scalar=1 + len(self.judge_output_mutations))
        if normalized == _normalize(module.COUNT_CANDIDATE_BUNDLES_FOR_TARGET_QUERY):
            return FakeResult(scalar=1 + len(self.candidate_bundle_mutations))
        if normalized == _normalize(module.SELECT_CURRENT_ANALYSIS_ID_QUERY):
            return FakeResult(scalar=self.candidate_row["current_analysis_id"])
        if normalized == _normalize(module.SELECT_CANDIDATE_BUNDLE_FINGERPRINT_QUERY):
            return FakeResult(scalar=self.bundle_fingerprint)
        if normalized.startswith("INSERT INTO analyses"):
            self.analyses.append((self.analysis_id, dict(params)))
            return FakeResult(scalar=self.analysis_id)
        if normalized.startswith("INSERT INTO state_transitions"):
            self.state_transitions.append(dict(params))
            return FakeResult()
        if normalized.startswith("INSERT INTO event_outbox") and "notification.plan.created.v1" in normalized:
            self.notification_outbox.append(
                {
                    "event_type": "notification.plan.created.v1",
                    "aggregate_type": "analysis",
                    "aggregate_id": UUID(str(params["analysis_id"])),
                    "dedupe_key": params["dedupe_key"],
                    "payload_json": json.loads(params["payload_json"]),
                }
            )
            return FakeResult()

        first = normalized.split()[0].upper()
        if "JUDGE_OUTPUTS" in normalized and first in {"INSERT", "UPDATE", "DELETE"}:
            self.judge_output_mutations.append({"statement": normalized, "params": params})
            return FakeResult()
        if "CANDIDATE_EVIDENCE_BUNDLES" in normalized and first in {"INSERT", "UPDATE", "DELETE"}:
            self.candidate_bundle_mutations.append({"statement": normalized, "params": params})
            return FakeResult()
        if "CANDIDATE_GROUP_PROPOSALS" in normalized and first in {"UPDATE", "DELETE"}:
            self.candidate_current_analysis_mutations.append({"statement": normalized, "params": params})
            return FakeResult()
        if "NOTIFICATION_PLANS" in normalized and first in {"INSERT", "UPDATE", "DELETE"}:
            self.notification_plan_rows.append({"statement": normalized, "params": params})
            return FakeResult()
        if "NOTIFICATION_RENDERS" in normalized and first in {"INSERT", "UPDATE", "DELETE"}:
            self.notification_render_rows.append({"statement": normalized, "params": params})
            return FakeResult()
        if "NOTIFICATION_DELIVERY_RECORDS" in normalized and first in {"INSERT", "UPDATE", "DELETE"}:
            self.notification_delivery_rows.append({"statement": normalized, "params": params})
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
        "scripts.ops.dedicated_vps_policy_engine_analysis_handoff_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_reader(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "TELEGRAM_OPERATOR_CHAT_ID": FAKE_OPERATOR_CHAT_ID,
        "OPENAI_API_KEY": FAKE_DIRECT_KEY,
        "OPENAI_API_KEY_FILE": FAKE_KEY_FILE,
    }


def _runtime_reader_without_chat(_path: str | Path) -> dict[str, str]:
    return {"DATABASE_URL": FAKE_DATABASE_URL}


def _raising_runtime_reader(_path: str | Path) -> dict[str, str]:
    raise AssertionError("runtime env should not be read")


def _raising_database_factory(_database_url: str) -> Any:
    raise AssertionError("database should not be opened")


def _inspect_now_payload() -> dict[str, Any]:
    return {
        "judge_schema_version": "judge_output_v1",
        "headline": "Useful repo",
        "summary_one_line_ko": FAKE_SOURCE_TEXT,
        "scores": {
            "novelty": 70,
            "practical_usefulness": 76,
            "evidence_strength": 65,
            "hype_penalty": 20,
            "confidence": 70,
            "code_quality": 70,
            "maintenance_signal": 50,
            "specificity": 65,
            "reproducibility_signal": 50,
        },
        "reason_codes": ["has_repo"],
        "evidence_limitations_ko": ["limited"],
        "recommended_action_ko": FAKE_URL,
        "freshness_note_ko": "fresh",
        "model_proposed_verdict": "later",
        "model_confidence_band": "medium",
    }


def _later_payload() -> dict[str, Any]:
    payload = _inspect_now_payload()
    payload["scores"] = {
        **payload["scores"],
        "practical_usefulness": 50,
        "evidence_strength": 40,
        "confidence": 40,
        "code_quality": 20,
    }
    payload["model_proposed_verdict"] = "later"
    return payload


def _skip_payload() -> dict[str, Any]:
    payload = _inspect_now_payload()
    payload["scores"] = {
        **payload["scores"],
        "practical_usefulness": 20,
        "evidence_strength": 20,
        "confidence": 20,
        "code_quality": 10,
    }
    payload["model_proposed_verdict"] = "skip"
    return payload


def _run_report(
    *,
    approve_db_read: bool = True,
    approve_db_write: bool = False,
    session: FakeSession | None = None,
    runtime_reader=_runtime_reader,
    forbidden_raw_values: tuple[str, ...] = (),
) -> tuple[Any, FakeSession]:
    session = session or FakeSession()
    result = _module().generate_report(
        approve_db_read=approve_db_read,
        approve_db_write=approve_db_write,
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=runtime_reader,
        database_session_factory=lambda _url: session,
        forbidden_raw_values=(
            FAKE_RUNTIME_PATH,
            FAKE_DATABASE_URL,
            FAKE_OPERATOR_CHAT_ID,
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
            str(session.analysis_id),
            *forbidden_raw_values,
        ),
    )
    return result, session


def _assert_no_writes(session: FakeSession) -> None:
    assert session.committed is False
    assert session.rolled_back is True
    assert session.closed is True
    assert session.analyses == []
    assert session.state_transitions == []
    assert session.notification_outbox == []
    assert session.judge_output_mutations == []
    assert session.candidate_bundle_mutations == []
    assert session.candidate_current_analysis_mutations == []
    assert session.notification_plan_rows == []
    assert session.notification_render_rows == []
    assert session.notification_delivery_rows == []
    for statement in session.statements:
        first = statement.split()[0].upper()
        assert first in {"SET", "SHOW", "SELECT"}
        assert " INSERT " not in f" {statement.upper()} "
        assert " UPDATE " not in f" {statement.upper()} "
        assert " DELETE " not in f" {statement.upper()} "


def _assert_no_forbidden_downstream(report: dict[str, Any]) -> None:
    assert report["notification_plan_rows_written_bucket"] == "zero"
    assert report["notification_render_rows_written_bucket"] == "zero"
    assert report["notification_delivery_rows_written_bucket"] == "zero"
    assert report["notifier_started"] is False
    assert report["telegram_send_attempted"] is False
    assert report["redis_write_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert report["redis_delete_or_trim_attempted"] is False
    assert report["openai_call_attempted"] is False
    assert report["openai_key_read_bucket"] == "zero"


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
    assert report["contract_status"] == _module().STATUS_DEFAULT_PASSED
    assert report["runtime_env_read"] is False
    assert report["database_connected"] is False
    assert report["database_write_attempted"] is False
    assert report["redis_connected"] is False
    assert report["redis_write_attempted"] is False
    assert report["openai_call_attempted"] is False
    assert report["openai_key_read_bucket"] == "zero"
    assert report["analysis_rows_written_bucket"] == "zero"
    assert report["notification_plan_intent_outbox_written_bucket"] == "zero"
    assert report["raw_values_emitted"] is False
    assert report["checks_failed"] == []
    assert FAKE_DIRECT_KEY not in completed.stdout
    assert FAKE_DATABASE_URL not in completed.stdout
    assert completed.stderr == ""
    _assert_no_forbidden_downstream(report)


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
    assert report["target_policy_apply_event_found_bucket"] == "one"
    assert report["target_source_bucket"] == "live_replay_persistence_smoke"
    assert report["judge_run_found_bucket"] == "one"
    assert report["judge_output_found_bucket"] == "one"
    assert report["bundle_found_bucket"] == "one"
    assert report["candidate_group_found_bucket"] == "one"
    assert report["payload_identity_valid_bucket"] == "one"
    assert report["current_bundle_match_bucket"] == "one"
    assert report["existing_analysis_rows_for_output_bucket"] == "zero"
    assert report["existing_notification_plan_rows_for_output_bucket"] == "zero"
    assert report["existing_notification_delivery_rows_for_output_bucket"] == "zero"
    assert report["existing_notification_plan_intent_outbox_for_target_bucket"] == "zero"
    assert report["analysis_verdict_bucket"] == "inspect_now"
    assert report["analysis_delivery_decision_bucket"] == "send_now"
    assert report["analysis_policy_reconciled_bucket"] == "false"
    assert report["policy_engine_started"] is False
    assert report["raw_values_emitted"] is False
    _assert_no_forbidden_downstream(report)
    _assert_no_raw_values(
        report,
        FAKE_RUNTIME_PATH,
        FAKE_DATABASE_URL,
        FAKE_OPERATOR_CHAT_ID,
        FAKE_DIRECT_KEY,
        FAKE_KEY_FILE,
        FAKE_URL,
        FAKE_SOURCE_TEXT,
        FAKE_PROMPT_CONTEXT,
        str(session.trigger_event_id),
        str(session.judge_run_id),
        str(session.judge_output_id),
        str(session.analysis_id),
    )
    _assert_no_writes(session)


def test_db_read_preflight_rejects_missing_policy_apply_event() -> None:
    result, session = _run_report(session=FakeSession(include_policy_event=False))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DB_READ_FAILED
    assert result.report["checks_failed"] == ["event_outbox.policy_apply_missing"]
    assert result.report["target_policy_apply_event_found_bucket"] == "zero"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_identity_mismatch() -> None:
    result, session = _run_report(session=FakeSession(identity_mismatch=True))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_VALIDATION_FAILED
    assert result.report["checks_failed"] == ["payload.identity"]
    assert result.report["payload_identity_valid_bucket"] == "zero"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_current_bundle_mismatch() -> None:
    result, session = _run_report(session=FakeSession(current_bundle_matches=False))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_VALIDATION_FAILED
    assert result.report["checks_failed"] == ["candidate.current_bundle"]
    assert result.report["current_bundle_match_bucket"] == "zero"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_existing_analysis() -> None:
    result, session = _run_report(session=FakeSession(existing_analysis_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EXISTING_ANALYSIS
    assert result.report["checks_failed"] == ["analyses.existing"]
    assert result.report["existing_analysis_rows_for_output_bucket"] == "one"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_existing_notification_rows() -> None:
    result, session = _run_report(session=FakeSession(notification_plan_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert result.report["checks_failed"] == ["notification_plans.existing"]
    assert result.report["existing_notification_plan_rows_for_output_bucket"] == "one"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_existing_delivery_rows() -> None:
    result, session = _run_report(session=FakeSession(notification_delivery_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert result.report["checks_failed"] == ["notification_delivery_records.existing"]
    assert result.report["existing_notification_delivery_rows_for_output_bucket"] == "one"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_existing_notification_intent_outbox() -> None:
    result, session = _run_report(session=FakeSession(notification_intent_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert result.report["checks_failed"] == ["event_outbox.notification_plan_intent_existing"]
    assert result.report["existing_notification_plan_intent_outbox_for_target_bucket"] == "one"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_missing_target_chat_for_non_suppress() -> None:
    result, session = _run_report(runtime_reader=_runtime_reader_without_chat)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_VALIDATION_FAILED
    assert result.report["checks_failed"] == ["config.target_chat_id_unavailable"]
    assert result.report["analysis_delivery_decision_bucket"] == "send_now"
    _assert_no_writes(session)


def test_db_write_fake_success_writes_one_analysis_transition_and_plan_intent() -> None:
    result, session = _run_report(approve_db_write=True)

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_DB_WRITE_PASSED
    assert report["database_write_attempted"] is True
    assert report["policy_engine_started"] is True
    assert report["policy_engine_service_path_reused"] is True
    assert report["analysis_rows_written_bucket"] == "one"
    assert report["policy_state_transitions_written_bucket"] == "one"
    assert report["notification_plan_intent_outbox_written_bucket"] == "one"
    assert report["analysis_verdict_bucket"] == "inspect_now"
    assert report["analysis_delivery_decision_bucket"] == "send_now"
    assert report["analysis_policy_reconciled_bucket"] == "false"
    assert len(session.analyses) == 1
    assert len(session.state_transitions) == 1
    assert session.state_transitions[0]["object_type"] == "analysis"
    assert session.state_transitions[0]["object_id"] == str(session.analysis_id)
    assert session.state_transitions[0]["from_state"] == "analysis_validated"
    assert session.state_transitions[0]["to_state"] == "analysis_finalized"
    assert session.state_transitions[0]["reason_code"] == "policy_applied:inspect_now:send_now"
    assert len(session.notification_outbox) == 1
    payload = session.notification_outbox[0]["payload_json"]
    assert payload["analysis_id"] == str(session.analysis_id)
    assert payload["candidate_group_id"] == str(session.candidate_group_id)
    assert payload["delivery_decision"] == "send_now"
    assert payload["urgency_profile"] == "high"
    assert payload["target_chat_id"] == int(FAKE_OPERATOR_CHAT_ID)
    assert payload["target_thread_id"] is None
    assert payload["render_profile"] == "single_alert_v1"
    assert payload["send_after"] is None
    assert payload["suppress_reason_code"] is None
    assert session.committed is True
    assert session.rolled_back is False
    assert session.closed is True


def test_db_write_fake_later_path_emits_plan_intent_normal_silent() -> None:
    result, session = _run_report(
        approve_db_write=True,
        session=FakeSession(payload_override=_later_payload()),
    )

    report = result.report
    assert result.exit_code == 0
    assert report["analysis_verdict_bucket"] == "later"
    assert report["analysis_delivery_decision_bucket"] == "send_now"
    assert report["notification_plan_intent_outbox_written_bucket"] == "one"
    payload = session.notification_outbox[0]["payload_json"]
    assert payload["urgency_profile"] == "normal_silent"
    assert payload["delivery_decision"] == "send_now"
    assert payload["send_after"] is None


def test_db_write_fake_skip_suppress_writes_analysis_without_plan_intent() -> None:
    result, session = _run_report(
        approve_db_write=True,
        session=FakeSession(payload_override=_skip_payload()),
        runtime_reader=_runtime_reader_without_chat,
    )

    report = result.report
    assert result.exit_code == 0
    assert report["analysis_rows_written_bucket"] == "one"
    assert report["policy_state_transitions_written_bucket"] == "one"
    assert report["notification_plan_intent_outbox_written_bucket"] == "zero"
    assert report["analysis_verdict_bucket"] == "skip"
    assert report["analysis_delivery_decision_bucket"] == "suppress"
    assert session.notification_outbox == []
    assert session.state_transitions[0]["to_state"] == "analysis_suppressed"


def test_db_write_fake_success_writes_no_forbidden_downstream_or_mutations() -> None:
    result, session = _run_report(approve_db_write=True)

    report = result.report
    _assert_no_forbidden_downstream(report)
    assert report["judge_outputs_written_bucket"] == "zero"
    assert report["judge_outputs_mutation_attempted"] is False
    assert report["candidate_bundle_mutation_attempted"] is False
    assert report["candidate_current_analysis_mutation_attempted"] is False
    assert session.judge_output_mutations == []
    assert session.candidate_bundle_mutations == []
    assert session.candidate_current_analysis_mutations == []
    assert session.notification_plan_rows == []
    assert session.notification_render_rows == []
    assert session.notification_delivery_rows == []
    assert report["stops_at_event_type"] == "notification.plan.created.v1"


def test_db_write_fake_success_has_no_openai_key_redis_or_raw_output() -> None:
    result, session = _run_report(approve_db_write=True)

    report = result.report
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_call_attempted"] is False
    assert report["redis_connected"] is False
    assert report["redis_write_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert report["redis_delete_or_trim_attempted"] is False
    assert report["raw_values_emitted"] is False
    _assert_no_raw_values(
        report,
        FAKE_RUNTIME_PATH,
        FAKE_DATABASE_URL,
        FAKE_OPERATOR_CHAT_ID,
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
        str(session.analysis_id),
    )
