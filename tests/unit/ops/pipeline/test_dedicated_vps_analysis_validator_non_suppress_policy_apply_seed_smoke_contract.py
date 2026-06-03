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
    / "dedicated_vps_analysis_validator_non_suppress_policy_apply_seed_smoke.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-seed"
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
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-seed-runtime.env"
FAKE_OPERATOR_CHAT_ID = "123456789"
FAKE_DIRECT_KEY = "direct" + "-openai" + "-secret" + "-seed"
FAKE_KEY_FILE = "/etc/github-ai-catchbot/private-openai-key-seed"
FAKE_URL = "https" + ":" + "/" + "/" + "example.invalid/private/seed"
FAKE_SOURCE_TEXT = "private source text seed must not leak"
FAKE_PROMPT_CONTEXT = "private prompt context seed must not leak"


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
        include_candidate: bool = True,
        include_bundle: bool = True,
        current_bundle_matches: bool = True,
        existing_seed_judge_run_count: int = 0,
        existing_seed_judge_output_count: int = 0,
        existing_seed_ready_count: int = 0,
        existing_seed_policy_apply_count: int = 0,
        read_only_value: str = "on",
        implicit_read_transaction: bool = False,
        fail_begin_if_transaction_open: bool = False,
        drop_policy_apply_insert: bool = False,
    ) -> None:
        self.candidate_group_id = uuid4()
        self.bundle_id = uuid4()
        self.current_bundle_id = self.bundle_id if current_bundle_matches else uuid4()
        self.artifact_id = uuid4()
        self.judge_run_id = uuid4()
        self.judge_output_id = uuid4()
        self.trigger_event_id = uuid4()
        self.bundle_fingerprint = "bundle-fingerprint-private-seed"
        self.include_candidate = include_candidate
        self.include_bundle = include_bundle
        self.existing_seed_judge_run_count = existing_seed_judge_run_count
        self.existing_seed_judge_output_count = existing_seed_judge_output_count
        self.existing_seed_ready_count = existing_seed_ready_count
        self.existing_seed_policy_apply_count = existing_seed_policy_apply_count
        self.read_only_value = read_only_value
        self.implicit_read_transaction = implicit_read_transaction
        self.fail_begin_if_transaction_open = fail_begin_if_transaction_open
        self.drop_policy_apply_insert = drop_policy_apply_insert
        self.transaction_open = False
        self.explicit_transaction_open = False
        self.statements: list[str] = []
        self.judge_runs: list[dict[str, Any]] = []
        self.judge_outputs: list[dict[str, Any]] = []
        self.ready_outbox: list[dict[str, Any]] = []
        self.state_transitions: list[dict[str, Any]] = []
        self.policy_apply_outbox: list[dict[str, Any]] = []
        self.analyses: list[dict[str, Any]] = []
        self.notification_intents: list[dict[str, Any]] = []
        self.notification_plan_rows: list[dict[str, Any]] = []
        self.notification_render_rows: list[dict[str, Any]] = []
        self.notification_delivery_rows: list[dict[str, Any]] = []
        self.candidate_bundle_mutations: list[dict[str, Any]] = []
        self.candidate_current_analysis_mutations: list[dict[str, Any]] = []
        self.committed = False
        self.rolled_back = False
        self.rollback_count = 0
        self.begin_count = 0
        self.closed = False

        self.target_row = {
            "candidate_group_id": self.candidate_group_id,
            "current_bundle_id": self.current_bundle_id,
            "bundle_id": self.bundle_id,
            "current_primary_artifact_id": self.artifact_id,
            "current_primary_artifact_type": "github_repo",
            "bundle_created_at": datetime(2026, 6, 4, 1, 2, 3, tzinfo=timezone.utc),
        }

    def in_transaction(self) -> bool:
        return self.transaction_open

    @asynccontextmanager
    async def _begin(self):
        self.transaction_open = True
        self.explicit_transaction_open = True
        snapshot = self._snapshot_writes()
        try:
            yield self
        except Exception:
            self._restore_writes(snapshot)
            self.rolled_back = True
            raise
        else:
            self.committed = True
        finally:
            self.explicit_transaction_open = False
            self.transaction_open = False

    def begin(self):
        self.begin_count += 1
        if self.fail_begin_if_transaction_open and self.transaction_open:
            raise AssertionError("explicit begin attempted while read transaction is open")
        return self._begin()

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(str(statement))
        self.statements.append(normalized)
        module = _module()
        base = _validator_base()
        policy_base = _policy_base()
        first = normalized.split()[0].upper()
        if self.implicit_read_transaction and not self.explicit_transaction_open and first == "SELECT":
            self.transaction_open = True

        if normalized == _normalize(base.SET_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult()
        if normalized == _normalize(base.SHOW_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult(scalar=self.read_only_value)
        if normalized == _normalize(base.SELECT_ONE_QUERY):
            return FakeResult(scalar=1)
        if normalized == _normalize(module.SELECT_SEEDABLE_CURRENT_BUNDLE_QUERY):
            rows = [self.target_row] if self.include_candidate and self.include_bundle else []
            return FakeResult(rows=rows)
        if normalized == _normalize(module.COUNT_SEED_JUDGE_RUNS_QUERY):
            return FakeResult(scalar=self.existing_seed_judge_run_count)
        if normalized == _normalize(module.COUNT_SEED_JUDGE_OUTPUTS_QUERY):
            return FakeResult(scalar=self.existing_seed_judge_output_count)
        if normalized == _normalize(module.COUNT_SEED_READY_OUTBOX_QUERY):
            return FakeResult(scalar=self.existing_seed_ready_count)
        if normalized == _normalize(module.COUNT_SEED_POLICY_APPLY_OUTBOX_QUERY):
            return FakeResult(scalar=self.existing_seed_policy_apply_count)
        if normalized == _normalize(module.INSERT_JUDGE_RUN_QUERY):
            row = {**params, "judge_run_id": self.judge_run_id}
            self.judge_runs.append(row)
            return FakeResult(scalar=self.judge_run_id)
        if normalized == _normalize(module.INSERT_JUDGE_OUTPUT_QUERY):
            payload = json.loads(params["payload_json"])
            row = {**params, "judge_output_id": self.judge_output_id, "payload_json": payload}
            self.judge_outputs.append(row)
            return FakeResult(scalar=self.judge_output_id)
        if normalized == _normalize(module.INSERT_READY_OUTBOX_QUERY):
            payload = json.loads(params["payload_json"])
            row = {
                **params,
                "event_id": self.trigger_event_id,
                "event_type": "judge.output.ready.v1",
                "payload_json": payload,
            }
            self.ready_outbox.append(row)
            return FakeResult(scalar=self.trigger_event_id)
        if "SELECT event_id, event_type, payload_json FROM event_outbox" in normalized:
            row = None
            if self.ready_outbox and str(params["event_id"]) == str(self.trigger_event_id):
                ready = self.ready_outbox[0]
                row = {
                    "event_id": self.trigger_event_id,
                    "event_type": "judge.output.ready.v1",
                    "payload_json": ready["payload_json"],
                }
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT judge_run_id, bundle_id, judge_profile"):
            row = None
            if self.judge_runs and str(params["judge_run_id"]) == str(self.judge_run_id):
                row = {
                    "judge_run_id": self.judge_run_id,
                    "bundle_id": self.bundle_id,
                    "judge_profile": self.judge_runs[0]["judge_profile"],
                    "schema_version": self.judge_runs[0]["schema_version"],
                    "policy_version": self.judge_runs[0]["policy_version"],
                    "status": "succeeded",
                    "finish_reason": "completed",
                    "refusal_detected": False,
                }
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT judge_output_id, judge_run_id, candidate_group_id"):
            row = None
            if self.judge_outputs and str(params["judge_output_id"]) == str(self.judge_output_id):
                output = self.judge_outputs[0]
                row = {
                    "judge_output_id": self.judge_output_id,
                    "judge_run_id": self.judge_run_id,
                    "candidate_group_id": self.candidate_group_id,
                    "judge_schema_version": "judge_output_v1",
                    "payload_json": output["payload_json"],
                    "model_proposed_verdict": output["model_proposed_verdict"],
                    "model_confidence_band": output["model_confidence_band"],
                    "created_at": datetime(2026, 6, 4, 1, 2, 4, tzinfo=timezone.utc),
                }
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT b.bundle_id, b.candidate_group_id"):
            row = {
                "bundle_id": self.bundle_id,
                "candidate_group_id": self.candidate_group_id,
                "current_primary_artifact_id": self.artifact_id,
                "current_primary_artifact_type": "github_repo",
                "created_at": datetime(2026, 6, 4, 1, 2, 3, tzinfo=timezone.utc),
            }
            return FakeResult(rows=[row])
        if normalized == _normalize(module.COUNT_JUDGE_RUN_FOR_TARGET_QUERY):
            return FakeResult(scalar=len(self.judge_runs))
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUT_FOR_TARGET_QUERY):
            return FakeResult(scalar=len(self.judge_outputs))
        if normalized == _normalize(module.COUNT_READY_OUTBOX_FOR_TARGET_QUERY):
            return FakeResult(scalar=len(self.ready_outbox))
        if normalized == _normalize(module.COUNT_POLICY_APPLY_FOR_TARGET_QUERY):
            return FakeResult(scalar=len(self.policy_apply_outbox))
        if normalized == _normalize(module.COUNT_VALIDATOR_STATE_TRANSITIONS_FOR_TARGET_QUERY):
            return FakeResult(scalar=len(self.state_transitions))
        if normalized == _normalize(module.COUNT_ANALYSES_FOR_OUTPUT_QUERY):
            return FakeResult(scalar=len(self.analyses))
        if normalized == _normalize(module.COUNT_NOTIFICATION_PLAN_INTENT_OUTBOX_FOR_TARGET_QUERY):
            return FakeResult(scalar=len(self.notification_intents))
        if normalized == _normalize(module.COUNT_NOTIFICATION_PLAN_ROWS_FOR_OUTPUT_QUERY):
            return FakeResult(scalar=len(self.notification_plan_rows))
        if normalized == _normalize(module.COUNT_NOTIFICATION_RENDER_ROWS_FOR_OUTPUT_QUERY):
            return FakeResult(scalar=len(self.notification_render_rows))
        if normalized == _normalize(module.COUNT_NOTIFICATION_DELIVERY_ROWS_FOR_OUTPUT_QUERY):
            return FakeResult(scalar=len(self.notification_delivery_rows))
        if normalized == _normalize(policy_base.COUNT_CANDIDATE_BUNDLES_FOR_TARGET_QUERY):
            return FakeResult(scalar=1 + len(self.candidate_bundle_mutations))
        if normalized == _normalize(module.SELECT_CURRENT_ANALYSIS_ID_QUERY):
            return FakeResult(scalar=None)
        if normalized == _normalize(module.SELECT_CANDIDATE_BUNDLE_FINGERPRINT_QUERY):
            return FakeResult(scalar=self.bundle_fingerprint)
        if normalized.startswith("INSERT INTO state_transitions"):
            self.state_transitions.append(dict(params))
            return FakeResult()
        if normalized.startswith("INSERT INTO event_outbox") and "analysis.policy.apply.v1" in normalized:
            if self.drop_policy_apply_insert:
                return FakeResult()
            self.policy_apply_outbox.append(
                {
                    "event_type": "analysis.policy.apply.v1",
                    "aggregate_type": "judge_run",
                    "aggregate_id": UUID(str(params["judge_run_id"])),
                    "payload_json": json.loads(params["payload_json"]),
                }
            )
            return FakeResult()
        if "ANALYSES" in normalized and first in {"INSERT", "UPDATE", "DELETE"}:
            self.analyses.append({"statement": normalized, "params": params})
            return FakeResult()
        if "NOTIFICATION" in normalized and first in {"INSERT", "UPDATE", "DELETE"}:
            if "NOTIFICATION_PLANS" in normalized:
                self.notification_plan_rows.append({"statement": normalized, "params": params})
            elif "NOTIFICATION_RENDERS" in normalized:
                self.notification_render_rows.append({"statement": normalized, "params": params})
            elif "NOTIFICATION_DELIVERY_RECORDS" in normalized:
                self.notification_delivery_rows.append({"statement": normalized, "params": params})
            else:
                self.notification_intents.append({"statement": normalized, "params": params})
            return FakeResult()
        if "CANDIDATE_EVIDENCE_BUNDLES" in normalized and first in {"INSERT", "UPDATE", "DELETE"}:
            self.candidate_bundle_mutations.append({"statement": normalized, "params": params})
            return FakeResult()
        if "CANDIDATE_GROUP_PROPOSALS" in normalized and first in {"UPDATE", "DELETE"}:
            self.candidate_current_analysis_mutations.append({"statement": normalized, "params": params})
            return FakeResult()

        raise AssertionError(f"unexpected SQL: {statement}")

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True
        self.rollback_count += 1
        self.transaction_open = False

    async def close(self) -> None:
        self.closed = True

    def _snapshot_writes(self) -> dict[str, Any]:
        return {
            "judge_runs": list(self.judge_runs),
            "judge_outputs": list(self.judge_outputs),
            "ready_outbox": list(self.ready_outbox),
            "state_transitions": list(self.state_transitions),
            "policy_apply_outbox": list(self.policy_apply_outbox),
            "analyses": list(self.analyses),
            "notification_intents": list(self.notification_intents),
            "notification_plan_rows": list(self.notification_plan_rows),
            "notification_render_rows": list(self.notification_render_rows),
            "notification_delivery_rows": list(self.notification_delivery_rows),
            "candidate_bundle_mutations": list(self.candidate_bundle_mutations),
            "candidate_current_analysis_mutations": list(self.candidate_current_analysis_mutations),
        }

    def _restore_writes(self, snapshot: dict[str, Any]) -> None:
        self.judge_runs = snapshot["judge_runs"]
        self.judge_outputs = snapshot["judge_outputs"]
        self.ready_outbox = snapshot["ready_outbox"]
        self.state_transitions = snapshot["state_transitions"]
        self.policy_apply_outbox = snapshot["policy_apply_outbox"]
        self.analyses = snapshot["analyses"]
        self.notification_intents = snapshot["notification_intents"]
        self.notification_plan_rows = snapshot["notification_plan_rows"]
        self.notification_render_rows = snapshot["notification_render_rows"]
        self.notification_delivery_rows = snapshot["notification_delivery_rows"]
        self.candidate_bundle_mutations = snapshot["candidate_bundle_mutations"]
        self.candidate_current_analysis_mutations = snapshot["candidate_current_analysis_mutations"]


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_analysis_validator_non_suppress_policy_apply_seed_smoke"
    )


def _validator_base():
    return importlib.import_module("scripts.ops.dedicated_vps_analysis_validator_policy_apply_handoff_smoke")


def _policy_base():
    return importlib.import_module("scripts.ops.dedicated_vps_policy_engine_analysis_handoff_smoke")


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_reader(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "TELEGRAM_OPERATOR_CHAT_ID": FAKE_OPERATOR_CHAT_ID,
        "OPENAI_API_KEY": FAKE_DIRECT_KEY,
        "OPENAI_API_KEY_FILE": FAKE_KEY_FILE,
    }


def _raising_runtime_reader(_path: str | Path) -> dict[str, str]:
    raise AssertionError("runtime env should not be read")


def _raising_database_factory(_database_url: str) -> Any:
    raise AssertionError("database should not be opened")


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
            str(session.current_bundle_id),
            str(session.candidate_group_id),
            str(session.artifact_id),
            *forbidden_raw_values,
        ),
    )
    return result, session


def _assert_no_writes(session: FakeSession) -> None:
    assert session.committed is False
    assert session.rolled_back is True
    assert session.closed is True
    assert session.judge_runs == []
    assert session.judge_outputs == []
    assert session.ready_outbox == []
    assert session.state_transitions == []
    assert session.policy_apply_outbox == []
    assert session.analyses == []
    assert session.notification_intents == []
    assert session.notification_plan_rows == []
    assert session.notification_render_rows == []
    assert session.notification_delivery_rows == []
    for statement in session.statements:
        first = statement.split()[0].upper()
        assert first in {"SET", "SHOW", "SELECT"}
        assert " INSERT " not in f" {statement.upper()} "
        assert " UPDATE " not in f" {statement.upper()} "
        assert " DELETE " not in f" {statement.upper()} "


def _assert_no_downstream(report: dict[str, Any]) -> None:
    assert report["analysis_rows_written_bucket"] == "zero"
    assert report["notification_plan_intent_outbox_written_bucket"] == "zero"
    assert report["notification_plan_rows_written_bucket"] == "zero"
    assert report["notification_render_rows_written_bucket"] == "zero"
    assert report["notification_delivery_rows_written_bucket"] == "zero"
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


def test_parser_has_no_openai_policy_or_telegram_send_approval_flags() -> None:
    parser = _module().build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--approve-key-read"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--approve-live-openai"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--approve-policy-engine"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--approve-telegram-send"])


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
    assert report["judge_run_written_bucket"] == "zero"
    assert report["judge_output_written_bucket"] == "zero"
    assert report["analysis_policy_apply_outbox_written_bucket"] == "zero"
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


def test_db_read_preflight_finds_seedable_current_bundle_and_writes_nothing() -> None:
    result, session = _run_report()

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_DB_PREFLIGHT_PASSED
    assert report["runtime_env_read"] is True
    assert report["database_configured"] is True
    assert report["database_connected"] is True
    assert report["read_only_transaction"] is True
    assert report["seed_target_candidate_found_bucket"] == "one"
    assert report["seed_target_bundle_found_bucket"] == "one"
    assert report["current_bundle_match_bucket"] == "one"
    assert report["seed_existing_judge_run_bucket"] == "zero"
    assert report["seed_existing_judge_output_bucket"] == "zero"
    assert report["seed_existing_judge_output_ready_outbox_bucket"] == "zero"
    assert report["seed_existing_policy_apply_outbox_bucket"] == "zero"
    assert report["output_schema_valid_bucket"] == "one"
    assert report["business_rules_valid_bucket"] == "one"
    assert report["analysis_verdict_bucket"] == "inspect_now"
    assert report["analysis_delivery_decision_bucket"] == "send_now"
    assert report["database_write_attempted"] is False
    assert report["raw_values_emitted"] is False
    _assert_no_downstream(report)
    _assert_no_raw_values(
        report,
        FAKE_RUNTIME_PATH,
        FAKE_DATABASE_URL,
        FAKE_OPERATOR_CHAT_ID,
        FAKE_DIRECT_KEY,
        FAKE_KEY_FILE,
        str(session.bundle_id),
        str(session.candidate_group_id),
        str(session.artifact_id),
    )
    _assert_no_writes(session)


def test_db_read_preflight_rejects_missing_candidate_or_bundle() -> None:
    result, session = _run_report(session=FakeSession(include_candidate=False))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DB_READ_FAILED
    assert result.report["checks_failed"] == ["seed_target.missing"]
    assert result.report["seed_target_candidate_found_bucket"] == "zero"
    assert result.report["seed_target_bundle_found_bucket"] == "zero"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_current_bundle_mismatch() -> None:
    result, session = _run_report(session=FakeSession(current_bundle_matches=False))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_VALIDATION_FAILED
    assert result.report["checks_failed"] == ["candidate.current_bundle"]
    assert result.report["current_bundle_match_bucket"] == "zero"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_existing_seed_policy_apply_event() -> None:
    result, session = _run_report(session=FakeSession(existing_seed_policy_apply_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EXISTING_SEED
    assert result.report["checks_failed"] == ["seed.policy_apply_existing"]
    assert result.report["seed_existing_policy_apply_outbox_bucket"] == "one"
    _assert_no_writes(session)


def test_db_write_fake_success_writes_seed_judge_output_and_policy_apply_only() -> None:
    result, session = _run_report(approve_db_write=True)

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_DB_WRITE_PASSED
    assert report["database_write_attempted"] is True
    assert report["judge_run_written_bucket"] == "one"
    assert report["judge_output_written_bucket"] == "one"
    assert report["judge_output_ready_outbox_written_bucket"] == "one"
    assert report["validator_state_transitions_written_bucket"] == "one"
    assert report["analysis_policy_apply_outbox_written_bucket"] == "one"
    assert len(session.judge_runs) == 1
    assert session.judge_runs[0]["prompt_version"].endswith(_module().SEED_SUFFIX)
    assert len(session.judge_outputs) == 1
    assert session.judge_outputs[0]["payload_json"]["candidate_group_id"] == str(session.candidate_group_id)
    assert len(session.ready_outbox) == 1
    assert len(session.state_transitions) == 1
    assert session.state_transitions[0]["object_type"] == "judge_run"
    assert session.state_transitions[0]["object_id"] == str(session.judge_run_id)
    assert session.state_transitions[0]["from_state"] == "succeeded"
    assert session.state_transitions[0]["to_state"] == "analysis_validated"
    assert len(session.policy_apply_outbox) == 1
    assert session.policy_apply_outbox[0]["payload_json"] == {
        "judge_run_id": str(session.judge_run_id),
        "judge_output_id": str(session.judge_output_id),
        "candidate_group_id": str(session.candidate_group_id),
        "bundle_id": str(session.bundle_id),
    }
    assert session.committed is True
    assert session.rolled_back is False
    assert session.closed is True


def test_db_write_handles_existing_read_transaction_before_explicit_write() -> None:
    result, session = _run_report(
        approve_db_write=True,
        session=FakeSession(
            implicit_read_transaction=True,
            fail_begin_if_transaction_open=True,
        ),
    )

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_DB_WRITE_PASSED
    assert report["database_write_attempted"] is True
    assert report["judge_run_written_bucket"] == "one"
    assert report["analysis_policy_apply_outbox_written_bucket"] == "one"
    assert session.rollback_count >= 1
    assert session.begin_count == 1
    assert session.committed is True
    assert session.closed is True


def test_db_write_expected_effect_failure_rolls_back() -> None:
    result, session = _run_report(
        approve_db_write=True,
        session=FakeSession(drop_policy_apply_insert=True),
    )

    report = result.report
    assert result.exit_code == 1
    assert report["contract_status"] == _module().STATUS_WRITE_FAILED
    assert report["checks_failed"] == ["db_write.expected_effects"]
    assert report["database_write_attempted"] is True
    assert report["judge_run_written_bucket"] == "one"
    assert report["judge_output_written_bucket"] == "one"
    assert report["analysis_policy_apply_outbox_written_bucket"] == "zero"
    assert session.committed is False
    assert session.rolled_back is True
    assert session.closed is True
    assert session.judge_runs == []
    assert session.judge_outputs == []
    assert session.ready_outbox == []
    assert session.state_transitions == []
    assert session.policy_apply_outbox == []


def test_db_write_fake_success_does_not_write_analysis_or_notification_rows() -> None:
    result, session = _run_report(approve_db_write=True)

    report = result.report
    assert report["analysis_rows_written_bucket"] == "zero"
    assert report["notification_plan_intent_outbox_written_bucket"] == "zero"
    assert report["notification_plan_rows_written_bucket"] == "zero"
    assert report["notification_render_rows_written_bucket"] == "zero"
    assert report["notification_delivery_rows_written_bucket"] == "zero"
    assert session.analyses == []
    assert session.notification_intents == []
    assert session.notification_plan_rows == []
    assert session.notification_render_rows == []
    assert session.notification_delivery_rows == []
    assert report["stops_at_event_type"] == "analysis.policy.apply.v1"


def test_db_write_fake_success_has_no_openai_key_redis_telegram_or_raw_output() -> None:
    result, session = _run_report(approve_db_write=True)

    report = result.report
    assert report["openai_key_read_bucket"] == "zero"
    assert report["openai_call_attempted"] is False
    assert report["redis_connected"] is False
    assert report["redis_write_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert report["redis_delete_or_trim_attempted"] is False
    assert report["policy_engine_started"] is False
    assert report["notifier_started"] is False
    assert report["telegram_send_attempted"] is False
    assert report["candidate_bundle_mutation_attempted"] is False
    assert report["candidate_current_analysis_mutation_attempted"] is False
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
        str(session.current_bundle_id),
        str(session.candidate_group_id),
        str(session.artifact_id),
    )
