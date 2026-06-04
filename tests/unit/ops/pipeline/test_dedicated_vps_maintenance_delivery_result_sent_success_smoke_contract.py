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
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_maintenance_delivery_result_sent_success_smoke.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-maintenance-sent-success"
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
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-maintenance-sent-success-runtime.env"
FAKE_BOT_TOKEN = "private" + "-telegram" + "-bot" + "-maintenance-sent-success"
FAKE_OPENAI_KEY = "direct" + "-openai" + "-secret" + "-maintenance-sent-success"
FAKE_REDIS_URL = "redis" + ":/" + "/private-maintenance-sent-success"
FAKE_URL = "https" + ":" + "/" + "/" + "example.invalid/private/maintenance-sent-success"
FAKE_SOURCE_TEXT = "private source text maintenance sent success must not leak"
FAKE_MESSAGE_BODY = "private rendered telegram body maintenance sent success must not leak"


class TrackingEnv(dict[str, str]):
    def __init__(self) -> None:
        super().__init__(
            {
                "DATABASE_URL": FAKE_DATABASE_URL,
                "TELEGRAM_BOT_TOKEN": FAKE_BOT_TOKEN,
                "OPENAI_API_KEY": FAKE_OPENAI_KEY,
                "REDIS_URL": FAKE_REDIS_URL,
            }
        )
        self.accessed: list[str] = []

    def get(self, key: str, default: Any = None) -> Any:
        self.accessed.append(key)
        return super().get(key, default)


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
        include_target: bool = True,
        target_count: int = 1,
        delivery_status: str = "sent",
        delivery_result_outbox_count: int = 1,
        notification_plan_count: int = 1,
        notification_render_count: int = 1,
        notification_delivery_count: int = 2,
        notifier_transition_count: int = 3,
        analysis_count: int = 1,
        policy_transition_count: int = 1,
        judge_output_count: int = 1,
        candidate_count: int = 1,
        existing_sent_success_marker_count: int = 0,
        existing_retry_intent_count: int = 0,
        existing_dead_letter_count: int = 0,
        existing_replay_request_count: int = 0,
        implicit_read_transaction: bool = False,
        fail_begin_if_transaction_open: bool = False,
        drop_job_attempt_insert: bool = False,
    ) -> None:
        self.delivery_result_event_id = uuid4()
        self.notification_plan_id = uuid4()
        self.notification_delivery_record_id = uuid4()
        self.analysis_id = uuid4()
        self.judge_output_id = uuid4()
        self.candidate_group_id = uuid4()
        self.current_analysis_id = self.analysis_id
        self.bundle_fingerprint = "candidate-bundle-fingerprint-private-maintenance-sent"
        self.include_target = include_target
        self.target_count = target_count
        self.delivery_status = delivery_status
        self.delivery_result_outbox_count = delivery_result_outbox_count
        self.notification_plan_count = notification_plan_count
        self.notification_render_count = notification_render_count
        self.notification_delivery_count = notification_delivery_count
        self.notifier_transition_count = notifier_transition_count
        self.analysis_count = analysis_count
        self.policy_transition_count = policy_transition_count
        self.judge_output_count = judge_output_count
        self.candidate_count = candidate_count
        self.existing_sent_success_marker_count = existing_sent_success_marker_count
        self.existing_retry_intent_count = existing_retry_intent_count
        self.existing_dead_letter_count = existing_dead_letter_count
        self.existing_replay_request_count = existing_replay_request_count
        self.implicit_read_transaction = implicit_read_transaction
        self.fail_begin_if_transaction_open = fail_begin_if_transaction_open
        self.drop_job_attempt_insert = drop_job_attempt_insert
        self.transaction_open = False
        self.explicit_transaction_open = False
        self.begin_count = 0
        self.rollback_count = 0
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.statements: list[str] = []
        self.job_attempts: list[dict[str, Any]] = []
        self.retry_intents: list[dict[str, Any]] = []
        self.dead_letters: list[dict[str, Any]] = []
        self.replay_requests: list[dict[str, Any]] = []
        self.notification_plan_mutations: list[dict[str, Any]] = []
        self.notification_render_mutations: list[dict[str, Any]] = []
        self.notification_delivery_mutations: list[dict[str, Any]] = []
        self.notifier_transition_mutations: list[dict[str, Any]] = []
        self.analysis_mutations: list[dict[str, Any]] = []
        self.judge_output_mutations: list[dict[str, Any]] = []
        self.policy_transition_mutations: list[dict[str, Any]] = []
        self.candidate_bundle_mutations: list[dict[str, Any]] = []
        self.candidate_current_analysis_mutations: list[dict[str, Any]] = []

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
        first = normalized.split()[0].upper()
        self.statements.append(normalized)
        module = _module()
        base_module = _base_module()
        if self.implicit_read_transaction and not self.explicit_transaction_open and first == "SELECT":
            self.transaction_open = True

        if normalized == _normalize(base_module.SET_TRANSACTION_READ_ONLY_QUERY):
            self.transaction_open = True
            return FakeResult()
        if normalized == _normalize(base_module.SHOW_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult(scalar="on")
        if normalized == _normalize(base_module.SELECT_ONE_QUERY):
            return FakeResult(scalar=1)
        if normalized == _normalize(module.SELECT_SENT_DELIVERY_RESULT_TARGETS_QUERY):
            if not self.include_target:
                return FakeResult(rows=[])
            return FakeResult(rows=[self._target_row(index) for index in range(self.target_count)])
        if normalized == _normalize(module.COUNT_DELIVERY_RESULT_OUTBOX_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.delivery_result_outbox_count)
        if normalized == _normalize(module.COUNT_NOTIFICATION_PLAN_ROWS_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.notification_plan_count + len(self.notification_plan_mutations))
        if normalized == _normalize(module.COUNT_NOTIFICATION_RENDER_ROWS_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.notification_render_count + len(self.notification_render_mutations))
        if normalized == _normalize(module.COUNT_NOTIFICATION_DELIVERY_ROWS_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.notification_delivery_count + len(self.notification_delivery_mutations))
        if normalized == _normalize(module.COUNT_NOTIFIER_STATE_TRANSITIONS_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.notifier_transition_count + len(self.notifier_transition_mutations))
        if normalized == _normalize(module.COUNT_ANALYSIS_ROWS_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.analysis_count + len(self.analysis_mutations))
        if normalized == _normalize(module.COUNT_POLICY_STATE_TRANSITIONS_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.policy_transition_count + len(self.policy_transition_mutations))
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUT_ROWS_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.judge_output_count + len(self.judge_output_mutations))
        if normalized == _normalize(module.COUNT_CANDIDATE_ROWS_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.candidate_count + len(self.candidate_current_analysis_mutations))
        if normalized == _normalize(module.COUNT_MAINTENANCE_SENT_SUCCESS_MARKER_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.existing_sent_success_marker_count + len(self.job_attempts))
        if normalized == _normalize(module.COUNT_RETRY_INTENT_OUTBOX_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.existing_retry_intent_count + len(self.retry_intents))
        if normalized == _normalize(module.COUNT_DEAD_LETTER_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.existing_dead_letter_count + len(self.dead_letters))
        if normalized == _normalize(module.COUNT_REPLAY_REQUEST_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.existing_replay_request_count + len(self.replay_requests))
        if normalized == _normalize(module.SELECT_CANDIDATE_CURRENT_ANALYSIS_ID_QUERY):
            return FakeResult(scalar=self.current_analysis_id)
        if normalized == _normalize(module.SELECT_CANDIDATE_BUNDLE_FINGERPRINT_QUERY):
            return FakeResult(scalar=self.bundle_fingerprint)

        if normalized.startswith("INSERT INTO job_attempts"):
            if not self.drop_job_attempt_insert:
                self.job_attempts.append(dict(params))
            return FakeResult()

        if first in {"INSERT", "UPDATE", "DELETE"}:
            upper = normalized.upper()
            if "NOTIFICATION_PLANS" in upper:
                self.notification_plan_mutations.append(dict(params))
                return FakeResult()
            if "NOTIFICATION_RENDERS" in upper:
                self.notification_render_mutations.append(dict(params))
                return FakeResult()
            if "NOTIFICATION_DELIVERY_RECORDS" in upper:
                self.notification_delivery_mutations.append(dict(params))
                return FakeResult()
            if "STATE_TRANSITIONS" in upper and params.get("object_type") == "notification_plan":
                self.notifier_transition_mutations.append(dict(params))
                return FakeResult()
            if "STATE_TRANSITIONS" in upper and params.get("object_type") == "analysis":
                self.policy_transition_mutations.append(dict(params))
                return FakeResult()
            if "EVENT_OUTBOX" in upper:
                self.retry_intents.append(dict(params))
                return FakeResult()
            if "DEAD_LETTER_ENTRIES" in upper:
                self.dead_letters.append(dict(params))
                return FakeResult()
            if "REPLAY_REQUESTS" in upper:
                self.replay_requests.append(dict(params))
                return FakeResult()
            if "ANALYSES" in upper:
                self.analysis_mutations.append(dict(params))
                return FakeResult()
            if "JUDGE_OUTPUTS" in upper:
                self.judge_output_mutations.append(dict(params))
                return FakeResult()
            if "CANDIDATE_EVIDENCE_BUNDLES" in upper:
                self.candidate_bundle_mutations.append(dict(params))
                return FakeResult()
            if "CANDIDATE_GROUP_PROPOSALS" in upper:
                self.candidate_current_analysis_mutations.append(dict(params))
                return FakeResult()

        raise AssertionError(f"unexpected SQL: {statement}")

    async def rollback(self) -> None:
        self.rolled_back = True
        self.rollback_count += 1
        self.transaction_open = False

    async def close(self) -> None:
        self.closed = True

    def _target_row(self, index: int = 0) -> dict[str, Any]:
        return {
            "trigger_event_id": self.delivery_result_event_id if index == 0 else uuid4(),
            "delivery_result_created_at": datetime(2026, 6, 3, 4, 5, 6, tzinfo=timezone.utc),
            "notification_plan_id": self.notification_plan_id if index == 0 else uuid4(),
            "notification_delivery_record_id": self.notification_delivery_record_id if index == 0 else uuid4(),
            "delivery_status": self.delivery_status,
            "delivery_reason": None,
            "analysis_id": self.analysis_id,
            "candidate_group_id": self.candidate_group_id,
            "judge_output_id": self.judge_output_id,
            "judge_output_row_id": self.judge_output_id,
            "candidate_row_id": self.candidate_group_id,
        }

    def _snapshot_writes(self) -> dict[str, Any]:
        return {
            "job_attempts": list(self.job_attempts),
            "retry_intents": list(self.retry_intents),
            "dead_letters": list(self.dead_letters),
            "replay_requests": list(self.replay_requests),
            "notification_plan_mutations": list(self.notification_plan_mutations),
            "notification_render_mutations": list(self.notification_render_mutations),
            "notification_delivery_mutations": list(self.notification_delivery_mutations),
            "notifier_transition_mutations": list(self.notifier_transition_mutations),
            "analysis_mutations": list(self.analysis_mutations),
            "judge_output_mutations": list(self.judge_output_mutations),
            "policy_transition_mutations": list(self.policy_transition_mutations),
            "candidate_bundle_mutations": list(self.candidate_bundle_mutations),
            "candidate_current_analysis_mutations": list(self.candidate_current_analysis_mutations),
        }

    def _restore_writes(self, snapshot: dict[str, Any]) -> None:
        self.job_attempts = snapshot["job_attempts"]
        self.retry_intents = snapshot["retry_intents"]
        self.dead_letters = snapshot["dead_letters"]
        self.replay_requests = snapshot["replay_requests"]
        self.notification_plan_mutations = snapshot["notification_plan_mutations"]
        self.notification_render_mutations = snapshot["notification_render_mutations"]
        self.notification_delivery_mutations = snapshot["notification_delivery_mutations"]
        self.notifier_transition_mutations = snapshot["notifier_transition_mutations"]
        self.analysis_mutations = snapshot["analysis_mutations"]
        self.judge_output_mutations = snapshot["judge_output_mutations"]
        self.policy_transition_mutations = snapshot["policy_transition_mutations"]
        self.candidate_bundle_mutations = snapshot["candidate_bundle_mutations"]
        self.candidate_current_analysis_mutations = snapshot["candidate_current_analysis_mutations"]


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_maintenance_delivery_result_sent_success_smoke"
    )


def _base_module():
    return importlib.import_module("scripts.ops.dedicated_vps_policy_engine_analysis_handoff_smoke")


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_reader(env: TrackingEnv):
    def reader(_path: str | Path) -> TrackingEnv:
        return env

    return reader


def _raising_runtime_reader(_path: str | Path) -> dict[str, str]:
    raise AssertionError("runtime env should not be read")


def _raising_database_factory(_database_url: str) -> Any:
    raise AssertionError("database should not be opened")


def _run_report(
    *,
    approve_db_read: bool = True,
    approve_db_write: bool = False,
    session: FakeSession | None = None,
    env: TrackingEnv | None = None,
    forbidden_raw_values: tuple[str, ...] = (),
) -> tuple[Any, FakeSession, TrackingEnv]:
    session = session or FakeSession()
    env = env or TrackingEnv()
    result = _module().generate_report(
        approve_db_read=approve_db_read,
        approve_db_write=approve_db_write,
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=_runtime_reader(env),
        database_session_factory=lambda _url: session,
        forbidden_raw_values=(
            FAKE_RUNTIME_PATH,
            FAKE_DATABASE_URL,
            FAKE_BOT_TOKEN,
            FAKE_OPENAI_KEY,
            FAKE_REDIS_URL,
            FAKE_URL,
            FAKE_SOURCE_TEXT,
            FAKE_MESSAGE_BODY,
            str(session.delivery_result_event_id),
            str(session.notification_plan_id),
            str(session.notification_delivery_record_id),
            str(session.analysis_id),
            str(session.judge_output_id),
            str(session.candidate_group_id),
            *forbidden_raw_values,
        ),
    )
    return result, session, env


def _assert_no_writes(session: FakeSession) -> None:
    assert session.committed is False
    assert session.rolled_back is True
    assert session.closed is True
    assert session.job_attempts == []
    assert session.retry_intents == []
    assert session.dead_letters == []
    assert session.replay_requests == []
    assert session.notification_plan_mutations == []
    assert session.notification_render_mutations == []
    assert session.notification_delivery_mutations == []
    assert session.notifier_transition_mutations == []
    assert session.analysis_mutations == []
    assert session.judge_output_mutations == []
    assert session.policy_transition_mutations == []
    assert session.candidate_bundle_mutations == []
    assert session.candidate_current_analysis_mutations == []
    for statement in session.statements:
        first = statement.split()[0].upper()
        assert first in {"SET", "SHOW", "SELECT", "WITH"}


def _assert_no_forbidden_writes(report: dict[str, Any], session: FakeSession) -> None:
    assert report["retry_intent_outbox_written_bucket"] == "zero"
    assert report["dead_letter_rows_written_bucket"] == "zero"
    assert report["replay_requests_written_bucket"] == "zero"
    assert report["notification_plan_rows_written_bucket"] == "zero"
    assert report["notification_render_rows_written_bucket"] == "zero"
    assert report["notification_delivery_rows_written_bucket"] == "zero"
    assert report["notifier_state_transitions_written_bucket"] == "zero"
    assert report["analysis_rows_written_bucket"] == "zero"
    assert report["judge_outputs_written_bucket"] == "zero"
    assert report["policy_state_transitions_written_bucket"] == "zero"
    assert report["candidate_bundle_mutation_attempted"] is False
    assert report["candidate_current_analysis_mutation_attempted"] is False
    assert report["redis_connected"] is False
    assert report["redis_write_attempted"] is False
    assert report["telegram_bot_token_read_bucket"] == "zero"
    assert report["telegram_send_attempted"] is False
    assert report["telegram_edit_attempted"] is False
    assert report["openai_call_attempted"] is False
    assert session.retry_intents == []
    assert session.dead_letters == []
    assert session.replay_requests == []
    assert session.notification_plan_mutations == []
    assert session.notification_render_mutations == []
    assert session.notification_delivery_mutations == []
    assert session.notifier_transition_mutations == []
    assert session.analysis_mutations == []
    assert session.judge_output_mutations == []
    assert session.policy_transition_mutations == []
    assert session.candidate_bundle_mutations == []
    assert session.candidate_current_analysis_mutations == []


def _assert_no_raw_values(report: dict[str, Any], *values: str) -> None:
    rendered = _module().render_json(report)
    for value in values:
        assert value not in rendered


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_parser_has_no_redis_openai_or_telegram_send_approval_flags() -> None:
    parser = _module().build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--approve-redis-write"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--approve-live-openai"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--approve-telegram-send"])


def test_runtime_parser_reads_database_url_only(tmp_path: Path) -> None:
    runtime_env = tmp_path / "runtime.env"
    runtime_env.write_text(
        "\n".join(
            [
                f"DATABASE_URL={FAKE_DATABASE_URL}",
                f"TELEGRAM_BOT_TOKEN={FAKE_BOT_TOKEN}",
                f"OPENAI_API_KEY={FAKE_OPENAI_KEY}",
                f"REDIS_URL={FAKE_REDIS_URL}",
                "TELEGRAM_OPERATOR_CHAT_ID=123456789",
            ]
        ),
        encoding="utf-8",
    )

    values = _module().parse_runtime_env_file(runtime_env)

    assert values == {"DATABASE_URL": FAKE_DATABASE_URL}


def test_default_mode_does_not_read_env_db_redis_key_or_telegram() -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = FAKE_DATABASE_URL
    env["OPENAI_API_KEY"] = FAKE_OPENAI_KEY
    env["TELEGRAM_BOT_TOKEN"] = FAKE_BOT_TOKEN
    env["REDIS_URL"] = FAKE_REDIS_URL
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
    assert report["database_configured"] is False
    assert report["database_connected"] is False
    assert report["database_write_attempted"] is False
    assert report["telegram_bot_token_read_bucket"] == "zero"
    assert report["telegram_send_attempted"] is False
    assert report["telegram_edit_attempted"] is False
    assert report["redis_connected"] is False
    assert report["redis_write_attempted"] is False
    assert report["openai_call_attempted"] is False
    assert report["checks_failed"] == []
    assert FAKE_DATABASE_URL not in completed.stdout
    assert FAKE_OPENAI_KEY not in completed.stdout
    assert FAKE_BOT_TOKEN not in completed.stdout
    assert FAKE_REDIS_URL not in completed.stdout
    assert completed.stderr == ""


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
    assert result.report["database_write_attempted"] is False


def test_db_read_preflight_finds_existing_sent_result_and_writes_nothing() -> None:
    result, session, env = _run_report()

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_DB_PREFLIGHT_PASSED
    assert report["runtime_env_read"] is True
    assert report["database_configured"] is True
    assert report["database_connected"] is True
    assert report["read_only_transaction"] is True
    assert report["database_write_attempted"] is False
    assert report["target_delivery_result_outbox_found_bucket"] == "one"
    assert report["target_notification_plan_found_bucket"] == "one"
    assert report["target_delivery_record_found_bucket"] == "one"
    assert report["target_notification_delivery_status_bucket"] == "sent"
    assert report["target_delivery_status_bucket"] == "sent"
    assert report["target_analysis_found_bucket"] == "one"
    assert report["target_judge_output_found_bucket"] == "one"
    assert report["target_candidate_found_bucket"] == "one"
    assert report["existing_maintenance_sent_success_marker_bucket"] == "zero"
    assert report["maintenance_classification_bucket"] == "terminal_success"
    assert report["telegram_bot_token_read_bucket"] == "zero"
    assert report["telegram_send_attempted"] is False
    assert report["telegram_edit_attempted"] is False
    assert report["redis_write_attempted"] is False
    assert report["openai_call_attempted"] is False
    assert report["raw_values_emitted"] is False
    assert report["checks_failed"] == []
    assert env.accessed == ["DATABASE_URL"]
    _assert_no_writes(session)
    _assert_no_raw_values(
        report,
        FAKE_RUNTIME_PATH,
        FAKE_DATABASE_URL,
        FAKE_BOT_TOKEN,
        FAKE_OPENAI_KEY,
        FAKE_REDIS_URL,
        FAKE_URL,
        FAKE_SOURCE_TEXT,
        FAKE_MESSAGE_BODY,
        str(session.delivery_result_event_id),
        str(session.notification_plan_id),
        str(session.notification_delivery_record_id),
        str(session.analysis_id),
        str(session.judge_output_id),
        str(session.candidate_group_id),
    )


def test_db_read_preflight_rejects_missing_target() -> None:
    result, session, _env = _run_report(session=FakeSession(include_target=False))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NO_TARGET
    assert result.report["checks_failed"] == ["target.delivery_result_outbox_missing"]
    assert result.report["target_delivery_result_outbox_found_bucket"] == "zero"
    assert result.report["database_write_attempted"] is False
    _assert_no_writes(session)


def test_db_read_preflight_rejects_multiple_targets() -> None:
    result, session, _env = _run_report(session=FakeSession(target_count=2))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_MULTIPLE_TARGETS
    assert result.report["checks_failed"] == ["target.delivery_result_outbox_not_exactly_one"]
    assert result.report["target_delivery_result_outbox_found_bucket"] == "multiple"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_wrong_status() -> None:
    result, session, _env = _run_report(session=FakeSession(delivery_status="failed_retryable"))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_WRONG_DELIVERY_RESULT
    assert result.report["checks_failed"] == ["failed_retryable_requires_due_retry_path"]
    assert result.report["target_delivery_status_bucket"] == "failed_retryable"
    assert result.report["maintenance_classification_bucket"] == "out_of_scope"
    assert result.report["database_write_attempted"] is False
    _assert_no_writes(session)


def test_db_read_preflight_rejects_existing_sent_success_marker() -> None:
    result, session, _env = _run_report(session=FakeSession(existing_sent_success_marker_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EXISTING_MARKER
    assert result.report["checks_failed"] == ["maintenance_sent_success_marker.existing"]
    assert result.report["existing_maintenance_sent_success_marker_bucket"] == "one"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_existing_retry_dead_letter_or_replay_effect() -> None:
    result, session, _env = _run_report(session=FakeSession(existing_retry_intent_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_EXISTING_EFFECT
    assert result.report["checks_failed"] == ["retry_intent_outbox.existing"]
    assert result.report["existing_retry_intent_outbox_for_target_bucket"] == "one"
    _assert_no_writes(session)


def test_db_write_fake_success_writes_one_maintenance_marker_only() -> None:
    result, session, env = _run_report(approve_db_write=True)

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_DB_WRITE_PASSED
    assert report["database_write_attempted"] is True
    assert report["maintenance_started"] is True
    assert report["maintenance_classification_bucket"] == "terminal_success"
    assert report["job_attempts_written_bucket"] == "one"
    assert report["target_delivery_status_bucket"] == "sent"
    assert len(session.job_attempts) == 1
    attempt = session.job_attempts[0]
    assert attempt["stage_name"] == "maintenance_delivery_result"
    assert attempt["queue_name"] == "q.maintenance"
    assert attempt["root_object_type"] == "notification_delivery_record"
    assert str(attempt["root_object_id"]) == str(session.notification_delivery_record_id)
    assert attempt["attempt_status"] == "succeeded"
    assert attempt["error_code"] == "delivery_result_sent_terminal_success"
    assert session.committed is True
    assert session.rolled_back is False
    assert session.closed is True
    assert env.accessed == ["DATABASE_URL"]
    _assert_no_forbidden_writes(report, session)


def test_db_write_blocks_existing_marker_without_duplicate() -> None:
    result, session, _env = _run_report(
        approve_db_write=True,
        session=FakeSession(existing_sent_success_marker_count=1),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EXISTING_MARKER
    assert result.report["database_write_attempted"] is False
    assert result.report["job_attempts_written_bucket"] == "zero"
    assert session.job_attempts == []
    _assert_no_writes(session)


def test_db_write_handles_existing_read_transaction_before_explicit_write() -> None:
    result, session, _env = _run_report(
        approve_db_write=True,
        session=FakeSession(
            implicit_read_transaction=True,
            fail_begin_if_transaction_open=True,
        ),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_DB_WRITE_PASSED
    assert result.report["database_write_attempted"] is True
    assert session.rollback_count >= 1
    assert session.begin_count == 1
    assert session.committed is True
    assert len(session.job_attempts) == 1


def test_db_write_expected_effect_failure_rolls_back() -> None:
    result, session, _env = _run_report(
        approve_db_write=True,
        session=FakeSession(drop_job_attempt_insert=True),
    )

    report = result.report
    assert result.exit_code == 1
    assert report["contract_status"] == _module().STATUS_WRITE_FAILED
    assert report["checks_failed"] == ["db_write.expected_effects"]
    assert report["database_write_attempted"] is True
    assert report["job_attempts_written_bucket"] == "zero"
    assert session.committed is False
    assert session.rolled_back is True
    assert session.closed is True
    assert session.job_attempts == []


def test_db_write_fake_success_has_no_transport_recovery_upstream_or_raw_output() -> None:
    result, session, _env = _run_report(approve_db_write=True)

    report = result.report
    assert report["raw_values_emitted"] is False
    assert report["checks_failed"] == []
    _assert_no_forbidden_writes(report, session)
    _assert_no_raw_values(
        report,
        FAKE_RUNTIME_PATH,
        FAKE_DATABASE_URL,
        FAKE_BOT_TOKEN,
        FAKE_OPENAI_KEY,
        FAKE_REDIS_URL,
        FAKE_URL,
        FAKE_SOURCE_TEXT,
        FAKE_MESSAGE_BODY,
        str(session.delivery_result_event_id),
        str(session.notification_plan_id),
        str(session.notification_delivery_record_id),
        str(session.analysis_id),
        str(session.judge_output_id),
        str(session.candidate_group_id),
    )
