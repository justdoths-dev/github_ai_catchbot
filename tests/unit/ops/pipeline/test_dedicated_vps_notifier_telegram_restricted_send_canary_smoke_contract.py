from __future__ import annotations

import importlib
import json
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
    / "dedicated_vps_notifier_telegram_restricted_send_canary_smoke.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-restricted-send-canary"
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
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-restricted-send-canary-runtime.env"
FAKE_OPERATOR_CHAT_ID = "123456789"
FAKE_OTHER_CHAT_ID = "987654321"
FAKE_BOT_TOKEN = "private" + "-telegram" + "-bot" + "-restricted-send-canary"
FAKE_OPENAI_KEY = "direct" + "-openai" + "-secret" + "-restricted-send-canary"
FAKE_REDIS_URL = "redis" + ":/" + "/private-restricted-send-canary"
FAKE_RENDERED_TEXT = "private rendered telegram body restricted canary must not leak"
FAKE_URL = "https" + ":" + "/" + "/" + "example.invalid/private/restricted-canary"
FAKE_MESSAGE_ID = 7777777


class TrackingEnv(dict[str, str]):
    def __init__(self) -> None:
        super().__init__(
            {
                "DATABASE_URL": FAKE_DATABASE_URL,
                "TELEGRAM_OPERATOR_CHAT_ID": FAKE_OPERATOR_CHAT_ID,
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

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None


class FakeSession:
    def __init__(
        self,
        *,
        include_target: bool = True,
        target_count: int = 1,
        render_count: int = 1,
        prior_dry_run_delivery_count: int = 1,
        prior_delivery_result_outbox_count: int = 1,
        prior_maintenance_noop_marker_count: int = 1,
        existing_successful_delivery_count: int = 0,
        target_chat_id: int = int(FAKE_OPERATOR_CHAT_ID),
        drop_delivery_result_outbox_insert: bool = False,
    ) -> None:
        self.include_target = include_target
        self.target_count = target_count
        self.render_count = render_count
        self.prior_dry_run_delivery_count = prior_dry_run_delivery_count
        self.prior_delivery_result_outbox_count = prior_delivery_result_outbox_count
        self.prior_maintenance_noop_marker_count = prior_maintenance_noop_marker_count
        self.existing_successful_delivery_count = existing_successful_delivery_count
        self.target_chat_id = target_chat_id
        self.drop_delivery_result_outbox_insert = drop_delivery_result_outbox_insert
        self.plan_intent_event_id = uuid4()
        self.notification_plan_id = uuid4()
        self.analysis_id = uuid4()
        self.judge_output_id = uuid4()
        self.candidate_group_id = uuid4()
        self.current_analysis_id = self.analysis_id
        self.bundle_fingerprint = "private-bundle-fingerprint-restricted-canary"
        self.transaction_open = False
        self.explicit_transaction_open = False
        self.begin_count = 0
        self.rollback_count = 0
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.statements: list[str] = []
        self.plan_status_updates: list[dict[str, Any]] = []
        self.notification_delivery_records: list[dict[str, Any]] = []
        self.notifier_state_transitions: list[dict[str, Any]] = []
        self.delivery_result_outbox: list[dict[str, Any]] = []
        self.notification_render_mutations: list[dict[str, Any]] = []
        self.analysis_mutations: list[dict[str, Any]] = []
        self.judge_output_mutations: list[dict[str, Any]] = []
        self.policy_state_transition_mutations: list[dict[str, Any]] = []
        self.candidate_bundle_mutations: list[dict[str, Any]] = []
        self.candidate_current_analysis_mutations: list[dict[str, Any]] = []

    def in_transaction(self) -> bool:
        return self.transaction_open

    @asynccontextmanager
    async def _begin(self):
        self.transaction_open = True
        self.explicit_transaction_open = True
        snapshot = self._snapshot()
        try:
            yield self
        except Exception:
            self._restore(snapshot)
            self.rolled_back = True
            raise
        else:
            self.committed = True
        finally:
            self.explicit_transaction_open = False
            self.transaction_open = False

    def begin(self):
        self.begin_count += 1
        return self._begin()

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(str(statement))
        upper = normalized.upper()
        self.statements.append(normalized)
        module = _module()
        base_module = _base_module()

        if normalized == _normalize(base_module.SET_TRANSACTION_READ_ONLY_QUERY):
            self.transaction_open = True
            return FakeResult()
        if normalized == _normalize(base_module.SHOW_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult(scalar="on")
        if normalized == _normalize(base_module.SELECT_ONE_QUERY):
            return FakeResult(scalar=1)
        if normalized == _normalize(module.SELECT_CANARY_TARGETS_QUERY):
            if not self.include_target:
                return FakeResult(rows=[])
            return FakeResult(rows=[self._target_row(index) for index in range(self.target_count)])
        if normalized == _normalize(module.SELECT_RENDER_FOR_TARGET_QUERY):
            rows = [self._render_row(index) for index in range(min(self.render_count, 2))]
            return FakeResult(rows=rows)
        if normalized == _normalize(module.SELECT_CANDIDATE_CURRENT_ANALYSIS_ID_QUERY):
            return FakeResult(scalar=self.current_analysis_id)
        if normalized == _normalize(module.SELECT_CANDIDATE_BUNDLE_FINGERPRINT_QUERY):
            return FakeResult(scalar=self.bundle_fingerprint)

        if upper.startswith("SELECT COUNT(*) FROM NOTIFICATION_PLANS"):
            return FakeResult(scalar=1)
        if upper.startswith("SELECT COUNT(*) FROM NOTIFICATION_RENDERS"):
            return FakeResult(scalar=self.render_count + len(self.notification_render_mutations))
        if upper.startswith("SELECT COUNT(*) FROM NOTIFICATION_DELIVERY_RECORDS"):
            return FakeResult(
                scalar=self.prior_dry_run_delivery_count
                + self.existing_successful_delivery_count
                + len(self.notification_delivery_records)
            )
        if upper.startswith("SELECT COUNT(*) FROM STATE_TRANSITIONS WHERE OBJECT_TYPE = 'NOTIFICATION_PLAN'"):
            return FakeResult(scalar=2 + len(self.notifier_state_transitions))
        if upper.startswith("SELECT COUNT(*) FROM EVENT_OUTBOX"):
            return FakeResult(scalar=self.prior_delivery_result_outbox_count + len(self.delivery_result_outbox))
        if upper.startswith("SELECT COUNT(*) FROM ANALYSES"):
            return FakeResult(scalar=1 + len(self.analysis_mutations))
        if upper.startswith("SELECT COUNT(*) FROM STATE_TRANSITIONS WHERE OBJECT_TYPE = 'ANALYSIS'"):
            return FakeResult(scalar=1 + len(self.policy_state_transition_mutations))
        if upper.startswith("SELECT COUNT(*) FROM JUDGE_OUTPUTS"):
            return FakeResult(scalar=1 + len(self.judge_output_mutations))
        if upper.startswith("SELECT COUNT(*) FROM CANDIDATE_GROUP_PROPOSALS"):
            return FakeResult(scalar=1 + len(self.candidate_current_analysis_mutations))

        if upper.startswith("UPDATE NOTIFICATION_PLANS"):
            self.plan_status_updates.append(dict(params))
            return FakeResult()
        if upper.startswith("INSERT INTO NOTIFICATION_DELIVERY_RECORDS"):
            delivery_record_id = uuid4()
            record = dict(params)
            record["notification_delivery_record_id"] = delivery_record_id
            self.notification_delivery_records.append(record)
            return FakeResult(scalar=delivery_record_id)
        if upper.startswith("INSERT INTO STATE_TRANSITIONS"):
            if params.get("object_type") == "notification_plan":
                self.notifier_state_transitions.append(dict(params))
                return FakeResult()
            if params.get("object_type") == "analysis":
                self.policy_state_transition_mutations.append(dict(params))
                return FakeResult()
        if upper.startswith("INSERT INTO EVENT_OUTBOX"):
            if not self.drop_delivery_result_outbox_insert:
                self.delivery_result_outbox.append(dict(params))
            return FakeResult()
        if upper.startswith("INSERT INTO NOTIFICATION_RENDERS"):
            self.notification_render_mutations.append(dict(params))
            return FakeResult()
        if upper.startswith("INSERT INTO ANALYSES"):
            self.analysis_mutations.append(dict(params))
            return FakeResult()
        if upper.startswith("INSERT INTO JUDGE_OUTPUTS"):
            self.judge_output_mutations.append(dict(params))
            return FakeResult()
        if upper.startswith("INSERT INTO CANDIDATE_EVIDENCE_BUNDLES"):
            self.candidate_bundle_mutations.append(dict(params))
            return FakeResult()
        if upper.startswith("UPDATE CANDIDATE_GROUP_PROPOSALS"):
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
        notification_plan_id = self.notification_plan_id if index == 0 else uuid4()
        analysis_id = self.analysis_id if index == 0 else uuid4()
        candidate_group_id = self.candidate_group_id if index == 0 else uuid4()
        judge_output_id = self.judge_output_id if index == 0 else uuid4()
        return {
            "plan_intent_event_id": self.plan_intent_event_id if index == 0 else uuid4(),
            "plan_intent_created_at": datetime(2026, 6, 3, 5, 6, 7, tzinfo=timezone.utc),
            "notification_plan_id": notification_plan_id,
            "analysis_id": analysis_id,
            "candidate_group_id": candidate_group_id,
            "delivery_decision": "send_now",
            "urgency_profile": "high",
            "target_chat_id": self.target_chat_id,
            "target_thread_id": None,
            "render_profile": "single_alert_v1",
            "dedupe_subject_key": "private-dedupe-subject-restricted-canary",
            "material_change_hash": "private-material-hash-restricted-canary",
            "send_after": None,
            "suppress_reason_code": None,
            "plan_status": "suppressed",
            "judge_output_id": judge_output_id,
            "judge_output_row_id": judge_output_id,
            "candidate_row_id": candidate_group_id,
            "notification_plan_count": 1,
            "analysis_count": 1,
            "judge_output_count": 1,
            "candidate_count": 1,
            "render_count": self.render_count,
            "prior_dry_run_delivery_count": self.prior_dry_run_delivery_count,
            "prior_delivery_result_outbox_count": self.prior_delivery_result_outbox_count,
            "prior_maintenance_noop_marker_count": self.prior_maintenance_noop_marker_count,
            "existing_successful_delivery_count": self.existing_successful_delivery_count,
        }

    def _render_row(self, index: int = 0) -> dict[str, Any]:
        return {
            "notification_plan_id": self.notification_plan_id,
            "message_text": FAKE_RENDERED_TEXT,
            "entities_json": [],
            "link_preview_options_json": {"is_disabled": True},
            "reply_markup_json": None,
            "disable_notification": False,
            "protect_content": False,
            "parse_strategy": "entities",
            "render_hash": f"private-render-hash-restricted-canary-{index}",
        }

    def _snapshot(self) -> dict[str, Any]:
        return {
            "plan_status_updates": list(self.plan_status_updates),
            "notification_delivery_records": list(self.notification_delivery_records),
            "notifier_state_transitions": list(self.notifier_state_transitions),
            "delivery_result_outbox": list(self.delivery_result_outbox),
            "notification_render_mutations": list(self.notification_render_mutations),
            "analysis_mutations": list(self.analysis_mutations),
            "judge_output_mutations": list(self.judge_output_mutations),
            "policy_state_transition_mutations": list(self.policy_state_transition_mutations),
            "candidate_bundle_mutations": list(self.candidate_bundle_mutations),
            "candidate_current_analysis_mutations": list(self.candidate_current_analysis_mutations),
        }

    def _restore(self, snapshot: dict[str, Any]) -> None:
        self.plan_status_updates = snapshot["plan_status_updates"]
        self.notification_delivery_records = snapshot["notification_delivery_records"]
        self.notifier_state_transitions = snapshot["notifier_state_transitions"]
        self.delivery_result_outbox = snapshot["delivery_result_outbox"]
        self.notification_render_mutations = snapshot["notification_render_mutations"]
        self.analysis_mutations = snapshot["analysis_mutations"]
        self.judge_output_mutations = snapshot["judge_output_mutations"]
        self.policy_state_transition_mutations = snapshot["policy_state_transition_mutations"]
        self.candidate_bundle_mutations = snapshot["candidate_bundle_mutations"]
        self.candidate_current_analysis_mutations = snapshot["candidate_current_analysis_mutations"]


class FakeTelegramClient:
    def __init__(self, *, exc: Exception | None = None, include_message_id: bool = True) -> None:
        self.exc = exc
        self.include_message_id = include_message_id
        self.send_calls: list[dict[str, Any]] = []
        self.edit_calls: list[dict[str, Any]] = []

    async def send_message(self, **kwargs: Any) -> dict[str, Any]:
        self.send_calls.append(dict(kwargs))
        if self.exc is not None:
            raise self.exc
        result: dict[str, Any] = {"chat": {"id": kwargs["chat_id"]}}
        if self.include_message_id:
            result["message_id"] = FAKE_MESSAGE_ID
        return {"ok": True, "result": result}

    async def edit_message_text(self, **kwargs: Any) -> dict[str, Any]:
        self.edit_calls.append(dict(kwargs))
        return {"ok": True, "result": {"message_id": FAKE_MESSAGE_ID, "chat": {"id": kwargs["chat_id"]}}}


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_notifier_telegram_restricted_send_canary_smoke"
    )


def _base_module():
    return importlib.import_module("scripts.ops.dedicated_vps_policy_engine_analysis_handoff_smoke")


def _telegram_client_module():
    return importlib.import_module("src.services.notifier_telegram.telegram_client")


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_reader(env: TrackingEnv):
    def reader(_path: str | Path) -> TrackingEnv:
        return env

    return reader


def _run(
    session: FakeSession,
    *,
    approve_db_read: bool = True,
    approve_db_write: bool = False,
    approve_telegram_send: bool = False,
    env: TrackingEnv | None = None,
    client: FakeTelegramClient | None = None,
):
    module = _module()
    env = env or TrackingEnv()
    return module.generate_report(
        approve_db_read=approve_db_read,
        approve_db_write=approve_db_write,
        approve_telegram_send=approve_telegram_send,
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=_runtime_reader(env),
        database_session_factory=lambda _database_url: session,
        telegram_client_factory=(lambda _token: client) if client is not None else None,
        forbidden_raw_values=[
            FAKE_DATABASE_URL,
            FAKE_BOT_TOKEN,
            FAKE_OPENAI_KEY,
            FAKE_REDIS_URL,
            FAKE_RUNTIME_PATH,
            FAKE_RENDERED_TEXT,
            FAKE_URL,
            FAKE_OPERATOR_CHAT_ID,
            str(FAKE_MESSAGE_ID),
        ],
    )


def test_script_exists_and_default_file_path_execution_is_safe_json() -> None:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0
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


@pytest.mark.parametrize(
    "approvals",
    [
        {"approve_db_read": False, "approve_db_write": True, "approve_telegram_send": False},
        {"approve_db_read": False, "approve_db_write": False, "approve_telegram_send": True},
        {"approve_db_read": True, "approve_db_write": True, "approve_telegram_send": False},
        {"approve_db_read": True, "approve_db_write": False, "approve_telegram_send": True},
        {"approve_db_read": False, "approve_db_write": True, "approve_telegram_send": True},
    ],
)
def test_partial_approval_combinations_block_before_env_token_db_write_or_transport(approvals: dict[str, bool]) -> None:
    module = _module()

    def raising_runtime_reader(_path: str | Path) -> dict[str, str]:
        raise AssertionError("runtime env should not be read for partial approvals")

    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        runtime_env_reader=raising_runtime_reader,
        database_session_factory=lambda _database_url: FakeSession(),
        **approvals,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_NOT_APPROVED
    assert result.report["runtime_env_read"] is False
    assert result.report["database_connected"] is False
    assert result.report["database_write_attempted"] is False
    assert result.report["telegram_bot_token_read_bucket"] == "zero"
    assert result.report["telegram_send_attempted"] is False


def test_db_read_preflight_success_uses_read_only_transaction_and_does_not_read_token() -> None:
    session = FakeSession()
    env = TrackingEnv()
    result = _run(session, env=env)

    assert result.exit_code == 0
    report = result.report
    assert report["contract_status"] == _module().STATUS_DB_PREFLIGHT_PASSED
    assert report["runtime_env_read"] is True
    assert report["database_configured"] is True
    assert report["database_connected"] is True
    assert report["read_only_transaction"] is True
    assert report["target_notification_plan_found_bucket"] == "one"
    assert report["target_notification_render_found_bucket"] == "one"
    assert report["target_prior_dry_run_delivery_found_bucket"] == "one"
    assert report["target_prior_delivery_result_outbox_found_bucket"] == "one"
    assert report["target_prior_maintenance_noop_marker_found_bucket"] == "one"
    assert report["target_analysis_found_bucket"] == "one"
    assert report["target_judge_output_found_bucket"] == "one"
    assert report["target_candidate_found_bucket"] == "one"
    assert report["target_chat_id_available_bucket"] == "one"
    assert report["target_chat_id_matches_runtime_bucket"] == "one"
    assert report["existing_successful_delivery_for_target_bucket"] == "zero"
    assert report["database_write_attempted"] is False
    assert report["telegram_bot_token_read_bucket"] == "zero"
    assert report["telegram_send_attempted"] is False
    assert report["telegram_edit_attempted"] is False
    assert report["redis_write_attempted"] is False
    assert report["openai_call_attempted"] is False
    assert report["checks_failed"] == []
    assert "TELEGRAM_BOT_TOKEN" not in env.accessed
    assert "OPENAI_API_KEY" not in env.accessed
    assert "REDIS_URL" not in env.accessed


@pytest.mark.parametrize(
    ("session", "expected_check"),
    [
        (FakeSession(render_count=0), "notification_render.count"),
        (FakeSession(prior_dry_run_delivery_count=0), "prior_dry_run_delivery.missing"),
        (FakeSession(prior_maintenance_noop_marker_count=0), "prior_maintenance_noop_marker.missing"),
        (FakeSession(target_chat_id=int(FAKE_OTHER_CHAT_ID)), "target_chat_id.mismatch"),
    ],
)
def test_db_read_preflight_blocks_invalid_existing_chain(session: FakeSession, expected_check: str) -> None:
    result = _run(session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_VALIDATION_FAILED
    assert expected_check in result.report["checks_failed"]
    assert result.report["database_write_attempted"] is False
    assert result.report["telegram_bot_token_read_bucket"] == "zero"
    assert result.report["telegram_send_attempted"] is False


def test_db_read_preflight_blocks_missing_prior_delivery_result_outbox() -> None:
    result = _run(FakeSession(prior_delivery_result_outbox_count=0))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_VALIDATION_FAILED
    assert "prior_delivery_result_outbox.missing" in result.report["checks_failed"]
    assert result.report["database_write_attempted"] is False


def test_db_read_preflight_blocks_existing_successful_delivery_for_target() -> None:
    result = _run(FakeSession(existing_successful_delivery_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EXISTING_SUCCESSFUL_DELIVERY
    assert result.report["existing_successful_delivery_for_target_bucket"] == "one"
    assert result.report["database_write_attempted"] is False


def test_fake_approved_send_success_writes_one_delivery_result_and_no_upstream_rows() -> None:
    session = FakeSession()
    env = TrackingEnv()
    client = FakeTelegramClient()
    result = _run(
        session,
        approve_db_read=True,
        approve_db_write=True,
        approve_telegram_send=True,
        env=env,
        client=client,
    )

    assert result.exit_code == 0
    report = result.report
    assert report["contract_status"] == _module().STATUS_APPROVED_SEND_PASSED
    assert report["database_write_attempted"] is True
    assert report["telegram_bot_token_read_bucket"] == "one"
    assert report["telegram_send_attempted"] is True
    assert report["telegram_send_result_bucket"] == "sent"
    assert report["telegram_edit_attempted"] is False
    assert report["notification_plan_rows_written_bucket"] == "zero"
    assert report["notification_render_rows_written_bucket"] == "zero"
    assert report["notification_delivery_rows_written_bucket"] == "one"
    assert report["notifier_state_transitions_written_bucket"] == "one"
    assert report["notification_delivery_result_outbox_written_bucket"] == "one"
    assert report["notification_delivery_status_bucket"] == "sent"
    assert report["telegram_message_id_available_bucket"] == "one"
    assert report["analysis_rows_written_bucket"] == "zero"
    assert report["judge_outputs_written_bucket"] == "zero"
    assert report["policy_state_transitions_written_bucket"] == "zero"
    assert report["candidate_bundle_mutation_attempted"] is False
    assert report["candidate_current_analysis_mutation_attempted"] is False
    assert report["redis_connected"] is False
    assert report["redis_write_attempted"] is False
    assert report["openai_call_attempted"] is False
    assert report["openai_key_read_bucket"] == "zero"
    assert report["raw_values_emitted"] is False
    assert report["checks_failed"] == []
    assert len(client.send_calls) == 1
    assert client.edit_calls == []
    assert client.send_calls[0]["chat_id"] == int(FAKE_OPERATOR_CHAT_ID)
    assert len(session.plan_status_updates) == 1
    assert session.plan_status_updates[0]["status"] == "sent"
    assert len(session.notification_delivery_records) == 1
    assert len(session.notifier_state_transitions) >= 1
    assert len(session.delivery_result_outbox) == 1
    assert session.notification_render_mutations == []
    assert session.analysis_mutations == []
    assert "TELEGRAM_BOT_TOKEN" in env.accessed
    assert "OPENAI_API_KEY" not in env.accessed
    assert "REDIS_URL" not in env.accessed


def test_real_telegram_client_constructor_uses_notifier_runtime_config(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _module()
    constructor_kwargs: dict[str, Any] = {}

    class ConstructorTrackingTelegramClient:
        def __init__(self, *, base_url: str, bot_token: str, timeout_sec: float) -> None:
            constructor_kwargs.update(
                {
                    "base_url": base_url,
                    "bot_token": bot_token,
                    "timeout_sec": timeout_sec,
                }
            )
            self.send_calls: list[dict[str, Any]] = []
            self.edit_calls: list[dict[str, Any]] = []

        async def send_message(self, **kwargs: Any) -> dict[str, Any]:
            self.send_calls.append(dict(kwargs))
            return {
                "ok": True,
                "result": {"message_id": FAKE_MESSAGE_ID, "chat": {"id": kwargs["chat_id"]}},
            }

        async def edit_message_text(self, **kwargs: Any) -> dict[str, Any]:
            self.edit_calls.append(dict(kwargs))
            raise AssertionError("restricted send canary must not call editMessageText")

    monkeypatch.setattr(module, "TelegramBotClient", ConstructorTrackingTelegramClient)
    session = FakeSession()
    result = _run(
        session,
        approve_db_read=True,
        approve_db_write=True,
        approve_telegram_send=True,
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == module.STATUS_APPROVED_SEND_PASSED
    assert constructor_kwargs == {
        "base_url": "https://api.telegram.org",
        "bot_token": FAKE_BOT_TOKEN,
        "timeout_sec": 10.0,
    }
    assert len(session.notification_delivery_records) == 1
    assert session.plan_status_updates[0]["status"] == "sent"


def test_fake_telegram_retryable_failure_blocks_without_db_write() -> None:
    exc = _telegram_client_module().TelegramTransportRetryableError(
        "too many requests private details",
        error_code="telegram_rate_limited",
        retry_after_seconds=42,
    )
    session = FakeSession()
    client = FakeTelegramClient(exc=exc)
    result = _run(
        session,
        approve_db_read=True,
        approve_db_write=True,
        approve_telegram_send=True,
        client=client,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_TELEGRAM_RETRYABLE
    assert result.report["telegram_send_attempted"] is True
    assert result.report["database_write_attempted"] is False
    assert session.plan_status_updates == []
    assert session.notification_delivery_records == []
    assert session.notifier_state_transitions == []
    assert session.delivery_result_outbox == []
    assert len(client.send_calls) == 1
    assert client.edit_calls == []


def test_fake_telegram_terminal_failure_blocks_without_db_write() -> None:
    exc = _telegram_client_module().TelegramTransportTerminalError(
        "chat not found private details",
        error_code="telegram_invalid_chat",
    )
    session = FakeSession()
    client = FakeTelegramClient(exc=exc)
    result = _run(
        session,
        approve_db_read=True,
        approve_db_write=True,
        approve_telegram_send=True,
        client=client,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_TELEGRAM_TERMINAL
    assert result.report["telegram_send_attempted"] is True
    assert result.report["database_write_attempted"] is False
    assert session.plan_status_updates == []
    assert session.notification_delivery_records == []
    assert session.notifier_state_transitions == []
    assert session.delivery_result_outbox == []
    assert len(client.send_calls) == 1
    assert client.edit_calls == []


def test_expected_effect_failure_rolls_back_db_writes_after_send() -> None:
    session = FakeSession(drop_delivery_result_outbox_insert=True)
    client = FakeTelegramClient()
    result = _run(
        session,
        approve_db_read=True,
        approve_db_write=True,
        approve_telegram_send=True,
        client=client,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_WRITE_FAILED
    assert result.report["database_write_attempted"] is True
    assert result.report["telegram_send_attempted"] is True
    assert result.report["notification_delivery_result_outbox_written_bucket"] == "zero"
    assert session.rolled_back is True
    assert session.plan_status_updates == []
    assert session.notification_delivery_records == []
    assert session.notifier_state_transitions == []
    assert session.delivery_result_outbox == []
    assert len(client.send_calls) == 1


def test_report_does_not_emit_raw_runtime_db_token_chat_message_id_or_render_text() -> None:
    result = _run(
        FakeSession(),
        approve_db_read=True,
        approve_db_write=True,
        approve_telegram_send=True,
        client=FakeTelegramClient(),
    )

    rendered = json.dumps(result.report, sort_keys=True)
    forbidden = [
        FAKE_DATABASE_URL,
        FAKE_DATABASE_CREDENTIAL,
        FAKE_BOT_TOKEN,
        FAKE_OPENAI_KEY,
        FAKE_REDIS_URL,
        FAKE_RUNTIME_PATH,
        FAKE_OPERATOR_CHAT_ID,
        str(FAKE_MESSAGE_ID),
        FAKE_RENDERED_TEXT,
        FAKE_URL,
    ]
    assert result.report["raw_values_emitted"] is False
    for value in forbidden:
        assert value not in rendered


def test_no_redis_openai_edit_or_closed_predecessor_execution_flags_are_set() -> None:
    result = _run(
        FakeSession(),
        approve_db_read=True,
        approve_db_write=True,
        approve_telegram_send=True,
        client=FakeTelegramClient(),
    )
    report = result.report

    assert report["redis_connected"] is False
    assert report["redis_write_attempted"] is False
    assert report["openai_call_attempted"] is False
    assert report["openai_key_read_bucket"] == "zero"
    assert report["telegram_edit_attempted"] is False
    assert report["closed_predecessor_smoke_execution_attempted"] is False
