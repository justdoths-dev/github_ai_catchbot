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
    / "dedicated_vps_notifier_telegram_plan_intent_dry_run_smoke.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-notifier-plan"
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
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-notifier-plan-runtime.env"
FAKE_OPERATOR_CHAT_ID = "123456789"
FAKE_BOT_TOKEN = "private" + "-telegram" + "-bot" + "-notifier-plan"
FAKE_DIRECT_KEY = "direct" + "-openai" + "-secret" + "-notifier-plan"
FAKE_REDIS_URL = "redis" + ":/" + "/private-notifier-plan"
FAKE_URL = "https" + ":" + "/" + "/" + "example.invalid/private/notifier-plan"
FAKE_SOURCE_TEXT = "private source text notifier plan must not leak"
FAKE_MESSAGE_BODY = "private rendered telegram body must not leak"


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
        include_plan_intent: bool = True,
        analysis_delivery_decision: str = "send_now",
        payload_target_chat_id: int | None = int(FAKE_OPERATOR_CHAT_ID),
        existing_notification_plan_count: int = 0,
        existing_notification_render_count: int = 0,
        existing_notification_delivery_count: int = 0,
        existing_delivery_result_outbox_count: int = 0,
        implicit_read_transaction: bool = False,
        fail_begin_if_transaction_open: bool = False,
        drop_delivery_result_outbox_insert: bool = False,
    ) -> None:
        self.trigger_event_id = uuid4()
        self.notification_plan_id = uuid4()
        self.analysis_id = uuid4()
        self.judge_output_id = uuid4()
        self.candidate_group_id = uuid4()
        self.source_message_id = uuid4()
        self.artifact_id = uuid4()
        self.delivery_record_id = uuid4()
        self.bundle_fingerprint = "candidate-bundle-fingerprint-private-notifier"
        self.current_analysis_id = self.analysis_id
        self.include_plan_intent = include_plan_intent
        self.analysis_delivery_decision = analysis_delivery_decision
        self.payload_target_chat_id = payload_target_chat_id
        self.existing_notification_plan_count = existing_notification_plan_count
        self.existing_notification_render_count = existing_notification_render_count
        self.existing_notification_delivery_count = existing_notification_delivery_count
        self.existing_delivery_result_outbox_count = existing_delivery_result_outbox_count
        self.implicit_read_transaction = implicit_read_transaction
        self.fail_begin_if_transaction_open = fail_begin_if_transaction_open
        self.drop_delivery_result_outbox_insert = drop_delivery_result_outbox_insert
        self.transaction_open = False
        self.explicit_transaction_open = False
        self.begin_count = 0
        self.rollback_count = 0
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.statements: list[str] = []
        self.notification_plans: list[dict[str, Any]] = []
        self.notification_renders: list[dict[str, Any]] = []
        self.notification_deliveries: list[dict[str, Any]] = []
        self.notifier_state_transitions: list[dict[str, Any]] = []
        self.delivery_result_outbox: list[dict[str, Any]] = []
        self.analysis_mutations: list[dict[str, Any]] = []
        self.policy_state_transition_mutations: list[dict[str, Any]] = []
        self.plan_intent_outbox_mutations: list[dict[str, Any]] = []
        self.judge_output_mutations: list[dict[str, Any]] = []
        self.candidate_bundle_mutations: list[dict[str, Any]] = []
        self.candidate_current_analysis_mutations: list[dict[str, Any]] = []

        self.payload = {
            "notification_plan_id": str(self.notification_plan_id),
            "analysis_id": str(self.analysis_id),
            "candidate_group_id": str(self.candidate_group_id),
            "delivery_decision": analysis_delivery_decision,
            "urgency_profile": "high",
            "target_chat_id": payload_target_chat_id,
            "target_thread_id": None,
            "render_profile": "single_alert_v1",
            "dedupe_subject_key": str(self.candidate_group_id),
            "material_change_hash": "material-hash-private-notifier",
            "send_after": None,
            "suppress_reason_code": None,
        }
        self.target_row = {
            "trigger_event_id": self.trigger_event_id,
            "plan_intent_created_at": datetime(2026, 6, 3, 3, 4, 5, tzinfo=timezone.utc),
            "aggregate_analysis_id": self.analysis_id,
            "payload_json": self.payload,
            "analysis_id": self.analysis_id,
            "judge_output_id": self.judge_output_id,
            "candidate_group_id": self.candidate_group_id,
            "analysis_delivery_decision": analysis_delivery_decision,
            "judge_output_row_id": self.judge_output_id,
            "candidate_row_id": self.candidate_group_id,
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
        first = normalized.split()[0].upper()
        self.statements.append(normalized)
        module = _module()
        base_module = _base_module()
        if self.implicit_read_transaction and not self.explicit_transaction_open and first == "SELECT":
            self.transaction_open = True

        if normalized == _normalize(base_module.SET_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult()
        if normalized == _normalize(base_module.SHOW_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult(scalar="on")
        if normalized == _normalize(base_module.SELECT_ONE_QUERY):
            return FakeResult(scalar=1)
        if normalized == _normalize(module.SELECT_RECENT_PLAN_INTENTS_QUERY):
            return FakeResult(rows=[self.target_row] if self.include_plan_intent else [])

        if "SELECT event_id, event_type, payload_json FROM event_outbox" in normalized:
            row = None
            if self.include_plan_intent and str(params.get("event_id")) == str(self.trigger_event_id):
                row = {
                    "event_id": self.trigger_event_id,
                    "event_type": "notification.plan.created.v1",
                    "payload_json": self.payload,
                }
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT analysis_id, candidate_group_id, judge_output_id"):
            if str(params.get("analysis_id")) != str(self.analysis_id):
                return FakeResult(rows=[])
            return FakeResult(
                rows=[
                    {
                        "analysis_id": self.analysis_id,
                        "candidate_group_id": self.candidate_group_id,
                        "judge_output_id": self.judge_output_id,
                        "verdict": "inspect_now",
                        "delivery_decision": self.analysis_delivery_decision,
                        "reason_codes_json": ["repo_has_clear_scope"],
                        "evidence_limitations_ko": "private limitation",
                        "recommended_action_ko": "private action",
                        "freshness_note_ko": "fresh",
                        "created_at": datetime(2026, 6, 3, 3, 4, 6, tzinfo=timezone.utc),
                    }
                ]
            )
        if normalized.startswith("SELECT judge_output_id, payload_json, model_confidence_band"):
            if str(params.get("judge_output_id")) != str(self.judge_output_id):
                return FakeResult(rows=[])
            return FakeResult(
                rows=[
                    {
                        "judge_output_id": self.judge_output_id,
                        "payload_json": {
                            "headline": "Useful repo",
                            "summary_one_line_ko": FAKE_MESSAGE_BODY,
                            "skeptical_take_ko": "check maintenance",
                        },
                        "model_confidence_band": "medium",
                    }
                ]
            )
        if normalized.startswith("SELECT cgp.candidate_group_id, cgp.source_message_id"):
            if str(params.get("candidate_group_id")) != str(self.candidate_group_id):
                return FakeResult(rows=[])
            return FakeResult(
                rows=[
                    {
                        "candidate_group_id": self.candidate_group_id,
                        "source_message_id": self.source_message_id,
                        "current_primary_artifact_id": self.artifact_id,
                        "primary_artifact_type": "github_repo",
                        "primary_canonical_url": FAKE_URL,
                        "primary_canonical_id": "github.com/private/notifier-plan",
                        "source_message_link": FAKE_URL + "/source",
                        "source_text_surface": FAKE_SOURCE_TEXT,
                    }
                ]
            )
        if normalized.startswith("SELECT notification_plan_id, analysis_id, candidate_group_id, target_chat_id"):
            row = self._plan_row(str(params.get("notification_plan_id")))
            return FakeResult(rows=[row] if row else [])
        if "FROM notification_plans WHERE analysis_id = CAST(:analysis_id AS uuid)" in normalized:
            row = self._material_plan_row(params)
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT p.notification_plan_id") and "FROM notification_delivery_records d" in normalized:
            return FakeResult(rows=[])
        if normalized.startswith("SELECT 1 FROM notification_delivery_records"):
            return FakeResult(rows=[])
        if normalized == _normalize(module.COUNT_ANALYSIS_ROWS_FOR_TARGET_QUERY):
            return FakeResult(scalar=1 + len(self.analysis_mutations))
        if normalized == _normalize(module.COUNT_POLICY_STATE_TRANSITIONS_FOR_TARGET_QUERY):
            return FakeResult(scalar=1 + len(self.policy_state_transition_mutations))
        if normalized == _normalize(module.COUNT_PLAN_INTENT_OUTBOX_FOR_TARGET_QUERY):
            return FakeResult(scalar=1 + len(self.plan_intent_outbox_mutations))
        if normalized == _normalize(module.COUNT_JUDGE_OUTPUTS_FOR_TARGET_QUERY):
            return FakeResult(scalar=1 + len(self.judge_output_mutations))
        if normalized == _normalize(module.COUNT_NOTIFICATION_PLAN_ROWS_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.existing_notification_plan_count + len(self.notification_plans))
        if normalized == _normalize(module.COUNT_NOTIFICATION_RENDER_ROWS_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.existing_notification_render_count + len(self.notification_renders))
        if normalized == _normalize(module.COUNT_NOTIFICATION_DELIVERY_ROWS_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.existing_notification_delivery_count + len(self.notification_deliveries))
        if normalized == _normalize(module.COUNT_NOTIFIER_STATE_TRANSITIONS_FOR_TARGET_QUERY):
            return FakeResult(scalar=len(self.notifier_state_transitions))
        if normalized == _normalize(module.COUNT_DELIVERY_RESULT_OUTBOX_FOR_TARGET_QUERY):
            return FakeResult(scalar=self.existing_delivery_result_outbox_count + len(self.delivery_result_outbox))
        if normalized == _normalize(module.SELECT_CANDIDATE_CURRENT_ANALYSIS_ID_QUERY):
            return FakeResult(scalar=self.current_analysis_id)
        if normalized == _normalize(module.SELECT_CANDIDATE_BUNDLE_FINGERPRINT_QUERY):
            return FakeResult(scalar=self.bundle_fingerprint)
        if normalized == _normalize(module.SELECT_LATEST_DELIVERY_RESULT_FOR_TARGET_QUERY):
            if not self.notification_deliveries:
                return FakeResult(rows=[])
            record = self.notification_deliveries[-1]
            return FakeResult(
                rows=[
                    {
                        "delivery_status": record["delivery_status"],
                        "transport_error_code": record["transport_error_code"],
                    }
                ]
            )
        if normalized.startswith("SELECT count(*) FROM notification_delivery_records"):
            return FakeResult(scalar=len(self.notification_deliveries))

        if normalized.startswith("INSERT INTO notification_plans"):
            self.notification_plans.append(dict(params))
            return FakeResult(scalar=self.notification_plan_id)
        if normalized.startswith("INSERT INTO notification_renders"):
            self.notification_renders.append(dict(params))
            return FakeResult(scalar=uuid4())
        if normalized.startswith("UPDATE notification_plans"):
            for plan in self.notification_plans:
                if str(plan["notification_plan_id"]) == str(params["notification_plan_id"]):
                    plan["status"] = params["status"]
                    plan["send_after"] = params.get("send_after") or plan.get("send_after")
            return FakeResult()
        if normalized.startswith("INSERT INTO notification_delivery_records"):
            record = {
                "notification_delivery_record_id": self.delivery_record_id,
                "notification_plan_id": UUID(str(params["notification_plan_id"])),
                "telegram_chat_id": params["telegram_chat_id"],
                "telegram_message_id": params["telegram_message_id"],
                "delivery_status": params["delivery_status"],
                "attempt_count": params["attempt_count"],
                "transport_error_code": params["transport_error_code"],
                "transport_error_class": params["transport_error_class"],
                "telegram_response_json": json.loads(params["telegram_response_json"]),
                "created_at": datetime.now(timezone.utc),
            }
            self.notification_deliveries.append(record)
            return FakeResult(scalar=self.delivery_record_id)
        if normalized.startswith("INSERT INTO state_transitions"):
            if params.get("object_type") == "notification_plan":
                self.notifier_state_transitions.append(dict(params))
            elif params.get("object_type") == "analysis":
                self.policy_state_transition_mutations.append(dict(params))
            return FakeResult()
        if normalized.startswith("INSERT INTO event_outbox") and "notification.delivery.result.v1" in normalized:
            if not self.drop_delivery_result_outbox_insert:
                self.delivery_result_outbox.append(
                    {
                        "notification_plan_id": UUID(str(params["notification_plan_id"])),
                        "payload_json": json.loads(params["payload_json"]),
                    }
                )
            return FakeResult()

        if first in {"INSERT", "UPDATE", "DELETE"}:
            upper = normalized.upper()
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
            if "NOTIFICATION.PLAN.CREATED.V1" in upper:
                self.plan_intent_outbox_mutations.append(dict(params))
                return FakeResult()

        raise AssertionError(f"unexpected SQL: {statement}")

    async def rollback(self) -> None:
        self.rolled_back = True
        self.rollback_count += 1
        self.transaction_open = False

    async def close(self) -> None:
        self.closed = True

    def _plan_row(self, notification_plan_id: str) -> dict[str, Any] | None:
        for plan in self.notification_plans:
            if str(plan["notification_plan_id"]) == notification_plan_id:
                return {
                    "notification_plan_id": UUID(str(plan["notification_plan_id"])),
                    "analysis_id": UUID(str(plan["analysis_id"])),
                    "candidate_group_id": UUID(str(plan["candidate_group_id"])),
                    "target_chat_id": plan["target_chat_id"],
                    "target_thread_id": plan["target_thread_id"],
                    "render_profile": plan["render_profile"],
                    "dedupe_subject_key": plan["dedupe_subject_key"],
                    "material_change_hash": plan["material_change_hash"],
                    "send_after": plan.get("send_after"),
                    "status": plan.get("status", "planned"),
                }
        if self.existing_notification_plan_count and notification_plan_id == str(self.notification_plan_id):
            return {
                "notification_plan_id": self.notification_plan_id,
                "analysis_id": self.analysis_id,
                "candidate_group_id": self.candidate_group_id,
                "target_chat_id": self.payload_target_chat_id,
                "target_thread_id": None,
                "render_profile": "single_alert_v1",
                "dedupe_subject_key": str(self.candidate_group_id),
                "material_change_hash": "material-hash-private-notifier",
                "send_after": None,
                "status": "planned",
            }
        return None

    def _material_plan_row(self, params: dict[str, Any]) -> dict[str, Any] | None:
        for plan in self.notification_plans:
            if (
                str(plan["analysis_id"]) == str(params.get("analysis_id"))
                and int(plan["target_chat_id"]) == int(params.get("target_chat_id"))
                and plan["material_change_hash"] == params.get("material_change_hash")
            ):
                return self._plan_row(str(plan["notification_plan_id"]))
        return self._plan_row(str(self.notification_plan_id)) if self.existing_notification_plan_count else None

    def _snapshot_writes(self) -> dict[str, Any]:
        return {
            "notification_plans": list(self.notification_plans),
            "notification_renders": list(self.notification_renders),
            "notification_deliveries": list(self.notification_deliveries),
            "notifier_state_transitions": list(self.notifier_state_transitions),
            "delivery_result_outbox": list(self.delivery_result_outbox),
            "analysis_mutations": list(self.analysis_mutations),
            "policy_state_transition_mutations": list(self.policy_state_transition_mutations),
            "plan_intent_outbox_mutations": list(self.plan_intent_outbox_mutations),
            "judge_output_mutations": list(self.judge_output_mutations),
            "candidate_bundle_mutations": list(self.candidate_bundle_mutations),
            "candidate_current_analysis_mutations": list(self.candidate_current_analysis_mutations),
        }

    def _restore_writes(self, snapshot: dict[str, Any]) -> None:
        self.notification_plans = snapshot["notification_plans"]
        self.notification_renders = snapshot["notification_renders"]
        self.notification_deliveries = snapshot["notification_deliveries"]
        self.notifier_state_transitions = snapshot["notifier_state_transitions"]
        self.delivery_result_outbox = snapshot["delivery_result_outbox"]
        self.analysis_mutations = snapshot["analysis_mutations"]
        self.policy_state_transition_mutations = snapshot["policy_state_transition_mutations"]
        self.plan_intent_outbox_mutations = snapshot["plan_intent_outbox_mutations"]
        self.judge_output_mutations = snapshot["judge_output_mutations"]
        self.candidate_bundle_mutations = snapshot["candidate_bundle_mutations"]
        self.candidate_current_analysis_mutations = snapshot["candidate_current_analysis_mutations"]


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_notifier_telegram_plan_intent_dry_run_smoke"
    )


def _base_module():
    return importlib.import_module("scripts.ops.dedicated_vps_policy_engine_analysis_handoff_smoke")


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_reader(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "TELEGRAM_OPERATOR_CHAT_ID": FAKE_OPERATOR_CHAT_ID,
        "TELEGRAM_BOT_TOKEN": FAKE_BOT_TOKEN,
        "OPENAI_API_KEY": FAKE_DIRECT_KEY,
        "REDIS_URL": FAKE_REDIS_URL,
    }


def _runtime_reader_without_chat(_path: str | Path) -> dict[str, str]:
    return {"DATABASE_URL": FAKE_DATABASE_URL, "TELEGRAM_BOT_TOKEN": FAKE_BOT_TOKEN}


def _raising_runtime_reader(_path: str | Path) -> dict[str, str]:
    raise AssertionError("runtime env should not be read")


def _raising_database_factory(_database_url: str) -> Any:
    raise AssertionError("database should not be opened")


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
            FAKE_BOT_TOKEN,
            FAKE_DIRECT_KEY,
            FAKE_REDIS_URL,
            FAKE_URL,
            FAKE_SOURCE_TEXT,
            FAKE_MESSAGE_BODY,
            str(session.trigger_event_id),
            str(session.notification_plan_id),
            str(session.analysis_id),
            str(session.judge_output_id),
            str(session.candidate_group_id),
            *forbidden_raw_values,
        ),
    )
    return result, session


def _assert_no_writes(session: FakeSession) -> None:
    assert session.committed is False
    assert session.rolled_back is True
    assert session.closed is True
    assert session.notification_plans == []
    assert session.notification_renders == []
    assert session.notification_deliveries == []
    assert session.notifier_state_transitions == []
    assert session.delivery_result_outbox == []
    assert session.analysis_mutations == []
    assert session.policy_state_transition_mutations == []
    assert session.plan_intent_outbox_mutations == []
    assert session.judge_output_mutations == []
    assert session.candidate_bundle_mutations == []
    assert session.candidate_current_analysis_mutations == []
    for statement in session.statements:
        first = statement.split()[0].upper()
        assert first in {"SET", "SHOW", "SELECT"}


def _assert_no_forbidden_upstream_or_transport(report: dict[str, Any]) -> None:
    assert report["analysis_rows_written_bucket"] == "zero"
    assert report["policy_state_transitions_written_bucket"] == "zero"
    assert report["notification_plan_intent_outbox_written_bucket"] == "zero"
    assert report["judge_outputs_written_bucket"] == "zero"
    assert report["candidate_bundle_mutation_attempted"] is False
    assert report["candidate_current_analysis_mutation_attempted"] is False
    assert report["policy_engine_started"] is False
    assert report["telegram_send_attempted"] is False
    assert report["telegram_edit_attempted"] is False
    assert report["redis_connected"] is False
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


def test_default_mode_does_not_read_env_db_redis_key_or_telegram() -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = FAKE_DATABASE_URL
    env["OPENAI_API_KEY"] = FAKE_DIRECT_KEY
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
    assert report["database_connected"] is False
    assert report["database_write_attempted"] is False
    assert report["redis_connected"] is False
    assert report["openai_call_attempted"] is False
    assert report["openai_key_read_bucket"] == "zero"
    assert report["telegram_send_attempted"] is False
    assert report["telegram_edit_attempted"] is False
    assert report["target_plan_intent_found_bucket"] == "zero"
    assert report["raw_values_emitted"] is False
    assert report["checks_failed"] == []
    assert FAKE_DATABASE_URL not in completed.stdout
    assert FAKE_DIRECT_KEY not in completed.stdout
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


def test_db_read_preflight_finds_clean_plan_intent_and_writes_nothing() -> None:
    result, session = _run_report()

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_DB_PREFLIGHT_PASSED
    assert report["runtime_env_read"] is True
    assert report["database_configured"] is True
    assert report["database_connected"] is True
    assert report["read_only_transaction"] is True
    assert report["database_write_attempted"] is False
    assert report["target_plan_intent_found_bucket"] == "one"
    assert report["target_analysis_found_bucket"] == "one"
    assert report["target_judge_output_found_bucket"] == "one"
    assert report["target_candidate_found_bucket"] == "one"
    assert report["target_chat_id_available_bucket"] == "one"
    assert report["payload_identity_valid_bucket"] == "one"
    assert report["existing_notification_plan_rows_for_target_bucket"] == "zero"
    assert report["existing_notification_render_rows_for_target_bucket"] == "zero"
    assert report["existing_notification_delivery_rows_for_target_bucket"] == "zero"
    assert report["existing_notification_delivery_result_outbox_for_target_bucket"] == "zero"
    assert report["notifier_started"] is False
    assert report["raw_values_emitted"] is False
    _assert_no_forbidden_upstream_or_transport(report)
    _assert_no_raw_values(
        report,
        FAKE_RUNTIME_PATH,
        FAKE_DATABASE_URL,
        FAKE_OPERATOR_CHAT_ID,
        FAKE_BOT_TOKEN,
        FAKE_DIRECT_KEY,
        FAKE_REDIS_URL,
        FAKE_URL,
        FAKE_SOURCE_TEXT,
        FAKE_MESSAGE_BODY,
        str(session.trigger_event_id),
        str(session.notification_plan_id),
        str(session.analysis_id),
        str(session.judge_output_id),
        str(session.candidate_group_id),
    )
    _assert_no_writes(session)


def test_db_read_preflight_rejects_missing_plan_intent() -> None:
    result, session = _run_report(session=FakeSession(include_plan_intent=False))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NO_CLEAN_PLAN_INTENT
    assert result.report["checks_failed"] == ["target.notification_plan_intent_missing"]
    assert result.report["target_plan_intent_found_bucket"] == "zero"
    assert result.report["database_write_attempted"] is False
    _assert_no_writes(session)


def test_db_read_preflight_rejects_suppress_analysis() -> None:
    result, session = _run_report(session=FakeSession(analysis_delivery_decision="suppress"))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_SUPPRESS_ANALYSIS
    assert result.report["checks_failed"] == ["analysis.delivery_decision_suppress"]
    assert result.report["target_analysis_found_bucket"] == "one"
    assert result.report["database_write_attempted"] is False
    _assert_no_writes(session)


def test_db_read_preflight_rejects_missing_target_chat() -> None:
    result, session = _run_report(runtime_reader=_runtime_reader_without_chat)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_VALIDATION_FAILED
    assert result.report["checks_failed"] == ["config.target_chat_id_unavailable"]
    assert result.report["target_chat_id_available_bucket"] == "zero"
    assert result.report["database_write_attempted"] is False
    _assert_no_writes(session)


def test_db_read_preflight_rejects_existing_notification_plan_row() -> None:
    result, session = _run_report(session=FakeSession(existing_notification_plan_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EXISTING_NOTIFIER_ROW
    assert result.report["checks_failed"] == ["notification_plans.existing"]
    assert result.report["existing_notification_plan_rows_for_target_bucket"] == "one"
    _assert_no_writes(session)


def test_db_read_preflight_rejects_existing_delivery_result_outbox() -> None:
    result, session = _run_report(session=FakeSession(existing_delivery_result_outbox_count=1))

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EXISTING_NOTIFIER_ROW
    assert result.report["checks_failed"] == ["event_outbox.notification_delivery_result_existing"]
    assert result.report["existing_notification_delivery_result_outbox_for_target_bucket"] == "one"
    _assert_no_writes(session)


def test_db_write_fake_success_concretizes_plan_render_and_dry_run_delivery() -> None:
    result, session = _run_report(approve_db_write=True)

    report = result.report
    assert result.exit_code == 0
    assert report["contract_status"] == _module().STATUS_DB_WRITE_PASSED
    assert report["database_write_attempted"] is True
    assert report["notifier_started"] is True
    assert report["notification_plan_rows_written_bucket"] == "one"
    assert report["notification_render_rows_written_bucket"] == "one"
    assert report["notification_delivery_rows_written_bucket"] == "one"
    assert report["notifier_state_transitions_written_bucket"] == "multiple"
    assert report["notification_delivery_result_outbox_written_bucket"] == "one"
    assert report["notification_delivery_status_bucket"] == "suppressed"
    assert report["notification_delivery_reason_bucket"] == "dry_run_skip_transport"
    assert len(session.notification_plans) == 1
    assert len(session.notification_renders) == 1
    assert len(session.notification_deliveries) == 1
    assert len(session.notifier_state_transitions) >= 1
    assert len(session.delivery_result_outbox) == 1
    assert session.notification_deliveries[0]["delivery_status"] == "suppressed"
    assert session.notification_deliveries[0]["transport_error_code"] == "dry_run_skip_transport"
    assert session.notification_deliveries[0]["telegram_response_json"]["dry_run"] is True
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
    assert session.rollback_count >= 1
    assert session.begin_count == 1
    assert session.committed is True
    assert session.closed is True
    assert len(session.notification_plans) == 1
    assert len(session.notification_renders) == 1
    assert len(session.notification_deliveries) == 1


def test_db_write_expected_effect_failure_rolls_back() -> None:
    result, session = _run_report(
        approve_db_write=True,
        session=FakeSession(drop_delivery_result_outbox_insert=True),
    )

    report = result.report
    assert result.exit_code == 1
    assert report["contract_status"] == _module().STATUS_WRITE_FAILED
    assert report["checks_failed"] == ["db_write.expected_effects"]
    assert report["database_write_attempted"] is True
    assert report["notification_plan_rows_written_bucket"] == "one"
    assert report["notification_render_rows_written_bucket"] == "one"
    assert report["notification_delivery_rows_written_bucket"] == "one"
    assert report["notification_delivery_result_outbox_written_bucket"] == "zero"
    assert session.committed is False
    assert session.rolled_back is True
    assert session.closed is True
    assert session.notification_plans == []
    assert session.notification_renders == []
    assert session.notification_deliveries == []
    assert session.delivery_result_outbox == []


def test_db_write_fake_success_does_not_write_analysis_policy_or_judge_rows() -> None:
    result, session = _run_report(approve_db_write=True)

    report = result.report
    _assert_no_forbidden_upstream_or_transport(report)
    assert session.analysis_mutations == []
    assert session.policy_state_transition_mutations == []
    assert session.plan_intent_outbox_mutations == []
    assert session.judge_output_mutations == []
    assert session.candidate_bundle_mutations == []
    assert session.candidate_current_analysis_mutations == []


def test_db_write_fake_success_has_no_openai_redis_telegram_or_raw_output() -> None:
    result, session = _run_report(approve_db_write=True)

    report = result.report
    assert report["raw_values_emitted"] is False
    _assert_no_forbidden_upstream_or_transport(report)
    _assert_no_raw_values(
        report,
        FAKE_RUNTIME_PATH,
        FAKE_DATABASE_URL,
        FAKE_OPERATOR_CHAT_ID,
        FAKE_BOT_TOKEN,
        FAKE_DIRECT_KEY,
        FAKE_REDIS_URL,
        FAKE_URL,
        FAKE_SOURCE_TEXT,
        FAKE_MESSAGE_BODY,
        str(session.trigger_event_id),
        str(session.notification_plan_id),
        str(session.analysis_id),
        str(session.judge_output_id),
        str(session.candidate_group_id),
    )
