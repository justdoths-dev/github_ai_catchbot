from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_joined_channel_collector_bounded_startup_ingest_gate.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-for-bounded-startup@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = "redis://:unit-redis-secret-bounded-startup@127.0.0.1:6379/0"
FAKE_DATABASE_PASSWORD = "unit-db-password-for-bounded-startup"
FAKE_TELEGRAM_SECRET = "unit-telegram-api-hash-bounded-startup"
RAW_CHAT_ID = 9876543210123
RAW_SOURCE_VALUE = "SensitiveBoundedStartupChannel"
RAW_USERNAME = "SensitiveBoundedStartupUsername"
RAW_TITLE = "Sensitive Bounded Startup Channel Title"
RAW_INVITE_LINK = "https://t.me/+sensitiveInviteLinkForBoundedStartup"
RAW_TDLIB_PAYLOAD_VALUE = "unit-raw-tdlib-payload-value-bounded-startup"
RAW_EXTRA = "raw-extra-bounded-startup"
RAW_TEMP_PATH = "/tmp/sensitive-bounded-startup-path"
RAW_PHONE = "+15555550124"


class FakeResult:
    def __init__(
        self,
        *,
        scalar: Any = None,
        rows: list[Any] | None = None,
    ) -> None:
        self._scalar = scalar
        self._rows = rows or []

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def fetchall(self) -> list[Any]:
        return self._rows


class FakeTransaction:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


class FakeDatabaseConnection:
    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        table_available: dict[str, bool] | None = None,
        fail_select_1: bool = False,
    ) -> None:
        self.rows = rows or []
        self.table_available = table_available or {}
        self.fail_select_1 = fail_select_1
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.closed = False
        self.transaction: FakeTransaction | None = None

    def begin(self) -> FakeTransaction:
        self.transaction = FakeTransaction()
        return self.transaction

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(statement)
        self.statements.append(normalized)
        self.params.append(dict(params))
        module = _module()

        forbidden_sql = (
            " INSERT ",
            " UPDATE ",
            " DELETE ",
            " JOINCHAT",
            " SEARCHPUBLICCHAT",
            " GETCHATHISTORY",
        )
        padded = f" {normalized.upper()} "
        assert not any(marker in padded for marker in forbidden_sql), statement

        if normalized == _normalize(module.SET_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult()

        if normalized == _normalize(module.SELECT_ONE_QUERY):
            if self.fail_select_1:
                raise RuntimeError(f"cannot connect to {FAKE_DATABASE_URL}")
            return FakeResult(scalar=1)

        if normalized == _normalize(module.TABLE_AVAILABLE_QUERY):
            qualified = params.get("qualified_table_name")
            table = str(qualified).rsplit(".", 1)[-1]
            return FakeResult(scalar=self.table_available.get(table, True))

        if normalized == _normalize(module.COUNT_JOINED_ROWS_QUERY):
            return FakeResult(scalar=len(self._joined_rows()))

        if normalized == _normalize(module.SELECT_JOINED_ROWS_LIMIT_QUERY):
            rows = self._joined_rows()
            limit = params.get("limit")
            if isinstance(limit, int):
                rows = rows[:limit]
            return FakeResult(rows=[{"chat_id": row["chat_id"]} for row in rows])

        raise AssertionError(f"unexpected SQL: {statement}")

    def close(self) -> None:
        self.closed = True

    def _joined_rows(self) -> list[dict[str, Any]]:
        rows = [
            row
            for row in self.rows
            if row["desired_state"] == "active"
            and row["access_state"] == "joined"
            and row["chat_id"] is not None
        ]
        return sorted(
            rows,
            key=lambda row: (-int(row.get("priority_weight", 0)), row["registry_id"]),
        )


class FakeTDLibReadinessProbe:
    def __init__(
        self,
        *,
        status: str = "ready",
        final_authorization_state: str | None = "authorizationStateReady",
        helper_status: str = "ready",
        request_types_sent: list[str] | None = None,
        authorization_states_seen: list[str] | None = None,
        fail_initialize: Exception | None = None,
    ) -> None:
        self.status = status
        self.final_authorization_state = final_authorization_state
        self.helper_status = helper_status
        self.request_types_sent = request_types_sent or [
            "getAuthorizationState",
            "setTdlibParameters",
        ]
        self.authorization_states_seen = authorization_states_seen or (
            [final_authorization_state] if final_authorization_state else []
        )
        self.fail_initialize = fail_initialize
        self.initialized = False
        self.closed = False
        self.tdlib_send_called = False
        self.tdlib_receive_called = False

    @property
    def tdlib_ready_probe_summary(self) -> dict[str, Any]:
        return {
            "tdlib_ready_probe_attempted": self.initialized,
            "tdlib_ready_probe_status": self.status,
            "tdlib_ready_probe_final_authorization_state": (
                self.final_authorization_state
            ),
            "tdlib_ready_helper_status": self.helper_status,
            "tdlib_ready_helper_manual_intervention_required": (
                self.status == "manual_intervention_required"
            ),
            "tdlib_ready_probe_manual_intervention_required": (
                self.status == "manual_intervention_required"
            ),
            "tdlib_ready_probe_error_class": None,
            "tdlib_ready_probe_request_types_sent": list(self.request_types_sent),
            "tdlib_ready_probe_authorization_states_seen": list(
                self.authorization_states_seen
            ),
        }

    async def initialize(self) -> None:
        self.initialized = True
        self.tdlib_send_called = True
        self.tdlib_receive_called = True
        if self.fail_initialize is not None:
            raise self.fail_initialize

    async def close(self) -> None:
        self.closed = True


class FakeCollectorSmokeRunner:
    def __init__(self, result: Any) -> None:
        self.result = result
        self.started = False
        self.bounds_seen: Any | None = None
        self.runtime_env_seen: Any | None = None

    async def run(self, *, runtime_env: Any, bounds: Any) -> Any:
        self.started = True
        self.runtime_env_seen = runtime_env
        self.bounds_seen = bounds
        return self.result


def _module():
    from scripts.ops import (
        dedicated_vps_joined_channel_collector_bounded_startup_ingest_gate as module,
    )

    return module


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(tmp_path: Path) -> dict[str, str]:
    tdlib_state_dir = tmp_path / "tdlib-state"
    tdlib_files_dir = tmp_path / "tdlib-files"
    lock_dir = tmp_path / "locks"
    tdlib_state_dir.mkdir(parents=True, exist_ok=True)
    tdlib_files_dir.mkdir(parents=True, exist_ok=True)
    lock_dir.mkdir(parents=True, exist_ok=True)
    return {
        "APP_ENV": "test",
        "COLLECTOR_MODE": "replay",
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "TELEGRAM_API_HASH": FAKE_TELEGRAM_SECRET,
        "TELEGRAM_API_ID": "12345",
        "TELEGRAM_PHONE_NUMBER": RAW_PHONE,
        "TELEGRAM_2FA_PASSWORD": "unit-2fa-secret",
        "TDLIB_DB_ENCRYPTION_KEY": "fake-tdlib-encryption-key",
        "TDLIB_STATE_DIR": str(tdlib_state_dir),
        "TDLIB_FILES_DIR": str(tdlib_files_dir),
        "COLLECTOR_SINGLETON_LOCK_PATH": str(lock_dir / "collector.lock"),
        "BOUNDED_STARTUP_TEMP_PATH": RAW_TEMP_PATH,
        "BOUNDED_STARTUP_INVITE_LINK": RAW_INVITE_LINK,
    }


def _runtime_env_reader(tmp_path: Path):
    return lambda _path: _runtime_env(tmp_path)


def _registry_row(
    registry_id: str,
    *,
    desired_state: str = "active",
    access_state: str = "joined",
    chat_id: int | None = RAW_CHAT_ID,
    priority_weight: int = 100,
) -> dict[str, Any]:
    return {
        "registry_id": registry_id,
        "source_kind": "public_username",
        "source_value": RAW_SOURCE_VALUE,
        "username_snapshot": RAW_USERNAME,
        "title_snapshot": RAW_TITLE,
        "desired_state": desired_state,
        "access_state": access_state,
        "chat_id": chat_id,
        "priority_weight": priority_weight,
    }


def _run_report(
    tmp_path: Path,
    *,
    db: FakeDatabaseConnection | None = None,
    runtime_env_reader: Any | None = None,
    approved_tdlib: bool = False,
    approved_startup: bool = False,
    approved_db_write: bool = False,
    smoke_bounds: tuple[int | None, int | None, int | None] | None = None,
    probe: FakeTDLibReadinessProbe | None = None,
    smoke_runner: FakeCollectorSmokeRunner | None = None,
    module_importer: Any | None = None,
) -> tuple[
    dict[str, Any],
    FakeDatabaseConnection,
    FakeTDLibReadinessProbe | None,
    FakeCollectorSmokeRunner | None,
]:
    module = _module()
    fake_db = db or FakeDatabaseConnection([_registry_row("registry-1")])
    duration, updates, writes = smoke_bounds or (None, None, None)
    result = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        approved_tdlib_readiness_probe=approved_tdlib,
        approved_live_collector_startup_smoke=approved_startup,
        approved_collector_ingest_db_write=approved_db_write,
        joined_row_limit=10,
        collector_smoke_max_duration_sec=duration,
        collector_smoke_max_updates=updates,
        collector_smoke_max_db_writes=writes,
        runtime_env_reader=runtime_env_reader or _runtime_env_reader(tmp_path),
        database_connection_factory=lambda _database_url: fake_db,
        module_importer=module_importer,
        tdlib_readiness_probe_factory=(
            (lambda _values, _max, _timeout, _overall: probe)
            if probe is not None
            else None
        ),
        collector_smoke_runner_factory=(
            (lambda _values: smoke_runner) if smoke_runner is not None else None
        ),
    )
    return result.report, fake_db, probe, smoke_runner


def test_default_dry_run_confirms_joined_rows_and_readiness_without_side_effects(
    tmp_path: Path,
) -> None:
    report, db, probe, runner = _run_report(tmp_path)

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_startup_ingest_gate_ready"
    )
    assert report["runtime_env_read"] is True
    assert report["database_connected"] is True
    assert report["required_tables_checked"] == list(_module().REQUIRED_TABLES)
    assert all(report["required_tables_available"].values())
    assert report["joined_rows_checked"] is True
    assert report["joined_row_count_bucket"] == "one"
    assert report["tdlib_readiness_probe_attempted"] is False
    assert report["collector_smoke_attempted"] is False
    assert report["live_collector_started"] is False
    assert report["database_mutation_performed"] is False
    assert report["redis_mutation_performed"] is False
    assert db.transaction is not None
    assert db.transaction.rolled_back is True
    assert probe is None
    assert runner is None


def test_no_joined_rows_blocks_without_side_effects(tmp_path: Path) -> None:
    db = FakeDatabaseConnection(
        [_registry_row("registry-unjoined", access_state="resolved_not_joined")]
    )

    report, _db, _probe, _runner = _run_report(tmp_path, db=db)

    assert report["contract_status"] == "blocked_no_joined_channel_rows"
    assert report["joined_row_count_bucket"] == "zero"
    assert report["side_effects"]["tdlib_initialized"] is False
    assert report["side_effects"]["telegram_api_called"] is False
    assert report["side_effects"]["database_mutation_performed"] is False


def test_missing_required_table_blocks(tmp_path: Path) -> None:
    availability = {table: True for table in _module().REQUIRED_TABLES}
    availability["source_message_versions"] = False
    db = FakeDatabaseConnection([_registry_row("registry-1")], table_available=availability)

    report, _db, _probe, _runner = _run_report(tmp_path, db=db)

    assert report["contract_status"] == "blocked_required_tables_unavailable"
    assert report["required_tables_available"]["source_message_versions"] is False
    assert report["side_effects"]["database_mutation_performed"] is False


def test_runtime_env_unreadable_blocks_and_leaks_no_raw_values(tmp_path: Path) -> None:
    def unreadable(_path: str | Path) -> dict[str, str]:
        raise RuntimeError(f"cannot read {FAKE_DATABASE_URL} {RAW_TEMP_PATH}")

    report, _db, _probe, _runner = _run_report(
        tmp_path,
        runtime_env_reader=unreadable,
    )
    rendered = _module().render_json(report)

    assert report["contract_status"] == "blocked_runtime_env_unreadable"
    assert report["runtime_env_read"] is False
    assert FAKE_DATABASE_URL not in rendered
    assert RAW_TEMP_PATH not in rendered


def test_database_unavailable_blocks_and_leaks_no_raw_database_url(
    tmp_path: Path,
) -> None:
    db = FakeDatabaseConnection([_registry_row("registry-1")], fail_select_1=True)

    report, _db, _probe, _runner = _run_report(tmp_path, db=db)
    rendered = _module().render_json(report)

    assert report["contract_status"] == "blocked_database_unavailable"
    assert report["database_connected"] is False
    assert FAKE_DATABASE_URL not in rendered
    assert FAKE_DATABASE_PASSWORD not in rendered


def test_collector_import_failures_are_classified(tmp_path: Path) -> None:
    def importer(module_name: str) -> Any:
        if module_name.endswith(".service"):
            raise ImportError(f"broken import {FAKE_TELEGRAM_SECRET}")
        return __import__(module_name, fromlist=["*"])

    report, _db, _probe, _runner = _run_report(tmp_path, module_importer=importer)
    rendered = _module().render_json(report)

    assert report["contract_status"] == "blocked_collector_import_contract_failed"
    assert report["collector_config_import_ok"] is True
    assert report["collector_runtime_import_ok"] is True
    assert report["collector_service_import_ok"] is False
    assert "collector_import.service" in report["checks_failed"]
    assert FAKE_TELEGRAM_SECRET not in rendered


def test_singleton_config_unavailable_fails_closed(tmp_path: Path) -> None:
    values = _runtime_env(tmp_path)
    values["COLLECTOR_SINGLETON_LOCK_PATH"] = "relative-lock-file"

    report, _db, _probe, _runner = _run_report(
        tmp_path,
        runtime_env_reader=lambda _path: values,
    )

    assert report["contract_status"] == "blocked_singleton_config_unavailable"
    assert report["collector_config_contract_ok"] is True
    assert report["singleton_lock_path_configured"] is True
    assert report["singleton_lock_parent_available"] is False


def test_tdlib_readiness_probe_not_attempted_without_explicit_approval(
    tmp_path: Path,
) -> None:
    probe = FakeTDLibReadinessProbe()

    report, _db, probe, _runner = _run_report(
        tmp_path,
        probe=probe,
        approved_tdlib=False,
    )

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_startup_ingest_gate_ready"
    )
    assert report["tdlib_readiness_probe_approved"] is False
    assert report["tdlib_readiness_probe_attempted"] is False
    assert probe is not None
    assert probe.initialized is False
    assert report["side_effects"]["tdlib_initialized"] is False


def test_approved_tdlib_readiness_probe_reports_ready_from_fake_helper(
    tmp_path: Path,
) -> None:
    probe = FakeTDLibReadinessProbe()

    report, _db, probe, _runner = _run_report(
        tmp_path,
        probe=probe,
        approved_tdlib=True,
    )

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_startup_tdlib_ready"
    )
    assert report["tdlib_readiness_probe_attempted"] is True
    assert report["tdlib_ready_probe_status"] == "ready"
    assert report["tdlib_ready_probe_final_authorization_state"] == (
        "authorizationStateReady"
    )
    assert report["tdlib_ready_helper_status"] == "ready"
    assert report["tdlib_ready_probe_request_types_sent"] == [
        "getAuthorizationState",
        "setTdlibParameters",
    ]
    assert report["side_effects"]["tdlib_initialized"] is True
    assert report["side_effects"]["tdlib_send_called"] is True
    assert report["side_effects"]["tdlib_receive_called"] is True
    assert probe is not None
    assert probe.closed is True


def test_manual_login_code_password_states_fail_closed_without_auth_submission(
    tmp_path: Path,
) -> None:
    forbidden_auth_requests = {
        "setAuthenticationPhoneNumber",
        "checkAuthenticationCode",
        "checkAuthenticationPassword",
    }
    for state in (
        "authorizationStateWaitPhoneNumber",
        "authorizationStateWaitCode",
        "authorizationStateWaitPassword",
    ):
        probe = FakeTDLibReadinessProbe(
            status="manual_intervention_required",
            final_authorization_state=state,
            helper_status="degraded",
            request_types_sent=["getAuthorizationState"],
            authorization_states_seen=[state],
        )

        report, _db, _probe, _runner = _run_report(
            tmp_path,
            probe=probe,
            approved_tdlib=True,
        )

        assert report["contract_status"] == "blocked_tdlib_not_ready"
        assert report["tdlib_ready_probe_final_authorization_state"] == state
        assert report["tdlib_ready_probe_manual_intervention_required"] is True
        assert not (
            forbidden_auth_requests
            & set(report["tdlib_ready_probe_request_types_sent"])
        )
        assert report["side_effects"]["tdlib_auth_attempted"] is False
        assert report["side_effects"]["tdlib_phone_number_submitted"] is False
        assert report["side_effects"]["tdlib_code_submitted"] is False
        assert report["side_effects"]["tdlib_password_submitted"] is False


def test_startup_smoke_approval_without_valid_bounds_blocks(tmp_path: Path) -> None:
    report, _db, _probe, _runner = _run_report(
        tmp_path,
        approved_startup=True,
    )

    assert report["contract_status"] == "blocked_invalid_smoke_bounds"
    assert report["collector_smoke_attempted"] is False
    assert report["live_collector_started"] is False


def test_db_write_approval_without_startup_approval_blocks(tmp_path: Path) -> None:
    report, _db, _probe, _runner = _run_report(
        tmp_path,
        approved_db_write=True,
    )

    assert report["contract_status"] == "blocked_approval_required"
    assert "approval.live_collector_startup_smoke_required" in report["checks_failed"]
    assert report["collector_smoke_attempted"] is False
    assert report["database_mutation_performed"] is False


def test_startup_smoke_without_db_write_approval_reports_no_write_readiness(
    tmp_path: Path,
) -> None:
    runner = FakeCollectorSmokeRunner(_module().CollectorSmokeResult())

    report, _db, _probe, runner = _run_report(
        tmp_path,
        approved_startup=True,
        smoke_bounds=(30, 10, 10),
        smoke_runner=runner,
    )

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_startup_smoke_no_write_ready"
    )
    assert report["collector_smoke_attempted"] is False
    assert report["collector_smoke_no_updates_observed"] is True
    assert report["live_collector_started"] is False
    assert report["database_mutation_performed"] is False
    assert runner is not None
    assert runner.started is False


def test_fully_approved_fake_smoke_with_no_updates_reports_distinct_status(
    tmp_path: Path,
) -> None:
    module = _module()
    probe = FakeTDLibReadinessProbe()
    runner = FakeCollectorSmokeRunner(
        module.CollectorSmokeResult(
            updates_observed=0,
            side_effects={
                "live_collector_started": True,
                "collector_runtime_started": True,
                "tdlib_initialized": True,
                "tdlib_send_called": True,
                "tdlib_receive_called": True,
                "telegram_api_called": True,
            },
        )
    )

    report, _db, _probe, runner = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_startup=True,
        approved_db_write=True,
        smoke_bounds=(30, 10, 10),
        probe=probe,
        smoke_runner=runner,
    )

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_startup_no_updates_observed"
    )
    assert report["collector_smoke_attempted"] is True
    assert report["collector_smoke_no_updates_observed"] is True
    assert report["collector_smoke_updates_observed_bucket"] == "zero"
    assert report["live_collector_started"] is True
    assert report["collector_runtime_started"] is True
    assert report["database_mutation_performed"] is False
    assert runner is not None
    assert runner.started is True
    assert runner.bounds_seen.max_duration_sec == 30


def test_fully_approved_fake_smoke_with_writes_reports_collector_owned_buckets(
    tmp_path: Path,
) -> None:
    module = _module()
    probe = FakeTDLibReadinessProbe()
    runner = FakeCollectorSmokeRunner(
        module.CollectorSmokeResult(
            updates_observed=3,
            telegram_raw_updates_written=3,
            source_messages_written=2,
            source_message_versions_written=2,
            event_outbox_written=2,
            written_tables=(
                "telegram_raw_updates",
                "source_messages",
                "source_message_versions",
                "event_outbox",
            ),
            side_effects={
                "live_collector_started": True,
                "collector_runtime_started": True,
                "tdlib_initialized": True,
                "tdlib_send_called": True,
                "tdlib_receive_called": True,
                "telegram_api_called": True,
            },
        )
    )

    report, _db, _probe, _runner = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_startup=True,
        approved_db_write=True,
        smoke_bounds=(30, 10, 10),
        probe=probe,
        smoke_runner=runner,
    )

    assert report["contract_status"] == (
        "joined_channel_collector_bounded_startup_writes_observed"
    )
    assert report["collector_smoke_updates_observed_bucket"] == "two_to_five"
    assert report["collector_smoke_raw_updates_written_bucket"] == "two_to_five"
    assert report["collector_smoke_source_messages_written_bucket"] == "two_to_five"
    assert (
        report["collector_smoke_source_message_versions_written_bucket"]
        == "two_to_five"
    )
    assert report["collector_smoke_event_outbox_written_bucket"] == "two_to_five"
    assert report["database_mutation_performed"] is True
    assert report["telegram_raw_updates_written"] is True
    assert report["source_messages_written"] is True
    assert report["source_message_versions_written"] is True
    assert report["event_outbox_written"] is True


def test_forbidden_write_or_side_effect_from_fake_smoke_blocks(tmp_path: Path) -> None:
    module = _module()
    probe = FakeTDLibReadinessProbe()
    runner = FakeCollectorSmokeRunner(
        module.CollectorSmokeResult(
            updates_observed=1,
            telegram_raw_updates_written=1,
            written_tables=("telegram_channel_registry",),
            side_effects={
                "live_collector_started": True,
                "collector_runtime_started": True,
                "tdlib_history_fetch_called": True,
            },
        )
    )

    report, _db, _probe, _runner = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_startup=True,
        approved_db_write=True,
        smoke_bounds=(30, 10, 10),
        probe=probe,
        smoke_runner=runner,
    )

    assert report["contract_status"] == "blocked_forbidden_side_effect_detected"
    assert report["side_effects"]["tdlib_history_fetch_called"] is True
    assert report["history_fetch_attempted"] is True


def test_forbidden_runtime_and_infra_side_effects_remain_false(tmp_path: Path) -> None:
    module = _module()
    probe = FakeTDLibReadinessProbe()
    runner = FakeCollectorSmokeRunner(
        module.CollectorSmokeResult(
            updates_observed=1,
            telegram_raw_updates_written=1,
            source_messages_written=1,
            source_message_versions_written=1,
            event_outbox_written=1,
            written_tables=(
                "telegram_raw_updates",
                "source_messages",
                "source_message_versions",
                "event_outbox",
            ),
            side_effects={
                "live_collector_started": True,
                "collector_runtime_started": True,
                "tdlib_initialized": True,
                "tdlib_send_called": True,
                "tdlib_receive_called": True,
                "telegram_api_called": True,
            },
        )
    )

    report, _db, _probe, _runner = _run_report(
        tmp_path,
        approved_tdlib=True,
        approved_startup=True,
        approved_db_write=True,
        smoke_bounds=(30, 10, 10),
        probe=probe,
        smoke_runner=runner,
    )

    side_effects = report["side_effects"]
    assert side_effects["tdlib_join_called"] is False
    assert side_effects["tdlib_history_fetch_called"] is False
    assert side_effects["tdlib_public_username_resolve_called"] is False
    assert side_effects["tdlib_search_public_chat_called"] is False
    assert side_effects["redis_mutation_performed"] is False
    assert side_effects["notifier_transport_enabled"] is False
    assert side_effects["outbox_relay_started"] is False
    assert side_effects["router_normalizer_started"] is False
    assert side_effects["alembic_upgrade_run"] is False
    assert side_effects["alembic_downgrade_run"] is False
    assert side_effects["alembic_stamp_run"] is False
    assert side_effects["docker_or_systemd_changed"] is False
    assert side_effects["files_mutated_outside_repo"] is False
    assert side_effects["telegram_channel_registry_updated"] is False
    assert side_effects["telegram_channel_registry_inserted"] is False
    assert side_effects["telegram_channel_registry_deleted"] is False


def test_rendered_report_excludes_raw_sensitive_values_and_temp_paths(
    tmp_path: Path,
) -> None:
    probe = FakeTDLibReadinessProbe(
        request_types_sent=["getAuthorizationState", "setTdlibParameters"],
        authorization_states_seen=["authorizationStateReady"],
    )

    report, _db, _probe, _runner = _run_report(
        tmp_path,
        probe=probe,
        approved_tdlib=True,
    )
    rendered = json.dumps(report, sort_keys=True)

    for forbidden in (
        str(RAW_CHAT_ID),
        RAW_SOURCE_VALUE,
        RAW_USERNAME,
        RAW_TITLE,
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_DATABASE_PASSWORD,
        FAKE_TELEGRAM_SECRET,
        RAW_PHONE,
        RAW_TDLIB_PAYLOAD_VALUE,
        RAW_EXTRA,
        RAW_INVITE_LINK,
        RAW_TEMP_PATH,
        str(tmp_path),
    ):
        assert forbidden not in rendered


def test_help_outputs_gate_and_smoke_options_without_runtime_actions() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        check=False,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0
    assert "--approved-tdlib-readiness-probe" in result.stdout
    assert "--approved-live-collector-startup-smoke" in result.stdout
    assert "--approved-collector-ingest-db-write" in result.stdout
    assert "--collector-smoke-max-duration-sec" in result.stdout
    assert "--collector-smoke-max-updates" in result.stdout
    assert "--collector-smoke-max-db-writes" in result.stdout
    assert "getChatHistory" in result.stdout
