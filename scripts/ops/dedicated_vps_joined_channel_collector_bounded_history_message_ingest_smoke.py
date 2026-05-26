from __future__ import annotations

import argparse
import asyncio
import json
import math
import re
import sys
import time
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, Sequence


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = (
    "dedicated_vps_joined_channel_collector_bounded_history_message_ingest_smoke"
)
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_HISTORY_INGEST_MAX_CHATS = 1
MAX_HISTORY_INGEST_MAX_CHATS = 3
DEFAULT_HISTORY_INGEST_HISTORY_LIMIT = 3
MAX_HISTORY_INGEST_HISTORY_LIMIT = 10
DEFAULT_HISTORY_INGEST_MAX_MESSAGES = 3
MAX_HISTORY_INGEST_MAX_MESSAGES = 10
DEFAULT_HISTORY_INGEST_MAX_DB_WRITES = 30
MAX_HISTORY_INGEST_MAX_DB_WRITES = 100
DEFAULT_TDLIB_AUTH_MAX_UPDATES = 200
DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC = 1.0
DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC = 240.0
DEFAULT_HISTORY_RPC_MAX_UPDATES = 120
DEFAULT_HISTORY_RPC_MAX_DURATION_SEC = 60.0
TDLIB_READY_STATE = "authorizationStateReady"

SELECT_HISTORY_TARGET_ROWS_LIMIT_QUERY = """
SELECT chat_id
FROM telegram_channel_registry
WHERE desired_state = 'active'
  AND access_state = 'joined'
  AND chat_id IS NOT NULL
ORDER BY priority_weight DESC, registry_id ASC
LIMIT :limit
"""

SAFE_TDLIB_OBJECT_TYPE_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,80}\Z")
ACCESS_DENIED_ERROR_MARKERS = (
    "FORBIDDEN",
    "CHANNEL_PRIVATE",
    "USER_BANNED_IN_CHANNEL",
    "CHAT_WRITE_FORBIDDEN",
    "USER_NOT_PARTICIPANT",
)
AUTH_SUBMISSION_REQUEST_TYPES = frozenset(
    {
        "setAuthenticationPhoneNumber",
        "checkAuthenticationCode",
        "checkAuthenticationPassword",
    }
)
FORBIDDEN_TELEGRAM_REQUEST_TYPES = frozenset(
    {
        "joinChat",
        "joinChatByInviteLink",
        "searchPublicChat",
        "sendMessage",
        "getMessageLink",
    }
)

SIDE_EFFECT_FLAG_NAMES = (
    "database_mutation_performed",
    "redis_mutation_performed",
    "telegram_channel_registry_updated",
    "telegram_channel_registry_inserted",
    "telegram_channel_registry_deleted",
    "telegram_raw_updates_written",
    "source_messages_written",
    "source_message_versions_written",
    "event_outbox_written",
    "telegram_api_called",
    "tdlib_initialized",
    "tdlib_send_called",
    "tdlib_receive_called",
    "tdlib_auth_attempted",
    "tdlib_phone_number_submitted",
    "tdlib_code_submitted",
    "tdlib_password_submitted",
    "tdlib_join_called",
    "tdlib_history_fetch_called",
    "tdlib_search_public_chat_called",
    "tdlib_send_message_called",
    "tdlib_get_message_link_called",
    "dispatcher_dispatch_called",
    "update_handlers_called",
    "outbox_relay_started",
    "router_normalizer_started",
    "notifier_transport_enabled",
    "enricher_started",
    "judge_started",
    "policy_engine_started",
    "live_collector_started",
    "collector_runtime_started",
    "alembic_upgrade_run",
    "alembic_downgrade_run",
    "alembic_stamp_run",
    "docker_run",
    "systemd_changed",
    "docker_or_systemd_changed",
)


class DatabaseConnection(Protocol):
    def begin(self) -> Any: ...

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> Any: ...

    def close(self) -> None: ...


class HistoryIngestProbe(Protocol):
    tdlib_send_called: bool
    tdlib_receive_called: bool

    @property
    def tdlib_ready_probe_summary(self) -> Mapping[str, Any]: ...

    async def initialize(self) -> None: ...

    async def fetch_chat_history(
        self,
        *,
        chat_id: int,
        limit: int,
    ) -> "HistoryIngestChatResult": ...

    async def close(self) -> None: ...


class HistoryIngestRepository(Protocol):
    def transaction(self) -> Any: ...

    async def get_source_message(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
    ) -> Mapping[str, Any] | None: ...

    async def upsert_source_message(
        self,
        projection: Any,
        *,
        platform: str = "telegram",
    ) -> Mapping[str, Any]: ...

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: Any,
        version_reason: str,
        observed_at: datetime | None = None,
        telegram_edit_date: datetime | None = None,
    ) -> tuple[bool, Mapping[str, Any] | None]: ...

    async def insert_outbox_event(self, event: Any) -> None: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseConnectionFactory = Callable[[str], DatabaseConnection]
ModuleImporter = Callable[[str], Any]
HistoryIngestProbeFactory = Callable[
    [Mapping[str, str], int, float, float],
    HistoryIngestProbe,
]
HistoryIngestRepositoryContextFactory = Callable[
    [Mapping[str, str]],
    AbstractAsyncContextManager[HistoryIngestRepository],
]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class HistoryIngestChatResult:
    status: str
    messages: tuple[Mapping[str, Any], ...] = ()
    request_types_sent: tuple[str, ...] = ("getChatHistory",)


@dataclass(frozen=True, slots=True)
class HistoryMessageApplyResult:
    message_written: bool
    noop: bool
    source_messages_written: int = 0
    source_message_versions_written: int = 0
    event_outbox_written: int = 0
    created_events: int = 0
    reconciled_events: int = 0

    @property
    def total_writes(self) -> int:
        return (
            self.source_messages_written
            + self.source_message_versions_written
            + self.event_outbox_written
        )


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ops import (  # noqa: E402
    dedicated_vps_joined_channel_collector_bounded_startup_ingest_gate as gate,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Explicitly approved bounded history-message ingest smoke for active "
            "joined Telegram channels. Stdout is sanitized JSON only; redirect "
            "TDLib stderr separately in operator use."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument(
        "--approved-tdlib-readiness-probe",
        action="store_true",
        help=(
            "Authorize the existing-session TDLib readiness helper only. This "
            "does not authorize auth phone/code/password submission, join/search/"
            "send/getMessageLink, Redis mutation, or downstream service work."
        ),
    )
    parser.add_argument(
        "--approved-history-message-ingest-smoke",
        action="store_true",
        help=(
            "Authorize bounded getChatHistory reads for selected active joined "
            "registry rows after TDLib reaches authorizationStateReady."
        ),
    )
    parser.add_argument(
        "--approved-history-message-ingest-db-write",
        action="store_true",
        help=(
            "Authorize canonical collector-owned source_messages, "
            "source_message_versions, and event_outbox writes for bounded "
            "history ingest. This is valid only with the ingest smoke approval."
        ),
    )
    parser.add_argument(
        "--history-ingest-max-chats",
        type=_bounded_positive_int_named(
            "history-ingest-max-chats",
            upper_bound=MAX_HISTORY_INGEST_MAX_CHATS,
        ),
        default=DEFAULT_HISTORY_INGEST_MAX_CHATS,
    )
    parser.add_argument(
        "--history-ingest-history-limit",
        type=_bounded_positive_int_named(
            "history-ingest-history-limit",
            upper_bound=MAX_HISTORY_INGEST_HISTORY_LIMIT,
        ),
        default=DEFAULT_HISTORY_INGEST_HISTORY_LIMIT,
    )
    parser.add_argument(
        "--history-ingest-max-messages",
        type=_bounded_positive_int_named(
            "history-ingest-max-messages",
            upper_bound=MAX_HISTORY_INGEST_MAX_MESSAGES,
        ),
        default=DEFAULT_HISTORY_INGEST_MAX_MESSAGES,
    )
    parser.add_argument(
        "--history-ingest-max-db-writes",
        type=_bounded_positive_int_named(
            "history-ingest-max-db-writes",
            upper_bound=MAX_HISTORY_INGEST_MAX_DB_WRITES,
        ),
        default=DEFAULT_HISTORY_INGEST_MAX_DB_WRITES,
    )
    parser.add_argument(
        "--tdlib-auth-max-updates",
        type=_positive_int_named("tdlib-auth-max-updates"),
        default=DEFAULT_TDLIB_AUTH_MAX_UPDATES,
    )
    parser.add_argument(
        "--tdlib-receive-timeout-sec",
        type=_non_negative_float_named("tdlib-receive-timeout-sec"),
        default=DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC,
    )
    parser.add_argument(
        "--tdlib-overall-timeout-sec",
        type=_non_negative_float_named("tdlib-overall-timeout-sec"),
        default=DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC,
    )
    return parser


def _positive_int_named(field_name: str) -> Callable[[str], int]:
    def parse(raw: str) -> int:
        try:
            value = int(raw)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be a positive integer"
            ) from exc
        if value <= 0:
            raise argparse.ArgumentTypeError(f"{field_name} must be a positive integer")
        return value

    return parse


def _bounded_positive_int_named(
    field_name: str,
    *,
    upper_bound: int,
) -> Callable[[str], int]:
    parse_positive = _positive_int_named(field_name)

    def parse(raw: str) -> int:
        value = parse_positive(raw)
        if value > upper_bound:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be less than or equal to {upper_bound}"
            )
        return value

    return parse


def _non_negative_float_named(field_name: str) -> Callable[[str], float]:
    def parse(raw: str) -> float:
        try:
            value = float(raw)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be a finite non-negative number"
            ) from exc
        if not math.isfinite(value) or value < 0:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be a finite non-negative number"
            )
        return value

    return parse


def _side_effects() -> dict[str, bool]:
    return {flag: False for flag in SIDE_EFFECT_FLAG_NAMES}


def _base_report(
    *,
    approved_tdlib_readiness_probe: bool,
    approved_history_ingest: bool,
    approved_history_ingest_db_write: bool,
    history_ingest_max_chats: int,
    history_ingest_history_limit: int,
    history_ingest_max_messages: int,
    history_ingest_max_db_writes: int,
) -> dict[str, Any]:
    side_effects = _side_effects()
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "contract_status": "blocked_history_message_ingest_not_ready",
        "checks_failed": [],
        "runtime_env_read": False,
        "database_connected": False,
        "required_tables_checked": [],
        "required_tables_available": {
            table: False for table in gate.REQUIRED_TABLES
        },
        "collector_config_import_ok": False,
        "collector_runtime_import_ok": False,
        "collector_service_import_ok": False,
        "collector_repository_import_ok": False,
        "singleton_guard_import_ok": False,
        "collector_config_contract_ok": False,
        "singleton_lock_path_configured": False,
        "singleton_lock_parent_available": False,
        "joined_rows_checked": False,
        "joined_row_count_bucket": "unknown",
        "tdlib_readiness_probe_approved": approved_tdlib_readiness_probe,
        "tdlib_readiness_probe_attempted": False,
        "tdlib_ready_probe_status": "not_attempted",
        "tdlib_ready_probe_final_authorization_state": None,
        "history_ingest_approved": approved_history_ingest,
        "history_ingest_db_write_approved": approved_history_ingest_db_write,
        "history_ingest_attempted": False,
        "history_ingest_status": "not_attempted",
        "history_ingest_max_chats": history_ingest_max_chats,
        "history_ingest_history_limit": history_ingest_history_limit,
        "history_ingest_max_messages": history_ingest_max_messages,
        "history_ingest_max_db_writes": history_ingest_max_db_writes,
        "history_ingest_selected_chats_bucket": "zero",
        "history_ingest_get_chat_history_requests_bucket": "zero",
        "history_ingest_history_messages_observed_bucket": "zero",
        "history_ingest_message_bearing_messages_observed_bucket": "zero",
        "history_ingest_messages_considered_bucket": "zero",
        "history_ingest_messages_written_bucket": "zero",
        "history_ingest_messages_noop_bucket": "zero",
        "history_ingest_source_messages_written_bucket": "zero",
        "history_ingest_source_message_versions_written_bucket": "zero",
        "history_ingest_event_outbox_written_bucket": "zero",
        "history_ingest_created_events_bucket": "zero",
        "history_ingest_reconciled_events_bucket": "zero",
        "history_ingest_db_write_cap_exhausted": False,
        "history_ingest_message_cap_exhausted": False,
        "history_ingest_content_type_buckets": {},
        "database_mutation_performed": False,
        "redis_mutation_performed": False,
        "telegram_channel_registry_updated": False,
        "telegram_channel_registry_inserted": False,
        "telegram_channel_registry_deleted": False,
        "telegram_raw_updates_written": False,
        "source_messages_written": False,
        "source_message_versions_written": False,
        "event_outbox_written": False,
        "tdlib_join_called": False,
        "tdlib_history_fetch_called": False,
        "tdlib_search_public_chat_called": False,
        "tdlib_send_message_called": False,
        "tdlib_get_message_link_called": False,
        "dispatcher_dispatch_called": False,
        "update_handlers_called": False,
        "outbox_relay_started": False,
        "router_normalizer_started": False,
        "notifier_transport_enabled": False,
        "alembic_upgrade_run": False,
        "alembic_downgrade_run": False,
        "alembic_stamp_run": False,
        "docker_run": False,
        "systemd_changed": False,
        "docker_or_systemd_changed": False,
        "side_effects": side_effects,
    }
    for flag, value in side_effects.items():
        report.setdefault(flag, value)
    return report


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _sync_side_effects(report: dict[str, Any]) -> None:
    side_effects = report["side_effects"]
    for flag, value in side_effects.items():
        report[flag] = value
    report["database_mutation_performed"] = side_effects["database_mutation_performed"]
    report["redis_mutation_performed"] = side_effects["redis_mutation_performed"]
    report["telegram_channel_registry_updated"] = side_effects[
        "telegram_channel_registry_updated"
    ]
    report["telegram_channel_registry_inserted"] = side_effects[
        "telegram_channel_registry_inserted"
    ]
    report["telegram_channel_registry_deleted"] = side_effects[
        "telegram_channel_registry_deleted"
    ]
    report["telegram_raw_updates_written"] = side_effects["telegram_raw_updates_written"]
    report["source_messages_written"] = side_effects["source_messages_written"]
    report["source_message_versions_written"] = side_effects[
        "source_message_versions_written"
    ]
    report["event_outbox_written"] = side_effects["event_outbox_written"]


def _valid_history_bound(value: Any, *, upper_bound: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 < value <= upper_bound


def _validate_history_bounds(report: dict[str, Any]) -> bool:
    bounds = (
        (
            "history_ingest.max_chats_out_of_bounds",
            report["history_ingest_max_chats"],
            MAX_HISTORY_INGEST_MAX_CHATS,
        ),
        (
            "history_ingest.history_limit_out_of_bounds",
            report["history_ingest_history_limit"],
            MAX_HISTORY_INGEST_HISTORY_LIMIT,
        ),
        (
            "history_ingest.max_messages_out_of_bounds",
            report["history_ingest_max_messages"],
            MAX_HISTORY_INGEST_MAX_MESSAGES,
        ),
        (
            "history_ingest.max_db_writes_out_of_bounds",
            report["history_ingest_max_db_writes"],
            MAX_HISTORY_INGEST_MAX_DB_WRITES,
        ),
    )
    for check, value, upper_bound in bounds:
        if not _valid_history_bound(value, upper_bound=upper_bound):
            _set_status(report, "blocked_history_message_ingest_not_ready", check)
            return False
    return True


def _normalize_sql(statement: str) -> str:
    return " ".join(statement.strip().split())


def _allowed_read_statements() -> set[str]:
    return {
        _normalize_sql(statement)
        for statement in (
            gate.SELECT_ONE_QUERY,
            gate.SET_TRANSACTION_READ_ONLY_QUERY,
            gate.TABLE_AVAILABLE_QUERY,
            gate.COUNT_JOINED_ROWS_QUERY,
            SELECT_HISTORY_TARGET_ROWS_LIMIT_QUERY,
        )
    }


def _assert_read_sql(statement: str) -> None:
    if _normalize_sql(statement) not in _allowed_read_statements():
        raise ValueError("SQL statement is not in the history-ingest read allowlist")


def _execute_read(
    connection: DatabaseConnection,
    statement: str,
    params: dict[str, Any] | None = None,
) -> Any:
    _assert_read_sql(statement)
    return connection.execute(statement, params or {})


def _select_history_target_chat_ids(
    connection: DatabaseConnection,
    *,
    limit: int,
) -> list[int]:
    rows = gate._rows(  # noqa: SLF001
        _execute_read(
            connection,
            SELECT_HISTORY_TARGET_ROWS_LIMIT_QUERY,
            {"limit": limit},
        )
    )
    chat_ids: list[int] = []
    for row in rows:
        raw_chat_id: Any
        if isinstance(row, Mapping):
            raw_chat_id = row.get("chat_id")
        elif hasattr(row, "_mapping"):
            raw_chat_id = row._mapping.get("chat_id")
        elif isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
            raw_chat_id = row[0] if row else None
        else:
            raw_chat_id = None
        if isinstance(raw_chat_id, bool):
            continue
        try:
            chat_id = int(raw_chat_id)
        except (TypeError, ValueError):
            continue
        chat_ids.append(chat_id)
    return chat_ids[:limit]


def _safe_tdlib_object_type(value: Any, *, default: str | None = None) -> str | None:
    if isinstance(value, str) and SAFE_TDLIB_OBJECT_TYPE_RE.fullmatch(value):
        return value
    return default


def _safe_text(value: Any, *, default: str | None = None) -> str | None:
    return gate._safe_text(value, default=default)  # noqa: SLF001


def _safe_text_list(value: Any) -> list[str]:
    return gate._safe_text_list(value)  # noqa: SLF001


def _safe_non_negative_int(value: Any) -> int:
    return gate._safe_non_negative_int(value)  # noqa: SLF001


def _bucket_count(count: int | None) -> str:
    return gate._bucket_count(count)  # noqa: SLF001


def _mapping_get(row: Mapping[str, Any] | Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    if hasattr(row, "_mapping"):
        return row._mapping.get(key)
    return None


def _merge_tdlib_ready_fields(
    report: dict[str, Any],
    probe: HistoryIngestProbe | None,
) -> None:
    summary: Mapping[str, Any] = {}
    if probe is not None:
        try:
            raw_summary = probe.tdlib_ready_probe_summary
        except Exception:
            raw_summary = {}
        if isinstance(raw_summary, Mapping):
            summary = raw_summary
    report["tdlib_readiness_probe_attempted"] = bool(
        summary.get(
            "tdlib_ready_probe_attempted",
            report["tdlib_readiness_probe_attempted"],
        )
    )
    report["tdlib_ready_probe_status"] = _safe_text(
        summary.get("tdlib_ready_probe_status"),
        default=report["tdlib_ready_probe_status"],
    )
    report["tdlib_ready_probe_final_authorization_state"] = _safe_text(
        summary.get("tdlib_ready_probe_final_authorization_state"),
        default=report["tdlib_ready_probe_final_authorization_state"],
    )
    request_types = _safe_text_list(summary.get("tdlib_ready_probe_request_types_sent"))
    _apply_request_side_effects(
        report,
        request_types,
        allow_get_chat_history=False,
    )
    if probe is not None:
        if bool(getattr(probe, "tdlib_send_called", False)):
            report["side_effects"]["tdlib_send_called"] = True
        if bool(getattr(probe, "tdlib_receive_called", False)):
            report["side_effects"]["tdlib_receive_called"] = True
        if (
            report["side_effects"]["tdlib_send_called"]
            or report["side_effects"]["tdlib_receive_called"]
        ):
            report["side_effects"]["telegram_api_called"] = True


def _apply_request_side_effects(
    report: dict[str, Any],
    request_types: Sequence[str],
    *,
    allow_get_chat_history: bool,
) -> None:
    side_effects = report["side_effects"]
    for request_type in request_types:
        safe_request_type = _safe_text(request_type)
        if safe_request_type is None:
            continue
        if safe_request_type in AUTH_SUBMISSION_REQUEST_TYPES:
            side_effects["tdlib_auth_attempted"] = True
            if safe_request_type == "setAuthenticationPhoneNumber":
                side_effects["tdlib_phone_number_submitted"] = True
            elif safe_request_type == "checkAuthenticationCode":
                side_effects["tdlib_code_submitted"] = True
            elif safe_request_type == "checkAuthenticationPassword":
                side_effects["tdlib_password_submitted"] = True
        elif safe_request_type == "getChatHistory":
            side_effects["tdlib_history_fetch_called"] = True
            if not allow_get_chat_history:
                _set_status(
                    report,
                    "blocked_forbidden_side_effect_detected",
                    "tdlib.get_chat_history_before_ingest_approval",
                )
        elif safe_request_type == "joinChat":
            side_effects["tdlib_join_called"] = True
        elif safe_request_type == "joinChatByInviteLink":
            side_effects["tdlib_join_called"] = True
        elif safe_request_type == "searchPublicChat":
            side_effects["tdlib_search_public_chat_called"] = True
        elif safe_request_type == "sendMessage":
            side_effects["tdlib_send_message_called"] = True
        elif safe_request_type == "getMessageLink":
            side_effects["tdlib_get_message_link_called"] = True


def _tdlib_ready(report: Mapping[str, Any]) -> bool:
    return (
        report.get("tdlib_ready_probe_status") == "ready"
        and report.get("tdlib_ready_probe_final_authorization_state") == TDLIB_READY_STATE
    )


def _forbidden_side_effect_detected(report: Mapping[str, Any]) -> bool:
    side_effects = report["side_effects"]
    return any(
        side_effects[flag]
        for flag in (
            "redis_mutation_performed",
            "telegram_channel_registry_updated",
            "telegram_channel_registry_inserted",
            "telegram_channel_registry_deleted",
            "telegram_raw_updates_written",
            "tdlib_auth_attempted",
            "tdlib_phone_number_submitted",
            "tdlib_code_submitted",
            "tdlib_password_submitted",
            "tdlib_join_called",
            "tdlib_search_public_chat_called",
            "tdlib_send_message_called",
            "tdlib_get_message_link_called",
            "dispatcher_dispatch_called",
            "update_handlers_called",
            "outbox_relay_started",
            "router_normalizer_started",
            "notifier_transport_enabled",
            "enricher_started",
            "judge_started",
            "policy_engine_started",
            "live_collector_started",
            "collector_runtime_started",
            "alembic_upgrade_run",
            "alembic_downgrade_run",
            "alembic_stamp_run",
            "docker_run",
            "systemd_changed",
            "docker_or_systemd_changed",
        )
    )


async def _close_probe(probe: HistoryIngestProbe | None) -> None:
    if probe is None:
        return
    try:
        await probe.close()
    except Exception:
        return


def _authorization_state_type_from_payload(payload: Mapping[str, Any]) -> str | None:
    if payload.get("@type") != "updateAuthorizationState":
        return None
    state = payload.get("authorization_state")
    if not isinstance(state, Mapping):
        return None
    return _safe_tdlib_object_type(state.get("@type"))


def _response_type_from_payload(payload: Mapping[str, Any]) -> str | None:
    return _safe_tdlib_object_type(payload.get("@type"))


def _is_access_denied_error(payload: Mapping[str, Any]) -> bool:
    if payload.get("@type") != "error":
        return False
    message = payload.get("message")
    code = payload.get("code")
    haystack = f"{code} {message}".upper()
    return any(marker in haystack for marker in ACCESS_DENIED_ERROR_MARKERS)


def _message_content_type(message: Mapping[str, Any]) -> str:
    content = message.get("content")
    if not isinstance(content, Mapping):
        return "unknown"
    return _safe_tdlib_object_type(content.get("@type"), default="unknown") or "unknown"


def _formatted_text_has_text(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    text = value.get("text")
    return isinstance(text, str) and bool(text.strip())


def _message_has_text_surface(message: Mapping[str, Any]) -> bool:
    content = message.get("content")
    if not isinstance(content, Mapping):
        return False
    if _message_content_type(message) == "messageText":
        return True
    return _formatted_text_has_text(content.get("text")) or _formatted_text_has_text(
        content.get("caption")
    )


def _history_messages_from_payload(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    messages_value = payload.get("messages")
    if not isinstance(messages_value, Sequence) or isinstance(
        messages_value,
        (str, bytes, bytearray),
    ):
        return ()
    return tuple(
        message
        for message in messages_value
        if isinstance(message, Mapping) and message.get("@type") == "message"
    )


class TDLibHistoryIngestProbe:
    def __init__(
        self,
        runtime_env: Mapping[str, str],
        *,
        auth_max_updates: int = DEFAULT_TDLIB_AUTH_MAX_UPDATES,
        receive_timeout_sec: float = DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC,
        overall_timeout_sec: float = DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC,
        history_rpc_max_updates: int = DEFAULT_HISTORY_RPC_MAX_UPDATES,
        history_rpc_max_duration_sec: float = DEFAULT_HISTORY_RPC_MAX_DURATION_SEC,
    ) -> None:
        from scripts.ops import (  # noqa: PLC0415
            dedicated_vps_telegram_channel_registry_public_username_resolve_operator
            as resolve_operator,
        )

        self._resolver = resolve_operator.TDLibPublicUsernameResolver(
            runtime_env,
            auth_max_updates=auth_max_updates,
            receive_timeout_sec=receive_timeout_sec,
            overall_timeout_sec=overall_timeout_sec,
            single_rpc_max_updates=history_rpc_max_updates,
            single_rpc_receive_timeout_sec=receive_timeout_sec,
            single_rpc_max_duration_sec=history_rpc_max_duration_sec,
        )
        self._receive_timeout_sec = receive_timeout_sec
        self._history_rpc_max_updates = history_rpc_max_updates
        self._history_rpc_max_duration_sec = history_rpc_max_duration_sec
        self._request_sequence = 0
        self.tdlib_send_called = False
        self.tdlib_receive_called = False

    @property
    def tdlib_ready_probe_summary(self) -> Mapping[str, Any]:
        return self._resolver.tdlib_ready_probe_summary

    async def initialize(self) -> None:
        await self._resolver.initialize()
        self._sync_flags()

    async def close(self) -> None:
        await self._resolver.close()
        self._sync_flags()

    async def fetch_chat_history(
        self,
        *,
        chat_id: int,
        limit: int,
    ) -> HistoryIngestChatResult:
        request = self._resolver._client.build_get_chat_history_request(  # noqa: SLF001
            chat_id=chat_id,
            from_message_id=0,
            offset=0,
            limit=limit,
            only_local=False,
        ).payload
        extra = self._next_extra("get_chat_history")
        try:
            await self._resolver._send({**request, "@extra": extra})  # noqa: SLF001
        except Exception:
            self._sync_flags()
            return HistoryIngestChatResult(status="transient_error")
        self._sync_flags()
        result = await self._receive_history_response(extra)
        self._sync_flags()
        return result

    async def _receive_history_response(self, extra: str) -> HistoryIngestChatResult:
        started_at = time.monotonic()
        for _ in range(self._history_rpc_max_updates):
            elapsed_sec = time.monotonic() - started_at
            remaining_sec = self._history_rpc_max_duration_sec - elapsed_sec
            if remaining_sec <= 0:
                break
            try:
                payload = await self._resolver._receive(  # noqa: SLF001
                    min(self._receive_timeout_sec, max(remaining_sec, 0.0))
                )
            except Exception:
                return HistoryIngestChatResult(status="transient_error")
            if payload is None or not isinstance(payload, Mapping):
                continue
            state_type = _authorization_state_type_from_payload(payload)
            if state_type is not None and state_type != TDLIB_READY_STATE:
                return HistoryIngestChatResult(status="transient_error")
            if payload.get("@extra") != extra:
                continue
            response_type = _response_type_from_payload(payload)
            if response_type == "error":
                if _is_access_denied_error(payload):
                    return HistoryIngestChatResult(status="access_denied")
                return HistoryIngestChatResult(status="transient_error")
            if response_type == "messages":
                return HistoryIngestChatResult(
                    status="history",
                    messages=_history_messages_from_payload(payload),
                )
            return HistoryIngestChatResult(status="transient_error")
        return HistoryIngestChatResult(status="transient_error")

    def _next_extra(self, label: str) -> str:
        self._request_sequence += 1
        return f"{SCRIPT_NAME}.{label}.{self._request_sequence}"

    def _sync_flags(self) -> None:
        self.tdlib_send_called = bool(getattr(self._resolver, "tdlib_send_called", False))
        self.tdlib_receive_called = bool(
            getattr(self._resolver, "tdlib_receive_called", False)
        )


class _DefaultIngestRepositoryContext:
    def __init__(self, runtime_env: Mapping[str, str]) -> None:
        self._runtime_env = runtime_env
        self._engine: Any | None = None
        self._session: Any | None = None

    async def __aenter__(self) -> HistoryIngestRepository:
        from sqlalchemy.ext.asyncio import (  # noqa: PLC0415
            async_sessionmaker,
            create_async_engine,
        )
        from src.services.collector_telegram.repositories import (  # noqa: PLC0415
            CollectorRepository,
        )

        database_url = self._runtime_env.get("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL is required for history ingest")
        self._engine = create_async_engine(_async_database_url(database_url), future=True)
        session_factory = async_sessionmaker(self._engine, expire_on_commit=False)
        self._session = session_factory()
        return CollectorRepository(self._session)

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None


def _async_database_url(database_url: str) -> str:
    if database_url.startswith("postgresql://"):
        return "postgresql+psycopg://" + database_url.removeprefix("postgresql://")
    return database_url


def _default_history_probe_factory(
    runtime_env: Mapping[str, str],
    tdlib_auth_max_updates: int,
    tdlib_receive_timeout_sec: float,
    tdlib_overall_timeout_sec: float,
) -> HistoryIngestProbe:
    return TDLibHistoryIngestProbe(
        runtime_env,
        auth_max_updates=tdlib_auth_max_updates,
        receive_timeout_sec=tdlib_receive_timeout_sec,
        overall_timeout_sec=tdlib_overall_timeout_sec,
    )


def _default_repository_context_factory(
    runtime_env: Mapping[str, str],
) -> AbstractAsyncContextManager[HistoryIngestRepository]:
    return _DefaultIngestRepositoryContext(runtime_env)


async def _apply_history_message(
    *,
    repository: HistoryIngestRepository,
    projection_builder: Any,
    outbox_builder: Any,
    message: Mapping[str, Any],
    observed_at: datetime,
) -> HistoryMessageApplyResult:
    projection = projection_builder.build_source_projection(dict(message))
    async with repository.transaction():
        existing = await repository.get_source_message(
            platform="telegram",
            chat_id=projection.chat_id,
            message_id=projection.message_id,
        )
        if existing is None:
            current_row = await repository.upsert_source_message(
                projection,
                platform="telegram",
            )
            source_message_id = str(_mapping_get(current_row, "source_message_id") or "")
            changed, version_row = await repository.append_source_message_version_if_changed(
                source_message_id=source_message_id,
                projection=projection,
                version_reason="new",
                observed_at=observed_at,
                telegram_edit_date=projection.edited_at,
            )
            if not changed or version_row is None:
                return HistoryMessageApplyResult(message_written=False, noop=True)
            version_no = _safe_non_negative_int(_mapping_get(version_row, "version_no"))
            event = outbox_builder.build_created(
                source_message_id=source_message_id,
                current_version_no=version_no,
                logical_post_key=projection.logical_post_key,
                occurred_at=observed_at,
            )
            await repository.insert_outbox_event(event)
            return HistoryMessageApplyResult(
                message_written=True,
                noop=False,
                source_messages_written=1,
                source_message_versions_written=1,
                event_outbox_written=1,
                created_events=1,
            )

        source_message_id = str(_mapping_get(existing, "source_message_id") or "")
        changed, version_row = await repository.append_source_message_version_if_changed(
            source_message_id=source_message_id,
            projection=projection,
            version_reason="reconcile",
            observed_at=observed_at,
            telegram_edit_date=projection.edited_at,
        )
        if not changed or version_row is None:
            return HistoryMessageApplyResult(message_written=False, noop=True)
        version_no = _safe_non_negative_int(_mapping_get(version_row, "version_no"))
        event = outbox_builder.build_reconciled(
            source_message_id=source_message_id,
            current_version_no=version_no,
            logical_post_key=projection.logical_post_key,
            occurred_at=observed_at,
            reconcile_reason="bounded_history_ingest_smoke",
        )
        await repository.insert_outbox_event(event)
        return HistoryMessageApplyResult(
            message_written=True,
            noop=False,
            source_messages_written=1,
            source_message_versions_written=1,
            event_outbox_written=1,
            reconciled_events=1,
        )


def _apply_write_counts(
    report: dict[str, Any],
    result: HistoryMessageApplyResult,
    counters: dict[str, int],
) -> None:
    counters["messages_written"] += 1 if result.message_written else 0
    counters["messages_noop"] += 1 if result.noop else 0
    counters["source_messages_written"] += result.source_messages_written
    counters["source_message_versions_written"] += result.source_message_versions_written
    counters["event_outbox_written"] += result.event_outbox_written
    counters["created_events"] += result.created_events
    counters["reconciled_events"] += result.reconciled_events
    side_effects = report["side_effects"]
    if result.total_writes > 0:
        side_effects["database_mutation_performed"] = True
    if result.source_messages_written > 0:
        side_effects["source_messages_written"] = True
    if result.source_message_versions_written > 0:
        side_effects["source_message_versions_written"] = True
    if result.event_outbox_written > 0:
        side_effects["event_outbox_written"] = True


async def _run_history_ingest(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    chat_ids: Sequence[int],
    history_limit: int,
    max_messages: int,
    max_db_writes: int,
    tdlib_auth_max_updates: int,
    tdlib_receive_timeout_sec: float,
    tdlib_overall_timeout_sec: float,
    history_probe_factory: HistoryIngestProbeFactory | None,
    repository_context_factory: HistoryIngestRepositoryContextFactory | None,
) -> None:
    from src.services.collector_telegram.idempotency import IdempotencyPolicy  # noqa: PLC0415
    from src.services.collector_telegram.message_projection import (  # noqa: PLC0415
        MessageProjectionBuilder,
    )
    from src.services.collector_telegram.outbox import (  # noqa: PLC0415
        CollectorOutboxBuilder,
    )

    report["tdlib_readiness_probe_attempted"] = True
    probe: HistoryIngestProbe | None = None
    request_count = 0
    history_messages_observed = 0
    message_bearing_observed = 0
    messages_considered = 0
    content_type_counts: dict[str, int] = {}
    counters = {
        "messages_written": 0,
        "messages_noop": 0,
        "source_messages_written": 0,
        "source_message_versions_written": 0,
        "event_outbox_written": 0,
        "created_events": 0,
        "reconciled_events": 0,
    }
    observed_at = datetime.now(timezone.utc)

    try:
        factory = history_probe_factory or _default_history_probe_factory
        probe = factory(
            values,
            tdlib_auth_max_updates,
            tdlib_receive_timeout_sec,
            tdlib_overall_timeout_sec,
        )
        await probe.initialize()
        report["side_effects"]["tdlib_initialized"] = True
        _merge_tdlib_ready_fields(report, probe)
        if report["contract_status"] == "blocked_forbidden_side_effect_detected":
            return
        if _forbidden_side_effect_detected(report):
            _set_status(
                report,
                "blocked_forbidden_side_effect_detected",
                "tdlib.forbidden_request",
            )
            return
        if not _tdlib_ready(report):
            report["history_ingest_status"] = "blocked_not_ready"
            _set_status(
                report,
                "blocked_history_message_ingest_not_ready",
                "tdlib.not_ready",
            )
            return

        report["history_ingest_attempted"] = True
        report["history_ingest_status"] = "running"
        projection_builder = MessageProjectionBuilder()
        outbox_builder = CollectorOutboxBuilder(IdempotencyPolicy())
        context_factory = repository_context_factory or _default_repository_context_factory
        async with context_factory(values) as repository:
            stop = False
            for chat_id in chat_ids:
                if stop:
                    break
                request_count += 1
                result = await probe.fetch_chat_history(
                    chat_id=chat_id,
                    limit=history_limit,
                )
                _apply_request_side_effects(
                    report,
                    result.request_types_sent,
                    allow_get_chat_history=True,
                )
                _merge_tdlib_ready_fields(report, probe)
                if report["contract_status"] == "blocked_forbidden_side_effect_detected":
                    return
                if _forbidden_side_effect_detected(report):
                    _set_status(
                        report,
                        "blocked_forbidden_side_effect_detected",
                        "tdlib.forbidden_request",
                    )
                    return
                if result.status != "history":
                    continue
                for message in result.messages:
                    history_messages_observed += 1
                    content_type = _message_content_type(message)
                    content_type_counts[content_type] = (
                        content_type_counts.get(content_type, 0) + 1
                    )
                    if not _message_has_text_surface(message):
                        continue
                    message_bearing_observed += 1
                    if messages_considered >= max_messages:
                        report["history_ingest_message_cap_exhausted"] = True
                        stop = True
                        break
                    remaining_writes = max_db_writes - (
                        counters["source_messages_written"]
                        + counters["source_message_versions_written"]
                        + counters["event_outbox_written"]
                    )
                    if remaining_writes < 3:
                        report["history_ingest_db_write_cap_exhausted"] = True
                        stop = True
                        break
                    messages_considered += 1
                    apply_result = await _apply_history_message(
                        repository=repository,
                        projection_builder=projection_builder,
                        outbox_builder=outbox_builder,
                        message=message,
                        observed_at=observed_at,
                    )
                    _apply_write_counts(report, apply_result, counters)
                    if messages_considered >= max_messages:
                        report["history_ingest_message_cap_exhausted"] = True
                        stop = True
                        break
    finally:
        await _close_probe(probe)
        _merge_tdlib_ready_fields(report, probe)
        report["history_ingest_get_chat_history_requests_bucket"] = _bucket_count(
            request_count
        )
        report["history_ingest_history_messages_observed_bucket"] = _bucket_count(
            history_messages_observed
        )
        report["history_ingest_message_bearing_messages_observed_bucket"] = (
            _bucket_count(message_bearing_observed)
        )
        report["history_ingest_messages_considered_bucket"] = _bucket_count(
            messages_considered
        )
        report["history_ingest_messages_written_bucket"] = _bucket_count(
            counters["messages_written"]
        )
        report["history_ingest_messages_noop_bucket"] = _bucket_count(
            counters["messages_noop"]
        )
        report["history_ingest_source_messages_written_bucket"] = _bucket_count(
            counters["source_messages_written"]
        )
        report["history_ingest_source_message_versions_written_bucket"] = _bucket_count(
            counters["source_message_versions_written"]
        )
        report["history_ingest_event_outbox_written_bucket"] = _bucket_count(
            counters["event_outbox_written"]
        )
        report["history_ingest_created_events_bucket"] = _bucket_count(
            counters["created_events"]
        )
        report["history_ingest_reconciled_events_bucket"] = _bucket_count(
            counters["reconciled_events"]
        )
        report["history_ingest_content_type_buckets"] = {
            content_type: _bucket_count(count)
            for content_type, count in sorted(content_type_counts.items())
        }

    if report["contract_status"] == "blocked_forbidden_side_effect_detected":
        return
    if counters["messages_written"] > 0:
        report["history_ingest_status"] = "writes_observed"
        _set_status(
            report,
            "joined_channel_collector_bounded_history_message_ingest_writes_observed",
        )
    elif message_bearing_observed > 0:
        report["history_ingest_status"] = "noop_observed"
        _set_status(
            report,
            "joined_channel_collector_bounded_history_message_ingest_noop_observed",
        )
    else:
        report["history_ingest_status"] = "no_messages_observed"
        _set_status(
            report,
            "joined_channel_collector_bounded_history_message_ingest_no_messages_observed",
        )


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    approved_tdlib_readiness_probe: bool = False,
    approved_history_message_ingest_smoke: bool = False,
    approved_history_message_ingest_db_write: bool = False,
    history_ingest_max_chats: int = DEFAULT_HISTORY_INGEST_MAX_CHATS,
    history_ingest_history_limit: int = DEFAULT_HISTORY_INGEST_HISTORY_LIMIT,
    history_ingest_max_messages: int = DEFAULT_HISTORY_INGEST_MAX_MESSAGES,
    history_ingest_max_db_writes: int = DEFAULT_HISTORY_INGEST_MAX_DB_WRITES,
    tdlib_auth_max_updates: int = DEFAULT_TDLIB_AUTH_MAX_UPDATES,
    tdlib_receive_timeout_sec: float = DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC,
    tdlib_overall_timeout_sec: float = DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_connection_factory: DatabaseConnectionFactory | None = None,
    module_importer: ModuleImporter | None = None,
    history_probe_factory: HistoryIngestProbeFactory | None = None,
    repository_context_factory: HistoryIngestRepositoryContextFactory | None = None,
) -> ScriptResult:
    report = _base_report(
        approved_tdlib_readiness_probe=approved_tdlib_readiness_probe,
        approved_history_ingest=approved_history_message_ingest_smoke,
        approved_history_ingest_db_write=approved_history_message_ingest_db_write,
        history_ingest_max_chats=history_ingest_max_chats,
        history_ingest_history_limit=history_ingest_history_limit,
        history_ingest_max_messages=history_ingest_max_messages,
        history_ingest_max_db_writes=history_ingest_max_db_writes,
    )
    if not _validate_history_bounds(report):
        _sync_side_effects(report)
        return ScriptResult(exit_code=1, report=report)

    try:
        values = gate._read_runtime_env(runtime_env_path, runtime_env_reader)  # noqa: SLF001
    except Exception:
        _set_status(
            report,
            "blocked_history_message_ingest_not_ready",
            "runtime_env.unreadable",
        )
        _sync_side_effects(report)
        return ScriptResult(exit_code=1, report=report)
    report["runtime_env_read"] = True

    modules = gate._import_collector_modules(report, module_importer)  # noqa: SLF001
    if modules is None:
        _set_status(
            report,
            "blocked_history_message_ingest_not_ready",
            "collector_import.failed",
        )
        _sync_side_effects(report)
        return ScriptResult(exit_code=1, report=report)

    if not gate._check_collector_config_and_singleton(report, modules, values):  # noqa: SLF001
        _set_status(
            report,
            "blocked_history_message_ingest_not_ready",
            "collector_config_or_singleton.invalid",
        )
        _sync_side_effects(report)
        return ScriptResult(exit_code=1, report=report)

    database_url = values.get("DATABASE_URL")
    if not database_url or not database_url.strip():
        _set_status(
            report,
            "blocked_history_message_ingest_not_ready",
            "database.url_missing",
        )
        _sync_side_effects(report)
        return ScriptResult(exit_code=1, report=report)
    if not gate._database_url_is_supported(database_url):  # noqa: SLF001
        _set_status(
            report,
            "blocked_history_message_ingest_not_ready",
            "database.url_unsupported",
        )
        _sync_side_effects(report)
        return ScriptResult(exit_code=1, report=report)

    connection: DatabaseConnection | None = None
    cleanup: Callable[[], None] | None = None
    transaction: Any | None = None
    try:
        try:
            connection, cleanup = gate._open_database_connection(  # noqa: SLF001
                database_url,
                database_connection_factory,
            )
            transaction = connection.begin()
            _execute_read(connection, gate.SET_TRANSACTION_READ_ONLY_QUERY)
            _execute_read(connection, gate.SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not gate._check_required_tables(report, connection):  # noqa: SLF001
                _set_status(
                    report,
                    "blocked_history_message_ingest_not_ready",
                    "database.required_tables_unavailable",
                )
                return ScriptResult(exit_code=1, report=report)
        except Exception:
            _set_status(
                report,
                "blocked_history_message_ingest_not_ready",
                "database.connection",
            )
            return ScriptResult(exit_code=1, report=report)

        joined_count = gate._count_joined_rows(connection)  # noqa: SLF001
        report["joined_rows_checked"] = True
        report["joined_row_count_bucket"] = _bucket_count(joined_count)
        if joined_count <= 0:
            _set_status(
                report,
                "blocked_history_message_ingest_not_ready",
                "registry.no_active_joined_rows",
            )
            return ScriptResult(exit_code=1, report=report)

        chat_ids = _select_history_target_chat_ids(
            connection,
            limit=history_ingest_max_chats,
        )
        report["history_ingest_selected_chats_bucket"] = _bucket_count(len(chat_ids))
        if not chat_ids:
            _set_status(
                report,
                "blocked_history_message_ingest_not_ready",
                "registry.no_selectable_joined_chat_ids",
            )
            return ScriptResult(exit_code=1, report=report)

        if (
            not approved_history_message_ingest_smoke
            or not approved_history_message_ingest_db_write
        ):
            _set_status(
                report,
                "joined_channel_collector_bounded_history_message_ingest_ready",
            )
            return ScriptResult(exit_code=0, report=report)

        if not approved_tdlib_readiness_probe:
            report["history_ingest_status"] = "blocked_not_ready"
            _set_status(
                report,
                "blocked_history_message_ingest_not_ready",
                "approval.tdlib_readiness_probe_required",
            )
            return ScriptResult(exit_code=1, report=report)

        try:
            asyncio.run(
                _run_history_ingest(
                    report=report,
                    values=values,
                    chat_ids=chat_ids,
                    history_limit=history_ingest_history_limit,
                    max_messages=history_ingest_max_messages,
                    max_db_writes=history_ingest_max_db_writes,
                    tdlib_auth_max_updates=tdlib_auth_max_updates,
                    tdlib_receive_timeout_sec=tdlib_receive_timeout_sec,
                    tdlib_overall_timeout_sec=tdlib_overall_timeout_sec,
                    history_probe_factory=history_probe_factory,
                    repository_context_factory=repository_context_factory,
                )
            )
        except Exception:
            if _forbidden_side_effect_detected(report):
                _set_status(
                    report,
                    "blocked_forbidden_side_effect_detected",
                    "tdlib.forbidden_request",
                )
            else:
                report["history_ingest_status"] = "failed"
                _set_status(
                    report,
                    "blocked_history_message_ingest_failed",
                    "history_ingest.unexpected_failure",
                )
            return ScriptResult(exit_code=1, report=report)

        if _forbidden_side_effect_detected(report):
            _set_status(
                report,
                "blocked_forbidden_side_effect_detected",
                "forbidden_side_effect",
            )
            return ScriptResult(exit_code=1, report=report)
        if report["contract_status"].startswith("blocked_"):
            return ScriptResult(exit_code=1, report=report)
        return ScriptResult(exit_code=0, report=report)
    except Exception:
        _set_status(
            report,
            "blocked_history_message_ingest_failed",
            "unexpected",
        )
        return ScriptResult(exit_code=1, report=report)
    finally:
        gate._rollback_transaction(transaction)  # noqa: SLF001
        gate._close_connection(cleanup, connection)  # noqa: SLF001
        _sync_side_effects(report)


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        approved_tdlib_readiness_probe=args.approved_tdlib_readiness_probe,
        approved_history_message_ingest_smoke=(
            args.approved_history_message_ingest_smoke
        ),
        approved_history_message_ingest_db_write=(
            args.approved_history_message_ingest_db_write
        ),
        history_ingest_max_chats=args.history_ingest_max_chats,
        history_ingest_history_limit=args.history_ingest_history_limit,
        history_ingest_max_messages=args.history_ingest_max_messages,
        history_ingest_max_db_writes=args.history_ingest_max_db_writes,
        tdlib_auth_max_updates=args.tdlib_auth_max_updates,
        tdlib_receive_timeout_sec=args.tdlib_receive_timeout_sec,
        tdlib_overall_timeout_sec=args.tdlib_overall_timeout_sec,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
