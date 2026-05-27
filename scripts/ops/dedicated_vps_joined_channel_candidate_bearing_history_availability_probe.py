from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_joined_channel_candidate_bearing_history_availability_probe"
REPORT_TYPE = "joined_channel_candidate_bearing_history_availability_probe_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_MAX_CHATS = 3
MAX_CHATS_HARD_LIMIT = 10
DEFAULT_HISTORY_LIMIT_PER_CHAT = 20
MAX_HISTORY_LIMIT_PER_CHAT = 100
DEFAULT_TDLIB_AUTH_MAX_UPDATES = 200
DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC = 1.0
DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC = 240.0
DEFAULT_HISTORY_RPC_MAX_UPDATES = 120
DEFAULT_HISTORY_RPC_MAX_DURATION_SEC = 60.0
TDLIB_READY_STATE = "authorizationStateReady"

STATUS_READY = "joined_channel_candidate_bearing_history_availability_probe_ready"
STATUS_CANDIDATE_FOUND = (
    "joined_channel_candidate_bearing_history_availability_probe_candidate_found"
)
STATUS_NO_CANDIDATE_FOUND = (
    "joined_channel_candidate_bearing_history_availability_probe_no_candidate_found"
)
STATUS_NO_JOINED_CHANNELS = (
    "joined_channel_candidate_bearing_history_availability_probe_no_joined_channels"
)
STATUS_BLOCKED_NOT_READY = (
    "blocked_joined_channel_candidate_bearing_history_availability_probe_not_ready"
)
STATUS_BLOCKED_SIDE_EFFECT = "blocked_forbidden_side_effect_detected"

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
SELECT_JOINED_CHANNEL_ROWS_LIMIT_QUERY = """
SELECT registry_id, chat_id
FROM telegram_channel_registry
WHERE desired_state = 'active'
  AND access_state = 'joined'
  AND chat_id IS NOT NULL
ORDER BY priority_weight DESC, registry_id ASC
LIMIT :limit
"""

AUTH_SUBMISSION_REQUEST_TYPES = frozenset(
    {
        "setAuthenticationPhoneNumber",
        "checkAuthenticationCode",
        "checkAuthenticationPassword",
    }
)
FORBIDDEN_TDLIB_REQUEST_TYPES = frozenset(
    {
        "joinChat",
        "joinChatByInviteLink",
        "searchPublicChat",
        "sendMessage",
        "getMessageLink",
    }
)
ACCESS_DENIED_ERROR_MARKERS = (
    "FORBIDDEN",
    "CHANNEL_PRIVATE",
    "USER_BANNED_IN_CHANNEL",
    "CHAT_WRITE_FORBIDDEN",
    "USER_NOT_PARTICIPANT",
)
SAFE_TDLIB_OBJECT_TYPE_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,80}\Z")
URL_RE = re.compile(r"https?://[^\s<>'\")\]]+", re.IGNORECASE)

SIDE_EFFECT_REPORT_FIELDS = (
    "source_tables_mutation_performed",
    "telegram_raw_updates_mutation_performed",
    "registry_mutation_performed",
    "normalizer_tables_mutation_performed",
    "event_outbox_mutation_performed",
    "redis_mutation_performed",
    "downstream_service_started",
    "external_network_attempted",
    "docker_or_systemd_changed",
    "alembic_run",
    "raw_values_emitted",
)


class AsyncSessionLike(Protocol):
    async def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> Any: ...

    async def rollback(self) -> None: ...

    async def close(self) -> None: ...


class HistoryProbe(Protocol):
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
    ) -> "HistoryFetchResult": ...

    async def close(self) -> None: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]
HistoryProbeFactory = Callable[[Mapping[str, str], int, float, float], HistoryProbe]
ShortUrlResolverFactory = Callable[[], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class JoinedChannelRow:
    registry_id: str
    chat_id: int


@dataclass(frozen=True, slots=True)
class HistoryFetchResult:
    status: str
    messages: tuple[Mapping[str, Any], ...] = ()
    request_types_sent: tuple[str, ...] = ("getChatHistory",)
    failure_class: str | None = None


@dataclass(frozen=True, slots=True)
class CandidateLocator:
    chat_id: int
    message_id: int
    message_date: int | None
    registry_id: str | None

    def as_private_json(self) -> dict[str, Any]:
        return {
            "chat_id": self.chat_id,
            "message_id": self.message_id,
            "message_date": self.message_date,
            "registry_id": self.registry_id,
        }


@dataclass(frozen=True, slots=True)
class PlanningResult:
    signal_detected: bool
    candidate_eligible: bool
    suppression_only: bool
    artifact_count: int
    candidate_group_count: int
    has_github_route: bool
    has_x_route: bool
    has_web_route: bool
    text_idea_only: bool
    raw_artifacts: tuple[Any, ...]


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.collector_telegram.message_projection import (  # noqa: E402
    MessageProjectionBuilder,
)
from src.services.router_normalizer.canonicalizer import (  # noqa: E402
    build_text_idea_artifact,
    canonicalize_resolved_urls,
)
from src.services.router_normalizer.models import ResolvedUrl, SourceMessageSnapshot  # noqa: E402
from src.services.router_normalizer.service import _with_inferred_repo_anchors  # noqa: E402
from src.services.router_normalizer.text_surfaces import build_text_surfaces  # noqa: E402
from src.services.router_normalizer.trigger_rules import evaluate_triggers  # noqa: E402
from src.services.router_normalizer.url_extraction import extract_urls  # noqa: E402


class _DefaultDatabaseSession:
    def __init__(self, engine: Any, session: Any) -> None:
        self._engine = engine
        self._session = session

    async def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._session.execute(statement, params or {})

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()
        await self._engine.dispose()


class _NoNetworkShortUrlResolver:
    async def resolve(self, url: Any) -> ResolvedUrl:
        return ResolvedUrl(
            observed_url=url.observed_url,
            normalized_url=_strip_url_fragment(url.observed_url),
            resolved_url=None,
            source_kind=url.source_kind,
            context_path=url.context_path,
            resolution_status="network_disabled",
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "No-write joined-channel Telegram history availability probe for real "
            "candidate-bearing messages. Default mode is DB-readiness only. "
            "Live getChatHistory reads require all approval flags."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument(
        "--max-chats",
        type=_positive_int_named("max-chats"),
        default=DEFAULT_MAX_CHATS,
    )
    parser.add_argument(
        "--history-limit-per-chat",
        type=_positive_int_named("history-limit-per-chat"),
        default=DEFAULT_HISTORY_LIMIT_PER_CHAT,
    )
    parser.add_argument(
        "--approved-candidate-bearing-history-probe",
        action="store_true",
    )
    parser.add_argument(
        "--approved-tdlib-existing-session-read",
        action="store_true",
    )
    parser.add_argument(
        "--approved-get-chat-history",
        action="store_true",
    )
    parser.add_argument("--private-candidate-locator-output")
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


def _base_report(
    *,
    approved_candidate_bearing_history_probe: bool,
    approved_tdlib_existing_session_read: bool,
    approved_get_chat_history: bool,
    private_locator_path_configured: bool,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_BLOCKED_NOT_READY,
        "checks_failed": [],
        "approved_candidate_bearing_history_probe": approved_candidate_bearing_history_probe,
        "approved_tdlib_existing_session_read": approved_tdlib_existing_session_read,
        "approved_get_chat_history": approved_get_chat_history,
        "runtime_env_read": False,
        "database_connected": False,
        "read_only_transaction": False,
        "tdlib_connection_attempted": False,
        "tdlib_ready": False,
        "joined_channels_available_bucket": "zero",
        "selected_channels_bucket": "zero",
        "history_requests_attempted_bucket": "zero",
        "history_requests_succeeded_bucket": "zero",
        "history_requests_failed_bucket": "zero",
        "history_messages_seen_bucket": "zero",
        "history_messages_projected_bucket": "zero",
        "signal_detected_history_messages_bucket": "zero",
        "candidate_eligible_history_messages_bucket": "zero",
        "suppression_only_history_messages_bucket": "zero",
        "planned_artifacts_bucket": "zero",
        "planned_candidate_groups_bucket": "zero",
        "planned_github_route_bucket": "zero",
        "planned_x_route_bucket": "zero",
        "planned_web_route_bucket": "zero",
        "planned_text_idea_bucket": "zero",
        "private_locator_path_configured": private_locator_path_configured,
        "private_locator_written": False,
        "tdlib_forbidden_request_detected": False,
        "tdlib_auth_attempted": False,
    }
    for field in SIDE_EFFECT_REPORT_FIELDS:
        report[field] = False
    return report


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def parse_runtime_env_text(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        values[key] = _strip_optional_quotes(raw_value)
    return values


def parse_runtime_env_file(path: str | Path) -> dict[str, str]:
    return parse_runtime_env_text(Path(path).read_text(encoding="utf-8", errors="replace"))


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def _read_runtime_env(
    path: str | Path,
    runtime_env_reader: RuntimeEnvReader | None,
) -> Mapping[str, str]:
    if runtime_env_reader is not None:
        return runtime_env_reader(path)
    return parse_runtime_env_file(path)


def _database_url_is_supported(database_url: str) -> bool:
    scheme_match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", database_url)
    if not scheme_match:
        return False
    scheme = scheme_match.group(1).lower()
    return scheme == "postgresql" or scheme.startswith("postgresql+")


def _transaction_read_only_enabled(raw_value: Any) -> bool:
    return str(raw_value).strip().lower() in {"on", "true", "1", "yes"}


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def _sql(statement: str) -> Any:
    from sqlalchemy import text  # type: ignore[import-not-found]

    return text(statement)


async def _open_default_database_session(database_url: str) -> AsyncSessionLike:
    from sqlalchemy.ext.asyncio import (  # type: ignore[import-not-found]
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    engine = create_async_engine(database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return _DefaultDatabaseSession(engine, session_factory())


async def _open_database_session(
    database_url: str,
    database_session_factory: DatabaseSessionFactory | None,
) -> AsyncSessionLike:
    if database_session_factory is not None:
        return await _maybe_await(database_session_factory(database_url))
    return await _open_default_database_session(database_url)


async def _close_database_session(session: AsyncSessionLike | None) -> None:
    if session is not None:
        await _maybe_await(session.close())


async def _execute(
    session: AsyncSessionLike,
    statement: str,
    params: dict[str, Any] | None = None,
) -> Any:
    return await session.execute(_sql(statement), params or {})


def _first_mapping(result: Any) -> Mapping[str, Any] | None:
    if hasattr(result, "mappings"):
        mappings = result.mappings()
        if hasattr(mappings, "first"):
            return mappings.first()
        if hasattr(mappings, "all"):
            rows = list(mappings.all())
            return rows[0] if rows else None
    if hasattr(result, "fetchall"):
        rows = list(result.fetchall())
        return rows[0] if rows else None
    if isinstance(result, list):
        return result[0] if result else None
    return None


def _all_mappings(result: Any) -> list[Mapping[str, Any]]:
    if hasattr(result, "mappings"):
        mappings = result.mappings()
        if hasattr(mappings, "all"):
            return list(mappings.all())
        if hasattr(mappings, "first"):
            row = mappings.first()
            return [] if row is None else [row]
    if hasattr(result, "fetchall"):
        return list(result.fetchall())
    if isinstance(result, list):
        return list(result)
    first = _first_mapping(result)
    return [] if first is None else [first]


async def _scalar(result: Any) -> Any:
    if hasattr(result, "scalar_one_or_none"):
        return result.scalar_one_or_none()
    if hasattr(result, "scalar"):
        return result.scalar()
    row = _first_mapping(result)
    if not row:
        return None
    if hasattr(row, "_mapping"):
        return next(iter(row._mapping.values()))
    if isinstance(row, Mapping):
        return next(iter(row.values()))
    if isinstance(row, (tuple, list)):
        return row[0] if row else None
    return row


async def _check_registry_table(session: AsyncSessionLike) -> bool:
    return bool(
        await _scalar(
            await _execute(
                session,
                TABLE_AVAILABLE_QUERY,
                {"qualified_table_name": "public.telegram_channel_registry"},
            )
        )
    )


async def _load_joined_channel_rows(
    *,
    session: AsyncSessionLike,
    max_chats: int,
) -> list[JoinedChannelRow]:
    result = await _execute(
        session,
        SELECT_JOINED_CHANNEL_ROWS_LIMIT_QUERY,
        {"limit": max_chats},
    )
    rows: list[JoinedChannelRow] = []
    for row in _all_mappings(result):
        registry_id = _string_or_none(row.get("registry_id"))
        if registry_id is None:
            continue
        raw_chat_id = row.get("chat_id")
        if isinstance(raw_chat_id, bool):
            continue
        try:
            chat_id = int(raw_chat_id)
        except (TypeError, ValueError):
            continue
        rows.append(JoinedChannelRow(registry_id=registry_id, chat_id=chat_id))
    return rows[:max_chats]


class TDLibCandidateHistoryProbe:
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
    ) -> HistoryFetchResult:
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
            return HistoryFetchResult(status="failed", failure_class="transport_error")
        self._sync_flags()
        result = await self._receive_history_response(extra)
        self._sync_flags()
        return result

    async def _receive_history_response(self, extra: str) -> HistoryFetchResult:
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
                return HistoryFetchResult(status="failed", failure_class="transport_error")
            if payload is None or not isinstance(payload, Mapping):
                continue
            state_type = _authorization_state_type_from_payload(payload)
            if state_type is not None and state_type != TDLIB_READY_STATE:
                return HistoryFetchResult(status="failed", failure_class="authorization_lost")
            if payload.get("@extra") != extra:
                continue
            response_type = _response_type_from_payload(payload)
            if response_type == "error":
                if _is_access_denied_error(payload):
                    return HistoryFetchResult(status="failed", failure_class="access_denied")
                return HistoryFetchResult(status="failed", failure_class="tdlib_error")
            if response_type == "messages":
                messages = _messages_from_payload(payload)
                return HistoryFetchResult(status="history" if messages else "empty", messages=messages)
            return HistoryFetchResult(status="failed", failure_class="response_shape")
        return HistoryFetchResult(status="failed", failure_class="response_timeout")

    def _next_extra(self, label: str) -> str:
        self._request_sequence += 1
        return f"{SCRIPT_NAME}.{label}.{self._request_sequence}"

    def _sync_flags(self) -> None:
        self.tdlib_send_called = bool(getattr(self._resolver, "tdlib_send_called", False))
        self.tdlib_receive_called = bool(
            getattr(self._resolver, "tdlib_receive_called", False)
        )


def _default_history_probe_factory(
    runtime_env: Mapping[str, str],
    tdlib_auth_max_updates: int,
    tdlib_receive_timeout_sec: float,
    tdlib_overall_timeout_sec: float,
) -> HistoryProbe:
    return TDLibCandidateHistoryProbe(
        runtime_env,
        auth_max_updates=tdlib_auth_max_updates,
        receive_timeout_sec=tdlib_receive_timeout_sec,
        overall_timeout_sec=tdlib_overall_timeout_sec,
    )


async def _run_live_history_probe(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    channel_rows: Sequence[JoinedChannelRow],
    history_limit_per_chat: int,
    private_locator_output: str | Path | None,
    tdlib_auth_max_updates: int,
    tdlib_receive_timeout_sec: float,
    tdlib_overall_timeout_sec: float,
    history_probe_factory: HistoryProbeFactory | None,
    short_url_resolver_factory: ShortUrlResolverFactory | None,
    raw_values: set[str],
) -> int:
    report["tdlib_connection_attempted"] = True
    probe: HistoryProbe | None = None
    selected_count = len(channel_rows)
    attempted = 0
    succeeded = 0
    failed = 0
    messages_seen = 0
    messages_projected = 0
    plans: list[PlanningResult] = []
    candidate_locator: CandidateLocator | None = None
    try:
        factory = history_probe_factory or _default_history_probe_factory
        probe = factory(
            values,
            tdlib_auth_max_updates,
            tdlib_receive_timeout_sec,
            tdlib_overall_timeout_sec,
        )
        await probe.initialize()
        _merge_tdlib_ready_fields(report, probe)
        if _forbidden_tdlib_request_detected(report):
            _set_status(report, STATUS_BLOCKED_SIDE_EFFECT, "tdlib.forbidden_request")
            return 1
        if not report["tdlib_ready"]:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "tdlib.not_ready")
            return 1

        for channel_row in channel_rows:
            attempted += 1
            try:
                result = await probe.fetch_chat_history(
                    chat_id=channel_row.chat_id,
                    limit=history_limit_per_chat,
                )
            except Exception:
                failed += 1
                continue
            _apply_tdlib_request_types(report, result.request_types_sent)
            _merge_tdlib_ready_fields(report, probe)
            if _forbidden_tdlib_request_detected(report):
                _set_status(report, STATUS_BLOCKED_SIDE_EFFECT, "tdlib.forbidden_request")
                return 1
            if result.status in {"history", "empty"}:
                succeeded += 1
            else:
                failed += 1
                continue

            for message in result.messages:
                messages_seen += 1
                _collect_raw_values_from_tdlib_message(message, raw_values)
                snapshot = _project_tdlib_message(message)
                if snapshot is None:
                    continue
                messages_projected += 1
                _collect_raw_values_from_snapshot(snapshot, raw_values)
                plan = await _build_planning_result(snapshot, short_url_resolver_factory)
                _collect_raw_values_from_plan(plan, raw_values)
                plans.append(plan)
                if candidate_locator is None and plan.candidate_eligible:
                    locator = _locator_from_message(
                        message,
                        registry_id=channel_row.registry_id,
                    )
                    if locator is not None:
                        candidate_locator = locator
    finally:
        await _close_probe(probe)
        _merge_tdlib_ready_fields(report, probe)

    report["selected_channels_bucket"] = _bucket_count(selected_count)
    report["history_requests_attempted_bucket"] = _bucket_count(attempted)
    report["history_requests_succeeded_bucket"] = _bucket_count(succeeded)
    report["history_requests_failed_bucket"] = _bucket_count(failed)
    report["history_messages_seen_bucket"] = _bucket_count(messages_seen)
    report["history_messages_projected_bucket"] = _bucket_count(messages_projected)
    _apply_plans_to_report(report=report, plans=plans)

    if attempted > 0 and succeeded == 0:
        _set_status(report, STATUS_BLOCKED_NOT_READY, "history.all_reads_failed")
        return 1

    if any(plan.candidate_eligible for plan in plans):
        if private_locator_output is not None and candidate_locator is not None:
            if not _write_private_locator(private_locator_output, candidate_locator):
                _set_status(report, STATUS_BLOCKED_NOT_READY, "private_locator.write_failed")
                return 1
            report["private_locator_written"] = True
        _set_status(report, STATUS_CANDIDATE_FOUND)
        return 0

    _set_status(report, STATUS_NO_CANDIDATE_FOUND)
    return 0


async def _close_probe(probe: HistoryProbe | None) -> None:
    if probe is None:
        return
    try:
        await probe.close()
    except Exception:
        return


def _project_tdlib_message(message: Mapping[str, Any]) -> SourceMessageSnapshot | None:
    if message.get("@type") != "message":
        return None
    try:
        projection = MessageProjectionBuilder().build_source_projection(dict(message))
    except Exception:
        return None
    content = message.get("content")
    deleted_at = None
    if isinstance(content, Mapping) and content.get("@type") == "messageDeleted":
        deleted_at = datetime.now(timezone.utc)
    return SourceMessageSnapshot(
        source_message_id=uuid.uuid4(),
        source_version_no=1,
        text_body=projection.text_body,
        caption_text=projection.caption_text,
        text_surface=projection.text_surface,
        entities_json=projection.entities_json,
        url_surface_json=projection.url_surface_json,
        raw_message_json=projection.raw_message_json,
        deleted_at=deleted_at,
    )


async def _build_planning_result(
    snapshot: SourceMessageSnapshot,
    resolver_factory: ShortUrlResolverFactory | None,
) -> PlanningResult:
    if snapshot.deleted_at is not None:
        return PlanningResult(
            signal_detected=False,
            candidate_eligible=False,
            suppression_only=True,
            artifact_count=0,
            candidate_group_count=0,
            has_github_route=False,
            has_x_route=False,
            has_web_route=False,
            text_idea_only=False,
            raw_artifacts=(),
        )

    surfaces = build_text_surfaces(snapshot)
    extracted_urls = extract_urls(snapshot, surfaces)
    resolver = resolver_factory() if resolver_factory is not None else _NoNetworkShortUrlResolver()
    resolved_urls = [await _maybe_await(resolver.resolve(url)) for url in extracted_urls]
    artifacts = _with_inferred_repo_anchors(canonicalize_resolved_urls(resolved_urls))
    evaluation = evaluate_triggers(surfaces, artifacts)
    if evaluation.candidate_eligible and not artifacts:
        artifacts = [build_text_idea_artifact(surfaces)]

    provider_routes = {artifact.provider_route for artifact in artifacts}
    artifact_types = {artifact.artifact_type for artifact in artifacts}
    return PlanningResult(
        signal_detected=evaluation.signal_detected,
        candidate_eligible=evaluation.candidate_eligible,
        suppression_only=not evaluation.candidate_eligible and evaluation.signal_detected,
        artifact_count=len(artifacts) if evaluation.candidate_eligible else 0,
        candidate_group_count=_planned_candidate_group_count(artifacts)
        if evaluation.candidate_eligible
        else 0,
        has_github_route=evaluation.candidate_eligible and "github" in provider_routes,
        has_x_route=evaluation.candidate_eligible and "x" in provider_routes,
        has_web_route=evaluation.candidate_eligible and "web" in provider_routes,
        text_idea_only=evaluation.candidate_eligible and artifact_types == {"text_idea"},
        raw_artifacts=tuple(artifacts),
    )


def _planned_candidate_group_count(artifacts: Sequence[Any]) -> int:
    primary_ids: set[str] = set()
    for artifact in artifacts:
        if (
            artifact.artifact_type in {"github_subpath", "github_repo_page"}
            and artifact.inferred_repo is not None
        ):
            primary_ids.add(artifact.inferred_repo.canonical_id)
        else:
            primary_ids.add(artifact.canonical_id)
    return len(primary_ids)


def _apply_plans_to_report(
    *,
    report: dict[str, Any],
    plans: Sequence[PlanningResult],
) -> None:
    report["signal_detected_history_messages_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.signal_detected)
    )
    report["candidate_eligible_history_messages_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.candidate_eligible)
    )
    report["suppression_only_history_messages_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.suppression_only)
    )
    report["planned_artifacts_bucket"] = _bucket_count(
        sum(plan.artifact_count for plan in plans)
    )
    report["planned_candidate_groups_bucket"] = _bucket_count(
        sum(plan.candidate_group_count for plan in plans)
    )
    report["planned_github_route_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.has_github_route)
    )
    report["planned_x_route_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.has_x_route)
    )
    report["planned_web_route_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.has_web_route)
    )
    report["planned_text_idea_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.text_idea_only)
    )


def _approval_count(
    *,
    approved_candidate_bearing_history_probe: bool,
    approved_tdlib_existing_session_read: bool,
    approved_get_chat_history: bool,
) -> int:
    return sum(
        [
            bool(approved_candidate_bearing_history_probe),
            bool(approved_tdlib_existing_session_read),
            bool(approved_get_chat_history),
        ]
    )


def _bounded(value: int, *, default: int, hard_limit: int) -> int:
    if isinstance(value, bool):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(1, min(parsed, hard_limit))


def _apply_side_effect_flags(
    report: dict[str, Any],
    side_effect_flags: Mapping[str, bool] | None,
) -> None:
    if not side_effect_flags:
        return
    for field in SIDE_EFFECT_REPORT_FIELDS:
        if bool(side_effect_flags.get(field, False)):
            report[field] = True


def _forbidden_side_effect_detected(report: Mapping[str, Any]) -> bool:
    return any(bool(report[field]) for field in SIDE_EFFECT_REPORT_FIELDS)


def _merge_tdlib_ready_fields(report: dict[str, Any], probe: HistoryProbe | None) -> None:
    if probe is None:
        return
    if bool(getattr(probe, "tdlib_send_called", False)) or bool(
        getattr(probe, "tdlib_receive_called", False)
    ):
        report["tdlib_connection_attempted"] = True
    try:
        summary = probe.tdlib_ready_probe_summary
    except Exception:
        summary = {}
    if not isinstance(summary, Mapping):
        summary = {}
    report["tdlib_ready"] = (
        summary.get("tdlib_ready_probe_status") == "ready"
        and summary.get("tdlib_ready_probe_final_authorization_state")
        == TDLIB_READY_STATE
    )
    request_types = _safe_text_list(summary.get("tdlib_ready_probe_request_types_sent"))
    _apply_tdlib_request_types(report, request_types)


def _apply_tdlib_request_types(report: dict[str, Any], request_types: Sequence[str]) -> None:
    for request_type in request_types:
        safe_request_type = _safe_tdlib_object_type(request_type)
        if safe_request_type is None:
            continue
        if safe_request_type in AUTH_SUBMISSION_REQUEST_TYPES:
            report["tdlib_auth_attempted"] = True
            report["tdlib_forbidden_request_detected"] = True
        elif safe_request_type in FORBIDDEN_TDLIB_REQUEST_TYPES:
            report["tdlib_forbidden_request_detected"] = True


def _forbidden_tdlib_request_detected(report: Mapping[str, Any]) -> bool:
    return bool(
        report.get("tdlib_auth_attempted")
        or report.get("tdlib_forbidden_request_detected")
    )


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


def _messages_from_payload(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw_messages = payload.get("messages")
    if not isinstance(raw_messages, Sequence) or isinstance(
        raw_messages,
        (str, bytes, bytearray),
    ):
        return ()
    return tuple(
        message
        for message in raw_messages
        if isinstance(message, Mapping) and message.get("@type") == "message"
    )


def _safe_tdlib_object_type(value: Any) -> str | None:
    if isinstance(value, str) and SAFE_TDLIB_OBJECT_TYPE_RE.fullmatch(value):
        return value
    return None


def _safe_text_list(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [item for item in value if isinstance(item, str)]


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _bucket_count(count: int | None) -> str:
    if count is None or count <= 0:
        return "zero"
    if count == 1:
        return "one"
    return "multiple"


def _locator_from_message(
    message: Mapping[str, Any],
    *,
    registry_id: str | None,
) -> CandidateLocator | None:
    raw_chat_id = message.get("chat_id")
    raw_message_id = message.get("id")
    if isinstance(raw_chat_id, bool) or isinstance(raw_message_id, bool):
        return None
    try:
        chat_id = int(raw_chat_id)
        message_id = int(raw_message_id)
    except (TypeError, ValueError):
        return None
    raw_date = message.get("date")
    message_date = None
    if isinstance(raw_date, int) and not isinstance(raw_date, bool):
        message_date = raw_date
    return CandidateLocator(
        chat_id=chat_id,
        message_id=message_id,
        message_date=message_date,
        registry_id=registry_id,
    )


def _write_private_locator(path: str | Path, locator: CandidateLocator) -> bool:
    try:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(locator.as_private_json(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return True
    except Exception:
        return False


def _collect_raw_values_from_tdlib_message(
    message: Mapping[str, Any],
    raw_values: set[str],
) -> None:
    for key in ("chat_id", "id", "date"):
        value = message.get(key)
        if value is not None:
            raw_values.add(str(value))
    content = message.get("content")
    if isinstance(content, Mapping):
        _collect_raw_strings(content, raw_values)
    raw_values.add(json.dumps(message, sort_keys=True, default=str))


def _collect_raw_values_from_snapshot(
    snapshot: SourceMessageSnapshot,
    raw_values: set[str],
) -> None:
    raw_values.add(str(snapshot.source_message_id))
    for value in (snapshot.text_body, snapshot.caption_text, snapshot.text_surface):
        if isinstance(value, str):
            raw_values.add(value)
            for url in URL_RE.findall(value):
                raw_values.add(url.rstrip(".,;:!?"))
    raw_values.add(json.dumps(snapshot.raw_message_json, sort_keys=True, default=str))
    raw_values.add(json.dumps(snapshot.entities_json, sort_keys=True, default=str))
    raw_values.add(json.dumps(snapshot.url_surface_json, sort_keys=True, default=str))


def _collect_raw_values_from_plan(plan: PlanningResult, raw_values: set[str]) -> None:
    for artifact in plan.raw_artifacts:
        for value in (
            artifact.canonical_id,
            artifact.canonical_url,
            artifact.observed_url,
            artifact.normalized_url,
            artifact.resolved_url,
        ):
            if isinstance(value, str):
                raw_values.add(value)


def _collect_raw_strings(value: Any, raw_values: set[str]) -> None:
    if isinstance(value, str):
        raw_values.add(value)
        for url in URL_RE.findall(value):
            raw_values.add(url.rstrip(".,;:!?"))
        return
    if isinstance(value, Mapping):
        for nested in value.values():
            _collect_raw_strings(nested, raw_values)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for nested in value:
            _collect_raw_strings(nested, raw_values)


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(value in rendered for value in raw_values if len(value) >= 6)


def _strip_url_fragment(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    return parsed._replace(fragment="").geturl()


def _finalize_result(
    *,
    report: dict[str, Any],
    raw_values: set[str],
    exit_code: int,
) -> ScriptResult:
    if _report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_BLOCKED_SIDE_EFFECT, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)
    return ScriptResult(exit_code=exit_code, report=report)


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_chats: int = DEFAULT_MAX_CHATS,
    history_limit_per_chat: int = DEFAULT_HISTORY_LIMIT_PER_CHAT,
    approved_candidate_bearing_history_probe: bool = False,
    approved_tdlib_existing_session_read: bool = False,
    approved_get_chat_history: bool = False,
    private_candidate_locator_output: str | Path | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    history_probe_factory: HistoryProbeFactory | None = None,
    short_url_resolver_factory: ShortUrlResolverFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
    tdlib_auth_max_updates: int = DEFAULT_TDLIB_AUTH_MAX_UPDATES,
    tdlib_receive_timeout_sec: float = DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC,
    tdlib_overall_timeout_sec: float = DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC,
) -> ScriptResult:
    report = _base_report(
        approved_candidate_bearing_history_probe=approved_candidate_bearing_history_probe,
        approved_tdlib_existing_session_read=approved_tdlib_existing_session_read,
        approved_get_chat_history=approved_get_chat_history,
        private_locator_path_configured=private_candidate_locator_output is not None,
    )
    _apply_side_effect_flags(report, side_effect_flags)
    raw_values: set[str] = {value for value in forbidden_raw_values if len(value) >= 6}
    raw_values.add(str(runtime_env_path))
    if private_candidate_locator_output is not None:
        raw_values.add(str(private_candidate_locator_output))

    if _forbidden_side_effect_detected(report):
        _set_status(report, STATUS_BLOCKED_SIDE_EFFECT, "side_effect.forbidden")
        return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

    approvals = _approval_count(
        approved_candidate_bearing_history_probe=approved_candidate_bearing_history_probe,
        approved_tdlib_existing_session_read=approved_tdlib_existing_session_read,
        approved_get_chat_history=approved_get_chat_history,
    )
    if 0 < approvals < 3:
        _set_status(report, STATUS_BLOCKED_NOT_READY, "approval.partial")
        return _finalize_result(report=report, raw_values=raw_values, exit_code=1)
    live_mode = approvals == 3

    bounded_max_chats = _bounded(
        max_chats,
        default=DEFAULT_MAX_CHATS,
        hard_limit=MAX_CHATS_HARD_LIMIT,
    )
    bounded_history_limit = _bounded(
        history_limit_per_chat,
        default=DEFAULT_HISTORY_LIMIT_PER_CHAT,
        hard_limit=MAX_HISTORY_LIMIT_PER_CHAT,
    )

    session: AsyncSessionLike | None = None
    try:
        try:
            values = _read_runtime_env(runtime_env_path, runtime_env_reader)
            report["runtime_env_read"] = True
        except Exception:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "runtime_env.read")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

        database_url = str(values.get("DATABASE_URL", "")).strip()
        if database_url:
            raw_values.add(database_url)
        if not database_url:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "database.url_missing")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)
        if not _database_url_is_supported(database_url):
            _set_status(report, STATUS_BLOCKED_NOT_READY, "database.url_unsupported")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

        try:
            session = await _open_database_session(database_url, database_session_factory)
        except Exception:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "database.connection")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

        try:
            await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
            read_only_value = await _scalar(
                await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY)
            )
            report["read_only_transaction"] = _transaction_read_only_enabled(
                read_only_value
            )
        except Exception:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "database.read_only_transaction")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)
        if not report["read_only_transaction"]:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "database.read_only_transaction")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

        try:
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not await _check_registry_table(session):
                _set_status(report, STATUS_BLOCKED_NOT_READY, "database.registry_table")
                return _finalize_result(report=report, raw_values=raw_values, exit_code=1)
            channel_rows = await _load_joined_channel_rows(
                session=session,
                max_chats=bounded_max_chats,
            )
        except Exception:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "database.connection_or_schema")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

        for row in channel_rows:
            raw_values.add(row.registry_id)
            raw_values.add(str(row.chat_id))
        report["joined_channels_available_bucket"] = _bucket_count(len(channel_rows))
        report["selected_channels_bucket"] = _bucket_count(len(channel_rows))

        if not channel_rows:
            _set_status(report, STATUS_NO_JOINED_CHANNELS)
            return _finalize_result(report=report, raw_values=raw_values, exit_code=0)
        if not live_mode:
            _set_status(report, STATUS_READY)
            return _finalize_result(report=report, raw_values=raw_values, exit_code=0)

        exit_code = await _run_live_history_probe(
            report=report,
            values=values,
            channel_rows=channel_rows,
            history_limit_per_chat=bounded_history_limit,
            private_locator_output=private_candidate_locator_output,
            tdlib_auth_max_updates=tdlib_auth_max_updates,
            tdlib_receive_timeout_sec=tdlib_receive_timeout_sec,
            tdlib_overall_timeout_sec=tdlib_overall_timeout_sec,
            history_probe_factory=history_probe_factory,
            short_url_resolver_factory=short_url_resolver_factory,
            raw_values=raw_values,
        )
        if _forbidden_side_effect_detected(report):
            _set_status(report, STATUS_BLOCKED_SIDE_EFFECT, "side_effect.forbidden")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)
        return _finalize_result(report=report, raw_values=raw_values, exit_code=exit_code)
    except Exception:
        if session is not None:
            await _maybe_await(session.rollback())
        _set_status(report, STATUS_BLOCKED_NOT_READY, "unexpected")
        return _finalize_result(report=report, raw_values=raw_values, exit_code=1)
    finally:
        if session is not None:
            await _maybe_await(session.rollback())
        await _close_database_session(session)


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_chats: int = DEFAULT_MAX_CHATS,
    history_limit_per_chat: int = DEFAULT_HISTORY_LIMIT_PER_CHAT,
    approved_candidate_bearing_history_probe: bool = False,
    approved_tdlib_existing_session_read: bool = False,
    approved_get_chat_history: bool = False,
    private_candidate_locator_output: str | Path | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    history_probe_factory: HistoryProbeFactory | None = None,
    short_url_resolver_factory: ShortUrlResolverFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            max_chats=max_chats,
            history_limit_per_chat=history_limit_per_chat,
            approved_candidate_bearing_history_probe=approved_candidate_bearing_history_probe,
            approved_tdlib_existing_session_read=approved_tdlib_existing_session_read,
            approved_get_chat_history=approved_get_chat_history,
            private_candidate_locator_output=private_candidate_locator_output,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            history_probe_factory=history_probe_factory,
            short_url_resolver_factory=short_url_resolver_factory,
            side_effect_flags=side_effect_flags,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        max_chats=args.max_chats,
        history_limit_per_chat=args.history_limit_per_chat,
        approved_candidate_bearing_history_probe=(
            args.approved_candidate_bearing_history_probe
        ),
        approved_tdlib_existing_session_read=args.approved_tdlib_existing_session_read,
        approved_get_chat_history=args.approved_get_chat_history,
        private_candidate_locator_output=args.private_candidate_locator_output,
    )
    print(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
