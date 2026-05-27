from __future__ import annotations

import argparse
import asyncio
import json
import math
import re
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_candidate_bearing_history_locator_bounded_ingest_smoke"
REPORT_TYPE = "candidate_bearing_history_locator_bounded_ingest_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_HISTORY_LIMIT = 5
MAX_HISTORY_LIMIT = 20
DEFAULT_TDLIB_AUTH_MAX_UPDATES = 200
DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC = 1.0
DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC = 240.0
DEFAULT_HISTORY_RPC_MAX_UPDATES = 120
DEFAULT_HISTORY_RPC_MAX_DURATION_SEC = 60.0
TDLIB_READY_STATE = "authorizationStateReady"

STATUS_READY = "candidate_bearing_history_locator_bounded_ingest_smoke_ready"
STATUS_INGESTED = "candidate_bearing_history_locator_bounded_ingest_smoke_ingested"
STATUS_ALREADY_INGESTED = (
    "candidate_bearing_history_locator_bounded_ingest_smoke_already_ingested"
)
STATUS_BLOCKED_NOT_READY = (
    "blocked_candidate_bearing_history_locator_bounded_ingest_smoke_not_ready"
)
STATUS_BLOCKED_NOT_CANDIDATE = (
    "blocked_candidate_bearing_history_locator_bounded_ingest_smoke_not_candidate"
)
STATUS_BLOCKED_EXACT_MESSAGE_MISSING = (
    "blocked_candidate_bearing_history_locator_bounded_ingest_smoke_exact_message_missing"
)
STATUS_BLOCKED_SIDE_EFFECT = "blocked_forbidden_side_effect_detected"

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"

REQUIRED_TABLES = (
    "telegram_channel_registry",
    "source_messages",
    "source_message_versions",
    "event_outbox",
)

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
SAFE_EXCEPTION_CLASS_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,80}\Z")
URL_RE = re.compile(r"https?://[^\s<>'\")\]]+", re.IGNORECASE)

SIDE_EFFECT_REPORT_FIELDS = (
    "source_tables_mutation_performed",
    "telegram_raw_updates_mutation_performed",
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


class LocatorHistoryProbe(Protocol):
    tdlib_send_called: bool
    tdlib_receive_called: bool

    @property
    def tdlib_ready_probe_summary(self) -> Mapping[str, Any]: ...

    async def initialize(self) -> None: ...

    async def fetch_chat_history(
        self,
        *,
        chat_id: int,
        from_message_id: int,
        limit: int,
    ) -> "HistoryFetchResult": ...

    async def close(self) -> None: ...


class LocatorIngestRepository(Protocol):
    def transaction(self) -> Any: ...

    async def get_source_message(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
    ) -> Mapping[str, Any] | None: ...

    async def count_pending_source_events(self, *, source_message_id: str) -> int: ...

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
DatabaseSessionFactory = Callable[[str], Any]
LocatorExistsChecker = Callable[[str | Path], bool]
LocatorReader = Callable[[str | Path], str]
HistoryProbeFactory = Callable[[Mapping[str, str], int, float, float], LocatorHistoryProbe]
ShortUrlResolverFactory = Callable[[], Any]
RepositoryContextFactory = Callable[
    [Mapping[str, str]],
    AbstractAsyncContextManager[LocatorIngestRepository],
]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class CandidateLocator:
    chat_id: int
    message_id: int
    message_date: int | None
    registry_id: str | None


@dataclass(frozen=True, slots=True)
class HistoryFetchResult:
    status: str
    messages: tuple[Mapping[str, Any], ...] = ()
    request_types_sent: tuple[str, ...] = ("getChatHistory",)
    failure_class: str | None = None


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


@dataclass(slots=True)
class WriteDiagnostics:
    failure_class: str | None = None
    source_write_attempted: bool = False
    transaction_entered: bool = False
    get_existing_attempted: bool = False
    upsert_attempted: bool = False
    version_append_attempted: bool = False
    outbox_insert_attempted: bool = False
    pending_event_check_attempted: bool = False
    transaction_completed: bool = False

    def capture_exception(self, exc: BaseException) -> None:
        self.failure_class = _safe_exception_class_name(exc)


@dataclass(frozen=True, slots=True)
class WriteResult:
    status: str
    source_messages_written: int = 0
    source_message_versions_written: int = 0
    event_outbox_written: int = 0
    pending_source_events: int = 0
    existing_source_message: bool = False
    existing_event_outbox: bool = False
    blocked_existing_without_pending_outbox: bool = False


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


class TDLibLocatorHistoryProbe:
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
        from_message_id: int,
        limit: int,
    ) -> HistoryFetchResult:
        bounded_limit = max(1, min(int(limit), MAX_HISTORY_LIMIT))
        request = self._resolver._client.build_get_chat_history_request(  # noqa: SLF001
            chat_id=chat_id,
            from_message_id=from_message_id,
            offset=-(bounded_limit // 2),
            limit=bounded_limit,
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
                return HistoryFetchResult(
                    status="history" if messages else "empty",
                    messages=messages,
                )
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


class _DefaultLocatorIngestRepository:
    def __init__(self, repository: Any, session: Any) -> None:
        self._repository = repository
        self._session = session

    def transaction(self) -> Any:
        return self._repository.transaction()

    async def get_source_message(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
    ) -> Mapping[str, Any] | None:
        return await self._repository.get_source_message(
            platform=platform,
            chat_id=chat_id,
            message_id=message_id,
        )

    async def count_pending_source_events(self, *, source_message_id: str) -> int:
        from sqlalchemy import text  # noqa: PLC0415

        result = await self._session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM event_outbox
                WHERE aggregate_type = 'source_message'
                  AND aggregate_id = CAST(:source_message_id AS uuid)
                  AND event_type IN (
                    'source_message.created.v1',
                    'source_message.reconciled.v1'
                  )
                  AND status = 'pending'
                """
            ),
            {"source_message_id": source_message_id},
        )
        if hasattr(result, "scalar_one"):
            return int(result.scalar_one())
        if hasattr(result, "scalar"):
            return int(result.scalar() or 0)
        return 0

    async def upsert_source_message(
        self,
        projection: Any,
        *,
        platform: str = "telegram",
    ) -> Mapping[str, Any]:
        return await self._repository.upsert_source_message(
            projection,
            platform=platform,
        )

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: Any,
        version_reason: str,
        observed_at: datetime | None = None,
        telegram_edit_date: datetime | None = None,
    ) -> tuple[bool, Mapping[str, Any] | None]:
        return await self._repository.append_source_message_version_if_changed(
            source_message_id=source_message_id,
            projection=projection,
            version_reason=version_reason,
            observed_at=observed_at,
            telegram_edit_date=telegram_edit_date,
        )

    async def insert_outbox_event(self, event: Any) -> None:
        await self._repository.insert_outbox_event(event)


class _DefaultRepositoryContext:
    def __init__(self, runtime_env: Mapping[str, str]) -> None:
        self._runtime_env = runtime_env
        self._engine: Any | None = None
        self._session: Any | None = None

    async def __aenter__(self) -> LocatorIngestRepository:
        from sqlalchemy.ext.asyncio import (  # noqa: PLC0415
            async_sessionmaker,
            create_async_engine,
        )
        from src.services.collector_telegram.repositories import (  # noqa: PLC0415
            CollectorRepository,
        )

        database_url = str(self._runtime_env.get("DATABASE_URL", "")).strip()
        if not database_url:
            raise RuntimeError("DATABASE_URL is required for locator ingest")
        self._engine = create_async_engine(_async_database_url(database_url), future=True)
        session_factory = async_sessionmaker(self._engine, expire_on_commit=False)
        self._session = session_factory()
        return _DefaultLocatorIngestRepository(
            CollectorRepository(self._session),
            self._session,
        )

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Bounded candidate-bearing history locator ingest smoke. Default "
            "mode checks readiness without reading the private locator, TDLib, "
            "or mutating DB. Live mode requires all approval flags."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument(
        "--private-candidate-locator-input",
        required=True,
        help="Private locator file path. The path and contents are never emitted.",
    )
    parser.add_argument(
        "--history-limit",
        type=_bounded_positive_int_named("history-limit", upper_bound=MAX_HISTORY_LIMIT),
        default=DEFAULT_HISTORY_LIMIT,
    )
    parser.add_argument("--approved-candidate-locator-ingest-smoke", action="store_true")
    parser.add_argument("--approved-private-locator-read", action="store_true")
    parser.add_argument("--approved-tdlib-existing-session-read", action="store_true")
    parser.add_argument("--approved-get-chat-history", action="store_true")
    parser.add_argument("--approved-source-table-write", action="store_true")
    parser.add_argument("--approved-event-outbox-write", action="store_true")
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


def _base_report(
    *,
    private_locator_path_configured: bool,
    approved_candidate_locator_ingest_smoke: bool,
    approved_private_locator_read: bool,
    approved_tdlib_existing_session_read: bool,
    approved_get_chat_history: bool,
    approved_source_table_write: bool,
    approved_event_outbox_write: bool,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_BLOCKED_NOT_READY,
        "checks_failed": [],
        "approved_candidate_locator_ingest_smoke": approved_candidate_locator_ingest_smoke,
        "approved_private_locator_read": approved_private_locator_read,
        "approved_tdlib_existing_session_read": approved_tdlib_existing_session_read,
        "approved_get_chat_history": approved_get_chat_history,
        "approved_source_table_write": approved_source_table_write,
        "approved_event_outbox_write": approved_event_outbox_write,
        "runtime_env_read": False,
        "database_connected": False,
        "read_only_transaction": False,
        "required_tables_available": {table: False for table in REQUIRED_TABLES},
        "private_locator_path_configured": private_locator_path_configured,
        "private_locator_exists": False,
        "private_locator_read_attempted": False,
        "private_locator_shape_valid_bucket": "unknown",
        "tdlib_connection_attempted": False,
        "tdlib_ready": False,
        "history_request_attempted": False,
        "history_request_succeeded_bucket": "zero",
        "exact_message_found_bucket": "zero",
        "message_projected_bucket": "zero",
        "candidate_eligible_bucket": "zero",
        "signal_detected_bucket": "zero",
        "planned_artifacts_bucket": "zero",
        "planned_candidate_groups_bucket": "zero",
        "planned_github_route_bucket": "zero",
        "planned_x_route_bucket": "zero",
        "planned_web_route_bucket": "zero",
        "planned_text_idea_bucket": "zero",
        "source_write_attempted": False,
        "source_messages_written_bucket": "zero",
        "source_message_versions_written_bucket": "zero",
        "event_outbox_source_events_written_bucket": "zero",
        "event_outbox_pending_bucket": "zero",
        "existing_source_message_bucket": "zero",
        "existing_event_outbox_bucket": "zero",
        "blocked_existing_without_pending_outbox": False,
        "db_write_failure_class": None,
        "telegram_raw_updates_written_bucket": "zero",
        "source_tables_mutation_performed": False,
        "telegram_raw_updates_mutation_performed": False,
        "event_outbox_mutation_performed": False,
        "redis_mutation_performed": False,
        "downstream_service_started": False,
        "external_network_attempted": False,
        "docker_or_systemd_changed": False,
        "alembic_run": False,
        "raw_values_emitted": False,
        "tdlib_forbidden_request_detected": False,
        "tdlib_auth_attempted": False,
    }
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
        if line.startswith("export "):
            line = line[len("export ") :].strip()
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


def _async_database_url(database_url: str) -> str:
    if database_url.startswith("postgresql://"):
        return "postgresql+psycopg://" + database_url.removeprefix("postgresql://")
    return database_url


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

    engine = create_async_engine(_async_database_url(database_url), future=True)
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


async def _check_required_tables(
    *,
    session: AsyncSessionLike,
    report: dict[str, Any],
) -> bool:
    all_available = True
    for table in REQUIRED_TABLES:
        available = bool(
            await _scalar(
                await _execute(
                    session,
                    TABLE_AVAILABLE_QUERY,
                    {"qualified_table_name": f"public.{table}"},
                )
            )
        )
        report["required_tables_available"][table] = available
        all_available = all_available and available
    return all_available


def _locator_exists(path: str | Path) -> bool:
    try:
        candidate = Path(path)
        return candidate.exists() and candidate.is_file()
    except OSError:
        return False


def _read_locator_text(path: str | Path) -> str:
    return Path(path).read_text(encoding="utf-8")


def _parse_locator(
    raw_text: str,
    *,
    raw_values: set[str],
) -> CandidateLocator | None:
    try:
        payload = json.loads(raw_text)
    except Exception:
        return None
    if not isinstance(payload, Mapping):
        return None
    raw_chat_id = payload.get("chat_id")
    raw_message_id = payload.get("message_id")
    if isinstance(raw_chat_id, bool) or isinstance(raw_message_id, bool):
        return None
    try:
        chat_id = int(raw_chat_id)
        message_id = int(raw_message_id)
    except (TypeError, ValueError):
        return None
    if chat_id == 0 or message_id <= 0:
        return None
    raw_values.add(str(chat_id))
    raw_values.add(str(message_id))

    raw_message_date = payload.get("message_date")
    message_date: int | None = None
    if raw_message_date is not None:
        if isinstance(raw_message_date, bool):
            return None
        try:
            message_date = int(raw_message_date)
        except (TypeError, ValueError):
            return None
        raw_values.add(str(message_date))

    raw_registry_id = payload.get("registry_id")
    registry_id: str | None = None
    if raw_registry_id is not None:
        if not isinstance(raw_registry_id, str) or not raw_registry_id.strip():
            return None
        registry_id = raw_registry_id.strip()
        raw_values.add(registry_id)

    return CandidateLocator(
        chat_id=chat_id,
        message_id=message_id,
        message_date=message_date,
        registry_id=registry_id,
    )


def _default_history_probe_factory(
    runtime_env: Mapping[str, str],
    tdlib_auth_max_updates: int,
    tdlib_receive_timeout_sec: float,
    tdlib_overall_timeout_sec: float,
) -> LocatorHistoryProbe:
    return TDLibLocatorHistoryProbe(
        runtime_env,
        auth_max_updates=tdlib_auth_max_updates,
        receive_timeout_sec=tdlib_receive_timeout_sec,
        overall_timeout_sec=tdlib_overall_timeout_sec,
    )


def _default_repository_context_factory(
    runtime_env: Mapping[str, str],
) -> AbstractAsyncContextManager[LocatorIngestRepository]:
    return _DefaultRepositoryContext(runtime_env)


async def _close_probe(probe: LocatorHistoryProbe | None) -> None:
    if probe is None:
        return
    try:
        await probe.close()
    except Exception:
        return


def _safe_exception_class_name(exc: BaseException) -> str:
    class_name = exc.__class__.__name__
    if SAFE_EXCEPTION_CLASS_RE.fullmatch(class_name):
        return class_name
    return "unknown"


def _safe_tdlib_object_type(value: Any) -> str | None:
    if isinstance(value, str) and SAFE_TDLIB_OBJECT_TYPE_RE.fullmatch(value):
        return value
    return None


def _safe_text_list(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [item for item in value if isinstance(item, str)]


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


def _merge_tdlib_ready_fields(
    report: dict[str, Any],
    probe: LocatorHistoryProbe | None,
) -> None:
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
    _apply_tdlib_request_types(
        report,
        _safe_text_list(summary.get("tdlib_ready_probe_request_types_sent")),
    )


def _apply_tdlib_request_types(
    report: dict[str, Any],
    request_types: Sequence[str],
) -> None:
    for request_type in request_types:
        safe_request_type = _safe_tdlib_object_type(request_type)
        if safe_request_type is None:
            continue
        if safe_request_type in AUTH_SUBMISSION_REQUEST_TYPES:
            report["tdlib_auth_attempted"] = True
            report["tdlib_forbidden_request_detected"] = True
        elif safe_request_type in FORBIDDEN_TDLIB_REQUEST_TYPES:
            report["tdlib_forbidden_request_detected"] = True


def _forbidden_side_effect_detected(
    report: Mapping[str, Any],
    *,
    allow_source_and_outbox: bool = False,
) -> bool:
    allowed = (
        {"source_tables_mutation_performed", "event_outbox_mutation_performed"}
        if allow_source_and_outbox
        else set()
    )
    return any(
        bool(report[field])
        for field in SIDE_EFFECT_REPORT_FIELDS
        if field not in allowed
    )


def _forbidden_tdlib_request_detected(report: Mapping[str, Any]) -> bool:
    return bool(
        report.get("tdlib_auth_attempted")
        or report.get("tdlib_forbidden_request_detected")
    )


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


def _apply_plan_to_report(report: dict[str, Any], plan: PlanningResult | None) -> None:
    if plan is None:
        return
    report["signal_detected_bucket"] = _bucket_count(1 if plan.signal_detected else 0)
    report["candidate_eligible_bucket"] = _bucket_count(
        1 if plan.candidate_eligible else 0
    )
    report["planned_artifacts_bucket"] = _bucket_count(plan.artifact_count)
    report["planned_candidate_groups_bucket"] = _bucket_count(plan.candidate_group_count)
    report["planned_github_route_bucket"] = _bucket_count(1 if plan.has_github_route else 0)
    report["planned_x_route_bucket"] = _bucket_count(1 if plan.has_x_route else 0)
    report["planned_web_route_bucket"] = _bucket_count(1 if plan.has_web_route else 0)
    report["planned_text_idea_bucket"] = _bucket_count(1 if plan.text_idea_only else 0)


def _collect_raw_values_from_locator(locator: CandidateLocator, raw_values: set[str]) -> None:
    raw_values.add(str(locator.chat_id))
    raw_values.add(str(locator.message_id))
    if locator.message_date is not None:
        raw_values.add(str(locator.message_date))
    if locator.registry_id is not None:
        raw_values.add(locator.registry_id)


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


def _mapping_get(row: Mapping[str, Any] | Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    if hasattr(row, "_mapping"):
        return row._mapping.get(key)
    return None


def _safe_non_negative_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return max(parsed, 0)


async def _write_candidate_message(
    *,
    repository: LocatorIngestRepository,
    message: Mapping[str, Any],
    observed_at: datetime,
    diagnostics: WriteDiagnostics,
) -> WriteResult:
    from src.services.collector_telegram.idempotency import IdempotencyPolicy  # noqa: PLC0415
    from src.services.collector_telegram.outbox import CollectorOutboxBuilder  # noqa: PLC0415

    projection_builder = MessageProjectionBuilder()
    outbox_builder = CollectorOutboxBuilder(IdempotencyPolicy())
    try:
        diagnostics.source_write_attempted = True
        projection = projection_builder.build_source_projection(dict(message))
        transaction = repository.transaction()
        async with transaction:
            diagnostics.transaction_entered = True
            diagnostics.get_existing_attempted = True
            existing = await repository.get_source_message(
                platform="telegram",
                chat_id=projection.chat_id,
                message_id=projection.message_id,
            )
            if existing is not None:
                source_message_id = str(_mapping_get(existing, "source_message_id") or "")
                diagnostics.pending_event_check_attempted = True
                pending_events = await repository.count_pending_source_events(
                    source_message_id=source_message_id
                )
                if pending_events > 0:
                    diagnostics.transaction_completed = True
                    return WriteResult(
                        status="already_ingested",
                        pending_source_events=pending_events,
                        existing_source_message=True,
                        existing_event_outbox=True,
                    )
                diagnostics.transaction_completed = True
                return WriteResult(
                    status="blocked_existing_without_pending_outbox",
                    existing_source_message=True,
                    blocked_existing_without_pending_outbox=True,
                )

            diagnostics.upsert_attempted = True
            current_row = await repository.upsert_source_message(
                projection,
                platform="telegram",
            )
            source_message_id = str(_mapping_get(current_row, "source_message_id") or "")
            diagnostics.version_append_attempted = True
            changed, version_row = await repository.append_source_message_version_if_changed(
                source_message_id=source_message_id,
                projection=projection,
                version_reason="new",
                observed_at=observed_at,
                telegram_edit_date=projection.edited_at,
            )
            if not changed or version_row is None:
                diagnostics.transaction_completed = True
                return WriteResult(status="noop")
            version_no = _safe_non_negative_int(_mapping_get(version_row, "version_no"))
            event = outbox_builder.build_created(
                source_message_id=source_message_id,
                current_version_no=version_no,
                logical_post_key=projection.logical_post_key,
                occurred_at=observed_at,
            )
            diagnostics.outbox_insert_attempted = True
            await repository.insert_outbox_event(event)
            diagnostics.pending_event_check_attempted = True
            pending_events = await repository.count_pending_source_events(
                source_message_id=source_message_id
            )
            diagnostics.transaction_completed = True
            return WriteResult(
                status="ingested",
                source_messages_written=1,
                source_message_versions_written=1,
                event_outbox_written=1,
                pending_source_events=pending_events,
            )
    except Exception as exc:
        diagnostics.capture_exception(exc)
        raise


def _apply_write_result_to_report(
    *,
    report: dict[str, Any],
    result: WriteResult,
    diagnostics: WriteDiagnostics,
) -> None:
    report["source_write_attempted"] = diagnostics.source_write_attempted
    report["source_messages_written_bucket"] = _bucket_count(
        result.source_messages_written
    )
    report["source_message_versions_written_bucket"] = _bucket_count(
        result.source_message_versions_written
    )
    report["event_outbox_source_events_written_bucket"] = _bucket_count(
        result.event_outbox_written
    )
    report["event_outbox_pending_bucket"] = _bucket_count(result.pending_source_events)
    report["existing_source_message_bucket"] = _bucket_count(
        1 if result.existing_source_message else 0
    )
    report["existing_event_outbox_bucket"] = _bucket_count(
        1 if result.existing_event_outbox else 0
    )
    report["blocked_existing_without_pending_outbox"] = (
        result.blocked_existing_without_pending_outbox
    )
    if result.source_messages_written or result.source_message_versions_written:
        report["source_tables_mutation_performed"] = True
    if result.event_outbox_written:
        report["event_outbox_mutation_performed"] = True


def _bucket_count(count: int | None) -> str:
    if count is None or count <= 0:
        return "zero"
    if count == 1:
        return "one"
    return "multiple"


def _bounded(value: int, *, default: int, hard_limit: int) -> int:
    if isinstance(value, bool):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(1, min(parsed, hard_limit))


def _approval_count(
    *,
    approved_candidate_locator_ingest_smoke: bool,
    approved_private_locator_read: bool,
    approved_tdlib_existing_session_read: bool,
    approved_get_chat_history: bool,
    approved_source_table_write: bool,
    approved_event_outbox_write: bool,
) -> int:
    return sum(
        [
            bool(approved_candidate_locator_ingest_smoke),
            bool(approved_private_locator_read),
            bool(approved_tdlib_existing_session_read),
            bool(approved_get_chat_history),
            bool(approved_source_table_write),
            bool(approved_event_outbox_write),
        ]
    )


def _apply_side_effect_flags(
    report: dict[str, Any],
    side_effect_flags: Mapping[str, bool] | None,
) -> None:
    if not side_effect_flags:
        return
    for field in SIDE_EFFECT_REPORT_FIELDS:
        if bool(side_effect_flags.get(field, False)):
            report[field] = True


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


async def _run_live_locator_ingest(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    locator: CandidateLocator,
    history_limit: int,
    tdlib_auth_max_updates: int,
    tdlib_receive_timeout_sec: float,
    tdlib_overall_timeout_sec: float,
    history_probe_factory: HistoryProbeFactory | None,
    short_url_resolver_factory: ShortUrlResolverFactory | None,
    repository_context_factory: RepositoryContextFactory | None,
    raw_values: set[str],
) -> int:
    report["tdlib_connection_attempted"] = True
    probe: LocatorHistoryProbe | None = None
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

        report["history_request_attempted"] = True
        history_result = await probe.fetch_chat_history(
            chat_id=locator.chat_id,
            from_message_id=locator.message_id,
            limit=history_limit,
        )
        _apply_tdlib_request_types(report, history_result.request_types_sent)
        _merge_tdlib_ready_fields(report, probe)
        if _forbidden_tdlib_request_detected(report):
            _set_status(report, STATUS_BLOCKED_SIDE_EFFECT, "tdlib.forbidden_request")
            return 1
        if history_result.status not in {"history", "empty"}:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "history.failed")
            return 1
        report["history_request_succeeded_bucket"] = "one"

        exact_message: Mapping[str, Any] | None = None
        for message in history_result.messages:
            raw_message_id = message.get("id")
            if isinstance(raw_message_id, bool):
                continue
            try:
                message_id = int(raw_message_id)
            except (TypeError, ValueError):
                continue
            if message_id == locator.message_id:
                exact_message = message
                break

        if exact_message is None:
            _set_status(report, STATUS_BLOCKED_EXACT_MESSAGE_MISSING, "history.exact_message_missing")
            return 1
        report["exact_message_found_bucket"] = "one"
        _collect_raw_values_from_tdlib_message(exact_message, raw_values)

        snapshot = _project_tdlib_message(exact_message)
        if snapshot is None:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "message.projection_failed")
            return 1
        report["message_projected_bucket"] = "one"
        _collect_raw_values_from_snapshot(snapshot, raw_values)

        plan = await _build_planning_result(snapshot, short_url_resolver_factory)
        _collect_raw_values_from_plan(plan, raw_values)
        _apply_plan_to_report(report, plan)
        if not plan.candidate_eligible:
            _set_status(report, STATUS_BLOCKED_NOT_CANDIDATE, "planning.not_candidate")
            return 1

        diagnostics = WriteDiagnostics()
        try:
            context_factory = repository_context_factory or _default_repository_context_factory
            async with context_factory(values) as repository:
                write_result = await _write_candidate_message(
                    repository=repository,
                    message=exact_message,
                    observed_at=datetime.now(timezone.utc),
                    diagnostics=diagnostics,
                )
        except Exception as exc:
            diagnostics.capture_exception(exc)
            report["source_write_attempted"] = diagnostics.source_write_attempted
            report["db_write_failure_class"] = diagnostics.failure_class
            _set_status(report, STATUS_BLOCKED_NOT_READY, "database.write_failed")
            return 1

        _apply_write_result_to_report(
            report=report,
            result=write_result,
            diagnostics=diagnostics,
        )
        if write_result.status == "already_ingested":
            _set_status(report, STATUS_ALREADY_INGESTED)
            return 0
        if write_result.status == "blocked_existing_without_pending_outbox":
            _set_status(report, STATUS_BLOCKED_NOT_READY, "blocked_existing_without_pending_outbox")
            return 1
        if write_result.status == "ingested":
            _set_status(report, STATUS_INGESTED)
            return 0
        _set_status(report, STATUS_BLOCKED_NOT_READY, "database.write_noop")
        return 1
    finally:
        await _close_probe(probe)
        _merge_tdlib_ready_fields(report, probe)


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    private_candidate_locator_input: str | Path | None = None,
    history_limit: int = DEFAULT_HISTORY_LIMIT,
    approved_candidate_locator_ingest_smoke: bool = False,
    approved_private_locator_read: bool = False,
    approved_tdlib_existing_session_read: bool = False,
    approved_get_chat_history: bool = False,
    approved_source_table_write: bool = False,
    approved_event_outbox_write: bool = False,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    locator_exists_checker: LocatorExistsChecker | None = None,
    locator_reader: LocatorReader | None = None,
    history_probe_factory: HistoryProbeFactory | None = None,
    short_url_resolver_factory: ShortUrlResolverFactory | None = None,
    repository_context_factory: RepositoryContextFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
    tdlib_auth_max_updates: int = DEFAULT_TDLIB_AUTH_MAX_UPDATES,
    tdlib_receive_timeout_sec: float = DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC,
    tdlib_overall_timeout_sec: float = DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC,
) -> ScriptResult:
    report = _base_report(
        private_locator_path_configured=private_candidate_locator_input is not None,
        approved_candidate_locator_ingest_smoke=approved_candidate_locator_ingest_smoke,
        approved_private_locator_read=approved_private_locator_read,
        approved_tdlib_existing_session_read=approved_tdlib_existing_session_read,
        approved_get_chat_history=approved_get_chat_history,
        approved_source_table_write=approved_source_table_write,
        approved_event_outbox_write=approved_event_outbox_write,
    )
    _apply_side_effect_flags(report, side_effect_flags)
    raw_values: set[str] = {value for value in forbidden_raw_values if len(value) >= 6}
    raw_values.add(str(runtime_env_path))
    if private_candidate_locator_input is not None:
        raw_values.add(str(private_candidate_locator_input))

    if _forbidden_side_effect_detected(report):
        _set_status(report, STATUS_BLOCKED_SIDE_EFFECT, "side_effect.forbidden")
        return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

    approvals = _approval_count(
        approved_candidate_locator_ingest_smoke=approved_candidate_locator_ingest_smoke,
        approved_private_locator_read=approved_private_locator_read,
        approved_tdlib_existing_session_read=approved_tdlib_existing_session_read,
        approved_get_chat_history=approved_get_chat_history,
        approved_source_table_write=approved_source_table_write,
        approved_event_outbox_write=approved_event_outbox_write,
    )
    if 0 < approvals < 6:
        _set_status(report, STATUS_BLOCKED_NOT_READY, "approval.partial")
        return _finalize_result(report=report, raw_values=raw_values, exit_code=1)
    live_mode = approvals == 6

    if private_candidate_locator_input is None:
        _set_status(report, STATUS_BLOCKED_NOT_READY, "private_locator.path_required")
        return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

    exists_checker = locator_exists_checker or _locator_exists
    try:
        report["private_locator_exists"] = bool(
            exists_checker(private_candidate_locator_input)
        )
    except Exception:
        report["private_locator_exists"] = False
    if not report["private_locator_exists"]:
        _set_status(report, STATUS_BLOCKED_NOT_READY, "private_locator.missing")
        return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

    bounded_history_limit = _bounded(
        history_limit,
        default=DEFAULT_HISTORY_LIMIT,
        hard_limit=MAX_HISTORY_LIMIT,
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
            if not await _check_required_tables(session=session, report=report):
                _set_status(report, STATUS_BLOCKED_NOT_READY, "database.required_tables")
                return _finalize_result(report=report, raw_values=raw_values, exit_code=1)
        except Exception:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "database.connection_or_schema")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

        if not live_mode:
            report["private_locator_shape_valid_bucket"] = "not_read"
            _set_status(report, STATUS_READY)
            return _finalize_result(report=report, raw_values=raw_values, exit_code=0)

        report["private_locator_read_attempted"] = True
        try:
            raw_locator_text = (locator_reader or _read_locator_text)(
                private_candidate_locator_input
            )
        except Exception:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "private_locator.read")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)
        locator = _parse_locator(raw_locator_text, raw_values=raw_values)
        if locator is None:
            report["private_locator_shape_valid_bucket"] = "malformed"
            _set_status(report, STATUS_BLOCKED_NOT_READY, "private_locator.malformed")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)
        _collect_raw_values_from_locator(locator, raw_values)
        report["private_locator_shape_valid_bucket"] = "valid"

        if session is not None:
            await _maybe_await(session.rollback())
            await _close_database_session(session)
            session = None

        exit_code = await _run_live_locator_ingest(
            report=report,
            values=values,
            locator=locator,
            history_limit=bounded_history_limit,
            tdlib_auth_max_updates=tdlib_auth_max_updates,
            tdlib_receive_timeout_sec=tdlib_receive_timeout_sec,
            tdlib_overall_timeout_sec=tdlib_overall_timeout_sec,
            history_probe_factory=history_probe_factory,
            short_url_resolver_factory=short_url_resolver_factory,
            repository_context_factory=repository_context_factory,
            raw_values=raw_values,
        )
        if _forbidden_side_effect_detected(
            report,
            allow_source_and_outbox=True,
        ):
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
    private_candidate_locator_input: str | Path | None = None,
    history_limit: int = DEFAULT_HISTORY_LIMIT,
    approved_candidate_locator_ingest_smoke: bool = False,
    approved_private_locator_read: bool = False,
    approved_tdlib_existing_session_read: bool = False,
    approved_get_chat_history: bool = False,
    approved_source_table_write: bool = False,
    approved_event_outbox_write: bool = False,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    locator_exists_checker: LocatorExistsChecker | None = None,
    locator_reader: LocatorReader | None = None,
    history_probe_factory: HistoryProbeFactory | None = None,
    short_url_resolver_factory: ShortUrlResolverFactory | None = None,
    repository_context_factory: RepositoryContextFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
    tdlib_auth_max_updates: int = DEFAULT_TDLIB_AUTH_MAX_UPDATES,
    tdlib_receive_timeout_sec: float = DEFAULT_TDLIB_RECEIVE_TIMEOUT_SEC,
    tdlib_overall_timeout_sec: float = DEFAULT_TDLIB_OVERALL_TIMEOUT_SEC,
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            private_candidate_locator_input=private_candidate_locator_input,
            history_limit=history_limit,
            approved_candidate_locator_ingest_smoke=approved_candidate_locator_ingest_smoke,
            approved_private_locator_read=approved_private_locator_read,
            approved_tdlib_existing_session_read=approved_tdlib_existing_session_read,
            approved_get_chat_history=approved_get_chat_history,
            approved_source_table_write=approved_source_table_write,
            approved_event_outbox_write=approved_event_outbox_write,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            locator_exists_checker=locator_exists_checker,
            locator_reader=locator_reader,
            history_probe_factory=history_probe_factory,
            short_url_resolver_factory=short_url_resolver_factory,
            repository_context_factory=repository_context_factory,
            side_effect_flags=side_effect_flags,
            forbidden_raw_values=forbidden_raw_values,
            tdlib_auth_max_updates=tdlib_auth_max_updates,
            tdlib_receive_timeout_sec=tdlib_receive_timeout_sec,
            tdlib_overall_timeout_sec=tdlib_overall_timeout_sec,
        )
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        private_candidate_locator_input=args.private_candidate_locator_input,
        history_limit=args.history_limit,
        approved_candidate_locator_ingest_smoke=(
            args.approved_candidate_locator_ingest_smoke
        ),
        approved_private_locator_read=args.approved_private_locator_read,
        approved_tdlib_existing_session_read=args.approved_tdlib_existing_session_read,
        approved_get_chat_history=args.approved_get_chat_history,
        approved_source_table_write=args.approved_source_table_write,
        approved_event_outbox_write=args.approved_event_outbox_write,
        tdlib_auth_max_updates=args.tdlib_auth_max_updates,
        tdlib_receive_timeout_sec=args.tdlib_receive_timeout_sec,
        tdlib_overall_timeout_sec=args.tdlib_overall_timeout_sec,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
