from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_analysis_requested_route_publish_smoke"
REPORT_TYPE = "analysis_requested_route_publish_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_PENDING_SCAN_LIMIT = 20
MAX_PENDING_SCAN_LIMIT = 100
EXPECTED_EVENT_TYPE = "analysis.requested.v1"
EXPECTED_AGGREGATE_TYPE = "candidate_group"
EXPECTED_QUEUE_NAME = "q.analysis.route"
EXPECTED_STAGE_NAME = "analysis_route"
ALLOWED_JUDGE_PROFILES = {"github_primary", "x_primary", "text_idea_primary"}
ALLOWED_REDIS_THIN_FIELDS = {
    "job_id",
    "stage_name",
    "root_object_type",
    "root_object_id",
    "idempotency_key",
    "pipeline_run_id",
    "not_before",
    "trigger_event_id",
}
FORBIDDEN_REDIS_FIELD_TOKENS = (
    "payload",
    "payload_json",
    "source",
    "raw",
    "text",
    "caption",
    "url",
    "message",
    "database_url",
    "redis_url",
    "secret",
    "token",
    "password",
    "api_key",
    "apikey",
    "body",
    "stderr",
)
SIDE_EFFECT_REPORT_FIELDS = (
    "analysis_router_started",
    "judge_openai_started",
    "policy_engine_started",
    "notifier_started",
    "source_tables_mutation_performed",
    "telegram_raw_updates_mutation_performed",
    "candidate_mutation_performed",
    "artifact_registry_mutation_performed",
    "artifact_snapshot_mutation_performed",
    "evidence_bundle_mutation_performed",
    "docker_or_systemd_changed",
    "alembic_run",
    "external_network_attempted",
    "raw_values_emitted",
)

STATUS_READY = "analysis_requested_route_publish_smoke_ready"
STATUS_PUBLISHED = "analysis_requested_route_publish_smoke_published"
STATUS_MISSING_APPROVAL = "blocked_analysis_requested_route_publish_smoke_missing_approval"
STATUS_NO_PENDING = "blocked_analysis_requested_route_publish_smoke_no_pending_analysis_request"
STATUS_INVALID_EVENT = "blocked_analysis_requested_route_publish_smoke_invalid_event"
STATUS_INVALID_BUNDLE = (
    "blocked_analysis_requested_route_publish_smoke_invalid_bundle_current_pointer"
)
STATUS_INVALID_JUDGE_PROFILE = (
    "blocked_analysis_requested_route_publish_smoke_invalid_judge_profile"
)
STATUS_ROUTE_MISMATCH = "blocked_analysis_requested_route_publish_smoke_route_mismatch"
STATUS_REDIS_PUBLISH_FAILED = (
    "blocked_analysis_requested_route_publish_smoke_redis_publish_failed"
)
STATUS_DB_UPDATE_FAILED = "blocked_analysis_requested_route_publish_smoke_db_update_failed"
STATUS_FORBIDDEN_SIDE_EFFECT = "blocked_forbidden_side_effect_detected"
STATUS_RAW_VALUE_EMISSION = "blocked_analysis_requested_route_publish_smoke_raw_value_emission"
STATUS_NOT_READY = "blocked_analysis_requested_route_publish_smoke_not_ready"

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
SELECT_PENDING_ANALYSIS_REQUESTED_EVENTS_QUERY = """
SELECT
    event_id,
    event_type,
    aggregate_type,
    aggregate_id,
    dedupe_key,
    payload_json,
    status,
    fail_count,
    created_at
FROM event_outbox
WHERE status = 'pending'::outbox_status_enum
  AND event_type = 'analysis.requested.v1'
ORDER BY created_at DESC, event_id DESC
LIMIT :limit
"""
SELECT_CANDIDATE_GROUP_STATE_QUERY = """
SELECT candidate_group_id, current_bundle_id
FROM candidate_group_proposals
WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
"""
SELECT_BUNDLE_STATE_QUERY = """
SELECT bundle_id, candidate_group_id, ready_for_analysis
FROM candidate_evidence_bundles
WHERE bundle_id = CAST(:bundle_id AS uuid)
"""
COUNT_CANDIDATE_EVIDENCE_MEMBERS_QUERY = """
SELECT COUNT(*)
FROM candidate_evidence_members
WHERE bundle_id = CAST(:bundle_id AS uuid)
"""
REQUIRED_TABLES = (
    "event_outbox",
    "job_attempts",
    "candidate_group_proposals",
    "candidate_evidence_bundles",
    "candidate_evidence_members",
)


class AsyncSessionLike(Protocol):
    async def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> Any: ...

    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def close(self) -> None: ...


class RedisClientLike(Protocol):
    async def ping(self) -> Any: ...
    async def xlen(self, name: str) -> Any: ...

    async def xadd(
        self,
        name: str,
        fields: dict[str, str],
        maxlen: int | None = None,
        approximate: bool | None = None,
    ) -> Any: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]
RedisClientFactory = Callable[[str], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PublishApprovals:
    analysis_requested_route_publish: bool
    redis_publish: bool
    event_outbox_update: bool
    job_attempt_write: bool

    @property
    def all_granted(self) -> bool:
        return (
            self.analysis_requested_route_publish
            and self.redis_publish
            and self.event_outbox_update
            and self.job_attempt_write
        )

    @property
    def any_granted(self) -> bool:
        return (
            self.analysis_requested_route_publish
            or self.redis_publish
            or self.event_outbox_update
            or self.job_attempt_write
        )

    def missing_checks(self) -> list[str]:
        checks: list[str] = []
        if not self.analysis_requested_route_publish:
            checks.append("approval.analysis_requested_route_publish")
        if not self.redis_publish:
            checks.append("approval.redis_publish")
        if not self.event_outbox_update:
            checks.append("approval.event_outbox_update")
        if not self.job_attempt_write:
            checks.append("approval.job_attempt_write")
        return checks


@dataclass(frozen=True, slots=True)
class SelectedTarget:
    row: Any
    route: Any
    message: Any


@dataclass(frozen=True, slots=True)
class CandidateGroupState:
    candidate_group_id: UUID
    current_bundle_id: UUID | None


@dataclass(frozen=True, slots=True)
class BundleState:
    bundle_id: UUID
    candidate_group_id: UUID
    ready_for_analysis: bool


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage  # noqa: E402
from src.services.outbox_relay.redis_streams import RedisStreamsPublisher  # noqa: E402
from src.services.outbox_relay.repositories import OutboxRelayRepository, _sql  # noqa: E402
from src.services.outbox_relay.routing import (  # noqa: E402
    OutboxRouteResolver,
    UnsupportedOutboxEventTypeError,
)


class AnalysisRequestedOutboxRepository(OutboxRelayRepository):
    async def fetch_recent_pending_analysis_requested_events(
        self,
        *,
        limit: int,
    ) -> list[OutboxEventRow]:
        result = await self._session.execute(
            _sql(SELECT_PENDING_ANALYSIS_REQUESTED_EVENTS_QUERY),
            {"limit": limit},
        )
        rows: list[OutboxEventRow] = []
        for raw_row in result.mappings().all():
            payload = raw_row["payload_json"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            rows.append(
                OutboxEventRow(
                    event_id=_coerce_uuid(raw_row["event_id"]),
                    event_type=str(raw_row["event_type"]),
                    aggregate_type=str(raw_row["aggregate_type"]),
                    aggregate_id=_coerce_uuid(raw_row["aggregate_id"]),
                    dedupe_key=str(raw_row["dedupe_key"]),
                    payload_json=payload if isinstance(payload, dict) else {},
                    status=str(raw_row["status"]),
                    fail_count=int(raw_row["fail_count"]),
                    created_at=raw_row["created_at"],
                )
            )
        return rows

    async def load_candidate_group_state(
        self,
        candidate_group_id: UUID,
    ) -> CandidateGroupState | None:
        result = await self._session.execute(
            _sql(SELECT_CANDIDATE_GROUP_STATE_QUERY),
            {"candidate_group_id": str(candidate_group_id)},
        )
        row = _first_mapping(result)
        if row is None:
            return None
        return CandidateGroupState(
            candidate_group_id=_coerce_uuid(row["candidate_group_id"]),
            current_bundle_id=_uuid_or_none(row["current_bundle_id"]),
        )

    async def load_bundle_state(self, bundle_id: UUID) -> BundleState | None:
        result = await self._session.execute(
            _sql(SELECT_BUNDLE_STATE_QUERY),
            {"bundle_id": str(bundle_id)},
        )
        row = _first_mapping(result)
        if row is None:
            return None
        return BundleState(
            bundle_id=_coerce_uuid(row["bundle_id"]),
            candidate_group_id=_coerce_uuid(row["candidate_group_id"]),
            ready_for_analysis=bool(row["ready_for_analysis"]),
        )

    async def count_candidate_evidence_members(self, bundle_id: UUID) -> int:
        value = await _scalar(
            await self._session.execute(
                _sql(COUNT_CANDIDATE_EVIDENCE_MEMBERS_QUERY),
                {"bundle_id": str(bundle_id)},
            )
        )
        return _safe_count(value)


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

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()
        await self._engine.dispose()


def _bounded_positive_int_named(field_name: str, *, upper_bound: int) -> Callable[[str], int]:
    def parse(raw: str) -> int:
        try:
            value = int(raw)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be a positive integer"
            ) from exc
        if value <= 0 or value > upper_bound:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be between 1 and {upper_bound}"
            )
        return value

    return parse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Bounded analysis.requested.v1 route publish smoke. Default mode "
            "validates one pending analysis request and the intended thin Redis "
            "handoff without publishing or mutating durable state."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument(
        "--pending-scan-limit",
        type=_bounded_positive_int_named(
            "pending-scan-limit",
            upper_bound=MAX_PENDING_SCAN_LIMIT,
        ),
        default=DEFAULT_PENDING_SCAN_LIMIT,
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument(
        "--approved-analysis-requested-route-publish",
        action="store_true",
    )
    parser.add_argument("--approved-redis-publish", action="store_true")
    parser.add_argument("--approved-event-outbox-update", action="store_true")
    parser.add_argument("--approved-job-attempt-write", action="store_true")
    return parser


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_NOT_READY,
        "runtime_env_read": False,
        "database_connected": False,
        "redis_connected": False,
        "read_only_transaction": False,
        "analysis_requested_pending_event_found_bucket": "zero",
        "analysis_requested_event_valid_bucket": "zero",
        "candidate_group_current_bundle_match_bucket": "zero",
        "bundle_ready_for_analysis_bucket": "zero",
        "candidate_evidence_member_found_bucket": "zero",
        "judge_profile_allowed_bucket": "zero",
        "redis_publish_planned_bucket": "zero",
        "redis_stream_name_bucket": "zero",
        "redis_thin_payload_valid_bucket": "zero",
        "redis_publish_attempted": False,
        "redis_publish_succeeded_bucket": "zero",
        "event_outbox_update_attempted": False,
        "event_outbox_published_bucket": "zero",
        "job_attempt_write_attempted": False,
        "job_attempt_succeeded_bucket": "zero",
        "analysis_router_started": False,
        "judge_run_written_bucket": "zero",
        "judge_call_requested_outbox_written_bucket": "zero",
        "judge_openai_started": False,
        "policy_engine_started": False,
        "notifier_started": False,
        "source_tables_mutation_performed": False,
        "telegram_raw_updates_mutation_performed": False,
        "candidate_mutation_performed": False,
        "artifact_registry_mutation_performed": False,
        "artifact_snapshot_mutation_performed": False,
        "evidence_bundle_mutation_performed": False,
        "docker_or_systemd_changed": False,
        "alembic_run": False,
        "external_network_attempted": False,
        "raw_values_emitted": False,
        "checks_failed": [],
    }


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _bucket_count(count: int) -> str:
    if count <= 0:
        return "zero"
    if count == 1:
        return "one"
    return "multiple"


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


def _redis_url_is_supported(redis_url: str) -> bool:
    scheme_match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", redis_url)
    if not scheme_match:
        return False
    return scheme_match.group(1).lower() in {"redis", "rediss", "unix"}


def _transaction_read_only_enabled(raw_value: Any) -> bool:
    return str(raw_value).strip().lower() in {"on", "true", "1", "yes"}


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


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


async def _open_default_redis_client(redis_url: str) -> RedisClientLike:
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    return Redis.from_url(redis_url, decode_responses=True)


async def _open_redis_client(
    redis_url: str,
    redis_client_factory: RedisClientFactory | None,
) -> RedisClientLike:
    if redis_client_factory is not None:
        return await _maybe_await(redis_client_factory(redis_url))
    return await _open_default_redis_client(redis_url)


async def _close_database_session(session: AsyncSessionLike | None) -> None:
    if session is not None:
        await _maybe_await(session.close())


async def _close_redis_client(client: Any | None) -> None:
    if client is None:
        return
    close = getattr(client, "aclose", None) or getattr(client, "close", None)
    if close is not None:
        await _maybe_await(close())


async def _execute(
    session: AsyncSessionLike,
    statement: str,
    params: dict[str, Any] | None = None,
) -> Any:
    return await session.execute(_sql(statement), params or {})


async def _scalar(result: Any) -> Any:
    if hasattr(result, "scalar_one_or_none"):
        return result.scalar_one_or_none()
    if hasattr(result, "scalar"):
        return result.scalar()
    rows = _rows(result)
    if not rows:
        return None
    first = rows[0]
    if isinstance(first, (tuple, list)):
        return first[0] if first else None
    if hasattr(first, "_mapping"):
        return next(iter(first._mapping.values()))
    if isinstance(first, Mapping):
        return next(iter(first.values()))
    return first


def _rows(result: Any) -> list[Any]:
    if hasattr(result, "fetchall"):
        return list(result.fetchall())
    if hasattr(result, "mappings"):
        return list(result.mappings().all())
    if isinstance(result, list):
        return result
    return list(result)


def _first_mapping(result: Any) -> Mapping[str, Any] | None:
    mappings = result.mappings() if hasattr(result, "mappings") else None
    if mappings is not None:
        if hasattr(mappings, "first"):
            return mappings.first()
        rows = list(mappings.all())
        return rows[0] if rows else None
    rows = _rows(result)
    if not rows:
        return None
    first = rows[0]
    if hasattr(first, "_mapping"):
        return first._mapping
    if isinstance(first, Mapping):
        return first
    return None


async def _check_required_tables(session: AsyncSessionLike) -> bool:
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
        if not available:
            return False
    return True


def _coerce_uuid(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return _coerce_uuid(value)
    except (TypeError, ValueError, AttributeError):
        return None


def _safe_count(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _payload_str(payload: Mapping[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _payload_uuid(payload: Mapping[str, Any], key: str) -> UUID | None:
    value = _payload_str(payload, key)
    if value is None:
        return None
    return _uuid_or_none(value)


def _extract_runtime_config(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> tuple[str, str, int | None] | None:
    database_url = str(values.get("DATABASE_URL", "")).strip()
    redis_url = str(values.get("REDIS_URL", "")).strip()
    xadd_maxlen_raw = str(values.get("OUTBOX_RELAY_XADD_MAXLEN", "10000")).strip()
    if database_url:
        raw_values.add(database_url)
    if redis_url:
        raw_values.add(redis_url)
    if not database_url:
        _set_status(report, STATUS_NOT_READY, "database.url_missing")
        return None
    if not _database_url_is_supported(database_url):
        _set_status(report, STATUS_NOT_READY, "database.url_unsupported")
        return None
    if not redis_url:
        _set_status(report, STATUS_NOT_READY, "redis.url_missing")
        return None
    if not _redis_url_is_supported(redis_url):
        _set_status(report, STATUS_NOT_READY, "redis.url_unsupported")
        return None
    try:
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
    except ValueError:
        _set_status(report, STATUS_NOT_READY, "redis.xadd_maxlen_invalid")
        return None
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        _set_status(report, STATUS_NOT_READY, "redis.xadd_maxlen_invalid")
        return None
    return database_url, redis_url, xadd_maxlen


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


def _approval_block_status(report: dict[str, Any], approvals: PublishApprovals) -> None:
    _set_status(report, STATUS_MISSING_APPROVAL)
    for check in approvals.missing_checks():
        _set_status(report, report["contract_status"], check)


def _raw_values_from_rows(rows: Sequence[OutboxEventRow]) -> set[str]:
    values: set[str] = set()
    for row in rows:
        values.update({str(row.event_id), str(row.aggregate_id), row.dedupe_key})
        values.add(json.dumps(row.payload_json, sort_keys=True, default=str))
        for _key, value in row.payload_json.items():
            if isinstance(value, str) and value:
                values.add(value)
    return {value for value in values if len(value) >= 6}


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(value in rendered for value in raw_values)


def build_redis_queued_message(row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
    return RedisQueuedMessage(
        job_id=str(row.event_id),
        stage_name=route.stage_name,
        root_object_type=row.aggregate_type,
        root_object_id=str(row.aggregate_id),
        idempotency_key=row.dedupe_key,
        pipeline_run_id=None,
        not_before=None,
        trigger_event_id=str(row.event_id),
    )


def validate_redis_thin_payload_shape(fields: Mapping[str, Any]) -> bool:
    keys = set(fields)
    if keys != ALLOWED_REDIS_THIN_FIELDS:
        return False
    return not any(_is_forbidden_redis_field(key) for key in keys)


def _is_forbidden_redis_field(field_name: str) -> bool:
    lowered = field_name.lower()
    return any(token in lowered for token in FORBIDDEN_REDIS_FIELD_TOKENS)


async def _validate_analysis_requested_event(
    *,
    report: dict[str, Any],
    repository: AnalysisRequestedOutboxRepository,
    row: OutboxEventRow,
) -> tuple[bool, str]:
    payload = row.payload_json
    candidate_group_id = _payload_uuid(payload, "candidate_group_id")
    bundle_id = _payload_uuid(payload, "bundle_id")
    judge_profile = _payload_str(payload, "judge_profile")

    if row.event_type != EXPECTED_EVENT_TYPE:
        return False, "event_outbox.event_type"
    if row.aggregate_type != EXPECTED_AGGREGATE_TYPE:
        return False, "event_outbox.aggregate_type"
    if row.status != "pending":
        return False, "event_outbox.status"
    if candidate_group_id is None:
        return False, "payload.candidate_group_id"
    if bundle_id is None:
        return False, "payload.bundle_id"
    if row.aggregate_id != candidate_group_id:
        return False, "aggregate.candidate_group_id"
    report["analysis_requested_event_valid_bucket"] = "one"

    if judge_profile not in ALLOWED_JUDGE_PROFILES:
        return False, "judge_profile.allowed"
    report["judge_profile_allowed_bucket"] = "one"

    candidate_group = await repository.load_candidate_group_state(candidate_group_id)
    if candidate_group is None:
        return False, "candidate_group.exists"
    if candidate_group.current_bundle_id != bundle_id:
        return False, "candidate_group.current_bundle_id"
    report["candidate_group_current_bundle_match_bucket"] = "one"

    bundle = await repository.load_bundle_state(bundle_id)
    if bundle is None:
        return False, "bundle.exists"
    if bundle.candidate_group_id != candidate_group_id:
        return False, "bundle.candidate_group_id"
    if not bundle.ready_for_analysis:
        return False, "bundle.ready_for_analysis"
    report["bundle_ready_for_analysis_bucket"] = "one"

    member_count = await repository.count_candidate_evidence_members(bundle_id)
    report["candidate_evidence_member_found_bucket"] = _bucket_count(member_count)
    if member_count <= 0:
        return False, "bundle.candidate_evidence_members"

    return True, ""


def _status_for_validation_check(check: str) -> str:
    if check == "judge_profile.allowed":
        return STATUS_INVALID_JUDGE_PROFILE
    if check.startswith("route.") or check == "redis.thin_payload_shape":
        return STATUS_ROUTE_MISMATCH
    if (
        check.startswith("candidate_group.")
        or check.startswith("bundle.")
    ):
        return STATUS_INVALID_BUNDLE
    return STATUS_INVALID_EVENT


async def _select_target(
    *,
    report: dict[str, Any],
    repository: AnalysisRequestedOutboxRepository,
    route_resolver: Any,
    pending_scan_limit: int,
) -> tuple[SelectedTarget | None, set[str], bool]:
    rows = await repository.fetch_recent_pending_analysis_requested_events(
        limit=pending_scan_limit
    )
    raw_values = _raw_values_from_rows(rows)
    report["analysis_requested_pending_event_found_bucket"] = _bucket_count(len(rows))
    if not rows:
        _set_status(report, STATUS_NO_PENDING, "event_outbox.no_pending_analysis_request")
        return None, raw_values, False
    if len(rows) != 1:
        _set_status(report, STATUS_INVALID_EVENT, "event_outbox.pending_count_not_one")
        return None, raw_values, False

    row = rows[0]
    valid, check = await _validate_analysis_requested_event(
        report=report,
        repository=repository,
        row=row,
    )
    if not valid:
        status = _status_for_validation_check(check)
        _set_status(report, status, check)
        return None, raw_values, False

    try:
        route = route_resolver.resolve(row)
    except UnsupportedOutboxEventTypeError:
        _set_status(report, STATUS_ROUTE_MISMATCH, "route.unsupported")
        return None, raw_values, False

    if route.queue_name != EXPECTED_QUEUE_NAME:
        _set_status(report, STATUS_ROUTE_MISMATCH, "route.queue")
        return None, raw_values, False
    report["redis_stream_name_bucket"] = "one"

    if route.stage_name != EXPECTED_STAGE_NAME:
        _set_status(report, STATUS_ROUTE_MISMATCH, "route.stage")
        return None, raw_values, False

    message = build_redis_queued_message(row, route)
    fields = message.as_stream_fields()
    if fields.get("stage_name") != EXPECTED_STAGE_NAME:
        _set_status(report, STATUS_ROUTE_MISMATCH, "redis.stage_name")
        return None, raw_values, False
    if fields.get("root_object_type") != EXPECTED_AGGREGATE_TYPE:
        _set_status(report, STATUS_ROUTE_MISMATCH, "redis.root_object_type")
        return None, raw_values, False
    if not validate_redis_thin_payload_shape(fields):
        _set_status(report, STATUS_ROUTE_MISMATCH, "redis.thin_payload_shape")
        return None, raw_values, False

    report["redis_publish_planned_bucket"] = "one"
    report["redis_thin_payload_valid_bucket"] = "one"
    return SelectedTarget(row=row, route=route, message=message), raw_values, True


async def _inspect_redis_target_stream(redis_client: RedisClientLike) -> None:
    await _maybe_await(redis_client.ping())
    await _maybe_await(redis_client.xlen(EXPECTED_QUEUE_NAME))


async def _publish_selected_target(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    redis_client: RedisClientLike,
    repository: AnalysisRequestedOutboxRepository,
    target: SelectedTarget,
    xadd_maxlen: int | None,
    raw_values: set[str],
) -> ScriptResult | None:
    publisher = RedisStreamsPublisher(redis_client, maxlen=xadd_maxlen)
    try:
        report["redis_publish_attempted"] = True
        redis_message_id = await publisher.publish(target.route, target.message)
        if redis_message_id:
            raw_values.add(str(redis_message_id))
        report["redis_publish_succeeded_bucket"] = "one"
    except Exception:
        await session.rollback()
        _set_status(report, STATUS_REDIS_PUBLISH_FAILED, "redis.publish")
        return ScriptResult(exit_code=1, report=report)

    try:
        report["event_outbox_update_attempted"] = True
        await repository.mark_published(
            event_id=target.row.event_id,
            published_at=datetime.now(timezone.utc),
        )
        report["job_attempt_write_attempted"] = True
        await repository.insert_job_attempt(
            stage_name=target.route.stage_name,
            queue_name=target.route.queue_name,
            root_object_type=target.row.aggregate_type,
            root_object_id=target.row.aggregate_id,
            attempt_status="succeeded",
            error_code=None,
        )
        await session.commit()
    except Exception:
        await session.rollback()
        _set_status(report, STATUS_DB_UPDATE_FAILED, "database.write_after_redis_publish")
        return ScriptResult(exit_code=1, report=report)

    report["event_outbox_published_bucket"] = "one"
    report["job_attempt_succeeded_bucket"] = "one"
    if _report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)
    _set_status(report, STATUS_PUBLISHED)
    return ScriptResult(exit_code=0, report=report)


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    pending_scan_limit: int = DEFAULT_PENDING_SCAN_LIMIT,
    approvals: PublishApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    route_resolver: Any | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    approvals = approvals or PublishApprovals(False, False, False, False)
    report = _base_report()
    _apply_side_effect_flags(report, side_effect_flags)
    if _forbidden_side_effect_detected(report):
        _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "side_effect.forbidden")
        return ScriptResult(exit_code=1, report=report)

    if pending_scan_limit <= 0 or pending_scan_limit > MAX_PENDING_SCAN_LIMIT:
        _set_status(report, STATUS_NOT_READY, "pending_scan_limit.out_of_bounds")
        return ScriptResult(exit_code=1, report=report)

    session: AsyncSessionLike | None = None
    redis_client: RedisClientLike | None = None
    raw_values: set[str] = {value for value in forbidden_raw_values if len(value) >= 6}

    try:
        try:
            values = _read_runtime_env(runtime_env_path, runtime_env_reader)
            report["runtime_env_read"] = True
            raw_values.add(str(runtime_env_path))
        except Exception:
            _set_status(report, STATUS_NOT_READY, "runtime_env.read")
            return ScriptResult(exit_code=1, report=report)

        runtime_config = _extract_runtime_config(
            report=report,
            values=values,
            raw_values=raw_values,
        )
        if runtime_config is None:
            return ScriptResult(exit_code=1, report=report)
        database_url, redis_url, xadd_maxlen = runtime_config

        if approvals.any_granted and not approvals.all_granted:
            _approval_block_status(report, approvals)
            return ScriptResult(exit_code=1, report=report)

        try:
            session = await _open_database_session(database_url, database_session_factory)
            if not approvals.all_granted:
                await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
                read_only_value = await _scalar(
                    await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY)
                )
                report["read_only_transaction"] = _transaction_read_only_enabled(
                    read_only_value
                )
                if not report["read_only_transaction"]:
                    _set_status(report, STATUS_NOT_READY, "database.read_only_transaction")
                    return ScriptResult(exit_code=1, report=report)
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not await _check_required_tables(session):
                _set_status(report, STATUS_NOT_READY, "database.required_tables")
                return ScriptResult(exit_code=1, report=report)
        except Exception:
            _set_status(report, STATUS_NOT_READY, "database.connection_or_schema")
            return ScriptResult(exit_code=1, report=report)

        try:
            redis_client = await _open_redis_client(redis_url, redis_client_factory)
            await _inspect_redis_target_stream(redis_client)
            report["redis_connected"] = True
        except Exception:
            _set_status(report, STATUS_NOT_READY, "redis.connection_or_stream_inspection")
            return ScriptResult(exit_code=1, report=report)

        repository = AnalysisRequestedOutboxRepository(session)
        target, row_raw_values, selection_valid = await _select_target(
            report=report,
            repository=repository,
            route_resolver=route_resolver or OutboxRouteResolver(),
            pending_scan_limit=pending_scan_limit,
        )
        raw_values.update(row_raw_values)
        if _report_contains_raw_values(report, raw_values):
            report["raw_values_emitted"] = True
            _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
            return ScriptResult(exit_code=1, report=report)
        if target is None:
            return ScriptResult(exit_code=0 if selection_valid else 1, report=report)

        if not approvals.all_granted:
            _set_status(report, STATUS_READY)
            return ScriptResult(exit_code=0, report=report)

        publish_result = await _publish_selected_target(
            report=report,
            session=session,
            redis_client=redis_client,
            repository=repository,
            target=target,
            xadd_maxlen=xadd_maxlen,
            raw_values=raw_values,
        )
        if publish_result is not None:
            return publish_result

        _set_status(report, STATUS_NOT_READY, "unexpected")
        return ScriptResult(exit_code=1, report=report)
    except Exception:
        if session is not None:
            await session.rollback()
        _set_status(report, STATUS_NOT_READY, "unexpected")
        return ScriptResult(exit_code=1, report=report)
    finally:
        if session is not None and not approvals.all_granted:
            await session.rollback()
        await _close_redis_client(redis_client)
        await _close_database_session(session)


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    pending_scan_limit: int = DEFAULT_PENDING_SCAN_LIMIT,
    approvals: PublishApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    route_resolver: Any | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            pending_scan_limit=pending_scan_limit,
            approvals=approvals,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            redis_client_factory=redis_client_factory,
            route_resolver=route_resolver,
            side_effect_flags=side_effect_flags,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=True, sort_keys=True, indent=2)


def _approvals_from_args(args: argparse.Namespace) -> PublishApprovals:
    return PublishApprovals(
        analysis_requested_route_publish=bool(
            args.approved_analysis_requested_route_publish
        ),
        redis_publish=bool(args.approved_redis_publish),
        event_outbox_update=bool(args.approved_event_outbox_update),
        job_attempt_write=bool(args.approved_job_attempt_write),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        pending_scan_limit=args.pending_scan_limit,
        approvals=_approvals_from_args(args),
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
