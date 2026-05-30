from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke"
REPORT_TYPE = "artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_STREAM_SCAN_LIMIT = 20
MAX_STREAM_SCAN_LIMIT = 100
EXPECTED_QUEUE_NAME = "q.candidate.bundle"
EXPECTED_STAGE_NAME = "bundle"
EXPECTED_ROOT_OBJECT_TYPE = "artifact"
EXPECTED_EVENT_TYPE = "artifact.snapshot.updated.v1"
EXPECTED_AGGREGATE_TYPE = "artifact"
EXPECTED_PROVIDER = "x"
CONSUMER_GROUP = "evidence-assembler"
CONSUMER_NAME = "evidence-assembler-consume-smoke"
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
    "artifact_registry_mutation_performed",
    "artifact_snapshot_mutation_performed",
    "docker_or_systemd_changed",
    "alembic_run",
    "external_network_attempted",
    "raw_values_emitted",
)

STATUS_READY = "artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_ready"
STATUS_CONSUMED = "artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_consumed"
STATUS_MISSING_APPROVAL = (
    "blocked_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_missing_approval"
)
STATUS_NO_STREAM_ENTRY = (
    "blocked_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_no_candidate_bundle_stream_entry"
)
STATUS_INVALID_REDIS_PAYLOAD = (
    "blocked_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_invalid_redis_thin_payload"
)
STATUS_TRIGGER_REHYDRATE_FAILURE = (
    "blocked_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_trigger_event_rehydrate_failure"
)
STATUS_INVALID_EVENT = (
    "blocked_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_invalid_snapshot_updated_event"
)
STATUS_NO_IMPACTED_CANDIDATE_GROUP = (
    "blocked_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_no_impacted_candidate_group"
)
STATUS_EVIDENCE_BUNDLE_WRITE_FAILURE = (
    "blocked_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_evidence_bundle_write_failure"
)
STATUS_ANALYSIS_REQUESTED_OUTBOX_WRITE_FAILURE = (
    "blocked_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_analysis_requested_outbox_write_failure"
)
STATUS_REDIS_ACK_FAILURE = (
    "blocked_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_redis_ack_failure"
)
STATUS_FORBIDDEN_SIDE_EFFECT = "blocked_forbidden_side_effect_detected"
STATUS_RAW_VALUE_EMISSION = (
    "blocked_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_raw_value_emission"
)
STATUS_NOT_READY = (
    "blocked_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke_not_ready"
)

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
SELECT_TRIGGER_EVENT_QUERY = """
SELECT event_id, event_type, aggregate_type, aggregate_id, payload_json, status
FROM event_outbox
WHERE event_id = CAST(:event_id AS uuid)
"""
COUNT_X_SNAPSHOT_QUERY = """
SELECT COUNT(*)
FROM artifact_snapshots s
JOIN artifact_snapshot_x_post x ON x.snapshot_id = s.snapshot_id
WHERE s.snapshot_id = CAST(:snapshot_id AS uuid)
  AND s.artifact_id = CAST(:artifact_id AS uuid)
  AND s.provider = 'x'
"""
SELECT_IMPACTED_CANDIDATE_GROUPS_QUERY = """
SELECT DISTINCT cgm.candidate_group_id
FROM candidate_group_members cgm
WHERE cgm.artifact_id = CAST(:artifact_id AS uuid)
ORDER BY cgm.candidate_group_id
"""
SELECT_PRIMARY_ARTIFACT_TYPES_QUERY = """
SELECT cgp.candidate_group_id, ar.artifact_type
FROM candidate_group_proposals cgp
JOIN artifact_registry ar ON ar.artifact_id = cgp.current_primary_artifact_id
WHERE cgp.candidate_group_id = ANY(CAST(:candidate_group_ids AS uuid[]))
ORDER BY cgp.candidate_group_id
"""
SELECT_CURRENT_BUNDLE_QUERY = """
SELECT current_bundle_id
FROM candidate_group_proposals
WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
"""
COUNT_CANDIDATE_EVIDENCE_BUNDLES_QUERY = """
SELECT COUNT(*)
FROM candidate_evidence_bundles
WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
"""
COUNT_CANDIDATE_EVIDENCE_MEMBERS_QUERY = """
SELECT COUNT(*)
FROM candidate_evidence_members cem
JOIN candidate_evidence_bundles ceb ON ceb.bundle_id = cem.bundle_id
WHERE ceb.candidate_group_id = CAST(:candidate_group_id AS uuid)
"""
COUNT_CURRENT_BUNDLE_MEMBERS_QUERY = """
SELECT COUNT(*)
FROM candidate_evidence_members
WHERE bundle_id = CAST(:bundle_id AS uuid)
"""
SELECT_CURRENT_BUNDLE_READY_QUERY = """
SELECT ready_for_analysis
FROM candidate_evidence_bundles
WHERE bundle_id = CAST(:bundle_id AS uuid)
"""
COUNT_PENDING_ANALYSIS_REQUESTED_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'analysis.requested.v1'
  AND aggregate_type = 'candidate_group'
  AND aggregate_id = CAST(:candidate_group_id AS uuid)
  AND status = 'pending'
"""
COUNT_PENDING_ANALYSIS_REQUESTED_FOR_BUNDLE_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'analysis.requested.v1'
  AND aggregate_type = 'candidate_group'
  AND aggregate_id = CAST(:candidate_group_id AS uuid)
  AND status = 'pending'
  AND payload_json->>'bundle_id' = :bundle_id
  AND payload_json->>'judge_profile' IN ('github_primary', 'x_primary', 'text_idea_primary')
"""
REQUIRED_TABLES = (
    "event_outbox",
    "artifact_snapshots",
    "artifact_snapshot_x_post",
    "candidate_group_members",
    "candidate_group_proposals",
    "artifact_registry",
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
    async def xrange(
        self,
        name: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> Any: ...

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "0",
        mkstream: bool = False,
    ) -> Any: ...

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> Any: ...

    async def xack(self, name: str, groupname: str, *ids: str) -> Any: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]
RedisClientFactory = Callable[[str], Any]
ServiceFactory = Callable[[Any, Any], Any]
RepositoryFactory = Callable[[AsyncSessionLike], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ConsumeApprovals:
    artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume: bool
    candidate_evidence_bundle_write: bool
    analysis_requested_outbox_write: bool
    redis_ack: bool

    @property
    def all_granted(self) -> bool:
        return (
            self.artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume
            and self.candidate_evidence_bundle_write
            and self.analysis_requested_outbox_write
            and self.redis_ack
        )

    @property
    def any_granted(self) -> bool:
        return (
            self.artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume
            or self.candidate_evidence_bundle_write
            or self.analysis_requested_outbox_write
            or self.redis_ack
        )

    def missing_checks(self) -> list[str]:
        checks: list[str] = []
        if not self.artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume:
            checks.append("approval.artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume")
        if not self.candidate_evidence_bundle_write:
            checks.append("approval.candidate_evidence_bundle_write")
        if not self.analysis_requested_outbox_write:
            checks.append("approval.analysis_requested_outbox_write")
        if not self.redis_ack:
            checks.append("approval.redis_ack")
        return checks


@dataclass(frozen=True, slots=True)
class StreamEntry:
    message_id: str
    fields: dict[str, str]


@dataclass(frozen=True, slots=True)
class ValidationTarget:
    stream_entry: StreamEntry
    trigger_event_id: UUID
    artifact_id: UUID
    snapshot_id: UUID
    impacted_candidate_group_ids: tuple[UUID, ...]
    planned_analysis_requested: bool


@dataclass(frozen=True, slots=True)
class CandidateBundlePostconditions:
    current_bundle_id: UUID | None
    candidate_evidence_bundle_count: int
    candidate_evidence_member_count: int
    current_bundle_member_count: int
    pending_analysis_requested_count: int
    pending_analysis_requested_for_current_bundle_count: int
    current_bundle_ready_for_analysis: bool
    valid_judge_profile: bool


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.evidence_assembler.config import EvidenceAssemblerConfig  # noqa: E402
from src.services.evidence_assembler.repositories import EvidenceAssemblerRepository  # noqa: E402
from src.services.evidence_assembler.service import EvidenceAssemblerService  # noqa: E402
from src.services.outbox_relay.repositories import _sql  # noqa: E402


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

    async def commit(self) -> None:
        await self._session.commit()

    def in_transaction(self) -> bool:
        return self._session.in_transaction()

    def begin(self) -> Any:
        return self._session.begin()

    async def close(self) -> None:
        await self._session.close()
        await self._engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Bounded artifact.snapshot.updated.v1 candidate-bundle evidence-assembler consume smoke. "
            "Default mode validates and plans without PostgreSQL writes or Redis ack."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument(
        "--stream-scan-limit",
        type=_bounded_positive_int_named("stream-scan-limit", upper_bound=MAX_STREAM_SCAN_LIMIT),
        default=DEFAULT_STREAM_SCAN_LIMIT,
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument(
        "--approved-artifact-snapshot-updated-candidate-bundle-evidence-assembler-consume",
        action="store_true",
    )
    parser.add_argument("--approved-candidate-evidence-bundle-write", action="store_true")
    parser.add_argument("--approved-analysis-requested-outbox-write", action="store_true")
    parser.add_argument("--approved-redis-ack", action="store_true")
    return parser


def _bounded_positive_int_named(field_name: str, *, upper_bound: int) -> Callable[[str], int]:
    def parse(raw: str) -> int:
        try:
            value = int(raw)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"{field_name} must be a positive integer") from exc
        if value <= 0 or value > upper_bound:
            raise argparse.ArgumentTypeError(f"{field_name} must be between 1 and {upper_bound}")
        return value

    return parse


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
        "candidate_bundle_stream_found_bucket": "zero",
        "candidate_bundle_entry_valid_bucket": "zero",
        "trigger_event_rehydrated_bucket": "zero",
        "snapshot_updated_event_valid_bucket": "zero",
        "impacted_candidate_group_bucket": "zero",
        "current_snapshot_found_bucket": "zero",
        "evidence_bundle_write_planned_bucket": "zero",
        "candidate_evidence_member_write_planned_bucket": "zero",
        "current_bundle_update_planned_bucket": "zero",
        "analysis_requested_outbox_planned_bucket": "zero",
        "redis_ack_planned_bucket": "zero",
        "baseline_current_bundle_present_bucket": "zero",
        "baseline_candidate_evidence_bundle_count_bucket": "zero",
        "baseline_candidate_evidence_member_count_bucket": "zero",
        "baseline_analysis_requested_outbox_pending_bucket": "zero",
        "postcondition_current_bundle_present_bucket": "zero",
        "postcondition_current_bundle_changed_bucket": "zero",
        "postcondition_candidate_evidence_bundle_count_bucket": "zero",
        "postcondition_candidate_evidence_member_count_bucket": "zero",
        "postcondition_current_bundle_member_count_bucket": "zero",
        "postcondition_analysis_requested_outbox_pending_bucket": "zero",
        "evidence_bundle_write_attempted": False,
        "evidence_bundle_written_bucket": "zero",
        "candidate_evidence_member_write_attempted": False,
        "candidate_evidence_member_written_bucket": "zero",
        "current_bundle_update_attempted": False,
        "current_bundle_updated_bucket": "zero",
        "analysis_requested_outbox_write_attempted": False,
        "analysis_requested_outbox_written_bucket": "zero",
        "redis_ack_attempted": False,
        "redis_ack_succeeded_bucket": "zero",
        "analysis_router_started": False,
        "judge_openai_started": False,
        "policy_engine_started": False,
        "notifier_started": False,
        "source_tables_mutation_performed": False,
        "telegram_raw_updates_mutation_performed": False,
        "artifact_registry_mutation_performed": False,
        "artifact_snapshot_mutation_performed": False,
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


async def _commit_if_supported(session: AsyncSessionLike) -> None:
    commit = getattr(session, "commit", None)
    if commit is not None:
        await _maybe_await(commit())


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
    if hasattr(result, "mappings"):
        return list(result.mappings().all())
    if hasattr(result, "fetchall"):
        return list(result.fetchall())
    if isinstance(result, list):
        return result
    return list(result)


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


def _safe_count(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _decode_value(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _decode_fields(fields: Mapping[Any, Any]) -> dict[str, str]:
    return {_decode_value(key): _decode_value(value) for key, value in fields.items()}


def _decode_stream_entries(raw: Any) -> list[StreamEntry]:
    entries: list[StreamEntry] = []
    for message_id, fields in raw or []:
        if isinstance(fields, Mapping):
            entries.append(StreamEntry(message_id=_decode_value(message_id), fields=_decode_fields(fields)))
    return entries


def _decode_xreadgroup_entries(raw: Any) -> list[StreamEntry]:
    entries: list[StreamEntry] = []
    for _stream_name, raw_entries in raw or []:
        entries.extend(_decode_stream_entries(raw_entries))
    return entries


def validate_redis_thin_payload_shape(fields: Mapping[str, Any]) -> bool:
    keys = set(fields)
    if keys != ALLOWED_REDIS_THIN_FIELDS:
        return False
    return not any(_is_forbidden_redis_field(key) for key in keys)


def _is_forbidden_redis_field(field_name: str) -> bool:
    lowered = field_name.lower()
    return any(token in lowered for token in FORBIDDEN_REDIS_FIELD_TOKENS)


def _payload_str(payload: Mapping[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _extract_runtime_config(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> tuple[str, str, str] | None:
    database_url = str(values.get("DATABASE_URL", "")).strip()
    redis_url = str(values.get("REDIS_URL", "")).strip()
    bundle_profile_version = str(values.get("EVIDENCE_ASSEMBLER_BUNDLE_PROFILE_VERSION", "bundle_profile_v1")).strip()
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
    if not bundle_profile_version:
        _set_status(report, STATUS_NOT_READY, "evidence_assembler.bundle_profile_version")
        return None
    return database_url, redis_url, bundle_profile_version


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


def _approval_block_status(report: dict[str, Any], approvals: ConsumeApprovals) -> None:
    _set_status(report, STATUS_MISSING_APPROVAL)
    for check in approvals.missing_checks():
        _set_status(report, report["contract_status"], check)


async def _inspect_redis_entries(
    *,
    redis_client: RedisClientLike,
    stream_scan_limit: int,
) -> list[StreamEntry]:
    await _maybe_await(redis_client.ping())
    await _maybe_await(redis_client.xlen(EXPECTED_QUEUE_NAME))
    raw_entries = await _maybe_await(
        redis_client.xrange(EXPECTED_QUEUE_NAME, min="-", max="+", count=stream_scan_limit)
    )
    return _decode_stream_entries(raw_entries)


def _entry_base_shape_is_valid(entry: StreamEntry) -> bool:
    fields = entry.fields
    if not validate_redis_thin_payload_shape(fields):
        return False
    if fields.get("stage_name") != EXPECTED_STAGE_NAME:
        return False
    if fields.get("root_object_type") != EXPECTED_ROOT_OBJECT_TYPE:
        return False
    if not fields.get("trigger_event_id"):
        return False
    if not fields.get("root_object_id"):
        return False
    if not fields.get("job_id"):
        return False
    if not fields.get("idempotency_key"):
        return False
    return True


def _raw_values_from_entry(entry: StreamEntry) -> set[str]:
    values = {entry.message_id}
    values.update(str(value) for value in entry.fields.values())
    return _sensitive_values(values)


async def _load_trigger_event(
    *,
    session: AsyncSessionLike,
    trigger_event_id: UUID,
) -> Mapping[str, Any] | None:
    result = await _execute(session, SELECT_TRIGGER_EVENT_QUERY, {"event_id": str(trigger_event_id)})
    rows = _rows(result)
    if not rows:
        return None
    row = rows[0]
    if hasattr(row, "_mapping"):
        return row._mapping
    return row


async def _x_snapshot_exists(
    *,
    session: AsyncSessionLike,
    artifact_id: UUID,
    snapshot_id: UUID,
) -> bool:
    count = _safe_count(
        await _scalar(
            await _execute(
                session,
                COUNT_X_SNAPSHOT_QUERY,
                {"artifact_id": str(artifact_id), "snapshot_id": str(snapshot_id)},
            )
        )
    )
    return count > 0


async def _load_impacted_candidate_group_ids(
    *,
    session: AsyncSessionLike,
    artifact_id: UUID,
) -> tuple[UUID, ...]:
    result = await _execute(
        session,
        SELECT_IMPACTED_CANDIDATE_GROUPS_QUERY,
        {"artifact_id": str(artifact_id)},
    )
    ids: list[UUID] = []
    for row in _rows(result):
        value = row["candidate_group_id"] if isinstance(row, Mapping) else row[0]
        ids.append(_coerce_uuid(value))
    return tuple(ids)


async def _planned_analysis_requested(
    *,
    session: AsyncSessionLike,
    candidate_group_ids: tuple[UUID, ...],
) -> bool:
    if not candidate_group_ids:
        return False
    result = await _execute(
        session,
        SELECT_PRIMARY_ARTIFACT_TYPES_QUERY,
        {"candidate_group_ids": [str(candidate_group_id) for candidate_group_id in candidate_group_ids]},
    )
    rows = _rows(result)
    if len(rows) != len(candidate_group_ids):
        return False
    for row in rows:
        artifact_type = str(row["artifact_type"] if isinstance(row, Mapping) else row[1])
        if _judge_profile_for_artifact_type(artifact_type) not in ALLOWED_JUDGE_PROFILES:
            return False
    return True


async def _load_candidate_bundle_postconditions(
    *,
    session: AsyncSessionLike,
    candidate_group_id: UUID,
) -> CandidateBundlePostconditions:
    current_bundle_raw = await _scalar(
        await _execute(
            session,
            SELECT_CURRENT_BUNDLE_QUERY,
            {"candidate_group_id": str(candidate_group_id)},
        )
    )
    current_bundle_id = _coerce_uuid(current_bundle_raw) if current_bundle_raw is not None else None
    candidate_evidence_bundle_count = _safe_count(
        await _scalar(
            await _execute(
                session,
                COUNT_CANDIDATE_EVIDENCE_BUNDLES_QUERY,
                {"candidate_group_id": str(candidate_group_id)},
            )
        )
    )
    candidate_evidence_member_count = _safe_count(
        await _scalar(
            await _execute(
                session,
                COUNT_CANDIDATE_EVIDENCE_MEMBERS_QUERY,
                {"candidate_group_id": str(candidate_group_id)},
            )
        )
    )
    pending_analysis_requested_count = _safe_count(
        await _scalar(
            await _execute(
                session,
                COUNT_PENDING_ANALYSIS_REQUESTED_QUERY,
                {"candidate_group_id": str(candidate_group_id)},
            )
        )
    )

    current_bundle_member_count = 0
    current_bundle_ready_for_analysis = False
    pending_analysis_requested_for_current_bundle_count = 0
    if current_bundle_id is not None:
        current_bundle_member_count = _safe_count(
            await _scalar(
                await _execute(
                    session,
                    COUNT_CURRENT_BUNDLE_MEMBERS_QUERY,
                    {"bundle_id": str(current_bundle_id)},
                )
            )
        )
        current_bundle_ready_for_analysis = bool(
            await _scalar(
                await _execute(
                    session,
                    SELECT_CURRENT_BUNDLE_READY_QUERY,
                    {"bundle_id": str(current_bundle_id)},
                )
            )
        )
        pending_analysis_requested_for_current_bundle_count = _safe_count(
            await _scalar(
                await _execute(
                    session,
                    COUNT_PENDING_ANALYSIS_REQUESTED_FOR_BUNDLE_QUERY,
                    {"candidate_group_id": str(candidate_group_id), "bundle_id": str(current_bundle_id)},
                )
            )
        )

    valid_judge_profile = await _planned_analysis_requested(
        session=session,
        candidate_group_ids=(candidate_group_id,),
    )
    return CandidateBundlePostconditions(
        current_bundle_id=current_bundle_id,
        candidate_evidence_bundle_count=candidate_evidence_bundle_count,
        candidate_evidence_member_count=candidate_evidence_member_count,
        current_bundle_member_count=current_bundle_member_count,
        pending_analysis_requested_count=pending_analysis_requested_count,
        pending_analysis_requested_for_current_bundle_count=pending_analysis_requested_for_current_bundle_count,
        current_bundle_ready_for_analysis=current_bundle_ready_for_analysis,
        valid_judge_profile=valid_judge_profile,
    )


def _record_baseline_postconditions(
    report: dict[str, Any],
    snapshot: CandidateBundlePostconditions,
) -> None:
    report["baseline_current_bundle_present_bucket"] = _bucket_count(1 if snapshot.current_bundle_id else 0)
    report["baseline_candidate_evidence_bundle_count_bucket"] = _bucket_count(
        snapshot.candidate_evidence_bundle_count
    )
    report["baseline_candidate_evidence_member_count_bucket"] = _bucket_count(
        snapshot.candidate_evidence_member_count
    )
    report["baseline_analysis_requested_outbox_pending_bucket"] = _bucket_count(
        snapshot.pending_analysis_requested_count
    )


def _record_after_postconditions(
    report: dict[str, Any],
    *,
    before: CandidateBundlePostconditions,
    after: CandidateBundlePostconditions,
) -> None:
    report["postcondition_current_bundle_present_bucket"] = _bucket_count(1 if after.current_bundle_id else 0)
    report["postcondition_current_bundle_changed_bucket"] = _bucket_count(
        1 if after.current_bundle_id is not None and after.current_bundle_id != before.current_bundle_id else 0
    )
    report["postcondition_candidate_evidence_bundle_count_bucket"] = _bucket_count(
        after.candidate_evidence_bundle_count
    )
    report["postcondition_candidate_evidence_member_count_bucket"] = _bucket_count(
        after.candidate_evidence_member_count
    )
    report["postcondition_current_bundle_member_count_bucket"] = _bucket_count(after.current_bundle_member_count)
    report["postcondition_analysis_requested_outbox_pending_bucket"] = _bucket_count(
        after.pending_analysis_requested_count
    )


def _add_postcondition_raw_values(
    raw_values: set[str],
    *snapshots: CandidateBundlePostconditions,
) -> None:
    raw_values.update(
        _sensitive_values(
            str(snapshot.current_bundle_id)
            for snapshot in snapshots
            if snapshot.current_bundle_id is not None
        )
    )


def _candidate_bundle_postconditions_pass(
    *,
    report: dict[str, Any],
    before: CandidateBundlePostconditions,
    after: CandidateBundlePostconditions,
) -> bool:
    passed = True
    if after.current_bundle_id is None:
        _set_status(report, STATUS_EVIDENCE_BUNDLE_WRITE_FAILURE, "postcondition.current_bundle_id")
        passed = False
    if after.candidate_evidence_bundle_count <= 0:
        _set_status(report, STATUS_EVIDENCE_BUNDLE_WRITE_FAILURE, "postcondition.candidate_evidence_bundles")
        passed = False
    if after.current_bundle_member_count <= 0:
        _set_status(report, STATUS_EVIDENCE_BUNDLE_WRITE_FAILURE, "postcondition.candidate_evidence_members")
        passed = False

    new_bundle_created = after.candidate_evidence_bundle_count > before.candidate_evidence_bundle_count
    pending_delta = after.pending_analysis_requested_count - before.pending_analysis_requested_count
    should_request_analysis = after.current_bundle_ready_for_analysis and after.valid_judge_profile
    if new_bundle_created and should_request_analysis:
        if after.pending_analysis_requested_for_current_bundle_count <= 0 or pending_delta <= 0:
            _set_status(
                report,
                STATUS_ANALYSIS_REQUESTED_OUTBOX_WRITE_FAILURE,
                "postcondition.analysis_requested_outbox",
            )
            passed = False
    elif pending_delta > 0:
        _set_status(
            report,
            STATUS_ANALYSIS_REQUESTED_OUTBOX_WRITE_FAILURE,
            "postcondition.analysis_requested_duplicate_or_unexpected",
        )
        passed = False
    return passed


def _judge_profile_for_artifact_type(artifact_type: str) -> str | None:
    if artifact_type in {"github_repo", "github_subpath", "github_repo_page", "github_gist"}:
        return "github_primary"
    if artifact_type == "x_post":
        return "x_primary"
    if artifact_type in {"web_article", "text_idea"}:
        return "text_idea_primary"
    return None


async def _select_and_validate_target(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    redis_entries: list[StreamEntry],
    raw_values: set[str],
) -> tuple[ValidationTarget | None, bool]:
    report["candidate_bundle_stream_found_bucket"] = _bucket_count(len(redis_entries))
    raw_values.update(_sensitive_values(entry.message_id for entry in redis_entries))
    for entry in redis_entries:
        raw_values.update(_raw_values_from_entry(entry))

    base_valid_entries = [entry for entry in redis_entries if _entry_base_shape_is_valid(entry)]
    report["candidate_bundle_entry_valid_bucket"] = _bucket_count(len(base_valid_entries))

    if not redis_entries:
        _set_status(report, STATUS_NO_STREAM_ENTRY)
        return None, False
    if not base_valid_entries:
        _set_status(report, STATUS_INVALID_REDIS_PAYLOAD, "redis.thin_payload_shape")
        return None, False
    if len(base_valid_entries) > 1:
        _set_status(report, STATUS_INVALID_REDIS_PAYLOAD, "redis.multiple_valid_entries")
        return None, False

    entry = base_valid_entries[0]
    fields = entry.fields
    try:
        trigger_event_id = _coerce_uuid(fields["trigger_event_id"])
        root_object_id = _coerce_uuid(fields["root_object_id"])
    except ValueError:
        _set_status(report, STATUS_INVALID_REDIS_PAYLOAD, "redis.uuid_fields")
        return None, False

    event = await _load_trigger_event(session=session, trigger_event_id=trigger_event_id)
    if event is None:
        _set_status(report, STATUS_TRIGGER_REHYDRATE_FAILURE, "event_outbox.trigger_event_id")
        return None, False
    report["trigger_event_rehydrated_bucket"] = "one"

    payload = _json_loads(event.get("payload_json")) or {}
    if not isinstance(payload, Mapping):
        payload = {}
    raw_values.update(
        _sensitive_values(
            {
                event.get("event_id"),
                event.get("aggregate_id"),
                json.dumps(payload, sort_keys=True, default=str),
                *_mapping_values(payload),
            }
        )
    )

    artifact_raw = _payload_str(payload, "artifact_id")
    snapshot_raw = _payload_str(payload, "snapshot_id")
    provider = _payload_str(payload, "provider")
    provider_route = _payload_str(payload, "provider_route")
    try:
        artifact_id = _coerce_uuid(artifact_raw)
        snapshot_id = _coerce_uuid(snapshot_raw)
        aggregate_id = _coerce_uuid(event.get("aggregate_id"))
    except (TypeError, ValueError):
        _set_status(report, STATUS_INVALID_EVENT, "event_outbox.payload_ids")
        return None, False

    event_valid = True
    checks: list[str] = []
    if str(event.get("event_type")) != EXPECTED_EVENT_TYPE:
        event_valid = False
        checks.append("event_outbox.event_type")
    if str(event.get("aggregate_type")) != EXPECTED_AGGREGATE_TYPE:
        event_valid = False
        checks.append("event_outbox.aggregate_type")
    if str(event.get("status")) != "published":
        event_valid = False
        checks.append("event_outbox.status")
    if aggregate_id != artifact_id or root_object_id != artifact_id:
        event_valid = False
        checks.append("event_outbox.artifact_identity")
    if provider is not None and provider != EXPECTED_PROVIDER:
        event_valid = False
        checks.append("payload.provider")
    if provider_route is not None and provider_route != EXPECTED_PROVIDER:
        event_valid = False
        checks.append("payload.provider_route")
    if not await _x_snapshot_exists(session=session, artifact_id=artifact_id, snapshot_id=snapshot_id):
        event_valid = False
        checks.append("snapshot.x_post")

    if not event_valid:
        _set_status(report, STATUS_INVALID_EVENT)
        for check in checks:
            _set_status(report, report["contract_status"], check)
        return None, False

    report["snapshot_updated_event_valid_bucket"] = "one"
    report["current_snapshot_found_bucket"] = "one"

    candidate_group_ids = await _load_impacted_candidate_group_ids(session=session, artifact_id=artifact_id)
    raw_values.update(_sensitive_values(str(candidate_group_id) for candidate_group_id in candidate_group_ids))
    report["impacted_candidate_group_bucket"] = _bucket_count(len(candidate_group_ids))
    if not candidate_group_ids:
        _set_status(report, STATUS_NO_IMPACTED_CANDIDATE_GROUP, "candidate_group_members.artifact_id")
        return None, False
    if len(candidate_group_ids) > 1:
        _set_status(report, STATUS_INVALID_EVENT, "candidate_group_members.multiple_impacted_groups")
        return None, False

    planned_analysis_requested = await _planned_analysis_requested(
        session=session,
        candidate_group_ids=candidate_group_ids,
    )
    report["evidence_bundle_write_planned_bucket"] = "one"
    report["candidate_evidence_member_write_planned_bucket"] = "one"
    report["current_bundle_update_planned_bucket"] = "one"
    report["analysis_requested_outbox_planned_bucket"] = "one" if planned_analysis_requested else "zero"
    report["redis_ack_planned_bucket"] = "one"

    return (
        ValidationTarget(
            stream_entry=entry,
            trigger_event_id=trigger_event_id,
            artifact_id=artifact_id,
            snapshot_id=snapshot_id,
            impacted_candidate_group_ids=candidate_group_ids,
            planned_analysis_requested=planned_analysis_requested,
        ),
        True,
    )


def _mapping_values(payload: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for value in payload.values():
        if isinstance(value, str):
            values.append(value)
    return values


async def _consume_one_stream_entry(
    *,
    redis_client: RedisClientLike,
    expected_message_id: str,
    stream_scan_limit: int,
) -> StreamEntry | None:
    try:
        await _maybe_await(
            redis_client.xgroup_create(
                EXPECTED_QUEUE_NAME,
                CONSUMER_GROUP,
                id="0",
                mkstream=False,
            )
        )
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise

    raw = await _maybe_await(
        redis_client.xreadgroup(
            CONSUMER_GROUP,
            CONSUMER_NAME,
            {EXPECTED_QUEUE_NAME: ">"},
            count=stream_scan_limit,
            block=1000,
        )
    )
    for entry in _decode_xreadgroup_entries(raw):
        if entry.message_id == expected_message_id:
            return entry
    return None


async def _ack_stream_entry(
    *,
    redis_client: RedisClientLike,
    message_id: str,
) -> bool:
    acked = await _maybe_await(redis_client.xack(EXPECTED_QUEUE_NAME, CONSUMER_GROUP, message_id))
    return _safe_count(acked) > 0


def _build_evidence_assembler_config(
    *,
    database_url: str,
    redis_url: str,
    bundle_profile_version: str,
) -> EvidenceAssemblerConfig:
    return EvidenceAssemblerConfig(
        app_env="ops",
        database_url=database_url,
        redis_url=redis_url,
        queue_name=EXPECTED_QUEUE_NAME,
        consumer_group=CONSUMER_GROUP,
        consumer_name=CONSUMER_NAME,
        batch_size=1,
        block_ms=1000,
        bundle_profile_version=bundle_profile_version,
        enable_text_idea=False,
        enable_reroot=False,
        log_level="INFO",
    )


def _default_repository_factory(session: AsyncSessionLike) -> EvidenceAssemblerRepository:
    return EvidenceAssemblerRepository(session)  # type: ignore[arg-type]


def _default_service_factory(config: EvidenceAssemblerConfig, repository: Any) -> EvidenceAssemblerService:
    return EvidenceAssemblerService(config, repository=repository)


async def _invoke_evidence_assembler(
    *,
    report: dict[str, Any],
    target: ValidationTarget,
    session: AsyncSessionLike,
    database_url: str,
    redis_url: str,
    bundle_profile_version: str,
    service_factory: ServiceFactory | None,
    repository_factory: RepositoryFactory | None,
    raw_values: set[str],
) -> ScriptResult | None:
    candidate_group_id = target.impacted_candidate_group_ids[0]
    await _commit_if_supported(session)
    before = await _load_candidate_bundle_postconditions(
        session=session,
        candidate_group_id=candidate_group_id,
    )
    _record_baseline_postconditions(report, before)
    _add_postcondition_raw_values(raw_values, before)
    await _commit_if_supported(session)

    config = _build_evidence_assembler_config(
        database_url=database_url,
        redis_url=redis_url,
        bundle_profile_version=bundle_profile_version,
    )
    repository = (repository_factory or _default_repository_factory)(session)
    service = (service_factory or _default_service_factory)(config, repository)
    try:
        report["evidence_bundle_write_attempted"] = True
        report["candidate_evidence_member_write_attempted"] = True
        report["current_bundle_update_attempted"] = True
        if target.planned_analysis_requested:
            report["analysis_requested_outbox_write_attempted"] = True
        await _maybe_await(service.handle_trigger_event(str(target.trigger_event_id)))
    except Exception as exc:
        await session.rollback()
        if _looks_like_analysis_outbox_failure(exc):
            _set_status(report, STATUS_ANALYSIS_REQUESTED_OUTBOX_WRITE_FAILURE, "event_outbox.analysis_requested")
        else:
            _set_status(report, STATUS_EVIDENCE_BUNDLE_WRITE_FAILURE, "candidate_evidence_bundle.write")
        return ScriptResult(exit_code=1, report=report)

    await _commit_if_supported(session)
    after = await _load_candidate_bundle_postconditions(
        session=session,
        candidate_group_id=candidate_group_id,
    )
    _record_after_postconditions(report, before=before, after=after)
    _add_postcondition_raw_values(raw_values, after)
    if not _candidate_bundle_postconditions_pass(report=report, before=before, after=after):
        await session.rollback()
        return ScriptResult(exit_code=1, report=report)

    report["evidence_bundle_written_bucket"] = _bucket_count(
        after.candidate_evidence_bundle_count - before.candidate_evidence_bundle_count
    )
    report["candidate_evidence_member_written_bucket"] = _bucket_count(
        after.candidate_evidence_member_count - before.candidate_evidence_member_count
    )
    report["current_bundle_updated_bucket"] = _bucket_count(
        1 if after.current_bundle_id is not None and after.current_bundle_id != before.current_bundle_id else 0
    )
    report["analysis_requested_outbox_written_bucket"] = _bucket_count(
        after.pending_analysis_requested_count - before.pending_analysis_requested_count
    )
    return None


def _looks_like_analysis_outbox_failure(exc: Exception) -> bool:
    name = type(exc).__name__.lower()
    return "analysis" in name or "outbox" in name


async def _approved_consume(
    *,
    report: dict[str, Any],
    target: ValidationTarget,
    session: AsyncSessionLike,
    redis_client: RedisClientLike,
    stream_scan_limit: int,
    database_url: str,
    redis_url: str,
    bundle_profile_version: str,
    service_factory: ServiceFactory | None,
    repository_factory: RepositoryFactory | None,
    raw_values: set[str],
) -> ScriptResult:
    consumed_entry = await _consume_one_stream_entry(
        redis_client=redis_client,
        expected_message_id=target.stream_entry.message_id,
        stream_scan_limit=stream_scan_limit,
    )
    if consumed_entry is None or not _entry_base_shape_is_valid(consumed_entry):
        _set_status(report, STATUS_INVALID_REDIS_PAYLOAD, "redis.consumed_entry")
        return ScriptResult(exit_code=1, report=report)

    service_result = await _invoke_evidence_assembler(
        report=report,
        target=target,
        session=session,
        database_url=database_url,
        redis_url=redis_url,
        bundle_profile_version=bundle_profile_version,
        service_factory=service_factory,
        repository_factory=repository_factory,
        raw_values=raw_values,
    )
    if service_result is not None:
        return service_result

    if _report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)

    try:
        report["redis_ack_attempted"] = True
        if not await _ack_stream_entry(redis_client=redis_client, message_id=target.stream_entry.message_id):
            _set_status(report, STATUS_REDIS_ACK_FAILURE, "redis.ack")
            return ScriptResult(exit_code=1, report=report)
        report["redis_ack_succeeded_bucket"] = "one"
    except Exception:
        _set_status(report, STATUS_REDIS_ACK_FAILURE, "redis.ack")
        return ScriptResult(exit_code=1, report=report)

    if _report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)

    _set_status(report, STATUS_CONSUMED)
    return ScriptResult(exit_code=0, report=report)


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    stream_scan_limit: int = DEFAULT_STREAM_SCAN_LIMIT,
    approvals: ConsumeApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    service_factory: ServiceFactory | None = None,
    repository_factory: RepositoryFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    approvals = approvals or ConsumeApprovals(False, False, False, False)
    report = _base_report()
    _apply_side_effect_flags(report, side_effect_flags)
    if _forbidden_side_effect_detected(report):
        _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "side_effect.forbidden")
        return ScriptResult(exit_code=1, report=report)
    if stream_scan_limit <= 0 or stream_scan_limit > MAX_STREAM_SCAN_LIMIT:
        _set_status(report, STATUS_NOT_READY, "stream_scan_limit.out_of_bounds")
        return ScriptResult(exit_code=1, report=report)

    session: AsyncSessionLike | None = None
    redis_client: RedisClientLike | None = None
    raw_values: set[str] = {str(value) for value in forbidden_raw_values if str(value)}

    try:
        try:
            values = _read_runtime_env(runtime_env_path, runtime_env_reader)
            report["runtime_env_read"] = True
            raw_values.add(str(runtime_env_path))
        except Exception:
            _set_status(report, STATUS_NOT_READY, "runtime_env.read")
            return ScriptResult(exit_code=1, report=report)

        runtime_config = _extract_runtime_config(report=report, values=values, raw_values=raw_values)
        if runtime_config is None:
            return ScriptResult(exit_code=1, report=report)
        database_url, redis_url, bundle_profile_version = runtime_config

        if approvals.any_granted and not approvals.all_granted:
            _approval_block_status(report, approvals)
            return ScriptResult(exit_code=1, report=report)

        try:
            session = await _open_database_session(database_url, database_session_factory)
            if not approvals.all_granted:
                await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
                read_only_value = await _scalar(await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY))
                report["read_only_transaction"] = _transaction_read_only_enabled(read_only_value)
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
            redis_entries = await _inspect_redis_entries(redis_client=redis_client, stream_scan_limit=stream_scan_limit)
            report["redis_connected"] = True
        except Exception:
            _set_status(report, STATUS_NOT_READY, "redis.connection_or_stream_inspection")
            return ScriptResult(exit_code=1, report=report)

        target, selection_valid = await _select_and_validate_target(
            report=report,
            session=session,
            redis_entries=redis_entries,
            raw_values=raw_values,
        )
        if _report_contains_raw_values(report, raw_values):
            report["raw_values_emitted"] = True
            _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
            return ScriptResult(exit_code=1, report=report)
        if target is None:
            return ScriptResult(exit_code=0 if selection_valid else 1, report=report)

        if not approvals.all_granted:
            _set_status(report, STATUS_READY)
            return ScriptResult(exit_code=0, report=report)

        return await _approved_consume(
            report=report,
            target=target,
            session=session,
            redis_client=redis_client,
            stream_scan_limit=stream_scan_limit,
            database_url=database_url,
            redis_url=redis_url,
            bundle_profile_version=bundle_profile_version,
            service_factory=service_factory,
            repository_factory=repository_factory,
            raw_values=raw_values,
        )
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
    stream_scan_limit: int = DEFAULT_STREAM_SCAN_LIMIT,
    approvals: ConsumeApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    service_factory: ServiceFactory | None = None,
    repository_factory: RepositoryFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            stream_scan_limit=stream_scan_limit,
            approvals=approvals,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            redis_client_factory=redis_client_factory,
            service_factory=service_factory,
            repository_factory=repository_factory,
            side_effect_flags=side_effect_flags,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def _sensitive_values(values: Any) -> set[str]:
    sensitive: set[str] = set()
    for value in values:
        if value is None:
            continue
        text = str(value)
        if not text:
            continue
        lowered = text.lower()
        if "://" in text or _looks_like_uuid(text):
            sensitive.add(text)
            continue
        if any(token in lowered for token in ("secret", "token", "password", "apikey", "api_key")):
            sensitive.add(text)
            continue
        if len(text) >= 16 and text not in {EXPECTED_EVENT_TYPE}:
            sensitive.add(text)
    return sensitive


def _looks_like_uuid(value: str) -> bool:
    return bool(
        re.fullmatch(
            r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
            value,
        )
    )


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(value and value in rendered for value in raw_values)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        stream_scan_limit=args.stream_scan_limit,
        approvals=ConsumeApprovals(
            artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume=(
                args.approved_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume
            ),
            candidate_evidence_bundle_write=args.approved_candidate_evidence_bundle_write,
            analysis_requested_outbox_write=args.approved_analysis_requested_outbox_write,
            redis_ack=args.approved_redis_ack,
        ),
    )
    print(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
