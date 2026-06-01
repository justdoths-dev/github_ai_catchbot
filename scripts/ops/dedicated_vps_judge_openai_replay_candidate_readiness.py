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

import sqlalchemy as sa


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_judge_openai_replay_candidate_readiness"
REPORT_TYPE = "judge_openai_replay_candidate_readiness_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_SCAN_LIMIT = 100
MAX_SCAN_LIMIT = 250
CANDIDATE_DETAIL_LIMIT = 10

ANALYSIS_REQUESTED_EVENT_TYPE = "analysis.requested.v1"
JUDGE_CALL_REQUESTED_EVENT_TYPE = "judge.call.requested.v1"
JUDGE_OUTPUT_READY_EVENT_TYPE = "judge.output.ready.v1"
EXPECTED_QUEUE_NAME = "q.analysis.judge"
EXPECTED_STAGE_NAME = "judge"
EXPECTED_AGGREGATE_TYPE = "judge_run"
EXPECTED_SCHEMA_VERSION = "judge_output_v1"
EXPECTED_POLICY_VERSION = "verdict_policy_v1"
DEFAULT_MODEL = "gpt-5.4-mini"
DEFAULT_REASONING_EFFORT = "low"
ESCALATION_MODEL = "gpt-5.4"
ESCALATION_REASONING_EFFORT = "medium"
REPLAY_PROMPT_SUFFIX = "__replay_live_smoke_v1"
REPLAY_REASON_CODE = "manual_live_smoke_replay"
PROMPT_VERSION_BY_PROFILE = {
    "github_primary": "judge_github_primary_v1",
    "x_primary": "judge_x_primary_v1",
    "text_idea_primary": "judge_text_idea_primary_v1",
}
PROFILE_BY_ARTIFACT_TYPE = {
    "github_repo": "github_primary",
    "github_subpath": "github_primary",
    "github_gist": "github_primary",
    "github_repo_page": "github_primary",
    "x_post": "x_primary",
    "web_article": "text_idea_primary",
    "text_idea": "text_idea_primary",
}
ALLOWED_JUDGE_PROFILES = frozenset(PROMPT_VERSION_BY_PROFILE)
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

STATUS_PREFLIGHT_PASSED = "judge_openai_replay_candidate_readiness_preflight_passed"
STATUS_APPROVED_PREPARED = "judge_openai_replay_candidate_readiness_approved_prepared"
STATUS_NOT_READY = "blocked_judge_openai_replay_candidate_readiness_not_ready"
STATUS_MISSING_APPROVAL = (
    "blocked_judge_openai_replay_candidate_readiness_missing_approval"
)
STATUS_NO_CANDIDATE = "blocked_judge_openai_replay_candidate_readiness_no_candidate"
STATUS_AMBIGUOUS_CANDIDATE = (
    "blocked_judge_openai_replay_candidate_readiness_ambiguous_candidate"
)
STATUS_EXISTING_REPLAY_RUN = (
    "blocked_judge_openai_replay_candidate_readiness_existing_replay_run"
)
STATUS_EXISTING_Q_CANDIDATE = (
    "blocked_judge_openai_replay_candidate_readiness_existing_q_analysis_judge_candidate"
)
STATUS_EXISTING_REPLAY_OUTPUT = (
    "blocked_judge_openai_replay_candidate_readiness_existing_replay_output"
)
STATUS_EXISTING_REPLAY_READY_OUTBOX = (
    "blocked_judge_openai_replay_candidate_readiness_existing_replay_ready_outbox"
)
STATUS_FORBIDDEN_SIDE_EFFECT = (
    "blocked_judge_openai_replay_candidate_readiness_forbidden_side_effect"
)
STATUS_WRITE_FAILED = "blocked_judge_openai_replay_candidate_readiness_write_failed"
STATUS_REDIS_PUBLISH_FAILED = (
    "blocked_judge_openai_replay_candidate_readiness_redis_publish_failed"
)
STATUS_RAW_VALUE_EMISSION = (
    "blocked_judge_openai_replay_candidate_readiness_raw_value_emission"
)

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
SELECT_PENDING_ANALYSIS_REQUESTED_EVENTS_QUERY = """
SELECT event_id, event_type, aggregate_type, aggregate_id, dedupe_key,
       payload_json, status, created_at
FROM event_outbox
WHERE status = 'pending'::outbox_status_enum
  AND event_type = 'analysis.requested.v1'
ORDER BY created_at DESC, event_id DESC
LIMIT :limit
"""
SELECT_BUNDLE_BY_ID_QUERY = """
SELECT b.bundle_id,
       b.candidate_group_id,
       b.ready_for_analysis,
       b.primary_summary,
       b.reroot_count,
       b.token_budget_profile,
       cgp.current_bundle_id,
       ar.artifact_type
FROM candidate_evidence_bundles b
JOIN candidate_group_proposals cgp
  ON cgp.candidate_group_id = b.candidate_group_id
LEFT JOIN artifact_registry ar
  ON ar.artifact_id = b.current_primary_artifact_id
WHERE b.bundle_id = CAST(:bundle_id AS uuid)
"""
SELECT_CURRENT_READY_BUNDLES_QUERY = """
SELECT b.bundle_id,
       b.candidate_group_id,
       b.ready_for_analysis,
       b.primary_summary,
       b.reroot_count,
       b.token_budget_profile,
       cgp.current_bundle_id,
       ar.artifact_type
FROM candidate_evidence_bundles b
JOIN candidate_group_proposals cgp
  ON cgp.candidate_group_id = b.candidate_group_id
LEFT JOIN artifact_registry ar
  ON ar.artifact_id = b.current_primary_artifact_id
WHERE b.ready_for_analysis IS TRUE
  AND cgp.current_bundle_id = b.bundle_id
ORDER BY b.created_at DESC, b.bundle_id DESC
LIMIT :limit
"""
SELECT_BUNDLE_SHAPE_STATS_QUERY = """
SELECT COUNT(*) AS member_count,
       COUNT(*) FILTER (WHERE member_role = 'supporting') AS supporting_count
FROM candidate_evidence_members
WHERE bundle_id = CAST(:bundle_id AS uuid)
"""
SELECT_ANALYSIS_REQUESTED_EVENTS_FOR_BUNDLE_QUERY = """
SELECT event_id, event_type, aggregate_type, aggregate_id, dedupe_key,
       payload_json, status, fail_count, created_at
FROM event_outbox
WHERE event_type = 'analysis.requested.v1'
  AND payload_json->>'bundle_id' = :bundle_id
ORDER BY created_at DESC, event_id DESC
LIMIT :limit
"""
SELECT_HISTORICAL_TERMINAL_JUDGE_RUNS_FOR_BUNDLE_QUERY = """
SELECT judge_run_id, bundle_id, judge_profile, model, reasoning_effort,
       prompt_version, schema_version, policy_version, prompt_cache_key, status
FROM judge_runs
WHERE bundle_id = CAST(:bundle_id AS uuid)
  AND status IN ('succeeded', 'failed_terminal')
  AND prompt_version NOT LIKE :replay_prompt_like
ORDER BY finished_at DESC NULLS LAST,
         started_at DESC NULLS LAST,
         judge_run_id DESC
LIMIT :limit
"""
SELECT_JUDGE_RUNS_FOR_DECISION_QUERY = """
SELECT judge_run_id, bundle_id, judge_profile, model, reasoning_effort,
       prompt_version, schema_version, policy_version, prompt_cache_key, status
FROM judge_runs
WHERE bundle_id = CAST(:bundle_id AS uuid)
  AND prompt_version = :prompt_version
  AND model = :model
  AND reasoning_effort = :reasoning_effort
"""
SELECT_JUDGE_RUN_BY_ID_QUERY = """
SELECT judge_run_id, bundle_id, judge_profile, model, reasoning_effort,
       prompt_version, schema_version, policy_version, prompt_cache_key, status
FROM judge_runs
WHERE judge_run_id = CAST(:judge_run_id AS uuid)
"""
COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM judge_outputs
WHERE judge_run_id = CAST(:judge_run_id AS uuid)
"""
COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'judge.output.ready.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
"""
COUNT_ANALYSES_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM analyses a
JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
"""
COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM notification_plans np
JOIN analyses a ON a.analysis_id = np.analysis_id
JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
"""
SELECT_EVENT_OUTBOX_BY_ID_QUERY = """
SELECT event_id, event_type, aggregate_type, aggregate_id, dedupe_key,
       payload_json, status, fail_count, created_at
FROM event_outbox
WHERE event_id = CAST(:event_id AS uuid)
"""
SELECT_PENDING_JUDGE_CALL_OUTBOX_FOR_RUN_QUERY = """
SELECT event_id, event_type, aggregate_type, aggregate_id, dedupe_key,
       payload_json, status, fail_count, created_at
FROM event_outbox
WHERE event_type = 'judge.call.requested.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
  AND status = 'pending'::outbox_status_enum
ORDER BY created_at DESC, event_id DESC
"""
SELECT_JUDGE_CALL_OUTBOX_BY_DEDUPE_KEY_QUERY = """
SELECT event_id, event_type, aggregate_type, aggregate_id, dedupe_key,
       payload_json, status, fail_count, created_at
FROM event_outbox
WHERE dedupe_key = :dedupe_key
  AND event_type = 'judge.call.requested.v1'
  AND aggregate_type = 'judge_run'
LIMIT 1
"""
INSERT_JUDGE_RUN_QUERY = """
INSERT INTO judge_runs (
    bundle_id,
    judge_profile,
    model,
    reasoning_effort,
    prompt_version,
    schema_version,
    policy_version,
    prompt_cache_key,
    status
) VALUES (
    CAST(:bundle_id AS uuid),
    :judge_profile,
    :model,
    :reasoning_effort,
    :prompt_version,
    :schema_version,
    :policy_version,
    :prompt_cache_key,
    'pending'
)
ON CONFLICT ON CONSTRAINT uq_judge_runs_bundle_prompt_model_effort
DO NOTHING
RETURNING judge_run_id
"""
INSERT_JUDGE_CALL_REQUESTED_OUTBOX_QUERY = """
INSERT INTO event_outbox (
    event_type, aggregate_type, aggregate_id, dedupe_key,
    payload_json, status, created_at
) VALUES (
    'judge.call.requested.v1',
    'judge_run',
    CAST(:judge_run_id AS uuid),
    :dedupe_key,
    CAST(:payload_json AS jsonb),
    'pending'::outbox_status_enum,
    now()
)
ON CONFLICT (dedupe_key) DO NOTHING
RETURNING event_id, event_type, aggregate_type, aggregate_id, dedupe_key,
          payload_json, status, fail_count, created_at
"""
MARK_EVENT_OUTBOX_PUBLISHED_QUERY = """
UPDATE event_outbox
SET status = 'published'::outbox_status_enum,
    published_at = :published_at
WHERE event_id = CAST(:event_id AS uuid)
  AND status = 'pending'::outbox_status_enum
"""
REQUIRED_TABLES = (
    "event_outbox",
    "candidate_group_proposals",
    "candidate_evidence_bundles",
    "candidate_evidence_members",
    "artifact_registry",
    "judge_runs",
    "judge_outputs",
    "analyses",
    "notification_plans",
)
SIDE_EFFECT_REPORT_FIELDS = (
    "openai_call_attempted",
    "analysis_validator_started",
    "policy_engine_started",
    "notifier_started",
    "telegram_send_attempted",
    "q_analysis_validate_published",
    "q_analysis_policy_published",
    "q_notification_send_published",
    "redis_ack_attempted",
    "redis_delete_or_trim_attempted",
    "judge_outputs_written_bucket",
    "judge_output_ready_outbox_written_bucket",
    "analysis_rows_written_bucket",
    "notification_rows_written_bucket",
    "raw_values_emitted",
)


class AsyncSessionLike(Protocol):
    def in_transaction(self) -> bool: ...
    def begin(self) -> Any: ...
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def close(self) -> None: ...


class RedisClientLike(Protocol):
    async def ping(self) -> Any: ...
    async def xrevrange(self, name: str, count: int | None = None) -> Any: ...
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
class ReplayCandidateApprovals:
    db_write: bool = False
    redis_publish: bool = False
    replay_candidate: bool = False

    @property
    def all_granted(self) -> bool:
        return self.db_write and self.redis_publish and self.replay_candidate

    @property
    def any_granted(self) -> bool:
        return self.db_write or self.redis_publish or self.replay_candidate

    def missing_checks(self) -> list[str]:
        checks: list[str] = []
        if not self.db_write:
            checks.append("approval.db_write")
        if not self.redis_publish:
            checks.append("approval.redis_publish")
        if not self.replay_candidate:
            checks.append("approval.replay_candidate")
        return checks


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    database_url: str
    redis_url: str
    xadd_maxlen: int | None
    default_model: str
    escalation_model: str
    default_reasoning_effort: str
    escalation_reasoning_effort: str
    enable_model_escalation: bool
    prompt_versions: dict[str, str]
    schema_version: str
    policy_version: str


@dataclass(frozen=True, slots=True)
class EventOutboxRow:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    dedupe_key: str
    payload_json: dict[str, Any]
    status: str
    fail_count: int
    created_at: Any


@dataclass(frozen=True, slots=True)
class BundleCandidate:
    bundle_id: UUID
    candidate_group_id: UUID
    ready_for_analysis: bool
    primary_summary: dict[str, Any]
    current_bundle_id: UUID | None
    artifact_type: str | None
    reroot_count: int
    token_budget_profile: str | None


@dataclass(frozen=True, slots=True)
class BundleShapeStats:
    member_count: int
    supporting_count: int


@dataclass(frozen=True, slots=True)
class JudgeDecision:
    judge_profile: str
    model: str
    reasoning_effort: str
    prompt_version: str
    schema_version: str
    policy_version: str
    prompt_cache_key: str


@dataclass(frozen=True, slots=True)
class JudgeRunRow:
    judge_run_id: UUID
    bundle_id: UUID
    judge_profile: str
    model: str
    reasoning_effort: str
    prompt_version: str
    schema_version: str
    policy_version: str
    prompt_cache_key: str | None
    status: str


@dataclass(frozen=True, slots=True)
class SelectedCandidate:
    source: str
    bundle: BundleCandidate
    shape: BundleShapeStats
    decision: JudgeDecision
    analysis_event_id: UUID | None = None
    historical_judge_run_id: UUID | None = None


class _DefaultDatabaseSession:
    def __init__(self, engine: Any, session: Any) -> None:
        self._engine = engine
        self._session = session

    def in_transaction(self) -> bool:
        return bool(self._session.in_transaction())

    def begin(self) -> Any:
        return self._session.begin()

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any:
        return await self._session.execute(statement, params or {})

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()
        await self._engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare one replay/manual pending judge-openai candidate for the "
            "single-live-call smoke. Default mode is read-only; approved mode "
            "needs DB-write, Redis-publish, and replay-candidate approvals."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument(
        "--scan-limit",
        type=_bounded_positive_int_named("scan-limit", upper_bound=MAX_SCAN_LIMIT),
        default=DEFAULT_SCAN_LIMIT,
    )
    parser.add_argument("--approve-db-write", action="store_true")
    parser.add_argument("--approve-redis-publish", action="store_true")
    parser.add_argument("--approve-replay-candidate", action="store_true")
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


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


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_NOT_READY,
        "runtime_env_read": False,
        "database_configured": False,
        "redis_configured": False,
        "database_connected": False,
        "redis_connected": False,
        "replay_candidate_found_bucket": "zero",
        "analysis_requested_event_found_bucket": "zero",
        "current_ready_bundle_found_bucket": "zero",
        "historical_terminal_judge_run_found_bucket": "zero",
        "existing_replay_judge_run_bucket": "zero",
        "existing_replay_output_bucket": "zero",
        "existing_replay_ready_outbox_bucket": "zero",
        "existing_q_analysis_judge_candidate_bucket": "zero",
        "replay_judge_run_created_bucket": "zero",
        "replay_judge_call_requested_outbox_created_bucket": "zero",
        "replay_judge_call_requested_outbox_reused_bucket": "zero",
        "q_analysis_judge_published_bucket": "zero",
        "event_outbox_marked_published_bucket": "zero",
        "openai_call_attempted": False,
        "openai_key_file_read_bucket": "zero",
        "judge_outputs_written_bucket": "zero",
        "judge_output_ready_outbox_written_bucket": "zero",
        "analysis_rows_written_bucket": "zero",
        "notification_rows_written_bucket": "zero",
        "analysis_validator_started": False,
        "policy_engine_started": False,
        "notifier_started": False,
        "telegram_send_attempted": False,
        "q_analysis_validate_published": False,
        "q_analysis_policy_published": False,
        "q_notification_send_published": False,
        "redis_ack_attempted": False,
        "redis_delete_or_trim_attempted": False,
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
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
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


def _extract_runtime_config(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> RuntimeConfig | None:
    database_url = str(values.get("DATABASE_URL", "")).strip()
    redis_url = str(values.get("REDIS_URL", "")).strip()
    if database_url:
        raw_values.add(database_url)
    if redis_url:
        raw_values.add(redis_url)
    report["database_configured"] = bool(database_url)
    report["redis_configured"] = bool(redis_url)

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

    xadd_maxlen_raw = str(values.get("OUTBOX_RELAY_XADD_MAXLEN", "10000")).strip()
    try:
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
    except ValueError:
        _set_status(report, STATUS_NOT_READY, "redis.xadd_maxlen_invalid")
        return None
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        _set_status(report, STATUS_NOT_READY, "redis.xadd_maxlen_invalid")
        return None

    return RuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        xadd_maxlen=xadd_maxlen,
        default_model=DEFAULT_MODEL,
        escalation_model=ESCALATION_MODEL,
        default_reasoning_effort=DEFAULT_REASONING_EFFORT,
        escalation_reasoning_effort=ESCALATION_REASONING_EFFORT,
        enable_model_escalation=False,
        prompt_versions=dict(PROMPT_VERSION_BY_PROFILE),
        schema_version=EXPECTED_SCHEMA_VERSION,
        policy_version=EXPECTED_POLICY_VERSION,
    )


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
    return await session.execute(sa.text(statement), params or {})


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


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _json_default(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"unsupported json type: {type(value)!r}")


def _jsonb_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


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


def _event_row_from_mapping(row: Mapping[str, Any]) -> EventOutboxRow:
    payload = _json_loads(row["payload_json"])
    return EventOutboxRow(
        event_id=_coerce_uuid(row["event_id"]),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=_coerce_uuid(row["aggregate_id"]),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload if isinstance(payload, dict) else {},
        status=str(row["status"]),
        fail_count=int(row.get("fail_count", 0) or 0),
        created_at=row.get("created_at"),
    )


def _bundle_from_mapping(row: Mapping[str, Any]) -> BundleCandidate:
    primary_summary = _json_loads(row["primary_summary"])
    return BundleCandidate(
        bundle_id=_coerce_uuid(row["bundle_id"]),
        candidate_group_id=_coerce_uuid(row["candidate_group_id"]),
        ready_for_analysis=bool(row["ready_for_analysis"]),
        primary_summary=primary_summary if isinstance(primary_summary, dict) else {},
        current_bundle_id=_uuid_or_none(row["current_bundle_id"]),
        artifact_type=str(row["artifact_type"]) if row["artifact_type"] is not None else None,
        reroot_count=int(row["reroot_count"] or 0),
        token_budget_profile=(
            str(row["token_budget_profile"]) if row["token_budget_profile"] else None
        ),
    )


def _judge_run_from_mapping(row: Mapping[str, Any]) -> JudgeRunRow:
    return JudgeRunRow(
        judge_run_id=_coerce_uuid(row["judge_run_id"]),
        bundle_id=_coerce_uuid(row["bundle_id"]),
        judge_profile=str(row["judge_profile"]),
        model=str(row["model"]),
        reasoning_effort=str(row["reasoning_effort"]),
        prompt_version=str(row["prompt_version"]),
        schema_version=str(row["schema_version"]),
        policy_version=str(row["policy_version"]),
        prompt_cache_key=(
            str(row["prompt_cache_key"]) if row["prompt_cache_key"] is not None else None
        ),
        status=str(row["status"]),
    )


def _derive_profile_from_artifact_type(artifact_type: str | None) -> str | None:
    if artifact_type is None:
        return None
    return PROFILE_BY_ARTIFACT_TYPE.get(artifact_type)


def _decision_for_candidate(
    *,
    config: RuntimeConfig,
    profile: str,
    base_prompt_version: str | None = None,
) -> JudgeDecision | None:
    if profile not in ALLOWED_JUDGE_PROFILES:
        return None
    base_prompt_version = (base_prompt_version or config.prompt_versions[profile]).strip()
    if not base_prompt_version:
        return None
    prompt_version = (
        base_prompt_version
        if base_prompt_version.endswith(REPLAY_PROMPT_SUFFIX)
        else f"{base_prompt_version}{REPLAY_PROMPT_SUFFIX}"
    )
    schema_version = config.schema_version
    policy_version = config.policy_version
    return JudgeDecision(
        judge_profile=profile,
        model=config.default_model,
        reasoning_effort=config.default_reasoning_effort,
        prompt_version=prompt_version,
        schema_version=schema_version,
        policy_version=policy_version,
        prompt_cache_key=f"judge:{profile}:{prompt_version}:{schema_version}:{policy_version}",
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


def _forbidden_side_effect_detected(report: Mapping[str, Any]) -> bool:
    for field in SIDE_EFFECT_REPORT_FIELDS:
        value = report[field]
        if isinstance(value, str):
            if value != "zero":
                return True
            continue
        if bool(value):
            return True
    return False


def _approval_block_status(report: dict[str, Any], approvals: ReplayCandidateApprovals) -> None:
    _set_status(report, STATUS_MISSING_APPROVAL)
    for check in approvals.missing_checks():
        _set_status(report, report["contract_status"], check)


def _raw_values_from_event_rows(rows: Sequence[EventOutboxRow]) -> set[str]:
    values: set[str] = set()
    for row in rows:
        values.update({str(row.event_id), str(row.aggregate_id), row.dedupe_key})
        values.add(json.dumps(row.payload_json, sort_keys=True, default=str))
        for value in row.payload_json.values():
            if isinstance(value, str) and value:
                values.add(value)
    return {value for value in values if len(value) >= 6}


def _raw_values_from_candidate(candidate: SelectedCandidate) -> set[str]:
    values = {
        str(candidate.bundle.bundle_id),
        str(candidate.bundle.candidate_group_id),
    }
    if candidate.analysis_event_id is not None:
        values.add(str(candidate.analysis_event_id))
    if candidate.historical_judge_run_id is not None:
        values.add(str(candidate.historical_judge_run_id))
    if candidate.bundle.current_bundle_id is not None:
        values.add(str(candidate.bundle.current_bundle_id))
    return {value for value in values if len(value) >= 6}


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(value in rendered for value in raw_values)


async def _load_bundle(
    repository: "ReplayCandidateRepository",
    bundle_id: UUID,
) -> BundleCandidate | None:
    row = await repository.load_bundle_by_id(bundle_id)
    if row is None:
        return None
    return row


async def _active_q_analysis_judge_candidate_count(
    *,
    report: dict[str, Any],
    repository: "ReplayCandidateRepository",
    redis_client: RedisClientLike,
    raw_values: set[str],
    scan_limit: int,
) -> int:
    entries = await _maybe_await(redis_client.xrevrange(EXPECTED_QUEUE_NAME, count=scan_limit))
    active = 0
    for entry in entries:
        fields = _redis_entry_fields(entry)
        trigger_event_id = _uuid_or_none(fields.get("trigger_event_id"))
        root_object_id = _uuid_or_none(fields.get("root_object_id"))
        if trigger_event_id is None or root_object_id is None:
            continue
        raw_values.update({str(trigger_event_id), str(root_object_id)})
        if fields.get("stage_name") != EXPECTED_STAGE_NAME:
            continue
        if fields.get("root_object_type") != EXPECTED_AGGREGATE_TYPE:
            continue
        event = await repository.load_event_by_id(trigger_event_id)
        if event is None:
            continue
        raw_values.update(_raw_values_from_event_rows([event]))
        if event.event_type != JUDGE_CALL_REQUESTED_EVENT_TYPE or event.status != "pending":
            continue
        judge_run = await repository.load_judge_run_by_id(root_object_id)
        if judge_run is None or judge_run.status != "pending":
            continue
        outputs = await repository.count_judge_outputs_for_run(root_object_id)
        ready_outbox = await repository.count_judge_output_ready_outbox_for_run(root_object_id)
        if outputs == 0 and ready_outbox == 0:
            active += 1
    report["existing_q_analysis_judge_candidate_bucket"] = _bucket_count(active)
    return active


def _redis_entry_fields(entry: Any) -> dict[str, str]:
    if isinstance(entry, Mapping):
        fields = entry
    elif isinstance(entry, (tuple, list)) and len(entry) >= 2:
        fields = entry[1]
    else:
        return {}
    normalized: dict[str, str] = {}
    for key, value in dict(fields).items():
        k = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        if isinstance(value, bytes):
            normalized[k] = value.decode("utf-8")
        else:
            normalized[k] = str(value)
    return normalized


async def _evaluate_candidate(
    *,
    report: dict[str, Any],
    repository: "ReplayCandidateRepository",
    config: RuntimeConfig,
    bundle: BundleCandidate,
    source: str,
) -> tuple[SelectedCandidate | None, str | None, str | None]:
    if bundle.current_bundle_id != bundle.bundle_id:
        return None, None, None
    if not bundle.ready_for_analysis:
        return None, None, None
    if not bundle.primary_summary:
        return None, None, None
    shape = await repository.load_bundle_shape_stats(bundle.bundle_id)
    if shape.member_count <= 0:
        return None, None, None

    historical_runs = await repository.load_historical_terminal_judge_runs_for_bundle(
        bundle.bundle_id,
        limit=CANDIDATE_DETAIL_LIMIT,
    )
    report["historical_terminal_judge_run_found_bucket"] = _bucket_count(
        len(historical_runs)
    )
    if not historical_runs:
        return None, None, None

    analysis_events = await repository.fetch_analysis_requested_events_for_bundle(
        bundle.bundle_id,
        limit=CANDIDATE_DETAIL_LIMIT,
    )
    report["analysis_requested_event_found_bucket"] = _bucket_count(len(analysis_events))
    profile, base_prompt_version = _derive_profile_for_replay(
        historical_runs=historical_runs,
        analysis_events=analysis_events,
        artifact_type=bundle.artifact_type,
    )
    if profile not in ALLOWED_JUDGE_PROFILES:
        return None, None, None

    decision = _decision_for_candidate(
        config=config,
        profile=profile,
        base_prompt_version=base_prompt_version,
    )
    if decision is None:
        return None, None, None

    existing_replay_runs = await repository.load_judge_runs_for_decision(
        bundle_id=bundle.bundle_id,
        decision=decision,
    )
    report["existing_replay_judge_run_bucket"] = _bucket_count(len(existing_replay_runs))
    if existing_replay_runs:
        for replay_run in existing_replay_runs:
            output_count = await repository.count_judge_outputs_for_run(
                replay_run.judge_run_id
            )
            report["existing_replay_output_bucket"] = _bucket_count(output_count)
            if output_count:
                return None, STATUS_EXISTING_REPLAY_OUTPUT, "judge_output.replay_exists"
            ready_count = await repository.count_judge_output_ready_outbox_for_run(
                replay_run.judge_run_id
            )
            report["existing_replay_ready_outbox_bucket"] = _bucket_count(ready_count)
            if ready_count:
                return (
                    None,
                    STATUS_EXISTING_REPLAY_READY_OUTBOX,
                    "judge_output_ready_outbox.replay_exists",
                )
        return None, STATUS_EXISTING_REPLAY_RUN, "judge_run.replay_exists"

    return (
        SelectedCandidate(
            source=source,
            bundle=bundle,
            shape=shape,
            decision=decision,
            analysis_event_id=analysis_events[0].event_id if analysis_events else None,
            historical_judge_run_id=historical_runs[0].judge_run_id,
        ),
        None,
        None,
    )


def _derive_profile_for_replay(
    *,
    historical_runs: Sequence[JudgeRunRow],
    analysis_events: Sequence[EventOutboxRow],
    artifact_type: str | None,
) -> tuple[str | None, str | None]:
    for run in historical_runs:
        if (
            run.judge_profile in ALLOWED_JUDGE_PROFILES
            and run.prompt_version
            and not run.prompt_version.endswith(REPLAY_PROMPT_SUFFIX)
        ):
            return run.judge_profile, run.prompt_version
    for event in analysis_events:
        profile = _payload_str(event.payload_json, "judge_profile")
        if profile in ALLOWED_JUDGE_PROFILES:
            return profile, _payload_str(event.payload_json, "prompt_version")
    return _derive_profile_from_artifact_type(artifact_type), None


def _judge_run_matches_decision(
    row: JudgeRunRow,
    bundle_id: UUID,
    decision: JudgeDecision,
) -> bool:
    return (
        row.bundle_id == bundle_id
        and row.judge_profile == decision.judge_profile
        and row.model == decision.model
        and row.reasoning_effort == decision.reasoning_effort
        and row.prompt_version == decision.prompt_version
        and row.schema_version == decision.schema_version
        and row.policy_version == decision.policy_version
        and row.prompt_cache_key == decision.prompt_cache_key
    )


async def _select_candidate(
    *,
    report: dict[str, Any],
    repository: "ReplayCandidateRepository",
    config: RuntimeConfig,
    scan_limit: int,
) -> tuple[SelectedCandidate | None, set[str], str | None, str | None]:
    raw_values: set[str] = set()
    bundle_rows = await repository.fetch_current_ready_bundles(limit=scan_limit)
    bundle_candidates: list[SelectedCandidate] = []
    for bundle in bundle_rows:
        selected, status, check = await _evaluate_candidate(
            report=report,
            repository=repository,
            config=config,
            bundle=bundle,
            source="current_ready_bundle",
        )
        if status is not None:
            return None, raw_values, status, check
        if selected is not None:
            bundle_candidates.append(selected)
            raw_values.update(_raw_values_from_candidate(selected))

    report["current_ready_bundle_found_bucket"] = _bucket_count(len(bundle_rows))
    if len(bundle_candidates) > 1:
        return None, raw_values, STATUS_AMBIGUOUS_CANDIDATE, "current_ready_bundle.multiple"
    if len(bundle_candidates) == 1:
        report["replay_candidate_found_bucket"] = "one"
        return bundle_candidates[0], raw_values, None, None
    return None, raw_values, STATUS_NO_CANDIDATE, "candidate.none"


def _build_judge_call_payload(judge_run_id: UUID, candidate: SelectedCandidate) -> dict[str, str]:
    decision = candidate.decision
    return {
        "judge_run_id": str(judge_run_id),
        "bundle_id": str(candidate.bundle.bundle_id),
        "model": decision.model,
        "reasoning_effort": decision.reasoning_effort,
        "prompt_version": decision.prompt_version,
        "prompt_cache_key": decision.prompt_cache_key,
        "replay_reason_code": REPLAY_REASON_CODE,
    }


def _build_redis_fields(row: EventOutboxRow) -> dict[str, str]:
    return {
        "job_id": str(row.event_id),
        "stage_name": EXPECTED_STAGE_NAME,
        "root_object_type": row.aggregate_type,
        "root_object_id": str(row.aggregate_id),
        "idempotency_key": row.dedupe_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(row.event_id),
    }


def _redis_fields_are_thin(fields: Mapping[str, str]) -> bool:
    return set(fields) == ALLOWED_REDIS_THIN_FIELDS


async def _prepare_and_publish(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    redis_client: RedisClientLike,
    repository: "ReplayCandidateRepository",
    candidate: SelectedCandidate,
    raw_values: set[str],
    xadd_maxlen: int | None,
) -> ScriptResult:
    try:
        judge_run_id = await repository.insert_judge_run(candidate)
        if judge_run_id is None:
            existing = await repository.load_single_existing_judge_run(candidate)
            await session.rollback()
            if existing is not None:
                report["existing_replay_judge_run_bucket"] = "one"
                _set_status(report, STATUS_EXISTING_REPLAY_RUN, "judge_run.replay_exists")
                return ScriptResult(exit_code=1, report=report)
            _set_status(report, STATUS_WRITE_FAILED, "judge_run.insert")
            return ScriptResult(exit_code=1, report=report)
        report["replay_judge_run_created_bucket"] = "one"

        output_count = await repository.count_judge_outputs_for_run(judge_run_id)
        report["existing_replay_output_bucket"] = _bucket_count(output_count)
        if output_count:
            await session.rollback()
            _set_status(report, STATUS_EXISTING_REPLAY_OUTPUT, "judge_output.exists")
            return ScriptResult(exit_code=1, report=report)
        ready_count = await repository.count_judge_output_ready_outbox_for_run(judge_run_id)
        report["existing_replay_ready_outbox_bucket"] = _bucket_count(ready_count)
        if ready_count:
            await session.rollback()
            _set_status(
                report,
                STATUS_EXISTING_REPLAY_READY_OUTBOX,
                "judge_output_ready_outbox.exists",
            )
            return ScriptResult(exit_code=1, report=report)

        outbox_rows = await repository.load_pending_judge_call_outbox_for_run(judge_run_id)
        raw_values.update(_raw_values_from_event_rows(outbox_rows))
        if outbox_rows:
            await session.rollback()
            _set_status(report, STATUS_WRITE_FAILED, "judge_call_requested.exists")
            return ScriptResult(exit_code=1, report=report)
        outbox_row = await repository.insert_judge_call_requested_outbox(
            judge_run_id=judge_run_id,
            candidate=candidate,
        )
        if outbox_row is None or outbox_row.status != "pending":
            await session.rollback()
            _set_status(report, STATUS_WRITE_FAILED, "judge_call_requested.insert")
            return ScriptResult(exit_code=1, report=report)
        report["replay_judge_call_requested_outbox_created_bucket"] = "one"

        raw_values.update(_raw_values_from_event_rows([outbox_row]))
        fields = _build_redis_fields(outbox_row)
        if (
            outbox_row.event_type != JUDGE_CALL_REQUESTED_EVENT_TYPE
            or outbox_row.aggregate_type != EXPECTED_AGGREGATE_TYPE
            or outbox_row.aggregate_id != judge_run_id
            or outbox_row.status != "pending"
            or not _redis_fields_are_thin(fields)
        ):
            await session.rollback()
            _set_status(report, STATUS_WRITE_FAILED, "judge_call_requested.contract")
            return ScriptResult(exit_code=1, report=report)

        await session.commit()
    except Exception:
        await session.rollback()
        _set_status(report, STATUS_WRITE_FAILED, "database.write")
        return ScriptResult(exit_code=1, report=report)

    try:
        if xadd_maxlen is None:
            redis_message_id = await _maybe_await(
                redis_client.xadd(EXPECTED_QUEUE_NAME, fields)
            )
        else:
            redis_message_id = await _maybe_await(
                redis_client.xadd(
                    EXPECTED_QUEUE_NAME,
                    fields,
                    maxlen=xadd_maxlen,
                    approximate=True,
                )
            )
        if redis_message_id:
            raw_values.add(str(redis_message_id))
        report["q_analysis_judge_published_bucket"] = "one"
    except Exception:
        _set_status(report, STATUS_REDIS_PUBLISH_FAILED, "redis.publish")
        return ScriptResult(exit_code=1, report=report)

    try:
        await repository.mark_event_outbox_published(outbox_row.event_id)
        await session.commit()
        report["event_outbox_marked_published_bucket"] = "one"
    except Exception:
        await session.rollback()
        _set_status(report, STATUS_WRITE_FAILED, "event_outbox.mark_published")
        return ScriptResult(exit_code=1, report=report)

    if _report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)

    _set_status(report, STATUS_APPROVED_PREPARED)
    return ScriptResult(exit_code=0, report=report)


class ReplayCandidateRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session

    async def fetch_pending_analysis_requested_events(self, *, limit: int) -> list[EventOutboxRow]:
        result = await _execute(
            self._session,
            SELECT_PENDING_ANALYSIS_REQUESTED_EVENTS_QUERY,
            {"limit": limit},
        )
        return [_event_row_from_mapping(row) for row in result.mappings().all()]

    async def fetch_current_ready_bundles(self, *, limit: int) -> list[BundleCandidate]:
        result = await _execute(
            self._session,
            SELECT_CURRENT_READY_BUNDLES_QUERY,
            {"limit": limit},
        )
        return [_bundle_from_mapping(row) for row in result.mappings().all()]

    async def fetch_analysis_requested_events_for_bundle(
        self,
        bundle_id: UUID,
        *,
        limit: int,
    ) -> list[EventOutboxRow]:
        result = await _execute(
            self._session,
            SELECT_ANALYSIS_REQUESTED_EVENTS_FOR_BUNDLE_QUERY,
            {"bundle_id": str(bundle_id), "limit": limit},
        )
        return [_event_row_from_mapping(row) for row in result.mappings().all()]

    async def load_bundle_by_id(self, bundle_id: UUID) -> BundleCandidate | None:
        result = await _execute(
            self._session,
            SELECT_BUNDLE_BY_ID_QUERY,
            {"bundle_id": str(bundle_id)},
        )
        row = _first_mapping(result)
        return _bundle_from_mapping(row) if row is not None else None

    async def load_bundle_shape_stats(self, bundle_id: UUID) -> BundleShapeStats:
        result = await _execute(
            self._session,
            SELECT_BUNDLE_SHAPE_STATS_QUERY,
            {"bundle_id": str(bundle_id)},
        )
        row = _first_mapping(result)
        if row is None:
            return BundleShapeStats(member_count=0, supporting_count=0)
        return BundleShapeStats(
            member_count=int(row["member_count"] or 0),
            supporting_count=int(row["supporting_count"] or 0),
        )

    async def load_judge_runs_for_decision(
        self,
        *,
        bundle_id: UUID,
        decision: JudgeDecision,
    ) -> list[JudgeRunRow]:
        result = await _execute(
            self._session,
            SELECT_JUDGE_RUNS_FOR_DECISION_QUERY,
            {
                "bundle_id": str(bundle_id),
                "prompt_version": decision.prompt_version,
                "model": decision.model,
                "reasoning_effort": decision.reasoning_effort,
            },
        )
        return [_judge_run_from_mapping(row) for row in result.mappings().all()]

    async def load_historical_terminal_judge_runs_for_bundle(
        self,
        bundle_id: UUID,
        *,
        limit: int,
    ) -> list[JudgeRunRow]:
        result = await _execute(
            self._session,
            SELECT_HISTORICAL_TERMINAL_JUDGE_RUNS_FOR_BUNDLE_QUERY,
            {
                "bundle_id": str(bundle_id),
                "replay_prompt_like": f"%{REPLAY_PROMPT_SUFFIX}",
                "limit": limit,
            },
        )
        return [_judge_run_from_mapping(row) for row in result.mappings().all()]

    async def load_single_existing_judge_run(
        self,
        candidate: SelectedCandidate,
    ) -> JudgeRunRow | None:
        rows = await self.load_judge_runs_for_decision(
            bundle_id=candidate.bundle.bundle_id,
            decision=candidate.decision,
        )
        return rows[0] if len(rows) == 1 else None

    async def load_judge_run_by_id(self, judge_run_id: UUID) -> JudgeRunRow | None:
        result = await _execute(
            self._session,
            SELECT_JUDGE_RUN_BY_ID_QUERY,
            {"judge_run_id": str(judge_run_id)},
        )
        row = _first_mapping(result)
        return _judge_run_from_mapping(row) if row is not None else None

    async def count_judge_outputs_for_run(self, judge_run_id: UUID) -> int:
        return _safe_count(
            await _scalar(
                await _execute(
                    self._session,
                    COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY,
                    {"judge_run_id": str(judge_run_id)},
                )
            )
        )

    async def count_judge_output_ready_outbox_for_run(self, judge_run_id: UUID) -> int:
        return _safe_count(
            await _scalar(
                await _execute(
                    self._session,
                    COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY,
                    {"judge_run_id": str(judge_run_id)},
                )
            )
        )

    async def count_analyses_for_run(self, judge_run_id: UUID) -> int:
        return _safe_count(
            await _scalar(
                await _execute(
                    self._session,
                    COUNT_ANALYSES_FOR_RUN_QUERY,
                    {"judge_run_id": str(judge_run_id)},
                )
            )
        )

    async def count_notification_rows_for_run(self, judge_run_id: UUID) -> int:
        return _safe_count(
            await _scalar(
                await _execute(
                    self._session,
                    COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY,
                    {"judge_run_id": str(judge_run_id)},
                )
            )
        )

    async def load_event_by_id(self, event_id: UUID) -> EventOutboxRow | None:
        result = await _execute(
            self._session,
            SELECT_EVENT_OUTBOX_BY_ID_QUERY,
            {"event_id": str(event_id)},
        )
        row = _first_mapping(result)
        return _event_row_from_mapping(row) if row is not None else None

    async def load_pending_judge_call_outbox_for_run(
        self,
        judge_run_id: UUID,
    ) -> list[EventOutboxRow]:
        result = await _execute(
            self._session,
            SELECT_PENDING_JUDGE_CALL_OUTBOX_FOR_RUN_QUERY,
            {"judge_run_id": str(judge_run_id)},
        )
        return [_event_row_from_mapping(row) for row in result.mappings().all()]

    async def load_judge_call_outbox_by_dedupe_key(
        self,
        dedupe_key: str,
    ) -> EventOutboxRow | None:
        result = await _execute(
            self._session,
            SELECT_JUDGE_CALL_OUTBOX_BY_DEDUPE_KEY_QUERY,
            {"dedupe_key": dedupe_key},
        )
        row = _first_mapping(result)
        return _event_row_from_mapping(row) if row is not None else None

    async def insert_judge_run(self, candidate: SelectedCandidate) -> UUID | None:
        decision = candidate.decision
        result = await _execute(
            self._session,
            INSERT_JUDGE_RUN_QUERY,
            {
                "bundle_id": str(candidate.bundle.bundle_id),
                "judge_profile": decision.judge_profile,
                "model": decision.model,
                "reasoning_effort": decision.reasoning_effort,
                "prompt_version": decision.prompt_version,
                "schema_version": decision.schema_version,
                "policy_version": decision.policy_version,
                "prompt_cache_key": decision.prompt_cache_key,
            },
        )
        value = await _scalar(result)
        return _coerce_uuid(value) if value else None

    async def insert_judge_call_requested_outbox(
        self,
        *,
        judge_run_id: UUID,
        candidate: SelectedCandidate,
    ) -> EventOutboxRow | None:
        payload = _build_judge_call_payload(judge_run_id, candidate)
        result = await _execute(
            self._session,
            INSERT_JUDGE_CALL_REQUESTED_OUTBOX_QUERY,
            {
                "judge_run_id": str(judge_run_id),
                "dedupe_key": f"judge-call:{judge_run_id}",
                "payload_json": _jsonb_dumps(payload),
            },
        )
        row = _first_mapping(result)
        return _event_row_from_mapping(row) if row is not None else None

    async def mark_event_outbox_published(self, event_id: UUID) -> None:
        result = await _execute(
            self._session,
            MARK_EVENT_OUTBOX_PUBLISHED_QUERY,
            {
                "event_id": str(event_id),
                "published_at": datetime.now(timezone.utc),
            },
        )
        if getattr(result, "rowcount", None) != 1:
            raise RuntimeError("event_outbox_mark_published_rowcount_mismatch")


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    scan_limit: int = DEFAULT_SCAN_LIMIT,
    approvals: ReplayCandidateApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    approvals = approvals or ReplayCandidateApprovals()
    report = _base_report()
    _apply_side_effect_flags(report, side_effect_flags)
    if _forbidden_side_effect_detected(report):
        _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "side_effect.forbidden")
        return ScriptResult(exit_code=1, report=report)

    if scan_limit <= 0 or scan_limit > MAX_SCAN_LIMIT:
        _set_status(report, STATUS_NOT_READY, "scan_limit.out_of_bounds")
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

        config = _extract_runtime_config(
            report=report,
            values=values,
            raw_values=raw_values,
        )
        if config is None:
            return ScriptResult(exit_code=1, report=report)

        if approvals.any_granted and not approvals.all_granted:
            _approval_block_status(report, approvals)
            return ScriptResult(exit_code=1, report=report)

        try:
            session = await _open_database_session(config.database_url, database_session_factory)
            if not approvals.all_granted:
                await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
                read_only_value = await _scalar(
                    await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY)
                )
                if str(read_only_value).strip().lower() not in {"on", "true", "1", "yes"}:
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
            redis_client = await _open_redis_client(config.redis_url, redis_client_factory)
            await _maybe_await(redis_client.ping())
            report["redis_connected"] = True
        except Exception:
            _set_status(report, STATUS_NOT_READY, "redis.connection")
            return ScriptResult(exit_code=1, report=report)

        repository = ReplayCandidateRepository(session)
        active_q_count = await _active_q_analysis_judge_candidate_count(
            report=report,
            repository=repository,
            redis_client=redis_client,
            raw_values=raw_values,
            scan_limit=scan_limit,
        )
        if active_q_count:
            _set_status(report, STATUS_EXISTING_Q_CANDIDATE, "redis.q_analysis_judge.active")
            return ScriptResult(exit_code=1, report=report)

        candidate, selection_raw_values, status, check = await _select_candidate(
            report=report,
            repository=repository,
            config=config,
            scan_limit=scan_limit,
        )
        raw_values.update(selection_raw_values)
        if status is not None:
            _set_status(report, status, check)
            return ScriptResult(exit_code=1, report=report)
        if candidate is None:
            _set_status(report, STATUS_NO_CANDIDATE, "candidate.none")
            return ScriptResult(exit_code=1, report=report)

        if _report_contains_raw_values(report, raw_values):
            report["raw_values_emitted"] = True
            _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
            return ScriptResult(exit_code=1, report=report)

        if not approvals.all_granted:
            _set_status(report, STATUS_PREFLIGHT_PASSED)
            return ScriptResult(exit_code=0, report=report)

        return await _prepare_and_publish(
            report=report,
            session=session,
            redis_client=redis_client,
            repository=repository,
            candidate=candidate,
            raw_values=raw_values,
            xadd_maxlen=config.xadd_maxlen,
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
    scan_limit: int = DEFAULT_SCAN_LIMIT,
    approvals: ReplayCandidateApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            scan_limit=scan_limit,
            approvals=approvals,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            redis_client_factory=redis_client_factory,
            side_effect_flags=side_effect_flags,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=True, sort_keys=True, indent=2)


def _approvals_from_args(args: argparse.Namespace) -> ReplayCandidateApprovals:
    return ReplayCandidateApprovals(
        db_write=bool(args.approve_db_write),
        redis_publish=bool(args.approve_redis_publish),
        replay_candidate=bool(args.approve_replay_candidate),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        scan_limit=args.scan_limit,
        approvals=_approvals_from_args(args),
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
