from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

import sqlalchemy as sa

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.services.judge_openai.config import (  # noqa: E402
    JudgeOpenAIConfig,
    JudgeOpenAIConfigurationError,
)
from src.services.judge_openai.models import JudgeCallJob  # noqa: E402
from src.services.judge_openai.openai_client import (  # noqa: E402
    OpenAIJudgeClient,
    OpenAIPermanentError,
)
from src.services.judge_openai.prompt_library import (  # noqa: E402
    PromptLibrary,
    UnsupportedJudgeProfileError,
)
from src.services.judge_openai.repositories import JudgeOpenAIRepository  # noqa: E402
from src.services.judge_openai.service import JudgeOpenAIService  # noqa: E402


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_judge_openai_single_live_call_smoke"
REPORT_TYPE = "judge_openai_single_live_call_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
EXPECTED_STREAM_NAME = "q.analysis.judge"
EXPECTED_EVENT_TYPE = "judge.call.requested.v1"
READY_EVENT_TYPE = "judge.output.ready.v1"

STATUS_PREFLIGHT_PASSED = "judge_openai_single_live_call_smoke_preflight_passed"
STATUS_LIVE_CALL_PASSED = "judge_openai_single_live_call_smoke_approved_live_call_passed"
STATUS_NOT_READY = "blocked_judge_openai_single_live_call_smoke_not_ready"
STATUS_APPROVAL_MISSING = "blocked_judge_openai_single_live_call_smoke_missing_approval"
STATUS_NO_CANDIDATE = "blocked_judge_openai_single_live_call_smoke_no_candidate"
STATUS_AMBIGUOUS_CANDIDATE = "blocked_judge_openai_single_live_call_smoke_ambiguous_candidate"
STATUS_INVALID_CANDIDATE = "blocked_judge_openai_single_live_call_smoke_invalid_candidate"
STATUS_NON_PENDING_RUN = "blocked_judge_openai_single_live_call_smoke_non_pending_run"
STATUS_DUPLICATE_OUTPUT = "blocked_judge_openai_single_live_call_smoke_existing_output"
STATUS_DUPLICATE_READY_OUTBOX = (
    "blocked_judge_openai_single_live_call_smoke_existing_ready_outbox"
)
STATUS_MISSING_BUNDLE = (
    "blocked_judge_openai_single_live_call_smoke_missing_or_invalid_bundle"
)
STATUS_OPENAI_SECRET_NOT_READY = (
    "blocked_judge_openai_single_live_call_smoke_openai_secret_not_ready"
)
STATUS_DIRECT_OPENAI_API_KEY_PRESENT = (
    "blocked_judge_openai_single_live_call_smoke_direct_openai_api_key_present"
)
STATUS_FORBIDDEN_SIDE_EFFECT = (
    "blocked_judge_openai_single_live_call_smoke_forbidden_side_effect"
)
STATUS_LIVE_CALL_FAILED = "blocked_judge_openai_single_live_call_smoke_live_call_failed"
STATUS_WRITE_FAILED = "blocked_judge_openai_single_live_call_smoke_write_failed"
STATUS_RAW_VALUE_EMISSION = "blocked_judge_openai_single_live_call_smoke_raw_value_emission"

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
SELECT_EVENT_OUTBOX_BY_ID_QUERY = """
SELECT event_id, event_type, payload_json
FROM event_outbox
WHERE event_id = CAST(:event_id AS uuid)
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
SELECT_JUDGE_RUN_FINISH_STATE_QUERY = """
SELECT status, refusal_detected
FROM judge_runs
WHERE judge_run_id = CAST(:judge_run_id AS uuid)
"""
COUNT_ANALYSES_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM analyses a
JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
"""
COUNT_POLICY_OUTBOX_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'analysis.policy.apply.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
"""
COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM notification_plans np
JOIN analyses a ON a.analysis_id = np.analysis_id
JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
"""

_ANALYSIS_VALIDATOR_STARTED_FIELD = "analysis_" + "validator_started"
_POLICY_ENGINE_STARTED_FIELD = "policy_" + "engine_started"


class AsyncSessionLike(Protocol):
    def in_transaction(self) -> bool: ...
    def begin(self) -> Any: ...
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def close(self) -> None: ...


class RedisClientLike(Protocol):
    async def ping(self) -> Any: ...
    async def exists(self, name: str) -> Any: ...
    async def xrevrange(self, name: str, count: int | None = None) -> Any: ...


class OpenAIClientLike(Protocol):
    async def create_structured_response(self, **kwargs: Any) -> Any: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]
RedisClientFactory = Callable[[str], Any]
OpenAIClientFactory = Callable[[JudgeOpenAIConfig], OpenAIClientLike]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class CandidateSelection:
    trigger_event_id: UUID
    raw_fields: Mapping[str, str]


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


class _SingleLiveOpenAIClient:
    def __init__(self, inner: OpenAIClientLike) -> None:
        self._inner = inner
        self.live_calls = 0
        self.blocked_second_call = False

    async def create_structured_response(self, **kwargs: Any) -> Any:
        if self.live_calls >= 1:
            self.blocked_second_call = True
            raise OpenAIPermanentError("single_live_call_contract")
        self.live_calls += 1
        return await self._inner.create_structured_response(**kwargs)

    @property
    def attempted_count(self) -> int:
        return self.live_calls + int(self.blocked_second_call)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Approval-gated single live OpenAI judge-openai smoke for one pending "
            "judge.call.requested.v1 job on q.analysis.judge."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--approve-live-openai", action="store_true")
    parser.add_argument("--approve-db-write", action="store_true")
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_NOT_READY,
        "runtime_env_read": False,
        "database_configured": False,
        "redis_configured": False,
        "openai_key_file_configured": False,
        "direct_openai_api_key_present": False,
        "openai_key_file_read_bucket": "zero",
        "database_connected": False,
        "redis_connected": False,
        "q_analysis_judge_stream_exists": False,
        "candidate_judge_message_found_bucket": "zero",
        "trigger_event_id_present": False,
        "event_outbox_rehydrated_bucket": "zero",
        "event_type_is_judge_call_requested": False,
        "judge_run_linked": False,
        "judge_run_pending_bucket": "zero",
        "bundle_ready_for_judge_bucket": "zero",
        "existing_judge_output_for_run_bucket": "zero",
        "existing_judge_output_ready_outbox_for_run_bucket": "zero",
        "live_openai_call_attempted": False,
        "live_openai_call_attempted_bucket": "zero",
        "fake_openai_used": False,
        "judge_outputs_written_bucket": "zero",
        "judge_run_updated_bucket": "zero",
        "judge_output_ready_outbox_written_bucket": "zero",
        "analysis_rows_written_bucket": "zero",
        "notification_rows_written_bucket": "zero",
        _ANALYSIS_VALIDATOR_STARTED_FIELD: False,
        _POLICY_ENGINE_STARTED_FIELD: False,
        "notifier_started": False,
        "telegram_send_attempted": False,
        "q_analysis_validate_published": False,
        "q_analysis_policy_published": False,
        "q_notification_send_published": False,
        "redis_ack_attempted": False,
        "redis_ack_skipped_by_contract": True,
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


def _database_url_is_supported(database_url: str) -> bool:
    match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", database_url)
    if not match:
        return False
    scheme = match.group(1).lower()
    return scheme == "postgresql" or scheme.startswith("postgresql+")


def _redis_url_is_supported(redis_url: str) -> bool:
    match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", redis_url)
    return bool(match and match.group(1).lower() in {"redis", "rediss", "unix"})


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


def _rows(result: Any) -> list[Any]:
    if hasattr(result, "mappings"):
        mappings = result.mappings()
        if hasattr(mappings, "all"):
            return list(mappings.all())
    if hasattr(result, "fetchall"):
        return list(result.fetchall())
    if isinstance(result, list):
        return result
    return list(result)


def _first_mapping(result: Any) -> Mapping[str, Any] | None:
    if hasattr(result, "mappings"):
        mappings = result.mappings()
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
    return first if isinstance(first, Mapping) else None


def _scalar(result: Any) -> Any:
    if hasattr(result, "scalar_one_or_none"):
        return result.scalar_one_or_none()
    if hasattr(result, "scalar"):
        return result.scalar()
    if hasattr(result, "scalar_one"):
        return result.scalar_one()
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


def _safe_count(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _coerce_uuid(value: Any) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _decode_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _decode_stream_fields(fields: Mapping[Any, Any]) -> dict[str, str]:
    return {_decode_text(key): _decode_text(value) for key, value in fields.items()}


def _collect_raw_strings(*values: Any) -> set[str]:
    raw: set[str] = set()
    for value in values:
        if value is None:
            continue
        if isinstance(value, Mapping):
            raw.update(_collect_raw_strings(*value.values()))
            continue
        if is_dataclass(value):
            raw.update(_collect_raw_strings(asdict(value)))
            continue
        if isinstance(value, (list, tuple, set)):
            raw.update(_collect_raw_strings(*value))
            continue
        text = str(value)
        if len(text) >= 6:
            raw.add(text)
    return raw


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    public_literals = {
        EXPECTED_STREAM_NAME,
        EXPECTED_EVENT_TYPE,
        READY_EVENT_TYPE,
        SCRIPT_NAME,
        REPORT_TYPE,
        "judge",
        "judge_run",
        "pending",
        "succeeded",
    }
    return any(value not in public_literals and value in rendered for value in raw_values)


def _extract_runtime_config(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> tuple[str, str] | None:
    database_url = str(values.get("DATABASE_URL", "")).strip()
    redis_url = str(values.get("REDIS_URL", "")).strip()
    direct_openai_api_key = str(values.get("OPENAI_API_KEY", "")).strip()
    openai_api_key_file = str(values.get("OPENAI_API_KEY_FILE", "")).strip()

    raw_values.update(
        _collect_raw_strings(
            database_url,
            redis_url,
            direct_openai_api_key,
            openai_api_key_file,
        )
    )
    report["direct_openai_api_key_present"] = bool(direct_openai_api_key)
    report["openai_key_file_configured"] = bool(openai_api_key_file)

    if report["direct_openai_api_key_present"]:
        _set_status(
            report,
            STATUS_DIRECT_OPENAI_API_KEY_PRESENT,
            "runtime_env.direct_openai_api_key",
        )
        return None

    report["database_configured"] = bool(database_url and _database_url_is_supported(database_url))
    report["redis_configured"] = bool(redis_url and _redis_url_is_supported(redis_url))
    if not report["database_configured"]:
        _set_status(report, STATUS_NOT_READY, "database.config")
        return None
    if not report["redis_configured"]:
        _set_status(report, STATUS_NOT_READY, "redis.config")
        return None
    if not report["openai_key_file_configured"]:
        _set_status(report, STATUS_OPENAI_SECRET_NOT_READY, "runtime_env.openai_key_file")
        return None
    return database_url, redis_url


def _build_live_config(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> JudgeOpenAIConfig | None:
    try:
        config = JudgeOpenAIConfig.from_env(values)
    except JudgeOpenAIConfigurationError:
        _set_status(report, STATUS_OPENAI_SECRET_NOT_READY, "openai_secret.read")
        return None
    raw_values.update(_collect_raw_strings(config.openai_api_key))
    report["openai_key_file_read_bucket"] = "one"
    return config


async def _count_query(
    session: AsyncSessionLike,
    query: str,
    params: dict[str, Any] | None = None,
) -> int:
    return _safe_count(_scalar(await _execute(session, query, params or {})))


async def _select_candidate_from_redis(
    *,
    report: dict[str, Any],
    redis_client: RedisClientLike,
    raw_values: set[str],
) -> tuple[CandidateSelection | None, str | None]:
    exists_count = _safe_count(await _maybe_await(redis_client.exists(EXPECTED_STREAM_NAME)))
    report["q_analysis_judge_stream_exists"] = exists_count > 0
    if exists_count <= 0:
        return None, "redis.stream_missing"

    entries = await _maybe_await(redis_client.xrevrange(EXPECTED_STREAM_NAME, count=2))
    entry_count = len(entries or [])
    report["candidate_judge_message_found_bucket"] = _bucket_count(entry_count)
    if entry_count <= 0:
        return None, "redis.candidate_missing"
    if entry_count > 1:
        raw_values.update(_collect_raw_strings(entries))
        return None, "redis.candidate_ambiguous"

    entry_id, raw_fields = entries[0]
    fields = _decode_stream_fields(raw_fields)
    raw_values.update(_collect_raw_strings(entry_id, fields))
    trigger_event_id = _coerce_uuid(fields.get("trigger_event_id"))
    report["trigger_event_id_present"] = trigger_event_id is not None
    if trigger_event_id is None:
        return None, "redis.trigger_event_id"
    return CandidateSelection(trigger_event_id=trigger_event_id, raw_fields=fields), None


async def _load_event_row(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    trigger_event_id: UUID,
    raw_values: set[str],
) -> Mapping[str, Any] | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_EVENT_OUTBOX_BY_ID_QUERY,
            {"event_id": str(trigger_event_id)},
        )
    )
    report["event_outbox_rehydrated_bucket"] = "one" if row is not None else "zero"
    if row is None:
        return None
    raw_values.update(_collect_raw_strings(row.get("event_id"), row.get("payload_json")))
    report["event_type_is_judge_call_requested"] = str(row.get("event_type")) == EXPECTED_EVENT_TYPE
    return row


async def _inspect_job_preconditions(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    repository: JudgeOpenAIRepository,
    trigger_event_id: UUID,
    raw_values: set[str],
) -> tuple[JudgeCallJob | None, str | None]:
    job = await repository.load_job_by_trigger_event_id(trigger_event_id)
    if job is None:
        return None, "event_outbox.job"

    judge_run = await repository.load_judge_run(job.judge_run_id)
    raw_values.update(_collect_raw_strings(job.judge_run_id, job.bundle_id, judge_run))
    report["judge_run_linked"] = bool(judge_run and judge_run.bundle_id == job.bundle_id)
    if judge_run is None or judge_run.bundle_id != job.bundle_id:
        return None, "judge_run.link"

    report["judge_run_pending_bucket"] = "one" if judge_run.status == "pending" else "zero"
    if judge_run.status != "pending":
        return None, "judge_run.pending"
    if _job_conflicts_with_run(job, judge_run):
        return None, "judge_run.locked_config"
    try:
        PromptLibrary().render(
            judge_profile=judge_run.judge_profile,
            prompt_version=judge_run.prompt_version,
        )
    except UnsupportedJudgeProfileError:
        return None, "judge_run.prompt"

    params = {"judge_run_id": str(job.judge_run_id)}
    output_count = await _count_query(session, COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY, params)
    report["existing_judge_output_for_run_bucket"] = _bucket_count(output_count)
    if output_count:
        return None, "judge_outputs.existing"

    ready_count = await _count_query(session, COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY, params)
    report["existing_judge_output_ready_outbox_for_run_bucket"] = _bucket_count(ready_count)
    if ready_count:
        return None, "event_outbox.ready_existing"

    bundle = await repository.load_bundle_context(job.bundle_id)
    raw_values.update(_collect_raw_strings(bundle))
    bundle_ready = bool(bundle and bundle.is_structurally_usable())
    report["bundle_ready_for_judge_bucket"] = "one" if bundle_ready else "zero"
    if not bundle_ready:
        return None, "bundle.ready_for_judge"

    analysis_count = await _count_query(session, COUNT_ANALYSES_FOR_RUN_QUERY, params)
    policy_count = await _count_query(session, COUNT_POLICY_OUTBOX_FOR_RUN_QUERY, params)
    notification_count = await _count_query(session, COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY, params)
    report["analysis_rows_written_bucket"] = _bucket_count(analysis_count)
    report["notification_rows_written_bucket"] = _bucket_count(notification_count)
    report["q_analysis_policy_published"] = policy_count > 0
    if analysis_count or policy_count or notification_count:
        return None, "downstream.side_effect"

    return job, None


def _job_conflicts_with_run(job: Any, judge_run: Any) -> bool:
    return bool(
        job.model != judge_run.model
        or job.reasoning_effort != judge_run.reasoning_effort
        or job.prompt_version != judge_run.prompt_version
        or job.prompt_cache_key != judge_run.prompt_cache_key
    )


def _status_for_precondition_check(check: str) -> str:
    if check == "redis.candidate_missing":
        return STATUS_NO_CANDIDATE
    if check == "redis.candidate_ambiguous":
        return STATUS_AMBIGUOUS_CANDIDATE
    if check == "judge_run.pending":
        return STATUS_NON_PENDING_RUN
    if check == "judge_outputs.existing":
        return STATUS_DUPLICATE_OUTPUT
    if check == "event_outbox.ready_existing":
        return STATUS_DUPLICATE_READY_OUTBOX
    if check == "bundle.ready_for_judge":
        return STATUS_MISSING_BUNDLE
    if check == "downstream.side_effect":
        return STATUS_FORBIDDEN_SIDE_EFFECT
    return STATUS_INVALID_CANDIDATE


def _build_real_openai_client(config: JudgeOpenAIConfig) -> OpenAIClientLike:
    return OpenAIJudgeClient(
        api_key=config.openai_api_key,
        project=config.openai_project,
        timeout_sec=config.request_timeout_sec,
    )


async def _run_approved_live_call(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    repository: JudgeOpenAIRepository,
    config: JudgeOpenAIConfig,
    job: JudgeCallJob,
    openai_client_factory: OpenAIClientFactory | None,
) -> None:
    inner_client = (
        openai_client_factory(config)
        if openai_client_factory is not None
        else _build_real_openai_client(config)
    )
    guarded_client = _SingleLiveOpenAIClient(inner_client)
    service = JudgeOpenAIService(
        config,
        repository=repository,
        openai_client=guarded_client,
    )
    await service.handle_job(job)

    report["live_openai_call_attempted"] = guarded_client.live_calls > 0
    report["live_openai_call_attempted_bucket"] = _bucket_count(guarded_client.attempted_count)
    await session.commit()

    params = {"judge_run_id": str(job.judge_run_id)}
    output_count = await _count_query(session, COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY, params)
    ready_count = await _count_query(session, COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY, params)
    analysis_count = await _count_query(session, COUNT_ANALYSES_FOR_RUN_QUERY, params)
    policy_count = await _count_query(session, COUNT_POLICY_OUTBOX_FOR_RUN_QUERY, params)
    notification_count = await _count_query(session, COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY, params)
    run_state = _first_mapping(
        await _execute(session, SELECT_JUDGE_RUN_FINISH_STATE_QUERY, params)
    )

    report["judge_outputs_written_bucket"] = _bucket_count(output_count)
    report["judge_output_ready_outbox_written_bucket"] = _bucket_count(ready_count)
    report["analysis_rows_written_bucket"] = _bucket_count(analysis_count)
    report["notification_rows_written_bucket"] = _bucket_count(notification_count)
    report["q_analysis_policy_published"] = policy_count > 0
    report["judge_run_updated_bucket"] = "one" if _run_succeeded(run_state) else "zero"


def _run_succeeded(row: Mapping[str, Any] | None) -> bool:
    return bool(row and str(row.get("status")) == "succeeded")


def _approved_execution_succeeded(report: Mapping[str, Any]) -> bool:
    return bool(
        report["live_openai_call_attempted"]
        and report["live_openai_call_attempted_bucket"] == "one"
        and not report["fake_openai_used"]
        and report["judge_outputs_written_bucket"] == "one"
        and report["judge_run_updated_bucket"] == "one"
        and report["judge_output_ready_outbox_written_bucket"] == "one"
        and report["analysis_rows_written_bucket"] == "zero"
        and report["notification_rows_written_bucket"] == "zero"
        and not report["q_analysis_validate_published"]
        and not report["q_analysis_policy_published"]
        and not report["q_notification_send_published"]
        and not report[_ANALYSIS_VALIDATOR_STARTED_FIELD]
        and not report[_POLICY_ENGINE_STARTED_FIELD]
        and not report["notifier_started"]
        and not report["telegram_send_attempted"]
    )


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    approve_live_openai: bool = False,
    approve_db_write: bool = False,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    openai_client_factory: OpenAIClientFactory | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    report = _base_report()
    session: AsyncSessionLike | None = None
    redis_client: RedisClientLike | None = None
    committed = False
    raw_values: set[str] = set(forbidden_raw_values)
    raw_values.update(_collect_raw_strings(runtime_env_path))

    try:
        try:
            values = (
                runtime_env_reader(runtime_env_path)
                if runtime_env_reader is not None
                else parse_runtime_env_file(runtime_env_path)
            )
            report["runtime_env_read"] = True
        except Exception:
            _set_status(report, STATUS_NOT_READY, "runtime_env.read")
            return _finalize(report, raw_values, exit_code=1)

        runtime_config = _extract_runtime_config(
            report=report,
            values=values,
            raw_values=raw_values,
        )
        if runtime_config is None:
            return _finalize(report, raw_values, exit_code=1)
        database_url, redis_url = runtime_config

        try:
            session = await _open_database_session(database_url, database_session_factory)
            if not approve_db_write:
                await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
                read_only = _scalar(await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY))
                if not _transaction_read_only_enabled(read_only):
                    _set_status(report, STATUS_NOT_READY, "database.read_only")
                    return _finalize(report, raw_values, exit_code=1)
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
        except Exception:
            _set_status(report, STATUS_NOT_READY, "database.connection")
            return _finalize(report, raw_values, exit_code=1)

        try:
            redis_client = await _open_redis_client(redis_url, redis_client_factory)
            await _maybe_await(redis_client.ping())
            report["redis_connected"] = True
        except Exception:
            _set_status(report, STATUS_NOT_READY, "redis.connection")
            return _finalize(report, raw_values, exit_code=1)

        selection, selection_check = await _select_candidate_from_redis(
            report=report,
            redis_client=redis_client,
            raw_values=raw_values,
        )
        if selection is None:
            status = _status_for_precondition_check(selection_check or "redis.candidate_missing")
            _set_status(report, status, selection_check)
            return _finalize(report, raw_values, exit_code=1)

        event_row = await _load_event_row(
            report=report,
            session=session,
            trigger_event_id=selection.trigger_event_id,
            raw_values=raw_values,
        )
        if event_row is None or not report["event_type_is_judge_call_requested"]:
            _set_status(report, STATUS_INVALID_CANDIDATE, "event_outbox.event_type")
            return _finalize(report, raw_values, exit_code=1)

        repository = JudgeOpenAIRepository(session)
        job, job_check = await _inspect_job_preconditions(
            report=report,
            session=session,
            repository=repository,
            trigger_event_id=selection.trigger_event_id,
            raw_values=raw_values,
        )
        if job is None:
            _set_status(
                report,
                _status_for_precondition_check(job_check or "event_outbox.job"),
                job_check,
            )
            return _finalize(report, raw_values, exit_code=1)

        if not approve_live_openai and not approve_db_write:
            _set_status(report, STATUS_PREFLIGHT_PASSED)
            return _finalize(report, raw_values, exit_code=0)
        if not approve_live_openai or not approve_db_write:
            _set_status(report, STATUS_APPROVAL_MISSING, "approval.required_pair")
            return _finalize(report, raw_values, exit_code=1)

        config = _build_live_config(report=report, values=values, raw_values=raw_values)
        if config is None:
            return _finalize(report, raw_values, exit_code=1)

        try:
            await _run_approved_live_call(
                report=report,
                session=session,
                repository=repository,
                config=config,
                job=job,
                openai_client_factory=openai_client_factory,
            )
            committed = True
        except Exception:
            _set_status(report, STATUS_LIVE_CALL_FAILED, "openai.live_call")
            return _finalize(report, raw_values, exit_code=1)

        if not _approved_execution_succeeded(report):
            _set_status(report, STATUS_WRITE_FAILED, "db_write.expected_effects")
            return _finalize(report, raw_values, exit_code=1)

        _set_status(report, STATUS_LIVE_CALL_PASSED)
        return _finalize(report, raw_values, exit_code=0)
    except Exception:
        _set_status(report, STATUS_NOT_READY, "unexpected")
        return _finalize(report, raw_values, exit_code=1)
    finally:
        if session is not None:
            if not committed:
                await _maybe_await(session.rollback())
            await _maybe_await(session.close())
        await _close_redis_client(redis_client)


def _finalize(report: dict[str, Any], raw_values: set[str], *, exit_code: int) -> ScriptResult:
    if _report_contains_raw_values(report, {value for value in raw_values if len(value) >= 6}):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)
    return ScriptResult(exit_code=exit_code, report=report)


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    approve_live_openai: bool = False,
    approve_db_write: bool = False,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    openai_client_factory: OpenAIClientFactory | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            approve_live_openai=approve_live_openai,
            approve_db_write=approve_db_write,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            redis_client_factory=redis_client_factory,
            openai_client_factory=openai_client_factory,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=True, sort_keys=True, indent=2)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        approve_live_openai=args.approve_live_openai,
        approve_db_write=args.approve_db_write,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
