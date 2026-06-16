from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Protocol
from uuid import UUID

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - static/local validation fallback
    sa = None

from .context_builder import JudgeContextBuilder
from .models import BundleJudgeContext, JudgeRunRecord
from .preflight import HeuristicSanitizingPreflight, NoopModelContextPreflight
from .prompt_library import UnsupportedJudgeProfileError, UnsupportedPromptVersionError
from .request_shape import (
    JudgeOpenAIRequestEnvelope,
    JudgeOpenAIRequestEnvelopeBuilder,
    JudgeOpenAIRequestEnvelopeError,
    summarize_responses_request_shape,
)
from .repositories import _json_loads


SCHEMA_VERSION = "bounded_judge_openai_request_envelope_v1"
RUNNER_NAME = "bounded_judge_openai_request_envelope_runner"
MODE = "read_only_request_envelope_dry_run"
QUEUE_NAME = "q.analysis.judge"
STAGE_NAME = "judge"
EVENT_TYPE = "judge.call.requested.v1"
ROOT_OBJECT_TYPE = "judge_run"
DEFAULT_SCAN_LIMIT = 25
MAX_SCAN_LIMIT = 500
MIN_PRIMARY_SUMMARY_CHARS = 16
MAX_SUPPORTING_SUMMARY_COUNT = 100
MAX_DISCOVERED_LINK_COUNT = 100
MAX_EVIDENCE_LIMITATION_COUNT = 100

REQUIRED_REDIS_FIELDS = frozenset(
    {
        "idempotency_key",
        "job_id",
        "not_before",
        "pipeline_run_id",
        "root_object_id",
        "root_object_type",
        "stage_name",
        "trigger_event_id",
    }
)
FORBIDDEN_REDIS_BUSINESS_FIELDS = frozenset(
    {
        "payload_json",
        "bundle_id",
        "model",
        "reasoning_effort",
        "prompt_version",
        "prompt_cache_key",
        "prompt_material",
        "bundle_data",
        "raw_text",
        "model_output",
    }
)
REQUIRED_EVENT_PAYLOAD_FIELDS = frozenset(
    {
        "judge_run_id",
        "bundle_id",
        "model",
        "reasoning_effort",
        "prompt_version",
        "prompt_cache_key",
    }
)
UUID_SUFFIX_RE = re.compile(r"^[0-9a-f-]{4,36}$")
REDIS_ID_RE = re.compile(r"^[0-9]+-[0-9]+$")
REDIS_ID_SUFFIX_RE = re.compile(r"^[0-9-]{3,64}$")


@dataclass(frozen=True, slots=True)
class BoundedJudgeOpenAIRequestEnvelopeConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_read: bool = False
    allow_database_read: bool = False
    trigger_event_id: UUID | None = None
    trigger_event_suffix: str | None = None
    redis_message_id: str | None = None
    redis_message_suffix: str | None = None
    scan_limit: int = DEFAULT_SCAN_LIMIT


@dataclass(frozen=True, slots=True)
class BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig:
    database_url: str
    redis_url: str
    queue_name: str = QUEUE_NAME
    request_timeout_sec: float | None = 60.0
    max_output_tokens: int | None = None
    enable_prompt_guard_preflight: bool = False


@dataclass(slots=True)
class BoundedJudgeOpenAIRequestEnvelopeState:
    runtime_config_loaded: bool = False
    redis_reader_created: bool = False
    redis_read_attempted: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False


@dataclass(frozen=True, slots=True)
class RedisStreamMessage:
    message_id: str
    fields: dict[str, str]


@dataclass(frozen=True, slots=True)
class JudgeCallOutboxRecord:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    payload_json: dict[str, Any]
    status: str


@dataclass(frozen=True, slots=True)
class BundleEnvelopeRecord:
    bundle: BundleJudgeContext
    ready_for_analysis: bool


class BoundedJudgeOpenAIRequestEnvelopeError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class _BoundedResultReady(Exception):
    pass


class ReadOnlyRedisMessageReader(Protocol):
    async def read_candidate_messages(
        self,
        *,
        queue_name: str,
        config: BoundedJudgeOpenAIRequestEnvelopeConfig,
    ) -> list[RedisStreamMessage]: ...


class BoundedJudgeOpenAIEnvelopeRepository(Protocol):
    async def load_event_outbox(self, trigger_event_id: UUID) -> JudgeCallOutboxRecord | None: ...
    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunRecord | None: ...
    async def load_bundle(self, bundle_id: UUID) -> BundleEnvelopeRecord | None: ...


@dataclass(frozen=True, slots=True)
class BoundedJudgeOpenAIRedisReaderHandle:
    reader: ReadOnlyRedisMessageReader
    close: Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedJudgeOpenAIEnvelopeRepositoryHandle:
    repository: BoundedJudgeOpenAIEnvelopeRepository
    close: Callable[[], Awaitable[None]]


class BoundedJudgeOpenAIRedisReaderBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig,
        state: BoundedJudgeOpenAIRequestEnvelopeState,
        logger: logging.Logger,
    ) -> BoundedJudgeOpenAIRedisReaderHandle: ...


class BoundedJudgeOpenAIEnvelopeRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig,
        state: BoundedJudgeOpenAIRequestEnvelopeState,
        logger: logging.Logger,
    ) -> BoundedJudgeOpenAIEnvelopeRepositoryHandle: ...


@dataclass(frozen=True, slots=True)
class BoundedJudgeOpenAIRequestEnvelopeResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedJudgeOpenAIRequestEnvelopeConfig
    state: BoundedJudgeOpenAIRequestEnvelopeState = field(
        default_factory=BoundedJudgeOpenAIRequestEnvelopeState
    )
    selector_type: str | None = None
    queue_name: str = QUEUE_NAME
    stage_name: str | None = None
    target_redis_message_id_suffix: str | None = None
    target_trigger_event_id_suffix: str | None = None
    target_judge_run_id_suffix: str | None = None
    target_bundle_id_suffix: str | None = None
    target_candidate_group_suffix: str | None = None
    request_envelope_built: bool = False
    structured_output_schema_present: bool = False
    model: str | None = None
    reasoning_effort: str | None = None
    judge_profile: str | None = None
    prompt_version: str | None = None
    schema_version_value: str | None = None
    policy_version: str | None = None
    prompt_cache_key_present: bool = False
    bundle_ready_for_analysis: bool = False
    primary_summary_present: bool = False
    supporting_summary_count: int = 0
    discovered_link_count: int = 0
    evidence_limitation_count: int = 0
    context_character_count: int = 0
    redis_message_count: int = 0
    event_outbox_found: bool = False
    judge_run_found: bool = False
    bundle_found: bool = False

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "selector_type": self.selector_type,
            "target_redis_message_id_suffix": self.target_redis_message_id_suffix,
            "target_trigger_event_id_suffix": self.target_trigger_event_id_suffix,
            "target_judge_run_id_suffix": self.target_judge_run_id_suffix,
            "target_bundle_id_suffix": self.target_bundle_id_suffix,
            "target_candidate_group_suffix": self.target_candidate_group_suffix,
            "redis_read_attempted": self.state.redis_read_attempted,
            "database_read_attempted": self.state.database_read_attempted,
            "request_envelope_built": self.request_envelope_built,
            "structured_output_schema_present": self.structured_output_schema_present,
            "model": self.model,
            "reasoning_effort": self.reasoning_effort,
            "judge_profile": self.judge_profile,
            "prompt_version": self.prompt_version,
            "schema_version_value": self.schema_version_value,
            "policy_version": self.policy_version,
            "prompt_cache_key_present": self.prompt_cache_key_present,
            "bundle_ready_for_analysis": self.bundle_ready_for_analysis,
            "primary_summary_present": self.primary_summary_present,
            "supporting_summary_count": self.supporting_summary_count,
            "discovered_link_count": self.discovered_link_count,
            "evidence_limitation_count": self.evidence_limitation_count,
            "context_character_count": self.context_character_count,
            "redis_message_count": self.redis_message_count,
            "event_outbox_found": self.event_outbox_found,
            "judge_run_found": self.judge_run_found,
            "bundle_found": self.bundle_found,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "redis_read_allowed": self.config.allow_redis_read,
                "database_read_allowed": self.config.allow_database_read,
                "scan_limit": self.config.scan_limit,
            },
            "side_effects": {
                "redis_mutation": False,
                "redis_consume_called": False,
                "redis_ack_called": False,
                "redis_publish_called": False,
                "db_write": False,
                "judge_output_written": False,
                "judge_output_ready_emitted": False,
                "openai_called": False,
                "judge_openai_live_called": False,
                ("analysis_" + "validator_called"): False,
                "policy_called": False,
                "notifier_called": False,
                "telegram_send_called": False,
                "github_api_called": False,
                "x_api_called": False,
                "web_fetch_called": False,
                "worker_started": False,
                ("run_" + "forever_called"): False,
                ("system" + "d_called"): False,
                ("dock" + "er_called"): False,
                ("alem" + "bic_called"): False,
                ("sub" + "process_called"): False,
            },
            "redactions_applied": {
                "full_redis_message_id_omitted": True,
                "full_trigger_event_id_omitted": True,
                "full_judge_run_id_omitted": True,
                "full_bundle_id_omitted": True,
                "full_candidate_group_id_omitted": True,
                "idempotency_key_omitted": True,
                "event_payload_omitted": True,
                "prompt_cache_key_value_omitted": True,
                "prompt_text_omitted": True,
                "bundle_context_omitted": True,
                "raw_source_text_omitted": True,
                "model_output_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class RedisReadOnlyStreamReader:
    def __init__(self, client: Any) -> None:
        self._client = client

    async def read_candidate_messages(
        self,
        *,
        queue_name: str,
        config: BoundedJudgeOpenAIRequestEnvelopeConfig,
    ) -> list[RedisStreamMessage]:
        if config.redis_message_id is not None:
            raw_messages = await self._client.xrange(
                queue_name,
                min=config.redis_message_id,
                max=config.redis_message_id,
                count=1,
            )
        else:
            raw_messages = await self._client.xrevrange(
                queue_name,
                count=config.scan_limit,
            )
        return [_normalize_redis_message(message) for message in raw_messages]


class SqlAlchemyJudgeOpenAIEnvelopeRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def load_event_outbox(self, trigger_event_id: UUID) -> JudgeCallOutboxRecord | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id, payload_json, status
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(trigger_event_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        payload = _json_loads(row["payload_json"]) or {}
        return JudgeCallOutboxRecord(
            event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            aggregate_type=str(row["aggregate_type"]),
            aggregate_id=UUID(str(row["aggregate_id"])),
            payload_json=payload if isinstance(payload, dict) else {},
            status=str(row["status"]),
        )

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunRecord | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT judge_run_id, bundle_id, judge_profile, model, reasoning_effort,
                       prompt_version, schema_version, policy_version, prompt_cache_key,
                       status, schema_retry_count
                FROM judge_runs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": str(judge_run_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeRunRecord(
            judge_run_id=UUID(str(row["judge_run_id"])),
            bundle_id=UUID(str(row["bundle_id"])),
            judge_profile=str(row["judge_profile"]),
            model=str(row["model"]),
            reasoning_effort=str(row["reasoning_effort"]),
            prompt_version=str(row["prompt_version"]),
            schema_version=str(row["schema_version"]),
            policy_version=str(row["policy_version"]),
            prompt_cache_key=str(row["prompt_cache_key"]) if row["prompt_cache_key"] else None,
            status=str(row["status"]),
            schema_retry_count=int(row["schema_retry_count"]),
        )

    async def load_bundle(self, bundle_id: UUID) -> BundleEnvelopeRecord | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT bundle_id, candidate_group_id, current_primary_artifact_id,
                       primary_summary, supporting_summaries_json,
                       discovered_links_summary_json, evidence_limitations,
                       token_budget_profile, reroot_count, ready_for_analysis, created_at
                FROM candidate_evidence_bundles
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": str(bundle_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        bundle = BundleJudgeContext(
            bundle_id=UUID(str(row["bundle_id"])),
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            current_primary_artifact_id=UUID(str(row["current_primary_artifact_id"])),
            primary_summary=_json_loads(row["primary_summary"]) or {},
            supporting_summaries_json=_json_loads(row["supporting_summaries_json"]) or [],
            discovered_links_summary_json=_json_loads(row["discovered_links_summary_json"]) or [],
            evidence_limitations=_json_loads(row["evidence_limitations"]) or [],
            token_budget_profile=str(row["token_budget_profile"]) if row["token_budget_profile"] else None,
            reroot_count=int(row["reroot_count"]),
            created_at=row["created_at"],
        )
        return BundleEnvelopeRecord(
            bundle=bundle,
            ready_for_analysis=bool(row["ready_for_analysis"]),
        )


def load_bounded_judge_openai_request_envelope_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedJudgeOpenAIRequestEnvelopeError("database_url_missing")
    if not redis_url:
        raise BoundedJudgeOpenAIRequestEnvelopeError("redis_url_missing")
    queue_name = _env_value(source, "JUDGE_OPENAI_QUEUE_NAME", QUEUE_NAME)
    if queue_name != QUEUE_NAME:
        raise BoundedJudgeOpenAIRequestEnvelopeError("queue_not_allowed")
    max_output_tokens_raw = _env_value(source, "JUDGE_MAX_OUTPUT_TOKENS")
    request_timeout_raw = _env_value(source, "JUDGE_OPENAI_REQUEST_TIMEOUT_SEC", "60")
    try:
        max_output_tokens = int(max_output_tokens_raw) if max_output_tokens_raw else None
        request_timeout_sec = float(request_timeout_raw)
    except ValueError as exc:
        raise BoundedJudgeOpenAIRequestEnvelopeError("runtime_config_error") from exc
    if max_output_tokens is not None and max_output_tokens <= 0:
        raise BoundedJudgeOpenAIRequestEnvelopeError("runtime_config_error")
    if request_timeout_sec <= 0:
        raise BoundedJudgeOpenAIRequestEnvelopeError("runtime_config_error")
    return BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        queue_name=queue_name,
        request_timeout_sec=request_timeout_sec,
        max_output_tokens=max_output_tokens,
        enable_prompt_guard_preflight=_parse_bool(
            _env_value(source, "ENABLE_PROMPT_GUARD_PREFLIGHT", "false")
        ),
    )


async def build_default_bounded_judge_openai_redis_reader(
    runtime_config: BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig,
    state: BoundedJudgeOpenAIRequestEnvelopeState,
    logger: logging.Logger,
) -> BoundedJudgeOpenAIRedisReaderHandle:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    state.redis_reader_created = True
    reader = RedisReadOnlyStreamReader(client)

    async def close() -> None:
        close_client = getattr(client, "aclose", None) or getattr(client, "close", None)
        if close_client is None:
            return
        result = close_client()
        if hasattr(result, "__await__"):
            await result

    return BoundedJudgeOpenAIRedisReaderHandle(reader=reader, close=close)


async def build_default_bounded_judge_openai_repository(
    runtime_config: BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig,
    state: BoundedJudgeOpenAIRequestEnvelopeState,
    logger: logging.Logger,
) -> BoundedJudgeOpenAIEnvelopeRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    repository = SqlAlchemyJudgeOpenAIEnvelopeRepository(session)

    async def close() -> None:
        try:
            await session.rollback()
        finally:
            await session.close()
            await engine.dispose()

    return BoundedJudgeOpenAIEnvelopeRepositoryHandle(repository=repository, close=close)


async def run_bounded_judge_openai_request_envelope(
    config: BoundedJudgeOpenAIRequestEnvelopeConfig,
    *,
    runtime_config_loader: Callable[[], BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig] = (
        load_bounded_judge_openai_request_envelope_runtime_config
    ),
    redis_reader_builder: BoundedJudgeOpenAIRedisReaderBuilder | None = None,
    repository_builder: BoundedJudgeOpenAIEnvelopeRepositoryBuilder | None = None,
    envelope_builder_factory: Callable[
        [BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig], JudgeOpenAIRequestEnvelopeBuilder
    ]
    | None = None,
    logger: logging.Logger | None = None,
) -> BoundedJudgeOpenAIRequestEnvelopeResult:
    state = BoundedJudgeOpenAIRequestEnvelopeState()
    selector_type = _selector_type(config)
    if not config.operator_approved:
        return _result("blocked", "operator_approval_missing", config=config, state=state)
    if not _valid_scan_limit(config.scan_limit):
        return _result("blocked", "invalid_scan_limit", config=config, state=state)
    if selector_type is None:
        return _result("blocked", "target_missing", config=config, state=state)
    selector_validation_error = _selector_validation_error(config)
    if selector_validation_error is not None:
        return _result("blocked", selector_validation_error, config=config, state=state)
    if not config.allow_runtime_config:
        return _result("blocked", "runtime_config_not_allowed", config=config, state=state)
    if not config.allow_redis_read:
        return _result("blocked", "redis_read_not_allowed", config=config, state=state)
    if not config.allow_database_read:
        return _result("blocked", "database_read_not_allowed", config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedJudgeOpenAIRequestEnvelopeError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception as exc:
        return _result(
            "blocked",
            "runtime_config_error",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
        )

    redis_handle: BoundedJudgeOpenAIRedisReaderHandle | None = None
    repository_handle: BoundedJudgeOpenAIEnvelopeRepositoryHandle | None = None
    result: BoundedJudgeOpenAIRequestEnvelopeResult | None = None
    try:
        redis_handle = await (redis_reader_builder or build_default_bounded_judge_openai_redis_reader)(
            runtime_config,
            state,
            effective_logger,
        )
        state.redis_read_attempted = True
        candidates = await redis_handle.reader.read_candidate_messages(
            queue_name=runtime_config.queue_name,
            config=config,
        )
        matches = [message for message in candidates if _message_matches_selectors(message, config)]
        if not matches:
            result = _result(
                "blocked",
                "redis_message_not_found",
                config=config,
                state=state,
                redis_message_count=0,
            )
            raise _BoundedResultReady
        if len(matches) > 1:
            result = _result(
                "blocked",
                "redis_message_count_exceeded",
                config=config,
                state=state,
                redis_message_count=len(matches),
            )
            raise _BoundedResultReady

        redis_message = matches[0]
        redis_error = _validate_redis_message(redis_message)
        if redis_error is not None:
            result = _result(
                "blocked",
                redis_error,
                config=config,
                state=state,
                redis_message=redis_message,
                redis_message_count=1,
            )
            raise _BoundedResultReady
        trigger_event_id = UUID(redis_message.fields["trigger_event_id"])
        root_judge_run_id = UUID(redis_message.fields["root_object_id"])

        repository_handle = await (repository_builder or build_default_bounded_judge_openai_repository)(
            runtime_config,
            state,
            effective_logger,
        )
        repository = repository_handle.repository
        state.database_read_attempted = True
        event = await repository.load_event_outbox(trigger_event_id)
        event_error = _validate_event(event, trigger_event_id=trigger_event_id, root_judge_run_id=root_judge_run_id)
        if event_error is not None:
            result = _result(
                "blocked",
                event_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        assert event is not None
        payload_judge_run_id = _payload_uuid(event.payload_json, "judge_run_id")
        payload_bundle_id = _payload_uuid(event.payload_json, "bundle_id")
        assert payload_judge_run_id is not None
        assert payload_bundle_id is not None
        judge_run = await repository.load_judge_run(payload_judge_run_id)
        judge_run_error = _validate_judge_run(
            judge_run,
            event=event,
            payload_bundle_id=payload_bundle_id,
        )
        if judge_run_error is not None:
            result = _result(
                "blocked",
                judge_run_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                payload_bundle_id=payload_bundle_id,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        assert judge_run is not None
        bundle_record = await repository.load_bundle(judge_run.bundle_id)
        bundle_error = _validate_bundle(bundle_record)
        if bundle_error is not None:
            result = _result(
                "blocked",
                bundle_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                bundle_record=bundle_record,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        assert bundle_record is not None
        envelope_builder = (
            envelope_builder_factory(runtime_config)
            if envelope_builder_factory is not None
            else _default_envelope_builder(runtime_config)
        )
        try:
            envelope = envelope_builder.build(
                judge_run=judge_run,
                bundle=bundle_record.bundle,
            )
        except (UnsupportedJudgeProfileError, UnsupportedPromptVersionError):
            result = _result(
                "blocked",
                "unsupported_prompt_or_profile",
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                bundle_record=bundle_record,
                redis_message_count=1,
            )
            raise _BoundedResultReady
        except JudgeOpenAIRequestEnvelopeError:
            result = _result(
                "blocked",
                "request_envelope_invalid",
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                bundle_record=bundle_record,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        shape_summary = summarize_responses_request_shape(envelope.to_responses_request())
        if shape_summary["request_shape_valid_bucket"] != "one":
            result = _result(
                "blocked",
                "request_shape_invalid",
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                bundle_record=bundle_record,
                envelope=envelope,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        result = _result(
            "request_envelope_built",
            None,
            config=config,
            state=state,
            redis_message=redis_message,
            event=event,
            judge_run=judge_run,
            bundle_record=bundle_record,
            envelope=envelope,
            redis_message_count=1,
        )
    except _BoundedResultReady:
        pass
    except Exception as exc:
        result = _result(
            "failed",
            "bounded_request_envelope_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
        )
    finally:
        if repository_handle is not None:
            try:
                await repository_handle.close()
            except Exception as exc:
                result = _close_failed_result(
                    existing=result,
                    error_code="repository_close_failed",
                    error_class=_safe_exception_class(exc),
                    config=config,
                    state=state,
                )
        if redis_handle is not None:
            try:
                await redis_handle.close()
            except Exception as exc:
                result = _close_failed_result(
                    existing=result,
                    error_code="redis_reader_close_failed",
                    error_class=_safe_exception_class(exc),
                    config=config,
                    state=state,
                )

    assert result is not None
    return result


def run_bounded_judge_openai_request_envelope_sync(
    config: BoundedJudgeOpenAIRequestEnvelopeConfig,
    *,
    runtime_config_loader: Callable[[], BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig] = (
        load_bounded_judge_openai_request_envelope_runtime_config
    ),
    redis_reader_builder: BoundedJudgeOpenAIRedisReaderBuilder | None = None,
    repository_builder: BoundedJudgeOpenAIEnvelopeRepositoryBuilder | None = None,
    envelope_builder_factory: Callable[
        [BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig], JudgeOpenAIRequestEnvelopeBuilder
    ]
    | None = None,
    logger: logging.Logger | None = None,
) -> BoundedJudgeOpenAIRequestEnvelopeResult:
    return asyncio.run(
        run_bounded_judge_openai_request_envelope(
            config,
            runtime_config_loader=runtime_config_loader,
            redis_reader_builder=redis_reader_builder,
            repository_builder=repository_builder,
            envelope_builder_factory=envelope_builder_factory,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        error_code,
        config=BoundedJudgeOpenAIRequestEnvelopeConfig(),
        state=BoundedJudgeOpenAIRequestEnvelopeState(),
    ).to_sanitized_dict()


def _default_envelope_builder(
    runtime_config: BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig,
) -> JudgeOpenAIRequestEnvelopeBuilder:
    preflight = (
        HeuristicSanitizingPreflight()
        if runtime_config.enable_prompt_guard_preflight
        else NoopModelContextPreflight()
    )
    return JudgeOpenAIRequestEnvelopeBuilder(
        context_builder=JudgeContextBuilder(preflight=preflight),
        max_output_tokens=runtime_config.max_output_tokens,
        request_timeout_sec=runtime_config.request_timeout_sec,
    )


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedJudgeOpenAIRequestEnvelopeConfig,
    state: BoundedJudgeOpenAIRequestEnvelopeState,
    error_class: str | None = None,
    redis_message: RedisStreamMessage | None = None,
    event: JudgeCallOutboxRecord | None = None,
    judge_run: JudgeRunRecord | None = None,
    payload_bundle_id: UUID | None = None,
    bundle_record: BundleEnvelopeRecord | None = None,
    envelope: JudgeOpenAIRequestEnvelope | None = None,
    redis_message_count: int = 0,
) -> BoundedJudgeOpenAIRequestEnvelopeResult:
    bundle = bundle_record.bundle if bundle_record is not None else None
    event_payload = event.payload_json if event is not None and isinstance(event.payload_json, Mapping) else {}
    event_payload_bundle_id = payload_bundle_id or _payload_uuid(event_payload, "bundle_id")
    return BoundedJudgeOpenAIRequestEnvelopeResult(
        status=status,
        ok=status == "request_envelope_built" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        selector_type=_selector_type(config),
        queue_name=QUEUE_NAME,
        stage_name=redis_message.fields.get("stage_name") if redis_message is not None else None,
        target_redis_message_id_suffix=_redis_message_id_suffix(
            redis_message.message_id if redis_message is not None else config.redis_message_id
        )
        or config.redis_message_suffix,
        target_trigger_event_id_suffix=_optional_id_suffix(
            _safe_uuid(redis_message.fields.get("trigger_event_id")) if redis_message is not None else config.trigger_event_id
        )
        or config.trigger_event_suffix,
        target_judge_run_id_suffix=_optional_id_suffix(
            judge_run.judge_run_id
            if judge_run is not None
            else _safe_uuid(redis_message.fields.get("root_object_id")) if redis_message is not None else None
        ),
        target_bundle_id_suffix=_optional_id_suffix(
            bundle.bundle_id if bundle is not None else event_payload_bundle_id
        ),
        target_candidate_group_suffix=_optional_id_suffix(bundle.candidate_group_id if bundle is not None else None),
        request_envelope_built=envelope is not None,
        structured_output_schema_present=bool(envelope and envelope.structured_output_schema),
        model=judge_run.model if judge_run is not None else _payload_string(event_payload, "model"),
        reasoning_effort=judge_run.reasoning_effort
        if judge_run is not None
        else _payload_string(event_payload, "reasoning_effort"),
        judge_profile=judge_run.judge_profile if judge_run is not None else None,
        prompt_version=judge_run.prompt_version if judge_run is not None else _payload_string(event_payload, "prompt_version"),
        schema_version_value=judge_run.schema_version if judge_run is not None else None,
        policy_version=judge_run.policy_version if judge_run is not None else None,
        prompt_cache_key_present=bool(
            judge_run.prompt_cache_key if judge_run is not None else _payload_string(event_payload, "prompt_cache_key")
        ),
        bundle_ready_for_analysis=bool(bundle_record and bundle_record.ready_for_analysis),
        primary_summary_present=bool(bundle and bundle.primary_summary),
        supporting_summary_count=len(bundle.supporting_summaries_json) if bundle is not None else 0,
        discovered_link_count=len(bundle.discovered_links_summary_json) if bundle is not None else 0,
        evidence_limitation_count=len(bundle.evidence_limitations) if bundle is not None else 0,
        context_character_count=envelope.context_character_count if envelope is not None else 0,
        redis_message_count=redis_message_count,
        event_outbox_found=event is not None,
        judge_run_found=judge_run is not None,
        bundle_found=bundle_record is not None,
    )


def _close_failed_result(
    *,
    existing: BoundedJudgeOpenAIRequestEnvelopeResult | None,
    error_code: str,
    error_class: str,
    config: BoundedJudgeOpenAIRequestEnvelopeConfig,
    state: BoundedJudgeOpenAIRequestEnvelopeState,
) -> BoundedJudgeOpenAIRequestEnvelopeResult:
    if existing is None:
        return _result(
            "failed",
            error_code,
            error_class=error_class,
            config=config,
            state=state,
        )
    return replace(existing, status="failed", ok=False, error_code=error_code, error_class=error_class)


def _validate_redis_message(message: RedisStreamMessage) -> str | None:
    if FORBIDDEN_REDIS_BUSINESS_FIELDS & set(message.fields):
        return "redis_message_forbidden_business_fields"
    if REQUIRED_REDIS_FIELDS - set(message.fields):
        return "redis_message_required_fields_missing"
    if message.fields.get("stage_name") != STAGE_NAME:
        return "redis_message_wrong_stage"
    if message.fields.get("root_object_type") != ROOT_OBJECT_TYPE:
        return "redis_message_wrong_root_object_type"
    if _safe_uuid(message.fields.get("trigger_event_id")) is None:
        return "redis_message_invalid_trigger_event_id"
    if _safe_uuid(message.fields.get("job_id")) is None:
        return "redis_message_invalid_job_id"
    if _safe_uuid(message.fields.get("root_object_id")) is None:
        return "redis_message_invalid_root_object_id"
    if message.fields.get("job_id") != message.fields.get("trigger_event_id"):
        return "redis_message_job_trigger_mismatch"
    return None


def _validate_event(
    event: JudgeCallOutboxRecord | None,
    *,
    trigger_event_id: UUID,
    root_judge_run_id: UUID,
) -> str | None:
    if event is None:
        return "event_outbox_missing"
    if event.event_id != trigger_event_id:
        return "event_outbox_id_mismatch"
    if event.event_type != EVENT_TYPE:
        return "event_outbox_wrong_event_type"
    if event.status != "published":
        return "event_outbox_not_published"
    if event.aggregate_type != ROOT_OBJECT_TYPE:
        return "event_outbox_wrong_aggregate_type"
    if event.aggregate_id != root_judge_run_id:
        return "event_outbox_aggregate_mismatch"
    if not isinstance(event.payload_json, dict):
        return "event_payload_malformed"
    if REQUIRED_EVENT_PAYLOAD_FIELDS - set(event.payload_json):
        return "event_payload_missing_required_field"
    payload_judge_run_id = _payload_uuid(event.payload_json, "judge_run_id")
    payload_bundle_id = _payload_uuid(event.payload_json, "bundle_id")
    if payload_judge_run_id is None or payload_bundle_id is None:
        return "event_payload_malformed"
    if payload_judge_run_id != event.aggregate_id:
        return "event_payload_judge_run_id_mismatch"
    for field_name in ("model", "reasoning_effort", "prompt_version", "prompt_cache_key"):
        if not _payload_string(event.payload_json, field_name):
            return "event_payload_missing_required_field"
    return None


def _validate_judge_run(
    judge_run: JudgeRunRecord | None,
    *,
    event: JudgeCallOutboxRecord,
    payload_bundle_id: UUID,
) -> str | None:
    if judge_run is None:
        return "judge_run_missing"
    if judge_run.status != "pending":
        return "judge_run_not_pending"
    if judge_run.judge_run_id != event.aggregate_id:
        return "judge_run_id_mismatch"
    if judge_run.bundle_id != payload_bundle_id:
        return "judge_run_bundle_mismatch"
    if judge_run.model != _payload_string(event.payload_json, "model"):
        return "judge_run_model_mismatch"
    if judge_run.reasoning_effort != _payload_string(event.payload_json, "reasoning_effort"):
        return "judge_run_reasoning_effort_mismatch"
    if judge_run.prompt_version != _payload_string(event.payload_json, "prompt_version"):
        return "judge_run_prompt_version_mismatch"
    payload_prompt_cache_key = _payload_string(event.payload_json, "prompt_cache_key")
    if payload_prompt_cache_key and judge_run.prompt_cache_key != payload_prompt_cache_key:
        return "judge_run_prompt_cache_key_mismatch"
    if not all(
        [
            judge_run.model,
            judge_run.reasoning_effort,
            judge_run.prompt_version,
            judge_run.schema_version,
            judge_run.policy_version,
            judge_run.prompt_cache_key,
            judge_run.judge_profile,
        ]
    ):
        return "judge_run_required_field_missing"
    return None


def _validate_bundle(bundle_record: BundleEnvelopeRecord | None) -> str | None:
    if bundle_record is None:
        return "bundle_missing"
    bundle = bundle_record.bundle
    if not bundle_record.ready_for_analysis:
        return "bundle_not_ready"
    if bundle.candidate_group_id is None:
        return "bundle_candidate_group_missing"
    if bundle.current_primary_artifact_id is None:
        return "bundle_primary_artifact_missing"
    if not _summary_usable(bundle.primary_summary):
        return "bundle_primary_summary_missing"
    if not isinstance(bundle.supporting_summaries_json, list):
        return "bundle_supporting_summaries_invalid"
    if len(bundle.supporting_summaries_json) > MAX_SUPPORTING_SUMMARY_COUNT:
        return "bundle_supporting_summaries_unbounded"
    if not isinstance(bundle.discovered_links_summary_json, list):
        return "bundle_discovered_links_invalid"
    if len(bundle.discovered_links_summary_json) > MAX_DISCOVERED_LINK_COUNT:
        return "bundle_discovered_links_unbounded"
    if not isinstance(bundle.evidence_limitations, list):
        return "bundle_evidence_limitations_invalid"
    if len(bundle.evidence_limitations) > MAX_EVIDENCE_LIMITATION_COUNT:
        return "bundle_evidence_limitations_unbounded"
    return None


def _summary_usable(value: Any) -> bool:
    if not isinstance(value, dict) or not value:
        return False
    return len(json.dumps(value, ensure_ascii=False, sort_keys=True)) >= MIN_PRIMARY_SUMMARY_CHARS


def _message_matches_selectors(
    message: RedisStreamMessage,
    config: BoundedJudgeOpenAIRequestEnvelopeConfig,
) -> bool:
    if config.redis_message_id is not None and message.message_id != config.redis_message_id:
        return False
    if config.redis_message_suffix is not None and not message.message_id.endswith(config.redis_message_suffix):
        return False
    trigger_event_id = message.fields.get("trigger_event_id", "")
    if config.trigger_event_id is not None and trigger_event_id != str(config.trigger_event_id):
        return False
    if config.trigger_event_suffix is not None and not trigger_event_id.endswith(config.trigger_event_suffix):
        return False
    return True


def _normalize_redis_message(raw_message: Any) -> RedisStreamMessage:
    message_id: Any
    fields: Any
    if isinstance(raw_message, (list, tuple)) and len(raw_message) == 2:
        message_id, fields = raw_message
    else:
        raise BoundedJudgeOpenAIRequestEnvelopeError("redis_message_malformed")
    return RedisStreamMessage(
        message_id=_decode_text(message_id),
        fields={_decode_text(key): _decode_text(value) for key, value in dict(fields).items()},
    )


def _selector_type(config: BoundedJudgeOpenAIRequestEnvelopeConfig) -> str | None:
    selectors = []
    if config.redis_message_id is not None:
        selectors.append("redis_message_id")
    if config.redis_message_suffix is not None:
        selectors.append("redis_message_suffix")
    if config.trigger_event_id is not None:
        selectors.append("trigger_event_id")
    if config.trigger_event_suffix is not None:
        selectors.append("trigger_event_suffix")
    return "+".join(selectors) if selectors else None


def _selector_validation_error(config: BoundedJudgeOpenAIRequestEnvelopeConfig) -> str | None:
    if config.redis_message_id is not None and not REDIS_ID_RE.fullmatch(config.redis_message_id):
        return "invalid_redis_message_id"
    if config.redis_message_suffix is not None and not REDIS_ID_SUFFIX_RE.fullmatch(config.redis_message_suffix):
        return "invalid_redis_message_suffix"
    if config.trigger_event_suffix is not None and not UUID_SUFFIX_RE.fullmatch(config.trigger_event_suffix):
        return "invalid_trigger_event_suffix"
    return None


def _valid_scan_limit(value: int) -> bool:
    return 1 <= value <= MAX_SCAN_LIMIT


def _payload_uuid(payload: Mapping[str, Any], field_name: str) -> UUID | None:
    return _safe_uuid(payload.get(field_name))


def _payload_string(payload: Mapping[str, Any], field_name: str) -> str | None:
    value = payload.get(field_name)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _safe_uuid(value: Any) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _optional_id_suffix(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    return str(value)[-8:]


def _redis_message_id_suffix(value: str | None) -> str | None:
    if not value:
        return None
    normalized = str(value)
    return normalized[-8:] if len(normalized) > 8 else "present"


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


def _decode_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _env_value(env: Mapping[str, str], name: str, default: str = "") -> str:
    return str(env.get(name, default)).strip()


def _parse_bool(raw_value: str) -> bool:
    return raw_value.lower() not in {"", "0", "false", "no"}


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)


__all__ = [
    "BoundedJudgeOpenAIEnvelopeRepositoryBuilder",
    "BoundedJudgeOpenAIRequestEnvelopeConfig",
    "BoundedJudgeOpenAIRequestEnvelopeError",
    "BoundedJudgeOpenAIRequestEnvelopeResult",
    "BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig",
    "BoundedJudgeOpenAIRequestEnvelopeState",
    "BoundedJudgeOpenAIRedisReaderBuilder",
    "BoundedJudgeOpenAIRedisReaderHandle",
    "BoundedJudgeOpenAIEnvelopeRepositoryHandle",
    "BundleEnvelopeRecord",
    "JudgeCallOutboxRecord",
    "RedisStreamMessage",
    "argument_error_report",
    "load_bounded_judge_openai_request_envelope_runtime_config",
    "render_sanitized_json",
    "run_bounded_judge_openai_request_envelope",
    "run_bounded_judge_openai_request_envelope_sync",
]
