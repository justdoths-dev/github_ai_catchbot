from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID, uuid4

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - local fallback for static validation
    sa = None

from .config import AnalysisRouterConfig, AnalysisRouterConfigurationError
from .models import AnalysisRequestedJob, BundleRouteRecord, BundleShapeStats, CandidateRouteState
from .repositories import AnalysisRouterRepository, AsyncSessionLike
from .routing_policy import ALLOWED_JUDGE_PROFILES, AnalysisRoutingPolicy


SCHEMA_VERSION = "bounded_analysis_router_runner_v1"
RUNNER_NAME = "bounded_analysis_router_job_runner"
MODE = "analysis_route_one_shot_consume"
QUEUE_NAME = "q.analysis.route"
STAGE_NAME = "analysis_route"
ROOT_OBJECT_TYPE = "candidate_group"
EVENT_TYPE = "analysis.requested.v1"
OUTBOX_STATUS_PUBLISHED = "published"
DEFAULT_MAX_MESSAGES = 1
HARD_MAX_MESSAGES = 1
DEFAULT_SCAN_LIMIT = 25
HARD_SCAN_LIMIT = 100
EXPECTED_STREAM_FIELDS = frozenset(
    {
        "job_id",
        "stage_name",
        "root_object_type",
        "root_object_id",
        "idempotency_key",
        "pipeline_run_id",
        "not_before",
        "trigger_event_id",
    }
)
FORBIDDEN_STREAM_FIELDS = frozenset(
    {
        "payload_json",
        "bundle_id",
        "judge_profile",
        "scores",
        "score",
        "prompt",
        "prompt_material",
        "raw_text",
        "source_text",
        "message_text",
        "model_output",
        "judge_output",
    }
)
REQUIRED_EVENT_PAYLOAD_FIELDS = (
    "candidate_group_id",
    "bundle_id",
    "judge_profile",
    "escalation_allowed",
)


@dataclass(frozen=True, slots=True)
class BoundedAnalysisRouterConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_consume: bool = False
    allow_database_write: bool = False
    allow_redis_ack: bool = False
    trigger_event_id: UUID | None = None
    trigger_event_suffix: str | None = None
    redis_message_id: str | None = None
    max_messages: int = DEFAULT_MAX_MESSAGES
    scan_limit: int = DEFAULT_SCAN_LIMIT


@dataclass(frozen=True, slots=True)
class BoundedAnalysisRouterRuntimeConfig:
    router_config: AnalysisRouterConfig

    @property
    def database_url(self) -> str:
        return self.router_config.database_url

    @property
    def redis_url(self) -> str:
        return self.router_config.redis_url


@dataclass(slots=True)
class BoundedAnalysisRouterState:
    runtime_config_loaded: bool = False
    redis_consume_attempted: bool = False
    redis_group_created: bool = False
    redis_ack_attempted: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    database_commit_attempted: bool = False


@dataclass(slots=True)
class BoundedAnalysisRouterCounters:
    judge_runs_written_count: int = 0
    existing_judge_run_reused_count: int = 0
    judge_call_requested_outbox_count: int = 0


@dataclass(frozen=True, slots=True)
class AnalysisRequestOutboxEvent:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    payload_json: dict[str, Any]
    status: str
    dedupe_key: str | None = None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class TargetedAnalysisRouteMessage:
    redis_message_id: str
    fields: dict[str, Any]

    @property
    def trigger_event_id(self) -> UUID | None:
        return _uuid_or_none(self.fields.get("trigger_event_id"))

    @property
    def root_object_id(self) -> UUID | None:
        return _uuid_or_none(self.fields.get("root_object_id"))


@dataclass(frozen=True, slots=True)
class BoundedAnalysisRouterResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedAnalysisRouterConfig
    state: BoundedAnalysisRouterState = field(default_factory=BoundedAnalysisRouterState)
    counters: BoundedAnalysisRouterCounters = field(default_factory=BoundedAnalysisRouterCounters)
    target_trigger_event_id_suffix: str | None = None
    target_candidate_group_suffix: str | None = None
    target_bundle_id_suffix: str | None = None
    redis_message_id_suffix: str | None = None
    queue_name: str = QUEUE_NAME
    stage_name: str = STAGE_NAME
    messages_seen: int = 0
    messages_matched: int = 0
    messages_processed_count: int = 0
    redis_ack_status: str = "not_attempted"
    redis_acked_count: int = 0

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "target_trigger_event_id_suffix": self.target_trigger_event_id_suffix,
            "target_candidate_group_suffix": self.target_candidate_group_suffix,
            "target_bundle_id_suffix": self.target_bundle_id_suffix,
            "redis_message_id_suffix": self.redis_message_id_suffix,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "messages_seen": self.messages_seen,
            "messages_matched": self.messages_matched,
            "messages_processed_count": self.messages_processed_count,
            "database_write_attempted": self.state.database_write_attempted,
            "judge_runs_written_count": self.counters.judge_runs_written_count,
            "existing_judge_run_reused_count": self.counters.existing_judge_run_reused_count,
            "judge_call_requested_outbox_count": self.counters.judge_call_requested_outbox_count,
            "redis_ack_attempted": self.state.redis_ack_attempted,
            "redis_ack_status": self.redis_ack_status,
            "redis_acked_count": self.redis_acked_count,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "redis_consume_allowed": self.config.allow_redis_consume,
                "database_write_allowed": self.config.allow_database_write,
                "redis_ack_allowed": self.config.allow_redis_ack,
                "max_messages": self.config.max_messages,
                "scan_limit": self.config.scan_limit,
            },
            "side_effects": {
                "redis_consume_called": self.state.redis_consume_attempted,
                "redis_group_created": self.state.redis_group_created,
                "redis_ack_called": self.state.redis_ack_attempted,
                "redis_mutation": self.state.redis_group_created or self.state.redis_ack_attempted,
                "db_read": self.state.database_read_attempted,
                "db_write": self.state.database_write_attempted,
                "judge_run_created": self.counters.judge_runs_written_count > 0,
                "judge_call_requested_event_emitted": self.counters.judge_call_requested_outbox_count > 0,
                "evidence_assembler_called": False,
                "normalizer_called": False,
                "enricher_called": False,
                "judge_called": False,
                "judge_openai_called": False,
                "analysis_validator_called": False,
                "policy_called": False,
                "notifier_called": False,
                "telegram_read_called": False,
                "telegram_send_called": False,
                "openai_called": False,
                "github_api_called": False,
                "x_api_called": False,
                "web_fetch_called": False,
                "worker_started": False,
                "run_forever_called": False,
                "systemd_called": False,
                "docker_called": False,
                "alembic_called": False,
                "subprocess_called": False,
            },
            "redactions_applied": {
                "full_trigger_event_id_omitted": True,
                "full_candidate_group_id_omitted": True,
                "full_bundle_id_omitted": True,
                "full_judge_run_id_omitted": True,
                "redis_message_id_omitted": True,
                "idempotency_key_omitted": True,
                "payload_json_omitted": True,
                "bundle_data_omitted": True,
                "judge_profile_value_omitted": True,
                "raw_text_omitted": True,
                "prompt_material_omitted": True,
                "model_output_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class BoundedAnalysisRouterError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class _AnalysisRouterResultReady(Exception):
    pass


class RedisTargetConsumer(Protocol):
    async def find_target(
        self,
        config: BoundedAnalysisRouterConfig,
        state: BoundedAnalysisRouterState,
    ) -> tuple[TargetedAnalysisRouteMessage | None, int, int]: ...

    async def ack(self, message_id: str, state: BoundedAnalysisRouterState) -> int: ...


class RedisTargetConsumerClient(Protocol):
    async def xlen(self, name: str) -> int: ...

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
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


@dataclass(frozen=True, slots=True)
class BoundedAnalysisRouterRedisHandle:
    consumer: RedisTargetConsumer
    close: Callable[[], Awaitable[None]]


class AnalysisRouteRepository(Protocol):
    async def fetch_analysis_request_event(
        self,
        trigger_event_id: str,
    ) -> AnalysisRequestOutboxEvent | None: ...

    async def load_candidate_route_state(self, candidate_group_id: str) -> CandidateRouteState | None: ...
    async def load_bundle(self, bundle_id: str) -> BundleRouteRecord | None: ...
    async def load_bundle_shape_stats(self, bundle_id: str) -> BundleShapeStats: ...

    async def get_or_create_judge_run(
        self,
        *,
        bundle_id: str,
        judge_profile: str,
        model: str,
        reasoning_effort: str,
        prompt_version: str,
        schema_version: str,
        policy_version: str,
        prompt_cache_key: str,
    ) -> tuple[UUID, bool]: ...

    async def insert_judge_call_requested_outbox(
        self,
        *,
        judge_run_id: UUID,
        candidate_group_id: str,
        bundle_id: str,
        judge_profile: str,
        model: str,
        reasoning_effort: str,
        prompt_version: str,
        prompt_cache_key: str,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class BoundedAnalysisRouterDatabaseHandle:
    repository: AnalysisRouteRepository
    close: Callable[[bool], Awaitable[None]]


class BoundedAnalysisRouterRedisBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedAnalysisRouterRuntimeConfig,
        state: BoundedAnalysisRouterState,
        logger: logging.Logger,
    ) -> BoundedAnalysisRouterRedisHandle: ...


class BoundedAnalysisRouterDatabaseBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedAnalysisRouterRuntimeConfig,
        state: BoundedAnalysisRouterState,
        logger: logging.Logger,
    ) -> BoundedAnalysisRouterDatabaseHandle: ...


class BoundedAnalysisRouteRedisConsumer:
    def __init__(
        self,
        client: RedisTargetConsumerClient,
        *,
        queue_name: str,
        consumer_group: str,
        consumer_name: str | None = None,
    ) -> None:
        self._client = client
        self._queue_name = queue_name
        self._consumer_group = consumer_group
        self._consumer_name = consumer_name or f"bounded-analysis-router-{uuid4().hex}"

    async def find_target(
        self,
        config: BoundedAnalysisRouterConfig,
        state: BoundedAnalysisRouterState,
    ) -> tuple[TargetedAnalysisRouteMessage | None, int, int]:
        state.redis_consume_attempted = True
        if await self._client.xlen(self._queue_name) <= 0:
            return None, 0, 0
        try:
            await self._client.xgroup_create(
                self._queue_name,
                self._consumer_group,
                id="0",
                mkstream=False,
            )
            state.redis_group_created = True
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

        selected: TargetedAnalysisRouteMessage | None = None
        messages_seen = 0
        messages_matched = 0
        while messages_seen < config.scan_limit:
            count = max(1, min(config.scan_limit - messages_seen, config.scan_limit))
            raw = await self._client.xreadgroup(
                self._consumer_group,
                self._consumer_name,
                {self._queue_name: ">"},
                count=count,
            )
            entries = _flatten_stream_entries(raw)
            if not entries:
                break
            for message_id, fields in entries:
                messages_seen += 1
                decoded_fields = _decode_fields(fields)
                if _matches_target(message_id, decoded_fields, config):
                    messages_matched += 1
                    if selected is None:
                        selected = TargetedAnalysisRouteMessage(
                            redis_message_id=message_id,
                            fields=decoded_fields,
                        )
                if messages_seen >= config.scan_limit:
                    break
        return selected, messages_seen, messages_matched

    async def ack(self, message_id: str, state: BoundedAnalysisRouterState) -> int:
        state.redis_ack_attempted = True
        result = await self._client.xack(self._queue_name, self._consumer_group, message_id)
        try:
            return int(result)
        except (TypeError, ValueError):
            return 1 if result else 0


class SqlAlchemyBoundedAnalysisRouteRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session
        self._router_repository = AnalysisRouterRepository(session)

    async def fetch_analysis_request_event(
        self,
        trigger_event_id: str,
    ) -> AnalysisRequestOutboxEvent | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       dedupe_key, payload_json, status, created_at
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(trigger_event_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return _event_from_mapping(row)

    async def load_candidate_route_state(self, candidate_group_id: str) -> CandidateRouteState | None:
        return await self._router_repository.load_candidate_route_state(candidate_group_id)

    async def load_bundle(self, bundle_id: str) -> BundleRouteRecord | None:
        return await self._router_repository.load_bundle(bundle_id)

    async def load_bundle_shape_stats(self, bundle_id: str) -> BundleShapeStats:
        return await self._router_repository.load_bundle_shape_stats(bundle_id)

    async def get_or_create_judge_run(self, **kwargs: Any) -> tuple[UUID, bool]:
        return await self._router_repository.get_or_create_judge_run(**kwargs)

    async def insert_judge_call_requested_outbox(self, **kwargs: Any) -> None:
        await self._router_repository.insert_judge_call_requested_outbox(**kwargs)


async def build_default_bounded_analysis_router_redis_consumer(
    runtime_config: BoundedAnalysisRouterRuntimeConfig,
    state: BoundedAnalysisRouterState,
    logger: logging.Logger,
) -> BoundedAnalysisRouterRedisHandle:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    consumer = BoundedAnalysisRouteRedisConsumer(
        redis_client,
        queue_name=runtime_config.router_config.queue_name,
        consumer_group=runtime_config.router_config.consumer_group,
        consumer_name=f"{runtime_config.router_config.consumer_name}-bounded-{uuid4().hex[:8]}",
    )

    async def close() -> None:
        close_client = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close_client is None:
            return
        result = close_client()
        if hasattr(result, "__await__"):
            await result

    return BoundedAnalysisRouterRedisHandle(consumer=consumer, close=close)


async def build_default_bounded_analysis_router_database(
    runtime_config: BoundedAnalysisRouterRuntimeConfig,
    state: BoundedAnalysisRouterState,
    logger: logging.Logger,
) -> BoundedAnalysisRouterDatabaseHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedAnalysisRouteRepository(session)

    async def close(commit: bool) -> None:
        try:
            if commit:
                state.database_commit_attempted = True
                await session.commit()
            else:
                await session.rollback()
        finally:
            try:
                await session.close()
            finally:
                await engine.dispose()

    return BoundedAnalysisRouterDatabaseHandle(repository=repository, close=close)


def load_bounded_analysis_router_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedAnalysisRouterRuntimeConfig:
    source = os.environ if env is None else env
    try:
        router_config = AnalysisRouterConfig(
            app_env=_env_value(source, "APP_ENV", "dev").lower(),
            database_url=_env_value(source, "DATABASE_URL"),
            redis_url=_env_value(source, "REDIS_URL"),
            queue_name=_env_value(source, "ANALYSIS_ROUTER_QUEUE_NAME", QUEUE_NAME),
            consumer_group=_env_value(source, "ANALYSIS_ROUTER_CONSUMER_GROUP", "analysis-router"),
            consumer_name=_env_value(source, "ANALYSIS_ROUTER_CONSUMER_NAME", "analysis-router-1"),
            batch_size=int(_env_value(source, "ANALYSIS_ROUTER_BATCH_SIZE", "20")),
            block_ms=int(_env_value(source, "ANALYSIS_ROUTER_BLOCK_MS", "5000")),
            enable_model_escalation=_env_bool(source, "ENABLE_MODEL_ESCALATION", False),
            default_model=_env_value(source, "JUDGE_DEFAULT_MODEL", "gpt-5.4-mini"),
            escalation_model=_env_value(source, "JUDGE_ESCALATION_MODEL", "gpt-5.4"),
            default_reasoning_effort=_env_value(source, "JUDGE_REASONING_EFFORT_DEFAULT", "low"),
            escalation_reasoning_effort=_env_value(
                source,
                "JUDGE_REASONING_EFFORT_ESCALATION",
                "medium",
            ),
            github_prompt_version=_env_value(source, "JUDGE_PROMPT_VERSION_GITHUB", "judge_github_primary_v1"),
            x_prompt_version=_env_value(source, "JUDGE_PROMPT_VERSION_X", "judge_x_primary_v1"),
            text_idea_prompt_version=_env_value(
                source,
                "JUDGE_PROMPT_VERSION_TEXT_IDEA",
                "judge_text_idea_primary_v1",
            ),
            judge_schema_version=_env_value(source, "JUDGE_SCHEMA_VERSION", "judge_output_v1"),
            policy_version=_env_value(source, "VERDICT_POLICY_VERSION", "verdict_policy_v1"),
            log_level=_env_value(source, "LOG_LEVEL", "INFO").upper(),
        )
        router_config.validate()
    except AnalysisRouterConfigurationError as exc:
        text = str(exc)
        if "DATABASE_URL" in text:
            raise BoundedAnalysisRouterError("database_url_missing") from exc
        if "REDIS_URL" in text:
            raise BoundedAnalysisRouterError("redis_url_missing") from exc
        raise BoundedAnalysisRouterError("runtime_config_error") from exc
    except Exception as exc:
        raise BoundedAnalysisRouterError("runtime_config_error") from exc
    return BoundedAnalysisRouterRuntimeConfig(router_config=router_config)


async def run_bounded_analysis_router(
    config: BoundedAnalysisRouterConfig,
    *,
    runtime_config_loader: Callable[[], BoundedAnalysisRouterRuntimeConfig] = (
        load_bounded_analysis_router_runtime_config
    ),
    redis_builder: BoundedAnalysisRouterRedisBuilder | None = None,
    database_builder: BoundedAnalysisRouterDatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedAnalysisRouterResult:
    state = BoundedAnalysisRouterState()
    target_error = _target_error(config)
    if not config.operator_approved:
        return _result("blocked", "operator_approval_missing", config=config, state=state)
    if target_error is not None:
        return _result("blocked", target_error, config=config, state=state)
    if config.trigger_event_suffix is not None and not _is_valid_event_suffix(config.trigger_event_suffix):
        return _result("blocked", "invalid_trigger_event_suffix", config=config, state=state)
    if config.max_messages != HARD_MAX_MESSAGES:
        return _result("blocked", "max_messages_must_be_one", config=config, state=state)
    if config.scan_limit <= 0 or config.scan_limit > HARD_SCAN_LIMIT:
        return _result("blocked", "scan_limit_out_of_range", config=config, state=state)
    if not config.allow_runtime_config:
        return _result("blocked", "runtime_config_not_allowed", config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedAnalysisRouterError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception:
        return _result("blocked", "runtime_config_error", config=config, state=state)

    if runtime_config.router_config.queue_name != QUEUE_NAME:
        return _result("blocked", "queue_not_allowed", config=config, state=state)
    if not config.allow_redis_consume:
        return _result("blocked", "redis_consume_not_allowed", config=config, state=state)
    if not config.allow_database_write:
        return _result("blocked", "database_write_not_allowed", config=config, state=state)
    if not config.allow_redis_ack:
        return _result("blocked", "redis_ack_not_allowed", config=config, state=state)

    redis_handle: BoundedAnalysisRouterRedisHandle | None = None
    database_handle: BoundedAnalysisRouterDatabaseHandle | None = None
    selected: TargetedAnalysisRouteMessage | None = None
    event: AnalysisRequestOutboxEvent | None = None
    job: AnalysisRequestedJob | None = None
    result: BoundedAnalysisRouterResult | None = None
    counters = BoundedAnalysisRouterCounters()
    messages_seen = 0
    messages_matched = 0

    try:
        redis_handle = await (redis_builder or build_default_bounded_analysis_router_redis_consumer)(
            runtime_config,
            state,
            effective_logger,
        )
        selected, messages_seen, messages_matched = await redis_handle.consumer.find_target(config, state)
        if selected is None:
            result = _result(
                "blocked",
                "target_message_not_found",
                config=config,
                state=state,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
            )
            raise _AnalysisRouterResultReady
        if messages_matched != HARD_MAX_MESSAGES:
            result = _result(
                "blocked",
                "duplicate_target_message",
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
            )
            raise _AnalysisRouterResultReady

        contract_error = _selected_message_contract_error(selected)
        if contract_error is not None:
            result = _result(
                "blocked",
                contract_error,
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
            )
            raise _AnalysisRouterResultReady

        database_handle = await (database_builder or build_default_bounded_analysis_router_database)(
            runtime_config,
            state,
            effective_logger,
        )
        repository = database_handle.repository
        state.database_read_attempted = True
        try:
            trigger_event_id = selected.trigger_event_id
            assert trigger_event_id is not None
            event = await repository.fetch_analysis_request_event(str(trigger_event_id))
            if event is None:
                result = _result(
                    "blocked",
                    "event_outbox_missing",
                    config=config,
                    state=state,
                    selected=selected,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                )
                raise _AnalysisRouterResultReady
            event_error, job = _validate_event_outbox(event, selected)
            if event_error is not None or job is None:
                result = _result(
                    "blocked",
                    event_error or "analysis_requested_event_contract_mismatch",
                    config=config,
                    state=state,
                    selected=selected,
                    event=event,
                    job=job,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                )
                raise _AnalysisRouterResultReady

            candidate_state = await repository.load_candidate_route_state(job.candidate_group_id)
            if candidate_state is None:
                result = _result(
                    "blocked",
                    "candidate_group_missing",
                    config=config,
                    state=state,
                    selected=selected,
                    event=event,
                    job=job,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                )
                raise _AnalysisRouterResultReady

            bundle = await repository.load_bundle(job.bundle_id)
            if bundle is None:
                result = _result(
                    "blocked",
                    "bundle_missing",
                    config=config,
                    state=state,
                    selected=selected,
                    event=event,
                    job=job,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                )
                raise _AnalysisRouterResultReady
            if _id_text(bundle.candidate_group_id) != _id_text(job.candidate_group_id):
                result = _result(
                    "blocked",
                    "bundle_candidate_group_mismatch",
                    config=config,
                    state=state,
                    selected=selected,
                    event=event,
                    job=job,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                )
                raise _AnalysisRouterResultReady
            if _id_text(candidate_state.current_bundle_id) != _id_text(job.bundle_id):
                result = _result(
                    "blocked",
                    "stale_bundle_request",
                    config=config,
                    state=state,
                    selected=selected,
                    event=event,
                    job=job,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                )
                raise _AnalysisRouterResultReady
            if not bundle.ready_for_analysis:
                result = _result(
                    "blocked",
                    "bundle_not_ready",
                    config=config,
                    state=state,
                    selected=selected,
                    event=event,
                    job=job,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                )
                raise _AnalysisRouterResultReady

            shape = await repository.load_bundle_shape_stats(job.bundle_id)
            if shape.member_count <= 0:
                result = _result(
                    "blocked",
                    "bundle_members_missing",
                    config=config,
                    state=state,
                    selected=selected,
                    event=event,
                    job=job,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                )
                raise _AnalysisRouterResultReady

            decision = AnalysisRoutingPolicy(runtime_config.router_config).decide(
                job=job,
                current_bundle_id=_id_text(candidate_state.current_bundle_id),
                bundle=bundle,
                shape=shape,
            )
            if decision.action != "judge":
                result = _result(
                    "blocked",
                    decision.refresh_reason or "analysis_route_noop",
                    config=config,
                    state=state,
                    selected=selected,
                    event=event,
                    job=job,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                )
                raise _AnalysisRouterResultReady

            state.database_write_attempted = True
            judge_run_id, created = await repository.get_or_create_judge_run(
                bundle_id=job.bundle_id,
                judge_profile=decision.judge_profile or "",
                model=decision.model or "",
                reasoning_effort=decision.reasoning_effort or "",
                prompt_version=decision.prompt_version or "",
                schema_version=decision.schema_version or "",
                policy_version=decision.policy_version or "",
                prompt_cache_key=decision.prompt_cache_key or "",
            )
            if created:
                counters.judge_runs_written_count = 1
                await repository.insert_judge_call_requested_outbox(
                    judge_run_id=judge_run_id,
                    candidate_group_id=job.candidate_group_id,
                    bundle_id=job.bundle_id,
                    judge_profile=decision.judge_profile or "",
                    model=decision.model or "",
                    reasoning_effort=decision.reasoning_effort or "",
                    prompt_version=decision.prompt_version or "",
                    prompt_cache_key=decision.prompt_cache_key or "",
                )
                counters.judge_call_requested_outbox_count = 1
            else:
                counters.existing_judge_run_reused_count = 1
        except _AnalysisRouterResultReady:
            raise
        except Exception as exc:
            result = _result(
                "failed",
                "database_write_failed" if state.database_write_attempted else "database_read_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected=selected,
                event=event,
                job=job,
                counters=counters,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
            )
            raise _AnalysisRouterResultReady

        try:
            await database_handle.close(True)
        except Exception as exc:
            database_handle = None
            result = _result(
                "failed",
                "database_write_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected=selected,
                event=event,
                job=job,
                counters=counters,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
            )
            raise _AnalysisRouterResultReady
        database_handle = None

        try:
            acked_count = await redis_handle.consumer.ack(selected.redis_message_id, state)
        except Exception as exc:
            result = _result(
                "failed",
                "redis_ack_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected=selected,
                event=event,
                job=job,
                counters=counters,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                messages_processed_count=1,
                redis_ack_status="failed",
            )
            raise _AnalysisRouterResultReady
        if acked_count != 1:
            result = _result(
                "failed",
                "redis_ack_failed",
                config=config,
                state=state,
                selected=selected,
                event=event,
                job=job,
                counters=counters,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                messages_processed_count=1,
                redis_ack_status="failed",
                redis_acked_count=acked_count,
            )
            raise _AnalysisRouterResultReady

        result = _result(
            "routed" if counters.judge_runs_written_count else "reused",
            None,
            config=config,
            state=state,
            selected=selected,
            event=event,
            job=job,
            counters=counters,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
            messages_processed_count=1,
            redis_ack_status="acked",
            redis_acked_count=acked_count,
        )
    except _AnalysisRouterResultReady:
        pass
    except Exception as exc:
        result = _result(
            "failed",
            "bounded_analysis_router_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            selected=selected,
            event=event,
            job=job,
            counters=counters,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
        )
    finally:
        if database_handle is not None:
            try:
                await database_handle.close(False)
            except Exception as exc:
                result = _close_failure_result(
                    result,
                    exc,
                    config=config,
                    state=state,
                    selected=selected,
                    event=event,
                    job=job,
                    counters=counters,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                )
        if redis_handle is not None:
            try:
                await redis_handle.close()
            except Exception:
                pass

    assert result is not None
    return result


def run_bounded_analysis_router_sync(
    config: BoundedAnalysisRouterConfig,
    *,
    runtime_config_loader: Callable[[], BoundedAnalysisRouterRuntimeConfig] = (
        load_bounded_analysis_router_runtime_config
    ),
    redis_builder: BoundedAnalysisRouterRedisBuilder | None = None,
    database_builder: BoundedAnalysisRouterDatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedAnalysisRouterResult:
    return asyncio.run(
        run_bounded_analysis_router(
            config,
            runtime_config_loader=runtime_config_loader,
            redis_builder=redis_builder,
            database_builder=database_builder,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        error_code,
        config=BoundedAnalysisRouterConfig(),
        state=BoundedAnalysisRouterState(),
    ).to_sanitized_dict()


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedAnalysisRouterConfig,
    state: BoundedAnalysisRouterState,
    error_class: str | None = None,
    selected: TargetedAnalysisRouteMessage | None = None,
    event: AnalysisRequestOutboxEvent | None = None,
    job: AnalysisRequestedJob | None = None,
    counters: BoundedAnalysisRouterCounters | None = None,
    messages_seen: int = 0,
    messages_matched: int = 0,
    messages_processed_count: int = 0,
    redis_ack_status: str = "not_attempted",
    redis_acked_count: int = 0,
) -> BoundedAnalysisRouterResult:
    trigger_event_id: UUID | str | None = config.trigger_event_id or config.trigger_event_suffix
    candidate_group_id: UUID | str | None = None
    bundle_id: UUID | str | None = None
    redis_message_id = config.redis_message_id
    if selected is not None:
        redis_message_id = selected.redis_message_id
        trigger_event_id = selected.trigger_event_id or trigger_event_id
        candidate_group_id = selected.root_object_id
    if event is not None:
        trigger_event_id = event.event_id
        if event.aggregate_type == ROOT_OBJECT_TYPE:
            candidate_group_id = event.aggregate_id
    if job is not None:
        candidate_group_id = job.candidate_group_id
        bundle_id = job.bundle_id
    return BoundedAnalysisRouterResult(
        status=status,
        ok=status in {"routed", "reused"} and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        counters=counters or BoundedAnalysisRouterCounters(),
        target_trigger_event_id_suffix=_optional_id_suffix(trigger_event_id),
        target_candidate_group_suffix=_optional_id_suffix(candidate_group_id),
        target_bundle_id_suffix=_optional_id_suffix(bundle_id),
        redis_message_id_suffix=_optional_id_suffix(redis_message_id),
        messages_seen=messages_seen,
        messages_matched=messages_matched,
        messages_processed_count=messages_processed_count,
        redis_ack_status=redis_ack_status,
        redis_acked_count=redis_acked_count,
    )


def _close_failure_result(
    result: BoundedAnalysisRouterResult | None,
    exc: Exception,
    *,
    config: BoundedAnalysisRouterConfig,
    state: BoundedAnalysisRouterState,
    selected: TargetedAnalysisRouteMessage | None,
    event: AnalysisRequestOutboxEvent | None,
    job: AnalysisRequestedJob | None,
    counters: BoundedAnalysisRouterCounters,
    messages_seen: int,
    messages_matched: int,
) -> BoundedAnalysisRouterResult:
    if result is None:
        return _result(
            "failed",
            "database_write_failed" if state.database_write_attempted else "database_rollback_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            selected=selected,
            event=event,
            job=job,
            counters=counters,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
        )
    return replace(
        result,
        status="failed",
        ok=False,
        error_code="database_write_failed" if state.database_write_attempted else "database_rollback_failed",
        error_class=_safe_exception_class(exc),
    )


def _target_error(config: BoundedAnalysisRouterConfig) -> str | None:
    selected = [
        config.trigger_event_id is not None,
        config.trigger_event_suffix is not None,
        bool(config.redis_message_id),
    ]
    count = sum(1 for item in selected if item)
    if count == 0:
        return "target_missing"
    if count > 1:
        return "target_conflict"
    return None


def _matches_target(message_id: str, fields: Mapping[str, Any], config: BoundedAnalysisRouterConfig) -> bool:
    if config.redis_message_id:
        return message_id == config.redis_message_id
    trigger_event_id = str(fields.get("trigger_event_id", "")).strip().lower()
    if config.trigger_event_id is not None:
        return trigger_event_id == str(config.trigger_event_id)
    if config.trigger_event_suffix is not None:
        return trigger_event_id.endswith(config.trigger_event_suffix.lower())
    return False


def _selected_message_contract_error(selected: TargetedAnalysisRouteMessage) -> str | None:
    field_names = frozenset(selected.fields)
    if FORBIDDEN_STREAM_FIELDS.intersection(field_names):
        return "redis_message_business_payload"
    if field_names != EXPECTED_STREAM_FIELDS:
        return "redis_message_contract_invalid"
    if str(selected.fields.get("stage_name", "")) != STAGE_NAME:
        return "stage_not_allowed"
    if str(selected.fields.get("root_object_type", "")) != ROOT_OBJECT_TYPE:
        return "root_object_type_not_allowed"
    if _uuid_or_none(selected.fields.get("trigger_event_id")) is None:
        return "trigger_event_id_invalid"
    if _uuid_or_none(selected.fields.get("root_object_id")) is None:
        return "root_object_id_invalid"
    if not str(selected.fields.get("job_id", "")).strip():
        return "redis_message_contract_invalid"
    if not str(selected.fields.get("idempotency_key", "")).strip():
        return "redis_message_contract_invalid"
    return None


def _validate_event_outbox(
    event: AnalysisRequestOutboxEvent,
    selected: TargetedAnalysisRouteMessage,
) -> tuple[str | None, AnalysisRequestedJob | None]:
    if event.event_type != EVENT_TYPE:
        return "event_type_not_allowed", None
    if event.status != OUTBOX_STATUS_PUBLISHED:
        return "event_outbox_not_published", None
    if event.aggregate_type != ROOT_OBJECT_TYPE:
        return "aggregate_type_not_allowed", None
    selected_root_object_id = _id_text(selected.root_object_id)
    aggregate_id = _id_text(event.aggregate_id)
    if selected_root_object_id is None or aggregate_id != selected_root_object_id:
        return "aggregate_id_mismatch", None
    payload = event.payload_json
    if not all(key in payload for key in REQUIRED_EVENT_PAYLOAD_FIELDS):
        return "malformed_event_payload", None
    candidate_group_uuid = _uuid_or_none(payload.get("candidate_group_id"))
    bundle_uuid = _uuid_or_none(payload.get("bundle_id"))
    judge_profile = payload.get("judge_profile")
    escalation_allowed = payload.get("escalation_allowed")
    if candidate_group_uuid is None or bundle_uuid is None:
        return "malformed_event_payload", None
    candidate_group_id = str(candidate_group_uuid)
    bundle_id = str(bundle_uuid)
    if candidate_group_id != aggregate_id:
        return "candidate_group_id_mismatch", None
    if not isinstance(judge_profile, str) or not judge_profile.strip():
        return "malformed_event_payload", None
    if judge_profile.strip() not in ALLOWED_JUDGE_PROFILES:
        return "unknown_judge_profile", None
    if not isinstance(escalation_allowed, bool):
        return "malformed_event_payload", None
    return (
        None,
        AnalysisRequestedJob(
            trigger_event_id=str(event.event_id),
            event_type=event.event_type,
            candidate_group_id=candidate_group_id,
            bundle_id=bundle_id,
            judge_profile=judge_profile.strip(),
            escalation_allowed=escalation_allowed,
        ),
    )


def _flatten_stream_entries(raw: Any) -> list[tuple[str, Mapping[str, Any]]]:
    messages: list[tuple[str, Mapping[str, Any]]] = []
    for _stream_name, entries in raw or []:
        for message_id, fields in entries:
            messages.append((str(_decode_value(message_id)), fields))
    return messages


def _decode_fields(fields: Mapping[Any, Any]) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for key, value in fields.items():
        decoded[str(_decode_value(key))] = _decode_value(value)
    return decoded


def _decode_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _event_from_mapping(row: Mapping[str, Any]) -> AnalysisRequestOutboxEvent:
    payload = row["payload_json"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return AnalysisRequestOutboxEvent(
        event_id=UUID(str(row["event_id"])),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=UUID(str(row["aggregate_id"])),
        payload_json=payload or {},
        status=str(row["status"]),
        dedupe_key=str(row["dedupe_key"]) if row.get("dedupe_key") is not None else None,
        created_at=row.get("created_at"),
    )


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _optional_id_suffix(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text[-8:] if text else None


def _id_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _is_valid_event_suffix(value: str) -> bool:
    stripped = value.strip().lower()
    return 4 <= len(stripped) <= 36 and all(char in "0123456789abcdef-" for char in stripped)


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


def _env_value(env: Mapping[str, str], name: str, default: str = "") -> str:
    return str(env.get(name, default) or "").strip()


def _env_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = _env_value(env, name, "true" if default else "false").lower()
    return raw not in {"0", "false", "no", "off"}


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)


__all__ = [
    "BoundedAnalysisRouteRedisConsumer",
    "BoundedAnalysisRouterConfig",
    "BoundedAnalysisRouterDatabaseBuilder",
    "BoundedAnalysisRouterDatabaseHandle",
    "BoundedAnalysisRouterError",
    "BoundedAnalysisRouterRedisBuilder",
    "BoundedAnalysisRouterRedisHandle",
    "BoundedAnalysisRouterResult",
    "BoundedAnalysisRouterRuntimeConfig",
    "DEFAULT_MAX_MESSAGES",
    "DEFAULT_SCAN_LIMIT",
    "EVENT_TYPE",
    "EXPECTED_STREAM_FIELDS",
    "HARD_MAX_MESSAGES",
    "HARD_SCAN_LIMIT",
    "MODE",
    "QUEUE_NAME",
    "ROOT_OBJECT_TYPE",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "STAGE_NAME",
    "SqlAlchemyBoundedAnalysisRouteRepository",
    "argument_error_report",
    "build_default_bounded_analysis_router_database",
    "build_default_bounded_analysis_router_redis_consumer",
    "load_bounded_analysis_router_runtime_config",
    "render_sanitized_json",
    "run_bounded_analysis_router",
    "run_bounded_analysis_router_sync",
]
