from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Protocol
from urllib.parse import urlparse
from uuid import UUID, uuid4

from .config import RouterNormalizerConfig, RouterNormalizerConfigurationError
from .models import ExtractedUrl, NormalizationResult, RedisNormalizeMessage, ResolvedUrl
from .repositories import RouterNormalizerRepository
from .service import RouterNormalizerService


SCHEMA_VERSION = "bounded_source_normalize_runner_v1"
RUNNER_NAME = "bounded_router_normalizer_source_job_runner"
MODE = "source_normalize_one_shot"
QUEUE_NAME = "q.source.normalize"
STAGE_NAME = "normalize"
ROOT_OBJECT_TYPE = "source_message"
DEFAULT_MAX_MESSAGES = 1
HARD_MAX_MESSAGES = 1
DEFAULT_SCAN_LIMIT = 25
HARD_SCAN_LIMIT = 100
FORBIDDEN_STREAM_FIELDS = frozenset(
    {
        "payload_json",
        "message_text",
        "source_text",
        "text_body",
        "caption_text",
        "raw_message_json",
    }
)


@dataclass(frozen=True, slots=True)
class BoundedSourceNormalizeConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_consume: bool = False
    allow_database_write: bool = False
    allow_redis_ack: bool = False
    trigger_event_id: UUID | None = None
    source_message_id: UUID | None = None
    redis_message_id: str | None = None
    max_messages: int = DEFAULT_MAX_MESSAGES
    scan_limit: int = DEFAULT_SCAN_LIMIT


@dataclass(slots=True)
class BoundedSourceNormalizeState:
    runtime_config_loaded: bool = False
    redis_consume_attempted: bool = False
    redis_group_created: bool = False
    redis_cleanup_attempted: bool = False
    redis_cleanup_suppressed: bool = False
    redis_ack_attempted: bool = False
    database_session_opened: bool = False
    database_write_attempted: bool = False


@dataclass(slots=True)
class BoundedSourceNormalizeCounters:
    normalization_runs_written_count: int = 0
    suppression_traces_written_count: int = 0
    artifacts_upserted_count: int = 0
    artifact_observations_written_count: int = 0
    candidate_groups_upserted_count: int = 0
    candidate_members_written_count: int = 0
    enrich_outbox_inserted_count: int = 0


@dataclass(frozen=True, slots=True)
class BoundedSourceNormalizeRuntimeConfig:
    router_config: RouterNormalizerConfig

    @property
    def database_url(self) -> str:
        return self.router_config.database_url

    @property
    def redis_url(self) -> str:
        return self.router_config.redis_url


@dataclass(frozen=True, slots=True)
class TargetedRedisMessage:
    redis_message_id: str
    fields: dict[str, Any]
    message: RedisNormalizeMessage


@dataclass(frozen=True, slots=True)
class BoundedSourceNormalizeResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedSourceNormalizeConfig
    state: BoundedSourceNormalizeState = field(default_factory=BoundedSourceNormalizeState)
    counters: BoundedSourceNormalizeCounters = field(default_factory=BoundedSourceNormalizeCounters)
    target_trigger_event_id_suffix: str | None = None
    target_source_message_id_suffix: str | None = None
    redis_message_id_suffix: str | None = None
    messages_seen: int = 0
    messages_matched: int = 0
    messages_processed_count: int = 0
    redis_acked_count: int = 0
    queue_name: str = QUEUE_NAME
    stage_name: str = STAGE_NAME
    candidate_eligible: bool | None = None
    signal_detected: bool | None = None

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
            "target_source_message_id_suffix": self.target_source_message_id_suffix,
            "redis_message_id_suffix": self.redis_message_id_suffix,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "messages_seen": self.messages_seen,
            "messages_matched": self.messages_matched,
            "messages_processed_count": self.messages_processed_count,
            "redis_consume_attempted": self.state.redis_consume_attempted,
            "redis_ack_attempted": self.state.redis_ack_attempted,
            "redis_acked_count": self.redis_acked_count,
            "database_write_attempted": self.state.database_write_attempted,
            "normalization_runs_written_count": self.counters.normalization_runs_written_count,
            "suppression_traces_written_count": self.counters.suppression_traces_written_count,
            "artifacts_upserted_count": self.counters.artifacts_upserted_count,
            "artifact_observations_written_count": self.counters.artifact_observations_written_count,
            "candidate_groups_upserted_count": self.counters.candidate_groups_upserted_count,
            "candidate_members_written_count": self.counters.candidate_members_written_count,
            "enrich_outbox_inserted_count": self.counters.enrich_outbox_inserted_count,
            "candidate_eligible": self.candidate_eligible,
            "signal_detected": self.signal_detected,
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
                "redis_mutation": self.state.redis_group_created or self.state.redis_ack_attempted,
                "db_write": self.state.database_write_attempted,
                "telegram_read_called": False,
                "telegram_send_called": False,
                "openai_called": False,
                "github_called": False,
                "x_called": False,
                "web_called": False,
                "notification_table_write": False,
                "policy_called": False,
                "worker_started": False,
                "run_forever_called": False,
                "systemd_called": False,
                "docker_called": False,
                "alembic_called": False,
            },
            "redactions_applied": {
                "full_trigger_event_id_omitted": True,
                "full_source_message_id_omitted": True,
                "redis_message_id_omitted": True,
                "raw_message_text_omitted": True,
                "raw_message_json_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class BoundedSourceNormalizeError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class _BoundedSourceNormalizeResultReady(Exception):
    pass


class RedisTargetConsumer(Protocol):
    async def find_target(
        self,
        config: BoundedSourceNormalizeConfig,
        state: BoundedSourceNormalizeState,
    ) -> tuple[TargetedRedisMessage | None, int, int]: ...

    async def ack(self, message_id: str, state: BoundedSourceNormalizeState) -> int: ...


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

    async def xgroup_destroy(self, name: str, groupname: str) -> Any: ...


@dataclass(frozen=True, slots=True)
class BoundedSourceNormalizeRedisHandle:
    consumer: RedisTargetConsumer
    close: Callable[[], Awaitable[None]]


class BoundedSourceNormalizeService(Protocol):
    async def process_stream_message(self, message: RedisNormalizeMessage) -> NormalizationResult: ...


@dataclass(frozen=True, slots=True)
class BoundedSourceNormalizeDatabaseHandle:
    service: BoundedSourceNormalizeService
    counters: BoundedSourceNormalizeCounters
    close: Callable[[bool], Awaitable[None]]


class BoundedSourceNormalizeRedisBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedSourceNormalizeRuntimeConfig,
        state: BoundedSourceNormalizeState,
        logger: logging.Logger,
    ) -> BoundedSourceNormalizeRedisHandle: ...


class BoundedSourceNormalizeDatabaseBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedSourceNormalizeRuntimeConfig,
        state: BoundedSourceNormalizeState,
        logger: logging.Logger,
    ) -> BoundedSourceNormalizeDatabaseHandle: ...


class TemporaryGroupRedisTargetConsumer:
    def __init__(
        self,
        client: RedisTargetConsumerClient,
        *,
        queue_name: str,
        group_name: str | None = None,
        consumer_name: str | None = None,
    ) -> None:
        self._client = client
        self._queue_name = queue_name
        unique = uuid4().hex
        self._group_name = group_name or f"bounded-router-normalizer-{unique}"
        self._consumer_name = consumer_name or f"bounded-source-normalize-{unique}"
        self._group_created = False

    async def find_target(
        self,
        config: BoundedSourceNormalizeConfig,
        state: BoundedSourceNormalizeState,
    ) -> tuple[TargetedRedisMessage | None, int, int]:
        state.redis_consume_attempted = True
        if await self._client.xlen(self._queue_name) <= 0:
            return None, 0, 0
        await self._client.xgroup_create(self._queue_name, self._group_name, id="0", mkstream=False)
        self._group_created = True
        state.redis_group_created = True

        messages_seen = 0
        messages_matched = 0
        selected: TargetedRedisMessage | None = None
        while messages_seen < config.scan_limit and selected is None:
            count = max(1, min(config.scan_limit - messages_seen, config.scan_limit))
            raw = await self._client.xreadgroup(
                self._group_name,
                self._consumer_name,
                {self._queue_name: ">"},
                count=count,
                block=0,
            )
            entries = _flatten_stream_entries(raw)
            if not entries:
                break
            for message_id, fields in entries:
                messages_seen += 1
                decoded_fields = _decode_fields(fields)
                if _matches_target(message_id, decoded_fields, config):
                    messages_matched += 1
                    selected = TargetedRedisMessage(
                        redis_message_id=message_id,
                        fields=decoded_fields,
                        message=RedisNormalizeMessage.from_stream_fields(decoded_fields),
                    )
                    break
                if messages_seen >= config.scan_limit:
                    break
        return selected, messages_seen, messages_matched

    async def ack(self, message_id: str, state: BoundedSourceNormalizeState) -> int:
        state.redis_ack_attempted = True
        result = await self._client.xack(self._queue_name, self._group_name, message_id)
        try:
            return int(result)
        except (TypeError, ValueError):
            return 1 if result else 0

    async def cleanup(self, state: BoundedSourceNormalizeState) -> None:
        if not self._group_created:
            return
        state.redis_cleanup_attempted = True
        try:
            await self._client.xgroup_destroy(self._queue_name, self._group_name)
        except Exception:
            state.redis_cleanup_suppressed = True


class CountingRouterNormalizerRepository:
    def __init__(
        self,
        repository: RouterNormalizerRepository,
        counters: BoundedSourceNormalizeCounters,
    ) -> None:
        self._repository = repository
        self._counters = counters

    async def get_outbox_event(self, event_id: UUID):
        return await self._repository.get_outbox_event(event_id)

    async def get_current_source_message(self, source_message_id: UUID):
        return await self._repository.get_current_source_message(source_message_id)

    async def get_source_message_version(self, *, source_message_id: UUID, version_no: int):
        return await self._repository.get_source_message_version(
            source_message_id=source_message_id,
            version_no=version_no,
        )

    async def upsert_normalization_run(self, **kwargs):
        result = await self._repository.upsert_normalization_run(**kwargs)
        self._counters.normalization_runs_written_count += 1
        return result

    async def insert_suppression_trace(self, **kwargs) -> None:
        await self._repository.insert_suppression_trace(**kwargs)
        self._counters.suppression_traces_written_count += 1

    async def upsert_artifact_registry(self, artifact):
        result = await self._repository.upsert_artifact_registry(artifact)
        self._counters.artifacts_upserted_count += 1
        return result

    async def insert_artifact_observation_if_absent(self, **kwargs) -> None:
        await self._repository.insert_artifact_observation_if_absent(**kwargs)
        self._counters.artifact_observations_written_count += 1

    async def upsert_candidate_group(self, **kwargs):
        result = await self._repository.upsert_candidate_group(**kwargs)
        self._counters.candidate_groups_upserted_count += 1
        return result

    async def upsert_candidate_member(self, **kwargs) -> None:
        await self._repository.upsert_candidate_member(**kwargs)
        self._counters.candidate_members_written_count += 1

    async def insert_enrichment_requested_outbox(self, **kwargs) -> None:
        await self._repository.insert_enrichment_requested_outbox(**kwargs)
        artifact = kwargs.get("artifact")
        if getattr(artifact, "provider_route", None) is not None:
            self._counters.enrich_outbox_inserted_count += 1


@dataclass(slots=True, frozen=True)
class OfflineShortUrlResolver:
    allowlist: tuple[str, ...]

    async def resolve(self, url: ExtractedUrl) -> ResolvedUrl:
        normalized = _strip_fragment(url.observed_url)
        status = "short_url_unresolved" if _host(normalized) in self.allowlist else "not_short_url"
        return ResolvedUrl(
            observed_url=url.observed_url,
            normalized_url=normalized,
            resolved_url=None,
            source_kind=url.source_kind,
            context_path=url.context_path,
            resolution_status=status,
        )


async def build_default_bounded_source_normalize_redis_consumer(
    runtime_config: BoundedSourceNormalizeRuntimeConfig,
    state: BoundedSourceNormalizeState,
    logger: logging.Logger,
) -> BoundedSourceNormalizeRedisHandle:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    consumer = TemporaryGroupRedisTargetConsumer(
        redis_client,
        queue_name=runtime_config.router_config.queue_name,
    )

    async def close() -> None:
        await consumer.cleanup(state)
        close_client = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close_client is None:
            return
        result = close_client()
        if hasattr(result, "__await__"):
            await result

    return BoundedSourceNormalizeRedisHandle(consumer=consumer, close=close)


async def build_default_bounded_source_normalize_database(
    runtime_config: BoundedSourceNormalizeRuntimeConfig,
    state: BoundedSourceNormalizeState,
    logger: logging.Logger,
) -> BoundedSourceNormalizeDatabaseHandle:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    counters = BoundedSourceNormalizeCounters()
    repository = CountingRouterNormalizerRepository(RouterNormalizerRepository(session), counters)
    service = RouterNormalizerService(
        runtime_config.router_config,
        repository=repository,  # type: ignore[arg-type]
        short_url_resolver=OfflineShortUrlResolver(runtime_config.router_config.short_url_allowlist),
        logger=logger,
    )

    async def close(commit: bool) -> None:
        try:
            if commit:
                await session.commit()
            else:
                await session.rollback()
        finally:
            try:
                await session.close()
            finally:
                await engine.dispose()

    return BoundedSourceNormalizeDatabaseHandle(service=service, counters=counters, close=close)


def load_bounded_source_normalize_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedSourceNormalizeRuntimeConfig:
    source = os.environ if env is None else env
    try:
        router_config = RouterNormalizerConfig.from_env(source)
    except RouterNormalizerConfigurationError as exc:
        text = str(exc)
        if "DATABASE_URL" in text:
            raise BoundedSourceNormalizeError("database_url_missing") from exc
        if "REDIS_URL" in text:
            raise BoundedSourceNormalizeError("redis_url_missing") from exc
        raise BoundedSourceNormalizeError("runtime_config_error") from exc
    except Exception as exc:
        raise BoundedSourceNormalizeError("runtime_config_error") from exc
    return BoundedSourceNormalizeRuntimeConfig(router_config=router_config)


async def run_bounded_source_normalize(
    config: BoundedSourceNormalizeConfig,
    *,
    runtime_config_loader: Callable[[], BoundedSourceNormalizeRuntimeConfig] = (
        load_bounded_source_normalize_runtime_config
    ),
    redis_builder: BoundedSourceNormalizeRedisBuilder | None = None,
    database_builder: BoundedSourceNormalizeDatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedSourceNormalizeResult:
    state = BoundedSourceNormalizeState()
    target_error = _target_error(config)
    if not config.operator_approved:
        return _result("blocked", "operator_approval_missing", config=config, state=state)
    if target_error is not None:
        return _result("blocked", target_error, config=config, state=state)
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
    except BoundedSourceNormalizeError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception:
        return _result("blocked", "runtime_config_error", config=config, state=state)

    if not config.allow_redis_consume:
        return _result("blocked", "redis_consume_not_allowed", config=config, state=state)
    if not config.allow_database_write:
        return _result("blocked", "database_write_not_allowed", config=config, state=state)
    if not config.allow_redis_ack:
        return _result("blocked", "redis_ack_not_allowed", config=config, state=state)

    redis_handle: BoundedSourceNormalizeRedisHandle | None = None
    database_handle: BoundedSourceNormalizeDatabaseHandle | None = None
    result: BoundedSourceNormalizeResult | None = None
    selected: TargetedRedisMessage | None = None
    messages_seen = 0
    messages_matched = 0
    service_result: NormalizationResult | None = None
    database_counters: BoundedSourceNormalizeCounters | None = None

    try:
        redis_handle = await (redis_builder or build_default_bounded_source_normalize_redis_consumer)(
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
            raise _BoundedSourceNormalizeResultReady

        contract_error = _selected_message_contract_error(selected, config)
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
            raise _BoundedSourceNormalizeResultReady

        database_handle = await (database_builder or build_default_bounded_source_normalize_database)(
            runtime_config,
            state,
            effective_logger,
        )
        database_counters = database_handle.counters
        try:
            service_result = await database_handle.service.process_stream_message(selected.message)
        except Exception as exc:
            service_error_code = _service_error_code(exc)
            if service_error_code == "database_write_failed" or _counters_total(database_handle.counters) > 0:
                state.database_write_attempted = True
            result = _result(
                "failed",
                service_error_code,
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=database_handle.counters,
            )
            raise _BoundedSourceNormalizeResultReady

        _derive_missing_counters(database_handle.counters, service_result)
        state.database_write_attempted = True
        try:
            await database_handle.close(True)
        except Exception as exc:
            failed_counters = database_handle.counters
            database_handle = None
            result = _result(
                "failed",
                "database_write_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=failed_counters,
                service_result=service_result,
            )
            raise _BoundedSourceNormalizeResultReady
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
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                messages_processed_count=1,
                counters=database_counters,
                service_result=service_result,
            )
            raise _BoundedSourceNormalizeResultReady
        if acked_count != 1:
            result = _result(
                "failed",
                "redis_ack_failed",
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                messages_processed_count=1,
                redis_acked_count=acked_count,
                counters=database_counters,
                service_result=service_result,
            )
            raise _BoundedSourceNormalizeResultReady

        result = _result(
            "normalized",
            None,
            config=config,
            state=state,
            selected=selected,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
            messages_processed_count=1,
            redis_acked_count=acked_count,
            counters=database_counters,
            service_result=service_result,
        )
    except _BoundedSourceNormalizeResultReady:
        pass
    except Exception as exc:
        result = _result(
            "failed",
            "bounded_source_normalize_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            selected=selected,
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
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                )
        if redis_handle is not None:
            try:
                await redis_handle.close()
            except Exception:
                state.redis_cleanup_suppressed = True

    assert result is not None
    return result


def run_bounded_source_normalize_sync(
    config: BoundedSourceNormalizeConfig,
    *,
    runtime_config_loader: Callable[[], BoundedSourceNormalizeRuntimeConfig] = (
        load_bounded_source_normalize_runtime_config
    ),
    redis_builder: BoundedSourceNormalizeRedisBuilder | None = None,
    database_builder: BoundedSourceNormalizeDatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedSourceNormalizeResult:
    return asyncio.run(
        run_bounded_source_normalize(
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
        config=BoundedSourceNormalizeConfig(),
        state=BoundedSourceNormalizeState(),
    ).to_sanitized_dict()


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedSourceNormalizeConfig,
    state: BoundedSourceNormalizeState,
    error_class: str | None = None,
    selected: TargetedRedisMessage | None = None,
    messages_seen: int = 0,
    messages_matched: int = 0,
    messages_processed_count: int = 0,
    redis_acked_count: int = 0,
    counters: BoundedSourceNormalizeCounters | None = None,
    service_result: NormalizationResult | None = None,
) -> BoundedSourceNormalizeResult:
    effective_counters = counters or BoundedSourceNormalizeCounters()
    trigger_id = config.trigger_event_id
    source_message_id = config.source_message_id
    redis_message_id = config.redis_message_id
    if selected is not None:
        redis_message_id = selected.redis_message_id
        trigger_id = _uuid_or_none(selected.message.trigger_event_id) or trigger_id
        source_message_id = _uuid_or_none(selected.message.root_object_id) or source_message_id
    return BoundedSourceNormalizeResult(
        status=status,
        ok=status == "normalized" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        counters=effective_counters,
        target_trigger_event_id_suffix=_optional_id_suffix(trigger_id),
        target_source_message_id_suffix=_optional_id_suffix(source_message_id),
        redis_message_id_suffix=_optional_id_suffix(redis_message_id),
        messages_seen=messages_seen,
        messages_matched=messages_matched,
        messages_processed_count=messages_processed_count,
        redis_acked_count=redis_acked_count,
        candidate_eligible=None if service_result is None else service_result.candidate_eligible,
        signal_detected=None if service_result is None else service_result.signal_detected,
    )


def _close_failure_result(
    result: BoundedSourceNormalizeResult | None,
    exc: Exception,
    *,
    config: BoundedSourceNormalizeConfig,
    state: BoundedSourceNormalizeState,
    selected: TargetedRedisMessage | None,
    messages_seen: int,
    messages_matched: int,
) -> BoundedSourceNormalizeResult:
    if result is None:
        return _result(
            "failed",
            "database_write_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            selected=selected,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
        )
    return replace(
        result,
        status="failed",
        ok=False,
        error_code="database_write_failed",
        error_class=_safe_exception_class(exc),
    )


def _target_error(config: BoundedSourceNormalizeConfig) -> str | None:
    selected = [
        config.trigger_event_id is not None,
        config.source_message_id is not None,
        bool(config.redis_message_id),
    ]
    count = sum(1 for item in selected if item)
    if count == 0:
        return "target_missing"
    if count > 1:
        return "target_conflict"
    return None


def _matches_target(message_id: str, fields: Mapping[str, Any], config: BoundedSourceNormalizeConfig) -> bool:
    if config.redis_message_id:
        return message_id == config.redis_message_id
    if config.trigger_event_id is not None:
        return str(fields.get("trigger_event_id", "")) == str(config.trigger_event_id)
    if config.source_message_id is not None:
        return str(fields.get("root_object_id", "")) == str(config.source_message_id)
    return False


def _selected_message_contract_error(
    selected: TargetedRedisMessage,
    config: BoundedSourceNormalizeConfig,
) -> str | None:
    if FORBIDDEN_STREAM_FIELDS.intersection(selected.fields):
        return "redis_message_contract_invalid"
    required = {
        "job_id",
        "stage_name",
        "root_object_type",
        "root_object_id",
        "idempotency_key",
        "trigger_event_id",
    }
    if any(not str(selected.fields.get(key, "")).strip() for key in required):
        return "redis_message_contract_invalid"
    if selected.message.stage_name != STAGE_NAME:
        return "stage_not_allowed"
    if selected.message.root_object_type != ROOT_OBJECT_TYPE:
        return "root_object_type_not_allowed"
    if _uuid_or_none(selected.message.trigger_event_id) is None:
        return "trigger_event_id_invalid"
    selected_source_message_id = _uuid_or_none(selected.message.root_object_id)
    if selected_source_message_id is None:
        return "redis_message_contract_invalid"
    if config.source_message_id is not None and selected_source_message_id != config.source_message_id:
        return "target_source_message_mismatch"
    return None


def _service_error_code(exc: Exception) -> str:
    if isinstance(exc, ValueError):
        text = str(exc)
        if "source message not found" in text or "source message version not found" in text:
            return "source_message_missing"
        if "trigger event not found" in text:
            return "trigger_event_id_invalid"
    return "database_write_failed"


def _derive_missing_counters(
    counters: BoundedSourceNormalizeCounters,
    service_result: NormalizationResult,
) -> None:
    if counters.normalization_runs_written_count == 0:
        counters.normalization_runs_written_count = 1
    if counters.suppression_traces_written_count == 0:
        counters.suppression_traces_written_count = len(service_result.suppression_reason_codes)
    if counters.artifacts_upserted_count == 0:
        counters.artifacts_upserted_count = service_result.artifact_count
    if counters.artifact_observations_written_count == 0:
        counters.artifact_observations_written_count = service_result.artifact_count
    if counters.candidate_groups_upserted_count == 0:
        counters.candidate_groups_upserted_count = service_result.candidate_group_count


def _counters_total(counters: BoundedSourceNormalizeCounters) -> int:
    return (
        counters.normalization_runs_written_count
        + counters.suppression_traces_written_count
        + counters.artifacts_upserted_count
        + counters.artifact_observations_written_count
        + counters.candidate_groups_upserted_count
        + counters.candidate_members_written_count
        + counters.enrich_outbox_inserted_count
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
        decoded_key = _decode_value(key)
        decoded_value = _decode_value(value)
        decoded[str(decoded_key)] = decoded_value
    return decoded


def _decode_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _uuid_or_none(value: UUID | str | None) -> UUID | None:
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


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


def _strip_fragment(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl()


def _host(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host
