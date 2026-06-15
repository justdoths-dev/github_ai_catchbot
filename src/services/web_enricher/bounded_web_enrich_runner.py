from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import UUID, uuid4

from .article_parser import ArticleParser
from .config import WebEnricherConfig, WebEnricherConfigurationError
from .models import ArtifactEnrichmentJob, EnrichmentResult, FetchedDocument
from .repositories import WebEnricherRepository
from .service import WebEnricherService
from .url_discovery import WebUrlDiscovery
from .web_fetch_client import WebFetchClient


SCHEMA_VERSION = "bounded_web_enrich_runner_v1"
RUNNER_NAME = "bounded_web_enricher_job_runner"
MODE = "web_enrich_one_shot"
QUEUE_NAME = "q.artifact.enrich.web"
STAGE_NAME = "enrich_web"
ROOT_OBJECT_TYPE = "artifact"
DEFAULT_MAX_MESSAGES = 1
HARD_MAX_MESSAGES = 1
DEFAULT_SCAN_LIMIT = 25
HARD_SCAN_LIMIT = 100
HARD_MAX_REDIRECTS = 10
HARD_MAX_RESPONSE_BYTES = 1_048_576
HARD_MAX_TIMEOUT_SEC = 10.0
HARD_MAX_OUTBOUND_LINKS = 100
ALLOWED_CONTENT_TYPES = frozenset(
    {
        "text/html",
        "application/xhtml+xml",
        "text/plain",
        "text/markdown",
    }
)
FORBIDDEN_STREAM_FIELDS = frozenset(
    {
        "payload_json",
        "observed_url",
        "canonical_url",
        "article_text",
        "source_text",
        "raw_message_json",
        "html",
        "body",
    }
)
REQUIRED_STREAM_FIELDS = frozenset(
    {
        "job_id",
        "stage_name",
        "root_object_type",
        "root_object_id",
        "idempotency_key",
        "trigger_event_id",
    }
)


@dataclass(frozen=True, slots=True)
class BoundedWebEnrichConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_consume: bool = False
    allow_database_write: bool = False
    allow_redis_ack: bool = False
    allow_web_fetch: bool = False
    trigger_event_id: UUID | None = None
    artifact_id: UUID | None = None
    redis_message_id: str | None = None
    max_messages: int = DEFAULT_MAX_MESSAGES
    scan_limit: int = DEFAULT_SCAN_LIMIT


@dataclass(slots=True)
class BoundedWebEnrichState:
    runtime_config_loaded: bool = False
    redis_consume_attempted: bool = False
    redis_group_created: bool = False
    redis_cleanup_attempted: bool = False
    redis_cleanup_suppressed: bool = False
    redis_ack_attempted: bool = False
    database_session_opened: bool = False
    database_write_attempted: bool = False
    web_fetch_attempted: bool = False


@dataclass(slots=True)
class BoundedWebEnrichCounters:
    artifact_enrichment_runs_inserted_count: int = 0
    artifact_enrichment_runs_finished_count: int = 0
    artifact_snapshots_written_count: int = 0
    artifact_snapshot_web_article_written_count: int = 0
    discovered_url_observations_written_count: int = 0
    artifact_registry_updates_count: int = 0
    artifact_snapshot_updated_outbox_count: int = 0


@dataclass(frozen=True, slots=True)
class BoundedWebEnrichRuntimeConfig:
    web_config: WebEnricherConfig

    @property
    def database_url(self) -> str:
        return self.web_config.database_url

    @property
    def redis_url(self) -> str:
        return self.web_config.redis_url


@dataclass(frozen=True, slots=True)
class TargetedRedisMessage:
    redis_message_id: str
    fields: dict[str, Any]


@dataclass(frozen=True, slots=True)
class BoundedWebEnrichResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedWebEnrichConfig
    state: BoundedWebEnrichState = field(default_factory=BoundedWebEnrichState)
    counters: BoundedWebEnrichCounters = field(default_factory=BoundedWebEnrichCounters)
    target_trigger_event_id_suffix: str | None = None
    target_artifact_id_suffix: str | None = None
    target_redis_message_id_suffix: str | None = None
    messages_seen: int = 0
    messages_matched: int = 0
    messages_processed_count: int = 0
    redis_acked_count: int = 0
    queue_name: str = QUEUE_NAME
    stage_name: str = STAGE_NAME
    snapshot_status: str | None = None
    snapshot_id_suffix: str | None = None
    content_anchor_prefix: str | None = None

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
            "target_artifact_id_suffix": self.target_artifact_id_suffix,
            "target_redis_message_id_suffix": self.target_redis_message_id_suffix,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "messages_seen": self.messages_seen,
            "messages_matched": self.messages_matched,
            "messages_processed_count": self.messages_processed_count,
            "redis_consume_attempted": self.state.redis_consume_attempted,
            "redis_ack_attempted": self.state.redis_ack_attempted,
            "redis_ack_status": _redis_ack_status(
                attempted=self.state.redis_ack_attempted,
                acked_count=self.redis_acked_count,
                error_code=self.error_code,
            ),
            "redis_acked_count": self.redis_acked_count,
            "database_write_attempted": self.state.database_write_attempted,
            "web_fetch_attempted": self.state.web_fetch_attempted,
            "artifact_enrichment_runs_inserted_count": self.counters.artifact_enrichment_runs_inserted_count,
            "artifact_enrichment_runs_finished_count": self.counters.artifact_enrichment_runs_finished_count,
            "artifact_snapshots_written_count": self.counters.artifact_snapshots_written_count,
            "artifact_snapshot_web_article_written_count": (
                self.counters.artifact_snapshot_web_article_written_count
            ),
            "discovered_url_observations_written_count": (
                self.counters.discovered_url_observations_written_count
            ),
            "artifact_registry_updates_count": self.counters.artifact_registry_updates_count,
            "artifact_snapshot_updated_outbox_count": self.counters.artifact_snapshot_updated_outbox_count,
            "snapshot_status": self.snapshot_status,
            "snapshot_id_suffix": self.snapshot_id_suffix,
            "content_anchor_prefix": self.content_anchor_prefix,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "redis_consume_allowed": self.config.allow_redis_consume,
                "database_write_allowed": self.config.allow_database_write,
                "redis_ack_allowed": self.config.allow_redis_ack,
                "web_fetch_allowed": self.config.allow_web_fetch,
                "max_messages": self.config.max_messages,
                "scan_limit": self.config.scan_limit,
            },
            "side_effects": {
                "redis_consume": self.state.redis_consume_attempted,
                "redis_group_created": self.state.redis_group_created,
                "redis_ack": self.state.redis_ack_attempted,
                "db_write": self.state.database_write_attempted,
                "web_fetch": self.state.web_fetch_attempted,
                "telegram_read_called": False,
                "telegram_send_called": False,
                "openai_called": False,
                "github_called": False,
                "x_called": False,
                "notifier_called": False,
                "policy_called": False,
                "normalizer_called": False,
                "evidence_assembler_called": False,
                "worker_started": False,
                "run_forever_called": False,
                "systemd_called": False,
                "docker_called": False,
                "alembic_called": False,
                "subprocess_called": False,
            },
            "redactions_applied": {
                "full_trigger_event_id_omitted": True,
                "full_artifact_id_omitted": True,
                "redis_message_id_omitted": True,
                "idempotency_key_omitted": True,
                "raw_url_omitted": True,
                "raw_html_omitted": True,
                "raw_article_text_omitted": True,
                "raw_body_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class BoundedWebEnrichError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class _BoundedWebEnrichResultReady(Exception):
    pass


class RedisTargetConsumer(Protocol):
    async def find_target(
        self,
        config: BoundedWebEnrichConfig,
        state: BoundedWebEnrichState,
    ) -> tuple[TargetedRedisMessage | None, int, int]: ...

    async def ack(self, message_id: str, state: BoundedWebEnrichState) -> int: ...


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
class BoundedWebEnrichRedisHandle:
    consumer: RedisTargetConsumer
    close: Callable[[], Awaitable[None]]


class BoundedWebEnrichService(Protocol):
    async def rehydrate_job(self, trigger_event_id: str) -> ArtifactEnrichmentJob | None: ...

    async def handle_job(self, job: ArtifactEnrichmentJob) -> EnrichmentResult: ...


@dataclass(frozen=True, slots=True)
class BoundedWebEnrichDatabaseHandle:
    service: BoundedWebEnrichService
    counters: BoundedWebEnrichCounters
    close: Callable[[], Awaitable[None]]


class BoundedWebEnrichRedisBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedWebEnrichRuntimeConfig,
        state: BoundedWebEnrichState,
        logger: logging.Logger,
    ) -> BoundedWebEnrichRedisHandle: ...


class BoundedWebEnrichDatabaseBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedWebEnrichRuntimeConfig,
        state: BoundedWebEnrichState,
        logger: logging.Logger,
    ) -> BoundedWebEnrichDatabaseHandle: ...


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
        self._group_name = group_name or f"bounded-web-enricher-{unique}"
        self._consumer_name = consumer_name or f"bounded-web-enrich-{unique}"
        self._group_created = False

    async def find_target(
        self,
        config: BoundedWebEnrichConfig,
        state: BoundedWebEnrichState,
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
                    selected = TargetedRedisMessage(redis_message_id=message_id, fields=decoded_fields)
                    break
                if messages_seen >= config.scan_limit:
                    break
        return selected, messages_seen, messages_matched

    async def ack(self, message_id: str, state: BoundedWebEnrichState) -> int:
        state.redis_ack_attempted = True
        result = await self._client.xack(self._queue_name, self._group_name, message_id)
        try:
            return int(result)
        except (TypeError, ValueError):
            return 1 if result else 0

    async def cleanup(self, state: BoundedWebEnrichState) -> None:
        if not self._group_created:
            return
        state.redis_cleanup_attempted = True
        try:
            await self._client.xgroup_destroy(self._queue_name, self._group_name)
        except Exception:
            state.redis_cleanup_suppressed = True


class CountingWebEnricherRepository:
    def __init__(
        self,
        repository: WebEnricherRepository,
        counters: BoundedWebEnrichCounters,
    ) -> None:
        self._repository = repository
        self._counters = counters

    def transaction(self):
        return self._repository.transaction()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID):
        return await self._repository.load_job_by_trigger_event_id(trigger_event_id)

    async def load_artifact(self, artifact_id: UUID):
        return await self._repository.load_artifact(artifact_id)

    async def load_current_snapshot(self, snapshot_id: UUID | None):
        return await self._repository.load_current_snapshot(snapshot_id)

    async def insert_enrichment_run_if_absent(self, **kwargs):
        result = await self._repository.insert_enrichment_run_if_absent(**kwargs)
        if result is not None:
            self._counters.artifact_enrichment_runs_inserted_count += 1
        return result

    async def mark_enrichment_run_started(self, run_id: UUID) -> None:
        await self._repository.mark_enrichment_run_started(run_id)

    async def mark_enrichment_run_finished(self, **kwargs) -> None:
        await self._repository.mark_enrichment_run_finished(**kwargs)
        self._counters.artifact_enrichment_runs_finished_count += 1

    async def insert_snapshot(self, **kwargs):
        result = await self._repository.insert_snapshot(**kwargs)
        self._counters.artifact_snapshots_written_count += 1
        return result

    async def upsert_web_article_child(self, **kwargs) -> None:
        await self._repository.upsert_web_article_child(**kwargs)
        self._counters.artifact_snapshot_web_article_written_count += 1

    async def insert_discovered_url(self, **kwargs) -> None:
        await self._repository.insert_discovered_url(**kwargs)
        self._counters.discovered_url_observations_written_count += 1

    async def update_artifact_current_snapshot(self, **kwargs) -> None:
        await self._repository.update_artifact_current_snapshot(**kwargs)
        self._counters.artifact_registry_updates_count += 1

    async def insert_snapshot_updated_outbox(self, **kwargs) -> None:
        result = await self._repository.insert_snapshot_updated_outbox(**kwargs)
        if result is None:
            self._counters.artifact_snapshot_updated_outbox_count += 1
        else:
            try:
                self._counters.artifact_snapshot_updated_outbox_count += int(result)
            except (TypeError, ValueError):
                self._counters.artifact_snapshot_updated_outbox_count += 1 if result else 0


class GateTrackedWebFetchClient:
    def __init__(self, fetch_client: WebFetchClient, state: BoundedWebEnrichState) -> None:
        self._fetch_client = fetch_client
        self._state = state

    async def fetch(self, url: str) -> FetchedDocument:
        self._state.web_fetch_attempted = True
        return await self._fetch_client.fetch(url)

    async def close(self) -> None:
        await self._fetch_client.close()


async def build_default_bounded_web_enrich_redis_consumer(
    runtime_config: BoundedWebEnrichRuntimeConfig,
    state: BoundedWebEnrichState,
    logger: logging.Logger,
) -> BoundedWebEnrichRedisHandle:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    consumer = TemporaryGroupRedisTargetConsumer(
        redis_client,
        queue_name=runtime_config.web_config.queue_name,
    )

    async def close() -> None:
        await consumer.cleanup(state)
        close_client = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close_client is None:
            return
        result = close_client()
        if hasattr(result, "__await__"):
            await result

    return BoundedWebEnrichRedisHandle(consumer=consumer, close=close)


async def build_default_bounded_web_enrich_database(
    runtime_config: BoundedWebEnrichRuntimeConfig,
    state: BoundedWebEnrichState,
    logger: logging.Logger,
) -> BoundedWebEnrichDatabaseHandle:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    config = runtime_config.web_config
    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    counters = BoundedWebEnrichCounters()
    repository = CountingWebEnricherRepository(WebEnricherRepository(session), counters)
    fetch_client = GateTrackedWebFetchClient(
        WebFetchClient(
            timeout_sec=config.request_timeout_sec,
            max_redirects=config.max_redirects,
            max_bytes=config.max_bytes,
            user_agent=config.user_agent,
            content_type_allowlist=config.content_type_allowlist,
        ),
        state,
    )
    service = WebEnricherService(
        config,
        repository=repository,  # type: ignore[arg-type]
        fetch_client=fetch_client,  # type: ignore[arg-type]
        article_parser=ArticleParser(
            excerpt_chars=config.excerpt_chars,
            max_outbound_links=config.max_outbound_links,
        ),
        url_discovery=WebUrlDiscovery(),
        logger=logger,
    )

    async def close() -> None:
        try:
            await fetch_client.close()
        finally:
            try:
                await session.close()
            finally:
                await engine.dispose()

    return BoundedWebEnrichDatabaseHandle(service=service, counters=counters, close=close)


def load_bounded_web_enrich_runtime_config() -> BoundedWebEnrichRuntimeConfig:
    try:
        web_config = WebEnricherConfig.from_env()
        _validate_runtime_caps(web_config)
    except WebEnricherConfigurationError as exc:
        text = str(exc)
        if "DATABASE_URL" in text:
            raise BoundedWebEnrichError("database_url_missing") from exc
        if "REDIS_URL" in text:
            raise BoundedWebEnrichError("redis_url_missing") from exc
        raise BoundedWebEnrichError("runtime_config_error") from exc
    except BoundedWebEnrichError:
        raise
    except Exception as exc:
        raise BoundedWebEnrichError("runtime_config_error") from exc
    return BoundedWebEnrichRuntimeConfig(web_config=web_config)


async def run_bounded_web_enrich(
    config: BoundedWebEnrichConfig,
    *,
    runtime_config_loader: Callable[[], BoundedWebEnrichRuntimeConfig] = load_bounded_web_enrich_runtime_config,
    redis_builder: BoundedWebEnrichRedisBuilder | None = None,
    database_builder: BoundedWebEnrichDatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedWebEnrichResult:
    state = BoundedWebEnrichState()
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
    except BoundedWebEnrichError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception:
        return _result("blocked", "runtime_config_error", config=config, state=state)

    if not config.allow_redis_consume:
        return _result("blocked", "redis_consume_not_allowed", config=config, state=state)
    if not config.allow_database_write:
        return _result("blocked", "database_write_not_allowed", config=config, state=state)
    if not config.allow_web_fetch:
        return _result("blocked", "web_fetch_not_allowed", config=config, state=state)
    if not config.allow_redis_ack:
        return _result("blocked", "redis_ack_not_allowed", config=config, state=state)

    redis_handle: BoundedWebEnrichRedisHandle | None = None
    database_handle: BoundedWebEnrichDatabaseHandle | None = None
    result: BoundedWebEnrichResult | None = None
    selected: TargetedRedisMessage | None = None
    job: ArtifactEnrichmentJob | None = None
    service_result: EnrichmentResult | None = None
    messages_seen = 0
    messages_matched = 0

    try:
        redis_handle = await (redis_builder or build_default_bounded_web_enrich_redis_consumer)(
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
            raise _BoundedWebEnrichResultReady

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
            raise _BoundedWebEnrichResultReady

        database_handle = await (database_builder or build_default_bounded_web_enrich_database)(
            runtime_config,
            state,
            effective_logger,
        )
        trigger_event_id = str(selected.fields["trigger_event_id"])
        try:
            job = await database_handle.service.rehydrate_job(trigger_event_id)
        except Exception as exc:
            result = _result(
                "failed",
                _service_error_code(exc),
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=database_handle.counters,
            )
            raise _BoundedWebEnrichResultReady
        if job is None:
            result = _result(
                "blocked",
                "trigger_event_not_found_or_not_web",
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=database_handle.counters,
            )
            raise _BoundedWebEnrichResultReady

        job_error = _job_contract_error(job, selected, config)
        if job_error is not None:
            result = _result(
                "blocked",
                job_error,
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=database_handle.counters,
                job=job,
            )
            raise _BoundedWebEnrichResultReady

        try:
            service_result = await database_handle.service.handle_job(job)
        except Exception as exc:
            if _counters_total(database_handle.counters) > 0:
                state.database_write_attempted = True
            result = _result(
                "failed",
                _service_error_code(exc),
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=database_handle.counters,
                job=job,
            )
            raise _BoundedWebEnrichResultReady

        state.database_write_attempted = _counters_total(database_handle.counters) > 0
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
                counters=database_handle.counters,
                job=job,
                service_result=service_result,
            )
            raise _BoundedWebEnrichResultReady
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
                counters=database_handle.counters,
                job=job,
                service_result=service_result,
            )
            raise _BoundedWebEnrichResultReady

        result = _result(
            "processed",
            None,
            config=config,
            state=state,
            selected=selected,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
            messages_processed_count=1,
            redis_acked_count=acked_count,
            counters=database_handle.counters,
            job=job,
            service_result=service_result,
        )
    except _BoundedWebEnrichResultReady:
        pass
    except Exception as exc:
        result = _result(
            "failed",
            "bounded_web_enrich_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            selected=selected,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
            job=job,
            service_result=service_result,
        )
    finally:
        if database_handle is not None:
            try:
                await database_handle.close()
            except Exception as exc:
                result = _close_failure_result(
                    result,
                    exc,
                    config=config,
                    state=state,
                    selected=selected,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                    job=job,
                    service_result=service_result,
                )
        if redis_handle is not None:
            try:
                await redis_handle.close()
            except Exception:
                state.redis_cleanup_suppressed = True

    assert result is not None
    return result


def run_bounded_web_enrich_sync(
    config: BoundedWebEnrichConfig,
    *,
    runtime_config_loader: Callable[[], BoundedWebEnrichRuntimeConfig] = load_bounded_web_enrich_runtime_config,
    redis_builder: BoundedWebEnrichRedisBuilder | None = None,
    database_builder: BoundedWebEnrichDatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedWebEnrichResult:
    return asyncio.run(
        run_bounded_web_enrich(
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
        config=BoundedWebEnrichConfig(),
        state=BoundedWebEnrichState(),
    ).to_sanitized_dict()


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedWebEnrichConfig,
    state: BoundedWebEnrichState,
    error_class: str | None = None,
    selected: TargetedRedisMessage | None = None,
    messages_seen: int = 0,
    messages_matched: int = 0,
    messages_processed_count: int = 0,
    redis_acked_count: int = 0,
    counters: BoundedWebEnrichCounters | None = None,
    job: ArtifactEnrichmentJob | None = None,
    service_result: EnrichmentResult | None = None,
) -> BoundedWebEnrichResult:
    effective_counters = counters or BoundedWebEnrichCounters()
    trigger_id = config.trigger_event_id
    artifact_id = config.artifact_id
    redis_message_id = config.redis_message_id
    if selected is not None:
        redis_message_id = selected.redis_message_id
        trigger_id = _uuid_or_none(selected.fields.get("trigger_event_id")) or trigger_id
        artifact_id = _uuid_or_none(selected.fields.get("root_object_id")) or artifact_id
    if job is not None:
        trigger_id = job.trigger_event_id
        artifact_id = job.artifact_id
    snapshot_id = service_result.snapshot_id if service_result is not None else None
    content_anchor = service_result.content_anchor if service_result is not None else None
    return BoundedWebEnrichResult(
        status=status,
        ok=status == "processed" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        counters=effective_counters,
        target_trigger_event_id_suffix=_optional_uuid_suffix(trigger_id),
        target_artifact_id_suffix=_optional_uuid_suffix(artifact_id),
        target_redis_message_id_suffix=_optional_redis_id_suffix(redis_message_id),
        messages_seen=messages_seen,
        messages_matched=messages_matched,
        messages_processed_count=messages_processed_count,
        redis_acked_count=redis_acked_count,
        snapshot_status=service_result.status if service_result is not None else None,
        snapshot_id_suffix=_optional_uuid_suffix(snapshot_id),
        content_anchor_prefix=_content_anchor_prefix(content_anchor),
    )


def _close_failure_result(
    result: BoundedWebEnrichResult | None,
    exc: Exception,
    *,
    config: BoundedWebEnrichConfig,
    state: BoundedWebEnrichState,
    selected: TargetedRedisMessage | None,
    messages_seen: int,
    messages_matched: int,
    job: ArtifactEnrichmentJob | None,
    service_result: EnrichmentResult | None,
) -> BoundedWebEnrichResult:
    if result is None:
        return _result(
            "failed",
            "database_close_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            selected=selected,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
            job=job,
            service_result=service_result,
        )
    if result.ok:
        return _result(
            "failed",
            "database_close_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            selected=selected,
            messages_seen=result.messages_seen,
            messages_matched=result.messages_matched,
            messages_processed_count=result.messages_processed_count,
            redis_acked_count=result.redis_acked_count,
            counters=result.counters,
            job=job,
            service_result=service_result,
        )
    return result


def _target_error(config: BoundedWebEnrichConfig) -> str | None:
    selected = [
        config.trigger_event_id is not None,
        config.artifact_id is not None,
        bool(config.redis_message_id),
    ]
    count = sum(1 for item in selected if item)
    if count == 0:
        return "target_missing"
    if count > 1:
        return "target_conflict"
    return None


def _matches_target(message_id: str, fields: Mapping[str, Any], config: BoundedWebEnrichConfig) -> bool:
    if config.redis_message_id:
        return message_id == config.redis_message_id
    if config.trigger_event_id is not None:
        return str(fields.get("trigger_event_id", "")) == str(config.trigger_event_id)
    if config.artifact_id is not None:
        return str(fields.get("root_object_id", "")) == str(config.artifact_id)
    return False


def _selected_message_contract_error(
    selected: TargetedRedisMessage,
    config: BoundedWebEnrichConfig,
) -> str | None:
    if FORBIDDEN_STREAM_FIELDS.intersection(selected.fields):
        return "redis_message_contract_invalid"
    if any(not str(selected.fields.get(key, "")).strip() for key in REQUIRED_STREAM_FIELDS):
        return "redis_message_contract_invalid"
    if selected.fields.get("stage_name") != STAGE_NAME:
        return "stage_not_allowed"
    if selected.fields.get("root_object_type") != ROOT_OBJECT_TYPE:
        return "root_object_type_not_allowed"
    selected_trigger_event_id = _uuid_or_none(selected.fields.get("trigger_event_id"))
    if selected_trigger_event_id is None:
        return "trigger_event_id_invalid"
    selected_artifact_id = _uuid_or_none(selected.fields.get("root_object_id"))
    if selected_artifact_id is None:
        return "redis_message_contract_invalid"
    if config.trigger_event_id is not None and selected_trigger_event_id != config.trigger_event_id:
        return "target_trigger_event_mismatch"
    if config.artifact_id is not None and selected_artifact_id != config.artifact_id:
        return "target_artifact_mismatch"
    return None


def _job_contract_error(
    job: ArtifactEnrichmentJob,
    selected: TargetedRedisMessage,
    config: BoundedWebEnrichConfig,
) -> str | None:
    selected_trigger_event_id = _uuid_or_none(selected.fields.get("trigger_event_id"))
    selected_artifact_id = _uuid_or_none(selected.fields.get("root_object_id"))
    if selected_trigger_event_id != job.trigger_event_id:
        return "target_trigger_event_mismatch"
    if selected_artifact_id != job.artifact_id:
        return "target_artifact_mismatch"
    if config.trigger_event_id is not None and config.trigger_event_id != job.trigger_event_id:
        return "target_trigger_event_mismatch"
    if config.artifact_id is not None and config.artifact_id != job.artifact_id:
        return "target_artifact_mismatch"
    if job.event_type != "artifact.enrich.requested.v1":
        return "trigger_event_contract_invalid"
    if job.provider_route != "web":
        return "provider_route_not_allowed"
    if job.artifact_type != "web_article":
        return "artifact_type_not_allowed"
    return None


def _validate_runtime_caps(config: WebEnricherConfig) -> None:
    if config.queue_name != QUEUE_NAME:
        raise BoundedWebEnrichError("queue_name_mismatch")
    if config.max_redirects < 0 or config.max_redirects > HARD_MAX_REDIRECTS:
        raise BoundedWebEnrichError("redirect_cap_out_of_range")
    if config.max_bytes <= 0 or config.max_bytes > HARD_MAX_RESPONSE_BYTES:
        raise BoundedWebEnrichError("response_body_cap_out_of_range")
    if config.request_timeout_sec <= 0 or config.request_timeout_sec > HARD_MAX_TIMEOUT_SEC:
        raise BoundedWebEnrichError("timeout_cap_out_of_range")
    if config.max_outbound_links <= 0 or config.max_outbound_links > HARD_MAX_OUTBOUND_LINKS:
        raise BoundedWebEnrichError("outbound_link_cap_out_of_range")
    if not set(config.content_type_allowlist).issubset(ALLOWED_CONTENT_TYPES):
        raise BoundedWebEnrichError("content_type_allowlist_invalid")


def _service_error_code(exc: Exception) -> str:
    if isinstance(exc, ValueError):
        return "trigger_event_contract_invalid"
    return "database_write_failed"


def _counters_total(counters: BoundedWebEnrichCounters) -> int:
    return (
        counters.artifact_enrichment_runs_inserted_count
        + counters.artifact_enrichment_runs_finished_count
        + counters.artifact_snapshots_written_count
        + counters.artifact_snapshot_web_article_written_count
        + counters.discovered_url_observations_written_count
        + counters.artifact_registry_updates_count
        + counters.artifact_snapshot_updated_outbox_count
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


def _uuid_or_none(value: UUID | str | None | Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _optional_uuid_suffix(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    parsed = _uuid_or_none(value)
    text = str(parsed or value)
    return text[-8:] if text else None


def _optional_redis_id_suffix(value: str | None) -> str | None:
    if not value:
        return None
    return value[-9:] if "-" in value else value[-8:]


def _content_anchor_prefix(value: str | None) -> str | None:
    if not value:
        return None
    return value[:12]


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


def _redis_ack_status(*, attempted: bool, acked_count: int, error_code: str | None) -> str:
    if not attempted:
        return "not_attempted"
    if error_code == "redis_ack_failed":
        return "failed"
    if acked_count == 1:
        return "acked"
    return "unknown"
