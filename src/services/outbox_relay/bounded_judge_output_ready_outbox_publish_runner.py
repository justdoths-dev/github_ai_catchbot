from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - local fallback for static validation
    sa = None

from .eligibility import stale_resolution_exclusion_not_exists_sql
from .models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from .redis_streams import RedisStreamsPublisher
from .repositories import AsyncSessionLike, OutboxRelayRepository
from .routing import OutboxRouteResolver, UnsupportedOutboxEventTypeError

SCHEMA_VERSION = "bounded_judge_output_ready_outbox_publish_v1"
RUNNER_NAME = "bounded_judge_output_ready_outbox_publish_runner"
MODE = "judge_output_ready_outbox_one_shot_publish"
EVENT_TYPE = "judge.output.ready.v1"
ROOT_OBJECT_TYPE = "judge_run"
QUEUE_NAME = "q.analysis.validate"
STAGE_NAME = "analysis_validate"
DEFAULT_XADD_MAXLEN = 10000
DEFAULT_SCAN_LIMIT = 10
MIN_SCAN_LIMIT = 2
HARD_SCAN_LIMIT = 100
REQUIRED_PAYLOAD_FIELDS = ("judge_run_id", "judge_output_id")
THIN_STREAM_FIELDS = {
    "job_id",
    "stage_name",
    "root_object_type",
    "root_object_id",
    "idempotency_key",
    "pipeline_run_id",
    "not_before",
    "trigger_event_id",
}


@dataclass(frozen=True, slots=True)
class BoundedJudgeOutputReadyOutboxPublishConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_database_write: bool = False
    allow_redis_publish: bool = False
    trigger_event_suffix: str | None = None
    judge_run_suffix: str | None = None
    judge_output_suffix: str | None = None
    scan_limit: int = DEFAULT_SCAN_LIMIT


@dataclass(frozen=True, slots=True)
class BoundedJudgeOutputReadyPublishRuntimeConfig:
    database_url: str
    redis_url: str
    xadd_maxlen: int | None = DEFAULT_XADD_MAXLEN


@dataclass(slots=True)
class BoundedJudgeOutputReadyOutboxPublishState:
    runtime_config_loaded: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    redis_publisher_created: bool = False
    redis_publish_attempted: bool = False
    event_outbox_status_write_attempted: bool = False
    job_attempt_insert_attempted: bool = False

    @property
    def database_write_attempted(self) -> bool:
        return self.event_outbox_status_write_attempted or self.job_attempt_insert_attempted


class BoundedJudgeOutputReadyOutboxPublishError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class _PublishResultReady(Exception):
    pass


class BoundedJudgeOutputReadyOutboxRepository(Protocol):
    async def fetch_target_events(
        self,
        *,
        trigger_event_suffix: str,
        limit: int,
    ) -> list[OutboxEventRow]: ...

    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None: ...

    async def insert_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None: ...


class RedisPublisher(Protocol):
    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str: ...


@dataclass(frozen=True, slots=True)
class BoundedJudgeOutputReadyRepositoryHandle:
    repository: BoundedJudgeOutputReadyOutboxRepository
    close: Callable[[bool], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedJudgeOutputReadyRedisPublisherHandle:
    publisher: RedisPublisher
    close: Callable[[], Awaitable[None]]


class BoundedJudgeOutputReadyRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedJudgeOutputReadyPublishRuntimeConfig,
        state: BoundedJudgeOutputReadyOutboxPublishState,
        logger: logging.Logger,
    ) -> BoundedJudgeOutputReadyRepositoryHandle: ...


class BoundedJudgeOutputReadyRedisPublisherBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedJudgeOutputReadyPublishRuntimeConfig,
        state: BoundedJudgeOutputReadyOutboxPublishState,
        logger: logging.Logger,
    ) -> BoundedJudgeOutputReadyRedisPublisherHandle: ...


@dataclass(frozen=True, slots=True)
class BoundedJudgeOutputReadyOutboxPublishResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedJudgeOutputReadyOutboxPublishConfig
    state: BoundedJudgeOutputReadyOutboxPublishState = field(
        default_factory=BoundedJudgeOutputReadyOutboxPublishState
    )
    selector_type: str | None = None
    target_trigger_event_id_suffix: str | None = None
    target_judge_run_id_suffix: str | None = None
    target_judge_output_id_suffix: str | None = None
    redis_message_id_suffix: str | None = None
    events_seen: int = 0
    redis_published_count: int = 0
    event_outbox_status_updated_count: int = 0
    job_attempts_written_count: int = 0
    queue_name: str | None = None
    stage_name: str | None = None
    selected_event_status: str | None = None
    selected_event_type: str | None = None
    selected_aggregate_type: str | None = None
    payload_has_judge_run_id: bool = False
    payload_has_judge_output_id: bool = False
    aggregate_judge_run_suffix_matches: bool = False
    payload_judge_run_suffix_matches: bool = False
    payload_judge_output_suffix_matches: bool = False
    payload_judge_run_id_matches_aggregate: bool = False
    thin_stream_fields_valid: bool = False
    duplicate_handling_status: str | None = None

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "selector_type": self.selector_type,
            "target_trigger_event_id_suffix": self.target_trigger_event_id_suffix,
            "target_judge_run_id_suffix": self.target_judge_run_id_suffix,
            "target_judge_output_id_suffix": self.target_judge_output_id_suffix,
            "redis_message_id_suffix": self.redis_message_id_suffix,
            "events_seen": self.events_seen,
            "scan_limit": self.config.scan_limit,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "selected_event_status": self.selected_event_status,
            "selected_event_type": self.selected_event_type,
            "selected_aggregate_type": self.selected_aggregate_type,
            "payload_has_judge_run_id": self.payload_has_judge_run_id,
            "payload_has_judge_output_id": self.payload_has_judge_output_id,
            "aggregate_judge_run_suffix_matches": self.aggregate_judge_run_suffix_matches,
            "payload_judge_run_suffix_matches": self.payload_judge_run_suffix_matches,
            "payload_judge_output_suffix_matches": self.payload_judge_output_suffix_matches,
            "payload_judge_run_id_matches_aggregate": self.payload_judge_run_id_matches_aggregate,
            "thin_stream_fields_valid": self.thin_stream_fields_valid,
            "duplicate_handling_status": self.duplicate_handling_status,
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": self.state.database_write_attempted,
            "redis_publish_attempted": self.state.redis_publish_attempted,
            "redis_published_count": self.redis_published_count,
            "event_outbox_status_updated_count": self.event_outbox_status_updated_count,
            "job_attempts_written_count": self.job_attempts_written_count,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "database_read_allowed": self.config.allow_database_read,
                "database_write_allowed": self.config.allow_database_write,
                "redis_publish_allowed": self.config.allow_redis_publish,
                "scan_limit": self.config.scan_limit,
            },
            "side_effects": {
                "redis_mutation": self.redis_published_count > 0,
                "db_write": self.state.database_write_attempted,
                "queue_consume_called": False,
                "redis_ack_called": False,
                "redis_claim_called": False,
                "redis_delete_called": False,
                "redis_group_create_called": False,
                "analysis_validator_called": False,
                "policy_called": False,
                "notifier_called": False,
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
                "full_judge_run_id_omitted": True,
                "full_judge_output_id_omitted": True,
                "idempotency_key_omitted": True,
                "payload_json_omitted": True,
                "business_fields_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "redis_message_id_truncated": True,
                "exception_detail_omitted": True,
            },
        }


class SqlAlchemyBoundedJudgeOutputReadyOutboxRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session
        self._relay_repository = OutboxRelayRepository(session)

    async def fetch_target_events(
        self,
        *,
        trigger_event_suffix: str,
        limit: int,
    ) -> list[OutboxEventRow]:
        result = await self._session.execute(
            _sql(
                f"""
                SELECT
                    eo.event_id,
                    eo.event_type,
                    eo.aggregate_type,
                    eo.aggregate_id,
                    eo.dedupe_key,
                    eo.payload_json,
                    eo.status,
                    eo.fail_count,
                    eo.created_at
                FROM event_outbox eo
                WHERE eo.event_type = :event_type
                  AND lower(CAST(eo.event_id AS text)) LIKE :event_suffix_pattern
                  AND {stale_resolution_exclusion_not_exists_sql("eo")}
                ORDER BY eo.created_at ASC, eo.event_id ASC
                LIMIT :limit
                """
            ),
            {
                "event_type": EVENT_TYPE,
                "event_suffix_pattern": f"%{trigger_event_suffix.lower()}",
                "limit": limit,
            },
        )
        return [_row_from_mapping(row) for row in result.mappings().all()]

    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None:
        await self._relay_repository.mark_published(event_id=event_id, published_at=published_at)

    async def insert_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None:
        await self._relay_repository.insert_job_attempt(
            stage_name=stage_name,
            queue_name=queue_name,
            root_object_type=root_object_type,
            root_object_id=root_object_id,
            attempt_status=attempt_status,
            error_code=error_code,
        )


def load_bounded_judge_output_ready_publish_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedJudgeOutputReadyPublishRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedJudgeOutputReadyOutboxPublishError("database_url_missing")
    if not redis_url:
        raise BoundedJudgeOutputReadyOutboxPublishError("redis_url_missing")
    xadd_maxlen_raw = _env_value(source, "OUTBOX_RELAY_XADD_MAXLEN", str(DEFAULT_XADD_MAXLEN))
    try:
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
    except ValueError as exc:
        raise BoundedJudgeOutputReadyOutboxPublishError("runtime_config_error") from exc
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        raise BoundedJudgeOutputReadyOutboxPublishError("runtime_config_error")
    return BoundedJudgeOutputReadyPublishRuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        xadd_maxlen=xadd_maxlen,
    )


async def build_default_bounded_judge_output_ready_repository(
    runtime_config: BoundedJudgeOutputReadyPublishRuntimeConfig,
    state: BoundedJudgeOutputReadyOutboxPublishState,
    logger: logging.Logger,
) -> BoundedJudgeOutputReadyRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session_context = session_factory.begin()
    session = await session_context.__aenter__()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedJudgeOutputReadyOutboxRepository(session)

    async def close(commit: bool) -> None:
        if not commit:
            await session.rollback()
        await session_context.__aexit__(None, None, None)
        await engine.dispose()

    return BoundedJudgeOutputReadyRepositoryHandle(repository=repository, close=close)


async def build_default_bounded_judge_output_ready_redis_publisher(
    runtime_config: BoundedJudgeOutputReadyPublishRuntimeConfig,
    state: BoundedJudgeOutputReadyOutboxPublishState,
    logger: logging.Logger,
) -> BoundedJudgeOutputReadyRedisPublisherHandle:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    state.redis_publisher_created = True
    publisher = RedisStreamsPublisher(redis_client, maxlen=runtime_config.xadd_maxlen)

    async def close() -> None:
        close_client = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close_client is None:
            return
        result = close_client()
        if hasattr(result, "__await__"):
            await result

    return BoundedJudgeOutputReadyRedisPublisherHandle(publisher=publisher, close=close)


async def run_bounded_judge_output_ready_outbox_publish(
    config: BoundedJudgeOutputReadyOutboxPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedJudgeOutputReadyPublishRuntimeConfig] = (
        load_bounded_judge_output_ready_publish_runtime_config
    ),
    repository_builder: BoundedJudgeOutputReadyRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedJudgeOutputReadyRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedJudgeOutputReadyOutboxPublishResult:
    state = BoundedJudgeOutputReadyOutboxPublishState()
    gate_error = _gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedJudgeOutputReadyOutboxPublishError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception:
        return _result("blocked", "runtime_config_error", config=config, state=state)

    repository_handle: BoundedJudgeOutputReadyRepositoryHandle | None = None
    publisher_handle: BoundedJudgeOutputReadyRedisPublisherHandle | None = None
    commit_repository = False
    result: BoundedJudgeOutputReadyOutboxPublishResult | None = None
    try:
        repository_handle = await (repository_builder or build_default_bounded_judge_output_ready_repository)(
            runtime_config,
            state,
            effective_logger,
        )
        repository = repository_handle.repository
        state.database_read_attempted = True
        rows = await repository.fetch_target_events(
            trigger_event_suffix=str(config.trigger_event_suffix),
            limit=config.scan_limit,
        )
        events_seen = len(rows)
        if events_seen == 0:
            result = _result("blocked", "target_event_not_found", config=config, state=state)
            raise _PublishResultReady
        if events_seen != 1:
            result = _result(
                "blocked",
                "target_event_not_unique",
                config=config,
                state=state,
                events_seen=events_seen,
            )
            raise _PublishResultReady

        row = rows[0]
        payload_flags = _payload_presence_flags(row.payload_json)
        payload_judge_run_id = _payload_uuid(row.payload_json, "judge_run_id")
        payload_judge_output_id = _payload_uuid(row.payload_json, "judge_output_id")
        aggregate_suffix_matches = _ends_with_suffix(row.aggregate_id, str(config.judge_run_suffix))
        payload_judge_run_suffix_matches = (
            _ends_with_suffix(payload_judge_run_id, str(config.judge_run_suffix))
            if payload_judge_run_id is not None
            else False
        )
        payload_judge_output_suffix_matches = (
            _ends_with_suffix(payload_judge_output_id, str(config.judge_output_suffix))
            if payload_judge_output_id is not None
            else False
        )
        judge_run_id_matches_aggregate = payload_judge_run_id == row.aggregate_id if payload_judge_run_id else False

        selected_kwargs = {
            "selected_event": row,
            "events_seen": events_seen,
            "payload_flags": payload_flags,
            "payload_judge_run_id": payload_judge_run_id,
            "payload_judge_output_id": payload_judge_output_id,
            "aggregate_suffix_matches": aggregate_suffix_matches,
            "payload_judge_run_suffix_matches": payload_judge_run_suffix_matches,
            "payload_judge_output_suffix_matches": payload_judge_output_suffix_matches,
            "judge_run_id_matches_aggregate": judge_run_id_matches_aggregate,
        }
        if row.event_type != EVENT_TYPE:
            result = _blocked_selected("wrong_event_type", config, state, **selected_kwargs)
            raise _PublishResultReady
        if row.aggregate_type != ROOT_OBJECT_TYPE:
            result = _blocked_selected("wrong_aggregate_type", config, state, **selected_kwargs)
            raise _PublishResultReady
        if not aggregate_suffix_matches:
            result = _blocked_selected("judge_run_suffix_mismatch", config, state, **selected_kwargs)
            raise _PublishResultReady
        if not all(payload_flags.values()) or payload_judge_run_id is None or payload_judge_output_id is None:
            result = _blocked_selected("malformed_event_payload", config, state, **selected_kwargs)
            raise _PublishResultReady
        if not judge_run_id_matches_aggregate:
            result = _blocked_selected("judge_run_id_mismatch", config, state, **selected_kwargs)
            raise _PublishResultReady
        if not payload_judge_run_suffix_matches:
            result = _blocked_selected("payload_judge_run_suffix_mismatch", config, state, **selected_kwargs)
            raise _PublishResultReady
        if not payload_judge_output_suffix_matches:
            result = _blocked_selected("payload_judge_output_suffix_mismatch", config, state, **selected_kwargs)
            raise _PublishResultReady

        try:
            canonical_route = OutboxRouteResolver().resolve(row)
            route = route_resolver.resolve(row) if route_resolver is not None else canonical_route
        except UnsupportedOutboxEventTypeError:
            result = _blocked_selected("unsupported_event_type", config, state, **selected_kwargs)
            raise _PublishResultReady
        if route != canonical_route or route.queue_name != QUEUE_NAME or route.stage_name != STAGE_NAME:
            result = _blocked_selected("route_not_allowed", config, state, route=route, **selected_kwargs)
            raise _PublishResultReady

        if row.status == "published":
            result = _result(
                "noop",
                None,
                config=config,
                state=state,
                queue_name=route.queue_name,
                stage_name=route.stage_name,
                duplicate_handling_status="already_published_noop",
                **selected_kwargs,
            )
            raise _PublishResultReady
        if row.status != "pending":
            result = _blocked_selected(
                "target_event_status_not_allowed",
                config,
                state,
                route=route,
                **selected_kwargs,
            )
            raise _PublishResultReady

        message = _build_stream_message(row, route)
        if set(message.as_stream_fields()) != THIN_STREAM_FIELDS:
            result = _blocked_selected("redis_message_shape_invalid", config, state, route=route, **selected_kwargs)
            raise _PublishResultReady

        publisher_handle = await (redis_publisher_builder or build_default_bounded_judge_output_ready_redis_publisher)(
            runtime_config,
            state,
            effective_logger,
        )
        try:
            state.redis_publish_attempted = True
            redis_message_id = await publisher_handle.publisher.publish(route, message)
        except Exception as exc:
            result = _result(
                "failed",
                "redis_xadd_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                queue_name=route.queue_name,
                stage_name=route.stage_name,
                duplicate_handling_status="pending_publish_failed",
                **selected_kwargs,
            )
            raise _PublishResultReady

        event_outbox_status_updated_count = 0
        job_attempts_written_count = 0
        try:
            state.event_outbox_status_write_attempted = True
            await repository.mark_published(
                event_id=row.event_id,
                published_at=(clock or _utc_now)(),
            )
            event_outbox_status_updated_count = 1
            state.job_attempt_insert_attempted = True
            await repository.insert_job_attempt(
                stage_name=route.stage_name,
                queue_name=route.queue_name,
                root_object_type=row.aggregate_type,
                root_object_id=row.aggregate_id,
                attempt_status="succeeded",
                error_code=None,
            )
            job_attempts_written_count = 1
        except Exception as exc:
            result = _result(
                "failed",
                "database_write_failed_after_redis_publish",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                redis_published_count=1,
                event_outbox_status_updated_count=event_outbox_status_updated_count,
                job_attempts_written_count=job_attempts_written_count,
                redis_message_id=redis_message_id,
                queue_name=route.queue_name,
                stage_name=route.stage_name,
                duplicate_handling_status="pending_publish_partial_failure",
                **selected_kwargs,
            )
            raise _PublishResultReady

        commit_repository = True
        result = _result(
            "published",
            None,
            config=config,
            state=state,
            redis_published_count=1,
            event_outbox_status_updated_count=1,
            job_attempts_written_count=1,
            redis_message_id=redis_message_id,
            queue_name=route.queue_name,
            stage_name=route.stage_name,
            duplicate_handling_status="published_once",
            **selected_kwargs,
        )
    except _PublishResultReady:
        pass
    except Exception as exc:
        result = _result(
            "failed",
            "bounded_publish_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
        )
    finally:
        if publisher_handle is not None:
            try:
                await publisher_handle.close()
            except Exception:
                pass
        if repository_handle is not None:
            try:
                await repository_handle.close(commit_repository)
            except Exception as exc:
                error_code = _repository_close_error_code(commit_repository)
                if result is None:
                    result = _result(
                        "failed",
                        error_code,
                        error_class=_safe_exception_class(exc),
                        config=config,
                        state=state,
                    )
                else:
                    result = replace(
                        result,
                        status="failed",
                        ok=False,
                        error_code=error_code,
                        error_class=_safe_exception_class(exc),
                    )

    assert result is not None
    return result


def run_bounded_judge_output_ready_outbox_publish_sync(
    config: BoundedJudgeOutputReadyOutboxPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedJudgeOutputReadyPublishRuntimeConfig] = (
        load_bounded_judge_output_ready_publish_runtime_config
    ),
    repository_builder: BoundedJudgeOutputReadyRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedJudgeOutputReadyRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedJudgeOutputReadyOutboxPublishResult:
    return asyncio.run(
        run_bounded_judge_output_ready_outbox_publish(
            config,
            runtime_config_loader=runtime_config_loader,
            repository_builder=repository_builder,
            redis_publisher_builder=redis_publisher_builder,
            route_resolver=route_resolver,
            clock=clock,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        error_code,
        config=BoundedJudgeOutputReadyOutboxPublishConfig(),
        state=BoundedJudgeOutputReadyOutboxPublishState(),
    ).to_sanitized_dict()


def _gate_error(config: BoundedJudgeOutputReadyOutboxPublishConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    suffix_errors = (
        _suffix_error(config.trigger_event_suffix, "trigger_event_suffix"),
        _suffix_error(config.judge_run_suffix, "judge_run_suffix"),
        _suffix_error(config.judge_output_suffix, "judge_output_suffix"),
    )
    for error in suffix_errors:
        if error is not None:
            return error
    if config.scan_limit < MIN_SCAN_LIMIT:
        return "scan_limit_too_small"
    if config.scan_limit > HARD_SCAN_LIMIT:
        return "scan_limit_too_large"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if not config.allow_database_write:
        return "database_write_not_allowed"
    if not config.allow_redis_publish:
        return "redis_publish_not_allowed"
    return None


def _blocked_selected(
    error_code: str,
    config: BoundedJudgeOutputReadyOutboxPublishConfig,
    state: BoundedJudgeOutputReadyOutboxPublishState,
    *,
    selected_event: OutboxEventRow,
    events_seen: int,
    payload_flags: Mapping[str, bool],
    payload_judge_run_id: UUID | None,
    payload_judge_output_id: UUID | None,
    aggregate_suffix_matches: bool,
    payload_judge_run_suffix_matches: bool,
    payload_judge_output_suffix_matches: bool,
    judge_run_id_matches_aggregate: bool,
    route: QueueRoute | None = None,
) -> BoundedJudgeOutputReadyOutboxPublishResult:
    return _result(
        "blocked",
        error_code,
        config=config,
        state=state,
        selected_event=selected_event,
        events_seen=events_seen,
        payload_flags=payload_flags,
        payload_judge_run_id=payload_judge_run_id,
        payload_judge_output_id=payload_judge_output_id,
        aggregate_suffix_matches=aggregate_suffix_matches,
        payload_judge_run_suffix_matches=payload_judge_run_suffix_matches,
        payload_judge_output_suffix_matches=payload_judge_output_suffix_matches,
        judge_run_id_matches_aggregate=judge_run_id_matches_aggregate,
        queue_name=route.queue_name if route else None,
        stage_name=route.stage_name if route else None,
        duplicate_handling_status="not_applicable",
    )


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedJudgeOutputReadyOutboxPublishConfig,
    state: BoundedJudgeOutputReadyOutboxPublishState,
    error_class: str | None = None,
    selected_event: OutboxEventRow | None = None,
    events_seen: int = 0,
    redis_published_count: int = 0,
    event_outbox_status_updated_count: int = 0,
    job_attempts_written_count: int = 0,
    payload_flags: Mapping[str, bool] | None = None,
    payload_judge_run_id: UUID | None = None,
    payload_judge_output_id: UUID | None = None,
    aggregate_suffix_matches: bool = False,
    payload_judge_run_suffix_matches: bool = False,
    payload_judge_output_suffix_matches: bool = False,
    judge_run_id_matches_aggregate: bool = False,
    redis_message_id: str | None = None,
    queue_name: str | None = None,
    stage_name: str | None = None,
    duplicate_handling_status: str | None = None,
) -> BoundedJudgeOutputReadyOutboxPublishResult:
    flags = dict(payload_flags or {})
    event_id = selected_event.event_id if selected_event is not None else None
    judge_run_id = (
        selected_event.aggregate_id
        if selected_event is not None and selected_event.aggregate_type == ROOT_OBJECT_TYPE
        else payload_judge_run_id
    )
    ok = error_code is None and status in {"published", "noop"}
    return BoundedJudgeOutputReadyOutboxPublishResult(
        status=status,
        ok=ok,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        selector_type="trigger_event_suffix" if config.trigger_event_suffix else None,
        target_trigger_event_id_suffix=_optional_id_suffix(event_id) or _clean_suffix(config.trigger_event_suffix),
        target_judge_run_id_suffix=_optional_id_suffix(judge_run_id) or _clean_suffix(config.judge_run_suffix),
        target_judge_output_id_suffix=_optional_id_suffix(payload_judge_output_id)
        or _clean_suffix(config.judge_output_suffix),
        redis_message_id_suffix=_redis_message_id_suffix(redis_message_id),
        events_seen=events_seen,
        redis_published_count=redis_published_count,
        event_outbox_status_updated_count=event_outbox_status_updated_count,
        job_attempts_written_count=job_attempts_written_count,
        queue_name=queue_name,
        stage_name=stage_name,
        selected_event_status=selected_event.status if selected_event is not None else None,
        selected_event_type=selected_event.event_type if selected_event is not None else None,
        selected_aggregate_type=selected_event.aggregate_type if selected_event is not None else None,
        payload_has_judge_run_id=flags.get("judge_run_id", False),
        payload_has_judge_output_id=flags.get("judge_output_id", False),
        aggregate_judge_run_suffix_matches=aggregate_suffix_matches,
        payload_judge_run_suffix_matches=payload_judge_run_suffix_matches,
        payload_judge_output_suffix_matches=payload_judge_output_suffix_matches,
        payload_judge_run_id_matches_aggregate=judge_run_id_matches_aggregate,
        thin_stream_fields_valid=queue_name == QUEUE_NAME and stage_name == STAGE_NAME and selected_event is not None,
        duplicate_handling_status=duplicate_handling_status,
    )


def _build_stream_message(row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
    return RedisQueuedMessage(
        job_id=str(row.event_id),
        stage_name=route.stage_name,
        root_object_type=row.aggregate_type,
        root_object_id=str(row.aggregate_id),
        idempotency_key=row.dedupe_key,
        pipeline_run_id="",
        not_before="",
        trigger_event_id=str(row.event_id),
    )


def _payload_presence_flags(payload: Mapping[str, Any]) -> dict[str, bool]:
    return {field_name: _payload_field_present(payload.get(field_name)) for field_name in REQUIRED_PAYLOAD_FIELDS}


def _payload_field_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _payload_uuid(payload: Mapping[str, Any], field_name: str) -> UUID | None:
    try:
        return UUID(str(payload.get(field_name)))
    except (TypeError, ValueError):
        return None


def _row_from_mapping(row: Mapping[str, Any]) -> OutboxEventRow:
    payload = row["payload_json"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return OutboxEventRow(
        event_id=UUID(str(row["event_id"])),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=UUID(str(row["aggregate_id"])),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload or {},
        status=str(row["status"]),
        fail_count=int(row["fail_count"]),
        created_at=row["created_at"],
    )


def _env_value(source: Mapping[str, str], key: str, default: str | None = None) -> str | None:
    value = source.get(key, default)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _suffix_error(value: str | None, label: str) -> str | None:
    cleaned = _clean_suffix(value)
    if cleaned is None:
        return f"{label}_missing"
    if _looks_like_full_uuid(cleaned):
        return f"raw_{label}_not_allowed"
    if not _is_valid_suffix(cleaned):
        return f"invalid_{label}"
    return None


def _clean_suffix(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip().lower()
    return stripped or None


def _is_valid_suffix(value: str) -> bool:
    return 4 <= len(value) <= 16 and all(char in "0123456789abcdef" for char in value)


def _looks_like_full_uuid(value: str) -> bool:
    try:
        UUID(value)
    except ValueError:
        return False
    return True


def _ends_with_suffix(value: UUID | None, suffix: str) -> bool:
    if value is None:
        return False
    return str(value).replace("-", "").lower().endswith(suffix.lower())


def _optional_id_suffix(value: UUID | None) -> str | None:
    if value is None:
        return None
    return str(value).replace("-", "")[-8:]


def _redis_message_id_suffix(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip()
    return cleaned[-8:] if len(cleaned) > 8 else cleaned


def _safe_exception_class(exc: BaseException) -> str:
    return exc.__class__.__name__


def _repository_close_error_code(commit_repository: bool) -> str:
    if commit_repository:
        return "database_commit_failed_after_redis_publish"
    return "database_rollback_failed"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)
