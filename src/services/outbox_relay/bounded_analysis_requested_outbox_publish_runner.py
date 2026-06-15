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

from .models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from .redis_streams import RedisStreamsPublisher
from .repositories import AsyncSessionLike, OutboxRelayRepository
from .routing import OutboxRouteResolver, UnsupportedOutboxEventTypeError

SCHEMA_VERSION = "bounded_analysis_requested_outbox_publish_v1"
RUNNER_NAME = "bounded_analysis_requested_outbox_publish_runner"
MODE = "analysis_requested_outbox_one_shot_publish"
EVENT_TYPE = "analysis.requested.v1"
ROOT_OBJECT_TYPE = "candidate_group"
QUEUE_NAME = "q.analysis.route"
STAGE_NAME = "analysis_route"
DEFAULT_XADD_MAXLEN = 10000
DEFAULT_MAX_EVENTS = 1
HARD_MAX_EVENTS = 1
REQUIRED_PAYLOAD_FIELDS = (
    "candidate_group_id",
    "bundle_id",
    "judge_profile",
    "escalation_allowed",
)
ALLOWED_JUDGE_PROFILES = frozenset(
    {
        "github_primary",
        "x_primary",
        "text_idea_primary",
    }
)


@dataclass(frozen=True, slots=True)
class BoundedAnalysisRequestedOutboxPublishConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_publish: bool = False
    allow_database_write: bool = False
    event_id: UUID | None = None
    event_suffix: str | None = None
    max_events: int = DEFAULT_MAX_EVENTS


@dataclass(frozen=True, slots=True)
class BoundedAnalysisRequestedPublishRuntimeConfig:
    database_url: str
    redis_url: str
    xadd_maxlen: int | None = DEFAULT_XADD_MAXLEN


@dataclass(slots=True)
class BoundedAnalysisRequestedOutboxPublishState:
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


class BoundedAnalysisRequestedOutboxPublishError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class _PublishResultReady(Exception):
    pass


class BoundedAnalysisRequestedOutboxRepository(Protocol):
    async def fetch_target_events(
        self,
        *,
        event_id: UUID | None,
        event_suffix: str | None,
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
class BoundedAnalysisRequestedRepositoryHandle:
    repository: BoundedAnalysisRequestedOutboxRepository
    close: Callable[[bool], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedAnalysisRequestedRedisPublisherHandle:
    publisher: RedisPublisher
    close: Callable[[], Awaitable[None]]


class BoundedAnalysisRequestedRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedAnalysisRequestedPublishRuntimeConfig,
        state: BoundedAnalysisRequestedOutboxPublishState,
        logger: logging.Logger,
    ) -> BoundedAnalysisRequestedRepositoryHandle: ...


class BoundedAnalysisRequestedRedisPublisherBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedAnalysisRequestedPublishRuntimeConfig,
        state: BoundedAnalysisRequestedOutboxPublishState,
        logger: logging.Logger,
    ) -> BoundedAnalysisRequestedRedisPublisherHandle: ...


@dataclass(frozen=True, slots=True)
class BoundedAnalysisRequestedOutboxPublishResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedAnalysisRequestedOutboxPublishConfig
    state: BoundedAnalysisRequestedOutboxPublishState = field(
        default_factory=BoundedAnalysisRequestedOutboxPublishState
    )
    selector_type: str | None = None
    target_event_id_suffix: str | None = None
    target_candidate_group_suffix: str | None = None
    events_seen: int = 0
    redis_published_count: int = 0
    event_outbox_status_updated_count: int = 0
    job_attempts_written_count: int = 0
    queue_name: str | None = None
    stage_name: str | None = None
    selected_event_status: str | None = None
    selected_event_type: str | None = None
    selected_aggregate_type: str | None = None
    payload_has_candidate_group_id: bool = False
    payload_has_bundle_id: bool = False
    payload_has_judge_profile: bool = False
    payload_has_escalation_allowed: bool = False
    payload_candidate_group_id_matches: bool = False
    payload_judge_profile_allowed: bool = False

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
            "target_event_id_suffix": self.target_event_id_suffix,
            "target_candidate_group_suffix": self.target_candidate_group_suffix,
            "events_seen": self.events_seen,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "redis_publish_attempted": self.state.redis_publish_attempted,
            "redis_published_count": self.redis_published_count,
            "database_write_attempted": self.state.database_write_attempted,
            "event_outbox_status_updated_count": self.event_outbox_status_updated_count,
            "job_attempts_written_count": self.job_attempts_written_count,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "redis_publish_allowed": self.config.allow_redis_publish,
                "database_write_allowed": self.config.allow_database_write,
                "max_events": self.config.max_events,
            },
            "selected_event_status": self.selected_event_status,
            "selected_event_type": self.selected_event_type,
            "selected_aggregate_type": self.selected_aggregate_type,
            "payload_has_candidate_group_id": self.payload_has_candidate_group_id,
            "payload_has_bundle_id": self.payload_has_bundle_id,
            "payload_has_judge_profile": self.payload_has_judge_profile,
            "payload_has_escalation_allowed": self.payload_has_escalation_allowed,
            "payload_candidate_group_id_matches": self.payload_candidate_group_id_matches,
            "payload_judge_profile_allowed": self.payload_judge_profile_allowed,
            "redactions_applied": {
                "full_event_id_omitted": True,
                "full_candidate_group_id_omitted": True,
                "full_bundle_id_omitted": True,
                "idempotency_key_omitted": True,
                "payload_json_omitted": True,
                "bundle_data_omitted": True,
                "judge_profile_value_omitted": True,
                "raw_text_omitted": True,
                "prompt_material_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "redis_message_id_omitted": True,
                "exception_detail_omitted": True,
            },
            "side_effects": {
                "redis_mutation": self.redis_published_count > 0,
                "db_write": self.state.database_write_attempted,
                "queue_consume_called": False,
                "analysis_router_called": False,
                "judge_run_created": False,
                "judge_call_requested_event_emitted": False,
                "evidence_assembler_called": False,
                "judge_called": False,
                "policy_called": False,
                "notifier_called": False,
                "telegram_read_called": False,
                "telegram_send_called": False,
                "openai_called": False,
                "github_api_called": False,
                "x_api_called": False,
                "web_fetch_called": False,
                "normalizer_called": False,
                "enricher_called": False,
                "worker_started": False,
                "run_forever_called": False,
                "systemd_called": False,
                "docker_called": False,
                "alembic_called": False,
                "subprocess_called": False,
            },
        }


class SqlAlchemyBoundedAnalysisRequestedOutboxRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session
        self._relay_repository = OutboxRelayRepository(session)

    async def fetch_target_events(
        self,
        *,
        event_id: UUID | None,
        event_suffix: str | None,
        limit: int,
    ) -> list[OutboxEventRow]:
        if event_id is not None:
            statement = _SELECT_TARGET_EVENT + """
                WHERE event_id = CAST(:event_id AS uuid)
                ORDER BY created_at ASC, event_id ASC
                LIMIT :limit
                """
            params: dict[str, Any] = {"event_id": str(event_id), "limit": limit}
        elif event_suffix is not None:
            statement = _SELECT_TARGET_EVENT + """
                WHERE lower(CAST(event_id AS text)) LIKE :event_suffix_pattern
                ORDER BY created_at ASC, event_id ASC
                LIMIT :limit
                """
            params = {"event_suffix_pattern": f"%{event_suffix.lower()}", "limit": limit}
        else:  # pragma: no cover - guarded before repository calls
            raise BoundedAnalysisRequestedOutboxPublishError("target_missing")

        result = await self._session.execute(_sql(statement), params)
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


def load_bounded_analysis_requested_publish_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedAnalysisRequestedPublishRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedAnalysisRequestedOutboxPublishError("database_url_missing")
    if not redis_url:
        raise BoundedAnalysisRequestedOutboxPublishError("redis_url_missing")
    xadd_maxlen_raw = _env_value(source, "OUTBOX_RELAY_XADD_MAXLEN", str(DEFAULT_XADD_MAXLEN))
    try:
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
    except ValueError as exc:
        raise BoundedAnalysisRequestedOutboxPublishError("runtime_config_error") from exc
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        raise BoundedAnalysisRequestedOutboxPublishError("runtime_config_error")
    return BoundedAnalysisRequestedPublishRuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        xadd_maxlen=xadd_maxlen,
    )


async def build_default_bounded_analysis_requested_repository(
    runtime_config: BoundedAnalysisRequestedPublishRuntimeConfig,
    state: BoundedAnalysisRequestedOutboxPublishState,
    logger: logging.Logger,
) -> BoundedAnalysisRequestedRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session_context = session_factory.begin()
    session = await session_context.__aenter__()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedAnalysisRequestedOutboxRepository(session)

    async def close(commit: bool) -> None:
        if not commit:
            await session.rollback()
        await session_context.__aexit__(None, None, None)
        await engine.dispose()

    return BoundedAnalysisRequestedRepositoryHandle(repository=repository, close=close)


async def build_default_bounded_analysis_requested_redis_publisher(
    runtime_config: BoundedAnalysisRequestedPublishRuntimeConfig,
    state: BoundedAnalysisRequestedOutboxPublishState,
    logger: logging.Logger,
) -> BoundedAnalysisRequestedRedisPublisherHandle:
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

    return BoundedAnalysisRequestedRedisPublisherHandle(publisher=publisher, close=close)


async def run_bounded_analysis_requested_outbox_publish(
    config: BoundedAnalysisRequestedOutboxPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedAnalysisRequestedPublishRuntimeConfig] = (
        load_bounded_analysis_requested_publish_runtime_config
    ),
    repository_builder: BoundedAnalysisRequestedRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedAnalysisRequestedRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedAnalysisRequestedOutboxPublishResult:
    state = BoundedAnalysisRequestedOutboxPublishState()
    if not config.operator_approved:
        return _result("blocked", "operator_approval_missing", config=config, state=state)
    if _selector_count(config) != 1:
        error_code = "target_missing" if _selector_count(config) == 0 else "target_conflict"
        return _result("blocked", error_code, config=config, state=state)
    if config.event_suffix is not None and not _is_valid_event_suffix(config.event_suffix):
        return _result("blocked", "invalid_event_suffix", config=config, state=state)
    if config.max_events != HARD_MAX_EVENTS:
        return _result("blocked", "max_events_must_be_one", config=config, state=state)
    if not config.allow_runtime_config:
        return _result("blocked", "runtime_config_not_allowed", config=config, state=state)
    if not config.allow_database_write:
        return _result("blocked", "database_write_not_allowed", config=config, state=state)
    if not config.allow_redis_publish:
        return _result("blocked", "redis_publish_not_allowed", config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedAnalysisRequestedOutboxPublishError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception:
        return _result("blocked", "runtime_config_error", config=config, state=state)

    repository_handle: BoundedAnalysisRequestedRepositoryHandle | None = None
    publisher_handle: BoundedAnalysisRequestedRedisPublisherHandle | None = None
    commit_repository = False
    result: BoundedAnalysisRequestedOutboxPublishResult | None = None
    try:
        repository_handle = await (repository_builder or build_default_bounded_analysis_requested_repository)(
            runtime_config,
            state,
            effective_logger,
        )
        repository = repository_handle.repository
        state.database_read_attempted = True
        rows = await repository.fetch_target_events(
            event_id=config.event_id,
            event_suffix=config.event_suffix,
            limit=HARD_MAX_EVENTS + 1,
        )
        events_seen = len(rows)
        if events_seen == 0:
            result = _result("blocked", "target_event_not_found", config=config, state=state)
            raise _PublishResultReady
        if events_seen > HARD_MAX_EVENTS:
            result = _result(
                "blocked",
                "target_event_count_exceeded",
                config=config,
                state=state,
                events_seen=events_seen,
            )
            raise _PublishResultReady

        row = rows[0]
        payload_flags = _payload_presence_flags(row.payload_json)
        candidate_group_id_matches = _payload_candidate_group_id_matches(row)
        judge_profile_allowed = _payload_judge_profile_allowed(row.payload_json)
        if row.status != "pending":
            result = _result(
                "blocked",
                "target_event_not_pending",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                candidate_group_id_matches=candidate_group_id_matches,
                judge_profile_allowed=judge_profile_allowed,
            )
            raise _PublishResultReady
        if row.event_type != EVENT_TYPE or row.aggregate_type != ROOT_OBJECT_TYPE:
            result = _result(
                "blocked",
                "analysis_requested_event_contract_mismatch",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                candidate_group_id_matches=candidate_group_id_matches,
                judge_profile_allowed=judge_profile_allowed,
            )
            raise _PublishResultReady
        if not all(payload_flags.values()):
            result = _result(
                "blocked",
                "malformed_event_payload",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                candidate_group_id_matches=candidate_group_id_matches,
                judge_profile_allowed=judge_profile_allowed,
            )
            raise _PublishResultReady
        if not candidate_group_id_matches:
            result = _result(
                "blocked",
                "candidate_group_id_mismatch",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                candidate_group_id_matches=False,
                judge_profile_allowed=judge_profile_allowed,
            )
            raise _PublishResultReady
        if not judge_profile_allowed:
            result = _result(
                "blocked",
                "unknown_judge_profile",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                candidate_group_id_matches=True,
                judge_profile_allowed=False,
            )
            raise _PublishResultReady

        try:
            canonical_route = OutboxRouteResolver().resolve(row)
            route = route_resolver.resolve(row) if route_resolver is not None else canonical_route
        except UnsupportedOutboxEventTypeError:
            result = _result(
                "blocked",
                "unsupported_event_type",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                candidate_group_id_matches=True,
                judge_profile_allowed=True,
            )
            raise _PublishResultReady
        if route != canonical_route or route.queue_name != QUEUE_NAME or route.stage_name != STAGE_NAME:
            result = _result(
                "blocked",
                "route_not_allowed",
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                candidate_group_id_matches=True,
                judge_profile_allowed=True,
            )
            raise _PublishResultReady

        publisher_handle = await (redis_publisher_builder or build_default_bounded_analysis_requested_redis_publisher)(
            runtime_config,
            state,
            effective_logger,
        )
        message = _build_stream_message(row, route)
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
                selected_event=row,
                events_seen=events_seen,
                payload_flags=payload_flags,
                candidate_group_id_matches=True,
                judge_profile_allowed=True,
                queue_name=route.queue_name,
                stage_name=route.stage_name,
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
                "database_write_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected_event=row,
                events_seen=events_seen,
                redis_published_count=1,
                event_outbox_status_updated_count=event_outbox_status_updated_count,
                job_attempts_written_count=job_attempts_written_count,
                payload_flags=payload_flags,
                candidate_group_id_matches=True,
                judge_profile_allowed=True,
                queue_name=route.queue_name,
                stage_name=route.stage_name,
            )
            raise _PublishResultReady

        commit_repository = True
        result = _result(
            "published",
            None,
            config=config,
            state=state,
            selected_event=row,
            events_seen=events_seen,
            redis_published_count=1 if redis_message_id else 1,
            event_outbox_status_updated_count=1,
            job_attempts_written_count=1,
            payload_flags=payload_flags,
            candidate_group_id_matches=True,
            judge_profile_allowed=True,
            queue_name=route.queue_name,
            stage_name=route.stage_name,
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


def run_bounded_analysis_requested_outbox_publish_sync(
    config: BoundedAnalysisRequestedOutboxPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedAnalysisRequestedPublishRuntimeConfig] = (
        load_bounded_analysis_requested_publish_runtime_config
    ),
    repository_builder: BoundedAnalysisRequestedRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedAnalysisRequestedRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedAnalysisRequestedOutboxPublishResult:
    return asyncio.run(
        run_bounded_analysis_requested_outbox_publish(
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
        config=BoundedAnalysisRequestedOutboxPublishConfig(),
        state=BoundedAnalysisRequestedOutboxPublishState(),
    ).to_sanitized_dict()


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedAnalysisRequestedOutboxPublishConfig,
    state: BoundedAnalysisRequestedOutboxPublishState,
    error_class: str | None = None,
    selected_event: OutboxEventRow | None = None,
    events_seen: int = 0,
    redis_published_count: int = 0,
    event_outbox_status_updated_count: int = 0,
    job_attempts_written_count: int = 0,
    payload_flags: Mapping[str, bool] | None = None,
    candidate_group_id_matches: bool = False,
    judge_profile_allowed: bool = False,
    queue_name: str | None = None,
    stage_name: str | None = None,
) -> BoundedAnalysisRequestedOutboxPublishResult:
    flags = dict(payload_flags or {})
    selected_event_id = selected_event.event_id if selected_event is not None else config.event_id
    selected_candidate_group_id = (
        selected_event.aggregate_id
        if selected_event is not None and selected_event.aggregate_type == ROOT_OBJECT_TYPE
        else None
    )
    return BoundedAnalysisRequestedOutboxPublishResult(
        status=status,
        ok=status == "published" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        selector_type=_selector_type(config),
        target_event_id_suffix=_optional_id_suffix(selected_event_id),
        target_candidate_group_suffix=_optional_id_suffix(selected_candidate_group_id),
        events_seen=events_seen,
        redis_published_count=redis_published_count,
        event_outbox_status_updated_count=event_outbox_status_updated_count,
        job_attempts_written_count=job_attempts_written_count,
        queue_name=queue_name,
        stage_name=stage_name,
        selected_event_status=selected_event.status if selected_event is not None else None,
        selected_event_type=selected_event.event_type if selected_event is not None else None,
        selected_aggregate_type=selected_event.aggregate_type if selected_event is not None else None,
        payload_has_candidate_group_id=flags.get("candidate_group_id", False),
        payload_has_bundle_id=flags.get("bundle_id", False),
        payload_has_judge_profile=flags.get("judge_profile", False),
        payload_has_escalation_allowed=flags.get("escalation_allowed", False),
        payload_candidate_group_id_matches=candidate_group_id_matches,
        payload_judge_profile_allowed=judge_profile_allowed,
    )


def _build_stream_message(row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
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


def _payload_presence_flags(payload: Mapping[str, Any]) -> dict[str, bool]:
    return {field_name: _payload_field_present(payload.get(field_name)) for field_name in REQUIRED_PAYLOAD_FIELDS}


def _payload_field_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _payload_candidate_group_id_matches(row: OutboxEventRow) -> bool:
    try:
        return UUID(str(row.payload_json.get("candidate_group_id"))) == row.aggregate_id
    except (TypeError, ValueError):
        return False


def _payload_judge_profile_allowed(payload: Mapping[str, Any]) -> bool:
    value = payload.get("judge_profile")
    if not isinstance(value, str):
        return False
    return value.strip() in ALLOWED_JUDGE_PROFILES


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


def _selector_count(config: BoundedAnalysisRequestedOutboxPublishConfig) -> int:
    return sum(value is not None for value in (config.event_id, config.event_suffix))


def _selector_type(config: BoundedAnalysisRequestedOutboxPublishConfig) -> str | None:
    if config.event_id is not None:
        return "event_id"
    if config.event_suffix is not None:
        return "event_suffix"
    return None


def _is_valid_event_suffix(value: str) -> bool:
    stripped = value.strip().lower()
    return 4 <= len(stripped) <= 36 and all(char in "0123456789abcdef-" for char in stripped)


def _optional_id_suffix(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    return str(value)[-8:]


def _repository_close_error_code(commit: bool) -> str:
    return "repository_commit_failed" if commit else "repository_rollback_failed"


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _env_value(env: Mapping[str, str], name: str, default: str = "") -> str:
    return str(env.get(name, default) or "").strip()


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)


_SELECT_TARGET_EVENT = """
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
"""


__all__ = [
    "ALLOWED_JUDGE_PROFILES",
    "BoundedAnalysisRequestedOutboxPublishConfig",
    "BoundedAnalysisRequestedOutboxPublishError",
    "BoundedAnalysisRequestedOutboxPublishResult",
    "BoundedAnalysisRequestedPublishRuntimeConfig",
    "BoundedAnalysisRequestedRedisPublisherBuilder",
    "BoundedAnalysisRequestedRedisPublisherHandle",
    "BoundedAnalysisRequestedRepositoryBuilder",
    "BoundedAnalysisRequestedRepositoryHandle",
    "DEFAULT_MAX_EVENTS",
    "EVENT_TYPE",
    "HARD_MAX_EVENTS",
    "MODE",
    "QUEUE_NAME",
    "REQUIRED_PAYLOAD_FIELDS",
    "ROOT_OBJECT_TYPE",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "STAGE_NAME",
    "SqlAlchemyBoundedAnalysisRequestedOutboxRepository",
    "argument_error_report",
    "build_default_bounded_analysis_requested_redis_publisher",
    "build_default_bounded_analysis_requested_repository",
    "load_bounded_analysis_requested_publish_runtime_config",
    "render_sanitized_json",
    "run_bounded_analysis_requested_outbox_publish",
    "run_bounded_analysis_requested_outbox_publish_sync",
]
