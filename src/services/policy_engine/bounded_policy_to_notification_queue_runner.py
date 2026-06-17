from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from json import JSONDecodeError
from typing import Any, Protocol
from uuid import UUID

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - static validation fallback
    sa = None

from ..outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from ..outbox_relay.redis_streams import RedisStreamsPublisher
from ..outbox_relay.repositories import OutboxRelayRepository
from ..outbox_relay.routing import OutboxRouteResolver
from .bounded_policy_apply_runner import (
    BoundedPolicyApplyConfig,
    BoundedPolicyApplyError,
    BoundedPolicyApplyResult,
    BoundedPolicyApplyRuntimeConfig,
    PolicyApplyCommitContext,
    RedisBuilder,
    RepositoryBuilder,
    load_runtime_config,
    run_bounded_policy_apply,
)


SCHEMA_VERSION = "bounded_policy_to_notification_queue_runner_v1"
RUNNER_NAME = "bounded_policy_to_notification_queue_runner"
MODE_PREVIEW = "preview"
MODE_EXECUTE = "execute"
NOTIFICATION_EVENT_TYPE = "notification.plan.created.v1"
QUEUE_NAME = "q.notification.send"
STAGE_NAME = "notify"
THIN_REDIS_FIELDS = frozenset(
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
REQUIRED_NOTIFICATION_PAYLOAD_FIELDS = frozenset(
    {
        "notification_plan_id",
        "analysis_id",
        "candidate_group_id",
        "delivery_decision",
        "urgency_profile",
        "render_profile",
        "dedupe_subject_key",
        "material_change_hash",
        "target_chat_id",
        "target_thread_id",
        "send_after",
        "suppress_reason_code",
    }
)
FORBIDDEN_NOTIFICATION_PAYLOAD_FIELDS = frozenset(
    {
        "message_text",
        "payload_json",
        "scores",
        "judge_output",
        "raw_text",
        "database_url",
        "redis_url",
        "entities_json",
        "reply_markup_json",
        "render_hash",
    }
)
FORBIDDEN_REDIS_FIELDS = frozenset(
    {
        "message_text",
        "payload_json",
        "scores",
        "judge_output",
        "target_chat_id",
        "raw_text",
        "database_url",
        "redis_url",
        "notification_plan_id",
        "analysis_id",
        "candidate_group_id",
        "material_change_hash",
    }
)


@dataclass(frozen=True, slots=True)
class BoundedPolicyToNotificationQueueConfig:
    mode: str = MODE_PREVIEW
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_read: bool = False
    allow_database_read: bool = False
    allow_redis_consume: bool = False
    allow_database_write: bool = False
    allow_redis_ack: bool = False
    allow_notification_outbox_publish: bool = False
    allow_notification_redis_publish: bool = False
    allow_redis_group_create: bool = False
    trigger_event_suffix: str | None = None
    judge_run_suffix: str | None = None
    judge_output_suffix: str | None = None
    notification_plan_event_suffix: str | None = None
    scan_limit: int = 25


@dataclass(slots=True)
class NotificationQueuePublishState:
    notification_repository_opened: bool = False
    notification_outbox_read_attempted: bool = False
    redis_publisher_created: bool = False
    redis_publish_attempted: bool = False
    notification_outbox_status_update_attempted: bool = False
    notification_job_attempt_insert_attempted: bool = False
    notification_outbox_commit_attempted: bool = False


class NotificationQueuePublishRepository(Protocol):
    async def load_event(self, event_id: UUID) -> OutboxEventRow | None: ...
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


class NotificationRedisPublisher(Protocol):
    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str: ...


@dataclass(frozen=True, slots=True)
class NotificationQueuePublishRepositoryHandle:
    repository: NotificationQueuePublishRepository
    close: Callable[[bool], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class NotificationQueueRedisPublisherHandle:
    publisher: NotificationRedisPublisher
    close: Callable[[], Awaitable[None]]


class NotificationQueuePublishRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedPolicyApplyRuntimeConfig,
        state: NotificationQueuePublishState,
        logger: logging.Logger,
    ) -> NotificationQueuePublishRepositoryHandle: ...


class NotificationQueueRedisPublisherBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedPolicyApplyRuntimeConfig,
        state: NotificationQueuePublishState,
        logger: logging.Logger,
    ) -> NotificationQueueRedisPublisherHandle: ...


@dataclass(frozen=True, slots=True)
class NotificationQueuePublishResult:
    status: str
    ok: bool
    error_code: str | None
    event_id_suffix: str | None = None
    notification_plan_id_suffix: str | None = None
    aggregate_id_suffix: str | None = None
    selected_event_present: bool = False
    selected_event_status: str | None = None
    redis_message_id_suffix: str | None = None
    redis_xadd_count: int = 0
    event_outbox_marked_published: bool = False
    job_attempt_inserted: bool = False
    state: NotificationQueuePublishState = field(default_factory=NotificationQueuePublishState)


@dataclass(frozen=True, slots=True)
class BoundedPolicyToNotificationQueueResult:
    status: str
    ok: bool
    error_code: str | None
    config: BoundedPolicyToNotificationQueueConfig
    policy_result: BoundedPolicyApplyResult | None = None
    publish_result: NotificationQueuePublishResult | None = None
    planned_action: Mapping[str, str | bool | None] = field(default_factory=dict)

    def to_sanitized_dict(self) -> dict[str, Any]:
        policy = self.policy_result.to_sanitized_dict() if self.policy_result is not None else {}
        publish = self.publish_result
        policy_suffixes = {
            "redis_message": policy.get("target_redis_message_id_suffix"),
            "trigger_event": policy.get("target_policy_apply_event_suffix"),
            "judge_run": policy.get("target_judge_run_id_suffix"),
            "judge_output": policy.get("target_judge_output_id_suffix"),
            "bundle": policy.get("target_bundle_id_suffix"),
            "candidate_group": policy.get("target_candidate_group_suffix"),
        }
        notification_plan_event_suffix = (
            publish.event_id_suffix
            if publish is not None and publish.event_id_suffix is not None
            else policy.get("target_notification_plan_event_suffix")
        )
        notification_plan_id_suffix = (
            publish.notification_plan_id_suffix
            if publish is not None and publish.notification_plan_id_suffix is not None
            else policy.get("target_notification_plan_id_suffix")
        )
        policy_side_effects = policy.get("side_effects") if isinstance(policy.get("side_effects"), dict) else {}
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": self.config.mode,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "policy_apply_status": policy.get("status"),
            "policy_target_suffixes": policy_suffixes,
            "analysis_id_suffix": policy.get("target_analysis_id_suffix"),
            "verdict": policy.get("verdict"),
            "delivery_decision": policy.get("delivery_decision"),
            "urgency_profile": policy.get("urgency_profile"),
            "notification_plan_event_suffix": notification_plan_event_suffix,
            "notification_plan_id_suffix": notification_plan_id_suffix,
            "notification_outbox_found": notification_plan_event_suffix is not None,
            "notification_outbox_written": bool(policy.get("notification_plan_intent_outbox_written")),
            "notification_outbox_published": bool(publish and publish.event_outbox_marked_published),
            "q_notification_send_published": bool(publish and publish.redis_xadd_count == 1),
            "q_notification_send_message_suffix": publish.redis_message_id_suffix if publish is not None else None,
            "planned_action": dict(self.planned_action),
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "redis_read_allowed": self.config.allow_redis_read,
                "database_read_allowed": self.config.allow_database_read,
                "redis_consume_allowed": self.config.allow_redis_consume,
                "database_write_allowed": self.config.allow_database_write,
                "redis_ack_allowed": self.config.allow_redis_ack,
                "notification_outbox_publish_allowed": self.config.allow_notification_outbox_publish,
                "notification_redis_publish_allowed": self.config.allow_notification_redis_publish,
                "redis_group_create_allowed": self.config.allow_redis_group_create,
                "scan_limit": self.config.scan_limit,
            },
            "side_effects": {
                "policy_redis_read_called": bool(policy_side_effects.get("redis_read_called")),
                "policy_redis_consume_called": bool(policy_side_effects.get("redis_consume_called")),
                "policy_redis_ack_called": bool(policy_side_effects.get("redis_ack_called")),
                "policy_db_read": bool(policy_side_effects.get("db_read")),
                "policy_db_write": bool(policy_side_effects.get("db_write")),
                "policy_db_commit": bool(policy_side_effects.get("db_commit")),
                "notification_outbox_read": bool(
                    publish and publish.state.notification_outbox_read_attempted
                ),
                "notification_redis_publish_called": bool(publish and publish.state.redis_publish_attempted),
                "notification_outbox_status_update_called": bool(
                    publish and publish.state.notification_outbox_status_update_attempted
                ),
                "notification_outbox_commit": bool(
                    publish and publish.state.notification_outbox_commit_attempted
                ),
                "notification_plans_table_written": False,
                "notification_renders_written": False,
                "notification_delivery_records_written": False,
                "telegram_send_called": False,
                "telegram_edit_called": False,
                "notifier_called": False,
                "openai_called": False,
                "github_api_called": False,
                "x_api_called": False,
                "web_fetch_called": False,
                "worker_started": False,
                "run_forever_called": False,
                "systemd_called": False,
                "docker_called": False,
                "alembic_called": False,
            },
            "redactions_applied": {
                "full_redis_ids_omitted": True,
                "full_uuids_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "idempotency_key_omitted": True,
                "target_chat_id_omitted": True,
                "raw_source_text_omitted": True,
                "judge_output_payload_omitted": True,
                "rendered_message_text_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class SqlAlchemyNotificationQueuePublishRepository:
    def __init__(self, session: Any) -> None:
        self._session = session
        self._relay_repository = OutboxRelayRepository(session)

    async def load_event(self, event_id: UUID) -> OutboxEventRow | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       dedupe_key, payload_json, status, fail_count, created_at
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(event_id)},
        )
        row = result.mappings().first()
        return _outbox_row_from_mapping(row) if row is not None else None

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


async def build_default_notification_queue_publish_repository(
    runtime_config: BoundedPolicyApplyRuntimeConfig,
    state: NotificationQueuePublishState,
    logger: logging.Logger,
) -> NotificationQueuePublishRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.notification_repository_opened = True
    repository = SqlAlchemyNotificationQueuePublishRepository(session)

    async def close(commit: bool) -> None:
        try:
            if commit:
                state.notification_outbox_commit_attempted = True
                await session.commit()
            else:
                await session.rollback()
        finally:
            await session.close()
            await engine.dispose()

    return NotificationQueuePublishRepositoryHandle(repository=repository, close=close)


async def build_default_notification_queue_redis_publisher(
    runtime_config: BoundedPolicyApplyRuntimeConfig,
    state: NotificationQueuePublishState,
    logger: logging.Logger,
) -> NotificationQueueRedisPublisherHandle:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    state.redis_publisher_created = True
    publisher = RedisStreamsPublisher(redis_client)

    async def close() -> None:
        close_client = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close_client is None:
            return
        result = close_client()
        if hasattr(result, "__await__"):
            await result

    return NotificationQueueRedisPublisherHandle(publisher=publisher, close=close)


async def run_bounded_policy_to_notification_queue(
    config: BoundedPolicyToNotificationQueueConfig,
    *,
    runtime_config_loader: Callable[[], BoundedPolicyApplyRuntimeConfig] = load_runtime_config,
    policy_redis_builder: RedisBuilder | None = None,
    policy_repository_builder: RepositoryBuilder | None = None,
    notification_repository_builder: NotificationQueuePublishRepositoryBuilder | None = None,
    notification_redis_publisher_builder: NotificationQueueRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedPolicyToNotificationQueueResult:
    gate_error = _gate_error(config)
    if gate_error is not None:
        return _macro_result("blocked", gate_error, config=config)

    effective_logger = logger or logging.getLogger(__name__)
    publish_result: NotificationQueuePublishResult | None = None

    async def after_policy_commit(context: PolicyApplyCommitContext) -> None:
        nonlocal publish_result
        if context.analysis is None or context.analysis.delivery_decision == "suppress":
            return
        if context.notification_outbox is None:
            publish_result = _notification_publish_result(
                "failed",
                "notification_plan_intent_missing_after_policy_apply",
            )
            raise BoundedPolicyApplyError("notification_plan_intent_missing_after_policy_apply")
        publish_result = await _publish_exact_notification_outbox(
            context.notification_outbox.event_id,
            config=config,
            runtime_config=context.runtime_config,
            notification_repository_builder=notification_repository_builder,
            notification_redis_publisher_builder=notification_redis_publisher_builder,
            route_resolver=route_resolver,
            clock=clock,
            logger=effective_logger,
        )
        if not publish_result.ok:
            raise BoundedPolicyApplyError(publish_result.error_code or "notification_outbox_publish_failed")

    policy_result = await run_bounded_policy_apply(
        BoundedPolicyApplyConfig(
            mode=config.mode,
            operator_approved=config.operator_approved,
            allow_runtime_config=config.allow_runtime_config,
            allow_redis_read=config.allow_redis_read,
            allow_redis_group_create=config.allow_redis_group_create,
            allow_database_read=config.allow_database_read,
            allow_redis_consume=config.allow_redis_consume,
            allow_database_write=config.allow_database_write,
            allow_redis_ack=config.allow_redis_ack,
            trigger_event_suffix=config.trigger_event_suffix,
            judge_run_suffix=config.judge_run_suffix,
            judge_output_suffix=config.judge_output_suffix,
            scan_limit=config.scan_limit,
        ),
        runtime_config_loader=runtime_config_loader,
        redis_builder=policy_redis_builder,
        repository_builder=policy_repository_builder,
        after_commit_before_ack=after_policy_commit if config.mode == MODE_EXECUTE else None,
        logger=effective_logger,
    )
    planned_action = _planned_action(policy_result=policy_result, publish_result=publish_result, mode=config.mode)
    if not policy_result.ok:
        if publish_result is not None:
            return _macro_result(
                "notification_queue_handoff_failed",
                publish_result.error_code or policy_result.error_code,
                config=config,
                policy_result=policy_result,
                publish_result=publish_result,
                planned_action=planned_action,
            )
        return _macro_result(
            "policy_apply_failed",
            policy_result.error_code,
            config=config,
            policy_result=policy_result,
            planned_action=planned_action,
        )

    if policy_result.delivery_decision == "suppress":
        return _macro_result(
            "policy_suppressed_no_notification",
            None,
            config=config,
            policy_result=policy_result,
            publish_result=publish_result,
            planned_action=planned_action,
        )

    if config.mode == MODE_PREVIEW:
        return _macro_result(
            "preview_non_suppress_would_publish_notification_queue",
            None,
            config=config,
            policy_result=policy_result,
            planned_action=planned_action,
        )

    if publish_result is not None and publish_result.ok:
        return _macro_result(
            "notification_queue_handoff_published",
            None,
            config=config,
            policy_result=policy_result,
            publish_result=publish_result,
            planned_action=planned_action,
        )
    return _macro_result(
        "notification_queue_handoff_failed",
        "notification_outbox_publish_missing",
        config=config,
        policy_result=policy_result,
        publish_result=publish_result,
        planned_action=planned_action,
    )


def run_bounded_policy_to_notification_queue_sync(
    config: BoundedPolicyToNotificationQueueConfig,
    *,
    runtime_config_loader: Callable[[], BoundedPolicyApplyRuntimeConfig] = load_runtime_config,
    policy_redis_builder: RedisBuilder | None = None,
    policy_repository_builder: RepositoryBuilder | None = None,
    notification_repository_builder: NotificationQueuePublishRepositoryBuilder | None = None,
    notification_redis_publisher_builder: NotificationQueueRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedPolicyToNotificationQueueResult:
    return asyncio.run(
        run_bounded_policy_to_notification_queue(
            config,
            runtime_config_loader=runtime_config_loader,
            policy_redis_builder=policy_redis_builder,
            policy_repository_builder=policy_repository_builder,
            notification_repository_builder=notification_repository_builder,
            notification_redis_publisher_builder=notification_redis_publisher_builder,
            route_resolver=route_resolver,
            clock=clock,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return _macro_result("blocked", error_code, config=BoundedPolicyToNotificationQueueConfig()).to_sanitized_dict()


async def _publish_exact_notification_outbox(
    event_id: UUID,
    *,
    config: BoundedPolicyToNotificationQueueConfig,
    runtime_config: BoundedPolicyApplyRuntimeConfig,
    notification_repository_builder: NotificationQueuePublishRepositoryBuilder | None,
    notification_redis_publisher_builder: NotificationQueueRedisPublisherBuilder | None,
    route_resolver: OutboxRouteResolver | None,
    clock: Callable[[], datetime] | None,
    logger: logging.Logger,
) -> NotificationQueuePublishResult:
    state = NotificationQueuePublishState()
    repository_handle: NotificationQueuePublishRepositoryHandle | None = None
    publisher_handle: NotificationQueueRedisPublisherHandle | None = None
    commit_repository = False
    selected_event: OutboxEventRow | None = None
    redis_message_id: str | None = None
    try:
        repository_handle = await (
            notification_repository_builder or build_default_notification_queue_publish_repository
        )(runtime_config, state, logger)
        state.notification_outbox_read_attempted = True
        selected_event = await repository_handle.repository.load_event(event_id)
        event_error = _notification_event_error(selected_event, config=config)
        if event_error is not None:
            return _notification_publish_result(event_error[0], event_error[1], state=state, row=selected_event)
        assert selected_event is not None
        route = (route_resolver or OutboxRouteResolver()).resolve(selected_event)
        if route.queue_name != QUEUE_NAME or route.stage_name != STAGE_NAME:
            return _notification_publish_result("failed", "notification_route_not_allowed", state=state, row=selected_event)
        message = _build_stream_message(selected_event, route)
        message_error = _thin_message_error(message)
        if message_error is not None:
            return _notification_publish_result("failed", message_error, state=state, row=selected_event)
        publisher_handle = await (
            notification_redis_publisher_builder or build_default_notification_queue_redis_publisher
        )(runtime_config, state, logger)
        try:
            state.redis_publish_attempted = True
            redis_message_id = await publisher_handle.publisher.publish(route, message)
        except Exception:
            return _notification_publish_result("failed", "notification_redis_xadd_failed", state=state, row=selected_event)
        try:
            state.notification_outbox_status_update_attempted = True
            await repository_handle.repository.mark_published(
                event_id=selected_event.event_id,
                published_at=(clock or _utc_now)(),
            )
            state.notification_job_attempt_insert_attempted = True
            await repository_handle.repository.insert_job_attempt(
                stage_name=route.stage_name,
                queue_name=route.queue_name,
                root_object_type=selected_event.aggregate_type,
                root_object_id=selected_event.aggregate_id,
                attempt_status="succeeded",
                error_code=None,
            )
        except Exception:
            return _notification_publish_result(
                "failed",
                "notification_outbox_status_update_failed",
                state=state,
                row=selected_event,
                redis_message_id=redis_message_id,
                redis_xadd_count=1,
            )
        commit_repository = True
        try:
            await repository_handle.close(True)
            repository_handle = None
        except Exception:
            return _notification_publish_result(
                "failed",
                "notification_outbox_commit_failed",
                state=state,
                row=selected_event,
                redis_message_id=redis_message_id,
                redis_xadd_count=1,
            )
        return _notification_publish_result(
            "published",
            None,
            state=state,
            row=selected_event,
            redis_message_id=redis_message_id,
            redis_xadd_count=1,
            event_outbox_marked_published=True,
            job_attempt_inserted=True,
        )
    except Exception:
        return _notification_publish_result("failed", "notification_outbox_publish_failed", state=state, row=selected_event)
    finally:
        if publisher_handle is not None:
            try:
                await publisher_handle.close()
            except Exception:
                pass
        if repository_handle is not None:
            try:
                await repository_handle.close(commit_repository)
            except Exception:
                pass


def _gate_error(config: BoundedPolicyToNotificationQueueConfig) -> str | None:
    if config.mode not in {MODE_PREVIEW, MODE_EXECUTE}:
        return "invalid_mode"
    if not config.operator_approved:
        return "operator_approval_missing"
    if not 1 <= config.scan_limit <= 100:
        return "invalid_scan_limit"
    if not config.trigger_event_suffix:
        return "target_missing"
    if not (config.judge_run_suffix or config.judge_output_suffix):
        return "judge_selector_missing"
    for value, error_code in (
        (config.trigger_event_suffix, "invalid_trigger_event_suffix"),
        (config.judge_run_suffix, "invalid_judge_run_suffix"),
        (config.judge_output_suffix, "invalid_judge_output_suffix"),
        (config.notification_plan_event_suffix, "invalid_notification_plan_event_suffix"),
    ):
        if value is not None and not _valid_uuid_suffix(value):
            return error_code
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_redis_read:
        return "redis_read_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if config.mode == MODE_EXECUTE:
        if not config.allow_redis_consume:
            return "redis_consume_not_allowed"
        if not config.allow_database_write:
            return "database_write_not_allowed"
        if not config.allow_notification_outbox_publish:
            return "notification_outbox_publish_not_allowed"
        if not config.allow_notification_redis_publish:
            return "notification_redis_publish_not_allowed"
        if not config.allow_redis_ack:
            return "redis_ack_not_allowed"
    return None


def _macro_result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedPolicyToNotificationQueueConfig,
    policy_result: BoundedPolicyApplyResult | None = None,
    publish_result: NotificationQueuePublishResult | None = None,
    planned_action: Mapping[str, str | bool | None] | None = None,
) -> BoundedPolicyToNotificationQueueResult:
    return BoundedPolicyToNotificationQueueResult(
        status=status,
        ok=error_code is None
        and status
        in {
            "policy_suppressed_no_notification",
            "preview_non_suppress_would_publish_notification_queue",
            "notification_queue_handoff_published",
        },
        error_code=error_code,
        config=config,
        policy_result=policy_result,
        publish_result=publish_result,
        planned_action=planned_action or {},
    )


def _planned_action(
    *,
    policy_result: BoundedPolicyApplyResult,
    publish_result: NotificationQueuePublishResult | None,
    mode: str,
) -> dict[str, str | bool | None]:
    non_suppress = policy_result.delivery_decision is not None and policy_result.delivery_decision != "suppress"
    return {
        "policy_action": policy_result.planned_action,
        "expected_analysis_action": _analysis_action(policy_result),
        "expected_notification_intent_action": _notification_intent_action(policy_result),
        "expected_outbox_relay_action": _outbox_relay_action(mode=mode, non_suppress=non_suppress, publish_result=publish_result),
        "q_notification_send_would_receive_thin_message": non_suppress,
    }


def _analysis_action(policy_result: BoundedPolicyApplyResult) -> str:
    if policy_result.analysis_written:
        return "create_analysis"
    if policy_result.existing_analysis_found:
        return "reuse_analysis"
    if policy_result.planned_action and "create_analysis" in policy_result.planned_action:
        return "would_create_analysis"
    return "none"


def _notification_intent_action(policy_result: BoundedPolicyApplyResult) -> str:
    if policy_result.delivery_decision == "suppress":
        return "none_policy_suppressed"
    if policy_result.notification_plan_intent_outbox_written:
        return "create_notification_plan_intent"
    if policy_result.target_notification_plan_event_suffix:
        return "reuse_notification_plan_intent"
    if policy_result.planned_action and "notification_intent" in policy_result.planned_action:
        return "would_create_notification_plan_intent"
    return "none"


def _outbox_relay_action(
    *,
    mode: str,
    non_suppress: bool,
    publish_result: NotificationQueuePublishResult | None,
) -> str:
    if not non_suppress:
        return "none_policy_suppressed"
    if mode == MODE_PREVIEW:
        return "would_publish_exact_notification_plan_event_to_q_notification_send"
    if publish_result is not None and publish_result.ok:
        return "published_exact_notification_plan_event_to_q_notification_send"
    return "failed_or_not_attempted"


def _notification_event_error(
    row: OutboxEventRow | None,
    *,
    config: BoundedPolicyToNotificationQueueConfig,
) -> tuple[str, str] | None:
    if row is None:
        return "failed", "notification_outbox_missing"
    if config.notification_plan_event_suffix and not str(row.event_id).endswith(config.notification_plan_event_suffix):
        return "failed", "notification_plan_event_selector_mismatch"
    if row.event_type != NOTIFICATION_EVENT_TYPE:
        return "failed", "notification_outbox_wrong_event_type"
    if row.aggregate_type != "analysis":
        return "failed", "notification_outbox_wrong_aggregate_type"
    if row.status != "pending":
        return "failed", "notification_outbox_not_pending"
    if not row.dedupe_key:
        return "failed", "notification_outbox_dedupe_key_missing"
    if not isinstance(row.payload_json, dict):
        return "failed", "notification_outbox_payload_malformed"
    if REQUIRED_NOTIFICATION_PAYLOAD_FIELDS - set(row.payload_json):
        return "failed", "notification_outbox_payload_missing_required_field"
    if FORBIDDEN_NOTIFICATION_PAYLOAD_FIELDS & set(row.payload_json):
        return "failed", "notification_outbox_payload_forbidden_field"
    if _payload_uuid(row.payload_json, "notification_plan_id") is None:
        return "failed", "notification_outbox_plan_id_invalid"
    if _payload_uuid(row.payload_json, "analysis_id") != row.aggregate_id:
        return "failed", "notification_outbox_analysis_mismatch"
    if _payload_uuid(row.payload_json, "candidate_group_id") is None:
        return "failed", "notification_outbox_candidate_group_invalid"
    if not _payload_text(row.payload_json, "material_change_hash"):
        return "failed", "notification_outbox_material_hash_missing"
    return None


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


def _thin_message_error(message: RedisQueuedMessage) -> str | None:
    fields = message.as_stream_fields()
    if set(fields) != THIN_REDIS_FIELDS:
        return "notification_redis_message_wrong_shape"
    if FORBIDDEN_REDIS_FIELDS & set(fields):
        return "notification_redis_message_forbidden_field"
    return None


def _notification_publish_result(
    status: str,
    error_code: str | None,
    *,
    state: NotificationQueuePublishState | None = None,
    row: OutboxEventRow | None = None,
    redis_message_id: str | None = None,
    redis_xadd_count: int = 0,
    event_outbox_marked_published: bool = False,
    job_attempt_inserted: bool = False,
) -> NotificationQueuePublishResult:
    payload = row.payload_json if row is not None and isinstance(row.payload_json, Mapping) else {}
    return NotificationQueuePublishResult(
        status=status,
        ok=error_code is None and status == "published",
        error_code=error_code,
        event_id_suffix=_optional_id_suffix(row.event_id if row is not None else None),
        notification_plan_id_suffix=_optional_id_suffix(_payload_uuid(payload, "notification_plan_id")),
        aggregate_id_suffix=_optional_id_suffix(row.aggregate_id if row is not None else None),
        selected_event_present=row is not None,
        selected_event_status=row.status if row is not None else None,
        redis_message_id_suffix=_optional_id_suffix(redis_message_id),
        redis_xadd_count=redis_xadd_count,
        event_outbox_marked_published=event_outbox_marked_published,
        job_attempt_inserted=job_attempt_inserted,
        state=state or NotificationQueuePublishState(),
    )


def _valid_uuid_suffix(value: str) -> bool:
    stripped = value.strip().lower()
    return 4 <= len(stripped) <= 12 and "-" not in stripped and all(char in "0123456789abcdef" for char in stripped)


def _optional_id_suffix(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    return text[-8:]


def _payload_uuid(payload: Mapping[str, Any], key: str) -> UUID | None:
    value = payload.get(key)
    if not isinstance(value, str):
        return None
    try:
        return UUID(value)
    except (TypeError, ValueError):
        return None


def _payload_text(payload: Mapping[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (JSONDecodeError, TypeError):
            return None
    return value


def _outbox_row_from_mapping(row: Mapping[str, Any]) -> OutboxEventRow:
    payload = _json_loads(row["payload_json"]) or {}
    return OutboxEventRow(
        event_id=UUID(str(row["event_id"])),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=UUID(str(row["aggregate_id"])),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload if isinstance(payload, dict) else {},
        status=str(row["status"]),
        fail_count=int(row["fail_count"]),
        created_at=row["created_at"],
    )


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)


__all__ = [
    "BoundedPolicyToNotificationQueueConfig",
    "BoundedPolicyToNotificationQueueResult",
    "NotificationQueuePublishRepositoryBuilder",
    "NotificationQueuePublishRepositoryHandle",
    "NotificationQueueRedisPublisherBuilder",
    "NotificationQueueRedisPublisherHandle",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "argument_error_report",
    "build_default_notification_queue_publish_repository",
    "build_default_notification_queue_redis_publisher",
    "render_sanitized_json",
    "run_bounded_policy_to_notification_queue",
    "run_bounded_policy_to_notification_queue_sync",
]
