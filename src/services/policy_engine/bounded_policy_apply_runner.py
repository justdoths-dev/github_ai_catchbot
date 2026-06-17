from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from json import JSONDecodeError
from typing import Any, Protocol
from uuid import UUID, uuid4

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - static validation fallback
    sa = None

from ..outbox_relay.models import OutboxEventRow
from .config import PolicyEngineConfig
from .delivery_policy import DeliveryPolicy
from .models import (
    AnalysisDraft,
    BundlePolicyContext,
    CandidatePolicyContext,
    ExistingAnalysisRecord,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
    NotificationPlanIntent,
    PolicyEvaluation,
)
from .notification_intent import NotificationIntentBuilder
from .repositories import PolicyEngineRepository
from .verdict_policy import VerdictPolicy, reconcile_model_verdict


SCHEMA_VERSION = "bounded_policy_apply_runner_v1"
RUNNER_NAME = "bounded_policy_apply_runner"
MODE_PREVIEW = "preview"
MODE_EXECUTE = "execute"
QUEUE_NAME = "q.analysis.policy"
STAGE_NAME = "analysis_policy"
ROOT_OBJECT_TYPE = "judge_run"
EVENT_TYPE = "analysis.policy.apply.v1"
NOTIFICATION_EVENT_TYPE = "notification.plan.created.v1"
CONSUMER_GROUP = "policy-engine"
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
        "analysis_id",
        "bundle_id",
        "candidate_group_id",
        "database_url",
        "judge_output_id",
        "judge_profile",
        "message_text",
        "notification_plan_id",
        "payload_json",
        "prompt",
        "prompt_material",
        "raw_payload",
        "raw_text",
        "redis_url",
        "scores",
        "source_text",
        "target_chat_id",
    }
)
REQUIRED_EVENT_PAYLOAD_FIELDS = frozenset(
    {
        "judge_run_id",
        "judge_output_id",
        "candidate_group_id",
        "bundle_id",
    }
)


@dataclass(frozen=True, slots=True)
class BoundedPolicyApplyConfig:
    mode: str = MODE_PREVIEW
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_read: bool = False
    allow_database_read: bool = False
    allow_redis_consume: bool = False
    allow_database_write: bool = False
    allow_redis_ack: bool = False
    trigger_event_suffix: str | None = None
    judge_run_suffix: str | None = None
    judge_output_suffix: str | None = None
    scan_limit: int = DEFAULT_SCAN_LIMIT


@dataclass(frozen=True, slots=True)
class BoundedPolicyApplyRuntimeConfig:
    database_url: str
    redis_url: str
    queue_name: str = QUEUE_NAME
    consumer_group: str = CONSUMER_GROUP
    consumer_name: str = "bounded-policy-apply"
    policy_version: str = "verdict_policy_v1"
    delivery_policy_version: str = "delivery_policy_v1"
    operator_chat_id: int = 0
    enable_later_delivery: bool = True
    render_profile_high: str = "telegram_single_alert_high_v1"
    render_profile_normal: str = "telegram_single_alert_normal_v1"

    def to_policy_config(self) -> PolicyEngineConfig:
        return PolicyEngineConfig(
            app_env="runtime",
            database_url=self.database_url,
            redis_url=self.redis_url,
            queue_name=self.queue_name,
            consumer_group=self.consumer_group,
            consumer_name=self.consumer_name,
            batch_size=1,
            block_ms=1,
            policy_version=self.policy_version,
            delivery_policy_version=self.delivery_policy_version,
            operator_chat_id=self.operator_chat_id,
            enable_later_delivery=self.enable_later_delivery,
            enable_silent_later=True,
            enable_notification_send=True,
            render_profile_high=self.render_profile_high,
            render_profile_normal=self.render_profile_normal,
            log_level="INFO",
        )


@dataclass(slots=True)
class BoundedPolicyApplyState:
    runtime_config_loaded: bool = False
    redis_read_attempted: bool = False
    redis_consume_attempted: bool = False
    redis_ack_attempted: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    database_commit_attempted: bool = False
    group_name: str | None = None
    group_exists: bool | None = None
    group_pending: int | None = None
    group_lag: int | None = None
    group_last_delivered_id_suffix: str | None = None
    target_after_group_last_delivered: bool | None = None
    target_is_next_deliverable: bool | None = None


@dataclass(frozen=True, slots=True)
class TargetPolicyApplyMessage:
    redis_message_id: str
    fields: dict[str, str]

    @property
    def trigger_event_id(self) -> UUID | None:
        return _safe_uuid(self.fields.get("trigger_event_id"))

    @property
    def judge_run_id(self) -> UUID | None:
        return _safe_uuid(self.fields.get("root_object_id"))


@dataclass(frozen=True, slots=True)
class BoundedPolicyApplyResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedPolicyApplyConfig
    state: BoundedPolicyApplyState = field(default_factory=BoundedPolicyApplyState)
    queue_name: str = QUEUE_NAME
    stage_name: str = STAGE_NAME
    messages_seen: int = 0
    messages_matched: int = 0
    messages_processed_count: int = 0
    redis_ack_status: str = "not_attempted"
    redis_acked_count: int = 0
    target_redis_message_id_suffix: str | None = None
    target_policy_apply_event_suffix: str | None = None
    target_judge_run_id_suffix: str | None = None
    target_judge_output_id_suffix: str | None = None
    target_bundle_id_suffix: str | None = None
    target_candidate_group_suffix: str | None = None
    target_analysis_id_suffix: str | None = None
    target_notification_plan_event_suffix: str | None = None
    target_notification_plan_id_suffix: str | None = None
    target_message_found: bool = False
    event_outbox_found: bool = False
    judge_run_found: bool = False
    judge_output_found: bool = False
    bundle_found: bool = False
    candidate_group_found: bool = False
    existing_analysis_found: bool = False
    analysis_written: bool = False
    state_transition_written: bool = False
    notification_plan_intent_outbox_written: bool = False
    verdict: str | None = None
    delivery_decision: str | None = None
    urgency_profile: str | None = None
    policy_reconciled_flag: bool | None = None
    planned_action: str | None = None
    would_fail_closed: bool = False

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": self.config.mode,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "messages_seen": self.messages_seen,
            "messages_matched": self.messages_matched,
            "messages_processed_count": self.messages_processed_count,
            "target_redis_message_id_suffix": self.target_redis_message_id_suffix,
            "target_policy_apply_event_suffix": self.target_policy_apply_event_suffix,
            "target_judge_run_id_suffix": self.target_judge_run_id_suffix,
            "target_judge_output_id_suffix": self.target_judge_output_id_suffix,
            "target_bundle_id_suffix": self.target_bundle_id_suffix,
            "target_candidate_group_suffix": self.target_candidate_group_suffix,
            "target_analysis_id_suffix": self.target_analysis_id_suffix,
            "target_notification_plan_event_suffix": self.target_notification_plan_event_suffix,
            "target_notification_plan_id_suffix": self.target_notification_plan_id_suffix,
            "target_message_found": self.target_message_found,
            "event_outbox_found": self.event_outbox_found,
            "judge_run_found": self.judge_run_found,
            "judge_output_found": self.judge_output_found,
            "bundle_found": self.bundle_found,
            "candidate_group_found": self.candidate_group_found,
            "existing_analysis_found": self.existing_analysis_found,
            "analysis_written": self.analysis_written,
            "state_transition_written": self.state_transition_written,
            "notification_plan_intent_outbox_written": self.notification_plan_intent_outbox_written,
            "verdict": self.verdict,
            "delivery_decision": self.delivery_decision,
            "urgency_profile": self.urgency_profile,
            "policy_reconciled_flag": self.policy_reconciled_flag,
            "planned_action": self.planned_action,
            "would_fail_closed": self.would_fail_closed,
            "redis_ack_status": self.redis_ack_status,
            "redis_acked_count": self.redis_acked_count,
            "group_name": self.state.group_name,
            "group_exists": self.state.group_exists,
            "group_pending": self.state.group_pending,
            "group_lag": self.state.group_lag,
            "group_last_delivered_id_suffix": self.state.group_last_delivered_id_suffix,
            "target_after_group_last_delivered": self.state.target_after_group_last_delivered,
            "target_is_next_deliverable": self.state.target_is_next_deliverable,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "redis_read_allowed": self.config.allow_redis_read,
                "database_read_allowed": self.config.allow_database_read,
                "redis_consume_allowed": self.config.allow_redis_consume,
                "database_write_allowed": self.config.allow_database_write,
                "redis_ack_allowed": self.config.allow_redis_ack,
                "scan_limit": self.config.scan_limit,
            },
            "side_effects": {
                "redis_read_called": self.state.redis_read_attempted,
                "redis_consume_called": self.state.redis_consume_attempted,
                "redis_ack_called": self.state.redis_ack_attempted,
                "db_read": self.state.database_read_attempted,
                "db_write": self.state.database_write_attempted,
                "db_commit": self.state.database_commit_attempted,
                "analysis_created": self.analysis_written,
                "notification_plan_intent_created": self.notification_plan_intent_outbox_written,
                "notification_plans_table_written": False,
                "q_notification_send_published": False,
                "notifier_called": False,
                "telegram_send_called": False,
                "openai_called": False,
                "github_api_called": False,
                "x_api_called": False,
                "web_fetch_called": False,
                "worker_started": False,
            },
            "redactions_applied": {
                "full_redis_message_id_omitted": True,
                "full_policy_apply_event_id_omitted": True,
                "full_judge_run_id_omitted": True,
                "full_judge_output_id_omitted": True,
                "full_bundle_id_omitted": True,
                "full_candidate_group_id_omitted": True,
                "full_analysis_id_omitted": True,
                "full_notification_plan_event_id_omitted": True,
                "full_notification_plan_id_omitted": True,
                "idempotency_key_omitted": True,
                "target_chat_id_omitted": True,
                "judge_output_payload_omitted": True,
                "raw_source_text_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class BoundedPolicyApplyError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class _ResultReady(Exception):
    pass


class RedisPolicyApplyConsumerClient(Protocol):
    async def xlen(self, name: str) -> int: ...
    async def xrange(self, name: str, min: str = "-", max: str = "+", count: int | None = None) -> Any: ...
    async def xinfo_groups(self, name: str) -> Any: ...
    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> Any: ...
    async def xack(self, name: str, groupname: str, *ids: str) -> Any: ...


class RedisPolicyApplyConsumer:
    def __init__(
        self,
        client: RedisPolicyApplyConsumerClient,
        *,
        queue_name: str,
        consumer_group: str,
        consumer_name: str | None = None,
    ) -> None:
        self._client = client
        self._queue_name = queue_name
        self._consumer_group = consumer_group
        self._consumer_name = consumer_name or f"bounded-policy-apply-{uuid4().hex[:8]}"

    async def find_target(
        self,
        config: BoundedPolicyApplyConfig,
        state: BoundedPolicyApplyState,
    ) -> tuple[TargetPolicyApplyMessage | None, int, int]:
        state.redis_read_attempted = True
        if await self._client.xlen(self._queue_name) <= 0:
            return None, 0, 0
        raw = await self._client.xrange(self._queue_name, min="-", max="+", count=config.scan_limit)
        return _select_target_from_entries(_flatten_direct_stream_entries(raw), config, config.scan_limit)

    async def preflight_group(self, selected: TargetPolicyApplyMessage, state: BoundedPolicyApplyState) -> None:
        state.redis_read_attempted = True
        state.group_name = self._consumer_group
        state.group_exists = False
        state.group_pending = None
        state.group_lag = None
        state.group_last_delivered_id_suffix = None
        state.target_after_group_last_delivered = False
        state.target_is_next_deliverable = False

        raw_groups = await self._client.xinfo_groups(self._queue_name)
        group = _find_consumer_group(raw_groups, self._consumer_group)
        if group is None:
            return
        state.group_exists = True
        state.group_pending = _int_or_none(group.get("pending"))
        state.group_lag = _int_or_none(group.get("lag"))
        last_delivered_id = _string_or_none(group.get("last-delivered-id"))
        state.group_last_delivered_id_suffix = _optional_id_suffix(last_delivered_id)
        if not last_delivered_id:
            return
        target_after_last = _redis_stream_id_greater(selected.redis_message_id, last_delivered_id)
        state.target_after_group_last_delivered = target_after_last
        if state.group_pending != 0 or not target_after_last:
            return
        normalized_last_delivered_id = _normalize_redis_stream_id(last_delivered_id)
        if normalized_last_delivered_id is None:
            return
        raw_next = await self._client.xrange(
            self._queue_name,
            min=f"({normalized_last_delivered_id}",
            max="+",
            count=1,
        )
        next_entries = _flatten_direct_stream_entries(raw_next)
        state.target_is_next_deliverable = bool(next_entries and next_entries[0][0] == selected.redis_message_id)

    async def consume_target(
        self,
        config: BoundedPolicyApplyConfig,
        state: BoundedPolicyApplyState,
    ) -> tuple[TargetPolicyApplyMessage | None, int, int]:
        state.redis_consume_attempted = True
        raw = await self._client.xreadgroup(
            self._consumer_group,
            self._consumer_name,
            {self._queue_name: ">"},
            count=1,
        )
        selected: TargetPolicyApplyMessage | None = None
        messages_seen = 0
        messages_matched = 0
        for message_id, fields in _flatten_group_stream_entries(raw)[:1]:
            messages_seen += 1
            decoded_fields = _decode_fields(fields)
            if _matches_target(message_id, decoded_fields, config):
                messages_matched += 1
                selected = TargetPolicyApplyMessage(redis_message_id=message_id, fields=decoded_fields)
        return selected, messages_seen, messages_matched

    async def ack(self, message_id: str, state: BoundedPolicyApplyState) -> int:
        state.redis_ack_attempted = True
        result = await self._client.xack(self._queue_name, self._consumer_group, message_id)
        try:
            return int(result)
        except (TypeError, ValueError):
            return 1 if result else 0


class PolicyApplyRepository(Protocol):
    async def load_event_outbox(self, trigger_event_id: UUID) -> OutboxEventRow | None: ...
    async def load_candidate_context(self, candidate_group_id: UUID) -> CandidatePolicyContext | None: ...
    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunPolicyContext | None: ...
    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputPolicyContext | None: ...
    async def load_bundle_context(self, bundle_id: UUID) -> BundlePolicyContext | None: ...
    async def load_existing_analysis(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> ExistingAnalysisRecord | None: ...
    async def insert_analysis(self, draft: AnalysisDraft) -> UUID: ...
    async def insert_state_transition(
        self,
        *,
        object_type: str,
        object_id: UUID,
        from_state: str | None,
        to_state: str,
        reason_code: str | None,
    ) -> None: ...
    async def load_notification_plan_intent_outboxes(self, intent: NotificationPlanIntent) -> list[OutboxEventRow]: ...
    async def insert_or_load_notification_plan_intent_outbox(
        self,
        intent: NotificationPlanIntent,
    ) -> tuple[OutboxEventRow, bool]: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...


class SqlAlchemyPolicyApplyRepository:
    def __init__(self, session: Any) -> None:
        self._session = session
        self._policy_repository = PolicyEngineRepository(session)

    async def load_event_outbox(self, trigger_event_id: UUID) -> OutboxEventRow | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       dedupe_key, payload_json, status, fail_count, created_at
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(trigger_event_id)},
        )
        row = result.mappings().first()
        return _outbox_row_from_mapping(row) if row is not None else None

    async def load_candidate_context(self, candidate_group_id: UUID) -> CandidatePolicyContext | None:
        return await self._policy_repository.load_candidate_context(candidate_group_id)

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunPolicyContext | None:
        return await self._policy_repository.load_judge_run(judge_run_id)

    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputPolicyContext | None:
        return await self._policy_repository.load_judge_output(judge_output_id)

    async def load_bundle_context(self, bundle_id: UUID) -> BundlePolicyContext | None:
        return await self._policy_repository.load_bundle_context(bundle_id)

    async def load_existing_analysis(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> ExistingAnalysisRecord | None:
        return await self._policy_repository.load_existing_analysis(
            judge_output_id=judge_output_id,
            policy_version=policy_version,
            delivery_policy_version=delivery_policy_version,
        )

    async def insert_analysis(self, draft: AnalysisDraft) -> UUID:
        return await self._policy_repository.insert_analysis(draft)

    async def insert_state_transition(
        self,
        *,
        object_type: str,
        object_id: UUID,
        from_state: str | None,
        to_state: str,
        reason_code: str | None,
    ) -> None:
        await self._policy_repository.insert_state_transition(
            object_type=object_type,
            object_id=object_id,
            from_state=from_state,
            to_state=to_state,
            reason_code=reason_code,
        )

    async def load_notification_plan_intent_outboxes(self, intent: NotificationPlanIntent) -> list[OutboxEventRow]:
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       dedupe_key, payload_json, status, fail_count, created_at
                FROM event_outbox
                WHERE event_type = 'notification.plan.created.v1'
                  AND aggregate_type = 'analysis'
                  AND aggregate_id = CAST(:analysis_id AS uuid)
                  AND dedupe_key = :dedupe_key
                ORDER BY created_at ASC, event_id ASC
                LIMIT 2
                """
            ),
            {
                "analysis_id": str(intent.analysis_id),
                "dedupe_key": _notification_plan_dedupe_key(intent),
            },
        )
        return [_outbox_row_from_mapping(row) for row in result.mappings().all()]

    async def insert_or_load_notification_plan_intent_outbox(
        self,
        intent: NotificationPlanIntent,
    ) -> tuple[OutboxEventRow, bool]:
        payload = _notification_plan_payload(intent)
        dedupe_key = _notification_plan_dedupe_key(intent)
        result = await self._session.execute(
            _sql(
                """
                WITH inserted AS (
                    INSERT INTO event_outbox (
                        event_type, aggregate_type, aggregate_id, dedupe_key,
                        payload_json, status, created_at
                    ) VALUES (
                        'notification.plan.created.v1',
                        'analysis',
                        CAST(:analysis_id AS uuid),
                        :dedupe_key,
                        CAST(:payload_json AS jsonb),
                        'pending'::outbox_status_enum,
                        now()
                    )
                    ON CONFLICT (dedupe_key) DO NOTHING
                    RETURNING event_id, event_type, aggregate_type, aggregate_id,
                              dedupe_key, payload_json, status, fail_count, created_at,
                              true AS inserted
                )
                SELECT * FROM inserted
                UNION ALL
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       dedupe_key, payload_json, status, fail_count, created_at,
                       false AS inserted
                FROM event_outbox
                WHERE dedupe_key = :dedupe_key
                LIMIT 1
                """
            ),
            {
                "analysis_id": str(intent.analysis_id),
                "dedupe_key": dedupe_key,
                "payload_json": _jsonb_dumps(payload),
            },
        )
        row = result.mappings().first()
        if row is None:
            raise BoundedPolicyApplyError("notification_plan_intent_outbox_missing")
        return _outbox_row_from_mapping(row), bool(row["inserted"])

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()


@dataclass(frozen=True, slots=True)
class BoundedPolicyApplyRedisHandle:
    consumer: RedisPolicyApplyConsumer
    close: Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedPolicyApplyRepositoryHandle:
    repository: PolicyApplyRepository
    close: Callable[[], Awaitable[None]]


class RedisBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedPolicyApplyRuntimeConfig,
        state: BoundedPolicyApplyState,
        logger: logging.Logger,
    ) -> BoundedPolicyApplyRedisHandle: ...


class RepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedPolicyApplyRuntimeConfig,
        state: BoundedPolicyApplyState,
        logger: logging.Logger,
    ) -> BoundedPolicyApplyRepositoryHandle: ...


async def build_default_redis_consumer(
    runtime_config: BoundedPolicyApplyRuntimeConfig,
    state: BoundedPolicyApplyState,
    logger: logging.Logger,
) -> BoundedPolicyApplyRedisHandle:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    consumer = RedisPolicyApplyConsumer(
        redis_client,
        queue_name=runtime_config.queue_name,
        consumer_group=runtime_config.consumer_group,
        consumer_name=f"{runtime_config.consumer_name}-{uuid4().hex[:8]}",
    )

    async def close() -> None:
        close_client = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close_client is None:
            return
        result = close_client()
        if hasattr(result, "__await__"):
            await result

    return BoundedPolicyApplyRedisHandle(consumer=consumer, close=close)


async def build_default_repository(
    runtime_config: BoundedPolicyApplyRuntimeConfig,
    state: BoundedPolicyApplyState,
    logger: logging.Logger,
) -> BoundedPolicyApplyRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    repository = SqlAlchemyPolicyApplyRepository(session)

    async def close() -> None:
        try:
            await session.close()
        finally:
            await engine.dispose()

    return BoundedPolicyApplyRepositoryHandle(repository=repository, close=close)


def load_runtime_config(env: Mapping[str, str] | None = None) -> BoundedPolicyApplyRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedPolicyApplyError("database_url_missing")
    if not redis_url:
        raise BoundedPolicyApplyError("redis_url_missing")
    queue_name = _env_value(source, "POLICY_ENGINE_QUEUE_NAME", QUEUE_NAME)
    if queue_name != QUEUE_NAME:
        raise BoundedPolicyApplyError("queue_not_allowed")
    try:
        operator_chat_id = int(_env_value(source, "TELEGRAM_OPERATOR_CHAT_ID", "0"))
    except ValueError as exc:
        raise BoundedPolicyApplyError("runtime_config_error") from exc
    return BoundedPolicyApplyRuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        queue_name=queue_name,
        consumer_group=_env_value(source, "POLICY_ENGINE_CONSUMER_GROUP", CONSUMER_GROUP),
        consumer_name=_env_value(source, "POLICY_ENGINE_CONSUMER_NAME", "bounded-policy-apply"),
        policy_version=_env_value(source, "VERDICT_POLICY_VERSION", "verdict_policy_v1"),
        delivery_policy_version=_env_value(source, "DELIVERY_POLICY_VERSION", "delivery_policy_v1"),
        operator_chat_id=operator_chat_id,
        enable_later_delivery=_bool_env(_env_value(source, "ENABLE_LATER_DELIVERY", "true")),
        render_profile_high=_env_value(source, "NOTIFY_RENDER_PROFILE_HIGH", "telegram_single_alert_high_v1"),
        render_profile_normal=_env_value(source, "NOTIFY_RENDER_PROFILE_NORMAL", "telegram_single_alert_normal_v1"),
    )


async def run_bounded_policy_apply(
    config: BoundedPolicyApplyConfig,
    *,
    runtime_config_loader: Callable[[], BoundedPolicyApplyRuntimeConfig] = load_runtime_config,
    redis_builder: RedisBuilder | None = None,
    repository_builder: RepositoryBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedPolicyApplyResult:
    state = BoundedPolicyApplyState()
    gate_error = _gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedPolicyApplyError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception as exc:
        return _result("blocked", "runtime_config_error", error_class=_safe_exception_class(exc), config=config, state=state)

    if runtime_config.queue_name != QUEUE_NAME:
        return _result("blocked", "queue_not_allowed", config=config, state=state)

    redis_handle: BoundedPolicyApplyRedisHandle | None = None
    repository_handle: BoundedPolicyApplyRepositoryHandle | None = None
    selected: TargetPolicyApplyMessage | None = None
    event: OutboxEventRow | None = None
    candidate: CandidatePolicyContext | None = None
    judge_run: JudgeRunPolicyContext | None = None
    judge_output: JudgeOutputPolicyContext | None = None
    bundle: BundlePolicyContext | None = None
    existing_analysis: ExistingAnalysisRecord | None = None
    analysis: AnalysisDraft | None = None
    evaluation: PolicyEvaluation | None = None
    analysis_id: UUID | None = None
    notification_intent: NotificationPlanIntent | None = None
    notification_outbox: OutboxEventRow | None = None
    planned_action: str | None = None
    would_fail_closed = False
    messages_seen = 0
    messages_matched = 0
    analysis_written = False
    state_transition_written = False
    notification_outbox_written = False
    result: BoundedPolicyApplyResult | None = None

    try:
        redis_handle = await (redis_builder or build_default_redis_consumer)(
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
            raise _ResultReady
        if messages_matched != 1:
            result = _result(
                "blocked",
                "duplicate_target_message",
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
            )
            raise _ResultReady

        message_error = _message_contract_error(selected)
        if message_error is not None:
            result = _result(
                "blocked",
                message_error,
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
            )
            raise _ResultReady

        await redis_handle.consumer.preflight_group(selected, state)
        group_error = _group_preflight_error(state)
        if group_error is not None:
            would_fail_closed = True
            if config.mode == MODE_EXECUTE:
                result = _result(
                    "blocked",
                    group_error,
                    config=config,
                    state=state,
                    selected=selected,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                    planned_action="fail_closed",
                    would_fail_closed=would_fail_closed,
                )
                raise _ResultReady

        repository_handle = await (repository_builder or build_default_repository)(
            runtime_config,
            state,
            effective_logger,
        )
        repository = repository_handle.repository
        state.database_read_attempted = True

        trigger_event_id = selected.trigger_event_id
        root_judge_run_id = selected.judge_run_id
        assert trigger_event_id is not None
        assert root_judge_run_id is not None
        event = await repository.load_event_outbox(trigger_event_id)
        event_error = _event_contract_error(event, trigger_event_id=trigger_event_id, root_judge_run_id=root_judge_run_id)
        if event_error is not None:
            would_fail_closed = True
            result = _result(
                "blocked",
                event_error,
                config=config,
                state=state,
                selected=selected,
                event=event,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                planned_action="fail_closed",
                would_fail_closed=would_fail_closed,
            )
            raise _ResultReady

        assert event is not None
        payload_judge_run_id = _payload_uuid(event.payload_json, "judge_run_id")
        payload_judge_output_id = _payload_uuid(event.payload_json, "judge_output_id")
        payload_candidate_group_id = _payload_uuid(event.payload_json, "candidate_group_id")
        payload_bundle_id = _payload_uuid(event.payload_json, "bundle_id")
        selector_error = _payload_selector_error(
            config=config,
            judge_run_id=payload_judge_run_id,
            judge_output_id=payload_judge_output_id,
        )
        if selector_error is not None or None in {
            payload_judge_run_id,
            payload_judge_output_id,
            payload_candidate_group_id,
            payload_bundle_id,
        }:
            result = _result(
                "blocked",
                selector_error or "event_payload_malformed",
                config=config,
                state=state,
                selected=selected,
                event=event,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                planned_action="fail_closed",
                would_fail_closed=True,
            )
            raise _ResultReady

        assert payload_judge_run_id is not None
        assert payload_judge_output_id is not None
        assert payload_candidate_group_id is not None
        assert payload_bundle_id is not None
        candidate = await repository.load_candidate_context(payload_candidate_group_id)
        judge_run = await repository.load_judge_run(payload_judge_run_id)
        judge_output = await repository.load_judge_output(payload_judge_output_id)
        bundle = await repository.load_bundle_context(payload_bundle_id)
        context_error = _context_error(
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            judge_run_id=payload_judge_run_id,
            judge_output_id=payload_judge_output_id,
            candidate_group_id=payload_candidate_group_id,
            bundle_id=payload_bundle_id,
        )
        if context_error is not None:
            result = _result(
                "blocked",
                context_error,
                config=config,
                state=state,
                selected=selected,
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                planned_action="noop" if context_error == "stale_bundle" else "fail_closed",
                would_fail_closed=True,
            )
            raise _ResultReady

        assert candidate is not None
        assert judge_run is not None
        assert judge_output is not None
        assert bundle is not None
        analysis, evaluation = _build_analysis(
            policy_config=runtime_config.to_policy_config(),
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
        )
        existing_analysis = await repository.load_existing_analysis(
            judge_output_id=judge_output.judge_output_id,
            policy_version=analysis.policy_version,
            delivery_policy_version=analysis.delivery_policy_version,
        )
        if existing_analysis is not None:
            analysis_id = existing_analysis.analysis_id
            planned_action = "reuse_existing_analysis"
        elif analysis.delivery_decision == "suppress":
            planned_action = "create_analysis_suppress_only"
        else:
            if runtime_config.operator_chat_id == 0:
                result = _result(
                    "blocked",
                    "notification_target_missing",
                    config=config,
                    state=state,
                    selected=selected,
                    event=event,
                    candidate=candidate,
                    judge_run=judge_run,
                    judge_output=judge_output,
                    bundle=bundle,
                    analysis=analysis,
                    evaluation=evaluation,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                    planned_action="fail_closed",
                    would_fail_closed=True,
                )
                raise _ResultReady
            planned_action = "create_analysis_and_notification_intent"

        if config.mode == MODE_PREVIEW:
            result = _result(
                "preview",
                None,
                config=config,
                state=state,
                selected=selected,
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                existing_analysis=existing_analysis,
                analysis_id=analysis_id,
                analysis=analysis,
                evaluation=evaluation,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                planned_action=planned_action,
                would_fail_closed=would_fail_closed,
            )
            raise _ResultReady

        consumed, consume_seen, consume_matched = await redis_handle.consumer.consume_target(config, state)
        if (
            consumed is None
            or consume_seen != 1
            or consume_matched != 1
            or consumed.redis_message_id != selected.redis_message_id
        ):
            result = _result(
                "blocked",
                "target_message_not_consumable_exactly",
                config=config,
                state=state,
                selected=selected,
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                existing_analysis=existing_analysis,
                analysis_id=analysis_id,
                analysis=analysis,
                evaluation=evaluation,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                planned_action="fail_closed",
                would_fail_closed=True,
            )
            raise _ResultReady

        if existing_analysis is None:
            state.database_write_attempted = True
            analysis_id = await repository.insert_analysis(analysis)
            analysis_written = True
            await repository.insert_state_transition(
                object_type="analysis",
                object_id=analysis_id,
                from_state="analysis_validated",
                to_state="analysis_policy_suppressed"
                if analysis.delivery_decision == "suppress"
                else "analysis_policy_applied",
                reason_code=f"policy_applied:{analysis.verdict}:{analysis.delivery_decision}",
            )
            state_transition_written = True
            if analysis.delivery_decision != "suppress":
                notification_intent = NotificationIntentBuilder(config=runtime_config.to_policy_config()).build(
                    analysis_id=analysis_id,
                    analysis=analysis,
                    evaluation=evaluation,
                )
                if notification_intent is None:
                    raise BoundedPolicyApplyError("notification_plan_intent_missing")
                matching_outboxes = await repository.load_notification_plan_intent_outboxes(notification_intent)
                if len(matching_outboxes) > 1:
                    result = _result(
                        "blocked",
                        "duplicate_notification_plan_intent_outbox",
                        config=config,
                        state=state,
                        selected=selected,
                        event=event,
                        candidate=candidate,
                        judge_run=judge_run,
                        judge_output=judge_output,
                        bundle=bundle,
                        analysis_id=analysis_id,
                        analysis=analysis,
                        evaluation=evaluation,
                        messages_seen=messages_seen,
                        messages_matched=messages_matched,
                        planned_action="fail_closed",
                        would_fail_closed=True,
                    )
                    raise _ResultReady
                if matching_outboxes:
                    notification_outbox = matching_outboxes[0]
                else:
                    notification_outbox, notification_outbox_written = (
                        await repository.insert_or_load_notification_plan_intent_outbox(notification_intent)
                    )
                outbox_error = _notification_outbox_error(notification_outbox, intent=notification_intent)
                if outbox_error is not None:
                    result = _result(
                        "blocked",
                        outbox_error,
                        config=config,
                        state=state,
                        selected=selected,
                        event=event,
                        candidate=candidate,
                        judge_run=judge_run,
                        judge_output=judge_output,
                        bundle=bundle,
                        analysis_id=analysis_id,
                        analysis=analysis,
                        evaluation=evaluation,
                        notification_outbox=notification_outbox,
                        notification_outbox_written=notification_outbox_written,
                        messages_seen=messages_seen,
                        messages_matched=messages_matched,
                        planned_action="fail_closed",
                        would_fail_closed=True,
                    )
                    raise _ResultReady

        try:
            state.database_commit_attempted = True
            await repository.commit()
        except Exception as exc:
            result = _result(
                "failed",
                "database_commit_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected=selected,
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                existing_analysis=existing_analysis,
                analysis_id=analysis_id,
                analysis=analysis,
                evaluation=evaluation,
                notification_outbox=notification_outbox,
                notification_outbox_written=notification_outbox_written,
                analysis_written=analysis_written,
                state_transition_written=state_transition_written,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                messages_processed_count=1,
                planned_action=planned_action,
                would_fail_closed=would_fail_closed,
            )
            raise _ResultReady

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
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                existing_analysis=existing_analysis,
                analysis_id=analysis_id,
                analysis=analysis,
                evaluation=evaluation,
                notification_outbox=notification_outbox,
                notification_outbox_written=notification_outbox_written,
                analysis_written=analysis_written,
                state_transition_written=state_transition_written,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                messages_processed_count=1,
                redis_ack_status="failed",
                planned_action=planned_action,
                would_fail_closed=would_fail_closed,
            )
            raise _ResultReady
        if acked_count != 1:
            result = _result(
                "failed",
                "redis_ack_failed",
                config=config,
                state=state,
                selected=selected,
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                existing_analysis=existing_analysis,
                analysis_id=analysis_id,
                analysis=analysis,
                evaluation=evaluation,
                notification_outbox=notification_outbox,
                notification_outbox_written=notification_outbox_written,
                analysis_written=analysis_written,
                state_transition_written=state_transition_written,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                messages_processed_count=1,
                redis_ack_status="failed",
                planned_action=planned_action,
                would_fail_closed=would_fail_closed,
            )
            raise _ResultReady

        result = _result(
            "applied",
            None,
            config=config,
            state=state,
            selected=selected,
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            existing_analysis=existing_analysis,
            analysis_id=analysis_id,
            analysis=analysis,
            evaluation=evaluation,
            notification_outbox=notification_outbox,
            notification_outbox_written=notification_outbox_written,
            analysis_written=analysis_written,
            state_transition_written=state_transition_written,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
            messages_processed_count=1,
            redis_ack_status="acked",
            redis_acked_count=acked_count,
            planned_action=planned_action,
            would_fail_closed=would_fail_closed,
        )
    except _ResultReady:
        pass
    except BoundedPolicyApplyError as exc:
        result = _result(
            "blocked",
            exc.error_code,
            config=config,
            state=state,
            selected=selected,
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            existing_analysis=existing_analysis,
            analysis_id=analysis_id,
            analysis=analysis,
            evaluation=evaluation,
            notification_outbox=notification_outbox,
            notification_outbox_written=notification_outbox_written,
            analysis_written=analysis_written,
            state_transition_written=state_transition_written,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
            planned_action=planned_action or "fail_closed",
            would_fail_closed=True,
        )
    except Exception as exc:
        result = _result(
            "failed",
            "database_write_failed" if state.database_write_attempted else "bounded_policy_apply_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            selected=selected,
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            existing_analysis=existing_analysis,
            analysis_id=analysis_id,
            analysis=analysis,
            evaluation=evaluation,
            notification_outbox=notification_outbox,
            notification_outbox_written=notification_outbox_written,
            analysis_written=analysis_written,
            state_transition_written=state_transition_written,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
            planned_action=planned_action or "fail_closed",
            would_fail_closed=True,
        )
    finally:
        if repository_handle is not None:
            try:
                await repository_handle.repository.rollback()
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
                    error_code="redis_close_failed",
                    error_class=_safe_exception_class(exc),
                    config=config,
                    state=state,
                )

    assert result is not None
    return result


def run_bounded_policy_apply_sync(
    config: BoundedPolicyApplyConfig,
    *,
    runtime_config_loader: Callable[[], BoundedPolicyApplyRuntimeConfig] = load_runtime_config,
    redis_builder: RedisBuilder | None = None,
    repository_builder: RepositoryBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedPolicyApplyResult:
    return asyncio.run(
        run_bounded_policy_apply(
            config,
            runtime_config_loader=runtime_config_loader,
            redis_builder=redis_builder,
            repository_builder=repository_builder,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        error_code,
        config=BoundedPolicyApplyConfig(),
        state=BoundedPolicyApplyState(),
    ).to_sanitized_dict()


def _gate_error(config: BoundedPolicyApplyConfig) -> str | None:
    if config.mode not in {MODE_PREVIEW, MODE_EXECUTE}:
        return "invalid_mode"
    if not config.operator_approved:
        return "operator_approval_missing"
    if not 1 <= config.scan_limit <= HARD_SCAN_LIMIT:
        return "invalid_scan_limit"
    if not config.trigger_event_suffix:
        return "target_missing"
    for value, error_code in (
        (config.trigger_event_suffix, "invalid_trigger_event_suffix"),
        (config.judge_run_suffix, "invalid_judge_run_suffix"),
        (config.judge_output_suffix, "invalid_judge_output_suffix"),
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
        if not config.allow_redis_ack:
            return "redis_ack_not_allowed"
    return None


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedPolicyApplyConfig,
    state: BoundedPolicyApplyState,
    error_class: str | None = None,
    selected: TargetPolicyApplyMessage | None = None,
    event: OutboxEventRow | None = None,
    candidate: CandidatePolicyContext | None = None,
    judge_run: JudgeRunPolicyContext | None = None,
    judge_output: JudgeOutputPolicyContext | None = None,
    bundle: BundlePolicyContext | None = None,
    existing_analysis: ExistingAnalysisRecord | None = None,
    analysis_id: UUID | None = None,
    analysis: AnalysisDraft | None = None,
    evaluation: PolicyEvaluation | None = None,
    notification_outbox: OutboxEventRow | None = None,
    notification_outbox_written: bool = False,
    analysis_written: bool = False,
    state_transition_written: bool = False,
    messages_seen: int = 0,
    messages_matched: int = 0,
    messages_processed_count: int = 0,
    redis_ack_status: str = "not_attempted",
    redis_acked_count: int = 0,
    planned_action: str | None = None,
    would_fail_closed: bool = False,
) -> BoundedPolicyApplyResult:
    event_payload = event.payload_json if event is not None and isinstance(event.payload_json, Mapping) else {}
    resolved_analysis_id = analysis_id or (existing_analysis.analysis_id if existing_analysis is not None else None)
    notification_plan_id = _payload_uuid(
        notification_outbox.payload_json if notification_outbox is not None else {},
        "notification_plan_id",
    )
    return BoundedPolicyApplyResult(
        status=status,
        ok=error_code is None and status in {"preview", "applied"},
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        messages_seen=messages_seen,
        messages_matched=messages_matched,
        messages_processed_count=messages_processed_count,
        redis_ack_status=redis_ack_status,
        redis_acked_count=redis_acked_count,
        target_redis_message_id_suffix=_optional_id_suffix(selected.redis_message_id if selected else None),
        target_policy_apply_event_suffix=_optional_id_suffix(
            event.event_id if event is not None else selected.trigger_event_id if selected is not None else None
        )
        or config.trigger_event_suffix,
        target_judge_run_id_suffix=_optional_id_suffix(
            judge_run.judge_run_id if judge_run is not None else _payload_uuid(event_payload, "judge_run_id")
        )
        or config.judge_run_suffix,
        target_judge_output_id_suffix=_optional_id_suffix(
            judge_output.judge_output_id if judge_output is not None else _payload_uuid(event_payload, "judge_output_id")
        )
        or config.judge_output_suffix,
        target_bundle_id_suffix=_optional_id_suffix(
            bundle.bundle_id if bundle is not None else _payload_uuid(event_payload, "bundle_id")
        ),
        target_candidate_group_suffix=_optional_id_suffix(
            candidate.candidate_group_id
            if candidate is not None
            else bundle.candidate_group_id
            if bundle is not None
            else _payload_uuid(event_payload, "candidate_group_id")
        ),
        target_analysis_id_suffix=_optional_id_suffix(resolved_analysis_id),
        target_notification_plan_event_suffix=_optional_id_suffix(notification_outbox.event_id if notification_outbox else None),
        target_notification_plan_id_suffix=_optional_id_suffix(notification_plan_id),
        target_message_found=selected is not None,
        event_outbox_found=event is not None,
        judge_run_found=judge_run is not None,
        judge_output_found=judge_output is not None,
        bundle_found=bundle is not None,
        candidate_group_found=candidate is not None,
        existing_analysis_found=existing_analysis is not None,
        analysis_written=analysis_written,
        state_transition_written=state_transition_written,
        notification_plan_intent_outbox_written=notification_outbox_written,
        verdict=analysis.verdict if analysis is not None else None,
        delivery_decision=analysis.delivery_decision if analysis is not None else None,
        urgency_profile=evaluation.urgency_profile if evaluation is not None else None,
        policy_reconciled_flag=evaluation.policy_reconciled_flag if evaluation is not None else None,
        planned_action=planned_action,
        would_fail_closed=would_fail_closed,
    )


def _close_failed_result(
    *,
    existing: BoundedPolicyApplyResult | None,
    error_code: str,
    error_class: str,
    config: BoundedPolicyApplyConfig,
    state: BoundedPolicyApplyState,
) -> BoundedPolicyApplyResult:
    if existing is None:
        return _result("failed", error_code, error_class=error_class, config=config, state=state)
    return replace(existing, status="failed", ok=False, error_code=error_code, error_class=error_class)


def _message_contract_error(selected: TargetPolicyApplyMessage) -> str | None:
    fields = set(selected.fields)
    if FORBIDDEN_STREAM_FIELDS & fields:
        return "redis_message_forbidden_business_fields"
    if EXPECTED_STREAM_FIELDS - fields:
        return "redis_message_required_fields_missing"
    if fields - EXPECTED_STREAM_FIELDS:
        return "redis_message_unexpected_fields"
    if selected.fields.get("stage_name") != STAGE_NAME:
        return "redis_message_wrong_stage"
    if selected.fields.get("root_object_type") != ROOT_OBJECT_TYPE:
        return "redis_message_wrong_root_object_type"
    if _safe_uuid(selected.fields.get("job_id")) is None:
        return "redis_message_invalid_job_id"
    if _safe_uuid(selected.fields.get("trigger_event_id")) is None:
        return "redis_message_invalid_trigger_event_id"
    if _safe_uuid(selected.fields.get("root_object_id")) is None:
        return "redis_message_invalid_root_object_id"
    if selected.fields.get("job_id") != selected.fields.get("trigger_event_id"):
        return "redis_message_job_trigger_mismatch"
    return None


def _group_preflight_error(state: BoundedPolicyApplyState) -> str | None:
    if state.group_exists is not True:
        return "consumer_group_missing"
    if state.group_pending != 0:
        return "consumer_group_pending_nonzero"
    if state.target_after_group_last_delivered is not True:
        return "target_not_after_group_last_delivered"
    if state.target_is_next_deliverable is not True:
        return "target_not_next_deliverable"
    return None


def _event_contract_error(
    event: OutboxEventRow | None,
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
    if _payload_uuid(event.payload_json, "judge_run_id") != root_judge_run_id:
        return "event_payload_judge_run_id_mismatch"
    return None


def _payload_selector_error(
    *,
    config: BoundedPolicyApplyConfig,
    judge_run_id: UUID | None,
    judge_output_id: UUID | None,
) -> str | None:
    if judge_run_id is not None and config.judge_run_suffix and not str(judge_run_id).endswith(config.judge_run_suffix):
        return "judge_run_selector_mismatch"
    if (
        judge_output_id is not None
        and config.judge_output_suffix
        and not str(judge_output_id).endswith(config.judge_output_suffix)
    ):
        return "judge_output_selector_mismatch"
    return None


def _context_error(
    *,
    event: OutboxEventRow,
    candidate: CandidatePolicyContext | None,
    judge_run: JudgeRunPolicyContext | None,
    judge_output: JudgeOutputPolicyContext | None,
    bundle: BundlePolicyContext | None,
    judge_run_id: UUID,
    judge_output_id: UUID,
    candidate_group_id: UUID,
    bundle_id: UUID,
) -> str | None:
    if candidate is None:
        return "candidate_group_missing"
    if judge_run is None:
        return "judge_run_missing"
    if judge_output is None:
        return "judge_output_missing"
    if bundle is None:
        return "bundle_missing"
    if event.aggregate_id != judge_run_id:
        return "event_judge_run_mismatch"
    if judge_run.status != "succeeded":
        return "judge_run_not_succeeded"
    if judge_run.bundle_id != bundle_id:
        return "judge_run_bundle_mismatch"
    if judge_output.judge_output_id != judge_output_id:
        return "judge_output_id_mismatch"
    if judge_output.judge_run_id != judge_run_id:
        return "judge_output_judge_run_mismatch"
    if judge_output.candidate_group_id != candidate_group_id:
        return "judge_output_candidate_group_mismatch"
    if bundle.bundle_id != bundle_id:
        return "bundle_id_mismatch"
    if bundle.candidate_group_id != candidate_group_id:
        return "bundle_candidate_group_mismatch"
    if candidate.current_bundle_id != bundle_id:
        return "stale_bundle"
    if _judge_output_schema_version(judge_output) != "judge_output_v1":
        return "judge_output_schema_invalid"
    if _judge_output_refusal_detected(judge_output.payload_json):
        return "judge_output_refusal_detected"
    return None


def _build_analysis(
    *,
    policy_config: PolicyEngineConfig,
    judge_run: JudgeRunPolicyContext,
    judge_output: JudgeOutputPolicyContext,
    bundle: BundlePolicyContext,
) -> tuple[AnalysisDraft, PolicyEvaluation]:
    payload = judge_output.payload_json if isinstance(judge_output.payload_json, dict) else {}
    scores = payload.get("scores") if isinstance(payload.get("scores"), dict) else {}
    verdict_decision = VerdictPolicy().evaluate(
        scores=scores,
        current_primary_artifact_type=bundle.current_primary_artifact_type,
    )
    delivery_decision = DeliveryPolicy(enable_later_delivery=policy_config.enable_later_delivery).evaluate(
        verdict=verdict_decision.verdict
    )
    reason_codes = [
        *_string_list(payload.get("reason_codes")),
        *verdict_decision.reason_codes,
    ]
    if delivery_decision.suppress_reason_code:
        reason_codes.append(delivery_decision.suppress_reason_code)
    policy_reconciled_flag, reason_codes = reconcile_model_verdict(
        model_proposed_verdict=judge_output.model_proposed_verdict,
        final_verdict=verdict_decision.verdict,
        reason_codes=reason_codes,
    )
    analysis = AnalysisDraft(
        candidate_group_id=judge_output.candidate_group_id,
        judge_output_id=judge_output.judge_output_id,
        schema_version="analysis_v1",
        policy_version=policy_config.policy_version,
        prompt_version=judge_run.prompt_version,
        delivery_policy_version=policy_config.delivery_policy_version,
        verdict=verdict_decision.verdict,
        delivery_decision=delivery_decision.delivery_decision,
        scores_json=scores,
        reason_codes_json=reason_codes,
        evidence_limitations_ko=_text_column_value(payload.get("evidence_limitations_ko")),
        recommended_action_ko=_text_column_value(payload.get("recommended_action_ko")),
        freshness_note_ko=_text_column_value(payload.get("freshness_note_ko")),
        model_proposed_verdict=judge_output.model_proposed_verdict,
        policy_reconciled_flag=policy_reconciled_flag,
    )
    evaluation = PolicyEvaluation(
        verdict=analysis.verdict,
        delivery_decision=analysis.delivery_decision,
        urgency_profile=delivery_decision.urgency_profile,
        reason_codes=reason_codes,
        policy_reconciled_flag=policy_reconciled_flag,
        suppress_reason_code=delivery_decision.suppress_reason_code,
    )
    return analysis, evaluation


def _notification_outbox_error(row: OutboxEventRow | None, *, intent: NotificationPlanIntent) -> str | None:
    if row is None:
        return "notification_plan_intent_outbox_missing"
    if row.event_type != NOTIFICATION_EVENT_TYPE:
        return "notification_plan_intent_wrong_event_type"
    if row.aggregate_type != "analysis":
        return "notification_plan_intent_wrong_aggregate_type"
    if row.aggregate_id != intent.analysis_id:
        return "notification_plan_intent_aggregate_mismatch"
    if row.status != "pending":
        return "notification_plan_intent_not_pending"
    if not isinstance(row.payload_json, dict):
        return "notification_plan_intent_payload_malformed"
    required = {
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
    if required - set(row.payload_json):
        return "notification_plan_intent_payload_missing_required_field"
    if _payload_uuid(row.payload_json, "notification_plan_id") is None:
        return "notification_plan_intent_plan_id_invalid"
    if _payload_uuid(row.payload_json, "analysis_id") != intent.analysis_id:
        return "notification_plan_intent_analysis_mismatch"
    if _payload_uuid(row.payload_json, "candidate_group_id") != intent.candidate_group_id:
        return "notification_plan_intent_candidate_mismatch"
    if row.payload_json.get("material_change_hash") != intent.material_change_hash:
        return "notification_plan_intent_material_hash_mismatch"
    for forbidden in ("message_text", "entities_json", "reply_markup_json", "render_hash"):
        if forbidden in row.payload_json:
            return "notification_plan_intent_payload_contains_rendered_text"
    return None


def _select_target_from_entries(
    entries: list[tuple[str, Mapping[str, Any]]],
    config: BoundedPolicyApplyConfig,
    scan_limit: int,
) -> tuple[TargetPolicyApplyMessage | None, int, int]:
    selected: TargetPolicyApplyMessage | None = None
    messages_seen = 0
    messages_matched = 0
    for message_id, fields in entries[:scan_limit]:
        messages_seen += 1
        decoded = _decode_fields(fields)
        if _matches_target(message_id, decoded, config):
            messages_matched += 1
            selected = TargetPolicyApplyMessage(redis_message_id=message_id, fields=decoded)
    if messages_matched != 1:
        return selected, messages_seen, messages_matched
    return selected, messages_seen, messages_matched


def _matches_target(message_id: str, fields: Mapping[str, str], config: BoundedPolicyApplyConfig) -> bool:
    del message_id
    if fields.get("stage_name") != STAGE_NAME:
        return False
    if fields.get("root_object_type") != ROOT_OBJECT_TYPE:
        return False
    if not str(fields.get("trigger_event_id", "")).endswith(config.trigger_event_suffix or ""):
        return False
    if config.judge_run_suffix and not str(fields.get("root_object_id", "")).endswith(config.judge_run_suffix):
        return False
    return True


def _flatten_direct_stream_entries(raw: Any) -> list[tuple[str, Mapping[str, Any]]]:
    if not isinstance(raw, list):
        return []
    entries: list[tuple[str, Mapping[str, Any]]] = []
    for item in raw:
        if isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], Mapping):
            entries.append((str(_decode_scalar(item[0])), item[1]))
    return entries


def _flatten_group_stream_entries(raw: Any) -> list[tuple[str, Mapping[str, Any]]]:
    if not isinstance(raw, list):
        return []
    entries: list[tuple[str, Mapping[str, Any]]] = []
    for stream_item in raw:
        if not isinstance(stream_item, tuple) or len(stream_item) != 2:
            continue
        _, stream_entries = stream_item
        if not isinstance(stream_entries, list):
            continue
        for entry in stream_entries:
            if isinstance(entry, tuple) and len(entry) == 2 and isinstance(entry[1], Mapping):
                entries.append((str(_decode_scalar(entry[0])), entry[1]))
    return entries


def _decode_fields(fields: Mapping[str, Any]) -> dict[str, str]:
    decoded: dict[str, str] = {}
    for key, value in fields.items():
        decoded[str(_decode_scalar(key))] = str(_decode_scalar(value))
    return decoded


def _decode_scalar(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _find_consumer_group(raw_groups: Any, name: str) -> Mapping[str, Any] | None:
    if not isinstance(raw_groups, list):
        return None
    for group in raw_groups:
        if not isinstance(group, Mapping):
            continue
        raw_name = group.get("name")
        group_name = raw_name.decode("utf-8") if isinstance(raw_name, bytes) else str(raw_name)
        if group_name == name:
            return group
    return None


def _notification_plan_payload(intent: NotificationPlanIntent) -> dict[str, Any]:
    return {
        "notification_plan_id": str(intent.notification_plan_id),
        "analysis_id": str(intent.analysis_id),
        "candidate_group_id": str(intent.candidate_group_id),
        "delivery_decision": intent.delivery_decision,
        "urgency_profile": intent.urgency_profile,
        "render_profile": intent.render_profile,
        "dedupe_subject_key": intent.dedupe_subject_key,
        "material_change_hash": intent.material_change_hash,
        "target_chat_id": intent.target_chat_id,
        "target_thread_id": intent.target_thread_id,
        "send_after": intent.send_after,
        "suppress_reason_code": intent.suppress_reason_code,
    }


def _notification_plan_dedupe_key(intent: NotificationPlanIntent) -> str:
    return f"notification-plan-created:{intent.analysis_id}:{intent.target_chat_id}:{intent.material_change_hash}"


def _judge_output_schema_version(judge_output: JudgeOutputPolicyContext) -> str | None:
    if judge_output.judge_schema_version:
        return judge_output.judge_schema_version
    payload = judge_output.payload_json if isinstance(judge_output.payload_json, dict) else {}
    value = payload.get("judge_schema_version") or payload.get("schema_version")
    return value if isinstance(value, str) else None


def _judge_output_refusal_detected(payload: Mapping[str, Any]) -> bool:
    if payload.get("refusal_detected") is True:
        return True
    if payload.get("refusal") is True:
        return True
    finish_reason = payload.get("finish_reason")
    return isinstance(finish_reason, str) and finish_reason.strip().lower() == "refusal"


def _payload_uuid(payload: Mapping[str, Any], key: str) -> UUID | None:
    return _safe_uuid(payload.get(key))


def _safe_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _valid_uuid_suffix(value: str) -> bool:
    if len(value) < 4 or len(value) > 12:
        return False
    if "-" in value:
        return False
    return all(char in "0123456789abcdef" for char in value.lower())


def _redis_stream_id_greater(left: str, right: str) -> bool:
    left_id = _parse_redis_stream_id(left)
    right_id = _parse_redis_stream_id(right)
    if left_id is None or right_id is None:
        return False
    return left_id > right_id


def _normalize_redis_stream_id(value: str) -> str | None:
    parsed = _parse_redis_stream_id(value)
    if parsed is None:
        return None
    return f"{parsed[0]}-{parsed[1]}"


def _parse_redis_stream_id(value: str | None) -> tuple[int, int] | None:
    if not value or "-" not in value:
        return None
    left, right = value.split("-", 1)
    try:
        return int(left), int(right)
    except ValueError:
        return None


def _optional_id_suffix(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    return str(value)[-8:]


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = value.decode("utf-8") if isinstance(value, bytes) else str(value)
    return text if text else None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _text_column_value(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        lines = [item for item in value if isinstance(item, str)]
        return "\n".join(lines) if lines else None
    return None


def _env_value(source: Mapping[str, str], key: str, default: str = "") -> str:
    return str(source.get(key, default) or "").strip()


def _bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_exception_class(exc: BaseException) -> str:
    return exc.__class__.__name__


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (JSONDecodeError, TypeError):
            return None
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
    "BoundedPolicyApplyConfig",
    "BoundedPolicyApplyError",
    "BoundedPolicyApplyRedisHandle",
    "BoundedPolicyApplyRepositoryHandle",
    "BoundedPolicyApplyResult",
    "BoundedPolicyApplyRuntimeConfig",
    "BoundedPolicyApplyState",
    "RedisPolicyApplyConsumer",
    "SqlAlchemyPolicyApplyRepository",
    "TargetPolicyApplyMessage",
    "argument_error_report",
    "load_runtime_config",
    "render_sanitized_json",
    "run_bounded_policy_apply",
    "run_bounded_policy_apply_sync",
]
