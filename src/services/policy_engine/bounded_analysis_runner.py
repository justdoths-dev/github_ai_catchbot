from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
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
from ..outbox_relay.routing import OutboxRouteResolver, UnsupportedOutboxEventTypeError
from .config import PolicyEngineConfig, PolicyEngineConfigurationError
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
from .verdict_policy import VerdictPolicy, normalize_scores_for_policy, reconcile_model_verdict


SCHEMA_VERSION = "bounded_policy_engine_analysis_v1"
RUNNER_NAME = "bounded_policy_engine_analysis_runner"
MODE = "policy_engine_exact_target_analysis_insert"
INPUT_QUEUE_NAME = "q.analysis.policy"
INPUT_STAGE_NAME = "analysis_policy"
INPUT_EVENT_TYPE = "analysis.policy.apply.v1"
OUTPUT_EVENT_TYPE = "notification.plan.created.v1"
OUTPUT_QUEUE_NAME = "q.notification.send"
OUTPUT_STAGE_NAME = "notify"
ROOT_OBJECT_TYPE = "judge_run"
DEFAULT_SCAN_LIMIT = 25
MAX_SCAN_LIMIT = 500
DEFAULT_XADD_MAXLEN = 10000

UUID_SUFFIX_RE = re.compile(r"^[0-9a-f-]{4,36}$")
REDIS_ID_SUFFIX_RE = re.compile(r"^[0-9-]{3,64}$")
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
        "analysis_id",
        "bundle_data",
        "bundle_id",
        "candidate_group_id",
        "database_url",
        "judge_output_id",
        "judge_output_payload",
        "judge_run_id",
        "notification_plan_id",
        "payload_json",
        "raw_payload",
        "raw_text",
        "redis_url",
        "scores",
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
class BoundedPolicyEngineAnalysisConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_read: bool = False
    allow_redis_publish: bool = False
    allow_database_read: bool = False
    allow_database_write: bool = False
    allow_policy_engine: bool = False
    redis_message_suffix: str | None = None
    trigger_event_suffix: str | None = None
    judge_run_suffix: str | None = None
    judge_output_suffix: str | None = None
    bundle_suffix: str | None = None
    candidate_group_suffix: str | None = None
    scan_limit: int = DEFAULT_SCAN_LIMIT


@dataclass(frozen=True, slots=True)
class BoundedPolicyEngineAnalysisRuntimeConfig:
    database_url: str
    redis_url: str
    input_queue_name: str = INPUT_QUEUE_NAME
    xadd_maxlen: int | None = DEFAULT_XADD_MAXLEN
    policy_version: str = "verdict_policy_v1"
    delivery_policy_version: str = "delivery_policy_v1"
    operator_chat_id: int = 0
    debug_chat_id: int | None = None
    digest_chat_id: int | None = None
    enable_later_delivery: bool = True
    enable_notification_send: bool = True
    render_profile_high: str = "telegram_single_alert_high_v1"
    render_profile_normal: str = "telegram_single_alert_normal_v1"

    def to_policy_config(self) -> PolicyEngineConfig:
        return PolicyEngineConfig(
            app_env="runtime",
            database_url=self.database_url,
            redis_url=self.redis_url,
            queue_name=self.input_queue_name,
            consumer_group="policy-engine",
            consumer_name="bounded-policy-engine-analysis-runner",
            batch_size=1,
            block_ms=1,
            policy_version=self.policy_version,
            delivery_policy_version=self.delivery_policy_version,
            operator_chat_id=self.operator_chat_id,
            enable_later_delivery=self.enable_later_delivery,
            enable_silent_later=True,
            enable_notification_send=self.enable_notification_send,
            render_profile_high=self.render_profile_high,
            render_profile_normal=self.render_profile_normal,
            log_level="INFO",
        )


@dataclass(slots=True)
class BoundedPolicyEngineAnalysisState:
    runtime_config_loaded: bool = False
    redis_reader_created: bool = False
    redis_read_attempted: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    redis_publisher_created: bool = False
    redis_publish_attempted: bool = False
    policy_engine_called: bool = False


@dataclass(frozen=True, slots=True)
class RedisStreamMessage:
    message_id: str
    fields: dict[str, str]


class BoundedPolicyEngineAnalysisError(RuntimeError):
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
        config: BoundedPolicyEngineAnalysisConfig,
    ) -> list[RedisStreamMessage]: ...


class RedisPublisher(Protocol):
    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str: ...


class BoundedPolicyEngineAnalysisRepository(Protocol):
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
    async def mark_notification_plan_intent_outbox_published(
        self,
        *,
        event_id: UUID,
        analysis_id: UUID,
        published_at: datetime,
    ) -> None: ...
    async def insert_publish_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...


@dataclass(frozen=True, slots=True)
class BoundedPolicyEngineRedisReaderHandle:
    reader: ReadOnlyRedisMessageReader
    close: Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedPolicyEngineRepositoryHandle:
    repository: BoundedPolicyEngineAnalysisRepository
    close: Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedPolicyEngineRedisPublisherHandle:
    publisher: RedisPublisher
    close: Callable[[], Awaitable[None]]


class BoundedPolicyEngineRedisReaderBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedPolicyEngineAnalysisRuntimeConfig,
        state: BoundedPolicyEngineAnalysisState,
        logger: logging.Logger,
    ) -> BoundedPolicyEngineRedisReaderHandle: ...


class BoundedPolicyEngineRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedPolicyEngineAnalysisRuntimeConfig,
        state: BoundedPolicyEngineAnalysisState,
        logger: logging.Logger,
    ) -> BoundedPolicyEngineRepositoryHandle: ...


class BoundedPolicyEngineRedisPublisherBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedPolicyEngineAnalysisRuntimeConfig,
        state: BoundedPolicyEngineAnalysisState,
        logger: logging.Logger,
    ) -> BoundedPolicyEngineRedisPublisherHandle: ...


@dataclass(frozen=True, slots=True)
class BoundedPolicyEngineAnalysisResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedPolicyEngineAnalysisConfig
    state: BoundedPolicyEngineAnalysisState = field(default_factory=BoundedPolicyEngineAnalysisState)
    target_redis_message_id_suffix: str | None = None
    target_policy_apply_event_suffix: str | None = None
    target_judge_run_id_suffix: str | None = None
    target_judge_output_id_suffix: str | None = None
    target_bundle_id_suffix: str | None = None
    target_candidate_group_suffix: str | None = None
    target_analysis_id_suffix: str | None = None
    target_notification_plan_event_suffix: str | None = None
    target_notification_plan_id_suffix: str | None = None
    analysis_written: bool = False
    analysis_reused: bool = False
    analysis_id_suffix: str | None = None
    verdict: str | None = None
    delivery_decision: str | None = None
    urgency_profile: str | None = None
    policy_reconciled_flag: bool | None = None
    state_transition_written: bool = False
    notification_plan_intent_outbox_written: bool = False
    notification_plan_intent_published: bool = False
    q_notification_send_message_id_suffix: str | None = None
    redis_message_count: int = 0
    event_outbox_found: bool = False
    judge_run_found: bool = False
    judge_output_found: bool = False
    bundle_found: bool = False
    candidate_group_found: bool = False
    analysis_found: bool = False

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "target_redis_message_id_suffix": self.target_redis_message_id_suffix,
            "target_policy_apply_event_suffix": self.target_policy_apply_event_suffix,
            "target_judge_run_id_suffix": self.target_judge_run_id_suffix,
            "target_judge_output_id_suffix": self.target_judge_output_id_suffix,
            "target_bundle_id_suffix": self.target_bundle_id_suffix,
            "target_candidate_group_suffix": self.target_candidate_group_suffix,
            "target_analysis_id_suffix": self.target_analysis_id_suffix,
            "target_notification_plan_event_suffix": self.target_notification_plan_event_suffix,
            "target_notification_plan_id_suffix": self.target_notification_plan_id_suffix,
            "analysis_written": self.analysis_written,
            "analysis_reused": self.analysis_reused,
            "analysis_id_suffix": self.analysis_id_suffix,
            "verdict": self.verdict,
            "delivery_decision": self.delivery_decision,
            "urgency_profile": self.urgency_profile,
            "policy_reconciled_flag": self.policy_reconciled_flag,
            "state_transition_written": self.state_transition_written,
            "notification_plan_intent_outbox_written": self.notification_plan_intent_outbox_written,
            "notification_plan_intent_published": self.notification_plan_intent_published,
            "q_notification_send_message_id_suffix": self.q_notification_send_message_id_suffix,
            "redis_message_count": self.redis_message_count,
            "event_outbox_found": self.event_outbox_found,
            "judge_run_found": self.judge_run_found,
            "judge_output_found": self.judge_output_found,
            "bundle_found": self.bundle_found,
            "candidate_group_found": self.candidate_group_found,
            "analysis_found": self.analysis_found,
            "redis_read_attempted": self.state.redis_read_attempted,
            "redis_publish_attempted": self.state.redis_publish_attempted,
            "redis_ack_called": False,
            "redis_consume_called": False,
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": self.state.database_write_attempted,
            "policy_engine_called": self.state.policy_engine_called,
            "notifier_called": False,
            "telegram_send_called": False,
            "openai_called": False,
            "github_api_called": False,
            "x_api_called": False,
            "web_fetch_called": False,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "redis_read_allowed": self.config.allow_redis_read,
                "redis_publish_allowed": self.config.allow_redis_publish,
                "database_read_allowed": self.config.allow_database_read,
                "database_write_allowed": self.config.allow_database_write,
                "policy_engine_allowed": self.config.allow_policy_engine,
                "scan_limit": self.config.scan_limit,
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
                "bundle_context_omitted": True,
                "raw_source_text_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "sql_text_omitted": True,
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
        config: BoundedPolicyEngineAnalysisConfig,
    ) -> list[RedisStreamMessage]:
        raw_messages = await self._client.xrevrange(queue_name, count=config.scan_limit)
        return [_normalize_redis_message(message) for message in raw_messages]


class SqlAlchemyBoundedPolicyEngineAnalysisRepository:
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
            raise BoundedPolicyEngineAnalysisError("notification_plan_intent_outbox_missing")
        return _outbox_row_from_mapping(row), bool(row["inserted"])

    async def mark_notification_plan_intent_outbox_published(
        self,
        *,
        event_id: UUID,
        analysis_id: UUID,
        published_at: datetime,
    ) -> None:
        await self._session.execute(
            _sql(
                """
                UPDATE event_outbox
                SET status = 'published'::outbox_status_enum,
                    published_at = :published_at,
                    last_error = NULL
                WHERE event_id = CAST(:event_id AS uuid)
                  AND event_type = 'notification.plan.created.v1'
                  AND aggregate_type = 'analysis'
                  AND aggregate_id = CAST(:analysis_id AS uuid)
                """
            ),
            {
                "event_id": str(event_id),
                "analysis_id": str(analysis_id),
                "published_at": published_at,
            },
        )

    async def insert_publish_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None:
        await self._session.execute(
            _sql(
                """
                INSERT INTO job_attempts (
                    stage_name,
                    queue_name,
                    root_object_type,
                    root_object_id,
                    attempt_no,
                    lease_owner,
                    started_at,
                    finished_at,
                    attempt_status,
                    error_code,
                    retry_after_at
                ) VALUES (
                    :stage_name,
                    :queue_name,
                    :root_object_type,
                    CAST(:root_object_id AS uuid),
                    1,
                    NULL,
                    now(),
                    now(),
                    CAST(:attempt_status AS job_attempt_status_enum),
                    :error_code,
                    NULL
                )
                """
            ),
            {
                "stage_name": stage_name,
                "queue_name": queue_name,
                "root_object_type": root_object_type,
                "root_object_id": str(root_object_id),
                "attempt_status": attempt_status,
                "error_code": error_code,
            },
        )

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()


def load_bounded_policy_engine_analysis_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedPolicyEngineAnalysisRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedPolicyEngineAnalysisError("database_url_missing")
    if not redis_url:
        raise BoundedPolicyEngineAnalysisError("redis_url_missing")
    input_queue_name = _env_value(source, "POLICY_ENGINE_QUEUE_NAME", INPUT_QUEUE_NAME)
    if input_queue_name != INPUT_QUEUE_NAME:
        raise BoundedPolicyEngineAnalysisError("queue_not_allowed")
    xadd_maxlen_raw = _env_value(source, "OUTBOX_RELAY_XADD_MAXLEN", str(DEFAULT_XADD_MAXLEN))
    try:
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
        operator_chat_id = int(_env_value(source, "TELEGRAM_OPERATOR_CHAT_ID", "0"))
        debug_chat_id = _optional_int(_env_value(source, "TELEGRAM_DEBUG_CHAT_ID"))
        digest_chat_id = _optional_int(_env_value(source, "TELEGRAM_DIGEST_CHAT_ID"))
    except ValueError as exc:
        raise BoundedPolicyEngineAnalysisError("runtime_config_error") from exc
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        raise BoundedPolicyEngineAnalysisError("runtime_config_error")
    config = BoundedPolicyEngineAnalysisRuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        input_queue_name=input_queue_name,
        xadd_maxlen=xadd_maxlen,
        policy_version=_env_value(source, "VERDICT_POLICY_VERSION", "verdict_policy_v1"),
        delivery_policy_version=_env_value(source, "DELIVERY_POLICY_VERSION", "delivery_policy_v1"),
        operator_chat_id=operator_chat_id,
        debug_chat_id=debug_chat_id,
        digest_chat_id=digest_chat_id,
        enable_later_delivery=_bool_env(_env_value(source, "ENABLE_LATER_DELIVERY", "true")),
        enable_notification_send=_bool_env(_env_value(source, "ENABLE_NOTIFICATION_SEND", "true")),
        render_profile_high=_env_value(source, "NOTIFY_RENDER_PROFILE_HIGH", "telegram_single_alert_high_v1"),
        render_profile_normal=_env_value(source, "NOTIFY_RENDER_PROFILE_NORMAL", "telegram_single_alert_normal_v1"),
    )
    try:
        config.to_policy_config().validate()
    except PolicyEngineConfigurationError as exc:
        raise BoundedPolicyEngineAnalysisError("runtime_config_error") from exc
    return config


async def build_default_bounded_policy_engine_redis_reader(
    runtime_config: BoundedPolicyEngineAnalysisRuntimeConfig,
    state: BoundedPolicyEngineAnalysisState,
    logger: logging.Logger,
) -> BoundedPolicyEngineRedisReaderHandle:
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

    return BoundedPolicyEngineRedisReaderHandle(reader=reader, close=close)


async def build_default_bounded_policy_engine_repository(
    runtime_config: BoundedPolicyEngineAnalysisRuntimeConfig,
    state: BoundedPolicyEngineAnalysisState,
    logger: logging.Logger,
) -> BoundedPolicyEngineRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedPolicyEngineAnalysisRepository(session)

    async def close() -> None:
        await session.close()
        await engine.dispose()

    return BoundedPolicyEngineRepositoryHandle(repository=repository, close=close)


async def build_default_bounded_policy_engine_redis_publisher(
    runtime_config: BoundedPolicyEngineAnalysisRuntimeConfig,
    state: BoundedPolicyEngineAnalysisState,
    logger: logging.Logger,
) -> BoundedPolicyEngineRedisPublisherHandle:
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

    return BoundedPolicyEngineRedisPublisherHandle(publisher=publisher, close=close)


async def run_bounded_policy_engine_analysis(
    config: BoundedPolicyEngineAnalysisConfig,
    *,
    runtime_config_loader: Callable[[], BoundedPolicyEngineAnalysisRuntimeConfig] = (
        load_bounded_policy_engine_analysis_runtime_config
    ),
    redis_reader_builder: BoundedPolicyEngineRedisReaderBuilder | None = None,
    repository_builder: BoundedPolicyEngineRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedPolicyEngineRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedPolicyEngineAnalysisResult:
    state = BoundedPolicyEngineAnalysisState()
    gate_error = _authority_gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedPolicyEngineAnalysisError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception as exc:
        return _result(
            "blocked",
            "runtime_config_error",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
        )

    redis_handle: BoundedPolicyEngineRedisReaderHandle | None = None
    repository_handle: BoundedPolicyEngineRepositoryHandle | None = None
    publisher_handle: BoundedPolicyEngineRedisPublisherHandle | None = None
    result: BoundedPolicyEngineAnalysisResult | None = None
    redis_message: RedisStreamMessage | None = None
    event: OutboxEventRow | None = None
    candidate: CandidatePolicyContext | None = None
    judge_run: JudgeRunPolicyContext | None = None
    judge_output: JudgeOutputPolicyContext | None = None
    bundle: BundlePolicyContext | None = None
    existing_analysis: ExistingAnalysisRecord | None = None
    analysis_id: UUID | None = None
    analysis_draft: AnalysisDraft | None = None
    evaluation: PolicyEvaluation | None = None
    notification_intent: NotificationPlanIntent | None = None
    notification_outbox: OutboxEventRow | None = None
    redis_output_message_id: str | None = None
    analysis_written = False
    analysis_reused = False
    state_transition_written = False
    notification_outbox_written = False
    notification_outbox_published = False
    redis_message_count = 0
    try:
        redis_handle = await (redis_reader_builder or build_default_bounded_policy_engine_redis_reader)(
            runtime_config,
            state,
            effective_logger,
        )
        state.redis_read_attempted = True
        candidates = await redis_handle.reader.read_candidate_messages(
            queue_name=runtime_config.input_queue_name,
            config=config,
        )
        matches = [message for message in candidates if _message_matches_selectors(message, config)]
        if not matches:
            result = _result("blocked", "redis_message_not_found", config=config, state=state)
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
        redis_message_count = 1
        redis_error = _validate_redis_message(redis_message)
        if redis_error is not None:
            result = _result(
                "blocked",
                redis_error,
                config=config,
                state=state,
                redis_message=redis_message,
                redis_message_count=redis_message_count,
            )
            raise _BoundedResultReady

        trigger_event_id = UUID(redis_message.fields["trigger_event_id"])
        root_judge_run_id = UUID(redis_message.fields["root_object_id"])

        repository_handle = await (repository_builder or build_default_bounded_policy_engine_repository)(
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
                redis_message_count=redis_message_count,
            )
            raise _BoundedResultReady

        assert event is not None
        payload_judge_run_id = _payload_uuid(event.payload_json, "judge_run_id")
        payload_judge_output_id = _payload_uuid(event.payload_json, "judge_output_id")
        payload_candidate_group_id = _payload_uuid(event.payload_json, "candidate_group_id")
        payload_bundle_id = _payload_uuid(event.payload_json, "bundle_id")
        selector_error = _validate_payload_selectors(
            config=config,
            judge_run_id=payload_judge_run_id,
            judge_output_id=payload_judge_output_id,
            candidate_group_id=payload_candidate_group_id,
            bundle_id=payload_bundle_id,
        )
        if selector_error is not None:
            result = _result(
                "blocked",
                selector_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                redis_message_count=redis_message_count,
            )
            raise _BoundedResultReady

        assert payload_judge_run_id is not None
        assert payload_judge_output_id is not None
        assert payload_candidate_group_id is not None
        assert payload_bundle_id is not None

        candidate = await repository.load_candidate_context(payload_candidate_group_id)
        judge_run = await repository.load_judge_run(payload_judge_run_id)
        judge_output = await repository.load_judge_output(payload_judge_output_id)
        bundle = await repository.load_bundle_context(payload_bundle_id)
        context_error = _validate_context(
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
                "blocked" if context_error != "stale_bundle" else "noop",
                context_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                redis_message_count=redis_message_count,
            )
            raise _BoundedResultReady

        assert candidate is not None
        assert judge_run is not None
        assert judge_output is not None
        assert bundle is not None

        state.policy_engine_called = True
        analysis_draft, evaluation = _build_analysis(
            policy_config=runtime_config.to_policy_config(),
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
        )
        existing_analysis = await repository.load_existing_analysis(
            judge_output_id=judge_output.judge_output_id,
            policy_version=analysis_draft.policy_version,
            delivery_policy_version=analysis_draft.delivery_policy_version,
        )
        analysis_reused = existing_analysis is not None
        if existing_analysis is not None:
            analysis_id = existing_analysis.analysis_id
        else:
            state.database_write_attempted = True
            analysis_id = await repository.insert_analysis(analysis_draft)
            analysis_written = True
            await repository.insert_state_transition(
                object_type="analysis",
                object_id=analysis_id,
                from_state="analysis_validated",
                to_state="analysis_policy_applied",
                reason_code="policy_engine_applied",
            )
            state_transition_written = True

        if analysis_draft.delivery_decision != "suppress":
            notification_intent = NotificationIntentBuilder(config=runtime_config.to_policy_config()).build(
                analysis_id=analysis_id,
                analysis=analysis_draft,
                evaluation=evaluation,
            )

        if notification_intent is not None:
            matching_outboxes = await repository.load_notification_plan_intent_outboxes(notification_intent)
            if len(matching_outboxes) > 1:
                result = _result(
                    "blocked",
                    "duplicate_notification_plan_intent_outbox",
                    config=config,
                    state=state,
                    redis_message=redis_message,
                    event=event,
                    candidate=candidate,
                    judge_run=judge_run,
                    judge_output=judge_output,
                    bundle=bundle,
                    existing_analysis=existing_analysis,
                    analysis_id=analysis_id,
                    analysis_written=analysis_written,
                    analysis_reused=analysis_reused,
                    analysis=analysis_draft,
                    evaluation=evaluation,
                    state_transition_written=state_transition_written,
                    redis_message_count=redis_message_count,
                )
                raise _BoundedResultReady
            if matching_outboxes:
                notification_outbox = matching_outboxes[0]
            else:
                state.database_write_attempted = True
                notification_outbox, notification_outbox_written = (
                    await repository.insert_or_load_notification_plan_intent_outbox(notification_intent)
                )
                outbox_error = _validate_notification_outbox(notification_outbox, intent=notification_intent)
                if outbox_error is not None:
                    result = _result(
                        "blocked",
                        outbox_error,
                        config=config,
                        state=state,
                        redis_message=redis_message,
                        event=event,
                        candidate=candidate,
                        judge_run=judge_run,
                        judge_output=judge_output,
                        bundle=bundle,
                        existing_analysis=existing_analysis,
                        analysis_id=analysis_id,
                        analysis_written=analysis_written,
                        analysis_reused=analysis_reused,
                        analysis=analysis_draft,
                        evaluation=evaluation,
                        notification_intent=notification_intent,
                        notification_outbox=notification_outbox,
                        notification_outbox_written=notification_outbox_written,
                        state_transition_written=state_transition_written,
                        redis_message_count=redis_message_count,
                    )
                    raise _BoundedResultReady

        if analysis_written or notification_outbox_written:
            try:
                await repository.commit()
            except Exception as exc:
                result = _result(
                    "failed",
                    "database_commit_failed_before_redis_publish",
                    error_class=_safe_exception_class(exc),
                    config=config,
                    state=state,
                    redis_message=redis_message,
                    event=event,
                    candidate=candidate,
                    judge_run=judge_run,
                    judge_output=judge_output,
                    bundle=bundle,
                    existing_analysis=existing_analysis,
                    analysis_id=analysis_id,
                    analysis_written=analysis_written,
                    analysis_reused=analysis_reused,
                    analysis=analysis_draft,
                    evaluation=evaluation,
                    notification_intent=notification_intent,
                    notification_outbox=notification_outbox,
                    notification_outbox_written=notification_outbox_written,
                    state_transition_written=state_transition_written,
                    redis_message_count=redis_message_count,
                )
                raise _BoundedResultReady

        if notification_outbox is None:
            result = _result(
                "applied",
                None,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                existing_analysis=existing_analysis,
                analysis_id=analysis_id,
                analysis_written=analysis_written,
                analysis_reused=analysis_reused,
                analysis=analysis_draft,
                evaluation=evaluation,
                state_transition_written=state_transition_written,
                redis_message_count=redis_message_count,
            )
            raise _BoundedResultReady

        notification_outbox_error = _validate_notification_outbox(notification_outbox, intent=notification_intent)
        if notification_outbox_error is not None:
            result = _result(
                "blocked",
                notification_outbox_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                existing_analysis=existing_analysis,
                analysis_id=analysis_id,
                analysis_written=analysis_written,
                analysis_reused=analysis_reused,
                analysis=analysis_draft,
                evaluation=evaluation,
                notification_intent=notification_intent,
                notification_outbox=notification_outbox,
                notification_outbox_written=notification_outbox_written,
                state_transition_written=state_transition_written,
                redis_message_count=redis_message_count,
            )
            raise _BoundedResultReady

        if notification_outbox.status == "published":
            notification_outbox_published = True
            result = _result(
                "noop",
                None,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                existing_analysis=existing_analysis,
                analysis_id=analysis_id,
                analysis_written=analysis_written,
                analysis_reused=analysis_reused,
                analysis=analysis_draft,
                evaluation=evaluation,
                notification_intent=notification_intent,
                notification_outbox=notification_outbox,
                notification_outbox_written=notification_outbox_written,
                notification_outbox_published=notification_outbox_published,
                state_transition_written=state_transition_written,
                redis_message_count=redis_message_count,
            )
            raise _BoundedResultReady
        if notification_outbox.status != "pending":
            result = _result(
                "blocked",
                "notification_plan_intent_outbox_not_pending",
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                existing_analysis=existing_analysis,
                analysis_id=analysis_id,
                analysis_written=analysis_written,
                analysis_reused=analysis_reused,
                analysis=analysis_draft,
                evaluation=evaluation,
                notification_intent=notification_intent,
                notification_outbox=notification_outbox,
                notification_outbox_written=notification_outbox_written,
                state_transition_written=state_transition_written,
                redis_message_count=redis_message_count,
            )
            raise _BoundedResultReady

        route = _resolve_notification_route(notification_outbox, route_resolver=route_resolver)
        publisher_handle = await (redis_publisher_builder or build_default_bounded_policy_engine_redis_publisher)(
            runtime_config,
            state,
            effective_logger,
        )
        message = _build_notification_stream_message(notification_outbox, route)
        state.redis_publish_attempted = True
        redis_output_message_id = await publisher_handle.publisher.publish(route, message)
        state.database_write_attempted = True
        await repository.mark_notification_plan_intent_outbox_published(
            event_id=notification_outbox.event_id,
            analysis_id=analysis_id,
            published_at=(clock or _utc_now)(),
        )
        await repository.insert_publish_job_attempt(
            stage_name=route.stage_name,
            queue_name=route.queue_name,
            root_object_type=notification_outbox.aggregate_type,
            root_object_id=notification_outbox.aggregate_id,
            attempt_status="succeeded",
            error_code=None,
        )
        try:
            await repository.commit()
        except Exception as exc:
            result = _result(
                "failed",
                "database_commit_failed_after_redis_publish",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                existing_analysis=existing_analysis,
                analysis_id=analysis_id,
                analysis_written=analysis_written,
                analysis_reused=analysis_reused,
                analysis=analysis_draft,
                evaluation=evaluation,
                notification_intent=notification_intent,
                notification_outbox=notification_outbox,
                notification_outbox_written=notification_outbox_written,
                state_transition_written=state_transition_written,
                redis_output_message_id=redis_output_message_id,
                redis_message_count=redis_message_count,
            )
            raise _BoundedResultReady
        notification_outbox_published = True
        result = _result(
            "published",
            None,
            config=config,
            state=state,
            redis_message=redis_message,
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            existing_analysis=existing_analysis,
            analysis_id=analysis_id,
            analysis_written=analysis_written,
            analysis_reused=analysis_reused,
            analysis=analysis_draft,
            evaluation=evaluation,
            notification_intent=notification_intent,
            notification_outbox=replace(notification_outbox, status="published"),
            notification_outbox_written=notification_outbox_written,
            notification_outbox_published=notification_outbox_published,
            state_transition_written=state_transition_written,
            redis_output_message_id=redis_output_message_id,
            redis_message_count=redis_message_count,
        )
    except _BoundedResultReady:
        pass
    except UnsupportedOutboxEventTypeError as exc:
        result = _result(
            "blocked",
            "route_not_allowed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            redis_message=redis_message,
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            existing_analysis=existing_analysis,
            analysis_id=analysis_id,
            analysis_written=analysis_written,
            analysis_reused=analysis_reused,
            analysis=analysis_draft,
            evaluation=evaluation,
            notification_intent=notification_intent,
            notification_outbox=notification_outbox,
            notification_outbox_written=notification_outbox_written,
            notification_outbox_published=notification_outbox_published,
            state_transition_written=state_transition_written,
            redis_output_message_id=redis_output_message_id,
            redis_message_count=redis_message_count,
        )
    except Exception as exc:
        error_code = "redis_xadd_failed" if state.redis_publish_attempted else "bounded_policy_engine_analysis_failed"
        result = _result(
            "failed",
            error_code,
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            redis_message=redis_message,
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            existing_analysis=existing_analysis,
            analysis_id=analysis_id,
            analysis_written=analysis_written,
            analysis_reused=analysis_reused,
            analysis=analysis_draft,
            evaluation=evaluation,
            notification_intent=notification_intent,
            notification_outbox=notification_outbox,
            notification_outbox_written=notification_outbox_written,
            notification_outbox_published=notification_outbox_published,
            state_transition_written=state_transition_written,
            redis_output_message_id=redis_output_message_id,
            redis_message_count=redis_message_count,
        )
    finally:
        if publisher_handle is not None:
            try:
                await publisher_handle.close()
            except Exception as exc:
                result = _close_failed_result(
                    existing=result,
                    error_code="redis_publisher_close_failed",
                    error_class=_safe_exception_class(exc),
                    config=config,
                    state=state,
                )
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
                    error_code="redis_reader_close_failed",
                    error_class=_safe_exception_class(exc),
                    config=config,
                    state=state,
                )

    assert result is not None
    return result


def run_bounded_policy_engine_analysis_sync(
    config: BoundedPolicyEngineAnalysisConfig,
    *,
    runtime_config_loader: Callable[[], BoundedPolicyEngineAnalysisRuntimeConfig] = (
        load_bounded_policy_engine_analysis_runtime_config
    ),
    redis_reader_builder: BoundedPolicyEngineRedisReaderBuilder | None = None,
    repository_builder: BoundedPolicyEngineRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedPolicyEngineRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedPolicyEngineAnalysisResult:
    return asyncio.run(
        run_bounded_policy_engine_analysis(
            config,
            runtime_config_loader=runtime_config_loader,
            redis_reader_builder=redis_reader_builder,
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
        config=BoundedPolicyEngineAnalysisConfig(),
        state=BoundedPolicyEngineAnalysisState(),
    ).to_sanitized_dict()


def _authority_gate_error(config: BoundedPolicyEngineAnalysisConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    if not _valid_scan_limit(config.scan_limit):
        return "invalid_scan_limit"
    if not all(
        [
            config.redis_message_suffix,
            config.trigger_event_suffix,
            config.judge_run_suffix,
            config.judge_output_suffix,
            config.bundle_suffix,
            config.candidate_group_suffix,
        ]
    ):
        return "target_missing"
    if not REDIS_ID_SUFFIX_RE.fullmatch(config.redis_message_suffix or ""):
        return "invalid_redis_message_suffix"
    for value, error_code in (
        (config.trigger_event_suffix, "invalid_trigger_event_suffix"),
        (config.judge_run_suffix, "invalid_judge_run_suffix"),
        (config.judge_output_suffix, "invalid_judge_output_suffix"),
        (config.bundle_suffix, "invalid_bundle_suffix"),
        (config.candidate_group_suffix, "invalid_candidate_group_suffix"),
    ):
        if not UUID_SUFFIX_RE.fullmatch(value or ""):
            return error_code
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_redis_read:
        return "redis_read_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if not config.allow_database_write:
        return "database_write_not_allowed"
    if not config.allow_redis_publish:
        return "redis_publish_not_allowed"
    if not config.allow_policy_engine:
        return "policy_engine_not_allowed"
    return None


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedPolicyEngineAnalysisConfig,
    state: BoundedPolicyEngineAnalysisState,
    error_class: str | None = None,
    redis_message: RedisStreamMessage | None = None,
    event: OutboxEventRow | None = None,
    candidate: CandidatePolicyContext | None = None,
    judge_run: JudgeRunPolicyContext | None = None,
    judge_output: JudgeOutputPolicyContext | None = None,
    bundle: BundlePolicyContext | None = None,
    existing_analysis: ExistingAnalysisRecord | None = None,
    analysis_id: UUID | None = None,
    analysis_written: bool = False,
    analysis_reused: bool = False,
    analysis: AnalysisDraft | None = None,
    evaluation: PolicyEvaluation | None = None,
    notification_intent: NotificationPlanIntent | None = None,
    notification_outbox: OutboxEventRow | None = None,
    notification_outbox_written: bool = False,
    notification_outbox_published: bool = False,
    state_transition_written: bool = False,
    redis_output_message_id: str | None = None,
    redis_message_count: int = 0,
) -> BoundedPolicyEngineAnalysisResult:
    event_payload = event.payload_json if event is not None and isinstance(event.payload_json, Mapping) else {}
    resolved_analysis_id = analysis_id or (existing_analysis.analysis_id if existing_analysis is not None else None)
    notification_plan_id = _payload_uuid(
        notification_outbox.payload_json if notification_outbox is not None else {},
        "notification_plan_id",
    ) or (notification_intent.notification_plan_id if notification_intent is not None else None)
    return BoundedPolicyEngineAnalysisResult(
        status=status,
        ok=status in {"applied", "published", "noop"} and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        target_redis_message_id_suffix=_redis_message_id_suffix(
            redis_message.message_id if redis_message is not None else None
        )
        or config.redis_message_suffix,
        target_policy_apply_event_suffix=_optional_id_suffix(
            event.event_id if event is not None else _safe_uuid(redis_message.fields.get("trigger_event_id")) if redis_message else None
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
        )
        or config.bundle_suffix,
        target_candidate_group_suffix=_optional_id_suffix(
            candidate.candidate_group_id
            if candidate is not None
            else bundle.candidate_group_id
            if bundle is not None
            else _payload_uuid(event_payload, "candidate_group_id")
        )
        or config.candidate_group_suffix,
        target_analysis_id_suffix=_optional_id_suffix(resolved_analysis_id),
        target_notification_plan_event_suffix=_optional_id_suffix(notification_outbox.event_id if notification_outbox else None),
        target_notification_plan_id_suffix=_optional_id_suffix(notification_plan_id),
        analysis_written=analysis_written,
        analysis_reused=analysis_reused,
        analysis_id_suffix=_optional_id_suffix(resolved_analysis_id),
        verdict=analysis.verdict if analysis is not None else None,
        delivery_decision=analysis.delivery_decision if analysis is not None else None,
        urgency_profile=evaluation.urgency_profile if evaluation is not None else None,
        policy_reconciled_flag=evaluation.policy_reconciled_flag if evaluation is not None else None,
        state_transition_written=state_transition_written,
        notification_plan_intent_outbox_written=notification_outbox_written,
        notification_plan_intent_published=notification_outbox_published,
        q_notification_send_message_id_suffix=_redis_message_id_suffix(redis_output_message_id),
        redis_message_count=redis_message_count,
        event_outbox_found=event is not None,
        judge_run_found=judge_run is not None,
        judge_output_found=judge_output is not None,
        bundle_found=bundle is not None,
        candidate_group_found=candidate is not None,
        analysis_found=(resolved_analysis_id is not None),
    )


def _close_failed_result(
    *,
    existing: BoundedPolicyEngineAnalysisResult | None,
    error_code: str,
    error_class: str,
    config: BoundedPolicyEngineAnalysisConfig,
    state: BoundedPolicyEngineAnalysisState,
) -> BoundedPolicyEngineAnalysisResult:
    if existing is None:
        return _result("failed", error_code, error_class=error_class, config=config, state=state)
    return replace(existing, status="failed", ok=False, error_code=error_code, error_class=error_class)


def _validate_redis_message(message: RedisStreamMessage) -> str | None:
    field_names = set(message.fields)
    if FORBIDDEN_REDIS_BUSINESS_FIELDS & field_names:
        return "redis_message_forbidden_business_fields"
    if REQUIRED_REDIS_FIELDS - field_names:
        return "redis_message_required_fields_missing"
    if field_names - REQUIRED_REDIS_FIELDS:
        return "redis_message_unexpected_fields"
    if message.fields.get("stage_name") != INPUT_STAGE_NAME:
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
    event: OutboxEventRow | None,
    *,
    trigger_event_id: UUID,
    root_judge_run_id: UUID,
) -> str | None:
    if event is None:
        return "event_outbox_missing"
    if event.event_id != trigger_event_id:
        return "event_outbox_id_mismatch"
    if event.event_type != INPUT_EVENT_TYPE:
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
    if payload_judge_run_id is None:
        return "event_payload_malformed"
    if payload_judge_run_id != event.aggregate_id:
        return "event_payload_judge_run_id_mismatch"
    return None


def _validate_payload_selectors(
    *,
    config: BoundedPolicyEngineAnalysisConfig,
    judge_run_id: UUID | None,
    judge_output_id: UUID | None,
    candidate_group_id: UUID | None,
    bundle_id: UUID | None,
) -> str | None:
    if None in {judge_run_id, judge_output_id, candidate_group_id, bundle_id}:
        return "event_payload_malformed"
    assert judge_run_id is not None
    assert judge_output_id is not None
    assert candidate_group_id is not None
    assert bundle_id is not None
    if not str(judge_run_id).endswith(config.judge_run_suffix or ""):
        return "judge_run_selector_mismatch"
    if not str(judge_output_id).endswith(config.judge_output_suffix or ""):
        return "judge_output_selector_mismatch"
    if not str(candidate_group_id).endswith(config.candidate_group_suffix or ""):
        return "candidate_group_selector_mismatch"
    if not str(bundle_id).endswith(config.bundle_suffix or ""):
        return "bundle_selector_mismatch"
    return None


def _validate_context(
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
    if judge_run.judge_run_id != judge_run_id:
        return "judge_run_id_mismatch"
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
    scores, score_scale_normalized = normalize_scores_for_policy(
        scores,
        model_proposed_verdict=judge_output.model_proposed_verdict,
    )
    verdict_decision = VerdictPolicy().evaluate(
        scores=scores,
        current_primary_artifact_type=bundle.current_primary_artifact_type,
    )
    delivery_decision = DeliveryPolicy(enable_later_delivery=policy_config.enable_later_delivery).evaluate(
        verdict=verdict_decision.verdict
    )
    reason_codes = [
        *_string_list(payload.get("reason_codes")),
    ]
    if score_scale_normalized:
        reason_codes.append("policy_score_scale_normalized_0_10_to_0_100")
    reason_codes.extend(verdict_decision.reason_codes)
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


def _validate_notification_outbox(
    row: OutboxEventRow | None,
    *,
    intent: NotificationPlanIntent | None,
) -> str | None:
    if row is None:
        return "notification_plan_intent_outbox_missing"
    if intent is None:
        return "notification_plan_intent_missing"
    if row.event_type != OUTPUT_EVENT_TYPE:
        return "notification_plan_intent_wrong_event_type"
    if row.aggregate_type != "analysis":
        return "notification_plan_intent_wrong_aggregate_type"
    if row.aggregate_id != intent.analysis_id:
        return "notification_plan_intent_aggregate_mismatch"
    if not isinstance(row.payload_json, dict):
        return "notification_plan_intent_payload_malformed"
    required = {
        "notification_plan_id",
        "analysis_id",
        "candidate_group_id",
        "delivery_decision",
        "urgency_profile",
        "target_chat_id",
        "target_thread_id",
        "render_profile",
        "dedupe_subject_key",
        "material_change_hash",
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
    return None


def _resolve_notification_route(
    row: OutboxEventRow,
    *,
    route_resolver: OutboxRouteResolver | None = None,
) -> QueueRoute:
    resolver = route_resolver or OutboxRouteResolver()
    route = resolver.resolve(row)
    if route.queue_name != OUTPUT_QUEUE_NAME or route.stage_name != OUTPUT_STAGE_NAME:
        raise UnsupportedOutboxEventTypeError("notification_plan_route_not_allowed")
    return route


def _build_notification_stream_message(row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
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


def _message_matches_selectors(
    message: RedisStreamMessage,
    config: BoundedPolicyEngineAnalysisConfig,
) -> bool:
    return (
        message.message_id.endswith(config.redis_message_suffix or "")
        and str(message.fields.get("trigger_event_id", "")).endswith(config.trigger_event_suffix or "")
        and str(message.fields.get("root_object_id", "")).endswith(config.judge_run_suffix or "")
        and message.fields.get("stage_name") == INPUT_STAGE_NAME
        and message.fields.get("root_object_type") == ROOT_OBJECT_TYPE
    )


def _normalize_redis_message(raw_message: Any) -> RedisStreamMessage:
    if isinstance(raw_message, tuple) and len(raw_message) == 2:
        message_id, fields = raw_message
    else:
        message_id = getattr(raw_message, "message_id", "")
        fields = getattr(raw_message, "fields", {})
    if isinstance(message_id, bytes):
        message_id = message_id.decode("utf-8")
    normalized_fields: dict[str, str] = {}
    if isinstance(fields, Mapping):
        for key, value in fields.items():
            field_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            if isinstance(value, bytes):
                normalized_fields[field_key] = value.decode("utf-8")
            else:
                normalized_fields[field_key] = str(value)
    return RedisStreamMessage(message_id=str(message_id), fields=normalized_fields)


def _notification_plan_payload(intent: NotificationPlanIntent) -> dict[str, Any]:
    return {
        "notification_plan_id": str(intent.notification_plan_id),
        "analysis_id": str(intent.analysis_id),
        "candidate_group_id": str(intent.candidate_group_id),
        "delivery_decision": intent.delivery_decision,
        "urgency_profile": intent.urgency_profile,
        "target_chat_id": intent.target_chat_id,
        "target_thread_id": intent.target_thread_id,
        "render_profile": intent.render_profile,
        "dedupe_subject_key": intent.dedupe_subject_key,
        "material_change_hash": intent.material_change_hash,
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


def _optional_id_suffix(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    return str(value)[-8:]


def _redis_message_id_suffix(value: str | None) -> str | None:
    if not value:
        return None
    return value[-8:]


def _valid_scan_limit(scan_limit: int) -> bool:
    return 1 <= scan_limit <= MAX_SCAN_LIMIT


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


def _optional_int(value: str) -> int | None:
    if not value:
        return None
    return int(value)


def _bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
    "BoundedPolicyEngineAnalysisConfig",
    "BoundedPolicyEngineAnalysisError",
    "BoundedPolicyEngineAnalysisRepository",
    "BoundedPolicyEngineAnalysisResult",
    "BoundedPolicyEngineAnalysisRuntimeConfig",
    "BoundedPolicyEngineAnalysisState",
    "BoundedPolicyEngineRedisPublisherBuilder",
    "BoundedPolicyEngineRedisPublisherHandle",
    "BoundedPolicyEngineRedisReaderBuilder",
    "BoundedPolicyEngineRedisReaderHandle",
    "BoundedPolicyEngineRepositoryBuilder",
    "BoundedPolicyEngineRepositoryHandle",
    "RedisStreamMessage",
    "SqlAlchemyBoundedPolicyEngineAnalysisRepository",
    "argument_error_report",
    "load_bounded_policy_engine_analysis_runtime_config",
    "render_sanitized_json",
    "run_bounded_policy_engine_analysis",
    "run_bounded_policy_engine_analysis_sync",
]
