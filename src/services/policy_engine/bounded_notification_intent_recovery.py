from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from json import JSONDecodeError
from typing import Any, Protocol
from uuid import UUID

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - local fallback for static validation
    sa = None

from ..outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from ..outbox_relay.redis_streams import RedisStreamsPublisher
from ..outbox_relay.routing import OutboxRouteResolver
from .bounded_analysis_runner import (
    OUTPUT_EVENT_TYPE,
    OUTPUT_QUEUE_NAME,
    OUTPUT_STAGE_NAME,
    _build_notification_stream_message,
    _jsonb_dumps,
    _notification_plan_dedupe_key,
    _notification_plan_payload,
    _optional_id_suffix,
    _outbox_row_from_mapping,
    _payload_uuid,
    _redis_message_id_suffix,
    _safe_exception_class,
    _sql,
)
from .config import PolicyEngineConfig
from .delivery_policy import DeliveryPolicy
from .models import (
    AnalysisDraft,
    BundlePolicyContext,
    CandidatePolicyContext,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
    NotificationPlanIntent,
    PolicyEvaluation,
)
from .notification_intent import NotificationIntentBuilder
from .repositories import PolicyEngineRepository


SCHEMA_VERSION = "bounded_notification_intent_recovery_v1"
RUNNER_NAME = "bounded_notification_intent_recovery"
PREVIEW_MODE = "preview"
WRITE_MODE = "write"
WRITE_PUBLISH_MODE = "write_publish"
INPUT_EVENT_TYPE = "analysis.policy.apply.v1"
ROOT_OBJECT_TYPE = "judge_run"
DEFAULT_DB_SCAN_LIMIT = 500
DEFAULT_XADD_MAXLEN = 10000
UUID_SUFFIX_RE = re.compile(r"^[0-9a-f-]{4,36}$")
REQUIRED_TARGET_SUFFIX_FIELDS = (
    "policy_apply_event_suffix",
    "judge_run_suffix",
    "judge_output_suffix",
    "bundle_suffix",
    "candidate_group_suffix",
)


@dataclass(frozen=True, slots=True)
class BoundedNotificationIntentRecoveryConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_policy_preview: bool = False
    allow_database_write: bool = False
    allow_notification_intent_write: bool = False
    require_notification_send_enabled: bool = False
    allow_redis_read: bool = False
    allow_redis_publish: bool = False
    allow_notification_send_queue_publish: bool = False
    policy_apply_event_suffix: str | None = None
    judge_run_suffix: str | None = None
    judge_output_suffix: str | None = None
    bundle_suffix: str | None = None
    candidate_group_suffix: str | None = None
    analysis_suffix: str | None = None
    db_scan_limit: int = DEFAULT_DB_SCAN_LIMIT


@dataclass(frozen=True, slots=True)
class BoundedNotificationIntentRecoveryRuntimeConfig:
    database_url: str
    redis_url: str = ""
    xadd_maxlen: int | None = DEFAULT_XADD_MAXLEN
    policy_version: str = "verdict_policy_v1"
    delivery_policy_version: str = "delivery_policy_v1"
    operator_chat_id: int = 0
    enable_later_delivery: bool = True
    enable_silent_later: bool = True
    enable_notification_send: bool = True
    render_profile_high: str = "telegram_single_alert_high_v1"
    render_profile_normal: str = "telegram_single_alert_normal_v1"

    def to_policy_config(self) -> PolicyEngineConfig:
        return PolicyEngineConfig(
            app_env="runtime",
            database_url=self.database_url,
            redis_url=self.redis_url,
            queue_name="q.analysis.policy",
            consumer_group="policy-engine",
            consumer_name="bounded-notification-intent-recovery",
            batch_size=1,
            block_ms=1,
            policy_version=self.policy_version,
            delivery_policy_version=self.delivery_policy_version,
            operator_chat_id=self.operator_chat_id,
            enable_later_delivery=self.enable_later_delivery,
            enable_silent_later=self.enable_silent_later,
            enable_notification_send=self.enable_notification_send,
            render_profile_high=self.render_profile_high,
            render_profile_normal=self.render_profile_normal,
            log_level="INFO",
        )


@dataclass(slots=True)
class BoundedNotificationIntentRecoveryState:
    runtime_config_loaded: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    redis_publisher_created: bool = False
    redis_read_attempted: bool = False
    redis_publish_attempted: bool = False
    policy_preview_called: bool = False


class BoundedNotificationIntentRecoveryError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


@dataclass(frozen=True, slots=True)
class AnalysisRecoveryRecord:
    analysis_id: UUID
    candidate_group_id: UUID
    judge_output_id: UUID
    schema_version: str
    policy_version: str
    prompt_version: str
    delivery_policy_version: str
    verdict: str
    delivery_decision: str
    scores_json: dict[str, Any]
    reason_codes_json: list[str]
    evidence_limitations_ko: str | None
    recommended_action_ko: str | None
    freshness_note_ko: str | None
    model_proposed_verdict: str | None
    policy_reconciled_flag: bool

    def to_analysis_draft(self) -> AnalysisDraft:
        return AnalysisDraft(
            candidate_group_id=self.candidate_group_id,
            judge_output_id=self.judge_output_id,
            schema_version=self.schema_version,
            policy_version=self.policy_version,
            prompt_version=self.prompt_version,
            delivery_policy_version=self.delivery_policy_version,
            verdict=self.verdict,  # type: ignore[arg-type]
            delivery_decision=self.delivery_decision,  # type: ignore[arg-type]
            scores_json=self.scores_json,
            reason_codes_json=self.reason_codes_json,
            evidence_limitations_ko=self.evidence_limitations_ko,
            recommended_action_ko=self.recommended_action_ko,
            freshness_note_ko=self.freshness_note_ko,
            model_proposed_verdict=self.model_proposed_verdict,
            policy_reconciled_flag=self.policy_reconciled_flag,
        )


class BoundedNotificationIntentRecoveryRepository(Protocol):
    async def load_policy_apply_events(self, *, db_scan_limit: int) -> list[OutboxEventRow]: ...
    async def load_candidate_context(self, candidate_group_id: UUID) -> CandidatePolicyContext | None: ...
    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunPolicyContext | None: ...
    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputPolicyContext | None: ...
    async def load_bundle_context(self, bundle_id: UUID) -> BundlePolicyContext | None: ...
    async def load_existing_analysis_recovery(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> AnalysisRecoveryRecord | None: ...
    async def load_analysis_recovery_by_id(self, analysis_id: UUID) -> AnalysisRecoveryRecord | None: ...
    async def load_notification_plan_intent_outboxes(self, intent: NotificationPlanIntent) -> list[OutboxEventRow]: ...
    async def insert_or_load_notification_plan_intent_outbox(
        self,
        intent: NotificationPlanIntent,
    ) -> tuple[OutboxEventRow, bool]: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...


class RedisPublisher(Protocol):
    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str: ...


@dataclass(frozen=True, slots=True)
class BoundedNotificationIntentRecoveryRepositoryHandle:
    repository: BoundedNotificationIntentRecoveryRepository
    close: Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedNotificationIntentRecoveryRedisPublisherHandle:
    publisher: RedisPublisher
    close: Callable[[], Awaitable[None]]


class BoundedNotificationIntentRecoveryRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedNotificationIntentRecoveryRuntimeConfig,
        state: BoundedNotificationIntentRecoveryState,
        logger: logging.Logger,
    ) -> BoundedNotificationIntentRecoveryRepositoryHandle: ...


class BoundedNotificationIntentRecoveryRedisPublisherBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedNotificationIntentRecoveryRuntimeConfig,
        state: BoundedNotificationIntentRecoveryState,
        logger: logging.Logger,
    ) -> BoundedNotificationIntentRecoveryRedisPublisherHandle: ...


@dataclass(frozen=True, slots=True)
class BoundedNotificationIntentRecoveryResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedNotificationIntentRecoveryConfig
    mode: str
    state: BoundedNotificationIntentRecoveryState = field(default_factory=BoundedNotificationIntentRecoveryState)
    target_policy_apply_event_suffix: str | None = None
    target_judge_run_suffix: str | None = None
    target_judge_output_suffix: str | None = None
    target_bundle_suffix: str | None = None
    target_candidate_group_suffix: str | None = None
    target_analysis_suffix: str | None = None
    policy_apply_outbox_status: str | None = None
    predicted_verdict: str | None = None
    predicted_delivery_decision: str | None = None
    predicted_urgency_profile: str | None = None
    recovered_verdict: str | None = None
    recovered_delivery_decision: str | None = None
    recovered_urgency_profile: str | None = None
    notification_intent_possible: bool = False
    notification_intent_recovery_reason_code: str | None = None
    notification_intent_outbox_written: bool = False
    notification_intent_outbox_existing: bool = False
    notification_intent_outbox_event_suffix: str | None = None
    q_notification_send_published: bool = False
    q_notification_send_message_id_suffix: str | None = None

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": self.mode,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "target_policy_apply_event_suffix": self.target_policy_apply_event_suffix,
            "target_judge_run_suffix": self.target_judge_run_suffix,
            "target_judge_output_suffix": self.target_judge_output_suffix,
            "target_bundle_suffix": self.target_bundle_suffix,
            "target_candidate_group_suffix": self.target_candidate_group_suffix,
            "target_analysis_suffix": self.target_analysis_suffix,
            "policy_apply_outbox_status": self.policy_apply_outbox_status,
            "predicted_verdict": self.predicted_verdict,
            "predicted_delivery_decision": self.predicted_delivery_decision,
            "predicted_urgency_profile": self.predicted_urgency_profile,
            "recovered_verdict": self.recovered_verdict,
            "recovered_delivery_decision": self.recovered_delivery_decision,
            "recovered_urgency_profile": self.recovered_urgency_profile,
            "notification_intent_possible": self.notification_intent_possible,
            "notification_intent_recovery_reason_code": self.notification_intent_recovery_reason_code,
            "notification_intent_outbox_written": self.notification_intent_outbox_written,
            "notification_intent_outbox_existing": self.notification_intent_outbox_existing,
            "notification_intent_outbox_event_suffix": self.notification_intent_outbox_event_suffix,
            "q_notification_send_published": self.q_notification_send_published,
            "q_notification_send_message_id_suffix": self.q_notification_send_message_id_suffix,
            "operator_approved": self.config.operator_approved,
            "runtime_config_allowed": self.config.allow_runtime_config,
            "database_read_allowed": self.config.allow_database_read,
            "policy_preview_allowed": self.config.allow_policy_preview,
            "database_write_allowed": self.config.allow_database_write,
            "notification_intent_write_allowed": self.config.allow_notification_intent_write,
            "require_notification_send_enabled": self.config.require_notification_send_enabled,
            "redis_read_allowed": self.config.allow_redis_read,
            "redis_publish_allowed": self.config.allow_redis_publish,
            "notification_send_queue_publish_allowed": self.config.allow_notification_send_queue_publish,
            "runtime_config_loaded": self.state.runtime_config_loaded,
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": self.state.database_write_attempted,
            "redis_read_attempted": self.state.redis_read_attempted,
            "redis_publish_attempted": self.state.redis_publish_attempted,
            "policy_preview_called": self.state.policy_preview_called,
            "redis_ack_called": False,
            "redis_consume_called": False,
            "notifier_called": False,
            "telegram_send_called": False,
            "openai_called": False,
            "github_api_called": False,
            "x_api_called": False,
            "web_fetch_called": False,
            "redactions_applied": {
                "full_policy_apply_event_id_omitted": True,
                "full_judge_run_id_omitted": True,
                "full_judge_output_id_omitted": True,
                "full_bundle_id_omitted": True,
                "full_candidate_group_id_omitted": True,
                "full_analysis_id_omitted": True,
                "full_notification_intent_outbox_event_id_omitted": True,
                "full_notification_plan_id_omitted": True,
                "target_chat_id_omitted": True,
                "idempotency_key_omitted": True,
                "payload_json_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "sql_text_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class SqlAlchemyBoundedNotificationIntentRecoveryRepository:
    def __init__(self, session: Any) -> None:
        self._session = session
        self._policy_repository = PolicyEngineRepository(session)

    async def load_policy_apply_events(self, *, db_scan_limit: int) -> list[OutboxEventRow]:
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       dedupe_key, payload_json, status, fail_count, created_at
                FROM event_outbox
                WHERE event_type = :event_type
                ORDER BY created_at DESC, event_id DESC
                LIMIT :limit
                """
            ),
            {"event_type": INPUT_EVENT_TYPE, "limit": db_scan_limit},
        )
        return [_outbox_row_from_mapping(row) for row in result.mappings().all()]

    async def load_candidate_context(self, candidate_group_id: UUID) -> CandidatePolicyContext | None:
        return await self._policy_repository.load_candidate_context(candidate_group_id)

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunPolicyContext | None:
        return await self._policy_repository.load_judge_run(judge_run_id)

    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputPolicyContext | None:
        return await self._policy_repository.load_judge_output(judge_output_id)

    async def load_bundle_context(self, bundle_id: UUID) -> BundlePolicyContext | None:
        return await self._policy_repository.load_bundle_context(bundle_id)

    async def load_existing_analysis_recovery(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> AnalysisRecoveryRecord | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT analysis_id, candidate_group_id, judge_output_id, schema_version,
                       policy_version, prompt_version, delivery_policy_version, verdict,
                       delivery_decision, scores_json, reason_codes_json,
                       evidence_limitations_ko, recommended_action_ko, freshness_note_ko,
                       model_proposed_verdict, policy_reconciled_flag
                FROM analyses
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                  AND policy_version = :policy_version
                  AND delivery_policy_version = :delivery_policy_version
                """
            ),
            {
                "judge_output_id": str(judge_output_id),
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )
        row = result.mappings().first()
        return _analysis_recovery_from_mapping(row) if row is not None else None

    async def load_analysis_recovery_by_id(self, analysis_id: UUID) -> AnalysisRecoveryRecord | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT analysis_id, candidate_group_id, judge_output_id, schema_version,
                       policy_version, prompt_version, delivery_policy_version, verdict,
                       delivery_decision, scores_json, reason_codes_json,
                       evidence_limitations_ko, recommended_action_ko, freshness_note_ko,
                       model_proposed_verdict, policy_reconciled_flag
                FROM analyses
                WHERE analysis_id = CAST(:analysis_id AS uuid)
                """
            ),
            {"analysis_id": str(analysis_id)},
        )
        row = result.mappings().first()
        return _analysis_recovery_from_mapping(row) if row is not None else None

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
            raise BoundedNotificationIntentRecoveryError("notification_intent_unavailable")
        return _outbox_row_from_mapping(row), bool(row["inserted"])

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()


def load_bounded_notification_intent_recovery_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedNotificationIntentRecoveryRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    if not database_url:
        raise BoundedNotificationIntentRecoveryError("database_url_missing")
    xadd_maxlen_raw = _env_value(source, "OUTBOX_RELAY_XADD_MAXLEN", str(DEFAULT_XADD_MAXLEN))
    xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        raise BoundedNotificationIntentRecoveryError("runtime_config_error")
    return BoundedNotificationIntentRecoveryRuntimeConfig(
        database_url=database_url,
        redis_url=_env_value(source, "REDIS_URL"),
        xadd_maxlen=xadd_maxlen,
        policy_version=_env_value(source, "VERDICT_POLICY_VERSION", "verdict_policy_v1"),
        delivery_policy_version=_env_value(source, "DELIVERY_POLICY_VERSION", "delivery_policy_v1"),
        operator_chat_id=int(_env_value(source, "TELEGRAM_OPERATOR_CHAT_ID", "0")),
        enable_later_delivery=_bool_env(_env_value(source, "ENABLE_LATER_DELIVERY", "true")),
        enable_silent_later=_bool_env(_env_value(source, "ENABLE_SILENT_LATER", "true")),
        enable_notification_send=_bool_env(_env_value(source, "ENABLE_NOTIFICATION_SEND", "true")),
        render_profile_high=_env_value(source, "NOTIFY_RENDER_PROFILE_HIGH", "telegram_single_alert_high_v1"),
        render_profile_normal=_env_value(source, "NOTIFY_RENDER_PROFILE_NORMAL", "telegram_single_alert_normal_v1"),
    )


async def build_default_bounded_notification_intent_recovery_repository(
    runtime_config: BoundedNotificationIntentRecoveryRuntimeConfig,
    state: BoundedNotificationIntentRecoveryState,
    logger: logging.Logger,
) -> BoundedNotificationIntentRecoveryRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedNotificationIntentRecoveryRepository(session)

    async def close() -> None:
        await session.close()
        await engine.dispose()

    return BoundedNotificationIntentRecoveryRepositoryHandle(repository=repository, close=close)


async def build_default_bounded_notification_intent_recovery_redis_publisher(
    runtime_config: BoundedNotificationIntentRecoveryRuntimeConfig,
    state: BoundedNotificationIntentRecoveryState,
    logger: logging.Logger,
) -> BoundedNotificationIntentRecoveryRedisPublisherHandle:
    del logger
    if not runtime_config.redis_url:
        raise BoundedNotificationIntentRecoveryError("redis_url_missing")
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

    return BoundedNotificationIntentRecoveryRedisPublisherHandle(publisher=publisher, close=close)


async def run_bounded_notification_intent_recovery(
    config: BoundedNotificationIntentRecoveryConfig,
    *,
    runtime_config_loader: Callable[[], BoundedNotificationIntentRecoveryRuntimeConfig] = (
        load_bounded_notification_intent_recovery_runtime_config
    ),
    repository_builder: BoundedNotificationIntentRecoveryRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedNotificationIntentRecoveryRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotificationIntentRecoveryResult:
    state = BoundedNotificationIntentRecoveryState()
    mode = _mode(config)
    gate_error = _authority_gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, mode=mode, state=state)

    suffix_error = _target_suffix_error(config)
    if suffix_error is not None:
        return _result("blocked", suffix_error, config=config, mode=mode, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedNotificationIntentRecoveryError as exc:
        return _result("blocked", exc.error_code, config=config, mode=mode, state=state)
    except Exception as exc:
        return _result(
            "blocked",
            "runtime_config_error",
            error_class=_safe_exception_class(exc),
            config=config,
            mode=mode,
            state=state,
        )

    if _publish_requested(config) and not runtime_config.redis_url:
        return _result("blocked", "redis_url_missing", config=config, mode=mode, state=state)

    repository_handle: BoundedNotificationIntentRecoveryRepositoryHandle | None = None
    publisher_handle: BoundedNotificationIntentRecoveryRedisPublisherHandle | None = None
    event: OutboxEventRow | None = None
    candidate: CandidatePolicyContext | None = None
    judge_run: JudgeRunPolicyContext | None = None
    judge_output: JudgeOutputPolicyContext | None = None
    bundle: BundlePolicyContext | None = None
    analysis: AnalysisRecoveryRecord | None = None
    evaluation: PolicyEvaluation | None = None
    intent: NotificationPlanIntent | None = None
    notification_outbox: OutboxEventRow | None = None
    outbox_written = False
    outbox_existing = False
    redis_message_id: str | None = None
    committed_after_write = False
    try:
        repository_handle = await (repository_builder or build_default_bounded_notification_intent_recovery_repository)(
            runtime_config,
            state,
            effective_logger,
        )
        repository = repository_handle.repository
        state.database_read_attempted = True

        events = await repository.load_policy_apply_events(db_scan_limit=config.db_scan_limit)
        matching_events = [row for row in events if str(row.event_id).endswith(config.policy_apply_event_suffix or "")]
        if len(matching_events) != 1:
            return _result("blocked", "suffix_ambiguous_or_missing", config=config, mode=mode, state=state)
        event = matching_events[0]
        payload = event.payload_json if isinstance(event.payload_json, Mapping) else {}
        judge_run_id = _payload_uuid(payload, "judge_run_id")
        judge_output_id = _payload_uuid(payload, "judge_output_id")
        candidate_group_id = _payload_uuid(payload, "candidate_group_id")
        bundle_id = _payload_uuid(payload, "bundle_id")
        if None in {judge_run_id, judge_output_id, candidate_group_id, bundle_id}:
            return _result("blocked", "context_mismatch", config=config, mode=mode, state=state, event=event)
        assert judge_run_id is not None
        assert judge_output_id is not None
        assert candidate_group_id is not None
        assert bundle_id is not None

        selector_error = _selector_mismatch_reason(
            config=config,
            judge_run_id=judge_run_id,
            judge_output_id=judge_output_id,
            candidate_group_id=candidate_group_id,
            bundle_id=bundle_id,
        )
        if selector_error is not None:
            return _result("blocked", selector_error, config=config, mode=mode, state=state, event=event)

        candidate = await repository.load_candidate_context(candidate_group_id)
        judge_run = await repository.load_judge_run(judge_run_id)
        judge_output = await repository.load_judge_output(judge_output_id)
        bundle = await repository.load_bundle_context(bundle_id)
        context_error = _context_mismatch_reason(
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            judge_run_id=judge_run_id,
            judge_output_id=judge_output_id,
            candidate_group_id=candidate_group_id,
            bundle_id=bundle_id,
        )
        if context_error is not None:
            return _result("blocked", context_error, config=config, mode=mode, state=state, event=event)

        assert candidate is not None
        assert judge_run is not None
        assert judge_output is not None
        assert bundle is not None

        analysis = await repository.load_existing_analysis_recovery(
            judge_output_id=judge_output.judge_output_id,
            policy_version=runtime_config.policy_version,
            delivery_policy_version=runtime_config.delivery_policy_version,
        )
        if analysis is None and candidate.current_analysis_id is not None:
            candidate_analysis = await repository.load_analysis_recovery_by_id(candidate.current_analysis_id)
            if candidate_analysis is not None and candidate_analysis.judge_output_id == judge_output.judge_output_id:
                analysis = candidate_analysis
        if analysis is None:
            return _result("blocked", "analysis_missing", config=config, mode=mode, state=state, event=event)
        if config.analysis_suffix and not str(analysis.analysis_id).endswith(config.analysis_suffix):
            return _result(
                "blocked",
                "context_mismatch",
                config=config,
                mode=mode,
                state=state,
                event=event,
                analysis=analysis,
            )
        state.policy_preview_called = True
        evaluation = _evaluation_for_analysis(runtime_config=runtime_config, analysis=analysis)
        if evaluation.delivery_decision != analysis.delivery_decision:
            return _result(
                "blocked",
                "context_mismatch",
                config=config,
                mode=mode,
                state=state,
                event=event,
                analysis=analysis,
                evaluation=evaluation,
            )
        if analysis.delivery_decision == "suppress":
            return _result(
                "blocked",
                "analysis_delivery_suppress",
                config=config,
                mode=mode,
                state=state,
                event=event,
                analysis=analysis,
                evaluation=evaluation,
            )
        if not runtime_config.enable_notification_send:
            return _result(
                "blocked",
                "notification_send_disabled",
                config=config,
                mode=mode,
                state=state,
                event=event,
                analysis=analysis,
                evaluation=evaluation,
            )

        intent = NotificationIntentBuilder(config=runtime_config.to_policy_config()).build(
            analysis_id=analysis.analysis_id,
            analysis=analysis.to_analysis_draft(),
            evaluation=evaluation,
        )
        if intent is None:
            return _result(
                "blocked",
                "notification_intent_unavailable",
                config=config,
                mode=mode,
                state=state,
                event=event,
                analysis=analysis,
                evaluation=evaluation,
            )

        matching_outboxes = await repository.load_notification_plan_intent_outboxes(intent)
        if len(matching_outboxes) > 1:
            return _result(
                "blocked",
                "suffix_ambiguous_or_missing",
                config=config,
                mode=mode,
                state=state,
                event=event,
                analysis=analysis,
                evaluation=evaluation,
                intent=intent,
            )
        if matching_outboxes:
            notification_outbox = matching_outboxes[0]
            outbox_existing = True
            outbox_error = _validate_notification_outbox(notification_outbox, intent=intent)
            if outbox_error is not None:
                return _result(
                    "blocked",
                    "context_mismatch",
                    config=config,
                    mode=mode,
                    state=state,
                    event=event,
                    analysis=analysis,
                    evaluation=evaluation,
                    intent=intent,
                    notification_outbox=notification_outbox,
                    outbox_existing=True,
                )
        elif _write_requested(config):
            state.database_write_attempted = True
            notification_outbox, inserted = await repository.insert_or_load_notification_plan_intent_outbox(intent)
            outbox_written = inserted
            outbox_existing = not inserted
            outbox_error = _validate_notification_outbox(notification_outbox, intent=intent)
            if outbox_error is not None:
                await repository.rollback()
                return _result(
                    "blocked",
                    "context_mismatch",
                    config=config,
                    mode=mode,
                    state=state,
                    event=event,
                    analysis=analysis,
                    evaluation=evaluation,
                    intent=intent,
                    notification_outbox=notification_outbox,
                    outbox_written=outbox_written,
                    outbox_existing=outbox_existing,
                )
            try:
                await repository.commit()
                committed_after_write = True
            except Exception as exc:
                return _result(
                    "failed",
                    "database_commit_failed_before_redis_publish",
                    error_class=_safe_exception_class(exc),
                    config=config,
                    mode=mode,
                    state=state,
                    event=event,
                    analysis=analysis,
                    evaluation=evaluation,
                    intent=intent,
                    notification_outbox=notification_outbox,
                    outbox_written=outbox_written,
                    outbox_existing=outbox_existing,
                )
        else:
            return _result(
                "pass",
                None,
                config=config,
                mode=mode,
                state=state,
                event=event,
                analysis=analysis,
                evaluation=evaluation,
                intent=intent,
            )

        if _publish_requested(config) and notification_outbox is not None and outbox_written:
            route = (route_resolver or OutboxRouteResolver()).resolve(notification_outbox)
            if route.queue_name != OUTPUT_QUEUE_NAME or route.stage_name != OUTPUT_STAGE_NAME:
                return _result(
                    "blocked",
                    "context_mismatch",
                    config=config,
                    mode=mode,
                    state=state,
                    event=event,
                    analysis=analysis,
                    evaluation=evaluation,
                    intent=intent,
                    notification_outbox=notification_outbox,
                    outbox_written=outbox_written,
                    outbox_existing=outbox_existing,
                )
            publisher_handle = await (
                redis_publisher_builder or build_default_bounded_notification_intent_recovery_redis_publisher
            )(
                runtime_config,
                state,
                effective_logger,
            )
            try:
                state.redis_publish_attempted = True
                redis_message_id = await publisher_handle.publisher.publish(
                    route,
                    _build_notification_stream_message(notification_outbox, route),
                )
            except Exception as exc:
                return _result(
                    "failed",
                    "redis_publish_failed",
                    error_class=_safe_exception_class(exc),
                    config=config,
                    mode=mode,
                    state=state,
                    event=event,
                    analysis=analysis,
                    evaluation=evaluation,
                    intent=intent,
                    notification_outbox=notification_outbox,
                    outbox_written=outbox_written,
                    outbox_existing=outbox_existing,
                )

        if notification_outbox is not None and outbox_existing:
            reason = "notification_intent_already_exists"
        else:
            reason = None
        return _result(
            "pass",
            reason,
            config=config,
            mode=mode,
            state=state,
            event=event,
            analysis=analysis,
            evaluation=evaluation,
            intent=intent,
            notification_outbox=notification_outbox,
            outbox_written=outbox_written,
            outbox_existing=outbox_existing,
            redis_message_id=redis_message_id,
        )
    except Exception as exc:
        if repository_handle is not None and state.database_write_attempted and not committed_after_write:
            try:
                await repository_handle.repository.rollback()
            except Exception:
                pass
        return _result(
            "failed",
            "bounded_notification_intent_recovery_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            mode=mode,
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
                await repository_handle.close()
            except Exception:
                pass


def run_bounded_notification_intent_recovery_sync(
    config: BoundedNotificationIntentRecoveryConfig,
    *,
    runtime_config_loader: Callable[[], BoundedNotificationIntentRecoveryRuntimeConfig] = (
        load_bounded_notification_intent_recovery_runtime_config
    ),
    repository_builder: BoundedNotificationIntentRecoveryRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedNotificationIntentRecoveryRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    logger: logging.Logger | None = None,
) -> BoundedNotificationIntentRecoveryResult:
    return asyncio.run(
        run_bounded_notification_intent_recovery(
            config,
            runtime_config_loader=runtime_config_loader,
            repository_builder=repository_builder,
            redis_publisher_builder=redis_publisher_builder,
            route_resolver=route_resolver,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        error_code,
        config=BoundedNotificationIntentRecoveryConfig(),
        mode=PREVIEW_MODE,
        state=BoundedNotificationIntentRecoveryState(),
    ).to_sanitized_dict()


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedNotificationIntentRecoveryConfig,
    mode: str,
    state: BoundedNotificationIntentRecoveryState,
    error_class: str | None = None,
    event: OutboxEventRow | None = None,
    analysis: AnalysisRecoveryRecord | None = None,
    evaluation: PolicyEvaluation | None = None,
    intent: NotificationPlanIntent | None = None,
    notification_outbox: OutboxEventRow | None = None,
    outbox_written: bool = False,
    outbox_existing: bool = False,
    redis_message_id: str | None = None,
) -> BoundedNotificationIntentRecoveryResult:
    target = _target_suffixes(config=config, event=event, analysis=analysis)
    possible = intent is not None
    if error_code == "notification_intent_already_exists":
        possible = True
    return BoundedNotificationIntentRecoveryResult(
        status=status,
        ok=status == "pass" and error_code not in _blocking_reason_codes(),
        error_code=error_code if status in {"blocked", "failed"} else None,
        error_class=error_class,
        config=config,
        mode=mode,
        state=state,
        target_policy_apply_event_suffix=target["policy_apply_event_suffix"],
        target_judge_run_suffix=target["judge_run_suffix"],
        target_judge_output_suffix=target["judge_output_suffix"],
        target_bundle_suffix=target["bundle_suffix"],
        target_candidate_group_suffix=target["candidate_group_suffix"],
        target_analysis_suffix=target["analysis_suffix"],
        policy_apply_outbox_status=event.status if event is not None else None,
        predicted_verdict=analysis.verdict if analysis is not None else None,
        predicted_delivery_decision=analysis.delivery_decision if analysis is not None else None,
        predicted_urgency_profile=evaluation.urgency_profile if evaluation is not None else None,
        recovered_verdict=analysis.verdict if analysis is not None else None,
        recovered_delivery_decision=analysis.delivery_decision if analysis is not None else None,
        recovered_urgency_profile=evaluation.urgency_profile if evaluation is not None else None,
        notification_intent_possible=possible,
        notification_intent_recovery_reason_code=error_code,
        notification_intent_outbox_written=outbox_written,
        notification_intent_outbox_existing=outbox_existing,
        notification_intent_outbox_event_suffix=_optional_id_suffix(notification_outbox.event_id)
        if notification_outbox is not None
        else None,
        q_notification_send_published=bool(redis_message_id),
        q_notification_send_message_id_suffix=_redis_message_id_suffix(redis_message_id),
    )


def _authority_gate_error(config: BoundedNotificationIntentRecoveryConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if not config.allow_policy_preview:
        return "policy_preview_not_allowed"
    if config.db_scan_limit < 1 or config.db_scan_limit > DEFAULT_DB_SCAN_LIMIT:
        return "invalid_db_scan_limit"
    if config.allow_notification_intent_write and not config.allow_database_write:
        return "database_write_not_allowed"
    if config.allow_database_write and not config.allow_notification_intent_write:
        return "notification_intent_write_not_allowed"
    if _write_requested(config) and not config.require_notification_send_enabled:
        return "notification_send_enabled_requirement_missing"
    if _publish_requested(config):
        if not config.allow_database_write or not config.allow_notification_intent_write:
            return "database_write_not_allowed"
        if not config.allow_redis_read:
            return "redis_read_not_allowed"
        if not config.allow_redis_publish:
            return "redis_publish_not_allowed"
        if not config.allow_notification_send_queue_publish:
            return "notification_send_queue_publish_not_allowed"
    return None


def _target_suffix_error(config: BoundedNotificationIntentRecoveryConfig) -> str | None:
    for field_name in REQUIRED_TARGET_SUFFIX_FIELDS:
        value = getattr(config, field_name)
        if not _valid_suffix(value):
            return "suffix_ambiguous_or_missing"
    if config.analysis_suffix is not None and not _valid_suffix(config.analysis_suffix):
        return "suffix_ambiguous_or_missing"
    return None


def _selector_mismatch_reason(
    *,
    config: BoundedNotificationIntentRecoveryConfig,
    judge_run_id: UUID,
    judge_output_id: UUID,
    candidate_group_id: UUID,
    bundle_id: UUID,
) -> str | None:
    if not str(judge_run_id).endswith(config.judge_run_suffix or ""):
        return "context_mismatch"
    if not str(judge_output_id).endswith(config.judge_output_suffix or ""):
        return "context_mismatch"
    if not str(bundle_id).endswith(config.bundle_suffix or ""):
        return "context_mismatch"
    if not str(candidate_group_id).endswith(config.candidate_group_suffix or ""):
        return "context_mismatch"
    return None


def _context_mismatch_reason(
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
    if event.event_type != INPUT_EVENT_TYPE:
        return "context_mismatch"
    if event.aggregate_type != ROOT_OBJECT_TYPE:
        return "context_mismatch"
    if event.aggregate_id != judge_run_id:
        return "context_mismatch"
    if candidate is None or judge_run is None or judge_output is None or bundle is None:
        return "context_mismatch"
    if candidate.candidate_group_id != candidate_group_id:
        return "context_mismatch"
    if candidate.current_bundle_id != bundle_id:
        return "context_mismatch"
    if judge_run.judge_run_id != judge_run_id or judge_run.bundle_id != bundle_id:
        return "context_mismatch"
    if judge_run.status != "succeeded":
        return "context_mismatch"
    if judge_output.judge_output_id != judge_output_id:
        return "context_mismatch"
    if judge_output.judge_run_id != judge_run_id:
        return "context_mismatch"
    if judge_output.candidate_group_id != candidate_group_id:
        return "context_mismatch"
    if bundle.bundle_id != bundle_id or bundle.candidate_group_id != candidate_group_id:
        return "context_mismatch"
    return None


def _evaluation_for_analysis(
    *,
    runtime_config: BoundedNotificationIntentRecoveryRuntimeConfig,
    analysis: AnalysisRecoveryRecord,
) -> PolicyEvaluation:
    delivery = DeliveryPolicy(
        enable_later_delivery=runtime_config.enable_later_delivery,
        enable_silent_later=runtime_config.enable_silent_later,
    ).evaluate(verdict=analysis.verdict)  # type: ignore[arg-type]
    return PolicyEvaluation(
        verdict=analysis.verdict,  # type: ignore[arg-type]
        delivery_decision=delivery.delivery_decision,
        urgency_profile=delivery.urgency_profile,
        reason_codes=list(analysis.reason_codes_json),
        policy_reconciled_flag=analysis.policy_reconciled_flag,
        suppress_reason_code=delivery.suppress_reason_code,
    )


def _validate_notification_outbox(row: OutboxEventRow, *, intent: NotificationPlanIntent) -> str | None:
    if row.event_type != OUTPUT_EVENT_TYPE:
        return "wrong_event_type"
    if row.aggregate_type != "analysis":
        return "wrong_aggregate_type"
    if row.aggregate_id != intent.analysis_id:
        return "aggregate_mismatch"
    if row.dedupe_key != _notification_plan_dedupe_key(intent):
        return "dedupe_mismatch"
    payload = row.payload_json if isinstance(row.payload_json, Mapping) else {}
    if _payload_uuid(payload, "analysis_id") != intent.analysis_id:
        return "payload_analysis_mismatch"
    if _payload_uuid(payload, "candidate_group_id") != intent.candidate_group_id:
        return "payload_candidate_mismatch"
    if _payload_uuid(payload, "notification_plan_id") is None:
        return "payload_plan_id_missing"
    if payload.get("material_change_hash") != intent.material_change_hash:
        return "payload_material_hash_mismatch"
    return None


def _target_suffixes(
    *,
    config: BoundedNotificationIntentRecoveryConfig,
    event: OutboxEventRow | None,
    analysis: AnalysisRecoveryRecord | None,
) -> dict[str, str | None]:
    payload = event.payload_json if event is not None and isinstance(event.payload_json, Mapping) else {}
    return {
        "policy_apply_event_suffix": _optional_id_suffix(event.event_id if event is not None else None)
        or config.policy_apply_event_suffix,
        "judge_run_suffix": _optional_id_suffix(_payload_uuid(payload, "judge_run_id")) or config.judge_run_suffix,
        "judge_output_suffix": _optional_id_suffix(_payload_uuid(payload, "judge_output_id"))
        or config.judge_output_suffix,
        "bundle_suffix": _optional_id_suffix(_payload_uuid(payload, "bundle_id")) or config.bundle_suffix,
        "candidate_group_suffix": _optional_id_suffix(_payload_uuid(payload, "candidate_group_id"))
        or config.candidate_group_suffix,
        "analysis_suffix": _optional_id_suffix(analysis.analysis_id if analysis is not None else None)
        or config.analysis_suffix,
    }


def _analysis_recovery_from_mapping(row: Mapping[str, Any]) -> AnalysisRecoveryRecord:
    return AnalysisRecoveryRecord(
        analysis_id=UUID(str(row["analysis_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        judge_output_id=UUID(str(row["judge_output_id"])),
        schema_version=str(row["schema_version"]),
        policy_version=str(row["policy_version"]),
        prompt_version=str(row["prompt_version"]),
        delivery_policy_version=str(row["delivery_policy_version"]),
        verdict=str(row["verdict"]),
        delivery_decision=str(row["delivery_decision"]),
        scores_json=_json_dict(row["scores_json"]),
        reason_codes_json=_json_string_list(row["reason_codes_json"]),
        evidence_limitations_ko=_string_or_none(row["evidence_limitations_ko"]),
        recommended_action_ko=_string_or_none(row["recommended_action_ko"]),
        freshness_note_ko=_string_or_none(row["freshness_note_ko"]),
        model_proposed_verdict=_string_or_none(row["model_proposed_verdict"]),
        policy_reconciled_flag=bool(row["policy_reconciled_flag"]),
    )


def _mode(config: BoundedNotificationIntentRecoveryConfig) -> str:
    if _publish_requested(config):
        return WRITE_PUBLISH_MODE
    if _write_requested(config):
        return WRITE_MODE
    return PREVIEW_MODE


def _write_requested(config: BoundedNotificationIntentRecoveryConfig) -> bool:
    return config.allow_database_write or config.allow_notification_intent_write


def _publish_requested(config: BoundedNotificationIntentRecoveryConfig) -> bool:
    return (
        config.allow_redis_read
        or config.allow_redis_publish
        or config.allow_notification_send_queue_publish
    )


def _valid_suffix(value: str | None) -> bool:
    if value is None:
        return False
    normalized = value.strip().lower()
    return bool(normalized) and UUID_SUFFIX_RE.fullmatch(normalized) is not None


def _blocking_reason_codes() -> set[str]:
    return {
        "operator_approval_missing",
        "runtime_config_not_allowed",
        "database_read_not_allowed",
        "policy_preview_not_allowed",
        "invalid_db_scan_limit",
        "database_write_not_allowed",
        "notification_intent_write_not_allowed",
        "notification_send_enabled_requirement_missing",
        "redis_read_not_allowed",
        "redis_publish_not_allowed",
        "notification_send_queue_publish_not_allowed",
        "database_url_missing",
        "redis_url_missing",
        "runtime_config_error",
        "suffix_ambiguous_or_missing",
        "context_mismatch",
        "analysis_missing",
        "analysis_delivery_suppress",
        "notification_send_disabled",
        "notification_intent_unavailable",
    }


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (JSONDecodeError, TypeError):
            return None
    return value


def _json_dict(value: Any) -> dict[str, Any]:
    loaded = _json_loads(value)
    return loaded if isinstance(loaded, dict) else {}


def _json_string_list(value: Any) -> list[str]:
    loaded = _json_loads(value)
    if not isinstance(loaded, list):
        return []
    return [item for item in loaded if isinstance(item, str)]


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _env_value(source: Mapping[str, str], key: str, default: str = "") -> str:
    return str(source.get(key, default) or "").strip()


def _bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


__all__ = [
    "AnalysisRecoveryRecord",
    "BoundedNotificationIntentRecoveryConfig",
    "BoundedNotificationIntentRecoveryError",
    "BoundedNotificationIntentRecoveryRedisPublisherBuilder",
    "BoundedNotificationIntentRecoveryRedisPublisherHandle",
    "BoundedNotificationIntentRecoveryRepository",
    "BoundedNotificationIntentRecoveryRepositoryBuilder",
    "BoundedNotificationIntentRecoveryRepositoryHandle",
    "BoundedNotificationIntentRecoveryResult",
    "BoundedNotificationIntentRecoveryRuntimeConfig",
    "BoundedNotificationIntentRecoveryState",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "SqlAlchemyBoundedNotificationIntentRecoveryRepository",
    "argument_error_report",
    "build_default_bounded_notification_intent_recovery_redis_publisher",
    "build_default_bounded_notification_intent_recovery_repository",
    "load_bounded_notification_intent_recovery_runtime_config",
    "render_sanitized_json",
    "run_bounded_notification_intent_recovery",
    "run_bounded_notification_intent_recovery_sync",
]
