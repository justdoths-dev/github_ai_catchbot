from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from json import JSONDecodeError
from typing import Any, Literal, Protocol, cast
from uuid import UUID

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - static validation fallback
    sa = None

from ..outbox_relay.models import OutboxEventRow
from .bounded_analysis_runner import (
    INPUT_EVENT_TYPE,
    INPUT_QUEUE_NAME,
    REQUIRED_EVENT_PAYLOAD_FIELDS,
    ROOT_OBJECT_TYPE,
    RedisStreamMessage,
    _build_analysis,
    _normalize_redis_message,
    _notification_plan_dedupe_key,
    _optional_id_suffix,
    _outbox_row_from_mapping,
    _payload_uuid,
    _redis_message_id_suffix,
    _safe_exception_class,
    _safe_uuid,
    _sql,
    _validate_context,
    _validate_notification_outbox,
    _validate_redis_message,
)
from .config import PolicyEngineConfig, PolicyEngineConfigurationError
from .models import (
    AnalysisDraft,
    BundlePolicyContext,
    CandidatePolicyContext,
    DeliveryDecision,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
    NotificationPlanIntent,
    PolicyEvaluation,
    UrgencyProfile,
    Verdict,
)
from .notification_intent import NotificationIntentBuilder
from .repositories import PolicyEngineRepository


SCHEMA_VERSION = "bounded_policy_apply_inventory_v1"
RUNNER_NAME = "bounded_policy_apply_inventory"
MODE = "policy_apply_db_inventory_read_only"
DEFAULT_DB_LIMIT = 100
MAX_DB_LIMIT = 500
DEFAULT_REDIS_SCAN_LIMIT = 100
MAX_REDIS_SCAN_LIMIT = 500
DEFAULT_MAX_RESULTS = 10
MAX_RESULTS = 50
READY_RUNNER_PATH = "tools/bounded_policy_engine_analysis_runner.py"

PreferVerdict = Literal["inspect_now", "later", "any"]
Classification = Literal[
    "direct_runnable_non_suppress",
    "db_non_suppress_missing_redis",
    "db_non_suppress_unpublished_outbox",
    "unprocessed_suppress",
    "processed_suppress",
    "processed_notification_pending",
    "processed_notification_published",
    "processed_notification_missing",
    "processed_notification_invalid",
    "blocked",
]


@dataclass(frozen=True, slots=True)
class BoundedPolicyApplyInventoryConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_redis_read: bool = False
    allow_policy_preview: bool = False
    db_limit: int = DEFAULT_DB_LIMIT
    redis_scan_limit: int = DEFAULT_REDIS_SCAN_LIMIT
    max_results: int = DEFAULT_MAX_RESULTS
    prefer_verdict: str = "any"
    include_processed: bool = False
    include_suppressed: bool = False


@dataclass(frozen=True, slots=True)
class BoundedPolicyApplyInventoryRuntimeConfig:
    database_url: str
    redis_url: str
    queue_name: str = INPUT_QUEUE_NAME
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
            queue_name=self.queue_name,
            consumer_group="policy-engine",
            consumer_name="bounded-policy-apply-inventory",
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
class BoundedPolicyApplyInventoryState:
    runtime_config_loaded: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    redis_reader_created: bool = False
    redis_read_attempted: bool = False
    policy_preview_called: bool = False


@dataclass(frozen=True, slots=True)
class ExistingAnalysisInventoryRecord:
    analysis_id: UUID
    judge_output_id: UUID
    policy_version: str
    delivery_policy_version: str
    verdict: str
    delivery_decision: str


@dataclass(frozen=True, slots=True)
class InventoryCounts:
    db_policy_apply_event_count: int = 0
    redis_message_count: int = 0
    rehydrated_event_count: int = 0
    direct_runnable_non_suppress_count: int = 0
    db_non_suppress_missing_redis_count: int = 0
    db_non_suppress_unpublished_outbox_count: int = 0
    unprocessed_suppress_count: int = 0
    processed_suppress_count: int = 0
    processed_notification_pending_count: int = 0
    processed_notification_published_count: int = 0
    processed_notification_missing_count: int = 0
    processed_notification_invalid_count: int = 0
    blocked_count: int = 0


@dataclass(frozen=True, slots=True)
class InventoryCandidate:
    classification: Classification
    policy_apply_event_suffix: str | None
    judge_run_suffix: str | None
    judge_output_suffix: str | None
    bundle_suffix: str | None
    candidate_group_suffix: str | None
    policy_apply_outbox_status: str | None
    predicted_verdict: str | None
    predicted_delivery_decision: str | None
    predicted_urgency_profile: str | None
    analysis_exists: bool
    redis_message_id_suffix: str | None = None
    notification_outbox_status: str | None = None
    reason_code: str | None = None
    created_at: datetime | None = field(default=None, repr=False)

    def to_sanitized_dict(self, *, redis_scan_limit: int) -> dict[str, Any]:
        report: dict[str, Any] = {
            "classification": self.classification,
            "redis_message_id_suffix": self.redis_message_id_suffix,
            "policy_apply_event_suffix": self.policy_apply_event_suffix,
            "judge_run_suffix": self.judge_run_suffix,
            "judge_output_suffix": self.judge_output_suffix,
            "bundle_suffix": self.bundle_suffix,
            "candidate_group_suffix": self.candidate_group_suffix,
            "policy_apply_outbox_status": self.policy_apply_outbox_status,
            "predicted_verdict": self.predicted_verdict,
            "predicted_delivery_decision": self.predicted_delivery_decision,
            "predicted_urgency_profile": self.predicted_urgency_profile,
            "analysis_exists": self.analysis_exists,
            "notification_outbox_status": self.notification_outbox_status,
            "reason_code": self.reason_code,
        }
        if self.classification == "direct_runnable_non_suppress":
            report["ready_policy_runner_argv"] = _ready_policy_runner_argv(self, redis_scan_limit=redis_scan_limit)
        if self.classification == "db_non_suppress_missing_redis":
            report["requires_requeue_or_db_direct_runner"] = True
        if self.classification == "db_non_suppress_unpublished_outbox":
            report["requires_outbox_relay_or_db_direct_runner"] = True
        return report

    def to_direct_target(self, *, redis_scan_limit: int) -> dict[str, Any]:
        return {
            "redis_message_id_suffix": self.redis_message_id_suffix,
            "policy_apply_event_suffix": self.policy_apply_event_suffix,
            "judge_run_suffix": self.judge_run_suffix,
            "judge_output_suffix": self.judge_output_suffix,
            "bundle_suffix": self.bundle_suffix,
            "candidate_group_suffix": self.candidate_group_suffix,
            "policy_apply_outbox_status": self.policy_apply_outbox_status,
            "predicted_verdict": self.predicted_verdict,
            "predicted_delivery_decision": self.predicted_delivery_decision,
            "predicted_urgency_profile": self.predicted_urgency_profile,
            "analysis_exists": self.analysis_exists,
            "classification": self.classification,
            "ready_policy_runner_argv": _ready_policy_runner_argv(self, redis_scan_limit=redis_scan_limit),
        }

    def to_recovery_target(self) -> dict[str, Any]:
        report: dict[str, Any] = {
            "policy_apply_event_suffix": self.policy_apply_event_suffix,
            "judge_run_suffix": self.judge_run_suffix,
            "judge_output_suffix": self.judge_output_suffix,
            "bundle_suffix": self.bundle_suffix,
            "candidate_group_suffix": self.candidate_group_suffix,
            "policy_apply_outbox_status": self.policy_apply_outbox_status,
            "predicted_verdict": self.predicted_verdict,
            "predicted_delivery_decision": self.predicted_delivery_decision,
            "predicted_urgency_profile": self.predicted_urgency_profile,
            "classification": self.classification,
            "reason_code": self.reason_code,
        }
        if self.classification == "db_non_suppress_missing_redis":
            report["requires_requeue_or_db_direct_runner"] = True
        if self.classification == "db_non_suppress_unpublished_outbox":
            report["requires_outbox_relay_or_db_direct_runner"] = True
        return report


@dataclass(frozen=True, slots=True)
class BoundedPolicyApplyInventoryResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedPolicyApplyInventoryConfig
    state: BoundedPolicyApplyInventoryState = field(default_factory=BoundedPolicyApplyInventoryState)
    counts: InventoryCounts = field(default_factory=InventoryCounts)
    selected_direct_target: InventoryCandidate | None = None
    selected_recovery_target: InventoryCandidate | None = None
    candidates: tuple[InventoryCandidate, ...] = ()

    def to_sanitized_dict(self) -> dict[str, Any]:
        report: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "db_limit": self.config.db_limit,
            "redis_scan_limit": self.config.redis_scan_limit,
            "max_results": self.config.max_results,
            "prefer_verdict": self.config.prefer_verdict,
            "include_processed": self.config.include_processed,
            "include_suppressed": self.config.include_suppressed,
            "db_policy_apply_event_count": self.counts.db_policy_apply_event_count,
            "redis_message_count": self.counts.redis_message_count,
            "rehydrated_event_count": self.counts.rehydrated_event_count,
            "direct_runnable_non_suppress_count": self.counts.direct_runnable_non_suppress_count,
            "db_non_suppress_missing_redis_count": self.counts.db_non_suppress_missing_redis_count,
            "db_non_suppress_unpublished_outbox_count": self.counts.db_non_suppress_unpublished_outbox_count,
            "unprocessed_suppress_count": self.counts.unprocessed_suppress_count,
            "processed_suppress_count": self.counts.processed_suppress_count,
            "processed_notification_pending_count": self.counts.processed_notification_pending_count,
            "processed_notification_published_count": self.counts.processed_notification_published_count,
            "processed_notification_missing_count": self.counts.processed_notification_missing_count,
            "processed_notification_invalid_count": self.counts.processed_notification_invalid_count,
            "blocked_count": self.counts.blocked_count,
            "selected_direct_target": self.selected_direct_target.to_direct_target(
                redis_scan_limit=self.config.redis_scan_limit
            )
            if self.selected_direct_target is not None
            else None,
            "selected_recovery_target": self.selected_recovery_target.to_recovery_target()
            if self.selected_recovery_target is not None
            else None,
            "candidates": [
                candidate.to_sanitized_dict(redis_scan_limit=self.config.redis_scan_limit)
                for candidate in self.candidates
            ],
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": False,
            "redis_read_attempted": self.state.redis_read_attempted,
            "redis_publish_attempted": False,
            "redis_ack_called": False,
            "redis_consume_called": False,
            "policy_preview_called": self.state.policy_preview_called,
            "policy_engine_called": False,
            "notifier_called": False,
            "telegram_send_called": False,
            "openai_called": False,
            "github_api_called": False,
            "x_api_called": False,
            "web_fetch_called": False,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "database_read_allowed": self.config.allow_database_read,
                "redis_read_allowed": self.config.allow_redis_read,
                "policy_preview_allowed": self.config.allow_policy_preview,
                "db_limit": self.config.db_limit,
                "redis_scan_limit": self.config.redis_scan_limit,
                "max_results": self.config.max_results,
                "prefer_verdict": self.config.prefer_verdict,
                "include_processed": self.config.include_processed,
                "include_suppressed": self.config.include_suppressed,
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
                "event_payload_omitted": True,
                "bundle_context_omitted": True,
                "raw_source_text_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "sql_text_omitted": True,
                "exception_detail_omitted": True,
            },
        }
        return report


class BoundedPolicyApplyInventoryError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class ReadOnlyRedisMessageReader(Protocol):
    async def read_candidate_messages(
        self,
        *,
        queue_name: str,
        config: BoundedPolicyApplyInventoryConfig,
    ) -> list[RedisStreamMessage]: ...


class BoundedPolicyApplyInventoryRepository(Protocol):
    async def load_policy_apply_events(self, *, db_limit: int) -> list[OutboxEventRow]: ...
    async def load_candidate_context(self, candidate_group_id: UUID) -> CandidatePolicyContext | None: ...
    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunPolicyContext | None: ...
    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputPolicyContext | None: ...
    async def load_bundle_context(self, bundle_id: UUID) -> BundlePolicyContext | None: ...
    async def load_existing_analysis_inventory(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> ExistingAnalysisInventoryRecord | None: ...
    async def load_notification_plan_intent_outboxes(self, intent: NotificationPlanIntent) -> list[OutboxEventRow]: ...


@dataclass(frozen=True, slots=True)
class BoundedPolicyApplyInventoryRedisReaderHandle:
    reader: ReadOnlyRedisMessageReader
    close: Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedPolicyApplyInventoryRepositoryHandle:
    repository: BoundedPolicyApplyInventoryRepository
    close: Callable[[], Awaitable[None]]


class BoundedPolicyApplyInventoryRedisReaderBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedPolicyApplyInventoryRuntimeConfig,
        state: BoundedPolicyApplyInventoryState,
        logger: logging.Logger,
    ) -> BoundedPolicyApplyInventoryRedisReaderHandle: ...


class BoundedPolicyApplyInventoryRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedPolicyApplyInventoryRuntimeConfig,
        state: BoundedPolicyApplyInventoryState,
        logger: logging.Logger,
    ) -> BoundedPolicyApplyInventoryRepositoryHandle: ...


class RedisReadOnlyStreamReader:
    def __init__(self, client: Any) -> None:
        self._client = client

    async def read_candidate_messages(
        self,
        *,
        queue_name: str,
        config: BoundedPolicyApplyInventoryConfig,
    ) -> list[RedisStreamMessage]:
        raw_messages = await self._client.xrevrange(queue_name, count=config.redis_scan_limit)
        return [_normalize_redis_message(message) for message in raw_messages]


class SqlAlchemyBoundedPolicyApplyInventoryRepository:
    def __init__(self, session: Any) -> None:
        self._session = session
        self._policy_repository = PolicyEngineRepository(session)

    async def load_policy_apply_events(self, *, db_limit: int) -> list[OutboxEventRow]:
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       dedupe_key, payload_json, status, fail_count, created_at
                FROM event_outbox
                WHERE event_type = 'analysis.policy.apply.v1'
                ORDER BY created_at DESC, event_id DESC
                LIMIT :limit
                """
            ),
            {"limit": db_limit},
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

    async def load_existing_analysis_inventory(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> ExistingAnalysisInventoryRecord | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT analysis_id, judge_output_id, policy_version,
                       delivery_policy_version, verdict, delivery_decision
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
        if row is None:
            return None
        return ExistingAnalysisInventoryRecord(
            analysis_id=UUID(str(row["analysis_id"])),
            judge_output_id=UUID(str(row["judge_output_id"])),
            policy_version=str(row["policy_version"]),
            delivery_policy_version=str(row["delivery_policy_version"]),
            verdict=str(row["verdict"]),
            delivery_decision=str(row["delivery_decision"]),
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


def load_bounded_policy_apply_inventory_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedPolicyApplyInventoryRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedPolicyApplyInventoryError("database_url_missing")
    if not redis_url:
        raise BoundedPolicyApplyInventoryError("redis_url_missing")
    queue_name = _env_value(source, "POLICY_ENGINE_QUEUE_NAME", INPUT_QUEUE_NAME)
    if queue_name != INPUT_QUEUE_NAME:
        raise BoundedPolicyApplyInventoryError("queue_not_allowed")
    try:
        operator_chat_id = int(_env_value(source, "TELEGRAM_OPERATOR_CHAT_ID", "0"))
        debug_chat_id = _optional_int(_env_value(source, "TELEGRAM_DEBUG_CHAT_ID"))
        digest_chat_id = _optional_int(_env_value(source, "TELEGRAM_DIGEST_CHAT_ID"))
    except ValueError as exc:
        raise BoundedPolicyApplyInventoryError("runtime_config_error") from exc
    config = BoundedPolicyApplyInventoryRuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        queue_name=queue_name,
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
        raise BoundedPolicyApplyInventoryError("runtime_config_error") from exc
    return config


async def build_default_bounded_policy_apply_inventory_redis_reader(
    runtime_config: BoundedPolicyApplyInventoryRuntimeConfig,
    state: BoundedPolicyApplyInventoryState,
    logger: logging.Logger,
) -> BoundedPolicyApplyInventoryRedisReaderHandle:
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

    return BoundedPolicyApplyInventoryRedisReaderHandle(reader=reader, close=close)


async def build_default_bounded_policy_apply_inventory_repository(
    runtime_config: BoundedPolicyApplyInventoryRuntimeConfig,
    state: BoundedPolicyApplyInventoryState,
    logger: logging.Logger,
) -> BoundedPolicyApplyInventoryRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedPolicyApplyInventoryRepository(session)

    async def close() -> None:
        await session.close()
        await engine.dispose()

    return BoundedPolicyApplyInventoryRepositoryHandle(repository=repository, close=close)


async def run_bounded_policy_apply_inventory(
    config: BoundedPolicyApplyInventoryConfig,
    *,
    runtime_config_loader: Callable[[], BoundedPolicyApplyInventoryRuntimeConfig] = (
        load_bounded_policy_apply_inventory_runtime_config
    ),
    redis_reader_builder: BoundedPolicyApplyInventoryRedisReaderBuilder | None = None,
    repository_builder: BoundedPolicyApplyInventoryRepositoryBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedPolicyApplyInventoryResult:
    state = BoundedPolicyApplyInventoryState()
    gate_error = _authority_gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
        policy_config = runtime_config.to_policy_config()
    except BoundedPolicyApplyInventoryError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception as exc:
        return _result(
            "blocked",
            "runtime_config_error",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
        )

    repository_handle: BoundedPolicyApplyInventoryRepositoryHandle | None = None
    redis_handle: BoundedPolicyApplyInventoryRedisReaderHandle | None = None
    candidates: tuple[InventoryCandidate, ...] = ()
    counts = InventoryCounts()
    try:
        repository_handle = await (repository_builder or build_default_bounded_policy_apply_inventory_repository)(
            runtime_config,
            state,
            effective_logger,
        )
        repository = repository_handle.repository
        state.database_read_attempted = True
        events = await repository.load_policy_apply_events(db_limit=config.db_limit)

        redis_handle = await (redis_reader_builder or build_default_bounded_policy_apply_inventory_redis_reader)(
            runtime_config,
            state,
            effective_logger,
        )
        state.redis_read_attempted = True
        redis_messages = await redis_handle.reader.read_candidate_messages(
            queue_name=runtime_config.queue_name,
            config=config,
        )
        redis_by_trigger_event_id = _valid_redis_messages_by_trigger_event_id(redis_messages)

        inventory = []
        for event in events:
            inventory.append(
                await _classify_event(
                    event=event,
                    repository=repository,
                    policy_config=policy_config,
                    redis_by_trigger_event_id=redis_by_trigger_event_id,
                    state=state,
                )
            )

        counts = _counts(inventory, db_count=len(events), redis_count=len(redis_messages))
        visible = [
            candidate
            for candidate in inventory
            if _candidate_visible(
                candidate,
                include_processed=config.include_processed,
                include_suppressed=config.include_suppressed,
                prefer_verdict=config.prefer_verdict,
            )
        ]
        candidates = tuple(sorted(visible, key=_sort_key)[: config.max_results])
        selected_direct = next(
            (
                candidate
                for candidate in sorted(inventory, key=_sort_key)
                if candidate.classification == "direct_runnable_non_suppress"
                and _preferred_verdict_matches(config.prefer_verdict, candidate.predicted_verdict)
            ),
            None,
        )
        selected_recovery = next(
            (
                candidate
                for candidate in sorted(inventory, key=_sort_key)
                if candidate.classification == "db_non_suppress_missing_redis"
                and _preferred_verdict_matches(config.prefer_verdict, candidate.predicted_verdict)
            ),
            None,
        )
        if selected_recovery is None:
            selected_recovery = next(
                (
                    candidate
                    for candidate in sorted(inventory, key=_sort_key)
                    if candidate.classification == "db_non_suppress_unpublished_outbox"
                    and _preferred_verdict_matches(config.prefer_verdict, candidate.predicted_verdict)
                ),
                None,
            )
        return _result(
            "inventory_completed",
            None,
            config=config,
            state=state,
            counts=counts,
            selected_direct_target=selected_direct,
            selected_recovery_target=selected_recovery,
            candidates=candidates,
        )
    except Exception as exc:
        return _result(
            "failed",
            "bounded_policy_apply_inventory_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            counts=counts,
            candidates=candidates,
        )
    finally:
        if redis_handle is not None:
            try:
                await redis_handle.close()
            except Exception:
                pass
        if repository_handle is not None:
            try:
                await repository_handle.close()
            except Exception:
                pass


def run_bounded_policy_apply_inventory_sync(
    config: BoundedPolicyApplyInventoryConfig,
    *,
    runtime_config_loader: Callable[[], BoundedPolicyApplyInventoryRuntimeConfig] = (
        load_bounded_policy_apply_inventory_runtime_config
    ),
    redis_reader_builder: BoundedPolicyApplyInventoryRedisReaderBuilder | None = None,
    repository_builder: BoundedPolicyApplyInventoryRepositoryBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedPolicyApplyInventoryResult:
    return asyncio.run(
        run_bounded_policy_apply_inventory(
            config,
            runtime_config_loader=runtime_config_loader,
            redis_reader_builder=redis_reader_builder,
            repository_builder=repository_builder,
            logger=logger,
        )
    )


async def _classify_event(
    *,
    event: OutboxEventRow,
    repository: BoundedPolicyApplyInventoryRepository,
    policy_config: PolicyEngineConfig,
    redis_by_trigger_event_id: Mapping[UUID, RedisStreamMessage],
    state: BoundedPolicyApplyInventoryState,
) -> InventoryCandidate:
    event_error = _validate_inventory_event(event, trigger_event_id=event.event_id, root_judge_run_id=event.aggregate_id)
    if event_error is not None:
        return _blocked_candidate(event=event, reason_code=event_error)

    payload_judge_run_id = _payload_uuid(event.payload_json, "judge_run_id")
    payload_judge_output_id = _payload_uuid(event.payload_json, "judge_output_id")
    payload_candidate_group_id = _payload_uuid(event.payload_json, "candidate_group_id")
    payload_bundle_id = _payload_uuid(event.payload_json, "bundle_id")
    if None in {payload_judge_run_id, payload_judge_output_id, payload_candidate_group_id, payload_bundle_id}:
        return _blocked_candidate(event=event, reason_code="event_payload_malformed")
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
        return _blocked_candidate(
            event=event,
            reason_code=context_error,
            judge_run_id=payload_judge_run_id,
            judge_output_id=payload_judge_output_id,
            bundle_id=payload_bundle_id,
            candidate_group_id=payload_candidate_group_id,
        )

    assert candidate is not None
    assert judge_run is not None
    assert judge_output is not None
    assert bundle is not None

    state.policy_preview_called = True
    analysis_draft, evaluation = _build_analysis(
        policy_config=policy_config,
        judge_run=judge_run,
        judge_output=judge_output,
        bundle=bundle,
    )
    existing_analysis = await repository.load_existing_analysis_inventory(
        judge_output_id=judge_output.judge_output_id,
        policy_version=analysis_draft.policy_version,
        delivery_policy_version=analysis_draft.delivery_policy_version,
    )
    redis_message = redis_by_trigger_event_id.get(event.event_id)
    if existing_analysis is None:
        if analysis_draft.delivery_decision == "suppress":
            return _candidate_from_context(
                classification="unprocessed_suppress",
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                analysis=analysis_draft,
                evaluation=evaluation,
                analysis_exists=False,
                redis_message=redis_message,
                reason_code="preview_delivery_suppress",
            )
        if event.status != "published":
            return _candidate_from_context(
                classification="db_non_suppress_unpublished_outbox",
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                analysis=analysis_draft,
                evaluation=evaluation,
                analysis_exists=False,
                redis_message=redis_message,
                reason_code="outbox_status_not_published",
            )
        if redis_message is None:
            return _candidate_from_context(
                classification="db_non_suppress_missing_redis",
                event=event,
                candidate=candidate,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                analysis=analysis_draft,
                evaluation=evaluation,
                analysis_exists=False,
                reason_code="redis_message_missing",
            )
        return _candidate_from_context(
            classification="direct_runnable_non_suppress",
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            analysis=analysis_draft,
            evaluation=evaluation,
            analysis_exists=False,
            redis_message=redis_message,
        )

    analysis_draft, evaluation = _apply_existing_analysis_decision(
        analysis=analysis_draft,
        evaluation=evaluation,
        existing_analysis=existing_analysis,
    )
    if analysis_draft.delivery_decision == "suppress":
        return _candidate_from_context(
            classification="processed_suppress",
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            analysis=analysis_draft,
            evaluation=evaluation,
            analysis_exists=True,
            redis_message=redis_message,
            reason_code="existing_analysis_suppress",
        )

    notification_intent = NotificationIntentBuilder(config=policy_config).build(
        analysis_id=existing_analysis.analysis_id,
        analysis=analysis_draft,
        evaluation=evaluation,
    )
    if notification_intent is None:
        return _candidate_from_context(
            classification="processed_notification_invalid",
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            analysis=analysis_draft,
            evaluation=evaluation,
            analysis_exists=True,
            redis_message=redis_message,
            reason_code="notification_intent_unavailable",
        )
    matching_outboxes = await repository.load_notification_plan_intent_outboxes(notification_intent)
    if not matching_outboxes:
        return _candidate_from_context(
            classification="processed_notification_missing",
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            analysis=analysis_draft,
            evaluation=evaluation,
            analysis_exists=True,
            redis_message=redis_message,
            reason_code="notification_plan_intent_missing",
        )
    if len(matching_outboxes) != 1:
        return _candidate_from_context(
            classification="processed_notification_invalid",
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            analysis=analysis_draft,
            evaluation=evaluation,
            analysis_exists=True,
            redis_message=redis_message,
            reason_code="duplicate_notification_plan_intent_outbox",
        )
    notification_outbox = matching_outboxes[0]
    notification_error = _validate_notification_outbox(notification_outbox, intent=notification_intent)
    if notification_error is not None:
        return _candidate_from_context(
            classification="processed_notification_invalid",
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            analysis=analysis_draft,
            evaluation=evaluation,
            analysis_exists=True,
            redis_message=redis_message,
            notification_outbox_status=notification_outbox.status,
            reason_code=notification_error,
        )
    if notification_outbox.status == "pending":
        return _candidate_from_context(
            classification="processed_notification_pending",
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            analysis=analysis_draft,
            evaluation=evaluation,
            analysis_exists=True,
            redis_message=redis_message,
            notification_outbox_status=notification_outbox.status,
        )
    if notification_outbox.status == "published":
        return _candidate_from_context(
            classification="processed_notification_published",
            event=event,
            candidate=candidate,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            analysis=analysis_draft,
            evaluation=evaluation,
            analysis_exists=True,
            redis_message=redis_message,
            notification_outbox_status=notification_outbox.status,
        )
    return _candidate_from_context(
        classification="processed_notification_invalid",
        event=event,
        candidate=candidate,
        judge_run=judge_run,
        judge_output=judge_output,
        bundle=bundle,
        analysis=analysis_draft,
        evaluation=evaluation,
        analysis_exists=True,
        redis_message=redis_message,
        notification_outbox_status=notification_outbox.status,
        reason_code="notification_plan_intent_outbox_status_invalid",
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        error_code,
        config=BoundedPolicyApplyInventoryConfig(),
        state=BoundedPolicyApplyInventoryState(),
    ).to_sanitized_dict()


def _authority_gate_error(config: BoundedPolicyApplyInventoryConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    if not 1 <= config.db_limit <= MAX_DB_LIMIT:
        return "invalid_db_limit"
    if not 1 <= config.redis_scan_limit <= MAX_REDIS_SCAN_LIMIT:
        return "invalid_redis_scan_limit"
    if not 1 <= config.max_results <= MAX_RESULTS:
        return "invalid_max_results"
    if config.prefer_verdict not in {"inspect_now", "later", "any"}:
        return "invalid_prefer_verdict"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if not config.allow_redis_read:
        return "redis_read_not_allowed"
    if not config.allow_policy_preview:
        return "policy_preview_not_allowed"
    return None


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedPolicyApplyInventoryConfig,
    state: BoundedPolicyApplyInventoryState,
    error_class: str | None = None,
    counts: InventoryCounts | None = None,
    selected_direct_target: InventoryCandidate | None = None,
    selected_recovery_target: InventoryCandidate | None = None,
    candidates: tuple[InventoryCandidate, ...] = (),
) -> BoundedPolicyApplyInventoryResult:
    return BoundedPolicyApplyInventoryResult(
        status=status,
        ok=status == "inventory_completed" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        counts=counts or InventoryCounts(),
        selected_direct_target=selected_direct_target,
        selected_recovery_target=selected_recovery_target,
        candidates=candidates,
    )


def _valid_redis_messages_by_trigger_event_id(messages: list[RedisStreamMessage]) -> dict[UUID, RedisStreamMessage]:
    by_event_id: dict[UUID, RedisStreamMessage] = {}
    for message in messages:
        if _validate_redis_message(message) is not None:
            continue
        trigger_event_id = _safe_uuid(message.fields.get("trigger_event_id"))
        if trigger_event_id is None or trigger_event_id in by_event_id:
            continue
        by_event_id[trigger_event_id] = message
    return by_event_id


def _validate_inventory_event(
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


def _candidate_from_context(
    *,
    classification: Classification,
    event: OutboxEventRow,
    candidate: CandidatePolicyContext,
    judge_run: JudgeRunPolicyContext,
    judge_output: JudgeOutputPolicyContext,
    bundle: BundlePolicyContext,
    analysis: AnalysisDraft,
    evaluation: PolicyEvaluation,
    analysis_exists: bool,
    redis_message: RedisStreamMessage | None = None,
    notification_outbox_status: str | None = None,
    reason_code: str | None = None,
) -> InventoryCandidate:
    return InventoryCandidate(
        classification=classification,
        policy_apply_event_suffix=_optional_id_suffix(event.event_id),
        judge_run_suffix=_optional_id_suffix(judge_run.judge_run_id),
        judge_output_suffix=_optional_id_suffix(judge_output.judge_output_id),
        bundle_suffix=_optional_id_suffix(bundle.bundle_id),
        candidate_group_suffix=_optional_id_suffix(candidate.candidate_group_id),
        policy_apply_outbox_status=event.status,
        predicted_verdict=analysis.verdict,
        predicted_delivery_decision=analysis.delivery_decision,
        predicted_urgency_profile=evaluation.urgency_profile,
        analysis_exists=analysis_exists,
        redis_message_id_suffix=_redis_message_id_suffix(redis_message.message_id if redis_message is not None else None),
        notification_outbox_status=notification_outbox_status,
        reason_code=reason_code,
        created_at=event.created_at,
    )


def _apply_existing_analysis_decision(
    *,
    analysis: AnalysisDraft,
    evaluation: PolicyEvaluation,
    existing_analysis: ExistingAnalysisInventoryRecord,
) -> tuple[AnalysisDraft, PolicyEvaluation]:
    verdict = cast(Verdict, existing_analysis.verdict)
    delivery_decision = cast(DeliveryDecision, existing_analysis.delivery_decision)
    urgency_profile = _urgency_profile_from_existing_analysis(
        verdict=verdict,
        delivery_decision=delivery_decision,
        fallback=evaluation.urgency_profile,
    )
    suppress_reason_code = evaluation.suppress_reason_code
    if delivery_decision == "suppress" and suppress_reason_code is None:
        suppress_reason_code = "existing_analysis_suppress"
    if delivery_decision != "suppress":
        suppress_reason_code = None
    return (
        replace(analysis, verdict=verdict, delivery_decision=delivery_decision),
        replace(
            evaluation,
            verdict=verdict,
            delivery_decision=delivery_decision,
            urgency_profile=urgency_profile,
            suppress_reason_code=suppress_reason_code,
        ),
    )


def _urgency_profile_from_existing_analysis(
    *,
    verdict: Verdict,
    delivery_decision: DeliveryDecision,
    fallback: UrgencyProfile,
) -> UrgencyProfile:
    if delivery_decision == "suppress":
        return "suppressed"
    if delivery_decision == "send_digest":
        return "digest"
    if verdict == "inspect_now":
        return "high"
    if verdict == "later":
        return "normal_silent"
    return fallback


def _blocked_candidate(
    *,
    event: OutboxEventRow,
    reason_code: str,
    judge_run_id: UUID | None = None,
    judge_output_id: UUID | None = None,
    bundle_id: UUID | None = None,
    candidate_group_id: UUID | None = None,
) -> InventoryCandidate:
    payload = event.payload_json if isinstance(event.payload_json, Mapping) else {}
    return InventoryCandidate(
        classification="blocked",
        policy_apply_event_suffix=_optional_id_suffix(event.event_id),
        judge_run_suffix=_optional_id_suffix(judge_run_id or _payload_uuid(payload, "judge_run_id")),
        judge_output_suffix=_optional_id_suffix(judge_output_id or _payload_uuid(payload, "judge_output_id")),
        bundle_suffix=_optional_id_suffix(bundle_id or _payload_uuid(payload, "bundle_id")),
        candidate_group_suffix=_optional_id_suffix(candidate_group_id or _payload_uuid(payload, "candidate_group_id")),
        policy_apply_outbox_status=event.status,
        predicted_verdict=None,
        predicted_delivery_decision=None,
        predicted_urgency_profile=None,
        analysis_exists=False,
        reason_code=reason_code,
        created_at=event.created_at,
    )


def _counts(
    candidates: list[InventoryCandidate],
    *,
    db_count: int,
    redis_count: int,
) -> InventoryCounts:
    return InventoryCounts(
        db_policy_apply_event_count=db_count,
        redis_message_count=redis_count,
        rehydrated_event_count=sum(1 for item in candidates if item.classification != "blocked"),
        direct_runnable_non_suppress_count=sum(
            1 for item in candidates if item.classification == "direct_runnable_non_suppress"
        ),
        db_non_suppress_missing_redis_count=sum(
            1 for item in candidates if item.classification == "db_non_suppress_missing_redis"
        ),
        db_non_suppress_unpublished_outbox_count=sum(
            1 for item in candidates if item.classification == "db_non_suppress_unpublished_outbox"
        ),
        unprocessed_suppress_count=sum(1 for item in candidates if item.classification == "unprocessed_suppress"),
        processed_suppress_count=sum(1 for item in candidates if item.classification == "processed_suppress"),
        processed_notification_pending_count=sum(
            1 for item in candidates if item.classification == "processed_notification_pending"
        ),
        processed_notification_published_count=sum(
            1 for item in candidates if item.classification == "processed_notification_published"
        ),
        processed_notification_missing_count=sum(
            1 for item in candidates if item.classification == "processed_notification_missing"
        ),
        processed_notification_invalid_count=sum(
            1 for item in candidates if item.classification == "processed_notification_invalid"
        ),
        blocked_count=sum(1 for item in candidates if item.classification == "blocked"),
    )


def _candidate_visible(
    candidate: InventoryCandidate,
    *,
    include_processed: bool,
    include_suppressed: bool,
    prefer_verdict: str,
) -> bool:
    if candidate.classification in {"unprocessed_suppress", "processed_suppress"}:
        return include_suppressed
    if candidate.analysis_exists and not include_processed:
        return False
    if candidate.classification in {
        "direct_runnable_non_suppress",
        "db_non_suppress_missing_redis",
        "db_non_suppress_unpublished_outbox",
    }:
        return _preferred_verdict_matches(prefer_verdict, candidate.predicted_verdict)
    return True


def _preferred_verdict_matches(prefer_verdict: str, verdict: str | None) -> bool:
    return prefer_verdict == "any" or verdict == prefer_verdict


def _sort_key(candidate: InventoryCandidate) -> tuple[int, int]:
    created_micros = 0
    if candidate.created_at is not None:
        created_micros = int(candidate.created_at.timestamp() * 1_000_000)
    return (_classification_priority(candidate), -created_micros)


def _classification_priority(candidate: InventoryCandidate) -> int:
    verdict = candidate.predicted_verdict
    if candidate.classification == "direct_runnable_non_suppress" and verdict == "inspect_now":
        return 0
    if candidate.classification == "direct_runnable_non_suppress" and verdict == "later":
        return 1
    if candidate.classification == "db_non_suppress_missing_redis" and verdict == "inspect_now":
        return 2
    if candidate.classification == "db_non_suppress_missing_redis" and verdict == "later":
        return 3
    if candidate.classification == "db_non_suppress_unpublished_outbox" and verdict == "inspect_now":
        return 4
    if candidate.classification == "db_non_suppress_unpublished_outbox" and verdict == "later":
        return 5
    if candidate.classification == "processed_notification_pending":
        return 6
    if candidate.classification in {"processed_notification_missing", "processed_notification_invalid", "blocked"}:
        return 7
    if candidate.classification == "processed_notification_published":
        return 8
    return 9


def _ready_policy_runner_argv(candidate: InventoryCandidate, *, redis_scan_limit: int) -> list[str]:
    return [
        "venv/bin/python",
        READY_RUNNER_PATH,
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-redis-read",
        "--allow-redis-publish",
        "--allow-database-read",
        "--allow-database-write",
        "--allow-policy-engine",
        "--redis-message-suffix",
        candidate.redis_message_id_suffix or "",
        "--trigger-event-suffix",
        candidate.policy_apply_event_suffix or "",
        "--judge-run-suffix",
        candidate.judge_run_suffix or "",
        "--judge-output-suffix",
        candidate.judge_output_suffix or "",
        "--bundle-suffix",
        candidate.bundle_suffix or "",
        "--candidate-group-suffix",
        candidate.candidate_group_suffix or "",
        "--scan-limit",
        str(redis_scan_limit),
    ]


def _env_value(source: Mapping[str, str], key: str, default: str = "") -> str:
    return str(source.get(key, default) or "").strip()


def _optional_int(value: str) -> int | None:
    if not value:
        return None
    return int(value)


def _bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (JSONDecodeError, TypeError):
            return None
    return value


__all__ = [
    "BoundedPolicyApplyInventoryConfig",
    "BoundedPolicyApplyInventoryError",
    "BoundedPolicyApplyInventoryRedisReaderBuilder",
    "BoundedPolicyApplyInventoryRedisReaderHandle",
    "BoundedPolicyApplyInventoryRepository",
    "BoundedPolicyApplyInventoryRepositoryBuilder",
    "BoundedPolicyApplyInventoryRepositoryHandle",
    "BoundedPolicyApplyInventoryResult",
    "BoundedPolicyApplyInventoryRuntimeConfig",
    "BoundedPolicyApplyInventoryState",
    "ExistingAnalysisInventoryRecord",
    "InventoryCandidate",
    "RedisReadOnlyStreamReader",
    "SqlAlchemyBoundedPolicyApplyInventoryRepository",
    "argument_error_report",
    "load_bounded_policy_apply_inventory_runtime_config",
    "render_sanitized_json",
    "run_bounded_policy_apply_inventory",
    "run_bounded_policy_apply_inventory_sync",
]
