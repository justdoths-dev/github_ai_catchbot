from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from json import JSONDecodeError
from typing import Any, Literal, Protocol
from uuid import UUID

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - static validation fallback
    sa = None

from ..outbox_relay.models import OutboxEventRow
from .bounded_analysis_runner import (
    INPUT_EVENT_TYPE,
    INPUT_QUEUE_NAME,
    INPUT_STAGE_NAME,
    ROOT_OBJECT_TYPE,
    REQUIRED_EVENT_PAYLOAD_FIELDS,
    RedisStreamMessage,
    _build_analysis,
    _judge_output_refusal_detected,
    _judge_output_schema_version,
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
    _validate_event,
    _validate_notification_outbox,
    _validate_redis_message,
)
from .config import PolicyEngineConfig, PolicyEngineConfigurationError
from .models import (
    BundlePolicyContext,
    CandidatePolicyContext,
    ExistingAnalysisRecord,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
)
from .notification_intent import NotificationIntentBuilder
from .repositories import PolicyEngineRepository


SCHEMA_VERSION = "bounded_policy_non_suppress_target_selector_v1"
RUNNER_NAME = "bounded_policy_non_suppress_target_selector"
MODE = "policy_non_suppress_exact_target_selection"
DEFAULT_SCAN_LIMIT = 100
MAX_SCAN_LIMIT = 500
DEFAULT_MAX_RESULTS = 5
MAX_RESULTS = 25
READY_RUNNER_PATH = "tools/bounded_policy_engine_analysis_runner.py"

PreferVerdict = Literal["inspect_now", "later", "any"]


@dataclass(frozen=True, slots=True)
class BoundedPolicyNonSuppressTargetSelectorConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_read: bool = False
    allow_database_read: bool = False
    allow_policy_preview: bool = False
    scan_limit: int = DEFAULT_SCAN_LIMIT
    max_results: int = DEFAULT_MAX_RESULTS
    prefer_verdict: str = "any"


@dataclass(frozen=True, slots=True)
class BoundedPolicyNonSuppressTargetSelectorRuntimeConfig:
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
            consumer_name="bounded-policy-non-suppress-target-selector",
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
class BoundedPolicyNonSuppressTargetSelectorState:
    runtime_config_loaded: bool = False
    redis_reader_created: bool = False
    redis_read_attempted: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    policy_preview_called: bool = False


@dataclass(frozen=True, slots=True)
class SelectedPolicyTarget:
    redis_message_id_suffix: str | None
    policy_apply_event_suffix: str | None
    judge_run_suffix: str | None
    judge_output_suffix: str | None
    bundle_suffix: str | None
    candidate_group_suffix: str | None
    predicted_verdict: str
    predicted_delivery_decision: str
    predicted_urgency_profile: str
    analysis_exists: bool
    notification_outbox_exists: bool
    notification_outbox_status: str | None
    sort_priority: int = field(default=99, repr=False)
    redis_index: int = field(default=0, repr=False)

    def to_sanitized_dict(self, *, scan_limit: int) -> dict[str, Any]:
        return {
            "redis_message_id_suffix": self.redis_message_id_suffix,
            "policy_apply_event_suffix": self.policy_apply_event_suffix,
            "judge_run_suffix": self.judge_run_suffix,
            "judge_output_suffix": self.judge_output_suffix,
            "bundle_suffix": self.bundle_suffix,
            "candidate_group_suffix": self.candidate_group_suffix,
            "predicted_verdict": self.predicted_verdict,
            "predicted_delivery_decision": self.predicted_delivery_decision,
            "predicted_urgency_profile": self.predicted_urgency_profile,
            "analysis_exists": self.analysis_exists,
            "notification_outbox_exists": self.notification_outbox_exists,
            "notification_outbox_status": self.notification_outbox_status,
            "ready_policy_runner_argv": _ready_policy_runner_argv(self, scan_limit=scan_limit),
        }


@dataclass(frozen=True, slots=True)
class SelectorCounts:
    redis_message_count: int = 0
    candidate_message_count: int = 0
    rehydrated_event_count: int = 0
    eligible_non_suppress_count: int = 0
    suppressed_count: int = 0
    already_processed_count: int = 0
    blocked_candidate_count: int = 0


@dataclass(frozen=True, slots=True)
class BoundedPolicyNonSuppressTargetSelectorResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedPolicyNonSuppressTargetSelectorConfig
    state: BoundedPolicyNonSuppressTargetSelectorState = field(
        default_factory=BoundedPolicyNonSuppressTargetSelectorState
    )
    counts: SelectorCounts = field(default_factory=SelectorCounts)
    selected_target: SelectedPolicyTarget | None = None
    candidates: tuple[SelectedPolicyTarget, ...] = ()

    def to_sanitized_dict(self) -> dict[str, Any]:
        report: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "scan_limit": self.config.scan_limit,
            "max_results": self.config.max_results,
            "prefer_verdict": self.config.prefer_verdict,
            "redis_message_count": self.counts.redis_message_count,
            "candidate_message_count": self.counts.candidate_message_count,
            "rehydrated_event_count": self.counts.rehydrated_event_count,
            "eligible_non_suppress_count": self.counts.eligible_non_suppress_count,
            "suppressed_count": self.counts.suppressed_count,
            "already_processed_count": self.counts.already_processed_count,
            "blocked_candidate_count": self.counts.blocked_candidate_count,
            "candidates": [
                candidate.to_sanitized_dict(scan_limit=self.config.scan_limit) for candidate in self.candidates
            ],
            "redis_read_attempted": self.state.redis_read_attempted,
            "redis_publish_attempted": False,
            "redis_ack_called": False,
            "redis_consume_called": False,
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": False,
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
                "redis_read_allowed": self.config.allow_redis_read,
                "database_read_allowed": self.config.allow_database_read,
                "policy_preview_allowed": self.config.allow_policy_preview,
                "scan_limit": self.config.scan_limit,
                "max_results": self.config.max_results,
                "prefer_verdict": self.config.prefer_verdict,
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
        if self.selected_target is not None:
            report["selected_target"] = self.selected_target.to_sanitized_dict(scan_limit=self.config.scan_limit)
        return report


class BoundedPolicyNonSuppressTargetSelectorError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class ReadOnlyRedisMessageReader(Protocol):
    async def read_candidate_messages(
        self,
        *,
        queue_name: str,
        config: BoundedPolicyNonSuppressTargetSelectorConfig,
    ) -> list[RedisStreamMessage]: ...


class BoundedPolicyNonSuppressTargetSelectorRepository(Protocol):
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
    async def load_notification_plan_intent_outboxes(self, intent: Any) -> list[OutboxEventRow]: ...


@dataclass(frozen=True, slots=True)
class BoundedPolicyNonSuppressRedisReaderHandle:
    reader: ReadOnlyRedisMessageReader
    close: Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedPolicyNonSuppressRepositoryHandle:
    repository: BoundedPolicyNonSuppressTargetSelectorRepository
    close: Callable[[], Awaitable[None]]


class BoundedPolicyNonSuppressRedisReaderBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedPolicyNonSuppressTargetSelectorRuntimeConfig,
        state: BoundedPolicyNonSuppressTargetSelectorState,
        logger: logging.Logger,
    ) -> BoundedPolicyNonSuppressRedisReaderHandle: ...


class BoundedPolicyNonSuppressRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedPolicyNonSuppressTargetSelectorRuntimeConfig,
        state: BoundedPolicyNonSuppressTargetSelectorState,
        logger: logging.Logger,
    ) -> BoundedPolicyNonSuppressRepositoryHandle: ...


class RedisReadOnlyStreamReader:
    def __init__(self, client: Any) -> None:
        self._client = client

    async def read_candidate_messages(
        self,
        *,
        queue_name: str,
        config: BoundedPolicyNonSuppressTargetSelectorConfig,
    ) -> list[RedisStreamMessage]:
        raw_messages = await self._client.xrevrange(queue_name, count=config.scan_limit)
        return [_normalize_redis_message(message) for message in raw_messages]


class SqlAlchemyBoundedPolicyNonSuppressTargetSelectorRepository:
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

    async def load_notification_plan_intent_outboxes(self, intent: Any) -> list[OutboxEventRow]:
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


def load_bounded_policy_non_suppress_target_selector_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedPolicyNonSuppressTargetSelectorRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedPolicyNonSuppressTargetSelectorError("database_url_missing")
    if not redis_url:
        raise BoundedPolicyNonSuppressTargetSelectorError("redis_url_missing")
    queue_name = _env_value(source, "POLICY_ENGINE_QUEUE_NAME", INPUT_QUEUE_NAME)
    if queue_name != INPUT_QUEUE_NAME:
        raise BoundedPolicyNonSuppressTargetSelectorError("queue_not_allowed")
    try:
        operator_chat_id = int(_env_value(source, "TELEGRAM_OPERATOR_CHAT_ID", "0"))
        debug_chat_id = _optional_int(_env_value(source, "TELEGRAM_DEBUG_CHAT_ID"))
        digest_chat_id = _optional_int(_env_value(source, "TELEGRAM_DIGEST_CHAT_ID"))
    except ValueError as exc:
        raise BoundedPolicyNonSuppressTargetSelectorError("runtime_config_error") from exc
    config = BoundedPolicyNonSuppressTargetSelectorRuntimeConfig(
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
        raise BoundedPolicyNonSuppressTargetSelectorError("runtime_config_error") from exc
    return config


async def build_default_bounded_policy_non_suppress_redis_reader(
    runtime_config: BoundedPolicyNonSuppressTargetSelectorRuntimeConfig,
    state: BoundedPolicyNonSuppressTargetSelectorState,
    logger: logging.Logger,
) -> BoundedPolicyNonSuppressRedisReaderHandle:
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

    return BoundedPolicyNonSuppressRedisReaderHandle(reader=reader, close=close)


async def build_default_bounded_policy_non_suppress_repository(
    runtime_config: BoundedPolicyNonSuppressTargetSelectorRuntimeConfig,
    state: BoundedPolicyNonSuppressTargetSelectorState,
    logger: logging.Logger,
) -> BoundedPolicyNonSuppressRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedPolicyNonSuppressTargetSelectorRepository(session)

    async def close() -> None:
        await session.close()
        await engine.dispose()

    return BoundedPolicyNonSuppressRepositoryHandle(repository=repository, close=close)


async def run_bounded_policy_non_suppress_target_selector(
    config: BoundedPolicyNonSuppressTargetSelectorConfig,
    *,
    runtime_config_loader: Callable[[], BoundedPolicyNonSuppressTargetSelectorRuntimeConfig] = (
        load_bounded_policy_non_suppress_target_selector_runtime_config
    ),
    redis_reader_builder: BoundedPolicyNonSuppressRedisReaderBuilder | None = None,
    repository_builder: BoundedPolicyNonSuppressRepositoryBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedPolicyNonSuppressTargetSelectorResult:
    state = BoundedPolicyNonSuppressTargetSelectorState()
    gate_error = _authority_gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
        policy_config = runtime_config.to_policy_config()
    except BoundedPolicyNonSuppressTargetSelectorError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception as exc:
        return _result(
            "blocked",
            "runtime_config_error",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
        )

    redis_handle: BoundedPolicyNonSuppressRedisReaderHandle | None = None
    repository_handle: BoundedPolicyNonSuppressRepositoryHandle | None = None
    selected: tuple[SelectedPolicyTarget, ...] = ()
    counts = SelectorCounts()
    try:
        redis_handle = await (redis_reader_builder or build_default_bounded_policy_non_suppress_redis_reader)(
            runtime_config,
            state,
            effective_logger,
        )
        state.redis_read_attempted = True
        messages = await redis_handle.reader.read_candidate_messages(queue_name=runtime_config.queue_name, config=config)
        counts = SelectorCounts(redis_message_count=len(messages), candidate_message_count=len(messages))

        repository: BoundedPolicyNonSuppressTargetSelectorRepository | None = None

        async def ensure_repository() -> BoundedPolicyNonSuppressTargetSelectorRepository:
            nonlocal repository_handle, repository
            if repository_handle is None:
                repository_handle = await (
                    repository_builder or build_default_bounded_policy_non_suppress_repository
                )(
                    runtime_config,
                    state,
                    effective_logger,
                )
                repository = repository_handle.repository
                state.database_read_attempted = True
            assert repository is not None
            return repository

        selected_list: list[SelectedPolicyTarget] = []
        seen_trigger_event_ids: set[UUID] = set()
        rehydrated_event_count = 0
        suppressed_count = 0
        already_processed_count = 0
        blocked_candidate_count = 0
        for redis_index, message in enumerate(messages):
            redis_error = _validate_redis_message(message)
            if redis_error is not None:
                blocked_candidate_count += 1
                continue

            trigger_event_id = _safe_uuid(message.fields.get("trigger_event_id"))
            root_judge_run_id = _safe_uuid(message.fields.get("root_object_id"))
            if trigger_event_id is None or root_judge_run_id is None:
                blocked_candidate_count += 1
                continue
            if trigger_event_id in seen_trigger_event_ids:
                blocked_candidate_count += 1
                continue
            seen_trigger_event_ids.add(trigger_event_id)

            repository = await ensure_repository()
            event = await repository.load_event_outbox(trigger_event_id)
            if event is not None:
                rehydrated_event_count += 1
            event_error = _validate_event(event, trigger_event_id=trigger_event_id, root_judge_run_id=root_judge_run_id)
            if event_error is not None:
                blocked_candidate_count += 1
                continue

            assert event is not None
            payload_judge_run_id = _payload_uuid(event.payload_json, "judge_run_id")
            payload_judge_output_id = _payload_uuid(event.payload_json, "judge_output_id")
            payload_candidate_group_id = _payload_uuid(event.payload_json, "candidate_group_id")
            payload_bundle_id = _payload_uuid(event.payload_json, "bundle_id")
            if None in {payload_judge_run_id, payload_judge_output_id, payload_candidate_group_id, payload_bundle_id}:
                blocked_candidate_count += 1
                continue
            assert payload_judge_run_id is not None
            assert payload_judge_output_id is not None
            assert payload_candidate_group_id is not None
            assert payload_bundle_id is not None

            candidate = await repository.load_candidate_context(payload_candidate_group_id)
            judge_run = await repository.load_judge_run(payload_judge_run_id)
            judge_output = await repository.load_judge_output(payload_judge_output_id)
            bundle = await repository.load_bundle_context(payload_bundle_id)
            context_error = _validate_selector_context(
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
                blocked_candidate_count += 1
                continue

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
            if analysis_draft.delivery_decision == "suppress":
                suppressed_count += 1
                continue
            if not policy_config.enable_notification_send:
                blocked_candidate_count += 1
                continue
            if not _preferred_verdict_matches(config.prefer_verdict, analysis_draft.verdict):
                continue

            existing_analysis = await repository.load_existing_analysis(
                judge_output_id=judge_output.judge_output_id,
                policy_version=analysis_draft.policy_version,
                delivery_policy_version=analysis_draft.delivery_policy_version,
            )
            notification_outbox_exists = False
            notification_outbox_status: str | None = None
            if existing_analysis is not None:
                notification_intent = NotificationIntentBuilder(config=policy_config).build(
                    analysis_id=existing_analysis.analysis_id,
                    analysis=analysis_draft,
                    evaluation=evaluation,
                )
                if notification_intent is None:
                    blocked_candidate_count += 1
                    continue
                matching_outboxes = await repository.load_notification_plan_intent_outboxes(notification_intent)
                if len(matching_outboxes) != 1:
                    blocked_candidate_count += 1
                    continue
                notification_outbox = matching_outboxes[0]
                notification_outbox_exists = True
                notification_outbox_status = notification_outbox.status
                notification_error = _validate_notification_outbox(notification_outbox, intent=notification_intent)
                if notification_error is not None:
                    blocked_candidate_count += 1
                    continue
                if notification_outbox.status == "published":
                    already_processed_count += 1
                    continue
                if notification_outbox.status != "pending":
                    blocked_candidate_count += 1
                    continue

            selected_list.append(
                SelectedPolicyTarget(
                    redis_message_id_suffix=_redis_message_id_suffix(message.message_id),
                    policy_apply_event_suffix=_optional_id_suffix(event.event_id),
                    judge_run_suffix=_optional_id_suffix(judge_run.judge_run_id),
                    judge_output_suffix=_optional_id_suffix(judge_output.judge_output_id),
                    bundle_suffix=_optional_id_suffix(bundle.bundle_id),
                    candidate_group_suffix=_optional_id_suffix(candidate.candidate_group_id),
                    predicted_verdict=analysis_draft.verdict,
                    predicted_delivery_decision=analysis_draft.delivery_decision,
                    predicted_urgency_profile=evaluation.urgency_profile,
                    analysis_exists=existing_analysis is not None,
                    notification_outbox_exists=notification_outbox_exists,
                    notification_outbox_status=notification_outbox_status,
                    sort_priority=_rank_priority(analysis_draft.verdict, evaluation.urgency_profile),
                    redis_index=redis_index,
                )
            )

        selected = tuple(
            sorted(selected_list, key=lambda item: (item.sort_priority, item.redis_index))[: config.max_results]
        )
        counts = SelectorCounts(
            redis_message_count=len(messages),
            candidate_message_count=len(messages),
            rehydrated_event_count=rehydrated_event_count,
            eligible_non_suppress_count=len(selected_list),
            suppressed_count=suppressed_count,
            already_processed_count=already_processed_count,
            blocked_candidate_count=blocked_candidate_count,
        )
        if not selected:
            return _result(
                "no_candidate_found",
                "no_eligible_non_suppress_target_found",
                config=config,
                state=state,
                counts=counts,
            )
        return _result(
            "selected",
            None,
            config=config,
            state=state,
            counts=counts,
            selected_target=selected[0],
            candidates=selected,
        )
    except Exception as exc:
        return _result(
            "failed",
            "bounded_policy_non_suppress_target_selector_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            counts=counts,
            candidates=selected,
        )
    finally:
        if repository_handle is not None:
            try:
                await repository_handle.close()
            except Exception:
                pass
        if redis_handle is not None:
            try:
                await redis_handle.close()
            except Exception:
                pass


def run_bounded_policy_non_suppress_target_selector_sync(
    config: BoundedPolicyNonSuppressTargetSelectorConfig,
    *,
    runtime_config_loader: Callable[[], BoundedPolicyNonSuppressTargetSelectorRuntimeConfig] = (
        load_bounded_policy_non_suppress_target_selector_runtime_config
    ),
    redis_reader_builder: BoundedPolicyNonSuppressRedisReaderBuilder | None = None,
    repository_builder: BoundedPolicyNonSuppressRepositoryBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedPolicyNonSuppressTargetSelectorResult:
    return asyncio.run(
        run_bounded_policy_non_suppress_target_selector(
            config,
            runtime_config_loader=runtime_config_loader,
            redis_reader_builder=redis_reader_builder,
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
        config=BoundedPolicyNonSuppressTargetSelectorConfig(),
        state=BoundedPolicyNonSuppressTargetSelectorState(),
    ).to_sanitized_dict()


def _authority_gate_error(config: BoundedPolicyNonSuppressTargetSelectorConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    if not 1 <= config.scan_limit <= MAX_SCAN_LIMIT:
        return "invalid_scan_limit"
    if not 1 <= config.max_results <= MAX_RESULTS:
        return "invalid_max_results"
    if config.prefer_verdict not in {"inspect_now", "later", "any"}:
        return "invalid_prefer_verdict"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_redis_read:
        return "redis_read_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if not config.allow_policy_preview:
        return "policy_preview_not_allowed"
    return None


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedPolicyNonSuppressTargetSelectorConfig,
    state: BoundedPolicyNonSuppressTargetSelectorState,
    error_class: str | None = None,
    counts: SelectorCounts | None = None,
    selected_target: SelectedPolicyTarget | None = None,
    candidates: tuple[SelectedPolicyTarget, ...] = (),
) -> BoundedPolicyNonSuppressTargetSelectorResult:
    return BoundedPolicyNonSuppressTargetSelectorResult(
        status=status,
        ok=status == "selected" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        counts=counts or SelectorCounts(),
        selected_target=selected_target,
        candidates=candidates,
    )


def _validate_selector_context(
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
    context_error = _validate_context(
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
        return context_error
    assert judge_output is not None
    if _judge_output_schema_version(judge_output) != "judge_output_v1":
        return "judge_output_schema_invalid"
    if _judge_output_refusal_detected(judge_output.payload_json):
        return "judge_output_refusal_detected"
    return None


def _rank_priority(verdict: str, urgency_profile: str) -> int:
    if verdict == "inspect_now" and urgency_profile == "high":
        return 0
    if verdict == "later" and urgency_profile == "normal_silent":
        return 1
    return 2


def _preferred_verdict_matches(prefer_verdict: str, verdict: str) -> bool:
    return prefer_verdict == "any" or prefer_verdict == verdict


def _ready_policy_runner_argv(target: SelectedPolicyTarget, *, scan_limit: int) -> list[str]:
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
        target.redis_message_id_suffix or "",
        "--trigger-event-suffix",
        target.policy_apply_event_suffix or "",
        "--judge-run-suffix",
        target.judge_run_suffix or "",
        "--judge-output-suffix",
        target.judge_output_suffix or "",
        "--bundle-suffix",
        target.bundle_suffix or "",
        "--candidate-group-suffix",
        target.candidate_group_suffix or "",
        "--scan-limit",
        str(scan_limit),
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
    "BoundedPolicyNonSuppressRedisReaderBuilder",
    "BoundedPolicyNonSuppressRedisReaderHandle",
    "BoundedPolicyNonSuppressRepositoryBuilder",
    "BoundedPolicyNonSuppressRepositoryHandle",
    "BoundedPolicyNonSuppressTargetSelectorConfig",
    "BoundedPolicyNonSuppressTargetSelectorError",
    "BoundedPolicyNonSuppressTargetSelectorRepository",
    "BoundedPolicyNonSuppressTargetSelectorResult",
    "BoundedPolicyNonSuppressTargetSelectorRuntimeConfig",
    "BoundedPolicyNonSuppressTargetSelectorState",
    "RedisReadOnlyStreamReader",
    "SelectedPolicyTarget",
    "SqlAlchemyBoundedPolicyNonSuppressTargetSelectorRepository",
    "argument_error_report",
    "load_bounded_policy_non_suppress_target_selector_runtime_config",
    "render_sanitized_json",
    "run_bounded_policy_non_suppress_target_selector",
    "run_bounded_policy_non_suppress_target_selector_sync",
]
