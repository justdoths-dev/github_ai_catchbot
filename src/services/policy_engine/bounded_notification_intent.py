from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from json import JSONDecodeError
from typing import Any, Protocol
from uuid import UUID

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - local fallback for static validation
    sa = None

from .config import PolicyEngineConfig, PolicyEngineConfigurationError
from .delivery_policy import DeliveryPolicy
from .models import AnalysisDraft, NotificationPlanIntent
from .notification_intent import NotificationIntentBuilder
from .repositories import PolicyEngineRepository
from .service import PolicyEngineRepositoryProtocol, PolicyEngineService
from .verdict_policy import VerdictPolicy

SCHEMA_VERSION = "bounded_policy_notification_intent_v1"
RUNNER_NAME = "bounded_policy_notification_intent_runner"
MODE = "policy_apply_one_shot_notification_intent"
EVENT_TYPE = "analysis.policy.apply.v1"
REQUIRED_PAYLOAD_FIELDS = (
    "judge_run_id",
    "judge_output_id",
    "candidate_group_id",
    "bundle_id",
)
UUID_TEXT_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"


@dataclass(frozen=True, slots=True)
class BoundedPolicyNotificationIntentConfig:
    operator_approved: bool = False
    allow_database_read: bool = False
    allow_policy_write: bool = False
    expected_pending_count: int = 1
    expected_eligible_pending_count: int | None = None

    @property
    def resolved_expected_eligible_pending_count(self) -> int:
        if self.expected_eligible_pending_count is not None:
            return self.expected_eligible_pending_count
        return self.expected_pending_count


@dataclass(slots=True)
class BoundedPolicyNotificationIntentState:
    database_session_opened: bool = False
    database_read_attempted: bool = False
    policy_invocation_attempted: bool = False
    database_write_attempted: bool = False
    event_outbox_emit_attempted: bool = False


class BoundedPolicyNotificationIntentError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


@dataclass(frozen=True, slots=True)
class PolicyApplyEventRow:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    payload_json: Mapping[str, Any]
    status: str
    created_at: Any = None


@dataclass(frozen=True, slots=True)
class PolicyApplyBacklogCounts:
    raw_pending: int = 0
    eligible_pending: int = 0
    stale_already_analyzed: int = 0
    malformed_pending: int = 0


@dataclass(frozen=True, slots=True)
class PolicyInvocationSummary:
    processed_event_count: int = 0
    analysis_created: bool = False
    state_transition_inserted: bool = False
    notification_plan_created_event_emitted: bool = False
    notification_plan_created_event_id: UUID | None = None
    delivery_decision: str | None = None
    verdict: str | None = None

    @property
    def database_write_attempted(self) -> bool:
        return self.analysis_created or self.state_transition_inserted or self.notification_plan_created_event_emitted

    @property
    def event_outbox_emit_attempted(self) -> bool:
        return self.notification_plan_created_event_emitted


class BoundedPolicyNotificationIntentRepository(Protocol):
    async def load_pending_policy_apply_event_counts(self) -> PolicyApplyBacklogCounts: ...
    async def fetch_oldest_eligible_pending_policy_apply_event(self) -> PolicyApplyEventRow | None: ...


class BoundedPolicyInvoker(Protocol):
    async def __call__(self, trigger_event_id: UUID) -> PolicyInvocationSummary: ...


@dataclass(frozen=True, slots=True)
class BoundedPolicyNotificationIntentRuntimeHandle:
    repository: BoundedPolicyNotificationIntentRepository
    policy_invoker: BoundedPolicyInvoker
    close: Callable[[bool], Awaitable[None]]


class BoundedPolicyNotificationIntentRuntimeBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: PolicyEngineConfig,
        state: BoundedPolicyNotificationIntentState,
        logger: logging.Logger,
    ) -> BoundedPolicyNotificationIntentRuntimeHandle: ...


@dataclass(frozen=True, slots=True)
class BoundedPolicyNotificationIntentResult:
    status: str
    ok: bool
    error_code: str | None
    operator_approved: bool
    database_read_allowed: bool
    policy_write_allowed: bool
    pending_policy_apply_count_observed: int | None = None
    raw_pending_policy_apply_count_observed: int | None = None
    eligible_pending_policy_apply_count_observed: int | None = None
    stale_already_analyzed_policy_apply_count_observed: int | None = None
    malformed_pending_policy_apply_count_observed: int | None = None
    selected_event_present: bool = False
    selected_event_status: str | None = None
    selected_event_id_suffix: str | None = None
    selected_aggregate_type: str | None = None
    selected_aggregate_id_suffix: str | None = None
    payload_has_judge_run_id: bool = False
    payload_has_judge_output_id: bool = False
    payload_has_candidate_group_id: bool = False
    payload_has_bundle_id: bool = False
    processed_event_count: int = 0
    analysis_created: bool = False
    state_transition_inserted: bool = False
    notification_plan_created_event_emitted: bool = False
    notification_plan_created_event_id_suffix: str | None = None
    delivery_decision: str | None = None
    verdict: str | None = None
    recommended_next_action: str | None = None
    state: BoundedPolicyNotificationIntentState = field(default_factory=BoundedPolicyNotificationIntentState)

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "operator_approved": self.operator_approved,
            "database_read_allowed": self.database_read_allowed,
            "policy_write_allowed": self.policy_write_allowed,
            "database_read_attempted": self.state.database_read_attempted,
            "pending_policy_apply_count_observed": self.pending_policy_apply_count_observed,
            "raw_pending_policy_apply_count_observed": self.raw_pending_policy_apply_count_observed,
            "eligible_pending_policy_apply_count_observed": self.eligible_pending_policy_apply_count_observed,
            "stale_already_analyzed_policy_apply_count_observed": (
                self.stale_already_analyzed_policy_apply_count_observed
            ),
            "malformed_pending_policy_apply_count_observed": self.malformed_pending_policy_apply_count_observed,
            "selected_event_present": self.selected_event_present,
            "selected_event_status": self.selected_event_status,
            "selected_event_id_suffix": self.selected_event_id_suffix,
            "selected_aggregate_type": self.selected_aggregate_type,
            "selected_aggregate_id_suffix": self.selected_aggregate_id_suffix,
            "payload_has_judge_run_id": self.payload_has_judge_run_id,
            "payload_has_judge_output_id": self.payload_has_judge_output_id,
            "payload_has_candidate_group_id": self.payload_has_candidate_group_id,
            "payload_has_bundle_id": self.payload_has_bundle_id,
            "policy_invocation_attempted": self.state.policy_invocation_attempted,
            "database_write_attempted": self.state.database_write_attempted,
            "event_outbox_emit_attempted": self.state.event_outbox_emit_attempted,
            "processed_event_count": self.processed_event_count,
            "analysis_created": self.analysis_created,
            "state_transition_inserted": self.state_transition_inserted,
            "notification_plan_created_event_emitted": self.notification_plan_created_event_emitted,
            "notification_plan_created_event_id_suffix": self.notification_plan_created_event_id_suffix,
            "delivery_decision": self.delivery_decision,
            "verdict": self.verdict,
            "recommended_next_action": self.recommended_next_action,
            "status": self.status,
            "ok": self.ok,
            "error_code": self.error_code,
            "redactions_applied": [
                "full_event_id_omitted",
                "full_aggregate_id_omitted",
                "payload_json_omitted",
                "database_url_omitted",
                "telegram_token_omitted",
                "exception_detail_omitted",
                "rendered_message_text_omitted",
            ],
            "side_effects": {
                "db_write": self.state.database_write_attempted,
                "redis_mutation": False,
                "notification_plan_table_write": False,
                "notification_render_write": False,
                "notification_delivery_record_write": False,
                "telegram_send_called": False,
                "telegram_edit_called": False,
                "worker_started": False,
                "run_forever_called": False,
                "systemd_called": False,
                "docker_called": False,
                "alembic_called": False,
                "openai_called": False,
                "github_called": False,
                "x_called": False,
                "web_called": False,
            },
        }


class SqlAlchemyBoundedPolicyNotificationIntentRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def load_pending_policy_apply_event_counts(self) -> PolicyApplyBacklogCounts:
        result = await self._session.execute(
            _sql(
                """
                WITH pending AS (
                    SELECT event_id, payload_json
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND status = 'pending'::outbox_status_enum
                ),
                payload_checked AS (
                    SELECT
                        event_id,
                        payload_json,
                        COALESCE((payload_json ->> 'judge_run_id') ~* :uuid_pattern, false) AS has_judge_run_id,
                        COALESCE((payload_json ->> 'judge_output_id') ~* :uuid_pattern, false) AS has_judge_output_id,
                        COALESCE((payload_json ->> 'candidate_group_id') ~* :uuid_pattern, false)
                            AS has_candidate_group_id,
                        COALESCE((payload_json ->> 'bundle_id') ~* :uuid_pattern, false) AS has_bundle_id
                    FROM pending
                ),
                classified AS (
                    SELECT
                        event_id,
                        payload_json,
                        (
                            has_judge_run_id
                            AND has_judge_output_id
                            AND has_candidate_group_id
                            AND has_bundle_id
                        ) AS payload_valid,
                        CASE
                            WHEN (
                                has_judge_run_id
                                AND has_judge_output_id
                                AND has_candidate_group_id
                                AND has_bundle_id
                            )
                            THEN CAST(payload_json ->> 'judge_output_id' AS uuid)
                            ELSE NULL::uuid
                        END AS judge_output_uuid
                    FROM payload_checked
                ),
                classified_with_analysis AS (
                    SELECT
                        classified.event_id,
                        classified.payload_valid,
                        EXISTS (
                            SELECT 1
                            FROM analyses
                            WHERE analyses.judge_output_id = classified.judge_output_uuid
                        ) AS already_analyzed
                    FROM classified
                )
                SELECT
                    (SELECT count(*) FROM pending) AS raw_pending_count,
                    count(*) FILTER (WHERE payload_valid AND already_analyzed)
                        AS stale_already_analyzed_count,
                    count(*) FILTER (WHERE payload_valid AND NOT already_analyzed)
                        AS eligible_pending_count,
                    count(*) FILTER (WHERE NOT payload_valid) AS malformed_pending_count
                FROM classified_with_analysis
                """
            ),
            {"event_type": EVENT_TYPE, "uuid_pattern": UUID_TEXT_PATTERN},
        )
        row = result.mappings().one()
        return PolicyApplyBacklogCounts(
            raw_pending=int(row["raw_pending_count"]),
            eligible_pending=int(row["eligible_pending_count"]),
            stale_already_analyzed=int(row["stale_already_analyzed_count"]),
            malformed_pending=int(row["malformed_pending_count"]),
        )

    async def fetch_oldest_eligible_pending_policy_apply_event(self) -> PolicyApplyEventRow | None:
        result = await self._session.execute(
            _sql(
                """
                WITH pending AS (
                    SELECT event_id, event_type, aggregate_type, aggregate_id, payload_json, status, created_at
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND status = 'pending'::outbox_status_enum
                ),
                payload_checked AS (
                    SELECT
                        event_id,
                        event_type,
                        aggregate_type,
                        aggregate_id,
                        payload_json,
                        status,
                        created_at,
                        COALESCE((payload_json ->> 'judge_run_id') ~* :uuid_pattern, false) AS has_judge_run_id,
                        COALESCE((payload_json ->> 'judge_output_id') ~* :uuid_pattern, false) AS has_judge_output_id,
                        COALESCE((payload_json ->> 'candidate_group_id') ~* :uuid_pattern, false)
                            AS has_candidate_group_id,
                        COALESCE((payload_json ->> 'bundle_id') ~* :uuid_pattern, false) AS has_bundle_id
                    FROM pending
                ),
                classified AS (
                    SELECT
                        event_id,
                        event_type,
                        aggregate_type,
                        aggregate_id,
                        payload_json,
                        status,
                        created_at,
                        (
                            has_judge_run_id
                            AND has_judge_output_id
                            AND has_candidate_group_id
                            AND has_bundle_id
                        ) AS payload_valid,
                        CASE
                            WHEN (
                                has_judge_run_id
                                AND has_judge_output_id
                                AND has_candidate_group_id
                                AND has_bundle_id
                            )
                            THEN CAST(payload_json ->> 'judge_output_id' AS uuid)
                            ELSE NULL::uuid
                        END AS judge_output_uuid
                    FROM payload_checked
                )
                SELECT event_id, event_type, aggregate_type, aggregate_id, payload_json, status, created_at
                FROM classified
                WHERE payload_valid
                  AND NOT EXISTS (
                      SELECT 1
                      FROM analyses
                      WHERE analyses.judge_output_id = classified.judge_output_uuid
                  )
                ORDER BY created_at ASC, event_id ASC
                LIMIT 1
                """
            ),
            {"event_type": EVENT_TYPE, "uuid_pattern": UUID_TEXT_PATTERN},
        )
        row = result.mappings().first()
        if row is None:
            return None
        payload = _json_loads(row["payload_json"]) or {}
        return PolicyApplyEventRow(
            event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            aggregate_type=str(row["aggregate_type"]),
            aggregate_id=UUID(str(row["aggregate_id"])),
            payload_json=payload if isinstance(payload, Mapping) else {},
            status=str(row["status"]),
            created_at=row["created_at"],
        )

    async def load_notification_plan_created_event_id(self, intent: NotificationPlanIntent) -> UUID | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id
                FROM event_outbox
                WHERE event_type = 'notification.plan.created.v1'
                  AND aggregate_type = 'analysis'
                  AND aggregate_id = CAST(:analysis_id AS uuid)
                  AND payload_json ->> 'notification_plan_id' = :notification_plan_id
                ORDER BY created_at DESC, event_id DESC
                LIMIT 1
                """
            ),
            {
                "analysis_id": str(intent.analysis_id),
                "notification_plan_id": str(intent.notification_plan_id),
            },
        )
        value = result.scalar_one_or_none()
        return UUID(str(value)) if value is not None else None


class RecordingPolicyEngineRepository:
    def __init__(self, delegate: PolicyEngineRepositoryProtocol) -> None:
        self._delegate = delegate
        self.analysis_created = False
        self.state_transition_inserted = False
        self.notification_plan_created_event_emitted = False
        self.analysis_draft: AnalysisDraft | None = None
        self.notification_intent: NotificationPlanIntent | None = None

    def transaction(self):
        return self._delegate.transaction()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID):
        return await self._delegate.load_job_by_trigger_event_id(trigger_event_id)

    async def load_candidate_context(self, candidate_group_id: UUID):
        return await self._delegate.load_candidate_context(candidate_group_id)

    async def load_judge_run(self, judge_run_id: UUID):
        return await self._delegate.load_judge_run(judge_run_id)

    async def load_judge_output(self, judge_output_id: UUID):
        return await self._delegate.load_judge_output(judge_output_id)

    async def load_bundle_context(self, bundle_id: UUID):
        return await self._delegate.load_bundle_context(bundle_id)

    async def load_existing_analysis(self, **kwargs):
        return await self._delegate.load_existing_analysis(**kwargs)

    async def insert_analysis(self, draft: AnalysisDraft) -> UUID:
        self.analysis_created = True
        self.analysis_draft = draft
        return await self._delegate.insert_analysis(draft)

    async def insert_state_transition(self, **kwargs) -> None:
        self.state_transition_inserted = True
        await self._delegate.insert_state_transition(**kwargs)

    async def insert_notification_plan_created_outbox(self, intent: NotificationPlanIntent) -> None:
        self.notification_plan_created_event_emitted = True
        self.notification_intent = intent
        await self._delegate.insert_notification_plan_created_outbox(intent)


class PolicyEngineTriggerInvoker:
    def __init__(
        self,
        *,
        service: PolicyEngineService,
        recorder: RecordingPolicyEngineRepository,
        notification_event_id_loader: Callable[[NotificationPlanIntent], Awaitable[UUID | None]] | None = None,
    ) -> None:
        self._service = service
        self._recorder = recorder
        self._notification_event_id_loader = notification_event_id_loader

    async def __call__(self, trigger_event_id: UUID) -> PolicyInvocationSummary:
        await self._service.handle_trigger_event(trigger_event_id)
        notification_event_id = None
        if self._recorder.notification_intent is not None and self._notification_event_id_loader is not None:
            notification_event_id = await self._notification_event_id_loader(self._recorder.notification_intent)
        return PolicyInvocationSummary(
            processed_event_count=1,
            analysis_created=self._recorder.analysis_created,
            state_transition_inserted=self._recorder.state_transition_inserted,
            notification_plan_created_event_emitted=self._recorder.notification_plan_created_event_emitted,
            notification_plan_created_event_id=notification_event_id,
            delivery_decision=(
                self._recorder.analysis_draft.delivery_decision if self._recorder.analysis_draft is not None else None
            ),
            verdict=self._recorder.analysis_draft.verdict if self._recorder.analysis_draft is not None else None,
        )


def load_bounded_policy_notification_intent_runtime_config(
    env: Mapping[str, str] | None = None,
) -> PolicyEngineConfig:
    if env is None:
        try:
            return PolicyEngineConfig.from_env()
        except PolicyEngineConfigurationError as exc:
            raise BoundedPolicyNotificationIntentError("runtime_config_error") from exc

    def _read(name: str, default: str = "") -> str:
        return str(env.get(name, default) or "").strip()

    try:
        cfg = PolicyEngineConfig(
            app_env=_read("APP_ENV", "dev").lower(),
            database_url=_read("DATABASE_URL"),
            redis_url=_read("REDIS_URL"),
            queue_name=_read("POLICY_ENGINE_QUEUE_NAME", "q.analysis.policy"),
            consumer_group=_read("POLICY_ENGINE_CONSUMER_GROUP", "policy-engine"),
            consumer_name=_read("POLICY_ENGINE_CONSUMER_NAME", "policy-engine-1"),
            batch_size=int(_read("POLICY_ENGINE_BATCH_SIZE", "20")),
            block_ms=int(_read("POLICY_ENGINE_BLOCK_MS", "5000")),
            policy_version=_read("VERDICT_POLICY_VERSION", "verdict_policy_v1"),
            delivery_policy_version=_read("DELIVERY_POLICY_VERSION", "delivery_policy_v1"),
            operator_chat_id=int(_read("TELEGRAM_OPERATOR_CHAT_ID", "0")),
            enable_later_delivery=_bool_env(_read("ENABLE_LATER_DELIVERY", "true")),
            enable_silent_later=_bool_env(_read("ENABLE_SILENT_LATER", "true")),
            enable_notification_send=_bool_env(_read("ENABLE_NOTIFICATION_SEND", "true")),
            render_profile_high=_read("NOTIFY_RENDER_PROFILE_HIGH", "telegram_single_alert_high_v1"),
            render_profile_normal=_read("NOTIFY_RENDER_PROFILE_NORMAL", "telegram_single_alert_normal_v1"),
            log_level=_read("LOG_LEVEL", "INFO").upper(),
        )
        cfg.validate()
        return cfg
    except (PolicyEngineConfigurationError, ValueError) as exc:
        raise BoundedPolicyNotificationIntentError("runtime_config_error") from exc


async def build_default_bounded_policy_notification_intent_runtime(
    runtime_config: PolicyEngineConfig,
    state: BoundedPolicyNotificationIntentState,
    logger: logging.Logger,
) -> BoundedPolicyNotificationIntentRuntimeHandle:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session_context = session_factory.begin()
    session = await session_context.__aenter__()
    state.database_session_opened = True

    bounded_repository = SqlAlchemyBoundedPolicyNotificationIntentRepository(session)
    policy_repository = PolicyEngineRepository(session)
    recording_repository = RecordingPolicyEngineRepository(policy_repository)
    service = PolicyEngineService(
        runtime_config,
        repository=recording_repository,
        verdict_policy=VerdictPolicy(),
        delivery_policy=DeliveryPolicy(
            enable_later_delivery=runtime_config.enable_later_delivery,
            enable_silent_later=runtime_config.enable_silent_later,
        ),
        notification_intent_builder=NotificationIntentBuilder(config=runtime_config),
        logger=logger,
    )
    policy_invoker = PolicyEngineTriggerInvoker(
        service=service,
        recorder=recording_repository,
        notification_event_id_loader=bounded_repository.load_notification_plan_created_event_id,
    )

    async def close(commit: bool) -> None:
        if not commit:
            await session.rollback()
        await session_context.__aexit__(None, None, None)
        await engine.dispose()

    return BoundedPolicyNotificationIntentRuntimeHandle(
        repository=bounded_repository,
        policy_invoker=policy_invoker,
        close=close,
    )


async def run_bounded_policy_notification_intent(
    config: BoundedPolicyNotificationIntentConfig,
    *,
    runtime_config_loader: Callable[[], PolicyEngineConfig] = load_bounded_policy_notification_intent_runtime_config,
    runtime_builder: BoundedPolicyNotificationIntentRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedPolicyNotificationIntentResult:
    state = BoundedPolicyNotificationIntentState()
    if not config.operator_approved:
        return _result("blocked", "operator_approval_missing", config=config, state=state)
    if not config.allow_database_read:
        return _result("blocked", "database_read_not_allowed", config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
    except BoundedPolicyNotificationIntentError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception:
        return _result("blocked", "runtime_config_error", config=config, state=state)

    runtime_handle: BoundedPolicyNotificationIntentRuntimeHandle | None = None
    commit_runtime = False
    try:
        runtime_handle = await (runtime_builder or build_default_bounded_policy_notification_intent_runtime)(
            runtime_config,
            state,
            effective_logger,
        )
        repository = runtime_handle.repository
        state.database_read_attempted = True
        policy_apply_counts = await repository.load_pending_policy_apply_event_counts()
        if policy_apply_counts.malformed_pending:
            return _result(
                "blocked",
                "malformed_policy_apply_payload",
                config=config,
                state=state,
                policy_apply_counts=policy_apply_counts,
            )
        expected_eligible_count = config.resolved_expected_eligible_pending_count
        if policy_apply_counts.eligible_pending != expected_eligible_count:
            return _result(
                "blocked",
                "eligible_pending_count_mismatch",
                config=config,
                state=state,
                policy_apply_counts=policy_apply_counts,
            )
        if policy_apply_counts.eligible_pending != 1:
            return _result(
                "blocked",
                "eligible_pending_count_not_one",
                config=config,
                state=state,
                policy_apply_counts=policy_apply_counts,
            )

        row = await repository.fetch_oldest_eligible_pending_policy_apply_event()
        if row is None:
            return _result(
                "blocked",
                "eligible_policy_apply_missing",
                config=config,
                state=state,
                policy_apply_counts=policy_apply_counts,
            )
        payload_flags = _payload_presence_flags(row.payload_json)
        if not all(payload_flags.values()):
            return _result(
                "blocked",
                "malformed_event_payload",
                config=config,
                state=state,
                policy_apply_counts=policy_apply_counts,
                selected_event=row,
                payload_flags=payload_flags,
            )
        if not config.allow_policy_write:
            return _result(
                "blocked",
                "policy_write_not_allowed",
                config=config,
                state=state,
                policy_apply_counts=policy_apply_counts,
                selected_event=row,
                payload_flags=payload_flags,
            )

        try:
            state.policy_invocation_attempted = True
            invocation = await runtime_handle.policy_invoker(row.event_id)
        except Exception:
            return _result(
                "failed",
                "policy_invocation_failed",
                config=config,
                state=state,
                policy_apply_counts=policy_apply_counts,
                selected_event=row,
                payload_flags=payload_flags,
            )

        state.database_write_attempted = invocation.database_write_attempted
        state.event_outbox_emit_attempted = invocation.event_outbox_emit_attempted
        commit_runtime = True
        return _result(
            "pass",
            None,
            config=config,
            state=state,
            policy_apply_counts=policy_apply_counts,
            selected_event=row,
            payload_flags=payload_flags,
            invocation=invocation,
        )
    except Exception:
        return _result("failed", "bounded_policy_notification_intent_failed", config=config, state=state)
    finally:
        if runtime_handle is not None:
            try:
                await runtime_handle.close(commit_runtime)
            except Exception:
                pass


def run_bounded_policy_notification_intent_sync(
    config: BoundedPolicyNotificationIntentConfig,
    *,
    runtime_config_loader: Callable[[], PolicyEngineConfig] = load_bounded_policy_notification_intent_runtime_config,
    runtime_builder: BoundedPolicyNotificationIntentRuntimeBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedPolicyNotificationIntentResult:
    return asyncio.run(
        run_bounded_policy_notification_intent(
            config,
            runtime_config_loader=runtime_config_loader,
            runtime_builder=runtime_builder,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        error_code,
        config=BoundedPolicyNotificationIntentConfig(),
        state=BoundedPolicyNotificationIntentState(),
    ).to_sanitized_dict()


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedPolicyNotificationIntentConfig,
    state: BoundedPolicyNotificationIntentState,
    policy_apply_counts: PolicyApplyBacklogCounts | None = None,
    pending_count_observed: int | None = None,
    selected_event: PolicyApplyEventRow | None = None,
    payload_flags: Mapping[str, bool] | None = None,
    invocation: PolicyInvocationSummary | None = None,
) -> BoundedPolicyNotificationIntentResult:
    flags = dict(payload_flags or {})
    selected = _selected_summary(selected_event)
    notification_event_id = invocation.notification_plan_created_event_id if invocation is not None else None
    recommended_next_action = None
    if invocation is not None and invocation.delivery_decision == "suppress":
        recommended_next_action = "no_notification_intent_emitted_by_policy"
    raw_pending_count = policy_apply_counts.raw_pending if policy_apply_counts is not None else pending_count_observed
    return BoundedPolicyNotificationIntentResult(
        status=status,
        ok=status == "pass" and error_code is None,
        error_code=error_code,
        operator_approved=config.operator_approved,
        database_read_allowed=config.allow_database_read,
        policy_write_allowed=config.allow_policy_write,
        pending_policy_apply_count_observed=raw_pending_count,
        raw_pending_policy_apply_count_observed=raw_pending_count,
        eligible_pending_policy_apply_count_observed=(
            policy_apply_counts.eligible_pending if policy_apply_counts is not None else None
        ),
        stale_already_analyzed_policy_apply_count_observed=(
            policy_apply_counts.stale_already_analyzed if policy_apply_counts is not None else None
        ),
        malformed_pending_policy_apply_count_observed=(
            policy_apply_counts.malformed_pending if policy_apply_counts is not None else None
        ),
        selected_event_present=selected["present"],
        selected_event_status=selected["status"],
        selected_event_id_suffix=selected["event_id_suffix"],
        selected_aggregate_type=selected["aggregate_type"],
        selected_aggregate_id_suffix=selected["aggregate_id_suffix"],
        payload_has_judge_run_id=flags.get("judge_run_id", False),
        payload_has_judge_output_id=flags.get("judge_output_id", False),
        payload_has_candidate_group_id=flags.get("candidate_group_id", False),
        payload_has_bundle_id=flags.get("bundle_id", False),
        processed_event_count=invocation.processed_event_count if invocation is not None else 0,
        analysis_created=invocation.analysis_created if invocation is not None else False,
        state_transition_inserted=invocation.state_transition_inserted if invocation is not None else False,
        notification_plan_created_event_emitted=(
            invocation.notification_plan_created_event_emitted if invocation is not None else False
        ),
        notification_plan_created_event_id_suffix=_id_suffix(notification_event_id)
        if notification_event_id is not None
        else None,
        delivery_decision=invocation.delivery_decision if invocation is not None else None,
        verdict=invocation.verdict if invocation is not None else None,
        recommended_next_action=recommended_next_action,
        state=state,
    )


def _payload_presence_flags(payload: Mapping[str, Any]) -> dict[str, bool]:
    return {field_name: _payload_uuid_present(payload.get(field_name)) for field_name in REQUIRED_PAYLOAD_FIELDS}


def _payload_uuid_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    try:
        UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return False
    return True


def _selected_summary(row: PolicyApplyEventRow | None) -> dict[str, Any]:
    if row is None:
        return {
            "present": False,
            "status": None,
            "event_id_suffix": None,
            "aggregate_type": None,
            "aggregate_id_suffix": None,
        }
    return {
        "present": True,
        "status": row.status,
        "event_id_suffix": _id_suffix(row.event_id),
        "aggregate_type": row.aggregate_type,
        "aggregate_id_suffix": _id_suffix(row.aggregate_id),
    }


def _id_suffix(value: UUID | str) -> str:
    return str(value)[-8:]


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (JSONDecodeError, TypeError):
            return None
    return value


def _bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)


__all__ = [
    "BoundedPolicyInvoker",
    "BoundedPolicyNotificationIntentConfig",
    "BoundedPolicyNotificationIntentError",
    "BoundedPolicyNotificationIntentRepository",
    "BoundedPolicyNotificationIntentResult",
    "BoundedPolicyNotificationIntentRuntimeBuilder",
    "BoundedPolicyNotificationIntentRuntimeHandle",
    "BoundedPolicyNotificationIntentState",
    "EVENT_TYPE",
    "MODE",
    "PolicyApplyBacklogCounts",
    "PolicyApplyEventRow",
    "PolicyEngineTriggerInvoker",
    "PolicyInvocationSummary",
    "REQUIRED_PAYLOAD_FIELDS",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "SqlAlchemyBoundedPolicyNotificationIntentRepository",
    "argument_error_report",
    "build_default_bounded_policy_notification_intent_runtime",
    "load_bounded_policy_notification_intent_runtime_config",
    "render_sanitized_json",
    "run_bounded_policy_notification_intent",
    "run_bounded_policy_notification_intent_sync",
]
