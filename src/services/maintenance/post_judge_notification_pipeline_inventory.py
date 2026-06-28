from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, AsyncIterator, Protocol
from uuid import UUID

import sqlalchemy as sa

from ..analysis_validator.config import AnalysisValidatorConfig
from ..analysis_validator.repositories import AnalysisValidatorRepository
from ..analysis_validator.service import AnalysisValidatorService
from ..notifier_telegram.config import NotifierTelegramConfig
from ..notifier_telegram.repositories import NotifierTelegramRepository
from ..notifier_telegram.service import NotifierTelegramService
from ..notifier_telegram.transport import TelegramTransportTerminalError
from ..policy_engine.config import PolicyEngineConfig
from ..policy_engine.repositories import PolicyEngineRepository
from ..policy_engine.service import PolicyEngineService


SCHEMA_VERSION = "post_judge_notification_pipeline_inventory_report_v1"
READY_EVENT_TYPE = "judge.output.ready.v1"
POLICY_EVENT_TYPE = "analysis.policy.apply.v1"
NOTIFICATION_EVENT_TYPE = "notification.plan.created.v1"
DELIVERY_RESULT_EVENT_TYPE = "notification.delivery.result.v1"
JUDGE_OUTPUT_SELECTION_CONFIRM_TOKEN = "latest-eligible-judge-output-ready"
POLICY_APPLY_SELECTION_CONFIRM_TOKEN = "latest-eligible-policy-apply"
NOTIFICATION_INTENT_SELECTION_CONFIRM_TOKEN = "latest-send-worthy-notification-intent"
VALIDATOR_CONFIRM_TOKEN = "exact-validator-apply"
POLICY_CONFIRM_TOKEN = "exact-policy-apply"
NOTIFIER_CONFIRM_TOKEN = "send-worthy-notification-intent-to-send-disabled-proof"
MACRO_CONFIRM_TOKEN = "macro-send-worthy-to-send-disabled-proof"
PLACEHOLDER_REDIS_URL = "redis_locator_not_attempted"
VALID_MODES = {"plan", "execute-validator", "execute-policy", "execute-notifier", "execute-macro"}
STRICT_UUID_TEXT_SQL_RE = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
SIGNED_BIGINT_TEXT_SQL_RE = r"^-?(0|[1-9][0-9]{0,18})$"

RUNTIME_VALUE_KEYS = {
    "APP_ENV",
    "DATABASE_URL",
    "ANALYSIS_VALIDATOR_QUEUE_NAME",
    "ANALYSIS_VALIDATOR_CONSUMER_GROUP",
    "ANALYSIS_VALIDATOR_CONSUMER_NAME",
    "ANALYSIS_VALIDATOR_BATCH_SIZE",
    "ANALYSIS_VALIDATOR_BLOCK_MS",
    "ANALYSIS_VALIDATOR_MAX_HEADLINE_CHARS",
    "ANALYSIS_VALIDATOR_MAX_SUMMARY_CHARS",
    "ANALYSIS_VALIDATOR_MAX_TEXT_ITEMS",
    "POLICY_ENGINE_QUEUE_NAME",
    "POLICY_ENGINE_CONSUMER_GROUP",
    "POLICY_ENGINE_CONSUMER_NAME",
    "POLICY_ENGINE_BATCH_SIZE",
    "POLICY_ENGINE_BLOCK_MS",
    "VERDICT_POLICY_VERSION",
    "DELIVERY_POLICY_VERSION",
    "TELEGRAM_OPERATOR_CHAT_ID",
    "ENABLE_LATER_DELIVERY",
    "ENABLE_SILENT_LATER",
    "ENABLE_NOTIFICATION_SEND",
    "ENABLE_DIGEST_RUNTIME",
    "NOTIFY_RENDER_PROFILE_HIGH",
    "NOTIFY_RENDER_PROFILE_NORMAL",
    "NOTIFIER_TELEGRAM_QUEUE_NAME",
    "NOTIFIER_TELEGRAM_CONSUMER_GROUP",
    "NOTIFIER_TELEGRAM_CONSUMER_NAME",
    "NOTIFIER_TELEGRAM_BATCH_SIZE",
    "NOTIFIER_TELEGRAM_BLOCK_MS",
    "NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS",
    "NOTIFIER_TELEGRAM_EDIT_WINDOW_MINUTES",
    "TELEGRAM_API_BASE_URL",
    "NOTIFIER_TELEGRAM_REQUEST_TIMEOUT_SEC",
    "LOG_LEVEL",
}
RUNTIME_FILE_KEYS = {"DATABASE_URL_FILE"}
RUNTIME_ENV_KEYS = RUNTIME_VALUE_KEYS | RUNTIME_FILE_KEYS
EXPECTED_FINGERPRINT_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
SAFE_REASON_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,96}$")
VALIDATOR_CONTROL_FLOW_REASON_CODES = ("model_refusal", "analysis_failed_truncation")
VALIDATOR_CONTROL_FLOW_REASON_SQL_LIST = ", ".join(
    f"'{reason_code}'" for reason_code in VALIDATOR_CONTROL_FLOW_REASON_CODES
)


def _validator_control_flow_transition_predicate(alias: str) -> str:
    return (
        f"({alias}.reason_code LIKE 'validator_%' "
        f"OR {alias}.reason_code IN ({VALIDATOR_CONTROL_FLOW_REASON_SQL_LIST}))"
    )


class PostJudgeNotificationPipelineInventoryConfigError(ValueError):
    pass


class SilentArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # pragma: no cover - argparse calls this
        del message
        raise PostJudgeNotificationPipelineInventoryConfigError("invalid_cli_arguments")


@dataclass(slots=True, frozen=True)
class InventoryCounts:
    judge_call_requested_v1_count: int = 0
    judge_run_status_counts: dict[str, int] = field(default_factory=dict)
    retryable_finish_reason_counts: list[dict[str, int | str]] = field(default_factory=list)
    judge_outputs_count: int = 0
    judge_output_ready_v1_count: int = 0
    analysis_policy_apply_v1_count: int = 0
    analyses_count: int = 0
    analyses_by_verdict_count: dict[str, int] = field(default_factory=dict)
    analyses_by_delivery_decision_count: dict[str, int] = field(default_factory=dict)
    notification_plan_created_v1_count: int = 0
    notification_plans_by_status_count: dict[str, int] = field(default_factory=dict)
    notification_renders_count: int = 0
    notification_delivery_records_by_delivery_status_count: dict[str, int] = field(default_factory=dict)
    eligible_judge_output_ready_count: int = 0
    validator_processed_judge_output_ready_count: int = 0
    eligible_policy_apply_count: int = 0
    policy_apply_already_materialized_count: int = 0
    send_worthy_notification_intent_count: int = 0
    send_worthy_notification_intent_already_materialized_count: int = 0
    live_delivered_notification_proof_count: int = 0


@dataclass(slots=True, frozen=True)
class SelectedTarget:
    event_id: UUID
    judge_run_id: UUID
    judge_output_id: UUID
    candidate_group_id: UUID
    bundle_id: UUID

    @property
    def event_fingerprint(self) -> str:
        return _fingerprint(self.event_id)


@dataclass(slots=True, frozen=True)
class SelectedNotificationIntentTarget:
    event_id: UUID
    analysis_id: UUID
    notification_plan_id: UUID
    judge_output_id: UUID
    candidate_group_id: UUID
    verdict: str
    delivery_decision: str

    @property
    def event_fingerprint(self) -> str:
        return _fingerprint(self.event_id)


@dataclass(slots=True, frozen=True)
class LiveDeliveredNotificationProofTarget:
    event_id: UUID
    analysis_id: UUID
    notification_plan_id: UUID
    judge_output_id: UUID
    candidate_group_id: UUID
    verdict: str
    delivery_decision: str
    render_count: int
    sent_or_edited_delivery_count: int
    delivery_result_event_count: int

    @property
    def event_fingerprint(self) -> str:
        return _fingerprint(self.event_id)


@dataclass(slots=True, frozen=True)
class ValidatorReadback:
    policy_event_count: int = 0
    policy_event_id: UUID | None = None
    validator_passed_transition_count: int = 0
    validator_terminal_or_retryable_count: int = 0
    validator_terminal_or_retryable_reason_code: str | None = None
    active_analysis_count: int = 0


@dataclass(slots=True, frozen=True)
class PolicyReadback:
    analysis_count: int = 0
    analysis_id: UUID | None = None
    verdict: str | None = None
    delivery_decision: str | None = None
    notification_intent_event_count: int = 0
    notification_plan_count: int = 0


@dataclass(slots=True, frozen=True)
class NotificationProofReadback:
    notification_intent_event_count: int = 0
    notification_plan_count: int = 0
    notification_render_count: int = 0
    send_disabled_delivery_record_count: int = 0
    notification_delivery_result_event_count: int = 0


@dataclass(slots=True, frozen=True)
class RuntimeConfigBundle:
    database_url: str
    values: Mapping[str, str]
    validator_config: AnalysisValidatorConfig
    policy_config: PolicyEngineConfig
    notifier_config: NotifierTelegramConfig


@dataclass(slots=True, frozen=True)
class PostJudgeNotificationPipelineInventoryRequest:
    mode: str
    lookback_hours: int
    sample_limit: int
    select_latest_eligible_judge_output_ready: bool = False
    expected_judge_output_ready_fingerprint: str | None = None
    select_latest_eligible_policy_apply: bool = False
    expected_policy_apply_fingerprint: str | None = None
    select_latest_send_worthy_notification_intent: bool = False
    expected_notification_intent_fingerprint: str | None = None


@dataclass(slots=True, frozen=True)
class PostJudgeNotificationPipelineInventoryReport:
    schema_version: str
    mode: str
    status: str
    reason_code: str
    lookback_hours: int
    sample_limit: int
    counts: dict[str, Any]
    selected_judge_output_ready_fingerprint: str | None
    selected_policy_apply_fingerprint: str | None
    selected_judge_run_fingerprint: str | None
    selected_judge_output_fingerprint: str | None
    selected_bundle_fingerprint: str | None
    selected_candidate_group_fingerprint: str | None
    selected_analysis_fingerprint: str | None
    selected_notification_intent_fingerprint: str | None
    selected_notification_plan_fingerprint: str | None
    selected_live_delivered_notification_intent_fingerprint: str | None
    selected_live_delivered_analysis_fingerprint: str | None
    selected_live_delivered_notification_plan_fingerprint: str | None
    selected_live_delivered_candidate_group_fingerprint: str | None
    selected_live_delivered_judge_output_fingerprint: str | None
    selected_live_delivered_render_count: int
    selected_live_delivered_sent_or_edited_delivery_count: int
    selected_live_delivered_delivery_result_event_count: int
    nearest_send_worthy_missing_stage: str | None
    final_verdict: str | None
    delivery_decision: str | None
    runtime_rollout_readiness: str | None
    validator_attempted: bool
    policy_attempted: bool
    notifier_attempted: bool
    policy_event_created_or_present: bool
    analysis_created_or_present: bool
    notification_intent_created_or_present: bool
    notification_plan_created_or_present: bool
    notification_render_created_or_present: bool
    send_disabled_delivery_record_created_or_present: bool
    notification_delivery_result_event_created_or_present: bool
    redis_attempted: bool
    telegram_attempted: bool
    telegram_transport_attempted: bool
    openai_attempted: bool
    external_network_attempted: bool
    db_write_attempted: bool
    redactions_applied: bool
    cleanup_completed: bool


class InventoryRepositoryProtocol(Protocol):
    async def load_inventory_counts(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> InventoryCounts: ...

    async def select_latest_eligible_judge_output_ready(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> SelectedTarget | None: ...

    async def select_latest_eligible_policy_apply(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> SelectedTarget | None: ...

    async def select_latest_send_worthy_notification_intent(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> SelectedNotificationIntentTarget | None: ...

    async def select_latest_live_delivered_notification_proof(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> LiveDeliveredNotificationProofTarget | None: ...

    async def load_validator_readback(
        self,
        *,
        judge_run_id: UUID,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> ValidatorReadback: ...

    async def load_policy_readback(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> PolicyReadback: ...

    async def load_notification_proof_readback(
        self,
        *,
        notification_intent_event_id: UUID,
        analysis_id: UUID,
        notification_plan_id: UUID,
    ) -> NotificationProofReadback: ...


class TriggerServiceProtocol(Protocol):
    async def handle_trigger_event(self, trigger_event_id: str | UUID) -> None: ...


class FailClosedTelegramTransport:
    def __init__(self) -> None:
        self.attempted = False

    async def send_message(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        self.attempted = True
        raise TelegramTransportTerminalError(
            "telegram_transport_forbidden",
            error_code="telegram_transport_forbidden",
        )

    async def edit_message_text(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        self.attempted = True
        raise TelegramTransportTerminalError(
            "telegram_transport_forbidden",
            error_code="telegram_transport_forbidden",
        )


@dataclass(slots=True, frozen=True)
class PostJudgeNotificationPipelineInventoryComponents:
    inventory_repository: InventoryRepositoryProtocol
    validator_service: TriggerServiceProtocol
    policy_service: TriggerServiceProtocol
    commit_active_transaction: Callable[[], Awaitable[None]]
    notifier_service: TriggerServiceProtocol | None = None
    telegram_transport_attempted: Callable[[], bool] | None = None


class SqlPostJudgeNotificationPipelineInventoryRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def load_inventory_counts(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> InventoryCounts:
        del sample_limit
        row = await self._one(
            """
            WITH judge_runs_window AS (
                SELECT jr.judge_run_id, jr.status, jr.finish_reason
                FROM judge_runs jr
                WHERE COALESCE(jr.finished_at, jr.started_at) >= now() - make_interval(hours => :lookback_hours)
                   OR EXISTS (
                        SELECT 1
                        FROM event_outbox eo
                        WHERE eo.event_type = 'judge.call.requested.v1'
                          AND eo.aggregate_type = 'judge_run'
                          AND eo.aggregate_id = jr.judge_run_id
                          AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
                   )
            )
            SELECT
                (
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = 'judge.call.requested.v1'
                      AND created_at >= now() - make_interval(hours => :lookback_hours)
                ) AS judge_call_requested_count,
                (SELECT count(*) FROM judge_runs_window WHERE status = 'pending') AS judge_pending_count,
                (SELECT count(*) FROM judge_runs_window WHERE status = 'running') AS judge_running_count,
                (SELECT count(*) FROM judge_runs_window WHERE status = 'succeeded') AS judge_succeeded_count,
                (SELECT count(*) FROM judge_runs_window WHERE status = 'failed_retryable') AS judge_failed_retryable_count,
                (SELECT count(*) FROM judge_runs_window WHERE status = 'failed_terminal') AS judge_failed_terminal_count,
                (
                    SELECT count(*)
                    FROM judge_runs_window
                    WHERE status NOT IN ('pending', 'running', 'succeeded', 'failed_retryable', 'failed_terminal')
                ) AS judge_other_count,
                (
                    SELECT count(*)
                    FROM judge_outputs
                    WHERE created_at >= now() - make_interval(hours => :lookback_hours)
                ) AS judge_outputs_count,
                (
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = 'judge.output.ready.v1'
                      AND created_at >= now() - make_interval(hours => :lookback_hours)
                ) AS judge_output_ready_count,
                (
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = 'analysis.policy.apply.v1'
                      AND created_at >= now() - make_interval(hours => :lookback_hours)
                ) AS policy_apply_count,
                (
                    SELECT count(*)
                    FROM analyses
                    WHERE created_at >= now() - make_interval(hours => :lookback_hours)
                ) AS analyses_count,
                (
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = 'notification.plan.created.v1'
                      AND created_at >= now() - make_interval(hours => :lookback_hours)
                ) AS notification_plan_created_count,
                (
                    SELECT count(*)
                    FROM notification_renders
                    WHERE created_at >= now() - make_interval(hours => :lookback_hours)
                ) AS notification_renders_count
            """,
            {"lookback_hours": lookback_hours},
        )
        retryable_reasons = await self._rows(
            """
            WITH judge_runs_window AS (
                SELECT jr.finish_reason
                FROM judge_runs jr
                WHERE jr.status = 'failed_retryable'
                  AND (
                        COALESCE(jr.finished_at, jr.started_at) >= now() - make_interval(hours => :lookback_hours)
                        OR EXISTS (
                            SELECT 1
                            FROM event_outbox eo
                            WHERE eo.event_type = 'judge.call.requested.v1'
                              AND eo.aggregate_type = 'judge_run'
                              AND eo.aggregate_id = jr.judge_run_id
                              AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
                        )
                  )
            )
            SELECT COALESCE(finish_reason, 'missing') AS reason_code, count(*) AS reason_count
            FROM judge_runs_window
            GROUP BY COALESCE(finish_reason, 'missing')
            ORDER BY reason_count DESC, reason_code ASC
            LIMIT 20
            """,
            {"lookback_hours": lookback_hours},
        )
        analyses_by_verdict = await self._count_by(
            """
            SELECT verdict::text AS bucket, count(*) AS bucket_count
            FROM analyses
            WHERE created_at >= now() - make_interval(hours => :lookback_hours)
            GROUP BY verdict::text
            """,
            {"lookback_hours": lookback_hours},
        )
        analyses_by_delivery = await self._count_by(
            """
            SELECT delivery_decision::text AS bucket, count(*) AS bucket_count
            FROM analyses
            WHERE created_at >= now() - make_interval(hours => :lookback_hours)
            GROUP BY delivery_decision::text
            """,
            {"lookback_hours": lookback_hours},
        )
        notification_plans_by_status = await self._count_by(
            """
            SELECT status::text AS bucket, count(*) AS bucket_count
            FROM notification_plans
            WHERE created_at >= now() - make_interval(hours => :lookback_hours)
            GROUP BY status::text
            """,
            {"lookback_hours": lookback_hours},
        )
        delivery_by_status = await self._count_by(
            """
            SELECT delivery_status::text AS bucket, count(*) AS bucket_count
            FROM notification_delivery_records
            WHERE created_at >= now() - make_interval(hours => :lookback_hours)
            GROUP BY delivery_status::text
            """,
            {"lookback_hours": lookback_hours},
        )
        eligible_ready_count = await self._eligible_judge_output_ready_count(
            lookback_hours=lookback_hours,
            policy_version=policy_version,
            delivery_policy_version=delivery_policy_version,
        )
        validator_processed_ready_count = await self._validator_processed_judge_output_ready_count(
            lookback_hours=lookback_hours,
            policy_version=policy_version,
            delivery_policy_version=delivery_policy_version,
        )
        eligible_policy_count = await self._eligible_policy_apply_count(
            lookback_hours=lookback_hours,
            policy_version=policy_version,
            delivery_policy_version=delivery_policy_version,
        )
        materialized_policy_count = await self._policy_apply_already_materialized_count(
            lookback_hours=lookback_hours,
            policy_version=policy_version,
            delivery_policy_version=delivery_policy_version,
        )
        send_worthy_intent_count = await self._send_worthy_notification_intent_count(
            lookback_hours=lookback_hours,
            policy_version=policy_version,
            delivery_policy_version=delivery_policy_version,
        )
        materialized_notification_count = await self._send_worthy_notification_intent_already_materialized_count(
            lookback_hours=lookback_hours,
            policy_version=policy_version,
            delivery_policy_version=delivery_policy_version,
        )
        live_delivered_count = await self._live_delivered_notification_proof_count(
            lookback_hours=lookback_hours,
            policy_version=policy_version,
            delivery_policy_version=delivery_policy_version,
        )
        return InventoryCounts(
            judge_call_requested_v1_count=_int(row["judge_call_requested_count"]),
            judge_run_status_counts={
                "pending": _int(row["judge_pending_count"]),
                "running": _int(row["judge_running_count"]),
                "succeeded": _int(row["judge_succeeded_count"]),
                "failed_retryable": _int(row["judge_failed_retryable_count"]),
                "failed_terminal": _int(row["judge_failed_terminal_count"]),
                "other": _int(row["judge_other_count"]),
            },
            retryable_finish_reason_counts=[
                {
                    "reason_code": _safe_reason_code_value(reason["reason_code"]),
                    "count": _int(reason["reason_count"]),
                }
                for reason in retryable_reasons
            ],
            judge_outputs_count=_int(row["judge_outputs_count"]),
            judge_output_ready_v1_count=_int(row["judge_output_ready_count"]),
            analysis_policy_apply_v1_count=_int(row["policy_apply_count"]),
            analyses_count=_int(row["analyses_count"]),
            analyses_by_verdict_count=analyses_by_verdict,
            analyses_by_delivery_decision_count=analyses_by_delivery,
            notification_plan_created_v1_count=_int(row["notification_plan_created_count"]),
            notification_plans_by_status_count=notification_plans_by_status,
            notification_renders_count=_int(row["notification_renders_count"]),
            notification_delivery_records_by_delivery_status_count=delivery_by_status,
            eligible_judge_output_ready_count=eligible_ready_count,
            validator_processed_judge_output_ready_count=validator_processed_ready_count,
            eligible_policy_apply_count=eligible_policy_count,
            policy_apply_already_materialized_count=materialized_policy_count,
            send_worthy_notification_intent_count=send_worthy_intent_count,
            send_worthy_notification_intent_already_materialized_count=materialized_notification_count,
            live_delivered_notification_proof_count=live_delivered_count,
        )

    async def select_latest_eligible_judge_output_ready(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> SelectedTarget | None:
        rows = await self._eligible_judge_output_ready_rows(
            lookback_hours=lookback_hours,
            sample_limit=sample_limit,
            policy_version=policy_version,
            delivery_policy_version=delivery_policy_version,
        )
        return _selected_from_row(rows[0]) if rows else None

    async def select_latest_eligible_policy_apply(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> SelectedTarget | None:
        rows = await self._eligible_policy_apply_rows(
            lookback_hours=lookback_hours,
            sample_limit=sample_limit,
            policy_version=policy_version,
            delivery_policy_version=delivery_policy_version,
        )
        return _selected_from_row(rows[0]) if rows else None

    async def select_latest_send_worthy_notification_intent(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> SelectedNotificationIntentTarget | None:
        rows = await self._send_worthy_notification_intent_rows(
            lookback_hours=lookback_hours,
            sample_limit=sample_limit,
            policy_version=policy_version,
            delivery_policy_version=delivery_policy_version,
        )
        return _selected_notification_intent_from_row(rows[0]) if rows else None

    async def select_latest_live_delivered_notification_proof(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> LiveDeliveredNotificationProofTarget | None:
        rows = await self._live_delivered_notification_proof_rows(
            lookback_hours=lookback_hours,
            sample_limit=sample_limit,
            policy_version=policy_version,
            delivery_policy_version=delivery_policy_version,
        )
        return _live_delivered_notification_proof_from_row(rows[0]) if rows else None

    async def load_validator_readback(
        self,
        *,
        judge_run_id: UUID,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> ValidatorReadback:
        policy_rows = await self._policy_event_rows(judge_run_id=judge_run_id, judge_output_id=judge_output_id)
        transition_rows = await self._rows(
            f"""
            SELECT st.reason_code, st.to_state, count(*) AS transition_count
            FROM state_transitions st
            WHERE st.object_type = 'judge_run'
              AND st.object_id = CAST(:judge_run_id AS uuid)
              AND {_validator_control_flow_transition_predicate("st")}
            GROUP BY st.reason_code, st.to_state
            ORDER BY max(st.created_at) DESC, st.reason_code ASC
            """,
            {"judge_run_id": str(judge_run_id)},
        )
        active_analysis_count = await self._count(
            """
            SELECT count(*)
            FROM analyses
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
              AND policy_version = :policy_version
              AND delivery_policy_version = :delivery_policy_version
            """,
            {
                "judge_output_id": str(judge_output_id),
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )
        passed_count = 0
        terminal_count = 0
        terminal_reason: str | None = None
        for row in transition_rows:
            reason = _safe_reason_code_value(row["reason_code"])
            count = _int(row["transition_count"])
            if reason == "validator_passed":
                passed_count += count
                continue
            terminal_count += count
            if terminal_reason is None:
                terminal_reason = reason
        return ValidatorReadback(
            policy_event_count=len(policy_rows),
            policy_event_id=UUID(str(policy_rows[0]["event_id"])) if len(policy_rows) == 1 else None,
            validator_passed_transition_count=passed_count,
            validator_terminal_or_retryable_count=terminal_count,
            validator_terminal_or_retryable_reason_code=terminal_reason,
            active_analysis_count=active_analysis_count,
        )

    async def load_policy_readback(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> PolicyReadback:
        analysis_rows = await self._rows(
            """
            SELECT analysis_id, verdict::text AS verdict, delivery_decision::text AS delivery_decision
            FROM analyses
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
              AND policy_version = :policy_version
              AND delivery_policy_version = :delivery_policy_version
            ORDER BY created_at ASC, analysis_id ASC
            """,
            {
                "judge_output_id": str(judge_output_id),
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )
        analysis_id = UUID(str(analysis_rows[0]["analysis_id"])) if len(analysis_rows) == 1 else None
        notification_intents = 0
        notification_plans = 0
        if analysis_id is not None:
            notification_intents = await self._count(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = 'notification.plan.created.v1'
                  AND aggregate_type = 'analysis'
                  AND aggregate_id = CAST(:analysis_id AS uuid)
                  AND payload_json->>'analysis_id' = :analysis_id_text
                """,
                {"analysis_id": str(analysis_id), "analysis_id_text": str(analysis_id)},
            )
            notification_plans = await self._count(
                """
                SELECT count(*)
                FROM notification_plans
                WHERE analysis_id = CAST(:analysis_id AS uuid)
                """,
                {"analysis_id": str(analysis_id)},
            )
        return PolicyReadback(
            analysis_count=len(analysis_rows),
            analysis_id=analysis_id,
            verdict=str(analysis_rows[0]["verdict"]) if len(analysis_rows) == 1 else None,
            delivery_decision=str(analysis_rows[0]["delivery_decision"]) if len(analysis_rows) == 1 else None,
            notification_intent_event_count=notification_intents,
            notification_plan_count=notification_plans,
        )

    async def load_notification_proof_readback(
        self,
        *,
        notification_intent_event_id: UUID,
        analysis_id: UUID,
        notification_plan_id: UUID,
    ) -> NotificationProofReadback:
        row = await self._one(
            """
            SELECT
                (
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_id = CAST(:notification_intent_event_id AS uuid)
                      AND event_type = 'notification.plan.created.v1'
                      AND aggregate_type = 'analysis'
                      AND aggregate_id = CAST(:analysis_id AS uuid)
                      AND payload_json->>'analysis_id' = :analysis_id_text
                      AND payload_json->>'notification_plan_id' = :notification_plan_id_text
                ) AS notification_intent_event_count,
                (
                    SELECT count(*)
                    FROM notification_plans
                    WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                      AND analysis_id = CAST(:analysis_id AS uuid)
                ) AS notification_plan_count,
                (
                    SELECT count(*)
                    FROM notification_renders
                    WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                ) AS notification_render_count,
                (
                    SELECT count(*)
                    FROM notification_delivery_records
                    WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                      AND delivery_status::text = 'suppressed'
                      AND telegram_response_json->>'send_disabled' = 'true'
                      AND telegram_response_json->>'dry_run' = 'true'
                ) AS send_disabled_delivery_record_count,
                (
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = 'notification.delivery.result.v1'
                      AND aggregate_type = 'notification_plan'
                      AND aggregate_id = CAST(:notification_plan_id AS uuid)
                ) AS notification_delivery_result_event_count
            """,
            {
                "notification_intent_event_id": str(notification_intent_event_id),
                "analysis_id": str(analysis_id),
                "analysis_id_text": str(analysis_id),
                "notification_plan_id": str(notification_plan_id),
                "notification_plan_id_text": str(notification_plan_id),
            },
        )
        return NotificationProofReadback(
            notification_intent_event_count=_int(row["notification_intent_event_count"]),
            notification_plan_count=_int(row["notification_plan_count"]),
            notification_render_count=_int(row["notification_render_count"]),
            send_disabled_delivery_record_count=_int(row["send_disabled_delivery_record_count"]),
            notification_delivery_result_event_count=_int(row["notification_delivery_result_event_count"]),
        )

    async def _eligible_judge_output_ready_count(
        self,
        *,
        lookback_hours: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> int:
        return _int(
            (
                await self._one(
                    f"SELECT count(*) AS target_count FROM ({_ELIGIBLE_READY_SQL}) ready_targets",
                    {
                        "lookback_hours": lookback_hours,
                        "sample_limit": 500,
                        "policy_version": policy_version,
                        "delivery_policy_version": delivery_policy_version,
                    },
                )
            )["target_count"]
        )

    async def _validator_processed_judge_output_ready_count(
        self,
        *,
        lookback_hours: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> int:
        row = await self._one(
            f"""
            WITH candidate_events AS (
                SELECT
                    eo.event_id,
                    eo.aggregate_id,
                    eo.created_at,
                    eo.payload_json->>'judge_run_id' AS judge_run_id_text,
                    eo.payload_json->>'judge_output_id' AS judge_output_id_text
                FROM event_outbox eo
                WHERE eo.event_type = 'judge.output.ready.v1'
                  AND eo.aggregate_type = 'judge_run'
                  AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
                  AND eo.payload_json->>'judge_run_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
                  AND eo.payload_json->>'judge_output_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
            )
            SELECT count(DISTINCT ce.event_id) AS processed_count
            FROM candidate_events ce
            JOIN judge_runs jr
              ON jr.judge_run_id = ce.aggregate_id
            JOIN judge_outputs jo
              ON jo.judge_output_id = CAST(ce.judge_output_id_text AS uuid)
             AND jo.judge_run_id = jr.judge_run_id
            JOIN candidate_evidence_bundles ceb
              ON ceb.bundle_id = jr.bundle_id
             AND ceb.candidate_group_id = jo.candidate_group_id
            JOIN candidate_group_proposals cgp
              ON cgp.candidate_group_id = jo.candidate_group_id
             AND cgp.current_bundle_id = jr.bundle_id
            WHERE ce.judge_run_id_text = jr.judge_run_id::text
              AND ce.judge_output_id_text = jo.judge_output_id::text
              AND EXISTS (
                    SELECT 1
                    FROM state_transitions validator_state
                    WHERE validator_state.object_type = 'judge_run'
                      AND validator_state.object_id = jr.judge_run_id
                      AND {_validator_control_flow_transition_predicate("validator_state")}
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM event_outbox policy
                    WHERE policy.event_type = 'analysis.policy.apply.v1'
                      AND policy.aggregate_type = 'judge_run'
                      AND policy.aggregate_id = jr.judge_run_id
                      AND policy.payload_json->>'judge_run_id' = jr.judge_run_id::text
                      AND policy.payload_json->>'judge_output_id' = jo.judge_output_id::text
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM analyses a
                    WHERE a.judge_output_id = jo.judge_output_id
                      AND a.policy_version = :policy_version
                      AND a.delivery_policy_version = :delivery_policy_version
              )
            """,
            {
                "lookback_hours": lookback_hours,
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )
        return _int(row["processed_count"])

    async def _eligible_policy_apply_count(
        self,
        *,
        lookback_hours: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> int:
        return _int(
            (
                await self._one(
                    f"SELECT count(*) AS target_count FROM ({_ELIGIBLE_POLICY_SQL}) policy_targets",
                    {
                        "lookback_hours": lookback_hours,
                        "sample_limit": 500,
                        "policy_version": policy_version,
                        "delivery_policy_version": delivery_policy_version,
                    },
                )
            )["target_count"]
        )

    async def _policy_apply_already_materialized_count(
        self,
        *,
        lookback_hours: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> int:
        row = await self._one(
            f"""
            WITH candidate_events AS (
                SELECT
                    eo.payload_json->>'judge_run_id' AS judge_run_id_text,
                    eo.payload_json->>'judge_output_id' AS judge_output_id_text
                FROM event_outbox eo
                WHERE eo.event_type = 'analysis.policy.apply.v1'
                  AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
                  AND eo.payload_json->>'judge_run_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
                  AND eo.payload_json->>'judge_output_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
            )
            SELECT count(*) AS materialized_count
            FROM candidate_events ce
            JOIN judge_runs jr
              ON jr.judge_run_id = CAST(ce.judge_run_id_text AS uuid)
            JOIN judge_outputs jo
              ON jo.judge_output_id = CAST(ce.judge_output_id_text AS uuid)
             AND jo.judge_run_id = jr.judge_run_id
            JOIN analyses a
              ON a.judge_output_id = jo.judge_output_id
             AND a.policy_version = :policy_version
             AND a.delivery_policy_version = :delivery_policy_version
            """,
            {
                "lookback_hours": lookback_hours,
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )
        return _int(row["materialized_count"])

    async def _send_worthy_notification_intent_count(
        self,
        *,
        lookback_hours: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> int:
        return _int(
            (
                await self._one(
                    f"SELECT count(*) AS target_count FROM ({_SEND_WORTHY_NOTIFICATION_INTENT_SQL}) notification_targets",
                    {
                        "lookback_hours": lookback_hours,
                        "sample_limit": 500,
                        "policy_version": policy_version,
                        "delivery_policy_version": delivery_policy_version,
                    },
                )
            )["target_count"]
        )

    async def _send_worthy_notification_intent_already_materialized_count(
        self,
        *,
        lookback_hours: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> int:
        row = await self._one(
            f"""
            WITH send_worthy AS (
                {_SEND_WORTHY_NOTIFICATION_INTENT_SQL}
            )
            SELECT count(DISTINCT sw.event_id) AS materialized_count
            FROM send_worthy sw
            JOIN notification_renders nr
              ON nr.notification_plan_id = sw.notification_plan_id
            JOIN notification_delivery_records ndr
              ON ndr.notification_plan_id = sw.notification_plan_id
             AND ndr.delivery_status::text = 'suppressed'
             AND ndr.telegram_response_json->>'send_disabled' = 'true'
             AND ndr.telegram_response_json->>'dry_run' = 'true'
            JOIN event_outbox delivery_result
              ON delivery_result.event_type = 'notification.delivery.result.v1'
             AND delivery_result.aggregate_type = 'notification_plan'
             AND delivery_result.aggregate_id = sw.notification_plan_id
            """,
            {
                "lookback_hours": lookback_hours,
                "sample_limit": 500,
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )
        return _int(row["materialized_count"])

    async def _live_delivered_notification_proof_count(
        self,
        *,
        lookback_hours: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> int:
        return _int(
            (
                await self._one(
                    f"SELECT count(*) AS proof_count FROM ({_LIVE_DELIVERED_NOTIFICATION_PROOF_SQL}) live_proofs",
                    {
                        "lookback_hours": lookback_hours,
                        "sample_limit": 500,
                        "policy_version": policy_version,
                        "delivery_policy_version": delivery_policy_version,
                    },
                )
            )["proof_count"]
        )

    async def _eligible_judge_output_ready_rows(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> list[Mapping[str, Any]]:
        return await self._rows(
            _ELIGIBLE_READY_SQL,
            {
                "lookback_hours": lookback_hours,
                "sample_limit": sample_limit,
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )

    async def _eligible_policy_apply_rows(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> list[Mapping[str, Any]]:
        return await self._rows(
            _ELIGIBLE_POLICY_SQL,
            {
                "lookback_hours": lookback_hours,
                "sample_limit": sample_limit,
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )

    async def _send_worthy_notification_intent_rows(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> list[Mapping[str, Any]]:
        return await self._rows(
            _SEND_WORTHY_NOTIFICATION_INTENT_SQL,
            {
                "lookback_hours": lookback_hours,
                "sample_limit": sample_limit,
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )

    async def _live_delivered_notification_proof_rows(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        policy_version: str,
        delivery_policy_version: str,
    ) -> list[Mapping[str, Any]]:
        return await self._rows(
            _LIVE_DELIVERED_NOTIFICATION_PROOF_SQL,
            {
                "lookback_hours": lookback_hours,
                "sample_limit": sample_limit,
                "policy_version": policy_version,
                "delivery_policy_version": delivery_policy_version,
            },
        )

    async def _policy_event_rows(self, *, judge_run_id: UUID, judge_output_id: UUID) -> list[Mapping[str, Any]]:
        return await self._rows(
            """
            SELECT event_id
            FROM event_outbox
            WHERE event_type = 'analysis.policy.apply.v1'
              AND aggregate_type = 'judge_run'
              AND aggregate_id = CAST(:judge_run_id AS uuid)
              AND payload_json->>'judge_run_id' = :judge_run_id_text
              AND payload_json->>'judge_output_id' = :judge_output_id_text
            ORDER BY created_at ASC, event_id ASC
            """,
            {
                "judge_run_id": str(judge_run_id),
                "judge_run_id_text": str(judge_run_id),
                "judge_output_id_text": str(judge_output_id),
            },
        )

    async def _count_by(self, query: str, params: Mapping[str, Any]) -> dict[str, int]:
        rows = await self._rows(query, params)
        return {
            _safe_bucket_value(row["bucket"]): _int(row["bucket_count"])
            for row in rows
        }

    async def _count(self, query: str, params: Mapping[str, Any]) -> int:
        result = await self._session.execute(sa.text(query), dict(params))
        return _int(result.scalar_one())

    async def _rows(self, query: str, params: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        result = await self._session.execute(sa.text(query), dict(params))
        return list(result.mappings().all())

    async def _one(self, query: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        result = await self._session.execute(sa.text(query), dict(params))
        return result.mappings().one()


_ELIGIBLE_READY_SQL = f"""
WITH candidate_events AS (
    SELECT
        eo.event_id,
        eo.aggregate_id,
        eo.created_at,
        eo.payload_json->>'judge_run_id' AS judge_run_id_text,
        eo.payload_json->>'judge_output_id' AS judge_output_id_text
    FROM event_outbox eo
    WHERE eo.event_type = 'judge.output.ready.v1'
      AND eo.aggregate_type = 'judge_run'
      AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
      AND eo.payload_json->>'judge_run_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
      AND eo.payload_json->>'judge_output_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
)
SELECT
    ce.event_id,
    jr.judge_run_id,
    jo.judge_output_id,
    jo.candidate_group_id,
    jr.bundle_id
FROM candidate_events ce
JOIN judge_runs jr
  ON jr.judge_run_id = ce.aggregate_id
JOIN judge_outputs jo
  ON jo.judge_output_id = CAST(ce.judge_output_id_text AS uuid)
 AND jo.judge_run_id = jr.judge_run_id
JOIN candidate_evidence_bundles ceb
  ON ceb.bundle_id = jr.bundle_id
 AND ceb.candidate_group_id = jo.candidate_group_id
JOIN candidate_group_proposals cgp
  ON cgp.candidate_group_id = jo.candidate_group_id
 AND cgp.current_bundle_id = jr.bundle_id
WHERE ce.judge_run_id_text = jr.judge_run_id::text
  AND ce.judge_output_id_text = jo.judge_output_id::text
  AND jr.status = 'succeeded'
  AND NOT EXISTS (
        SELECT 1
        FROM event_outbox policy
        WHERE policy.event_type = 'analysis.policy.apply.v1'
          AND policy.aggregate_type = 'judge_run'
          AND policy.aggregate_id = jr.judge_run_id
          AND policy.payload_json->>'judge_run_id' = jr.judge_run_id::text
          AND policy.payload_json->>'judge_output_id' = jo.judge_output_id::text
  )
  AND NOT EXISTS (
        SELECT 1
        FROM analyses a
        WHERE a.judge_output_id = jo.judge_output_id
          AND a.policy_version = :policy_version
          AND a.delivery_policy_version = :delivery_policy_version
  )
  AND NOT EXISTS (
        SELECT 1
        FROM state_transitions validator_state
        WHERE validator_state.object_type = 'judge_run'
          AND validator_state.object_id = jr.judge_run_id
          AND {_validator_control_flow_transition_predicate("validator_state")}
  )
ORDER BY ce.created_at DESC, ce.event_id DESC
LIMIT :sample_limit
"""


_ELIGIBLE_POLICY_SQL = f"""
WITH candidate_events AS (
    SELECT
        eo.event_id,
        eo.aggregate_id,
        eo.created_at,
        eo.payload_json->>'judge_run_id' AS judge_run_id_text,
        eo.payload_json->>'judge_output_id' AS judge_output_id_text,
        eo.payload_json->>'candidate_group_id' AS candidate_group_id_text,
        eo.payload_json->>'bundle_id' AS bundle_id_text
    FROM event_outbox eo
    WHERE eo.event_type = 'analysis.policy.apply.v1'
      AND eo.aggregate_type = 'judge_run'
      AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
      AND eo.payload_json->>'judge_run_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
      AND eo.payload_json->>'judge_output_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
      AND eo.payload_json->>'candidate_group_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
      AND eo.payload_json->>'bundle_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
)
SELECT
    ce.event_id,
    jr.judge_run_id,
    jo.judge_output_id,
    jo.candidate_group_id,
    ceb.bundle_id
FROM candidate_events ce
JOIN judge_runs jr
  ON jr.judge_run_id = CAST(ce.judge_run_id_text AS uuid)
JOIN judge_outputs jo
  ON jo.judge_output_id = CAST(ce.judge_output_id_text AS uuid)
 AND jo.judge_run_id = jr.judge_run_id
JOIN candidate_group_proposals cgp
  ON cgp.candidate_group_id = CAST(ce.candidate_group_id_text AS uuid)
JOIN candidate_evidence_bundles ceb
  ON ceb.bundle_id = CAST(ce.bundle_id_text AS uuid)
 AND ceb.candidate_group_id = cgp.candidate_group_id
WHERE ce.aggregate_id = jr.judge_run_id
  AND ce.judge_run_id_text = jr.judge_run_id::text
  AND ce.judge_output_id_text = jo.judge_output_id::text
  AND ce.candidate_group_id_text = jo.candidate_group_id::text
  AND ce.bundle_id_text = ceb.bundle_id::text
  AND cgp.current_bundle_id = ceb.bundle_id
  AND jr.bundle_id = ceb.bundle_id
  AND jr.status = 'succeeded'
  AND NOT EXISTS (
        SELECT 1
        FROM analyses a
        WHERE a.judge_output_id = jo.judge_output_id
          AND a.policy_version = :policy_version
          AND a.delivery_policy_version = :delivery_policy_version
  )
ORDER BY ce.created_at DESC, ce.event_id DESC
LIMIT :sample_limit
"""


_SEND_WORTHY_NOTIFICATION_INTENT_SQL = f"""
WITH candidate_events AS (
    SELECT
        eo.event_id,
        eo.aggregate_id,
        eo.created_at,
        eo.payload_json->>'notification_plan_id' AS notification_plan_id_text,
        eo.payload_json->>'analysis_id' AS analysis_id_text,
        eo.payload_json->>'candidate_group_id' AS candidate_group_id_text,
        eo.payload_json->>'delivery_decision' AS delivery_decision_text,
        eo.payload_json->>'target_chat_id' AS target_chat_id_text,
        eo.payload_json->>'material_change_hash' AS material_change_hash
    FROM event_outbox eo
    WHERE eo.event_type = 'notification.plan.created.v1'
      AND eo.aggregate_type = 'analysis'
      AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
      AND eo.payload_json->>'notification_plan_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
      AND eo.payload_json->>'analysis_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
      AND eo.payload_json->>'candidate_group_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
      AND COALESCE(eo.payload_json->>'delivery_decision', '') <> 'suppress'
      AND COALESCE(eo.payload_json->>'target_chat_id', '') ~ '{SIGNED_BIGINT_TEXT_SQL_RE}'
      AND (
            length(ltrim(COALESCE(eo.payload_json->>'target_chat_id', ''), '-')) < 19
            OR ltrim(COALESCE(eo.payload_json->>'target_chat_id', ''), '-') <= CASE
                WHEN left(COALESCE(eo.payload_json->>'target_chat_id', ''), 1) = '-'
                THEN '9223372036854775808'
                ELSE '9223372036854775807'
            END
      )
      AND COALESCE(eo.payload_json->>'material_change_hash', '') <> ''
)
SELECT
    ce.event_id,
    a.analysis_id,
    CAST(ce.notification_plan_id_text AS uuid) AS notification_plan_id,
    a.judge_output_id,
    a.candidate_group_id,
    a.verdict::text AS verdict,
    a.delivery_decision::text AS delivery_decision
FROM candidate_events ce
JOIN analyses a
  ON a.analysis_id = CAST(ce.analysis_id_text AS uuid)
WHERE ce.aggregate_id = a.analysis_id
  AND ce.analysis_id_text = a.analysis_id::text
  AND ce.candidate_group_id_text = a.candidate_group_id::text
  AND ce.delivery_decision_text = a.delivery_decision::text
  AND a.policy_version = :policy_version
  AND a.delivery_policy_version = :delivery_policy_version
  AND a.delivery_decision::text <> 'suppress'
  AND NOT EXISTS (
        SELECT 1
        FROM notification_plans sent_plan
        JOIN notification_delivery_records sent_delivery
          ON sent_delivery.notification_plan_id = sent_plan.notification_plan_id
        WHERE sent_plan.analysis_id = a.analysis_id
          AND sent_plan.target_chat_id = CAST(ce.target_chat_id_text AS bigint)
          AND sent_plan.material_change_hash = ce.material_change_hash
          AND sent_delivery.delivery_status::text IN ('sent', 'edited')
  )
ORDER BY
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM notification_renders nr
            JOIN notification_delivery_records ndr
              ON ndr.notification_plan_id = CAST(ce.notification_plan_id_text AS uuid)
             AND ndr.delivery_status::text = 'suppressed'
             AND ndr.telegram_response_json->>'send_disabled' = 'true'
             AND ndr.telegram_response_json->>'dry_run' = 'true'
            JOIN event_outbox delivery_result
              ON delivery_result.event_type = 'notification.delivery.result.v1'
             AND delivery_result.aggregate_type = 'notification_plan'
             AND delivery_result.aggregate_id = CAST(ce.notification_plan_id_text AS uuid)
            WHERE nr.notification_plan_id = CAST(ce.notification_plan_id_text AS uuid)
        )
        THEN 1
        ELSE 0
    END ASC,
    ce.created_at DESC,
    ce.event_id DESC
LIMIT :sample_limit
"""


_LIVE_DELIVERED_NOTIFICATION_PROOF_SQL = f"""
WITH candidate_events AS (
    SELECT
        eo.event_id,
        eo.aggregate_id,
        eo.created_at,
        eo.payload_json->>'notification_plan_id' AS notification_plan_id_text,
        eo.payload_json->>'analysis_id' AS analysis_id_text,
        eo.payload_json->>'candidate_group_id' AS candidate_group_id_text,
        eo.payload_json->>'delivery_decision' AS delivery_decision_text,
        eo.payload_json->>'target_chat_id' AS target_chat_id_text,
        eo.payload_json->>'material_change_hash' AS material_change_hash
    FROM event_outbox eo
    WHERE eo.event_type = 'notification.plan.created.v1'
      AND eo.aggregate_type = 'analysis'
      AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
      AND eo.payload_json->>'notification_plan_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
      AND eo.payload_json->>'analysis_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
      AND eo.payload_json->>'candidate_group_id' ~ '{STRICT_UUID_TEXT_SQL_RE}'
      AND COALESCE(eo.payload_json->>'delivery_decision', '') <> 'suppress'
      AND COALESCE(eo.payload_json->>'target_chat_id', '') ~ '{SIGNED_BIGINT_TEXT_SQL_RE}'
      AND (
            length(ltrim(COALESCE(eo.payload_json->>'target_chat_id', ''), '-')) < 19
            OR ltrim(COALESCE(eo.payload_json->>'target_chat_id', ''), '-') <= CASE
                WHEN left(COALESCE(eo.payload_json->>'target_chat_id', ''), 1) = '-'
                THEN '9223372036854775808'
                ELSE '9223372036854775807'
            END
      )
      AND COALESCE(eo.payload_json->>'material_change_hash', '') <> ''
),
proof_rows AS (
    SELECT
        ce.event_id,
        ce.created_at,
        a.analysis_id,
        np.notification_plan_id,
        a.judge_output_id,
        a.candidate_group_id,
        a.verdict::text AS verdict,
        a.delivery_decision::text AS delivery_decision,
        count(DISTINCT nr.notification_render_id) AS render_count,
        count(DISTINCT sent_delivery.notification_delivery_record_id) AS sent_or_edited_delivery_count,
        count(DISTINCT delivery_result.event_id) AS delivery_result_event_count
    FROM candidate_events ce
    JOIN analyses a
      ON a.analysis_id = CAST(ce.analysis_id_text AS uuid)
    JOIN notification_plans np
      ON np.notification_plan_id = CAST(ce.notification_plan_id_text AS uuid)
     AND np.analysis_id = a.analysis_id
     AND np.candidate_group_id = a.candidate_group_id
     AND np.delivery_decision = a.delivery_decision
     AND np.target_chat_id = CAST(ce.target_chat_id_text AS bigint)
     AND np.material_change_hash = ce.material_change_hash
    LEFT JOIN notification_renders nr
      ON nr.notification_plan_id = np.notification_plan_id
    LEFT JOIN notification_delivery_records sent_delivery
      ON sent_delivery.notification_plan_id = np.notification_plan_id
     AND sent_delivery.delivery_status::text IN ('sent', 'edited')
    LEFT JOIN event_outbox delivery_result
      ON delivery_result.event_type = 'notification.delivery.result.v1'
     AND delivery_result.aggregate_type = 'notification_plan'
     AND delivery_result.aggregate_id = np.notification_plan_id
    WHERE ce.aggregate_id = a.analysis_id
      AND ce.analysis_id_text = a.analysis_id::text
      AND ce.candidate_group_id_text = a.candidate_group_id::text
      AND ce.delivery_decision_text = a.delivery_decision::text
      AND a.policy_version = :policy_version
      AND a.delivery_policy_version = :delivery_policy_version
      AND a.delivery_decision::text <> 'suppress'
    GROUP BY
        ce.event_id,
        ce.created_at,
        a.analysis_id,
        np.notification_plan_id,
        a.judge_output_id,
        a.candidate_group_id,
        a.verdict,
        a.delivery_decision
)
SELECT
    event_id,
    analysis_id,
    notification_plan_id,
    judge_output_id,
    candidate_group_id,
    verdict,
    delivery_decision,
    render_count,
    sent_or_edited_delivery_count,
    delivery_result_event_count
FROM proof_rows
WHERE render_count > 0
  AND sent_or_edited_delivery_count > 0
  AND delivery_result_event_count > 0
ORDER BY created_at DESC, event_id DESC
LIMIT :sample_limit
"""


async def run_post_judge_notification_pipeline_inventory(
    request: PostJudgeNotificationPipelineInventoryRequest,
    *,
    validator_config: AnalysisValidatorConfig,
    policy_config: PolicyEngineConfig,
    components: PostJudgeNotificationPipelineInventoryComponents,
) -> PostJudgeNotificationPipelineInventoryReport:
    report = _report(
        mode=request.mode,
        status="failed",
        reason_code="unhandled_error",
        lookback_hours=request.lookback_hours,
        sample_limit=request.sample_limit,
    )
    try:
        counts = await components.inventory_repository.load_inventory_counts(
            lookback_hours=request.lookback_hours,
            sample_limit=request.sample_limit,
            policy_version=policy_config.policy_version,
            delivery_policy_version=policy_config.delivery_policy_version,
        )
        report = _apply_counts(report, counts)
        selected_ready = await components.inventory_repository.select_latest_eligible_judge_output_ready(
            lookback_hours=request.lookback_hours,
            sample_limit=request.sample_limit,
            policy_version=policy_config.policy_version,
            delivery_policy_version=policy_config.delivery_policy_version,
        )
        selected_policy = await components.inventory_repository.select_latest_eligible_policy_apply(
            lookback_hours=request.lookback_hours,
            sample_limit=request.sample_limit,
            policy_version=policy_config.policy_version,
            delivery_policy_version=policy_config.delivery_policy_version,
        )
        selected_notification = await components.inventory_repository.select_latest_send_worthy_notification_intent(
            lookback_hours=request.lookback_hours,
            sample_limit=request.sample_limit,
            policy_version=policy_config.policy_version,
            delivery_policy_version=policy_config.delivery_policy_version,
        )
        selected_live_delivered = (
            await components.inventory_repository.select_latest_live_delivered_notification_proof(
                lookback_hours=request.lookback_hours,
                sample_limit=request.sample_limit,
                policy_version=policy_config.policy_version,
                delivery_policy_version=policy_config.delivery_policy_version,
            )
        )
        report = _apply_selected_ready(report, selected_ready)
        report = _apply_selected_policy(report, selected_policy)
        report = _apply_selected_notification(report, selected_notification)
        report = _apply_selected_live_delivered_notification_proof(report, selected_live_delivered)

        if request.mode == "plan":
            if (
                selected_notification is None
                and selected_live_delivered is not None
                and _closed_live_delivered_notification_proof_count(report.counts) == 1
            ):
                return replace(
                    _apply_closed_live_delivered_notification_proof_readiness(
                        report, selected_live_delivered
                    ),
                    status="pass",
                    reason_code="sent_or_edited_notification_proof_already_closed",
                )
            return replace(report, status="pass", reason_code="inventory_plan_complete")

        if request.mode == "execute-validator":
            return await _execute_validator(
                request=request,
                validator_config=validator_config,
                policy_config=policy_config,
                components=components,
                report=report,
                selected_ready=selected_ready,
            )

        if request.mode == "execute-policy":
            return await _execute_policy(
                request=request,
                policy_config=policy_config,
                components=components,
                report=report,
                selected_policy=selected_policy,
            )

        if request.mode == "execute-notifier":
            return await _execute_notifier(
                request=request,
                policy_config=policy_config,
                components=components,
                report=report,
                selected_notification=selected_notification,
            )

        if request.mode == "execute-macro":
            return await _execute_macro(
                request=request,
                validator_config=validator_config,
                policy_config=policy_config,
                components=components,
                report=report,
                selected_ready=selected_ready,
                selected_policy=selected_policy,
                selected_notification=selected_notification,
                selected_live_delivered=selected_live_delivered,
            )

        return replace(report, status="blocked", reason_code="invalid_mode")
    except PostJudgeNotificationPipelineInventoryConfigError as exc:
        return replace(report, status="blocked", reason_code=_safe_reason_code_value(exc))
    except Exception:
        return replace(report, status="failed", reason_code="unhandled_error")


async def _execute_validator(
    *,
    request: PostJudgeNotificationPipelineInventoryRequest,
    validator_config: AnalysisValidatorConfig,
    policy_config: PolicyEngineConfig,
    components: PostJudgeNotificationPipelineInventoryComponents,
    report: PostJudgeNotificationPipelineInventoryReport,
    selected_ready: SelectedTarget | None,
) -> PostJudgeNotificationPipelineInventoryReport:
    del validator_config
    if not request.select_latest_eligible_judge_output_ready:
        return replace(report, status="blocked", reason_code="validator_selector_required")
    if selected_ready is None:
        return replace(report, status="blocked", reason_code=_no_ready_target_reason(report.counts))
    if request.expected_judge_output_ready_fingerprint != selected_ready.event_fingerprint:
        return replace(report, status="blocked", reason_code="judge_output_ready_fingerprint_mismatch")

    preflight = await components.inventory_repository.select_latest_eligible_judge_output_ready(
        lookback_hours=request.lookback_hours,
        sample_limit=request.sample_limit,
        policy_version=policy_config.policy_version,
        delivery_policy_version=policy_config.delivery_policy_version,
    )
    if preflight is None:
        return replace(report, status="blocked", reason_code="validator_preflight_target_missing")
    if preflight.event_fingerprint != selected_ready.event_fingerprint:
        return replace(report, status="blocked", reason_code="validator_preflight_target_changed")

    report = replace(report, validator_attempted=True)
    await components.validator_service.handle_trigger_event(preflight.event_id)
    try:
        await components.commit_active_transaction()
    except Exception:
        return replace(report, status="failed", reason_code="validator_commit_failed")

    readback = await components.inventory_repository.load_validator_readback(
        judge_run_id=preflight.judge_run_id,
        judge_output_id=preflight.judge_output_id,
        policy_version=policy_config.policy_version,
        delivery_policy_version=policy_config.delivery_policy_version,
    )
    report = _apply_validator_readback(report, readback)
    if readback.policy_event_count == 1:
        if readback.active_analysis_count != 0:
            return replace(report, status="failed", reason_code="validator_analysis_changed_unexpectedly")
        return replace(report, status="pass", reason_code="validator_policy_apply_materialized")
    if readback.policy_event_count > 1:
        return replace(report, status="failed", reason_code="validator_policy_event_ambiguous")
    if readback.validator_terminal_or_retryable_count > 0 and readback.validator_terminal_or_retryable_reason_code:
        return replace(
            report,
            status="pass",
            reason_code=readback.validator_terminal_or_retryable_reason_code,
        )
    return replace(report, status="failed", reason_code="validator_policy_event_missing")


async def _execute_policy(
    *,
    request: PostJudgeNotificationPipelineInventoryRequest,
    policy_config: PolicyEngineConfig,
    components: PostJudgeNotificationPipelineInventoryComponents,
    report: PostJudgeNotificationPipelineInventoryReport,
    selected_policy: SelectedTarget | None,
) -> PostJudgeNotificationPipelineInventoryReport:
    if not request.select_latest_eligible_policy_apply:
        return replace(report, status="blocked", reason_code="policy_selector_required")
    if selected_policy is None:
        return replace(report, status="blocked", reason_code=_no_policy_target_reason(report.counts))
    if request.expected_policy_apply_fingerprint != selected_policy.event_fingerprint:
        return replace(report, status="blocked", reason_code="policy_apply_fingerprint_mismatch")

    preflight = await components.inventory_repository.select_latest_eligible_policy_apply(
        lookback_hours=request.lookback_hours,
        sample_limit=request.sample_limit,
        policy_version=policy_config.policy_version,
        delivery_policy_version=policy_config.delivery_policy_version,
    )
    if preflight is None:
        return replace(report, status="blocked", reason_code="policy_preflight_target_missing")
    if preflight.event_fingerprint != selected_policy.event_fingerprint:
        return replace(report, status="blocked", reason_code="policy_preflight_target_changed")

    report = replace(report, policy_attempted=True)
    await components.policy_service.handle_trigger_event(preflight.event_id)
    try:
        await components.commit_active_transaction()
    except Exception:
        return replace(report, status="failed", reason_code="policy_commit_failed")

    readback = await components.inventory_repository.load_policy_readback(
        judge_output_id=preflight.judge_output_id,
        policy_version=policy_config.policy_version,
        delivery_policy_version=policy_config.delivery_policy_version,
    )
    report = _apply_policy_readback(report, readback)
    if readback.analysis_count != 1 or readback.analysis_id is None:
        return replace(report, status="failed", reason_code="policy_analysis_missing")
    if readback.delivery_decision == "suppress":
        if readback.notification_intent_event_count != 0:
            return replace(report, status="failed", reason_code="policy_suppress_notification_intent_unexpected")
        return replace(report, status="pass", reason_code="policy_suppressed_no_notification_intent_required")
    if readback.notification_intent_event_count == 1:
        return replace(report, status="pass", reason_code="policy_analysis_notification_intent_materialized")
    if readback.notification_intent_event_count > 1:
        return replace(report, status="failed", reason_code="policy_notification_intent_ambiguous")
    return replace(report, status="failed", reason_code="policy_notification_intent_missing")


async def _execute_notifier(
    *,
    request: PostJudgeNotificationPipelineInventoryRequest,
    policy_config: PolicyEngineConfig,
    components: PostJudgeNotificationPipelineInventoryComponents,
    report: PostJudgeNotificationPipelineInventoryReport,
    selected_notification: SelectedNotificationIntentTarget | None,
) -> PostJudgeNotificationPipelineInventoryReport:
    if not request.select_latest_send_worthy_notification_intent:
        return replace(report, status="blocked", reason_code="notification_intent_selector_required")
    if selected_notification is None:
        return replace(report, status="blocked", reason_code=_no_send_worthy_target_reason(report.counts))
    if request.expected_notification_intent_fingerprint != selected_notification.event_fingerprint:
        return replace(report, status="blocked", reason_code="notification_intent_fingerprint_mismatch")
    if selected_notification.delivery_decision == "suppress":
        return replace(report, status="blocked", reason_code="send_worthy_target_suppress_worthy")
    if components.notifier_service is None:
        return replace(report, status="blocked", reason_code="notifier_service_missing")

    preflight = await components.inventory_repository.select_latest_send_worthy_notification_intent(
        lookback_hours=request.lookback_hours,
        sample_limit=request.sample_limit,
        policy_version=policy_config.policy_version,
        delivery_policy_version=policy_config.delivery_policy_version,
    )
    if preflight is None:
        return replace(report, status="blocked", reason_code="notification_intent_preflight_target_missing")
    if preflight.event_fingerprint != selected_notification.event_fingerprint:
        return replace(report, status="blocked", reason_code="notification_intent_preflight_target_changed")

    before = await components.inventory_repository.load_notification_proof_readback(
        notification_intent_event_id=preflight.event_id,
        analysis_id=preflight.analysis_id,
        notification_plan_id=preflight.notification_plan_id,
    )
    report = _apply_notification_readback(report, before)
    if _notification_proof_exactly_once(before):
        return replace(report, status="pass", reason_code="already_materialized")
    if _notification_proof_ambiguous(before):
        return replace(report, status="failed", reason_code="notification_proof_state_ambiguous")

    report = replace(report, notifier_attempted=True, db_write_attempted=True)
    try:
        await components.notifier_service.handle_trigger_event(preflight.event_id)
    except TelegramTransportTerminalError:
        attempted = _telegram_transport_attempted(components)
        return replace(
            report,
            telegram_attempted=attempted,
            telegram_transport_attempted=attempted,
            status="failed",
            reason_code="telegram_transport_attempted",
        )
    except Exception:
        return replace(report, status="failed", reason_code="notifier_execute_failed")

    try:
        await components.commit_active_transaction()
    except Exception:
        return replace(report, status="failed", reason_code="notifier_commit_failed")

    attempted = _telegram_transport_attempted(components)
    if attempted:
        return replace(
            report,
            telegram_attempted=True,
            telegram_transport_attempted=True,
            status="failed",
            reason_code="telegram_transport_attempted",
        )

    readback = await components.inventory_repository.load_notification_proof_readback(
        notification_intent_event_id=preflight.event_id,
        analysis_id=preflight.analysis_id,
        notification_plan_id=preflight.notification_plan_id,
    )
    report = _apply_notification_readback(
        replace(report, telegram_attempted=False, telegram_transport_attempted=False),
        readback,
    )
    if _notification_proof_exactly_once(readback):
        return replace(report, status="pass", reason_code="notification_send_disabled_suppressed")
    if readback.notification_plan_count != 1 or readback.notification_render_count != 1:
        return replace(report, status="failed", reason_code="notification_readback_invalid")
    if (
        readback.send_disabled_delivery_record_count != 1
        or readback.notification_delivery_result_event_count != 1
    ):
        return replace(report, status="failed", reason_code="send_disabled_delivery_missing")
    return replace(report, status="failed", reason_code="notification_readback_invalid")


async def _execute_macro(
    *,
    request: PostJudgeNotificationPipelineInventoryRequest,
    validator_config: AnalysisValidatorConfig,
    policy_config: PolicyEngineConfig,
    components: PostJudgeNotificationPipelineInventoryComponents,
    report: PostJudgeNotificationPipelineInventoryReport,
    selected_ready: SelectedTarget | None,
    selected_policy: SelectedTarget | None,
    selected_notification: SelectedNotificationIntentTarget | None,
    selected_live_delivered: LiveDeliveredNotificationProofTarget | None,
) -> PostJudgeNotificationPipelineInventoryReport:
    if request.select_latest_send_worthy_notification_intent:
        if selected_notification is None:
            closed = _closed_live_delivered_notification_proof_report(
                report,
                selected_live_delivered,
                expected_notification_intent_fingerprint=request.expected_notification_intent_fingerprint,
            )
            if closed is not None:
                return closed
        return await _execute_notifier(
            request=request,
            policy_config=policy_config,
            components=components,
            report=report,
            selected_notification=selected_notification,
        )

    if request.select_latest_eligible_policy_apply:
        if selected_policy is None:
            return replace(report, status="blocked", reason_code=_macro_no_send_worthy_target_reason(report.counts))
        policy_report = await _execute_policy(
            request=request,
            policy_config=policy_config,
            components=components,
            report=report,
            selected_policy=selected_policy,
        )
        return await _execute_macro_notifier_after_policy(
            request=request,
            policy_config=policy_config,
            components=components,
            report=policy_report,
            selected_policy=selected_policy,
            selected_live_delivered=selected_live_delivered,
        )

    if request.select_latest_eligible_judge_output_ready:
        if selected_ready is None:
            return replace(report, status="blocked", reason_code=_macro_no_send_worthy_target_reason(report.counts))
        validator_report = await _execute_validator(
            request=request,
            validator_config=validator_config,
            policy_config=policy_config,
            components=components,
            report=report,
            selected_ready=selected_ready,
        )
        if validator_report.status != "pass":
            return validator_report
        if validator_report.reason_code != "validator_policy_apply_materialized":
            return replace(validator_report, status="blocked", reason_code=validator_report.reason_code)

        selected_policy_after_validator = await components.inventory_repository.select_latest_eligible_policy_apply(
            lookback_hours=request.lookback_hours,
            sample_limit=request.sample_limit,
            policy_version=policy_config.policy_version,
            delivery_policy_version=policy_config.delivery_policy_version,
        )
        if selected_policy_after_validator is None:
            return replace(validator_report, status="blocked", reason_code="policy_target_missing_after_validator")
        if not _same_judge_target(selected_policy_after_validator, selected_ready):
            return replace(validator_report, status="blocked", reason_code="policy_target_changed_after_validator")

        policy_request = replace(
            request,
            select_latest_eligible_judge_output_ready=False,
            expected_judge_output_ready_fingerprint=None,
            select_latest_eligible_policy_apply=True,
            expected_policy_apply_fingerprint=selected_policy_after_validator.event_fingerprint,
        )
        policy_report = await _execute_policy(
            request=policy_request,
            policy_config=policy_config,
            components=components,
            report=_apply_selected_policy(validator_report, selected_policy_after_validator),
            selected_policy=selected_policy_after_validator,
        )
        return await _execute_macro_notifier_after_policy(
            request=request,
            policy_config=policy_config,
            components=components,
            report=policy_report,
            selected_policy=selected_policy_after_validator,
            selected_live_delivered=selected_live_delivered,
        )

    return replace(report, status="blocked", reason_code="macro_selector_required")


async def _execute_macro_notifier_after_policy(
    *,
    request: PostJudgeNotificationPipelineInventoryRequest,
    policy_config: PolicyEngineConfig,
    components: PostJudgeNotificationPipelineInventoryComponents,
    report: PostJudgeNotificationPipelineInventoryReport,
    selected_policy: SelectedTarget,
    selected_live_delivered: LiveDeliveredNotificationProofTarget | None,
) -> PostJudgeNotificationPipelineInventoryReport:
    if report.status != "pass":
        return report
    if report.reason_code == "policy_suppressed_no_notification_intent_required":
        return replace(report, status="blocked", reason_code="send_worthy_target_policy_suppressed")
    if report.reason_code != "policy_analysis_notification_intent_materialized":
        return replace(report, status="blocked", reason_code=report.reason_code)

    selected_notification = await components.inventory_repository.select_latest_send_worthy_notification_intent(
        lookback_hours=request.lookback_hours,
        sample_limit=request.sample_limit,
        policy_version=policy_config.policy_version,
        delivery_policy_version=policy_config.delivery_policy_version,
    )
    if selected_notification is None:
        if (
            selected_live_delivered is not None
            and selected_live_delivered.judge_output_id == selected_policy.judge_output_id
            and selected_live_delivered.candidate_group_id == selected_policy.candidate_group_id
        ):
            closed = _closed_live_delivered_notification_proof_report(
                report,
                selected_live_delivered,
                expected_notification_intent_fingerprint=selected_live_delivered.event_fingerprint,
            )
            if closed is not None:
                return closed
        return replace(report, status="blocked", reason_code="notification_intent_missing_after_policy")
    if (
        selected_notification.judge_output_id != selected_policy.judge_output_id
        or selected_notification.candidate_group_id != selected_policy.candidate_group_id
    ):
        return replace(report, status="blocked", reason_code="notification_intent_not_from_selected_policy")

    notifier_request = replace(
        request,
        select_latest_eligible_judge_output_ready=False,
        expected_judge_output_ready_fingerprint=None,
        select_latest_eligible_policy_apply=False,
        expected_policy_apply_fingerprint=None,
        select_latest_send_worthy_notification_intent=True,
        expected_notification_intent_fingerprint=selected_notification.event_fingerprint,
    )
    return await _execute_notifier(
        request=notifier_request,
        policy_config=policy_config,
        components=components,
        report=_apply_selected_notification(report, selected_notification),
        selected_notification=selected_notification,
    )


def _same_judge_target(left: SelectedTarget, right: SelectedTarget) -> bool:
    return (
        left.judge_run_id == right.judge_run_id
        and left.judge_output_id == right.judge_output_id
        and left.candidate_group_id == right.candidate_group_id
        and left.bundle_id == right.bundle_id
    )


def build_parser() -> argparse.ArgumentParser:
    parser = SilentArgumentParser(prog="post-judge-notification-pipeline-inventory", allow_abbrev=False)
    parser.add_argument("--mode")
    parser.add_argument("--env-file")
    parser.add_argument("--lookback-hours", type=int, default=72)
    parser.add_argument("--sample-limit", type=int, default=100)
    parser.add_argument("--select-latest-eligible-judge-output-ready", action="store_true")
    parser.add_argument("--judge-output-selection-confirm", default=None)
    parser.add_argument("--expected-judge-output-ready-fingerprint", default=None)
    parser.add_argument("--select-latest-eligible-policy-apply", action="store_true")
    parser.add_argument("--policy-apply-selection-confirm", default=None)
    parser.add_argument("--expected-policy-apply-fingerprint", default=None)
    parser.add_argument("--select-latest-send-worthy-notification-intent", action="store_true")
    parser.add_argument("--notification-intent-selection-confirm", default=None)
    parser.add_argument("--expected-notification-intent-fingerprint", default=None)
    parser.add_argument("--confirm", default=None)
    return parser


async def run_cli(
    argv: Sequence[str] | None = None,
    *,
    emit_json: Callable[[str], None] = print,
    runtime_config_loader: Callable[[str], RuntimeConfigBundle] | None = None,
    components_builder: Callable[[RuntimeConfigBundle], AsyncIterator[PostJudgeNotificationPipelineInventoryComponents]]
    | None = None,
) -> int:
    try:
        args = build_parser().parse_args(list(argv) if argv is not None else None)
    except PostJudgeNotificationPipelineInventoryConfigError as exc:
        emit_json(_compact_json(asdict(_argument_report(str(exc)))))
        return 2

    mode = str(args.mode) if args.mode in VALID_MODES else "unknown"
    validation_error = _cli_request_error(args)
    if validation_error is not None:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=mode,
                        status="blocked",
                        reason_code=validation_error,
                        lookback_hours=_bounded_cli_int(args.lookback_hours, default=72),
                        sample_limit=_bounded_cli_int(args.sample_limit, default=100),
                    )
                )
            )
        )
        return 2

    try:
        runtime = (runtime_config_loader or load_runtime_config)(str(args.env_file))
    except PostJudgeNotificationPipelineInventoryConfigError as exc:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=mode,
                        status="blocked",
                        reason_code=_safe_reason_code_value(exc),
                        lookback_hours=int(args.lookback_hours),
                        sample_limit=int(args.sample_limit),
                    )
                )
            )
        )
        return 2

    request = PostJudgeNotificationPipelineInventoryRequest(
        mode=str(args.mode),
        lookback_hours=int(args.lookback_hours),
        sample_limit=int(args.sample_limit),
        select_latest_eligible_judge_output_ready=bool(args.select_latest_eligible_judge_output_ready),
        expected_judge_output_ready_fingerprint=_optional_str(args.expected_judge_output_ready_fingerprint),
        select_latest_eligible_policy_apply=bool(args.select_latest_eligible_policy_apply),
        expected_policy_apply_fingerprint=_optional_str(args.expected_policy_apply_fingerprint),
        select_latest_send_worthy_notification_intent=bool(
            args.select_latest_send_worthy_notification_intent
        ),
        expected_notification_intent_fingerprint=_optional_str(args.expected_notification_intent_fingerprint),
    )

    builder = components_builder or sql_inventory_components
    async with builder(runtime) as components:
        report = await run_post_judge_notification_pipeline_inventory(
            request,
            validator_config=runtime.validator_config,
            policy_config=runtime.policy_config,
            components=components,
        )
    emit_json(_compact_json(asdict(report)))
    return 0 if report.status == "pass" else 2


def load_runtime_config(env_file: str) -> RuntimeConfigBundle:
    values = _read_runtime_env_file(env_file)
    resolved_values = dict(values)
    database_url = _resolve_file_indirection(
        resolved_values,
        value_key="DATABASE_URL",
        file_key="DATABASE_URL_FILE",
        missing_reason_code="database_url_missing",
        file_missing_reason_code="database_url_file_missing",
        file_empty_reason_code="database_url_file_empty",
    )
    try:
        validator_config = AnalysisValidatorConfig(
            app_env=_read(resolved_values, "APP_ENV", "dev").lower(),
            database_url=database_url,
            redis_url=PLACEHOLDER_REDIS_URL,
            queue_name=_read(resolved_values, "ANALYSIS_VALIDATOR_QUEUE_NAME", "q.analysis.validate"),
            consumer_group=_read(resolved_values, "ANALYSIS_VALIDATOR_CONSUMER_GROUP", "analysis-validator"),
            consumer_name=_read(
                resolved_values,
                "ANALYSIS_VALIDATOR_CONSUMER_NAME",
                "post-judge-notification-pipeline-inventory",
            ),
            batch_size=int(_read(resolved_values, "ANALYSIS_VALIDATOR_BATCH_SIZE", "20")),
            block_ms=int(_read(resolved_values, "ANALYSIS_VALIDATOR_BLOCK_MS", "5000")),
            max_headline_chars=int(_read(resolved_values, "ANALYSIS_VALIDATOR_MAX_HEADLINE_CHARS", "200")),
            max_summary_chars=int(_read(resolved_values, "ANALYSIS_VALIDATOR_MAX_SUMMARY_CHARS", "1200")),
            max_text_items=int(_read(resolved_values, "ANALYSIS_VALIDATOR_MAX_TEXT_ITEMS", "10")),
            log_level=_read(resolved_values, "LOG_LEVEL", "INFO").upper(),
        )
        validator_config.validate()
    except (TypeError, ValueError):
        raise PostJudgeNotificationPipelineInventoryConfigError("analysis_validator_config_invalid") from None
    try:
        policy_config = PolicyEngineConfig(
            app_env=_read(resolved_values, "APP_ENV", "dev").lower(),
            database_url=database_url,
            redis_url=PLACEHOLDER_REDIS_URL,
            queue_name=_read(resolved_values, "POLICY_ENGINE_QUEUE_NAME", "q.analysis.policy"),
            consumer_group=_read(resolved_values, "POLICY_ENGINE_CONSUMER_GROUP", "policy-engine"),
            consumer_name=_read(
                resolved_values,
                "POLICY_ENGINE_CONSUMER_NAME",
                "post-judge-notification-pipeline-inventory",
            ),
            batch_size=int(_read(resolved_values, "POLICY_ENGINE_BATCH_SIZE", "20")),
            block_ms=int(_read(resolved_values, "POLICY_ENGINE_BLOCK_MS", "5000")),
            policy_version=_read(resolved_values, "VERDICT_POLICY_VERSION", "verdict_policy_v1"),
            delivery_policy_version=_read(resolved_values, "DELIVERY_POLICY_VERSION", "delivery_policy_v1"),
            operator_chat_id=int(_read(resolved_values, "TELEGRAM_OPERATOR_CHAT_ID", "0")),
            enable_later_delivery=_bool_value(_read(resolved_values, "ENABLE_LATER_DELIVERY", "true")),
            enable_silent_later=_bool_value(_read(resolved_values, "ENABLE_SILENT_LATER", "true")),
            enable_notification_send=_bool_value(_read(resolved_values, "ENABLE_NOTIFICATION_SEND", "true")),
            render_profile_high=_read(
                resolved_values,
                "NOTIFY_RENDER_PROFILE_HIGH",
                "telegram_single_alert_high_v1",
            ),
            render_profile_normal=_read(
                resolved_values,
                "NOTIFY_RENDER_PROFILE_NORMAL",
                "telegram_single_alert_normal_v1",
            ),
            log_level=_read(resolved_values, "LOG_LEVEL", "INFO").upper(),
        )
        policy_config.validate()
    except (TypeError, ValueError):
        raise PostJudgeNotificationPipelineInventoryConfigError("policy_engine_config_invalid") from None
    try:
        notifier_config = NotifierTelegramConfig(
            app_env=_read(resolved_values, "APP_ENV", "dev").lower(),
            database_url=database_url,
            redis_url=PLACEHOLDER_REDIS_URL,
            telegram_bot_token="",
            queue_name=_read(resolved_values, "NOTIFIER_TELEGRAM_QUEUE_NAME", "q.notification.send"),
            consumer_group=_read(resolved_values, "NOTIFIER_TELEGRAM_CONSUMER_GROUP", "notifier-telegram"),
            consumer_name=_read(
                resolved_values,
                "NOTIFIER_TELEGRAM_CONSUMER_NAME",
                "post-judge-notification-pipeline-inventory",
            ),
            batch_size=int(_read(resolved_values, "NOTIFIER_TELEGRAM_BATCH_SIZE", "20")),
            block_ms=int(_read(resolved_values, "NOTIFIER_TELEGRAM_BLOCK_MS", "5000")),
            dry_run=True,
            allow_edits=False,
            enable_notification_send=False,
            enable_digest_runtime=_bool_value(_read(resolved_values, "ENABLE_DIGEST_RUNTIME", "false")),
            max_message_chars=int(_read(resolved_values, "NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS", "3800")),
            edit_window_minutes=int(_read(resolved_values, "NOTIFIER_TELEGRAM_EDIT_WINDOW_MINUTES", "180")),
            telegram_api_base_url=_read(resolved_values, "TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
            request_timeout_sec=float(_read(resolved_values, "NOTIFIER_TELEGRAM_REQUEST_TIMEOUT_SEC", "10")),
            log_level=_read(resolved_values, "LOG_LEVEL", "INFO").upper(),
        )
        notifier_config.validate(require_transport_token=False)
    except (TypeError, ValueError):
        raise PostJudgeNotificationPipelineInventoryConfigError("notifier_config_invalid") from None
    return RuntimeConfigBundle(
        database_url=database_url,
        values=resolved_values,
        validator_config=validator_config,
        policy_config=policy_config,
        notifier_config=notifier_config,
    )


@asynccontextmanager
async def sql_inventory_components(
    runtime: RuntimeConfigBundle,
) -> AsyncIterator[PostJudgeNotificationPipelineInventoryComponents]:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with session_factory() as session:
            notifier_transport = FailClosedTelegramTransport()

            async def commit_active_transaction() -> None:
                if session.in_transaction():
                    await session.commit()

            yield PostJudgeNotificationPipelineInventoryComponents(
                inventory_repository=SqlPostJudgeNotificationPipelineInventoryRepository(session),
                validator_service=AnalysisValidatorService(
                    runtime.validator_config,
                    repository=AnalysisValidatorRepository(session),
                ),
                policy_service=PolicyEngineService(
                    runtime.policy_config,
                    repository=PolicyEngineRepository(session),
                ),
                notifier_service=NotifierTelegramService(
                    runtime.notifier_config,
                    repository=NotifierTelegramRepository(session),
                    telegram_client=notifier_transport,
                ),
                commit_active_transaction=commit_active_transaction,
                telegram_transport_attempted=lambda: notifier_transport.attempted,
            )
    finally:
        await engine.dispose()


def _cli_request_error(args: argparse.Namespace) -> str | None:
    if args.mode not in VALID_MODES:
        return "invalid_mode"
    if not args.env_file:
        return "env_file_required"
    if args.lookback_hours < 1 or args.lookback_hours > 720:
        return "lookback_hours_out_of_range"
    if args.sample_limit < 1 or args.sample_limit > 500:
        return "sample_limit_out_of_range"
    if args.mode == "plan" and args.confirm is not None:
        return "confirm_not_allowed_for_plan"
    selector_error = _selector_args_error(args)
    if selector_error is not None:
        return selector_error
    if args.mode == "execute-validator":
        if args.confirm != VALIDATOR_CONFIRM_TOKEN:
            return "exact_validator_apply_confirm_missing"
        if not args.select_latest_eligible_judge_output_ready:
            return "validator_selector_required"
        if not _optional_str(args.expected_judge_output_ready_fingerprint):
            return "expected_judge_output_ready_fingerprint_missing"
    if args.mode == "execute-policy":
        if args.confirm != POLICY_CONFIRM_TOKEN:
            return "exact_policy_apply_confirm_missing"
        if not args.select_latest_eligible_policy_apply:
            return "policy_selector_required"
        if not _optional_str(args.expected_policy_apply_fingerprint):
            return "expected_policy_apply_fingerprint_missing"
    if args.mode == "execute-notifier":
        if args.confirm != NOTIFIER_CONFIRM_TOKEN:
            return "exact_notifier_send_disabled_confirm_missing"
        if not args.select_latest_send_worthy_notification_intent:
            return "notification_intent_selector_required"
        if not _optional_str(args.expected_notification_intent_fingerprint):
            return "expected_notification_intent_fingerprint_missing"
    if args.mode == "execute-macro":
        if args.confirm != MACRO_CONFIRM_TOKEN:
            return "macro_send_worthy_confirm_missing"
        selector_count = sum(
            bool(value)
            for value in (
                args.select_latest_eligible_judge_output_ready,
                args.select_latest_eligible_policy_apply,
                args.select_latest_send_worthy_notification_intent,
            )
        )
        if selector_count != 1:
            return "macro_exactly_one_selector_required"
        if args.select_latest_eligible_judge_output_ready and not _optional_str(
            args.expected_judge_output_ready_fingerprint
        ):
            return "expected_judge_output_ready_fingerprint_missing"
        if args.select_latest_eligible_policy_apply and not _optional_str(args.expected_policy_apply_fingerprint):
            return "expected_policy_apply_fingerprint_missing"
        if args.select_latest_send_worthy_notification_intent and not _optional_str(
            args.expected_notification_intent_fingerprint
        ):
            return "expected_notification_intent_fingerprint_missing"
    return None


def _selector_args_error(args: argparse.Namespace) -> str | None:
    ready_args = (
        args.select_latest_eligible_judge_output_ready
        or args.judge_output_selection_confirm is not None
        or args.expected_judge_output_ready_fingerprint is not None
    )
    policy_args = (
        args.select_latest_eligible_policy_apply
        or args.policy_apply_selection_confirm is not None
        or args.expected_policy_apply_fingerprint is not None
    )
    notification_args = (
        args.select_latest_send_worthy_notification_intent
        or args.notification_intent_selection_confirm is not None
        or args.expected_notification_intent_fingerprint is not None
    )
    if args.mode == "execute-validator" and policy_args:
        return "policy_selector_not_allowed_for_validator_execute"
    if args.mode == "execute-validator" and notification_args:
        return "notification_selector_not_allowed_for_validator_execute"
    if args.mode == "execute-policy" and ready_args:
        return "judge_output_selector_not_allowed_for_policy_execute"
    if args.mode == "execute-policy" and notification_args:
        return "notification_selector_not_allowed_for_policy_execute"
    if args.mode == "execute-notifier" and ready_args:
        return "judge_output_selector_not_allowed_for_notifier_execute"
    if args.mode == "execute-notifier" and policy_args:
        return "policy_selector_not_allowed_for_notifier_execute"
    if args.select_latest_eligible_judge_output_ready:
        if args.judge_output_selection_confirm != JUDGE_OUTPUT_SELECTION_CONFIRM_TOKEN:
            return "judge_output_selection_confirm_missing"
    elif args.judge_output_selection_confirm is not None:
        return "judge_output_selection_confirm_without_selector"
    if args.select_latest_eligible_policy_apply:
        if args.policy_apply_selection_confirm != POLICY_APPLY_SELECTION_CONFIRM_TOKEN:
            return "policy_apply_selection_confirm_missing"
    elif args.policy_apply_selection_confirm is not None:
        return "policy_apply_selection_confirm_without_selector"
    if args.select_latest_send_worthy_notification_intent:
        if args.notification_intent_selection_confirm != NOTIFICATION_INTENT_SELECTION_CONFIRM_TOKEN:
            return "notification_intent_selection_confirm_missing"
    elif args.notification_intent_selection_confirm is not None:
        return "notification_intent_selection_confirm_without_selector"
    for value, reason in (
        (args.expected_judge_output_ready_fingerprint, "expected_judge_output_ready_fingerprint_invalid"),
        (args.expected_policy_apply_fingerprint, "expected_policy_apply_fingerprint_invalid"),
        (args.expected_notification_intent_fingerprint, "expected_notification_intent_fingerprint_invalid"),
    ):
        if value is not None and not EXPECTED_FINGERPRINT_RE.fullmatch(str(value)):
            return reason
    return None


def _read_runtime_env_file(env_file: str) -> dict[str, str]:
    path = Path(env_file)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        raise PostJudgeNotificationPipelineInventoryConfigError("env_file_missing") from None
    except OSError:
        raise PostJudgeNotificationPipelineInventoryConfigError("env_file_unreadable") from None

    values: dict[str, str] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in RUNTIME_ENV_KEYS:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    if not values:
        raise PostJudgeNotificationPipelineInventoryConfigError("env_file_no_runtime_config")
    return values


def _resolve_file_indirection(
    values: dict[str, str],
    *,
    value_key: str,
    file_key: str,
    missing_reason_code: str,
    file_missing_reason_code: str,
    file_empty_reason_code: str,
) -> str:
    direct = values.get(value_key, "").strip()
    if direct:
        return direct
    file_path = values.get(file_key, "").strip()
    if not file_path:
        raise PostJudgeNotificationPipelineInventoryConfigError(missing_reason_code)
    path = Path(file_path)
    if not path.is_file():
        raise PostJudgeNotificationPipelineInventoryConfigError(file_missing_reason_code)
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError:
        raise PostJudgeNotificationPipelineInventoryConfigError(file_missing_reason_code) from None
    if not value:
        raise PostJudgeNotificationPipelineInventoryConfigError(file_empty_reason_code)
    values[value_key] = value
    return value


def _apply_counts(
    report: PostJudgeNotificationPipelineInventoryReport,
    counts: InventoryCounts,
) -> PostJudgeNotificationPipelineInventoryReport:
    report_counts = _counts_to_report(counts)
    return replace(
        report,
        counts=report_counts,
        nearest_send_worthy_missing_stage=_nearest_send_worthy_missing_stage(report_counts),
    )


def _apply_selected_ready(
    report: PostJudgeNotificationPipelineInventoryReport,
    selected: SelectedTarget | None,
) -> PostJudgeNotificationPipelineInventoryReport:
    if selected is None:
        return report
    return replace(
        report,
        selected_judge_output_ready_fingerprint=selected.event_fingerprint,
        selected_judge_run_fingerprint=_fingerprint(selected.judge_run_id),
        selected_judge_output_fingerprint=_fingerprint(selected.judge_output_id),
        selected_candidate_group_fingerprint=_fingerprint(selected.candidate_group_id),
        selected_bundle_fingerprint=_fingerprint(selected.bundle_id),
    )


def _apply_selected_policy(
    report: PostJudgeNotificationPipelineInventoryReport,
    selected: SelectedTarget | None,
) -> PostJudgeNotificationPipelineInventoryReport:
    if selected is None:
        return report
    return replace(
        report,
        selected_policy_apply_fingerprint=selected.event_fingerprint,
        selected_judge_run_fingerprint=report.selected_judge_run_fingerprint
        or _fingerprint(selected.judge_run_id),
        selected_judge_output_fingerprint=report.selected_judge_output_fingerprint
        or _fingerprint(selected.judge_output_id),
        selected_candidate_group_fingerprint=report.selected_candidate_group_fingerprint
        or _fingerprint(selected.candidate_group_id),
        selected_bundle_fingerprint=report.selected_bundle_fingerprint
        or _fingerprint(selected.bundle_id),
    )


def _apply_selected_notification(
    report: PostJudgeNotificationPipelineInventoryReport,
    selected: SelectedNotificationIntentTarget | None,
) -> PostJudgeNotificationPipelineInventoryReport:
    if selected is None:
        return report
    return replace(
        report,
        selected_notification_intent_fingerprint=selected.event_fingerprint,
        selected_analysis_fingerprint=_fingerprint(selected.analysis_id),
        selected_notification_plan_fingerprint=_fingerprint(selected.notification_plan_id),
        selected_judge_output_fingerprint=report.selected_judge_output_fingerprint
        or _fingerprint(selected.judge_output_id),
        selected_candidate_group_fingerprint=report.selected_candidate_group_fingerprint
        or _fingerprint(selected.candidate_group_id),
        final_verdict=_safe_bucket_value(selected.verdict),
        delivery_decision=_safe_bucket_value(selected.delivery_decision),
        analysis_created_or_present=True,
        notification_intent_created_or_present=True,
    )


def _apply_selected_live_delivered_notification_proof(
    report: PostJudgeNotificationPipelineInventoryReport,
    selected: LiveDeliveredNotificationProofTarget | None,
) -> PostJudgeNotificationPipelineInventoryReport:
    if selected is None:
        return report
    counts = dict(report.counts)
    counts.update(
        {
            "selected_live_delivered_render_count": _int(selected.render_count),
            "selected_live_delivered_sent_or_edited_delivery_count": _int(
                selected.sent_or_edited_delivery_count
            ),
            "selected_live_delivered_delivery_result_event_count": _int(
                selected.delivery_result_event_count
            ),
        }
    )
    return replace(
        report,
        counts=counts,
        selected_live_delivered_notification_intent_fingerprint=selected.event_fingerprint,
        selected_live_delivered_analysis_fingerprint=_fingerprint(selected.analysis_id),
        selected_live_delivered_notification_plan_fingerprint=_fingerprint(selected.notification_plan_id),
        selected_live_delivered_candidate_group_fingerprint=_fingerprint(selected.candidate_group_id),
        selected_live_delivered_judge_output_fingerprint=_fingerprint(selected.judge_output_id),
        selected_live_delivered_render_count=_int(selected.render_count),
        selected_live_delivered_sent_or_edited_delivery_count=_int(
            selected.sent_or_edited_delivery_count
        ),
        selected_live_delivered_delivery_result_event_count=_int(selected.delivery_result_event_count),
        selected_analysis_fingerprint=report.selected_analysis_fingerprint or _fingerprint(selected.analysis_id),
        selected_notification_plan_fingerprint=(
            report.selected_notification_plan_fingerprint or _fingerprint(selected.notification_plan_id)
        ),
        selected_judge_output_fingerprint=report.selected_judge_output_fingerprint
        or _fingerprint(selected.judge_output_id),
        selected_candidate_group_fingerprint=report.selected_candidate_group_fingerprint
        or _fingerprint(selected.candidate_group_id),
    )


def _apply_closed_live_delivered_notification_proof_readiness(
    report: PostJudgeNotificationPipelineInventoryReport,
    selected: LiveDeliveredNotificationProofTarget,
) -> PostJudgeNotificationPipelineInventoryReport:
    return replace(
        _apply_selected_live_delivered_notification_proof(report, selected),
        selected_analysis_fingerprint=report.selected_analysis_fingerprint or _fingerprint(selected.analysis_id),
        selected_notification_plan_fingerprint=(
            report.selected_notification_plan_fingerprint or _fingerprint(selected.notification_plan_id)
        ),
        selected_judge_output_fingerprint=report.selected_judge_output_fingerprint
        or _fingerprint(selected.judge_output_id),
        selected_candidate_group_fingerprint=report.selected_candidate_group_fingerprint
        or _fingerprint(selected.candidate_group_id),
        final_verdict=_safe_bucket_value(selected.verdict),
        delivery_decision=_safe_bucket_value(selected.delivery_decision),
        runtime_rollout_readiness="restricted_runtime_rollout_preflight_ready",
        analysis_created_or_present=True,
        notification_intent_created_or_present=True,
        notification_plan_created_or_present=True,
        notification_render_created_or_present=True,
        notification_delivery_result_event_created_or_present=True,
    )


def _apply_validator_readback(
    report: PostJudgeNotificationPipelineInventoryReport,
    readback: ValidatorReadback,
) -> PostJudgeNotificationPipelineInventoryReport:
    counts = dict(report.counts)
    counts.update(
        {
            "selected_policy_apply_event_count": _int(readback.policy_event_count),
            "selected_validator_passed_transition_count": _int(readback.validator_passed_transition_count),
            "selected_validator_terminal_or_retryable_count": _int(
                readback.validator_terminal_or_retryable_count
            ),
            "selected_active_analysis_count": _int(readback.active_analysis_count),
        }
    )
    if readback.validator_terminal_or_retryable_reason_code:
        counts["selected_validator_terminal_or_retryable_reason_code"] = (
            readback.validator_terminal_or_retryable_reason_code
        )
    return replace(
        report,
        counts=counts,
        selected_policy_apply_fingerprint=_fingerprint(readback.policy_event_id)
        or report.selected_policy_apply_fingerprint,
        policy_event_created_or_present=readback.policy_event_count == 1,
        analysis_created_or_present=readback.active_analysis_count > 0,
    )


def _apply_policy_readback(
    report: PostJudgeNotificationPipelineInventoryReport,
    readback: PolicyReadback,
) -> PostJudgeNotificationPipelineInventoryReport:
    counts = dict(report.counts)
    counts.update(
        {
            "selected_analysis_count": _int(readback.analysis_count),
            "selected_notification_intent_event_count": _int(readback.notification_intent_event_count),
            "selected_notification_plan_count": _int(readback.notification_plan_count),
        }
    )
    if readback.verdict is not None:
        counts["selected_analysis_verdict"] = _safe_bucket_value(readback.verdict)
    if readback.delivery_decision is not None:
        counts["selected_analysis_delivery_decision"] = _safe_bucket_value(readback.delivery_decision)
    return replace(
        report,
        counts=counts,
        selected_analysis_fingerprint=_fingerprint(readback.analysis_id) or report.selected_analysis_fingerprint,
        final_verdict=_safe_bucket_value(readback.verdict) if readback.verdict is not None else report.final_verdict,
        delivery_decision=(
            _safe_bucket_value(readback.delivery_decision)
            if readback.delivery_decision is not None
            else report.delivery_decision
        ),
        analysis_created_or_present=readback.analysis_count == 1,
        notification_intent_created_or_present=readback.notification_intent_event_count == 1,
        notification_plan_created_or_present=readback.notification_plan_count == 1,
    )


def _apply_notification_readback(
    report: PostJudgeNotificationPipelineInventoryReport,
    readback: NotificationProofReadback,
) -> PostJudgeNotificationPipelineInventoryReport:
    counts = dict(report.counts)
    counts.update(
        {
            "selected_notification_intent_event_count": _int(readback.notification_intent_event_count),
            "selected_notification_plan_count": _int(readback.notification_plan_count),
            "selected_notification_render_count": _int(readback.notification_render_count),
            "selected_send_disabled_delivery_record_count": _int(
                readback.send_disabled_delivery_record_count
            ),
            "selected_notification_delivery_result_event_count": _int(
                readback.notification_delivery_result_event_count
            ),
        }
    )
    return replace(
        report,
        counts=counts,
        notification_intent_created_or_present=readback.notification_intent_event_count == 1,
        notification_plan_created_or_present=readback.notification_plan_count == 1,
        notification_render_created_or_present=readback.notification_render_count == 1,
        send_disabled_delivery_record_created_or_present=readback.send_disabled_delivery_record_count == 1,
        notification_delivery_result_event_created_or_present=(
            readback.notification_delivery_result_event_count == 1
        ),
    )


def _counts_to_report(counts: InventoryCounts) -> dict[str, Any]:
    return {
        "judge_call_requested_v1_count": _int(counts.judge_call_requested_v1_count),
        "judge_run_status_counts": _safe_count_mapping(counts.judge_run_status_counts),
        "retryable_finish_reason_counts": [
            {
                "reason_code": _safe_reason_code_value(item.get("reason_code")),
                "count": _int(item.get("count")),
            }
            for item in counts.retryable_finish_reason_counts[:20]
        ],
        "judge_outputs_count": _int(counts.judge_outputs_count),
        "judge_output_ready_v1_count": _int(counts.judge_output_ready_v1_count),
        "analysis_policy_apply_v1_count": _int(counts.analysis_policy_apply_v1_count),
        "analyses_count": _int(counts.analyses_count),
        "analyses_by_verdict_count": _safe_count_mapping(counts.analyses_by_verdict_count),
        "analyses_by_delivery_decision_count": _safe_count_mapping(
            counts.analyses_by_delivery_decision_count
        ),
        "notification_plan_created_v1_count": _int(counts.notification_plan_created_v1_count),
        "notification_plans_by_status_count": _safe_count_mapping(counts.notification_plans_by_status_count),
        "notification_renders_count": _int(counts.notification_renders_count),
        "notification_delivery_records_by_delivery_status_count": _safe_count_mapping(
            counts.notification_delivery_records_by_delivery_status_count
        ),
        "eligible_judge_output_ready_count": _int(counts.eligible_judge_output_ready_count),
        "validator_processed_judge_output_ready_count": _int(
            counts.validator_processed_judge_output_ready_count
        ),
        "eligible_policy_apply_count": _int(counts.eligible_policy_apply_count),
        "policy_apply_already_materialized_count": _int(counts.policy_apply_already_materialized_count),
        "send_worthy_notification_intent_count": _int(counts.send_worthy_notification_intent_count),
        "send_worthy_notification_intent_already_materialized_count": _int(
            counts.send_worthy_notification_intent_already_materialized_count
        ),
        "live_delivered_notification_proof_count": _int(
            counts.live_delivered_notification_proof_count
        ),
    }


def _report(
    *,
    mode: str,
    status: str,
    reason_code: str,
    lookback_hours: int,
    sample_limit: int,
) -> PostJudgeNotificationPipelineInventoryReport:
    return PostJudgeNotificationPipelineInventoryReport(
        schema_version=SCHEMA_VERSION,
        mode=mode,
        status=status,
        reason_code=reason_code,
        lookback_hours=lookback_hours,
        sample_limit=sample_limit,
        counts=_counts_to_report(InventoryCounts()),
        selected_judge_output_ready_fingerprint=None,
        selected_policy_apply_fingerprint=None,
        selected_judge_run_fingerprint=None,
        selected_judge_output_fingerprint=None,
        selected_bundle_fingerprint=None,
        selected_candidate_group_fingerprint=None,
        selected_analysis_fingerprint=None,
        selected_notification_intent_fingerprint=None,
        selected_notification_plan_fingerprint=None,
        selected_live_delivered_notification_intent_fingerprint=None,
        selected_live_delivered_analysis_fingerprint=None,
        selected_live_delivered_notification_plan_fingerprint=None,
        selected_live_delivered_candidate_group_fingerprint=None,
        selected_live_delivered_judge_output_fingerprint=None,
        selected_live_delivered_render_count=0,
        selected_live_delivered_sent_or_edited_delivery_count=0,
        selected_live_delivered_delivery_result_event_count=0,
        nearest_send_worthy_missing_stage=None,
        final_verdict=None,
        delivery_decision=None,
        runtime_rollout_readiness=None,
        validator_attempted=False,
        policy_attempted=False,
        notifier_attempted=False,
        policy_event_created_or_present=False,
        analysis_created_or_present=False,
        notification_intent_created_or_present=False,
        notification_plan_created_or_present=False,
        notification_render_created_or_present=False,
        send_disabled_delivery_record_created_or_present=False,
        notification_delivery_result_event_created_or_present=False,
        redis_attempted=False,
        telegram_attempted=False,
        telegram_transport_attempted=False,
        openai_attempted=False,
        external_network_attempted=False,
        db_write_attempted=False,
        redactions_applied=True,
        cleanup_completed=True,
    )


def _argument_report(reason_code: str) -> PostJudgeNotificationPipelineInventoryReport:
    return _report(
        mode="unknown",
        status="blocked",
        reason_code=reason_code,
        lookback_hours=72,
        sample_limit=100,
    )


def _selected_from_row(row: Mapping[str, Any]) -> SelectedTarget:
    return SelectedTarget(
        event_id=UUID(str(row["event_id"])),
        judge_run_id=UUID(str(row["judge_run_id"])),
        judge_output_id=UUID(str(row["judge_output_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        bundle_id=UUID(str(row["bundle_id"])),
    )


def _selected_notification_intent_from_row(row: Mapping[str, Any]) -> SelectedNotificationIntentTarget:
    return SelectedNotificationIntentTarget(
        event_id=UUID(str(row["event_id"])),
        analysis_id=UUID(str(row["analysis_id"])),
        notification_plan_id=UUID(str(row["notification_plan_id"])),
        judge_output_id=UUID(str(row["judge_output_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        verdict=_safe_bucket_value(row["verdict"]),
        delivery_decision=_safe_bucket_value(row["delivery_decision"]),
    )


def _live_delivered_notification_proof_from_row(
    row: Mapping[str, Any],
) -> LiveDeliveredNotificationProofTarget:
    return LiveDeliveredNotificationProofTarget(
        event_id=UUID(str(row["event_id"])),
        analysis_id=UUID(str(row["analysis_id"])),
        notification_plan_id=UUID(str(row["notification_plan_id"])),
        judge_output_id=UUID(str(row["judge_output_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        verdict=_safe_bucket_value(row["verdict"]),
        delivery_decision=_safe_bucket_value(row["delivery_decision"]),
        render_count=_int(row["render_count"]),
        sent_or_edited_delivery_count=_int(row["sent_or_edited_delivery_count"]),
        delivery_result_event_count=_int(row["delivery_result_event_count"]),
    )


def _no_ready_target_reason(counts: Mapping[str, Any]) -> str:
    if _int(counts.get("judge_output_ready_v1_count")) == 0:
        return "no_judge_output_ready_events_in_lookback"
    if _int(counts.get("analysis_policy_apply_v1_count")) > 0 or _int(counts.get("analyses_count")) > 0:
        return "judge_output_ready_already_forwarded_or_materialized"
    if _all_ready_targets_validator_processed(counts):
        return "judge_output_ready_already_validator_processed"
    return "no_eligible_judge_output_ready_target"


def _no_policy_target_reason(counts: Mapping[str, Any]) -> str:
    if _int(counts.get("analysis_policy_apply_v1_count")) == 0:
        return "no_policy_apply_events_in_lookback"
    if _int(counts.get("policy_apply_already_materialized_count")) > 0:
        return "policy_apply_already_materialized"
    return "no_eligible_policy_apply_target"


def _no_send_worthy_target_reason(counts: Mapping[str, Any]) -> str:
    if _int(counts.get("notification_plan_created_v1_count")) == 0:
        return "notification_intent_missing"
    if _int(counts.get("send_worthy_notification_intent_count")) == 0:
        return "send_worthy_target_missing"
    return "no_eligible_send_worthy_notification_intent_target"


def _macro_no_send_worthy_target_reason(counts: Mapping[str, Any]) -> str:
    if _int(counts.get("send_worthy_notification_intent_count")) > 0:
        return "no_eligible_send_worthy_notification_intent_target"
    if _int(counts.get("eligible_policy_apply_count")) > 0:
        return "send_worthy_target_requires_policy_execute"
    if _int(counts.get("eligible_judge_output_ready_count")) > 0:
        return "send_worthy_target_requires_validator_execute"
    if _int(counts.get("analysis_policy_apply_v1_count")) > 0:
        return "send_worthy_target_requires_non_suppress_policy_output"
    if _int(counts.get("judge_output_ready_v1_count")) > 0:
        if _all_ready_targets_validator_processed(counts):
            if _int(counts.get("judge_call_requested_v1_count")) > 0:
                return "send_worthy_target_requires_live_openai_or_judge_recovery"
            return "send_worthy_target_requires_exact_source_or_live_openai"
        return "send_worthy_target_requires_validator_or_policy_recovery"
    if _int(counts.get("judge_call_requested_v1_count")) > 0:
        return "send_worthy_target_requires_live_openai_or_judge_recovery"
    return "send_worthy_target_requires_exact_source_or_live_openai"


def _nearest_send_worthy_missing_stage(counts: Mapping[str, Any]) -> str | None:
    if _int(counts.get("send_worthy_notification_intent_count")) > 0:
        return None
    if _closed_live_delivered_notification_proof_count(counts) == 1:
        return None
    if _int(counts.get("eligible_policy_apply_count")) > 0:
        return "analysis.policy.apply.v1"
    if _int(counts.get("eligible_judge_output_ready_count")) > 0:
        return "judge.output.ready.v1"
    if _int(counts.get("analysis_policy_apply_v1_count")) > 0:
        return "policy_engine_non_suppress_output"
    if _int(counts.get("judge_output_ready_v1_count")) > 0:
        if _all_ready_targets_validator_processed(counts):
            if _int(counts.get("judge_call_requested_v1_count")) > 0:
                return "judge_openai"
            return "source_or_live_openai"
        return "analysis_validator_or_policy_recovery"
    if _int(counts.get("judge_outputs_count")) > 0:
        return "judge.output.ready.v1"
    if _int(counts.get("judge_call_requested_v1_count")) > 0:
        return "judge_openai"
    return "source_or_live_openai"


def _all_ready_targets_validator_processed(counts: Mapping[str, Any]) -> bool:
    ready_count = _int(counts.get("judge_output_ready_v1_count"))
    if ready_count == 0:
        return False
    return (
        _int(counts.get("eligible_judge_output_ready_count")) == 0
        and _int(counts.get("analysis_policy_apply_v1_count")) == 0
        and _int(counts.get("analyses_count")) == 0
        and _int(counts.get("validator_processed_judge_output_ready_count")) >= ready_count
    )


def _notification_proof_exactly_once(readback: NotificationProofReadback) -> bool:
    return (
        readback.notification_intent_event_count == 1
        and readback.notification_plan_count == 1
        and readback.notification_render_count == 1
        and readback.send_disabled_delivery_record_count == 1
        and readback.notification_delivery_result_event_count == 1
    )


def _notification_proof_ambiguous(readback: NotificationProofReadback) -> bool:
    return (
        readback.notification_intent_event_count > 1
        or readback.notification_plan_count > 1
        or readback.notification_render_count > 1
        or readback.send_disabled_delivery_record_count > 1
        or readback.notification_delivery_result_event_count > 1
    )


def _closed_live_delivered_notification_proof_count(counts: Mapping[str, Any]) -> int:
    return _int(counts.get("live_delivered_notification_proof_count"))


def _closed_live_delivered_notification_proof_report(
    report: PostJudgeNotificationPipelineInventoryReport,
    selected: LiveDeliveredNotificationProofTarget | None,
    *,
    expected_notification_intent_fingerprint: str | None,
) -> PostJudgeNotificationPipelineInventoryReport | None:
    proof_count = _closed_live_delivered_notification_proof_count(report.counts)
    if proof_count == 0:
        return None
    if proof_count > 1:
        return replace(report, status="blocked", reason_code="sent_or_edited_notification_proof_ambiguous")
    if selected is None:
        return replace(report, status="blocked", reason_code="sent_or_edited_notification_proof_missing")
    if expected_notification_intent_fingerprint != selected.event_fingerprint:
        return replace(report, status="blocked", reason_code="notification_intent_fingerprint_mismatch")
    return replace(
        _apply_closed_live_delivered_notification_proof_readiness(report, selected),
        status="pass",
        reason_code="sent_or_edited_notification_proof_already_closed",
    )


def _telegram_transport_attempted(components: PostJudgeNotificationPipelineInventoryComponents) -> bool:
    if components.telegram_transport_attempted is None:
        return False
    try:
        return bool(components.telegram_transport_attempted())
    except Exception:
        return True


def _read(values: Mapping[str, str], key: str, default: str = "") -> str:
    return str(values.get(key, default)).strip()


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _bool_value(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_count_mapping(values: Mapping[str, int]) -> dict[str, int]:
    return {_safe_bucket_value(key): _int(value) for key, value in values.items()}


def _safe_bucket_value(value: Any) -> str:
    text = str(value if value is not None else "missing").strip()
    return text if SAFE_REASON_RE.fullmatch(text) else "other"


def _safe_reason_code_value(value: Any) -> str:
    return _safe_bucket_value(value)


def _int(value: Any) -> int:
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _bounded_cli_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _fingerprint(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]


def _compact_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run_cli(argv))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "JUDGE_OUTPUT_SELECTION_CONFIRM_TOKEN",
    "MACRO_CONFIRM_TOKEN",
    "NOTIFICATION_INTENT_SELECTION_CONFIRM_TOKEN",
    "NOTIFIER_CONFIRM_TOKEN",
    "POLICY_APPLY_SELECTION_CONFIRM_TOKEN",
    "POLICY_CONFIRM_TOKEN",
    "READY_EVENT_TYPE",
    "POLICY_EVENT_TYPE",
    "DELIVERY_RESULT_EVENT_TYPE",
    "FailClosedTelegramTransport",
    "LiveDeliveredNotificationProofTarget",
    "PostJudgeNotificationPipelineInventoryComponents",
    "PostJudgeNotificationPipelineInventoryConfigError",
    "PostJudgeNotificationPipelineInventoryReport",
    "PostJudgeNotificationPipelineInventoryRequest",
    "RuntimeConfigBundle",
    "SCHEMA_VERSION",
    "SIGNED_BIGINT_TEXT_SQL_RE",
    "SelectedNotificationIntentTarget",
    "SelectedTarget",
    "VALIDATOR_CONFIRM_TOKEN",
    "ValidatorReadback",
    "NotificationProofReadback",
    "PolicyReadback",
    "_LIVE_DELIVERED_NOTIFICATION_PROOF_SQL",
    "_SEND_WORTHY_NOTIFICATION_INTENT_SQL",
    "_fingerprint",
    "build_parser",
    "load_runtime_config",
    "main",
    "run_cli",
    "run_post_judge_notification_pipeline_inventory",
]
