from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, AsyncIterator, Protocol
from uuid import UUID

import sqlalchemy as sa

from ..analysis_validator.config import AnalysisValidatorConfig
from ..analysis_validator.repositories import AnalysisValidatorRepository
from ..analysis_validator.service import AnalysisValidatorService
from ..judge_openai.config import JudgeOpenAIConfig, JudgeOpenAIConfigurationError
from ..judge_openai.models import BundleJudgeContext, JudgeCallJob, JudgeRunRecord
from ..judge_openai.openai_client import OpenAIJudgeClient
from ..judge_openai.repositories import JudgeOpenAIRepository
from ..judge_openai.service import JudgeOpenAIService
from ..notifier_telegram.config import NotifierTelegramConfig
from ..notifier_telegram.repositories import NotifierTelegramRepository
from ..notifier_telegram.service import NotifierTelegramService
from ..notifier_telegram.transport import TelegramTransportTerminalError
from ..policy_engine.config import PolicyEngineConfig
from ..policy_engine.repositories import PolicyEngineRepository
from ..policy_engine.service import PolicyEngineService


SCHEMA_VERSION = "exact_target_live_openai_canary_report_v1"
READY_EVENT_TYPE = "judge.output.ready.v1"
POLICY_EVENT_TYPE = "analysis.policy.apply.v1"


RUNTIME_VALUE_KEYS = {
    "APP_ENV",
    "DATABASE_URL",
    "REDIS_URL",
    "OPENAI_PROJECT",
    "JUDGE_OPENAI_QUEUE_NAME",
    "JUDGE_OPENAI_CONSUMER_GROUP",
    "JUDGE_OPENAI_CONSUMER_NAME",
    "JUDGE_OPENAI_BATCH_SIZE",
    "JUDGE_OPENAI_BLOCK_MS",
    "JUDGE_OPENAI_REQUEST_TIMEOUT_SEC",
    "JUDGE_MAX_OUTPUT_TOKENS",
    "ENABLE_PROMPT_GUARD_PREFLIGHT",
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
    "NOTIFY_RENDER_PROFILE_HIGH",
    "NOTIFY_RENDER_PROFILE_NORMAL",
    "NOTIFIER_TELEGRAM_QUEUE_NAME",
    "NOTIFIER_TELEGRAM_CONSUMER_GROUP",
    "NOTIFIER_TELEGRAM_CONSUMER_NAME",
    "NOTIFIER_TELEGRAM_BATCH_SIZE",
    "NOTIFIER_TELEGRAM_BLOCK_MS",
    "NOTIFIER_TELEGRAM_DRY_RUN",
    "NOTIFIER_TELEGRAM_ALLOW_EDITS",
    "ENABLE_DIGEST_RUNTIME",
    "NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS",
    "NOTIFIER_TELEGRAM_EDIT_WINDOW_MINUTES",
    "TELEGRAM_API_BASE_URL",
    "NOTIFIER_TELEGRAM_REQUEST_TIMEOUT_SEC",
    "LOG_LEVEL",
}
RUNTIME_FILE_KEYS = {"DATABASE_URL_FILE", "REDIS_URL_FILE", "OPENAI_API_KEY_FILE"}
REJECTED_RUNTIME_KEYS = {"OPENAI_API_KEY"}
RUNTIME_ENV_KEYS = RUNTIME_VALUE_KEYS | RUNTIME_FILE_KEYS | REJECTED_RUNTIME_KEYS


class ExactTargetCanaryConfigError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class ExactTargetCanaryReport:
    schema_version: str
    mode: str
    status: str
    reason_code: str
    target_event_fingerprint: str | None
    target_run_fingerprint: str | None
    preflight_passed: bool
    openai_call_attempted: bool
    openai_request_count: int
    judge_status: str | None
    judge_output_created: bool
    judge_output_ready_event_created: bool
    refusal_detected: bool
    validator_attempted: bool
    validator_forwarded_policy: bool
    policy_attempted: bool
    analysis_created: bool
    final_verdict: str | None
    delivery_decision: str | None
    notification_intent_created: bool
    notifier_attempted: bool
    notification_plan_created: bool
    notification_render_created: bool
    send_disabled_delivery_record_created: bool
    telegram_transport_attempted: bool
    redis_attempted: bool
    cleanup_completed: bool
    redactions_applied: bool
    duration_ms: int


@dataclass(slots=True, frozen=True)
class ExactTargetCanaryRequest:
    mode: str
    trigger_event_id: UUID


@dataclass(slots=True, frozen=True)
class RuntimeConfigBundle:
    database_url: str
    values: Mapping[str, str]


@dataclass(slots=True, frozen=True)
class ServiceConfigBundle:
    judge: JudgeOpenAIConfig
    validator: AnalysisValidatorConfig
    policy: PolicyEngineConfig
    notifier: NotifierTelegramConfig


@dataclass(slots=True, frozen=True)
class ExactTargetEvent:
    event_id: UUID
    event_type: str
    payload_json: dict[str, Any]


@dataclass(slots=True, frozen=True)
class ExactTargetPreflight:
    event: ExactTargetEvent | None
    job: JudgeCallJob | None
    judge_run: JudgeRunRecord | None
    bundle: BundleJudgeContext | None
    judge_output_count: int = 0
    ready_event_count: int = 0
    policy_event_count: int = 0
    analysis_count: int = 0
    notification_intent_count: int = 0
    notification_plan_count: int = 0
    notification_render_count: int = 0
    notification_delivery_count: int = 0
    reason_code: str | None = None

    @property
    def passed(self) -> bool:
        return self.reason_code is None


@dataclass(slots=True, frozen=True)
class JudgeReadback:
    judge_status: str | None
    finish_reason: str | None
    refusal_detected: bool
    judge_output_count: int
    judge_output_id: UUID | None
    ready_event_count: int
    ready_event_id: UUID | None


@dataclass(slots=True, frozen=True)
class AnalysisReadback:
    analysis_count: int
    analysis_id: UUID | None
    final_verdict: str | None
    delivery_decision: str | None


@dataclass(slots=True, frozen=True)
class NotificationReadback:
    notification_plan_count: int
    notification_render_count: int
    send_disabled_delivery_record_count: int
    delivery_result_event_count: int


class ExactTargetCanaryRepositoryProtocol(Protocol):
    async def load_preflight(self, trigger_event_id: UUID) -> ExactTargetPreflight: ...
    async def load_judge_readback(self, *, judge_run_id: UUID) -> JudgeReadback: ...
    async def load_policy_event_ids(self, *, judge_run_id: UUID, judge_output_id: UUID) -> list[UUID]: ...
    async def load_analysis_readback(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> AnalysisReadback: ...
    async def load_notification_intent_event_ids(self, *, analysis_id: UUID) -> list[UUID]: ...
    async def load_notification_readback(
        self,
        *,
        analysis_id: UUID,
        notification_plan_id: UUID | None,
    ) -> NotificationReadback: ...


@dataclass(slots=True)
class ExactTargetCanaryComponents:
    canary_repository: ExactTargetCanaryRepositoryProtocol
    judge_repository: Any
    validator_repository: Any
    policy_repository: Any
    notifier_repository: Any


class CountingOpenAIJudgeClient:
    def __init__(self, wrapped: Any) -> None:
        self._wrapped = wrapped
        self.request_count = 0

    async def create_structured_response(self, **kwargs: Any) -> Any:
        self.request_count += 1
        return await self._wrapped.create_structured_response(**kwargs)


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


class SqlExactTargetCanaryRepository:
    def __init__(self, session: Any) -> None:
        self._session = session
        self._judge_repository = JudgeOpenAIRepository(session)

    async def load_preflight(self, trigger_event_id: UUID) -> ExactTargetPreflight:
        event = await self._load_target_event(trigger_event_id)
        if event is None:
            return ExactTargetPreflight(event=None, job=None, judge_run=None, bundle=None, reason_code="target_event_missing")
        if event.event_type != "judge.call.requested.v1":
            return ExactTargetPreflight(event=event, job=None, judge_run=None, bundle=None, reason_code="wrong_event_type")

        job = _job_from_event(event)
        if job is None:
            return ExactTargetPreflight(event=event, job=None, judge_run=None, bundle=None, reason_code="invalid_event_payload")

        judge_run = await self._judge_repository.load_judge_run(job.judge_run_id)
        if judge_run is None:
            return ExactTargetPreflight(event=event, job=job, judge_run=None, bundle=None, reason_code="judge_run_missing")

        bundle = await self._judge_repository.load_bundle_context(judge_run.bundle_id)
        counts = await self._downstream_counts(judge_run.judge_run_id)
        snapshot = ExactTargetPreflight(
            event=event,
            job=job,
            judge_run=judge_run,
            bundle=bundle,
            **counts,
        )
        return _validate_preflight_snapshot(snapshot)

    async def load_judge_readback(self, *, judge_run_id: UUID) -> JudgeReadback:
        run = await self._judge_repository.load_judge_run(judge_run_id)
        outputs = await self._rows(
            """
            SELECT judge_output_id
            FROM judge_outputs
            WHERE judge_run_id = CAST(:judge_run_id AS uuid)
            ORDER BY created_at ASC, judge_output_id ASC
            """,
            {"judge_run_id": str(judge_run_id)},
        )
        ready_events = await self._event_rows_for_run(
            event_type=READY_EVENT_TYPE,
            judge_run_id=judge_run_id,
            judge_output_id=UUID(str(outputs[0]["judge_output_id"])) if len(outputs) == 1 else None,
        )
        return JudgeReadback(
            judge_status=run.status if run else None,
            finish_reason=await self._judge_run_finish_reason(judge_run_id),
            refusal_detected=await self._judge_run_refusal_detected(judge_run_id),
            judge_output_count=len(outputs),
            judge_output_id=UUID(str(outputs[0]["judge_output_id"])) if len(outputs) == 1 else None,
            ready_event_count=len(ready_events),
            ready_event_id=UUID(str(ready_events[0]["event_id"])) if len(ready_events) == 1 else None,
        )

    async def load_policy_event_ids(self, *, judge_run_id: UUID, judge_output_id: UUID) -> list[UUID]:
        rows = await self._event_rows_for_run(
            event_type=POLICY_EVENT_TYPE,
            judge_run_id=judge_run_id,
            judge_output_id=judge_output_id,
        )
        return [UUID(str(row["event_id"])) for row in rows]

    async def load_analysis_readback(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> AnalysisReadback:
        rows = await self._rows(
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
        return AnalysisReadback(
            analysis_count=len(rows),
            analysis_id=UUID(str(rows[0]["analysis_id"])) if len(rows) == 1 else None,
            final_verdict=str(rows[0]["verdict"]) if len(rows) == 1 else None,
            delivery_decision=str(rows[0]["delivery_decision"]) if len(rows) == 1 else None,
        )

    async def load_notification_intent_event_ids(self, *, analysis_id: UUID) -> list[UUID]:
        rows = await self._rows(
            """
            SELECT event_id
            FROM event_outbox
            WHERE event_type = 'notification.plan.created.v1'
              AND aggregate_type = 'analysis'
              AND aggregate_id = CAST(:analysis_id AS uuid)
              AND payload_json->>'analysis_id' = :analysis_id_text
            ORDER BY created_at ASC, event_id ASC
            """,
            {"analysis_id": str(analysis_id), "analysis_id_text": str(analysis_id)},
        )
        return [UUID(str(row["event_id"])) for row in rows]

    async def load_notification_readback(
        self,
        *,
        analysis_id: UUID,
        notification_plan_id: UUID | None,
    ) -> NotificationReadback:
        plan_rows = await self._rows(
            """
            SELECT notification_plan_id
            FROM notification_plans
            WHERE analysis_id = CAST(:analysis_id AS uuid)
              AND (
                    CAST(:notification_plan_id AS uuid) IS NULL
                    OR notification_plan_id = CAST(:notification_plan_id AS uuid)
                  )
            ORDER BY created_at ASC, notification_plan_id ASC
            """,
            {
                "analysis_id": str(analysis_id),
                "notification_plan_id": str(notification_plan_id) if notification_plan_id else None,
            },
        )
        plan_ids = [UUID(str(row["notification_plan_id"])) for row in plan_rows]
        render_count = 0
        send_disabled_count = 0
        delivery_result_count = 0
        for plan_id in plan_ids:
            render_count += await self._count(
                """
                SELECT count(*)
                FROM notification_renders
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """,
                {"notification_plan_id": str(plan_id)},
            )
            send_disabled_count += await self._count(
                """
                SELECT count(*)
                FROM notification_delivery_records
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND delivery_status::text = 'suppressed'
                  AND telegram_response_json->>'send_disabled' = 'true'
                  AND telegram_response_json->>'dry_run' = 'true'
                """,
                {"notification_plan_id": str(plan_id)},
            )
            delivery_result_count += await self._count(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = 'notification.delivery.result.v1'
                  AND aggregate_type = 'notification_plan'
                  AND aggregate_id = CAST(:notification_plan_id AS uuid)
                """,
                {"notification_plan_id": str(plan_id)},
            )
        return NotificationReadback(
            notification_plan_count=len(plan_rows),
            notification_render_count=render_count,
            send_disabled_delivery_record_count=send_disabled_count,
            delivery_result_event_count=delivery_result_count,
        )

    async def _load_target_event(self, trigger_event_id: UUID) -> ExactTargetEvent | None:
        rows = await self._rows(
            """
            SELECT event_id, event_type, payload_json
            FROM event_outbox
            WHERE event_id = CAST(:event_id AS uuid)
            """,
            {"event_id": str(trigger_event_id)},
        )
        if not rows:
            return None
        row = rows[0]
        payload = _json_loads(row["payload_json"])
        return ExactTargetEvent(
            event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            payload_json=payload if isinstance(payload, dict) else {},
        )

    async def _downstream_counts(self, judge_run_id: UUID) -> dict[str, int]:
        judge_output_count = await self._count(
            "SELECT count(*) FROM judge_outputs WHERE judge_run_id = CAST(:judge_run_id AS uuid)",
            {"judge_run_id": str(judge_run_id)},
        )
        ready_event_count = len(await self._event_rows_for_run(event_type=READY_EVENT_TYPE, judge_run_id=judge_run_id))
        policy_event_count = len(await self._event_rows_for_run(event_type=POLICY_EVENT_TYPE, judge_run_id=judge_run_id))
        analysis_count = await self._count(
            """
            SELECT count(*)
            FROM analyses a
            JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
            WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
            """,
            {"judge_run_id": str(judge_run_id)},
        )
        notification_intent_count = await self._count(
            """
            SELECT count(*)
            FROM event_outbox eo
            JOIN analyses a ON a.analysis_id = eo.aggregate_id
            JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
            WHERE eo.event_type = 'notification.plan.created.v1'
              AND jo.judge_run_id = CAST(:judge_run_id AS uuid)
            """,
            {"judge_run_id": str(judge_run_id)},
        )
        notification_plan_count = await self._count(
            """
            SELECT count(*)
            FROM notification_plans np
            JOIN analyses a ON a.analysis_id = np.analysis_id
            JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
            WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
            """,
            {"judge_run_id": str(judge_run_id)},
        )
        notification_render_count = await self._count(
            """
            SELECT count(*)
            FROM notification_renders nr
            JOIN notification_plans np ON np.notification_plan_id = nr.notification_plan_id
            JOIN analyses a ON a.analysis_id = np.analysis_id
            JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
            WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
            """,
            {"judge_run_id": str(judge_run_id)},
        )
        notification_delivery_count = await self._count(
            """
            SELECT count(*)
            FROM notification_delivery_records ndr
            JOIN notification_plans np ON np.notification_plan_id = ndr.notification_plan_id
            JOIN analyses a ON a.analysis_id = np.analysis_id
            JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
            WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
            """,
            {"judge_run_id": str(judge_run_id)},
        )
        return {
            "judge_output_count": judge_output_count,
            "ready_event_count": ready_event_count,
            "policy_event_count": policy_event_count,
            "analysis_count": analysis_count,
            "notification_intent_count": notification_intent_count,
            "notification_plan_count": notification_plan_count,
            "notification_render_count": notification_render_count,
            "notification_delivery_count": notification_delivery_count,
        }

    async def _event_rows_for_run(
        self,
        *,
        event_type: str,
        judge_run_id: UUID,
        judge_output_id: UUID | None = None,
    ) -> list[Mapping[str, Any]]:
        judge_output_filter = ""
        params: dict[str, Any] = {
            "event_type": event_type,
            "judge_run_id_uuid": str(judge_run_id),
            "judge_run_id_text": str(judge_run_id),
        }
        if judge_output_id is not None:
            judge_output_filter = "AND payload_json->>'judge_output_id' = :judge_output_id_text"
            params["judge_output_id_text"] = str(judge_output_id)
        return await self._rows(
            f"""
            SELECT event_id, payload_json
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_type = 'judge_run'
              AND aggregate_id = CAST(:judge_run_id_uuid AS uuid)
              AND payload_json->>'judge_run_id' = :judge_run_id_text
              {judge_output_filter}
            ORDER BY created_at ASC, event_id ASC
            """,
            params,
        )

    async def _judge_run_finish_reason(self, judge_run_id: UUID) -> str | None:
        rows = await self._rows(
            "SELECT finish_reason FROM judge_runs WHERE judge_run_id = CAST(:judge_run_id AS uuid)",
            {"judge_run_id": str(judge_run_id)},
        )
        return str(rows[0]["finish_reason"]) if rows and rows[0]["finish_reason"] is not None else None

    async def _judge_run_refusal_detected(self, judge_run_id: UUID) -> bool:
        rows = await self._rows(
            "SELECT refusal_detected FROM judge_runs WHERE judge_run_id = CAST(:judge_run_id AS uuid)",
            {"judge_run_id": str(judge_run_id)},
        )
        return bool(rows and rows[0]["refusal_detected"])

    async def _count(self, query: str, params: Mapping[str, Any]) -> int:
        result = await self._session.execute(sa.text(query), dict(params))
        return int(result.scalar_one())

    async def _rows(self, query: str, params: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        result = await self._session.execute(sa.text(query), dict(params))
        return list(result.mappings().all())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="exact-target-live-openai-canary")
    parser.add_argument("--mode", choices=["plan", "execute"])
    parser.add_argument("--trigger-event-id", action="append", default=[])
    parser.add_argument("--env-file")
    parser.add_argument("--confirm", choices=["live-openai"], default=None)
    return parser


async def run_cli(
    argv: Sequence[str] | None = None,
    *,
    emit_json: Callable[[str], None] = print,
    session_components_builder: Callable[[RuntimeConfigBundle], AsyncIterator[ExactTargetCanaryComponents]] | None = None,
    openai_client_builder: Callable[[JudgeOpenAIConfig], Any] | None = None,
) -> int:
    started = time.monotonic()
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    validation_error = _cli_request_error(args)
    trigger_event_id = _uuid_or_none(args.trigger_event_id[0]) if len(args.trigger_event_id) == 1 else None
    if validation_error is not None:
        report = _report(
            mode=str(args.mode or "unknown"),
            status="blocked",
            reason_code=validation_error,
            started_monotonic=started,
            target_event_id=trigger_event_id,
        )
        emit_json(_compact_json(asdict(report)))
        return 2

    assert trigger_event_id is not None
    if args.mode == "execute" and args.confirm != "live-openai":
        report = _report(
            mode=args.mode,
            status="blocked",
            reason_code="live_openai_confirm_missing",
            started_monotonic=started,
            target_event_id=trigger_event_id,
        )
        emit_json(_compact_json(asdict(report)))
        return 2
    if args.mode == "plan" and args.confirm is not None:
        report = _report(
            mode=args.mode,
            status="blocked",
            reason_code="live_openai_confirm_not_allowed_for_plan",
            started_monotonic=started,
            target_event_id=trigger_event_id,
        )
        emit_json(_compact_json(asdict(report)))
        return 2

    try:
        runtime = load_runtime_config(args.env_file or "", require_redis=args.mode == "execute")
    except ExactTargetCanaryConfigError as exc:
        report = _report(
            mode=args.mode,
            status="blocked",
            reason_code=_safe_reason_code(exc),
            started_monotonic=started,
            target_event_id=trigger_event_id,
        )
        emit_json(_compact_json(asdict(report)))
        return 2

    builder = session_components_builder or sql_session_components
    async with builder(runtime) as components:
        report = await run_exact_target_canary(
            ExactTargetCanaryRequest(mode=args.mode, trigger_event_id=trigger_event_id),
            runtime=runtime,
            components=components,
            started_monotonic=started,
            openai_client_builder=openai_client_builder,
        )
    emit_json(_compact_json(asdict(report)))
    return 0 if report.status == "pass" else 2


async def run_exact_target_canary(
    request: ExactTargetCanaryRequest,
    *,
    runtime: RuntimeConfigBundle,
    components: ExactTargetCanaryComponents,
    started_monotonic: float | None = None,
    openai_client_builder: Callable[[JudgeOpenAIConfig], Any] | None = None,
) -> ExactTargetCanaryReport:
    started = time.monotonic() if started_monotonic is None else started_monotonic
    report = _report(
        mode=request.mode,
        status="failed",
        reason_code="unhandled_error",
        started_monotonic=started,
        target_event_id=request.trigger_event_id,
    )
    try:
        preflight = await components.canary_repository.load_preflight(request.trigger_event_id)
        report = _apply_preflight(report, preflight)
        if not preflight.passed:
            return replace(report, status="blocked", reason_code=preflight.reason_code or "preflight_blocked")
        if request.mode == "plan":
            return replace(report, status="pass", reason_code="plan_ready")

        assert preflight.judge_run is not None
        service_configs = load_service_configs(runtime.values)
        openai_client = CountingOpenAIJudgeClient(
            (openai_client_builder or _default_openai_client)(service_configs.judge)
        )
        judge_service = JudgeOpenAIService(
            service_configs.judge,
            repository=components.judge_repository,
            openai_client=openai_client,
        )
        await judge_service.handle_trigger_event(request.trigger_event_id)
        report = replace(
            report,
            openai_request_count=min(openai_client.request_count, 2),
            openai_call_attempted=openai_client.request_count > 0,
        )
        if openai_client.request_count > 2:
            return replace(report, status="failed", reason_code="openai_request_count_exceeded")

        judge_readback = await components.canary_repository.load_judge_readback(
            judge_run_id=preflight.judge_run.judge_run_id,
        )
        report = _apply_judge_readback(report, judge_readback)
        judge_block = _judge_readback_block_reason(judge_readback)
        if judge_block is not None:
            status = "failed" if judge_block.startswith("judge_") else "failed"
            return replace(report, status=status, reason_code=judge_block)
        if judge_readback.judge_output_id is None:
            return replace(report, status="failed", reason_code="judge_output_missing")

        validator = AnalysisValidatorService(
            service_configs.validator,
            repository=components.validator_repository,
        )
        await validator.handle_trigger_event(judge_readback.ready_event_id)
        policy_event_ids = await components.canary_repository.load_policy_event_ids(
            judge_run_id=preflight.judge_run.judge_run_id,
            judge_output_id=judge_readback.judge_output_id,
        )
        report = replace(
            report,
            validator_attempted=True,
            validator_forwarded_policy=len(policy_event_ids) == 1,
        )
        if len(policy_event_ids) > 1:
            return replace(report, status="failed", reason_code="multiple_policy_events")
        if not policy_event_ids:
            if judge_readback.refusal_detected:
                return replace(report, status="pass", reason_code="validator_refusal_terminal")
            return replace(report, status="failed", reason_code="validator_rejected")

        policy = PolicyEngineService(
            service_configs.policy,
            repository=components.policy_repository,
        )
        await policy.handle_trigger_event(policy_event_ids[0])
        analysis = await components.canary_repository.load_analysis_readback(
            judge_output_id=judge_readback.judge_output_id,
            policy_version=service_configs.policy.policy_version,
            delivery_policy_version=service_configs.policy.delivery_policy_version,
        )
        report = _apply_analysis_readback(replace(report, policy_attempted=True), analysis)
        if analysis.analysis_count > 1:
            return replace(report, status="failed", reason_code="multiple_analyses")
        if analysis.analysis_id is None:
            return replace(report, status="failed", reason_code="analysis_missing")

        notification_event_ids = await components.canary_repository.load_notification_intent_event_ids(
            analysis_id=analysis.analysis_id,
        )
        report = replace(report, notification_intent_created=len(notification_event_ids) == 1)
        if len(notification_event_ids) > 1:
            return replace(report, status="failed", reason_code="multiple_notification_intents")
        if not notification_event_ids:
            if analysis.delivery_decision == "suppress":
                return replace(report, status="pass", reason_code="policy_suppressed")
            return replace(report, status="failed", reason_code="notification_intent_missing")

        notifier_transport = FailClosedTelegramTransport()
        safe_notifier_config = replace(
            service_configs.notifier,
            enable_notification_send=False,
            dry_run=True,
            allow_edits=False,
            telegram_bot_token="",
        )
        notifier = NotifierTelegramService(
            safe_notifier_config,
            repository=components.notifier_repository,
            telegram_client=notifier_transport,
        )
        try:
            await notifier.handle_trigger_event(notification_event_ids[0])
        except TelegramTransportTerminalError:
            return replace(
                report,
                notifier_attempted=True,
                telegram_transport_attempted=notifier_transport.attempted,
                status="failed",
                reason_code="telegram_transport_attempted",
            )
        notification_plan_id = await _notification_plan_id_from_intent(
            components.notifier_repository,
            notification_event_ids[0],
        )
        notification = await components.canary_repository.load_notification_readback(
            analysis_id=analysis.analysis_id,
            notification_plan_id=notification_plan_id,
        )
        report = _apply_notification_readback(
            replace(
                report,
                notifier_attempted=True,
                telegram_transport_attempted=notifier_transport.attempted,
            ),
            notification,
        )
        if notifier_transport.attempted:
            return replace(report, status="failed", reason_code="telegram_transport_attempted")
        if not report.send_disabled_delivery_record_created:
            return replace(report, status="failed", reason_code="send_disabled_delivery_missing")
        return replace(report, status="pass", reason_code="notification_send_disabled_suppressed")
    except ExactTargetCanaryConfigError as exc:
        return replace(report, status="blocked", reason_code=_safe_reason_code(exc))
    except Exception:
        return replace(report, status="failed", reason_code="unhandled_error")


def load_runtime_config(env_file: str, *, require_redis: bool) -> RuntimeConfigBundle:
    del require_redis
    values = _read_runtime_env_file(env_file)
    if values.get("OPENAI_API_KEY", "").strip():
        raise ExactTargetCanaryConfigError("direct_openai_api_key_rejected")
    values = dict(values)
    database_url = _resolve_file_indirection(
        values,
        value_key="DATABASE_URL",
        file_key="DATABASE_URL_FILE",
        missing_reason_code="database_url_missing",
        file_missing_reason_code="database_url_file_missing",
        file_empty_reason_code="database_url_file_empty",
    )
    return RuntimeConfigBundle(database_url=database_url, values=values)


def load_service_configs(values: Mapping[str, str]) -> ServiceConfigBundle:
    resolved_values = dict(values)
    _resolve_file_indirection(
        resolved_values,
        value_key="REDIS_URL",
        file_key="REDIS_URL_FILE",
        missing_reason_code="redis_url_missing",
        file_missing_reason_code="redis_url_file_missing",
        file_empty_reason_code="redis_url_file_empty",
    )
    try:
        judge = JudgeOpenAIConfig.from_env(resolved_values)
    except (JudgeOpenAIConfigurationError, ValueError, TypeError):
        raise ExactTargetCanaryConfigError("judge_config_invalid") from None
    validator = _validator_config(resolved_values)
    policy = _policy_config(resolved_values)
    notifier = _notifier_config(resolved_values)
    return ServiceConfigBundle(judge=judge, validator=validator, policy=policy, notifier=notifier)


@asynccontextmanager
async def sql_session_components(runtime: RuntimeConfigBundle) -> AsyncIterator[ExactTargetCanaryComponents]:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with session_factory() as session:
            yield ExactTargetCanaryComponents(
                canary_repository=SqlExactTargetCanaryRepository(session),
                judge_repository=JudgeOpenAIRepository(session),
                validator_repository=AnalysisValidatorRepository(session),
                policy_repository=PolicyEngineRepository(session),
                notifier_repository=NotifierTelegramRepository(session),
            )
    finally:
        await engine.dispose()


def _default_openai_client(config: JudgeOpenAIConfig) -> OpenAIJudgeClient:
    return OpenAIJudgeClient(
        api_key=config.openai_api_key,
        project=config.openai_project,
        timeout_sec=config.request_timeout_sec,
    )


async def _notification_plan_id_from_intent(notifier_repository: Any, event_id: UUID) -> UUID | None:
    intent = await notifier_repository.load_intent_job(event_id)
    return intent.notification_plan_id if intent is not None else None


def _validate_preflight_snapshot(snapshot: ExactTargetPreflight) -> ExactTargetPreflight:
    if snapshot.event is None or snapshot.job is None:
        return replace(snapshot, reason_code=snapshot.reason_code or "invalid_event_payload")
    if snapshot.judge_run is None:
        return replace(snapshot, reason_code="judge_run_missing")
    if snapshot.judge_output_count or snapshot.ready_event_count:
        return replace(snapshot, reason_code="target_already_consumed")
    if (
        snapshot.policy_event_count
        or snapshot.analysis_count
        or snapshot.notification_intent_count
        or snapshot.notification_plan_count
        or snapshot.notification_render_count
        or snapshot.notification_delivery_count
    ):
        return replace(snapshot, reason_code="downstream_already_completed")
    if snapshot.judge_run.status != "pending":
        return replace(snapshot, reason_code="judge_run_not_pending")
    if snapshot.job.bundle_id != snapshot.judge_run.bundle_id:
        return replace(snapshot, reason_code="event_run_bundle_mismatch")
    if _job_conflicts_with_run(snapshot.job, snapshot.judge_run):
        return replace(snapshot, reason_code="event_run_config_conflict")
    if snapshot.bundle is None:
        return replace(snapshot, reason_code="bundle_missing")
    if not snapshot.bundle.is_structurally_usable():
        return replace(snapshot, reason_code="bundle_unusable")
    return snapshot


def _job_from_event(event: ExactTargetEvent) -> JudgeCallJob | None:
    payload = event.payload_json
    judge_run_id = _uuid_or_none(payload.get("judge_run_id"))
    bundle_id = _uuid_or_none(payload.get("bundle_id"))
    model = _nonempty_string(payload.get("model"))
    reasoning_effort = _nonempty_string(payload.get("reasoning_effort"))
    prompt_version = _nonempty_string(payload.get("prompt_version"))
    prompt_cache_key = _optional_string(payload.get("prompt_cache_key"))
    if None in {judge_run_id, bundle_id, model, reasoning_effort, prompt_version}:
        return None
    return JudgeCallJob(
        trigger_event_id=event.event_id,
        event_type=event.event_type,
        judge_run_id=judge_run_id,  # type: ignore[arg-type]
        bundle_id=bundle_id,  # type: ignore[arg-type]
        model=model,  # type: ignore[arg-type]
        reasoning_effort=reasoning_effort,  # type: ignore[arg-type]
        prompt_version=prompt_version,  # type: ignore[arg-type]
        prompt_cache_key=prompt_cache_key,
    )


def _job_conflicts_with_run(job: JudgeCallJob, judge_run: JudgeRunRecord) -> bool:
    return bool(
        job.model != judge_run.model
        or job.reasoning_effort != judge_run.reasoning_effort
        or job.prompt_version != judge_run.prompt_version
        or (
            job.prompt_cache_key is not None
            and judge_run.prompt_cache_key is not None
            and job.prompt_cache_key != judge_run.prompt_cache_key
        )
    )


def _judge_readback_block_reason(readback: JudgeReadback) -> str | None:
    if readback.judge_status == "succeeded":
        if readback.judge_output_count != 1:
            return "judge_output_cardinality_invalid"
        if readback.ready_event_count != 1:
            return "judge_output_ready_event_cardinality_invalid"
        return None
    if readback.judge_status == "failed_retryable":
        if readback.judge_output_count or readback.ready_event_count:
            return "failed_judge_created_downstream_rows"
        return "judge_failed_retryable"
    if readback.judge_status == "failed_terminal":
        if readback.judge_output_count or readback.ready_event_count:
            return "failed_judge_created_downstream_rows"
        return "judge_failed_terminal"
    return "judge_status_unexpected"


def _apply_preflight(report: ExactTargetCanaryReport, preflight: ExactTargetPreflight) -> ExactTargetCanaryReport:
    return replace(
        report,
        target_run_fingerprint=_fingerprint(preflight.judge_run.judge_run_id) if preflight.judge_run else None,
        preflight_passed=preflight.passed,
        judge_status=preflight.judge_run.status if preflight.judge_run else None,
    )


def _apply_judge_readback(report: ExactTargetCanaryReport, readback: JudgeReadback) -> ExactTargetCanaryReport:
    return replace(
        report,
        judge_status=readback.judge_status,
        judge_output_created=readback.judge_output_count == 1,
        judge_output_ready_event_created=readback.ready_event_count == 1,
        refusal_detected=readback.refusal_detected,
    )


def _apply_analysis_readback(report: ExactTargetCanaryReport, readback: AnalysisReadback) -> ExactTargetCanaryReport:
    return replace(
        report,
        analysis_created=readback.analysis_count == 1,
        final_verdict=readback.final_verdict,
        delivery_decision=readback.delivery_decision,
    )


def _apply_notification_readback(
    report: ExactTargetCanaryReport,
    readback: NotificationReadback,
) -> ExactTargetCanaryReport:
    return replace(
        report,
        notification_plan_created=readback.notification_plan_count == 1,
        notification_render_created=readback.notification_render_count == 1,
        send_disabled_delivery_record_created=readback.send_disabled_delivery_record_count == 1,
    )


def _report(
    *,
    mode: str,
    status: str,
    reason_code: str,
    started_monotonic: float,
    target_event_id: UUID | None = None,
) -> ExactTargetCanaryReport:
    return ExactTargetCanaryReport(
        schema_version=SCHEMA_VERSION,
        mode=mode,
        status=status,
        reason_code=reason_code,
        target_event_fingerprint=_fingerprint(target_event_id),
        target_run_fingerprint=None,
        preflight_passed=False,
        openai_call_attempted=False,
        openai_request_count=0,
        judge_status=None,
        judge_output_created=False,
        judge_output_ready_event_created=False,
        refusal_detected=False,
        validator_attempted=False,
        validator_forwarded_policy=False,
        policy_attempted=False,
        analysis_created=False,
        final_verdict=None,
        delivery_decision=None,
        notification_intent_created=False,
        notifier_attempted=False,
        notification_plan_created=False,
        notification_render_created=False,
        send_disabled_delivery_record_created=False,
        telegram_transport_attempted=False,
        redis_attempted=False,
        cleanup_completed=True,
        redactions_applied=True,
        duration_ms=max(0, int((time.monotonic() - started_monotonic) * 1000)),
    )


def _cli_request_error(args: argparse.Namespace) -> str | None:
    if args.mode not in {"plan", "execute"}:
        return "mode_required"
    if not args.env_file:
        return "env_file_required"
    if len(args.trigger_event_id) != 1:
        return "exactly_one_trigger_event_id_required"
    if _uuid_or_none(args.trigger_event_id[0]) is None:
        return "invalid_trigger_event_id"
    return None


def _read_runtime_env_file(env_file: str) -> dict[str, str]:
    path = Path(env_file)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        raise ExactTargetCanaryConfigError("env_file_missing") from None
    except OSError:
        raise ExactTargetCanaryConfigError("env_file_unreadable") from None

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
        raise ExactTargetCanaryConfigError("env_file_no_runtime_config")
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
        raise ExactTargetCanaryConfigError(missing_reason_code)
    path = Path(file_path)
    if not path.is_file():
        raise ExactTargetCanaryConfigError(file_missing_reason_code)
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError:
        raise ExactTargetCanaryConfigError(file_missing_reason_code) from None
    if not value:
        raise ExactTargetCanaryConfigError(file_empty_reason_code)
    values[value_key] = value
    return value


def _validator_config(values: Mapping[str, str]) -> AnalysisValidatorConfig:
    try:
        cfg = AnalysisValidatorConfig(
            app_env=_read(values, "APP_ENV", "dev").lower(),
            database_url=_read(values, "DATABASE_URL"),
            redis_url=_read(values, "REDIS_URL"),
            queue_name=_read(values, "ANALYSIS_VALIDATOR_QUEUE_NAME", "q.analysis.validate"),
            consumer_group=_read(values, "ANALYSIS_VALIDATOR_CONSUMER_GROUP", "analysis-validator"),
            consumer_name=_read(values, "ANALYSIS_VALIDATOR_CONSUMER_NAME", "analysis-validator-1"),
            batch_size=int(_read(values, "ANALYSIS_VALIDATOR_BATCH_SIZE", "20")),
            block_ms=int(_read(values, "ANALYSIS_VALIDATOR_BLOCK_MS", "5000")),
            max_headline_chars=int(_read(values, "ANALYSIS_VALIDATOR_MAX_HEADLINE_CHARS", "200")),
            max_summary_chars=int(_read(values, "ANALYSIS_VALIDATOR_MAX_SUMMARY_CHARS", "1200")),
            max_text_items=int(_read(values, "ANALYSIS_VALIDATOR_MAX_TEXT_ITEMS", "10")),
            log_level=_read(values, "LOG_LEVEL", "INFO").upper(),
        )
        cfg.validate()
        return cfg
    except (ValueError, TypeError):
        raise ExactTargetCanaryConfigError("validator_config_invalid") from None


def _policy_config(values: Mapping[str, str]) -> PolicyEngineConfig:
    try:
        cfg = PolicyEngineConfig(
            app_env=_read(values, "APP_ENV", "dev").lower(),
            database_url=_read(values, "DATABASE_URL"),
            redis_url=_read(values, "REDIS_URL"),
            queue_name=_read(values, "POLICY_ENGINE_QUEUE_NAME", "q.analysis.policy"),
            consumer_group=_read(values, "POLICY_ENGINE_CONSUMER_GROUP", "policy-engine"),
            consumer_name=_read(values, "POLICY_ENGINE_CONSUMER_NAME", "policy-engine-1"),
            batch_size=int(_read(values, "POLICY_ENGINE_BATCH_SIZE", "20")),
            block_ms=int(_read(values, "POLICY_ENGINE_BLOCK_MS", "5000")),
            policy_version=_read(values, "VERDICT_POLICY_VERSION", "verdict_policy_v1"),
            delivery_policy_version=_read(values, "DELIVERY_POLICY_VERSION", "delivery_policy_v1"),
            operator_chat_id=int(_read(values, "TELEGRAM_OPERATOR_CHAT_ID", "0")),
            enable_later_delivery=_bool_value(_read(values, "ENABLE_LATER_DELIVERY", "true")),
            enable_silent_later=_bool_value(_read(values, "ENABLE_SILENT_LATER", "true")),
            enable_notification_send=_bool_value(_read(values, "ENABLE_NOTIFICATION_SEND", "true")),
            render_profile_high=_read(values, "NOTIFY_RENDER_PROFILE_HIGH", "telegram_single_alert_high_v1"),
            render_profile_normal=_read(values, "NOTIFY_RENDER_PROFILE_NORMAL", "telegram_single_alert_normal_v1"),
            log_level=_read(values, "LOG_LEVEL", "INFO").upper(),
        )
        cfg.validate()
        return cfg
    except (ValueError, TypeError):
        raise ExactTargetCanaryConfigError("policy_config_invalid") from None


def _notifier_config(values: Mapping[str, str]) -> NotifierTelegramConfig:
    try:
        cfg = NotifierTelegramConfig(
            app_env=_read(values, "APP_ENV", "dev").lower(),
            database_url=_read(values, "DATABASE_URL"),
            redis_url=_read(values, "REDIS_URL"),
            telegram_bot_token="",
            queue_name=_read(values, "NOTIFIER_TELEGRAM_QUEUE_NAME", "q.notification.send"),
            consumer_group=_read(values, "NOTIFIER_TELEGRAM_CONSUMER_GROUP", "notifier-telegram"),
            consumer_name=_read(values, "NOTIFIER_TELEGRAM_CONSUMER_NAME", "notifier-telegram-1"),
            batch_size=int(_read(values, "NOTIFIER_TELEGRAM_BATCH_SIZE", "20")),
            block_ms=int(_read(values, "NOTIFIER_TELEGRAM_BLOCK_MS", "5000")),
            dry_run=True,
            allow_edits=False,
            enable_notification_send=False,
            enable_digest_runtime=_bool_value(_read(values, "ENABLE_DIGEST_RUNTIME", "false")),
            max_message_chars=int(_read(values, "NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS", "3800")),
            edit_window_minutes=int(_read(values, "NOTIFIER_TELEGRAM_EDIT_WINDOW_MINUTES", "180")),
            telegram_api_base_url=_read(values, "TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
            request_timeout_sec=float(_read(values, "NOTIFIER_TELEGRAM_REQUEST_TIMEOUT_SEC", "10")),
            log_level=_read(values, "LOG_LEVEL", "INFO").upper(),
        )
        cfg.validate(require_transport_token=False)
        return cfg
    except (ValueError, TypeError):
        raise ExactTargetCanaryConfigError("notifier_config_invalid") from None


def _read(values: Mapping[str, str], key: str, default: str = "") -> str:
    return str(values.get(key, default)).strip()


def _bool_value(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _uuid_or_none(value: Any) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _nonempty_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return value


def _fingerprint(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]


def _safe_reason_code(exc: Exception) -> str:
    text = str(exc)
    if text.replace("_", "").replace("-", "").isalnum() and 1 <= len(text) <= 80:
        return text
    return "configuration_error"


def _compact_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run_cli(argv))


__all__ = [
    "CountingOpenAIJudgeClient",
    "ExactTargetCanaryComponents",
    "ExactTargetCanaryConfigError",
    "ExactTargetCanaryReport",
    "ExactTargetCanaryRequest",
    "ExactTargetPreflight",
    "ExactTargetEvent",
    "FailClosedTelegramTransport",
    "JudgeReadback",
    "NotificationReadback",
    "RuntimeConfigBundle",
    "ServiceConfigBundle",
    "SqlExactTargetCanaryRepository",
    "build_parser",
    "load_runtime_config",
    "load_service_configs",
    "main",
    "run_cli",
    "run_exact_target_canary",
]
