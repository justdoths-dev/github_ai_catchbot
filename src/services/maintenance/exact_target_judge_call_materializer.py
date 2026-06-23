from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, AsyncIterator, Protocol
from uuid import UUID

import sqlalchemy as sa

from ..analysis_router.config import AnalysisRouterConfig
from ..analysis_router.models import (
    AnalysisRequestedJob,
    BundleRouteRecord,
    BundleShapeStats,
    CandidateRouteState,
    JudgeRouteDecision,
)
from ..analysis_router.repositories import AnalysisRouterRepository
from ..analysis_router.routing_policy import ALLOWED_JUDGE_PROFILES, AnalysisRoutingPolicy
from ..analysis_router.service import AnalysisRouterService


SCHEMA_VERSION = "exact_target_judge_call_materializer_report_v1"
ANALYSIS_REQUESTED_EVENT_TYPE = "analysis.requested.v1"
JUDGE_CALL_REQUESTED_EVENT_TYPE = "judge.call.requested.v1"
CONFIRM_TOKEN = "materialize-judge-call"
PLACEHOLDER_REDIS_URL = "redis_locator_not_attempted"

RUNTIME_VALUE_KEYS = {
    "APP_ENV",
    "DATABASE_URL",
    "REDIS_URL",
    "ANALYSIS_ROUTER_QUEUE_NAME",
    "ANALYSIS_ROUTER_CONSUMER_GROUP",
    "ANALYSIS_ROUTER_CONSUMER_NAME",
    "ANALYSIS_ROUTER_BATCH_SIZE",
    "ANALYSIS_ROUTER_BLOCK_MS",
    "ENABLE_MODEL_ESCALATION",
    "JUDGE_DEFAULT_MODEL",
    "JUDGE_ESCALATION_MODEL",
    "JUDGE_REASONING_EFFORT_DEFAULT",
    "JUDGE_REASONING_EFFORT_ESCALATION",
    "JUDGE_PROMPT_VERSION_GITHUB",
    "JUDGE_PROMPT_VERSION_X",
    "JUDGE_PROMPT_VERSION_TEXT_IDEA",
    "JUDGE_SCHEMA_VERSION",
    "VERDICT_POLICY_VERSION",
    "LOG_LEVEL",
}
RUNTIME_FILE_KEYS = {"DATABASE_URL_FILE"}
RUNTIME_ENV_KEYS = RUNTIME_VALUE_KEYS | RUNTIME_FILE_KEYS


class ExactTargetJudgeCallMaterializerConfigError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class ExactTargetJudgeCallMaterializerReport:
    schema_version: str
    mode: str
    status: str
    reason_code: str
    analysis_request_fingerprint: str | None
    candidate_group_fingerprint: str | None
    bundle_fingerprint: str | None
    judge_run_fingerprint: str | None
    judge_call_event_fingerprint: str | None
    preflight_passed: bool
    router_attempted: bool
    judge_run_created: bool
    judge_call_event_created: bool
    openai_attempted: bool
    redis_attempted: bool
    telegram_attempted: bool
    redactions_applied: bool
    bounded_counts: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ExactTargetJudgeCallMaterializerRequest:
    mode: str
    trigger_event_id: UUID


@dataclass(slots=True, frozen=True)
class RuntimeConfigBundle:
    database_url: str
    values: Mapping[str, str]
    router_config: AnalysisRouterConfig


@dataclass(slots=True, frozen=True)
class MaterializerEvent:
    event_id: UUID
    event_type: str
    payload_json: dict[str, Any]


@dataclass(slots=True, frozen=True)
class ExistingJudgeRunReadback:
    count: int
    judge_run_id: UUID | None = None


@dataclass(slots=True, frozen=True)
class JudgeCallEventReadback:
    count: int
    event_id: UUID | None = None


@dataclass(slots=True, frozen=True)
class DownstreamCounts:
    judge_outputs: int = 0
    judge_output_ready_events: int = 0
    policy_events: int = 0
    analyses: int = 0
    notification_intent_events: int = 0
    notification_plans: int = 0
    notification_renders: int = 0
    notification_delivery_records: int = 0

    @property
    def total(self) -> int:
        return (
            self.judge_outputs
            + self.judge_output_ready_events
            + self.policy_events
            + self.analyses
            + self.notification_intent_events
            + self.notification_plans
            + self.notification_renders
            + self.notification_delivery_records
        )

    def to_bounded_dict(self) -> dict[str, int]:
        return {
            "downstream_judge_outputs": _bounded_count(self.judge_outputs),
            "downstream_judge_output_ready_events": _bounded_count(
                self.judge_output_ready_events
            ),
            "downstream_policy_events": _bounded_count(self.policy_events),
            "downstream_analyses": _bounded_count(self.analyses),
            "downstream_notification_intent_events": _bounded_count(
                self.notification_intent_events
            ),
            "downstream_notification_plans": _bounded_count(self.notification_plans),
            "downstream_notification_renders": _bounded_count(self.notification_renders),
            "downstream_notification_delivery_records": _bounded_count(
                self.notification_delivery_records
            ),
        }


@dataclass(slots=True, frozen=True)
class PreflightSnapshot:
    event: MaterializerEvent | None = None
    job: AnalysisRequestedJob | None = None
    candidate_state: CandidateRouteState | None = None
    bundle: BundleRouteRecord | None = None
    shape: BundleShapeStats | None = None
    decision: JudgeRouteDecision | None = None
    existing_judge_run: ExistingJudgeRunReadback = field(
        default_factory=lambda: ExistingJudgeRunReadback(count=0)
    )
    existing_judge_call: JudgeCallEventReadback = field(
        default_factory=lambda: JudgeCallEventReadback(count=0)
    )
    downstream_counts: DownstreamCounts = field(default_factory=DownstreamCounts)
    reason_code: str | None = None

    @property
    def passed(self) -> bool:
        return self.reason_code is None


class MaterializerRepositoryProtocol(Protocol):
    async def load_event_by_id(self, event_id: UUID) -> MaterializerEvent | None: ...
    async def load_candidate_route_state(
        self, candidate_group_id: UUID
    ) -> CandidateRouteState | None: ...
    async def load_bundle(self, bundle_id: UUID) -> BundleRouteRecord | None: ...
    async def load_bundle_shape_stats(self, bundle_id: UUID) -> BundleShapeStats: ...
    async def load_exact_judge_run(
        self, decision: JudgeRouteDecision, *, bundle_id: UUID
    ) -> ExistingJudgeRunReadback: ...
    async def load_router_conflicting_judge_run(
        self, decision: JudgeRouteDecision, *, bundle_id: UUID
    ) -> ExistingJudgeRunReadback: ...
    async def load_judge_call_event_for_run(
        self, judge_run_id: UUID
    ) -> JudgeCallEventReadback: ...
    async def load_downstream_counts_for_run(self, judge_run_id: UUID) -> DownstreamCounts: ...
    async def close_preflight_transaction(self) -> None: ...


class RouterServiceProtocol(Protocol):
    async def handle_trigger_event(self, trigger_event_id: str | UUID) -> None: ...


@dataclass(slots=True)
class ExactTargetJudgeCallMaterializerComponents:
    materializer_repository: MaterializerRepositoryProtocol
    router_service: RouterServiceProtocol


class SqlExactTargetJudgeCallMaterializerRepository:
    def __init__(self, session: Any) -> None:
        self._session = session
        self._router_repository = AnalysisRouterRepository(session)

    async def load_event_by_id(self, event_id: UUID) -> MaterializerEvent | None:
        rows = await self._rows(
            """
            SELECT event_id, event_type, payload_json
            FROM event_outbox
            WHERE event_id = CAST(:event_id AS uuid)
            """,
            {"event_id": str(event_id)},
        )
        if not rows:
            return None
        row = rows[0]
        payload = _json_loads(row["payload_json"])
        return MaterializerEvent(
            event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            payload_json=payload if isinstance(payload, dict) else {},
        )

    async def load_candidate_route_state(
        self, candidate_group_id: UUID
    ) -> CandidateRouteState | None:
        return await self._router_repository.load_candidate_route_state(candidate_group_id)

    async def load_bundle(self, bundle_id: UUID) -> BundleRouteRecord | None:
        return await self._router_repository.load_bundle(bundle_id)

    async def load_bundle_shape_stats(self, bundle_id: UUID) -> BundleShapeStats:
        return await self._router_repository.load_bundle_shape_stats(bundle_id)

    async def load_exact_judge_run(
        self, decision: JudgeRouteDecision, *, bundle_id: UUID
    ) -> ExistingJudgeRunReadback:
        params = {
            "bundle_id": str(bundle_id),
            "judge_profile": decision.judge_profile or "",
            "model": decision.model or "",
            "reasoning_effort": decision.reasoning_effort or "",
            "prompt_version": decision.prompt_version or "",
            "schema_version": decision.schema_version or "",
            "policy_version": decision.policy_version or "",
        }
        rows = await self._rows(
            """
            SELECT judge_run_id
            FROM judge_runs
            WHERE bundle_id = CAST(:bundle_id AS uuid)
              AND judge_profile = :judge_profile
              AND model = :model
              AND reasoning_effort = :reasoning_effort
              AND prompt_version = :prompt_version
              AND schema_version = :schema_version
              AND policy_version = :policy_version
            ORDER BY started_at ASC NULLS FIRST, judge_run_id ASC
            LIMIT 2
            """,
            params,
        )
        return ExistingJudgeRunReadback(
            count=len(rows),
            judge_run_id=UUID(str(rows[0]["judge_run_id"])) if rows else None,
        )

    async def load_router_conflicting_judge_run(
        self, decision: JudgeRouteDecision, *, bundle_id: UUID
    ) -> ExistingJudgeRunReadback:
        rows = await self._rows(
            """
            SELECT judge_run_id
            FROM judge_runs
            WHERE bundle_id = CAST(:bundle_id AS uuid)
              AND model = :model
              AND reasoning_effort = :reasoning_effort
              AND prompt_version = :prompt_version
            ORDER BY started_at ASC NULLS FIRST, judge_run_id ASC
            LIMIT 2
            """,
            {
                "bundle_id": str(bundle_id),
                "model": decision.model or "",
                "reasoning_effort": decision.reasoning_effort or "",
                "prompt_version": decision.prompt_version or "",
            },
        )
        return ExistingJudgeRunReadback(
            count=len(rows),
            judge_run_id=UUID(str(rows[0]["judge_run_id"])) if rows else None,
        )

    async def load_judge_call_event_for_run(
        self, judge_run_id: UUID
    ) -> JudgeCallEventReadback:
        rows = await self._rows(
            """
            SELECT event_id
            FROM event_outbox
            WHERE event_type = 'judge.call.requested.v1'
              AND aggregate_type = 'judge_run'
              AND aggregate_id = CAST(:judge_run_id_uuid AS uuid)
              AND payload_json->>'judge_run_id' = :judge_run_id_text
            ORDER BY created_at ASC, event_id ASC
            LIMIT 2
            """,
            {
                "judge_run_id_uuid": str(judge_run_id),
                "judge_run_id_text": str(judge_run_id),
            },
        )
        return JudgeCallEventReadback(
            count=len(rows),
            event_id=UUID(str(rows[0]["event_id"])) if rows else None,
        )

    async def load_downstream_counts_for_run(self, judge_run_id: UUID) -> DownstreamCounts:
        return DownstreamCounts(
            judge_outputs=await self._count(
                "SELECT count(*) FROM judge_outputs WHERE judge_run_id = CAST(:judge_run_id AS uuid)",
                {"judge_run_id": str(judge_run_id)},
            ),
            judge_output_ready_events=await self._count(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = 'judge.output.ready.v1'
                  AND aggregate_type = 'judge_run'
                  AND aggregate_id = CAST(:judge_run_id_uuid AS uuid)
                  AND payload_json->>'judge_run_id' = :judge_run_id_text
                """,
                {
                    "judge_run_id_uuid": str(judge_run_id),
                    "judge_run_id_text": str(judge_run_id),
                },
            ),
            policy_events=await self._count(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = 'analysis.policy.apply.v1'
                  AND aggregate_type = 'judge_run'
                  AND aggregate_id = CAST(:judge_run_id_uuid AS uuid)
                  AND payload_json->>'judge_run_id' = :judge_run_id_text
                """,
                {
                    "judge_run_id_uuid": str(judge_run_id),
                    "judge_run_id_text": str(judge_run_id),
                },
            ),
            analyses=await self._count(
                """
                SELECT count(*)
                FROM analyses a
                JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
                WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
                """,
                {"judge_run_id": str(judge_run_id)},
            ),
            notification_intent_events=await self._count(
                """
                SELECT count(*)
                FROM event_outbox eo
                JOIN analyses a ON a.analysis_id = eo.aggregate_id
                JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
                WHERE eo.event_type = 'notification.plan.created.v1'
                  AND jo.judge_run_id = CAST(:judge_run_id AS uuid)
                """,
                {"judge_run_id": str(judge_run_id)},
            ),
            notification_plans=await self._count(
                """
                SELECT count(*)
                FROM notification_plans np
                JOIN analyses a ON a.analysis_id = np.analysis_id
                JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
                WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
                """,
                {"judge_run_id": str(judge_run_id)},
            ),
            notification_renders=await self._count(
                """
                SELECT count(*)
                FROM notification_renders nr
                JOIN notification_plans np ON np.notification_plan_id = nr.notification_plan_id
                JOIN analyses a ON a.analysis_id = np.analysis_id
                JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
                WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
                """,
                {"judge_run_id": str(judge_run_id)},
            ),
            notification_delivery_records=await self._count(
                """
                SELECT count(*)
                FROM notification_delivery_records ndr
                JOIN notification_plans np ON np.notification_plan_id = ndr.notification_plan_id
                JOIN analyses a ON a.analysis_id = np.analysis_id
                JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
                WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
                """,
                {"judge_run_id": str(judge_run_id)},
            ),
        )

    async def close_preflight_transaction(self) -> None:
        if self._session.in_transaction():
            await self._session.commit()

    async def _count(self, query: str, params: Mapping[str, Any]) -> int:
        result = await self._session.execute(sa.text(query), dict(params))
        return int(result.scalar_one())

    async def _rows(self, query: str, params: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        result = await self._session.execute(sa.text(query), dict(params))
        return list(result.mappings().all())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="exact-target-judge-call-materializer")
    parser.add_argument("--mode")
    parser.add_argument("--trigger-event-id", action="append", default=[])
    parser.add_argument("--env-file")
    parser.add_argument("--confirm", default=None)
    return parser


async def run_cli(
    argv: Sequence[str] | None = None,
    *,
    emit_json: Callable[[str], None] = print,
    runtime_config_loader: Callable[[str], RuntimeConfigBundle] | None = None,
    session_components_builder: Callable[
        [RuntimeConfigBundle],
        AsyncIterator[ExactTargetJudgeCallMaterializerComponents],
    ]
    | None = None,
) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    validation_error = _cli_request_error(args)
    trigger_event_id = (
        _uuid_or_none(args.trigger_event_id[0])
        if len(args.trigger_event_id) == 1
        else None
    )
    mode = str(args.mode) if args.mode in {"plan", "execute"} else "unknown"
    if validation_error is not None:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=mode,
                        status="blocked",
                        reason_code=validation_error,
                        analysis_request_id=trigger_event_id,
                    )
                )
            )
        )
        return 2

    assert trigger_event_id is not None
    if args.mode == "execute" and args.confirm != CONFIRM_TOKEN:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=args.mode,
                        status="blocked",
                        reason_code="materialize_judge_call_confirm_missing",
                        analysis_request_id=trigger_event_id,
                    )
                )
            )
        )
        return 2
    if args.mode == "plan" and args.confirm is not None:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=args.mode,
                        status="blocked",
                        reason_code="confirm_not_allowed_for_plan",
                        analysis_request_id=trigger_event_id,
                    )
                )
            )
        )
        return 2

    try:
        runtime = (runtime_config_loader or load_runtime_config)(str(args.env_file))
    except ExactTargetJudgeCallMaterializerConfigError as exc:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=args.mode,
                        status="blocked",
                        reason_code=_safe_reason_code(exc),
                        analysis_request_id=trigger_event_id,
                    )
                )
            )
        )
        return 2

    builder = session_components_builder or sql_session_components
    async with builder(runtime) as components:
        report = await run_exact_target_judge_call_materializer(
            ExactTargetJudgeCallMaterializerRequest(
                mode=args.mode,
                trigger_event_id=trigger_event_id,
            ),
            router_config=runtime.router_config,
            components=components,
        )
    emit_json(_compact_json(asdict(report)))
    return 0 if report.status == "pass" else 2


async def run_exact_target_judge_call_materializer(
    request: ExactTargetJudgeCallMaterializerRequest,
    *,
    router_config: AnalysisRouterConfig,
    components: ExactTargetJudgeCallMaterializerComponents,
) -> ExactTargetJudgeCallMaterializerReport:
    report = _report(
        mode=request.mode,
        status="failed",
        reason_code="unhandled_error",
        analysis_request_id=request.trigger_event_id,
    )
    try:
        preflight = await _load_preflight(
            components.materializer_repository,
            router_config=router_config,
            trigger_event_id=request.trigger_event_id,
        )
        report = _apply_preflight(report, preflight)
        if not preflight.passed:
            return replace(
                report,
                status="blocked",
                reason_code=preflight.reason_code or "preflight_blocked",
            )
        if request.mode == "plan":
            return replace(
                report,
                status="pass",
                reason_code="plan_ready",
            )

        assert preflight.job is not None
        assert preflight.decision is not None
        try:
            await components.materializer_repository.close_preflight_transaction()
        except Exception:
            return replace(
                report,
                status="failed",
                reason_code="preflight_transaction_close_failed",
            )

        report = replace(report, router_attempted=True)
        await components.router_service.handle_trigger_event(request.trigger_event_id)

        run_readback = await components.materializer_repository.load_exact_judge_run(
            preflight.decision,
            bundle_id=preflight.job.bundle_id,
        )
        report = _apply_judge_run_readback(report, run_readback)
        if run_readback.count != 1 or run_readback.judge_run_id is None:
            return replace(report, status="failed", reason_code="judge_run_cardinality_invalid")

        call_readback = await components.materializer_repository.load_judge_call_event_for_run(
            run_readback.judge_run_id
        )
        report = _apply_judge_call_readback(report, call_readback)
        if call_readback.count != 1 or call_readback.event_id is None:
            return replace(
                report,
                status="failed",
                reason_code="judge_call_event_cardinality_invalid",
            )

        downstream = await components.materializer_repository.load_downstream_counts_for_run(
            run_readback.judge_run_id
        )
        report = _with_counts(report, downstream.to_bounded_dict())
        if downstream.total:
            return replace(report, status="failed", reason_code="downstream_already_exists")

        return replace(
            report,
            status="pass",
            reason_code="judge_call_materialized",
            preflight_passed=True,
            judge_run_created=True,
            judge_call_event_created=True,
        )
    except ExactTargetJudgeCallMaterializerConfigError as exc:
        return replace(report, status="blocked", reason_code=_safe_reason_code(exc))
    except Exception:
        return replace(report, status="failed", reason_code="unhandled_error")


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
        router_config = AnalysisRouterConfig(
            app_env=_read(resolved_values, "APP_ENV", "dev").lower(),
            database_url=database_url,
            redis_url=_read(resolved_values, "REDIS_URL", PLACEHOLDER_REDIS_URL)
            or PLACEHOLDER_REDIS_URL,
            queue_name=_read(
                resolved_values,
                "ANALYSIS_ROUTER_QUEUE_NAME",
                "q.analysis.route",
            ),
            consumer_group=_read(
                resolved_values,
                "ANALYSIS_ROUTER_CONSUMER_GROUP",
                "analysis-router",
            ),
            consumer_name=_read(
                resolved_values,
                "ANALYSIS_ROUTER_CONSUMER_NAME",
                "analysis-router-1",
            ),
            batch_size=int(_read(resolved_values, "ANALYSIS_ROUTER_BATCH_SIZE", "20")),
            block_ms=int(_read(resolved_values, "ANALYSIS_ROUTER_BLOCK_MS", "5000")),
            enable_model_escalation=_bool_value(
                _read(resolved_values, "ENABLE_MODEL_ESCALATION", "false")
            ),
            default_model=_read(resolved_values, "JUDGE_DEFAULT_MODEL", "gpt-5.4-mini"),
            escalation_model=_read(resolved_values, "JUDGE_ESCALATION_MODEL", "gpt-5.4"),
            default_reasoning_effort=_read(
                resolved_values,
                "JUDGE_REASONING_EFFORT_DEFAULT",
                "low",
            ),
            escalation_reasoning_effort=_read(
                resolved_values,
                "JUDGE_REASONING_EFFORT_ESCALATION",
                "medium",
            ),
            github_prompt_version=_read(
                resolved_values,
                "JUDGE_PROMPT_VERSION_GITHUB",
                "judge_github_primary_v1",
            ),
            x_prompt_version=_read(
                resolved_values,
                "JUDGE_PROMPT_VERSION_X",
                "judge_x_primary_v1",
            ),
            text_idea_prompt_version=_read(
                resolved_values,
                "JUDGE_PROMPT_VERSION_TEXT_IDEA",
                "judge_text_idea_primary_v1",
            ),
            judge_schema_version=_read(
                resolved_values,
                "JUDGE_SCHEMA_VERSION",
                "judge_output_v1",
            ),
            policy_version=_read(
                resolved_values,
                "VERDICT_POLICY_VERSION",
                "verdict_policy_v1",
            ),
            log_level=_read(resolved_values, "LOG_LEVEL", "INFO").upper(),
        )
        router_config.validate()
    except (ValueError, TypeError):
        raise ExactTargetJudgeCallMaterializerConfigError("analysis_router_config_invalid") from None
    return RuntimeConfigBundle(
        database_url=database_url,
        values=resolved_values,
        router_config=router_config,
    )


@asynccontextmanager
async def sql_session_components(
    runtime: RuntimeConfigBundle,
) -> AsyncIterator[ExactTargetJudgeCallMaterializerComponents]:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with session_factory() as session:
            yield ExactTargetJudgeCallMaterializerComponents(
                materializer_repository=SqlExactTargetJudgeCallMaterializerRepository(session),
                router_service=AnalysisRouterService(
                    runtime.router_config,
                    repository=AnalysisRouterRepository(session),
                ),
            )
    finally:
        await engine.dispose()


async def _load_preflight(
    repository: MaterializerRepositoryProtocol,
    *,
    router_config: AnalysisRouterConfig,
    trigger_event_id: UUID,
) -> PreflightSnapshot:
    event = await repository.load_event_by_id(trigger_event_id)
    if event is None:
        return PreflightSnapshot(reason_code="event_missing")
    if event.event_type != ANALYSIS_REQUESTED_EVENT_TYPE:
        return PreflightSnapshot(event=event, reason_code="wrong_event_type")

    job = _job_from_event(event)
    if job is None:
        return PreflightSnapshot(event=event, reason_code="invalid_payload")

    candidate_state = await repository.load_candidate_route_state(job.candidate_group_id)
    if candidate_state is None:
        return PreflightSnapshot(event=event, job=job, reason_code="candidate_group_missing")
    if candidate_state.current_bundle_id != job.bundle_id:
        return PreflightSnapshot(
            event=event,
            job=job,
            candidate_state=candidate_state,
            reason_code="stale_bundle",
        )

    bundle = await repository.load_bundle(job.bundle_id)
    if bundle is None:
        return PreflightSnapshot(
            event=event,
            job=job,
            candidate_state=candidate_state,
            reason_code="bundle_missing",
        )
    if bundle.candidate_group_id != job.candidate_group_id:
        return PreflightSnapshot(
            event=event,
            job=job,
            candidate_state=candidate_state,
            bundle=bundle,
            reason_code="bundle_candidate_mismatch",
        )
    if not bundle.ready_for_analysis:
        return PreflightSnapshot(
            event=event,
            job=job,
            candidate_state=candidate_state,
            bundle=bundle,
            reason_code="bundle_not_ready",
        )

    judge_profile = (job.judge_profile or "").strip()
    if judge_profile not in ALLOWED_JUDGE_PROFILES:
        return PreflightSnapshot(
            event=event,
            job=job,
            candidate_state=candidate_state,
            bundle=bundle,
            reason_code="judge_profile_not_allowlisted",
        )

    shape = await repository.load_bundle_shape_stats(job.bundle_id)
    if shape.member_count <= 0:
        return PreflightSnapshot(
            event=event,
            job=job,
            candidate_state=candidate_state,
            bundle=bundle,
            shape=shape,
            reason_code="bundle_members_missing",
        )

    decision = AnalysisRoutingPolicy(router_config).decide(
        job=job,
        current_bundle_id=candidate_state.current_bundle_id,
        bundle=bundle,
        shape=shape,
    )
    if decision.action != "judge":
        return PreflightSnapshot(
            event=event,
            job=job,
            candidate_state=candidate_state,
            bundle=bundle,
            shape=shape,
            decision=decision,
            reason_code="analysis_router_would_not_create_judge_call",
        )

    existing_run = await repository.load_exact_judge_run(decision, bundle_id=job.bundle_id)
    existing_call = JudgeCallEventReadback(count=0)
    downstream = DownstreamCounts()
    if existing_run.judge_run_id is not None:
        existing_call = await repository.load_judge_call_event_for_run(existing_run.judge_run_id)
        downstream = await repository.load_downstream_counts_for_run(existing_run.judge_run_id)
        if downstream.total:
            reason = "downstream_already_exists"
        elif existing_call.count:
            reason = "existing_judge_call"
        else:
            reason = "existing_judge_run"
        return PreflightSnapshot(
            event=event,
            job=job,
            candidate_state=candidate_state,
            bundle=bundle,
            shape=shape,
            decision=decision,
            existing_judge_run=existing_run,
            existing_judge_call=existing_call,
            downstream_counts=downstream,
            reason_code=reason,
        )

    router_conflict = await repository.load_router_conflicting_judge_run(
        decision,
        bundle_id=job.bundle_id,
    )
    if router_conflict.judge_run_id is not None:
        return PreflightSnapshot(
            event=event,
            job=job,
            candidate_state=candidate_state,
            bundle=bundle,
            shape=shape,
            decision=decision,
            existing_judge_run=router_conflict,
            reason_code="analysis_router_would_not_create_judge_call",
        )

    return PreflightSnapshot(
        event=event,
        job=job,
        candidate_state=candidate_state,
        bundle=bundle,
        shape=shape,
        decision=decision,
    )


def _job_from_event(event: MaterializerEvent) -> AnalysisRequestedJob | None:
    payload = event.payload_json
    candidate_group_id = _uuid_or_none(payload.get("candidate_group_id"))
    bundle_id = _uuid_or_none(payload.get("bundle_id"))
    judge_profile = _nonempty_string(payload.get("judge_profile"))
    if candidate_group_id is None or bundle_id is None or judge_profile is None:
        return None
    return AnalysisRequestedJob(
        trigger_event_id=event.event_id,
        event_type=event.event_type,
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        judge_profile=judge_profile,
        escalation_allowed=bool(payload.get("escalation_allowed", False)),
    )


def _apply_preflight(
    report: ExactTargetJudgeCallMaterializerReport,
    preflight: PreflightSnapshot,
) -> ExactTargetJudgeCallMaterializerReport:
    counts = {
        "analysis_request_events": _bounded_count(1 if preflight.event else 0),
        "candidate_groups": _bounded_count(1 if preflight.candidate_state else 0),
        "bundles": _bounded_count(1 if preflight.bundle else 0),
        "bundle_members": _bounded_count(
            preflight.shape.member_count if preflight.shape else 0
        ),
        "existing_judge_runs": _bounded_count(preflight.existing_judge_run.count),
        "existing_judge_call_events": _bounded_count(preflight.existing_judge_call.count),
    }
    counts.update(preflight.downstream_counts.to_bounded_dict())
    return replace(
        _with_counts(report, counts),
        analysis_request_fingerprint=_fingerprint(preflight.event.event_id)
        if preflight.event
        else report.analysis_request_fingerprint,
        candidate_group_fingerprint=_fingerprint(preflight.job.candidate_group_id)
        if preflight.job
        else None,
        bundle_fingerprint=_fingerprint(preflight.job.bundle_id) if preflight.job else None,
        judge_run_fingerprint=_fingerprint(preflight.existing_judge_run.judge_run_id),
        judge_call_event_fingerprint=_fingerprint(preflight.existing_judge_call.event_id),
        preflight_passed=preflight.passed,
    )


def _apply_judge_run_readback(
    report: ExactTargetJudgeCallMaterializerReport,
    readback: ExistingJudgeRunReadback,
) -> ExactTargetJudgeCallMaterializerReport:
    return replace(
        _with_counts(report, {"existing_judge_runs": _bounded_count(readback.count)}),
        judge_run_fingerprint=_fingerprint(readback.judge_run_id),
    )


def _apply_judge_call_readback(
    report: ExactTargetJudgeCallMaterializerReport,
    readback: JudgeCallEventReadback,
) -> ExactTargetJudgeCallMaterializerReport:
    return replace(
        _with_counts(
            report,
            {"existing_judge_call_events": _bounded_count(readback.count)},
        ),
        judge_call_event_fingerprint=_fingerprint(readback.event_id),
    )


def _with_counts(
    report: ExactTargetJudgeCallMaterializerReport,
    counts: Mapping[str, int],
) -> ExactTargetJudgeCallMaterializerReport:
    merged = dict(report.bounded_counts)
    merged.update({key: _bounded_count(value) for key, value in counts.items()})
    return replace(report, bounded_counts=merged)


def _report(
    *,
    mode: str,
    status: str,
    reason_code: str,
    analysis_request_id: UUID | None = None,
) -> ExactTargetJudgeCallMaterializerReport:
    return ExactTargetJudgeCallMaterializerReport(
        schema_version=SCHEMA_VERSION,
        mode=mode,
        status=status,
        reason_code=reason_code,
        analysis_request_fingerprint=_fingerprint(analysis_request_id),
        candidate_group_fingerprint=None,
        bundle_fingerprint=None,
        judge_run_fingerprint=None,
        judge_call_event_fingerprint=None,
        preflight_passed=False,
        router_attempted=False,
        judge_run_created=False,
        judge_call_event_created=False,
        openai_attempted=False,
        redis_attempted=False,
        telegram_attempted=False,
        redactions_applied=True,
        bounded_counts={},
    )


def _cli_request_error(args: argparse.Namespace) -> str | None:
    if args.mode not in {"plan", "execute"}:
        return "mode_required"
    if len(args.trigger_event_id) != 1:
        return "exactly_one_trigger_event_id_required"
    if _uuid_or_none(args.trigger_event_id[0]) is None:
        return "invalid_trigger_event_id"
    if not args.env_file:
        return "env_file_required"
    return None


def _read_runtime_env_file(env_file: str) -> dict[str, str]:
    path = Path(env_file)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        raise ExactTargetJudgeCallMaterializerConfigError("env_file_missing") from None
    except OSError:
        raise ExactTargetJudgeCallMaterializerConfigError("env_file_unreadable") from None

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
        raise ExactTargetJudgeCallMaterializerConfigError("env_file_no_runtime_config")
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
        raise ExactTargetJudgeCallMaterializerConfigError(missing_reason_code)
    path = Path(file_path)
    if not path.is_file():
        raise ExactTargetJudgeCallMaterializerConfigError(file_missing_reason_code)
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError:
        raise ExactTargetJudgeCallMaterializerConfigError(file_missing_reason_code) from None
    if not value:
        raise ExactTargetJudgeCallMaterializerConfigError(file_empty_reason_code)
    values[value_key] = value
    return value


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


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return value


def _bounded_count(value: int) -> int:
    return min(max(int(value), 0), 2)


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
    "CONFIRM_TOKEN",
    "DownstreamCounts",
    "ExactTargetJudgeCallMaterializerComponents",
    "ExactTargetJudgeCallMaterializerConfigError",
    "ExactTargetJudgeCallMaterializerReport",
    "ExactTargetJudgeCallMaterializerRequest",
    "ExistingJudgeRunReadback",
    "JudgeCallEventReadback",
    "MaterializerEvent",
    "PreflightSnapshot",
    "RuntimeConfigBundle",
    "SqlExactTargetJudgeCallMaterializerRepository",
    "build_parser",
    "load_runtime_config",
    "main",
    "run_cli",
    "run_exact_target_judge_call_materializer",
]
