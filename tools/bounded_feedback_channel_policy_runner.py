from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Protocol, Sequence
from uuid import UUID


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.policy_engine.channel_override_policy import ChannelOverrideInput, ChannelOverridePolicy
from src.services.policy_engine.feedback_eval import (
    ALLOWED_FEEDBACK_LABELS,
    ChannelFeedbackObservation,
    ChannelFeedbackSample,
    FeedbackEvalEngine,
    FeedbackRecord,
    FeedbackTargetContext,
    NotificationFeedbackError,
    NotificationFeedbackReadbackResult,
    NotificationFeedbackRequest,
    NotificationFeedbackService,
    StoredNotificationFeedback,
    channel_fp,
)


SCHEMA_VERSION = "bounded_feedback_channel_policy_runner_v2"
RUNNER_NAME = "bounded_feedback_channel_policy_runner"
MODE_PLAN = "plan"
MODE_EXECUTE = "execute"
DEFAULT_MAX_ROWS = 25
HARD_MAX_ROWS = 100
MAX_FEEDBACK_BYTES = 256 * 1024
MAX_CHANNEL_POLICY_BYTES = 64 * 1024
UUID_SUFFIX_RE = re.compile(r"^[0-9a-f-]{4,36}$")


class CliArgumentError(ValueError):
    pass


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


class BoundedFeedbackChannelPolicyError(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


@dataclass(frozen=True, slots=True)
class BoundedFeedbackChannelPolicyConfig:
    mode: str = MODE_PLAN
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_database_write: bool = False
    analysis_id_suffix: str | None = None
    candidate_group_id_suffix: str | None = None
    analysis_id: UUID | None = None
    notification_plan_id: UUID | None = None
    notification_delivery_record_id: UUID | None = None
    feedback_category: str | None = None
    operator_action_key: str | None = None
    confirm_feedback_write: bool = False
    feedback_jsonl: str | None = None
    allow_feedback_file_read: bool = False
    channel_policy_json: str | None = None
    allow_channel_policy_file_read: bool = False
    max_rows: int = DEFAULT_MAX_ROWS


@dataclass(frozen=True, slots=True)
class BoundedFeedbackChannelPolicyRuntimeConfig:
    database_url: str


@dataclass(slots=True)
class BoundedFeedbackChannelPolicyState:
    runtime_config_loaded: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    feedback_file_read_attempted: bool = False
    channel_policy_file_read_attempted: bool = False
    database_write_attempted: bool = False
    redis_called: bool = False
    telegram_called: bool = False
    openai_called: bool = False
    external_network_called: bool = False


@dataclass(frozen=True, slots=True)
class PolicyReadbackRow:
    analysis_id: str | None
    candidate_group_id: str | None
    verdict: str
    delivery_decision: str
    urgency_profile: str
    reason_codes: tuple[str, ...] = ()
    primary_artifact_type: str = "unknown"
    notification_plan_count: int = 0
    render_count: int = 0
    delivery_record_count: int = 0
    sent_count: int = 0
    suppressed_count: int = 0
    channel_tier: str | None = None


class FeedbackChannelPolicyReadbackRepository(Protocol):
    async def load_policy_readbacks(
        self,
        *,
        analysis_id_suffix: str | None,
        candidate_group_id_suffix: str | None,
        max_rows: int,
    ) -> list[PolicyReadbackRow]: ...

    def transaction(self): ...

    async def load_feedback_target(
        self,
        *,
        analysis_id: UUID,
        notification_plan_id: UUID | None,
        notification_delivery_record_id: UUID | None,
    ) -> FeedbackTargetContext | None: ...

    async def load_feedback_by_action_key(
        self,
        operator_action_key: str,
    ) -> StoredNotificationFeedback | None: ...

    async def insert_notification_feedback(
        self,
        *,
        request: NotificationFeedbackRequest,
        target: FeedbackTargetContext,
    ) -> StoredNotificationFeedback | None: ...

    async def load_channel_feedback_sample(
        self,
        *,
        channel_registry_id: UUID,
        sample_limit: int,
        window_days: int,
    ) -> ChannelFeedbackSample: ...


@dataclass(frozen=True, slots=True)
class FeedbackChannelPolicyRepositoryHandle:
    repository: FeedbackChannelPolicyReadbackRepository
    close: Callable[[], Awaitable[None]]


RepositoryBuilder = Callable[
    [BoundedFeedbackChannelPolicyRuntimeConfig, BoundedFeedbackChannelPolicyState],
    Awaitable[FeedbackChannelPolicyRepositoryHandle],
]


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


class SqlAlchemyFeedbackChannelPolicyRepository:
    def __init__(self, session: Any, state: BoundedFeedbackChannelPolicyState) -> None:
        self._session = session
        self._state = state

    @asynccontextmanager
    async def transaction(self):
        if self._session.in_transaction():
            raise BoundedFeedbackChannelPolicyError("feedback_transaction_already_active")
        async with self._session.begin():
            yield self._session

    async def load_policy_readbacks(
        self,
        *,
        analysis_id_suffix: str | None,
        candidate_group_id_suffix: str | None,
        max_rows: int,
    ) -> list[PolicyReadbackRow]:
        self._state.database_read_attempted = True
        conditions: list[str] = []
        params: dict[str, Any] = {"limit": max_rows + 1}
        if analysis_id_suffix:
            conditions.append("lower(a.analysis_id::text) LIKE :analysis_id_suffix")
            params["analysis_id_suffix"] = f"%{analysis_id_suffix}"
        if candidate_group_id_suffix:
            conditions.append("lower(a.candidate_group_id::text) LIKE :candidate_group_id_suffix")
            params["candidate_group_id_suffix"] = f"%{candidate_group_id_suffix}"
        if not conditions:
            return []

        from sqlalchemy import text

        result = await self._session.execute(
            text(
                f"""
                SELECT a.analysis_id,
                       a.candidate_group_id,
                       a.verdict::text AS verdict,
                       a.delivery_decision::text AS delivery_decision,
                       a.reason_codes_json,
                       COALESCE(ar.artifact_type::text, 'unknown') AS primary_artifact_type,
                       COUNT(DISTINCT np.notification_plan_id)::int AS notification_plan_count,
                       COUNT(DISTINCT nr.notification_render_id)::int AS render_count,
                       COUNT(DISTINCT ndr.notification_delivery_record_id)::int AS delivery_record_count,
                       COUNT(DISTINCT ndr.notification_delivery_record_id) FILTER (
                           WHERE ndr.delivery_status::text IN ('sent', 'edited')
                       )::int AS sent_count,
                       COUNT(DISTINCT ndr.notification_delivery_record_id) FILTER (
                           WHERE ndr.delivery_status::text = 'suppressed'
                       )::int AS suppressed_count,
                       MAX(np.urgency_profile::text) AS urgency_profile
                FROM analyses a
                LEFT JOIN candidate_group_proposals cgp
                  ON cgp.candidate_group_id = a.candidate_group_id
                LEFT JOIN artifact_registry ar
                  ON ar.artifact_id = cgp.current_primary_artifact_id
                LEFT JOIN notification_plans np
                  ON np.analysis_id = a.analysis_id
                LEFT JOIN notification_renders nr
                  ON nr.notification_plan_id = np.notification_plan_id
                LEFT JOIN notification_delivery_records ndr
                  ON ndr.notification_plan_id = np.notification_plan_id
                WHERE {' AND '.join(conditions)}
                GROUP BY a.analysis_id, a.candidate_group_id, a.verdict, a.delivery_decision,
                         a.reason_codes_json, ar.artifact_type, a.created_at
                ORDER BY a.created_at DESC, a.analysis_id ASC
                LIMIT :limit
                """
            ),
            params,
        )
        rows: list[PolicyReadbackRow] = []
        for row in result.mappings().all():
            verdict = str(row["verdict"])
            delivery_decision = str(row["delivery_decision"])
            rows.append(
                PolicyReadbackRow(
                    analysis_id=str(row["analysis_id"]) if row["analysis_id"] else None,
                    candidate_group_id=str(row["candidate_group_id"]) if row["candidate_group_id"] else None,
                    verdict=verdict,
                    delivery_decision=delivery_decision,
                    urgency_profile=_safe_urgency_profile(row["urgency_profile"], verdict, delivery_decision),
                    reason_codes=_safe_reason_codes(_json_list(row["reason_codes_json"])),
                    primary_artifact_type=str(row["primary_artifact_type"] or "unknown"),
                    notification_plan_count=int(row["notification_plan_count"] or 0),
                    render_count=int(row["render_count"] or 0),
                    delivery_record_count=int(row["delivery_record_count"] or 0),
                    sent_count=int(row["sent_count"] or 0),
                    suppressed_count=int(row["suppressed_count"] or 0),
                )
            )
        return rows

    async def load_feedback_target(
        self,
        *,
        analysis_id: UUID,
        notification_plan_id: UUID | None,
        notification_delivery_record_id: UUID | None,
    ) -> FeedbackTargetContext | None:
        self._state.database_read_attempted = True
        from sqlalchemy import text

        result = await self._session.execute(
            text(
                """
                SELECT a.analysis_id,
                       a.candidate_group_id,
                       a.verdict::text AS verdict,
                       a.delivery_decision::text AS delivery_decision,
                       COALESCE(ar.artifact_type::text, 'unknown') AS primary_artifact_type,
                       np.notification_plan_id,
                       np.analysis_id AS plan_analysis_id,
                       np.candidate_group_id AS plan_candidate_group_id,
                       np.plan_match_count,
                       ndr.notification_delivery_record_id,
                       ndr.notification_plan_id AS delivery_plan_id,
                       CASE WHEN channel_ref.registry_count = 1 THEN channel_ref.registry_id END AS channel_registry_id
                FROM analyses a
                JOIN candidate_group_proposals cgp
                  ON cgp.candidate_group_id = a.candidate_group_id
                JOIN source_messages sm
                  ON sm.source_message_id = cgp.source_message_id
                LEFT JOIN judge_outputs jo
                  ON jo.judge_output_id = a.judge_output_id
                LEFT JOIN judge_runs jr
                  ON jr.judge_run_id = jo.judge_run_id
                LEFT JOIN candidate_evidence_bundles ceb
                  ON ceb.bundle_id = jr.bundle_id
                LEFT JOIN artifact_registry ar
                  ON ar.artifact_id = ceb.current_primary_artifact_id
                LEFT JOIN notification_delivery_records requested_delivery
                  ON requested_delivery.notification_delivery_record_id = CAST(:delivery_record_id AS uuid)
                LEFT JOIN LATERAL (
                    SELECT candidate_plan.notification_plan_id,
                           candidate_plan.analysis_id,
                           candidate_plan.candidate_group_id,
                           COUNT(*) OVER ()::int AS plan_match_count
                    FROM notification_plans candidate_plan
                    WHERE candidate_plan.analysis_id = a.analysis_id
                      AND candidate_plan.candidate_group_id = a.candidate_group_id
                      AND (
                          CAST(:notification_plan_id AS uuid) IS NULL
                          OR candidate_plan.notification_plan_id = CAST(:notification_plan_id AS uuid)
                      )
                      AND (
                          CAST(:delivery_record_id AS uuid) IS NULL
                          OR candidate_plan.notification_plan_id = requested_delivery.notification_plan_id
                      )
                    ORDER BY candidate_plan.created_at DESC,
                             candidate_plan.notification_plan_id DESC
                    LIMIT 1
                ) np ON TRUE
                LEFT JOIN LATERAL (
                    SELECT candidate_delivery.notification_delivery_record_id,
                           candidate_delivery.notification_plan_id
                    FROM notification_delivery_records candidate_delivery
                    WHERE candidate_delivery.notification_plan_id = np.notification_plan_id
                      AND (
                          CAST(:delivery_record_id AS uuid) IS NULL
                          OR candidate_delivery.notification_delivery_record_id = CAST(:delivery_record_id AS uuid)
                      )
                    ORDER BY candidate_delivery.created_at DESC,
                             candidate_delivery.notification_delivery_record_id DESC
                    LIMIT 1
                ) ndr ON TRUE
                LEFT JOIN LATERAL (
                    SELECT MIN(tcr.registry_id::text)::uuid AS registry_id,
                           COUNT(*)::int AS registry_count
                    FROM telegram_channel_registry tcr
                    WHERE tcr.chat_id = sm.chat_id
                      AND tcr.desired_state <> 'removed'
                ) channel_ref ON TRUE
                WHERE a.analysis_id = CAST(:analysis_id AS uuid)
                """
            ),
            {
                "analysis_id": str(analysis_id),
                "notification_plan_id": str(notification_plan_id) if notification_plan_id else None,
                "delivery_record_id": (
                    str(notification_delivery_record_id) if notification_delivery_record_id else None
                ),
            },
        )
        row = result.mappings().first()
        if row is None:
            return None

        resolved_plan_id = _uuid_or_none(row["notification_plan_id"])
        resolved_delivery_id = _uuid_or_none(row["notification_delivery_record_id"])
        if int(row["plan_match_count"] or 0) > 1:
            return None
        if notification_plan_id is not None and resolved_plan_id != notification_plan_id:
            return None
        if notification_delivery_record_id is not None and resolved_delivery_id != notification_delivery_record_id:
            return None
        if resolved_plan_id is not None:
            if _uuid_or_none(row["plan_analysis_id"]) != analysis_id:
                return None
            if _uuid_or_none(row["plan_candidate_group_id"]) != _uuid_or_none(row["candidate_group_id"]):
                return None
        if resolved_delivery_id is not None and _uuid_or_none(row["delivery_plan_id"]) != resolved_plan_id:
            return None

        channel_registry_id = _uuid_or_none(row["channel_registry_id"])
        return FeedbackTargetContext(
            analysis_id=analysis_id,
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            notification_plan_id=resolved_plan_id,
            notification_delivery_record_id=resolved_delivery_id,
            channel_registry_id=channel_registry_id,
            channel_fingerprint=channel_fp(channel_registry_id) if channel_registry_id else None,
            verdict=str(row["verdict"]),
            delivery_decision=str(row["delivery_decision"]),
            primary_artifact_type=str(row["primary_artifact_type"] or "unknown"),
        )

    async def load_feedback_by_action_key(
        self,
        operator_action_key: str,
    ) -> StoredNotificationFeedback | None:
        self._state.database_read_attempted = True
        from sqlalchemy import text

        result = await self._session.execute(
            text(
                """
                SELECT feedback_id,
                       operator_action_key,
                       feedback_category,
                       analysis_id,
                       candidate_group_id,
                       notification_plan_id,
                       notification_delivery_record_id,
                       channel_registry_id,
                       final_verdict::text AS verdict,
                       delivery_decision::text AS delivery_decision,
                       primary_artifact_type
                FROM notification_feedback
                WHERE operator_action_key = :operator_action_key
                """
            ),
            {"operator_action_key": operator_action_key},
        )
        row = result.mappings().first()
        return _stored_feedback_from_row(row) if row is not None else None

    async def insert_notification_feedback(
        self,
        *,
        request: NotificationFeedbackRequest,
        target: FeedbackTargetContext,
    ) -> StoredNotificationFeedback | None:
        self._state.database_write_attempted = True
        from sqlalchemy import text

        result = await self._session.execute(
            text(
                """
                INSERT INTO notification_feedback (
                    operator_action_key,
                    feedback_category,
                    analysis_id,
                    candidate_group_id,
                    notification_plan_id,
                    notification_delivery_record_id,
                    channel_registry_id,
                    final_verdict,
                    delivery_decision,
                    primary_artifact_type,
                    created_at
                ) VALUES (
                    :operator_action_key,
                    :feedback_category,
                    CAST(:analysis_id AS uuid),
                    CAST(:candidate_group_id AS uuid),
                    CAST(:notification_plan_id AS uuid),
                    CAST(:delivery_record_id AS uuid),
                    CAST(:channel_registry_id AS uuid),
                    CAST(:verdict AS verdict_enum),
                    CAST(:delivery_decision AS delivery_decision_enum),
                    :primary_artifact_type,
                    now()
                )
                ON CONFLICT (operator_action_key) DO NOTHING
                RETURNING feedback_id,
                          operator_action_key,
                          feedback_category,
                          analysis_id,
                          candidate_group_id,
                          notification_plan_id,
                          notification_delivery_record_id,
                          channel_registry_id,
                          final_verdict::text AS verdict,
                          delivery_decision::text AS delivery_decision,
                          primary_artifact_type
                """
            ),
            {
                "operator_action_key": request.operator_action_key,
                "feedback_category": request.feedback_category,
                "analysis_id": str(target.analysis_id),
                "candidate_group_id": str(target.candidate_group_id),
                "notification_plan_id": (
                    str(target.notification_plan_id) if target.notification_plan_id else None
                ),
                "delivery_record_id": (
                    str(target.notification_delivery_record_id)
                    if target.notification_delivery_record_id
                    else None
                ),
                "channel_registry_id": (
                    str(target.channel_registry_id) if target.channel_registry_id else None
                ),
                "verdict": target.verdict,
                "delivery_decision": target.delivery_decision,
                "primary_artifact_type": target.primary_artifact_type,
            },
        )
        row = result.mappings().first()
        return _stored_feedback_from_row(row) if row is not None else None

    async def load_channel_feedback_sample(
        self,
        *,
        channel_registry_id: UUID,
        sample_limit: int,
        window_days: int,
    ) -> ChannelFeedbackSample:
        if not 1 <= sample_limit <= HARD_MAX_ROWS:
            raise ValueError("sample_limit_out_of_range")
        if not 1 <= window_days <= 365:
            raise ValueError("window_days_out_of_range")
        self._state.database_read_attempted = True
        from sqlalchemy import text

        result = await self._session.execute(
            text(
                """
                SELECT feedback_category,
                       final_verdict::text AS verdict,
                       delivery_decision::text AS delivery_decision,
                       primary_artifact_type,
                       created_at
                FROM notification_feedback
                WHERE channel_registry_id = CAST(:channel_registry_id AS uuid)
                  AND created_at >= now() - (:window_days * INTERVAL '1 day')
                ORDER BY created_at DESC, feedback_id DESC
                LIMIT :sample_limit
                """
            ),
            {
                "channel_registry_id": str(channel_registry_id),
                "window_days": window_days,
                "sample_limit": sample_limit,
            },
        )
        observations = tuple(
            ChannelFeedbackObservation(
                feedback_category=str(row["feedback_category"]),
                verdict=str(row["verdict"]),
                delivery_decision=str(row["delivery_decision"]),
                primary_artifact_type=str(row["primary_artifact_type"] or "unknown"),
                created_at=row["created_at"],
            )
            for row in result.mappings().all()
        )
        return ChannelFeedbackSample(
            channel_fingerprint=channel_fp(channel_registry_id),
            observations=observations,
            sample_limit=sample_limit,
            window_days=window_days,
        )


async def build_default_repository(
    runtime_config: BoundedFeedbackChannelPolicyRuntimeConfig,
    state: BoundedFeedbackChannelPolicyState,
) -> FeedbackChannelPolicyRepositoryHandle:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    repository = SqlAlchemyFeedbackChannelPolicyRepository(session, state)

    async def close() -> None:
        try:
            await session.close()
        finally:
            await engine.dispose()

    return FeedbackChannelPolicyRepositoryHandle(repository=repository, close=close)


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Capture or read back bounded durable feedback and evaluate channel policy without live authority.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=(MODE_PLAN, MODE_EXECUTE), default=MODE_PLAN)
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--analysis-id-suffix")
    parser.add_argument("--candidate-group-id-suffix")
    parser.add_argument("--analysis-id")
    parser.add_argument("--notification-plan-id")
    parser.add_argument("--notification-delivery-record-id")
    parser.add_argument("--feedback-category", choices=sorted(ALLOWED_FEEDBACK_LABELS))
    parser.add_argument("--operator-action-key")
    parser.add_argument("--confirm-feedback-write", action="store_true")
    parser.add_argument("--feedback-jsonl")
    parser.add_argument("--allow-feedback-file-read", action="store_true")
    parser.add_argument("--channel-policy-json")
    parser.add_argument("--allow-channel-policy-file-read", action="store_true")
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS)
    return parser


def load_runtime_config(env: Mapping[str, str] | None = None) -> BoundedFeedbackChannelPolicyRuntimeConfig:
    source = os.environ if env is None else env
    database_url = source.get("DATABASE_URL", "").strip()
    if not database_url:
        raise BoundedFeedbackChannelPolicyError("database_url_missing")
    return BoundedFeedbackChannelPolicyRuntimeConfig(database_url=database_url)


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader: Callable[[], BoundedFeedbackChannelPolicyRuntimeConfig] = load_runtime_config,
    repository_builder: RepositoryBuilder | None = None,
) -> RunnerResult:
    config_or_report = _config_from_args(args)
    if isinstance(config_or_report, dict):
        return RunnerResult(exit_code=1, report=config_or_report)
    result = asyncio.run(
        run_bounded_feedback_channel_policy(
            config_or_report,
            runtime_config_loader=runtime_config_loader,
            repository_builder=repository_builder,
        )
    )
    return RunnerResult(exit_code=0 if result.get("status") == "pass" else 1, report=result)


async def run_bounded_feedback_channel_policy(
    config: BoundedFeedbackChannelPolicyConfig,
    *,
    runtime_config_loader: Callable[[], BoundedFeedbackChannelPolicyRuntimeConfig] = load_runtime_config,
    repository_builder: RepositoryBuilder | None = None,
) -> dict[str, Any]:
    state = BoundedFeedbackChannelPolicyState()
    gate_error = _gate_error(config)
    if gate_error is not None:
        return _base_report("blocked", gate_error, config=config, state=state)

    feedback_records: tuple[FeedbackRecord, ...] = ()
    invalid_feedback_count = 0
    invalid_reason_distribution: dict[str, int] = {}
    channel_policy_options: dict[str, Any] = {}
    channel_policy_file_status = "not_requested"
    policy_rows: list[PolicyReadbackRow] = []
    durable_capture_result: dict[str, Any] | None = None
    durable_readback_result: dict[str, Any] | None = None
    repository_handle: FeedbackChannelPolicyRepositoryHandle | None = None

    try:
        if config.feedback_jsonl:
            feedback_text = _read_limited_text(
                config.feedback_jsonl,
                max_bytes=MAX_FEEDBACK_BYTES,
                expected_suffix=".jsonl",
                state=state,
                file_kind="feedback",
            )
            parse_result = FeedbackEvalEngine().parse_jsonl(feedback_text, row_cap=config.max_rows)
            feedback_records = parse_result.records
            invalid_feedback_count = parse_result.invalid_feedback_count
            invalid_reason_distribution = parse_result.invalid_reason_distribution

        if config.channel_policy_json:
            channel_policy_file_status = "read"
            channel_policy_options = _read_channel_policy_options(config.channel_policy_json, state=state)

        if config.analysis_id_suffix or config.candidate_group_id_suffix or config.analysis_id:
            try:
                runtime_config = runtime_config_loader()
                state.runtime_config_loaded = True
            except BoundedFeedbackChannelPolicyError as exc:
                return _base_report("blocked", exc.reason_code, config=config, state=state)
            except Exception as exc:
                return _base_report(
                    "blocked",
                    "runtime_config_error",
                    config=config,
                    state=state,
                    error_class=type(exc).__name__,
                )

            repository_handle = await (repository_builder or build_default_repository)(runtime_config, state)
            if config.analysis_id_suffix or config.candidate_group_id_suffix:
                policy_rows = await repository_handle.repository.load_policy_readbacks(
                    analysis_id_suffix=config.analysis_id_suffix,
                    candidate_group_id_suffix=config.candidate_group_id_suffix,
                    max_rows=config.max_rows,
                )
                selector_error = _selector_error(config, policy_rows)
                if selector_error is not None:
                    return _base_report("blocked", selector_error, config=config, state=state)
            if config.analysis_id is not None:
                feedback_service = NotificationFeedbackService(repository_handle.repository)
                if _capture_requested(config):
                    capture = await feedback_service.capture(
                        NotificationFeedbackRequest(
                            operator_action_key=config.operator_action_key or "",
                            feedback_category=config.feedback_category or "",
                            analysis_id=config.analysis_id,
                            notification_plan_id=config.notification_plan_id,
                            notification_delivery_record_id=config.notification_delivery_record_id,
                        )
                    )
                    durable_capture_result = capture.to_sanitized_dict()
                    durable_readback_result = NotificationFeedbackReadbackResult(
                        analysis_bound=capture.analysis_bound,
                        notification_plan_bound=capture.notification_plan_bound,
                        notification_delivery_record_bound=capture.notification_delivery_record_bound,
                        candidate_group_bound=capture.candidate_group_bound,
                        channel_bound=capture.channel_bound,
                        aggregate=capture.aggregate,
                    ).to_sanitized_dict()
                else:
                    readback = await feedback_service.readback(
                        analysis_id=config.analysis_id,
                        notification_plan_id=config.notification_plan_id,
                        notification_delivery_record_id=config.notification_delivery_record_id,
                    )
                    durable_readback_result = readback.to_sanitized_dict()
    except BoundedFeedbackChannelPolicyError as exc:
        return _base_report("blocked", exc.reason_code, config=config, state=state)
    except NotificationFeedbackError as exc:
        return _base_report("blocked", exc.reason_code, config=config, state=state)
    except Exception as exc:
        return _base_report("blocked", "runner_error", config=config, state=state, error_class=type(exc).__name__)
    finally:
        if repository_handle is not None:
            await repository_handle.close()

    enriched_feedback = tuple(_attach_policy_outcomes(feedback_records, policy_rows))
    feedback_result = FeedbackEvalEngine().evaluate(
        enriched_feedback,
        invalid_feedback_count=invalid_feedback_count,
        invalid_reason_distribution=invalid_reason_distribution,
        total_feedback_count=len(enriched_feedback) + invalid_feedback_count,
    )
    channel_result = _evaluate_channel_override(
        policy_rows=policy_rows,
        channel_policy_options=channel_policy_options,
    )
    policy_distribution = _policy_distribution(policy_rows)
    report = _base_report(
        "pass",
        (
            "feedback_capture_and_channel_readback_complete"
            if durable_capture_result is not None
            else "feedback_channel_policy_readback_complete"
        ),
        config=config,
        state=state,
    )
    report.update(
        {
            "target_analysis_fingerprint": _single_id_fp(row.analysis_id for row in policy_rows),
            "target_candidate_group_fingerprint": _single_id_fp(row.candidate_group_id for row in policy_rows),
            "analysis_fingerprint": _single_id_fp(row.analysis_id for row in policy_rows),
            "candidate_group_fingerprint": _single_id_fp(row.candidate_group_id for row in policy_rows),
            "verdict": _single_value(row.verdict for row in policy_rows),
            "delivery_decision": _single_value(row.delivery_decision for row in policy_rows),
            "urgency_profile": _single_value(row.urgency_profile for row in policy_rows),
            "reason_code_buckets": policy_distribution["reason_code_buckets"],
            "primary_artifact_type": _single_value(row.primary_artifact_type for row in policy_rows) or "unknown",
            "notification_plan_count_bucket": _count_bucket(sum(row.notification_plan_count for row in policy_rows)),
            "render_count_bucket": _count_bucket(sum(row.render_count for row in policy_rows)),
            "delivery_record_count_bucket": _count_bucket(sum(row.delivery_record_count for row in policy_rows)),
            "sent_count_bucket": _count_bucket(sum(row.sent_count for row in policy_rows)),
            "suppressed_count_bucket": _count_bucket(sum(row.suppressed_count for row in policy_rows)),
            "channel_tier_observed_or_unknown": (
                durable_readback_result["aggregate"]["channel_tier"]
                if durable_readback_result is not None
                else _observed_channel_tier(policy_rows) or "unknown"
            ),
            "channel_context_status": (
                "durable_feedback_bound"
                if durable_readback_result is not None
                and durable_readback_result["identity_binding"]["channel_bound"] is True
                else "durable_feedback_unbound"
                if durable_readback_result is not None
                else "unavailable_in_current_schema"
            ),
            "durable_feedback_capture": durable_capture_result,
            "durable_feedback_readback": durable_readback_result,
            "feedback_distribution": feedback_result.to_sanitized_dict(),
            "usefulness_score_bucket": feedback_result.usefulness_score_average_bucket,
            "false_positive_bucket": feedback_result.false_positive_count_bucket,
            "false_negative_bucket": feedback_result.false_negative_count_bucket,
            "delivery_distribution": feedback_result.delivery_distribution,
            "policy_distribution": policy_distribution,
            "channel_override_result": channel_result["channel_override_result"],
            "text_idea_channel_control_result": channel_result["text_idea_channel_control_result"],
            "ai_noise_calibration_result": channel_result["ai_noise_calibration_result"],
            "channel_policy_file_status": channel_policy_file_status,
        }
    )
    return report


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader: Callable[[], BoundedFeedbackChannelPolicyRuntimeConfig] = load_runtime_config,
    repository_builder: RepositoryBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(argument_error_report(str(exc))))
        return 1
    result = run(args, runtime_config_loader=runtime_config_loader, repository_builder=repository_builder)
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


def _config_from_args(args: argparse.Namespace) -> BoundedFeedbackChannelPolicyConfig | dict[str, Any]:
    analysis_suffix = _parse_optional_suffix(args.analysis_id_suffix)
    if isinstance(analysis_suffix, dict):
        return argument_error_report("invalid_analysis_id_suffix")
    candidate_suffix = _parse_optional_suffix(args.candidate_group_id_suffix)
    if isinstance(candidate_suffix, dict):
        return argument_error_report("invalid_candidate_group_id_suffix")
    try:
        max_rows = int(args.max_rows)
    except (TypeError, ValueError):
        return argument_error_report("invalid_max_rows")
    analysis_id = _parse_optional_uuid(args.analysis_id)
    if isinstance(analysis_id, dict):
        return argument_error_report("invalid_analysis_id")
    notification_plan_id = _parse_optional_uuid(args.notification_plan_id)
    if isinstance(notification_plan_id, dict):
        return argument_error_report("invalid_notification_plan_id")
    delivery_record_id = _parse_optional_uuid(args.notification_delivery_record_id)
    if isinstance(delivery_record_id, dict):
        return argument_error_report("invalid_notification_delivery_record_id")
    return BoundedFeedbackChannelPolicyConfig(
        mode=str(args.mode),
        operator_approved=bool(args.operator_approved),
        allow_runtime_config=bool(args.allow_runtime_config),
        allow_database_read=bool(args.allow_database_read),
        allow_database_write=bool(args.allow_database_write),
        analysis_id_suffix=analysis_suffix,
        candidate_group_id_suffix=candidate_suffix,
        analysis_id=analysis_id,
        notification_plan_id=notification_plan_id,
        notification_delivery_record_id=delivery_record_id,
        feedback_category=args.feedback_category,
        operator_action_key=args.operator_action_key,
        confirm_feedback_write=bool(args.confirm_feedback_write),
        feedback_jsonl=args.feedback_jsonl,
        allow_feedback_file_read=bool(args.allow_feedback_file_read),
        channel_policy_json=args.channel_policy_json,
        allow_channel_policy_file_read=bool(args.allow_channel_policy_file_read),
        max_rows=max_rows,
    )


def _gate_error(config: BoundedFeedbackChannelPolicyConfig) -> str | None:
    if config.mode not in {MODE_PLAN, MODE_EXECUTE}:
        return "invalid_mode"
    if not 1 <= config.max_rows <= HARD_MAX_ROWS:
        return "invalid_max_rows"
    capture_requested = _capture_requested(config)
    if config.analysis_id is not None and (config.analysis_id_suffix or config.candidate_group_id_suffix):
        return "mixed_feedback_selector_modes"
    target_requested = (
        config.analysis_id is not None
        or config.notification_plan_id is not None
        or config.notification_delivery_record_id is not None
    )
    if not (
        config.analysis_id_suffix
        or config.candidate_group_id_suffix
        or config.feedback_jsonl
        or target_requested
        or capture_requested
    ):
        return "exact_selector_missing"
    if config.mode == MODE_EXECUTE and not config.operator_approved:
        return "operator_approval_missing"
    if config.feedback_jsonl and not config.allow_feedback_file_read:
        return "feedback_file_read_not_allowed"
    if config.channel_policy_json and not config.allow_channel_policy_file_read:
        return "channel_policy_file_read_not_allowed"
    if (config.analysis_id_suffix or config.candidate_group_id_suffix) and not config.allow_database_read:
        return "database_read_not_allowed"
    if (config.analysis_id_suffix or config.candidate_group_id_suffix) and not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if (config.notification_plan_id or config.notification_delivery_record_id) and config.analysis_id is None:
        return "feedback_target_analysis_required"
    if config.analysis_id is not None:
        if not config.allow_runtime_config:
            return "runtime_config_not_allowed"
        if not config.allow_database_read:
            return "database_read_not_allowed"
    if capture_requested:
        if config.analysis_id is None or not config.feedback_category or not config.operator_action_key:
            return "feedback_capture_fields_missing"
        if config.mode != MODE_EXECUTE:
            return "feedback_capture_execute_mode_required"
        if not config.allow_runtime_config:
            return "runtime_config_not_allowed"
        if not config.allow_database_read:
            return "database_read_not_allowed"
        if not config.allow_database_write:
            return "database_write_not_allowed"
        if not config.confirm_feedback_write:
            return "feedback_write_confirmation_missing"
    return None


def _selector_error(config: BoundedFeedbackChannelPolicyConfig, rows: list[PolicyReadbackRow]) -> str | None:
    if not rows:
        return "target_not_found"
    if len(rows) > config.max_rows:
        return "row_cap_exceeded"
    if config.analysis_id_suffix and len({row.analysis_id for row in rows if row.analysis_id}) > 1:
        return "ambiguous_analysis_id_suffix"
    if config.candidate_group_id_suffix and len({row.candidate_group_id for row in rows if row.candidate_group_id}) > 1:
        return "ambiguous_candidate_group_id_suffix"
    return None


def _read_limited_text(
    path_text: str,
    *,
    max_bytes: int,
    expected_suffix: str,
    state: BoundedFeedbackChannelPolicyState,
    file_kind: str,
) -> str:
    path = Path(path_text)
    if expected_suffix and path.suffix != expected_suffix:
        raise BoundedFeedbackChannelPolicyError(f"{file_kind}_file_extension_not_allowed")
    lowered_parts = {part.lower() for part in path.parts}
    if {".env", "runtime.env"} & lowered_parts:
        raise BoundedFeedbackChannelPolicyError(f"{file_kind}_file_not_allowed")
    if file_kind == "feedback":
        state.feedback_file_read_attempted = True
    elif file_kind == "channel_policy":
        state.channel_policy_file_read_attempted = True
    stat = path.stat()
    if stat.st_size > max_bytes:
        raise BoundedFeedbackChannelPolicyError(f"{file_kind}_file_too_large")
    return path.read_text(encoding="utf-8")


def _read_channel_policy_options(path_text: str, *, state: BoundedFeedbackChannelPolicyState) -> dict[str, Any]:
    text = _read_limited_text(
        path_text,
        max_bytes=MAX_CHANNEL_POLICY_BYTES,
        expected_suffix=".json",
        state=state,
        file_kind="channel_policy",
    )
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise BoundedFeedbackChannelPolicyError("channel_policy_invalid_json") from exc
    if not isinstance(payload, dict):
        raise BoundedFeedbackChannelPolicyError("channel_policy_invalid_json_object")
    tier = str(payload.get("default_channel_tier") or payload.get("channel_tier") or "B").upper()
    if tier not in {"A", "B", "C"}:
        tier = "B"
    return {
        "default_channel_tier": tier,
        "text_idea_enabled": bool(payload.get("text_idea_enabled", True)),
    }


def _attach_policy_outcomes(
    feedback_records: tuple[FeedbackRecord, ...],
    policy_rows: list[PolicyReadbackRow],
) -> list[FeedbackRecord]:
    enriched: list[FeedbackRecord] = []
    for record in feedback_records:
        matched = _matching_policy_row(record, policy_rows)
        if matched is None:
            enriched.append(record)
        else:
            enriched.append(
                record.with_policy_outcome(
                    verdict=matched.verdict,
                    delivery_decision=matched.delivery_decision,
                    urgency_profile=matched.urgency_profile,
                )
            )
    return enriched


def _matching_policy_row(record: FeedbackRecord, policy_rows: list[PolicyReadbackRow]) -> PolicyReadbackRow | None:
    for row in policy_rows:
        if record.analysis_id_suffix and row.analysis_id and row.analysis_id.endswith(record.analysis_id_suffix):
            return row
        if (
            record.candidate_group_id_suffix
            and row.candidate_group_id
            and row.candidate_group_id.endswith(record.candidate_group_id_suffix)
        ):
            return row
    return policy_rows[0] if len(policy_rows) == 1 else None


def _evaluate_channel_override(
    *,
    policy_rows: list[PolicyReadbackRow],
    channel_policy_options: Mapping[str, Any],
) -> dict[str, Any]:
    row = policy_rows[0] if policy_rows else None
    tier = str(channel_policy_options.get("default_channel_tier") or "B")
    reason_codes = row.reason_codes if row else ()
    ai_noise_signal_count = _ai_noise_signal_count(reason_codes)
    artifact_type = row.primary_artifact_type if row else "unknown"
    external_evidence_present = artifact_type not in {"unknown", "text_idea"}
    result = ChannelOverridePolicy().evaluate(
        ChannelOverrideInput(
            channel_tier=tier,
            artifact_type=artifact_type,
            verdict=row.verdict if row else "skip",
            delivery_decision=row.delivery_decision if row else "suppress",
            urgency_profile=row.urgency_profile if row else "suppressed",
            reason_codes=reason_codes,
            text_idea_enabled=bool(channel_policy_options.get("text_idea_enabled", True)),
            ai_noise_signal_count=ai_noise_signal_count,
            external_evidence_present=external_evidence_present,
        )
    )
    return {
        "channel_override_result": result.to_sanitized_dict(),
        "text_idea_channel_control_result": {
            "decision": result.decision,
            "text_idea_enabled_after": result.text_idea_enabled_after,
            "reason_codes": list(result.reason_codes),
        },
        "ai_noise_calibration_result": {
            "ai_noise_signal_count_bucket": _count_bucket(ai_noise_signal_count),
            "recommendation": "increase_suppression" if ai_noise_signal_count >= 2 else "keep_current",
        },
    }


def _policy_distribution(rows: list[PolicyReadbackRow]) -> dict[str, Any]:
    verdicts = {"inspect_now": 0, "later": 0, "skip": 0}
    delivery = {"send_now": 0, "send_digest": 0, "suppress": 0}
    urgency = {"high": 0, "normal_silent": 0, "digest": 0, "suppressed": 0}
    reason_codes: dict[str, int] = {}
    for row in rows:
        if row.verdict in verdicts:
            verdicts[row.verdict] += 1
        if row.delivery_decision in delivery:
            delivery[row.delivery_decision] += 1
        if row.urgency_profile in urgency:
            urgency[row.urgency_profile] += 1
        for reason_code in row.reason_codes:
            bucket = _reason_code_bucket(reason_code)
            reason_codes[bucket] = reason_codes.get(bucket, 0) + 1
    return {
        "verdict": verdicts,
        "delivery_decision": delivery,
        "urgency_profile": urgency,
        "reason_code_buckets": dict(sorted(reason_codes.items())),
    }


def _base_report(
    status: str,
    reason_code: str,
    *,
    config: BoundedFeedbackChannelPolicyConfig,
    state: BoundedFeedbackChannelPolicyState,
    error_class: str | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": config.mode,
        "ok": status == "pass",
        "status": status,
        "reason_code": reason_code,
        "error_code": None if status == "pass" else reason_code,
        "error_class": error_class,
        "target_analysis_fingerprint": None,
        "target_candidate_group_fingerprint": None,
        "policy_distribution": _policy_distribution([]),
        "feedback_distribution": FeedbackEvalEngine().evaluate(()).to_sanitized_dict(),
        "durable_feedback_capture": None,
        "durable_feedback_readback": None,
        "usefulness_score_bucket": "none",
        "false_positive_bucket": "zero",
        "false_negative_bucket": "zero",
        "delivery_distribution": FeedbackEvalEngine().evaluate(()).delivery_distribution,
        "channel_override_result": {
            "decision": "not_evaluated",
            "reason_codes": [],
            "simulation_result": "not_evaluated",
        },
        "text_idea_channel_control_result": {
            "decision": "not_evaluated",
            "text_idea_enabled_after": None,
            "reason_codes": [],
        },
        "ai_noise_calibration_result": {
            "ai_noise_signal_count_bucket": "zero",
            "recommendation": "not_evaluated",
        },
        "authority": {
            "operator_approved": config.operator_approved,
            "runtime_config_allowed": config.allow_runtime_config,
            "database_read_allowed": config.allow_database_read,
            "database_write_allowed": config.allow_database_write,
            "feedback_file_read_allowed": config.allow_feedback_file_read,
            "channel_policy_file_read_allowed": config.allow_channel_policy_file_read,
            "redis_allowed": False,
            "telegram_allowed": False,
            "openai_allowed": False,
            "external_network_allowed": False,
            "max_rows": config.max_rows,
        },
        "side_effects": {
            "runtime_config_loaded": state.runtime_config_loaded,
            "database_session_opened": state.database_session_opened,
            "database_read_attempted": state.database_read_attempted,
            "database_write_attempted": state.database_write_attempted,
            "feedback_file_read_attempted": state.feedback_file_read_attempted,
            "channel_policy_file_read_attempted": state.channel_policy_file_read_attempted,
            "redis_called": state.redis_called,
            "telegram_called": state.telegram_called,
            "openai_called": state.openai_called,
            "external_network_called": state.external_network_called,
        },
        "redactions_applied": {
            "full_ids_omitted": True,
            "raw_source_text_omitted": True,
            "raw_urls_omitted": True,
            "raw_feedback_notes_omitted": True,
            "raw_chat_ids_omitted": True,
            "dedupe_keys_omitted": True,
            "material_change_hash_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "env_values_omitted": True,
            "exception_body_omitted": True,
            "traceback_omitted": True,
        },
        "raw_values_printed": False,
    }


def _parse_optional_suffix(value: str | None) -> str | None | dict[str, Any]:
    if value is None:
        return None
    normalized = value.strip().lower()
    if UUID_SUFFIX_RE.fullmatch(normalized):
        return normalized
    return argument_error_report("invalid_suffix")


def _parse_optional_uuid(value: str | None) -> UUID | None | dict[str, Any]:
    if value is None:
        return None
    try:
        return UUID(value)
    except (TypeError, ValueError, AttributeError):
        return argument_error_report("invalid_uuid")


def _capture_requested(config: BoundedFeedbackChannelPolicyConfig) -> bool:
    return any(
        value is not None and value is not False
        for value in (
            config.feedback_category,
            config.operator_action_key,
            config.confirm_feedback_write,
            config.allow_database_write,
        )
    )


def _stored_feedback_from_row(row: Mapping[str, Any]) -> StoredNotificationFeedback:
    return StoredNotificationFeedback(
        feedback_id=UUID(str(row["feedback_id"])),
        operator_action_key=str(row["operator_action_key"]),
        feedback_category=str(row["feedback_category"]),
        analysis_id=UUID(str(row["analysis_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        notification_plan_id=_uuid_or_none(row["notification_plan_id"]),
        notification_delivery_record_id=_uuid_or_none(row["notification_delivery_record_id"]),
        channel_registry_id=_uuid_or_none(row["channel_registry_id"]),
        verdict=str(row["verdict"]),
        delivery_decision=str(row["delivery_decision"]),
        primary_artifact_type=str(row["primary_artifact_type"] or "unknown"),
    )


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _safe_reason_codes(values: Any) -> tuple[str, ...]:
    return tuple(_safe_reason_code(value) for value in _json_list(values))


def _safe_reason_code(value: Any) -> str:
    text = str(value or "").strip().lower()
    if (
        text
        and text[0].isalpha()
        and all(char.isalnum() or char in {"_", "-", ":"} for char in text)
        and len(text) <= 80
    ):
        return text
    return "unsafe_reason_code"


def _json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _safe_urgency_profile(value: Any, verdict: str, delivery_decision: str) -> str:
    text = str(value or "")
    if text in {"high", "normal_silent", "digest", "suppressed"}:
        return text
    if delivery_decision == "suppress":
        return "suppressed"
    if verdict == "inspect_now":
        return "high"
    if delivery_decision == "send_digest":
        return "digest"
    return "normal_silent"


def _single_id_fp(values: Any) -> str | None:
    unique = sorted({str(value) for value in values if value})
    if len(unique) != 1:
        return None
    return _fp_for_value(unique[0])


def _single_value(values: Any) -> str | None:
    unique = sorted({str(value) for value in values if value})
    return unique[0] if len(unique) == 1 else None


def _fp_for_value(value: str) -> str:
    digest = hashlib.sha256(f"github-ai-catchbot:{value}".encode("utf-8")).hexdigest()
    return f"fp_{digest[:12]}"


def _reason_code_bucket(value: Any) -> str:
    code = _safe_reason_code(value)
    if code == "unsafe_reason_code":
        return code
    if "ai_noise" in code or "ai_only" in code or "weak_ai" in code or "generic_ai" in code:
        return "ai_noise_reason"
    if "duplicate" in code:
        return "duplicate_reason"
    if "hype" in code:
        return "hype_reason"
    if "evidence" in code:
        return "evidence_reason"
    if code.startswith("policy_threshold"):
        return "policy_threshold_reason"
    if code.startswith("channel_policy"):
        return "channel_policy_reason"
    return "other_reason_code"


def _observed_channel_tier(rows: list[PolicyReadbackRow]) -> str | None:
    tiers = sorted({row.channel_tier for row in rows if row.channel_tier in {"A", "B", "C"}})
    return tiers[0] if len(tiers) == 1 else None


def _ai_noise_signal_count(reason_codes: tuple[str, ...]) -> int:
    noise_terms = ("ai_noise", "ai_only", "generic_ai", "weak_ai", "bad_channel_fit")
    return sum(1 for code in reason_codes if any(term in code for term in noise_terms))


def _count_bucket(count: int) -> str:
    if count <= 0:
        return "zero"
    if count == 1:
        return "one"
    if count <= 5:
        return "two_to_five"
    if count <= 20:
        return "six_to_twenty"
    return "over_twenty"


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True) + "\n"


def argument_error_report(reason_code: str) -> dict[str, Any]:
    return _base_report(
        "blocked",
        reason_code,
        config=BoundedFeedbackChannelPolicyConfig(),
        state=BoundedFeedbackChannelPolicyState(),
    )


__all__ = [
    "BoundedFeedbackChannelPolicyConfig",
    "BoundedFeedbackChannelPolicyRuntimeConfig",
    "FeedbackChannelPolicyRepositoryHandle",
    "PolicyReadbackRow",
    "RunnerResult",
    "argument_error_report",
    "build_parser",
    "main",
    "render_sanitized_json",
    "run",
    "run_bounded_feedback_channel_policy",
]


if __name__ == "__main__":
    raise SystemExit(main())
