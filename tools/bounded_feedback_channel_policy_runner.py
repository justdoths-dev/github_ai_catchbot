from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Protocol, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.policy_engine.channel_override_policy import ChannelOverrideInput, ChannelOverridePolicy
from src.services.policy_engine.feedback_eval import FeedbackEvalEngine, FeedbackRecord


SCHEMA_VERSION = "bounded_feedback_channel_policy_runner_v1"
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
    analysis_id_suffix: str | None = None
    candidate_group_id_suffix: str | None = None
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
        description="Read one exact policy target plus local operator feedback and simulate channel override policy.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=(MODE_PLAN, MODE_EXECUTE), default=MODE_PLAN)
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--analysis-id-suffix")
    parser.add_argument("--candidate-group-id-suffix")
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

        if config.analysis_id_suffix or config.candidate_group_id_suffix:
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
            policy_rows = await repository_handle.repository.load_policy_readbacks(
                analysis_id_suffix=config.analysis_id_suffix,
                candidate_group_id_suffix=config.candidate_group_id_suffix,
                max_rows=config.max_rows,
            )
            selector_error = _selector_error(config, policy_rows)
            if selector_error is not None:
                return _base_report("blocked", selector_error, config=config, state=state)
    except BoundedFeedbackChannelPolicyError as exc:
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
        feedback_records=enriched_feedback,
        channel_policy_options=channel_policy_options,
    )
    policy_distribution = _policy_distribution(policy_rows)
    report = _base_report(
        "pass",
        "feedback_channel_policy_readback_complete",
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
            "channel_tier_observed_or_unknown": _observed_channel_tier(policy_rows) or "unknown",
            "channel_context_status": "unavailable_in_current_schema",
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
    return BoundedFeedbackChannelPolicyConfig(
        mode=str(args.mode),
        operator_approved=bool(args.operator_approved),
        allow_runtime_config=bool(args.allow_runtime_config),
        allow_database_read=bool(args.allow_database_read),
        analysis_id_suffix=analysis_suffix,
        candidate_group_id_suffix=candidate_suffix,
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
    if not (config.analysis_id_suffix or config.candidate_group_id_suffix or config.feedback_jsonl):
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
    feedback_records: tuple[FeedbackRecord, ...],
    channel_policy_options: Mapping[str, Any],
) -> dict[str, Any]:
    row = policy_rows[0] if policy_rows else None
    feedback_tier = next((record.channel_tier for record in feedback_records if record.channel_tier), None)
    tier = feedback_tier or str(channel_policy_options.get("default_channel_tier") or "B")
    reason_codes = row.reason_codes if row else ()
    ai_noise_signal_count = _ai_noise_signal_count(reason_codes, feedback_records)
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
            "feedback_file_read_allowed": config.allow_feedback_file_read,
            "channel_policy_file_read_allowed": config.allow_channel_policy_file_read,
            "database_write_allowed": False,
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


def _ai_noise_signal_count(reason_codes: tuple[str, ...], feedback_records: tuple[FeedbackRecord, ...]) -> int:
    noise_terms = ("ai_noise", "ai_only", "generic_ai", "weak_ai", "bad_channel_fit")
    from_reason_codes = sum(1 for code in reason_codes if any(term in code for term in noise_terms))
    from_feedback = sum(1 for record in feedback_records if record.label in {"hype", "bad_channel_fit"})
    return from_reason_codes + from_feedback


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
