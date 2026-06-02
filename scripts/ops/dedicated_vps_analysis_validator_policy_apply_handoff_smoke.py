from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

import sqlalchemy as sa

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.services.analysis_validator.business_rules import AnalysisValidatorBusinessRules  # noqa: E402
from src.services.analysis_validator.config import AnalysisValidatorConfig  # noqa: E402
from src.services.analysis_validator.models import (  # noqa: E402
    BundleValidationContext,
    JudgeOutputReadyJob,
    JudgeOutputRecord,
    JudgeRunValidationRecord,
)
from src.services.analysis_validator.repositories import AnalysisValidatorRepository  # noqa: E402
from src.services.analysis_validator.schema_registry import JudgeOutputSchemaRegistry  # noqa: E402
from src.services.analysis_validator.service import AnalysisValidatorService  # noqa: E402


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_analysis_validator_policy_apply_handoff_smoke"
REPORT_TYPE = "analysis_validator_policy_apply_handoff_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
READY_EVENT_TYPE = "judge.output.ready.v1"
POLICY_APPLY_EVENT_TYPE = "analysis.policy.apply.v1"
REPLAY_PROMPT_SUFFIX = "__replay_live_smoke_v1"

STATUS_PREFLIGHT_PASSED = "analysis_validator_policy_apply_handoff_smoke_default_passed"
STATUS_DB_PREFLIGHT_PASSED = (
    "analysis_validator_policy_apply_handoff_smoke_db_read_preflight_passed"
)
STATUS_DB_WRITE_PASSED = (
    "analysis_validator_policy_apply_handoff_smoke_approved_db_write_passed"
)
STATUS_NOT_APPROVED = "blocked_analysis_validator_policy_apply_handoff_smoke_not_approved"
STATUS_DB_READ_FAILED = "blocked_analysis_validator_policy_apply_handoff_smoke_db_read_failed"
STATUS_VALIDATION_FAILED = (
    "blocked_analysis_validator_policy_apply_handoff_smoke_validation_failed"
)
STATUS_DUPLICATE_POLICY_APPLY = (
    "blocked_analysis_validator_policy_apply_handoff_smoke_existing_policy_apply"
)
STATUS_FORBIDDEN_SIDE_EFFECT = (
    "blocked_analysis_validator_policy_apply_handoff_smoke_forbidden_side_effect"
)
STATUS_WRITE_FAILED = "blocked_analysis_validator_policy_apply_handoff_smoke_write_failed"
STATUS_RAW_VALUE_EMISSION = (
    "blocked_analysis_validator_policy_apply_handoff_smoke_raw_value_emission"
)

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
SELECT_ELIGIBLE_READY_EVENT_QUERY = """
SELECT eo.event_id AS trigger_event_id,
       eo.created_at AS ready_event_created_at,
       jr.judge_run_id,
       jr.bundle_id,
       jo.judge_output_id,
       jo.candidate_group_id,
       jr.status AS judge_run_status,
       jr.finish_reason,
       jr.refusal_detected,
       jr.prompt_version,
       CASE
           WHEN jr.prompt_version LIKE :replay_prompt_like THEN 'live_replay_persistence_smoke'
           ELSE 'newest_ready_event'
       END AS target_source
FROM event_outbox eo
JOIN judge_runs jr
  ON jr.judge_run_id = eo.aggregate_id
JOIN judge_outputs jo
  ON jo.judge_run_id = jr.judge_run_id
 AND jo.judge_output_id::text = eo.payload_json->>'judge_output_id'
JOIN candidate_evidence_bundles b
  ON b.bundle_id = jr.bundle_id
 AND b.candidate_group_id = jo.candidate_group_id
WHERE eo.event_type = 'judge.output.ready.v1'
  AND eo.aggregate_type = 'judge_run'
  AND jr.status = 'succeeded'
  AND eo.payload_json->>'judge_run_id' = jr.judge_run_id::text
ORDER BY (jr.prompt_version LIKE :replay_prompt_like) DESC,
         eo.created_at DESC NULLS LAST,
         eo.event_id DESC
LIMIT 1
"""
COUNT_POLICY_OUTBOX_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'analysis.policy.apply.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
  AND payload_json->>'judge_output_id' = :judge_output_id
"""
COUNT_ANALYSES_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM analyses a
JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
"""
COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM notification_plans np
JOIN analyses a ON a.analysis_id = np.analysis_id
JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
WHERE jo.judge_run_id = CAST(:judge_run_id AS uuid)
"""
COUNT_STATE_TRANSITIONS_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM state_transitions
WHERE object_type = 'judge_run'
  AND object_id = CAST(:judge_run_id AS uuid)
"""
COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM judge_outputs
WHERE judge_run_id = CAST(:judge_run_id AS uuid)
"""

DATABASE_ENV_KEYS = frozenset({"DATABASE_URL"})
PUBLIC_LITERAL_VALUES = frozenset(
    {
        SCRIPT_NAME,
        REPORT_TYPE,
        READY_EVENT_TYPE,
        POLICY_APPLY_EVENT_TYPE,
        "judge_run",
        "succeeded",
        "completed",
        "zero",
        "one",
        "multiple",
        "true",
        "false",
        "disabled",
        "live_replay_persistence_smoke",
        "newest_ready_event",
    }
)


class AsyncSessionLike(Protocol):
    def in_transaction(self) -> bool: ...
    def begin(self) -> Any: ...
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def close(self) -> None: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class HandoffTarget:
    trigger_event_id: UUID
    judge_run_id: UUID
    judge_output_id: UUID
    bundle_id: UUID
    candidate_group_id: UUID
    target_source: str


@dataclass(frozen=True, slots=True)
class PreflightContext:
    target: HandoffTarget
    job: JudgeOutputReadyJob
    judge_run: JudgeRunValidationRecord
    judge_output: JudgeOutputRecord
    bundle: BundleValidationContext


@dataclass(frozen=True, slots=True)
class TargetCounts:
    policy_apply: int
    analyses: int
    notifications: int
    state_transitions: int
    judge_outputs: int


class _DefaultDatabaseSession:
    def __init__(self, engine: Any, session: Any) -> None:
        self._engine = engine
        self._session = session

    def in_transaction(self) -> bool:
        return bool(self._session.in_transaction())

    def begin(self) -> Any:
        return self._session.begin()

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any:
        return await self._session.execute(statement, params or {})

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()
        await self._engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Approval-gated analysis-validator handoff smoke from persisted "
            "judge.output.ready.v1 to persisted analysis.policy.apply.v1. "
            "Default mode reads no runtime env, DB, Redis, key material, or OpenAI."
        )
    )
    parser.add_argument("--approve-db-read", action="store_true")
    parser.add_argument("--approve-db-write", action="store_true")
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def _base_report(*, approve_db_read: bool, approve_db_write: bool) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_PREFLIGHT_PASSED,
        "approvals": {
            "db_read": approve_db_read,
            "db_write": approve_db_write,
        },
        "runtime_env_read": False,
        "database_configured": False,
        "database_connected": False,
        "read_only_transaction": False,
        "database_write_attempted": False,
        "redis_connected": False,
        "redis_write_attempted": False,
        "redis_ack_attempted": False,
        "redis_delete_or_trim_attempted": False,
        "openai_call_attempted": False,
        "openai_key_read_bucket": "zero",
        "target_ready_event_found_bucket": "zero",
        "target_source_bucket": "zero",
        "judge_run_found_bucket": "zero",
        "judge_run_succeeded_bucket": "zero",
        "judge_output_found_bucket": "zero",
        "bundle_found_bucket": "zero",
        "output_schema_valid_bucket": "zero",
        "business_rules_valid_bucket": "zero",
        "existing_analysis_policy_apply_outbox_for_run_bucket": "zero",
        "existing_analysis_rows_for_run_bucket": "zero",
        "existing_notification_rows_for_run_bucket": "zero",
        "validator_state_transitions_written_bucket": "zero",
        "analysis_policy_apply_outbox_written_bucket": "zero",
        "analysis_rows_written_bucket": "zero",
        "notification_rows_written_bucket": "zero",
        "judge_outputs_written_bucket": "zero",
        "judge_outputs_mutation_attempted": False,
        "policy_engine_started": False,
        "notifier_started": False,
        "telegram_send_attempted": False,
        "service_path_reused": "AnalysisValidatorService.handle_job",
        "stops_at_event_type": POLICY_APPLY_EVENT_TYPE,
        "raw_values_emitted": False,
        "checks_failed": [],
    }


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _bucket_count(count: int) -> str:
    if count <= 0:
        return "zero"
    if count == 1:
        return "one"
    return "multiple"


def parse_runtime_env_text(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if key in DATABASE_ENV_KEYS and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            values[key] = _strip_optional_quotes(raw_value)
    return values


def parse_runtime_env_file(path: str | Path) -> dict[str, str]:
    values: dict[str, str] = {}
    with Path(path).open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, raw_value = line.split("=", 1)
            key = key.strip()
            if key == "DATABASE_URL" and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                values[key] = _strip_optional_quotes(raw_value)
    return values


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def _database_url_is_supported(database_url: str) -> bool:
    match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", database_url)
    if not match:
        return False
    scheme = match.group(1).lower()
    return scheme == "postgresql" or scheme.startswith("postgresql+")


def _transaction_read_only_enabled(raw_value: Any) -> bool:
    return str(raw_value).strip().lower() in {"on", "true", "1", "yes"}


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


async def _open_default_database_session(database_url: str) -> AsyncSessionLike:
    from sqlalchemy.ext.asyncio import (  # type: ignore[import-not-found]
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    engine = create_async_engine(database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return _DefaultDatabaseSession(engine, session_factory())


async def _open_database_session(
    database_url: str,
    database_session_factory: DatabaseSessionFactory | None,
) -> AsyncSessionLike:
    if database_session_factory is not None:
        return await _maybe_await(database_session_factory(database_url))
    return await _open_default_database_session(database_url)


async def _execute(
    session: AsyncSessionLike,
    statement: str,
    params: dict[str, Any] | None = None,
) -> Any:
    return await session.execute(sa.text(statement), params or {})


def _rows(result: Any) -> list[Any]:
    if hasattr(result, "mappings"):
        mappings = result.mappings()
        if hasattr(mappings, "all"):
            return list(mappings.all())
    if hasattr(result, "fetchall"):
        return list(result.fetchall())
    if isinstance(result, list):
        return result
    return list(result)


def _first_mapping(result: Any) -> Mapping[str, Any] | None:
    if hasattr(result, "mappings"):
        mappings = result.mappings()
        if hasattr(mappings, "first"):
            return mappings.first()
        rows = list(mappings.all())
        return rows[0] if rows else None
    rows = _rows(result)
    if not rows:
        return None
    first = rows[0]
    if hasattr(first, "_mapping"):
        return first._mapping
    return first if isinstance(first, Mapping) else None


def _scalar(result: Any) -> Any:
    if hasattr(result, "scalar_one_or_none"):
        return result.scalar_one_or_none()
    if hasattr(result, "scalar"):
        return result.scalar()
    if hasattr(result, "scalar_one"):
        return result.scalar_one()
    rows = _rows(result)
    if not rows:
        return None
    first = rows[0]
    if isinstance(first, (tuple, list)):
        return first[0] if first else None
    if hasattr(first, "_mapping"):
        return next(iter(first._mapping.values()))
    if isinstance(first, Mapping):
        return next(iter(first.values()))
    return first


def _safe_count(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _coerce_uuid(value: Any) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _raw_values(*values: Any) -> set[str]:
    raw: set[str] = set()
    for value in values:
        if value is None:
            continue
        if isinstance(value, Mapping):
            raw.update(_raw_values(*value.values()))
            continue
        if isinstance(value, (list, tuple, set)):
            raw.update(_raw_values(*value))
            continue
        text = str(value)
        if len(text) >= 6 and text not in PUBLIC_LITERAL_VALUES:
            raw.add(text)
    return raw


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(value in rendered for value in raw_values if len(value) >= 6)


async def _count_query(
    session: AsyncSessionLike,
    query: str,
    params: dict[str, Any] | None = None,
) -> int:
    return _safe_count(_scalar(await _execute(session, query, params or {})))


async def _select_target(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    raw_values: set[str],
) -> HandoffTarget | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_ELIGIBLE_READY_EVENT_QUERY,
            {"replay_prompt_like": f"%{REPLAY_PROMPT_SUFFIX}"},
        )
    )
    report["target_ready_event_found_bucket"] = "one" if row is not None else "zero"
    if row is None:
        return None

    raw_values.update(_raw_values(row))
    trigger_event_id = _coerce_uuid(row.get("trigger_event_id"))
    judge_run_id = _coerce_uuid(row.get("judge_run_id"))
    judge_output_id = _coerce_uuid(row.get("judge_output_id"))
    bundle_id = _coerce_uuid(row.get("bundle_id"))
    candidate_group_id = _coerce_uuid(row.get("candidate_group_id"))
    if (
        trigger_event_id is None
        or judge_run_id is None
        or judge_output_id is None
        or bundle_id is None
        or candidate_group_id is None
    ):
        _set_status(report, STATUS_DB_READ_FAILED, "target.identity")
        return None

    target_source = str(row.get("target_source") or "newest_ready_event")
    report["target_source_bucket"] = (
        target_source if target_source in {"live_replay_persistence_smoke", "newest_ready_event"} else "unknown"
    )
    return HandoffTarget(
        trigger_event_id=trigger_event_id,
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
        bundle_id=bundle_id,
        candidate_group_id=candidate_group_id,
        target_source=target_source,
    )


def _validator_config(database_url: str) -> AnalysisValidatorConfig:
    return AnalysisValidatorConfig(
        app_env="prod",
        database_url=database_url,
        redis_url="redis://disabled.invalid/0",
        queue_name="q.analysis.validate",
        consumer_group="analysis-validator",
        consumer_name=SCRIPT_NAME,
        batch_size=1,
        block_ms=1,
        max_headline_chars=200,
        max_summary_chars=1200,
        max_text_items=10,
        log_level="INFO",
    )


async def _load_counts(session: AsyncSessionLike, target: HandoffTarget) -> TargetCounts:
    params = {
        "judge_run_id": str(target.judge_run_id),
        "judge_output_id": str(target.judge_output_id),
    }
    return TargetCounts(
        policy_apply=await _count_query(session, COUNT_POLICY_OUTBOX_FOR_TARGET_QUERY, params),
        analyses=await _count_query(session, COUNT_ANALYSES_FOR_RUN_QUERY, params),
        notifications=await _count_query(session, COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY, params),
        state_transitions=await _count_query(session, COUNT_STATE_TRANSITIONS_FOR_RUN_QUERY, params),
        judge_outputs=await _count_query(session, COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY, params),
    )


def _merge_existing_count_report(report: dict[str, Any], counts: TargetCounts) -> None:
    report["existing_analysis_policy_apply_outbox_for_run_bucket"] = _bucket_count(
        counts.policy_apply
    )
    report["existing_analysis_rows_for_run_bucket"] = _bucket_count(counts.analyses)
    report["existing_notification_rows_for_run_bucket"] = _bucket_count(counts.notifications)


def _merge_written_count_report(
    report: dict[str, Any],
    *,
    before: TargetCounts,
    after: TargetCounts,
) -> None:
    report["validator_state_transitions_written_bucket"] = _bucket_count(
        after.state_transitions - before.state_transitions
    )
    report["analysis_policy_apply_outbox_written_bucket"] = _bucket_count(
        after.policy_apply - before.policy_apply
    )
    report["analysis_rows_written_bucket"] = _bucket_count(after.analyses - before.analyses)
    report["notification_rows_written_bucket"] = _bucket_count(
        after.notifications - before.notifications
    )
    report["judge_outputs_written_bucket"] = _bucket_count(after.judge_outputs - before.judge_outputs)
    report["judge_outputs_mutation_attempted"] = after.judge_outputs != before.judge_outputs


def _status_for_count_block(counts: TargetCounts) -> tuple[str, str] | None:
    if counts.policy_apply:
        return STATUS_DUPLICATE_POLICY_APPLY, "event_outbox.policy_apply_existing"
    if counts.analyses:
        return STATUS_FORBIDDEN_SIDE_EFFECT, "analyses.existing"
    if counts.notifications:
        return STATUS_FORBIDDEN_SIDE_EFFECT, "notifications.existing"
    return None


async def _inspect_preflight(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    target: HandoffTarget,
    database_url: str,
    raw_values: set[str],
) -> PreflightContext | None:
    repository = AnalysisValidatorRepository(session)
    job = await repository.load_job_by_trigger_event_id(target.trigger_event_id)
    if job is None:
        _set_status(report, STATUS_DB_READ_FAILED, "event_outbox.ready")
        return None
    raw_values.update(
        _raw_values(
            job.trigger_event_id,
            job.event_type,
            job.judge_run_id,
            job.judge_output_id,
            job.finish_reason,
        )
    )
    if job.event_type != READY_EVENT_TYPE or job.judge_run_id != target.judge_run_id:
        _set_status(report, STATUS_DB_READ_FAILED, "event_outbox.ready_identity")
        return None

    judge_run = await repository.load_judge_run(job.judge_run_id)
    report["judge_run_found_bucket"] = "one" if judge_run is not None else "zero"
    if judge_run is None:
        _set_status(report, STATUS_DB_READ_FAILED, "judge_run.missing")
        return None
    raw_values.update(
        _raw_values(
            judge_run.judge_run_id,
            judge_run.bundle_id,
            judge_run.status,
            judge_run.finish_reason,
        )
    )
    report["judge_run_succeeded_bucket"] = "one" if judge_run.status == "succeeded" else "zero"
    if judge_run.status != "succeeded" or judge_run.bundle_id != target.bundle_id:
        _set_status(report, STATUS_DB_READ_FAILED, "judge_run.identity_or_status")
        return None

    judge_output = await repository.load_judge_output(job.judge_output_id)
    report["judge_output_found_bucket"] = "one" if judge_output is not None else "zero"
    if judge_output is None:
        _set_status(report, STATUS_DB_READ_FAILED, "judge_output.missing")
        return None
    raw_values.update(
        _raw_values(
            judge_output.judge_output_id,
            judge_output.judge_run_id,
            judge_output.candidate_group_id,
            judge_output.payload_json,
        )
    )
    if (
        judge_output.judge_run_id != judge_run.judge_run_id
        or judge_output.candidate_group_id != target.candidate_group_id
    ):
        _set_status(report, STATUS_DB_READ_FAILED, "judge_output.identity")
        return None

    bundle = await repository.load_bundle_context(judge_run.bundle_id)
    report["bundle_found_bucket"] = "one" if bundle is not None else "zero"
    if bundle is None:
        _set_status(report, STATUS_DB_READ_FAILED, "bundle.missing")
        return None
    raw_values.update(
        _raw_values(
            bundle.bundle_id,
            bundle.candidate_group_id,
            bundle.current_primary_artifact_id,
            bundle.current_primary_artifact_type,
        )
    )
    if bundle.candidate_group_id != judge_output.candidate_group_id:
        _set_status(report, STATUS_DB_READ_FAILED, "bundle.identity")
        return None

    config = _validator_config(database_url)
    business_rules = AnalysisValidatorBusinessRules()
    control_decision = business_rules.evaluate_control_flow(
        payload=judge_output.payload_json,
        finish_reason=job.finish_reason or judge_run.finish_reason,
        refusal_detected=job.refusal_detected,
    )
    if control_decision.action != "forward_policy":
        _set_status(report, STATUS_VALIDATION_FAILED, "validator.control_flow")
        return None

    schema_decision = JudgeOutputSchemaRegistry(
        max_headline_chars=config.max_headline_chars,
        max_summary_chars=config.max_summary_chars,
        max_text_items=config.max_text_items,
    ).validate(judge_output.payload_json)
    report["output_schema_valid_bucket"] = (
        "one" if schema_decision.action == "forward_policy" else "zero"
    )
    if schema_decision.action != "forward_policy":
        _set_status(report, STATUS_VALIDATION_FAILED, "validator.schema")
        return None

    semantic_decision = business_rules.validate_semantics(
        payload=judge_output.payload_json,
        bundle=bundle,
    )
    report["business_rules_valid_bucket"] = (
        "one" if semantic_decision.action == "forward_policy" else "zero"
    )
    if semantic_decision.action != "forward_policy":
        _set_status(report, STATUS_VALIDATION_FAILED, "validator.business_rules")
        return None

    return PreflightContext(
        target=target,
        job=job,
        judge_run=judge_run,
        judge_output=judge_output,
        bundle=bundle,
    )


async def _run_service_handoff(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    context: PreflightContext,
    database_url: str,
) -> None:
    repository = AnalysisValidatorRepository(session)
    service = AnalysisValidatorService(_validator_config(database_url), repository=repository)
    job = await service.rehydrate_job(context.target.trigger_event_id)
    if job is None:
        _set_status(report, STATUS_WRITE_FAILED, "service.rehydrate_job")
        return
    report["database_write_attempted"] = True
    await service.handle_job(job)
    await session.commit()


def _approved_execution_succeeded(report: Mapping[str, Any]) -> bool:
    return bool(
        report["validator_state_transitions_written_bucket"] == "one"
        and report["analysis_policy_apply_outbox_written_bucket"] == "one"
        and report["analysis_rows_written_bucket"] == "zero"
        and report["notification_rows_written_bucket"] == "zero"
        and report["judge_outputs_written_bucket"] == "zero"
        and report["judge_outputs_mutation_attempted"] is False
        and report["policy_engine_started"] is False
        and report["notifier_started"] is False
        and report["telegram_send_attempted"] is False
        and report["redis_write_attempted"] is False
        and report["openai_call_attempted"] is False
        and report["openai_key_read_bucket"] == "zero"
    )


async def _read_runtime_database_url(
    *,
    report: dict[str, Any],
    runtime_env_path: str | Path,
    runtime_env_reader: RuntimeEnvReader | None,
    raw_values: set[str],
) -> str | None:
    try:
        values = (
            runtime_env_reader(runtime_env_path)
            if runtime_env_reader is not None
            else parse_runtime_env_file(runtime_env_path)
        )
        report["runtime_env_read"] = True
    except Exception:
        _set_status(report, STATUS_DB_READ_FAILED, "runtime_env.read")
        return None

    database_url = str(values.get("DATABASE_URL", "")).strip()
    raw_values.update(_raw_values(database_url))
    report["database_configured"] = bool(database_url and _database_url_is_supported(database_url))
    if not report["database_configured"]:
        _set_status(report, STATUS_DB_READ_FAILED, "database.config")
        return None
    return database_url


async def generate_report_async(
    *,
    approve_db_read: bool = False,
    approve_db_write: bool = False,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    report = _base_report(approve_db_read=approve_db_read, approve_db_write=approve_db_write)
    raw_values = _raw_values(*forbidden_raw_values)
    raw_values.update(_raw_values(runtime_env_path))

    if not approve_db_read and not approve_db_write:
        _set_status(report, STATUS_PREFLIGHT_PASSED)
        return _finalize(report, raw_values, exit_code=0)

    db_preflight_mode = approve_db_read and not approve_db_write
    db_write_mode = approve_db_read and approve_db_write
    if not db_preflight_mode and not db_write_mode:
        _set_status(report, STATUS_NOT_APPROVED, "approval.required_mode")
        return _finalize(report, raw_values, exit_code=1)

    session: AsyncSessionLike | None = None
    committed = False
    try:
        database_url = await _read_runtime_database_url(
            report=report,
            runtime_env_path=runtime_env_path,
            runtime_env_reader=runtime_env_reader,
            raw_values=raw_values,
        )
        if database_url is None:
            return _finalize(report, raw_values, exit_code=1)

        try:
            session = await _open_database_session(database_url, database_session_factory)
            if db_preflight_mode:
                await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
                read_only = _scalar(await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY))
                if not _transaction_read_only_enabled(read_only):
                    _set_status(report, STATUS_DB_READ_FAILED, "database.read_only")
                    return _finalize(report, raw_values, exit_code=1)
                report["read_only_transaction"] = True
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
        except Exception:
            _set_status(report, STATUS_DB_READ_FAILED, "database.connection")
            return _finalize(report, raw_values, exit_code=1)

        target = await _select_target(report=report, session=session, raw_values=raw_values)
        if target is None:
            if not report["checks_failed"]:
                _set_status(report, STATUS_DB_READ_FAILED, "event_outbox.ready_missing")
            return _finalize(report, raw_values, exit_code=1)

        context = await _inspect_preflight(
            report=report,
            session=session,
            target=target,
            database_url=database_url,
            raw_values=raw_values,
        )
        if context is None:
            return _finalize(report, raw_values, exit_code=1)

        before_counts = await _load_counts(session, target)
        _merge_existing_count_report(report, before_counts)
        blocked = _status_for_count_block(before_counts)
        if blocked is not None:
            status, check = blocked
            _set_status(report, status, check)
            return _finalize(report, raw_values, exit_code=1)

        if db_preflight_mode:
            _set_status(report, STATUS_DB_PREFLIGHT_PASSED)
            return _finalize(report, raw_values, exit_code=0)

        try:
            await _run_service_handoff(
                report=report,
                session=session,
                context=context,
                database_url=database_url,
            )
            committed = True
        except Exception:
            _set_status(report, STATUS_WRITE_FAILED, "service.handle_job")
            return _finalize(report, raw_values, exit_code=1)

        after_counts = await _load_counts(session, target)
        _merge_written_count_report(report, before=before_counts, after=after_counts)
        if not _approved_execution_succeeded(report):
            _set_status(report, STATUS_WRITE_FAILED, "db_write.expected_effects")
            return _finalize(report, raw_values, exit_code=1)

        _set_status(report, STATUS_DB_WRITE_PASSED)
        return _finalize(report, raw_values, exit_code=0)
    except Exception:
        _set_status(report, STATUS_DB_READ_FAILED, "unexpected")
        return _finalize(report, raw_values, exit_code=1)
    finally:
        if session is not None:
            if not committed:
                await _maybe_await(session.rollback())
            await _maybe_await(session.close())


def _finalize(report: dict[str, Any], raw_values: set[str], *, exit_code: int) -> ScriptResult:
    if _report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)
    return ScriptResult(exit_code=exit_code, report=report)


def generate_report(
    *,
    approve_db_read: bool = False,
    approve_db_write: bool = False,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            approve_db_read=approve_db_read,
            approve_db_write=approve_db_write,
            runtime_env_path=runtime_env_path,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = generate_report(
        approve_db_read=args.approve_db_read,
        approve_db_write=args.approve_db_write,
        runtime_env_path=args.runtime_env_path,
    )
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
