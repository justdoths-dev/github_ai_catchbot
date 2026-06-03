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

from src.services.policy_engine.config import PolicyEngineConfig  # noqa: E402
from src.services.policy_engine.models import (  # noqa: E402
    AnalysisPolicyJob,
    BundlePolicyContext,
    CandidatePolicyContext,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
)
from src.services.policy_engine.repositories import PolicyEngineRepository  # noqa: E402
from src.services.policy_engine.service import PolicyEngineService  # noqa: E402


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_policy_engine_analysis_handoff_smoke"
REPORT_TYPE = "policy_engine_analysis_handoff_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
POLICY_APPLY_EVENT_TYPE = "analysis.policy.apply.v1"
NOTIFICATION_PLAN_EVENT_TYPE = "notification.plan.created.v1"
REPLAY_PROMPT_SUFFIX = "__replay_live_smoke_v1"

STATUS_DEFAULT_PASSED = "policy_engine_analysis_handoff_smoke_default_passed"
STATUS_DB_PREFLIGHT_PASSED = "policy_engine_analysis_handoff_smoke_db_read_preflight_passed"
STATUS_DB_WRITE_PASSED = "policy_engine_analysis_handoff_smoke_approved_db_write_passed"
STATUS_NOT_APPROVED = "blocked_policy_engine_analysis_handoff_smoke_not_approved"
STATUS_DB_READ_FAILED = "blocked_policy_engine_analysis_handoff_smoke_db_read_failed"
STATUS_EXISTING_ANALYSIS = "blocked_policy_engine_analysis_handoff_smoke_existing_analysis"
STATUS_FORBIDDEN_SIDE_EFFECT = "blocked_policy_engine_analysis_handoff_smoke_forbidden_side_effect"
STATUS_VALIDATION_FAILED = "blocked_policy_engine_analysis_handoff_smoke_validation_failed"
STATUS_WRITE_FAILED = "blocked_policy_engine_analysis_handoff_smoke_write_failed"
STATUS_RAW_VALUE_EMISSION = "blocked_policy_engine_analysis_handoff_smoke_raw_value_emission"

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"

SELECT_ELIGIBLE_POLICY_APPLY_EVENT_QUERY = """
SELECT eo.event_id AS trigger_event_id,
       eo.created_at AS policy_apply_event_created_at,
       eo.aggregate_id AS aggregate_judge_run_id,
       jr.judge_run_id,
       jr.bundle_id,
       jr.prompt_version,
       jr.status AS judge_run_status,
       jo.judge_output_id,
       jo.candidate_group_id,
       b.bundle_id AS bundle_row_id,
       b.candidate_group_id AS bundle_candidate_group_id,
       c.current_bundle_id,
       CASE
           WHEN jr.prompt_version LIKE :replay_prompt_like THEN 'live_replay_persistence_smoke'
           ELSE 'newest_policy_apply_event'
       END AS target_source
FROM event_outbox eo
JOIN judge_runs jr
  ON jr.judge_run_id = eo.aggregate_id
JOIN judge_outputs jo
  ON jo.judge_output_id::text = eo.payload_json->>'judge_output_id'
JOIN candidate_evidence_bundles b
  ON b.bundle_id::text = eo.payload_json->>'bundle_id'
JOIN candidate_group_proposals c
  ON c.candidate_group_id::text = eo.payload_json->>'candidate_group_id'
WHERE eo.event_type = 'analysis.policy.apply.v1'
  AND eo.aggregate_type = 'judge_run'
  AND jr.status = 'succeeded'
  AND eo.payload_json->>'judge_run_id' = jr.judge_run_id::text
  AND eo.payload_json->>'judge_output_id' = jo.judge_output_id::text
  AND eo.payload_json->>'candidate_group_id' = jo.candidate_group_id::text
  AND eo.payload_json->>'candidate_group_id' = b.candidate_group_id::text
  AND eo.payload_json->>'bundle_id' = jr.bundle_id::text
ORDER BY (jr.prompt_version LIKE :replay_prompt_like) DESC,
         eo.created_at DESC NULLS LAST,
         eo.event_id DESC
LIMIT 1
"""

COUNT_ANALYSES_FOR_OUTPUT_QUERY = """
SELECT COUNT(*)
FROM analyses
WHERE judge_output_id = CAST(:judge_output_id AS uuid)
  AND policy_version = :policy_version
  AND delivery_policy_version = :delivery_policy_version
"""

COUNT_POLICY_STATE_TRANSITIONS_FOR_OUTPUT_QUERY = """
SELECT COUNT(*)
FROM state_transitions st
JOIN analyses a
  ON a.analysis_id = st.object_id
WHERE st.object_type = 'analysis'
  AND a.judge_output_id = CAST(:judge_output_id AS uuid)
  AND a.policy_version = :policy_version
  AND a.delivery_policy_version = :delivery_policy_version
"""

COUNT_NOTIFICATION_PLAN_INTENT_OUTBOX_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM event_outbox eo
JOIN analyses a
  ON a.analysis_id = eo.aggregate_id
WHERE eo.event_type = 'notification.plan.created.v1'
  AND eo.aggregate_type = 'analysis'
  AND a.judge_output_id = CAST(:judge_output_id AS uuid)
  AND a.policy_version = :policy_version
  AND a.delivery_policy_version = :delivery_policy_version
"""

COUNT_NOTIFICATION_PLAN_ROWS_FOR_OUTPUT_QUERY = """
SELECT COUNT(*)
FROM notification_plans np
JOIN analyses a ON a.analysis_id = np.analysis_id
WHERE a.judge_output_id = CAST(:judge_output_id AS uuid)
  AND a.policy_version = :policy_version
  AND a.delivery_policy_version = :delivery_policy_version
"""

COUNT_NOTIFICATION_RENDER_ROWS_FOR_OUTPUT_QUERY = """
SELECT COUNT(*)
FROM notification_renders nr
JOIN notification_plans np ON np.notification_plan_id = nr.notification_plan_id
JOIN analyses a ON a.analysis_id = np.analysis_id
WHERE a.judge_output_id = CAST(:judge_output_id AS uuid)
  AND a.policy_version = :policy_version
  AND a.delivery_policy_version = :delivery_policy_version
"""

COUNT_NOTIFICATION_DELIVERY_ROWS_FOR_OUTPUT_QUERY = """
SELECT COUNT(*)
FROM notification_delivery_records ndr
JOIN notification_plans np ON np.notification_plan_id = ndr.notification_plan_id
JOIN analyses a ON a.analysis_id = np.analysis_id
WHERE a.judge_output_id = CAST(:judge_output_id AS uuid)
  AND a.policy_version = :policy_version
  AND a.delivery_policy_version = :delivery_policy_version
"""

COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY = """
SELECT COUNT(*)
FROM judge_outputs
WHERE judge_run_id = CAST(:judge_run_id AS uuid)
"""

COUNT_CANDIDATE_BUNDLES_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM candidate_evidence_bundles
WHERE bundle_id = CAST(:bundle_id AS uuid)
"""

SELECT_CURRENT_ANALYSIS_ID_QUERY = """
SELECT current_analysis_id
FROM candidate_group_proposals
WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
"""

SELECT_CANDIDATE_BUNDLE_FINGERPRINT_QUERY = """
SELECT md5(to_jsonb(candidate_evidence_bundles)::text)
FROM candidate_evidence_bundles
WHERE bundle_id = CAST(:bundle_id AS uuid)
"""

DATABASE_ENV_KEYS = frozenset({"DATABASE_URL", "TELEGRAM_OPERATOR_CHAT_ID"})
PUBLIC_LITERAL_VALUES = frozenset(
    {
        SCRIPT_NAME,
        REPORT_TYPE,
        POLICY_APPLY_EVENT_TYPE,
        NOTIFICATION_PLAN_EVENT_TYPE,
        "judge_run",
        "analysis",
        "succeeded",
        "completed",
        "inspect_now",
        "later",
        "skip",
        "send_now",
        "suppress",
        "high",
        "normal_silent",
        "suppressed",
        "single_alert_v1",
        "zero",
        "one",
        "multiple",
        "true",
        "false",
        "unknown",
        "disabled",
        "live_replay_persistence_smoke",
        "newest_policy_apply_event",
        "PolicyEngineService.handle_job",
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
    job: AnalysisPolicyJob
    candidate: CandidatePolicyContext
    judge_run: JudgeRunPolicyContext
    judge_output: JudgeOutputPolicyContext
    bundle: BundlePolicyContext


@dataclass(frozen=True, slots=True)
class TargetCounts:
    analyses: int
    policy_transitions: int
    notification_intents: int
    notification_plans: int
    notification_renders: int
    notification_deliveries: int
    judge_outputs: int
    candidate_bundles: int


@dataclass(frozen=True, slots=True)
class TargetSnapshots:
    candidate_current_analysis_id: Any
    candidate_bundle_fingerprint: Any


class ExpectedEffectsError(RuntimeError):
    pass


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
            "Approval-gated policy-engine smoke from persisted analysis.policy.apply.v1 "
            "to analysis append and notification.plan.created.v1 plan-intent. "
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
        "contract_status": STATUS_DEFAULT_PASSED,
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
        "target_policy_apply_event_found_bucket": "zero",
        "target_source_bucket": "zero",
        "judge_run_found_bucket": "zero",
        "judge_output_found_bucket": "zero",
        "bundle_found_bucket": "zero",
        "candidate_group_found_bucket": "zero",
        "payload_identity_valid_bucket": "zero",
        "current_bundle_match_bucket": "zero",
        "existing_analysis_rows_for_output_bucket": "zero",
        "existing_notification_plan_rows_for_output_bucket": "zero",
        "existing_notification_delivery_rows_for_output_bucket": "zero",
        "existing_notification_plan_intent_outbox_for_target_bucket": "zero",
        "analysis_rows_written_bucket": "zero",
        "policy_state_transitions_written_bucket": "zero",
        "notification_plan_intent_outbox_written_bucket": "zero",
        "analysis_verdict_bucket": "zero",
        "analysis_delivery_decision_bucket": "zero",
        "analysis_policy_reconciled_bucket": "zero",
        "judge_outputs_written_bucket": "zero",
        "judge_outputs_mutation_attempted": False,
        "candidate_bundle_mutation_attempted": False,
        "candidate_current_analysis_mutation_attempted": False,
        "notification_plan_rows_written_bucket": "zero",
        "notification_render_rows_written_bucket": "zero",
        "notification_delivery_rows_written_bucket": "zero",
        "policy_engine_service_path_reused": True,
        "stops_at_event_type": NOTIFICATION_PLAN_EVENT_TYPE,
        "policy_engine_started": False,
        "notifier_started": False,
        "telegram_send_attempted": False,
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
            if key in DATABASE_ENV_KEYS and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                values[key] = _strip_optional_quotes(raw_value)
    return values


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def _operator_chat_id(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not re.fullmatch(r"-?[0-9]{1,20}", stripped):
        return None
    parsed = int(stripped)
    return parsed if parsed != 0 else None


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
            SELECT_ELIGIBLE_POLICY_APPLY_EVENT_QUERY,
            {"replay_prompt_like": f"%{REPLAY_PROMPT_SUFFIX}"},
        )
    )
    report["target_policy_apply_event_found_bucket"] = "one" if row is not None else "zero"
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

    target_source = str(row.get("target_source") or "newest_policy_apply_event")
    report["target_source_bucket"] = (
        target_source if target_source in {"live_replay_persistence_smoke", "newest_policy_apply_event"} else "unknown"
    )
    return HandoffTarget(
        trigger_event_id=trigger_event_id,
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
        bundle_id=bundle_id,
        candidate_group_id=candidate_group_id,
        target_source=target_source,
    )


def _policy_config(*, database_url: str, operator_chat_id: int | None, enable_notification_send: bool) -> PolicyEngineConfig:
    return PolicyEngineConfig(
        app_env="prod",
        database_url=database_url,
        redis_url="redis://disabled.invalid/0",
        queue_name="q.analysis.policy",
        consumer_group="policy-engine",
        consumer_name=SCRIPT_NAME,
        batch_size=1,
        block_ms=1,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
        operator_chat_id=operator_chat_id or 0,
        enable_later_delivery=True,
        enable_silent_later=True,
        enable_notification_send=enable_notification_send,
        render_profile_high="single_alert_v1",
        render_profile_normal="single_alert_v1",
        log_level="INFO",
    )


async def _load_counts(
    session: AsyncSessionLike,
    target: HandoffTarget,
    *,
    policy_version: str,
    delivery_policy_version: str,
) -> TargetCounts:
    params = {
        "judge_run_id": str(target.judge_run_id),
        "judge_output_id": str(target.judge_output_id),
        "bundle_id": str(target.bundle_id),
        "policy_version": policy_version,
        "delivery_policy_version": delivery_policy_version,
    }
    return TargetCounts(
        analyses=await _count_query(session, COUNT_ANALYSES_FOR_OUTPUT_QUERY, params),
        policy_transitions=await _count_query(session, COUNT_POLICY_STATE_TRANSITIONS_FOR_OUTPUT_QUERY, params),
        notification_intents=await _count_query(session, COUNT_NOTIFICATION_PLAN_INTENT_OUTBOX_FOR_TARGET_QUERY, params),
        notification_plans=await _count_query(session, COUNT_NOTIFICATION_PLAN_ROWS_FOR_OUTPUT_QUERY, params),
        notification_renders=await _count_query(session, COUNT_NOTIFICATION_RENDER_ROWS_FOR_OUTPUT_QUERY, params),
        notification_deliveries=await _count_query(session, COUNT_NOTIFICATION_DELIVERY_ROWS_FOR_OUTPUT_QUERY, params),
        judge_outputs=await _count_query(session, COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY, params),
        candidate_bundles=await _count_query(session, COUNT_CANDIDATE_BUNDLES_FOR_TARGET_QUERY, params),
    )


async def _load_snapshots(session: AsyncSessionLike, target: HandoffTarget) -> TargetSnapshots:
    current_analysis_id = _scalar(
        await _execute(
            session,
            SELECT_CURRENT_ANALYSIS_ID_QUERY,
            {"candidate_group_id": str(target.candidate_group_id)},
        )
    )
    bundle_fingerprint = _scalar(
        await _execute(
            session,
            SELECT_CANDIDATE_BUNDLE_FINGERPRINT_QUERY,
            {"bundle_id": str(target.bundle_id)},
        )
    )
    return TargetSnapshots(
        candidate_current_analysis_id=current_analysis_id,
        candidate_bundle_fingerprint=bundle_fingerprint,
    )


def _merge_existing_count_report(report: dict[str, Any], counts: TargetCounts) -> None:
    report["existing_analysis_rows_for_output_bucket"] = _bucket_count(counts.analyses)
    report["existing_notification_plan_rows_for_output_bucket"] = _bucket_count(counts.notification_plans)
    report["existing_notification_delivery_rows_for_output_bucket"] = _bucket_count(counts.notification_deliveries)
    report["existing_notification_plan_intent_outbox_for_target_bucket"] = _bucket_count(counts.notification_intents)


def _merge_written_count_report(
    report: dict[str, Any],
    *,
    before: TargetCounts,
    after: TargetCounts,
) -> None:
    report["analysis_rows_written_bucket"] = _bucket_count(after.analyses - before.analyses)
    report["policy_state_transitions_written_bucket"] = _bucket_count(after.policy_transitions - before.policy_transitions)
    report["notification_plan_intent_outbox_written_bucket"] = _bucket_count(
        after.notification_intents - before.notification_intents
    )
    report["notification_plan_rows_written_bucket"] = _bucket_count(after.notification_plans - before.notification_plans)
    report["notification_render_rows_written_bucket"] = _bucket_count(after.notification_renders - before.notification_renders)
    report["notification_delivery_rows_written_bucket"] = _bucket_count(
        after.notification_deliveries - before.notification_deliveries
    )
    report["judge_outputs_written_bucket"] = _bucket_count(after.judge_outputs - before.judge_outputs)
    report["judge_outputs_mutation_attempted"] = after.judge_outputs != before.judge_outputs
    report["candidate_bundle_mutation_attempted"] = after.candidate_bundles != before.candidate_bundles


def _merge_snapshot_mutation_report(
    report: dict[str, Any],
    *,
    before: TargetSnapshots,
    after: TargetSnapshots,
) -> None:
    report["candidate_current_analysis_mutation_attempted"] = (
        str(after.candidate_current_analysis_id) != str(before.candidate_current_analysis_id)
    )
    report["candidate_bundle_mutation_attempted"] = report["candidate_bundle_mutation_attempted"] or (
        str(after.candidate_bundle_fingerprint) != str(before.candidate_bundle_fingerprint)
    )


def _status_for_count_block(counts: TargetCounts) -> tuple[str, str] | None:
    if counts.analyses:
        return STATUS_EXISTING_ANALYSIS, "analyses.existing"
    if counts.notification_plans:
        return STATUS_FORBIDDEN_SIDE_EFFECT, "notification_plans.existing"
    if counts.notification_deliveries:
        return STATUS_FORBIDDEN_SIDE_EFFECT, "notification_delivery_records.existing"
    if counts.notification_intents:
        return STATUS_FORBIDDEN_SIDE_EFFECT, "event_outbox.notification_plan_intent_existing"
    return None


async def _inspect_preflight(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    target: HandoffTarget,
    database_url: str,
    operator_chat_id: int | None,
    raw_values: set[str],
) -> PreflightContext | None:
    repository = PolicyEngineRepository(session)
    job = await repository.load_job_by_trigger_event_id(target.trigger_event_id)
    if job is None:
        _set_status(report, STATUS_DB_READ_FAILED, "event_outbox.policy_apply")
        return None
    raw_values.update(_raw_values(job.trigger_event_id, job.event_type, job.judge_run_id, job.judge_output_id))
    if job.event_type != POLICY_APPLY_EVENT_TYPE or job.judge_run_id != target.judge_run_id:
        _set_status(report, STATUS_VALIDATION_FAILED, "event_outbox.policy_apply_identity")
        return None

    candidate = await repository.load_candidate_context(job.candidate_group_id)
    report["candidate_group_found_bucket"] = "one" if candidate is not None else "zero"
    if candidate is None:
        _set_status(report, STATUS_DB_READ_FAILED, "candidate_group.missing")
        return None
    raw_values.update(_raw_values(candidate.candidate_group_id, candidate.current_bundle_id, candidate.current_analysis_id))

    judge_run = await repository.load_judge_run(job.judge_run_id)
    report["judge_run_found_bucket"] = "one" if judge_run is not None else "zero"
    if judge_run is None:
        _set_status(report, STATUS_DB_READ_FAILED, "judge_run.missing")
        return None
    raw_values.update(_raw_values(judge_run.judge_run_id, judge_run.bundle_id, judge_run.status, judge_run.prompt_version))

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

    bundle = await repository.load_bundle_context(job.bundle_id)
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

    identities_match = (
        job.judge_output_id == target.judge_output_id
        and job.candidate_group_id == target.candidate_group_id
        and job.bundle_id == target.bundle_id
        and judge_run.status == "succeeded"
        and judge_run.bundle_id == job.bundle_id
        and judge_output.judge_run_id == job.judge_run_id
        and judge_output.candidate_group_id == job.candidate_group_id
        and bundle.candidate_group_id == job.candidate_group_id
    )
    report["payload_identity_valid_bucket"] = "one" if identities_match else "zero"
    if not identities_match:
        _set_status(report, STATUS_VALIDATION_FAILED, "payload.identity")
        return None

    current_bundle_match = candidate.current_bundle_id == job.bundle_id
    report["current_bundle_match_bucket"] = "one" if current_bundle_match else "zero"
    if not current_bundle_match:
        _set_status(report, STATUS_VALIDATION_FAILED, "candidate.current_bundle")
        return None

    context = PreflightContext(
        target=target,
        job=job,
        candidate=candidate,
        judge_run=judge_run,
        judge_output=judge_output,
        bundle=bundle,
    )
    _merge_policy_preview(
        report=report,
        context=context,
        database_url=database_url,
        operator_chat_id=operator_chat_id,
    )
    if report["analysis_delivery_decision_bucket"] != "suppress" and operator_chat_id is None:
        _set_status(report, STATUS_VALIDATION_FAILED, "config.target_chat_id_unavailable")
        return None
    return context


def _merge_policy_preview(
    *,
    report: dict[str, Any],
    context: PreflightContext,
    database_url: str,
    operator_chat_id: int | None,
) -> None:
    config = _policy_config(
        database_url=database_url,
        operator_chat_id=operator_chat_id,
        enable_notification_send=operator_chat_id is not None,
    )
    service = PolicyEngineService(config, repository=_PreviewRepository())
    analysis, _evaluation = service._build_analysis(  # policy preview only; writes still go through handle_job.
        job=context.job,
        judge_run=context.judge_run,
        judge_output=context.judge_output,
        bundle=context.bundle,
    )
    report["analysis_verdict_bucket"] = analysis.verdict
    report["analysis_delivery_decision_bucket"] = analysis.delivery_decision
    report["analysis_policy_reconciled_bucket"] = "true" if analysis.policy_reconciled_flag else "false"


class _PreviewRepository:
    def transaction(self): ...


async def _run_service_handoff(
    *,
    report: dict[str, Any],
    session: AsyncSessionLike,
    context: PreflightContext,
    database_url: str,
    operator_chat_id: int | None,
) -> None:
    if context.target.trigger_event_id is None:
        _set_status(report, STATUS_WRITE_FAILED, "target.trigger_event_id")
        return

    before_counts = await _load_counts(
        session,
        context.target,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
    )
    before_snapshots = await _load_snapshots(session, context.target)
    repository = PolicyEngineRepository(session)
    config = _policy_config(
        database_url=database_url,
        operator_chat_id=operator_chat_id,
        enable_notification_send=operator_chat_id is not None,
    )
    service = PolicyEngineService(config, repository=repository)
    job = await service.rehydrate_job(context.target.trigger_event_id)
    if job is None:
        _set_status(report, STATUS_WRITE_FAILED, "service.rehydrate_job")
        return

    if session.in_transaction():
        await session.rollback()

    report["database_write_attempted"] = True
    report["policy_engine_started"] = True
    async with session.begin():
        await service.handle_job(job)
        after_counts = await _load_counts(
            session,
            context.target,
            policy_version=config.policy_version,
            delivery_policy_version=config.delivery_policy_version,
        )
        after_snapshots = await _load_snapshots(session, context.target)
        _merge_written_count_report(report, before=before_counts, after=after_counts)
        _merge_snapshot_mutation_report(report, before=before_snapshots, after=after_snapshots)
        if not _approved_execution_succeeded(report):
            raise ExpectedEffectsError("policy-engine smoke effects did not match contract")


def _approved_execution_succeeded(report: Mapping[str, Any]) -> bool:
    expected_intent_bucket = (
        "zero" if report["analysis_delivery_decision_bucket"] == "suppress" else "one"
    )
    return bool(
        report["analysis_rows_written_bucket"] == "one"
        and report["policy_state_transitions_written_bucket"] == "one"
        and report["notification_plan_intent_outbox_written_bucket"] == expected_intent_bucket
        and report["notification_plan_rows_written_bucket"] == "zero"
        and report["notification_render_rows_written_bucket"] == "zero"
        and report["notification_delivery_rows_written_bucket"] == "zero"
        and report["judge_outputs_written_bucket"] == "zero"
        and report["judge_outputs_mutation_attempted"] is False
        and report["candidate_bundle_mutation_attempted"] is False
        and report["candidate_current_analysis_mutation_attempted"] is False
        and report["policy_engine_service_path_reused"] is True
        and report["policy_engine_started"] is True
        and report["notifier_started"] is False
        and report["telegram_send_attempted"] is False
        and report["redis_write_attempted"] is False
        and report["redis_ack_attempted"] is False
        and report["redis_delete_or_trim_attempted"] is False
        and report["openai_call_attempted"] is False
        and report["openai_key_read_bucket"] == "zero"
    )


async def _read_runtime_config(
    *,
    report: dict[str, Any],
    runtime_env_path: str | Path,
    runtime_env_reader: RuntimeEnvReader | None,
    raw_values: set[str],
) -> tuple[str, int | None] | None:
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
    operator_chat_id_text = str(values.get("TELEGRAM_OPERATOR_CHAT_ID", "")).strip()
    raw_values.update(_raw_values(database_url, operator_chat_id_text))
    report["database_configured"] = bool(database_url and _database_url_is_supported(database_url))
    if not report["database_configured"]:
        _set_status(report, STATUS_DB_READ_FAILED, "database.config")
        return None
    return database_url, _operator_chat_id(operator_chat_id_text)


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
        _set_status(report, STATUS_DEFAULT_PASSED)
        return _finalize(report, raw_values, exit_code=0)

    db_preflight_mode = approve_db_read and not approve_db_write
    db_write_mode = approve_db_read and approve_db_write
    if not db_preflight_mode and not db_write_mode:
        _set_status(report, STATUS_NOT_APPROVED, "approval.required_mode")
        return _finalize(report, raw_values, exit_code=1)

    session: AsyncSessionLike | None = None
    committed = False
    try:
        runtime_config = await _read_runtime_config(
            report=report,
            runtime_env_path=runtime_env_path,
            runtime_env_reader=runtime_env_reader,
            raw_values=raw_values,
        )
        if runtime_config is None:
            return _finalize(report, raw_values, exit_code=1)
        database_url, operator_chat_id = runtime_config

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
                _set_status(report, STATUS_DB_READ_FAILED, "event_outbox.policy_apply_missing")
            return _finalize(report, raw_values, exit_code=1)

        context = await _inspect_preflight(
            report=report,
            session=session,
            target=target,
            database_url=database_url,
            operator_chat_id=operator_chat_id,
            raw_values=raw_values,
        )
        if context is None:
            return _finalize(report, raw_values, exit_code=1)

        counts = await _load_counts(
            session,
            target,
            policy_version="verdict_policy_v1",
            delivery_policy_version="delivery_policy_v1",
        )
        _merge_existing_count_report(report, counts)
        blocked = _status_for_count_block(counts)
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
                operator_chat_id=operator_chat_id,
            )
            committed = True
        except ExpectedEffectsError:
            _set_status(report, STATUS_WRITE_FAILED, "db_write.expected_effects")
            return _finalize(report, raw_values, exit_code=1)
        except Exception:
            _set_status(report, STATUS_WRITE_FAILED, "service.handle_job")
            return _finalize(report, raw_values, exit_code=1)

        _set_status(report, STATUS_DB_WRITE_PASSED)
        return _finalize(report, raw_values, exit_code=0)
    except Exception:
        _set_status(report, STATUS_DB_READ_FAILED, "unexpected")
        return _finalize(report, raw_values, exit_code=1)
    finally:
        if session is not None:
            try:
                if not committed:
                    await session.rollback()
            finally:
                await session.close()


def _finalize(report: dict[str, Any], raw_values: set[str], *, exit_code: int) -> ScriptResult:
    if _report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "report.raw_value_emission")
        exit_code = 1
    return ScriptResult(exit_code=exit_code, report=report)


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def generate_report(**kwargs: Any) -> ScriptResult:
    return asyncio.run(generate_report_async(**kwargs))


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        approve_db_read=args.approve_db_read,
        approve_db_write=args.approve_db_write,
        runtime_env_path=args.runtime_env_path,
    )
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
