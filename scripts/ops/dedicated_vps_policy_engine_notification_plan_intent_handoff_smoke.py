from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.ops import dedicated_vps_policy_engine_analysis_handoff_smoke as base  # noqa: E402
from src.services.policy_engine.repositories import PolicyEngineRepository  # noqa: E402
from src.services.policy_engine.service import PolicyEngineService  # noqa: E402


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_policy_engine_notification_plan_intent_handoff_smoke"
REPORT_TYPE = "policy_engine_notification_plan_intent_handoff_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = base.DEFAULT_RUNTIME_ENV_PATH
POLICY_APPLY_EVENT_TYPE = base.POLICY_APPLY_EVENT_TYPE
NOTIFICATION_PLAN_EVENT_TYPE = base.NOTIFICATION_PLAN_EVENT_TYPE
REPLAY_PROMPT_SUFFIX = base.REPLAY_PROMPT_SUFFIX

STATUS_DEFAULT_PASSED = "policy_engine_notification_plan_intent_handoff_smoke_default_passed"
STATUS_DB_PREFLIGHT_PASSED = "policy_engine_notification_plan_intent_handoff_smoke_db_read_preflight_passed"
STATUS_DB_WRITE_PASSED = "policy_engine_notification_plan_intent_handoff_smoke_approved_db_write_passed"
STATUS_NOT_APPROVED = "blocked_policy_engine_notification_plan_intent_handoff_smoke_not_approved"
STATUS_DB_READ_FAILED = "blocked_policy_engine_notification_plan_intent_handoff_smoke_db_read_failed"
STATUS_NO_NON_SUPPRESS_TARGET = (
    "blocked_policy_engine_notification_plan_intent_handoff_smoke_no_non_suppress_target"
)
STATUS_SUPPRESS_TARGET = "blocked_policy_engine_notification_plan_intent_handoff_smoke_suppress_target"
STATUS_EXISTING_ANALYSIS = "blocked_policy_engine_notification_plan_intent_handoff_smoke_existing_analysis"
STATUS_FORBIDDEN_SIDE_EFFECT = (
    "blocked_policy_engine_notification_plan_intent_handoff_smoke_forbidden_side_effect"
)
STATUS_VALIDATION_FAILED = "blocked_policy_engine_notification_plan_intent_handoff_smoke_validation_failed"
STATUS_WRITE_FAILED = "blocked_policy_engine_notification_plan_intent_handoff_smoke_write_failed"
STATUS_RAW_VALUE_EMISSION = "blocked_policy_engine_notification_plan_intent_handoff_smoke_raw_value_emission"

SELECT_RECENT_POLICY_APPLY_EVENTS_QUERY = """
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
  AND eo.payload_json->>'bundle_id' = c.current_bundle_id::text
  AND jo.judge_run_id = jr.judge_run_id
  AND b.candidate_group_id = jo.candidate_group_id
ORDER BY (jr.prompt_version LIKE :replay_prompt_like) DESC,
         eo.created_at DESC NULLS LAST,
         eo.event_id DESC
LIMIT 50
"""


@dataclass(frozen=True, slots=True)
class SelectedTarget:
    target: base.HandoffTarget
    context: base.PreflightContext
    counts: base.TargetCounts


class ExpectedEffectsError(RuntimeError):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Approval-gated policy-engine smoke from persisted analysis.policy.apply.v1 "
            "to non-suppress notification.plan.created.v1 plan-intent. Default mode reads "
            "no runtime env, DB, Redis, key material, OpenAI, notifier, or Telegram transport."
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
        "target_policy_apply_event_found_bucket": "zero",
        "target_non_suppress_policy_apply_event_found_bucket": "zero",
        "target_source_bucket": "zero",
        "target_chat_id_available_bucket": "zero",
        "judge_run_found_bucket": "zero",
        "judge_output_found_bucket": "zero",
        "bundle_found_bucket": "zero",
        "candidate_group_found_bucket": "zero",
        "payload_identity_valid_bucket": "zero",
        "current_bundle_match_bucket": "zero",
        "existing_analysis_rows_for_output_bucket": "zero",
        "existing_notification_plan_intent_outbox_for_target_bucket": "zero",
        "existing_notification_plan_rows_for_output_bucket": "zero",
        "existing_notification_render_rows_for_output_bucket": "zero",
        "existing_notification_delivery_rows_for_output_bucket": "zero",
        "analysis_verdict_bucket": "zero",
        "analysis_delivery_decision_bucket": "zero",
        "analysis_urgency_profile_bucket": "zero",
        "analysis_policy_reconciled_bucket": "zero",
        "analysis_rows_written_bucket": "zero",
        "policy_state_transitions_written_bucket": "zero",
        "notification_plan_intent_outbox_written_bucket": "zero",
        "notification_plan_rows_written_bucket": "zero",
        "notification_render_rows_written_bucket": "zero",
        "notification_delivery_rows_written_bucket": "zero",
        "judge_outputs_written_bucket": "zero",
        "judge_outputs_mutation_attempted": False,
        "candidate_bundle_mutation_attempted": False,
        "candidate_current_analysis_mutation_attempted": False,
        "policy_engine_service_path_reused": True,
        "stops_at_event_type": NOTIFICATION_PLAN_EVENT_TYPE,
        "policy_engine_started": False,
        "notifier_started": False,
        "telegram_send_attempted": False,
        "redis_connected": False,
        "redis_write_attempted": False,
        "redis_ack_attempted": False,
        "redis_delete_or_trim_attempted": False,
        "openai_call_attempted": False,
        "openai_key_read_bucket": "zero",
        "raw_values_emitted": False,
        "checks_failed": [],
    }


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _target_from_row(row: Mapping[str, Any], report: dict[str, Any]) -> base.HandoffTarget | None:
    trigger_event_id = base._coerce_uuid(row.get("trigger_event_id"))
    judge_run_id = base._coerce_uuid(row.get("judge_run_id"))
    judge_output_id = base._coerce_uuid(row.get("judge_output_id"))
    bundle_id = base._coerce_uuid(row.get("bundle_id"))
    candidate_group_id = base._coerce_uuid(row.get("candidate_group_id"))
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
    return base.HandoffTarget(
        trigger_event_id=trigger_event_id,
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
        bundle_id=bundle_id,
        candidate_group_id=candidate_group_id,
        target_source=target_source,
    )


def _policy_config(*, database_url: str, operator_chat_id: int | None, enable_notification_send: bool) -> Any:
    return base.PolicyEngineConfig(
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


def _merge_existing_count_report(report: dict[str, Any], counts: base.TargetCounts) -> None:
    report["existing_analysis_rows_for_output_bucket"] = base._bucket_count(counts.analyses)
    report["existing_notification_plan_intent_outbox_for_target_bucket"] = base._bucket_count(
        counts.notification_intents
    )
    report["existing_notification_plan_rows_for_output_bucket"] = base._bucket_count(counts.notification_plans)
    report["existing_notification_render_rows_for_output_bucket"] = base._bucket_count(counts.notification_renders)
    report["existing_notification_delivery_rows_for_output_bucket"] = base._bucket_count(
        counts.notification_deliveries
    )


def _merge_written_count_report(
    report: dict[str, Any],
    *,
    before: base.TargetCounts,
    after: base.TargetCounts,
) -> None:
    base._merge_written_count_report(report, before=before, after=after)


def _merge_snapshot_mutation_report(
    report: dict[str, Any],
    *,
    before: base.TargetSnapshots,
    after: base.TargetSnapshots,
) -> None:
    base._merge_snapshot_mutation_report(report, before=before, after=after)


def _status_for_count_block(counts: base.TargetCounts) -> tuple[str, str] | None:
    if counts.analyses:
        return STATUS_EXISTING_ANALYSIS, "analyses.existing"
    if counts.notification_intents:
        return STATUS_FORBIDDEN_SIDE_EFFECT, "event_outbox.notification_plan_intent_existing"
    if counts.notification_plans:
        return STATUS_FORBIDDEN_SIDE_EFFECT, "notification_plans.existing"
    if counts.notification_renders:
        return STATUS_FORBIDDEN_SIDE_EFFECT, "notification_renders.existing"
    if counts.notification_deliveries:
        return STATUS_FORBIDDEN_SIDE_EFFECT, "notification_delivery_records.existing"
    return None


async def _inspect_preflight(
    *,
    report: dict[str, Any],
    session: base.AsyncSessionLike,
    target: base.HandoffTarget,
    database_url: str,
    operator_chat_id: int | None,
    raw_values: set[str],
) -> base.PreflightContext | None:
    repository = PolicyEngineRepository(session)
    job = await repository.load_job_by_trigger_event_id(target.trigger_event_id)
    if job is None:
        _set_status(report, STATUS_DB_READ_FAILED, "event_outbox.policy_apply")
        return None
    raw_values.update(base._raw_values(job.trigger_event_id, job.event_type, job.judge_run_id, job.judge_output_id))
    if job.event_type != POLICY_APPLY_EVENT_TYPE or job.judge_run_id != target.judge_run_id:
        _set_status(report, STATUS_VALIDATION_FAILED, "event_outbox.policy_apply_identity")
        return None

    candidate = await repository.load_candidate_context(job.candidate_group_id)
    report["candidate_group_found_bucket"] = "one" if candidate is not None else "zero"
    if candidate is None:
        _set_status(report, STATUS_DB_READ_FAILED, "candidate_group.missing")
        return None
    raw_values.update(base._raw_values(candidate.candidate_group_id, candidate.current_bundle_id))

    judge_run = await repository.load_judge_run(job.judge_run_id)
    report["judge_run_found_bucket"] = "one" if judge_run is not None else "zero"
    if judge_run is None:
        _set_status(report, STATUS_DB_READ_FAILED, "judge_run.missing")
        return None
    raw_values.update(base._raw_values(judge_run.judge_run_id, judge_run.bundle_id, judge_run.status, judge_run.prompt_version))

    judge_output = await repository.load_judge_output(job.judge_output_id)
    report["judge_output_found_bucket"] = "one" if judge_output is not None else "zero"
    if judge_output is None:
        _set_status(report, STATUS_DB_READ_FAILED, "judge_output.missing")
        return None
    raw_values.update(
        base._raw_values(
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
        base._raw_values(
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

    context = base.PreflightContext(
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
    report["target_chat_id_available_bucket"] = "one" if operator_chat_id is not None else "zero"
    return context


def _merge_policy_preview(
    *,
    report: dict[str, Any],
    context: base.PreflightContext,
    database_url: str,
    operator_chat_id: int | None,
) -> None:
    config = _policy_config(
        database_url=database_url,
        operator_chat_id=operator_chat_id,
        enable_notification_send=operator_chat_id is not None,
    )
    service = PolicyEngineService(config, repository=base._PreviewRepository())
    analysis, evaluation = service._build_analysis(  # policy preview only; writes still go through handle_job.
        job=context.job,
        judge_run=context.judge_run,
        judge_output=context.judge_output,
        bundle=context.bundle,
    )
    report["analysis_verdict_bucket"] = analysis.verdict
    report["analysis_delivery_decision_bucket"] = analysis.delivery_decision
    report["analysis_urgency_profile_bucket"] = evaluation.urgency_profile
    report["analysis_policy_reconciled_bucket"] = "true" if analysis.policy_reconciled_flag else "false"


async def _select_non_suppress_target(
    *,
    report: dict[str, Any],
    session: base.AsyncSessionLike,
    database_url: str,
    operator_chat_id: int | None,
    raw_values: set[str],
) -> SelectedTarget | None:
    rows = base._rows(
        await base._execute(
            session,
            SELECT_RECENT_POLICY_APPLY_EVENTS_QUERY,
            {"replay_prompt_like": f"%{REPLAY_PROMPT_SUFFIX}"},
        )
    )
    report["target_policy_apply_event_found_bucket"] = "one" if rows else "zero"
    if not rows:
        _set_status(report, STATUS_NO_NON_SUPPRESS_TARGET, "target.non_suppress_policy_apply_missing")
        return None

    first_blocked: tuple[dict[str, Any], str, str] | None = None
    first_suppress: dict[str, Any] | None = None
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        raw_values.update(base._raw_values(row))
        target = _target_from_row(row, report)
        if target is None:
            return None
        context = await _inspect_preflight(
            report=report,
            session=session,
            target=target,
            database_url=database_url,
            operator_chat_id=operator_chat_id,
            raw_values=raw_values,
        )
        if context is None:
            return None

        counts = await base._load_counts(
            session,
            target,
            policy_version="verdict_policy_v1",
            delivery_policy_version="delivery_policy_v1",
        )
        _merge_existing_count_report(report, counts)
        if report["analysis_delivery_decision_bucket"] == "suppress":
            if first_suppress is None:
                first_suppress = dict(report)
            continue
        if report["analysis_verdict_bucket"] not in {"inspect_now", "later"}:
            if first_suppress is None:
                first_suppress = dict(report)
            continue
        report["target_non_suppress_policy_apply_event_found_bucket"] = "one"
        if operator_chat_id is None:
            _set_status(report, STATUS_VALIDATION_FAILED, "config.target_chat_id_unavailable")
            return None
        blocked = _status_for_count_block(counts)
        if blocked is not None:
            if first_blocked is None:
                status, check = blocked
                first_blocked = dict(report), status, check
            continue

        return SelectedTarget(target=target, context=context, counts=counts)

    if first_blocked is not None:
        snapshot, status, check = first_blocked
        report.clear()
        report.update(snapshot)
        _set_status(report, status, check)
        return None
    if first_suppress is not None:
        report.clear()
        report.update(first_suppress)
        _set_status(report, STATUS_SUPPRESS_TARGET, "target.delivery_decision_suppress")
        return None

    _set_status(report, STATUS_NO_NON_SUPPRESS_TARGET, "target.non_suppress_policy_apply_missing")
    return None


async def _run_service_handoff(
    *,
    report: dict[str, Any],
    session: base.AsyncSessionLike,
    selected: SelectedTarget,
    database_url: str,
    operator_chat_id: int | None,
) -> None:
    before_counts = await base._load_counts(
        session,
        selected.target,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
    )
    before_snapshots = await base._load_snapshots(session, selected.target)
    repository = PolicyEngineRepository(session)
    config = _policy_config(
        database_url=database_url,
        operator_chat_id=operator_chat_id,
        enable_notification_send=True,
    )
    service = PolicyEngineService(config, repository=repository)
    job = await service.rehydrate_job(selected.target.trigger_event_id)
    if job is None:
        _set_status(report, STATUS_WRITE_FAILED, "service.rehydrate_job")
        return

    if session.in_transaction():
        await session.rollback()

    report["database_write_attempted"] = True
    report["policy_engine_started"] = True
    async with session.begin():
        await service.handle_job(job)
        after_counts = await base._load_counts(
            session,
            selected.target,
            policy_version=config.policy_version,
            delivery_policy_version=config.delivery_policy_version,
        )
        after_snapshots = await base._load_snapshots(session, selected.target)
        _merge_written_count_report(report, before=before_counts, after=after_counts)
        _merge_snapshot_mutation_report(report, before=before_snapshots, after=after_snapshots)
        if not _approved_execution_succeeded(report):
            raise ExpectedEffectsError("policy-engine notification plan-intent smoke effects did not match contract")


def _approved_execution_succeeded(report: Mapping[str, Any]) -> bool:
    return bool(
        report["analysis_rows_written_bucket"] == "one"
        and report["policy_state_transitions_written_bucket"] == "one"
        and report["notification_plan_intent_outbox_written_bucket"] == "one"
        and report["analysis_verdict_bucket"] in {"inspect_now", "later"}
        and report["analysis_delivery_decision_bucket"] == "send_now"
        and report["analysis_urgency_profile_bucket"] in {"high", "normal_silent"}
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
    runtime_env_reader: base.RuntimeEnvReader | None,
    raw_values: set[str],
) -> tuple[str, int | None] | None:
    return await base._read_runtime_config(
        report=report,
        runtime_env_path=runtime_env_path,
        runtime_env_reader=runtime_env_reader,
        raw_values=raw_values,
    )


async def generate_report_async(
    *,
    approve_db_read: bool = False,
    approve_db_write: bool = False,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    runtime_env_reader: base.RuntimeEnvReader | None = None,
    database_session_factory: base.DatabaseSessionFactory | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> base.ScriptResult:
    report = _base_report(approve_db_read=approve_db_read, approve_db_write=approve_db_write)
    raw_values = base._raw_values(*forbidden_raw_values)
    raw_values.update(base._raw_values(runtime_env_path))

    if not approve_db_read and not approve_db_write:
        _set_status(report, STATUS_DEFAULT_PASSED)
        return _finalize(report, raw_values, exit_code=0)

    db_preflight_mode = approve_db_read and not approve_db_write
    db_write_mode = approve_db_read and approve_db_write
    if not db_preflight_mode and not db_write_mode:
        _set_status(report, STATUS_NOT_APPROVED, "approval.required_mode")
        return _finalize(report, raw_values, exit_code=1)

    session: base.AsyncSessionLike | None = None
    committed = False
    try:
        runtime_config = await _read_runtime_config(
            report=report,
            runtime_env_path=runtime_env_path,
            runtime_env_reader=runtime_env_reader,
            raw_values=raw_values,
        )
        if runtime_config is None:
            _map_base_status(report)
            return _finalize(report, raw_values, exit_code=1)
        database_url, operator_chat_id = runtime_config

        try:
            session = await base._open_database_session(database_url, database_session_factory)
            if db_preflight_mode:
                await base._execute(session, base.SET_TRANSACTION_READ_ONLY_QUERY)
                read_only = base._scalar(await base._execute(session, base.SHOW_TRANSACTION_READ_ONLY_QUERY))
                if not base._transaction_read_only_enabled(read_only):
                    _set_status(report, STATUS_DB_READ_FAILED, "database.read_only")
                    return _finalize(report, raw_values, exit_code=1)
                report["read_only_transaction"] = True
            await base._execute(session, base.SELECT_ONE_QUERY)
            report["database_connected"] = True
        except Exception:
            _set_status(report, STATUS_DB_READ_FAILED, "database.connection")
            return _finalize(report, raw_values, exit_code=1)

        selected = await _select_non_suppress_target(
            report=report,
            session=session,
            database_url=database_url,
            operator_chat_id=operator_chat_id,
            raw_values=raw_values,
        )
        if selected is None:
            return _finalize(report, raw_values, exit_code=1)

        _merge_existing_count_report(report, selected.counts)
        if db_preflight_mode:
            _set_status(report, STATUS_DB_PREFLIGHT_PASSED)
            return _finalize(report, raw_values, exit_code=0)

        try:
            await _run_service_handoff(
                report=report,
                session=session,
                selected=selected,
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


def _map_base_status(report: dict[str, Any]) -> None:
    if report["contract_status"] == base.STATUS_DB_READ_FAILED:
        report["contract_status"] = STATUS_DB_READ_FAILED


def _finalize(report: dict[str, Any], raw_values: set[str], *, exit_code: int) -> base.ScriptResult:
    if base._report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "report.raw_value_emission")
        exit_code = 1
    return base.ScriptResult(exit_code=exit_code, report=report)


def render_json(report: Mapping[str, Any]) -> str:
    return base.render_json(report)


def generate_report(**kwargs: Any) -> base.ScriptResult:
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
