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
from src.services.maintenance.repositories import MaintenanceRepository  # noqa: E402
from src.services.maintenance.retry_policy import (  # noqa: E402
    DELIVERY_RESULT_NOOP_CLASSIFICATION,
    DELIVERY_RESULT_NOOP_ERROR_CODE,
    DELIVERY_RESULT_NOOP_STAGE_NAME,
    classify_delivery_result_dry_run_noop,
)


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_maintenance_delivery_result_dry_run_noop_smoke"
REPORT_TYPE = "maintenance_delivery_result_dry_run_noop_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = base.DEFAULT_RUNTIME_ENV_PATH
DELIVERY_RESULT_EVENT_TYPE = "notification.delivery.result.v1"
MAINTENANCE_QUEUE_NAME = "q.maintenance"

STATUS_DEFAULT_PASSED = "maintenance_delivery_result_dry_run_noop_smoke_default_passed"
STATUS_DB_PREFLIGHT_PASSED = "maintenance_delivery_result_dry_run_noop_smoke_db_read_preflight_passed"
STATUS_DB_WRITE_PASSED = "maintenance_delivery_result_dry_run_noop_smoke_approved_db_write_passed"
STATUS_NOT_APPROVED = "blocked_maintenance_delivery_result_dry_run_noop_smoke_not_approved"
STATUS_DB_READ_FAILED = "blocked_maintenance_delivery_result_dry_run_noop_smoke_db_read_failed"
STATUS_NO_TARGET = "blocked_maintenance_delivery_result_dry_run_noop_smoke_no_clean_delivery_result"
STATUS_MULTIPLE_TARGETS = "blocked_maintenance_delivery_result_dry_run_noop_smoke_multiple_delivery_results"
STATUS_WRONG_DELIVERY_RESULT = "blocked_maintenance_delivery_result_dry_run_noop_smoke_wrong_delivery_result"
STATUS_EXISTING_MARKER = "blocked_maintenance_delivery_result_dry_run_noop_smoke_existing_marker"
STATUS_FORBIDDEN_EXISTING_EFFECT = "blocked_maintenance_delivery_result_dry_run_noop_smoke_forbidden_existing_effect"
STATUS_WRITE_FAILED = "blocked_maintenance_delivery_result_dry_run_noop_smoke_write_failed"
STATUS_RAW_VALUE_EMISSION = "blocked_maintenance_delivery_result_dry_run_noop_smoke_raw_value_emission"

UUID_PATTERN = (
    "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

SELECT_DRY_RUN_DELIVERY_RESULT_TARGETS_QUERY = f"""
WITH candidate_events AS (
    SELECT eo.event_id AS trigger_event_id,
           eo.created_at AS delivery_result_created_at,
           eo.aggregate_id AS aggregate_notification_plan_id,
           eo.payload_json,
           eo.payload_json->>'notification_delivery_record_id' AS payload_delivery_record_id,
           eo.payload_json->>'notification_plan_id' AS payload_notification_plan_id
    FROM event_outbox eo
    WHERE eo.event_type = 'notification.delivery.result.v1'
      AND eo.aggregate_type = 'notification_plan'
      AND lower(COALESCE(eo.payload_json->>'notification_delivery_record_id', '')) ~ '{UUID_PATTERN}'
      AND lower(COALESCE(eo.payload_json->>'notification_plan_id', '')) ~ '{UUID_PATTERN}'
)
SELECT ce.trigger_event_id,
       ce.delivery_result_created_at,
       np.notification_plan_id,
       ndr.notification_delivery_record_id,
       ndr.delivery_status,
       ndr.transport_error_code AS delivery_reason,
       np.analysis_id,
       np.candidate_group_id,
       a.judge_output_id,
       jo.judge_output_id AS judge_output_row_id,
       cgp.candidate_group_id AS candidate_row_id
FROM candidate_events ce
JOIN notification_plans np
  ON np.notification_plan_id = ce.aggregate_notification_plan_id
JOIN notification_delivery_records ndr
  ON ndr.notification_delivery_record_id = CAST(ce.payload_delivery_record_id AS uuid)
 AND ndr.notification_plan_id = np.notification_plan_id
JOIN analyses a
  ON a.analysis_id = np.analysis_id
LEFT JOIN judge_outputs jo
  ON jo.judge_output_id = a.judge_output_id
LEFT JOIN candidate_group_proposals cgp
  ON cgp.candidate_group_id = np.candidate_group_id
WHERE ce.payload_notification_plan_id = np.notification_plan_id::text
  AND ndr.delivery_status = 'suppressed'::notification_status_enum
  AND ndr.transport_error_code = 'dry_run_skip_transport'
ORDER BY ce.delivery_result_created_at DESC NULLS LAST,
         ce.trigger_event_id DESC
LIMIT 5
"""

COUNT_DELIVERY_RESULT_OUTBOX_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'notification.delivery.result.v1'
  AND aggregate_type = 'notification_plan'
  AND aggregate_id = CAST(:notification_plan_id AS uuid)
  AND payload_json->>'notification_delivery_record_id' = :notification_delivery_record_id
"""

COUNT_NOTIFICATION_PLAN_ROWS_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM notification_plans
WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
"""

COUNT_NOTIFICATION_RENDER_ROWS_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM notification_renders
WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
"""

COUNT_NOTIFICATION_DELIVERY_ROWS_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM notification_delivery_records
WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
"""

COUNT_NOTIFIER_STATE_TRANSITIONS_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM state_transitions
WHERE object_type = 'notification_plan'
  AND object_id = CAST(:notification_plan_id AS uuid)
"""

COUNT_ANALYSIS_ROWS_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM analyses
WHERE analysis_id = CAST(:analysis_id AS uuid)
"""

COUNT_POLICY_STATE_TRANSITIONS_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM state_transitions
WHERE object_type = 'analysis'
  AND object_id = CAST(:analysis_id AS uuid)
"""

COUNT_JUDGE_OUTPUT_ROWS_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM judge_outputs
WHERE judge_output_id = CAST(:judge_output_id AS uuid)
"""

COUNT_CANDIDATE_ROWS_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM candidate_group_proposals
WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
"""

COUNT_MAINTENANCE_JOB_ATTEMPTS_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM job_attempts
WHERE stage_name = :stage_name
  AND queue_name = :queue_name
  AND root_object_type = 'notification_plan'
  AND root_object_id = CAST(:notification_plan_id AS uuid)
  AND attempt_status = 'succeeded'::job_attempt_status_enum
  AND error_code = :error_code
"""

COUNT_RETRY_INTENT_OUTBOX_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'notification.plan.created.v1'
  AND aggregate_type = 'notification_plan'
  AND aggregate_id = CAST(:notification_plan_id AS uuid)
  AND (
      payload_json ? 'retry_reason'
      OR payload_json ? 'replay_reason'
  )
"""

COUNT_DEAD_LETTER_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM dead_letter_entries
WHERE root_object_type = 'notification_plan'
  AND root_object_id = CAST(:notification_plan_id AS uuid)
"""

COUNT_REPLAY_REQUEST_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM replay_requests
WHERE replay_type = 'delivery'::replay_type_enum
  AND root_object_type = 'notification_plan'
  AND root_object_id = CAST(:notification_plan_id AS uuid)
"""

SELECT_CANDIDATE_CURRENT_ANALYSIS_ID_QUERY = """
SELECT current_analysis_id
FROM candidate_group_proposals
WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
"""

SELECT_CANDIDATE_BUNDLE_FINGERPRINT_QUERY = """
SELECT md5(COALESCE(jsonb_agg(to_jsonb(candidate_evidence_bundles) ORDER BY bundle_id)::text, '[]'))
FROM candidate_evidence_bundles
WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
"""


@dataclass(frozen=True, slots=True)
class DeliveryResultTarget:
    trigger_event_id: UUID
    notification_plan_id: UUID
    notification_delivery_record_id: UUID
    delivery_status: str
    delivery_reason: str | None
    analysis_id: UUID
    judge_output_id: UUID
    candidate_group_id: UUID
    judge_output_found: bool
    candidate_found: bool


@dataclass(frozen=True, slots=True)
class TargetCounts:
    delivery_result_outbox: int
    notification_plans: int
    notification_renders: int
    notification_deliveries: int
    notifier_transitions: int
    analyses: int
    policy_transitions: int
    judge_outputs: int
    candidates: int
    maintenance_job_attempts: int
    retry_intents: int
    dead_letters: int
    replay_requests: int


@dataclass(frozen=True, slots=True)
class TargetSnapshots:
    candidate_current_analysis_id: Any
    candidate_bundle_fingerprint: Any


class ExpectedEffectsError(RuntimeError):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Approval-gated maintenance smoke for an existing notifier dry-run "
            "notification.delivery.result.v1. Default mode reads no runtime env, "
            "DB, Redis, Telegram token, OpenAI key, or transport."
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
        "maintenance_started": False,
        "maintenance_classification_bucket": "zero",
        "target_delivery_result_outbox_found_bucket": "zero",
        "target_notification_plan_found_bucket": "zero",
        "target_delivery_record_found_bucket": "zero",
        "target_delivery_status_bucket": "zero",
        "target_delivery_reason_bucket": "zero",
        "target_analysis_found_bucket": "zero",
        "target_judge_output_found_bucket": "zero",
        "target_candidate_found_bucket": "zero",
        "existing_maintenance_job_attempt_for_target_bucket": "zero",
        "existing_retry_intent_outbox_for_target_bucket": "zero",
        "existing_dead_letter_for_target_bucket": "zero",
        "existing_replay_request_for_target_bucket": "zero",
        "job_attempts_written_bucket": "zero",
        "retry_intent_outbox_written_bucket": "zero",
        "dead_letter_rows_written_bucket": "zero",
        "replay_requests_written_bucket": "zero",
        "notification_plan_rows_written_bucket": "zero",
        "notification_render_rows_written_bucket": "zero",
        "notification_delivery_rows_written_bucket": "zero",
        "notifier_state_transitions_written_bucket": "zero",
        "analysis_rows_written_bucket": "zero",
        "judge_outputs_written_bucket": "zero",
        "policy_state_transitions_written_bucket": "zero",
        "candidate_bundle_mutation_attempted": False,
        "candidate_current_analysis_mutation_attempted": False,
        "redis_connected": False,
        "redis_write_attempted": False,
        "redis_ack_attempted": False,
        "redis_delete_or_trim_attempted": False,
        "telegram_send_attempted": False,
        "telegram_edit_attempted": False,
        "openai_call_attempted": False,
        "openai_key_read_bucket": "zero",
        "raw_values_emitted": False,
        "checks_failed": [],
    }


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


async def _read_runtime_config(
    *,
    report: dict[str, Any],
    runtime_env_path: str | Path,
    runtime_env_reader: base.RuntimeEnvReader | None,
    raw_values: set[str],
) -> str | None:
    try:
        values = (
            runtime_env_reader(runtime_env_path)
            if runtime_env_reader is not None
            else base.parse_runtime_env_file(runtime_env_path)
        )
        report["runtime_env_read"] = True
    except Exception:
        _set_status(report, STATUS_DB_READ_FAILED, "runtime_env.read")
        return None

    database_url = str(values.get("DATABASE_URL", "")).strip()
    raw_values.update(base._raw_values(database_url))
    report["database_configured"] = bool(database_url and base._database_url_is_supported(database_url))
    if not report["database_configured"]:
        _set_status(report, STATUS_DB_READ_FAILED, "database.config")
        return None
    return database_url


async def _select_target(
    *,
    report: dict[str, Any],
    session: base.AsyncSessionLike,
    raw_values: set[str],
) -> DeliveryResultTarget | None:
    rows = base._rows(await base._execute(session, SELECT_DRY_RUN_DELIVERY_RESULT_TARGETS_QUERY))
    report["target_delivery_result_outbox_found_bucket"] = base._bucket_count(len(rows))
    if not rows:
        _set_status(report, STATUS_NO_TARGET, "target.delivery_result_outbox_missing")
        return None
    if len(rows) != 1:
        _set_status(report, STATUS_MULTIPLE_TARGETS, "target.delivery_result_outbox_not_exactly_one")
        return None

    row = rows[0]
    if not isinstance(row, Mapping):
        _set_status(report, STATUS_DB_READ_FAILED, "target.row_shape")
        return None

    target = _target_from_row(row, raw_values=raw_values)
    if target is None:
        _set_status(report, STATUS_DB_READ_FAILED, "target.identity")
        return None

    report["target_notification_plan_found_bucket"] = "one"
    report["target_delivery_record_found_bucket"] = "one"
    report["target_delivery_status_bucket"] = target.delivery_status or "zero"
    report["target_delivery_reason_bucket"] = target.delivery_reason or "zero"
    report["target_analysis_found_bucket"] = "one"
    report["target_judge_output_found_bucket"] = "one" if target.judge_output_found else "zero"
    report["target_candidate_found_bucket"] = "one" if target.candidate_found else "zero"

    if not target.judge_output_found:
        _set_status(report, STATUS_DB_READ_FAILED, "judge_output.missing")
        return None
    if not target.candidate_found:
        _set_status(report, STATUS_DB_READ_FAILED, "candidate.missing")
        return None

    decision = classify_delivery_result_dry_run_noop(
        delivery_status=target.delivery_status,
        delivery_reason=target.delivery_reason,
    )
    report["maintenance_classification_bucket"] = decision.maintenance_classification
    if decision.action != "mark_logical_noop_success":
        _set_status(report, STATUS_WRONG_DELIVERY_RESULT, decision.reason_code)
        return None

    counts = await _load_counts(session, target)
    _merge_existing_count_report(report, counts)
    if counts.maintenance_job_attempts:
        _set_status(report, STATUS_EXISTING_MARKER, "maintenance_job_attempt.existing")
        return None
    forbidden = _status_for_existing_forbidden_effect(counts)
    if forbidden is not None:
        _set_status(report, STATUS_FORBIDDEN_EXISTING_EFFECT, forbidden)
        return None
    return target


def _target_from_row(row: Mapping[str, Any], *, raw_values: set[str]) -> DeliveryResultTarget | None:
    trigger_event_id = base._coerce_uuid(row.get("trigger_event_id"))
    notification_plan_id = base._coerce_uuid(row.get("notification_plan_id"))
    notification_delivery_record_id = base._coerce_uuid(row.get("notification_delivery_record_id"))
    analysis_id = base._coerce_uuid(row.get("analysis_id"))
    judge_output_id = base._coerce_uuid(row.get("judge_output_id"))
    candidate_group_id = base._coerce_uuid(row.get("candidate_group_id"))
    if (
        trigger_event_id is None
        or notification_plan_id is None
        or notification_delivery_record_id is None
        or analysis_id is None
        or judge_output_id is None
        or candidate_group_id is None
    ):
        return None
    raw_values.update(
        base._raw_values(
            trigger_event_id,
            notification_plan_id,
            notification_delivery_record_id,
            analysis_id,
            judge_output_id,
            candidate_group_id,
        )
    )
    return DeliveryResultTarget(
        trigger_event_id=trigger_event_id,
        notification_plan_id=notification_plan_id,
        notification_delivery_record_id=notification_delivery_record_id,
        delivery_status=str(row.get("delivery_status") or ""),
        delivery_reason=_string_or_none(row.get("delivery_reason")),
        analysis_id=analysis_id,
        judge_output_id=judge_output_id,
        candidate_group_id=candidate_group_id,
        judge_output_found=row.get("judge_output_row_id") is not None,
        candidate_found=row.get("candidate_row_id") is not None,
    )


async def _load_counts(session: base.AsyncSessionLike, target: DeliveryResultTarget) -> TargetCounts:
    params = {
        "notification_plan_id": str(target.notification_plan_id),
        "notification_delivery_record_id": str(target.notification_delivery_record_id),
        "analysis_id": str(target.analysis_id),
        "judge_output_id": str(target.judge_output_id),
        "candidate_group_id": str(target.candidate_group_id),
        "stage_name": DELIVERY_RESULT_NOOP_STAGE_NAME,
        "queue_name": MAINTENANCE_QUEUE_NAME,
        "error_code": DELIVERY_RESULT_NOOP_ERROR_CODE,
    }
    return TargetCounts(
        delivery_result_outbox=await base._count_query(session, COUNT_DELIVERY_RESULT_OUTBOX_FOR_TARGET_QUERY, params),
        notification_plans=await base._count_query(session, COUNT_NOTIFICATION_PLAN_ROWS_FOR_TARGET_QUERY, params),
        notification_renders=await base._count_query(session, COUNT_NOTIFICATION_RENDER_ROWS_FOR_TARGET_QUERY, params),
        notification_deliveries=await base._count_query(
            session, COUNT_NOTIFICATION_DELIVERY_ROWS_FOR_TARGET_QUERY, params
        ),
        notifier_transitions=await base._count_query(
            session, COUNT_NOTIFIER_STATE_TRANSITIONS_FOR_TARGET_QUERY, params
        ),
        analyses=await base._count_query(session, COUNT_ANALYSIS_ROWS_FOR_TARGET_QUERY, params),
        policy_transitions=await base._count_query(session, COUNT_POLICY_STATE_TRANSITIONS_FOR_TARGET_QUERY, params),
        judge_outputs=await base._count_query(session, COUNT_JUDGE_OUTPUT_ROWS_FOR_TARGET_QUERY, params),
        candidates=await base._count_query(session, COUNT_CANDIDATE_ROWS_FOR_TARGET_QUERY, params),
        maintenance_job_attempts=await base._count_query(
            session, COUNT_MAINTENANCE_JOB_ATTEMPTS_FOR_TARGET_QUERY, params
        ),
        retry_intents=await base._count_query(session, COUNT_RETRY_INTENT_OUTBOX_FOR_TARGET_QUERY, params),
        dead_letters=await base._count_query(session, COUNT_DEAD_LETTER_FOR_TARGET_QUERY, params),
        replay_requests=await base._count_query(session, COUNT_REPLAY_REQUEST_FOR_TARGET_QUERY, params),
    )


async def _load_snapshots(session: base.AsyncSessionLike, target: DeliveryResultTarget) -> TargetSnapshots:
    current_analysis_id = base._scalar(
        await base._execute(
            session,
            SELECT_CANDIDATE_CURRENT_ANALYSIS_ID_QUERY,
            {"candidate_group_id": str(target.candidate_group_id)},
        )
    )
    bundle_fingerprint = base._scalar(
        await base._execute(
            session,
            SELECT_CANDIDATE_BUNDLE_FINGERPRINT_QUERY,
            {"candidate_group_id": str(target.candidate_group_id)},
        )
    )
    return TargetSnapshots(
        candidate_current_analysis_id=current_analysis_id,
        candidate_bundle_fingerprint=bundle_fingerprint,
    )


def _merge_existing_count_report(report: dict[str, Any], counts: TargetCounts) -> None:
    report["target_delivery_result_outbox_found_bucket"] = base._bucket_count(counts.delivery_result_outbox)
    report["target_notification_plan_found_bucket"] = base._bucket_count(counts.notification_plans)
    report["target_delivery_record_found_bucket"] = base._bucket_count(counts.notification_deliveries)
    report["target_analysis_found_bucket"] = base._bucket_count(counts.analyses)
    report["target_judge_output_found_bucket"] = base._bucket_count(counts.judge_outputs)
    report["target_candidate_found_bucket"] = base._bucket_count(counts.candidates)
    report["existing_maintenance_job_attempt_for_target_bucket"] = base._bucket_count(counts.maintenance_job_attempts)
    report["existing_retry_intent_outbox_for_target_bucket"] = base._bucket_count(counts.retry_intents)
    report["existing_dead_letter_for_target_bucket"] = base._bucket_count(counts.dead_letters)
    report["existing_replay_request_for_target_bucket"] = base._bucket_count(counts.replay_requests)


def _merge_written_count_report(report: dict[str, Any], *, before: TargetCounts, after: TargetCounts) -> None:
    report["job_attempts_written_bucket"] = base._bucket_count(
        after.maintenance_job_attempts - before.maintenance_job_attempts
    )
    report["retry_intent_outbox_written_bucket"] = base._bucket_count(after.retry_intents - before.retry_intents)
    report["dead_letter_rows_written_bucket"] = base._bucket_count(after.dead_letters - before.dead_letters)
    report["replay_requests_written_bucket"] = base._bucket_count(after.replay_requests - before.replay_requests)
    report["notification_plan_rows_written_bucket"] = base._bucket_count(
        after.notification_plans - before.notification_plans
    )
    report["notification_render_rows_written_bucket"] = base._bucket_count(
        after.notification_renders - before.notification_renders
    )
    report["notification_delivery_rows_written_bucket"] = base._bucket_count(
        after.notification_deliveries - before.notification_deliveries
    )
    report["notifier_state_transitions_written_bucket"] = base._bucket_count(
        after.notifier_transitions - before.notifier_transitions
    )
    report["analysis_rows_written_bucket"] = base._bucket_count(after.analyses - before.analyses)
    report["judge_outputs_written_bucket"] = base._bucket_count(after.judge_outputs - before.judge_outputs)
    report["policy_state_transitions_written_bucket"] = base._bucket_count(
        after.policy_transitions - before.policy_transitions
    )


def _merge_snapshot_mutation_report(
    report: dict[str, Any],
    *,
    before: TargetSnapshots,
    after: TargetSnapshots,
) -> None:
    report["candidate_current_analysis_mutation_attempted"] = (
        str(after.candidate_current_analysis_id) != str(before.candidate_current_analysis_id)
    )
    report["candidate_bundle_mutation_attempted"] = (
        str(after.candidate_bundle_fingerprint) != str(before.candidate_bundle_fingerprint)
    )


def _status_for_existing_forbidden_effect(counts: TargetCounts) -> str | None:
    if counts.retry_intents:
        return "retry_intent_outbox.existing"
    if counts.dead_letters:
        return "dead_letter.existing"
    if counts.replay_requests:
        return "replay_request.existing"
    return None


async def _run_maintenance_marker_write(
    *,
    report: dict[str, Any],
    session: base.AsyncSessionLike,
    target: DeliveryResultTarget,
) -> None:
    before_counts = await _load_counts(session, target)
    before_snapshots = await _load_snapshots(session, target)
    decision = classify_delivery_result_dry_run_noop(
        delivery_status=target.delivery_status,
        delivery_reason=target.delivery_reason,
    )
    if decision.action != "mark_logical_noop_success":
        raise ExpectedEffectsError("target delivery result is not a dry-run noop")

    if session.in_transaction():
        await session.rollback()

    report["database_write_attempted"] = True
    report["maintenance_started"] = True
    report["maintenance_classification_bucket"] = decision.maintenance_classification
    async with session.begin():
        repository = MaintenanceRepository(session)
        await repository.insert_delivery_result_noop_job_attempt(target.notification_plan_id)
        after_counts = await _load_counts(session, target)
        after_snapshots = await _load_snapshots(session, target)
        _merge_written_count_report(report, before=before_counts, after=after_counts)
        _merge_snapshot_mutation_report(report, before=before_snapshots, after=after_snapshots)
        if not _approved_execution_succeeded(report):
            raise ExpectedEffectsError("maintenance dry-run noop effects did not match contract")


def _approved_execution_succeeded(report: Mapping[str, Any]) -> bool:
    return bool(
        report["maintenance_started"] is True
        and report["maintenance_classification_bucket"] == DELIVERY_RESULT_NOOP_CLASSIFICATION
        and report["job_attempts_written_bucket"] == "one"
        and report["retry_intent_outbox_written_bucket"] == "zero"
        and report["dead_letter_rows_written_bucket"] == "zero"
        and report["replay_requests_written_bucket"] == "zero"
        and report["notification_plan_rows_written_bucket"] == "zero"
        and report["notification_render_rows_written_bucket"] == "zero"
        and report["notification_delivery_rows_written_bucket"] == "zero"
        and report["notifier_state_transitions_written_bucket"] == "zero"
        and report["analysis_rows_written_bucket"] == "zero"
        and report["judge_outputs_written_bucket"] == "zero"
        and report["policy_state_transitions_written_bucket"] == "zero"
        and report["candidate_bundle_mutation_attempted"] is False
        and report["candidate_current_analysis_mutation_attempted"] is False
        and report["telegram_send_attempted"] is False
        and report["telegram_edit_attempted"] is False
        and report["redis_write_attempted"] is False
        and report["openai_call_attempted"] is False
        and report["openai_key_read_bucket"] == "zero"
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
        database_url = await _read_runtime_config(
            report=report,
            runtime_env_path=runtime_env_path,
            runtime_env_reader=runtime_env_reader,
            raw_values=raw_values,
        )
        if database_url is None:
            return _finalize(report, raw_values, exit_code=1)

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

        target = await _select_target(report=report, session=session, raw_values=raw_values)
        if target is None:
            return _finalize(report, raw_values, exit_code=1)

        if db_preflight_mode:
            _set_status(report, STATUS_DB_PREFLIGHT_PASSED)
            return _finalize(report, raw_values, exit_code=0)

        try:
            await _run_maintenance_marker_write(report=report, session=session, target=target)
            committed = True
        except ExpectedEffectsError:
            _set_status(report, STATUS_WRITE_FAILED, "db_write.expected_effects")
            return _finalize(report, raw_values, exit_code=1)
        except Exception:
            _set_status(report, STATUS_WRITE_FAILED, "maintenance.marker_write")
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


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


if __name__ == "__main__":
    raise SystemExit(main())
