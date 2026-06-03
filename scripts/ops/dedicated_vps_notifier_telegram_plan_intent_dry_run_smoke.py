from __future__ import annotations

import argparse
import asyncio
import re
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
from src.services.notifier_telegram.config import NotifierTelegramConfig  # noqa: E402
from src.services.notifier_telegram.repositories import NotifierTelegramRepository  # noqa: E402
from src.services.notifier_telegram.service import NotifierTelegramService  # noqa: E402


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_notifier_telegram_plan_intent_dry_run_smoke"
REPORT_TYPE = "notifier_telegram_plan_intent_dry_run_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = base.DEFAULT_RUNTIME_ENV_PATH
PLAN_INTENT_EVENT_TYPE = "notification.plan.created.v1"
DELIVERY_RESULT_EVENT_TYPE = "notification.delivery.result.v1"

STATUS_DEFAULT_PASSED = "notifier_telegram_plan_intent_dry_run_smoke_default_passed"
STATUS_DB_PREFLIGHT_PASSED = "notifier_telegram_plan_intent_dry_run_smoke_db_read_preflight_passed"
STATUS_DB_WRITE_PASSED = "notifier_telegram_plan_intent_dry_run_smoke_approved_db_write_passed"
STATUS_NOT_APPROVED = "blocked_notifier_telegram_plan_intent_dry_run_smoke_not_approved"
STATUS_DB_READ_FAILED = "blocked_notifier_telegram_plan_intent_dry_run_smoke_db_read_failed"
STATUS_NO_CLEAN_PLAN_INTENT = "blocked_notifier_telegram_plan_intent_dry_run_smoke_no_clean_plan_intent"
STATUS_SUPPRESS_ANALYSIS = "blocked_notifier_telegram_plan_intent_dry_run_smoke_suppress_analysis"
STATUS_EXISTING_NOTIFIER_ROW = "blocked_notifier_telegram_plan_intent_dry_run_smoke_existing_notifier_row"
STATUS_VALIDATION_FAILED = "blocked_notifier_telegram_plan_intent_dry_run_smoke_validation_failed"
STATUS_WRITE_FAILED = "blocked_notifier_telegram_plan_intent_dry_run_smoke_write_failed"
STATUS_RAW_VALUE_EMISSION = "blocked_notifier_telegram_plan_intent_dry_run_smoke_raw_value_emission"

SELECT_RECENT_PLAN_INTENTS_QUERY = """
SELECT eo.event_id AS trigger_event_id,
       eo.created_at AS plan_intent_created_at,
       eo.aggregate_id AS aggregate_analysis_id,
       eo.payload_json,
       a.analysis_id,
       a.judge_output_id,
       a.candidate_group_id,
       a.delivery_decision AS analysis_delivery_decision,
       jo.judge_output_id AS judge_output_row_id,
       cgp.candidate_group_id AS candidate_row_id
FROM event_outbox eo
JOIN analyses a
  ON a.analysis_id = eo.aggregate_id
LEFT JOIN judge_outputs jo
  ON jo.judge_output_id = a.judge_output_id
LEFT JOIN candidate_group_proposals cgp
  ON cgp.candidate_group_id = a.candidate_group_id
WHERE eo.event_type = 'notification.plan.created.v1'
  AND eo.aggregate_type = 'analysis'
  AND eo.payload_json->>'notification_plan_id' IS NOT NULL
  AND eo.payload_json->>'analysis_id' = a.analysis_id::text
  AND eo.payload_json->>'candidate_group_id' = a.candidate_group_id::text
ORDER BY eo.created_at DESC NULLS LAST,
         eo.event_id DESC
LIMIT 50
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

COUNT_PLAN_INTENT_OUTBOX_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'notification.plan.created.v1'
  AND payload_json->>'notification_plan_id' = :notification_plan_id
"""

COUNT_JUDGE_OUTPUTS_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM judge_outputs
WHERE judge_output_id = CAST(:judge_output_id AS uuid)
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

COUNT_DELIVERY_RESULT_OUTBOX_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'notification.delivery.result.v1'
  AND aggregate_type = 'notification_plan'
  AND aggregate_id = CAST(:notification_plan_id AS uuid)
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

SELECT_LATEST_DELIVERY_RESULT_FOR_TARGET_QUERY = """
SELECT delivery_status, transport_error_code
FROM notification_delivery_records
WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
ORDER BY created_at DESC
LIMIT 1
"""


@dataclass(frozen=True, slots=True)
class PlanIntentTarget:
    trigger_event_id: UUID
    notification_plan_id: UUID
    analysis_id: UUID
    judge_output_id: UUID
    candidate_group_id: UUID


@dataclass(frozen=True, slots=True)
class SelectedTarget:
    target: PlanIntentTarget


@dataclass(frozen=True, slots=True)
class TargetCounts:
    analyses: int
    policy_transitions: int
    notification_plan_intents: int
    judge_outputs: int
    notification_plans: int
    notification_renders: int
    notification_deliveries: int
    notifier_transitions: int
    delivery_result_outbox: int


@dataclass(frozen=True, slots=True)
class TargetSnapshots:
    candidate_current_analysis_id: Any
    candidate_bundle_fingerprint: Any


class ExpectedEffectsError(RuntimeError):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Approval-gated notifier-telegram smoke from persisted notification.plan.created.v1 "
            "plan intent to dry-run notifier-owned durable rows. Default mode reads no runtime "
            "env, DB, Redis, OpenAI key, policy-engine, notifier worker, or Telegram transport."
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
        "target_plan_intent_found_bucket": "zero",
        "target_analysis_found_bucket": "zero",
        "target_judge_output_found_bucket": "zero",
        "target_candidate_found_bucket": "zero",
        "target_chat_id_available_bucket": "zero",
        "payload_identity_valid_bucket": "zero",
        "existing_notification_plan_rows_for_target_bucket": "zero",
        "existing_notification_render_rows_for_target_bucket": "zero",
        "existing_notification_delivery_rows_for_target_bucket": "zero",
        "existing_notification_delivery_result_outbox_for_target_bucket": "zero",
        "notification_plan_rows_written_bucket": "zero",
        "notification_render_rows_written_bucket": "zero",
        "notification_delivery_rows_written_bucket": "zero",
        "notifier_state_transitions_written_bucket": "zero",
        "notification_delivery_result_outbox_written_bucket": "zero",
        "notification_delivery_status_bucket": "zero",
        "notification_delivery_reason_bucket": "zero",
        "analysis_rows_written_bucket": "zero",
        "policy_state_transitions_written_bucket": "zero",
        "notification_plan_intent_outbox_written_bucket": "zero",
        "judge_outputs_written_bucket": "zero",
        "candidate_bundle_mutation_attempted": False,
        "candidate_current_analysis_mutation_attempted": False,
        "policy_engine_started": False,
        "notifier_started": False,
        "telegram_send_attempted": False,
        "telegram_edit_attempted": False,
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


async def _read_runtime_config(
    *,
    report: dict[str, Any],
    runtime_env_path: str | Path,
    runtime_env_reader: base.RuntimeEnvReader | None,
    raw_values: set[str],
) -> tuple[str, int | None] | None:
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
    operator_chat_id_text = str(values.get("TELEGRAM_OPERATOR_CHAT_ID", "")).strip()
    raw_values.update(base._raw_values(database_url, operator_chat_id_text))
    report["database_configured"] = bool(database_url and base._database_url_is_supported(database_url))
    if not report["database_configured"]:
        _set_status(report, STATUS_DB_READ_FAILED, "database.config")
        return None
    return database_url, base._operator_chat_id(operator_chat_id_text)


def _payload_from_row(row: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = row.get("payload_json")
    if isinstance(payload, Mapping):
        return payload
    return {}


def _target_from_row(row: Mapping[str, Any], report: dict[str, Any]) -> PlanIntentTarget | None:
    payload = _payload_from_row(row)
    trigger_event_id = base._coerce_uuid(row.get("trigger_event_id"))
    notification_plan_id = base._coerce_uuid(payload.get("notification_plan_id"))
    analysis_id = base._coerce_uuid(payload.get("analysis_id"))
    judge_output_id = base._coerce_uuid(row.get("judge_output_id"))
    candidate_group_id = base._coerce_uuid(payload.get("candidate_group_id"))
    if (
        trigger_event_id is None
        or notification_plan_id is None
        or analysis_id is None
        or judge_output_id is None
        or candidate_group_id is None
    ):
        _set_status(report, STATUS_DB_READ_FAILED, "target.identity")
        return None
    return PlanIntentTarget(
        trigger_event_id=trigger_event_id,
        notification_plan_id=notification_plan_id,
        analysis_id=analysis_id,
        judge_output_id=judge_output_id,
        candidate_group_id=candidate_group_id,
    )


async def _inspect_preflight(
    *,
    report: dict[str, Any],
    session: base.AsyncSessionLike,
    target: PlanIntentTarget,
    operator_chat_id: int | None,
    raw_values: set[str],
) -> bool:
    repository = NotifierTelegramRepository(session)
    intent = await repository.load_intent_job(target.trigger_event_id)
    if intent is None:
        _set_status(report, STATUS_VALIDATION_FAILED, "payload.identity")
        return False
    raw_values.update(
        base._raw_values(
            intent.trigger_event_id,
            intent.event_type,
            intent.notification_plan_id,
            intent.analysis_id,
            intent.candidate_group_id,
            intent.target_chat_id,
            intent.dedupe_subject_key,
            intent.material_change_hash,
        )
    )

    identity_valid = (
        intent.event_type == PLAN_INTENT_EVENT_TYPE
        and intent.notification_plan_id == target.notification_plan_id
        and intent.analysis_id == target.analysis_id
        and intent.candidate_group_id == target.candidate_group_id
    )
    report["payload_identity_valid_bucket"] = "one" if identity_valid else "zero"
    if not identity_valid:
        _set_status(report, STATUS_VALIDATION_FAILED, "payload.identity")
        return False

    analysis = await repository.load_analysis(intent.analysis_id)
    report["target_analysis_found_bucket"] = "one" if analysis is not None else "zero"
    if analysis is None:
        _set_status(report, STATUS_DB_READ_FAILED, "analysis.missing")
        return False
    raw_values.update(
        base._raw_values(
            analysis.analysis_id,
            analysis.candidate_group_id,
            analysis.judge_output_id,
            analysis.reason_codes_json,
            analysis.evidence_limitations_ko,
            analysis.recommended_action_ko,
            analysis.freshness_note_ko,
        )
    )
    if analysis.candidate_group_id != intent.candidate_group_id or analysis.judge_output_id != target.judge_output_id:
        report["payload_identity_valid_bucket"] = "zero"
        _set_status(report, STATUS_VALIDATION_FAILED, "analysis.identity")
        return False
    if analysis.delivery_decision == "suppress":
        _set_status(report, STATUS_SUPPRESS_ANALYSIS, "analysis.delivery_decision_suppress")
        return False
    if analysis.delivery_decision != intent.delivery_decision:
        report["payload_identity_valid_bucket"] = "zero"
        _set_status(report, STATUS_VALIDATION_FAILED, "analysis.delivery_decision_mismatch")
        return False

    judge_output = await repository.load_judge_output_render_fields(analysis.judge_output_id)
    report["target_judge_output_found_bucket"] = "one" if judge_output is not None else "zero"
    if judge_output is None:
        _set_status(report, STATUS_DB_READ_FAILED, "judge_output.missing")
        return False
    raw_values.update(base._raw_values(judge_output.judge_output_id, judge_output.payload_json))

    candidate = await repository.load_candidate_render_context(intent.candidate_group_id)
    report["target_candidate_found_bucket"] = "one" if candidate is not None else "zero"
    if candidate is None:
        _set_status(report, STATUS_DB_READ_FAILED, "candidate.missing")
        return False
    raw_values.update(
        base._raw_values(
            candidate.candidate_group_id,
            candidate.source_message_id,
            candidate.current_primary_artifact_id,
            candidate.primary_canonical_url,
            candidate.primary_canonical_id,
            candidate.source_message_link,
            candidate.source_text_surface,
        )
    )

    chat_id_matches = operator_chat_id is not None and intent.target_chat_id == operator_chat_id
    report["target_chat_id_available_bucket"] = "one" if chat_id_matches else "zero"
    if operator_chat_id is None:
        _set_status(report, STATUS_VALIDATION_FAILED, "config.target_chat_id_unavailable")
        return False
    if not chat_id_matches:
        _set_status(report, STATUS_VALIDATION_FAILED, "target.target_chat_id_mismatch")
        return False

    return True


async def _load_counts(session: base.AsyncSessionLike, target: PlanIntentTarget) -> TargetCounts:
    params = {
        "notification_plan_id": str(target.notification_plan_id),
        "analysis_id": str(target.analysis_id),
        "judge_output_id": str(target.judge_output_id),
    }
    return TargetCounts(
        analyses=await base._count_query(session, COUNT_ANALYSIS_ROWS_FOR_TARGET_QUERY, params),
        policy_transitions=await base._count_query(session, COUNT_POLICY_STATE_TRANSITIONS_FOR_TARGET_QUERY, params),
        notification_plan_intents=await base._count_query(session, COUNT_PLAN_INTENT_OUTBOX_FOR_TARGET_QUERY, params),
        judge_outputs=await base._count_query(session, COUNT_JUDGE_OUTPUTS_FOR_TARGET_QUERY, params),
        notification_plans=await base._count_query(session, COUNT_NOTIFICATION_PLAN_ROWS_FOR_TARGET_QUERY, params),
        notification_renders=await base._count_query(session, COUNT_NOTIFICATION_RENDER_ROWS_FOR_TARGET_QUERY, params),
        notification_deliveries=await base._count_query(session, COUNT_NOTIFICATION_DELIVERY_ROWS_FOR_TARGET_QUERY, params),
        notifier_transitions=await base._count_query(session, COUNT_NOTIFIER_STATE_TRANSITIONS_FOR_TARGET_QUERY, params),
        delivery_result_outbox=await base._count_query(session, COUNT_DELIVERY_RESULT_OUTBOX_FOR_TARGET_QUERY, params),
    )


async def _load_snapshots(session: base.AsyncSessionLike, target: PlanIntentTarget) -> TargetSnapshots:
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
    report["existing_notification_plan_rows_for_target_bucket"] = base._bucket_count(counts.notification_plans)
    report["existing_notification_render_rows_for_target_bucket"] = base._bucket_count(counts.notification_renders)
    report["existing_notification_delivery_rows_for_target_bucket"] = base._bucket_count(counts.notification_deliveries)
    report["existing_notification_delivery_result_outbox_for_target_bucket"] = base._bucket_count(
        counts.delivery_result_outbox
    )


def _merge_written_count_report(
    report: dict[str, Any],
    *,
    before: TargetCounts,
    after: TargetCounts,
) -> None:
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
    report["notification_delivery_result_outbox_written_bucket"] = base._bucket_count(
        after.delivery_result_outbox - before.delivery_result_outbox
    )
    report["analysis_rows_written_bucket"] = base._bucket_count(after.analyses - before.analyses)
    report["policy_state_transitions_written_bucket"] = base._bucket_count(
        after.policy_transitions - before.policy_transitions
    )
    report["notification_plan_intent_outbox_written_bucket"] = base._bucket_count(
        after.notification_plan_intents - before.notification_plan_intents
    )
    report["judge_outputs_written_bucket"] = base._bucket_count(after.judge_outputs - before.judge_outputs)


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


def _status_for_count_block(counts: TargetCounts) -> tuple[str, str] | None:
    if counts.notification_plans:
        return STATUS_EXISTING_NOTIFIER_ROW, "notification_plans.existing"
    if counts.notification_renders:
        return STATUS_EXISTING_NOTIFIER_ROW, "notification_renders.existing"
    if counts.notification_deliveries:
        return STATUS_EXISTING_NOTIFIER_ROW, "notification_delivery_records.existing"
    if counts.delivery_result_outbox:
        return STATUS_EXISTING_NOTIFIER_ROW, "event_outbox.notification_delivery_result_existing"
    return None


async def _select_clean_target(
    *,
    report: dict[str, Any],
    session: base.AsyncSessionLike,
    operator_chat_id: int | None,
    raw_values: set[str],
) -> SelectedTarget | None:
    rows = base._rows(await base._execute(session, SELECT_RECENT_PLAN_INTENTS_QUERY))
    report["target_plan_intent_found_bucket"] = "one" if rows else "zero"
    if not rows:
        _set_status(report, STATUS_NO_CLEAN_PLAN_INTENT, "target.notification_plan_intent_missing")
        return None

    first_blocked: tuple[dict[str, Any], str, str] | None = None
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        raw_values.update(base._raw_values(row))
        target = _target_from_row(row, report)
        if target is None:
            return None
        ok = await _inspect_preflight(
            report=report,
            session=session,
            target=target,
            operator_chat_id=operator_chat_id,
            raw_values=raw_values,
        )
        if not ok:
            return None

        counts = await _load_counts(session, target)
        _merge_existing_count_report(report, counts)
        blocked = _status_for_count_block(counts)
        if blocked is not None:
            if first_blocked is None:
                status, check = blocked
                first_blocked = dict(report), status, check
            continue
        return SelectedTarget(target=target)

    if first_blocked is not None:
        snapshot, status, check = first_blocked
        report.clear()
        report.update(snapshot)
        _set_status(report, status, check)
        return None

    _set_status(report, STATUS_NO_CLEAN_PLAN_INTENT, "target.notification_plan_intent_missing")
    return None


def _notifier_config(*, database_url: str) -> NotifierTelegramConfig:
    return NotifierTelegramConfig(
        app_env="prod",
        database_url=database_url,
        redis_url="redis://disabled.invalid/0",
        telegram_bot_token="",
        queue_name="q.notification.send",
        consumer_group="notifier-telegram",
        consumer_name=SCRIPT_NAME,
        batch_size=1,
        block_ms=1,
        dry_run=True,
        allow_edits=False,
        enable_notification_send=True,
        enable_digest_runtime=False,
        max_message_chars=3800,
        edit_window_minutes=180,
        telegram_api_base_url="https://api.telegram.org",
        request_timeout_sec=1.0,
        log_level="INFO",
    )


async def _merge_delivery_result_report(
    *,
    report: dict[str, Any],
    session: base.AsyncSessionLike,
    target: PlanIntentTarget,
) -> None:
    row = base._first_mapping(
        await base._execute(
            session,
            SELECT_LATEST_DELIVERY_RESULT_FOR_TARGET_QUERY,
            {"notification_plan_id": str(target.notification_plan_id)},
        )
    )
    if row is None:
        return
    status = str(row.get("delivery_status") or "")
    reason = str(row.get("transport_error_code") or "")
    report["notification_delivery_status_bucket"] = status if status else "zero"
    report["notification_delivery_reason_bucket"] = reason if reason else "zero"


async def _run_service_concretization(
    *,
    report: dict[str, Any],
    session: base.AsyncSessionLike,
    selected: SelectedTarget,
    database_url: str,
) -> None:
    before_counts = await _load_counts(session, selected.target)
    before_snapshots = await _load_snapshots(session, selected.target)
    repository = NotifierTelegramRepository(session)
    service = NotifierTelegramService(
        _notifier_config(database_url=database_url),
        repository=repository,
        telegram_client=None,
    )

    if session.in_transaction():
        await session.rollback()

    report["database_write_attempted"] = True
    report["notifier_started"] = True
    async with session.begin():
        intent = await service.rehydrate_intent(selected.target.trigger_event_id)
        if intent is None:
            raise ExpectedEffectsError("notifier failed to rehydrate plan intent")
        await service.handle_intent(intent)
        after_counts = await _load_counts(session, selected.target)
        after_snapshots = await _load_snapshots(session, selected.target)
        _merge_written_count_report(report, before=before_counts, after=after_counts)
        _merge_snapshot_mutation_report(report, before=before_snapshots, after=after_snapshots)
        await _merge_delivery_result_report(report=report, session=session, target=selected.target)
        if not _approved_execution_succeeded(report):
            raise ExpectedEffectsError("notifier dry-run smoke effects did not match contract")


def _approved_execution_succeeded(report: Mapping[str, Any]) -> bool:
    return bool(
        report["notification_plan_rows_written_bucket"] == "one"
        and report["notification_render_rows_written_bucket"] == "one"
        and report["notification_delivery_rows_written_bucket"] == "one"
        and report["notifier_state_transitions_written_bucket"] in {"one", "multiple"}
        and report["notification_delivery_result_outbox_written_bucket"] == "one"
        and report["notification_delivery_status_bucket"] == "suppressed"
        and report["notification_delivery_reason_bucket"] in {
            "dry_run_skip_transport",
            "notification_send_flag_disabled",
        }
        and report["analysis_rows_written_bucket"] == "zero"
        and report["policy_state_transitions_written_bucket"] == "zero"
        and report["notification_plan_intent_outbox_written_bucket"] == "zero"
        and report["judge_outputs_written_bucket"] == "zero"
        and report["candidate_bundle_mutation_attempted"] is False
        and report["candidate_current_analysis_mutation_attempted"] is False
        and report["policy_engine_started"] is False
        and report["notifier_started"] is True
        and report["telegram_send_attempted"] is False
        and report["telegram_edit_attempted"] is False
        and report["redis_write_attempted"] is False
        and report["redis_ack_attempted"] is False
        and report["redis_delete_or_trim_attempted"] is False
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

        selected = await _select_clean_target(
            report=report,
            session=session,
            operator_chat_id=operator_chat_id,
            raw_values=raw_values,
        )
        if selected is None:
            return _finalize(report, raw_values, exit_code=1)

        if db_preflight_mode:
            _set_status(report, STATUS_DB_PREFLIGHT_PASSED)
            return _finalize(report, raw_values, exit_code=0)

        try:
            await _run_service_concretization(
                report=report,
                session=session,
                selected=selected,
                database_url=database_url,
            )
            committed = True
        except ExpectedEffectsError:
            _set_status(report, STATUS_WRITE_FAILED, "db_write.expected_effects")
            return _finalize(report, raw_values, exit_code=1)
        except Exception:
            _set_status(report, STATUS_WRITE_FAILED, "service.handle_intent")
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


if __name__ == "__main__":
    raise SystemExit(main())
