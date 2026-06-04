from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.ops import dedicated_vps_policy_engine_analysis_handoff_smoke as base  # noqa: E402
from src.services.maintenance.retry_policy import (  # noqa: E402
    DELIVERY_RESULT_NOOP_ERROR_CODE,
    DELIVERY_RESULT_NOOP_STAGE_NAME,
)
from src.services.notifier_telegram.config import NotifierTelegramConfig  # noqa: E402
from src.services.notifier_telegram.models import (  # noqa: E402
    DeliveryResult,
    NotificationIntentJob,
    NotificationRenderDraft,
)
from src.services.notifier_telegram.repositories import NotifierTelegramRepository  # noqa: E402
from src.services.notifier_telegram.telegram_client import (  # noqa: E402
    TelegramBotClient,
    TelegramTransportRetryableError,
    TelegramTransportTerminalError,
)


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_notifier_telegram_restricted_send_canary_smoke"
REPORT_TYPE = "notifier_telegram_restricted_send_canary_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = base.DEFAULT_RUNTIME_ENV_PATH
PLAN_INTENT_EVENT_TYPE = "notification.plan.created.v1"
DELIVERY_RESULT_EVENT_TYPE = "notification.delivery.result.v1"
MAINTENANCE_QUEUE_NAME = "q.maintenance"

STATUS_DEFAULT_PASSED = "notifier_telegram_restricted_send_canary_smoke_default_passed"
STATUS_DB_PREFLIGHT_PASSED = "notifier_telegram_restricted_send_canary_smoke_db_read_preflight_passed"
STATUS_APPROVED_SEND_PASSED = "notifier_telegram_restricted_send_canary_smoke_approved_send_passed"
STATUS_NOT_APPROVED = "blocked_notifier_telegram_restricted_send_canary_smoke_not_approved"
STATUS_DB_READ_FAILED = "blocked_notifier_telegram_restricted_send_canary_smoke_db_read_failed"
STATUS_NO_CLEAN_TARGET = "blocked_notifier_telegram_restricted_send_canary_smoke_no_clean_target"
STATUS_MULTIPLE_CLEAN_TARGETS = "blocked_notifier_telegram_restricted_send_canary_smoke_multiple_clean_targets"
STATUS_VALIDATION_FAILED = "blocked_notifier_telegram_restricted_send_canary_smoke_validation_failed"
STATUS_EXISTING_SUCCESSFUL_DELIVERY = (
    "blocked_notifier_telegram_restricted_send_canary_smoke_existing_successful_delivery"
)
STATUS_TELEGRAM_RETRYABLE = "blocked_notifier_telegram_restricted_send_canary_smoke_telegram_retryable"
STATUS_TELEGRAM_TERMINAL = "blocked_notifier_telegram_restricted_send_canary_smoke_telegram_terminal"
STATUS_WRITE_FAILED = "blocked_notifier_telegram_restricted_send_canary_smoke_write_failed"
STATUS_RAW_VALUE_EMISSION = "blocked_notifier_telegram_restricted_send_canary_smoke_raw_value_emission"

RUNTIME_ENV_KEYS_WITHOUT_TOKEN = frozenset({"DATABASE_URL", "TELEGRAM_OPERATOR_CHAT_ID"})
RUNTIME_ENV_KEYS_WITH_TOKEN = frozenset(
    {"DATABASE_URL", "TELEGRAM_OPERATOR_CHAT_ID", "TELEGRAM_BOT_TOKEN"}
)
PUBLIC_LITERAL_VALUES = base.PUBLIC_LITERAL_VALUES | frozenset(
    {
        SCRIPT_NAME,
        REPORT_TYPE,
        PLAN_INTENT_EVENT_TYPE,
        DELIVERY_RESULT_EVENT_TYPE,
        MAINTENANCE_QUEUE_NAME,
        DELIVERY_RESULT_NOOP_ERROR_CODE,
        DELIVERY_RESULT_NOOP_STAGE_NAME,
        STATUS_DEFAULT_PASSED,
        STATUS_DB_PREFLIGHT_PASSED,
        STATUS_APPROVED_SEND_PASSED,
        STATUS_NOT_APPROVED,
        STATUS_DB_READ_FAILED,
        STATUS_NO_CLEAN_TARGET,
        STATUS_MULTIPLE_CLEAN_TARGETS,
        STATUS_VALIDATION_FAILED,
        STATUS_EXISTING_SUCCESSFUL_DELIVERY,
        STATUS_TELEGRAM_RETRYABLE,
        STATUS_TELEGRAM_TERMINAL,
        STATUS_WRITE_FAILED,
        STATUS_RAW_VALUE_EMISSION,
        "notification_plan",
        "notification_plan_id",
        "notification_delivery_record_id",
        "notification_delivery_result",
        "dry_run_skip_transport",
        "sent",
        "edited",
        "planned",
        "rendered",
        "queued",
        "failed_retryable",
        "failed_terminal",
        "TelegramTransportRetryableError",
        "TelegramTransportTerminalError",
        "ConfigurationError",
        "telegram_rate_limited",
        "telegram_5xx_retryable",
        "telegram_network_retryable",
        "telegram_flood_retryable",
        "telegram_retryable",
        "telegram_invalid_chat",
        "telegram_bot_blocked",
        "telegram_insufficient_rights",
        "telegram_edit_message_not_found",
        "telegram_message_cannot_be_edited",
        "telegram_malformed_message",
        "telegram_terminal",
        "telegram_client_missing",
        "NotifierTelegramRepository",
        "one_or_more",
    }
)

SELECT_CANARY_TARGETS_QUERY = f"""
WITH plan_intent_targets AS (
    SELECT DISTINCT ON (np.notification_plan_id)
           pi.event_id AS plan_intent_event_id,
           pi.created_at AS plan_intent_created_at,
           np.notification_plan_id,
           np.analysis_id,
           np.candidate_group_id,
           np.delivery_decision,
           np.urgency_profile,
           np.target_chat_id,
           np.target_thread_id,
           np.render_profile,
           np.dedupe_subject_key,
           np.material_change_hash,
           np.send_after,
           np.suppress_reason_code,
           np.status AS plan_status,
           a.judge_output_id,
           jo.judge_output_id AS judge_output_row_id,
           cgp.candidate_group_id AS candidate_row_id
    FROM notification_plans np
    JOIN analyses a
      ON a.analysis_id = np.analysis_id
    LEFT JOIN judge_outputs jo
      ON jo.judge_output_id = a.judge_output_id
    LEFT JOIN candidate_group_proposals cgp
      ON cgp.candidate_group_id = np.candidate_group_id
    JOIN event_outbox pi
      ON pi.event_type = 'notification.plan.created.v1'
     AND pi.aggregate_type = 'analysis'
     AND pi.aggregate_id = np.analysis_id
     AND pi.payload_json->>'notification_plan_id' = np.notification_plan_id::text
     AND pi.payload_json->>'analysis_id' = np.analysis_id::text
     AND pi.payload_json->>'candidate_group_id' = np.candidate_group_id::text
    WHERE np.delivery_decision <> 'suppress'::delivery_decision_enum
      AND np.target_chat_id IS NOT NULL
    ORDER BY np.notification_plan_id,
             pi.created_at DESC NULLS LAST,
             pi.event_id DESC
)
SELECT pit.*,
       1 AS notification_plan_count,
       1 AS analysis_count,
       CASE WHEN pit.judge_output_row_id IS NULL THEN 0 ELSE 1 END AS judge_output_count,
       CASE WHEN pit.candidate_row_id IS NULL THEN 0 ELSE 1 END AS candidate_count,
       (
           SELECT COUNT(*)
           FROM notification_renders nr
           WHERE nr.notification_plan_id = pit.notification_plan_id
       ) AS render_count,
       (
           SELECT COUNT(*)
           FROM notification_delivery_records ndr
           WHERE ndr.notification_plan_id = pit.notification_plan_id
             AND ndr.delivery_status = 'suppressed'::notification_status_enum
             AND ndr.transport_error_code = 'dry_run_skip_transport'
       ) AS prior_dry_run_delivery_count,
       (
           SELECT COUNT(*)
           FROM event_outbox eo
           WHERE eo.event_type = 'notification.delivery.result.v1'
             AND eo.aggregate_type = 'notification_plan'
             AND eo.aggregate_id = pit.notification_plan_id
             AND EXISTS (
                 SELECT 1
                 FROM notification_delivery_records ndr
                 WHERE ndr.notification_plan_id = pit.notification_plan_id
                   AND ndr.delivery_status = 'suppressed'::notification_status_enum
                   AND ndr.transport_error_code = 'dry_run_skip_transport'
                   AND eo.payload_json->>'notification_delivery_record_id'
                       = ndr.notification_delivery_record_id::text
             )
       ) AS prior_delivery_result_outbox_count,
       (
           SELECT COUNT(*)
           FROM job_attempts ja
           WHERE ja.stage_name = '{DELIVERY_RESULT_NOOP_STAGE_NAME}'
             AND ja.queue_name = '{MAINTENANCE_QUEUE_NAME}'
             AND ja.root_object_type = 'notification_plan'
             AND ja.root_object_id = pit.notification_plan_id
             AND ja.attempt_status = 'succeeded'::job_attempt_status_enum
             AND ja.error_code = '{DELIVERY_RESULT_NOOP_ERROR_CODE}'
       ) AS prior_maintenance_noop_marker_count,
       (
           SELECT COUNT(*)
           FROM notification_delivery_records ndr
           WHERE ndr.notification_plan_id = pit.notification_plan_id
             AND ndr.delivery_status IN (
                 'sent'::notification_status_enum,
                 'edited'::notification_status_enum
             )
       ) AS existing_successful_delivery_count
FROM plan_intent_targets pit
ORDER BY pit.plan_intent_created_at DESC NULLS LAST,
         pit.plan_intent_event_id DESC
LIMIT 20
"""

SELECT_RENDER_FOR_TARGET_QUERY = """
SELECT notification_plan_id,
       message_text,
       entities_json,
       link_preview_options_json,
       reply_markup_json,
       disable_notification,
       protect_content,
       parse_strategy,
       render_hash
FROM notification_renders
WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
ORDER BY created_at DESC NULLS LAST,
         notification_render_id DESC
LIMIT 2
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


class TelegramClientProtocol(Protocol):
    async def send_message(self, **kwargs: Any) -> dict[str, Any]: ...
    async def edit_message_text(self, **kwargs: Any) -> dict[str, Any]: ...


TelegramClientFactory = Callable[[str], TelegramClientProtocol]


@dataclass(frozen=True, slots=True)
class CanaryTarget:
    plan_intent_event_id: UUID
    notification_plan_id: UUID
    analysis_id: UUID
    judge_output_id: UUID
    candidate_group_id: UUID
    delivery_decision: str
    urgency_profile: str
    target_chat_id: int | None
    target_thread_id: int | None
    render_profile: str | None
    dedupe_subject_key: str
    material_change_hash: str
    send_after: datetime | None
    suppress_reason_code: str | None
    plan_status: str
    notification_plan_count: int
    render_count: int
    prior_dry_run_delivery_count: int
    prior_delivery_result_outbox_count: int
    prior_maintenance_noop_marker_count: int
    analysis_count: int
    judge_output_count: int
    candidate_count: int
    existing_successful_delivery_count: int


@dataclass(frozen=True, slots=True)
class TargetCounts:
    notification_plans: int
    notification_renders: int
    notification_deliveries: int
    notifier_transitions: int
    delivery_result_outbox: int
    analyses: int
    policy_transitions: int
    judge_outputs: int
    candidates: int


@dataclass(frozen=True, slots=True)
class TargetSnapshots:
    candidate_current_analysis_id: Any
    candidate_bundle_fingerprint: Any


class ExpectedEffectsError(RuntimeError):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Approval-gated restricted notifier-telegram live send canary over one existing "
            "non-suppress notification plan/render. Default mode reads no runtime env, DB, "
            "Telegram token, Redis, OpenAI key, or transport."
        )
    )
    parser.add_argument("--approve-db-read", action="store_true")
    parser.add_argument("--approve-db-write", action="store_true")
    parser.add_argument("--approve-telegram-send", action="store_true")
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def _base_report(
    *,
    approve_db_read: bool,
    approve_db_write: bool,
    approve_telegram_send: bool,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_DEFAULT_PASSED,
        "approvals": {
            "db_read": approve_db_read,
            "db_write": approve_db_write,
            "telegram_send": approve_telegram_send,
        },
        "runtime_env_read": False,
        "database_configured": False,
        "database_connected": False,
        "read_only_transaction": False,
        "database_write_attempted": False,
        "telegram_bot_token_read_bucket": "zero",
        "telegram_send_attempted": False,
        "telegram_send_result_bucket": "zero",
        "telegram_edit_attempted": False,
        "telegram_message_id_available_bucket": "zero",
        "notifier_transport_path_reused": False,
        "target_notification_plan_found_bucket": "zero",
        "target_notification_render_found_bucket": "zero",
        "target_prior_dry_run_delivery_found_bucket": "zero",
        "target_prior_delivery_result_outbox_found_bucket": "zero",
        "target_prior_maintenance_noop_marker_found_bucket": "zero",
        "target_analysis_found_bucket": "zero",
        "target_judge_output_found_bucket": "zero",
        "target_candidate_found_bucket": "zero",
        "target_chat_id_available_bucket": "zero",
        "target_chat_id_matches_runtime_bucket": "zero",
        "existing_successful_delivery_for_target_bucket": "zero",
        "notification_plan_rows_written_bucket": "zero",
        "notification_render_rows_written_bucket": "zero",
        "notification_delivery_rows_written_bucket": "zero",
        "notifier_state_transitions_written_bucket": "zero",
        "notification_delivery_result_outbox_written_bucket": "zero",
        "notification_delivery_status_bucket": "zero",
        "analysis_rows_written_bucket": "zero",
        "judge_outputs_written_bucket": "zero",
        "policy_state_transitions_written_bucket": "zero",
        "candidate_bundle_mutation_attempted": False,
        "candidate_current_analysis_mutation_attempted": False,
        "closed_predecessor_smoke_execution_attempted": False,
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


def parse_runtime_env_file(path: str | Path, *, include_telegram_bot_token: bool) -> dict[str, str]:
    allowed_keys = RUNTIME_ENV_KEYS_WITH_TOKEN if include_telegram_bot_token else RUNTIME_ENV_KEYS_WITHOUT_TOKEN
    values: dict[str, str] = {}
    with Path(path).open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, raw_value = line.split("=", 1)
            key = key.strip()
            if key in allowed_keys and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                values[key] = base._strip_optional_quotes(raw_value)
    return values


async def _read_runtime_config(
    *,
    report: dict[str, Any],
    runtime_env_path: str | Path,
    runtime_env_reader: base.RuntimeEnvReader | None,
    include_telegram_bot_token: bool,
    raw_values: set[str],
) -> tuple[str, int | None, str | None] | None:
    try:
        values = (
            runtime_env_reader(runtime_env_path)
            if runtime_env_reader is not None
            else parse_runtime_env_file(
                runtime_env_path,
                include_telegram_bot_token=include_telegram_bot_token,
            )
        )
        report["runtime_env_read"] = True
    except Exception:
        _set_status(report, STATUS_DB_READ_FAILED, "runtime_env.read")
        return None

    database_url = str(values.get("DATABASE_URL", "")).strip()
    operator_chat_id_text = str(values.get("TELEGRAM_OPERATOR_CHAT_ID", "")).strip()
    telegram_bot_token = None
    if include_telegram_bot_token:
        telegram_bot_token = str(values.get("TELEGRAM_BOT_TOKEN", "")).strip()
        report["telegram_bot_token_read_bucket"] = "one" if telegram_bot_token else "zero"

    raw_values.update(_raw_values(database_url, operator_chat_id_text, telegram_bot_token))
    report["database_configured"] = bool(database_url and base._database_url_is_supported(database_url))
    if not report["database_configured"]:
        _set_status(report, STATUS_DB_READ_FAILED, "database.config")
        return None
    if include_telegram_bot_token and not telegram_bot_token:
        _set_status(report, STATUS_VALIDATION_FAILED, "telegram_bot_token.missing")
        return None
    return database_url, base._operator_chat_id(operator_chat_id_text), telegram_bot_token


def _target_from_row(row: Mapping[str, Any], *, raw_values: set[str]) -> CanaryTarget | None:
    plan_intent_event_id = base._coerce_uuid(row.get("plan_intent_event_id"))
    notification_plan_id = base._coerce_uuid(row.get("notification_plan_id"))
    analysis_id = base._coerce_uuid(row.get("analysis_id"))
    judge_output_id = base._coerce_uuid(row.get("judge_output_id"))
    candidate_group_id = base._coerce_uuid(row.get("candidate_group_id"))
    if (
        plan_intent_event_id is None
        or notification_plan_id is None
        or analysis_id is None
        or judge_output_id is None
        or candidate_group_id is None
    ):
        return None

    target_chat_id = _int_or_none(row.get("target_chat_id"))
    target_thread_id = _int_or_none(row.get("target_thread_id"))
    target = CanaryTarget(
        plan_intent_event_id=plan_intent_event_id,
        notification_plan_id=notification_plan_id,
        analysis_id=analysis_id,
        judge_output_id=judge_output_id,
        candidate_group_id=candidate_group_id,
        delivery_decision=str(row.get("delivery_decision") or ""),
        urgency_profile=str(row.get("urgency_profile") or ""),
        target_chat_id=target_chat_id,
        target_thread_id=target_thread_id,
        render_profile=_string_or_none(row.get("render_profile")),
        dedupe_subject_key=str(row.get("dedupe_subject_key") or ""),
        material_change_hash=str(row.get("material_change_hash") or ""),
        send_after=_datetime_or_none(row.get("send_after")),
        suppress_reason_code=_string_or_none(row.get("suppress_reason_code")),
        plan_status=str(row.get("plan_status") or ""),
        notification_plan_count=base._safe_count(row.get("notification_plan_count")),
        render_count=base._safe_count(row.get("render_count")),
        prior_dry_run_delivery_count=base._safe_count(row.get("prior_dry_run_delivery_count")),
        prior_delivery_result_outbox_count=base._safe_count(row.get("prior_delivery_result_outbox_count")),
        prior_maintenance_noop_marker_count=base._safe_count(row.get("prior_maintenance_noop_marker_count")),
        analysis_count=base._safe_count(row.get("analysis_count")),
        judge_output_count=base._safe_count(row.get("judge_output_count")),
        candidate_count=base._safe_count(row.get("candidate_count")),
        existing_successful_delivery_count=base._safe_count(row.get("existing_successful_delivery_count")),
    )
    raw_values.update(
        _raw_values(
            plan_intent_event_id,
            notification_plan_id,
            analysis_id,
            judge_output_id,
            candidate_group_id,
            target_chat_id,
            target_thread_id,
            target.dedupe_subject_key,
            target.material_change_hash,
            target.send_after,
            target.suppress_reason_code,
        )
    )
    return target


async def _select_clean_target(
    *,
    report: dict[str, Any],
    session: base.AsyncSessionLike,
    operator_chat_id: int | None,
    raw_values: set[str],
) -> CanaryTarget | None:
    rows = base._rows(await base._execute(session, SELECT_CANARY_TARGETS_QUERY))
    if not rows:
        _set_status(report, STATUS_NO_CLEAN_TARGET, "target.notification_plan_missing")
        return None

    clean_targets: list[CanaryTarget] = []
    first_block: tuple[CanaryTarget, str, str] | None = None
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        target = _target_from_row(row, raw_values=raw_values)
        if target is None:
            _set_status(report, STATUS_DB_READ_FAILED, "target.identity")
            return None
        status_check = _target_block_status(target, operator_chat_id=operator_chat_id)
        if status_check is None:
            clean_targets.append(target)
            continue
        if first_block is None:
            status, check = status_check
            first_block = target, status, check

    if len(clean_targets) == 1:
        _merge_target_report(report, clean_targets[0], operator_chat_id=operator_chat_id)
        return clean_targets[0]
    if len(clean_targets) > 1:
        _merge_target_report(report, clean_targets[0], operator_chat_id=operator_chat_id)
        _set_status(report, STATUS_MULTIPLE_CLEAN_TARGETS, "target.clean_target_not_exactly_one")
        return None
    if first_block is not None:
        target, status, check = first_block
        _merge_target_report(report, target, operator_chat_id=operator_chat_id)
        _set_status(report, status, check)
        return None
    _set_status(report, STATUS_NO_CLEAN_TARGET, "target.notification_plan_missing")
    return None


def _target_block_status(
    target: CanaryTarget,
    *,
    operator_chat_id: int | None,
) -> tuple[str, str] | None:
    if target.notification_plan_count != 1:
        return STATUS_VALIDATION_FAILED, "notification_plan.count"
    if target.analysis_count != 1:
        return STATUS_VALIDATION_FAILED, "analysis.missing"
    if target.judge_output_count != 1:
        return STATUS_VALIDATION_FAILED, "judge_output.missing"
    if target.candidate_count != 1:
        return STATUS_VALIDATION_FAILED, "candidate.missing"
    if target.target_chat_id is None:
        return STATUS_VALIDATION_FAILED, "target_chat_id.missing"
    if operator_chat_id is None:
        return STATUS_VALIDATION_FAILED, "runtime_operator_chat_id.missing"
    if target.target_chat_id != operator_chat_id:
        return STATUS_VALIDATION_FAILED, "target_chat_id.mismatch"
    if target.render_count != 1:
        return STATUS_VALIDATION_FAILED, "notification_render.count"
    if target.prior_dry_run_delivery_count < 1:
        return STATUS_VALIDATION_FAILED, "prior_dry_run_delivery.missing"
    if target.prior_delivery_result_outbox_count < 1:
        return STATUS_VALIDATION_FAILED, "prior_delivery_result_outbox.missing"
    if target.prior_maintenance_noop_marker_count < 1:
        return STATUS_VALIDATION_FAILED, "prior_maintenance_noop_marker.missing"
    if target.existing_successful_delivery_count:
        return STATUS_EXISTING_SUCCESSFUL_DELIVERY, "successful_delivery.existing"
    if target.delivery_decision != "send_now":
        return STATUS_VALIDATION_FAILED, "delivery_decision.not_send_now"
    return None


def _merge_target_report(
    report: dict[str, Any],
    target: CanaryTarget,
    *,
    operator_chat_id: int | None,
) -> None:
    report["target_notification_plan_found_bucket"] = base._bucket_count(target.notification_plan_count)
    report["target_notification_render_found_bucket"] = base._bucket_count(target.render_count)
    report["target_prior_dry_run_delivery_found_bucket"] = base._bucket_count(target.prior_dry_run_delivery_count)
    report["target_prior_delivery_result_outbox_found_bucket"] = base._bucket_count(
        target.prior_delivery_result_outbox_count
    )
    report["target_prior_maintenance_noop_marker_found_bucket"] = base._bucket_count(
        target.prior_maintenance_noop_marker_count
    )
    report["target_analysis_found_bucket"] = base._bucket_count(target.analysis_count)
    report["target_judge_output_found_bucket"] = base._bucket_count(target.judge_output_count)
    report["target_candidate_found_bucket"] = base._bucket_count(target.candidate_count)
    report["target_chat_id_available_bucket"] = "one" if target.target_chat_id is not None else "zero"
    report["target_chat_id_matches_runtime_bucket"] = (
        "one" if target.target_chat_id is not None and target.target_chat_id == operator_chat_id else "zero"
    )
    report["existing_successful_delivery_for_target_bucket"] = base._bucket_count(
        target.existing_successful_delivery_count
    )


async def _load_existing_render(
    *,
    session: base.AsyncSessionLike,
    target: CanaryTarget,
    raw_values: set[str],
) -> NotificationRenderDraft | None:
    rows = base._rows(
        await base._execute(
            session,
            SELECT_RENDER_FOR_TARGET_QUERY,
            {"notification_plan_id": str(target.notification_plan_id)},
        )
    )
    if len(rows) != 1 or not isinstance(rows[0], Mapping):
        return None
    row = rows[0]
    raw_values.update(
        _raw_values(
            row.get("notification_plan_id"),
            row.get("message_text"),
            row.get("entities_json"),
            row.get("link_preview_options_json"),
            row.get("reply_markup_json"),
            row.get("render_hash"),
        )
    )
    return NotificationRenderDraft(
        notification_plan_id=target.notification_plan_id,
        message_text=str(row.get("message_text") or ""),
        entities_json=_json_list(row.get("entities_json")),
        link_preview_options_json=_json_dict(row.get("link_preview_options_json")),
        reply_markup_json=_json_dict_or_none(row.get("reply_markup_json")),
        disable_notification=bool(row.get("disable_notification")),
        protect_content=bool(row.get("protect_content")),
        parse_strategy=str(row.get("parse_strategy") or "entities"),
        render_hash=str(row.get("render_hash") or ""),
    )


async def _load_counts(session: base.AsyncSessionLike, target: CanaryTarget) -> TargetCounts:
    params = {
        "notification_plan_id": str(target.notification_plan_id),
        "analysis_id": str(target.analysis_id),
        "judge_output_id": str(target.judge_output_id),
        "candidate_group_id": str(target.candidate_group_id),
    }
    return TargetCounts(
        notification_plans=await base._count_query(session, COUNT_NOTIFICATION_PLAN_ROWS_FOR_TARGET_QUERY, params),
        notification_renders=await base._count_query(session, COUNT_NOTIFICATION_RENDER_ROWS_FOR_TARGET_QUERY, params),
        notification_deliveries=await base._count_query(
            session, COUNT_NOTIFICATION_DELIVERY_ROWS_FOR_TARGET_QUERY, params
        ),
        notifier_transitions=await base._count_query(
            session, COUNT_NOTIFIER_STATE_TRANSITIONS_FOR_TARGET_QUERY, params
        ),
        delivery_result_outbox=await base._count_query(session, COUNT_DELIVERY_RESULT_OUTBOX_FOR_TARGET_QUERY, params),
        analyses=await base._count_query(session, COUNT_ANALYSIS_ROWS_FOR_TARGET_QUERY, params),
        policy_transitions=await base._count_query(session, COUNT_POLICY_STATE_TRANSITIONS_FOR_TARGET_QUERY, params),
        judge_outputs=await base._count_query(session, COUNT_JUDGE_OUTPUT_ROWS_FOR_TARGET_QUERY, params),
        candidates=await base._count_query(session, COUNT_CANDIDATE_ROWS_FOR_TARGET_QUERY, params),
    )


async def _load_snapshots(session: base.AsyncSessionLike, target: CanaryTarget) -> TargetSnapshots:
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


def _merge_written_count_report(report: dict[str, Any], *, before: TargetCounts, after: TargetCounts) -> None:
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
    report["judge_outputs_written_bucket"] = base._bucket_count(after.judge_outputs - before.judge_outputs)


def _merge_snapshot_mutation_report(report: dict[str, Any], *, before: TargetSnapshots, after: TargetSnapshots) -> None:
    report["candidate_current_analysis_mutation_attempted"] = (
        str(after.candidate_current_analysis_id) != str(before.candidate_current_analysis_id)
    )
    report["candidate_bundle_mutation_attempted"] = (
        str(after.candidate_bundle_fingerprint) != str(before.candidate_bundle_fingerprint)
    )


def _notifier_config(*, database_url: str, telegram_bot_token: str) -> NotifierTelegramConfig:
    return NotifierTelegramConfig(
        app_env="prod",
        database_url=database_url,
        redis_url="redis://disabled.invalid/0",
        telegram_bot_token=telegram_bot_token,
        queue_name="q.notification.send",
        consumer_group="notifier-telegram",
        consumer_name=SCRIPT_NAME,
        batch_size=1,
        block_ms=1,
        dry_run=False,
        allow_edits=False,
        enable_notification_send=True,
        enable_digest_runtime=False,
        max_message_chars=3800,
        edit_window_minutes=180,
        telegram_api_base_url="https://api.telegram.org",
        request_timeout_sec=10.0,
        log_level="INFO",
    )


def _intent_from_target(target: CanaryTarget) -> NotificationIntentJob:
    return NotificationIntentJob(
        trigger_event_id=target.plan_intent_event_id,
        event_type=PLAN_INTENT_EVENT_TYPE,
        notification_plan_id=target.notification_plan_id,
        analysis_id=target.analysis_id,
        candidate_group_id=target.candidate_group_id,
        delivery_decision=target.delivery_decision,  # type: ignore[arg-type]
        urgency_profile=target.urgency_profile,  # type: ignore[arg-type]
        target_chat_id=int(target.target_chat_id or 0),
        target_thread_id=target.target_thread_id,
        render_profile=target.render_profile,
        dedupe_subject_key=target.dedupe_subject_key,
        material_change_hash=target.material_change_hash,
        send_after=target.send_after,
        suppress_reason_code=target.suppress_reason_code,
    )


async def _perform_live_send(
    *,
    report: dict[str, Any],
    session: base.AsyncSessionLike,
    target: CanaryTarget,
    database_url: str,
    telegram_bot_token: str,
    telegram_client_factory: TelegramClientFactory | None,
    raw_values: set[str],
) -> bool:
    render = await _load_existing_render(session=session, target=target, raw_values=raw_values)
    if render is None or not render.message_text:
        _set_status(report, STATUS_WRITE_FAILED, "notification_render.load")
        return False

    before_counts = await _load_counts(session, target)
    before_snapshots = await _load_snapshots(session, target)
    repository = NotifierTelegramRepository(session)
    config = _notifier_config(database_url=database_url, telegram_bot_token=telegram_bot_token)
    client = _build_telegram_client(
        config=config,
        telegram_bot_token=telegram_bot_token,
        telegram_client_factory=telegram_client_factory,
    )

    if session.in_transaction():
        await session.rollback()

    report["telegram_send_attempted"] = True
    report["notifier_transport_path_reused"] = True
    result = await _send_existing_render(
        client=client,
        intent=_intent_from_target(target),
        render=render,
        attempt_count=before_counts.notification_deliveries + 1,
    )
    raw_values.update(
        _raw_values(
            result.telegram_chat_id,
            result.telegram_message_id,
            result.transport_error_code,
            result.transport_error_class,
            result.telegram_response_json,
        )
    )
    report["telegram_send_result_bucket"] = result.delivery_status
    if result.delivery_status == "failed_retryable":
        _set_status(report, STATUS_TELEGRAM_RETRYABLE, result.transport_error_code or "telegram.retryable")
        return False
    if result.delivery_status != "sent":
        _set_status(report, STATUS_TELEGRAM_TERMINAL, result.transport_error_code or "telegram.terminal")
        return False
    if result.telegram_message_id is None:
        _set_status(report, STATUS_WRITE_FAILED, "telegram.message_id_missing")
        return False

    report["database_write_attempted"] = True
    async with session.begin():
        await _commit_successful_delivery(
            repository=repository,
            target=target,
            result=result,
        )
        after_counts = await _load_counts(session, target)
        after_snapshots = await _load_snapshots(session, target)
        _merge_written_count_report(report, before=before_counts, after=after_counts)
        _merge_snapshot_mutation_report(report, before=before_snapshots, after=after_snapshots)
        report["notification_delivery_status_bucket"] = result.delivery_status
        report["telegram_message_id_available_bucket"] = "one"
        if not _approved_execution_succeeded(report):
            raise ExpectedEffectsError("restricted send canary effects did not match contract")
    return True


def _build_telegram_client(
    *,
    config: NotifierTelegramConfig,
    telegram_bot_token: str,
    telegram_client_factory: TelegramClientFactory | None,
) -> TelegramClientProtocol:
    if telegram_client_factory is not None:
        return telegram_client_factory(telegram_bot_token)
    return TelegramBotClient(
        base_url=config.telegram_api_base_url,
        bot_token=telegram_bot_token,
        timeout_sec=config.request_timeout_sec,
    )


async def _send_existing_render(
    *,
    client: TelegramClientProtocol,
    intent: NotificationIntentJob,
    render: NotificationRenderDraft,
    attempt_count: int,
) -> DeliveryResult:
    try:
        response = await client.send_message(
            chat_id=intent.target_chat_id,
            text=render.message_text,
            entities=render.entities_json,
            reply_markup=render.reply_markup_json,
            disable_notification=render.disable_notification,
            link_preview_options=render.link_preview_options_json,
            message_thread_id=intent.target_thread_id,
        )
        message = response.get("result") if isinstance(response, dict) else {}
        return DeliveryResult(
            delivery_status="sent",
            telegram_chat_id=_extract_telegram_chat_id(message, intent.target_chat_id),
            telegram_message_id=_extract_telegram_message_id(message),
            attempt_count=attempt_count,
            telegram_response_json=response if isinstance(response, dict) else None,
        )
    except TelegramTransportRetryableError as exc:
        return DeliveryResult(
            delivery_status="failed_retryable",
            telegram_chat_id=intent.target_chat_id,
            telegram_message_id=None,
            attempt_count=attempt_count,
            transport_error_code=getattr(exc, "error_code", "telegram_retryable"),
            transport_error_class=type(exc).__name__,
            retry_after_seconds=getattr(exc, "retry_after_seconds", None),
        )
    except TelegramTransportTerminalError as exc:
        return DeliveryResult(
            delivery_status="failed_terminal",
            telegram_chat_id=intent.target_chat_id,
            telegram_message_id=None,
            attempt_count=attempt_count,
            transport_error_code=getattr(exc, "error_code", "telegram_terminal"),
            transport_error_class=type(exc).__name__,
        )


async def _commit_successful_delivery(
    *,
    repository: NotifierTelegramRepository,
    target: CanaryTarget,
    result: DeliveryResult,
) -> None:
    await repository.update_plan_status(
        notification_plan_id=target.notification_plan_id,
        status=result.delivery_status,
    )
    record_id = await repository.insert_delivery_record(
        notification_plan_id=target.notification_plan_id,
        result_status=result.delivery_status,
        telegram_chat_id=result.telegram_chat_id,
        telegram_message_id=result.telegram_message_id,
        attempt_count=result.attempt_count,
        transport_error_code=result.transport_error_code,
        transport_error_class=result.transport_error_class,
        telegram_response_json=result.telegram_response_json,
    )
    await repository.insert_state_transition(
        object_type="notification_plan",
        object_id=target.notification_plan_id,
        from_state=target.plan_status or None,
        to_state=result.delivery_status,
        reason_code="restricted_operator_canary_send",
    )
    await repository.insert_delivery_result_outbox(
        notification_plan_id=target.notification_plan_id,
        delivery_status=result.delivery_status,
        telegram_chat_id=result.telegram_chat_id,
        telegram_message_id=result.telegram_message_id,
        notification_delivery_record_id=record_id,
        attempt_count=result.attempt_count,
        transport_error_code=result.transport_error_code,
        transport_error_class=result.transport_error_class,
        edited=False,
    )


def _approved_execution_succeeded(report: Mapping[str, Any]) -> bool:
    return bool(
        report["database_write_attempted"] is True
        and report["telegram_bot_token_read_bucket"] == "one"
        and report["telegram_send_attempted"] is True
        and report["telegram_send_result_bucket"] == "sent"
        and report["telegram_edit_attempted"] is False
        and report["notification_plan_rows_written_bucket"] == "zero"
        and report["notification_render_rows_written_bucket"] == "zero"
        and report["notification_delivery_rows_written_bucket"] == "one"
        and report["notifier_state_transitions_written_bucket"] in {"one", "multiple"}
        and report["notification_delivery_result_outbox_written_bucket"] == "one"
        and report["notification_delivery_status_bucket"] == "sent"
        and report["telegram_message_id_available_bucket"] == "one"
        and report["analysis_rows_written_bucket"] == "zero"
        and report["judge_outputs_written_bucket"] == "zero"
        and report["policy_state_transitions_written_bucket"] == "zero"
        and report["candidate_bundle_mutation_attempted"] is False
        and report["candidate_current_analysis_mutation_attempted"] is False
        and report["redis_write_attempted"] is False
        and report["openai_call_attempted"] is False
        and report["openai_key_read_bucket"] == "zero"
    )


async def generate_report_async(
    *,
    approve_db_read: bool = False,
    approve_db_write: bool = False,
    approve_telegram_send: bool = False,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    runtime_env_reader: base.RuntimeEnvReader | None = None,
    database_session_factory: base.DatabaseSessionFactory | None = None,
    telegram_client_factory: TelegramClientFactory | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> base.ScriptResult:
    report = _base_report(
        approve_db_read=approve_db_read,
        approve_db_write=approve_db_write,
        approve_telegram_send=approve_telegram_send,
    )
    raw_values = _raw_values(*forbidden_raw_values)
    raw_values.update(_raw_values(runtime_env_path))

    if not approve_db_read and not approve_db_write and not approve_telegram_send:
        _set_status(report, STATUS_DEFAULT_PASSED)
        return _finalize(report, raw_values, exit_code=0)

    db_preflight_mode = approve_db_read and not approve_db_write and not approve_telegram_send
    live_send_mode = approve_db_read and approve_db_write and approve_telegram_send
    if not db_preflight_mode and not live_send_mode:
        _set_status(report, STATUS_NOT_APPROVED, "approval.required_mode")
        return _finalize(report, raw_values, exit_code=1)

    session: base.AsyncSessionLike | None = None
    committed = False
    try:
        runtime_config = await _read_runtime_config(
            report=report,
            runtime_env_path=runtime_env_path,
            runtime_env_reader=runtime_env_reader,
            include_telegram_bot_token=live_send_mode,
            raw_values=raw_values,
        )
        if runtime_config is None:
            return _finalize(report, raw_values, exit_code=1)
        database_url, operator_chat_id, telegram_bot_token = runtime_config

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

        target = await _select_clean_target(
            report=report,
            session=session,
            operator_chat_id=operator_chat_id,
            raw_values=raw_values,
        )
        if target is None:
            return _finalize(report, raw_values, exit_code=1)

        if db_preflight_mode:
            _set_status(report, STATUS_DB_PREFLIGHT_PASSED)
            return _finalize(report, raw_values, exit_code=0)

        if telegram_bot_token is None:
            _set_status(report, STATUS_VALIDATION_FAILED, "telegram_bot_token.missing")
            return _finalize(report, raw_values, exit_code=1)

        try:
            sent_and_written = await _perform_live_send(
                report=report,
                session=session,
                target=target,
                database_url=database_url,
                telegram_bot_token=telegram_bot_token,
                telegram_client_factory=telegram_client_factory,
                raw_values=raw_values,
            )
            committed = sent_and_written
        except ExpectedEffectsError:
            _set_status(report, STATUS_WRITE_FAILED, "db_write.expected_effects")
            return _finalize(report, raw_values, exit_code=1)
        except Exception:
            _set_status(report, STATUS_WRITE_FAILED, "db_write")
            return _finalize(report, raw_values, exit_code=1)

        if not sent_and_written:
            return _finalize(report, raw_values, exit_code=1)

        _set_status(report, STATUS_APPROVED_SEND_PASSED)
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
    if _report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "report.raw_value_emission")
        exit_code = 1
    return base.ScriptResult(exit_code=exit_code, report=report)


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


def _int_or_none(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _datetime_or_none(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _json_list(value: Any) -> list[dict[str, Any]]:
    loaded = _json_loads(value)
    if not isinstance(loaded, list):
        return []
    return [dict(item) for item in loaded if isinstance(item, Mapping)]


def _json_dict(value: Any) -> dict[str, Any]:
    loaded = _json_loads(value)
    return dict(loaded) if isinstance(loaded, Mapping) else {}


def _json_dict_or_none(value: Any) -> dict[str, Any] | None:
    loaded = _json_loads(value)
    return dict(loaded) if isinstance(loaded, Mapping) else None


def _extract_telegram_chat_id(message: object, fallback: int) -> int:
    if isinstance(message, Mapping):
        chat = message.get("chat")
        if isinstance(chat, Mapping) and chat.get("id") is not None:
            return int(chat["id"])
    return fallback


def _extract_telegram_message_id(message: object) -> int | None:
    if isinstance(message, Mapping) and message.get("message_id") is not None:
        return int(message["message_id"])
    return None


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
        approve_telegram_send=args.approve_telegram_send,
        runtime_env_path=args.runtime_env_path,
    )
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
