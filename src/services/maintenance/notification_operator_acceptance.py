from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any


SCHEMA_VERSION = "notification_operator_acceptance_readback_consolidation_v1"
REASON_PASSED = "operator_notification_acceptance_packet_closed"

UX_PREVIEW_SCHEMA_VERSION = "notification_ux_render_preview_v1"
SEND_DISABLED_SCHEMA_VERSION = "notifier_send_disabled_worker_once_proof_v1"
QUEUED_WORKER_SCHEMA_VERSION = "notifier_restricted_live_queued_worker_once_v1"
QUEUE_CHAIN_SCHEMA_VERSION = "restricted_live_notification_queue_chain_proof_runner_v1"
DELIVERY_DRAIN_SCHEMA_VERSION = "restricted_delivery_result_maintenance_drain_proof_v1"

OPEN_GATES = (
    "AUTHORITY_OPEN",
    "ROLLOUT_OPEN",
    "PRODUCTION_ROLLOUT_OPEN",
    "FUNCTION_COMPLETE_OPEN",
)
CLOSED_CAPABILITIES = (
    "UX_ACCEPTANCE_CLOSED_REAFFIRMED",
    "OPERATOR_NOTIFICATION_ACCEPTANCE_PACKET_CLOSED",
)
TOP_LEVEL_AUTHORITY_CLOSED = {
    "live_telegram_transport_attempted": False,
    "live_openai_called": False,
    "live_github_called": False,
    "live_x_called": False,
    "live_web_called": False,
    "docker_or_systemd_called": False,
    "alembic_or_ddl_ran": False,
    "runtime_values_printed": False,
    "raw_payload_printed": False,
    "raw_ids_printed": False,
}
COMPLETION_CLAIMS = {
    "PRODUCT_COMPLETE_CLOSED": False,
    "PRODUCTION_ROLLOUT_CLOSED": False,
    "final_bot_complete": False,
    "one_hundred_percent_complete": False,
    "production_rollout_complete": False,
}

_DB_REDIS_URL_RE = re.compile(
    r"\b(?:postgres(?:ql)?(?:\+[A-Za-z0-9_]+)?|redis(?:\+[A-Za-z0-9_]+)?)://[^\s<>)\"']+",
    flags=re.IGNORECASE,
)
_HTTP_URL_RE = re.compile(r"https?://[^\s<>)\"']+", flags=re.IGNORECASE)
_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_UNSAFE_LITERAL_MARKERS = (
    "DATABASE_URL",
    "REDIS_URL",
    "OPENAI_API_KEY",
    "TELEGRAM_BOT_TOKEN",
    "Traceback",
    "payload_json",
    "telegram_response_json",
    "runtime.env",
    "private stderr",
)
_UNSAFE_CASEFOLD_MARKERS = (
    "postgresql+",
    "password",
    "credential",
    "api_key",
)


def build_notification_operator_acceptance_readback(
    *,
    notification_ux_render_preview: Mapping[str, Any],
    restricted_send_disabled: Mapping[str, Any],
    restricted_queued_worker: Mapping[str, Any],
    restricted_queue_chain: Mapping[str, Any],
    delivery_result_drain: Mapping[str, Any],
    zero_preserving_readback: Mapping[str, Any] | None = None,
    input_file_read_attempted: bool = False,
) -> dict[str, Any]:
    surfaces = {
        "notification_ux_render_preview": _notification_ux_surface(notification_ux_render_preview),
        "restricted_send_disabled": _send_disabled_surface(restricted_send_disabled),
        "restricted_queued_worker": _queued_worker_surface(restricted_queued_worker),
        "restricted_queue_chain": _queue_chain_surface(restricted_queue_chain),
        "delivery_result_drain": _delivery_drain_surface(delivery_result_drain),
        "zero_preserving_readback": _zero_readback_surface(zero_preserving_readback or {}),
    }
    failed = [
        f"{surface_name}.{check_name}"
        for surface_name, surface in surfaces.items()
        for check_name, passed in _surface_checks(surface).items()
        if passed is not True
    ]
    packet = _base_packet(
        status="pass" if not failed else "fail",
        reason_code=REASON_PASSED if not failed else "operator_notification_acceptance_checks_failed",
        input_file_read_attempted=input_file_read_attempted,
    )
    packet["surfaces"] = surfaces
    packet["checks_failed"] = failed
    packet["redaction_audit"] = {
        "raw_source_text_omitted": True,
        "raw_urls_omitted": True,
        "full_ids_omitted": True,
        "runtime_values_omitted": True,
        "runtime_locators_omitted": True,
        "private_stderr_omitted": True,
        "raw_transport_body_omitted": True,
    }

    if not _packet_is_sanitized(packet):
        sanitized = _base_packet(
            status="fail",
            reason_code="operator_notification_acceptance_packet_not_sanitized",
            input_file_read_attempted=input_file_read_attempted,
        )
        sanitized["surfaces"] = {}
        sanitized["checks_failed"] = ["packet_sanitized_output"]
        sanitized["redaction_audit"] = {
            "raw_source_text_omitted": False,
            "raw_urls_omitted": False,
            "full_ids_omitted": False,
            "runtime_values_omitted": False,
            "runtime_locators_omitted": False,
            "private_stderr_omitted": False,
            "raw_transport_body_omitted": False,
        }
        return sanitized
    return packet


def blocked_notification_operator_acceptance_readback(
    reason_code: str,
    *,
    input_file_read_attempted: bool = False,
) -> dict[str, Any]:
    packet = _base_packet(
        status="blocked",
        reason_code=_safe_reason_code(reason_code),
        input_file_read_attempted=input_file_read_attempted,
    )
    packet["surfaces"] = {}
    packet["checks_failed"] = [_safe_reason_code(reason_code)]
    packet["redaction_audit"] = {
        "raw_source_text_omitted": True,
        "raw_urls_omitted": True,
        "full_ids_omitted": True,
        "runtime_values_omitted": True,
        "runtime_locators_omitted": True,
        "private_stderr_omitted": True,
        "raw_transport_body_omitted": True,
    }
    return packet


def render_sanitized_json(packet: Mapping[str, Any]) -> str:
    return json.dumps(packet, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def _base_packet(
    *,
    status: str,
    reason_code: str,
    input_file_read_attempted: bool,
) -> dict[str, Any]:
    runtime_authority = dict(TOP_LEVEL_AUTHORITY_CLOSED)
    runtime_authority.update(
        {
            "database_write_attempted": False,
            "redis_mutation_attempted": False,
            "notifier_transport_attempted": False,
            "broad_worker_started": False,
        }
    )
    authority = dict(TOP_LEVEL_AUTHORITY_CLOSED)
    authority.update(
        {
            "input_file_read_attempted": bool(input_file_read_attempted),
            "existing_surface_reports_consumed": bool(input_file_read_attempted),
            "acceptance_builder_executed": True,
            "database_read_attempted": False,
            "database_write_attempted": False,
            "redis_read_attempted": False,
            "redis_mutation_attempted": False,
            "notifier_transport_attempted": False,
            "broad_worker_started": False,
        }
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "reason_code": reason_code,
        "closed_capabilities": list(CLOSED_CAPABILITIES) if status == "pass" else [],
        "open_gates": list(OPEN_GATES),
        "report_semantics": {
            "open_gates": "global_lifecycle_state",
            "runtime_authority_opened_in_this_run": "per_invocation_approved_authority",
            "authority": "actual_attempted_operations",
        },
        "runtime_authority_opened_in_this_run": runtime_authority,
        "authority": authority,
        "completion_claims": dict(COMPLETION_CLAIMS),
    }


def _notification_ux_surface(report: Mapping[str, Any]) -> dict[str, Any]:
    checks = _mapping(report.get("checks"))
    summary = _mapping(report.get("render_summary"))
    delivery_quality = _mapping(report.get("delivery_quality_summary"))
    return {
        "status": _string(report.get("status")),
        "reason_code": _string(report.get("reason_code")),
        "schema_valid": report.get("schema_version") == UX_PREVIEW_SCHEMA_VERSION,
        "checks_failed_count": len(_list(report.get("checks_failed"))),
        "verdict_first_section": checks.get("verdict_visible_in_first_three_lines") is True,
        "source_type_first_section": checks.get("source_type_visible") is True,
        "severity_first_section": checks.get("severity_visible") is True,
        "urgency_first_section": checks.get("urgency_visible_in_first_three_lines") is True,
        "confidence_visible_or_not_applicable": checks.get("confidence_visible_or_not_applicable") is True,
        "skeptical_or_risk_visible": checks.get("skeptical_or_risk_marker_present") is True,
        "risk_visible": checks.get("risk_marker_present") is True,
        "recommended_action_visible": checks.get("recommended_action_marker_present") is True,
        "evidence_limitations_visible": checks.get("evidence_limitations_marker_present") is True,
        "primary_link_surface_visible": checks.get("url_button_present") is True,
        "link_buttons_present": checks.get("link_buttons_present") is True,
        "github_primary_expectations_preserved": checks.get("github_primary_button_label") is True,
        "later_or_low_urgency_not_misleading": checks.get("silent_later_or_normal_profile") is True,
        "high_urgency_not_silent": checks.get("high_profile_not_silent") is True,
        "message_under_limit": (
            checks.get("message_under_telegram_limit") is True
            and checks.get("message_under_configured_limit") is True
        ),
        "link_preview_disabled": checks.get("link_preview_disabled") is True,
        "protect_content_false": checks.get("protect_content_false") is True,
        "raw_leak_checks_passed": all(
            checks.get(name) is True
            for name in (
                "primary_url_not_in_message_text_when_button_exists",
                "source_url_not_in_message_text_when_button_exists",
                "no_url_in_message_text",
                "no_db_or_redis_url_in_message_text",
                "no_uuid_in_message_text",
                "no_sensitive_markers_in_message_text",
                "no_sensitive_marker_or_error_body_in_message_text",
                "no_source_or_raw_json_in_message_text",
            )
        ),
        "message_char_count": _int(summary.get("message_char_count")),
        "configured_message_char_limit": _int(summary.get("configured_message_char_limit")),
        "button_count": _int(summary.get("button_count")),
        "button_labels": [_safe_label(value) for value in _list(summary.get("button_labels"))],
        "disable_notification": _bool_or_none(summary.get("disable_notification")),
        "delivery_quality_summary": {
            "operator_actionability": _string(delivery_quality.get("operator_actionability")),
            "missing_sections": [_safe_label(value) for value in _list(delivery_quality.get("missing_sections"))],
            "visible_first_lines": [_safe_label(value) for value in _list(delivery_quality.get("visible_first_lines"))],
            "button_count": _int(delivery_quality.get("button_count")),
            "message_char_count": _int(delivery_quality.get("message_char_count")),
            "notifier_reinterpreted_policy": _bool_or_none(delivery_quality.get("notifier_reinterpreted_policy")),
        },
    }


def _send_disabled_surface(report: Mapping[str, Any]) -> dict[str, Any]:
    db = _mapping(report.get("db_verification"))
    worker = _mapping(report.get("worker_once"))
    authority = _mapping(report.get("authority"))
    return {
        "status": _string(report.get("status")),
        "reason_code": _string(report.get("reason_code")),
        "schema_valid": report.get("schema_version") == SEND_DISABLED_SCHEMA_VERSION,
        "transport_reason_code": _string(db.get("transport_error_code") or report.get("transport_reason_code")),
        "telegram_transport_attempted": authority.get("telegram_send_or_edit_called") is True,
        "telegram_transport_possible": _bool_or_none(authority.get("telegram_transport_possible")),
        "render_count": _int(db.get("notification_render_count") or report.get("render_count")),
        "delivery_record_count": _int(
            db.get("notification_delivery_record_count") or report.get("delivery_record_count")
        ),
        "delivery_outbox_count": _int(
            report.get("delivery_outbox_count")
            or (1 if db.get("delivery_result_outbox_exists") is True else 0)
        ),
        "worker_acked": worker.get("acked") is True or report.get("worker_acked") is True,
        "checks_failed_count": len(_list(report.get("checks_failed"))),
    }


def _queued_worker_surface(report: Mapping[str, Any]) -> dict[str, Any]:
    redis = _mapping(report.get("redis_precheck"))
    authority = _mapping(report.get("authority"))
    return {
        "status": _string(report.get("status")),
        "reason_code": _string(report.get("reason_code")),
        "schema_valid": report.get("schema_version") == QUEUED_WORKER_SCHEMA_VERSION,
        "redis_pending": _int(redis.get("pending")),
        "redis_lag": _int(redis.get("lag")),
        "worker_invoked": "worker_once" in report,
        "database_session_opened": authority.get("database_session_opened") is True,
        "telegram_transport_attempted": authority.get("telegram_transport_attempted") is True,
    }


def _queue_chain_surface(report: Mapping[str, Any]) -> dict[str, Any]:
    target = _mapping(report.get("target"))
    publisher = _mapping(report.get("publisher"))
    authority = _mapping(report.get("authority"))
    return {
        "status": _string(report.get("status")),
        "reason_code": _string(report.get("reason_code")),
        "schema_valid": report.get("schema_version") == QUEUE_CHAIN_SCHEMA_VERSION,
        "target_status_after_publish": _string(target.get("outbox_status_after_publish")),
        "redis_xadd_count": _int(publisher.get("redis_xadd_count")),
        "redis_consume_attempted": authority.get("redis_consume_attempted") is True,
        "telegram_transport_attempted": authority.get("telegram_transport_attempted") is True,
        "raw_payload_printed": authority.get("raw_payload_printed") is True,
        "raw_ids_printed": authority.get("raw_ids_printed") is True,
    }


def _delivery_drain_surface(report: Mapping[str, Any]) -> dict[str, Any]:
    publisher = _mapping(report.get("publisher"))
    worker = _mapping(report.get("worker_once"))
    readback = _mapping(report.get("readback"))
    authority = _mapping(report.get("authority"))
    return {
        "status": _string(report.get("status")),
        "reason_code": _string(report.get("reason_code")),
        "schema_valid": report.get("schema_version") == DELIVERY_DRAIN_SCHEMA_VERSION,
        "publisher_redis_xadd_count": _int(publisher.get("redis_xadd_count")),
        "worker_acked": worker.get("acked") is True,
        "readback_redis_pending": _int(readback.get("redis_pending")),
        "readback_redis_lag": _int(readback.get("redis_lag")),
        "maintenance_receipt_present": readback.get("maintenance_receipt_present") is True,
        "q_notification_send_consumed": authority.get("q_notification_send_consumed") is True,
        "telegram_transport_attempted": authority.get("telegram_transport_attempted") is True,
        "raw_payload_printed": authority.get("raw_payload_printed") is True,
        "raw_ids_printed": authority.get("raw_ids_printed") is True,
    }


def _zero_readback_surface(report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "string_key_lag": _int(report.get("string_key_lag")),
        "string_key_pending": _int(report.get("string_key_pending")),
        "bytes_key_lag": _int(report.get("bytes_key_lag")),
        "bytes_key_pending": _int(report.get("bytes_key_pending")),
    }


def _surface_checks(surface: Mapping[str, Any]) -> dict[str, bool]:
    if "verdict_first_section" in surface:
        return {
            "schema_valid": surface.get("schema_valid") is True,
            "status_pass": surface.get("status") == "pass",
            "checks_failed_zero": surface.get("checks_failed_count") == 0,
            "verdict_first_section": surface.get("verdict_first_section") is True,
            "source_type_first_section": surface.get("source_type_first_section") is True,
            "severity_first_section": surface.get("severity_first_section") is True,
            "urgency_first_section": surface.get("urgency_first_section") is True,
            "confidence_visible_or_not_applicable": surface.get("confidence_visible_or_not_applicable") is True,
            "skeptical_or_risk_visible": surface.get("skeptical_or_risk_visible") is True,
            "risk_visible": surface.get("risk_visible") is True,
            "recommended_action_visible": surface.get("recommended_action_visible") is True,
            "evidence_limitations_visible": surface.get("evidence_limitations_visible") is True,
            "primary_link_surface_visible": surface.get("primary_link_surface_visible") is True,
            "link_buttons_present": surface.get("link_buttons_present") is True,
            "github_primary_expectations_preserved": surface.get("github_primary_expectations_preserved") is True,
            "later_or_low_urgency_not_misleading": surface.get("later_or_low_urgency_not_misleading") is True,
            "high_urgency_not_silent": surface.get("high_urgency_not_silent") is True,
            "message_under_limit": surface.get("message_under_limit") is True,
            "link_preview_disabled": surface.get("link_preview_disabled") is True,
            "protect_content_false": surface.get("protect_content_false") is True,
            "raw_leak_checks_passed": surface.get("raw_leak_checks_passed") is True,
            "delivery_quality_actionable": _mapping(surface.get("delivery_quality_summary")).get(
                "operator_actionability"
            )
            == "pass",
            "delivery_quality_no_missing_sections": _mapping(surface.get("delivery_quality_summary")).get(
                "missing_sections"
            )
            == [],
            "notifier_did_not_reinterpret_policy": _mapping(surface.get("delivery_quality_summary")).get(
                "notifier_reinterpreted_policy"
            )
            is False,
        }
    if "transport_reason_code" in surface:
        return {
            "schema_valid": surface.get("schema_valid") is True,
            "status_pass": surface.get("status") == "pass",
            "reason_pass": surface.get("reason_code") == "send_disabled_worker_once_proof_passed",
            "send_disabled_reason": surface.get("transport_reason_code") == "notification_send_flag_disabled",
            "transport_not_attempted": surface.get("telegram_transport_attempted") is False,
            "transport_not_possible": surface.get("telegram_transport_possible") is False,
            "render_created": surface.get("render_count") == 1,
            "delivery_record_created": surface.get("delivery_record_count") == 1,
            "delivery_outbox_created": surface.get("delivery_outbox_count") == 1,
            "worker_acked": surface.get("worker_acked") is True,
            "checks_failed_zero": surface.get("checks_failed_count") == 0,
        }
    if "redis_pending" in surface:
        return {
            "schema_valid": surface.get("schema_valid") is True,
            "noop_status": surface.get("status") == "noop",
            "no_queued_message": surface.get("reason_code") == "no_queued_message",
            "redis_pending_zero": surface.get("redis_pending") == 0,
            "redis_lag_zero": surface.get("redis_lag") == 0,
            "worker_not_invoked": surface.get("worker_invoked") is False,
            "database_not_opened": surface.get("database_session_opened") is False,
            "transport_not_attempted": surface.get("telegram_transport_attempted") is False,
        }
    if "target_status_after_publish" in surface:
        return {
            "schema_valid": surface.get("schema_valid") is True,
            "status_pass": surface.get("status") == "pass",
            "target_published": surface.get("target_status_after_publish") == "published",
            "redis_xadd_once": surface.get("redis_xadd_count") == 1,
            "redis_consume_not_attempted": surface.get("redis_consume_attempted") is False,
            "transport_not_attempted": surface.get("telegram_transport_attempted") is False,
            "raw_payload_not_printed": surface.get("raw_payload_printed") is False,
            "raw_ids_not_printed": surface.get("raw_ids_printed") is False,
        }
    if "publisher_redis_xadd_count" in surface:
        return {
            "schema_valid": surface.get("schema_valid") is True,
            "status_pass": surface.get("status") == "pass",
            "redis_xadd_once": surface.get("publisher_redis_xadd_count") == 1,
            "worker_acked": surface.get("worker_acked") is True,
            "readback_pending_zero": surface.get("readback_redis_pending") == 0,
            "readback_lag_zero": surface.get("readback_redis_lag") == 0,
            "receipt_present": surface.get("maintenance_receipt_present") is True,
            "q_notification_send_not_consumed": surface.get("q_notification_send_consumed") is False,
            "transport_not_attempted": surface.get("telegram_transport_attempted") is False,
            "raw_payload_not_printed": surface.get("raw_payload_printed") is False,
            "raw_ids_not_printed": surface.get("raw_ids_printed") is False,
        }
    return {
        "string_key_lag_zero": surface.get("string_key_lag") == 0,
        "string_key_pending_zero": surface.get("string_key_pending") == 0,
        "bytes_key_lag_zero": surface.get("bytes_key_lag") == 0,
        "bytes_key_pending_zero": surface.get("bytes_key_pending") == 0,
    }


def _packet_is_sanitized(packet: Mapping[str, Any]) -> bool:
    rendered = json.dumps(packet, ensure_ascii=False, sort_keys=True, default=str)
    rendered_casefold = rendered.casefold()
    if _DB_REDIS_URL_RE.search(rendered) or _HTTP_URL_RE.search(rendered) or _UUID_RE.search(rendered):
        return False
    if any(marker in rendered for marker in _UNSAFE_LITERAL_MARKERS):
        return False
    return not any(marker in rendered_casefold for marker in _UNSAFE_CASEFOLD_MARKERS)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _string(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _safe_label(value: Any) -> str:
    text = str(value)
    if len(text) > 80:
        text = text[:80]
    return _HTTP_URL_RE.sub("[link]", _UUID_RE.sub("[id]", text))


def _safe_reason_code(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_:-]", "_", str(value))[:120]
    return safe or "blocked"
