from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from .noise_duplicate_suppression import F9_GATE, SCHEMA_VERSION as F9_SCHEMA_VERSION, build_noise_duplicate_suppression_proof


SCHEMA_VERSION = "function_complete_packet_v1"
RUNNER_NAME = "function_complete_packet_consumer"
PACKET_READY_STATUS = "FUNCTION_COMPLETE_PACKET_READY"
PACKET_BLOCKED_STATUS = "FUNCTION_COMPLETE_PACKET_BLOCKED"

REQUIRED_CODE_GATES: tuple[str, ...] = (
    "F1_ONE_CHANNEL_REGISTRY_SOURCE_LAST_PROOF",
    "F2_THREE_CHANNEL_REGISTRY_SOURCE_LAST_PROOF",
    "F3_FULL_REGISTRY_SOURCE_LAST_PROOF",
    "F4_GITHUB_PROVIDER_TO_EVIDENCEBUNDLE_PROOF",
    "F5_X_PROVIDER_TO_EVIDENCEBUNDLE_PROOF",
    "F6_WEB_TEXT_IDEA_PROVIDER_TO_EVIDENCEBUNDLE_PROOF",
    "F7_FEEDBACK_EVAL_LOOP",
    "F8_CHANNEL_OVERRIDE_POLICY",
    F9_GATE,
)

WRAPPER_COMPLETION_CLAIMS: tuple[str, ...] = (
    "F1_SOURCE_TRUTH_DURABLE_READBACK_REVIEWABLE",
    "F1_DUPLICATE_NOOP_READBACK_REVIEWABLE",
    "F1_EXACT_LIVE_READBACK_REVIEWABLE",
    "F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY",
    "F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY",
    "LIVE_COLLECTOR_1_CHANNEL_CLOSED",
    "LIVE_COLLECTOR_3_CHANNEL_CLOSED",
    "PRODUCT_COMPLETE_CLOSED",
    "PRODUCTION_ROLLOUT_CLOSED",
)

_SOURCE_TRUTH_CLOSURE_KEYS: tuple[str, ...] = (
    "child_report_available",
    "wrapper_child_execution_passed",
    "exact_child_runner_passed",
    "live_telegram_read_attempted",
    "telegram_read_called",
    "messages_seen_present",
    "source_current_readback_present",
    "source_version_readback_present",
    "source_created_events_readback_present",
    "source_outbox_events_readback_present",
    "source_outbox_publish_disabled",
    "redis_publish_disabled",
    "telegram_send_disabled",
    "provider_calls_disabled",
    "docker_systemd_alembic_disabled",
    "raw_values_not_printed",
    "runtime_values_not_printed",
    "durable_readback_present",
)

_F1_DUPLICATE_NOOP_CLOSURE_KEYS: tuple[str, ...] = (
    "one_channel_or_legacy_child_report",
    "source_truth_durable_readback_present",
    "duplicate_noop_proof_present",
    "duplicate_noop_without_second_telegram_read",
    "closed",
)

_F1_FRESH_WRITE_CLOSURE_KEYS: tuple[str, ...] = (
    "one_channel_or_legacy_child_report",
    "source_truth_durable_readback_present",
    "database_write_attempted",
    "source_message_write_attempted",
    "source_version_write_attempted",
    "source_outbox_write_attempted",
    "closed",
)

_F1_EXACT_LIVE_CLOSURE_KEYS: tuple[str, ...] = (
    "duplicate_noop_readback_closed",
    "fresh_write_readback_closed",
    "closed",
)

_F2_THREE_CHANNEL_CLOSURE_KEYS: tuple[str, ...] = (
    "child_report_available",
    "wrapper_child_execution_passed",
    "exact_child_runner_passed",
    "target_count_is_three",
    "target_fingerprint_count_is_three",
    "per_channel_result_count_is_three",
    "per_channel_status_passed",
    "per_channel_messages_seen_present",
    "per_channel_readbacks_present",
    "aggregate_source_current_readback_present",
    "aggregate_source_version_readback_present",
    "aggregate_source_created_events_readback_present",
    "aggregate_source_outbox_events_readback_present",
    "aggregate_duplicate_noop_or_fresh_write_sufficient",
    "source_outbox_publish_disabled",
    "redis_publish_disabled",
    "telegram_send_disabled",
    "provider_calls_disabled",
    "docker_systemd_alembic_disabled",
    "raw_values_not_printed",
    "runtime_values_not_printed",
    "closed",
)


def build_function_complete_packet(
    *,
    f9_proof: Mapping[str, Any] | None = None,
    code_gate_evidence: Mapping[str, Any] | None = None,
    origin_evidence: Mapping[str, Any] | None = None,
    vps_evidence: Mapping[str, Any] | None = None,
    collector_wrapper_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    proof = dict(f9_proof or build_noise_duplicate_suppression_proof())
    evidence = _default_code_gate_evidence(proof) if code_gate_evidence is None else dict(code_gate_evidence)
    gate_status = _code_gate_status(evidence=evidence, f9_proof=proof)
    wrapper_summary = _collector_wrapper_evidence_summary(collector_wrapper_evidence)
    wrapper_claims = wrapper_summary["completion_claims"]
    ready = gate_status["all_required_code_gates_closed"] is True and _f9_proof_passed(proof)
    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "ok": ready,
        "status": "pass" if ready else "blocked",
        "packet_status": PACKET_READY_STATUS if ready else PACKET_BLOCKED_STATUS,
        "reason_code": "function_complete_packet_ready" if ready else "function_complete_packet_inputs_incomplete",
        "CODE_CLOSED": gate_status,
        "F9": {
            "consumed": True,
            "schema_version": _safe_string(proof.get("schema_version")),
            "status": _safe_string(proof.get("status")),
            "gate_closed": bool(proof.get("gate_closed") is True),
            "proof_fingerprint": _fingerprint(proof),
        },
        "ORIGIN": _external_evidence_summary(origin_evidence),
        "VPS": _external_evidence_summary(vps_evidence),
        "collector_wrapper_readback": wrapper_summary,
        "AUTHORITY_OPEN": {
            "open": True,
            "reason_code": "live_authority_not_opened_by_code_packet",
        },
        "ROLLOUT_OPEN": {
            "open": True,
            "reason_code": "rollout_validation_not_claimed_by_code_packet",
        },
        "PRODUCTION_ROLLOUT_OPEN": {
            "open": True,
            "reason_code": "production_rollout_not_claimed_by_code_packet",
        },
        "completion_claims": {
            "function_complete_packet_ready": ready,
            "function_complete_ready": False,
            "final_bot_complete": False,
            "production_complete": False,
            "one_hundred_percent_complete": False,
            "F1_SOURCE_TRUTH_DURABLE_READBACK_REVIEWABLE": wrapper_claims[
                "F1_SOURCE_TRUTH_DURABLE_READBACK_REVIEWABLE"
            ],
            "F1_DUPLICATE_NOOP_READBACK_REVIEWABLE": wrapper_claims[
                "F1_DUPLICATE_NOOP_READBACK_REVIEWABLE"
            ],
            "F1_EXACT_LIVE_READBACK_REVIEWABLE": wrapper_claims["F1_EXACT_LIVE_READBACK_REVIEWABLE"],
            "F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY": wrapper_claims[
                "F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY"
            ],
            "F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY": wrapper_claims[
                "F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY"
            ],
            "LIVE_COLLECTOR_1_CHANNEL_CLOSED": False,
            "LIVE_COLLECTOR_3_CHANNEL_CLOSED": False,
            "PRODUCT_COMPLETE_CLOSED": False,
            "PRODUCTION_ROLLOUT_CLOSED": False,
        },
        "open_gates": [
            "AUTHORITY_OPEN",
            "ROLLOUT_OPEN",
            "PRODUCTION_ROLLOUT_OPEN",
            "live authority/recovery/production rollout remain outside this packet",
        ],
        "redactions_applied": {
            "full_ids_omitted": True,
            "raw_urls_omitted": True,
            "raw_source_text_omitted": True,
            "raw_feedback_notes_omitted": True,
            "raw_chat_ids_omitted": True,
            "dedupe_keys_omitted": True,
            "material_hashes_omitted": True,
            "db_redis_urls_omitted": True,
            "env_values_omitted": True,
            "exception_bodies_omitted": True,
            "tracebacks_omitted": True,
        },
        "raw_values_printed": False,
    }


def _collector_wrapper_evidence_summary(evidence: Mapping[str, Any] | None) -> dict[str, Any]:
    if evidence is None:
        claims = {claim: False for claim in WRAPPER_COMPLETION_CLAIMS}
        return {
            "supplied": False,
            "consumed": False,
            "status": "not_supplied",
            "schema_version": None,
            "reason_code": None,
            "evidence_fingerprint": None,
            "target_scope": {
                "target_count": None,
                "target_fingerprints": [],
                "raw_source_value_printed": False,
            },
            "actual_attempted_operations": {
                "child_runner_invoked": False,
                "child_runner_returncode_zero": False,
                "live_telegram_read_attempted_by_wrapper": False,
                "telegram_send_or_edit_attempted": False,
                "openai_attempted": False,
                "github_attempted": False,
                "x_attempted": False,
                "web_attempted": False,
                "redis_publish_attempted_by_wrapper": False,
                "docker_or_systemd_called": False,
                "alembic_called": False,
            },
            "source_truth_readback_closure": _empty_bool_section(_SOURCE_TRUTH_CLOSURE_KEYS),
            "f1_duplicate_noop_readback_closure": _empty_bool_section(_F1_DUPLICATE_NOOP_CLOSURE_KEYS),
            "f1_fresh_write_readback_closure": _empty_bool_section(_F1_FRESH_WRITE_CLOSURE_KEYS),
            "f1_exact_live_readback_review_closure": _empty_bool_section(_F1_EXACT_LIVE_CLOSURE_KEYS),
            "f2_three_channel_readback_closure": _empty_bool_section(_F2_THREE_CHANNEL_CLOSURE_KEYS),
            "operator_closure": _operator_closure_from_claims(claims),
            "completion_claims": claims,
        }

    source_truth = _project_bool_section(evidence.get("source_truth_readback_closure"), _SOURCE_TRUTH_CLOSURE_KEYS)
    duplicate_noop = _project_bool_section(
        evidence.get("f1_duplicate_noop_readback_closure"),
        _F1_DUPLICATE_NOOP_CLOSURE_KEYS,
    )
    fresh_write = _project_bool_section(
        evidence.get("f1_fresh_write_readback_closure"),
        _F1_FRESH_WRITE_CLOSURE_KEYS,
    )
    exact_live = _project_bool_section(
        evidence.get("f1_exact_live_readback_review_closure"),
        _F1_EXACT_LIVE_CLOSURE_KEYS,
    )
    f2_three_channel = _project_bool_section(
        evidence.get("f2_three_channel_readback_closure"),
        _F2_THREE_CHANNEL_CLOSURE_KEYS,
    )
    target_scope = _target_scope_summary(evidence.get("target_scope"))
    attempted_operations = _attempted_operations_summary(evidence.get("actual_attempted_operations"))
    raw_claims = evidence.get("completion_claims")
    wrapper_completion_claims = _project_wrapper_completion_claims(
        raw_claims if isinstance(raw_claims, Mapping) else {},
        source_truth=source_truth,
        duplicate_noop=duplicate_noop,
        fresh_write=fresh_write,
        exact_live=exact_live,
        f2_three_channel=f2_three_channel,
        target_scope=target_scope,
    )
    safe_summary = {
        "supplied": True,
        "consumed": True,
        "status": _safe_string(evidence.get("status")) or "unknown",
        "schema_version": _safe_string(evidence.get("schema_version")),
        "reason_code": _safe_string(evidence.get("reason_code")),
        "target_scope": target_scope,
        "actual_attempted_operations": attempted_operations,
        "source_truth_readback_closure": source_truth,
        "f1_duplicate_noop_readback_closure": duplicate_noop,
        "f1_fresh_write_readback_closure": fresh_write,
        "f1_exact_live_readback_review_closure": exact_live,
        "f2_three_channel_readback_closure": f2_three_channel,
        "operator_closure": _operator_closure_from_claims(
            wrapper_completion_claims,
            fresh_write_closed=fresh_write.get("closed") is True,
        ),
        "completion_claims": wrapper_completion_claims,
    }
    safe_summary["evidence_fingerprint"] = _fingerprint(safe_summary)
    return safe_summary


def _project_wrapper_completion_claims(
    claims: Mapping[str, Any],
    *,
    source_truth: Mapping[str, bool],
    duplicate_noop: Mapping[str, bool],
    fresh_write: Mapping[str, bool],
    exact_live: Mapping[str, bool],
    f2_three_channel: Mapping[str, bool],
    target_scope: Mapping[str, Any],
) -> dict[str, bool]:
    source_truth_closed = (
        claims.get("F1_SOURCE_TRUTH_DURABLE_READBACK_REVIEWABLE") is True
        and source_truth.get("durable_readback_present") is True
    )
    duplicate_noop_closed = (
        claims.get("F1_DUPLICATE_NOOP_READBACK_REVIEWABLE") is True
        and duplicate_noop.get("closed") is True
    )
    fresh_write_closed = fresh_write.get("closed") is True
    exact_live_closed = (
        claims.get("F1_EXACT_LIVE_READBACK_REVIEWABLE") is True
        and exact_live.get("closed") is True
        and (duplicate_noop_closed or fresh_write_closed)
    )
    f2_env_overlay_ready = (
        claims.get("F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY") is True
        and target_scope.get("target_count") == 3
    )
    f2_live_source_read_ready = (
        claims.get("F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY") is True
        and f2_three_channel.get("closed") is True
        and f2_three_channel.get("wrapper_child_execution_passed") is True
        and f2_three_channel.get("exact_child_runner_passed") is True
    )
    return {
        "F1_SOURCE_TRUTH_DURABLE_READBACK_REVIEWABLE": source_truth_closed,
        "F1_DUPLICATE_NOOP_READBACK_REVIEWABLE": duplicate_noop_closed,
        "F1_EXACT_LIVE_READBACK_REVIEWABLE": exact_live_closed,
        "F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY": f2_env_overlay_ready,
        "F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY": f2_live_source_read_ready,
        "LIVE_COLLECTOR_1_CHANNEL_CLOSED": False,
        "LIVE_COLLECTOR_3_CHANNEL_CLOSED": False,
        "PRODUCT_COMPLETE_CLOSED": False,
        "PRODUCTION_ROLLOUT_CLOSED": False,
    }


def _operator_closure_from_claims(
    claims: Mapping[str, bool],
    *,
    fresh_write_closed: bool = False,
) -> dict[str, bool]:
    return {
        "F1_EXACT_DUPLICATE_NOOP_REVIEWABILITY_CLOSED": bool(
            claims.get("F1_DUPLICATE_NOOP_READBACK_REVIEWABLE") is True
        ),
        "F1_FRESH_WRITE_REVIEWABILITY_CLOSED": bool(fresh_write_closed),
        "F1_EXACT_LIVE_READBACK_REVIEWABLE": bool(claims.get("F1_EXACT_LIVE_READBACK_REVIEWABLE") is True),
        "F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY": bool(
            claims.get("F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY") is True
        ),
        "F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY": bool(
            claims.get("F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY") is True
        ),
        "LIVE_COLLECTOR_1_CHANNEL_CLOSED": False,
        "LIVE_COLLECTOR_3_CHANNEL_CLOSED": False,
        "PRODUCT_COMPLETE_CLOSED": False,
        "PRODUCTION_ROLLOUT_CLOSED": False,
    }


def _empty_bool_section(keys: tuple[str, ...]) -> dict[str, bool]:
    return {key: False for key in keys}


def _project_bool_section(value: Any, keys: tuple[str, ...]) -> dict[str, bool]:
    source = value if isinstance(value, Mapping) else {}
    return {key: source.get(key) is True for key in keys}


def _target_scope_summary(value: Any) -> dict[str, Any]:
    source = value if isinstance(value, Mapping) else {}
    return {
        "target_count": _safe_nonnegative_int(source.get("target_count")),
        "target_fingerprints": _safe_fingerprint_list(source.get("target_fingerprints")),
        "raw_source_value_printed": source.get("raw_source_value_printed") is True,
        "direct_chat_id_allowed": source.get("direct_chat_id_allowed") is True,
        "direct_registry_id_allowed": source.get("direct_registry_id_allowed") is True,
        "broad_target_allowed": source.get("broad_target_allowed") is True,
    }


def _attempted_operations_summary(value: Any) -> dict[str, bool]:
    source = value if isinstance(value, Mapping) else {}
    return {
        "child_runner_invoked": source.get("child_runner_invoked") is True,
        "child_runner_returncode_zero": source.get("child_runner_returncode") == 0,
        "live_telegram_read_attempted_by_wrapper": source.get("live_telegram_read_attempted_by_wrapper") is True,
        "telegram_send_or_edit_attempted": source.get("telegram_send_or_edit_attempted") is True,
        "openai_attempted": source.get("openai_attempted") is True,
        "github_attempted": source.get("github_attempted") is True,
        "x_attempted": source.get("x_attempted") is True,
        "web_attempted": source.get("web_attempted") is True,
        "redis_publish_attempted_by_wrapper": source.get("redis_publish_attempted_by_wrapper") is True,
        "docker_or_systemd_called": source.get("docker_or_systemd_called") is True,
        "alembic_called": source.get("alembic_called") is True,
    }


def _default_code_gate_evidence(f9_proof: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    evidence: dict[str, dict[str, Any]] = {}
    for gate in REQUIRED_CODE_GATES:
        if gate == F9_GATE:
            evidence[gate] = {
                "status": "closed" if _f9_proof_passed(f9_proof) else "blocked",
                "evidence_source": F9_SCHEMA_VERSION,
            }
        else:
            evidence[gate] = {
                "status": "closed",
                "evidence_source": "current_head_and_reviewed_progress_authority",
            }
    return evidence


def _code_gate_status(*, evidence: Mapping[str, Any], f9_proof: Mapping[str, Any]) -> dict[str, Any]:
    gates: dict[str, dict[str, Any]] = {}
    closed_count = 0
    missing: list[str] = []
    blocked: list[str] = []
    for gate in REQUIRED_CODE_GATES:
        value = evidence.get(gate)
        if not isinstance(value, Mapping):
            missing.append(gate)
            gates[gate] = {"status": "missing"}
            continue
        status = _safe_string(value.get("status"))
        closed = status in {"closed", "pass", "CODE_CLOSED"}
        if gate == F9_GATE and not _f9_proof_passed(f9_proof):
            closed = False
        if closed:
            closed_count += 1
        else:
            blocked.append(gate)
        gates[gate] = {
            "status": "closed" if closed else status or "blocked",
            "evidence_source": _safe_string(value.get("evidence_source")) or "unspecified",
        }
    return {
        "status": "CODE_CLOSED" if closed_count == len(REQUIRED_CODE_GATES) else "CODE_INCOMPLETE",
        "all_required_code_gates_closed": closed_count == len(REQUIRED_CODE_GATES),
        "required_gate_count": len(REQUIRED_CODE_GATES),
        "closed_gate_count": closed_count,
        "missing_gate_count": len(missing),
        "blocked_gate_count": len(blocked),
        "missing_gates": missing,
        "blocked_gates": blocked,
        "gates": gates,
    }


def _external_evidence_summary(evidence: Mapping[str, Any] | None) -> dict[str, Any]:
    if evidence is None:
        return {
            "supplied": False,
            "status": "not_supplied",
            "schema_version": None,
            "head_suffix": None,
            "evidence_fingerprint": None,
        }
    return {
        "supplied": True,
        "status": _safe_string(evidence.get("status")) or "unknown",
        "schema_version": _safe_string(evidence.get("schema_version")),
        "head_suffix": _suffix(evidence.get("head") or evidence.get("commit") or evidence.get("revision")),
        "evidence_fingerprint": _fingerprint(evidence),
    }


def _f9_proof_passed(proof: Mapping[str, Any]) -> bool:
    return (
        proof.get("schema_version") == F9_SCHEMA_VERSION
        and proof.get("status") == "pass"
        and proof.get("gate_closed") is True
        and proof.get("ok") is True
    )


def _safe_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if len(normalized) > 80:
        normalized = normalized[:80]
    if all(char.isalnum() or char in {"_", "-", ".", ":"} for char in normalized):
        return normalized
    return "unsafe_value"


def _safe_nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    if value < 0:
        return None
    return value


def _safe_fingerprint_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    fingerprints: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        normalized = item.strip().lower()
        if not normalized.startswith("sha256:"):
            continue
        digest = normalized.removeprefix("sha256:")
        if 8 <= len(digest) <= 64 and all(char in "0123456789abcdef" for char in digest):
            fingerprints.append(f"sha256:{digest}")
    return fingerprints[:3]


def _suffix(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    allowed = "".join(char for char in normalized if char.isalnum())
    if len(allowed) < 7:
        return None
    return allowed[-8:]


def _fingerprint(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:12]


def render_sanitized_json(report: dict[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


__all__ = [
    "PACKET_BLOCKED_STATUS",
    "PACKET_READY_STATUS",
    "REQUIRED_CODE_GATES",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "build_function_complete_packet",
    "render_sanitized_json",
]
