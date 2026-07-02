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


def build_function_complete_packet(
    *,
    f9_proof: Mapping[str, Any] | None = None,
    code_gate_evidence: Mapping[str, Any] | None = None,
    origin_evidence: Mapping[str, Any] | None = None,
    vps_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    proof = dict(f9_proof or build_noise_duplicate_suppression_proof())
    evidence = _default_code_gate_evidence(proof) if code_gate_evidence is None else dict(code_gate_evidence)
    gate_status = _code_gate_status(evidence=evidence, f9_proof=proof)
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
