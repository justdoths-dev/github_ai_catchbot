from __future__ import annotations

import json

from src.services.policy_engine.function_complete_packet import (
    PACKET_BLOCKED_STATUS,
    PACKET_READY_STATUS,
    REQUIRED_CODE_GATES,
    build_function_complete_packet,
)
from src.services.policy_engine.noise_duplicate_suppression import build_noise_duplicate_suppression_proof


RAW_URL = "https://" + "private.example.invalid/raw"
RAW_ID = "11111111-1111-4111-8111-111111111111"
RAW_SECRET = "private stderr body"


def test_packet_ready_only_after_f1_f9_code_evidence_and_keeps_rollout_gates_open() -> None:
    packet = build_function_complete_packet(f9_proof=build_noise_duplicate_suppression_proof())

    assert packet["ok"] is True
    assert packet["status"] == "pass"
    assert packet["packet_status"] == PACKET_READY_STATUS
    assert packet["CODE_CLOSED"]["status"] == "CODE_CLOSED"
    assert packet["CODE_CLOSED"]["required_gate_count"] == 9
    assert packet["CODE_CLOSED"]["closed_gate_count"] == 9
    assert set(packet["CODE_CLOSED"]["gates"]) == set(REQUIRED_CODE_GATES)
    assert packet["AUTHORITY_OPEN"]["open"] is True
    assert packet["ROLLOUT_OPEN"]["open"] is True
    assert packet["PRODUCTION_ROLLOUT_OPEN"]["open"] is True
    assert packet["completion_claims"] == {
        "function_complete_packet_ready": True,
        "function_complete_ready": False,
        "final_bot_complete": False,
        "production_complete": False,
        "one_hundred_percent_complete": False,
    }


def test_packet_blocks_when_f9_proof_or_code_gate_evidence_is_missing() -> None:
    f9_proof = build_noise_duplicate_suppression_proof()
    evidence = {
        gate: {"status": "closed", "evidence_source": "unit_test"} for gate in REQUIRED_CODE_GATES[:-1]
    }
    packet = build_function_complete_packet(f9_proof=f9_proof, code_gate_evidence=evidence)

    assert packet["ok"] is False
    assert packet["packet_status"] == PACKET_BLOCKED_STATUS
    assert packet["CODE_CLOSED"]["status"] == "CODE_INCOMPLETE"
    assert packet["CODE_CLOSED"]["missing_gates"] == ["F9_NOISE_DUPLICATE_SUPPRESSION"]

    blocked_f9 = dict(f9_proof)
    blocked_f9["status"] = "blocked"
    packet = build_function_complete_packet(f9_proof=blocked_f9)
    assert packet["ok"] is False
    assert packet["packet_status"] == PACKET_BLOCKED_STATUS
    assert "F9_NOISE_DUPLICATE_SUPPRESSION" in packet["CODE_CLOSED"]["blocked_gates"]


def test_origin_and_vps_evidence_are_sanitized_without_completion_claims() -> None:
    packet = build_function_complete_packet(
        f9_proof=build_noise_duplicate_suppression_proof(),
        origin_evidence={
            "schema_version": "origin_readback_v1",
            "status": "pass",
            "head": "ba2c13d75c83ca7004d0e24938fc978e5221225d",
            "url": RAW_URL,
            "raw_id": RAW_ID,
        },
        vps_evidence={
            "schema_version": "vps_readback_v1",
            "status": "pass",
            "commit": "ba2c13d75c83ca7004d0e24938fc978e5221225d",
            "stderr": RAW_SECRET,
        },
    )
    rendered = json.dumps(packet, ensure_ascii=False, sort_keys=True)

    assert packet["ORIGIN"]["supplied"] is True
    assert packet["ORIGIN"]["status"] == "pass"
    assert packet["ORIGIN"]["head_suffix"] == "5221225d"
    assert packet["VPS"]["supplied"] is True
    assert packet["VPS"]["head_suffix"] == "5221225d"
    assert packet["completion_claims"]["production_complete"] is False
    for forbidden in (RAW_URL, RAW_ID, RAW_SECRET, "ba2c13d75c83ca7004d0e24938fc978e5221225d"):
        assert forbidden not in rendered
