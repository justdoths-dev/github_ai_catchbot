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
RAW_SOURCE_TEXT = "private source text"


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
        "F1_SOURCE_TRUTH_DURABLE_READBACK_REVIEWABLE": False,
        "F1_DUPLICATE_NOOP_READBACK_REVIEWABLE": False,
        "F1_FRESH_WRITE_REVIEWABILITY_CLOSED": False,
        "F1_EXACT_LIVE_READBACK_REVIEWABLE": False,
        "F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY": False,
        "F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY": False,
        "LIVE_COLLECTOR_1_CHANNEL_CLOSED": False,
        "LIVE_COLLECTOR_3_CHANNEL_CLOSED": False,
        "PRODUCT_COMPLETE_CLOSED": False,
        "PRODUCTION_ROLLOUT_CLOSED": False,
    }
    assert packet["collector_wrapper_readback"]["supplied"] is False


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


def test_collector_wrapper_duplicate_noop_sections_are_consumed_without_product_claims() -> None:
    packet = build_function_complete_packet(
        f9_proof=build_noise_duplicate_suppression_proof(),
        collector_wrapper_evidence=_duplicate_noop_wrapper_report(),
    )
    wrapper = packet["collector_wrapper_readback"]
    claims = packet["completion_claims"]

    assert wrapper["supplied"] is True
    assert wrapper["consumed"] is True
    assert wrapper["source_truth_readback_closure"]["durable_readback_present"] is True
    assert wrapper["f1_duplicate_noop_readback_closure"]["closed"] is True
    assert wrapper["f1_fresh_write_readback_closure"]["closed"] is False
    assert wrapper["operator_closure"]["F1_EXACT_DUPLICATE_NOOP_REVIEWABILITY_CLOSED"] is True
    assert wrapper["operator_closure"]["F1_FRESH_WRITE_REVIEWABILITY_CLOSED"] is False
    assert claims["F1_SOURCE_TRUTH_DURABLE_READBACK_REVIEWABLE"] is True
    assert claims["F1_DUPLICATE_NOOP_READBACK_REVIEWABLE"] is True
    assert claims["F1_FRESH_WRITE_REVIEWABILITY_CLOSED"] is False
    assert claims["F1_EXACT_LIVE_READBACK_REVIEWABLE"] is True
    assert claims["F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY"] is False
    assert claims["LIVE_COLLECTOR_1_CHANNEL_CLOSED"] is False
    assert claims["LIVE_COLLECTOR_3_CHANNEL_CLOSED"] is False
    assert claims["PRODUCTION_ROLLOUT_CLOSED"] is False
    assert claims["PRODUCT_COMPLETE_CLOSED"] is False


def test_collector_wrapper_three_channel_preflight_and_live_proof_are_distinct() -> None:
    preflight_packet = build_function_complete_packet(
        f9_proof=build_noise_duplicate_suppression_proof(),
        collector_wrapper_evidence=_three_channel_wrapper_report(live_source_read_closed=False),
    )
    live_packet = build_function_complete_packet(
        f9_proof=build_noise_duplicate_suppression_proof(),
        collector_wrapper_evidence=_three_channel_wrapper_report(live_source_read_closed=True),
    )

    assert preflight_packet["completion_claims"]["F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY"] is True
    assert preflight_packet["completion_claims"]["F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY"] is False
    assert preflight_packet["completion_claims"]["F1_DUPLICATE_NOOP_READBACK_REVIEWABLE"] is False
    assert live_packet["completion_claims"]["F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY"] is True
    assert live_packet["completion_claims"]["F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY"] is True
    assert live_packet["completion_claims"]["F1_DUPLICATE_NOOP_READBACK_REVIEWABLE"] is False
    assert live_packet["completion_claims"]["LIVE_COLLECTOR_3_CHANNEL_CLOSED"] is False
    assert live_packet["completion_claims"]["PRODUCTION_ROLLOUT_CLOSED"] is False
    assert live_packet["completion_claims"]["PRODUCT_COMPLETE_CLOSED"] is False


def test_multiple_collector_wrapper_reports_aggregate_without_synthetic_scope_merge() -> None:
    packet = build_function_complete_packet(
        f9_proof=build_noise_duplicate_suppression_proof(),
        collector_wrapper_evidence=[
            _duplicate_noop_wrapper_report(),
            _three_channel_wrapper_report(live_source_read_closed=False),
        ],
    )
    wrapper = packet["collector_wrapper_readback"]
    claims = packet["completion_claims"]

    assert wrapper["supplied"] is True
    assert wrapper["consumed"] is True
    assert wrapper["schema_version"] == "collector_wrapper_evidence_aggregate_v1"
    assert wrapper["evidence_report_count"] == 2
    assert len(wrapper["evidence_reports"]) == 2
    assert claims["F1_SOURCE_TRUTH_DURABLE_READBACK_REVIEWABLE"] is True
    assert claims["F1_DUPLICATE_NOOP_READBACK_REVIEWABLE"] is True
    assert claims["F1_FRESH_WRITE_REVIEWABILITY_CLOSED"] is False
    assert claims["F1_EXACT_LIVE_READBACK_REVIEWABLE"] is True
    assert claims["F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY"] is True
    assert claims["F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY"] is False
    assert wrapper["evidence_reports"][0]["completion_claims"]["F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY"] is False
    assert wrapper["evidence_reports"][1]["completion_claims"]["F1_DUPLICATE_NOOP_READBACK_REVIEWABLE"] is False
    assert claims["LIVE_COLLECTOR_1_CHANNEL_CLOSED"] is False
    assert claims["LIVE_COLLECTOR_3_CHANNEL_CLOSED"] is False
    assert claims["PRODUCT_COMPLETE_CLOSED"] is False
    assert claims["PRODUCTION_ROLLOUT_CLOSED"] is False


def test_wrapper_claims_do_not_bypass_first_class_closure_or_child_returncode_gates() -> None:
    wrapper_report = _three_channel_wrapper_report(live_source_read_closed=True)
    wrapper_report["f2_three_channel_readback_closure"]["closed"] = False
    wrapper_report["f2_three_channel_readback_closure"]["wrapper_child_execution_passed"] = False
    wrapper_report["f2_three_channel_readback_closure"]["exact_child_runner_passed"] = False
    wrapper_report["actual_attempted_operations"]["child_runner_returncode"] = 1
    wrapper_report["completion_claims"]["F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY"] = True

    packet = build_function_complete_packet(
        f9_proof=build_noise_duplicate_suppression_proof(),
        collector_wrapper_evidence=wrapper_report,
    )

    assert packet["completion_claims"]["F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY"] is False
    assert packet["collector_wrapper_readback"]["actual_attempted_operations"]["child_runner_returncode_zero"] is False


def test_collector_wrapper_evidence_is_sanitized_and_does_not_copy_child_report() -> None:
    wrapper_report = _duplicate_noop_wrapper_report()
    wrapper_report["child_report"] = {
        "stdout_parsed_as_json": True,
        "raw_url": RAW_URL,
        "raw_source_text": RAW_SOURCE_TEXT,
        "stderr": RAW_SECRET,
    }
    wrapper_report["unsafe_url"] = RAW_URL

    packet = build_function_complete_packet(
        f9_proof=build_noise_duplicate_suppression_proof(),
        collector_wrapper_evidence=wrapper_report,
    )
    rendered = json.dumps(packet, ensure_ascii=False, sort_keys=True)

    assert "child_report" not in packet["collector_wrapper_readback"]
    assert packet["collector_wrapper_readback"]["target_scope"]["target_fingerprints"] == ["sha256:1111111111111111"]
    for forbidden in (RAW_URL, RAW_SOURCE_TEXT, RAW_SECRET):
        assert forbidden not in rendered


def _duplicate_noop_wrapper_report() -> dict[str, object]:
    return {
        "schema_version": "restricted_live_collector_one_channel_source_read_env_overlay_runner_v1",
        "status": "pass",
        "reason_code": "child_bounded_runner_passed",
        "target_scope": {
            "target_count": 1,
            "target_fingerprints": ["sha256:1111111111111111"],
            "raw_source_value_printed": False,
            "direct_chat_id_allowed": False,
            "direct_registry_id_allowed": False,
            "broad_target_allowed": False,
        },
        "actual_attempted_operations": {
            "child_runner_invoked": True,
            "child_runner_returncode": 0,
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
        "source_truth_readback_closure": {
            "child_report_available": True,
            "wrapper_child_execution_passed": True,
            "exact_child_runner_passed": True,
            "live_telegram_read_attempted": True,
            "telegram_read_called": True,
            "messages_seen_present": True,
            "source_current_readback_present": True,
            "source_version_readback_present": True,
            "source_created_events_readback_present": True,
            "source_outbox_events_readback_present": True,
            "source_outbox_publish_disabled": True,
            "redis_publish_disabled": True,
            "telegram_send_disabled": True,
            "provider_calls_disabled": True,
            "docker_systemd_alembic_disabled": True,
            "raw_values_not_printed": True,
            "runtime_values_not_printed": True,
            "durable_readback_present": True,
        },
        "f1_duplicate_noop_readback_closure": {
            "one_channel_or_legacy_child_report": True,
            "source_truth_durable_readback_present": True,
            "duplicate_noop_proof_present": True,
            "duplicate_noop_without_second_telegram_read": True,
            "closed": True,
        },
        "f1_fresh_write_readback_closure": {
            "one_channel_or_legacy_child_report": True,
            "source_truth_durable_readback_present": True,
            "database_write_attempted": False,
            "source_message_write_attempted": False,
            "source_version_write_attempted": False,
            "source_outbox_write_attempted": False,
            "closed": False,
        },
        "f1_exact_live_readback_review_closure": {
            "duplicate_noop_readback_closed": True,
            "fresh_write_readback_closed": False,
            "closed": True,
        },
        "f2_three_channel_readback_closure": {"closed": False},
        "completion_claims": {
            "F1_SOURCE_TRUTH_DURABLE_READBACK_REVIEWABLE": True,
            "F1_DUPLICATE_NOOP_READBACK_REVIEWABLE": True,
            "F1_EXACT_LIVE_READBACK_REVIEWABLE": True,
            "F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY": False,
            "F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY": False,
            "LIVE_COLLECTOR_1_CHANNEL_CLOSED": False,
            "LIVE_COLLECTOR_3_CHANNEL_CLOSED": False,
            "PRODUCT_COMPLETE_CLOSED": False,
            "PRODUCTION_ROLLOUT_CLOSED": False,
        },
    }


def _three_channel_wrapper_report(*, live_source_read_closed: bool) -> dict[str, object]:
    report = _duplicate_noop_wrapper_report()
    report["target_scope"] = {
        "target_count": 3,
        "target_fingerprints": [
            "sha256:aaaaaaaaaaaaaaaa",
            "sha256:bbbbbbbbbbbbbbbb",
            "sha256:cccccccccccccccc",
        ],
        "raw_source_value_printed": False,
        "direct_chat_id_allowed": False,
        "direct_registry_id_allowed": False,
        "broad_target_allowed": False,
    }
    report["f2_three_channel_readback_closure"] = {
        "child_report_available": live_source_read_closed,
        "wrapper_child_execution_passed": live_source_read_closed,
        "exact_child_runner_passed": live_source_read_closed,
        "target_count_is_three": True,
        "target_fingerprint_count_is_three": True,
        "per_channel_result_count_is_three": live_source_read_closed,
        "per_channel_status_passed": live_source_read_closed,
        "per_channel_messages_seen_present": live_source_read_closed,
        "per_channel_readbacks_present": live_source_read_closed,
        "aggregate_source_current_readback_present": live_source_read_closed,
        "aggregate_source_version_readback_present": live_source_read_closed,
        "aggregate_source_created_events_readback_present": live_source_read_closed,
        "aggregate_source_outbox_events_readback_present": live_source_read_closed,
        "aggregate_duplicate_noop_or_fresh_write_sufficient": live_source_read_closed,
        "source_outbox_publish_disabled": live_source_read_closed,
        "redis_publish_disabled": live_source_read_closed,
        "telegram_send_disabled": live_source_read_closed,
        "provider_calls_disabled": live_source_read_closed,
        "docker_systemd_alembic_disabled": live_source_read_closed,
        "raw_values_not_printed": live_source_read_closed,
        "runtime_values_not_printed": live_source_read_closed,
        "closed": live_source_read_closed,
    }
    report["completion_claims"] = {
        "F1_SOURCE_TRUTH_DURABLE_READBACK_REVIEWABLE": True,
        "F1_DUPLICATE_NOOP_READBACK_REVIEWABLE": True,
        "F1_EXACT_LIVE_READBACK_REVIEWABLE": True,
        "F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY": True,
        "F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY": live_source_read_closed,
        "LIVE_COLLECTOR_1_CHANNEL_CLOSED": False,
        "LIVE_COLLECTOR_3_CHANNEL_CLOSED": False,
        "PRODUCT_COMPLETE_CLOSED": False,
        "PRODUCTION_ROLLOUT_CLOSED": False,
    }
    return report
