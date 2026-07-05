from __future__ import annotations

import ast
import json
from pathlib import Path

from src.services.policy_engine.noise_duplicate_suppression import (
    build_noise_duplicate_suppression_proof,
    render_sanitized_json,
)
from tools import bounded_function_complete_packet_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_function_complete_packet_runner.py"
F9_PROOF_PATH = ROOT / "src/services/policy_engine/noise_duplicate_suppression.py"
F10_PACKET_PATH = ROOT / "src/services/policy_engine/function_complete_packet.py"
RAW_URL = "https://" + "private.example.invalid/evidence"
RAW_SECRET = "private stderr value"


def test_runner_consumes_shared_f9_service_output_and_emits_packet_ready(capsys) -> None:
    exit_code = runner.main([])
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["schema_version"] == "function_complete_packet_v1"
    assert parsed["packet_status"] == "FUNCTION_COMPLETE_PACKET_READY"
    assert parsed["F9"]["consumed"] is True
    assert parsed["CODE_CLOSED"]["closed_gate_count"] == 9
    assert parsed["AUTHORITY_OPEN"]["open"] is True
    assert parsed["ROLLOUT_OPEN"]["open"] is True
    assert parsed["PRODUCTION_ROLLOUT_OPEN"]["open"] is True
    assert parsed["completion_claims"]["final_bot_complete"] is False
    assert parsed["raw_values_printed"] is False


def test_runner_requires_allow_flag_before_reading_f9_proof_file(tmp_path, capsys) -> None:
    proof_path = tmp_path / "f9-proof.json"
    proof_path.write_text(render_sanitized_json(build_noise_duplicate_suppression_proof()), encoding="utf-8")

    exit_code = runner.main(["--f9-proof-json", str(proof_path)])
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["status"] == "blocked"
    assert parsed["reason_code"] == "f9_proof_file_read_not_allowed"


def test_runner_consumes_f9_proof_file_and_sanitizes_origin_vps_evidence(tmp_path, capsys) -> None:
    proof_path = tmp_path / "f9-proof.json"
    proof_path.write_text(render_sanitized_json(build_noise_duplicate_suppression_proof()), encoding="utf-8")
    origin_path = tmp_path / "origin.json"
    origin_path.write_text(
        json.dumps(
            {
                "schema_version": "origin_readback_v1",
                "status": "pass",
                "head": "ba2c13d75c83ca7004d0e24938fc978e5221225d",
                "url": RAW_URL,
            }
        ),
        encoding="utf-8",
    )
    vps_path = tmp_path / "vps.json"
    vps_path.write_text(
        json.dumps(
            {
                "schema_version": "vps_readback_v1",
                "status": "pass",
                "commit": "ba2c13d75c83ca7004d0e24938fc978e5221225d",
                "stderr": RAW_SECRET,
            }
        ),
        encoding="utf-8",
    )

    exit_code = runner.main(
        [
            "--f9-proof-json",
            str(proof_path),
            "--allow-f9-proof-file-read",
            "--origin-evidence-json",
            str(origin_path),
            "--allow-origin-evidence-file-read",
            "--vps-evidence-json",
            str(vps_path),
            "--allow-vps-evidence-file-read",
        ]
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["ORIGIN"]["supplied"] is True
    assert parsed["ORIGIN"]["head_suffix"] == "5221225d"
    assert parsed["VPS"]["supplied"] is True
    assert parsed["VPS"]["head_suffix"] == "5221225d"
    for forbidden in (RAW_URL, RAW_SECRET, "ba2c13d75c83ca7004d0e24938fc978e5221225d"):
        assert forbidden not in output


def test_runner_requires_allow_flag_before_reading_collector_wrapper_evidence(tmp_path, capsys) -> None:
    wrapper_path = tmp_path / "collector-wrapper.json"
    wrapper_path.write_text(json.dumps(_collector_wrapper_report()), encoding="utf-8")

    exit_code = runner.main(["--collector-wrapper-evidence-json", str(wrapper_path)])
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["status"] == "blocked"
    assert parsed["reason_code"] == "collector_wrapper_evidence_file_read_not_allowed"


def test_runner_consumes_collector_wrapper_evidence_without_raw_child_report(tmp_path, capsys) -> None:
    wrapper_report = _collector_wrapper_report()
    wrapper_report["child_report"] = {
        "stdout_parsed_as_json": True,
        "url": RAW_URL,
        "stderr": RAW_SECRET,
    }
    wrapper_path = tmp_path / "collector-wrapper.json"
    wrapper_path.write_text(json.dumps(wrapper_report), encoding="utf-8")

    exit_code = runner.main(
        [
            "--collector-wrapper-evidence-json",
            str(wrapper_path),
            "--allow-collector-wrapper-evidence-file-read",
        ]
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["collector_wrapper_readback"]["consumed"] is True
    assert parsed["collector_wrapper_readback"]["f1_duplicate_noop_readback_closure"]["closed"] is True
    assert parsed["collector_wrapper_readback"]["operator_closure"][
        "F1_EXACT_DUPLICATE_NOOP_REVIEWABILITY_CLOSED"
    ] is True
    assert parsed["completion_claims"]["F1_DUPLICATE_NOOP_READBACK_REVIEWABLE"] is True
    assert parsed["completion_claims"]["F1_FRESH_WRITE_REVIEWABILITY_CLOSED"] is False
    assert parsed["completion_claims"]["F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY"] is False
    assert parsed["completion_claims"]["LIVE_COLLECTOR_1_CHANNEL_CLOSED"] is False
    assert parsed["completion_claims"]["PRODUCTION_ROLLOUT_CLOSED"] is False
    assert "child_report" not in parsed["collector_wrapper_readback"]
    for forbidden in (RAW_URL, RAW_SECRET):
        assert forbidden not in output


def test_runner_consumes_repeated_collector_wrapper_evidence_files(tmp_path, capsys) -> None:
    f1_path = tmp_path / "collector-wrapper-f1.json"
    f1_path.write_text(json.dumps(_collector_wrapper_report()), encoding="utf-8")
    f2_path = tmp_path / "collector-wrapper-f2.json"
    f2_path.write_text(json.dumps(_three_channel_wrapper_report(live_source_read_closed=False)), encoding="utf-8")

    exit_code = runner.main(
        [
            "--collector-wrapper-evidence-json",
            str(f1_path),
            "--collector-wrapper-evidence-json",
            str(f2_path),
            "--allow-collector-wrapper-evidence-file-read",
        ]
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["collector_wrapper_readback"]["consumed"] is True
    assert parsed["collector_wrapper_readback"]["schema_version"] == "collector_wrapper_evidence_aggregate_v1"
    assert parsed["collector_wrapper_readback"]["evidence_report_count"] == 2
    assert parsed["completion_claims"]["F1_DUPLICATE_NOOP_READBACK_REVIEWABLE"] is True
    assert parsed["completion_claims"]["F1_FRESH_WRITE_REVIEWABILITY_CLOSED"] is False
    assert parsed["completion_claims"]["F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY"] is True
    assert parsed["completion_claims"]["F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY"] is False
    assert parsed["completion_claims"]["LIVE_COLLECTOR_1_CHANNEL_CLOSED"] is False
    assert parsed["completion_claims"]["LIVE_COLLECTOR_3_CHANNEL_CLOSED"] is False
    assert parsed["completion_claims"]["PRODUCT_COMPLETE_CLOSED"] is False
    assert parsed["completion_claims"]["PRODUCTION_ROLLOUT_CLOSED"] is False
    for forbidden in (RAW_URL, RAW_SECRET):
        assert forbidden not in output


def test_static_runner_and_modules_have_no_forbidden_live_imports_or_calls() -> None:
    for path in (TOOL_PATH, F9_PROOF_PATH, F10_PACKET_PATH):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported_roots: set[str] = set()
        imported_modules: set[str] = set()
        call_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
                imported_modules.add(node.module)
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    call_names.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    call_names.add(node.func.attr)

        assert {"redis", "telegram", "openai", "requests", "httpx", "aiohttp", "subprocess"}.isdisjoint(
            imported_roots
        )
        assert not any(
            forbidden in module
            for module in imported_modules
            for forbidden in (
                "notifier_telegram",
                "judge_openai",
                "collector_telegram",
                "gh_enricher",
                "x_enricher",
                "web_enricher",
                "evidence_assembler",
            )
        )
        assert {"systemctl", "docker", "alembic", "run_forever"}.isdisjoint(call_names)
    assert "print(" not in TOOL_PATH.read_text(encoding="utf-8")


def _collector_wrapper_report() -> dict[str, object]:
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
    report = _collector_wrapper_report()
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
