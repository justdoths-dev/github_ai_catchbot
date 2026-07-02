from __future__ import annotations

import ast
import json
from pathlib import Path

from src.services.policy_engine.noise_duplicate_suppression import build_noise_duplicate_suppression_proof


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/policy_engine/noise_duplicate_suppression.py"
RAW_ID_SENTINEL = "11111111-1111-4111-8111-111111111111"
RAW_DEDUPE_SENTINEL = "notify:dedupe:private"
RAW_MATERIAL_SENTINEL = "material-secret-hash"


def test_same_subject_same_material_replay_suppresses_duplicate_send_intent() -> None:
    report = build_noise_duplicate_suppression_proof()

    assert report["ok"] is True
    assert report["status"] == "pass"
    assert report["gate"] == "F9_NOISE_DUPLICATE_SUPPRESSION"
    assert report["gates"]["same_subject_same_material_no_duplicate"] is True
    duplicate = report["duplicate_suppression"]
    assert duplicate["same_subject_same_material_first_action"] == "allow"
    assert duplicate["same_subject_same_material_replay_action"] == "suppress_duplicate"
    assert duplicate["same_subject_same_material_unique_send_intent_count"] == 1


def test_material_change_under_same_subject_is_distinct_and_stable() -> None:
    report = build_noise_duplicate_suppression_proof()

    assert report["gates"]["dedupe_subject_key_policy"] is True
    assert report["gates"]["material_change_hash_policy"] is True
    assert report["dedupe_subject_key_policy"]["same_subject_key_stable"] is True
    assert report["material_change_hash_policy"]["same_material_hash_stable"] is True
    assert report["material_change_hash_policy"]["same_subject_material_change_distinct"] is True
    distinction = report["material_change_distinction"]
    assert distinction["same_subject_material_change_first_action"] == "allow"
    assert distinction["same_subject_material_change_second_action"] == "allow_new_material"
    assert distinction["same_subject_material_change_unique_send_intent_count"] == 2


def test_suppress_later_high_distribution_and_f7_f8_compatibility() -> None:
    report = build_noise_duplicate_suppression_proof()

    assert report["distribution_compatibility"]["compatible"] is True
    cases = report["distribution_compatibility"]["cases"]
    assert cases["suppress"]["delivery_decision"] == "suppress"
    assert cases["suppress"]["send_intent_created"] is False
    assert cases["later"]["delivery_decision"] == "send_now"
    assert cases["later"]["urgency_profile"] == "normal_silent"
    assert cases["later"]["send_intent_created"] is True
    assert cases["high"]["delivery_decision"] == "send_now"
    assert cases["high"]["urgency_profile"] == "high"
    assert cases["high"]["send_intent_created"] is True

    compatibility = report["feedback_channel_compatibility"]
    assert compatibility["feedback_eval_consumed"] is True
    assert compatibility["channel_override_consumed"] is True
    assert compatibility["ai_noise_channel_decision"] == "suppress"
    assert compatibility["hot_path_enforcement_applied"] is False


def test_report_is_sanitized_and_does_not_emit_raw_identity_values() -> None:
    report = build_noise_duplicate_suppression_proof()
    rendered = json.dumps(report, ensure_ascii=False, sort_keys=True)

    assert report["raw_values_printed"] is False
    for forbidden in (
        RAW_ID_SENTINEL,
        RAW_DEDUPE_SENTINEL,
        RAW_MATERIAL_SENTINEL,
        "redacted-db-locator",
        "redacted-redis-locator",
        "4100",
    ):
        assert forbidden not in rendered
    assert report["redactions_applied"]["dedupe_keys_omitted"] is True
    assert report["redactions_applied"]["material_hashes_omitted"] is True


def test_static_source_reuses_required_policy_primitives_without_live_imports() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
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

    assert "NotificationIntentBuilder" in source
    assert "NotificationPlanIntent" in source
    assert "material_change_hash_for_analysis" in source
    assert "FeedbackEvalEngine" in source
    assert "ChannelOverridePolicy" in source
    assert {"redis", "telegram", "openai", "requests", "httpx", "aiohttp", "subprocess"}.isdisjoint(
        imported_roots
    )
    assert not any(
        forbidden in module
        for module in imported_modules
        for forbidden in ("notifier_telegram", "judge_openai", "collector_telegram", "gh_enricher", "x_enricher", "web_enricher")
    )
    assert {"systemctl", "docker", "alembic", "run_forever"}.isdisjoint(call_names)
