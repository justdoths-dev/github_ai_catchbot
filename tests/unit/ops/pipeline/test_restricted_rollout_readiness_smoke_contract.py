from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "restricted_rollout_readiness_smoke.md"


def _module():
    from scripts.ops import restricted_rollout_readiness_smoke as module

    return module


def test_report_type_and_selected_scenario_constants_are_locked() -> None:
    module = _module()

    assert module.REPORT_TYPE == "restricted_rollout_readiness_smoke_v1"
    assert module.SELECTED_SCENARIO == "repo_control_plane_readiness"
    assert module.PRODUCTION_AUTHORIZATION_STATUS == "not_authorized"
    assert module.ROLLOUT_STAGE == "pre_restricted_rollout_planning"


def test_required_flag_names_are_exactly_locked() -> None:
    module = _module()

    assert module.REQUIRED_FLAG_NAMES == [
        "ENABLE_NOTIFICATION_SEND",
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
        "NOTIFIER_TELEGRAM_DRY_RUN",
    ]


def test_forbidden_side_effect_booleans_are_false_in_passing_report(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "ready.md"
    asset_path.write_text("local marker\n", encoding="utf-8")
    manifest = (
        module.RequiredAsset(
            "delivery_gate_assets",
            "ready.md",
            (module.MarkerRequirement("local_marker", ("local marker",)),),
        ),
    )

    result = module.generate_report(tmp_path, asset_manifest=manifest)
    report = result.report

    assert result.exit_code == 0
    assert report["checks_failed"] == []
    assert report["failures"] == []
    assert report["runtime_worker_started"] is False
    assert report["external_network_used"] is False
    assert report["database_required"] is False
    assert report["redis_required"] is False
    assert report["database_mutated"] is False
    assert report["redis_mutated"] is False
    assert report["env_mutated"] is False
    assert report["feature_flags_mutated"] is False
    assert report["recommended_flag_patch_applied"] is False
    assert report["production_db_or_redis_used"] is False
    assert report["live_collector_started"] is False
    assert report["live_notifier_transport_used"] is False


def test_recommended_next_state_is_not_production_activation() -> None:
    module = _module()

    assert module.PRODUCTION_AUTHORIZATION_STATUS == "not_authorized"
    assert module.RECOMMENDED_NEXT_STATE == "ready_for_separately_approved_restricted_rollout_planning"
    assert module.RECOMMENDED_NEXT_STATE != "production_activation"
    assert "production" not in module.RECOMMENDED_NEXT_STATE


def test_required_asset_manifest_includes_expected_categories() -> None:
    module = _module()

    categories = {asset.category for asset in module.REQUIRED_ASSET_MANIFEST}

    assert "delivery_gate_assets" in categories
    assert "batch_recovery_assets" in categories
    assert "maintenance_assets" in categories
    assert "notifier_safety_assets" in categories
    assert "rollback_assets" in categories
    assert "handoff_assets" in categories


def test_missing_asset_produces_failed_check_and_nonzero_report(tmp_path) -> None:
    module = _module()
    manifest = (
        module.RequiredAsset(
            "delivery_gate_assets",
            "missing.md",
            (module.MarkerRequirement("required_marker", ("required marker",)),),
        ),
    )

    result = module.generate_report(tmp_path, asset_manifest=manifest)

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["asset.exists:missing.md"]
    assert result.report["required_assets"]["delivery_gate_assets:missing.md"]["exists"] is False
    assert result.report["required_assets"]["delivery_gate_assets:missing.md"]["markers_passed"] is False


def test_missing_marker_produces_failed_check_and_nonzero_report(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "asset.md"
    asset_path.write_text("different text\n", encoding="utf-8")
    manifest = (
        module.RequiredAsset(
            "delivery_gate_assets",
            "asset.md",
            (module.MarkerRequirement("required_marker", ("required marker",)),),
        ),
    )

    result = module.generate_report(tmp_path, asset_manifest=manifest)

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["asset.markers:asset.md"]
    asset_report = result.report["required_assets"]["delivery_gate_assets:asset.md"]
    assert asset_report["exists"] is True
    assert asset_report["markers_passed"] is False
    assert asset_report["missing_markers"] == ["required_marker"]


def test_delivery_gate_runbook_markers_reject_generic_positive_rollout_wording(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "gate.md"
    asset_path.write_text(
        "production rollout\n"
        "write `.env` files\n"
        "recommended_flag_patch\n",
        encoding="utf-8",
    )
    manifest = (
        module.RequiredAsset(
            "delivery_gate_assets",
            "gate.md",
            module.DELIVERY_GATE_RUNBOOK_MARKERS,
        ),
    )

    result = module.generate_report(tmp_path, asset_manifest=manifest)

    assert result.exit_code == 1
    asset_report = result.report["required_assets"]["delivery_gate_assets:gate.md"]
    assert asset_report["exists"] is True
    assert asset_report["markers_passed"] is False
    assert "production_rollout_not_authorized" in asset_report["missing_markers"]
    assert "env_or_feature_flags_not_mutated" in asset_report["missing_markers"]
    assert "recommended_flag_patch_output_only" in asset_report["missing_markers"]


def test_runbook_contains_hard_safety_warnings() -> None:
    text = RUNBOOK.read_text(encoding="utf-8").lower()

    assert "does not authorize production rollout" in text
    assert "does not apply recommended flag patches" in text
    assert "does not mutate env files" in text
    assert "does not require db or redis" in text
    assert "does not call external apis" in text
    assert "does not start runtime workers" in text
    assert "ready for separately approved restricted rollout planning" in text


def test_render_json_is_deterministic_json(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "ready.md"
    asset_path.write_text("ready\n", encoding="utf-8")

    result = module.generate_report(
        tmp_path,
        asset_manifest=(module.RequiredAsset("delivery_gate_assets", "ready.md"),),
    )

    rendered = module.render_json(result.report)

    assert json.loads(rendered)["report_type"] == "restricted_rollout_readiness_smoke_v1"
