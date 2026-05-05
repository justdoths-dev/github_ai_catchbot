from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RUNBOOK = ROOT / "ops" / "delivery" / "runbooks" / "restricted_rollout_planning_package.md"


def _module():
    from scripts.ops import restricted_rollout_planning_readiness_check as module

    return module


def test_report_type_and_selected_scenario_constants_are_locked() -> None:
    module = _module()

    assert module.REPORT_TYPE == "restricted_rollout_planning_readiness_check_v1"
    assert module.SELECTED_SCENARIO == "repo_restricted_rollout_planning_package"
    assert module.PRODUCTION_AUTHORIZATION_STATUS == "not_authorized"
    assert module.ROLLOUT_STAGE == "restricted_rollout_planning_only"


def test_recommended_next_state_is_planning_review_only() -> None:
    module = _module()

    assert module.RECOMMENDED_NEXT_STATE == "ready_for_operator_reviewed_restricted_rollout_plan"
    assert module.RECOMMENDED_NEXT_STATE != "production_activation"
    assert "full_go_live" not in module.RECOMMENDED_NEXT_STATE


def test_forbidden_side_effect_booleans_are_false_in_passing_report(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "ready.md"
    asset_path.write_text("ready\n", encoding="utf-8")
    manifest = (module.RequiredAsset("planning_package_present", "ready.md"),)

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
    assert report["secret_values_read"] is False
    assert report["production_db_or_redis_used"] is False
    assert report["live_collector_started"] is False
    assert report["live_notifier_transport_used"] is False
    assert report["systemd_or_compose_invoked"] is False


def test_required_rollout_phases_are_exactly_ordered() -> None:
    module = _module()

    assert module.REQUIRED_ROLLOUT_PHASES == [
        "offline_validation",
        "live_ingest",
        "shadow_analysis",
        "silent_delivery",
        "restricted_rollout",
        "full_go_live",
    ]


def test_required_manual_approval_domains_are_present() -> None:
    module = _module()

    assert module.REQUIRED_MANUAL_APPROVAL_DOMAINS == [
        "operator_approval",
        "secrets_inventory_review",
        "environment_plan_review",
        "one_live_collector_plan",
        "rollback_plan_review",
        "restricted_transport_smoke_plan",
    ]


def test_missing_planning_package_asset_fails(tmp_path) -> None:
    module = _module()
    manifest = (
        module.RequiredAsset(
            "planning_package_present",
            "missing.md",
            (module.MarkerRequirement("required_marker", ("required marker",)),),
        ),
    )

    result = module.generate_report(tmp_path, asset_manifest=manifest)

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["asset.exists:missing.md"]
    asset_report = result.report["required_assets"]["planning_package_present:missing.md"]
    assert asset_report["exists"] is False
    assert asset_report["markers_passed"] is False


def test_missing_marker_fails(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "package.md"
    asset_path.write_text("different text\n", encoding="utf-8")
    manifest = (
        module.RequiredAsset(
            "planning_package_present",
            "package.md",
            (module.MarkerRequirement("required_marker", ("required marker",)),),
        ),
    )

    result = module.generate_report(tmp_path, asset_manifest=manifest)

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["asset.markers:package.md"]
    asset_report = result.report["required_assets"]["planning_package_present:package.md"]
    assert asset_report["exists"] is True
    assert asset_report["markers_passed"] is False
    assert asset_report["missing_markers"] == ["required_marker"]


def test_generic_positive_rollout_wording_does_not_satisfy_no_authorization_marker(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "package.md"
    asset_path.write_text("production rollout\n", encoding="utf-8")
    manifest = (
        module.RequiredAsset(
            "planning_package_present",
            "package.md",
            (
                module.MarkerRequirement(
                    "production_rollout_not_authorized",
                    ("this does not authorize production rollout",),
                ),
            ),
        ),
    )

    result = module.generate_report(tmp_path, asset_manifest=manifest)

    assert result.exit_code == 1
    asset_report = result.report["required_assets"]["planning_package_present:package.md"]
    assert asset_report["missing_markers"] == ["production_rollout_not_authorized"]


def test_generic_secret_wording_does_not_satisfy_secret_values_marker(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "package.md"
    asset_path.write_text("secret inventory\n", encoding="utf-8")
    manifest = (
        module.RequiredAsset(
            "planning_package_present",
            "package.md",
            (
                module.MarkerRequirement(
                    "secret_values_not_read_or_printed",
                    ("this does not read or print secret values",),
                ),
            ),
        ),
    )

    result = module.generate_report(tmp_path, asset_manifest=manifest)

    assert result.exit_code == 1
    asset_report = result.report["required_assets"]["planning_package_present:package.md"]
    assert asset_report["missing_markers"] == ["secret_values_not_read_or_printed"]


def test_runbook_contains_hard_non_goals_and_phase_order() -> None:
    text = RUNBOOK.read_text(encoding="utf-8").lower()

    assert "this does not authorize production rollout" in text
    assert "this does not apply recommended flag patches" in text
    assert "this does not mutate env files" in text
    assert "this does not read or print secret values" in text
    assert "this does not require db or redis" in text
    assert "this does not call external apis" in text
    assert "this does not start runtime workers" in text
    assert "this does not start the telegram collector" in text
    assert "this does not start telegram notifier transport" in text
    assert "production must have exactly one live telegram collector instance" in text
    assert (
        "offline validation -> live ingest -> shadow analysis -> silent delivery -> restricted rollout -> full go-live"
        in text
    )
    assert "`recommended_flag_patch` is output-only" in text
    assert "actual flag changes require explicit operator approval" in text
    assert "rollback must disable `enable_notification_send=false`" in text
    assert "rollback must disable `maintenance_enable_notification_retry_promotion=false`" in text
    assert "restricted transport smoke requires separate explicit approval" in text
    assert "real secrets must be reviewed by presence/ownership only, never printed" in text


def test_current_repo_root_report_passes() -> None:
    module = _module()

    result = module.generate_report(ROOT)

    assert result.exit_code == 0
    assert result.report["checks_failed"] == []
    assert result.report["readiness_summary"] == {
        "planning_package_present": True,
        "slice16_readiness_smoke_present": True,
        "delivery_gate_handoff_present": True,
        "rollback_runbook_present": True,
        "db_acceptance_smoke_runbook_present": True,
        "one_live_collector_invariant_present": True,
        "feature_flag_output_only_boundary_present": True,
        "manual_approval_boundary_present": True,
    }


def test_render_json_is_deterministic_json(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "ready.md"
    asset_path.write_text("ready\n", encoding="utf-8")

    result = module.generate_report(
        tmp_path,
        asset_manifest=(module.RequiredAsset("planning_package_present", "ready.md"),),
    )
    rendered = module.render_json(result.report)

    assert json.loads(rendered)["report_type"] == "restricted_rollout_planning_readiness_check_v1"
