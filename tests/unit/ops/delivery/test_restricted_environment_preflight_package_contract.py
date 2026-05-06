from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
RUNBOOK = ROOT / "ops" / "delivery" / "runbooks" / "restricted_environment_preflight_package.md"


def _module():
    from scripts.ops import restricted_environment_preflight_check as module

    return module


def test_report_type_and_selected_scenario_constants_are_locked() -> None:
    module = _module()

    assert module.REPORT_TYPE == "restricted_environment_preflight_check_v1"
    assert module.SELECTED_SCENARIO == "repo_environment_preflight_package"
    assert module.PRODUCTION_AUTHORIZATION_STATUS == "not_authorized"
    assert module.ROLLOUT_STAGE == "environment_preflight_planning_only"


def test_recommended_next_state_is_environment_inventory_review_only() -> None:
    module = _module()

    assert module.RECOMMENDED_NEXT_STATE == "ready_for_operator_environment_inventory_review"
    assert module.RECOMMENDED_NEXT_STATE != "production_activation"
    assert "production" not in module.RECOMMENDED_NEXT_STATE


def test_forbidden_side_effect_booleans_are_false_in_passing_report(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "ready.md"
    asset_path.write_text("ready\n", encoding="utf-8")
    manifest = (module.RequiredAsset("environment_preflight_package_present", "ready.md"),)

    result = module.generate_report(tmp_path, asset_manifest=manifest)
    report = result.report

    assert result.exit_code == 0
    assert report["checks_failed"] == []
    assert report["failures"] == []
    assert report["runtime_worker_started"] is False
    assert report["external_network_used"] is False
    assert report["database_connected"] is False
    assert report["redis_connected"] is False
    assert report["database_mutated"] is False
    assert report["redis_mutated"] is False
    assert report["env_mutated"] is False
    assert report["feature_flags_mutated"] is False
    assert report["recommended_flag_patch_applied"] is False
    assert report["secret_values_read"] is False
    assert report["secret_values_printed"] is False
    assert report["secret_file_contents_read"] is False
    assert report["systemd_or_compose_invoked"] is False
    assert report["live_collector_started"] is False
    assert report["live_notifier_transport_used"] is False


def test_required_env_name_inventory_contains_required_groups_and_names() -> None:
    module = _module()

    assert module.REQUIRED_ENV_NAMES == {
        "common": ["APP_ENV", "DATABASE_URL", "REDIS_URL"],
        "collector": ["TELEGRAM_API_ID", "TELEGRAM_API_HASH_FILE", "TELEGRAM_PHONE_NUMBER", "TDLIB_STATE_DIR"],
        "notifier": [
            "TELEGRAM_BOT_TOKEN_FILE",
            "TELEGRAM_OPERATOR_CHAT_ID",
            "ENABLE_NOTIFICATION_SEND",
            "NOTIFIER_TELEGRAM_DRY_RUN",
        ],
        "maintenance_delivery": ["MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION"],
        "judge_openai": ["OPENAI_API_KEY_FILE"],
        "github": ["GITHUB_APP_ID", "GITHUB_INSTALLATION_ID", "GITHUB_PRIVATE_KEY_FILE"],
        "optional_future": ["X_BEARER_TOKEN_FILE"],
    }


def test_default_env_presence_mode_is_not_checked(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "ready.md"
    asset_path.write_text("ready\n", encoding="utf-8")
    manifest = (module.RequiredAsset("environment_preflight_package_present", "ready.md"),)

    result = module.generate_report(tmp_path, asset_manifest=manifest)
    report = result.report

    assert result.exit_code == 0
    assert report["env_presence_mode"] == "not_checked"
    assert report["env_presence"] == {
        "checked": False,
        "present": [],
        "missing": [],
        "not_checked_reason": "pass --check-env-presence to inspect variable-name presence only",
    }


def test_check_env_presence_checks_presence_by_name_only(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "ready.md"
    asset_path.write_text("ready\n", encoding="utf-8")
    manifest = (module.RequiredAsset("environment_preflight_package_present", "ready.md"),)
    environ = {
        "APP_ENV": "prod-secret-like-value",
        "DATABASE_URL": "postgres://should-not-render",
        "TELEGRAM_BOT_TOKEN_FILE": "/sensitive/host/path/token",
    }

    result = module.generate_report(
        tmp_path,
        check_env_presence=True,
        environ=environ,
        asset_manifest=manifest,
    )
    report = result.report

    assert report["env_presence_mode"] == "checked"
    assert report["env_presence"]["checked"] is True
    assert report["env_presence"]["presence_by_group"]["common"]["APP_ENV"] is True
    assert report["env_presence"]["presence_by_group"]["common"]["REDIS_URL"] is False
    assert "DATABASE_URL" in report["env_presence"]["present"]
    assert "REDIS_URL" in report["env_presence"]["missing"]
    rendered = json.dumps(report)
    assert "prod-secret-like-value" not in rendered
    assert "postgres://should-not-render" not in rendered
    assert "/sensitive/host/path/token" not in rendered


def test_missing_preflight_package_asset_fails(tmp_path) -> None:
    module = _module()
    manifest = (
        module.RequiredAsset(
            "environment_preflight_package_present",
            "missing.md",
            (module.MarkerRequirement("required_marker", ("required marker",)),),
        ),
    )

    result = module.generate_report(tmp_path, asset_manifest=manifest)

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["asset.exists:missing.md"]
    asset_report = result.report["required_assets"]["environment_preflight_package_present:missing.md"]
    assert asset_report["exists"] is False
    assert asset_report["markers_passed"] is False


def test_missing_marker_fails(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "package.md"
    asset_path.write_text("different text\n", encoding="utf-8")
    manifest = (
        module.RequiredAsset(
            "environment_preflight_package_present",
            "package.md",
            (module.MarkerRequirement("required_marker", ("required marker",)),),
        ),
    )

    result = module.generate_report(tmp_path, asset_manifest=manifest)

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["asset.markers:package.md"]
    asset_report = result.report["required_assets"]["environment_preflight_package_present:package.md"]
    assert asset_report["exists"] is True
    assert asset_report["markers_passed"] is False
    assert asset_report["missing_markers"] == ["required_marker"]


def test_generic_secret_wording_does_not_satisfy_secret_values_not_printed_marker(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "package.md"
    asset_path.write_text("secret inventory\n", encoding="utf-8")
    manifest = (
        module.RequiredAsset(
            "environment_preflight_package_present",
            "package.md",
            (
                module.MarkerRequirement(
                    "secret_values_not_printed",
                    ("this does not print secret values",),
                ),
            ),
        ),
    )

    result = module.generate_report(tmp_path, asset_manifest=manifest)

    assert result.exit_code == 1
    asset_report = result.report["required_assets"]["environment_preflight_package_present:package.md"]
    assert asset_report["missing_markers"] == ["secret_values_not_printed"]


def test_generic_database_wording_does_not_satisfy_no_db_connection_marker(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "package.md"
    asset_path.write_text("database inventory\n", encoding="utf-8")
    manifest = (
        module.RequiredAsset(
            "environment_preflight_package_present",
            "package.md",
            (
                module.MarkerRequirement(
                    "db_redis_non_connection_boundary",
                    ("this does not connect to db or redis",),
                ),
            ),
        ),
    )

    result = module.generate_report(tmp_path, asset_manifest=manifest)

    assert result.exit_code == 1
    asset_report = result.report["required_assets"]["environment_preflight_package_present:package.md"]
    assert asset_report["missing_markers"] == ["db_redis_non_connection_boundary"]


def test_runbook_contains_hard_non_goals() -> None:
    text = RUNBOOK.read_text(encoding="utf-8").lower()

    assert "this does not authorize production rollout" in text
    assert "this does not apply recommended flag patches" in text
    assert "this does not mutate env files" in text
    assert "this does not mutate feature flags" in text
    assert "this does not connect to db or redis" in text
    assert "this does not call external apis" in text
    assert "this does not start runtime workers" in text
    assert "this does not start the telegram collector" in text
    assert "this does not start telegram notifier transport" in text
    assert "this does not run docker compose or systemd" in text
    assert "this does not read secret file contents" in text
    assert "this does not print secret values" in text
    assert "this does not print secret file contents" in text
    assert "environment checks are presence-only" in text
    assert "real secret values must never be pasted into reports or logs" in text
    assert "production must have exactly one live telegram collector instance" in text
    assert "actual flag changes require explicit operator approval" in text
    assert "`recommended_flag_patch` is output-only" in text
    assert "restricted transport smoke requires separate explicit approval" in text
    assert "passing this package means only `ready_for_operator_environment_inventory_review`" in text


def test_current_repo_root_report_passes() -> None:
    module = _module()

    result = module.generate_report(ROOT)

    assert result.exit_code == 0
    assert result.report["checks_failed"] == []
    assert result.report["readiness_summary"] == {
        "environment_preflight_package_present": True,
        "slice17_planning_package_present": True,
        "slice16_readiness_smoke_present": True,
        "secret_value_redaction_boundary_present": True,
        "one_live_collector_boundary_present": True,
        "feature_flag_manual_apply_boundary_present": True,
    }


def test_render_json_is_deterministic_json(tmp_path) -> None:
    module = _module()
    asset_path = tmp_path / "ready.md"
    asset_path.write_text("ready\n", encoding="utf-8")

    result = module.generate_report(
        tmp_path,
        asset_manifest=(module.RequiredAsset("environment_preflight_package_present", "ready.md"),),
    )
    rendered = module.render_json(result.report)

    assert json.loads(rendered)["report_type"] == "restricted_environment_preflight_check_v1"
