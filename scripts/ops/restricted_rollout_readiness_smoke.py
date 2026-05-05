from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence


REPORT_TYPE = "restricted_rollout_readiness_smoke_v1"
SELECTED_SCENARIO = "repo_control_plane_readiness"
PRODUCTION_AUTHORIZATION_STATUS = "not_authorized"
ROLLOUT_STAGE = "pre_restricted_rollout_planning"
RECOMMENDED_NEXT_STATE = "ready_for_separately_approved_restricted_rollout_planning"
REQUIRED_FLAG_NAMES = [
    "ENABLE_NOTIFICATION_SEND",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
    "NOTIFIER_TELEGRAM_DRY_RUN",
]
NOTES = [
    "Success does not authorize production rollout.",
    (
        "Operator approval, secrets review, environment plan, one-live-collector plan, "
        "rollback plan, and restricted production transport smoke remain separate future steps."
    ),
]
FORBIDDEN_SIDE_EFFECT_FIELDS = {
    "runtime_worker_started": False,
    "external_network_used": False,
    "database_required": False,
    "redis_required": False,
    "database_mutated": False,
    "redis_mutated": False,
    "env_mutated": False,
    "feature_flags_mutated": False,
    "recommended_flag_patch_applied": False,
    "production_db_or_redis_used": False,
    "live_collector_started": False,
    "live_notifier_transport_used": False,
}


@dataclass(frozen=True, slots=True)
class MarkerRequirement:
    name: str
    any_of: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RequiredAsset:
    category: str
    path: str
    marker_requirements: tuple[MarkerRequirement, ...] = ()


@dataclass(frozen=True, slots=True)
class AssetResult:
    category: str
    path: str
    exists: bool
    markers_passed: bool
    missing_markers: list[str]


@dataclass(frozen=True, slots=True)
class ReadinessResult:
    exit_code: int
    report: dict[str, Any]


def _marker(name: str, *any_of: str) -> MarkerRequirement:
    return MarkerRequirement(name=name, any_of=tuple(item.lower() for item in any_of))


def _flag_marker(flag_name: str) -> MarkerRequirement:
    return _marker(flag_name, flag_name)


FLAG_MARKERS = tuple(_flag_marker(flag_name) for flag_name in REQUIRED_FLAG_NAMES)
DELIVERY_GATE_RUNBOOK_MARKERS = (
    _marker(
        "production_rollout_not_authorized",
        "does not authorize production rollout",
        "does not authorize a live rollout",
        "not a production rollout approval",
        "not a production rollout",
    ),
    _marker(
        "env_or_feature_flags_not_mutated",
        "does not mutate feature flags",
        "no feature flags or env files are mutated",
        "does not mutate env files",
        "does not write `.env` files",
    ),
    _marker(
        "recommended_flag_patch_output_only",
        "recommended_flag_patch is output-only",
        "does not auto-apply `recommended_flag_patch`",
        "does not apply recommended flag patches",
    ),
)
BATCH_RECOVERY_RUNBOOK_MARKERS = (
    _marker("production_rollout_not_authorized", "does not authorize production rollout", "not a production rollout"),
    _marker("env_or_feature_flags_not_mutated", "no feature flags or env files are mutated"),
    _marker("control_plane_action", "control-plane path", "retry-selected-due", "replay-selected"),
)
HANDOFF_MARKERS = (
    _marker("operator_controlled_handoff", "operator-controlled", "operator manually applies", "approve full rollout manually"),
    _marker("recommended_flags_not_auto_applied", "does not implement feature flag auto-apply", "does not apply flags automatically"),
)
ROLLBACK_MARKERS = (
    _marker("rollback_disables_enable_notification_send", "enable_notification_send=false"),
    _marker(
        "rollback_disables_maintenance_retry_promotion",
        "maintenance_enable_notification_retry_promotion=false",
    ),
)
NOTIFIER_ACCEPTANCE_MARKERS = (
    _marker("enable_notification_send_required_for_delivery", "enable_notification_send=true"),
    _marker("operator_approval_required", "explicit operator approval"),
)


REQUIRED_ASSET_MANIFEST: tuple[RequiredAsset, ...] = (
    RequiredAsset("delivery_gate_assets", "scripts/ops/delivery_gate_runtime_smoke.py", FLAG_MARKERS),
    RequiredAsset("delivery_gate_assets", "scripts/ops/delivery_gate_full_runtime_smoke.py", FLAG_MARKERS),
    RequiredAsset("delivery_gate_assets", "ops/pipeline/runbooks/delivery_gate_runtime_smoke.md", DELIVERY_GATE_RUNBOOK_MARKERS),
    RequiredAsset("delivery_gate_assets", "ops/pipeline/runbooks/delivery_gate_full_runtime_smoke.md", DELIVERY_GATE_RUNBOOK_MARKERS),
    RequiredAsset("delivery_gate_assets", "src/services/maintenance/delivery_gate_runner.py"),
    RequiredAsset("delivery_gate_assets", "src/services/maintenance/main.py"),
    RequiredAsset("delivery_gate_assets", "tests/unit/ops/pipeline/test_delivery_gate_runtime_smoke_contract.py"),
    RequiredAsset("delivery_gate_assets", "tests/unit/ops/pipeline/test_delivery_gate_full_runtime_smoke_contract.py"),
    RequiredAsset("delivery_gate_assets", "tests/integration/pipeline/test_delivery_gate_runtime_smoke_imports.py"),
    RequiredAsset("delivery_gate_assets", "tests/integration/pipeline/test_delivery_gate_full_runtime_smoke_imports.py"),
    RequiredAsset("batch_recovery_assets", "scripts/ops/batch_recovery_runtime_smoke.py"),
    RequiredAsset("batch_recovery_assets", "scripts/ops/batch_recovery_replay_runtime_smoke.py"),
    RequiredAsset("batch_recovery_assets", "ops/pipeline/runbooks/batch_recovery_runtime_smoke.md", BATCH_RECOVERY_RUNBOOK_MARKERS),
    RequiredAsset("batch_recovery_assets", "ops/pipeline/runbooks/batch_recovery_replay_runtime_smoke.md", BATCH_RECOVERY_RUNBOOK_MARKERS),
    RequiredAsset("batch_recovery_assets", "src/services/maintenance/batch_recovery_tool.py"),
    RequiredAsset("batch_recovery_assets", "tests/unit/ops/pipeline/test_batch_recovery_runtime_smoke_contract.py"),
    RequiredAsset("batch_recovery_assets", "tests/unit/ops/pipeline/test_batch_recovery_replay_runtime_smoke_contract.py"),
    RequiredAsset("batch_recovery_assets", "tests/integration/pipeline/test_batch_recovery_runtime_smoke_imports.py"),
    RequiredAsset("batch_recovery_assets", "tests/integration/pipeline/test_batch_recovery_replay_runtime_smoke_imports.py"),
    RequiredAsset("maintenance_assets", "scripts/ops/maintenance_runtime_smoke.py", (_flag_marker("MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION"),)),
    RequiredAsset("maintenance_assets", "ops/pipeline/runbooks/maintenance_runtime_smoke.md", (_flag_marker("MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION"),)),
    RequiredAsset("maintenance_assets", "src/services/maintenance/config.py", FLAG_MARKERS),
    RequiredAsset("notifier_safety_assets", "scripts/ops/notifier_telegram_runtime_smoke.py", (_flag_marker("ENABLE_NOTIFICATION_SEND"), _flag_marker("NOTIFIER_TELEGRAM_DRY_RUN"))),
    RequiredAsset("notifier_safety_assets", "ops/pipeline/runbooks/notifier_telegram_runtime_smoke.md", (_flag_marker("ENABLE_NOTIFICATION_SEND"), _flag_marker("NOTIFIER_TELEGRAM_DRY_RUN"))),
    RequiredAsset("notifier_safety_assets", "src/services/notifier_telegram/config.py", (_flag_marker("ENABLE_NOTIFICATION_SEND"), _flag_marker("NOTIFIER_TELEGRAM_DRY_RUN"))),
    RequiredAsset("notifier_safety_assets", "src/services/policy_engine/config.py", (_flag_marker("ENABLE_NOTIFICATION_SEND"),)),
    RequiredAsset("handoff_assets", "ops/delivery/runbooks/delivery_gate_handoff.md", HANDOFF_MARKERS + ROLLBACK_MARKERS),
    RequiredAsset("handoff_assets", "ops/delivery/runbooks/maintenance_cli_invocation.md"),
    RequiredAsset("handoff_assets", "ops/delivery/runbooks/db_backed_acceptance_smoke.md"),
    RequiredAsset("handoff_assets", "ops/delivery/sql/delivery_rollout_gate_queries.sql"),
    RequiredAsset("handoff_assets", "ops/delivery/dashboards/delivery_minimal_dashboard.md"),
    RequiredAsset("handoff_assets", "ops/delivery/alerts/delivery_minimal_alerts.yaml"),
    RequiredAsset("rollback_assets", "ops/runbooks/notifier_rollback.md", ROLLBACK_MARKERS),
    RequiredAsset("rollback_assets", "ops/runbooks/notifier_acceptance_checklist.md", NOTIFIER_ACCEPTANCE_MARKERS),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect local repository control-plane readiness assets for separately "
            "approved restricted rollout planning. Prints JSON only."
        )
    )
    parser.add_argument(
        "--format",
        choices=("json",),
        default="json",
        help="Output format. Only json is supported.",
    )
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root to inspect. Defaults to this script's repository root.",
    )
    return parser


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_lower(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace").lower()


def evaluate_asset(repo_root: Path, asset: RequiredAsset) -> AssetResult:
    asset_path = repo_root / asset.path
    exists = asset_path.is_file()
    missing_markers: list[str] = []
    if exists and asset.marker_requirements:
        text = _read_lower(asset_path)
        missing_markers = [
            requirement.name
            for requirement in asset.marker_requirements
            if not any(marker in text for marker in requirement.any_of)
        ]
    elif not exists:
        missing_markers = [requirement.name for requirement in asset.marker_requirements]

    return AssetResult(
        category=asset.category,
        path=asset.path,
        exists=exists,
        markers_passed=not missing_markers,
        missing_markers=missing_markers,
    )


def _asset_key(asset: RequiredAsset) -> str:
    return f"{asset.category}:{asset.path}"


def _category_passed(results: Sequence[AssetResult], category: str) -> bool:
    category_results = [result for result in results if result.category == category]
    return bool(category_results) and all(result.exists and result.markers_passed for result in category_results)


def generate_report(
    repo_root: str | Path | None = None,
    *,
    asset_manifest: Sequence[RequiredAsset] = REQUIRED_ASSET_MANIFEST,
) -> ReadinessResult:
    resolved_repo_root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    asset_results = [evaluate_asset(resolved_repo_root, asset) for asset in asset_manifest]
    checks_failed: list[str] = []
    failures: list[dict[str, str]] = []

    for result in asset_results:
        if not result.exists:
            check_name = f"asset.exists:{result.path}"
            checks_failed.append(check_name)
            failures.append(
                {
                    "check": check_name,
                    "category": result.category,
                    "path": result.path,
                    "message": "Required control-plane readiness asset is missing.",
                }
            )
        if result.exists and not result.markers_passed:
            check_name = f"asset.markers:{result.path}"
            checks_failed.append(check_name)
            failures.append(
                {
                    "check": check_name,
                    "category": result.category,
                    "path": result.path,
                    "message": "Required control-plane readiness markers are missing.",
                    "missing_markers": ", ".join(result.missing_markers),
                }
            )

    required_assets = {
        _asset_key(asset): asdict(result)
        for asset, result in zip(asset_manifest, asset_results, strict=True)
    }
    readiness_summary = {
        "delivery_gate_assets_present": _category_passed(asset_results, "delivery_gate_assets"),
        "batch_recovery_assets_present": _category_passed(asset_results, "batch_recovery_assets"),
        "maintenance_assets_present": _category_passed(asset_results, "maintenance_assets"),
        "notifier_safety_assets_present": _category_passed(asset_results, "notifier_safety_assets"),
        "rollback_assets_present": _category_passed(asset_results, "rollback_assets"),
        "handoff_assets_present": _category_passed(asset_results, "handoff_assets"),
    }

    report: dict[str, Any] = {
        "report_type": REPORT_TYPE,
        "selected_scenario": SELECTED_SCENARIO,
        "checks_failed": checks_failed,
        "failures": failures,
        "production_authorization_status": PRODUCTION_AUTHORIZATION_STATUS,
        "rollout_stage": ROLLOUT_STAGE,
        **FORBIDDEN_SIDE_EFFECT_FIELDS,
        "required_flag_names": list(REQUIRED_FLAG_NAMES),
        "required_assets": required_assets,
        "readiness_summary": readiness_summary,
        "recommended_next_state": RECOMMENDED_NEXT_STATE,
        "notes": list(NOTES),
    }
    return ReadinessResult(exit_code=1 if checks_failed or failures else 0, report=report)


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(args.repo_root)
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
