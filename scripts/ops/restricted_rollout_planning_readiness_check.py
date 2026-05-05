from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence


REPORT_TYPE = "restricted_rollout_planning_readiness_check_v1"
SELECTED_SCENARIO = "repo_restricted_rollout_planning_package"
PRODUCTION_AUTHORIZATION_STATUS = "not_authorized"
ROLLOUT_STAGE = "restricted_rollout_planning_only"
RECOMMENDED_NEXT_STATE = "ready_for_operator_reviewed_restricted_rollout_plan"
PLANNING_PACKAGE_PATH = "ops/delivery/runbooks/restricted_rollout_planning_package.md"
REQUIRED_MANUAL_APPROVAL_DOMAINS = [
    "operator_approval",
    "secrets_inventory_review",
    "environment_plan_review",
    "one_live_collector_plan",
    "rollback_plan_review",
    "restricted_transport_smoke_plan",
]
REQUIRED_ROLLOUT_PHASES = [
    "offline_validation",
    "live_ingest",
    "shadow_analysis",
    "silent_delivery",
    "restricted_rollout",
    "full_go_live",
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
    "secret_values_read": False,
    "production_db_or_redis_used": False,
    "live_collector_started": False,
    "live_notifier_transport_used": False,
    "systemd_or_compose_invoked": False,
}
NOTES = [
    "Success does not authorize production rollout.",
    "This check verifies repository planning assets only.",
    "Real secrets, real infrastructure, and real transport remain separate future approvals.",
]


@dataclass(frozen=True, slots=True)
class MarkerRequirement:
    name: str
    any_of: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RequiredAsset:
    summary_key: str
    path: str
    marker_requirements: tuple[MarkerRequirement, ...] = ()


@dataclass(frozen=True, slots=True)
class AssetResult:
    summary_key: str
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


PLANNING_PACKAGE_MARKERS = (
    _marker(
        "production_rollout_not_authorized",
        "this does not authorize production rollout",
        "production rollout remains unauthorized",
        "production rollout is not authorized",
    ),
    _marker(
        "recommended_flag_patch_output_only",
        "`recommended_flag_patch` is output-only",
        "recommended_flag_patch is output-only",
    ),
    _marker("env_mutation_forbidden", "this does not mutate env files"),
    _marker("feature_flag_mutation_forbidden", "this does not mutate feature flags"),
    _marker(
        "secret_values_not_read_or_printed",
        "this does not read or print secret values",
        "secret values must be reviewed by presence/ownership only, never printed",
    ),
    _marker("db_redis_not_required", "this does not require db or redis"),
    _marker("external_api_not_called", "this does not call external apis"),
    _marker("runtime_workers_not_started", "this does not start runtime workers"),
    _marker(
        "one_live_telegram_collector_invariant",
        "production must have exactly one live telegram collector instance",
    ),
    _marker(
        "rollout_phase_order",
        "offline validation -> live ingest -> shadow analysis -> silent delivery -> restricted rollout -> full go-live",
    ),
    _marker("rollback_disables_notification_send", "rollback must disable `enable_notification_send=false`"),
    _marker(
        "rollback_disables_maintenance_retry_promotion",
        "rollback must disable `maintenance_enable_notification_retry_promotion=false`",
    ),
    _marker(
        "restricted_transport_smoke_separate_approval",
        "restricted transport smoke requires separate explicit approval",
    ),
    _marker("manual_operator_approval_required", "actual flag changes require explicit operator approval"),
)

REQUIRED_ASSET_MANIFEST: tuple[RequiredAsset, ...] = (
    RequiredAsset("planning_package_present", PLANNING_PACKAGE_PATH, PLANNING_PACKAGE_MARKERS),
    RequiredAsset("slice16_readiness_smoke_present", "scripts/ops/restricted_rollout_readiness_smoke.py"),
    RequiredAsset("slice16_readiness_smoke_present", "ops/pipeline/runbooks/restricted_rollout_readiness_smoke.md"),
    RequiredAsset("delivery_gate_handoff_present", "ops/delivery/runbooks/delivery_gate_handoff.md"),
    RequiredAsset("rollback_runbook_present", "ops/runbooks/notifier_rollback.md"),
    RequiredAsset("db_acceptance_smoke_runbook_present", "ops/delivery/runbooks/db_backed_acceptance_smoke.md"),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect local repository planning assets for separately approved restricted rollout planning. "
            "Prints JSON only."
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
        summary_key=asset.summary_key,
        path=asset.path,
        exists=exists,
        markers_passed=not missing_markers,
        missing_markers=missing_markers,
    )


def _asset_key(asset: RequiredAsset) -> str:
    return f"{asset.summary_key}:{asset.path}"


def _summary_asset_passed(results: Sequence[AssetResult], summary_key: str) -> bool:
    matching_results = [result for result in results if result.summary_key == summary_key]
    return bool(matching_results) and all(result.exists and result.markers_passed for result in matching_results)


def _planning_marker_present(marker_name: str, results: Sequence[AssetResult]) -> bool:
    planning_result = next((result for result in results if result.path == PLANNING_PACKAGE_PATH), None)
    return planning_result is not None and planning_result.exists and marker_name not in planning_result.missing_markers


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
                    "path": result.path,
                    "message": "Required restricted rollout planning asset is missing.",
                }
            )
        if result.exists and not result.markers_passed:
            check_name = f"asset.markers:{result.path}"
            checks_failed.append(check_name)
            failures.append(
                {
                    "check": check_name,
                    "path": result.path,
                    "message": "Required restricted rollout planning markers are missing.",
                    "missing_markers": ", ".join(result.missing_markers),
                }
            )

    required_assets = {
        _asset_key(asset): asdict(result)
        for asset, result in zip(asset_manifest, asset_results, strict=True)
    }
    readiness_summary = {
        "planning_package_present": _summary_asset_passed(asset_results, "planning_package_present"),
        "slice16_readiness_smoke_present": _summary_asset_passed(asset_results, "slice16_readiness_smoke_present"),
        "delivery_gate_handoff_present": _summary_asset_passed(asset_results, "delivery_gate_handoff_present"),
        "rollback_runbook_present": _summary_asset_passed(asset_results, "rollback_runbook_present"),
        "db_acceptance_smoke_runbook_present": _summary_asset_passed(
            asset_results,
            "db_acceptance_smoke_runbook_present",
        ),
        "one_live_collector_invariant_present": _planning_marker_present(
            "one_live_telegram_collector_invariant",
            asset_results,
        ),
        "feature_flag_output_only_boundary_present": _planning_marker_present(
            "recommended_flag_patch_output_only",
            asset_results,
        ),
        "manual_approval_boundary_present": _planning_marker_present(
            "manual_operator_approval_required",
            asset_results,
        ),
    }

    report: dict[str, Any] = {
        "report_type": REPORT_TYPE,
        "selected_scenario": SELECTED_SCENARIO,
        "checks_failed": checks_failed,
        "failures": failures,
        "production_authorization_status": PRODUCTION_AUTHORIZATION_STATUS,
        "rollout_stage": ROLLOUT_STAGE,
        **FORBIDDEN_SIDE_EFFECT_FIELDS,
        "required_manual_approval_domains": list(REQUIRED_MANUAL_APPROVAL_DOMAINS),
        "required_rollout_phases": list(REQUIRED_ROLLOUT_PHASES),
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
