from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


REPORT_TYPE = "restricted_environment_preflight_check_v1"
SELECTED_SCENARIO = "repo_environment_preflight_package"
PRODUCTION_AUTHORIZATION_STATUS = "not_authorized"
ROLLOUT_STAGE = "environment_preflight_planning_only"
RECOMMENDED_NEXT_STATE = "ready_for_operator_environment_inventory_review"
PREFLIGHT_PACKAGE_PATH = "ops/delivery/runbooks/restricted_environment_preflight_package.md"
ENV_PRESENCE_NOT_CHECKED_REASON = "pass --check-env-presence to inspect variable-name presence only"
NOTES = [
    "Success does not authorize production rollout.",
    "This check does not validate secret values or infrastructure connectivity.",
    "Use --check-env-presence only for variable-name presence checks; values are never printed.",
]

REQUIRED_ENV_NAMES: dict[str, list[str]] = {
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

FORBIDDEN_SIDE_EFFECT_FIELDS = {
    "runtime_worker_started": False,
    "external_network_used": False,
    "database_connected": False,
    "redis_connected": False,
    "database_mutated": False,
    "redis_mutated": False,
    "env_mutated": False,
    "feature_flags_mutated": False,
    "recommended_flag_patch_applied": False,
    "secret_values_read": False,
    "secret_values_printed": False,
    "secret_file_contents_read": False,
    "systemd_or_compose_invoked": False,
    "live_collector_started": False,
    "live_notifier_transport_used": False,
}


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
class PreflightResult:
    exit_code: int
    report: dict[str, Any]


def _marker(name: str, *any_of: str) -> MarkerRequirement:
    return MarkerRequirement(name=name, any_of=tuple(item.lower() for item in any_of))


PREFLIGHT_PACKAGE_MARKERS = (
    _marker("production_rollout_not_authorized", "this does not authorize production rollout"),
    _marker("recommended_flag_patch_output_only", "`recommended_flag_patch` is output-only"),
    _marker("env_mutation_forbidden", "this does not mutate env files"),
    _marker("feature_flag_mutation_forbidden", "this does not mutate feature flags"),
    _marker("db_redis_non_connection_boundary", "this does not connect to db or redis"),
    _marker("external_api_not_called", "this does not call external apis"),
    _marker("runtime_workers_not_started", "this does not start runtime workers"),
    _marker("collector_not_started", "this does not start the telegram collector"),
    _marker("notifier_transport_not_started", "this does not start telegram notifier transport"),
    _marker("docker_systemd_not_invoked", "this does not run docker compose or systemd"),
    _marker("secret_file_contents_not_read", "this does not read secret file contents"),
    _marker("secret_values_not_printed", "this does not print secret values"),
    _marker("env_checks_presence_only", "environment checks are presence-only"),
    _marker(
        "one_live_telegram_collector_invariant",
        "production must have exactly one live telegram collector instance",
    ),
    _marker("manual_flag_changes_require_approval", "actual flag changes require explicit operator approval"),
    _marker(
        "restricted_transport_smoke_separate_approval",
        "restricted transport smoke requires separate explicit approval",
    ),
    _marker(
        "next_state_environment_inventory_review_only",
        "passing this package means only `ready_for_operator_environment_inventory_review`",
    ),
)

REQUIRED_ASSET_MANIFEST: tuple[RequiredAsset, ...] = (
    RequiredAsset("environment_preflight_package_present", PREFLIGHT_PACKAGE_PATH, PREFLIGHT_PACKAGE_MARKERS),
    RequiredAsset(
        "slice17_planning_package_present",
        "ops/delivery/runbooks/restricted_rollout_planning_package.md",
    ),
    RequiredAsset(
        "slice17_planning_package_present",
        "scripts/ops/restricted_rollout_planning_readiness_check.py",
    ),
    RequiredAsset("slice16_readiness_smoke_present", "scripts/ops/restricted_rollout_readiness_smoke.py"),
    RequiredAsset("slice16_readiness_smoke_present", "ops/pipeline/runbooks/restricted_rollout_readiness_smoke.md"),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect local repository environment preflight planning assets. "
            "Optionally checks environment variable-name presence only. Prints JSON only."
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
    parser.add_argument(
        "--check-env-presence",
        action="store_true",
        help="Check required environment variable-name presence only. Values are never read or printed.",
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


def _preflight_marker_present(marker_name: str, results: Sequence[AssetResult]) -> bool:
    preflight_result = next((result for result in results if result.path == PREFLIGHT_PACKAGE_PATH), None)
    return preflight_result is not None and preflight_result.exists and marker_name not in preflight_result.missing_markers


def _all_env_names() -> list[str]:
    return [name for names in REQUIRED_ENV_NAMES.values() for name in names]


def _required_env_names_for_failure() -> set[str]:
    return {
        name
        for group, names in REQUIRED_ENV_NAMES.items()
        if group != "optional_future"
        for name in names
    }


def evaluate_env_presence(
    *,
    check_env_presence: bool,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    if not check_env_presence:
        return {
            "checked": False,
            "present": [],
            "missing": [],
            "not_checked_reason": ENV_PRESENCE_NOT_CHECKED_REASON,
        }

    source = os.environ if environ is None else environ
    present_by_group = {
        group: {name: name in source for name in names}
        for group, names in REQUIRED_ENV_NAMES.items()
    }
    present = [name for name in _all_env_names() if name in source]
    missing = [name for name in _all_env_names() if name not in source]
    missing_required = [name for name in missing if name in _required_env_names_for_failure()]

    return {
        "checked": True,
        "present": present,
        "missing": missing,
        "missing_required": missing_required,
        "presence_by_group": present_by_group,
    }


def generate_report(
    repo_root: str | Path | None = None,
    *,
    check_env_presence: bool = False,
    environ: Mapping[str, str] | None = None,
    asset_manifest: Sequence[RequiredAsset] = REQUIRED_ASSET_MANIFEST,
) -> PreflightResult:
    resolved_repo_root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    asset_results = [evaluate_asset(resolved_repo_root, asset) for asset in asset_manifest]
    env_presence = evaluate_env_presence(check_env_presence=check_env_presence, environ=environ)
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
                    "message": "Required restricted environment preflight planning asset is missing.",
                }
            )
        if result.exists and not result.markers_passed:
            check_name = f"asset.markers:{result.path}"
            checks_failed.append(check_name)
            failures.append(
                {
                    "check": check_name,
                    "path": result.path,
                    "message": "Required restricted environment preflight markers are missing.",
                    "missing_markers": ", ".join(result.missing_markers),
                }
            )

    missing_required_env = env_presence.get("missing_required", [])
    if check_env_presence and missing_required_env:
        check_name = "env_presence.required_names_present"
        checks_failed.append(check_name)
        failures.append(
            {
                "check": check_name,
                "message": "Required environment variable names are missing.",
                "missing_env_names": ", ".join(missing_required_env),
            }
        )

    required_assets = {
        _asset_key(asset): asdict(result)
        for asset, result in zip(asset_manifest, asset_results, strict=True)
    }
    readiness_summary = {
        "environment_preflight_package_present": _summary_asset_passed(
            asset_results,
            "environment_preflight_package_present",
        ),
        "slice17_planning_package_present": _summary_asset_passed(
            asset_results,
            "slice17_planning_package_present",
        ),
        "slice16_readiness_smoke_present": _summary_asset_passed(
            asset_results,
            "slice16_readiness_smoke_present",
        ),
        "secret_value_redaction_boundary_present": _preflight_marker_present(
            "secret_values_not_printed",
            asset_results,
        )
        and _preflight_marker_present("secret_file_contents_not_read", asset_results),
        "one_live_collector_boundary_present": _preflight_marker_present(
            "one_live_telegram_collector_invariant",
            asset_results,
        ),
        "feature_flag_manual_apply_boundary_present": _preflight_marker_present(
            "manual_flag_changes_require_approval",
            asset_results,
        )
        and _preflight_marker_present("recommended_flag_patch_output_only", asset_results),
    }

    report: dict[str, Any] = {
        "report_type": REPORT_TYPE,
        "selected_scenario": SELECTED_SCENARIO,
        "checks_failed": checks_failed,
        "failures": failures,
        "production_authorization_status": PRODUCTION_AUTHORIZATION_STATUS,
        "rollout_stage": ROLLOUT_STAGE,
        **FORBIDDEN_SIDE_EFFECT_FIELDS,
        "env_presence_mode": "checked" if check_env_presence else "not_checked",
        "required_env_names": REQUIRED_ENV_NAMES,
        "env_presence": env_presence,
        "required_assets": required_assets,
        "readiness_summary": readiness_summary,
        "recommended_next_state": RECOMMENDED_NEXT_STATE,
        "notes": list(NOTES),
    }
    return PreflightResult(exit_code=1 if checks_failed or failures else 0, report=report)


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(args.repo_root, check_env_presence=args.check_env_presence)
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
