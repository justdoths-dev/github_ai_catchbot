from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


SCHEMA_VERSION = "dedicated_vps_tdjson_source_build_dependency_install_plan_v1"
CONTRACT_NAME = "dedicated_vps_tdjson_source_build_dependency_install_plan"

SUPPORTED_OS_IDS = {"ubuntu", "debian"}
REQUIRED_PACKAGES = ("cmake", "gperf")
OPTIONAL_EVIDENCE_PACKAGES = ("ninja-build", "clang", "libc++-dev", "libc++abi-dev")
DEPENDENCY_PACKAGES = REQUIRED_PACKAGES + OPTIONAL_EVIDENCE_PACKAGES

SAFETY_FLAGS = {
    "installation_attempted": False,
    "package_manager_mutation_attempted": False,
    "apt_update_attempted": False,
    "apt_install_attempted": False,
    "apt_upgrade_attempted": False,
    "apt_remove_attempted": False,
    "package_download_attempted": False,
    "source_build_attempted": False,
    "git_clone_attempted": False,
    "cmake_configure_attempted": False,
    "cmake_build_attempted": False,
    "make_attempted": False,
    "ninja_attempted": False,
    "binary_placement_attempted": False,
    "symlink_created": False,
    "build_directory_created": False,
    "runtime_env_read": False,
    "runtime_env_values_printed": False,
    "secret_values_printed": False,
    "tdlib_auth_attempted": False,
    "tdlib_client_created": False,
    "td_json_client_create_called": False,
    "td_json_client_send_called": False,
    "td_json_client_receive_called": False,
    "td_json_client_destroy_called": False,
    "telegram_network_contact_attempted": False,
    "database_connected": False,
    "redis_connected": False,
    "alembic_run": False,
    "docker_or_systemd_changed": False,
    "live_collector_started": False,
    "app_runtime_started": False,
    "notifier_transport_enabled": False,
    "production_rollout_performed": False,
}

FORBIDDEN_COMMAND_TOKENS = {
    "sudo",
    "curl",
    "wget",
    "cmake",
    "make",
    "ninja",
    "systemctl",
    "service",
    "docker",
}

FORBIDDEN_COMMAND_PHRASES = {
    ("apt", "install"),
    ("apt-get", "install"),
    ("apt", "upgrade"),
    ("apt-get", "upgrade"),
    ("apt", "update"),
    ("apt-get", "update"),
    ("apt", "remove"),
    ("apt-get", "remove"),
    ("apt", "purge"),
    ("apt-get", "purge"),
    ("dpkg", "-i"),
    ("git", "clone"),
    ("docker", "compose"),
}

CommandAvailability = Callable[[str], str | None]
SubprocessRunner = Callable[..., subprocess.CompletedProcess[str]]
MachineProvider = Callable[[], str]
PlatformProvider = Callable[[], str]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Produce a read-only TDLib/tdjson source-build dependency install "
            "plan for later operator review. This command installs nothing, "
            "runs no apt update, downloads nothing, builds nothing, reads no "
            "runtime.env, creates no TDLib client, and contacts no network."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument(
        "--source-build-plan-json",
        default=None,
        help="Optional explicit path to an existing tdjson source-build plan JSON report.",
    )
    parser.add_argument("--preflight-json", default=None)
    parser.add_argument("--package-decision-json", default=None)
    parser.add_argument("--apt-plan-json", default=None)
    parser.add_argument("--manual-evidence-json", default=None)
    return parser


def _parse_os_release(text: str) -> dict[str, str | None]:
    values: dict[str, str | None] = {
        "os_id": None,
        "os_version_id": None,
        "os_pretty_name": None,
    }
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        value = raw_value.strip().strip('"')
        if key == "ID":
            values["os_id"] = value.lower()
        elif key == "VERSION_ID":
            values["os_version_id"] = value
        elif key == "PRETTY_NAME":
            values["os_pretty_name"] = value
    return values


def _read_os_release() -> str:
    try:
        return Path("/etc/os-release").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _validate_allowed_command(argv: Sequence[str]) -> None:
    if not argv:
        raise ValueError("empty subprocess command is not allowed")

    normalized = tuple(str(part).lower() for part in argv)
    allowed = (
        _is_dpkg_query_command(normalized)
        or _is_apt_cache_policy_command(normalized)
        or _is_apt_cache_show_command(normalized)
        or _is_apt_cache_depends_command(normalized)
        or normalized == ("uname", "-m")
    )
    if not allowed:
        raise ValueError(f"subprocess command is not allowlisted: {list(argv)!r}")

    if normalized[0] in FORBIDDEN_COMMAND_TOKENS:
        raise ValueError(f"forbidden subprocess executable: {normalized[0]}")
    for phrase in FORBIDDEN_COMMAND_PHRASES:
        for index in range(0, len(normalized) - len(phrase) + 1):
            if normalized[index : index + len(phrase)] == phrase:
                raise ValueError(f"forbidden subprocess phrase: {' '.join(phrase)}")


def _is_dpkg_query_command(normalized: Sequence[str]) -> bool:
    return (
        len(normalized) == 4
        and normalized[0] == "dpkg-query"
        and normalized[1] == "-w"
        and normalized[2] == "-f=${binary:package}\\t${version}\\t${status}\\n"
        and normalized[3] in DEPENDENCY_PACKAGES
    )


def _is_apt_cache_policy_command(normalized: Sequence[str]) -> bool:
    return (
        len(normalized) == 3
        and normalized[:2] == ("apt-cache", "policy")
        and normalized[2] in DEPENDENCY_PACKAGES
    )


def _is_apt_cache_show_command(normalized: Sequence[str]) -> bool:
    return (
        len(normalized) == 3
        and normalized[:2] == ("apt-cache", "show")
        and normalized[2] in DEPENDENCY_PACKAGES
    )


def _is_apt_cache_depends_command(normalized: Sequence[str]) -> bool:
    return (
        len(normalized) == 3
        and normalized[:2] == ("apt-cache", "depends")
        and normalized[2] in DEPENDENCY_PACKAGES
    )


def _command_result(
    *,
    argv: Sequence[str],
    command_available: CommandAvailability,
    subprocess_runner: SubprocessRunner,
) -> dict[str, Any]:
    _validate_allowed_command(argv)
    executable = argv[0]
    if command_available(executable) is None:
        return {
            "command": list(argv),
            "available": False,
            "returncode": None,
            "stdout": "",
            "stderr": "",
        }

    completed = subprocess_runner(
        list(argv),
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    return {
        "command": list(argv),
        "available": True,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def _command_availability(command_available: CommandAvailability) -> dict[str, bool]:
    return {
        command: command_available(command) is not None
        for command in ("dpkg-query", "apt-cache", "uname", *DEPENDENCY_PACKAGES)
    }


def _extract_apt_candidate(output: str) -> str | None:
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped.lower().startswith("candidate:"):
            continue
        candidate = stripped.split(":", 1)[1].strip()
        if candidate and candidate.lower() != "(none)":
            return candidate
    return None


def _first_paragraph(text: str) -> str:
    return text.split("\n\n", 1)[0]


def _parse_apt_show_summary(output: str) -> dict[str, str | None]:
    fields = {
        "package": None,
        "version": None,
        "source": None,
        "depends": None,
        "description": None,
    }
    current_key: str | None = None
    for raw_line in _first_paragraph(output).splitlines():
        if raw_line.startswith(" ") and current_key in fields and fields[current_key]:
            fields[current_key] = f"{fields[current_key]} {raw_line.strip()}"[:500]
            continue
        if ":" not in raw_line:
            current_key = None
            continue
        key, value = raw_line.split(":", 1)
        normalized_key = key.lower()
        current_key = normalized_key if normalized_key in fields else None
        if current_key in fields:
            fields[current_key] = value.strip()[:500]
    return fields


def _inspect_apt_policy(
    *,
    command_available: CommandAvailability,
    subprocess_runner: SubprocessRunner,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for package in DEPENDENCY_PACKAGES:
        result = _command_result(
            argv=("apt-cache", "policy", package),
            command_available=command_available,
            subprocess_runner=subprocess_runner,
        )
        candidate = _extract_apt_candidate(result["stdout"])
        results.append(
            {
                "package": package,
                "command_available": result["available"],
                "returncode": result["returncode"],
                "candidate": candidate,
                "candidate_visible": candidate is not None,
            }
        )
    return results


def _inspect_apt_show(
    *,
    command_available: CommandAvailability,
    subprocess_runner: SubprocessRunner,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for package in DEPENDENCY_PACKAGES:
        result = _command_result(
            argv=("apt-cache", "show", package),
            command_available=command_available,
            subprocess_runner=subprocess_runner,
        )
        summary = _parse_apt_show_summary(result["stdout"]) if result["returncode"] == 0 else {}
        results.append(
            {
                "package": package,
                "command_available": result["available"],
                "returncode": result["returncode"],
                "summary": summary,
            }
        )
    return results


def _inspect_apt_depends(
    *,
    command_available: CommandAvailability,
    subprocess_runner: SubprocessRunner,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for package in DEPENDENCY_PACKAGES:
        result = _command_result(
            argv=("apt-cache", "depends", package),
            command_available=command_available,
            subprocess_runner=subprocess_runner,
        )
        lines = [line.strip()[:240] for line in result["stdout"].splitlines() if line.strip()]
        results.append(
            {
                "package": package,
                "command_available": result["available"],
                "returncode": result["returncode"],
                "lines": lines[:80],
            }
        )
    return results


def _inspect_installed_packages(
    *,
    command_available: CommandAvailability,
    subprocess_runner: SubprocessRunner,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for package in DEPENDENCY_PACKAGES:
        result = _command_result(
            argv=(
                "dpkg-query",
                "-W",
                "-f=${binary:Package}\\t${Version}\\t${Status}\\n",
                package,
            ),
            command_available=command_available,
            subprocess_runner=subprocess_runner,
        )
        installed = result["returncode"] == 0 and "install ok installed" in result["stdout"]
        fields = result["stdout"].strip().split("\t")
        results.append(
            {
                "package": package,
                "command_available": result["available"],
                "returncode": result["returncode"],
                "installed": installed,
                "version": fields[1] if len(fields) >= 2 and installed else None,
            }
        )
    return results


def _inspect_uname(
    *,
    command_available: CommandAvailability,
    subprocess_runner: SubprocessRunner,
) -> str | None:
    result = _command_result(
        argv=("uname", "-m"),
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    if result["available"] and result["returncode"] == 0:
        return result["stdout"].strip() or None
    return None


def _safe_scalar(value: object) -> str | None:
    if value is None:
        return None
    return str(value)[:120]


def _safe_string_list(value: object, *, allowed: set[str] | None = None) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        if allowed is not None and item not in allowed:
            continue
        result.append(item[:120])
    return result


def _load_json(path: str | None) -> tuple[bool, Mapping[str, Any] | None, str | None]:
    if not path:
        return False, None, None
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return True, None, exc.__class__.__name__
    if not isinstance(data, Mapping):
        return True, None, "not_a_json_object"
    return True, data, None


def _selected_plan_from(data: Mapping[str, Any]) -> Mapping[str, Any]:
    selected_plan = data.get("selected_plan")
    if isinstance(selected_plan, Mapping):
        return selected_plan
    return {}


def _summarize_source_build_plan(
    *,
    path: str | None = None,
    data: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if data is None:
        provided, loaded, error_class = _load_json(path)
        if not provided:
            return {
                "provided": False,
                "contract_status": None,
                "recommended_next_slice": None,
                "missing_required_tools": [],
                "missing_required_packages": [],
            }
        if loaded is None:
            return {
                "provided": True,
                "contract_status": "unreadable_or_invalid",
                "recommended_next_slice": None,
                "missing_required_tools": [],
                "missing_required_packages": [],
                "error_class": error_class,
            }
        data = loaded

    selected_plan = _selected_plan_from(data)
    missing_tools = _safe_string_list(selected_plan.get("missing_required_tools"))
    missing_packages = _safe_string_list(selected_plan.get("missing_required_packages"))
    if not missing_tools:
        missing_tools = _safe_string_list(data.get("missing_required_tools"))
    if not missing_packages:
        missing_packages = _safe_string_list(data.get("missing_required_packages"))
    return {
        "provided": True,
        "contract_status": _safe_scalar(data.get("contract_status")),
        "recommended_next_slice": _safe_scalar(data.get("recommended_next_slice")),
        "missing_required_tools": missing_tools,
        "missing_required_packages": missing_packages,
    }


def _summarize_preflight(
    *,
    path: str | None = None,
    data: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if data is None:
        provided, loaded, error_class = _load_json(path)
        if not provided:
            return {"provided": False, "contract_status": None, "tdjson_available": None}
        if loaded is None:
            return {
                "provided": True,
                "contract_status": "unreadable_or_invalid",
                "tdjson_available": None,
                "error_class": error_class,
            }
        data = loaded

    return {
        "provided": True,
        "contract_status": _safe_scalar(data.get("contract_status")),
        "tdjson_available": data.get("tdjson_available")
        if isinstance(data.get("tdjson_available"), bool)
        else None,
    }


def _summarize_next_slice_input(
    *,
    path: str | None = None,
    data: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if data is None:
        provided, loaded, error_class = _load_json(path)
        if not provided:
            return {
                "provided": False,
                "contract_status": None,
                "recommended_next_slice": None,
            }
        if loaded is None:
            return {
                "provided": True,
                "contract_status": "unreadable_or_invalid",
                "recommended_next_slice": None,
                "error_class": error_class,
            }
        data = loaded

    return {
        "provided": True,
        "contract_status": _safe_scalar(data.get("contract_status")),
        "recommended_next_slice": _safe_scalar(data.get("recommended_next_slice")),
    }


def _by_package(items: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    by_package: dict[str, Mapping[str, Any]] = {}
    for item in items:
        package = item.get("package")
        if isinstance(package, str):
            by_package[package] = item
    return by_package


def _future_validation_commands() -> list[str]:
    return [
        (
            "venv/bin/python scripts/ops/dedicated_vps_tdjson_source_build_plan.py "
            "--format json ..."
        ),
        (
            "venv/bin/python - <<'PY'\n"
            "import json\n"
            "from pathlib import Path\n"
            "data = json.loads(Path('/tmp/dedicated_vps_tdjson_source_build_plan.json').read_text(encoding='utf-8'))\n"
            "assert data['schema_version'] == 'dedicated_vps_tdjson_source_build_plan_v1'\n"
            "assert data['contract_name'] == 'dedicated_vps_tdjson_source_build_plan'\n"
            "assert data['boundary_check'] == 'pass'\n"
            "print('TDJSON_SOURCE_BUILD_PLAN_OUTPUT_CONTRACT_PASS', data['contract_status'], data['recommended_next_slice'])\n"
            "PY"
        ),
    ]


def _make_selected_plan(
    *,
    packages_to_install: Sequence[str],
    packages_excluded_as_optional: Sequence[str],
    package_versions: Mapping[str, str | None],
) -> dict[str, Any]:
    packages = list(packages_to_install)
    return {
        "packages_to_install": packages,
        "packages_excluded_as_optional": list(packages_excluded_as_optional),
        "package_versions": dict(package_versions),
        "install_method": "apt",
        "requires_apt_update": False,
        "future_operator_commands_not_run": [
            "sudo apt install " + " ".join(packages)
        ]
        if packages
        else [],
        "future_validation_commands_not_run": _future_validation_commands(),
        "future_rollback_commands_not_run": [
            "sudo apt remove " + " ".join(packages)
        ]
        if packages
        else [],
    }


def _required_missing_from_source(source_build_plan: Mapping[str, Any]) -> list[str] | None:
    if source_build_plan.get("provided") is not True:
        return None
    missing = []
    for package in REQUIRED_PACKAGES:
        if package in source_build_plan.get("missing_required_tools", []):
            missing.append(package)
        if package in source_build_plan.get("missing_required_packages", []):
            missing.append(package)
    return list(dict.fromkeys(missing))


def _decide(
    *,
    host: Mapping[str, Any],
    source_build_plan: Mapping[str, Any],
    dependency_policy_candidates: Sequence[Mapping[str, Any]],
    installed_dependency_matches: Sequence[Mapping[str, Any]],
) -> tuple[str, str, dict[str, Any], list[str], list[str], list[str]]:
    packages_excluded_as_optional = list(OPTIONAL_EVIDENCE_PACKAGES)
    policy_by_package = _by_package(dependency_policy_candidates)
    installed_by_package = _by_package(installed_dependency_matches)
    package_versions = {
        package: policy_by_package.get(package, {}).get("candidate")
        if isinstance(policy_by_package.get(package, {}).get("candidate"), str)
        else None
        for package in DEPENDENCY_PACKAGES
    }
    empty_plan = _make_selected_plan(
        packages_to_install=[],
        packages_excluded_as_optional=packages_excluded_as_optional,
        package_versions=package_versions,
    )
    stop_conditions = [
        "Stop before apt update/install/upgrade/remove/purge or any package-manager mutation.",
        "Stop before package download, git clone, source build, cmake configure/build, make, ninja, binary placement, symlink creation, or build directory creation.",
        "Stop if runtime.env, secret values, TDLib auth, TDLib client/session creation, Telegram network, DB, Redis, Alembic, Docker, systemd, collector, notifier, or rollout work is requested.",
    ]

    os_id = host.get("os_id")
    if os_id not in SUPPORTED_OS_IDS:
        return (
            "unsupported_host",
            "defer_manual_review",
            empty_plan,
            ["Host OS is not recognized as Ubuntu/Debian from os-release."],
            ["No dependency install plan is selected for unsupported or unreadable hosts."],
            stop_conditions,
        )

    explicit_missing = _required_missing_from_source(source_build_plan)
    if explicit_missing is None:
        target_packages = [
            package
            for package in REQUIRED_PACKAGES
            if installed_by_package.get(package, {}).get("installed") is not True
        ]
        decision_reasons = [
            "No explicit source-build-plan JSON was provided; package selection is based only on local read-only package inspection."
        ]
    else:
        unexpected_missing = [
            item
            for item in (
                source_build_plan.get("missing_required_tools", [])
                + source_build_plan.get("missing_required_packages", [])
            )
            if item not in set(REQUIRED_PACKAGES)
        ]
        if unexpected_missing:
            return (
                "dependency_install_plan_inconclusive",
                "defer_manual_review",
                empty_plan,
                [
                    "Explicit source-build-plan JSON listed missing required dependency outside cmake/gperf."
                ],
                [
                    "This conservative slice plans only the minimum cmake/gperf dependency path."
                ],
                stop_conditions,
            )
        target_packages = explicit_missing
        decision_reasons = [
            "Explicit source-build-plan JSON identified only cmake/gperf as missing source-build dependencies."
        ]

    packages_to_install = [
        package
        for package in target_packages
        if installed_by_package.get(package, {}).get("installed") is not True
    ]
    selected_plan = _make_selected_plan(
        packages_to_install=packages_to_install,
        packages_excluded_as_optional=packages_excluded_as_optional,
        package_versions=package_versions,
    )

    if not packages_to_install:
        decision_reasons.append("No cmake/gperf package remains to plan because required packages appear installed.")
        return (
            "dependency_install_plan_ready",
            "tdjson_source_build_plan_recheck",
            selected_plan,
            decision_reasons,
            ["No dependency installation success is claimed; rerun the source-build plan before considering build execution."],
            stop_conditions,
        )

    missing_candidates = [
        package
        for package in packages_to_install
        if not policy_by_package.get(package, {}).get("candidate")
    ]
    if missing_candidates:
        decision_reasons.append(
            "At least one selected cmake/gperf package lacks an apt policy candidate."
        )
        return (
            "dependency_install_plan_inconclusive",
            "defer_manual_review",
            selected_plan,
            decision_reasons,
            [
                "Missing apt candidate(s): " + ", ".join(missing_candidates) + ".",
                "No apt update is authorized by this plan slice.",
            ],
            stop_conditions,
        )

    decision_reasons.append(
        "Selected packages are candidate-visible, not installed, and limited to the minimum cmake/gperf dependency set."
    )
    return (
        "dependency_install_plan_ready",
        "tdjson_source_build_dependency_install_operator_execution",
        selected_plan,
        decision_reasons,
        [
            "Future operator commands are examples only and were not executed.",
            "This plan does not claim dependency installation success, source build readiness, tdjson availability, auth readiness, or auth rerun authorization.",
        ],
        stop_conditions,
    )


def run_plan(
    *,
    env: Mapping[str, str] | None = None,
    os_release_text: str | None = None,
    command_available: CommandAvailability = shutil.which,
    subprocess_runner: SubprocessRunner = subprocess.run,
    source_build_plan_json: str | None = None,
    preflight_json: str | None = None,
    package_decision_json: str | None = None,
    apt_plan_json: str | None = None,
    manual_evidence_json: str | None = None,
    prior_source_build_plan_json: Mapping[str, Any] | None = None,
    prior_preflight_json: Mapping[str, Any] | None = None,
    prior_package_decision_json: Mapping[str, Any] | None = None,
    prior_apt_plan_json: Mapping[str, Any] | None = None,
    prior_manual_evidence_json: Mapping[str, Any] | None = None,
    machine_provider: MachineProvider = platform.machine,
    platform_provider: PlatformProvider = platform.platform,
) -> dict[str, Any]:
    del env  # Environment values are intentionally not inspected in this slice.

    os_text = _read_os_release() if os_release_text is None else os_release_text
    parsed_os = _parse_os_release(os_text)
    uname_machine = _inspect_uname(
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    host = {
        **parsed_os,
        "architecture": uname_machine or machine_provider(),
        "platform": platform_provider(),
    }

    command_availability = _command_availability(command_available)
    dependency_policy_candidates = _inspect_apt_policy(
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    dependency_show_summaries = _inspect_apt_show(
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    dependency_depends_summaries = _inspect_apt_depends(
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    installed_dependency_matches = _inspect_installed_packages(
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    prior_inputs = {
        "source_build_plan": _summarize_source_build_plan(
            path=source_build_plan_json,
            data=prior_source_build_plan_json,
        ),
        "preflight": _summarize_preflight(path=preflight_json, data=prior_preflight_json),
        "package_decision": _summarize_next_slice_input(
            path=package_decision_json,
            data=prior_package_decision_json,
        ),
        "apt_plan": _summarize_next_slice_input(path=apt_plan_json, data=prior_apt_plan_json),
        "manual_evidence": _summarize_next_slice_input(
            path=manual_evidence_json,
            data=prior_manual_evidence_json,
        ),
    }

    (
        contract_status,
        recommended_next_slice,
        selected_plan,
        decision_reasons,
        risk_notes,
        stop_conditions,
    ) = _decide(
        host=host,
        source_build_plan=prior_inputs["source_build_plan"],
        dependency_policy_candidates=dependency_policy_candidates,
        installed_dependency_matches=installed_dependency_matches,
    )

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_name": CONTRACT_NAME,
        "contract_status": contract_status,
        "recommended_next_slice": recommended_next_slice,
        "host": host,
        "prior_inputs": prior_inputs,
        "inspection": {
            "command_availability": command_availability,
            "dependency_policy_candidates": dependency_policy_candidates,
            "dependency_show_summaries": dependency_show_summaries,
            "dependency_depends_summaries": dependency_depends_summaries,
            "installed_dependency_matches": installed_dependency_matches,
        },
        "selected_plan": selected_plan,
        "decision_reasons": decision_reasons,
        "risk_notes": risk_notes,
        "stop_conditions": stop_conditions,
        "boundary_check": "pass",
    }
    report.update(SAFETY_FLAGS)
    return report


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    print(
        render_json(
            run_plan(
                source_build_plan_json=args.source_build_plan_json,
                preflight_json=args.preflight_json,
                package_decision_json=args.package_decision_json,
                apt_plan_json=args.apt_plan_json,
                manual_evidence_json=args.manual_evidence_json,
            )
        ),
        end="",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
