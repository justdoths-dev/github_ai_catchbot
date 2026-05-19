from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


SCHEMA_VERSION = "dedicated_vps_tdjson_source_build_plan_v1"
CONTRACT_NAME = "dedicated_vps_tdjson_source_build_plan"

SUPPORTED_OS_IDS = {"ubuntu", "debian"}
SOURCE_REPO = "https://github.com/tdlib/td.git"
BUILD_TOOL_NAMES = (
    "git",
    "cmake",
    "g++",
    "gcc",
    "clang++",
    "clang",
    "make",
    "ninja",
    "pkg-config",
    "python3",
)
BUILD_DEPENDENCY_PACKAGES = (
    "git",
    "cmake",
    "build-essential",
    "g++",
    "gcc",
    "clang",
    "make",
    "ninja-build",
    "pkg-config",
    "zlib1g-dev",
    "libssl-dev",
    "gperf",
    "libc++-dev",
    "libc++abi-dev",
)
SEARCH_QUERIES = (
    "tdlib build dependencies",
    "cmake tdlib",
    "libtdjson build",
)
REQUIRED_PACKAGE_GROUPS = (
    ("zlib1g-dev",),
    ("libssl-dev",),
    ("gperf",),
    ("pkg-config",),
    ("build-essential", "g++", "gcc", "clang"),
)
REQUIRED_TOOL_GROUPS = (
    ("git",),
    ("cmake",),
    ("g++", "clang++"),
    ("make", "ninja"),
)

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

PACKAGE_TOOL_MAP = {
    "git": "git",
    "cmake": "cmake",
    "g++": "g++",
    "gcc": "gcc",
    "clang": "clang",
    "make": "make",
    "ninja-build": "ninja",
    "pkg-config": "pkg-config",
}

PACKAGE_REQUIRED_FOR = {
    "git": ["source checkout"],
    "cmake": ["configure", "install target"],
    "build-essential": ["compiler toolchain"],
    "g++": ["C++ compiler"],
    "gcc": ["C compiler"],
    "clang": ["alternate compiler"],
    "make": ["build backend"],
    "ninja-build": ["alternate build backend"],
    "pkg-config": ["dependency discovery"],
    "zlib1g-dev": ["TDLib compression dependency"],
    "libssl-dev": ["TDLib crypto/TLS dependency"],
    "gperf": ["TDLib code generation dependency"],
    "libc++-dev": ["optional clang C++ runtime"],
    "libc++abi-dev": ["optional clang C++ ABI runtime"],
}

CommandAvailability = Callable[[str], str | None]
SubprocessRunner = Callable[..., subprocess.CompletedProcess[str]]
DiskUsageProvider = Callable[[str], Any]
CpuCountProvider = Callable[[], int | None]
MachineProvider = Callable[[], str]
PlatformProvider = Callable[[], str]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Produce a read-only TDLib/tdjson source build plan for later review. "
            "This command clones nothing, downloads nothing, builds nothing, installs "
            "nothing, reads no runtime.env, creates no TDLib client, and contacts no "
            "Telegram network."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument(
        "--preflight-json",
        default=None,
        help="Optional explicit path to a tdjson runtime dependency preflight JSON report.",
    )
    parser.add_argument(
        "--package-decision-json",
        default=None,
        help="Optional explicit path to a tdjson package decision JSON report.",
    )
    parser.add_argument(
        "--apt-plan-json",
        default=None,
        help="Optional explicit path to a tdjson apt install plan JSON report.",
    )
    parser.add_argument(
        "--manual-evidence-json",
        default=None,
        help="Optional explicit path to a tdjson manual package evidence JSON report.",
    )
    return parser


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


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
        or _is_apt_cache_search_command(normalized)
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
        and normalized[3] in BUILD_DEPENDENCY_PACKAGES
    )


def _is_apt_cache_policy_command(normalized: Sequence[str]) -> bool:
    return (
        len(normalized) == 3
        and normalized[:2] == ("apt-cache", "policy")
        and normalized[2] in BUILD_DEPENDENCY_PACKAGES
    )


def _is_apt_cache_show_command(normalized: Sequence[str]) -> bool:
    return (
        len(normalized) == 3
        and normalized[:2] == ("apt-cache", "show")
        and normalized[2] in BUILD_DEPENDENCY_PACKAGES
    )


def _is_apt_cache_depends_command(normalized: Sequence[str]) -> bool:
    return (
        len(normalized) == 3
        and normalized[:2] == ("apt-cache", "depends")
        and normalized[2] in BUILD_DEPENDENCY_PACKAGES
    )


def _is_apt_cache_search_command(normalized: Sequence[str]) -> bool:
    return (
        len(normalized) == 3
        and normalized[:2] == ("apt-cache", "search")
        and normalized[2] in SEARCH_QUERIES
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
        for command in ("dpkg-query", "apt-cache", "uname")
    }


def _build_tool_availability(command_available: CommandAvailability) -> dict[str, dict[str, Any]]:
    availability: dict[str, dict[str, Any]] = {}
    for tool in BUILD_TOOL_NAMES:
        found = command_available(tool)
        availability[tool] = {
            "available": found is not None,
            "path_basename": Path(found).name if found else None,
        }
    return availability


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
        "provides": None,
        "description": None,
    }
    paragraph = _first_paragraph(output)
    current_key: str | None = None
    for raw_line in paragraph.splitlines():
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
    for package in BUILD_DEPENDENCY_PACKAGES:
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
    for package in BUILD_DEPENDENCY_PACKAGES:
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
    for package in BUILD_DEPENDENCY_PACKAGES:
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


def _inspect_apt_search(
    *,
    command_available: CommandAvailability,
    subprocess_runner: SubprocessRunner,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for query in SEARCH_QUERIES:
        result = _command_result(
            argv=("apt-cache", "search", query),
            command_available=command_available,
            subprocess_runner=subprocess_runner,
        )
        matches = []
        for line in result["stdout"].splitlines():
            package = line.split(" ", 1)[0].strip()
            if package:
                matches.append({"package": package, "summary": line[:240]})
        results.append(
            {
                "query": query,
                "command_available": result["available"],
                "returncode": result["returncode"],
                "matches": matches[:30],
            }
        )
    return results


def _inspect_installed_packages(
    *,
    command_available: CommandAvailability,
    subprocess_runner: SubprocessRunner,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for package in BUILD_DEPENDENCY_PACKAGES:
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
    include_selected_package: bool = False,
) -> dict[str, Any]:
    if data is None:
        provided, loaded, error_class = _load_json(path)
        if not provided:
            summary: dict[str, Any] = {
                "provided": False,
                "contract_status": None,
                "recommended_next_slice": None,
            }
            if include_selected_package:
                summary["selected_package"] = None
            return summary
        if loaded is None:
            summary = {
                "provided": True,
                "contract_status": "unreadable_or_invalid",
                "recommended_next_slice": None,
                "error_class": error_class,
            }
            if include_selected_package:
                summary["selected_package"] = None
            return summary
        data = loaded

    summary = {
        "provided": True,
        "contract_status": _safe_scalar(data.get("contract_status")),
        "recommended_next_slice": _safe_scalar(data.get("recommended_next_slice")),
    }
    if include_selected_package:
        selected_package = None
        selected_plan = data.get("selected_plan")
        if isinstance(selected_plan, Mapping):
            selected_package = _safe_scalar(selected_plan.get("package_name"))
        summary["selected_package"] = selected_package
    return summary


def _disk_free_mb(path: str, disk_usage_provider: DiskUsageProvider) -> int | None:
    try:
        usage = disk_usage_provider(path)
    except OSError:
        return None
    free = getattr(usage, "free", None)
    if isinstance(free, int):
        return free // (1024 * 1024)
    if isinstance(usage, tuple) and len(usage) >= 3 and isinstance(usage[2], int):
        return usage[2] // (1024 * 1024)
    return None


def _by_package(items: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    by_package: dict[str, Mapping[str, Any]] = {}
    for item in items:
        package = item.get("package")
        if isinstance(package, str):
            by_package[package] = item
    return by_package


def _confidence_for_package(
    *,
    package: str,
    tool_available: bool | None,
    candidate: str | None,
    installed: bool,
) -> tuple[str, list[str]]:
    risk_notes: list[str] = []
    if installed and (tool_available is not False or package not in PACKAGE_TOOL_MAP):
        return "high", risk_notes
    if installed:
        risk_notes.append("Package appears installed, but the associated command was not discovered.")
        return "medium", risk_notes
    if candidate:
        risk_notes.append("Package candidate is visible but not installed; package mutation needs a future approved slice.")
        return "medium", risk_notes
    if tool_available:
        risk_notes.append("Tool is available, but package installation evidence was not found.")
        return "low", risk_notes
    return "none", risk_notes


def _build_dependency_matrix(
    *,
    build_tool_availability: Mapping[str, Mapping[str, Any]],
    apt_policy_candidates: Sequence[Mapping[str, Any]],
    installed_package_matches: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    policy_by_package = _by_package(apt_policy_candidates)
    installed_by_package = _by_package(installed_package_matches)
    matrix: list[dict[str, Any]] = []

    for package in BUILD_DEPENDENCY_PACKAGES:
        tool_name = PACKAGE_TOOL_MAP.get(package)
        tool_available = None
        if tool_name:
            tool_available = build_tool_availability.get(tool_name, {}).get("available") is True
        policy = policy_by_package.get(package, {})
        installed_match = installed_by_package.get(package, {})
        candidate = policy.get("candidate") if isinstance(policy.get("candidate"), str) else None
        installed = installed_match.get("installed") is True
        confidence, risk_notes = _confidence_for_package(
            package=package,
            tool_available=tool_available,
            candidate=candidate,
            installed=installed,
        )
        matrix.append(
            {
                "package_name": package,
                "tool_name": tool_name,
                "tool_available": tool_available,
                "apt_policy_candidate": candidate,
                "installed": installed,
                "installed_version": installed_match.get("version") if installed else None,
                "required_for": PACKAGE_REQUIRED_FOR.get(package, []),
                "confidence": confidence,
                "risk_notes": risk_notes,
            }
        )
    return matrix


def _group_satisfied_by_installed(
    group: Sequence[str],
    matrix_by_package: Mapping[str, Mapping[str, Any]],
) -> bool:
    return any(matrix_by_package.get(package, {}).get("installed") is True for package in group)


def _group_has_candidate(
    group: Sequence[str],
    matrix_by_package: Mapping[str, Mapping[str, Any]],
) -> bool:
    return any(
        bool(matrix_by_package.get(package, {}).get("apt_policy_candidate")) for package in group
    )


def _missing_required_tool_groups(
    build_tool_availability: Mapping[str, Mapping[str, Any]]
) -> list[str]:
    missing: list[str] = []
    for group in REQUIRED_TOOL_GROUPS:
        if not any(build_tool_availability.get(tool, {}).get("available") is True for tool in group):
            missing.append("|".join(group))
    return missing


def _missing_required_package_groups(
    matrix_by_package: Mapping[str, Mapping[str, Any]],
    *,
    installed_only: bool,
) -> list[str]:
    missing: list[str] = []
    for group in REQUIRED_PACKAGE_GROUPS:
        satisfied = _group_satisfied_by_installed(group, matrix_by_package)
        if not satisfied and not installed_only:
            satisfied = _group_has_candidate(group, matrix_by_package)
        if not satisfied:
            missing.append("|".join(group))
    return missing


def _future_validation_commands() -> list[str]:
    return [
        "venv/bin/python scripts/ops/dedicated_vps_tdjson_runtime_dependency_preflight.py --format json",
        (
            "venv/bin/python - <<'PY'\n"
            "import json\n"
            "from pathlib import Path\n"
            "data = json.loads(Path('/tmp/dedicated_vps_tdjson_runtime_dependency_preflight.json').read_text(encoding='utf-8'))\n"
            "assert data['schema_version'] == 'dedicated_vps_tdjson_runtime_dependency_preflight_v1'\n"
            "assert data['contract_name'] == 'dedicated_vps_tdjson_runtime_dependency_preflight'\n"
            "assert data['boundary_check'] == 'pass'\n"
            "print('TDJSON_PREFLIGHT_OUTPUT_CONTRACT_PASS', data['contract_status'])\n"
            "PY"
        ),
    ]


def _selected_plan(
    *,
    requires_package_install: bool,
    missing_required_tools: Sequence[str],
    missing_required_packages: Sequence[str],
) -> dict[str, Any]:
    build_workspace_candidate = "/opt/github-ai-catchbot/build/tdlib-source"
    install_prefix_candidate = "/opt/github-ai-catchbot/tdlib"
    return {
        "source_repo": SOURCE_REPO,
        "build_workspace_candidate": build_workspace_candidate,
        "install_prefix_candidate": install_prefix_candidate,
        "install_method": "source_build",
        "requires_network": True,
        "requires_package_install": requires_package_install,
        "missing_required_tools": list(missing_required_tools),
        "missing_required_packages": list(missing_required_packages),
        "future_operator_commands_not_run": [
            f"git clone --depth 1 {SOURCE_REPO} {build_workspace_candidate}/td",
            (
                f"cmake -S {build_workspace_candidate}/td "
                f"-B {build_workspace_candidate}/td/build "
                f"-DCMAKE_BUILD_TYPE=Release -DTD_ENABLE_JNI=OFF "
                f"-DCMAKE_INSTALL_PREFIX={install_prefix_candidate}"
            ),
            f"cmake --build {build_workspace_candidate}/td/build --target install",
        ],
        "future_validation_commands_not_run": _future_validation_commands(),
        "future_rollback_commands_not_run": [
            f"rm -rf {build_workspace_candidate}/td",
            f"rm -rf {install_prefix_candidate}",
        ],
    }


def _decide(
    *,
    host: Mapping[str, Any],
    prior_inputs: Mapping[str, Mapping[str, Any]],
    build_tool_availability: Mapping[str, Mapping[str, Any]],
    build_dependency_matrix: Sequence[Mapping[str, Any]],
) -> tuple[str, str, dict[str, Any], list[str], list[str], list[str]]:
    os_id = host.get("os_id")
    if os_id not in SUPPORTED_OS_IDS:
        selected_plan = _selected_plan(
            requires_package_install=False,
            missing_required_tools=[],
            missing_required_packages=[],
        )
        selected_plan["future_operator_commands_not_run"] = []
        selected_plan["future_validation_commands_not_run"] = []
        selected_plan["future_rollback_commands_not_run"] = []
        return (
            "unsupported_host",
            "defer_manual_review",
            selected_plan,
            ["Host OS is not recognized as Ubuntu/Debian from os-release."],
            ["No source-build path is selected for unsupported or unreadable hosts."],
            ["Stop before any package, source build, auth, runtime, or rollout work."],
        )

    matrix_by_package = {
        str(item.get("package_name")): item for item in build_dependency_matrix
    }
    missing_tool_groups = _missing_required_tool_groups(build_tool_availability)
    missing_installed_package_groups = _missing_required_package_groups(
        matrix_by_package,
        installed_only=True,
    )
    missing_visible_package_groups = _missing_required_package_groups(
        matrix_by_package,
        installed_only=False,
    )
    requires_package_install = bool(missing_installed_package_groups)
    selected_plan = _selected_plan(
        requires_package_install=requires_package_install,
        missing_required_tools=missing_tool_groups,
        missing_required_packages=missing_installed_package_groups,
    )
    decision_reasons: list[str] = []
    risk_notes: list[str] = []
    stop_conditions = [
        "Stop unless a later operator slice explicitly approves source build execution.",
        "Stop before apt update/install/upgrade/remove/purge or any package-manager mutation.",
        "Stop before git clone, network download, cmake configure/build, make, ninja, binary placement, symlink creation, or build directory creation.",
        "Stop if runtime.env or secret values are needed.",
        "Stop if TDLib auth, TDLib client/session creation, Telegram network, DB, Redis, Alembic, Docker, systemd, collector, notifier, or rollout work is requested.",
    ]

    manual_evidence = prior_inputs.get("manual_evidence", {})
    manual_next = manual_evidence.get("recommended_next_slice")
    source_build_strong = (
        not missing_tool_groups
        and not missing_installed_package_groups
        and host.get("disk_free_mb") is not None
        and int(host.get("disk_free_mb") or 0) >= 4096
    )

    if source_build_strong:
        decision_reasons.append(
            "Required build tools are available and required dependency packages appear installed."
        )
        if manual_next == "tdjson_prebuilt_library_path_plan":
            decision_reasons.append(
                "Explicit prior manual evidence suggested prebuilt path planning, but complete source-build dependency evidence is stronger."
            )
        return (
            "source_build_plan_ready",
            "tdjson_source_build_operator_execution",
            selected_plan,
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    if manual_next == "tdjson_prebuilt_library_path_plan":
        decision_reasons.append(
            "Explicit prior manual evidence recommended tdjson_prebuilt_library_path_plan; this report does not override it without complete source-build evidence."
        )
        return (
            "source_build_plan_inconclusive",
            "tdjson_prebuilt_library_path_plan",
            selected_plan,
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    if missing_tool_groups:
        risk_notes.append(
            "Required build tool group(s) are unavailable by shutil.which: "
            + ", ".join(missing_tool_groups)
            + "."
        )
        decision_reasons.append("Source build cannot be selected while required tools are missing.")
        return (
            "source_build_plan_inconclusive",
            "defer_manual_review",
            selected_plan,
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    disk_free_mb = host.get("disk_free_mb")
    if disk_free_mb is None or int(disk_free_mb) < 4096:
        risk_notes.append("Disk free evidence is unreadable or below the conservative 4096 MB threshold.")
        decision_reasons.append("Host resources are insufficient or too weak for source-build planning.")
        return (
            "source_build_plan_inconclusive",
            "defer_manual_review",
            selected_plan,
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    if missing_visible_package_groups:
        risk_notes.append(
            "Required dependency package group(s) lack installed or candidate-visible evidence: "
            + ", ".join(missing_visible_package_groups)
            + "."
        )
        decision_reasons.append("Required dependency packages cannot be identified strongly enough.")
        return (
            "source_build_plan_inconclusive",
            "defer_manual_review",
            selected_plan,
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    if requires_package_install:
        decision_reasons.append(
            "Source build appears plausible, but required dependency package evidence is candidate-visible rather than installed."
        )
        risk_notes.append("Package install remains a separate future planning slice; this tool mutates nothing.")
        return (
            "source_build_plan_requires_dependency_plan",
            "tdjson_source_build_dependency_install_plan",
            selected_plan,
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    decision_reasons.append("Source-build evidence is insufficient to select an operator execution slice.")
    return (
        "source_build_plan_inconclusive",
        "defer_manual_review",
        selected_plan,
        decision_reasons,
        risk_notes,
        stop_conditions,
    )


def run_plan(
    *,
    env: Mapping[str, str] | None = None,
    os_release_text: str | None = None,
    command_available: CommandAvailability = shutil.which,
    subprocess_runner: SubprocessRunner = subprocess.run,
    preflight_json: str | None = None,
    package_decision_json: str | None = None,
    apt_plan_json: str | None = None,
    manual_evidence_json: str | None = None,
    prior_preflight_json: Mapping[str, Any] | None = None,
    prior_package_decision_json: Mapping[str, Any] | None = None,
    prior_apt_plan_json: Mapping[str, Any] | None = None,
    prior_manual_evidence_json: Mapping[str, Any] | None = None,
    disk_usage_provider: DiskUsageProvider = shutil.disk_usage,
    cpu_count_provider: CpuCountProvider = os.cpu_count,
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
    repo_root = str(_repo_root())
    host = {
        **parsed_os,
        "architecture": uname_machine or machine_provider(),
        "platform": platform_provider(),
        "cpu_count": cpu_count_provider(),
        "disk_free_mb": _disk_free_mb(repo_root, disk_usage_provider),
    }

    command_availability = _command_availability(command_available)
    build_tool_availability = _build_tool_availability(command_available)
    apt_policy_candidates = _inspect_apt_policy(
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    apt_show_summaries = _inspect_apt_show(
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    apt_depends_summaries = _inspect_apt_depends(
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    apt_search_matches = _inspect_apt_search(
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    installed_package_matches = _inspect_installed_packages(
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    prior_inputs = {
        "preflight": _summarize_preflight(path=preflight_json, data=prior_preflight_json),
        "package_decision": _summarize_next_slice_input(
            path=package_decision_json,
            data=prior_package_decision_json,
        ),
        "apt_plan": _summarize_next_slice_input(
            path=apt_plan_json,
            data=prior_apt_plan_json,
            include_selected_package=True,
        ),
        "manual_evidence": _summarize_next_slice_input(
            path=manual_evidence_json,
            data=prior_manual_evidence_json,
        ),
    }
    build_dependency_matrix = _build_dependency_matrix(
        build_tool_availability=build_tool_availability,
        apt_policy_candidates=apt_policy_candidates,
        installed_package_matches=installed_package_matches,
    )
    (
        contract_status,
        recommended_next_slice,
        selected_plan,
        decision_reasons,
        risk_notes,
        stop_conditions,
    ) = _decide(
        host=host,
        prior_inputs=prior_inputs,
        build_tool_availability=build_tool_availability,
        build_dependency_matrix=build_dependency_matrix,
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
            "build_tool_availability": build_tool_availability,
            "build_dependency_policy_candidates": apt_policy_candidates,
            "build_dependency_show_summaries": apt_show_summaries,
            "build_dependency_depends_summaries": apt_depends_summaries,
            "installed_build_dependency_matches": installed_package_matches,
            "apt_search_matches": apt_search_matches,
        },
        "build_dependency_matrix": build_dependency_matrix,
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
