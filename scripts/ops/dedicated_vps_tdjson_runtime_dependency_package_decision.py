from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


SCHEMA_VERSION = "dedicated_vps_tdjson_runtime_dependency_package_decision_v1"
CONTRACT_NAME = "dedicated_vps_tdjson_runtime_dependency_package_decision"

SUPPORTED_OS_IDS = {"ubuntu", "debian"}
PACKAGE_CANDIDATES = ("libtdjson", "libtdjson-dev", "tdlib", "tdlib-dev")
SEARCH_QUERIES = ("tdjson", "tdlib")

SAFETY_FLAGS = {
    "installation_attempted": False,
    "package_manager_mutation_attempted": False,
    "package_download_attempted": False,
    "source_build_attempted": False,
    "git_clone_attempted": False,
    "binary_placement_attempted": False,
    "symlink_created": False,
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
            "Report which tdjson runtime dependency provisioning path should be "
            "reviewed next. This installs nothing, changes no packages, reads no "
            "operator secret file, creates no TDLib client, and contacts no network."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument(
        "--preflight-json",
        default=None,
        help=(
            "Optional explicit path to an existing tdjson runtime dependency "
            "preflight JSON report. No default path is read."
        ),
    )
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
    path = Path("/etc/os-release")
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


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


def _validate_allowed_command(argv: Sequence[str]) -> None:
    if not argv:
        raise ValueError("empty subprocess command is not allowed")

    normalized = tuple(str(part).lower() for part in argv)
    allowed = (
        _is_dpkg_query_command(normalized)
        or _is_apt_cache_policy_command(normalized)
        or _is_apt_cache_search_command(normalized)
        or normalized == ("ldconfig", "-p")
        or normalized == ("uname", "-m")
    )
    if not allowed:
        raise ValueError(f"subprocess command is not allowlisted: {list(argv)!r}")

    for token in normalized:
        if token in FORBIDDEN_COMMAND_TOKENS:
            raise ValueError(f"forbidden subprocess token: {token}")
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
        and normalized[3] in PACKAGE_CANDIDATES
    )


def _is_apt_cache_policy_command(normalized: Sequence[str]) -> bool:
    return len(normalized) == 3 and normalized[:2] == ("apt-cache", "policy") and normalized[2] in PACKAGE_CANDIDATES


def _is_apt_cache_search_command(normalized: Sequence[str]) -> bool:
    return len(normalized) == 3 and normalized[:2] == ("apt-cache", "search") and normalized[2] in SEARCH_QUERIES


def _command_availability(command_available: CommandAvailability) -> dict[str, bool]:
    commands = ("dpkg-query", "apt-cache", "ldconfig", "uname")
    return {command: command_available(command) is not None for command in commands}


def _extract_apt_candidate(output: str) -> str | None:
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped.lower().startswith("candidate:"):
            continue
        candidate = stripped.split(":", 1)[1].strip()
        if candidate and candidate.lower() != "(none)":
            return candidate
    return None


def _inspect_apt_policy(
    *,
    command_available: CommandAvailability,
    subprocess_runner: SubprocessRunner,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for package in PACKAGE_CANDIDATES:
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
            if package and ("tdjson" in line.lower() or "tdlib" in line.lower()):
                matches.append({"package": package, "summary": line[:240]})
        results.append(
            {
                "query": query,
                "command_available": result["available"],
                "returncode": result["returncode"],
                "matches": matches[:20],
            }
        )
    return results


def _inspect_installed_packages(
    *,
    command_available: CommandAvailability,
    subprocess_runner: SubprocessRunner,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for package in PACKAGE_CANDIDATES:
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


def _inspect_ldconfig(
    *,
    command_available: CommandAvailability,
    subprocess_runner: SubprocessRunner,
) -> list[dict[str, str]]:
    result = _command_result(
        argv=("ldconfig", "-p"),
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    if not result["available"] or result["returncode"] not in (0, None):
        return []

    matches = []
    for line in result["stdout"].splitlines():
        if "tdjson" in line.lower() or "tdlib" in line.lower():
            matches.append({"entry": line.strip()[:240]})
    return matches[:50]


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


def _load_preflight_json(preflight_json: str | None) -> dict[str, Any]:
    if not preflight_json:
        return {"provided": False, "contract_status": None, "tdjson_available": None}

    try:
        data = json.loads(Path(preflight_json).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "provided": True,
            "contract_status": "unreadable_or_invalid",
            "tdjson_available": None,
            "error_class": exc.__class__.__name__,
        }

    return {
        "provided": True,
        "contract_status": _safe_scalar(data.get("contract_status")),
        "tdjson_available": data.get("tdjson_available")
        if isinstance(data.get("tdjson_available"), bool)
        else None,
    }


def _safe_scalar(value: object) -> str | None:
    if value is None:
        return None
    return str(value)[:120]


def _decide(
    *,
    host: Mapping[str, str | None],
    prior_preflight: Mapping[str, Any],
    apt_policy_candidates: Sequence[Mapping[str, Any]],
    apt_search_matches: Sequence[Mapping[str, Any]],
    ldconfig_tdjson_matches: Sequence[Mapping[str, Any]],
) -> tuple[str, str, list[str], list[str], list[dict[str, Any]]]:
    os_id = host.get("os_id")
    if os_id not in SUPPORTED_OS_IDS:
        return (
            "unsupported_host",
            "defer_manual_review",
            ["Host OS is not recognized as Ubuntu/Debian from os-release."],
            ["No provisioning path is selected for unsupported or unreadable hosts."],
            [_candidate_action("defer_manual_review", "Stop and perform manual host review.")],
        )

    apt_candidate_packages = [
        item["package"] for item in apt_policy_candidates if item.get("candidate_visible")
    ]
    apt_search_packages = [
        match["package"]
        for item in apt_search_matches
        for match in item.get("matches", [])
        if isinstance(match, Mapping)
    ]

    preflight_status = prior_preflight.get("contract_status")
    preflight_load_problem = preflight_status in {
        "tdjson_load_failed",
        "tdjson_missing_required_symbols",
    }
    if preflight_load_problem and ldconfig_tdjson_matches:
        return (
            "package_decision_ready",
            "tdjson_prebuilt_library_path_plan",
            [
                "Prior explicit preflight reported a tdjson load/symbol problem.",
                "ldconfig shows tdjson/tdlib-related library hints on this host.",
            ],
            [
                "This does not prove the hinted library is correct or safe to use.",
                "A later slice must review library path placement and TDJSON_LIBRARY_PATH explicitly.",
            ],
            [
                _candidate_action(
                    "tdjson_prebuilt_library_path_plan",
                    "Review whether an existing/prebuilt library path can be selected without auth.",
                    future_commands=("export TDJSON_LIBRARY_PATH=/reviewed/path/libtdjson.so",),
                )
            ],
        )

    if apt_candidate_packages:
        return (
            "package_decision_ready",
            "tdjson_apt_install_plan",
            [f"apt-cache policy exposes candidate package(s): {', '.join(apt_candidate_packages)}."],
            ["A later install slice must review exact package names and commands before mutation."],
            [
                _candidate_action(
                    "tdjson_apt_install_plan",
                    "Review package-manager installation using visible candidate package names.",
                    future_commands=tuple(
                        f"sudo apt install {package}" for package in apt_candidate_packages[:3]
                    ),
                )
            ],
        )

    if apt_search_packages:
        return (
            "package_decision_ready",
            "tdjson_apt_install_plan",
            [f"apt-cache search found tdjson/tdlib-related package(s): {', '.join(apt_search_packages[:5])}."],
            ["Search matches require human review because package names may not provide libtdjson.so."],
            [
                _candidate_action(
                    "tdjson_apt_install_plan",
                    "Review apt search matches and choose exact install package in a future slice.",
                    future_commands=tuple(
                        f"sudo apt install {package}" for package in apt_search_packages[:3]
                    ),
                )
            ],
        )

    if any(item.get("command_available") for item in apt_policy_candidates + apt_search_matches):
        return (
            "package_decision_ready",
            "tdjson_source_build_plan",
            ["Ubuntu/Debian host is supported but no apt tdjson candidate was visible."],
            [
                "Source build must be planned separately and must not be run by this decision tool.",
                "Build dependencies and artifact placement need explicit review.",
            ],
            [
                _candidate_action(
                    "tdjson_source_build_plan",
                    "Prepare a future source build plan with reviewed commands and rollback notes.",
                    future_commands=(
                        "git clone https://github.com/tdlib/td.git",
                        "cmake --build <reviewed-build-dir>",
                    ),
                )
            ],
        )

    return (
        "package_decision_inconclusive",
        "defer_manual_review",
        ["Supported host detected, but package-manager evidence was unavailable or insufficient."],
        ["Manual review is required before choosing package install, source build, or library placement."],
        [_candidate_action("defer_manual_review", "Collect reviewed host evidence in a future slice.")],
    )


def _candidate_action(
    next_slice: str,
    description: str,
    future_commands: Sequence[str] = (),
) -> dict[str, Any]:
    action: dict[str, Any] = {
        "recommended_next_slice": next_slice,
        "description": description,
        "commands_executed_by_this_tool": [],
    }
    if future_commands:
        action["future_operator_commands_not_run"] = list(future_commands)
    return action


def run_decision(
    *,
    env: Mapping[str, str] | None = None,
    os_release_text: str | None = None,
    command_available: CommandAvailability = shutil.which,
    subprocess_runner: SubprocessRunner = subprocess.run,
    preflight_json: str | None = None,
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
    apt_policy_candidates = _inspect_apt_policy(
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
    ldconfig_tdjson_matches = _inspect_ldconfig(
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    prior_preflight = _load_preflight_json(preflight_json)

    (
        contract_status,
        recommended_next_slice,
        decision_reasons,
        risk_notes,
        candidate_actions,
    ) = _decide(
        host=host,
        prior_preflight=prior_preflight,
        apt_policy_candidates=apt_policy_candidates,
        apt_search_matches=apt_search_matches,
        ldconfig_tdjson_matches=ldconfig_tdjson_matches,
    )

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_name": CONTRACT_NAME,
        "contract_status": contract_status,
        "recommended_next_slice": recommended_next_slice,
        "host": host,
        "prior_preflight": prior_preflight,
        "inspection": {
            "command_availability": command_availability,
            "apt_policy_candidates": apt_policy_candidates,
            "apt_search_matches": apt_search_matches,
            "installed_package_matches": installed_package_matches,
            "ldconfig_tdjson_matches": ldconfig_tdjson_matches,
        },
        "decision_reasons": decision_reasons,
        "risk_notes": risk_notes,
        "candidate_actions": candidate_actions,
        "boundary_check": "pass",
    }
    report.update(SAFETY_FLAGS)
    return report


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    print(render_json(run_decision(preflight_json=args.preflight_json)), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
