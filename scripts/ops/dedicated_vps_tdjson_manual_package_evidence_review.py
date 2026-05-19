from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


SCHEMA_VERSION = "dedicated_vps_tdjson_manual_package_evidence_review_v1"
CONTRACT_NAME = "dedicated_vps_tdjson_manual_package_evidence_review"

SUPPORTED_OS_IDS = {"ubuntu", "debian"}
PACKAGE_CANDIDATES = (
    "libtdjson",
    "libtdjson-dev",
    "tdlib",
    "tdlib-dev",
    "libtd-dev",
    "libtdactor-dev",
    "libtdutils-dev",
)
SEARCH_QUERIES = ("tdjson", "tdlib", "tdlib json", "telegram database library")

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
            "Collect read-only tdjson package evidence for manual planning. "
            "This command installs nothing, mutates no packages, reads no "
            "runtime.env, creates no TDLib client, and contacts no network."
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
    parser.add_argument(
        "--package-decision-json",
        default=None,
        help=(
            "Optional explicit path to an existing tdjson package-decision JSON "
            "report. No default path is read."
        ),
    )
    parser.add_argument(
        "--apt-plan-json",
        default=None,
        help=(
            "Optional explicit path to an existing tdjson apt install plan JSON "
            "report. No default path is read."
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
        or _is_apt_cache_madison_command(normalized)
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
    return (
        len(normalized) == 3
        and normalized[:2] == ("apt-cache", "policy")
        and normalized[2] in PACKAGE_CANDIDATES
    )


def _is_apt_cache_show_command(normalized: Sequence[str]) -> bool:
    return (
        len(normalized) == 3
        and normalized[:2] == ("apt-cache", "show")
        and normalized[2] in PACKAGE_CANDIDATES
    )


def _is_apt_cache_depends_command(normalized: Sequence[str]) -> bool:
    return (
        len(normalized) == 3
        and normalized[:2] == ("apt-cache", "depends")
        and normalized[2] in PACKAGE_CANDIDATES
    )


def _is_apt_cache_madison_command(normalized: Sequence[str]) -> bool:
    return (
        len(normalized) == 3
        and normalized[:2] == ("apt-cache", "madison")
        and normalized[2] in PACKAGE_CANDIDATES
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


def _inspect_apt_show(
    *,
    command_available: CommandAvailability,
    subprocess_runner: SubprocessRunner,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for package in PACKAGE_CANDIDATES:
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
    for package in PACKAGE_CANDIDATES:
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


def _inspect_apt_madison(
    *,
    command_available: CommandAvailability,
    subprocess_runner: SubprocessRunner,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for package in PACKAGE_CANDIDATES:
        result = _command_result(
            argv=("apt-cache", "madison", package),
            command_available=command_available,
            subprocess_runner=subprocess_runner,
        )
        lines = [line.strip()[:240] for line in result["stdout"].splitlines() if line.strip()]
        results.append(
            {
                "package": package,
                "command_available": result["available"],
                "returncode": result["returncode"],
                "lines": lines[:40],
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
            lower_line = line.lower()
            if package and any(term in lower_line for term in ("tdjson", "tdlib", "telegram database")):
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
    return matches[:80]


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


def _summarize_package_decision(
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


def _summarize_apt_plan(
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
                "selected_package": None,
            }
        if loaded is None:
            return {
                "provided": True,
                "contract_status": "unreadable_or_invalid",
                "recommended_next_slice": None,
                "selected_package": None,
                "error_class": error_class,
            }
        data = loaded

    selected_package = None
    selected_plan = data.get("selected_plan")
    if isinstance(selected_plan, Mapping):
        selected_package = _safe_scalar(selected_plan.get("package_name"))

    return {
        "provided": True,
        "contract_status": _safe_scalar(data.get("contract_status")),
        "recommended_next_slice": _safe_scalar(data.get("recommended_next_slice")),
        "selected_package": selected_package,
    }


def _by_package(items: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    by_package: dict[str, Mapping[str, Any]] = {}
    for item in items:
        package = item.get("package")
        if isinstance(package, str):
            by_package[package] = item
    return by_package


def _text_mentions_tdjson_or_tdlib(*values: object) -> bool:
    haystack = " ".join(str(value or "") for value in values).lower()
    return "tdjson" in haystack or "tdlib" in haystack or "telegram database library" in haystack


def _confidence_for_package(
    *,
    package: str,
    policy_candidate: str | None,
    show_summary: Mapping[str, Any],
    depends_mentions: bool,
    installed: bool,
) -> tuple[str, list[str]]:
    risk_notes: list[str] = []
    show_text_mentions = _text_mentions_tdjson_or_tdlib(
        show_summary.get("package"),
        show_summary.get("source"),
        show_summary.get("depends"),
        show_summary.get("provides"),
        show_summary.get("description"),
    )

    if package == "libtdjson" and policy_candidate and show_text_mentions:
        return "high", risk_notes
    if package == "libtdjson" and policy_candidate:
        risk_notes.append("Package name is strong, but apt-cache show did not clearly mention tdjson/tdlib.")
        return "medium", risk_notes
    if package.endswith("-dev") and policy_candidate:
        risk_notes.append("Development package evidence does not prove a runtime libtdjson.so provider.")
        return "medium" if depends_mentions else "low", risk_notes
    if package.startswith("tdlib") and policy_candidate:
        risk_notes.append("Generic tdlib package evidence does not prove libtdjson.so availability.")
        return "low", risk_notes
    if installed and package == "libtdjson":
        return "medium", ["Installed package evidence exists, but this slice does not claim runtime load success."]
    if policy_candidate:
        risk_notes.append("Package candidate is related but does not directly identify libtdjson.so.")
        return "low", risk_notes
    return "none", risk_notes


def _build_evidence_matrix(
    *,
    apt_policy_candidates: Sequence[Mapping[str, Any]],
    apt_show_summaries: Sequence[Mapping[str, Any]],
    apt_depends_summaries: Sequence[Mapping[str, Any]],
    installed_package_matches: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    policy_by_package = _by_package(apt_policy_candidates)
    show_by_package = _by_package(apt_show_summaries)
    depends_by_package = _by_package(apt_depends_summaries)
    installed_by_package = _by_package(installed_package_matches)
    matrix: list[dict[str, Any]] = []

    for package in PACKAGE_CANDIDATES:
        policy = policy_by_package.get(package, {})
        show = show_by_package.get(package, {})
        depends = depends_by_package.get(package, {})
        installed_match = installed_by_package.get(package, {})
        show_summary = show.get("summary") if isinstance(show.get("summary"), Mapping) else {}
        depends_lines = depends.get("lines") if isinstance(depends.get("lines"), list) else []
        depends_mentions = _text_mentions_tdjson_or_tdlib(*depends_lines)
        installed = installed_match.get("installed") is True
        candidate = policy.get("candidate") if isinstance(policy.get("candidate"), str) else None
        confidence, risk_notes = _confidence_for_package(
            package=package,
            policy_candidate=candidate,
            show_summary=show_summary,
            depends_mentions=depends_mentions,
            installed=installed,
        )

        matrix.append(
            {
                "package_name": package,
                "apt_policy_candidate": candidate,
                "apt_show_package": show_summary.get("package"),
                "apt_show_version": show_summary.get("version"),
                "apt_show_source": show_summary.get("source"),
                "apt_show_depends": show_summary.get("depends"),
                "apt_show_provides": show_summary.get("provides"),
                "apt_show_description": show_summary.get("description"),
                "apt_depends_mentions_tdjson_or_tdlib": depends_mentions,
                "installed": installed,
                "installed_version": installed_match.get("version") if installed else None,
                "confidence": confidence,
                "risk_notes": risk_notes,
            }
        )

    return matrix


def _future_validation_commands() -> list[str]:
    return [
        "venv/bin/python scripts/ops/dedicated_vps_tdjson_runtime_dependency_preflight.py --format json",
        (
            "venv/bin/python - <<'PY'\n"
            "import json\n"
            "from pathlib import Path\n"
            "data = json.loads(Path('/tmp/dedicated_vps_tdjson_runtime_dependency_preflight.json').read_text(encoding='utf-8'))\n"
            "assert data['boundary_check'] == 'pass'\n"
            "print('TDJSON_PREFLIGHT_OUTPUT_CONTRACT_PASS', data['contract_status'])\n"
            "PY"
        ),
    ]


def _candidate_next_actions() -> list[dict[str, Any]]:
    return [
        {
            "next_slice": "tdjson_apt_install_plan_recheck",
            "reason": "Re-run the apt install plan with this stronger package evidence matrix before any operator execution is considered.",
            "future_operator_commands_not_run": [
                "venv/bin/python scripts/ops/dedicated_vps_tdjson_apt_install_plan.py --format json --preflight-json /tmp/dedicated_vps_tdjson_runtime_dependency_preflight.json --package-decision-json /tmp/dedicated_vps_tdjson_runtime_dependency_package_decision.json"
            ],
            "future_validation_commands_not_run": _future_validation_commands(),
            "future_rollback_commands_not_run": [],
        },
        {
            "next_slice": "tdjson_source_build_plan",
            "reason": "Plan a source build only if package evidence cannot identify a likely libtdjson.so provider.",
            "future_operator_commands_not_run": [
                "cmake -S td -B build -DTD_ENABLE_JNI=OFF",
                "cmake --build build",
            ],
            "future_validation_commands_not_run": _future_validation_commands(),
            "future_rollback_commands_not_run": ["Remove build artifacts from the future build workspace."],
        },
        {
            "next_slice": "tdjson_prebuilt_library_path_plan",
            "reason": "Plan an explicit prebuilt library path only if ldconfig or operator evidence points to an existing libtdjson.so path.",
            "future_operator_commands_not_run": [
                "Export TDJSON_LIBRARY_PATH to a reviewed existing libtdjson.so path."
            ],
            "future_validation_commands_not_run": _future_validation_commands(),
            "future_rollback_commands_not_run": ["Unset the future TDJSON_LIBRARY_PATH override."],
        },
        {
            "next_slice": "defer_manual_review",
            "reason": "Stop when host, package, or library evidence is too weak to select a package/build/path planning slice.",
            "future_operator_commands_not_run": [],
            "future_validation_commands_not_run": [],
            "future_rollback_commands_not_run": [],
        },
    ]


def _ldconfig_points_to_existing_tdjson_path(
    ldconfig_tdjson_matches: Sequence[Mapping[str, str]],
) -> bool:
    for item in ldconfig_tdjson_matches:
        entry = item.get("entry", "").lower()
        if "libtdjson" in entry and "=>" in entry:
            return True
    return False


def _decide(
    *,
    host: Mapping[str, str | None],
    evidence_matrix: Sequence[Mapping[str, Any]],
    apt_madison_summaries: Sequence[Mapping[str, Any]],
    apt_search_matches: Sequence[Mapping[str, Any]],
    ldconfig_tdjson_matches: Sequence[Mapping[str, str]],
) -> tuple[str, str, list[str], list[str], list[str]]:
    os_id = host.get("os_id")
    if os_id not in SUPPORTED_OS_IDS:
        return (
            "unsupported_host",
            "defer_manual_review",
            ["Host OS is not recognized as Ubuntu/Debian from os-release."],
            ["No package evidence decision is selected for unsupported or unreadable hosts."],
            ["Stop before any package-manager mutation and perform manual host review."],
        )

    decision_reasons: list[str] = []
    risk_notes: list[str] = []
    stop_conditions = [
        "Stop before any package-manager mutation; this slice is read-only/report-only.",
        "Stop if runtime.env or secret values are needed.",
        "Stop if TDLib auth, TDLib client/session creation, Telegram network, DB, Redis, Alembic, Docker, systemd, collector, notifier, or rollout work is requested.",
    ]

    by_package = {str(item.get("package_name")): item for item in evidence_matrix}
    libtdjson = by_package.get("libtdjson")
    if libtdjson and libtdjson.get("confidence") in {"high", "medium"}:
        decision_reasons.append(
            "apt policy/show evidence identifies libtdjson as an installable candidate likely tied to tdjson runtime support."
        )
        return (
            "manual_package_evidence_ready",
            "tdjson_apt_install_plan_recheck",
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    visible_dev_or_generic = [
        item
        for item in evidence_matrix
        if item.get("apt_policy_candidate")
        and item.get("package_name") != "libtdjson"
        and item.get("confidence") in {"medium", "low"}
    ]
    if visible_dev_or_generic:
        names = ", ".join(str(item["package_name"]) for item in visible_dev_or_generic[:5])
        decision_reasons.append(
            f"Only development or generic TDLib package evidence is visible: {names}."
        )
        risk_notes.append("Visible package evidence may not provide libtdjson.so.")
        next_slice = "tdjson_source_build_plan"
        if not any(item.get("confidence") == "medium" for item in visible_dev_or_generic):
            next_slice = "defer_manual_review"
        return (
            "manual_package_evidence_inconclusive",
            next_slice,
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    madison_hits = [
        item.get("package")
        for item in apt_madison_summaries
        if isinstance(item.get("lines"), list) and item.get("lines")
    ]
    search_hits = []
    for item in apt_search_matches:
        for match in item.get("matches", []):
            if isinstance(match, Mapping) and isinstance(match.get("package"), str):
                search_hits.append(match["package"])
    if madison_hits or search_hits:
        decision_reasons.append(
            "Search or madison evidence found TDLib-related names, but apt policy did not identify an installable runtime package candidate."
        )
        risk_notes.append("Search/madison evidence alone does not prove libtdjson.so availability.")
        return (
            "manual_package_evidence_inconclusive",
            "defer_manual_review",
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    if ldconfig_tdjson_matches:
        decision_reasons.append("ldconfig shows tdjson/tdlib library hints without matching apt package evidence.")
        if _ldconfig_points_to_existing_tdjson_path(ldconfig_tdjson_matches):
            return (
                "manual_package_evidence_inconclusive",
                "tdjson_prebuilt_library_path_plan",
                decision_reasons,
                risk_notes,
                stop_conditions,
            )
        risk_notes.append("ldconfig hints do not clearly point to an existing libtdjson.so path.")
        return (
            "manual_package_evidence_inconclusive",
            "defer_manual_review",
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    decision_reasons.append("Ubuntu/Debian host is supported, but no visible apt package evidence was found.")
    risk_notes.append("A source-build plan may be needed, but source build commands are forbidden in this slice.")
    return (
        "manual_package_evidence_inconclusive",
        "tdjson_source_build_plan",
        decision_reasons,
        risk_notes,
        stop_conditions,
    )


def run_review(
    *,
    env: Mapping[str, str] | None = None,
    os_release_text: str | None = None,
    command_available: CommandAvailability = shutil.which,
    subprocess_runner: SubprocessRunner = subprocess.run,
    preflight_json: str | None = None,
    package_decision_json: str | None = None,
    apt_plan_json: str | None = None,
    prior_preflight_json: Mapping[str, Any] | None = None,
    prior_package_decision_json: Mapping[str, Any] | None = None,
    prior_apt_plan_json: Mapping[str, Any] | None = None,
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
    apt_show_summaries = _inspect_apt_show(
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    apt_depends_summaries = _inspect_apt_depends(
        command_available=command_available,
        subprocess_runner=subprocess_runner,
    )
    apt_madison_summaries = _inspect_apt_madison(
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

    prior_inputs = {
        "preflight": _summarize_preflight(path=preflight_json, data=prior_preflight_json),
        "package_decision": _summarize_package_decision(
            path=package_decision_json,
            data=prior_package_decision_json,
        ),
        "apt_plan": _summarize_apt_plan(path=apt_plan_json, data=prior_apt_plan_json),
    }
    evidence_matrix = _build_evidence_matrix(
        apt_policy_candidates=apt_policy_candidates,
        apt_show_summaries=apt_show_summaries,
        apt_depends_summaries=apt_depends_summaries,
        installed_package_matches=installed_package_matches,
    )
    (
        contract_status,
        recommended_next_slice,
        decision_reasons,
        risk_notes,
        stop_conditions,
    ) = _decide(
        host=host,
        evidence_matrix=evidence_matrix,
        apt_madison_summaries=apt_madison_summaries,
        apt_search_matches=apt_search_matches,
        ldconfig_tdjson_matches=ldconfig_tdjson_matches,
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
            "apt_policy_candidates": apt_policy_candidates,
            "apt_show_summaries": apt_show_summaries,
            "apt_depends_summaries": apt_depends_summaries,
            "apt_madison_summaries": apt_madison_summaries,
            "apt_search_matches": apt_search_matches,
            "installed_package_matches": installed_package_matches,
            "ldconfig_tdjson_matches": ldconfig_tdjson_matches,
        },
        "evidence_matrix": evidence_matrix,
        "decision_reasons": decision_reasons,
        "risk_notes": risk_notes,
        "stop_conditions": stop_conditions,
        "candidate_next_actions": _candidate_next_actions(),
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
            run_review(
                preflight_json=args.preflight_json,
                package_decision_json=args.package_decision_json,
                apt_plan_json=args.apt_plan_json,
            )
        ),
        end="",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
