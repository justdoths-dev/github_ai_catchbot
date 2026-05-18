from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


SCHEMA_VERSION = "dedicated_vps_tdjson_apt_install_plan_v1"
CONTRACT_NAME = "dedicated_vps_tdjson_apt_install_plan"

SUPPORTED_OS_IDS = {"ubuntu", "debian"}
PACKAGE_CANDIDATES = ("libtdjson", "libtdjson-dev", "tdlib", "tdlib-dev")
SEARCH_QUERIES = ("tdjson", "tdlib")

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
            "Produce a read-only tdjson apt install plan for later review. "
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
        or _is_apt_cache_show_command(normalized)
        or _is_apt_cache_depends_command(normalized)
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


def _is_apt_cache_show_command(normalized: Sequence[str]) -> bool:
    return len(normalized) == 3 and normalized[:2] == ("apt-cache", "show") and normalized[2] in PACKAGE_CANDIDATES


def _is_apt_cache_depends_command(normalized: Sequence[str]) -> bool:
    return len(normalized) == 3 and normalized[:2] == ("apt-cache", "depends") and normalized[2] in PACKAGE_CANDIDATES


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
        lines = [
            line.strip()[:240]
            for line in result["stdout"].splitlines()
            if line.strip()
        ]
        results.append(
            {
                "package": package,
                "command_available": result["available"],
                "returncode": result["returncode"],
                "lines": lines[:60],
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


def _policy_candidate_map(
    apt_policy_candidates: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    candidates: dict[str, str] = {}
    for item in apt_policy_candidates:
        package = item.get("package")
        candidate = item.get("candidate")
        if isinstance(package, str) and isinstance(candidate, str) and candidate:
            candidates[package] = candidate
    return candidates


def _search_packages(apt_search_matches: Sequence[Mapping[str, Any]]) -> list[str]:
    packages: list[str] = []
    for item in apt_search_matches:
        for match in item.get("matches", []):
            if isinstance(match, Mapping) and isinstance(match.get("package"), str):
                packages.append(match["package"])
    return packages


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


def _empty_selected_plan() -> dict[str, Any]:
    return {
        "package_name": None,
        "package_version_candidate": None,
        "reason": None,
        "install_method": None,
        "requires_apt_update": False,
        "future_operator_commands_not_run": [],
        "future_validation_commands_not_run": [],
        "future_rollback_commands_not_run": [],
    }


def _selected_plan(package: str, version: str, reason: str) -> dict[str, Any]:
    return {
        "package_name": package,
        "package_version_candidate": version,
        "reason": reason,
        "install_method": "apt",
        "requires_apt_update": False,
        "future_operator_commands_not_run": [f"sudo apt install {package}"],
        "future_validation_commands_not_run": _future_validation_commands(),
        "future_rollback_commands_not_run": [f"sudo apt remove {package}"],
    }


def _decide(
    *,
    host: Mapping[str, str | None],
    prior_package_decision: Mapping[str, Any],
    apt_policy_candidates: Sequence[Mapping[str, Any]],
    apt_search_matches: Sequence[Mapping[str, Any]],
) -> tuple[str, str, dict[str, Any], list[str], list[str], list[str]]:
    os_id = host.get("os_id")
    if os_id not in SUPPORTED_OS_IDS:
        return (
            "unsupported_host",
            "defer_manual_review",
            _empty_selected_plan(),
            ["Host OS is not recognized as Ubuntu/Debian from os-release."],
            ["No apt install plan is selected for unsupported or unreadable hosts."],
            ["Stop before any package-manager mutation and perform manual host review."],
        )

    decision_reasons: list[str] = []
    risk_notes: list[str] = []
    stop_conditions = [
        "Stop if any package-manager mutation is needed; this slice is report-only.",
        "Stop if runtime.env or secret values are needed.",
        "Stop if TDLib auth, Telegram network, DB, Redis, Alembic, Docker, systemd, collector, notifier, or rollout work is requested.",
    ]

    prior_next = prior_package_decision.get("recommended_next_slice")
    prior_conflict = (
        prior_package_decision.get("provided") is True
        and prior_next is not None
        and prior_next != "tdjson_apt_install_plan"
    )
    if prior_conflict:
        risk_notes.append(
            f"Explicit prior package decision recommended {prior_next}; apt install planning must not override that without strong apt evidence."
        )

    policy_candidates = _policy_candidate_map(apt_policy_candidates)
    if "libtdjson" in policy_candidates:
        reason = "apt-cache policy exposes a libtdjson candidate, which is the preferred runtime package name."
        if prior_conflict:
            decision_reasons.append("Strong apt evidence exists despite the explicit prior package-decision conflict.")
        decision_reasons.append(reason)
        return (
            "apt_install_plan_ready",
            "tdjson_apt_install_operator_execution",
            _selected_plan("libtdjson", policy_candidates["libtdjson"], reason),
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    if "libtdjson-dev" in policy_candidates:
        reason = "apt-cache policy exposes libtdjson-dev, but only the development package candidate is visible."
        decision_reasons.append(reason)
        risk_notes.append("libtdjson-dev may not be the runtime library package; the runtime package relationship must be reviewed before install.")
        if prior_conflict:
            return (
                "apt_install_plan_inconclusive",
                "defer_manual_review",
                _selected_plan("libtdjson-dev", policy_candidates["libtdjson-dev"], reason),
                decision_reasons,
                risk_notes,
                stop_conditions,
            )
        return (
            "apt_install_plan_ready",
            "tdjson_apt_install_operator_execution",
            _selected_plan("libtdjson-dev", policy_candidates["libtdjson-dev"], reason),
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    for package in ("tdlib", "tdlib-dev"):
        if package in policy_candidates:
            reason = f"apt-cache policy exposes {package}, but the package name does not prove libtdjson.so availability."
            decision_reasons.append(reason)
            risk_notes.append(f"{package} may not provide libtdjson.so; future validation must prove tdjson symbols before auth is considered.")
            if prior_conflict:
                return (
                    "apt_install_plan_inconclusive",
                    "defer_manual_review",
                    _selected_plan(package, policy_candidates[package], reason),
                    decision_reasons,
                    risk_notes,
                    stop_conditions,
                )
            return (
                "apt_install_plan_ready",
                "tdjson_apt_install_operator_execution",
                _selected_plan(package, policy_candidates[package], reason),
                decision_reasons,
                risk_notes,
                stop_conditions,
            )

    search_packages = _search_packages(apt_search_matches)
    clear_search_package = next((package for package in search_packages if package == "libtdjson"), None)
    if clear_search_package:
        reason = "apt-cache search found libtdjson, but apt-cache policy did not expose a candidate."
        decision_reasons.append(reason)
        risk_notes.append("Search-only evidence is weaker than apt-cache policy; operator review must confirm the repository candidate.")
        return (
            "apt_install_plan_inconclusive",
            "defer_manual_review",
            _empty_selected_plan(),
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    if search_packages:
        decision_reasons.append(
            f"apt-cache search found tdjson/tdlib-related package(s): {', '.join(search_packages[:5])}."
        )
        risk_notes.append("Search matches do not prove an installable package or libtdjson.so provider.")
        return (
            "apt_install_plan_inconclusive",
            "defer_manual_review",
            _empty_selected_plan(),
            decision_reasons,
            risk_notes,
            stop_conditions,
        )

    decision_reasons.append("Ubuntu/Debian host is supported, but no apt policy candidate was visible.")
    if prior_package_decision.get("recommended_next_slice") == "tdjson_apt_install_plan":
        risk_notes.append("Prior package decision expected an apt install plan, but current apt policy evidence shows no candidate.")
    risk_notes.append("A source-build plan may be needed, but source build commands are forbidden in this slice.")
    return (
        "apt_install_plan_inconclusive",
        "tdjson_source_build_plan",
        _empty_selected_plan(),
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
    prior_preflight_json: Mapping[str, Any] | None = None,
    prior_package_decision_json: Mapping[str, Any] | None = None,
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
    prior_preflight = _summarize_preflight(path=preflight_json, data=prior_preflight_json)
    prior_package_decision = _summarize_package_decision(
        path=package_decision_json,
        data=prior_package_decision_json,
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
        prior_package_decision=prior_package_decision,
        apt_policy_candidates=apt_policy_candidates,
        apt_search_matches=apt_search_matches,
    )

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_name": CONTRACT_NAME,
        "contract_status": contract_status,
        "recommended_next_slice": recommended_next_slice,
        "host": host,
        "prior_preflight": prior_preflight,
        "prior_package_decision": prior_package_decision,
        "inspection": {
            "command_availability": command_availability,
            "apt_policy_candidates": apt_policy_candidates,
            "apt_show_summaries": apt_show_summaries,
            "apt_depends_summaries": apt_depends_summaries,
            "apt_search_matches": apt_search_matches,
            "installed_package_matches": installed_package_matches,
            "ldconfig_tdjson_matches": ldconfig_tdjson_matches,
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
                preflight_json=args.preflight_json,
                package_decision_json=args.package_decision_json,
            )
        ),
        end="",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
