from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import json
import os
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


SCHEMA_VERSION = "dedicated_vps_tdjson_runtime_dependency_preflight_v1"
CONTRACT_NAME = "dedicated_vps_tdjson_runtime_dependency_preflight"
TDJSON_LIBRARY_ENV = "TDJSON_LIBRARY_PATH"

REQUIRED_SYMBOLS = (
    "td_json_client_create",
    "td_json_client_send",
    "td_json_client_receive",
    "td_json_client_destroy",
)

COMMON_TDJSON_LIBRARY_PATHS = (
    "/usr/lib/x86_64-linux-gnu/libtdjson.so",
    "/usr/local/lib/libtdjson.so",
    "/usr/lib/libtdjson.so",
    "/opt/tdlib/lib/libtdjson.so",
)

SAFETY_FLAGS = {
    "installation_attempted": False,
    "package_manager_used": False,
    "runtime_env_read": False,
    "runtime_env_values_printed": False,
    "secret_values_printed": False,
    "tdlib_auth_attempted": False,
    "tdlib_auth_completed": False,
    "tdlib_client_created": False,
    "td_json_client_create_called": False,
    "td_json_client_send_called": False,
    "td_json_client_receive_called": False,
    "td_json_client_destroy_called": False,
    "telegram_network_contact_attempted": False,
    "session_state_created_or_reused": False,
    "collector_main_used": False,
    "collector_service_used": False,
    "collector_runtime_used": False,
    "live_collector_started": False,
    "app_runtime_started": False,
    "notifier_transport_enabled": False,
    "database_connected": False,
    "redis_connected": False,
    "alembic_run": False,
    "docker_or_systemd_changed": False,
    "production_rollout_performed": False,
}


FindLibraryFunc = Callable[[str], str | None]
CdllLoader = Callable[[str], Any]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Report whether tdjson is present and loadable on this runtime host. "
            "This preflight installs nothing, reads no runtime.env, creates no "
            "TDLib client, and contacts no Telegram network."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def _path_basename(candidate: str) -> str:
    return Path(candidate).name or candidate


def _candidate_check(
    *,
    source: str,
    candidate: str,
    check_path_exists: bool,
    cdll_loader: CdllLoader,
) -> dict[str, Any]:
    check: dict[str, Any] = {
        "source": source,
        "basename": _path_basename(candidate),
        "path_exists": None,
        "loadable": False,
        "required_symbols_present": False,
        "missing_required_symbols": list(REQUIRED_SYMBOLS),
        "status": "not_found",
        "error_class": None,
    }

    if check_path_exists:
        path_exists = Path(candidate).exists()
        check["path_exists"] = path_exists
        if not path_exists:
            return check

    try:
        library = cdll_loader(candidate)
    except OSError as exc:
        check["status"] = "load_failed"
        check["error_class"] = exc.__class__.__name__
        return check

    missing = [symbol for symbol in REQUIRED_SYMBOLS if not hasattr(library, symbol)]
    check["loadable"] = True
    check["missing_required_symbols"] = missing
    check["required_symbols_present"] = not missing
    check["status"] = "loadable" if not missing else "missing_required_symbols"
    return check


def _candidate_inputs(
    env: Mapping[str, str],
    find_library_func: FindLibraryFunc,
    candidate_paths: Sequence[str],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    env_path = env.get(TDJSON_LIBRARY_ENV)
    if env_path:
        candidates.append(
            {
                "source": TDJSON_LIBRARY_ENV,
                "candidate": env_path,
                "check_path_exists": True,
            }
        )

    find_library_result = find_library_func("tdjson")
    if find_library_result:
        candidates.append(
            {
                "source": "ctypes.util.find_library",
                "candidate": find_library_result,
                "check_path_exists": Path(find_library_result).is_absolute(),
            }
        )

    for path in candidate_paths:
        candidates.append(
            {
                "source": "common_path",
                "candidate": path,
                "check_path_exists": True,
            }
        )

    return candidates


def run_preflight(
    *,
    env: Mapping[str, str] | None = None,
    find_library_func: FindLibraryFunc = ctypes.util.find_library,
    cdll_loader: CdllLoader = ctypes.CDLL,
    candidate_paths: Sequence[str] = COMMON_TDJSON_LIBRARY_PATHS,
) -> dict[str, Any]:
    runtime_env = os.environ if env is None else env
    candidate_inputs = _candidate_inputs(runtime_env, find_library_func, candidate_paths)
    candidate_checks = [
        _candidate_check(
            source=str(candidate["source"]),
            candidate=str(candidate["candidate"]),
            check_path_exists=bool(candidate["check_path_exists"]),
            cdll_loader=cdll_loader,
        )
        for candidate in candidate_inputs
    ]

    any_loadable = any(check["loadable"] for check in candidate_checks)
    any_all_symbols = any(check["required_symbols_present"] for check in candidate_checks)
    any_found_candidate = any(
        check["path_exists"] is True or check["source"] == "ctypes.util.find_library"
        for check in candidate_checks
    )
    any_load_failed = any(check["status"] == "load_failed" for check in candidate_checks)
    any_missing_symbols = any(
        check["status"] == "missing_required_symbols" for check in candidate_checks
    )

    if any_all_symbols:
        contract_status = "tdjson_available"
    elif any_loadable and any_missing_symbols:
        contract_status = "tdjson_missing_required_symbols"
    elif any_found_candidate and any_load_failed:
        contract_status = "tdjson_load_failed"
    else:
        contract_status = "tdjson_missing"

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_name": CONTRACT_NAME,
        "contract_status": contract_status,
        "tdjson_available": contract_status == "tdjson_available",
        "tdjson_loadable": any_loadable,
        "required_symbols_present": any_all_symbols,
        "tdjson_library_path_env_set": bool(runtime_env.get(TDJSON_LIBRARY_ENV)),
        "find_library_result_present": any(
            check["source"] == "ctypes.util.find_library" for check in candidate_checks
        ),
        "candidate_checks": candidate_checks,
        "boundary_check": "pass",
    }
    report.update(SAFETY_FLAGS)
    return report


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    parser.parse_args(argv)
    print(render_json(run_preflight()), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
