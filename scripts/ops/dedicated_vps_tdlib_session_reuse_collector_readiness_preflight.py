from __future__ import annotations

import argparse
import importlib
import json
import re
import sys
import types
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Sequence


REPORT_TYPE = "dedicated_vps_tdlib_session_reuse_collector_readiness_preflight_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"

REQUIRED_RUNTIME_KEYS = (
    "APP_ENV",
    "COLLECTOR_MODE",
    "DATABASE_URL",
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_PHONE_NUMBER",
    "TDLIB_DB_ENCRYPTION_KEY",
    "TDLIB_STATE_DIR",
    "TDLIB_FILES_DIR",
)

SIDE_EFFECT_FLAGS = (
    "runtime_env_values_printed",
    "secret_values_printed",
    "tdlib_session_values_printed",
    "tdlib_auth_attempted",
    "login_code_prompted",
    "login_code_submitted",
    "login_code_value_printed",
    "login_code_value_stored",
    "live_collector_started",
    "collector_runtime_started",
    "collector_main_used",
    "notifier_transport_enabled",
    "database_connected",
    "redis_connected",
    "alembic_run",
    "docker_or_systemd_changed",
    "files_mutated",
)

PLACEHOLDER_EXACT_VALUES = {
    "...",
    "changeme",
    "change_me",
    "change-me",
    "todo",
    "tbd",
    "placeholder",
    "replace",
    "replace_me",
    "replace-me",
    "example",
    "example_value",
    "example-value",
    "dummy",
    "none",
    "null",
}
PLACEHOLDER_FRAGMENTS = ("changeme", "placeholder", "replace", "your")

TdjsonAvailabilityChecker = Callable[[Path, Mapping[str, str]], None]
CollectorConfigBuilder = Callable[[Path, Mapping[str, str]], None]


@dataclass(frozen=True, slots=True)
class PreflightResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class DirectoryMetadata:
    present: bool
    is_dir: bool
    metadata_checked: bool
    has_entries: bool
    file_count_bucket: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Check dedicated VPS TDLib session reuse collector readiness without "
            "starting collector runtime, connecting services, or performing auth."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--repo-root", default=None)
    return parser


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _base_report(repo_root: Path, runtime_env_path: str) -> dict[str, Any]:
    report: dict[str, Any] = {
        "report_type": REPORT_TYPE,
        "contract_status": "blocked_runtime_env_unreadable",
        "checks_failed": [],
        "failures": [],
        "repo_root": str(repo_root),
        "repo_code_state_checked": True,
        "repo_code_state_safe": _repo_code_state_safe(repo_root),
        "runtime_env_path": runtime_env_path,
        "runtime_env_read": False,
        "tdjson_available": False,
        "tdjson_error_type": None,
        "collector_config_built": False,
        "collector_config_error_type": None,
        "required_runtime_keys_present": {
            key: False for key in REQUIRED_RUNTIME_KEYS
        },
        "required_runtime_keys_nonempty": {
            key: False for key in REQUIRED_RUNTIME_KEYS
        },
        "required_runtime_keys_missing": list(REQUIRED_RUNTIME_KEYS),
        "required_runtime_keys_empty": [],
        "placeholder_like_required_values_detected": False,
        "placeholder_like_required_keys": [],
        "TELEGRAM_API_ID_present": False,
        "TELEGRAM_API_ID_positive_int": False,
        "TELEGRAM_API_HASH_present": False,
        "TELEGRAM_API_HASH_nonempty": False,
        "TELEGRAM_API_HASH_hex32_like": False,
        "TELEGRAM_PHONE_NUMBER_present": False,
        "TELEGRAM_PHONE_NUMBER_e164_like": False,
        "TDLIB_DB_ENCRYPTION_KEY_present": False,
        "TDLIB_DB_ENCRYPTION_KEY_nontrivial_len": False,
        "DATABASE_URL_present": False,
        "DATABASE_URL_nonempty": False,
        "REDIS_URL_present": False,
        "REDIS_URL_nonempty": False,
        "tdlib_state_dir_present": False,
        "tdlib_state_dir_is_dir": False,
        "tdlib_files_dir_present": False,
        "tdlib_files_dir_is_dir": False,
        "tdlib_session_reuse_candidate": False,
        "tdlib_session_metadata_checked": False,
        "tdlib_session_file_count_bucket": "unknown",
        "tdlib_state_dir_has_entries": False,
        "tdjson_transport_initialized": False,
    }
    for flag in SIDE_EFFECT_FLAGS:
        report[flag] = False
    return report


def _repo_code_state_safe(repo_root: Path) -> bool:
    required_files = (
        "src/services/collector_telegram/config.py",
        "src/services/collector_telegram/tdlib_client.py",
    )
    return all((repo_root / relative).is_file() for relative in required_files)


def _failure(report: dict[str, Any], check: str, message: str) -> None:
    report["checks_failed"].append(check)
    report["failures"].append({"check": check, "message": message})


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def parse_runtime_env_text(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        values[key] = _strip_optional_quotes(raw_value)
    return values


def parse_runtime_env_file(path: str | Path) -> dict[str, str]:
    return parse_runtime_env_text(
        Path(path).read_text(encoding="utf-8", errors="replace")
    )


def _is_nonempty(value: str | None) -> bool:
    return value is not None and bool(value.strip())


def _positive_int(value: str | None) -> bool:
    if value is None:
        return False
    stripped = value.strip()
    if not re.fullmatch(r"[0-9]+", stripped):
        return False
    return int(stripped) > 0


def _is_placeholder_like(key: str, value: str) -> bool:
    stripped = value.strip()
    if key in {"TDLIB_STATE_DIR", "TDLIB_FILES_DIR"}:
        stripped = Path(stripped).name
    lowered = stripped.lower()
    compact = re.sub(r"[\s_-]+", "", lowered)
    if not stripped:
        return False
    if lowered in PLACEHOLDER_EXACT_VALUES or compact in PLACEHOLDER_EXACT_VALUES:
        return True
    if stripped.startswith("<") and stripped.endswith(">"):
        return True
    return any(fragment in compact for fragment in PLACEHOLDER_FRAGMENTS)


def _bucket_count(count: int | None) -> str:
    if count is None:
        return "unknown"
    if count == 0:
        return "zero"
    if count <= 5:
        return "one_to_five"
    if count <= 20:
        return "six_to_twenty"
    return "over_twenty"


def _inspect_directory_metadata(path_value: str | None) -> DirectoryMetadata:
    if not _is_nonempty(path_value):
        return DirectoryMetadata(
            present=False,
            is_dir=False,
            metadata_checked=False,
            has_entries=False,
            file_count_bucket="unknown",
        )

    path = Path(str(path_value).strip())
    try:
        present = path.exists()
        is_dir = path.is_dir()
    except OSError:
        return DirectoryMetadata(
            present=False,
            is_dir=False,
            metadata_checked=False,
            has_entries=False,
            file_count_bucket="unknown",
        )

    if not present or not is_dir:
        return DirectoryMetadata(
            present=present,
            is_dir=is_dir,
            metadata_checked=True,
            has_entries=False,
            file_count_bucket="unknown",
        )

    try:
        count = 0
        for count, _entry in enumerate(path.iterdir(), start=1):
            if count > 20:
                break
    except OSError:
        return DirectoryMetadata(
            present=True,
            is_dir=True,
            metadata_checked=False,
            has_entries=False,
            file_count_bucket="unknown",
        )

    return DirectoryMetadata(
        present=True,
        is_dir=True,
        metadata_checked=True,
        has_entries=count > 0,
        file_count_bucket=_bucket_count(count),
    )


def _private_collector_package(repo_root: Path) -> str:
    resolved = str(repo_root.resolve())
    suffix = abs(hash(resolved))
    return f"_tdlib_readiness_collector_telegram_{suffix}"


def _load_collector_module(repo_root: Path, module_name: str) -> ModuleType:
    package_dir = repo_root / "src" / "services" / "collector_telegram"
    package_name = _private_collector_package(repo_root)
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(package_dir)]  # type: ignore[attr-defined]
        package.__package__ = package_name
        sys.modules[package_name] = package
    return importlib.import_module(f"{package_name}.{module_name}")


def _build_collector_config(repo_root: Path, values: Mapping[str, str]) -> None:
    config_module = _load_collector_module(repo_root, "config")
    config_module.CollectorTelegramConfig.from_env(values)


def _assert_tdjson_available(repo_root: Path, values: Mapping[str, str]) -> None:
    tdlib_client_module = _load_collector_module(repo_root, "tdlib_client")
    library_path = values.get("TDJSON_LIBRARY_PATH")
    transport = tdlib_client_module.TDJsonTransport(
        library_path=library_path.strip() if _is_nonempty(library_path) else None
    )
    transport.assert_available()


def _apply_required_key_checks(
    report: dict[str, Any], values: Mapping[str, str]
) -> None:
    present = {key: key in values for key in REQUIRED_RUNTIME_KEYS}
    nonempty = {key: _is_nonempty(values.get(key)) for key in REQUIRED_RUNTIME_KEYS}
    missing = [key for key in REQUIRED_RUNTIME_KEYS if not present[key]]
    empty = [key for key in REQUIRED_RUNTIME_KEYS if present[key] and not nonempty[key]]

    report["required_runtime_keys_present"] = present
    report["required_runtime_keys_nonempty"] = nonempty
    report["required_runtime_keys_missing"] = missing
    report["required_runtime_keys_empty"] = empty
    if missing or empty:
        _failure(
            report,
            "runtime_env.required_keys",
            "One or more required runtime env keys are missing or empty.",
        )


def _apply_safe_semantic_checks(
    report: dict[str, Any], values: Mapping[str, str]
) -> None:
    api_id = values.get("TELEGRAM_API_ID")
    api_hash = values.get("TELEGRAM_API_HASH")
    phone_number = values.get("TELEGRAM_PHONE_NUMBER")
    encryption_key = values.get("TDLIB_DB_ENCRYPTION_KEY")
    database_url = values.get("DATABASE_URL")
    redis_url = values.get("REDIS_URL")

    report["TELEGRAM_API_ID_present"] = api_id is not None
    report["TELEGRAM_API_ID_positive_int"] = _positive_int(api_id)
    report["TELEGRAM_API_HASH_present"] = api_hash is not None
    report["TELEGRAM_API_HASH_nonempty"] = _is_nonempty(api_hash)
    report["TELEGRAM_API_HASH_hex32_like"] = bool(
        api_hash and re.fullmatch(r"[0-9a-fA-F]{32}", api_hash.strip())
    )
    report["TELEGRAM_PHONE_NUMBER_present"] = phone_number is not None
    report["TELEGRAM_PHONE_NUMBER_e164_like"] = bool(
        phone_number and re.fullmatch(r"\+[1-9][0-9]{6,14}", phone_number.strip())
    )
    report["TDLIB_DB_ENCRYPTION_KEY_present"] = encryption_key is not None
    report["TDLIB_DB_ENCRYPTION_KEY_nontrivial_len"] = bool(
        encryption_key and len(encryption_key.strip()) >= 16
    )
    report["DATABASE_URL_present"] = database_url is not None
    report["DATABASE_URL_nonempty"] = _is_nonempty(database_url)
    report["REDIS_URL_present"] = redis_url is not None
    report["REDIS_URL_nonempty"] = _is_nonempty(redis_url)


def _apply_placeholder_checks(
    report: dict[str, Any], values: Mapping[str, str]
) -> None:
    placeholder_keys = [
        key
        for key in REQUIRED_RUNTIME_KEYS
        if key in values and _is_placeholder_like(key, values[key])
    ]
    report["placeholder_like_required_values_detected"] = bool(placeholder_keys)
    report["placeholder_like_required_keys"] = placeholder_keys
    if placeholder_keys:
        _failure(
            report,
            "runtime_env.placeholder_like_required_values",
            "One or more required runtime env values look like placeholders.",
        )


def _apply_collector_config_check(
    report: dict[str, Any],
    repo_root: Path,
    values: Mapping[str, str],
    collector_config_builder: CollectorConfigBuilder | None,
) -> None:
    try:
        if collector_config_builder is None:
            _build_collector_config(repo_root, values)
        else:
            collector_config_builder(repo_root, values)
    except Exception as exc:
        report["collector_config_built"] = False
        report["collector_config_error_type"] = type(exc).__name__
        _failure(
            report,
            "collector_config.invalid",
            "CollectorTelegramConfig could not be built from runtime env keys.",
        )
        return
    report["collector_config_built"] = True
    report["collector_config_error_type"] = None


def _apply_tdjson_check(
    report: dict[str, Any],
    repo_root: Path,
    values: Mapping[str, str],
    tdjson_availability_checker: TdjsonAvailabilityChecker | None,
) -> None:
    try:
        if tdjson_availability_checker is None:
            _assert_tdjson_available(repo_root, values)
        else:
            tdjson_availability_checker(repo_root, values)
    except Exception as exc:
        report["tdjson_available"] = False
        report["tdjson_error_type"] = type(exc).__name__
        _failure(
            report,
            "tdjson.unavailable",
            "tdjson is not available through the configured loader path resolution.",
        )
        return
    report["tdjson_available"] = True
    report["tdjson_error_type"] = None


def _apply_tdlib_directory_checks(
    report: dict[str, Any], values: Mapping[str, str]
) -> None:
    state_metadata = _inspect_directory_metadata(values.get("TDLIB_STATE_DIR"))
    files_metadata = _inspect_directory_metadata(values.get("TDLIB_FILES_DIR"))

    report["tdlib_state_dir_present"] = state_metadata.present
    report["tdlib_state_dir_is_dir"] = state_metadata.is_dir
    report["tdlib_files_dir_present"] = files_metadata.present
    report["tdlib_files_dir_is_dir"] = files_metadata.is_dir
    report["tdlib_session_metadata_checked"] = (
        state_metadata.metadata_checked and files_metadata.metadata_checked
    )
    report["tdlib_session_file_count_bucket"] = state_metadata.file_count_bucket
    report["tdlib_state_dir_has_entries"] = state_metadata.has_entries
    report["tdlib_session_reuse_candidate"] = (
        state_metadata.present
        and state_metadata.is_dir
        and state_metadata.metadata_checked
        and state_metadata.has_entries
        and files_metadata.present
        and files_metadata.is_dir
        and files_metadata.metadata_checked
    )

    if not state_metadata.present:
        _failure(report, "tdlib_state_dir.missing", "TDLib state directory is absent.")
    elif not state_metadata.is_dir:
        _failure(
            report,
            "tdlib_state_dir.not_directory",
            "TDLib state path is not a directory.",
        )
    if not files_metadata.present or not files_metadata.is_dir:
        _failure(
            report,
            "tdlib_files_dir.missing_or_not_directory",
            "TDLib files path is missing or is not a directory.",
        )
    if not report["tdlib_session_reuse_candidate"]:
        _failure(
            report,
            "tdlib_session_reuse.not_confirmed",
            "Existing TDLib session reuse cannot be inferred from safe metadata.",
        )


def _apply_contract_status(report: dict[str, Any]) -> None:
    checks = set(report["checks_failed"])
    if "runtime_env.unreadable" in checks:
        report["contract_status"] = "blocked_runtime_env_unreadable"
    elif "runtime_env.required_keys" in checks:
        report["contract_status"] = "blocked_runtime_env_required_keys_missing"
    elif "runtime_env.placeholder_like_required_values" in checks:
        report["contract_status"] = "blocked_runtime_env_placeholder_like_values"
    elif "collector_config.invalid" in checks:
        report["contract_status"] = "blocked_collector_config_invalid"
    elif "tdjson.unavailable" in checks:
        report["contract_status"] = "blocked_tdjson_unavailable"
    elif "tdlib_state_dir.missing" in checks:
        report["contract_status"] = "blocked_tdlib_state_dir_missing"
    elif "tdlib_session_reuse.not_confirmed" in checks:
        report["contract_status"] = "blocked_tdlib_session_reuse_not_confirmed"
    else:
        report["contract_status"] = "collector_readiness_preflight_passed"


def generate_report(
    *,
    repo_root: str | Path | None = None,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    tdjson_availability_checker: TdjsonAvailabilityChecker | None = None,
    collector_config_builder: CollectorConfigBuilder | None = None,
) -> PreflightResult:
    resolved_repo_root = (
        Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    )
    report = _base_report(resolved_repo_root, str(runtime_env_path))

    try:
        values = parse_runtime_env_file(runtime_env_path)
    except Exception:
        _failure(
            report,
            "runtime_env.unreadable",
            "runtime env file could not be read safely.",
        )
        _apply_contract_status(report)
        return PreflightResult(exit_code=1, report=report)

    report["runtime_env_read"] = True
    _apply_required_key_checks(report, values)
    _apply_safe_semantic_checks(report, values)
    _apply_placeholder_checks(report, values)
    _apply_collector_config_check(
        report,
        resolved_repo_root,
        values,
        collector_config_builder,
    )
    _apply_tdjson_check(
        report,
        resolved_repo_root,
        values,
        tdjson_availability_checker,
    )
    _apply_tdlib_directory_checks(report, values)
    _apply_contract_status(report)
    return PreflightResult(
        exit_code=0
        if report["contract_status"] == "collector_readiness_preflight_passed"
        else 1,
        report=report,
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        repo_root=args.repo_root,
        runtime_env_path=args.runtime_env_path,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
