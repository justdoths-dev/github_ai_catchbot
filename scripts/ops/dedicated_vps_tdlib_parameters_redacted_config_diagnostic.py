from __future__ import annotations

import argparse
import ctypes.util
import json
import os
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from scripts.ops import (
    dedicated_vps_tdlib_session_reuse_collector_readiness_preflight as session_preflight,
)


SCHEMA_VERSION = "dedicated_vps_tdlib_parameters_redacted_config_diagnostic_v1"
SCRIPT_NAME = "dedicated_vps_tdlib_parameters_redacted_config_diagnostic"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"

REQUIRED_PARAMETER_FIELDS = (
    "api_id",
    "api_hash",
    "database_directory",
    "files_directory",
    "database_encryption_key",
    "use_message_database",
    "use_secret_chats",
    "system_language_code",
    "device_model",
    "application_version",
    "system_version",
)
BOOLEAN_PARAMETER_FIELDS = (
    "use_message_database",
    "use_secret_chats",
    "enable_storage_optimizer",
    "use_test_dc",
    "use_file_database",
    "use_chat_info_database",
)
PATH_PARAMETER_FIELDS = ("database_directory", "files_directory")
SECRET_PARAMETER_FIELDS = ("api_hash", "database_encryption_key")

SIDE_EFFECT_FLAG_NAMES = (
    "database_mutation_performed",
    "redis_mutation_performed",
    "telegram_api_called",
    "tdlib_initialized",
    "tdlib_send_called",
    "tdlib_receive_called",
    "tdlib_auth_attempted",
    "tdlib_public_username_resolve_called",
    "tdlib_join_called",
    "tdlib_history_fetch_called",
    "live_collector_started",
    "collector_runtime_started",
    "notifier_transport_enabled",
    "outbox_relay_started",
    "router_normalizer_started",
    "source_messages_written",
    "source_message_versions_written",
    "event_outbox_written",
    "alembic_upgrade_run",
    "alembic_downgrade_run",
    "alembic_stamp_run",
    "docker_or_systemd_changed",
    "files_mutated_outside_repo",
)

ParameterBuilder = Callable[[Path, Any, Mapping[str, str]], Mapping[str, Any]]
RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
CollectorConfigFactory = Callable[[Mapping[str, str]], Any]
TdjsonAvailabilityChecker = Callable[[Mapping[str, str]], Mapping[str, Any] | bool]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class SecretSourceMetadata:
    configured: bool
    source_kind: str
    non_empty: bool


class _ForbiddenTDLibTransport:
    async def initialize(self) -> None:
        raise RuntimeError("TDLib initialization is forbidden in this diagnostic")

    async def send(self, _request: Mapping[str, Any]) -> None:
        raise RuntimeError("TDLib send is forbidden in this diagnostic")

    async def receive(self, _timeout: float) -> dict[str, Any] | None:
        raise RuntimeError("TDLib receive is forbidden in this diagnostic")

    async def close(self) -> None:
        return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare redacted TDLib parameter structures for the auth/session path "
            "and public username resolve path without initializing TDLib."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--repo-root", default=None)
    return parser


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _side_effects() -> dict[str, bool]:
    return {flag: False for flag in SIDE_EFFECT_FLAG_NAMES}


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "contract_status": "blocked_runtime_env_unreadable",
        "checks_failed": [],
        "runtime_env_read": False,
        "collector_config_built": False,
        "auth_path_parameters_inspected": False,
        "resolve_path_parameters_inspected": False,
        "parameter_shapes_equivalent": False,
        "required_parameter_fields_present": {
            field: False for field in REQUIRED_PARAMETER_FIELDS
        },
        "required_parameter_fields_missing": list(REQUIRED_PARAMETER_FIELDS),
        "required_parameter_field_types_ok": False,
        "required_parameter_field_type_failures": [],
        "tdlib_state_dir_present": False,
        "tdlib_state_dir_is_dir": False,
        "tdlib_state_dir_writable": False,
        "tdlib_files_dir_present": False,
        "tdlib_files_dir_is_dir": False,
        "tdlib_files_dir_writable": False,
        "tdlib_db_encryption_key_configured": False,
        "tdlib_db_encryption_key_source_kind": "absent",
        "tdlib_db_encryption_key_non_empty": False,
        "tdlib_database_directory_kind": "absent",
        "tdlib_files_directory_kind": "absent",
        "tdjson_library_path_present": False,
        "tdjson_default_path_checked": False,
        "tdjson_available_import_check_performed": False,
        "tdjson_available": False,
        "auth_path_parameter_shape_summary": {},
        "resolve_path_parameter_shape_summary": {},
        "differences_summary": [],
        "operator_next_action": (
            "Read the runtime env and compare redacted TDLib parameter structures "
            "before retrying any live TDLib authorization or public username resolve."
        ),
        "side_effects": _side_effects(),
    }


def _set_check(report: dict[str, Any], check: str) -> None:
    if check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _ensure_repo_on_path(repo_root: Path) -> None:
    root = str(repo_root)
    if root not in sys.path:
        sys.path.insert(0, root)


def _build_collector_config_default(
    repo_root: Path,
    runtime_env: Mapping[str, str],
) -> Any:
    _ensure_repo_on_path(repo_root)
    from src.services.collector_telegram.config import CollectorTelegramConfig

    return CollectorTelegramConfig.from_env(runtime_env)


def _default_auth_parameter_builder(
    repo_root: Path,
    config: Any,
    _runtime_env: Mapping[str, str],
) -> Mapping[str, Any]:
    _ensure_repo_on_path(repo_root)
    from src.services.collector_telegram.auth_fsm import AuthorizationFSM

    fsm = AuthorizationFSM(config)
    result = fsm.handle_state({"@type": "authorizationStateWaitTdlibParameters"})
    if not result.requests:
        raise RuntimeError("auth path did not build setTdlibParameters")
    return dict(result.requests[0])


def _default_resolve_parameter_builder(
    repo_root: Path,
    _config: Any,
    runtime_env: Mapping[str, str],
) -> Mapping[str, Any]:
    _ensure_repo_on_path(repo_root)
    from scripts.ops.dedicated_vps_telegram_channel_registry_public_username_resolve_operator import (
        TDLibPublicUsernameResolver,
    )

    resolver = TDLibPublicUsernameResolver(
        runtime_env,
        transport=_ForbiddenTDLibTransport(),
    )
    client = getattr(resolver, "_client")
    return dict(client.build_set_tdlib_parameters_request().payload)


def _read_secret_source_metadata(
    values: Mapping[str, str],
    env_name: str,
) -> SecretSourceMetadata:
    file_env_name = f"{env_name}_FILE"
    file_value = values.get(file_env_name)
    direct_value = values.get(env_name)

    if isinstance(file_value, str) and file_value.strip():
        try:
            file_text = Path(file_value.strip()).read_text(
                encoding="utf-8",
                errors="replace",
            )
            non_empty = bool(file_text.strip())
        except OSError:
            non_empty = False
        return SecretSourceMetadata(
            configured=True,
            source_kind="file",
            non_empty=non_empty,
        )

    if direct_value is not None:
        return SecretSourceMetadata(
            configured=True,
            source_kind="env",
            non_empty=bool(str(direct_value).strip()),
        )

    return SecretSourceMetadata(
        configured=False,
        source_kind="absent",
        non_empty=False,
    )


def _is_non_empty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _positive_int(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value > 0
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip()) > 0
    return False


def _preliminary_required_field_presence(
    values: Mapping[str, str],
) -> dict[str, bool]:
    api_hash = _read_secret_source_metadata(values, "TELEGRAM_API_HASH")
    encryption_key = _read_secret_source_metadata(values, "TDLIB_DB_ENCRYPTION_KEY")
    state_dir = values.get("TDLIB_STATE_DIR")
    files_dir = values.get("TDLIB_FILES_DIR") or state_dir

    present = {field: True for field in REQUIRED_PARAMETER_FIELDS}
    present["api_id"] = _is_non_empty(values.get("TELEGRAM_API_ID"))
    present["api_hash"] = api_hash.non_empty
    present["database_directory"] = _is_non_empty(state_dir)
    present["files_directory"] = _is_non_empty(files_dir)
    present["database_encryption_key"] = encryption_key.non_empty
    return present


def _preliminary_type_failures(values: Mapping[str, str]) -> list[str]:
    api_id = values.get("TELEGRAM_API_ID")
    if api_id is not None and not _positive_int(api_id):
        return ["api_id.expected_positive_int"]
    return []


def _apply_db_key_metadata(
    report: dict[str, Any],
    values: Mapping[str, str],
) -> SecretSourceMetadata:
    metadata = _read_secret_source_metadata(values, "TDLIB_DB_ENCRYPTION_KEY")
    report["tdlib_db_encryption_key_configured"] = metadata.configured
    report["tdlib_db_encryption_key_source_kind"] = metadata.source_kind
    report["tdlib_db_encryption_key_non_empty"] = metadata.non_empty
    return metadata


def _path_kind(value: Any) -> str:
    if value is None:
        return "absent"
    if not isinstance(value, str):
        return "non_string"
    stripped = value.strip()
    if not stripped:
        return "empty"
    path = Path(stripped)
    return "absolute_path" if path.is_absolute() else "relative_path"


def _inspect_path(value: Any) -> dict[str, Any]:
    metadata = {
        "kind": _path_kind(value),
        "path_exists": False,
        "path_is_dir": False,
        "path_writable": False,
    }
    if not isinstance(value, str) or not value.strip():
        return metadata

    try:
        path = Path(value.strip())
        metadata["path_exists"] = path.exists()
        metadata["path_is_dir"] = path.is_dir()
        metadata["path_writable"] = bool(
            metadata["path_exists"]
            and metadata["path_is_dir"]
            and os.access(path, os.W_OK)
        )
    except OSError:
        return metadata
    return metadata


def _extract_parameter_fields(payload: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]]:
    parameters = payload.get("parameters")
    if isinstance(parameters, Mapping):
        return "nested_parameters", parameters
    return "flat", payload


def _safe_request_type(payload: Mapping[str, Any]) -> str | None:
    raw_type = payload.get("@type")
    return raw_type if isinstance(raw_type, str) else None


def _type_category(value: Any, *, field: str | None = None) -> str:
    if value is None:
        return "absent"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, str):
        if field in PATH_PARAMETER_FIELDS:
            return "path" if value.strip() else "empty_string"
        return "non_empty_string" if value.strip() else "empty_string"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, list):
        return "array"
    return type(value).__name__


def _field_presence(
    fields: Mapping[str, Any],
    encryption_key_source: SecretSourceMetadata,
) -> dict[str, bool]:
    present: dict[str, bool] = {}
    for field in REQUIRED_PARAMETER_FIELDS:
        if field == "database_encryption_key" and field not in fields:
            present[field] = encryption_key_source.non_empty
        else:
            present[field] = field in fields
    return present


def _field_type_failures(
    fields: Mapping[str, Any],
    *,
    label: str,
    encryption_key_source: SecretSourceMetadata,
) -> list[str]:
    failures: list[str] = []

    api_id = fields.get("api_id")
    if "api_id" in fields and (
        isinstance(api_id, bool) or not isinstance(api_id, int) or api_id <= 0
    ):
        failures.append(f"{label}.api_id.expected_positive_int")

    for field in (
        "api_hash",
        "database_directory",
        "files_directory",
        "system_language_code",
        "device_model",
        "application_version",
        "system_version",
    ):
        if field in fields and (not isinstance(fields[field], str) or not fields[field].strip()):
            failures.append(f"{label}.{field}.expected_non_empty_string")

    if "database_encryption_key" in fields:
        value = fields["database_encryption_key"]
        if not isinstance(value, str) or not value.strip():
            failures.append(f"{label}.database_encryption_key.expected_non_empty_string")
    elif not encryption_key_source.non_empty:
        failures.append(f"{label}.database_encryption_key.source_expected_non_empty")

    for field in BOOLEAN_PARAMETER_FIELDS:
        if field in fields and not isinstance(fields[field], bool):
            failures.append(f"{label}.{field}.expected_bool")

    return failures


def _shape_summary(
    payload: Mapping[str, Any],
    *,
    encryption_key_source: SecretSourceMetadata,
) -> dict[str, Any]:
    layout_kind, fields = _extract_parameter_fields(payload)
    return {
        "request_type": _safe_request_type(payload),
        "layout_kind": layout_kind,
        "field_count": len(fields),
        "fields_present": sorted(str(field) for field in fields),
        "required_fields_present": _field_presence(fields, encryption_key_source),
        "field_type_categories": {
            field: _type_category(fields.get(field), field=field)
            for field in sorted(set(REQUIRED_PARAMETER_FIELDS) | set(BOOLEAN_PARAMETER_FIELDS))
        },
        "path_fields": {
            field: _inspect_path(fields.get(field)) for field in PATH_PARAMETER_FIELDS
        },
        "secret_fields": {
            field: {
                "present": field in fields
                or (field == "database_encryption_key" and encryption_key_source.non_empty),
                "non_empty": (
                    bool(fields.get(field).strip())
                    if isinstance(fields.get(field), str)
                    else (
                        encryption_key_source.non_empty
                        if field == "database_encryption_key" and field not in fields
                        else False
                    )
                ),
                "redacted": True,
            }
            for field in SECRET_PARAMETER_FIELDS
        },
    }


def _compare_shapes(
    auth_payload: Mapping[str, Any],
    resolve_payload: Mapping[str, Any],
) -> list[str]:
    differences: list[str] = []
    auth_layout, auth_fields = _extract_parameter_fields(auth_payload)
    resolve_layout, resolve_fields = _extract_parameter_fields(resolve_payload)

    if _safe_request_type(auth_payload) != _safe_request_type(resolve_payload):
        differences.append("request_type_mismatch")
    if auth_layout != resolve_layout:
        differences.append(f"layout_mismatch: auth={auth_layout} resolve={resolve_layout}")

    auth_names = {str(field) for field in auth_fields}
    resolve_names = {str(field) for field in resolve_fields}
    missing_in_resolve = sorted(auth_names - resolve_names)
    missing_in_auth = sorted(resolve_names - auth_names)
    if missing_in_resolve:
        differences.append("missing_in_resolve: " + ",".join(missing_in_resolve))
    if missing_in_auth:
        differences.append("missing_in_auth: " + ",".join(missing_in_auth))

    for field in sorted(auth_names & resolve_names):
        auth_category = _type_category(auth_fields.get(field), field=field)
        resolve_category = _type_category(resolve_fields.get(field), field=field)
        if auth_category != resolve_category:
            differences.append(
                f"type_mismatch: {field} auth={auth_category} resolve={resolve_category}"
            )
    return differences


def _apply_directory_metadata_from_summary(
    report: dict[str, Any],
    summary: Mapping[str, Any],
) -> None:
    path_fields = summary.get("path_fields")
    if not isinstance(path_fields, Mapping):
        return

    database_directory = path_fields.get("database_directory")
    if isinstance(database_directory, Mapping):
        report["tdlib_database_directory_kind"] = database_directory.get("kind", "absent")
        report["tdlib_state_dir_present"] = bool(database_directory.get("path_exists"))
        report["tdlib_state_dir_is_dir"] = bool(database_directory.get("path_is_dir"))
        report["tdlib_state_dir_writable"] = bool(database_directory.get("path_writable"))

    files_directory = path_fields.get("files_directory")
    if isinstance(files_directory, Mapping):
        report["tdlib_files_directory_kind"] = files_directory.get("kind", "absent")
        report["tdlib_files_dir_present"] = bool(files_directory.get("path_exists"))
        report["tdlib_files_dir_is_dir"] = bool(files_directory.get("path_is_dir"))
        report["tdlib_files_dir_writable"] = bool(files_directory.get("path_writable"))


def _inspect_tdjson_availability(values: Mapping[str, str]) -> dict[str, bool]:
    explicit_path = values.get("TDJSON_LIBRARY_PATH")
    explicit_present = False
    if isinstance(explicit_path, str) and explicit_path.strip():
        try:
            explicit_present = Path(explicit_path.strip()).is_file()
        except OSError:
            explicit_present = False

    default_path_checked = True
    default_present = False
    for candidate in ("/opt/github-ai-catchbot/tdlib/lib/libtdjson.so",):
        try:
            if Path(candidate).is_file():
                default_present = True
                break
        except OSError:
            continue

    find_library_present = bool(ctypes.util.find_library("tdjson"))
    available = explicit_present or default_present or find_library_present
    return {
        "tdjson_library_path_present": available,
        "tdjson_default_path_checked": default_path_checked,
        "tdjson_available_import_check_performed": True,
        "tdjson_available": available,
    }


def _apply_tdjson_check(
    report: dict[str, Any],
    values: Mapping[str, str],
    checker: TdjsonAvailabilityChecker | None,
) -> None:
    try:
        result = checker(values) if checker is not None else _inspect_tdjson_availability(values)
    except Exception:
        result = {
            "tdjson_library_path_present": False,
            "tdjson_default_path_checked": False,
            "tdjson_available_import_check_performed": True,
            "tdjson_available": False,
        }

    if isinstance(result, bool):
        fields = {
            "tdjson_library_path_present": result,
            "tdjson_default_path_checked": False,
            "tdjson_available_import_check_performed": True,
            "tdjson_available": result,
        }
    else:
        fields = {
            "tdjson_library_path_present": bool(result.get("tdjson_library_path_present")),
            "tdjson_default_path_checked": bool(result.get("tdjson_default_path_checked")),
            "tdjson_available_import_check_performed": bool(
                result.get("tdjson_available_import_check_performed", True)
            ),
            "tdjson_available": bool(result.get("tdjson_available")),
        }

    report.update(fields)
    if not report["tdjson_available"]:
        _set_check(report, "tdjson.unavailable")


def _operator_next_action(status: str) -> str:
    if status == "tdlib_parameters_redacted_config_diagnostic_passed":
        return (
            "Both redacted TDLib parameter paths are structurally equivalent. "
            "Use this report as evidence before deciding whether the next slice "
            "should inspect TDLib runtime response behavior."
        )
    if status == "blocked_parameter_shape_mismatch":
        return (
            "Align the auth/session and public username resolve TDLib parameter "
            "builders before any live TDLib retry."
        )
    if status in {
        "blocked_required_parameter_missing",
        "blocked_required_parameter_type_invalid",
        "blocked_collector_config_invalid",
    }:
        return (
            "Fix runtime env/config or the redacted parameter builder shape on the "
            "VPS without sharing raw runtime.env, api_hash, phone number, DB URL, "
            "Redis URL, or encryption key values."
        )
    if status == "blocked_tdjson_unavailable":
        return (
            "Resolve tdjson loader availability using path/package evidence only; "
            "do not rerun login, code entry, 2FA, or TDLib network operations."
        )
    return (
        "Stop before any live TDLib operation and review the redacted diagnostic "
        "failure."
    )


def _apply_contract_status(report: dict[str, Any]) -> None:
    checks = set(report["checks_failed"])
    if "runtime_env.unreadable" in checks:
        status = "blocked_runtime_env_unreadable"
    elif "required_parameter.missing" in checks:
        status = "blocked_required_parameter_missing"
    elif "required_parameter.type_invalid" in checks:
        status = "blocked_required_parameter_type_invalid"
    elif "collector_config.invalid" in checks:
        status = "blocked_collector_config_invalid"
    elif "parameter_shape.mismatch" in checks:
        status = "blocked_parameter_shape_mismatch"
    elif "tdjson.unavailable" in checks:
        status = "blocked_tdjson_unavailable"
    elif "unexpected_error" in checks:
        status = "blocked_unexpected_error"
    else:
        status = "tdlib_parameters_redacted_config_diagnostic_passed"
    report["contract_status"] = status
    report["operator_next_action"] = _operator_next_action(status)


def generate_report(
    *,
    repo_root: str | Path | None = None,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    runtime_env_reader: RuntimeEnvReader | None = None,
    collector_config_factory: CollectorConfigFactory | None = None,
    auth_parameter_builder: ParameterBuilder | None = None,
    resolve_parameter_builder: ParameterBuilder | None = None,
    tdjson_availability_checker: TdjsonAvailabilityChecker | None = None,
) -> ScriptResult:
    report = _base_report()
    resolved_repo_root = (
        Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    )

    try:
        try:
            values = (
                runtime_env_reader(runtime_env_path)
                if runtime_env_reader is not None
                else session_preflight.parse_runtime_env_file(runtime_env_path)
            )
        except Exception:
            _set_check(report, "runtime_env.unreadable")
            _apply_contract_status(report)
            return ScriptResult(exit_code=1, report=report)

        report["runtime_env_read"] = True
        encryption_key_source = _apply_db_key_metadata(report, values)
        preliminary_present = _preliminary_required_field_presence(values)
        preliminary_type_failures = _preliminary_type_failures(values)
        report["required_parameter_fields_present"] = preliminary_present
        report["required_parameter_fields_missing"] = [
            field for field, present in preliminary_present.items() if not present
        ]
        report["required_parameter_field_type_failures"] = preliminary_type_failures

        if report["required_parameter_fields_missing"]:
            _set_check(report, "required_parameter.missing")
        if preliminary_type_failures:
            _set_check(report, "required_parameter.type_invalid")

        try:
            config = (
                collector_config_factory(values)
                if collector_config_factory is not None
                else _build_collector_config_default(resolved_repo_root, values)
            )
            report["collector_config_built"] = True
        except Exception:
            _set_check(report, "collector_config.invalid")
            _apply_tdjson_check(report, values, tdjson_availability_checker)
            _apply_contract_status(report)
            return ScriptResult(exit_code=1, report=report)

        auth_builder = auth_parameter_builder or _default_auth_parameter_builder
        resolve_builder = resolve_parameter_builder or _default_resolve_parameter_builder

        try:
            auth_payload = dict(auth_builder(resolved_repo_root, config, values))
            report["auth_path_parameters_inspected"] = True
            resolve_payload = dict(resolve_builder(resolved_repo_root, config, values))
            report["resolve_path_parameters_inspected"] = True
        except Exception:
            _set_check(report, "unexpected_error")
            _apply_tdjson_check(report, values, tdjson_availability_checker)
            _apply_contract_status(report)
            return ScriptResult(exit_code=1, report=report)

        auth_summary = _shape_summary(
            auth_payload,
            encryption_key_source=encryption_key_source,
        )
        resolve_summary = _shape_summary(
            resolve_payload,
            encryption_key_source=encryption_key_source,
        )
        report["auth_path_parameter_shape_summary"] = auth_summary
        report["resolve_path_parameter_shape_summary"] = resolve_summary
        _apply_directory_metadata_from_summary(report, auth_summary)

        differences = _compare_shapes(auth_payload, resolve_payload)
        report["differences_summary"] = differences
        report["parameter_shapes_equivalent"] = not differences
        if differences:
            _set_check(report, "parameter_shape.mismatch")

        auth_layout, auth_fields = _extract_parameter_fields(auth_payload)
        resolve_layout, resolve_fields = _extract_parameter_fields(resolve_payload)
        auth_present = _field_presence(auth_fields, encryption_key_source)
        resolve_present = _field_presence(resolve_fields, encryption_key_source)
        combined_present = {
            field: auth_present[field] and resolve_present[field]
            for field in REQUIRED_PARAMETER_FIELDS
        }
        report["required_parameter_fields_present"] = combined_present
        report["required_parameter_fields_missing"] = [
            field for field, present in combined_present.items() if not present
        ]
        if report["required_parameter_fields_missing"]:
            _set_check(report, "required_parameter.missing")

        type_failures = [
            *_field_type_failures(
                auth_fields,
                label="auth",
                encryption_key_source=encryption_key_source,
            ),
            *_field_type_failures(
                resolve_fields,
                label="resolve",
                encryption_key_source=encryption_key_source,
            ),
        ]
        if auth_layout not in {"flat", "nested_parameters"}:
            type_failures.append("auth.layout.unsupported")
        if resolve_layout not in {"flat", "nested_parameters"}:
            type_failures.append("resolve.layout.unsupported")
        report["required_parameter_field_type_failures"] = sorted(set(type_failures))
        report["required_parameter_field_types_ok"] = not type_failures
        if type_failures:
            _set_check(report, "required_parameter.type_invalid")

        _apply_tdjson_check(report, values, tdjson_availability_checker)
        _apply_contract_status(report)
        return ScriptResult(
            exit_code=(
                0
                if report["contract_status"]
                == "tdlib_parameters_redacted_config_diagnostic_passed"
                else 1
            ),
            report=report,
        )
    except Exception:
        _set_check(report, "unexpected_error")
        _apply_contract_status(report)
        return ScriptResult(exit_code=1, report=report)


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
