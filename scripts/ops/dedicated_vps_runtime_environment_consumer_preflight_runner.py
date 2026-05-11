from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPORT_TYPE = "dedicated_vps_runtime_environment_consumer_preflight_result_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"

REQUIRED_KEYS = (
    "APP_ENV",
    "DATABASE_URL",
    "REDIS_URL",
    "ENABLE_NOTIFICATION_SEND",
    "NOTIFIER_TELEGRAM_DRY_RUN",
    "NOTIFIER_TELEGRAM_ALLOW_EDITS",
    "ENABLE_REPLAY_TO_PROD_DB",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
)

OPTIONAL_KEYS = (
    "LOG_LEVEL",
    "ENABLE_LATER_DELIVERY",
    "ENABLE_SILENT_LATER",
    "NOTIFICATION_RETRY_MAX_ATTEMPTS",
    "MAINTENANCE_NOTIFICATION_RETRY_POLL_SEC",
    "MAINTENANCE_BATCH_SIZE",
    "MAINTENANCE_BLOCK_MS",
)

OPTIONAL_SENSITIVE_KEYS = (
    "OPENAI_API_KEY",
    "OPENAI_PROJECT",
    "TELEGRAM_BOT_TOKEN",
    "GITHUB_APP_ID",
    "GITHUB_INSTALLATION_ID",
    "GITHUB_PRIVATE_KEY",
    "X_BEARER_TOKEN",
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_PHONE_NUMBER",
    "TELEGRAM_2FA_PASSWORD",
    "TDLIB_DB_ENCRYPTION_KEY",
    "TDLIB_STATE_DIR",
    "TDLIB_FILES_DIR",
)

SAFETY_FLAG_EXPECTATIONS = {
    "ENABLE_NOTIFICATION_SEND": False,
    "NOTIFIER_TELEGRAM_DRY_RUN": False,
    "NOTIFIER_TELEGRAM_ALLOW_EDITS": True,
    "ENABLE_REPLAY_TO_PROD_DB": False,
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": False,
}


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate future dedicated VPS runtime.env consumer readiness. "
            "Without --approved-runtime-env-consumer-preflight this tool reads no "
            "runtime env file, inspects no process env vars, and connects nowhere."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--repo-root", default=None)
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--approved-runtime-env-consumer-preflight", action="store_true")
    return parser


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _base_report(repo_root: Path, runtime_env_path: str) -> dict[str, Any]:
    return {
        "report_type": REPORT_TYPE,
        "contract_status": "approval_required",
        "checks_failed": [],
        "failures": [],
        "warnings": [],
        "repo_root": str(repo_root),
        "runtime_env_path": runtime_env_path,
        "runtime_env_read": False,
        "runtime_env_values_printed": False,
        "database_url_printed": False,
        "redis_url_printed": False,
        "secret_values_printed": False,
        "process_env_inspected": False,
        "database_connected": False,
        "redis_connected": False,
        "db_write_performed": False,
        "redis_mutation_performed": False,
        "alembic_run": False,
        "app_runtime_started": False,
        "tdlib_auth_performed": False,
        "telegram_connected": False,
        "live_collector_started": False,
        "notifier_transport_enabled": False,
        "production_rollout_performed": False,
        "docker_used": False,
        "systemd_modified": False,
        "migration_files_modified": False,
        "required_keys_present": [],
        "required_keys_missing": list(REQUIRED_KEYS),
        "optional_keys_present": [],
        "optional_sensitive_keys_present": [],
        "runtime_env_key_count": 0,
        "app_env_seen": None,
        "database_url_shape": _empty_database_url_shape(),
        "redis_url_shape": _empty_redis_url_shape(),
        "feature_flags": {key: False for key in SAFETY_FLAG_EXPECTATIONS},
        "safety_profile": "prod_pre_runtime",
        "safety_profile_passed": False,
        "consumer_profile_summary": {
            "database_consumers_ready": False,
            "redis_consumers_ready": False,
            "notification_transport_disabled": False,
            "replay_to_prod_disabled": False,
            "maintenance_retry_promotion_disabled": False,
            "runtime_start_authorized": False,
            "tdlib_authorized": False,
            "telegram_authorized": False,
            "live_collector_authorized": False,
            "notifier_transport_authorized": False,
            "production_rollout_authorized": False,
        },
    }


def _empty_database_url_shape() -> dict[str, Any]:
    return {
        "present": False,
        "scheme": None,
        "has_credentials": False,
        "username": None,
        "host": None,
        "port": None,
        "database": None,
        "loopback_only": False,
    }


def _empty_redis_url_shape() -> dict[str, Any]:
    return {
        "present": False,
        "scheme": None,
        "host": None,
        "port": None,
        "database_index": None,
        "loopback_only": False,
    }


def _failure(report: dict[str, Any], check: str, message: str) -> None:
    report["checks_failed"].append(check)
    report["failures"].append({"check": check, "message": message})


def _warning(report: dict[str, Any], message: str) -> None:
    report["warnings"].append(message)


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def parse_runtime_env_text(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        values[key] = _strip_optional_quotes(raw_value)
    return values


def parse_runtime_env_file(path: str | Path) -> dict[str, str]:
    return parse_runtime_env_text(Path(path).read_text(encoding="utf-8", errors="replace"))


def _parse_authority_and_path(value: str) -> dict[str, Any]:
    match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*)://([^/?#]*)(?:/([^?#]*))?", value)
    if not match:
        return {
            "present": True,
            "scheme": None,
            "userinfo": None,
            "host": None,
            "port": None,
            "path": None,
        }

    scheme = match.group(1).lower()
    authority = match.group(2)
    path = match.group(3) or ""
    userinfo: str | None = None
    host_port = authority
    if "@" in authority:
        userinfo, host_port = authority.rsplit("@", 1)

    host = host_port
    port: int | None = None
    if ":" in host_port:
        host_candidate, port_candidate = host_port.rsplit(":", 1)
        host = host_candidate
        if port_candidate.isdigit():
            port = int(port_candidate)

    return {
        "present": True,
        "scheme": scheme,
        "userinfo": userinfo,
        "host": host or None,
        "port": port,
        "path": path,
    }


def _username_from_userinfo(userinfo: str | None) -> str | None:
    if not userinfo:
        return None
    return userinfo.split(":", 1)[0] or None


def _has_password_credentials(userinfo: str | None) -> bool:
    if not userinfo or ":" not in userinfo:
        return False
    password = userinfo.split(":", 1)[1]
    return bool(password)


def _is_loopback_host(host: str | None) -> bool:
    return host in {"127.0.0.1", "localhost"}


def database_url_shape(database_url: str | None) -> dict[str, Any]:
    if not database_url:
        return _empty_database_url_shape()

    parsed = _parse_authority_and_path(database_url)
    loopback_only = _is_loopback_host(parsed["host"])
    safe_host = parsed["host"] if loopback_only else "<non_loopback_redacted>" if parsed["host"] else None
    database = (parsed["path"] or "").split("/", 1)[0] or None
    return {
        "present": True,
        "scheme": parsed["scheme"],
        "has_credentials": _has_password_credentials(parsed["userinfo"]),
        "username": _username_from_userinfo(parsed["userinfo"]),
        "host": safe_host,
        "port": parsed["port"],
        "database": database,
        "loopback_only": loopback_only,
    }


def redis_url_shape(redis_url: str | None) -> dict[str, Any]:
    if not redis_url:
        return _empty_redis_url_shape()

    parsed = _parse_authority_and_path(redis_url)
    loopback_only = _is_loopback_host(parsed["host"])
    safe_host = parsed["host"] if loopback_only else "<non_loopback_redacted>" if parsed["host"] else None
    database_index: int | None = None
    path = parsed["path"] or ""
    if path.isdigit():
        database_index = int(path)
    return {
        "present": True,
        "scheme": parsed["scheme"],
        "host": safe_host,
        "port": parsed["port"],
        "database_index": database_index,
        "loopback_only": loopback_only,
    }


def _parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    return None


def _validate_required_keys(report: dict[str, Any], values: dict[str, str]) -> None:
    keys = set(values)
    present = [key for key in REQUIRED_KEYS if key in keys]
    missing = [key for key in REQUIRED_KEYS if key not in keys]
    report["required_keys_present"] = present
    report["required_keys_missing"] = missing
    if missing:
        _failure(report, "runtime_env.required_keys", "One or more required runtime env keys are missing.")


def _validate_database_url_shape(report: dict[str, Any], shape: dict[str, Any]) -> None:
    if not shape["present"]:
        _failure(report, "database_url.present", "DATABASE_URL is required.")
        return
    expected = {
        "scheme": "postgresql+psycopg",
        "username": "github_ai_catchbot_app",
        "host": None,
        "port": 5432,
        "database": "github_ai_catchbot",
    }
    for key in ("scheme", "username", "port", "database"):
        if shape[key] != expected[key]:
            _failure(report, f"database_url.{key}", f"DATABASE_URL {key} does not match the expected dedicated VPS shape.")
    if not shape["has_credentials"]:
        _failure(report, "database_url.credentials", "DATABASE_URL must include credential metadata.")
    if not shape["loopback_only"]:
        _failure(report, "database_url.loopback_only", "DATABASE_URL host must be loopback-only.")


def _validate_redis_url_shape(report: dict[str, Any], shape: dict[str, Any]) -> None:
    if not shape["present"]:
        _failure(report, "redis_url.present", "REDIS_URL is required.")
        return
    if shape["scheme"] != "redis":
        _failure(report, "redis_url.scheme", "REDIS_URL must use the redis scheme.")
    if not shape["loopback_only"]:
        _failure(report, "redis_url.loopback_only", "REDIS_URL host must be loopback-only.")
    if shape["port"] != 6379:
        _failure(report, "redis_url.port", "REDIS_URL port must be 6379.")


def _validate_safety_profile(report: dict[str, Any], values: dict[str, str]) -> None:
    app_env = values.get("APP_ENV")
    report["app_env_seen"] = app_env
    if app_env != "prod":
        _failure(report, "safety_profile.app_env", "APP_ENV must be prod for this pre-runtime profile.")

    feature_flags: dict[str, bool] = {}
    for key, expected_value in SAFETY_FLAG_EXPECTATIONS.items():
        observed = _parse_bool(values.get(key))
        feature_flags[key] = bool(observed) if observed is not None else False
        if observed is None:
            _failure(report, f"safety_profile.{key.lower()}.parse", f"{key} must be an explicit true or false value.")
        elif observed is not expected_value:
            _failure(report, f"safety_profile.{key.lower()}", f"{key} does not match the prod pre-runtime baseline.")
    report["feature_flags"] = feature_flags


def _apply_consumer_summary(report: dict[str, Any]) -> None:
    database_shape = report["database_url_shape"]
    redis_shape = report["redis_url_shape"]
    flags = report["feature_flags"]
    summary = report["consumer_profile_summary"]
    summary["database_consumers_ready"] = (
        database_shape["scheme"] == "postgresql+psycopg"
        and database_shape["has_credentials"] is True
        and database_shape["username"] == "github_ai_catchbot_app"
        and database_shape["loopback_only"] is True
        and database_shape["port"] == 5432
        and database_shape["database"] == "github_ai_catchbot"
    )
    summary["redis_consumers_ready"] = (
        redis_shape["scheme"] == "redis"
        and redis_shape["loopback_only"] is True
        and redis_shape["port"] == 6379
    )
    summary["notification_transport_disabled"] = flags["ENABLE_NOTIFICATION_SEND"] is False
    summary["replay_to_prod_disabled"] = flags["ENABLE_REPLAY_TO_PROD_DB"] is False
    summary["maintenance_retry_promotion_disabled"] = flags["MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION"] is False


def _validate_runtime_env_values(report: dict[str, Any], values: dict[str, str]) -> None:
    report["runtime_env_key_count"] = len(values)
    report["optional_keys_present"] = [key for key in OPTIONAL_KEYS if key in values]
    report["optional_sensitive_keys_present"] = [key for key in OPTIONAL_SENSITIVE_KEYS if key in values]
    if report["optional_sensitive_keys_present"]:
        _warning(report, "Optional sensitive consumer keys are present by key name only; those consumers remain unauthorized.")

    _validate_required_keys(report, values)
    report["database_url_shape"] = database_url_shape(values.get("DATABASE_URL"))
    report["redis_url_shape"] = redis_url_shape(values.get("REDIS_URL"))
    _validate_database_url_shape(report, report["database_url_shape"])
    _validate_redis_url_shape(report, report["redis_url_shape"])
    _validate_safety_profile(report, values)
    _apply_consumer_summary(report)

    summary = report["consumer_profile_summary"]
    safety_profile_failed = any(
        str(check).startswith("safety_profile.") for check in report["checks_failed"]
    )
    required_safety_flags_missing = any(
        key in report["required_keys_missing"] for key in SAFETY_FLAG_EXPECTATIONS
    )
    report["safety_profile_passed"] = (
        not safety_profile_failed
        and not required_safety_flags_missing
        and report["app_env_seen"] == "prod"
        and summary["notification_transport_disabled"] is True
        and report["feature_flags"]["NOTIFIER_TELEGRAM_DRY_RUN"] is False
        and report["feature_flags"]["NOTIFIER_TELEGRAM_ALLOW_EDITS"] is True
        and summary["replay_to_prod_disabled"] is True
        and summary["maintenance_retry_promotion_disabled"] is True
    )


def generate_report(
    repo_root: str | Path | None = None,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    approved_runtime_env_consumer_preflight: bool = False,
) -> RunnerResult:
    resolved_repo_root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    report = _base_report(resolved_repo_root, str(runtime_env_path))

    if not approved_runtime_env_consumer_preflight:
        _failure(
            report,
            "approval.required",
            "Pass --approved-runtime-env-consumer-preflight only after separate operator approval.",
        )
        return RunnerResult(exit_code=2, report=report)

    report["runtime_env_read"] = True
    try:
        values = parse_runtime_env_file(runtime_env_path)
    except Exception:
        _failure(report, "runtime_env.read", "Unable to read runtime env file.")
        report["contract_status"] = "failed"
        return RunnerResult(exit_code=1, report=report)

    _validate_runtime_env_values(report, values)
    report["contract_status"] = "failed" if report["checks_failed"] else "passed"
    return RunnerResult(exit_code=1 if report["checks_failed"] else 0, report=report)


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        repo_root=args.repo_root,
        runtime_env_path=args.runtime_env_path,
        approved_runtime_env_consumer_preflight=args.approved_runtime_env_consumer_preflight,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
