from __future__ import annotations

import argparse
import importlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "dedicated_vps_app_runtime_import_config_preflight_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"

SIDE_EFFECT_FALSE_FLAGS = (
    "runtime_start_authorized",
    "tdlib_authorized",
    "telegram_authorized",
    "live_collector_authorized",
    "notifier_transport_authorized",
    "production_rollout_authorized",
    "database_connected",
    "redis_connected",
    "db_write_performed",
    "redis_mutation_performed",
    "alembic_run",
    "app_runtime_started",
    "tdlib_auth_performed",
    "telegram_connected",
    "live_collector_started",
    "notifier_transport_enabled",
    "production_rollout_performed",
    "docker_used",
    "systemd_modified",
    "migration_files_modified",
)

FORBIDDEN_RUNTIME_MODULE_TOKENS = (
    ".main",
    "tdlib_client",
    "telegram_client",
    "github_client",
    "x_api_client",
    "openai_client",
    "redis_streams",
    "repositories",
    ".service",
    ".worker",
    "web_fetch_client",
)

FORBIDDEN_RUNTIME_METHOD_NAMES = (
    "run_forever",
    "run_once",
    "start",
    "serve",
    "connect",
    "send",
    "receive",
)

SAFE_CONFIG_LOADER_SPECS = {
    "src.services.outbox_relay.config": ("OutboxRelayConfig", "from_env"),
    "src.services.router_normalizer.config": ("RouterNormalizerConfig", "from_env"),
}

SECRET_BOUND_CONFIG_MODULES = {
    "src.services.collector_telegram.config": "telegram_tdlib_secret_bound",
    "src.services.gh_enricher.config": "github_secret_bound",
    "src.services.judge_openai.config": "openai_secret_bound",
    "src.services.notifier_telegram.config": "telegram_notifier_secret_bound",
    "src.services.x_enricher.config": "x_secret_bound",
}

RUNTIME_BOUND_CONFIG_MODULES = {
    "src.services.analysis_router.config",
    "src.services.analysis_validator.config",
    "src.services.evidence_assembler.config",
    "src.services.maintenance.config",
    "src.services.policy_engine.config",
    "src.services.web_enricher.config",
}

SAFE_ENV_KEYS_FOR_CONFIG_LOADERS = (
    "APP_ENV",
    "DATABASE_URL",
    "REDIS_URL",
    "LOG_LEVEL",
    "ENABLE_LATER_DELIVERY",
    "ENABLE_SILENT_LATER",
    "ENABLE_NOTIFICATION_SEND",
    "ENABLE_REPLAY_TO_PROD_DB",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
)


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate dedicated VPS app/runtime import and safe config-loader "
            "surfaces without starting runtime, clients, DB, Redis, TDLib, "
            "Telegram, notifier transport, Docker, systemd, or rollout."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--repo-root", default=None)
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--approved-app-runtime-import-config-preflight", action="store_true")
    return parser


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _base_report(repo_root: Path, runtime_env_path: str) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_status": "approval_required",
        "checks_failed": [],
        "failures": [],
        "warnings": [],
        "repo_root": str(repo_root),
        "runtime_env_path": runtime_env_path,
        "runtime_env_read": False,
        "runtime_env_values_printed": False,
        "secret_values_printed": False,
        "process_env_inspected": False,
        "runtime_env_key_count": 0,
        "app_env_seen": None,
        "import_surface_attempted": False,
        "app_imports_attempted": False,
        "config_surface_attempted": False,
        "import_surface_passed": False,
        "safe_config_surface_passed": False,
        "secret_bound_config_loaders_deferred": False,
        "runtime_bound_config_loaders_deferred": False,
        "database_url_shape": _empty_database_url_shape(),
        "redis_url_shape": _empty_redis_url_shape(),
        "safe_runtime_shape": {
            "app_env_present": False,
            "database_url_present": False,
            "redis_url_present": False,
            "log_level_present": False,
            "safe_feature_flag_names_present": [],
            "secret_bound_key_categories_present": [],
        },
        "import_results": [],
        "config_results": [],
    }
    for flag in SIDE_EFFECT_FALSE_FLAGS:
        report[flag] = False
    return report


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


def _parse_authority_and_path(value: str) -> dict[str, Any]:
    match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*)://([^/?#]*)(?:/([^?#]*))?", value)
    if not match:
        return {"scheme": None, "userinfo": None, "host": None, "port": None, "path": None}

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

    return {"scheme": scheme, "userinfo": userinfo, "host": host or None, "port": port, "path": path}


def _username_from_userinfo(userinfo: str | None) -> str | None:
    if not userinfo:
        return None
    return userinfo.split(":", 1)[0] or None


def _has_password_credentials(userinfo: str | None) -> bool:
    if not userinfo or ":" not in userinfo:
        return False
    return bool(userinfo.split(":", 1)[1])


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


def _safe_app_env(value: str | None) -> str | None:
    if value in {"prod", "dev", "test"}:
        return value
    if value is None:
        return None
    return "<unexpected_redacted>"


def _secret_bound_categories(values: Mapping[str, str]) -> list[str]:
    categories: list[str] = []
    category_keys = {
        "telegram_tdlib": (
            "TELEGRAM_API_ID",
            "TELEGRAM_API_HASH",
            "TELEGRAM_PHONE_NUMBER",
            "TELEGRAM_2FA_PASSWORD",
            "TDLIB_DB_ENCRYPTION_KEY",
            "TDLIB_STATE_DIR",
            "TDLIB_FILES_DIR",
        ),
        "openai": ("OPENAI_API_KEY", "OPENAI_PROJECT"),
        "github": ("GITHUB_APP_ID", "GITHUB_INSTALLATION_ID", "GITHUB_PRIVATE_KEY"),
        "x": ("X_BEARER_TOKEN",),
        "telegram_notifier": ("TELEGRAM_BOT_TOKEN", "TELEGRAM_OPERATOR_CHAT_ID"),
    }
    for category, keys in category_keys.items():
        if any(key in values for key in keys):
            categories.append(category)
    return categories


def _safe_runtime_shape(values: Mapping[str, str]) -> dict[str, Any]:
    safe_flags = [
        key
        for key in (
            "ENABLE_LATER_DELIVERY",
            "ENABLE_SILENT_LATER",
            "ENABLE_NOTIFICATION_SEND",
            "ENABLE_REPLAY_TO_PROD_DB",
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
        )
        if key in values
    ]
    return {
        "app_env_present": "APP_ENV" in values,
        "database_url_present": "DATABASE_URL" in values,
        "redis_url_present": "REDIS_URL" in values,
        "log_level_present": "LOG_LEVEL" in values,
        "safe_feature_flag_names_present": safe_flags,
        "secret_bound_key_categories_present": _secret_bound_categories(values),
    }


def _safe_loader_env(values: Mapping[str, str]) -> dict[str, str]:
    return {key: values[key] for key in SAFE_ENV_KEYS_FOR_CONFIG_LOADERS if key in values}


def discover_config_import_modules(repo_root: Path) -> list[str]:
    services_root = repo_root / "src" / "services"
    modules: list[str] = []
    for path in sorted(services_root.glob("*/config.py")):
        rel = path.relative_to(repo_root).with_suffix("")
        modules.append(".".join(rel.parts))
    return modules


def discover_forbidden_runtime_surface_modules(repo_root: Path) -> list[str]:
    services_root = repo_root / "src" / "services"
    modules: list[str] = []
    for path in sorted(services_root.glob("*/*.py")):
        if path.name == "config.py" or "__pycache__" in path.parts:
            continue
        rel = path.relative_to(repo_root).with_suffix("")
        module_name = ".".join(rel.parts)
        if _is_forbidden_runtime_module(module_name):
            modules.append(module_name)
    return modules


def _is_forbidden_runtime_module(module_name: str) -> bool:
    return any(token in module_name for token in FORBIDDEN_RUNTIME_MODULE_TOKENS)


def _ensure_repo_root_on_path(repo_root: Path) -> None:
    repo_root_text = str(repo_root)
    if repo_root_text not in sys.path:
        sys.path.insert(0, repo_root_text)


def _import_config_modules(repo_root: Path, report: dict[str, Any]) -> dict[str, Any]:
    _ensure_repo_root_on_path(repo_root)
    imported: dict[str, Any] = {}

    report["import_surface_attempted"] = True
    report["app_imports_attempted"] = True

    for module_name in discover_config_import_modules(repo_root):
        try:
            imported[module_name] = importlib.import_module(module_name)
        except Exception as exc:  # pragma: no cover - exercised through report contract
            _failure(report, f"import.{module_name}", "Allowlisted config module import failed.")
            report["import_results"].append(
                {
                    "module": module_name,
                    "status": "import_failed",
                    "surface": "service_config",
                    "error_type": type(exc).__name__,
                }
            )
        else:
            report["import_results"].append(
                {"module": module_name, "status": "import_ok", "surface": "service_config"}
            )

    for module_name in discover_forbidden_runtime_surface_modules(repo_root):
        report["import_results"].append(
            {
                "module": module_name,
                "status": "skipped_forbidden_runtime_surface",
                "surface": "runtime_or_client",
            }
        )

    report["import_surface_passed"] = not any(
        item["status"] == "import_failed" for item in report["import_results"]
    )
    return imported


def _safe_config_summary(config: Any) -> dict[str, Any]:
    log_level = getattr(config, "log_level", None)
    safe_log_level = log_level if log_level in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"} else "<unexpected_redacted>"
    return {
        "class_name": type(config).__name__,
        "app_env": getattr(config, "app_env", None),
        "database_url_present": bool(getattr(config, "database_url", "")),
        "redis_url_present": bool(getattr(config, "redis_url", "")),
        "queue_name": getattr(config, "queue_name", None),
        "consumer_group": getattr(config, "consumer_group", None),
        "log_level": safe_log_level,
    }


def _exercise_config_loaders(imported: Mapping[str, Any], values: Mapping[str, str], report: dict[str, Any]) -> None:
    report["config_surface_attempted"] = True
    safe_env = _safe_loader_env(values)
    safe_loader_failures = 0
    safe_loader_successes = 0

    for module_name in sorted(imported):
        if module_name in SAFE_CONFIG_LOADER_SPECS:
            class_name, method_name = SAFE_CONFIG_LOADER_SPECS[module_name]
            try:
                loader_class = getattr(imported[module_name], class_name)
                config = getattr(loader_class, method_name)(safe_env)
            except Exception as exc:
                safe_loader_failures += 1
                _failure(report, f"config_loader.{module_name}", "Safe config loader failed.")
                report["config_results"].append(
                {
                    "module": module_name,
                    "loader": f"{class_name}.{method_name}",
                    "status": "config_loader_failed",
                    "error_type": type(exc).__name__,
                }
            )
            else:
                safe_loader_successes += 1
                report["config_results"].append(
                    {
                        "module": module_name,
                        "loader": f"{class_name}.{method_name}",
                        "status": "config_loader_ok",
                        "loader_surface": "explicit_env_mapping",
                        "safe_config_shape": _safe_config_summary(config),
                    }
                )
        elif module_name in SECRET_BOUND_CONFIG_MODULES:
            report["config_results"].append(
                {
                    "module": module_name,
                    "status": "config_loader_deferred_secret_bound",
                    "deferred_category": SECRET_BOUND_CONFIG_MODULES[module_name],
                }
            )
        elif module_name in RUNTIME_BOUND_CONFIG_MODULES:
            report["config_results"].append(
                {
                    "module": module_name,
                    "status": "config_loader_deferred_runtime_bound",
                    "reason": "loader_reads_process_environment_directly_or_runtime_surface_not_needed_for_this_slice",
                }
            )
        else:
            report["config_results"].append(
                {"module": module_name, "status": "not_present", "reason": "no_config_loader_policy"}
            )

    report["secret_bound_config_loaders_deferred"] = any(
        item["status"] == "config_loader_deferred_secret_bound" for item in report["config_results"]
    )
    report["runtime_bound_config_loaders_deferred"] = any(
        item["status"] == "config_loader_deferred_runtime_bound" for item in report["config_results"]
    )
    report["safe_config_surface_passed"] = safe_loader_successes > 0 and safe_loader_failures == 0
    if report["secret_bound_config_loaders_deferred"]:
        _warning(report, "Secret-bound config loaders were deferred by design.")
    if report["runtime_bound_config_loaders_deferred"]:
        _warning(report, "Runtime-bound config loaders were deferred by design.")


def generate_report(
    repo_root: str | Path | None = None,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    approved_app_runtime_import_config_preflight: bool = False,
) -> RunnerResult:
    resolved_repo_root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    report = _base_report(resolved_repo_root, str(runtime_env_path))

    if not approved_app_runtime_import_config_preflight:
        _failure(
            report,
            "approval.required",
            "Pass --approved-app-runtime-import-config-preflight only after separate operator approval.",
        )
        return RunnerResult(exit_code=2, report=report)

    report["runtime_env_read"] = True
    try:
        values = parse_runtime_env_file(runtime_env_path)
    except Exception:
        _failure(report, "runtime_env.read", "Unable to read runtime env file.")
        report["contract_status"] = "failed"
        return RunnerResult(exit_code=1, report=report)

    report["runtime_env_key_count"] = len(values)
    report["app_env_seen"] = _safe_app_env(values.get("APP_ENV"))
    report["database_url_shape"] = database_url_shape(values.get("DATABASE_URL"))
    report["redis_url_shape"] = redis_url_shape(values.get("REDIS_URL"))
    report["safe_runtime_shape"] = _safe_runtime_shape(values)

    imported = _import_config_modules(resolved_repo_root, report)
    _exercise_config_loaders(imported, values, report)

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
        approved_app_runtime_import_config_preflight=args.approved_app_runtime_import_config_preflight,
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
