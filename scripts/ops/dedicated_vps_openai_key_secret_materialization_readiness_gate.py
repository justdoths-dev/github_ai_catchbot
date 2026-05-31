from __future__ import annotations

import argparse
import json
import re
import stat
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_openai_key_secret_materialization_readiness_gate"
REPORT_TYPE = "openai_key_secret_materialization_readiness_gate_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
EXPECTED_OPENAI_API_KEY_FILE_PATH = "/etc/github-ai-catchbot/secrets/openai_api_key"
REQUIRED_OWNER = "deploy"
REQUIRED_GROUP = "deploy"
REQUIRED_MODE = 0o600

STATUS_PASSED = "openai_key_secret_materialization_readiness_passed"
STATUS_RUNTIME_ENV_UNREADABLE = (
    "blocked_openai_key_secret_materialization_runtime_env_unreadable"
)
STATUS_RUNTIME_ENV_INVALID = (
    "blocked_openai_key_secret_materialization_runtime_env_invalid"
)
STATUS_DIRECT_KEY_PRESENT = (
    "blocked_openai_key_secret_materialization_direct_key_present"
)
STATUS_FILE_REF_MISSING = (
    "blocked_openai_key_secret_materialization_file_ref_missing"
)
STATUS_FILE_REF_INVALID = (
    "blocked_openai_key_secret_materialization_file_ref_invalid"
)
STATUS_SECRET_FILE_INVALID = (
    "blocked_openai_key_secret_materialization_secret_file_invalid"
)
STATUS_FORBIDDEN_SIDE_EFFECT = (
    "blocked_openai_key_secret_materialization_forbidden_side_effect"
)
STATUS_RAW_VALUE_EMISSION = (
    "blocked_openai_key_secret_materialization_raw_value_emission"
)

FORBIDDEN_SIDE_EFFECT_FIELDS = (
    "openai_secret_content_read",
    "openai_call_attempted",
    "judge_openai_started",
    "analysis_validator_started",
    "policy_engine_started",
    "notifier_started",
    "telegram_send_attempted",
    "database_connected",
    "redis_connected",
    "postgres_mutation_attempted",
    "redis_mutation_attempted",
    "docker_or_systemd_changed",
    "alembic_run",
    "external_network_attempted",
    "raw_values_emitted",
)


class StatResultLike(Protocol):
    st_mode: int
    st_uid: int
    st_gid: int
    st_size: int


ReadTextFunc = Callable[[str | Path], str]
StatFunc = Callable[[str | Path], StatResultLike]
OwnerNameResolver = Callable[[int], str]
GroupNameResolver = Callable[[int], str]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only OpenAI API key secret materialization readiness gate. "
            "The script reads runtime.env, stats the configured secret file, "
            "emits sanitized JSON, and performs no runtime service actions."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_RUNTIME_ENV_UNREADABLE,
        "runtime_env_read": False,
        "runtime_env_exists": False,
        "runtime_env_is_file": False,
        "runtime_env_owner_deploy_bucket": "zero",
        "runtime_env_group_deploy_bucket": "zero",
        "runtime_env_mode_600_bucket": "zero",
        "runtime_has_direct_openai_api_key": False,
        "runtime_has_openai_api_key_file": False,
        "openai_api_key_file_matches_expected_bucket": "zero",
        "secret_file_exists": False,
        "secret_file_is_file": False,
        "secret_file_is_symlink": False,
        "secret_file_non_empty_bucket": "zero",
        "secret_owner_deploy_bucket": "zero",
        "secret_group_deploy_bucket": "zero",
        "secret_mode_600_bucket": "zero",
        "openai_secret_content_read": False,
        "openai_call_attempted": False,
        "judge_openai_started": False,
        "analysis_validator_started": False,
        "policy_engine_started": False,
        "notifier_started": False,
        "telegram_send_attempted": False,
        "database_connected": False,
        "redis_connected": False,
        "postgres_mutation_attempted": False,
        "redis_mutation_attempted": False,
        "docker_or_systemd_changed": False,
        "alembic_run": False,
        "external_network_attempted": False,
        "raw_values_emitted": False,
        "checks_failed": [],
    }


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _bucket_one_zero(value: bool) -> str:
    return "one" if value else "zero"


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


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def _default_read_text(path: str | Path) -> str:
    return Path(path).read_text(encoding="utf-8", errors="replace")


def _default_stat(path: str | Path) -> StatResultLike:
    return Path(path).stat()


def _default_lstat(path: str | Path) -> StatResultLike:
    return Path(path).lstat()


def _default_owner_name(uid: int) -> str:
    import pwd

    return pwd.getpwuid(uid).pw_name


def _default_group_name(gid: int) -> str:
    import grp

    return grp.getgrgid(gid).gr_name


def _mode_bucket(metadata: StatResultLike) -> str:
    return _bucket_one_zero(stat.S_IMODE(metadata.st_mode) == REQUIRED_MODE)


def _owner_bucket(
    metadata: StatResultLike,
    owner_name_resolver: OwnerNameResolver,
) -> str:
    try:
        owner_name = owner_name_resolver(metadata.st_uid)
    except Exception:
        return "zero"
    return _bucket_one_zero(owner_name == REQUIRED_OWNER)


def _group_bucket(
    metadata: StatResultLike,
    group_name_resolver: GroupNameResolver,
) -> str:
    try:
        group_name = group_name_resolver(metadata.st_gid)
    except Exception:
        return "zero"
    return _bucket_one_zero(group_name == REQUIRED_GROUP)


def _report_contains_raw_values(
    report: Mapping[str, Any],
    raw_values: set[str],
) -> bool:
    rendered = json.dumps(report, ensure_ascii=True, sort_keys=True, default=str)
    return any(value and len(value) >= 4 and value in rendered for value in raw_values)


def _apply_side_effect_flags(
    report: dict[str, Any],
    side_effect_flags: Mapping[str, bool] | None,
) -> None:
    for key, value in (side_effect_flags or {}).items():
        if key in report:
            report[key] = bool(value)


def _forbidden_side_effect_detected(report: Mapping[str, Any]) -> bool:
    return any(bool(report.get(field)) for field in FORBIDDEN_SIDE_EFFECT_FIELDS)


def _finish(report: dict[str, Any], status: str, check: str | None = None) -> ScriptResult:
    _set_status(report, status, check)
    return ScriptResult(exit_code=0 if status == STATUS_PASSED else 1, report=report)


def _finish_after_redaction_check(
    report: dict[str, Any],
    raw_values: set[str],
    status: str,
    check: str | None = None,
) -> ScriptResult:
    _set_status(report, status, check)
    if _report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)
    return ScriptResult(exit_code=0 if status == STATUS_PASSED else 1, report=report)


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    read_text_func: ReadTextFunc = _default_read_text,
    stat_func: StatFunc = _default_stat,
    lstat_func: StatFunc = _default_lstat,
    owner_name_resolver: OwnerNameResolver = _default_owner_name,
    group_name_resolver: GroupNameResolver = _default_group_name,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    report = _base_report()
    _apply_side_effect_flags(report, side_effect_flags)
    if _forbidden_side_effect_detected(report):
        return _finish(report, STATUS_FORBIDDEN_SIDE_EFFECT, "side_effect.forbidden")

    raw_values: set[str] = {value for value in forbidden_raw_values if len(value) >= 4}
    raw_values.add(str(runtime_env_path))
    raw_values.add(EXPECTED_OPENAI_API_KEY_FILE_PATH)

    try:
        runtime_metadata = stat_func(runtime_env_path)
    except Exception:
        return _finish_after_redaction_check(
            report,
            raw_values,
            STATUS_RUNTIME_ENV_UNREADABLE,
            "runtime_env.stat",
        )

    report["runtime_env_exists"] = True
    report["runtime_env_is_file"] = stat.S_ISREG(runtime_metadata.st_mode)
    report["runtime_env_owner_deploy_bucket"] = _owner_bucket(
        runtime_metadata,
        owner_name_resolver,
    )
    report["runtime_env_group_deploy_bucket"] = _group_bucket(
        runtime_metadata,
        group_name_resolver,
    )
    report["runtime_env_mode_600_bucket"] = _mode_bucket(runtime_metadata)
    if (
        not report["runtime_env_is_file"]
        or report["runtime_env_owner_deploy_bucket"] != "one"
        or report["runtime_env_group_deploy_bucket"] != "one"
        or report["runtime_env_mode_600_bucket"] != "one"
    ):
        return _finish_after_redaction_check(
            report,
            raw_values,
            STATUS_RUNTIME_ENV_INVALID,
            "runtime_env.metadata",
        )

    try:
        runtime_text = read_text_func(runtime_env_path)
    except Exception:
        return _finish_after_redaction_check(
            report,
            raw_values,
            STATUS_RUNTIME_ENV_UNREADABLE,
            "runtime_env.read",
        )

    report["runtime_env_read"] = True
    raw_values.add(runtime_text)
    values = parse_runtime_env_text(runtime_text)

    direct_key_value = values.get("OPENAI_API_KEY")
    if direct_key_value is not None:
        raw_values.add(direct_key_value)
    key_file_value = values.get("OPENAI_API_KEY_FILE")
    if key_file_value is not None:
        raw_values.add(key_file_value)

    report["runtime_has_direct_openai_api_key"] = "OPENAI_API_KEY" in values
    if report["runtime_has_direct_openai_api_key"]:
        return _finish_after_redaction_check(
            report,
            raw_values,
            STATUS_DIRECT_KEY_PRESENT,
            "runtime_env.openai_api_key",
        )

    report["runtime_has_openai_api_key_file"] = key_file_value is not None
    if key_file_value is None:
        return _finish_after_redaction_check(
            report,
            raw_values,
            STATUS_FILE_REF_MISSING,
            "runtime_env.openai_api_key_file",
        )

    key_file_matches_expected = key_file_value == EXPECTED_OPENAI_API_KEY_FILE_PATH
    report["openai_api_key_file_matches_expected_bucket"] = _bucket_one_zero(
        key_file_matches_expected
    )
    if not key_file_matches_expected:
        return _finish_after_redaction_check(
            report,
            raw_values,
            STATUS_FILE_REF_INVALID,
            "runtime_env.openai_api_key_file.expected",
        )

    try:
        secret_lstat = lstat_func(key_file_value)
    except Exception:
        return _finish_after_redaction_check(
            report,
            raw_values,
            STATUS_SECRET_FILE_INVALID,
            "secret_file.stat",
        )

    report["secret_file_exists"] = True
    report["secret_file_is_symlink"] = stat.S_ISLNK(secret_lstat.st_mode)
    if report["secret_file_is_symlink"]:
        return _finish_after_redaction_check(
            report,
            raw_values,
            STATUS_SECRET_FILE_INVALID,
            "secret_file.symlink",
        )

    try:
        secret_metadata = stat_func(key_file_value)
    except Exception:
        return _finish_after_redaction_check(
            report,
            raw_values,
            STATUS_SECRET_FILE_INVALID,
            "secret_file.stat",
        )

    report["secret_file_is_file"] = stat.S_ISREG(secret_metadata.st_mode)
    report["secret_file_non_empty_bucket"] = _bucket_one_zero(
        secret_metadata.st_size > 0
    )
    report["secret_owner_deploy_bucket"] = _owner_bucket(
        secret_metadata,
        owner_name_resolver,
    )
    report["secret_group_deploy_bucket"] = _group_bucket(
        secret_metadata,
        group_name_resolver,
    )
    report["secret_mode_600_bucket"] = _mode_bucket(secret_metadata)
    if (
        not report["secret_file_is_file"]
        or report["secret_file_non_empty_bucket"] != "one"
        or report["secret_owner_deploy_bucket"] != "one"
        or report["secret_group_deploy_bucket"] != "one"
        or report["secret_mode_600_bucket"] != "one"
    ):
        return _finish_after_redaction_check(
            report,
            raw_values,
            STATUS_SECRET_FILE_INVALID,
            "secret_file.metadata",
        )

    return _finish_after_redaction_check(report, raw_values, STATUS_PASSED)


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=True, sort_keys=True, indent=2)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(runtime_env_path=args.runtime_env_path)
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
