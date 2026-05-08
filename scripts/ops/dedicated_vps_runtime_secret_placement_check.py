from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPORT_TYPE = "dedicated_vps_runtime_secret_placement_check_v1"
CHECKED_FILE = "ops/pipeline/runbooks/dedicated_vps_runtime_secret_placement.md"
SECRET_PATH = "/etc/github-ai-catchbot/runtime.env"
SECRET_DIR = "/etc/github-ai-catchbot"

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

EXPECTED_FIXED_VALUES = {
    "APP_ENV": "prod",
    "REDIS_URL": "redis://127.0.0.1:6379/0",
    "ENABLE_NOTIFICATION_SEND": "false",
    "NOTIFIER_TELEGRAM_DRY_RUN": "true",
    "NOTIFIER_TELEGRAM_ALLOW_EDITS": "false",
    "ENABLE_REPLAY_TO_PROD_DB": "false",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": "false",
}

OPERATOR_AUTHORIZATION = {
    "secret_directory_create_operator_command_present": True,
    "runtime_secret_file_create_operator_command_present": True,
    "sudoedit_runtime_secret_file_operator_command_present": True,
    "redacted_validation_operator_command_present": True,
    "runtime_secret_file_path_authorized": True,
    "runtime_secret_file_outside_repo": True,
    "repo_env_creation_authorized": False,
    "repo_env_directory_creation_authorized": False,
    "secret_values_printing_authorized": False,
    "database_connection_authorized": False,
    "redis_connection_authorized": False,
    "alembic_authorized": False,
    "app_runtime_authorized": False,
    "tdlib_auth_authorized": False,
    "telegram_connection_authorized": False,
    "live_collector_authorized": False,
    "notifier_transport_authorized": False,
    "production_rollout_authorized": False,
    "docker_authorized": False,
    "docker_compose_authorized": False,
    "systemd_unit_modification_authorized": False,
}

CHECKER_SIDE_EFFECTS = {
    "host_inspection_performed": False,
    "network_called": False,
    "services_mutated": False,
    "secrets_read": False,
    "secret_file_read": False,
    "env_file_read": False,
    "files_mutated": False,
    "database_connected": False,
    "redis_connected": False,
    "external_apis_called": False,
}


@dataclass(frozen=True, slots=True)
class RequiredMarker:
    name: str
    any_of: tuple[str, ...]
    message: str


@dataclass(frozen=True, slots=True)
class RequiredSection:
    heading: str


@dataclass(frozen=True, slots=True)
class ForbiddenPattern:
    name: str
    pattern: str
    message: str
    line_scoped: bool = True


@dataclass(frozen=True, slots=True)
class CheckResult:
    exit_code: int
    report: dict[str, Any]


def _normalize_text(text: str) -> str:
    return " ".join(text.lower().split())


def _marker(name: str, message: str, *any_of: str) -> RequiredMarker:
    return RequiredMarker(
        name=name,
        any_of=tuple(_normalize_text(item) for item in any_of),
        message=message,
    )


REQUIRED_SECTIONS: tuple[RequiredSection, ...] = (
    RequiredSection("# Dedicated VPS runtime secret placement"),
    RequiredSection("## Scope"),
    RequiredSection("## Source-of-truth handling"),
    RequiredSection("## Current prerequisite state"),
    RequiredSection("## Secret storage decision"),
    RequiredSection("## Allowed secret file path"),
    RequiredSection("## Allowed keys"),
    RequiredSection("## Explicitly forbidden keys/actions"),
    RequiredSection("## Operator command blocks"),
    RequiredSection("## Redacted validation"),
    RequiredSection("## Failure handling"),
    RequiredSection("## Rollback/cleanup"),
    RequiredSection("## What output to bring back to ChatGPT"),
    RequiredSection("## What remains unauthorized"),
    RequiredSection("## Next step"),
)

REQUIRED_MARKERS: tuple[RequiredMarker, ...] = (
    _marker(
        "readme_v20_authoritative",
        "State that README v20 remains authoritative.",
        "README_replacement_consolidated_v0_20.md` remains authoritative",
    ),
    _marker(
        "project_source_bundles_filename_order",
        "State that project-source bundles 00~10 are interpreted in filename order.",
        "bundles `00~10` remain the source-of-truth set and are interpreted in filename order",
    ),
    _marker(
        "application_plan_advisory_only",
        "State that the GitHub AI application plan is advisory only.",
        "GitHub AI application plan remains advisory only",
    ),
    _marker(
        "architecture_invariant",
        "Preserve the canonical architecture invariant.",
        "SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification",
    ),
    _marker("secret_path", "Authorize the expected host-local secret file path.", SECRET_PATH),
    _marker("secret_dir", "Authorize the expected parent directory.", SECRET_DIR),
    _marker("secret_dir_owner_mode", "Require parent directory root:deploy 0750.", "root:deploy 0750"),
    _marker("secret_file_owner_mode", "Require runtime.env root:deploy 0640.", "root:deploy 0640"),
    _marker(
        "outside_repo_secret_file",
        "State that the runtime secret file is host-local and outside the repository.",
        "host-local runtime secret file outside the repository",
    ),
    _marker("block0_pwd", "Block 0 must include pwd.", "pwd"),
    _marker("block0_whoami", "Block 0 must include whoami.", "whoami"),
    _marker("block0_id", "Block 0 must include id.", "id"),
    _marker(
        "block0_git_status",
        "Block 0 must include git status --short --branch.",
        "git status --short --branch",
    ),
    _marker("block0_git_log", "Block 0 must include git log --oneline -3.", "git log --oneline -3"),
    _marker(
        "block0_deploy_guard",
        "Block 0 must assert the deploy user.",
        'test "$(whoami)" = "deploy"',
    ),
    _marker(
        "install_secret_dir",
        "Block 1 must create the secret directory safely.",
        "sudo install -d -o root -g deploy -m 0750 /etc/github-ai-catchbot",
    ),
    _marker(
        "install_runtime_env",
        "Block 1 must create runtime.env safely without command-line secret values.",
        "sudo install -o root -g deploy -m 0640 /dev/null /etc/github-ai-catchbot/runtime.env",
    ),
    _marker("sudoedit_runtime_env", "Block 2 must prefer sudoedit.", "sudoedit /etc/github-ai-catchbot/runtime.env"),
    _marker(
        "database_url_template_shape",
        "Lock the DATABASE_URL editable template shape.",
        "DATABASE_URL=postgresql+psycopg://github_ai_catchbot_app:<DB_PASSWORD_FROM_PASSWORD_MANAGER>@127.0.0.1:5432/github_ai_catchbot",
    ),
    _marker(
        "db_password_placeholder",
        "Require the password-manager placeholder in the editable DATABASE_URL template.",
        "<DB_PASSWORD_FROM_PASSWORD_MANAGER>",
    ),
    _marker(
        "redacted_python_validation",
        "Block 3 must include a redacted local validation command.",
        "sudo python3 - <<'PY'",
        "secret_values_printed=false",
    ),
    _marker(
        "numeric_mode_extraction",
        "Validation must use numeric permission extraction before comparing 750 and 640.",
        "stat.S_IMODE(info.st_mode)",
        'f"{stat.S_IMODE(info.st_mode):04o}"[-3:]',
    ),
    _marker(
        "db_password_placeholder_failure_check",
        "Validation must fail if the DB password placeholder remains.",
        'if "<DB_PASSWORD_FROM_PASSWORD_MANAGER>" in value',
    ),
    _marker(
        "database_url_placeholder_failure_message",
        "Validation must use the allowed redacted DATABASE_URL placeholder failure message.",
        'fail("DATABASE_URL placeholder remains")',
    ),
    _marker(
        "generic_placeholder_failure_check",
        "Validation must fail if any active value still contains a placeholder.",
        'if "<" in value and ">" in value',
    ),
    _marker(
        "editor_placeholder_failure_check",
        "Validation must fail if the editor-replacement placeholder remains.",
        'if "<replace inside editor; do not print>" in value',
    ),
    _marker(
        "generic_placeholder_failure_message",
        "Validation must use the allowed redacted generic placeholder failure message.",
        'fail("placeholder value remains")',
    ),
    _marker(
        "database_url_prefix_shape_check",
        "Validation must check the DATABASE_URL application-role prefix without printing it.",
        'startswith("postgresql+psycopg://github_ai_catchbot_app:")',
    ),
    _marker(
        "database_url_host_db_suffix_shape_check",
        "Validation must check the DATABASE_URL loopback host/database suffix without printing it.",
        'if "@127.0.0.1:5432/github_ai_catchbot" not in database_url',
    ),
    _marker(
        "database_url_prefix_literal",
        "Require the expected DATABASE_URL application-role prefix literal.",
        "postgresql+psycopg://github_ai_catchbot_app:",
    ),
    _marker(
        "database_url_host_db_suffix_literal",
        "Require the expected DATABASE_URL loopback host/database suffix literal.",
        "@127.0.0.1:5432/github_ai_catchbot",
    ),
    _marker(
        "database_url_shape_mismatch_failure",
        "Validation must fail redacted DATABASE_URL shape mismatches without printing values.",
        'fail("DATABASE_URL shape mismatch")',
    ),
    _marker("database_connected_false", "Validation output must state no DB connection.", "database_connected=false"),
    _marker("redis_connected_false", "Validation output must state no Redis connection.", "redis_connected=false"),
    _marker("alembic_run_false", "Validation output must state Alembic was not run.", "alembic_run=false"),
    _marker(
        "app_runtime_started_false",
        "Validation output must state app runtime was not started.",
        "app_runtime_started=false",
    ),
    _marker(
        "tdlib_auth_performed_false",
        "Validation output must state TDLib auth was not performed.",
        "tdlib_auth_performed=false",
    ),
    _marker(
        "telegram_connected_false",
        "Validation output must state Telegram was not connected.",
        "telegram_connected=false",
    ),
    _marker(
        "live_collector_started_false",
        "Validation output must state live collector was not started.",
        "live_collector_started=false",
    ),
    _marker(
        "notifier_transport_enabled_false",
        "Validation output must state notifier transport was not enabled.",
        "notifier_transport_enabled=false",
    ),
    _marker("no_repo_env", "Forbid repo .env creation.", "Do not create repo `.env`"),
    _marker("no_repo_env_dir", "Forbid repo env/*.env creation.", "Do not create `env/*.env` under the repository"),
    _marker(
        "no_cat_runtime_env",
        "Forbid printing runtime.env with cat.",
        "Do not `cat /etc/github-ai-catchbot/runtime.env`",
    ),
    _marker(
        "no_source_runtime_env",
        "Forbid sourcing runtime.env.",
        "Do not `source /etc/github-ai-catchbot/runtime.env`",
    ),
    _marker(
        "no_dot_source_runtime_env",
        "Forbid dot-sourcing runtime.env.",
        "Do not `. /etc/github-ai-catchbot/runtime.env`",
    ),
    _marker("no_export_database_url", "Forbid exporting DATABASE_URL.", "Do not `export DATABASE_URL`"),
    _marker("no_export_redis_url", "Forbid exporting REDIS_URL.", "Do not `export REDIS_URL`"),
    _marker("no_database_url_print", "Forbid DATABASE_URL printing.", "Do not print `DATABASE_URL`"),
    _marker(
        "no_credential_redis_url_print",
        "Forbid credential-bearing REDIS_URL printing.",
        "Do not print `REDIS_URL` if it ever contains credentials",
    ),
    _marker("no_secret_values_printed", "Forbid secret value printing.", "Do not print any secret values"),
    _marker("no_secrets_chatgpt", "Forbid pasting secrets into ChatGPT.", "Do not paste secrets into ChatGPT"),
    _marker("no_secrets_git_tracked", "Forbid git-tracked secrets.", "Do not write secrets into git-tracked files"),
    _marker("no_alembic", "Forbid Alembic.", "Do not run Alembic"),
    _marker("no_app_runtime", "Forbid app runtime startup.", "Do not start app runtime"),
    _marker("no_tdlib_auth", "Forbid TDLib auth.", "Do not run TDLib auth"),
    _marker("no_telegram_connection", "Forbid Telegram connection.", "Do not connect Telegram"),
    _marker("no_live_collector", "Forbid live collector startup.", "Do not start live collector"),
    _marker("no_notifier_transport", "Forbid notifier transport.", "Do not enable notifier transport"),
    _marker("no_production_rollout", "Forbid production rollout.", "Do not perform production rollout"),
    _marker("no_docker_compose", "Forbid Docker and Docker Compose.", "Do not use Docker or Docker Compose"),
    _marker("no_systemd_unit_mod", "Forbid systemd unit modification.", "Do not modify systemd units in this slice"),
    _marker("no_db_connection", "Forbid PostgreSQL connections.", "Do not connect to PostgreSQL"),
    _marker("no_redis_connection", "Forbid Redis connections.", "Do not connect to Redis"),
    _marker(
        "future_alembic_preflight_consumer",
        "State the allowed later Alembic preflight consumer boundary.",
        "separately approved redacted Alembic preflight package",
    ),
)

NEGATION_HINTS = (
    "do not",
    "does not",
    "must not",
    "not authorize",
    "not authorized",
    "remains unauthorized",
    "forbid",
    "forbidden",
    "unauthorized",
)

FORBIDDEN_PATTERNS: tuple[ForbiddenPattern, ...] = (
    ForbiddenPattern("ssh_command", r"^\s*ssh\b", "Do not include SSH commands."),
    ForbiddenPattern(
        "repo_env_creation_command",
        r"\b(?:touch|tee|cat\s+>|printf\b.*>|install\b.*|cp\b.*|create|write|generate)\s+`?\.env`?\b",
        "Do not include repo .env creation commands.",
    ),
    ForbiddenPattern(
        "repo_env_dir_creation_command",
        r"\b(?:touch|tee|cat\s+>|printf\b.*>|install\b.*|cp\b.*|create|write|generate)\s+env/[^`\s]*\.env\b",
        "Do not include repo env/*.env creation commands.",
    ),
    ForbiddenPattern(
        "database_url_literal_assignment",
        r"^\s*database_url\s*=\s*(?!postgresql\+psycopg://github_ai_catchbot_app:<db_password_from_password_manager>@127\.0\.0\.1:5432/github_ai_catchbot\s*$)(?:postgres|postgresql)",
        "Do not include a literal DATABASE_URL value.",
    ),
    ForbiddenPattern(
        "cat_runtime_env",
        r"\bcat\s+/etc/github-ai-catchbot/runtime\.env\b",
        "Do not include cat commands for runtime.env.",
    ),
    ForbiddenPattern(
        "source_runtime_env",
        r"\bsource\s+/etc/github-ai-catchbot/runtime\.env\b",
        "Do not include source commands for runtime.env.",
    ),
    ForbiddenPattern(
        "dot_source_runtime_env",
        r"^\s*\.\s+/etc/github-ai-catchbot/runtime\.env\b",
        "Do not include dot-source commands for runtime.env.",
    ),
    ForbiddenPattern(
        "export_database_url",
        r"\bexport\s+DATABASE_URL\b",
        "Do not include DATABASE_URL export commands.",
    ),
    ForbiddenPattern(
        "export_redis_url",
        r"\bexport\s+REDIS_URL\b",
        "Do not include REDIS_URL export commands.",
    ),
    ForbiddenPattern(
        "textual_filemode_permission_extraction",
        r"stat\.filemode\(info\.st_mode\)\[-3:\]",
        "Use numeric permission extraction instead of textual file-mode rendering.",
    ),
    ForbiddenPattern(
        "enable_notification_send_true",
        r"^\s*#?\s*enable_notification_send\s*=\s*true\b",
        "Do not allow ENABLE_NOTIFICATION_SEND=true.",
    ),
    ForbiddenPattern(
        "notifier_dry_run_false",
        r"^\s*#?\s*notifier_telegram_dry_run\s*=\s*false\b",
        "Do not allow NOTIFIER_TELEGRAM_DRY_RUN=false.",
    ),
    ForbiddenPattern(
        "notifier_allow_edits_true",
        r"^\s*#?\s*notifier_telegram_allow_edits\s*=\s*true\b",
        "Do not allow NOTIFIER_TELEGRAM_ALLOW_EDITS=true.",
    ),
    ForbiddenPattern(
        "replay_to_prod_true",
        r"^\s*#?\s*enable_replay_to_prod_db\s*=\s*true\b",
        "Do not allow ENABLE_REPLAY_TO_PROD_DB=true.",
    ),
    ForbiddenPattern(
        "retry_promotion_true",
        r"^\s*#?\s*maintenance_enable_notification_retry_promotion\s*=\s*true\b",
        "Do not allow MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=true.",
    ),
    ForbiddenPattern(
        "alembic_execution",
        r"^\s*(?:python\s+-m\s+)?alembic\b|\brun\s+alembic\s+now\b|\balembic\s+upgrade\b",
        "Do not include Alembic execution.",
    ),
    ForbiddenPattern(
        "app_runtime_start",
        r"^\s*(?:python\s+main\.py|python\s+-m\s+src\b)|\bstart\s+(?:the\s+)?app\s+runtime\b",
        "Do not include app runtime startup.",
    ),
    ForbiddenPattern("psql_command", r"^\s*(?:sudo\s+-u\s+postgres\s+)?psql\b", "Do not include PostgreSQL commands."),
    ForbiddenPattern("redis_cli_command", r"^\s*redis-cli\b", "Do not include Redis commands."),
    ForbiddenPattern(
        "docker_execution",
        r"^\s*(?:sudo\s+)?docker\s+(?:compose|run|exec|start|restart|up|pull|build)\b|^\s*(?:sudo\s+)?docker-compose\s+(?:up|run|exec|start|restart|pull|build)\b",
        "Do not include Docker or Docker Compose execution.",
    ),
    ForbiddenPattern(
        "systemctl_command",
        r"^\s*(?:sudo\s+)?systemctl\b",
        "Do not include systemd unit commands.",
    ),
    ForbiddenPattern(
        "tdlib_auth",
        r"\b(?:perform|run|start)\s+tdlib\s+auth\b|\btdlib\s+auth\s+is\s+authorized\b",
        "Do not authorize TDLib auth.",
    ),
    ForbiddenPattern(
        "telegram_connection",
        r"\bconnect\s+(?:to\s+)?telegram\b|\btelegram\s+connection\s+is\s+authorized\b",
        "Do not authorize Telegram connection.",
    ),
    ForbiddenPattern(
        "live_collector_start",
        r"\b(?:start|run)\s+(?:the\s+)?live\s+collector\b|\blive\s+collector\s+start\s+is\s+authorized\b",
        "Do not authorize live collector start.",
    ),
    ForbiddenPattern(
        "notifier_transport",
        r"\b(?:enable|start|run)\s+(?:the\s+)?notifier\s+transport\b|\bnotifier\s+transport\s+is\s+authorized\b",
        "Do not authorize notifier transport.",
    ),
    ForbiddenPattern(
        "production_rollout",
        r"\bauthorize\s+(?:the\s+)?production\s+rollout\b|\bproduction\s+rollout\s+is\s+authorized\b",
        "Do not authorize production rollout.",
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the dedicated VPS runtime secret placement runbook from local repository text only. "
            "This checker does not inspect host files, read secrets, read .env files, connect to DB/Redis, "
            "call network services, call Docker, call systemd, or mutate files."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format. Defaults to text.",
    )
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root to inspect. Defaults to this script's repository root.",
    )
    return parser


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_runbook(repo_root: Path) -> tuple[bool, str]:
    runbook = repo_root / CHECKED_FILE
    if not runbook.is_file():
        return False, ""
    return True, runbook.read_text(encoding="utf-8", errors="replace")


def _missing_sections(text: str) -> list[RequiredSection]:
    return [section for section in REQUIRED_SECTIONS if text.count(section.heading) != 1]


def _missing_markers(text: str) -> list[RequiredMarker]:
    normalized = _normalize_text(text)
    return [
        marker
        for marker in REQUIRED_MARKERS
        if not any(required in normalized for required in marker.any_of)
    ]


def _is_negated_line(line: str) -> bool:
    normalized = _normalize_text(line)
    return any(hint in normalized for hint in NEGATION_HINTS)


def _matched_forbidden_patterns(text: str) -> list[ForbiddenPattern]:
    matches: list[ForbiddenPattern] = []
    for forbidden in FORBIDDEN_PATTERNS:
        if forbidden.line_scoped:
            if any(
                re.search(forbidden.pattern, line, flags=re.IGNORECASE)
                and not _is_negated_line(line)
                for line in text.splitlines()
            ):
                matches.append(forbidden)
        elif re.search(forbidden.pattern, text, flags=re.IGNORECASE):
            matches.append(forbidden)
    return matches


def _assigned_env_keys(text: str) -> set[str]:
    keys: set[str] = set()
    for line in text.splitlines():
        match = re.match(r"^\s*#?\s*([A-Z][A-Z0-9_]+)\s*=", line)
        if match:
            keys.add(match.group(1))
    return keys


def _unauthorized_assigned_keys(text: str) -> list[str]:
    allowed = set(REQUIRED_KEYS) | set(OPTIONAL_KEYS)
    return sorted(key for key in _assigned_env_keys(text) if key not in allowed)


def _missing_required_key_assignments(text: str) -> list[str]:
    assigned = _assigned_env_keys(text)
    return sorted(key for key in REQUIRED_KEYS if key not in assigned)


def _missing_optional_key_mentions(text: str) -> list[str]:
    return sorted(key for key in OPTIONAL_KEYS if key not in text)


def _missing_fixed_value_assignments(text: str) -> list[str]:
    normalized_lines = {
        line.strip().lower()
        for line in text.splitlines()
        if re.match(r"^\s*#?\s*[A-Z][A-Z0-9_]+\s*=", line)
    }
    missing = []
    for key, expected in EXPECTED_FIXED_VALUES.items():
        assignment = f"{key}={expected}".lower()
        commented_assignment = f"# {assignment}"
        if assignment not in normalized_lines and commented_assignment not in normalized_lines:
            missing.append(key)
    return sorted(missing)


def _disallowed_ip_literals(text: str) -> list[str]:
    allowed = {"127.0.0.1"}
    candidates = set(re.findall(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", text))
    disallowed = []
    for candidate in sorted(candidates):
        if candidate in allowed:
            continue
        octets = candidate.split(".")
        if all(octet.isdigit() and 0 <= int(octet) <= 255 for octet in octets):
            disallowed.append(candidate)
    return disallowed


def generate_report(repo_root: str | Path | None = None) -> CheckResult:
    resolved_repo_root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    runbook_exists, runbook_text = _read_runbook(resolved_repo_root)

    checks_failed: list[str] = []
    failures: list[dict[str, Any]] = []

    if not runbook_exists:
        checks_failed.append("runbook.exists")
        failures.append(
            {
                "check": "runbook.exists",
                "path": CHECKED_FILE,
                "message": "Dedicated VPS runtime secret placement runbook is missing.",
            }
        )
    else:
        missing_sections = _missing_sections(runbook_text)
        if missing_sections:
            checks_failed.append("runbook.required_sections")
            for section in missing_sections:
                failures.append(
                    {
                        "check": f"runbook.required_section:{section.heading}",
                        "path": CHECKED_FILE,
                        "message": "Required section heading is missing or duplicated.",
                    }
                )

        missing_markers = _missing_markers(runbook_text)
        if missing_markers:
            checks_failed.append("runbook.required_markers")
            for marker in missing_markers:
                failures.append(
                    {
                        "check": f"runbook.required_marker:{marker.name}",
                        "path": CHECKED_FILE,
                        "message": marker.message,
                    }
                )

        missing_keys = _missing_required_key_assignments(runbook_text)
        if missing_keys:
            checks_failed.append("runbook.required_key_assignments")
            failures.append(
                {
                    "check": "runbook.required_key_assignments",
                    "path": CHECKED_FILE,
                    "message": "Runbook template is missing required key assignments.",
                    "missing_keys": missing_keys,
                }
            )

        missing_optional = _missing_optional_key_mentions(runbook_text)
        if missing_optional:
            checks_failed.append("runbook.optional_key_mentions")
            failures.append(
                {
                    "check": "runbook.optional_key_mentions",
                    "path": CHECKED_FILE,
                    "message": "Runbook is missing optional allowed key mentions.",
                    "missing_keys": missing_optional,
                }
            )

        missing_fixed_values = _missing_fixed_value_assignments(runbook_text)
        if missing_fixed_values:
            checks_failed.append("runbook.fixed_safe_values")
            failures.append(
                {
                    "check": "runbook.fixed_safe_values",
                    "path": CHECKED_FILE,
                    "message": "Runbook is missing required fixed safe gate values.",
                    "missing_keys": missing_fixed_values,
                }
            )

        unauthorized_keys = _unauthorized_assigned_keys(runbook_text)
        if unauthorized_keys:
            checks_failed.append("runbook.unauthorized_key_assignments")
            failures.append(
                {
                    "check": "runbook.unauthorized_key_assignments",
                    "path": CHECKED_FILE,
                    "message": "Runbook contains key assignments outside the allowed key set.",
                    "unauthorized_keys": unauthorized_keys,
                }
            )

        forbidden_matches = _matched_forbidden_patterns(runbook_text)
        if forbidden_matches:
            checks_failed.append("runbook.forbidden_authorization")
            for forbidden in forbidden_matches:
                failures.append(
                    {
                        "check": f"runbook.forbidden_authorization:{forbidden.name}",
                        "path": CHECKED_FILE,
                        "message": forbidden.message,
                    }
                )

        disallowed_ips = _disallowed_ip_literals(runbook_text)
        if disallowed_ips:
            checks_failed.append("runbook.raw_ip_literals")
            failures.append(
                {
                    "check": "runbook.raw_ip_literals",
                    "path": CHECKED_FILE,
                    "message": "Runbook contains raw IP literals outside allowed loopback values.",
                    "ip_literals": disallowed_ips,
                }
            )

    report = {
        "report_type": REPORT_TYPE,
        "contract_status": "failed" if checks_failed else "passed",
        "checked_file": CHECKED_FILE,
        "secret_path": SECRET_PATH,
        "secret_dir": SECRET_DIR,
        "required_keys": list(REQUIRED_KEYS),
        "optional_keys": list(OPTIONAL_KEYS),
        "expected_fixed_values": dict(EXPECTED_FIXED_VALUES),
        "checks_failed": checks_failed,
        "failures": failures,
        "operator_authorization": dict(OPERATOR_AUTHORIZATION),
        "checker_side_effects": dict(CHECKER_SIDE_EFFECTS),
    }
    return CheckResult(exit_code=1 if checks_failed else 0, report=report)


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def render_text(report: dict[str, Any]) -> str:
    lines = [
        f"report_type: {report['report_type']}",
        f"contract_status: {report['contract_status']}",
        f"checked_file: {report['checked_file']}",
        f"secret_path: {report['secret_path']}",
        f"checks_failed: {', '.join(report['checks_failed']) if report['checks_failed'] else 'none'}",
    ]
    for failure in report["failures"]:
        lines.append(f"failure: {failure['check']} - {failure['message']}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(args.repo_root)
    if args.format == "json":
        sys.stdout.write(render_json(result.report))
    else:
        sys.stdout.write(render_text(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
