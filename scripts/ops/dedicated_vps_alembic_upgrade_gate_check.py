from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPORT_TYPE = "dedicated_vps_alembic_upgrade_gate_check_v1"
CHECKED_FILE = "ops/pipeline/runbooks/dedicated_vps_alembic_upgrade_gate.md"

AUTHORIZATION = {
    "repo_alembic_asset_check_present": True,
    "redacted_runtime_env_validation_present": True,
    "pre_upgrade_current_template_present": True,
    "explicit_upgrade_approval_checkpoint_present": True,
    "upgrade_head_template_present": True,
    "post_upgrade_current_template_present": True,
    "runtime_env_file_read_by_checker": False,
    "env_vars_read_by_checker": False,
    "db_connection_by_checker": False,
    "alembic_execution_by_checker": False,
    "alembic_upgrade_authorized_by_this_slice": False,
    "alembic_upgrade_template_after_approval_present": True,
    "alembic_downgrade_authorized": False,
    "alembic_stamp_authorized": False,
    "alembic_revision_authorized": False,
    "app_runtime_authorized": False,
    "tdlib_telegram_authorized": False,
    "live_collector_authorized": False,
    "notifier_transport_authorized": False,
    "production_rollout_authorized": False,
}

CHECKER_SIDE_EFFECTS = {
    "host_inspection_performed": False,
    "secret_file_read": False,
    "env_vars_read": False,
    "commands_executed": False,
    "files_mutated": False,
    "database_connected": False,
    "redis_connected": False,
}


@dataclass(frozen=True, slots=True)
class RequiredSection:
    heading: str


@dataclass(frozen=True, slots=True)
class RequiredMarker:
    name: str
    any_of: tuple[str, ...]
    message: str


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


REQUIRED_SECTIONS = (
    RequiredSection("# Dedicated VPS Alembic upgrade execution gate"),
    RequiredSection("## Scope"),
    RequiredSection("## Source-of-truth handling"),
    RequiredSection("## Current prerequisite state"),
    RequiredSection("## Allowed upgrade gate checks"),
    RequiredSection("## Explicitly forbidden actions"),
    RequiredSection("## Operator command blocks"),
    RequiredSection("## Redacted validation rules"),
    RequiredSection("## Failure handling"),
    RequiredSection("## What output to bring back to ChatGPT"),
    RequiredSection("## What remains unauthorized"),
    RequiredSection("## Next step"),
)

REQUIRED_MARKERS = (
    _marker("checked_file_path", "Mention the checked runbook path.", CHECKED_FILE),
    _marker("runtime_env_path", "Mention the dedicated VPS runtime env path.", "/etc/github-ai-catchbot/runtime.env"),
    _marker("alembic_ini", "Require an alembic.ini asset check.", "alembic.ini"),
    _marker("migrations_dir", "Require a migrations directory check.", "migrations"),
    _marker("migrations_env", "Require a migrations/env.py asset check.", "migrations/env.py"),
    _marker("migrations_versions", "Require a migrations/versions asset check.", "migrations/versions"),
    _marker("find_versions", "Require safe migration filename listing.", "find migrations/versions"),
    _marker(
        "database_url_prefix",
        "Require the expected DATABASE_URL application-role prefix shape.",
        "postgresql+psycopg://github_ai_catchbot_app:",
    ),
    _marker(
        "database_url_host_db",
        "Require the expected DATABASE_URL loopback host/database shape.",
        "@127.0.0.1:5432/github_ai_catchbot",
    ),
    _marker("redis_url", "Require the expected Redis URL fixed value.", "REDIS_URL=redis://127.0.0.1:6379/0"),
    _marker("notification_send_disabled", "Require notification send disabled.", "ENABLE_NOTIFICATION_SEND=false"),
    _marker("notifier_dry_run", "Require notifier dry-run enabled.", "NOTIFIER_TELEGRAM_DRY_RUN=true"),
    _marker(
        "notifier_edits_disabled",
        "Require notifier edits disabled.",
        "NOTIFIER_TELEGRAM_ALLOW_EDITS=false",
    ),
    _marker("replay_to_prod_disabled", "Require replay-to-prod disabled.", "ENABLE_REPLAY_TO_PROD_DB=false"),
    _marker(
        "retry_promotion_disabled",
        "Require retry promotion disabled.",
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false",
    ),
    _marker(
        "pre_upgrade_current_exit",
        "Require pre-upgrade Alembic current evidence.",
        "pre_upgrade_alembic_current_exit_code",
    ),
    _marker("upgrade_exit", "Require Alembic upgrade evidence.", "alembic_upgrade_exit_code"),
    _marker(
        "post_upgrade_current_exit",
        "Require post-upgrade Alembic current evidence.",
        "post_upgrade_alembic_current_exit_code",
    ),
    _marker("database_url_printed_false", "Require database_url_printed=false evidence.", "database_url_printed=false"),
    _marker("alembic_upgrade_run_true", "Require upgrade-run true evidence for Block 5.", "alembic_upgrade_run=true"),
    _marker("alembic_stamp_run_false", "Require stamp false evidence.", "alembic_stamp_run=false"),
    _marker("alembic_revision_run_false", "Require revision false evidence.", "alembic_revision_run=false"),
    _marker("alembic_downgrade_run_false", "Require downgrade false evidence.", "alembic_downgrade_run=false"),
    _marker(
        "explicit_stop_checkpoint",
        "Require explicit stop before Block 5.",
        "STOP: do not run Block 5 unless the user explicitly approves Alembic upgrade execution now",
    ),
    _marker(
        "run_only_after_approval",
        "Require approval-only wording for the upgrade template.",
        "Run only after explicit user approval",
    ),
    _marker("db_schema_mutation", "Require DB schema mutation warning.", "DB schema mutation"),
    _marker("alembic_current_command", "Document the read-only Alembic current command.", "python -m alembic current"),
    _marker("alembic_upgrade_command", "Document the approved upgrade command.", "python -m alembic upgrade head"),
    _marker("no_downgrade", "Explicitly forbid Alembic downgrade.", "Do not run `alembic downgrade`"),
    _marker("no_stamp", "Explicitly forbid Alembic stamp.", "Do not run `alembic stamp`"),
    _marker("no_revision", "Explicitly forbid Alembic revision.", "Do not run `alembic revision`"),
)

NEGATION_HINTS = (
    "do not",
    "does not",
    "must not",
    "not authorize",
    "not authorized",
    "not performed",
    "remains unauthorized",
    "remain unauthorized",
    "forbid",
    "forbidden",
    "unauthorized",
    "no ",
    "without",
)

FORBIDDEN_PATTERNS = (
    ForbiddenPattern(
        "cat_runtime_env",
        r"\bcat\s+(?:`)?/etc/github-ai-catchbot/runtime\.env(?:`)?\b",
        "Do not include cat commands for runtime.env.",
    ),
    ForbiddenPattern(
        "source_runtime_env",
        r"\bsource\s+(?:`)?/etc/github-ai-catchbot/runtime\.env(?:`)?\b",
        "Do not include source commands for runtime.env.",
    ),
    ForbiddenPattern(
        "dot_source_runtime_env",
        r"^\s*\.\s+(?:`)?/etc/github-ai-catchbot/runtime\.env(?:`)?\b",
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
        "direct_database_url_alembic_upgrade",
        r"\bDATABASE_URL\s*=\s*\S+\s+(?:python\s+-m\s+)?alembic\s+upgrade\s+head\b",
        "Do not include direct shell DATABASE_URL=... alembic upgrade head.",
    ),
    ForbiddenPattern(
        "direct_database_url_alembic_current",
        r"\bDATABASE_URL\s*=\s*\S+\s+(?:python\s+-m\s+)?alembic\s+current\b",
        "Do not include direct shell DATABASE_URL=... alembic current.",
    ),
    ForbiddenPattern("alembic_downgrade", r"\balembic\s+downgrade\b", "Do not authorize Alembic downgrade."),
    ForbiddenPattern("alembic_stamp", r"\balembic\s+stamp\b", "Do not authorize Alembic stamp."),
    ForbiddenPattern("alembic_revision", r"\balembic\s+revision\b", "Do not authorize Alembic revision."),
    ForbiddenPattern(
        "app_runtime_start",
        r"^\s*(?:python\s+main\.py|python\s+-m\s+src\b)|\b(?:start|run)\s+(?:the\s+)?app\s+runtime\b",
        "Do not authorize app runtime.",
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
    ForbiddenPattern(
        "docker_execution",
        r"^\s*(?:sudo\s+)?docker\s+(?:compose|run|exec|start|restart|up|pull|build)\b|^\s*(?:sudo\s+)?docker-compose\s+(?:up|run|exec|start|restart|pull|build)\b",
        "Do not include Docker or Docker Compose execution.",
    ),
    ForbiddenPattern(
        "systemd_unit_changes",
        r"^\s*(?:sudo\s+)?systemctl\b|\bmodify\s+systemd\s+units?\b|\bsystemd\s+unit\s+changes?\s+are\s+authorized\b",
        "Do not include systemd unit changes.",
    ),
    ForbiddenPattern(
        "repo_env_creation",
        r"\b(?:touch|tee|cat\s+>|printf\b.*>|install\b.*|cp\b.*|create|write|generate)\s+`?\.env`?\b|`?\.env`?\s+creation\s+is\s+authorized\b",
        "Do not authorize repo .env creation.",
    ),
    ForbiddenPattern(
        "repo_env_dir_creation",
        r"\b(?:touch|tee|cat\s+>|printf\b.*>|install\b.*|cp\b.*|create|write|generate)\s+`?env/[^`\s]*\.env`?\b|`?env/\*\.env`?\s+creation\s+is\s+authorized\b",
        "Do not authorize repo env/*.env creation.",
    ),
    ForbiddenPattern(
        "migration_editing",
        r"\b(?:edit|modify|change|rewrite|patch)\s+(?:the\s+)?migration\s+files?\b|\bmigration\s+file\s+editing\s+is\s+authorized\b",
        "Do not authorize migration file editing.",
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the dedicated VPS Alembic upgrade gate runbook from local repository text only. "
            "This checker does not read runtime.env, inspect environment variables, execute commands, "
            "connect to DB/Redis, run Alembic, or mutate files."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--format", choices=("text", "json"), default="text", help="Output format. Defaults to text.")
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
    return any(hint in normalized for hint in NEGATION_HINTS) or normalized.endswith(" no")


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
                "message": "Dedicated VPS Alembic upgrade gate runbook is missing.",
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
                        "message": "Required section is missing or appears more than once.",
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

    report = {
        "report_type": REPORT_TYPE,
        "contract_status": "failed" if checks_failed else "passed",
        "checked_file": CHECKED_FILE,
        "checks_failed": checks_failed,
        "failures": failures,
        "authorization": dict(AUTHORIZATION),
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
