from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPORT_TYPE = "dedicated_vps_post_migration_db_acceptance_smoke_check_v1"
RUNBOOK_PATH = "ops/pipeline/runbooks/dedicated_vps_post_migration_db_acceptance_smoke.md"
MIGRATIONS_DIR = "migrations/versions"
EXPECTED_TERMINAL_REVISION = "0004_judge_delivery_obs"

SIDE_EFFECTS = {
    "runtime_env_read": False,
    "env_vars_read": False,
    "database_connected": False,
    "redis_connected": False,
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
    "secret_values_printed": False,
}


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
class MigrationFacts:
    migration_files: list[str]
    revision_ids: list[str]
    tables: list[str]
    indexes: list[str]
    constraints: list[str]


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


REQUIRED_MARKERS = (
    _marker("purpose", "State the post-migration DB acceptance purpose.", "Verify the upgraded PostgreSQL database"),
    _marker("read_only_scope", "State read-only DB metadata/queryability scope.", "read-only DB metadata/queryability smoke"),
    _marker("readme_authority", "Preserve README v20 authority.", "README_replacement_consolidated_v0_20.md` remains authoritative"),
    _marker(
        "architecture_invariant",
        "Preserve the canonical architecture invariant.",
        "SourceMessage -> Artifact -> CandidateGroup -> EvidenceBundle -> JudgeOutput -> Analysis -> Notification",
    ),
    _marker("postgres_record", "Preserve PostgreSQL as durable source of record.", "PostgreSQL is durable source of record"),
    _marker("redis_boundary", "Preserve Redis queue/lock/short-lived state boundary.", "Redis is queue, lock, and short-lived execution state only"),
    _marker("production_unauthorized", "State production rollout remains unauthorized.", "production rollout remains unauthorized"),
    _marker("runtime_env_path", "Mention the host runtime env path.", "/etc/github-ai-catchbot/runtime.env"),
    _marker("alembic_recorded", "State Alembic upgrade was already completed and recorded.", "Alembic upgrade already completed and recorded"),
    _marker("terminal_revision", "Mention expected terminal revision.", EXPECTED_TERMINAL_REVISION),
    _marker("separate_approval", "Require separate approval before future smoke execution.", "Future execution of the smoke on VPS is separately approved later"),
    _marker("does_not_authorize_now", "State this package does not authorize execution now.", "This package does not authorize execution now"),
    _marker("no_app_runtime", "Forbid app runtime.", "No app runtime", "Do not run app runtime"),
    _marker("no_tdlib", "Forbid TDLib.", "No TDLib", "Do not run TDLib auth"),
    _marker("no_telegram", "Forbid Telegram.", "No Telegram", "Do not connect Telegram"),
    _marker("no_live_collector", "Forbid live collector.", "No live collector", "Do not start live collector"),
    _marker("no_notifier", "Forbid notifier transport.", "No notifier transport", "Do not enable notifier transport"),
    _marker("no_production_rollout", "Forbid production rollout.", "No production rollout", "Do not perform production rollout"),
    _marker("no_redis_mutation", "Forbid Redis mutation.", "No Redis mutation", "Do not mutate Redis"),
    _marker("no_db_mutation", "Forbid DB mutation.", "No DB mutation", "Do not mutate the database"),
    _marker("no_alembic_upgrade", "Forbid Alembic upgrade in the future smoke.", "Do not run `alembic upgrade` in the future smoke"),
    _marker("no_alembic_downgrade", "Forbid Alembic downgrade in the future smoke.", "Do not run `alembic downgrade` in the future smoke"),
    _marker("no_alembic_stamp", "Forbid Alembic stamp in the future smoke.", "Do not run `alembic stamp` in the future smoke"),
    _marker("no_alembic_revision", "Forbid Alembic revision in the future smoke.", "Do not run `alembic revision` in the future smoke"),
    _marker("no_secret_printing", "Forbid secret printing.", "Do not print any secret value", "Do not print secret values"),
    _marker("no_database_url_printing", "Forbid DATABASE_URL printing.", "Do not print `DATABASE_URL`"),
    _marker("no_db_password_printing", "Forbid DB password printing.", "Do not print DB password"),
    _marker("no_cat_runtime_env", "Forbid cat runtime.env.", "Do not `cat /etc/github-ai-catchbot/runtime.env`"),
    _marker("no_source_runtime_env", "Forbid source runtime.env.", "Do not `source /etc/github-ai-catchbot/runtime.env`"),
    _marker("no_dot_source_runtime_env", "Forbid dot-source runtime.env.", "Do not dot-source `/etc/github-ai-catchbot/runtime.env`"),
    _marker("no_export_database_url", "Forbid export DATABASE_URL.", "Do not `export DATABASE_URL`"),
    _marker("no_export_redis_url", "Forbid export REDIS_URL.", "Do not `export REDIS_URL`"),
    _marker("redacted_python_helper", "Require redacted Python helper only.", "Use a redacted Python helper only"),
    _marker("load_runtime_env_inside_python", "Future smoke must load runtime env inside Python without printing.", "Load runtime env from `/etc/github-ai-catchbot/runtime.env` inside Python without printing values"),
    _marker("read_only_postgres_connection", "Future smoke must be read-only PostgreSQL only.", "Connect to PostgreSQL read-only using SQLAlchemy/psycopg"),
    _marker("alembic_version_exists", "Future smoke must verify alembic_version exists.", "Verify `alembic_version` exists"),
    _marker("derive_from_migrations", "Expected facts must be derived from migrations.", "Derive expected table, index, and simple constraint facts from `migrations/versions/*.py`, not from memory"),
    _marker("verify_expected_tables", "Future smoke must verify expected tables.", "Verify expected tables exist"),
    _marker("query_key_tables", "Future smoke must query key tables read-only.", "Verify key tables are queryable via metadata-only reads or `SELECT COUNT(*)`"),
    _marker("print_redacted_json", "Future smoke must print only redacted JSON.", "Print only redacted JSON result"),
    _marker("no_docker", "Do not authorize Docker.", "No Docker or Docker Compose", "Do not use Docker or Docker Compose"),
    _marker("no_systemd", "Do not authorize systemd modification.", "No systemd modification", "Do not modify systemd units"),
    _marker("no_migration_edits", "Do not authorize migration edits.", "No migration edits", "Do not edit migration files"),
    _marker("secret_values_printed_false", "Expected result must include secret_values_printed=false.", '"secret_values_printed": false'),
    _marker("database_url_printed_false", "Expected result must include database_url_printed=false.", '"database_url_printed": false'),
    _marker("db_write_performed_false", "Expected result must include db_write_performed=false.", '"db_write_performed": false'),
    _marker("redis_connected_false", "Expected result must include redis_connected=false.", '"redis_connected": false'),
    _marker("redis_mutation_performed_false", "Expected result must include redis_mutation_performed=false.", '"redis_mutation_performed": false'),
    _marker("alembic_upgrade_run_false", "Expected result must include alembic_upgrade_run=false.", '"alembic_upgrade_run": false'),
    _marker("alembic_downgrade_run_false", "Expected result must include alembic_downgrade_run=false.", '"alembic_downgrade_run": false'),
    _marker("alembic_stamp_run_false", "Expected result must include alembic_stamp_run=false.", '"alembic_stamp_run": false'),
    _marker("alembic_revision_run_false", "Expected result must include alembic_revision_run=false.", '"alembic_revision_run": false'),
    _marker("app_runtime_started_false", "Expected result must include app_runtime_started=false.", '"app_runtime_started": false'),
    _marker("tdlib_auth_performed_false", "Expected result must include tdlib_auth_performed=false.", '"tdlib_auth_performed": false'),
    _marker("telegram_connected_false", "Expected result must include telegram_connected=false.", '"telegram_connected": false'),
    _marker("live_collector_started_false", "Expected result must include live_collector_started=false.", '"live_collector_started": false'),
    _marker("notifier_transport_enabled_false", "Expected result must include notifier_transport_enabled=false.", '"notifier_transport_enabled": false'),
    _marker("production_rollout_performed_false", "Expected result must include production_rollout_performed=false.", '"production_rollout_performed": false'),
)

NEGATION_HINTS = (
    "do not",
    "does not",
    "must not",
    "not authorize",
    "not authorized",
    "not performed",
    "already completed",
    "already recorded",
    "remains unauthorized",
    "remain unauthorized",
    "forbid",
    "forbidden",
    "unauthorized",
    "no ",
    "without",
)

FORBIDDEN_PATTERNS = (
    ForbiddenPattern("cat_runtime_env", r"\bcat\s+(?:`)?/etc/github-ai-catchbot/runtime\.env(?:`)?\b", "Do not include cat commands for runtime.env."),
    ForbiddenPattern("source_runtime_env", r"\bsource\s+(?:`)?/etc/github-ai-catchbot/runtime\.env(?:`)?\b", "Do not include source commands for runtime.env."),
    ForbiddenPattern("dot_source_runtime_env", r"^\s*\.\s+(?:`)?/etc/github-ai-catchbot/runtime\.env(?:`)?\b", "Do not include dot-source commands for runtime.env."),
    ForbiddenPattern("export_database_url", r"\bexport\s+DATABASE_URL\b", "Do not include DATABASE_URL export commands."),
    ForbiddenPattern("export_redis_url", r"\bexport\s+REDIS_URL\b", "Do not include REDIS_URL export commands."),
    ForbiddenPattern(
        "alembic_upgrade",
        r"^\s*(?:python\s+-m\s+)?alembic\s+upgrade\s+head\b|\balembic\s+upgrade\s+is\s+authorized\b",
        "Do not authorize Alembic upgrade.",
    ),
    ForbiddenPattern(
        "alembic_downgrade",
        r"^\s*(?:python\s+-m\s+)?alembic\s+downgrade\b|\balembic\s+downgrade\s+is\s+authorized\b",
        "Do not authorize Alembic downgrade.",
    ),
    ForbiddenPattern(
        "alembic_stamp",
        r"^\s*(?:python\s+-m\s+)?alembic\s+stamp\b|\balembic\s+stamp\s+is\s+authorized\b",
        "Do not authorize Alembic stamp.",
    ),
    ForbiddenPattern(
        "alembic_revision",
        r"^\s*(?:python\s+-m\s+)?alembic\s+revision\b|\balembic\s+revision\s+is\s+authorized\b",
        "Do not authorize Alembic revision.",
    ),
    ForbiddenPattern("db_mutation", r"\b(?:insert|update|delete|truncate|drop|alter|create)\s+(?:into\s+)?(?:table\s+)?[a-z_][a-z0-9_]*\b|\bdb mutation is authorized\b", "Do not authorize DB mutation."),
    ForbiddenPattern(
        "redis_mutation",
        r"^\s*redis-(?:cli|server)\b|^\s*(?:xadd|xdel|set|del|hset|lpush|rpush)\b|\bredis mutation is authorized\b",
        "Do not authorize Redis mutation.",
    ),
    ForbiddenPattern("app_runtime_start", r"^\s*(?:python\s+main\.py|python\s+-m\s+src\b)|\b(?:start|run)\s+(?:the\s+)?app\s+runtime\b", "Do not authorize app runtime."),
    ForbiddenPattern("tdlib_auth", r"\b(?:perform|run|start)\s+tdlib\s+auth\b|\btdlib\s+auth\s+is\s+authorized\b", "Do not authorize TDLib auth."),
    ForbiddenPattern("telegram_connection", r"\bconnect\s+(?:to\s+)?telegram\b|\btelegram\s+connection\s+is\s+authorized\b", "Do not authorize Telegram connection."),
    ForbiddenPattern("live_collector_start", r"\b(?:start|run)\s+(?:the\s+)?live\s+collector\b|\blive\s+collector\s+start\s+is\s+authorized\b", "Do not authorize live collector start."),
    ForbiddenPattern("notifier_transport", r"\b(?:enable|start|run)\s+(?:the\s+)?notifier\s+transport\b|\bnotifier\s+transport\s+is\s+authorized\b", "Do not authorize notifier transport."),
    ForbiddenPattern("production_rollout", r"\bauthorize\s+(?:the\s+)?production\s+rollout\b|\bproduction\s+rollout\s+is\s+authorized\b", "Do not authorize production rollout."),
    ForbiddenPattern("docker_execution", r"^\s*(?:sudo\s+)?docker\s+(?:compose|run|exec|start|restart|up|pull|build)\b|^\s*(?:sudo\s+)?docker-compose\s+(?:up|run|exec|start|restart|pull|build)\b", "Do not include Docker or Docker Compose execution."),
    ForbiddenPattern("systemd_unit_changes", r"^\s*(?:sudo\s+)?systemctl\b|\bmodify\s+systemd\s+units?\b|\bsystemd\s+unit\s+changes?\s+are\s+authorized\b", "Do not include systemd unit changes."),
    ForbiddenPattern("migration_editing", r"\b(?:edit|modify|change|rewrite|patch)\s+(?:the\s+)?migration\s+files?\b|\bmigration\s+file\s+editing\s+is\s+authorized\b", "Do not authorize migration file editing."),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the dedicated VPS post-migration DB acceptance smoke runbook from local "
            "repository text only. This checker does not read runtime.env, inspect environment "
            "variables, execute commands, connect to DB/Redis, run Alembic, or mutate files."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--format", choices=("text", "json"), default="text", help="Output format. Defaults to text.")
    parser.add_argument("--repo-root", default=None, help="Repository root to inspect. Defaults to this script's repository root.")
    return parser


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_runbook(repo_root: Path) -> tuple[bool, str]:
    runbook = repo_root / RUNBOOK_PATH
    if not runbook.is_file():
        return False, ""
    return True, runbook.read_text(encoding="utf-8", errors="replace")


def _missing_markers(text: str) -> list[RequiredMarker]:
    normalized = _normalize_text(text)
    return [marker for marker in REQUIRED_MARKERS if not any(required in normalized for required in marker.any_of)]


def _is_negated_line(line: str) -> bool:
    normalized = _normalize_text(line)
    return any(hint in normalized for hint in NEGATION_HINTS) or normalized.endswith(" no")


def _matched_forbidden_patterns(text: str) -> list[ForbiddenPattern]:
    matches: list[ForbiddenPattern] = []
    for forbidden in FORBIDDEN_PATTERNS:
        if forbidden.line_scoped:
            if any(
                re.search(forbidden.pattern, line, flags=re.IGNORECASE) and not _is_negated_line(line)
                for line in text.splitlines()
            ):
                matches.append(forbidden)
        elif re.search(forbidden.pattern, text, flags=re.IGNORECASE):
            matches.append(forbidden)
    return matches


def _literal_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


def _collect_unique(values: list[str]) -> list[str]:
    return sorted(set(values))


def derive_migration_facts(repo_root: str | Path | None = None) -> MigrationFacts:
    resolved_repo_root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    migrations_root = resolved_repo_root / MIGRATIONS_DIR
    migration_paths = sorted(migrations_root.glob("*.py")) if migrations_root.is_dir() else []

    revision_ids: list[str] = []
    tables: list[str] = []
    indexes: list[str] = []
    constraints: list[str] = []

    for path in migration_paths:
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "revision":
                        revision = _literal_string(node.value)
                        if revision:
                            revision_ids.append(revision)
            if isinstance(node, ast.Call):
                call_name = _call_name(node.func)
                if call_name == "create_table" and node.args:
                    table_name = _literal_string(node.args[0])
                    if table_name:
                        tables.append(table_name)
                elif call_name == "create_index" and node.args:
                    index_name = _literal_string(node.args[0])
                    if index_name:
                        indexes.append(index_name)
                elif call_name and call_name.startswith("create_") and call_name.endswith("_constraint"):
                    if node.args:
                        constraint_name = _literal_string(node.args[0])
                        if constraint_name:
                            constraints.append(constraint_name)
                elif call_name and call_name.endswith("Constraint"):
                    for keyword in node.keywords:
                        if keyword.arg == "name":
                            constraint_name = _literal_string(keyword.value)
                            if constraint_name:
                                constraints.append(constraint_name)

    return MigrationFacts(
        migration_files=[str(path.relative_to(resolved_repo_root)) for path in migration_paths],
        revision_ids=_collect_unique(revision_ids),
        tables=_collect_unique(tables),
        indexes=_collect_unique(indexes),
        constraints=_collect_unique(constraints),
    )


def generate_report(repo_root: str | Path | None = None) -> CheckResult:
    resolved_repo_root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    runbook_exists, runbook_text = _read_runbook(resolved_repo_root)
    migration_facts = derive_migration_facts(resolved_repo_root)

    checks_failed: list[str] = []
    failures: list[dict[str, Any]] = []

    if not runbook_exists:
        checks_failed.append("runbook.exists")
        failures.append({"check": "runbook.exists", "path": RUNBOOK_PATH, "message": "Runbook is missing."})
    else:
        missing_markers = _missing_markers(runbook_text)
        if missing_markers:
            checks_failed.append("runbook.required_markers")
            for marker in missing_markers:
                failures.append({"check": f"runbook.required_marker:{marker.name}", "path": RUNBOOK_PATH, "message": marker.message})

        forbidden_matches = _matched_forbidden_patterns(runbook_text)
        if forbidden_matches:
            checks_failed.append("runbook.forbidden_authorization")
            for forbidden in forbidden_matches:
                failures.append({"check": f"runbook.forbidden_authorization:{forbidden.name}", "path": RUNBOOK_PATH, "message": forbidden.message})

    if not migration_facts.migration_files:
        checks_failed.append("migrations.files_found")
        failures.append({"check": "migrations.files_found", "path": MIGRATIONS_DIR, "message": "No migration files were found."})

    if not migration_facts.tables:
        checks_failed.append("migrations.tables_derived")
        failures.append({"check": "migrations.tables_derived", "path": MIGRATIONS_DIR, "message": "No table names were derived from migration files."})

    terminal_revision_in_migrations = EXPECTED_TERMINAL_REVISION in migration_facts.revision_ids
    terminal_revision_in_runbook = EXPECTED_TERMINAL_REVISION in runbook_text
    if not terminal_revision_in_migrations or not terminal_revision_in_runbook:
        checks_failed.append("migrations.terminal_revision")
        failures.append(
            {
                "check": "migrations.terminal_revision",
                "path": MIGRATIONS_DIR,
                "message": f"Expected terminal revision {EXPECTED_TERMINAL_REVISION} must appear in migration files and runbook text.",
                "in_migrations": terminal_revision_in_migrations,
                "in_runbook": terminal_revision_in_runbook,
            }
        )

    report = {
        "report_type": REPORT_TYPE,
        "contract_status": "failed" if checks_failed else "passed",
        "checks_failed": checks_failed,
        "failures": failures,
        "runbook_path": RUNBOOK_PATH,
        "migration_files_inspected": migration_facts.migration_files,
        "derived_revision_ids": migration_facts.revision_ids,
        "derived_table_count": len(migration_facts.tables),
        "derived_expected_tables_sample": migration_facts.tables[:12],
        "derived_index_count": len(migration_facts.indexes),
        "derived_constraint_count": len(migration_facts.constraints),
        "repo_text_only": True,
        **SIDE_EFFECTS,
    }
    return CheckResult(exit_code=1 if checks_failed else 0, report=report)


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def render_text(report: dict[str, Any]) -> str:
    lines = [
        f"report_type: {report['report_type']}",
        f"contract_status: {report['contract_status']}",
        f"runbook_path: {report['runbook_path']}",
        f"migration_files_inspected: {len(report['migration_files_inspected'])}",
        f"derived_table_count: {report['derived_table_count']}",
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
