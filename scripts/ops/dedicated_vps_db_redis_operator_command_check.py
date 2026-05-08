from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPORT_TYPE = "dedicated_vps_db_redis_operator_command_check_v1"
CHECKED_FILE = "ops/pipeline/runbooks/dedicated_vps_db_redis_operator_provisioning.md"

OPERATOR_AUTHORIZATION = {
    "postgresql_apt_install_operator_command_present": True,
    "redis_apt_install_operator_command_present": True,
    "postgresql_systemd_operator_command_present": True,
    "redis_systemd_operator_command_present": True,
    "postgresql_local_bind_operator_command_present": True,
    "redis_local_bind_operator_command_present": True,
    "postgresql_role_database_operator_command_present": True,
    "local_health_check_operator_commands_present": True,
    "docker_install_authorized": False,
    "docker_compose_authorized": False,
    "env_creation_authorized": False,
    "secret_printing_authorized": False,
    "alembic_authorized": False,
    "app_runtime_authorized": False,
    "tdlib_telegram_authorized": False,
    "live_collector_authorized": False,
    "notifier_transport_authorized": False,
    "production_rollout_authorized": False,
}

CHECKER_SIDE_EFFECTS = {
    "host_inspection_performed": False,
    "network_called": False,
    "services_mutated": False,
    "secrets_read": False,
    "env_file_read": False,
    "files_mutated": False,
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


REQUIRED_MARKERS: tuple[RequiredMarker, ...] = (
    _marker(
        "immediate_host_apt_systemd_path",
        "State host apt/systemd PostgreSQL plus host apt/systemd Redis as the immediate DB/Redis path.",
        "immediate db/redis provisioning uses host apt/systemd postgresql + host apt/systemd redis",
        "host apt/systemd postgresql + host apt/systemd redis",
    ),
    _marker(
        "docker_compose_future_candidate_not_discarded",
        "State that Docker Compose remains a future full app stack candidate and is not discarded.",
        "docker compose remains a future full app stack candidate and is not discarded",
    ),
    _marker("sudo_apt_get_update", "Include the apt update operator command.", "sudo apt-get update"),
    _marker(
        "postgresql_redis_apt_install",
        "Include PostgreSQL and Redis apt package installation in one reviewed operator command.",
        "sudo apt-get install -y postgresql postgresql-contrib redis-server redis-tools",
    ),
    _marker(
        "systemd_enable",
        "Include PostgreSQL and Redis systemd enable operator command.",
        "sudo systemctl enable postgresql redis-server",
    ),
    _marker(
        "systemd_restart",
        "Include PostgreSQL and Redis systemd restart operator command.",
        "sudo systemctl restart postgresql redis-server",
    ),
    _marker(
        "postgresql_listen_addresses_local",
        "Set PostgreSQL listen_addresses to local loopback only.",
        "set listen_addresses \"'127.0.0.1'\"",
    ),
    _marker(
        "postgresql_listen_addresses_config_quoted",
        "Document that PostgreSQL listen_addresses must be written as a quoted config value.",
        "listen_addresses = '127.0.0.1'",
    ),
    _marker(
        "postgresql_cluster_count_guard",
        "Count PostgreSQL clusters before selecting PG_VERSION and PG_CLUSTER.",
        "pg_cluster_count",
    ),
    _marker(
        "postgresql_exactly_one_cluster_guard",
        "Require exactly one PostgreSQL cluster before selecting PG_VERSION and PG_CLUSTER.",
        "expected exactly one postgresql cluster",
    ),
    _marker(
        "postgresql_password_encryption_scram",
        "Set PostgreSQL password_encryption to scram-sha-256.",
        "set password_encryption 'scram-sha-256'",
    ),
    _marker("redis_bind_loopback", "Set Redis bind to loopback addresses only.", "bind 127.0.0.1 ::1"),
    _marker(
        "redis_bind_sudo_grep",
        "Use sudo when checking the Redis bind directive in /etc/redis/redis.conf.",
        "if sudo grep -qe '^[#[:space:]]*bind ' /etc/redis/redis.conf; then",
        "if sudo grep -qE '^[#[:space:]]*bind ' /etc/redis/redis.conf; then",
    ),
    _marker("redis_protected_mode", "Set Redis protected-mode to yes.", "protected-mode yes"),
    _marker(
        "redis_protected_mode_sudo_grep",
        "Use sudo when checking the Redis protected-mode directive in /etc/redis/redis.conf.",
        "if sudo grep -qe '^[#[:space:]]*protected-mode ' /etc/redis/redis.conf; then",
        "if sudo grep -qE '^[#[:space:]]*protected-mode ' /etc/redis/redis.conf; then",
    ),
    _marker(
        "redis_supervised_sudo_grep",
        "Use sudo when checking the Redis supervised directive in /etc/redis/redis.conf.",
        "if sudo grep -qe '^[#[:space:]]*supervised ' /etc/redis/redis.conf; then",
        "if sudo grep -qE '^[#[:space:]]*supervised ' /etc/redis/redis.conf; then",
    ),
    _marker(
        "redis_final_sudo_grep",
        "Use sudo when printing final Redis config directives from /etc/redis/redis.conf.",
        "sudo grep -e '^(bind|protected-mode|supervised) ' /etc/redis/redis.conf",
        "sudo grep -E '^(bind|protected-mode|supervised) ' /etc/redis/redis.conf",
    ),
    _marker(
        "postgresql_cluster_online_guard",
        "Require an explicit PostgreSQL cluster online check after restart.",
        '$4 == "online"',
        "is not online",
    ),
    _marker(
        "postgresql_cluster_status_after_restart",
        "Check PostgreSQL cluster status after restarting services.",
        "sudo systemctl restart postgresql redis-server pg_cluster_count",
    ),
    _marker(
        "postgresql_restart_readiness_before_systemd_status",
        "Run local PostgreSQL readiness after restart before relying on umbrella systemd status.",
        "pg_isready -h 127.0.0.1 -p 5432 sudo systemctl is-active postgresql redis-server",
    ),
    _marker("postgresql_app_role", "Include the PostgreSQL app role name.", "github_ai_catchbot_app"),
    _marker("postgresql_app_database", "Include the PostgreSQL app database name.", "github_ai_catchbot"),
    _marker(
        "interactive_password_prompt",
        "Use an interactive password command that does not expose the password in command-line arguments.",
        "\\password github_ai_catchbot_app",
    ),
    _marker(
        "postgresql_local_health",
        "Include local PostgreSQL readiness check.",
        "pg_isready -h 127.0.0.1 -p 5432",
    ),
    _marker("redis_local_health", "Include local Redis PING check.", "redis-cli -h 127.0.0.1 -p 6379 ping"),
    _marker("ss_ltn_check", "Include local listen socket inspection.", "ss -ltn"),
    _marker(
        "public_bind_checks",
        "Check for public PostgreSQL binds on 0.0.0.0, [::], *, and :::.",
        "0.0.0.0:5432",
        "[::]:5432",
    ),
    _marker("postgresql_star_public_bind_check", "Check for wildcard public PostgreSQL bind on *.", "*:5432"),
    _marker(
        "postgresql_triple_colon_public_bind_check",
        "Check for wildcard public PostgreSQL bind on :::.",
        ":::5432",
    ),
    _marker(
        "public_redis_bind_checks",
        "Check for public Redis binds on 0.0.0.0, [::], *, and :::.",
        "0.0.0.0:6379",
        "[::]:6379",
    ),
    _marker("redis_star_public_bind_check", "Check for wildcard public Redis bind on *.", "*:6379"),
    _marker(
        "redis_triple_colon_public_bind_check",
        "Check for wildcard public Redis bind on :::.",
        ":::6379",
    ),
    _marker("ufw_status_verbose", "Include UFW verbose status inspection.", "sudo ufw status verbose"),
    _marker("ufw_status_active_check", "Require UFW Status: active check.", "status: active"),
    _marker("no_public_5432", "Require explicit no public PostgreSQL exposure wording.", "no public 5432"),
    _marker("no_public_6379", "Require explicit no public Redis exposure wording.", "no public 6379"),
    _marker("no_env_creation", "Require explicit no .env creation wording.", "no `.env`", "no .env"),
    _marker("no_alembic", "Require explicit Alembic prohibition.", "alembic is not run", "no alembic"),
    _marker("no_app_runtime", "Require explicit app runtime prohibition.", "no app runtime"),
    _marker("no_tdlib_telegram", "Require explicit TDLib/Telegram prohibition.", "tdlib/telegram", "tdlib auth"),
    _marker(
        "no_live_collector",
        "Require explicit live collector prohibition.",
        "no live collector",
        "live collector, and production rollout remain explicitly forbidden",
        "live collector remains unauthorized",
    ),
    _marker(
        "no_notifier",
        "Require explicit notifier transport prohibition.",
        "no notifier transport",
        "notifier, live collector",
        "notifier transport remains unauthorized",
    ),
    _marker(
        "no_production_rollout",
        "Require explicit production rollout prohibition.",
        "no production rollout",
        "production rollout remain explicitly forbidden",
        "production rollout remains unauthorized",
    ),
    _marker(
        "no_raw_ip_ssh_secret_repo_docs",
        "Forbid raw server IP/operator IP/SSH key path/secret values in repo docs.",
        "raw server ip/operator ip/ssh key path/secret values in repo docs",
    ),
    _marker(
        "password_outside_repo_not_pasted",
        "Require password-manager storage outside repo and no password pasteback to ChatGPT.",
        "password must be stored outside the repo",
        "do not paste the password back into chatgpt",
    ),
)


FORBIDDEN_PATTERNS: tuple[ForbiddenPattern, ...] = (
    ForbiddenPattern(
        "docker_install_command",
        r"\b(?:sudo\s+)?apt-get\s+install\b.*\bdocker(?:\.io|-ce|-compose)?\b|\b(?:sudo\s+)?snap\s+install\s+docker\b",
        "Do not include Docker installation commands.",
    ),
    ForbiddenPattern(
        "docker_compose_execution",
        r"^\s*(?:sudo\s+)?docker\s+compose\s+(?:up|run|start|restart|exec)\b|^\s*(?:sudo\s+)?docker-compose\s+(?:up|run|start|restart|exec)\b",
        "Do not include Docker Compose execution commands.",
    ),
    ForbiddenPattern(
        "env_creation",
        r"\b(?:touch|tee|cat\s+>|printf\b.*>|install\b.*|cp\b.*|create|write|generate)\s+`?\.env`?\b|`?\.env`?\s+creation\s+is\s+authorized\b",
        "Do not authorize or command .env creation.",
    ),
    ForbiddenPattern(
        "database_url_assignment",
        r"\b(?:export\s+)?database_url\s*=",
        "Do not export or write DATABASE_URL.",
    ),
    ForbiddenPattern(
        "redis_url_assignment",
        r"\b(?:export\s+)?redis_url\s*=",
        "Do not export or write REDIS_URL.",
    ),
    ForbiddenPattern(
        "literal_password_assignment",
        r"\b(?:password|pgpassword|catchbot_db_password)\s*=\s*[^`\s]+",
        "Do not include literal-looking password assignments.",
    ),
    ForbiddenPattern(
        "alembic_execution",
        r"^\s*(?:python\s+-m\s+)?alembic\b|\brun\s+alembic\b|\balembic\s+upgrade\b",
        "Do not authorize Alembic migration execution.",
    ),
    ForbiddenPattern(
        "app_runtime_start",
        r"\b(?:start|run|restart)\s+(?:the\s+)?app\s+runtime\b|^\s*(?:python\s+main\.py|python\s+-m\s+src\b)",
        "Do not authorize app runtime start.",
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
        "raw_ip_placeholder",
        r"<(?:server|public|operator)[_-]?ip>|(?:server|public|operator)[_-]?ip\s*=",
        "Do not include raw server/operator IP placeholders.",
    ),
    ForbiddenPattern(
        "ssh_private_key_path",
        r"\bssh\s+-i\b|(?:^|[`\s])(?:~|/[^`\s]*)/\.ssh/[^`\s]+",
        "Do not include SSH private key paths.",
    ),
)

NEGATION_HINTS = (
    "do not",
    "must not",
    "not authorized",
    "unauthorized",
    "forbidden",
    "forbid",
    "without",
    "no ",
    "remain explicitly forbidden",
    "remains explicitly forbidden",
    "remains unauthorized",
)

ALLOWED_IP_LITERALS = {"127.0.0.1", "0.0.0.0"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate the dedicated VPS DB/Redis operator command runbook from local text only.",
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


def _disallowed_ip_literals(text: str) -> list[str]:
    candidates = set(re.findall(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", text))
    disallowed = []
    for candidate in sorted(candidates):
        if candidate in ALLOWED_IP_LITERALS:
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
                "message": "Dedicated VPS DB/Redis operator provisioning runbook is missing.",
            }
        )
    else:
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

        disallowed_ips = _disallowed_ip_literals(runbook_text)
        if disallowed_ips:
            checks_failed.append("runbook.raw_ip_literals")
            failures.append(
                {
                    "check": "runbook.raw_ip_literals",
                    "path": CHECKED_FILE,
                    "message": "Runbook contains raw IP literals outside allowed loopback/public-bind sentinel values.",
                    "ip_literals": disallowed_ips,
                }
            )

    report = {
        "report_type": REPORT_TYPE,
        "contract_status": "failed" if checks_failed else "passed",
        "checked_file": CHECKED_FILE,
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
