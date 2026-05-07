from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


CHECK_NAME = "dedicated_vps_db_redis_package_vs_docker_decision_check"
CHECKED_FILE = "ops/pipeline/runbooks/dedicated_vps_db_redis_package_vs_docker_decision.md"
SELECTED_DECISION = "host_apt_systemd_postgresql_and_redis"
DOCKER_COMPOSE_STATUS = "future_full_app_stack_candidate_not_discarded"

AUTHORIZATION = {
    "installs_authorized": False,
    "systemd_authorized": False,
    "db_connections_authorized": False,
    "redis_connections_authorized": False,
    "secrets_authorized": False,
    "alembic_authorized": False,
    "app_runtime_authorized": False,
    "tdlib_telegram_authorized": False,
    "notifier_transport_authorized": False,
    "production_rollout_authorized": False,
}

SIDE_EFFECTS = {
    "host_inspection_performed": False,
    "network_inspection_performed": False,
    "service_mutation_performed": False,
    "file_secret_read_performed": False,
    "env_file_read_performed": False,
}


@dataclass(frozen=True, slots=True)
class MarkerRequirement:
    name: str
    any_of: tuple[str, ...]
    message: str


@dataclass(frozen=True, slots=True)
class ForbiddenAuthorization:
    name: str
    pattern: str
    message: str


@dataclass(frozen=True, slots=True)
class CheckResult:
    exit_code: int
    report: dict[str, Any]


def _normalize_text(text: str) -> str:
    return " ".join(text.lower().split())


def _marker(name: str, message: str, *any_of: str) -> MarkerRequirement:
    return MarkerRequirement(
        name=name,
        any_of=tuple(_normalize_text(item) for item in any_of),
        message=message,
    )


REQUIRED_MARKERS: tuple[MarkerRequirement, ...] = (
    _marker(
        "selected_host_apt_systemd_db_redis",
        "State host apt/systemd PostgreSQL plus host apt/systemd Redis as the selected immediate path.",
        "first db/redis provisioning should use host apt/systemd postgresql and host apt/systemd redis",
        "host apt/systemd postgresql + host apt/systemd redis",
    ),
    _marker(
        "docker_compose_future_candidate",
        "State that Docker Compose remains a future full app stack candidate.",
        "docker compose remains a future full app stack candidate",
        "docker compose remains available for later full app stack",
    ),
    _marker(
        "docker_compose_not_discarded",
        "State that Docker Compose is not discarded.",
        "docker compose remains a future full app stack candidate and is not discarded",
        "this does not discard docker compose",
    ),
    _marker(
        "postgresql_durable_system_of_record",
        "Preserve PostgreSQL durable system-of-record responsibility.",
        "postgresql durable system of record",
    ),
    _marker(
        "redis_queue_lock_short_lived_state",
        "Preserve Redis queue/lock/short-lived execution-state responsibility.",
        "redis queue/lock/short-lived execution state",
    ),
    _marker("no_public_5432", "Forbid public PostgreSQL exposure.", "no public 5432"),
    _marker("no_public_6379", "Forbid public Redis exposure.", "no public 6379"),
    _marker("no_secret_values", "Forbid secret values in repo docs.", "no secret values in repo docs"),
    _marker("no_env_file", "Forbid .env in this slice.", "no `.env`", "no .env"),
    _marker("no_alembic", "Forbid Alembic in this slice.", "no alembic in this slice"),
    _marker("no_app_runtime", "Forbid app runtime in this slice.", "no app runtime"),
    _marker("no_tdlib_telegram", "Forbid TDLib/Telegram in this slice.", "no tdlib/telegram"),
    _marker("no_live_collector", "Forbid live collector in this slice.", "no live collector"),
    _marker("no_notifier_transport", "Forbid notifier transport in this slice.", "no notifier transport"),
    _marker("no_production_rollout", "Forbid production rollout in this slice.", "no production rollout"),
    _marker(
        "no_install_start_connect_side_effects",
        "Forbid install/start/connect side effects in this slice.",
        "no install/start/connect side effects in this slice",
    ),
)

FORBIDDEN_AUTHORIZATIONS: tuple[ForbiddenAuthorization, ...] = (
    ForbiddenAuthorization(
        "apt_install_now",
        r"\b(?:run|perform|execute|do)\s+apt(?:-get)?\s+install\b|\bapt(?:-get)?\s+install\s+now\b",
        "Do not authorize package installation now.",
    ),
    ForbiddenAuthorization(
        "docker_install_now",
        r"\b(?:install|set up)\s+docker\s+now\b|\bdocker\s+install\s+now\b",
        "Do not authorize Docker installation now.",
    ),
    ForbiddenAuthorization(
        "postgresql_install_now",
        r"\b(?:install|provision)\s+postgresql\s+now\b|\bpostgresql\s+installation\s+is\s+authorized\b",
        "Do not authorize PostgreSQL installation now.",
    ),
    ForbiddenAuthorization(
        "redis_install_now",
        r"\b(?:install|provision)\s+redis\s+now\b|\bredis\s+installation\s+is\s+authorized\b",
        "Do not authorize Redis installation now.",
    ),
    ForbiddenAuthorization(
        "service_start_restart_now",
        r"\bsystem" r"ctl\s+(?:start|restart)\b|\bstart\s+systemd\s+service\s+now\b",
        "Do not authorize systemd service start/restart now.",
    ),
    ForbiddenAuthorization(
        "db_connection_now",
        r"\b(?:connect|connection)\s+to\s+(?:db|database|postgresql)\s+now\b|\bdb\s+connection\s+now\b",
        "Do not authorize DB connection now.",
    ),
    ForbiddenAuthorization(
        "redis_connection_now",
        r"\b(?:connect|connection)\s+to\s+redis\s+now\b|\bredis\s+connection\s+now\b",
        "Do not authorize Redis connection now.",
    ),
    ForbiddenAuthorization(
        "alembic_now",
        r"(?:^|[.;]\s+)run\s+alembic\b|\balembic\s+(?:now|upgrade|migration\s+is\s+authorized)\b",
        "Do not authorize Alembic now.",
    ),
    ForbiddenAuthorization(
        "env_creation",
        r"(?:^|[.;]\s+)(?:create|write|generate)\s+`?\.env`?\b|`?\.env`?\s+creation\s+is\s+authorized\b",
        "Do not authorize .env creation.",
    ),
    ForbiddenAuthorization(
        "secret_placement",
        r"(?:^|[.;]\s+)(?:place|put|write|commit|print)\s+secrets?\b|\bsecret\s+placement\s+is\s+authorized\b",
        "Do not authorize secret placement or disclosure.",
    ),
    ForbiddenAuthorization(
        "app_runtime_start",
        r"(?:^|[.;]\s+)start\s+(?:the\s+)?app\s+runtime\b|\bapp\s+runtime\s+start\s+now\b",
        "Do not authorize app runtime start.",
    ),
    ForbiddenAuthorization(
        "tdlib_auth",
        r"\btdlib\s+auth\s+now\b|(?:^|[.;]\s+)perform\s+tdlib\s+auth\b",
        "Do not authorize TDLib auth.",
    ),
    ForbiddenAuthorization(
        "telegram_connection",
        r"\bconnect\s+(?:to\s+)?telegram\s+now\b|\btelegram\s+connection\s+now\b",
        "Do not authorize Telegram connection.",
    ),
    ForbiddenAuthorization(
        "live_collector_start",
        r"(?:^|[.;]\s+)start\s+(?:the\s+)?live\s+collector\b|\blive\s+collector\s+start\s+now\b",
        "Do not authorize live collector start.",
    ),
    ForbiddenAuthorization(
        "notifier_transport",
        r"(?:^|[.;]\s+)(?:enable|start)\s+(?:the\s+)?notifier\s+transport\b|\bnotifier\s+transport\s+now\b",
        "Do not authorize notifier transport.",
    ),
    ForbiddenAuthorization(
        "production_rollout",
        r"(?:^|[.;]\s+)authorize\s+(?:the\s+)?production\s+rollout\b|\bproduction\s+rollout\s+is\s+authorized\b",
        "Do not authorize production rollout.",
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate the dedicated VPS DB/Redis package-vs-Docker decision runbook from local text only.",
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


def _missing_markers(text: str) -> list[MarkerRequirement]:
    normalized = _normalize_text(text)
    return [
        requirement
        for requirement in REQUIRED_MARKERS
        if not any(marker in normalized for marker in requirement.any_of)
    ]


def _matched_forbidden_authorizations(text: str) -> list[ForbiddenAuthorization]:
    normalized = _normalize_text(text)
    return [
        authorization
        for authorization in FORBIDDEN_AUTHORIZATIONS
        if re.search(authorization.pattern, normalized)
    ]


def generate_report(repo_root: str | Path | None = None) -> CheckResult:
    resolved_repo_root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    runbook_exists, runbook_text = _read_runbook(resolved_repo_root)

    checks_failed: list[str] = []
    failures: list[dict[str, str]] = []

    if not runbook_exists:
        checks_failed.append("runbook.exists")
        failures.append(
            {
                "check": "runbook.exists",
                "message": "Decision runbook is missing.",
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
                        "message": marker.message,
                    }
                )

        forbidden_matches = _matched_forbidden_authorizations(runbook_text)
        if forbidden_matches:
            checks_failed.append("runbook.forbidden_authorization")
            for authorization in forbidden_matches:
                failures.append(
                    {
                        "check": f"runbook.forbidden_authorization:{authorization.name}",
                        "message": authorization.message,
                    }
                )

    report = {
        "check_name": CHECK_NAME,
        "contract_status": "failed" if checks_failed else "passed",
        "checked_file": CHECKED_FILE,
        "selected_decision": SELECTED_DECISION,
        "docker_compose_status": DOCKER_COMPOSE_STATUS,
        "checks_failed": checks_failed,
        "failures": failures,
        "authorization": dict(AUTHORIZATION),
        "side_effects": dict(SIDE_EFFECTS),
    }
    return CheckResult(exit_code=1 if checks_failed else 0, report=report)


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def render_text(report: dict[str, Any]) -> str:
    lines = [
        f"check_name: {report['check_name']}",
        f"contract_status: {report['contract_status']}",
        f"checked_file: {report['checked_file']}",
        f"selected_decision: {report['selected_decision']}",
        f"docker_compose_status: {report['docker_compose_status']}",
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
