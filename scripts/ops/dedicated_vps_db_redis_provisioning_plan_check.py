from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence


REPORT_TYPE = "dedicated_vps_db_redis_provisioning_plan_check_v1"
PLAN_PATH = "ops/pipeline/runbooks/dedicated_vps_db_redis_provisioning_plan.md"

SIDE_EFFECTS = {
    "postgresql_connected": False,
    "redis_connected": False,
    "docker_invoked": False,
    "systemd_invoked": False,
    "secrets_read": False,
    "env_file_inspected": False,
    "host_network_sockets_inspected": False,
    "files_mutated": False,
    "external_network_called": False,
    "tdlib_started": False,
    "telegram_called": False,
    "app_runtime_started": False,
    "live_collector_started": False,
    "notifier_transport_started": False,
}

AUTHORIZATION = {
    "postgresql_installation_authorized": False,
    "redis_installation_authorized": False,
    "docker_installation_authorized": False,
    "db_redis_connectivity_authorized": False,
    "secret_placement_authorized": False,
    "env_creation_authorized": False,
    "alembic_migration_authorized": False,
    "app_runtime_authorized": False,
    "live_collector_authorized": False,
    "notifier_transport_authorized": False,
    "production_rollout_authorized": False,
}


@dataclass(frozen=True, slots=True)
class PhraseRequirement:
    name: str
    phrase: str


@dataclass(frozen=True, slots=True)
class PlanCheckResult:
    exit_code: int
    report: dict[str, Any]


REQUIRED_PHRASES: tuple[PhraseRequirement, ...] = (
    PhraseRequirement("no_public_5432", "no public 5432"),
    PhraseRequirement("no_public_6379", "no public 6379"),
    PhraseRequirement("localhost_internal_network_only", "localhost/internal network only"),
    PhraseRequirement("secret_placement_unauthorized", "secret placement remains unauthorized"),
    PhraseRequirement("env_creation_unauthorized", ".env creation remains unauthorized"),
    PhraseRequirement("alembic_migration_unauthorized", "Alembic migration remains unauthorized"),
    PhraseRequirement("live_collector_unauthorized", "live collector remains unauthorized"),
    PhraseRequirement("notifier_transport_unauthorized", "notifier transport remains unauthorized"),
    PhraseRequirement("postgresql_durable_system_of_record", "PostgreSQL durable system of record"),
    PhraseRequirement(
        "redis_queue_lock_short_lived_state",
        "Redis queue/lock/short-lived execution state",
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the dedicated VPS DB/Redis provisioning plan contract from "
            "local repository text only. This checker does not connect to DB/Redis, "
            "inspect secrets, inspect .env files, call Docker, call systemd, inspect "
            "host sockets, or mutate files."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--format",
        choices=("json",),
        default="json",
        help="Output format. Only json is supported.",
    )
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root to inspect. Defaults to this script's repository root.",
    )
    return parser


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_plan_text(repo_root: Path) -> tuple[bool, str]:
    plan = repo_root / PLAN_PATH
    if not plan.is_file():
        return False, ""
    return True, plan.read_text(encoding="utf-8", errors="replace")


def _missing_phrase_names(text: str) -> list[str]:
    lower_text = text.lower()
    return [
        requirement.name
        for requirement in REQUIRED_PHRASES
        if requirement.phrase.lower() not in lower_text
    ]


def generate_report(repo_root: str | Path | None = None) -> PlanCheckResult:
    resolved_repo_root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    plan_exists, plan_text = _read_plan_text(resolved_repo_root)

    checks_failed: list[str] = []
    failures: list[dict[str, Any]] = []
    missing_phrases = _missing_phrase_names(plan_text) if plan_exists else []

    if not plan_exists:
        checks_failed.append("plan.exists")
        failures.append(
            {
                "check": "plan.exists",
                "path": PLAN_PATH,
                "message": "Dedicated VPS DB/Redis provisioning plan is missing.",
            }
        )

    if plan_exists and missing_phrases:
        checks_failed.append("plan.required_phrases")
        failures.append(
            {
                "check": "plan.required_phrases",
                "path": PLAN_PATH,
                "message": "Dedicated VPS DB/Redis provisioning plan is missing required contract phrases.",
                "missing_phrases": missing_phrases,
            }
        )

    report = {
        "report_type": REPORT_TYPE,
        "contract_status": "failed" if checks_failed else "passed",
        "checks_failed": checks_failed,
        "failures": failures,
        "plan_path": PLAN_PATH,
        "required_phrases": [asdict(requirement) for requirement in REQUIRED_PHRASES],
        "side_effects": dict(SIDE_EFFECTS),
        "authorization": dict(AUTHORIZATION),
    }
    return PlanCheckResult(exit_code=1 if checks_failed else 0, report=report)


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(args.repo_root)
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
