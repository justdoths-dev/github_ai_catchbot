from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPORT_TYPE = "dedicated_vps_telegram_runtime_secret_placement_update_package_check_v1"
CHECKED_FILE = (
    "ops/pipeline/runbooks/"
    "dedicated_vps_telegram_runtime_secret_placement_update_package.md"
)

RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"

REQUIRED_SECTIONS = (
    "# Dedicated VPS Telegram runtime secret placement update package",
    "## Purpose",
    "## Source-of-truth / architecture boundary",
    "## Current closed prerequisites",
    "## Scope",
    "## Non-authorizations",
    "## Runtime secret file target",
    "## Collector reader account / TDLib / MTProto keys",
    "## Notifier bot / Bot API keys",
    "## Operator pre-checklist",
    "## Safe edit procedure",
    "## Redacted validation procedure",
    "## Expected redacted validation output shape",
    "## Rollback / recovery notes",
    "## Acceptance criteria",
    "## Next bounded action",
)

COLLECTOR_KEYS = (
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_PHONE_NUMBER",
    "TELEGRAM_2FA_PASSWORD",
    "TDLIB_DB_ENCRYPTION_KEY",
    "TDLIB_STATE_DIR",
    "TDLIB_FILES_DIR",
)

NOTIFIER_KEYS = ("TELEGRAM_BOT_TOKEN",)

SEPARATION_MARKERS = (
    "collector reader account and notifier bot are separate credentials",
    "collector uses TDLib/MTProto reader account credentials",
    "notifier uses Telegram Bot API bot token",
    "bot token does not authorize channel collection",
    "reader account credentials do not authorize notifier transport",
)

NON_AUTHORIZATION_MARKERS = (
    "This package does not execute the operator package.",
    "This package does not mutate `/etc/github-ai-catchbot/runtime.env`.",
    "This package does not read or print runtime env values.",
    "This package does not run TDLib auth.",
    "This package does not connect Telegram.",
    "This package does not start live collector.",
    "This package does not enable notifier transport.",
    "This package does not start app runtime.",
    "This package does not connect to DB or Redis.",
    "This package does not run Alembic.",
    "This package does not modify Docker or systemd.",
    "This package does not perform production rollout.",
)

NEXT_ACTION_MARKERS = (
    "ChatGPT review.",
    "Commit/push if approved.",
    "VPS pull and repo-local validation.",
    "Separate explicit approval to execute the operator update.",
    "Create `dedicated_vps_telegram_runtime_secret_placement_result_record`.",
    "Only after that consider a TDLib auth package.",
)

SIDE_EFFECTS = {
    "runtime_env_modified": False,
    "runtime_env_values_printed": False,
    "database_connected": False,
    "redis_connected": False,
    "alembic_run": False,
    "app_runtime_started": False,
    "tdlib_auth_performed": False,
    "telegram_connected": False,
    "live_collector_started": False,
    "notifier_transport_enabled": False,
    "production_rollout_performed": False,
    "runtime_env_read": False,
    "files_mutated": False,
    "network_called": False,
}


@dataclass(frozen=True, slots=True)
class ForbiddenPattern:
    name: str
    pattern: str
    message: str


@dataclass(frozen=True, slots=True)
class CheckResult:
    exit_code: int
    report: dict[str, Any]


FORBIDDEN_PATTERNS = (
    ForbiddenPattern(
        "telegram_bot_token",
        r"\b\d{7,12}:[A-Za-z0-9_-]{30,}\b",
        "Telegram bot token-shaped value must not appear.",
    ),
    ForbiddenPattern(
        "telegram_api_hash_assignment",
        r"\bTELEGRAM_API_HASH\s*[:=]\s*[0-9a-fA-F]{32}\b",
        "Telegram API hash assignment must not appear.",
    ),
    ForbiddenPattern(
        "telegram_phone_assignment",
        r"\bTELEGRAM_PHONE_NUMBER\s*[:=]\s*\+?\d[\d\s().-]{6,}",
        "Telegram phone number assignment must not appear.",
    ),
    ForbiddenPattern(
        "private_invite_plus",
        r"https?://t\.me/\+",
        "Private Telegram invite links must not appear.",
    ),
    ForbiddenPattern(
        "private_invite_joinchat",
        r"https?://t\.me/joinchat/",
        "Private Telegram invite links must not appear.",
    ),
    ForbiddenPattern(
        "legacy_private_invite_joinchat",
        r"telegram\.me/joinchat/",
        "Private Telegram invite links must not appear.",
    ),
    ForbiddenPattern(
        "cat_runtime_env",
        r"\bcat /etc/github-ai-catchbot/runtime\.env\b",
        "Raw runtime env display command must not appear.",
    ),
    ForbiddenPattern(
        "source_runtime_env",
        r"\bsource /etc/github-ai-catchbot/runtime\.env\b",
        "Runtime env source command must not appear.",
    ),
    ForbiddenPattern(
        "dot_source_runtime_env",
        r"(?m)^\s*\.\s+/etc/github-ai-catchbot/runtime\.env\b",
        "Runtime env dot-source command must not appear.",
    ),
    ForbiddenPattern(
        "export_cat",
        r"\bexport \$\(cat\b",
        "Runtime env export-from-file command must not appear.",
    ),
    ForbiddenPattern(
        "postgresql_url",
        r"postgresql://",
        "Raw PostgreSQL URL must not appear.",
    ),
    ForbiddenPattern(
        "postgresql_psycopg_url",
        r"postgresql\+psycopg://",
        "Raw PostgreSQL psycopg URL must not appear.",
    ),
    ForbiddenPattern("redis_url", r"redis://", "Raw Redis URL must not appear."),
    ForbiddenPattern(
        "echo_secret_append",
        r"(?is)\becho\b[^\n]*TELEGRAM_[^\n]*>>",
        "Shell-history secret append command must not appear.",
    ),
    ForbiddenPattern(
        "tee_append_runtime_env",
        r"\btee\s+-a\s+/etc/github-ai-catchbot/runtime\.env\b",
        "Runtime env append through tee must not appear.",
    ),
    ForbiddenPattern(
        "ipv4_literal",
        r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
        "IP literals must not appear.",
    ),
)


def _normalize(text: str) -> str:
    return " ".join(text.split())


def _line_number(text: str, index: int) -> int:
    return text.count("\n", 0, index) + 1


def _section_body(text: str, heading: str) -> str:
    start = text.find(heading)
    if start == -1:
        return ""
    next_heading = text.find("\n## ", start + len(heading))
    if next_heading == -1:
        return text[start:]
    return text[start:next_heading]


def _failure(check: str, message: str, **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {"check": check, "message": message}
    payload.update(extra)
    return payload


def _check_required_sections(text: str) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    for section in REQUIRED_SECTIONS:
        count = text.count(section)
        if count != 1:
            failures.append(
                _failure(
                    "runbook.required_sections",
                    "Required section must appear exactly once.",
                    section=section,
                    count=count,
                )
            )
    return failures


def _check_required_keys(text: str) -> list[dict[str, Any]]:
    missing = [key for key in (*COLLECTOR_KEYS, *NOTIFIER_KEYS) if key not in text]
    if not missing:
        return []
    return [
        _failure(
            "runbook.required_key_names",
            "Required Telegram runtime key names are missing.",
            missing_keys=missing,
        )
    ]


def _check_markers(
    text: str,
    *,
    markers: Sequence[str],
    check_name: str,
    message: str,
) -> list[dict[str, Any]]:
    normalized = _normalize(text)
    missing = [marker for marker in markers if _normalize(marker) not in normalized]
    if not missing:
        return []
    return [_failure(check_name, message, missing_markers=missing)]


def _check_next_action(text: str) -> list[dict[str, Any]]:
    failures = _check_markers(
        text,
        markers=NEXT_ACTION_MARKERS,
        check_name="runbook.next_bounded_action",
        message="Expected next bounded action chain is incomplete.",
    )
    next_section = _section_body(text, "## Next bounded action")
    forbidden_immediate = (
        r"(?i)proceed only.*tdlib auth",
        r"(?i)proceed only.*telegram connection",
        r"(?i)proceed only.*live collector",
        r"(?i)proceed only.*notifier transport",
        r"(?i)proceed only.*production rollout",
    )
    for pattern in forbidden_immediate:
        if re.search(pattern, next_section):
            failures.append(
                _failure(
                    "runbook.next_bounded_action.forbidden_immediate_runtime_action",
                    "Next bounded action must not be runtime activation.",
                    pattern=pattern,
                )
            )
    return failures


def _check_forbidden_patterns(text: str) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    for forbidden in FORBIDDEN_PATTERNS:
        match = re.search(forbidden.pattern, text)
        if match:
            failures.append(
                _failure(
                    f"runbook.forbidden_pattern:{forbidden.name}",
                    forbidden.message,
                    line=_line_number(text, match.start()),
                )
            )
    return failures


def generate_report(root: Path | None = None) -> CheckResult:
    root = root or Path(__file__).resolve().parents[2]
    runbook = root / CHECKED_FILE
    failures: list[dict[str, Any]] = []

    if not runbook.exists():
        failures.append(
            _failure(
                "runbook.exists",
                "Runbook does not exist.",
                path=CHECKED_FILE,
            )
        )
        text = ""
    else:
        text = runbook.read_text(encoding="utf-8")

    if text:
        failures.extend(_check_required_sections(text))
        failures.extend(_check_required_keys(text))
        failures.extend(
            _check_markers(
                text,
                markers=SEPARATION_MARKERS,
                check_name="runbook.credential_surface_separation",
                message="Collector and notifier credential separation is incomplete.",
            )
        )
        failures.extend(
            _check_markers(
                text,
                markers=NON_AUTHORIZATION_MARKERS,
                check_name="runbook.non_authorizations",
                message="Required non-authorization language is incomplete.",
            )
        )
        failures.extend(_check_next_action(text))
        failures.extend(_check_forbidden_patterns(text))

    checks_failed = sorted({failure["check"].split(":")[0] for failure in failures})
    report = {
        "report_type": REPORT_TYPE,
        "contract_status": "failed" if failures else "passed",
        "checked_file": CHECKED_FILE,
        "runtime_env_path": RUNTIME_ENV_PATH,
        "collector_keys": list(COLLECTOR_KEYS),
        "notifier_keys": list(NOTIFIER_KEYS),
        "checks_failed": checks_failed,
        "failures": failures,
        "checker_side_effects": SIDE_EFFECTS,
    }
    return CheckResult(exit_code=1 if failures else 0, report=report)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Check the Telegram runtime secret placement update package using "
            "local repository text only."
        )
    )
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    result = generate_report(Path(__file__).resolve().parents[2])

    if args.format == "json":
        print(json.dumps(result.report, indent=2, sort_keys=True))
    else:
        print(f"contract_status={result.report['contract_status']}")
        for failure in result.report["failures"]:
            print(f"- {failure['check']}: {failure['message']}")
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
