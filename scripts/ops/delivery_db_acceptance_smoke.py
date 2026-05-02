from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.maintenance.repositories import MaintenanceRepository


REPORT_TYPE = "delivery_db_acceptance_smoke_v1"
MUTATION_SAFETY = "select_only"

READ_ONLY_NOTICE = (
    "This smoke harness is opt-in and SELECT-only. Use it against migrated dev/test "
    "databases by default; do not run it casually against production even though it does "
    "not write application tables."
)

REQUIRED_TABLES = (
    "event_outbox",
    "notification_plans",
    "notification_renders",
    "notification_delivery_records",
    "state_transitions",
    "replay_requests",
    "dead_letter_entries",
    "job_attempts",
    "pipeline_runs",
    "analyses",
    "candidate_group_proposals",
    "source_messages",
)

REQUIRED_COLUMNS = {
    "event_outbox": ("event_id", "event_type", "payload_json", "status"),
    "notification_plans": (
        "notification_plan_id",
        "analysis_id",
        "candidate_group_id",
        "status",
        "delivery_decision",
        "urgency_profile",
        "target_chat_id",
        "target_thread_id",
        "render_profile",
        "dedupe_subject_key",
        "material_change_hash",
        "send_after",
        "created_at",
    ),
    "notification_renders": ("notification_render_id", "notification_plan_id", "render_hash", "created_at"),
    "notification_delivery_records": (
        "notification_delivery_record_id",
        "notification_plan_id",
        "delivery_status",
        "attempt_count",
        "transport_error_code",
        "transport_error_class",
        "telegram_chat_id",
        "telegram_response_json",
        "sent_at",
        "edited_at",
        "created_at",
    ),
    "state_transitions": ("state_transition_id", "object_type", "reason_code", "created_at"),
    "replay_requests": (
        "replay_request_id",
        "replay_type",
        "root_object_type",
        "root_object_id",
        "status",
        "requested_at",
    ),
    "dead_letter_entries": (
        "dead_letter_entry_id",
        "root_object_type",
        "root_object_id",
        "queue_name",
        "last_error_code",
        "last_failed_at",
    ),
    "job_attempts": ("job_attempt_id", "attempt_status"),
    "pipeline_runs": ("pipeline_run_id",),
    "analyses": ("analysis_id",),
    "candidate_group_proposals": ("candidate_group_id", "source_message_id"),
    "source_messages": ("source_message_id", "posted_at"),
}

ENUM_CAST_PROBES = {
    "notification_status_enum": "SELECT 'planned'::notification_status_enum AS value",
    "delivery_decision_enum": "SELECT 'send_now'::delivery_decision_enum AS value",
    "urgency_profile_enum": "SELECT 'high'::urgency_profile_enum AS value",
    "replay_type_enum": "SELECT 'delivery'::replay_type_enum AS value",
    "outbox_status_enum": "SELECT 'pending'::outbox_status_enum AS value",
    "job_attempt_status_enum": "SELECT 'pending'::job_attempt_status_enum AS value",
}

DELIVERY_GATE_METRICS = (
    "success_rate_1h",
    "success_rate_24h",
    "high_source_to_delivery_p95_sec",
    "plan_to_transport_p95_sec",
    "due_retry_oldest_lag_sec",
    "open_delivery_dlq_count",
    "unexpected_send_disabled_count",
    "replay_guard_reject_count_24h",
    "retry_ceiling_exceeded_count_24h",
    "duplicate_noop_ratio_1h",
)


@dataclass
class SmokeReport:
    report_type: str
    checks_run: list[str]
    checks_passed: list[str]
    checks_failed: list[str]
    failures: list[dict[str, str]]
    warnings: list[str]
    database_url_redacted: bool
    mutation_safety: str

    def failed(self) -> bool:
        return bool(self.checks_failed)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run an opt-in DB-backed delivery acceptance smoke. "
            "All database probes are SELECT-only."
        )
    )
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument(
        "--check",
        choices=["all", "schema", "delivery-gate", "maintenance-cli"],
        default="all",
        help="Run every check or one smoke slice.",
    )
    parser.add_argument("--format", choices=["json", "human"], default="json")
    return parser


def _new_report() -> SmokeReport:
    return SmokeReport(
        report_type=REPORT_TYPE,
        checks_run=[],
        checks_passed=[],
        checks_failed=[],
        failures=[],
        warnings=[READ_ONLY_NOTICE],
        database_url_redacted=True,
        mutation_safety=MUTATION_SAFETY,
    )


def _redact_database_url(text: str, database_url: str) -> str:
    redacted = text
    if database_url:
        redacted = redacted.replace(database_url, "<redacted-database-url>")
        try:
            parsed = urlsplit(database_url)
        except ValueError:
            parsed = None
        if parsed and parsed.password:
            password_variants = {parsed.password, unquote(parsed.password), quote(unquote(parsed.password))}
            for password in password_variants:
                if password:
                    redacted = redacted.replace(password, "<redacted-credential>")
        if parsed and parsed.username and parsed.password:
            credential_variants = {
                f"{parsed.username}:{parsed.password}@",
                f"{unquote(parsed.username)}:{unquote(parsed.password)}@",
                f"{quote(unquote(parsed.username))}:{quote(unquote(parsed.password))}@",
            }
            for credentials in credential_variants:
                redacted = redacted.replace(credentials, "<redacted-credential>@")

    redacted = re.sub(
        r"(?i)\b(password|passwd|pwd)\s*=\s*[^,\s;]+",
        r"\1=<redacted-credential>",
        redacted,
    )
    redacted = re.sub(
        r"([a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s:@]+:[^@\s/]+@",
        r"\1<redacted-credential>@",
        redacted,
    )
    return redacted


def _mark_pass(report: SmokeReport, check_name: str) -> None:
    report.checks_run.append(check_name)
    report.checks_passed.append(check_name)


def _mark_fail(report: SmokeReport, check_name: str, message: str, database_url: str | None = None) -> None:
    report.checks_run.append(check_name)
    report.checks_failed.append(check_name)
    if database_url:
        message = _redact_database_url(message, database_url)
    report.failures.append({"check": check_name, "message": message})


async def _select_scalar(connection: AsyncConnection, statement: str, params: dict[str, Any] | None = None) -> Any:
    result = await connection.execute(sa.text(statement), params or {})
    return result.scalar_one_or_none()


async def _run_schema_checks(connection: AsyncConnection, report: SmokeReport, database_url: str) -> None:
    check_name = "migration.alembic_version_table"
    try:
        exists = await _select_scalar(
            connection,
            "SELECT to_regclass('public.alembic_version') IS NOT NULL",
        )
        if exists:
            _mark_pass(report, check_name)
        else:
            _mark_fail(report, check_name, "alembic_version table is missing", database_url)
    except Exception as exc:
        _mark_fail(report, check_name, str(exc), database_url)

    check_name = "migration.alembic_version_rows"
    try:
        version_count = await _select_scalar(connection, "SELECT COUNT(*) FROM alembic_version")
        if int(version_count or 0) > 0:
            _mark_pass(report, check_name)
        else:
            _mark_fail(report, check_name, "alembic_version has no applied revision", database_url)
    except Exception as exc:
        _mark_fail(report, check_name, str(exc), database_url)

    for table_name in REQUIRED_TABLES:
        check_name = f"schema.table.{table_name}"
        try:
            exists = await _select_scalar(
                connection,
                "SELECT to_regclass(:qualified_table_name) IS NOT NULL",
                {"qualified_table_name": f"public.{table_name}"},
            )
            if exists:
                _mark_pass(report, check_name)
            else:
                _mark_fail(report, check_name, "required table is missing", database_url)
        except Exception as exc:
            _mark_fail(report, check_name, str(exc), database_url)

    for table_name, columns in REQUIRED_COLUMNS.items():
        check_name = f"schema.columns.{table_name}"
        try:
            result = await connection.execute(
                sa.text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = :table_name
                    """
                ),
                {"table_name": table_name},
            )
            present = {str(row[0]) for row in result.fetchall()}
            missing = [column for column in columns if column not in present]
            if missing:
                _mark_fail(report, check_name, f"missing required columns: {', '.join(missing)}", database_url)
            else:
                _mark_pass(report, check_name)
        except Exception as exc:
            _mark_fail(report, check_name, str(exc), database_url)

    for enum_name, statement in ENUM_CAST_PROBES.items():
        check_name = f"schema.enum.{enum_name}"
        try:
            await _select_scalar(connection, statement)
            _mark_pass(report, check_name)
        except Exception as exc:
            _mark_fail(report, check_name, str(exc), database_url)


async def _run_delivery_gate_checks(session: AsyncSession, report: SmokeReport, database_url: str) -> None:
    repository = MaintenanceRepository(session)

    check_name = "delivery_gate.snapshot_query"
    try:
        snapshot = await repository.load_delivery_gate_snapshot()
        missing_metrics = [metric for metric in DELIVERY_GATE_METRICS if not hasattr(snapshot, metric)]
        if missing_metrics:
            _mark_fail(report, check_name, f"snapshot missing metrics: {', '.join(missing_metrics)}", database_url)
        else:
            _mark_pass(report, check_name)
    except Exception as exc:
        _mark_fail(report, check_name, str(exc), database_url)

    check_name = "batch_recovery.selected_rows_query"
    try:
        await repository.load_selected_recovery_rows([UUID("00000000-0000-0000-0000-000000000001")])
        _mark_pass(report, check_name)
    except Exception as exc:
        _mark_fail(report, check_name, str(exc), database_url)

    check_name = "batch_recovery.due_retry_query"
    try:
        await repository.load_due_retry_candidates(limit=1, now=datetime.now(UTC))
        _mark_pass(report, check_name)
    except Exception as exc:
        _mark_fail(report, check_name, str(exc), database_url)


def _run_help_command(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "src.services.maintenance.main", *args],
        check=False,
        capture_output=True,
        env=os.environ.copy(),
        text=True,
        timeout=20,
    )


def _run_maintenance_cli_checks(report: SmokeReport, database_url: str) -> None:
    commands = {
        "maintenance_cli.root_help": [],
        "maintenance_cli.delivery_gate_help": ["delivery-gate"],
        "maintenance_cli.batch_recovery_help": ["batch-recovery"],
        "maintenance_cli.batch_recovery_replay_help": ["batch-recovery", "replay-selected"],
        "maintenance_cli.batch_recovery_retry_help": ["batch-recovery", "retry-selected-due"],
    }
    outputs: dict[str, str] = {}
    for check_name, command_args in commands.items():
        result = _run_help_command([*command_args, "--help"])
        outputs[check_name] = f"{result.stdout}\n{result.stderr}"
        if result.returncode == 0 and "usage:" in outputs[check_name]:
            _mark_pass(report, check_name)
        else:
            _mark_fail(report, check_name, outputs[check_name].strip() or f"exit code {result.returncode}", database_url)

    confirm_text = "\n".join(
        outputs[name]
        for name in (
            "maintenance_cli.batch_recovery_help",
            "maintenance_cli.batch_recovery_replay_help",
            "maintenance_cli.batch_recovery_retry_help",
        )
    )
    if "--confirm {write}" in confirm_text or "--confirm" in confirm_text and "{write}" in confirm_text:
        _mark_pass(report, "maintenance_cli.batch_recovery_confirm_help")
    else:
        _mark_fail(
            report,
            "maintenance_cli.batch_recovery_confirm_help",
            "batch-recovery help lacks --confirm {write}",
            database_url,
        )


async def run_smoke(database_url: str, selected_check: str) -> SmokeReport:
    report = _new_report()
    requested = {"schema", "delivery-gate", "maintenance-cli"} if selected_check == "all" else {selected_check}

    engine = create_async_engine(database_url, future=True)
    try:
        if "schema" in requested:
            async with engine.connect() as connection:
                await _run_schema_checks(connection, report, database_url)

        if "delivery-gate" in requested:
            async with AsyncSession(engine, expire_on_commit=False) as session:
                await _run_delivery_gate_checks(session, report, database_url)

        if "maintenance-cli" in requested:
            _run_maintenance_cli_checks(report, database_url)
    finally:
        await engine.dispose()
    return report


def _render_json(report: SmokeReport) -> str:
    return json.dumps(report.__dict__, ensure_ascii=False, indent=2, sort_keys=False)


def _render_human(report: SmokeReport) -> str:
    lines = [
        f"{report.report_type}",
        f"mutation_safety: {report.mutation_safety}",
        f"database_url_redacted: {report.database_url_redacted}",
        f"checks_passed: {len(report.checks_passed)}",
        f"checks_failed: {len(report.checks_failed)}",
    ]
    for failure in report.failures:
        lines.append(f"FAIL {failure['check']}: {failure['message']}")
    for warning in report.warnings:
        lines.append(f"WARN {warning}")
    return "\n".join(lines)


async def _amain(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.exit(2, "error: --database-url or DATABASE_URL is required for DB-backed acceptance smoke\n")

    try:
        report = await run_smoke(database_url=args.database_url, selected_check=args.check)
    except Exception as exc:
        report = _new_report()
        _mark_fail(report, "smoke.unexpected_failure", str(exc), args.database_url)
    output = _render_json(report) if args.format == "json" else _render_human(report)
    print(output)
    return 1 if report.failed() else 0


def main() -> None:
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()
