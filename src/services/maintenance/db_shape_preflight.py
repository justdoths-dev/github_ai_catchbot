from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import sqlalchemy as sa


SCHEMA_VERSION = "db_shape_preflight_v1"


REQUIRED_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "event_outbox": (
        "event_id",
        "event_type",
        "aggregate_type",
        "aggregate_id",
        "dedupe_key",
        "payload_json",
        "status",
        "created_at",
        "published_at",
        "fail_count",
        "last_error",
    ),
    "judge_runs": (
        "judge_run_id",
        "bundle_id",
        "judge_profile",
        "model",
        "reasoning_effort",
        "prompt_version",
        "schema_version",
        "policy_version",
        "status",
    ),
    "judge_outputs": (
        "judge_output_id",
        "judge_run_id",
        "candidate_group_id",
        "judge_schema_version",
        "payload_json",
        "model_proposed_verdict",
        "model_confidence_band",
    ),
    "analyses": (
        "analysis_id",
        "candidate_group_id",
        "judge_output_id",
        "schema_version",
        "policy_version",
        "prompt_version",
        "delivery_policy_version",
        "verdict",
        "delivery_decision",
        "scores_json",
        "reason_codes_json",
        "model_proposed_verdict",
        "policy_reconciled_flag",
    ),
    "notification_plans": (
        "notification_plan_id",
        "analysis_id",
        "candidate_group_id",
        "delivery_decision",
        "urgency_profile",
        "target_chat_id",
        "target_thread_id",
        "render_profile",
        "dedupe_subject_key",
        "material_change_hash",
        "send_after",
        "suppress_reason_code",
        "status",
        "created_at",
    ),
    "notification_renders": (
        "notification_render_id",
        "notification_plan_id",
        "message_text",
        "entities_json",
        "link_preview_options_json",
        "reply_markup_json",
        "disable_notification",
        "protect_content",
        "parse_strategy",
        "render_hash",
        "created_at",
    ),
    "notification_delivery_records": (
        "notification_delivery_record_id",
        "notification_plan_id",
        "telegram_chat_id",
        "telegram_message_id",
        "delivery_status",
        "sent_at",
        "edited_at",
        "attempt_count",
        "transport_error_code",
        "transport_error_class",
        "telegram_response_json",
    ),
    "pipeline_runs": (
        "pipeline_run_id",
        "trigger_source",
        "run_kind",
        "root_object_type",
        "root_object_id",
        "started_at",
        "finished_at",
        "terminal_status",
    ),
    "job_attempts": (
        "job_attempt_id",
        "stage_name",
        "queue_name",
        "root_object_type",
        "root_object_id",
        "attempt_no",
        "lease_owner",
        "started_at",
        "finished_at",
        "attempt_status",
        "error_code",
        "retry_after_at",
    ),
    "dead_letter_entries": (
        "dead_letter_entry_id",
        "stage_name",
        "queue_name",
        "root_object_type",
        "root_object_id",
        "last_error_code",
        "last_error_snippet",
        "retry_count",
        "first_failed_at",
        "last_failed_at",
        "next_manual_action",
        "replay_hint",
    ),
    "replay_requests": (
        "replay_request_id",
        "replay_type",
        "root_object_type",
        "root_object_id",
        "requested_by",
        "requested_at",
        "status",
    ),
}

REQUIRED_ENUM_LABELS: dict[str, tuple[str, ...]] = {
    "outbox_status_enum": ("pending", "published", "failed"),
    "verdict_enum": ("inspect_now", "later", "skip"),
    "delivery_decision_enum": ("send_now", "send_digest", "suppress"),
    "notification_status_enum": (
        "planned",
        "rendered",
        "queued",
        "sent",
        "edited",
        "suppressed",
        "failed_retryable",
        "failed_terminal",
    ),
    "urgency_profile_enum": ("high", "normal_silent", "digest", "suppressed"),
    "job_attempt_status_enum": (
        "pending",
        "running",
        "succeeded",
        "failed_retryable",
        "failed_terminal",
        "abandoned",
    ),
    "replay_type_enum": ("source", "enrich", "judge", "delivery", "full_pipeline"),
}

BEGIN_READ_ONLY_SQL = "BEGIN READ ONLY"
SET_LOCAL_STATEMENT_TIMEOUT_SQL = "SET LOCAL statement_timeout = '5s'"
ROLLBACK_SQL = "ROLLBACK"
REQUIRED_TABLES_SQL = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = current_schema()
  AND table_name IN :table_names
"""
REQUIRED_COLUMNS_SQL = """
SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema = current_schema()
  AND table_name IN :table_names
"""
REQUIRED_ENUM_LABELS_SQL = """
SELECT t.typname AS enum_name, e.enumlabel AS enum_label
FROM pg_catalog.pg_type t
JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = current_schema()
  AND t.typname IN :enum_names
ORDER BY t.typname, e.enumsortorder
"""
READ_ONLY_DB_SHAPE_SQL_STATEMENTS = (
    BEGIN_READ_ONLY_SQL,
    SET_LOCAL_STATEMENT_TIMEOUT_SQL,
    REQUIRED_TABLES_SQL,
    REQUIRED_COLUMNS_SQL,
    REQUIRED_ENUM_LABELS_SQL,
    ROLLBACK_SQL,
)


@dataclass(frozen=True)
class DbShapeSnapshot:
    table_columns: Mapping[str, Collection[str]]
    enum_labels: Mapping[str, Collection[str]]
    read_only_guard_available: bool
    read_only_guard_violated: bool = False
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class DbShapeCheckResult:
    check_name: str
    status: str
    check_type: str
    expected: object
    observed: object


@dataclass(frozen=True)
class DbShapePreflightReport:
    schema_version: str
    status: str
    checks: list[DbShapeCheckResult]
    missing_tables: list[str]
    missing_columns: list[str]
    missing_enum_labels: list[str]
    warnings: list[str]


class DbShapeIntrospectionRepository(Protocol):
    async def load_db_shape_snapshot(self) -> DbShapeSnapshot: ...


class AsyncSessionLike(Protocol):
    def in_transaction(self) -> bool: ...
    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any: ...


class SqlAlchemyDbShapeIntrospectionRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session

    async def load_db_shape_snapshot(self) -> DbShapeSnapshot:
        if self._session.in_transaction():
            return DbShapeSnapshot(
                table_columns={},
                enum_labels={},
                read_only_guard_available=False,
                read_only_guard_violated=True,
                warnings=("db_shape_preflight_active_transaction_refused",),
            )

        began = False
        warnings: list[str] = []
        table_columns: dict[str, set[str]] = {}
        enum_labels: dict[str, set[str]] = {}
        read_only_guard_violated = False
        try:
            await self._session.execute(sa.text(BEGIN_READ_ONLY_SQL))
            began = True
            await self._session.execute(sa.text(SET_LOCAL_STATEMENT_TIMEOUT_SQL))
            table_columns = await self._load_required_table_columns()
            enum_labels = await self._load_required_enum_labels()
        except Exception:
            warnings.append("db_shape_preflight_metadata_inspection_failed")
            read_only_guard_violated = not began
        if began:
            try:
                await self._session.execute(sa.text(ROLLBACK_SQL))
            except Exception:
                warnings.append("db_shape_preflight_rollback_failed")
                read_only_guard_violated = True
        return DbShapeSnapshot(
            table_columns=table_columns,
            enum_labels=enum_labels,
            read_only_guard_available=began,
            read_only_guard_violated=read_only_guard_violated,
            warnings=tuple(warnings),
        )

    async def _load_required_table_columns(self) -> dict[str, set[str]]:
        table_names = tuple(REQUIRED_TABLE_COLUMNS)
        existing_tables_result = await self._session.execute(
            sa.text(REQUIRED_TABLES_SQL).bindparams(sa.bindparam("table_names", expanding=True)),
            {"table_names": table_names},
        )
        table_columns: dict[str, set[str]] = {
            str(row["table_name"]): set() for row in existing_tables_result.mappings().all()
        }

        columns_result = await self._session.execute(
            sa.text(REQUIRED_COLUMNS_SQL).bindparams(sa.bindparam("table_names", expanding=True)),
            {"table_names": table_names},
        )
        for row in columns_result.mappings().all():
            table_name = str(row["table_name"])
            if table_name in table_columns:
                table_columns[table_name].add(str(row["column_name"]))
        return table_columns

    async def _load_required_enum_labels(self) -> dict[str, set[str]]:
        enum_names = tuple(REQUIRED_ENUM_LABELS)
        result = await self._session.execute(
            sa.text(REQUIRED_ENUM_LABELS_SQL).bindparams(sa.bindparam("enum_names", expanding=True)),
            {"enum_names": enum_names},
        )
        enum_labels: dict[str, set[str]] = {}
        for row in result.mappings().all():
            enum_name = str(row["enum_name"])
            if enum_name not in REQUIRED_ENUM_LABELS:
                continue
            enum_labels.setdefault(enum_name, set()).add(str(row["enum_label"]))
        return enum_labels


async def run_db_shape_preflight(repository: DbShapeIntrospectionRepository) -> DbShapePreflightReport:
    return build_db_shape_preflight_report(await repository.load_db_shape_snapshot())


def build_db_shape_preflight_report(snapshot: DbShapeSnapshot) -> DbShapePreflightReport:
    checks: list[DbShapeCheckResult] = []
    missing_tables: list[str] = []
    missing_columns: list[str] = []
    missing_enum_labels: list[str] = []

    guard_passed = snapshot.read_only_guard_available and not snapshot.read_only_guard_violated
    checks.append(
        DbShapeCheckResult(
            check_name="read_only_transaction_guard",
            status="pass" if guard_passed else "fail",
            check_type="read_only_guard",
            expected="available_and_not_violated",
            observed=_guard_observed(snapshot),
        )
    )

    table_columns = {table: set(columns) for table, columns in snapshot.table_columns.items()}
    enum_labels = {enum_name: set(labels) for enum_name, labels in snapshot.enum_labels.items()}

    for table_name, required_columns in REQUIRED_TABLE_COLUMNS.items():
        table_exists = table_name in table_columns
        if not table_exists:
            missing_tables.append(table_name)
        checks.append(
            DbShapeCheckResult(
                check_name=f"required_table:{table_name}",
                status="pass" if table_exists else "fail",
                check_type="table",
                expected=table_name,
                observed=table_exists,
            )
        )
        if not table_exists:
            continue
        observed_columns = table_columns[table_name]
        for column_name in required_columns:
            column_exists = column_name in observed_columns
            qualified_column = f"{table_name}.{column_name}"
            if not column_exists:
                missing_columns.append(qualified_column)
            checks.append(
                DbShapeCheckResult(
                    check_name=f"required_column:{qualified_column}",
                    status="pass" if column_exists else "fail",
                    check_type="column",
                    expected=qualified_column,
                    observed=column_exists,
                )
            )

    for enum_name, required_labels in REQUIRED_ENUM_LABELS.items():
        observed_labels = enum_labels.get(enum_name, set())
        for enum_label in required_labels:
            label_exists = enum_label in observed_labels
            qualified_label = f"{enum_name}.{enum_label}"
            if not label_exists:
                missing_enum_labels.append(qualified_label)
            checks.append(
                DbShapeCheckResult(
                    check_name=f"required_enum_label:{qualified_label}",
                    status="pass" if label_exists else "fail",
                    check_type="enum_label",
                    expected=qualified_label,
                    observed=label_exists,
                )
            )

    warnings = _dedupe_stable(snapshot.warnings)
    for warning in warnings:
        checks.append(
            DbShapeCheckResult(
                check_name=f"warning:{warning}",
                status="warn",
                check_type="warning",
                expected="non_blocking",
                observed=warning,
            )
        )

    status = "fail" if (not guard_passed or missing_tables or missing_columns or missing_enum_labels) else "pass"
    return DbShapePreflightReport(
        schema_version=SCHEMA_VERSION,
        status=status,
        checks=checks,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        missing_enum_labels=missing_enum_labels,
        warnings=warnings,
    )


def _guard_observed(snapshot: DbShapeSnapshot) -> str:
    if not snapshot.read_only_guard_available:
        return "unavailable"
    if snapshot.read_only_guard_violated:
        return "violated"
    return "available"


def _dedupe_stable(values: Collection[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
