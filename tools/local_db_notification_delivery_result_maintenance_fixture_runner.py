from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from uuid import UUID

from tools import local_db_notification_plan_created_render_dry_run_fixture_runner as render_runner


SCHEMA_VERSION = "local_db_notification_delivery_result_maintenance_fixture_runner_v1"
NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE = "notification.delivery.result.v1"
DRY_RUN_REASON_CODE = "dry_run_skip_transport"
MAINTENANCE_TRIGGER_SOURCE = "local_db_notification_delivery_result_maintenance_fixture_runner"
MAINTENANCE_RUN_KIND = "local_test_fixture"
MAINTENANCE_STAGE_NAME = "maintenance_delivery_result"
MAINTENANCE_QUEUE_NAME = "q.maintenance"
MAINTENANCE_NOOP_ERROR_CODE = "delivery_result_suppressed_dry_run_noop"
MAINTENANCE_CLASSIFICATION = "logical_noop_success"
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "notification_delivery_result_event_found",
    "notification_plan_loaded",
    "latest_delivery_record_loaded",
    "delivery_result_matches_latest_record",
    "dry_run_suppressed_classified_logical_noop",
    "maintenance_pipeline_run_recorded",
    "maintenance_job_attempt_recorded",
)
FALSE_RESULT_KEYS = (
    "retry_intent_event_created",
    "replay_request_created",
    "dead_letter_created",
    "notification_plan_mutated",
    "notification_delivery_record_mutated",
    "analysis_mutated",
    "judge_output_mutated",
    "candidate_group_mutated",
    "openai_called",
    "telegram_called",
    "live_github_called",
    "workers_started",
    "redis_mutation",
    "production_db_write",
    "alembic_or_ddl_ran",
)
SAFE_EXCEPTION_MESSAGES = {
    "notification_delivery_result_event_missing_or_invalid",
    "notification_delivery_result_event_ambiguous",
    "notification_delivery_result_event_aggregate_mismatch",
    "notification_delivery_result_payload_invalid",
    "notification_delivery_result_not_dry_run_suppressed",
    "notification_plan_missing",
    "notification_delivery_record_missing",
    "notification_delivery_record_mismatch",
    "maintenance_pipeline_run_ambiguous",
    "maintenance_job_attempt_ambiguous",
}

source_candidate_runner = render_runner.source_candidate_runner
github_snapshot_runner = render_runner.github_snapshot_runner


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class DeliveryResultResolutionResult:
    notification_delivery_result_event_id: UUID | None
    notification_delivery_result_event_found: bool
    upstream_fixture_replayed: bool = False
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DeliveryResultEvent:
    event_id: UUID
    aggregate_type: str | None
    aggregate_id: UUID | None
    notification_plan_id: UUID
    notification_delivery_record_id: UUID | None
    delivery_status: str
    attempt_count: int | None
    reason_code: str | None
    dry_run: bool
    payload_json: dict[str, Any]


@dataclass(frozen=True, slots=True)
class NotificationPlanRecord:
    notification_plan_id: UUID
    analysis_id: UUID
    candidate_group_id: UUID
    status: str


@dataclass(frozen=True, slots=True)
class DeliveryRecord:
    notification_delivery_record_id: UUID
    notification_plan_id: UUID
    delivery_status: str
    attempt_count: int
    telegram_message_id: int | None
    transport_error_code: str | None
    transport_error_class: str | None
    telegram_response_json: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class MaintenanceExecutionResult:
    notification_delivery_result_event_found: bool
    notification_plan_loaded: bool
    latest_delivery_record_loaded: bool
    delivery_result_matches_latest_record: bool
    dry_run_suppressed_classified_logical_noop: bool
    maintenance_pipeline_run_recorded: bool
    maintenance_job_attempt_recorded: bool
    retry_intent_event_created: bool = False
    replay_request_created: bool = False
    dead_letter_created: bool = False
    notification_plan_mutated: bool = False
    notification_delivery_record_mutated: bool = False
    analysis_mutated: bool = False
    judge_output_mutated: bool = False
    candidate_group_mutated: bool = False
    checks_failed: tuple[str, ...] = ()


class DeliveryResultEventResolver(Protocol):
    def resolve(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> DeliveryResultResolutionResult: ...


class RenderDryRunRunner(Protocol):
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> render_runner.RunnerResult: ...


class MaintenanceExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        notification_delivery_result_event_id: UUID,
    ) -> MaintenanceExecutionResult: ...


class DefaultRenderDryRunRunner:
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> render_runner.RunnerResult:
        args = argparse.Namespace(
            database_url=database_url,
            notification_plan_created_event_id=None,
            source_fixture=str(source_fixture_path),
            github_snapshot_fixture=str(github_snapshot_fixture_path),
            replay_namespace=replay_namespace,
            confirm_local_test_db=True,
        )
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        return render_runner.run(args, env=predecessor_env, repo_root=repo_root)


class DefaultDeliveryResultEventResolver:
    def __init__(self, *, upstream: RenderDryRunRunner | None = None) -> None:
        self._upstream = upstream or DefaultRenderDryRunRunner()

    def resolve(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> DeliveryResultResolutionResult:
        before = _find_delivery_result_event_ids_by_namespace(
            database_url=database_url,
            replay_namespace=replay_namespace,
        )
        if len(before) > 1:
            return DeliveryResultResolutionResult(
                notification_delivery_result_event_id=None,
                notification_delivery_result_event_found=False,
                checks_failed=("notification_delivery_result_event_ambiguous",),
            )
        if len(before) == 1:
            return DeliveryResultResolutionResult(
                notification_delivery_result_event_id=before[0],
                notification_delivery_result_event_found=True,
            )

        predecessor = self._upstream.run(
            database_url=database_url,
            source_fixture_path=source_fixture_path,
            github_snapshot_fixture_path=github_snapshot_fixture_path,
            replay_namespace=replay_namespace,
            env=env,
            repo_root=repo_root,
        )
        if not _render_fixture_result_acceptable(predecessor):
            return DeliveryResultResolutionResult(
                notification_delivery_result_event_id=None,
                notification_delivery_result_event_found=False,
                upstream_fixture_replayed=True,
                checks_failed=("notification_render_dry_run_fixture_failed",),
            )

        after = _find_delivery_result_event_ids_by_namespace(
            database_url=database_url,
            replay_namespace=replay_namespace,
        )
        if len(after) > 1:
            return DeliveryResultResolutionResult(
                notification_delivery_result_event_id=None,
                notification_delivery_result_event_found=False,
                upstream_fixture_replayed=True,
                checks_failed=("notification_delivery_result_event_ambiguous",),
            )
        if len(after) != 1:
            return DeliveryResultResolutionResult(
                notification_delivery_result_event_id=None,
                notification_delivery_result_event_found=False,
                upstream_fixture_replayed=True,
                checks_failed=("notification_delivery_result_event_missing_or_invalid",),
            )
        return DeliveryResultResolutionResult(
            notification_delivery_result_event_id=after[0],
            notification_delivery_result_event_found=True,
            upstream_fixture_replayed=True,
        )


class SqlAlchemyMaintenanceExecutor:
    def execute(
        self,
        *,
        database_url: str,
        notification_delivery_result_event_id: UUID,
    ) -> MaintenanceExecutionResult:
        render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001 - local tool reuse.
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                return _execute_maintenance(
                    connection,
                    notification_delivery_result_event_id=notification_delivery_result_event_id,
                )
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Consume one local/test DB notification.delivery.result.v1 event, "
            "rehydrate notifier-owned rows read-only, classify suppressed dry-run "
            "delivery as a maintenance logical no-op success, and record only "
            "maintenance audit rows."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--notification-delivery-result-event-id")
    parser.add_argument("--source-fixture")
    parser.add_argument("--github-snapshot-fixture")
    parser.add_argument("--replay-namespace")
    parser.add_argument("--confirm-local-test-db", action="store_true")
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    resolver: DeliveryResultEventResolver | None = None,
    executor: MaintenanceExecutor | None = None,
    repo_root: Path | None = None,
) -> RunnerResult:
    effective_env = env if env is not None else os.environ
    root = repo_root or _repo_root()
    report = _base_report()
    checks_failed: list[str] = []

    if not args.confirm_local_test_db:
        checks_failed.append("confirm_local_test_db_required")

    if effective_env.get("APP_ENV", "").strip().lower() != "test":
        checks_failed.append("app_env_test_required")

    database_ok, database_failures, _ = validate_database_url(args.database_url)
    report["database_url_guard_passed"] = database_ok
    checks_failed.extend(database_failures)

    explicit_event_id = _uuid_or_none(getattr(args, "notification_delivery_result_event_id", None))
    if getattr(args, "notification_delivery_result_event_id", None) and explicit_event_id is None:
        checks_failed.append("notification_delivery_result_event_id_invalid")

    source_fixture = _path_or_none(getattr(args, "source_fixture", None))
    github_fixture = _path_or_none(getattr(args, "github_snapshot_fixture", None))
    replay_namespace = _string_or_none(getattr(args, "replay_namespace", None))
    fixture_selector_supplied = source_fixture is not None or github_fixture is not None or replay_namespace is not None

    if explicit_event_id is not None and fixture_selector_supplied:
        checks_failed.append("selector_mode_ambiguous")

    if explicit_event_id is None:
        if source_fixture is None or github_fixture is None or replay_namespace is None:
            checks_failed.append("fixture_selector_required")
        if fixture_selector_supplied and (source_fixture is None or github_fixture is None or replay_namespace is None):
            checks_failed.append("fixture_selector_incomplete")

    if replay_namespace is not None:
        namespace_ok, namespace_failures = source_candidate_runner.validate_replay_namespace(replay_namespace)
        checks_failed.extend(namespace_failures)
        if not namespace_ok:
            replay_namespace = None

    if explicit_event_id is None and source_fixture is not None and github_fixture is not None:
        try:
            source_candidate_runner.load_source_fixture(source_fixture, repo_root=root)
        except Exception:  # noqa: BLE001 - sanitized operator result only.
            checks_failed.append("source_fixture_load_failed")
        try:
            github_snapshot_runner.load_github_snapshot_fixture(github_fixture, repo_root=root)
        except Exception:  # noqa: BLE001 - sanitized operator result only.
            checks_failed.append("github_snapshot_fixture_load_failed")

    if checks_failed:
        return _finish(report, checks_failed)

    if explicit_event_id is not None:
        resolved_event_id = explicit_event_id
    else:
        if source_fixture is None or github_fixture is None or replay_namespace is None:
            return _finish(report, ["fixture_selector_required"])
        active_resolver = resolver or DefaultDeliveryResultEventResolver()
        try:
            resolution = active_resolver.resolve(
                database_url=args.database_url,
                source_fixture_path=source_fixture,
                github_snapshot_fixture_path=github_fixture,
                replay_namespace=replay_namespace,
                env=effective_env,
                repo_root=root,
            )
        except Exception:  # noqa: BLE001 - never echo DB errors or URLs.
            return _finish(report, ["notification_delivery_result_event_resolution_failed"])
        report[
            "notification_delivery_result_event_found"
        ] = resolution.notification_delivery_result_event_found
        checks_failed.extend(resolution.checks_failed)
        resolved_event_id = resolution.notification_delivery_result_event_id
        if resolved_event_id is None:
            checks_failed.append("notification_delivery_result_event_missing_or_invalid")
        if checks_failed:
            return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyMaintenanceExecutor()
    try:
        execution = active_executor.execute(
            database_url=args.database_url,
            notification_delivery_result_event_id=resolved_event_id,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
        return _finish(report, [_safe_failure_code(exc)])

    report.update(
        {
            "notification_delivery_result_event_found": execution.notification_delivery_result_event_found,
            "notification_plan_loaded": execution.notification_plan_loaded,
            "latest_delivery_record_loaded": execution.latest_delivery_record_loaded,
            "delivery_result_matches_latest_record": execution.delivery_result_matches_latest_record,
            "dry_run_suppressed_classified_logical_noop": execution.dry_run_suppressed_classified_logical_noop,
            "maintenance_pipeline_run_recorded": execution.maintenance_pipeline_run_recorded,
            "maintenance_job_attempt_recorded": execution.maintenance_job_attempt_recorded,
            "retry_intent_event_created": execution.retry_intent_event_created,
            "replay_request_created": execution.replay_request_created,
            "dead_letter_created": execution.dead_letter_created,
            "notification_plan_mutated": execution.notification_plan_mutated,
            "notification_delivery_record_mutated": execution.notification_delivery_record_mutated,
            "analysis_mutated": execution.analysis_mutated,
            "judge_output_mutated": execution.judge_output_mutated,
            "candidate_group_mutated": execution.candidate_group_mutated,
        }
    )
    checks_failed.extend(execution.checks_failed)

    for key in TRUE_RESULT_KEYS:
        if report.get(key) is not True:
            checks_failed.append(f"{key}:missing")
    for key in FALSE_RESULT_KEYS:
        if report.get(key) is not False:
            checks_failed.append(f"{key}:unexpected")

    return _finish(report, checks_failed)


def validate_database_url(database_url: str | None):
    return render_runner.validate_database_url(database_url)


def classify_dry_run_suppressed_logical_noop(*, delivery_status: str, delivery_reason: str | None) -> bool:
    render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001 - local tool reuse.
    from services.maintenance.retry_policy import classify_delivery_result_dry_run_noop

    decision = classify_delivery_result_dry_run_noop(
        delivery_status=delivery_status,
        delivery_reason=delivery_reason,
    )
    return (
        decision.action == "mark_logical_noop_success"
        and decision.maintenance_classification == MAINTENANCE_CLASSIFICATION
        and decision.reason_code == MAINTENANCE_NOOP_ERROR_CODE
        and decision.retry_intent_allowed is False
        and decision.replay_dispatch_allowed is False
        and decision.dead_letter_allowed is False
    )


def _execute_maintenance(
    connection: Any,
    *,
    notification_delivery_result_event_id: UUID,
) -> MaintenanceExecutionResult:
    event = _load_delivery_result_event_by_id(connection, notification_delivery_result_event_id)
    if event is None:
        return _execution_result(
            notification_delivery_result_event_found=False,
            checks_failed=("notification_delivery_result_event_missing_or_invalid",),
        )

    if event.aggregate_type != "notification_plan" or event.aggregate_id != event.notification_plan_id:
        return _execution_result(
            notification_delivery_result_event_found=True,
            checks_failed=("notification_delivery_result_event_aggregate_mismatch",),
        )

    plan = _load_notification_plan(connection, event.notification_plan_id)
    if plan is None:
        return _execution_result(
            notification_delivery_result_event_found=True,
            notification_plan_loaded=False,
            checks_failed=("notification_plan_missing",),
        )

    latest = _load_latest_delivery_record(connection, event.notification_plan_id)
    latest_loaded = latest is not None
    if latest is None:
        return _execution_result(
            notification_delivery_result_event_found=True,
            notification_plan_loaded=True,
            latest_delivery_record_loaded=False,
            checks_failed=("notification_delivery_record_missing",),
        )

    before = _capture_scope(connection, event=event, plan=plan, latest=latest)
    matches_latest = _event_matches_latest_record(event=event, latest=latest)
    dry_run_noop = classify_dry_run_suppressed_logical_noop(
        delivery_status=latest.delivery_status,
        delivery_reason=latest.transport_error_code,
    )
    if not matches_latest:
        return _execution_result(
            notification_delivery_result_event_found=True,
            notification_plan_loaded=True,
            latest_delivery_record_loaded=latest_loaded,
            delivery_result_matches_latest_record=False,
            checks_failed=("notification_delivery_record_mismatch",),
        )
    if not dry_run_noop:
        return _execution_result(
            notification_delivery_result_event_found=True,
            notification_plan_loaded=True,
            latest_delivery_record_loaded=latest_loaded,
            delivery_result_matches_latest_record=True,
            dry_run_suppressed_classified_logical_noop=False,
            checks_failed=("notification_delivery_result_not_dry_run_suppressed",),
        )

    _insert_or_reuse_pipeline_run(connection, event_id=event.event_id)
    _insert_or_reuse_noop_job_attempt(connection, notification_plan_id=plan.notification_plan_id)
    after = _capture_scope(connection, event=event, plan=plan, latest=latest)

    maintenance_pipeline_run_recorded = _maintenance_pipeline_run_count(
        connection, event_id=event.event_id
    ) == 1
    maintenance_job_attempt_recorded = _maintenance_noop_job_attempt_count(
        connection, notification_plan_id=plan.notification_plan_id
    ) == 1
    retry_intent_event_created = after["retry_intent_events"] > before["retry_intent_events"]
    replay_request_created = (
        after["replay_requests"] > before["replay_requests"]
        or after["replay_intent_events"] > before["replay_intent_events"]
    )
    dead_letter_created = after["dead_letters"] > before["dead_letters"]
    checks_failed: list[str] = []
    if not maintenance_pipeline_run_recorded:
        checks_failed.append("maintenance_pipeline_run_recorded:missing")
    if not maintenance_job_attempt_recorded:
        checks_failed.append("maintenance_job_attempt_recorded:missing")
    for key, failure in (
        ("notification_plan_digest", "notification_plan_mutated"),
        ("delivery_record_digest", "notification_delivery_record_mutated"),
        ("analysis_digest", "analysis_mutated"),
        ("judge_output_digest", "judge_output_mutated"),
        ("candidate_group_digest", "candidate_group_mutated"),
    ):
        if after[key] != before[key]:
            checks_failed.append(failure)
    if retry_intent_event_created:
        checks_failed.append("retry_intent_event_created")
    if replay_request_created:
        checks_failed.append("replay_request_created")
    if dead_letter_created:
        checks_failed.append("dead_letter_created")

    return MaintenanceExecutionResult(
        notification_delivery_result_event_found=True,
        notification_plan_loaded=True,
        latest_delivery_record_loaded=True,
        delivery_result_matches_latest_record=True,
        dry_run_suppressed_classified_logical_noop=True,
        maintenance_pipeline_run_recorded=maintenance_pipeline_run_recorded,
        maintenance_job_attempt_recorded=maintenance_job_attempt_recorded,
        retry_intent_event_created=retry_intent_event_created,
        replay_request_created=replay_request_created,
        dead_letter_created=dead_letter_created,
        notification_plan_mutated=after["notification_plan_digest"] != before["notification_plan_digest"],
        notification_delivery_record_mutated=after["delivery_record_digest"] != before["delivery_record_digest"],
        analysis_mutated=after["analysis_digest"] != before["analysis_digest"],
        judge_output_mutated=after["judge_output_digest"] != before["judge_output_digest"],
        candidate_group_mutated=after["candidate_group_digest"] != before["candidate_group_digest"],
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _load_delivery_result_event_by_id(connection: Any, event_id: UUID) -> DeliveryResultEvent | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT event_id, event_type, aggregate_type, aggregate_id, payload_json
            FROM event_outbox
            WHERE event_id = CAST(:event_id AS uuid)
            """
        ),
        {"event_id": str(event_id)},
    ).mappings().first()
    if row is None or str(row["event_type"]) != NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE:
        return None
    payload = _json_loads(row["payload_json"])
    if not isinstance(payload, dict):
        return None
    notification_plan_id = _uuid_or_none(payload.get("notification_plan_id"))
    if notification_plan_id is None:
        return None
    delivery_status = _string_or_none(payload.get("delivery_status"))
    if delivery_status is None:
        return None
    return DeliveryResultEvent(
        event_id=UUID(str(row["event_id"])),
        aggregate_type=_string_or_none(row["aggregate_type"]),
        aggregate_id=_uuid_or_none(row["aggregate_id"]),
        notification_plan_id=notification_plan_id,
        notification_delivery_record_id=_uuid_or_none(payload.get("notification_delivery_record_id")),
        delivery_status=delivery_status,
        attempt_count=_int_or_none(payload.get("attempt_count")),
        reason_code=_string_or_none(payload.get("reason_code") or payload.get("transport_error_code")),
        dry_run=payload.get("dry_run") is True,
        payload_json=payload,
    )


def _load_notification_plan(connection: Any, notification_plan_id: UUID) -> NotificationPlanRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT notification_plan_id, analysis_id, candidate_group_id, status
            FROM notification_plans
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
            """
        ),
        {"notification_plan_id": str(notification_plan_id)},
    ).mappings().first()
    if row is None:
        return None
    return NotificationPlanRecord(
        notification_plan_id=UUID(str(row["notification_plan_id"])),
        analysis_id=UUID(str(row["analysis_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        status=str(row["status"]),
    )


def _load_latest_delivery_record(connection: Any, notification_plan_id: UUID) -> DeliveryRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT notification_delivery_record_id, notification_plan_id, delivery_status,
                   attempt_count, telegram_message_id, transport_error_code,
                   transport_error_class, telegram_response_json
            FROM notification_delivery_records
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
            ORDER BY created_at DESC, notification_delivery_record_id DESC
            LIMIT 1
            """
        ),
        {"notification_plan_id": str(notification_plan_id)},
    ).mappings().first()
    if row is None:
        return None
    payload = _json_loads(row["telegram_response_json"])
    return DeliveryRecord(
        notification_delivery_record_id=UUID(str(row["notification_delivery_record_id"])),
        notification_plan_id=UUID(str(row["notification_plan_id"])),
        delivery_status=str(row["delivery_status"]),
        attempt_count=int(row["attempt_count"]),
        telegram_message_id=_int_or_none(row["telegram_message_id"]),
        transport_error_code=_string_or_none(row["transport_error_code"]),
        transport_error_class=_string_or_none(row["transport_error_class"]),
        telegram_response_json=payload if isinstance(payload, dict) else None,
    )


def _event_matches_latest_record(*, event: DeliveryResultEvent, latest: DeliveryRecord) -> bool:
    response = latest.telegram_response_json or {}
    return (
        event.notification_plan_id == latest.notification_plan_id
        and (event.notification_delivery_record_id is None or event.notification_delivery_record_id == latest.notification_delivery_record_id)
        and event.delivery_status == "suppressed"
        and event.attempt_count == 0
        and event.reason_code == DRY_RUN_REASON_CODE
        and event.dry_run is True
        and latest.delivery_status == "suppressed"
        and latest.attempt_count == 0
        and latest.telegram_message_id is None
        and latest.transport_error_code == DRY_RUN_REASON_CODE
        and latest.transport_error_class is None
        and response.get("dry_run") is True
        and response.get("reason_code") == DRY_RUN_REASON_CODE
    )


def _insert_or_reuse_pipeline_run(connection: Any, *, event_id: UUID) -> None:
    import sqlalchemy as sa

    existing = _maintenance_pipeline_run_count(connection, event_id=event_id)
    if existing > 1:
        raise ValueError("maintenance_pipeline_run_ambiguous")
    if existing == 1:
        return
    connection.execute(
        sa.text(
            """
            INSERT INTO pipeline_runs (
                trigger_source,
                run_kind,
                root_object_type,
                root_object_id,
                started_at,
                finished_at,
                terminal_status
            ) VALUES (
                :trigger_source,
                :run_kind,
                'event_outbox',
                CAST(:event_id AS uuid),
                now(),
                now(),
                'succeeded'
            )
            """
        ),
        {
            "trigger_source": MAINTENANCE_TRIGGER_SOURCE,
            "run_kind": MAINTENANCE_RUN_KIND,
            "event_id": str(event_id),
        },
    )


def _insert_or_reuse_noop_job_attempt(connection: Any, *, notification_plan_id: UUID) -> None:
    import sqlalchemy as sa

    existing = _maintenance_noop_job_attempt_count(connection, notification_plan_id=notification_plan_id)
    if existing > 1:
        raise ValueError("maintenance_job_attempt_ambiguous")
    if existing == 1:
        return
    connection.execute(
        sa.text(
            """
            INSERT INTO job_attempts (
                stage_name,
                queue_name,
                root_object_type,
                root_object_id,
                attempt_no,
                started_at,
                finished_at,
                attempt_status,
                error_code,
                created_at
            ) VALUES (
                :stage_name,
                :queue_name,
                'notification_plan',
                CAST(:notification_plan_id AS uuid),
                1,
                now(),
                now(),
                'succeeded'::job_attempt_status_enum,
                :error_code,
                now()
            )
            """
        ),
        {
            "stage_name": MAINTENANCE_STAGE_NAME,
            "queue_name": MAINTENANCE_QUEUE_NAME,
            "notification_plan_id": str(notification_plan_id),
            "error_code": MAINTENANCE_NOOP_ERROR_CODE,
        },
    )


def _maintenance_pipeline_run_count(connection: Any, *, event_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM pipeline_runs
                WHERE trigger_source = :trigger_source
                  AND run_kind = :run_kind
                  AND root_object_type = 'event_outbox'
                  AND root_object_id = CAST(:event_id AS uuid)
                  AND terminal_status = 'succeeded'
                """
            ),
            {
                "trigger_source": MAINTENANCE_TRIGGER_SOURCE,
                "run_kind": MAINTENANCE_RUN_KIND,
                "event_id": str(event_id),
            },
        ).scalar_one()
    )


def _maintenance_noop_job_attempt_count(connection: Any, *, notification_plan_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM job_attempts
                WHERE stage_name = :stage_name
                  AND queue_name = :queue_name
                  AND root_object_type = 'notification_plan'
                  AND root_object_id = CAST(:notification_plan_id AS uuid)
                  AND attempt_status = 'succeeded'::job_attempt_status_enum
                  AND error_code = :error_code
                """
            ),
            {
                "stage_name": MAINTENANCE_STAGE_NAME,
                "queue_name": MAINTENANCE_QUEUE_NAME,
                "notification_plan_id": str(notification_plan_id),
                "error_code": MAINTENANCE_NOOP_ERROR_CODE,
            },
        ).scalar_one()
    )


def _capture_scope(
    connection: Any,
    *,
    event: DeliveryResultEvent,
    plan: NotificationPlanRecord,
    latest: DeliveryRecord,
) -> dict[str, Any]:
    return {
        "notification_plan_digest": _notification_plan_digest(connection, notification_plan_id=plan.notification_plan_id),
        "delivery_record_digest": _delivery_record_digest(
            connection,
            notification_delivery_record_id=latest.notification_delivery_record_id,
        ),
        "analysis_digest": _analysis_digest(connection, analysis_id=plan.analysis_id),
        "judge_output_digest": _judge_output_digest_for_analysis(connection, analysis_id=plan.analysis_id),
        "candidate_group_digest": _candidate_group_digest(
            connection,
            candidate_group_id=plan.candidate_group_id,
        ),
        "retry_intent_events": _retry_intent_event_count(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "replay_intent_events": _replay_intent_event_count(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "replay_requests": _replay_request_count(connection, notification_plan_id=plan.notification_plan_id),
        "dead_letters": _dead_letter_count(connection, notification_plan_id=plan.notification_plan_id),
        "maintenance_pipeline_runs": _maintenance_pipeline_run_count(connection, event_id=event.event_id),
        "maintenance_job_attempts": _maintenance_noop_job_attempt_count(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
    }


def _notification_plan_digest(connection: Any, *, notification_plan_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT notification_plan_id::text AS notification_plan_id,
                   analysis_id::text AS analysis_id,
                   candidate_group_id::text AS candidate_group_id,
                   delivery_decision::text AS delivery_decision,
                   urgency_profile::text AS urgency_profile,
                   target_chat_id,
                   target_thread_id,
                   render_profile,
                   dedupe_subject_key,
                   material_change_hash,
                   send_after::text AS send_after,
                   suppress_reason_code,
                   status::text AS status,
                   created_at::text AS created_at
            FROM notification_plans
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
            """
        ),
        {"notification_plan_id": str(notification_plan_id)},
    ).mappings().first()
    return _stable_digest(dict(row) if row is not None else None)


def _delivery_record_digest(connection: Any, *, notification_delivery_record_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT notification_delivery_record_id::text AS notification_delivery_record_id,
                   notification_plan_id::text AS notification_plan_id,
                   telegram_chat_id,
                   telegram_message_id,
                   delivery_status::text AS delivery_status,
                   sent_at::text AS sent_at,
                   edited_at::text AS edited_at,
                   attempt_count,
                   transport_error_code,
                   transport_error_class,
                   telegram_response_json,
                   created_at::text AS created_at
            FROM notification_delivery_records
            WHERE notification_delivery_record_id = CAST(:notification_delivery_record_id AS uuid)
            """
        ),
        {"notification_delivery_record_id": str(notification_delivery_record_id)},
    ).mappings().first()
    return _stable_digest(dict(row) if row is not None else None)


def _analysis_digest(connection: Any, *, analysis_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT analysis_id,
                   candidate_group_id,
                   judge_output_id,
                   schema_version,
                   policy_version,
                   prompt_version,
                   delivery_policy_version,
                   verdict,
                   delivery_decision,
                   scores_json,
                   reason_codes_json,
                   evidence_limitations_ko,
                   recommended_action_ko,
                   freshness_note_ko,
                   model_proposed_verdict,
                   policy_reconciled_flag
            FROM analyses
            WHERE analysis_id = CAST(:analysis_id AS uuid)
            """
        ),
        {"analysis_id": str(analysis_id)},
    ).mappings().first()
    return _stable_digest(dict(row) if row is not None else None)


def _judge_output_digest_for_analysis(connection: Any, *, analysis_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT jo.judge_output_id,
                   jo.judge_run_id,
                   jo.candidate_group_id,
                   jo.judge_schema_version,
                   jo.payload_json,
                   jo.model_proposed_verdict,
                   jo.model_confidence_band
            FROM analyses a
            JOIN judge_outputs jo ON jo.judge_output_id = a.judge_output_id
            WHERE a.analysis_id = CAST(:analysis_id AS uuid)
            """
        ),
        {"analysis_id": str(analysis_id)},
    ).mappings().first()
    return _stable_digest(dict(row) if row is not None else None)


def _candidate_group_digest(connection: Any, *, candidate_group_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT candidate_group_id,
                   current_bundle_id,
                   current_analysis_id,
                   current_primary_artifact_id
            FROM candidate_group_proposals
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    ).mappings().first()
    return _stable_digest(dict(row) if row is not None else None)


def _retry_intent_event_count(connection: Any, *, notification_plan_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = 'notification.plan.created.v1'
                  AND payload_json ->> 'notification_plan_id' = :notification_plan_id
                  AND payload_json ? 'retry_reason'
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        ).scalar_one()
    )


def _replay_intent_event_count(connection: Any, *, notification_plan_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = 'notification.plan.created.v1'
                  AND payload_json ->> 'notification_plan_id' = :notification_plan_id
                  AND payload_json ? 'replay_request_id'
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        ).scalar_one()
    )


def _replay_request_count(connection: Any, *, notification_plan_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM replay_requests
                WHERE replay_type = 'delivery'::replay_type_enum
                  AND root_object_type = 'notification_plan'
                  AND root_object_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        ).scalar_one()
    )


def _dead_letter_count(connection: Any, *, notification_plan_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM dead_letter_entries
                WHERE root_object_type = 'notification_plan'
                  AND root_object_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        ).scalar_one()
    )


def _find_delivery_result_event_ids_by_namespace(
    *,
    database_url: str,
    replay_namespace: str,
) -> list[UUID]:
    render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001 - local tool reuse.
    import sqlalchemy as sa

    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.begin() as connection:
            rows = connection.execute(
                sa.text(
                    """
                    SELECT event_id
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND dedupe_key LIKE :dedupe_prefix
                    ORDER BY created_at, event_id
                    """
                ),
                {
                    "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
                    "dedupe_prefix": (
                        "local-db-notification-render-dry-run:"
                        f"{replay_namespace}:notification.delivery.result:%"
                    ),
                },
            ).scalars().all()
            return [UUID(str(row)) for row in rows]
    finally:
        engine.dispose()


def _render_fixture_result_acceptable(predecessor: render_runner.RunnerResult) -> bool:
    if predecessor.exit_code != 0 or predecessor.report.get("status") != "pass":
        return False
    expected_true = (
        "database_url_guard_passed",
        "notification_plan_created_event_found",
        "analysis_loaded",
        "judge_output_loaded",
        "candidate_group_loaded",
        "primary_artifact_loaded",
        "notification_plan_concretized",
        "notification_render_created",
        "dry_run_delivery_record_created",
        "notification_delivery_result_event_created",
    )
    expected_false = (
        "openai_called",
        "telegram_called",
        "live_github_called",
        "workers_started",
        "redis_mutation",
        "production_db_write",
        "alembic_or_ddl_ran",
        "verdict_recomputed",
        "delivery_decision_overridden",
    )
    return all(predecessor.report.get(key) is True for key in expected_true) and all(
        predecessor.report.get(key) is False for key in expected_false
    )


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "notification_delivery_result_event_found": False,
        "notification_plan_loaded": False,
        "latest_delivery_record_loaded": False,
        "delivery_result_matches_latest_record": False,
        "dry_run_suppressed_classified_logical_noop": False,
        "maintenance_pipeline_run_recorded": False,
        "maintenance_job_attempt_recorded": False,
        "retry_intent_event_created": False,
        "replay_request_created": False,
        "dead_letter_created": False,
        "notification_plan_mutated": False,
        "notification_delivery_record_mutated": False,
        "analysis_mutated": False,
        "judge_output_mutated": False,
        "candidate_group_mutated": False,
        "openai_called": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "checks_failed": [],
    }


def _execution_result(
    *,
    notification_delivery_result_event_found: bool = False,
    notification_plan_loaded: bool = False,
    latest_delivery_record_loaded: bool = False,
    delivery_result_matches_latest_record: bool = False,
    dry_run_suppressed_classified_logical_noop: bool = False,
    maintenance_pipeline_run_recorded: bool = False,
    maintenance_job_attempt_recorded: bool = False,
    retry_intent_event_created: bool = False,
    replay_request_created: bool = False,
    dead_letter_created: bool = False,
    notification_plan_mutated: bool = False,
    notification_delivery_record_mutated: bool = False,
    analysis_mutated: bool = False,
    judge_output_mutated: bool = False,
    candidate_group_mutated: bool = False,
    checks_failed: Sequence[str],
) -> MaintenanceExecutionResult:
    return MaintenanceExecutionResult(
        notification_delivery_result_event_found=notification_delivery_result_event_found,
        notification_plan_loaded=notification_plan_loaded,
        latest_delivery_record_loaded=latest_delivery_record_loaded,
        delivery_result_matches_latest_record=delivery_result_matches_latest_record,
        dry_run_suppressed_classified_logical_noop=dry_run_suppressed_classified_logical_noop,
        maintenance_pipeline_run_recorded=maintenance_pipeline_run_recorded,
        maintenance_job_attempt_recorded=maintenance_job_attempt_recorded,
        retry_intent_event_created=retry_intent_event_created,
        replay_request_created=replay_request_created,
        dead_letter_created=dead_letter_created,
        notification_plan_mutated=notification_plan_mutated,
        notification_delivery_record_mutated=notification_delivery_record_mutated,
        analysis_mutated=analysis_mutated,
        judge_output_mutated=judge_output_mutated,
        candidate_group_mutated=candidate_group_mutated,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return None


def _stable_digest(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


def _json_default(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return _as_utc(value).isoformat()
    return str(value)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _path_or_none(value: Any) -> Path | None:
    text = _string_or_none(value)
    return Path(text) if text is not None else None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc)
    if message in SAFE_EXCEPTION_MESSAGES:
        return message
    return exc.__class__.__name__


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
