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

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools import local_db_notification_plan_created_render_dry_run_fixture_runner as render_runner


SCHEMA_VERSION = "local_db_maintenance_due_failed_retryable_retry_intent_fixture_runner_v1"
NOTIFICATION_PLAN_CREATED_EVENT_TYPE = "notification.plan.created.v1"
FAILED_RETRYABLE_STATUS = "failed_retryable"
RETRY_REASON = "due_retry_promotion"
FIXTURE_TRANSPORT_ERROR_CODE = "telegram_retryable"
FIXTURE_TRANSPORT_ERROR_CLASS = "TelegramTransportRetryableError"
MAINTENANCE_TRIGGER_SOURCE = "local_db_maintenance_due_failed_retryable_retry_intent_fixture_runner"
MAINTENANCE_RUN_KIND = "local_test_due_retry_promotion"
MAINTENANCE_STAGE_NAME = "maintenance_due_retry_promotion"
MAINTENANCE_QUEUE_NAME = "q.maintenance"
DEFAULT_MAX_ATTEMPTS = 5
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "due_failed_retryable_plan_loaded",
    "latest_failed_retryable_delivery_record_loaded",
    "due_retry_candidate_valid",
    "retry_intent_event_created",
    "retry_intent_payload_matches_plan",
    "retry_intent_dedupe_key_stable",
    "maintenance_pipeline_run_recorded",
    "maintenance_job_attempt_recorded",
)
FALSE_RESULT_KEYS = (
    "replay_request_created",
    "dead_letter_created",
    "notification_plan_mutated_by_maintenance",
    "notification_delivery_record_mutated_by_maintenance",
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
    "fixture_notification_plan_ambiguous",
    "fixture_notification_plan_missing_or_invalid",
    "fixture_failed_retryable_preparation_failed",
    "due_failed_retryable_plan_missing_or_invalid",
    "latest_failed_retryable_delivery_record_missing_or_invalid",
    "notification_plan_not_due",
    "max_notification_retry_attempts_exceeded",
    "retry_intent_event_ambiguous",
    "retry_intent_event_missing_or_invalid",
    "retry_intent_payload_mismatch",
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
class FixtureResolutionResult:
    notification_plan_id: UUID | None
    failed_retryable_fixture_prepared: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class NotificationPlanRecord:
    notification_plan_id: UUID
    analysis_id: UUID
    candidate_group_id: UUID
    delivery_decision: str
    urgency_profile: str
    target_chat_id: int
    target_thread_id: int | None
    render_profile: str | None
    dedupe_subject_key: str
    material_change_hash: str
    send_after: datetime | None
    suppress_reason_code: str | None
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
    due_failed_retryable_plan_loaded: bool
    latest_failed_retryable_delivery_record_loaded: bool
    due_retry_candidate_valid: bool
    retry_intent_event_created: bool
    retry_intent_payload_matches_plan: bool
    retry_intent_dedupe_key_stable: bool
    maintenance_pipeline_run_recorded: bool
    maintenance_job_attempt_recorded: bool
    replay_request_created: bool = False
    dead_letter_created: bool = False
    notification_plan_mutated_by_maintenance: bool = False
    notification_delivery_record_mutated_by_maintenance: bool = False
    analysis_mutated: bool = False
    judge_output_mutated: bool = False
    candidate_group_mutated: bool = False
    retry_intent_event_count_before: int = 0
    retry_intent_event_count_after: int = 0
    maintenance_pipeline_run_count_before: int = 0
    maintenance_pipeline_run_count_after: int = 0
    maintenance_job_attempt_count_before: int = 0
    maintenance_job_attempt_count_after: int = 0
    checks_failed: tuple[str, ...] = ()


class FixturePlanResolver(Protocol):
    def resolve(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> FixtureResolutionResult: ...


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
        notification_plan_id: UUID,
        max_attempts: int,
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


class DefaultFixturePlanResolver:
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
    ) -> FixtureResolutionResult:
        predecessor = self._upstream.run(
            database_url=database_url,
            source_fixture_path=source_fixture_path,
            github_snapshot_fixture_path=github_snapshot_fixture_path,
            replay_namespace=replay_namespace,
            env=env,
            repo_root=repo_root,
        )
        if not _render_fixture_result_acceptable(predecessor):
            return FixtureResolutionResult(
                notification_plan_id=None,
                failed_retryable_fixture_prepared=False,
                checks_failed=("notification_render_dry_run_fixture_failed",),
            )

        plan_ids = _find_fixture_notification_plan_ids_by_namespace(
            database_url=database_url,
            replay_namespace=replay_namespace,
        )
        if len(plan_ids) > 1:
            return FixtureResolutionResult(
                notification_plan_id=None,
                failed_retryable_fixture_prepared=False,
                checks_failed=("fixture_notification_plan_ambiguous",),
            )
        if len(plan_ids) != 1:
            return FixtureResolutionResult(
                notification_plan_id=None,
                failed_retryable_fixture_prepared=False,
                checks_failed=("fixture_notification_plan_missing_or_invalid",),
            )

        prepared = _prepare_failed_retryable_fixture(
            database_url=database_url,
            notification_plan_id=plan_ids[0],
        )
        if not prepared:
            return FixtureResolutionResult(
                notification_plan_id=plan_ids[0],
                failed_retryable_fixture_prepared=False,
                checks_failed=("fixture_failed_retryable_preparation_failed",),
            )
        return FixtureResolutionResult(
            notification_plan_id=plan_ids[0],
            failed_retryable_fixture_prepared=True,
        )


class SqlAlchemyMaintenanceExecutor:
    def execute(
        self,
        *,
        database_url: str,
        notification_plan_id: UUID,
        max_attempts: int,
    ) -> MaintenanceExecutionResult:
        render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001 - local tool reuse.
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                return _execute_maintenance(
                    connection,
                    notification_plan_id=notification_plan_id,
                    max_attempts=max_attempts,
                )
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Promote one local/test DB due failed_retryable notification plan into "
            "a deterministic notification.plan.created.v1 retry intent, proving "
            "maintenance does not mutate notifier-owned or upstream durable rows."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--notification-plan-id")
    parser.add_argument("--source-fixture")
    parser.add_argument("--github-snapshot-fixture")
    parser.add_argument("--replay-namespace")
    parser.add_argument("--prepare-failed-retryable-fixture", action="store_true")
    parser.add_argument("--confirm-local-test-db", action="store_true")
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    resolver: FixturePlanResolver | None = None,
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

    max_attempts = _max_attempts_from_env(effective_env)
    if max_attempts is None:
        checks_failed.append("notification_retry_max_attempts_invalid")
        max_attempts = DEFAULT_MAX_ATTEMPTS

    explicit_plan_id = _uuid_or_none(getattr(args, "notification_plan_id", None))
    if getattr(args, "notification_plan_id", None) and explicit_plan_id is None:
        checks_failed.append("notification_plan_id_invalid")

    source_fixture = _path_or_none(getattr(args, "source_fixture", None))
    github_fixture = _path_or_none(getattr(args, "github_snapshot_fixture", None))
    replay_namespace = _string_or_none(getattr(args, "replay_namespace", None))
    prepare_fixture = bool(getattr(args, "prepare_failed_retryable_fixture", False))
    fixture_selector_supplied = (
        source_fixture is not None or github_fixture is not None or replay_namespace is not None or prepare_fixture
    )

    if explicit_plan_id is not None and fixture_selector_supplied:
        checks_failed.append("selector_mode_ambiguous")

    if explicit_plan_id is None:
        if source_fixture is None or github_fixture is None or replay_namespace is None or not prepare_fixture:
            checks_failed.append("fixture_selector_required")
        if fixture_selector_supplied and (
            source_fixture is None or github_fixture is None or replay_namespace is None or not prepare_fixture
        ):
            checks_failed.append("fixture_selector_incomplete")

    if replay_namespace is not None:
        namespace_ok, namespace_failures = source_candidate_runner.validate_replay_namespace(replay_namespace)
        checks_failed.extend(namespace_failures)
        if not namespace_ok:
            replay_namespace = None

    if explicit_plan_id is None and source_fixture is not None and github_fixture is not None:
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

    if explicit_plan_id is not None:
        resolved_plan_id = explicit_plan_id
    else:
        if source_fixture is None or github_fixture is None or replay_namespace is None:
            return _finish(report, ["fixture_selector_required"])
        active_resolver = resolver or DefaultFixturePlanResolver()
        try:
            resolution = active_resolver.resolve(
                database_url=args.database_url,
                source_fixture_path=source_fixture,
                github_snapshot_fixture_path=github_fixture,
                replay_namespace=replay_namespace,
                env=effective_env,
                repo_root=root,
            )
        except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
            return _finish(report, [_safe_failure_code(exc)])
        report["failed_retryable_fixture_prepared"] = resolution.failed_retryable_fixture_prepared
        checks_failed.extend(resolution.checks_failed)
        resolved_plan_id = resolution.notification_plan_id
        if resolved_plan_id is None:
            checks_failed.append("fixture_notification_plan_missing_or_invalid")
        if checks_failed:
            return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyMaintenanceExecutor()
    try:
        execution = active_executor.execute(
            database_url=args.database_url,
            notification_plan_id=resolved_plan_id,
            max_attempts=max_attempts,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
        return _finish(report, [_safe_failure_code(exc)])

    report.update(
        {
            "due_failed_retryable_plan_loaded": execution.due_failed_retryable_plan_loaded,
            "latest_failed_retryable_delivery_record_loaded": execution.latest_failed_retryable_delivery_record_loaded,
            "due_retry_candidate_valid": execution.due_retry_candidate_valid,
            "retry_intent_event_created": execution.retry_intent_event_created,
            "retry_intent_payload_matches_plan": execution.retry_intent_payload_matches_plan,
            "retry_intent_dedupe_key_stable": execution.retry_intent_dedupe_key_stable,
            "maintenance_pipeline_run_recorded": execution.maintenance_pipeline_run_recorded,
            "maintenance_job_attempt_recorded": execution.maintenance_job_attempt_recorded,
            "replay_request_created": execution.replay_request_created,
            "dead_letter_created": execution.dead_letter_created,
            "notification_plan_mutated_by_maintenance": execution.notification_plan_mutated_by_maintenance,
            "notification_delivery_record_mutated_by_maintenance": (
                execution.notification_delivery_record_mutated_by_maintenance
            ),
            "analysis_mutated": execution.analysis_mutated,
            "judge_output_mutated": execution.judge_output_mutated,
            "candidate_group_mutated": execution.candidate_group_mutated,
            "retry_intent_event_count_before": execution.retry_intent_event_count_before,
            "retry_intent_event_count_after": execution.retry_intent_event_count_after,
            "maintenance_pipeline_run_count_before": execution.maintenance_pipeline_run_count_before,
            "maintenance_pipeline_run_count_after": execution.maintenance_pipeline_run_count_after,
            "maintenance_job_attempt_count_before": execution.maintenance_job_attempt_count_before,
            "maintenance_job_attempt_count_after": execution.maintenance_job_attempt_count_after,
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


def build_retry_intent_dedupe_key(
    *,
    notification_plan_id: UUID,
    latest_attempt_count: int,
    send_after: datetime | None,
) -> str:
    render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001 - local tool reuse.
    from services.maintenance.delivery_retry import retry_intent_dedupe_key

    return retry_intent_dedupe_key(
        notification_plan_id=notification_plan_id,
        latest_attempt_count=latest_attempt_count,
        send_after=send_after,
    )


def _execute_maintenance(
    connection: Any,
    *,
    notification_plan_id: UUID,
    max_attempts: int,
) -> MaintenanceExecutionResult:
    plan = _load_notification_plan(connection, notification_plan_id)
    if plan is None:
        return _execution_result(
            due_failed_retryable_plan_loaded=False,
            checks_failed=("due_failed_retryable_plan_missing_or_invalid",),
        )

    now = _database_now(connection)
    latest = _load_latest_delivery_record(connection, notification_plan_id)
    latest_loaded = latest is not None and latest.delivery_status == FAILED_RETRYABLE_STATUS
    if latest is None:
        return _execution_result(
            due_failed_retryable_plan_loaded=_plan_is_failed_retryable_due(plan, now=now),
            latest_failed_retryable_delivery_record_loaded=False,
            checks_failed=("latest_failed_retryable_delivery_record_missing_or_invalid",),
        )

    before = _capture_scope(connection, plan=plan, latest=latest, dedupe_key=None)
    plan_due = _plan_is_failed_retryable_due(plan, now=now)
    if not plan_due:
        return _execution_result(
            due_failed_retryable_plan_loaded=False,
            latest_failed_retryable_delivery_record_loaded=latest_loaded,
            retry_intent_event_count_before=before["retry_intent_events"],
            retry_intent_event_count_after=before["retry_intent_events"],
            maintenance_pipeline_run_count_before=before["maintenance_pipeline_runs"],
            maintenance_pipeline_run_count_after=before["maintenance_pipeline_runs"],
            maintenance_job_attempt_count_before=before["maintenance_job_attempts"],
            maintenance_job_attempt_count_after=before["maintenance_job_attempts"],
            checks_failed=("notification_plan_not_due",),
        )
    if latest.delivery_status != FAILED_RETRYABLE_STATUS:
        return _execution_result(
            due_failed_retryable_plan_loaded=True,
            latest_failed_retryable_delivery_record_loaded=False,
            retry_intent_event_count_before=before["retry_intent_events"],
            retry_intent_event_count_after=before["retry_intent_events"],
            maintenance_pipeline_run_count_before=before["maintenance_pipeline_runs"],
            maintenance_pipeline_run_count_after=before["maintenance_pipeline_runs"],
            maintenance_job_attempt_count_before=before["maintenance_job_attempts"],
            maintenance_job_attempt_count_after=before["maintenance_job_attempts"],
            checks_failed=("latest_failed_retryable_delivery_record_missing_or_invalid",),
        )

    decision = _evaluate_retry_promotion(
        delivery_status=latest.delivery_status,
        plan=plan,
        latest_attempt_count=latest.attempt_count,
        max_attempts=max_attempts,
        now=now,
    )
    if decision["action"] != "emit_retry_intent":
        return _execution_result(
            due_failed_retryable_plan_loaded=True,
            latest_failed_retryable_delivery_record_loaded=True,
            retry_intent_event_count_before=before["retry_intent_events"],
            retry_intent_event_count_after=before["retry_intent_events"],
            maintenance_pipeline_run_count_before=before["maintenance_pipeline_runs"],
            maintenance_pipeline_run_count_after=before["maintenance_pipeline_runs"],
            maintenance_job_attempt_count_before=before["maintenance_job_attempts"],
            maintenance_job_attempt_count_after=before["maintenance_job_attempts"],
            checks_failed=(str(decision["reason_code"]),),
        )

    dedupe_key = str(decision["dedupe_key"])
    payload = dict(decision["payload"])
    before = _capture_scope(connection, plan=plan, latest=latest, dedupe_key=dedupe_key)
    _insert_or_reuse_retry_intent_event(connection, plan=plan, dedupe_key=dedupe_key, payload=payload)
    _insert_or_reuse_pipeline_run(connection, notification_plan_id=plan.notification_plan_id)
    _insert_or_reuse_job_attempt(connection, notification_plan_id=plan.notification_plan_id)
    after = _capture_scope(connection, plan=plan, latest=latest, dedupe_key=dedupe_key)

    retry_event = _load_retry_intent_event_by_dedupe_key(connection, dedupe_key=dedupe_key)
    payload_matches = retry_event == payload
    dedupe_stable = (
        dedupe_key
        == build_retry_intent_dedupe_key(
            notification_plan_id=plan.notification_plan_id,
            latest_attempt_count=latest.attempt_count,
            send_after=plan.send_after,
        )
        == build_retry_intent_dedupe_key(
            notification_plan_id=plan.notification_plan_id,
            latest_attempt_count=latest.attempt_count,
            send_after=plan.send_after,
        )
    )
    checks_failed: list[str] = []
    if after["retry_intent_events"] != 1:
        checks_failed.append("retry_intent_event_missing_or_invalid")
    if not payload_matches:
        checks_failed.append("retry_intent_payload_mismatch")
    if after["maintenance_pipeline_runs"] != 1:
        checks_failed.append("maintenance_pipeline_run_recorded:missing")
    if after["maintenance_job_attempts"] != 1:
        checks_failed.append("maintenance_job_attempt_recorded:missing")
    for key, failure in (
        ("notification_plan_digest", "notification_plan_mutated_by_maintenance"),
        ("delivery_record_digest", "notification_delivery_record_mutated_by_maintenance"),
        ("analysis_digest", "analysis_mutated"),
        ("judge_output_digest", "judge_output_mutated"),
        ("candidate_group_digest", "candidate_group_mutated"),
    ):
        if after[key] != before[key]:
            checks_failed.append(failure)
    replay_request_created = after["replay_requests"] > before["replay_requests"]
    dead_letter_created = after["dead_letters"] > before["dead_letters"]
    if replay_request_created:
        checks_failed.append("replay_request_created")
    if dead_letter_created:
        checks_failed.append("dead_letter_created")

    return MaintenanceExecutionResult(
        due_failed_retryable_plan_loaded=True,
        latest_failed_retryable_delivery_record_loaded=True,
        due_retry_candidate_valid=True,
        retry_intent_event_created=after["retry_intent_events"] == 1,
        retry_intent_payload_matches_plan=payload_matches,
        retry_intent_dedupe_key_stable=dedupe_stable,
        maintenance_pipeline_run_recorded=after["maintenance_pipeline_runs"] == 1,
        maintenance_job_attempt_recorded=after["maintenance_job_attempts"] == 1,
        replay_request_created=replay_request_created,
        dead_letter_created=dead_letter_created,
        notification_plan_mutated_by_maintenance=after["notification_plan_digest"] != before["notification_plan_digest"],
        notification_delivery_record_mutated_by_maintenance=after["delivery_record_digest"] != before["delivery_record_digest"],
        analysis_mutated=after["analysis_digest"] != before["analysis_digest"],
        judge_output_mutated=after["judge_output_digest"] != before["judge_output_digest"],
        candidate_group_mutated=after["candidate_group_digest"] != before["candidate_group_digest"],
        retry_intent_event_count_before=before["retry_intent_events"],
        retry_intent_event_count_after=after["retry_intent_events"],
        maintenance_pipeline_run_count_before=before["maintenance_pipeline_runs"],
        maintenance_pipeline_run_count_after=after["maintenance_pipeline_runs"],
        maintenance_job_attempt_count_before=before["maintenance_job_attempts"],
        maintenance_job_attempt_count_after=after["maintenance_job_attempts"],
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _evaluate_retry_promotion(
    *,
    delivery_status: str,
    plan: NotificationPlanRecord,
    latest_attempt_count: int,
    max_attempts: int,
    now: datetime,
) -> dict[str, Any]:
    render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001 - local tool reuse.
    from services.maintenance.delivery_retry import evaluate_retry_promotion

    decision = evaluate_retry_promotion(
        delivery_status=delivery_status,
        plan=plan,
        latest_attempt_count=latest_attempt_count,
        max_attempts=max_attempts,
        enabled=True,
        now=now,
    )
    return {
        "action": decision.action,
        "reason_code": decision.reason_code,
        "dedupe_key": decision.dedupe_key,
        "payload": decision.payload,
    }


def _load_notification_plan(connection: Any, notification_plan_id: UUID) -> NotificationPlanRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT notification_plan_id, analysis_id, candidate_group_id,
                   delivery_decision, urgency_profile, target_chat_id,
                   target_thread_id, render_profile, dedupe_subject_key,
                   material_change_hash, send_after, suppress_reason_code, status
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
        delivery_decision=str(row["delivery_decision"]),
        urgency_profile=str(row["urgency_profile"]),
        target_chat_id=int(row["target_chat_id"]),
        target_thread_id=_int_or_none(row["target_thread_id"]),
        render_profile=_string_or_none(row["render_profile"]),
        dedupe_subject_key=str(row["dedupe_subject_key"]),
        material_change_hash=str(row["material_change_hash"]),
        send_after=row["send_after"],
        suppress_reason_code=_string_or_none(row["suppress_reason_code"]),
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


def _insert_or_reuse_retry_intent_event(
    connection: Any,
    *,
    plan: NotificationPlanRecord,
    dedupe_key: str,
    payload: Mapping[str, Any],
) -> None:
    import sqlalchemy as sa

    connection.execute(
        sa.text(
            """
            INSERT INTO event_outbox (
                event_type,
                aggregate_type,
                aggregate_id,
                dedupe_key,
                payload_json,
                status,
                created_at
            ) VALUES (
                :event_type,
                'analysis',
                CAST(:analysis_id AS uuid),
                :dedupe_key,
                CAST(:payload_json AS jsonb),
                'pending'::outbox_status_enum,
                now()
            )
            ON CONFLICT ON CONSTRAINT uq_event_outbox_dedupe_key
            DO NOTHING
            """
        ),
        {
            "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
            "analysis_id": str(plan.analysis_id),
            "dedupe_key": dedupe_key,
            "payload_json": _json_dumps(dict(payload)),
        },
    )


def _insert_or_reuse_pipeline_run(connection: Any, *, notification_plan_id: UUID) -> None:
    import sqlalchemy as sa

    existing = _maintenance_pipeline_run_count(connection, notification_plan_id=notification_plan_id)
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
                'notification_plan',
                CAST(:notification_plan_id AS uuid),
                now(),
                now(),
                'succeeded'
            )
            """
        ),
        {
            "trigger_source": MAINTENANCE_TRIGGER_SOURCE,
            "run_kind": MAINTENANCE_RUN_KIND,
            "notification_plan_id": str(notification_plan_id),
        },
    )


def _insert_or_reuse_job_attempt(connection: Any, *, notification_plan_id: UUID) -> None:
    import sqlalchemy as sa

    existing = _maintenance_job_attempt_count(connection, notification_plan_id=notification_plan_id)
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
                NULL,
                now()
            )
            """
        ),
        {
            "stage_name": MAINTENANCE_STAGE_NAME,
            "queue_name": MAINTENANCE_QUEUE_NAME,
            "notification_plan_id": str(notification_plan_id),
        },
    )


def _capture_scope(
    connection: Any,
    *,
    plan: NotificationPlanRecord,
    latest: DeliveryRecord,
    dedupe_key: str | None,
) -> dict[str, Any]:
    return {
        "notification_plan_digest": _notification_plan_digest(connection, notification_plan_id=plan.notification_plan_id),
        "delivery_record_digest": _delivery_record_digest(
            connection,
            notification_delivery_record_id=latest.notification_delivery_record_id,
        ),
        "analysis_digest": _analysis_digest(connection, analysis_id=plan.analysis_id),
        "judge_output_digest": _judge_output_digest_for_analysis(connection, analysis_id=plan.analysis_id),
        "candidate_group_digest": _candidate_group_digest(connection, candidate_group_id=plan.candidate_group_id),
        "retry_intent_events": _retry_intent_event_count(connection, dedupe_key=dedupe_key),
        "replay_requests": _replay_request_count(connection, notification_plan_id=plan.notification_plan_id),
        "dead_letters": _dead_letter_count(connection, notification_plan_id=plan.notification_plan_id),
        "maintenance_pipeline_runs": _maintenance_pipeline_run_count(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "maintenance_job_attempts": _maintenance_job_attempt_count(
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


def _retry_intent_event_count(connection: Any, *, dedupe_key: str | None) -> int:
    if dedupe_key is None:
        return 0
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = :event_type
                  AND dedupe_key = :dedupe_key
                  AND payload_json ->> 'retry_reason' = :retry_reason
                """
            ),
            {
                "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                "dedupe_key": dedupe_key,
                "retry_reason": RETRY_REASON,
            },
        ).scalar_one()
    )


def _load_retry_intent_event_by_dedupe_key(connection: Any, *, dedupe_key: str) -> dict[str, Any] | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT payload_json
            FROM event_outbox
            WHERE event_type = :event_type
              AND dedupe_key = :dedupe_key
              AND aggregate_type = 'analysis'
            """
        ),
        {"event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE, "dedupe_key": dedupe_key},
    ).mappings().first()
    payload = _json_loads(row["payload_json"]) if row is not None else None
    return payload if isinstance(payload, dict) else None


def _maintenance_pipeline_run_count(connection: Any, *, notification_plan_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM pipeline_runs
                WHERE trigger_source = :trigger_source
                  AND run_kind = :run_kind
                  AND root_object_type = 'notification_plan'
                  AND root_object_id = CAST(:notification_plan_id AS uuid)
                  AND terminal_status = 'succeeded'
                """
            ),
            {
                "trigger_source": MAINTENANCE_TRIGGER_SOURCE,
                "run_kind": MAINTENANCE_RUN_KIND,
                "notification_plan_id": str(notification_plan_id),
            },
        ).scalar_one()
    )


def _maintenance_job_attempt_count(connection: Any, *, notification_plan_id: UUID) -> int:
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
                  AND error_code IS NULL
                """
            ),
            {
                "stage_name": MAINTENANCE_STAGE_NAME,
                "queue_name": MAINTENANCE_QUEUE_NAME,
                "notification_plan_id": str(notification_plan_id),
            },
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


def _database_now(connection: Any) -> datetime:
    import sqlalchemy as sa

    value = connection.execute(sa.text("SELECT now()")).scalar_one()
    return _as_utc(value)


def _plan_is_failed_retryable_due(plan: NotificationPlanRecord, *, now: datetime) -> bool:
    return (
        plan.status == FAILED_RETRYABLE_STATUS
        and plan.send_after is not None
        and _as_utc(plan.send_after) <= _as_utc(now)
    )


def _find_fixture_notification_plan_ids_by_namespace(
    *,
    database_url: str,
    replay_namespace: str,
) -> list[UUID]:
    render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001
    import sqlalchemy as sa

    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.begin() as connection:
            rows = connection.execute(
                sa.text(
                    """
                    SELECT DISTINCT payload_json ->> 'notification_plan_id' AS notification_plan_id
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND dedupe_key LIKE :dedupe_prefix
                      AND COALESCE(payload_json ->> 'delivery_decision', '') <> 'suppress'
                    ORDER BY notification_plan_id
                    """
                ),
                {
                    "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                    "dedupe_prefix": f"local-db-policy-engine:{replay_namespace}:notification.plan.created:%",
                },
            ).scalars().all()
            return [plan_id for value in rows if (plan_id := _uuid_or_none(value)) is not None]
    finally:
        engine.dispose()


def _prepare_failed_retryable_fixture(
    *,
    database_url: str,
    notification_plan_id: UUID,
) -> bool:
    render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001
    import sqlalchemy as sa

    engine = sa.create_engine(database_url, future=True)
    response = {
        "local_fixture": True,
        "transport_skipped": True,
        "retryable": True,
        "reason_code": FIXTURE_TRANSPORT_ERROR_CODE,
    }
    try:
        with engine.begin() as connection:
            updated = connection.execute(
                sa.text(
                    """
                    UPDATE notification_plans
                    SET status = 'failed_retryable'::notification_status_enum,
                        send_after = now() - interval '1 second'
                    WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                    RETURNING notification_plan_id
                    """
                ),
                {"notification_plan_id": str(notification_plan_id)},
            ).scalar_one_or_none()
            if updated is None:
                return False
            connection.execute(
                sa.text(
                    """
                    INSERT INTO notification_delivery_records (
                        notification_plan_id,
                        telegram_chat_id,
                        telegram_message_id,
                        delivery_status,
                        sent_at,
                        edited_at,
                        attempt_count,
                        transport_error_code,
                        transport_error_class,
                        telegram_response_json,
                        created_at
                    )
                    SELECT
                        notification_plan_id,
                        target_chat_id,
                        NULL,
                        'failed_retryable'::notification_status_enum,
                        NULL,
                        NULL,
                        1,
                        :transport_error_code,
                        :transport_error_class,
                        CAST(:telegram_response_json AS jsonb),
                        now()
                    FROM notification_plans
                    WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                    """
                ),
                {
                    "notification_plan_id": str(notification_plan_id),
                    "transport_error_code": FIXTURE_TRANSPORT_ERROR_CODE,
                    "transport_error_class": FIXTURE_TRANSPORT_ERROR_CLASS,
                    "telegram_response_json": _json_dumps(response),
                },
            )
            return True
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
        "failed_retryable_fixture_prepared": False,
        "due_failed_retryable_plan_loaded": False,
        "latest_failed_retryable_delivery_record_loaded": False,
        "due_retry_candidate_valid": False,
        "retry_intent_event_created": False,
        "retry_intent_payload_matches_plan": False,
        "retry_intent_dedupe_key_stable": False,
        "maintenance_pipeline_run_recorded": False,
        "maintenance_job_attempt_recorded": False,
        "replay_request_created": False,
        "dead_letter_created": False,
        "notification_plan_mutated_by_maintenance": False,
        "notification_delivery_record_mutated_by_maintenance": False,
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
        "retry_intent_event_count_before": 0,
        "retry_intent_event_count_after": 0,
        "maintenance_pipeline_run_count_before": 0,
        "maintenance_pipeline_run_count_after": 0,
        "maintenance_job_attempt_count_before": 0,
        "maintenance_job_attempt_count_after": 0,
        "checks_failed": [],
    }


def _execution_result(
    *,
    due_failed_retryable_plan_loaded: bool = False,
    latest_failed_retryable_delivery_record_loaded: bool = False,
    due_retry_candidate_valid: bool = False,
    retry_intent_event_created: bool = False,
    retry_intent_payload_matches_plan: bool = False,
    retry_intent_dedupe_key_stable: bool = False,
    maintenance_pipeline_run_recorded: bool = False,
    maintenance_job_attempt_recorded: bool = False,
    replay_request_created: bool = False,
    dead_letter_created: bool = False,
    notification_plan_mutated_by_maintenance: bool = False,
    notification_delivery_record_mutated_by_maintenance: bool = False,
    analysis_mutated: bool = False,
    judge_output_mutated: bool = False,
    candidate_group_mutated: bool = False,
    retry_intent_event_count_before: int = 0,
    retry_intent_event_count_after: int = 0,
    maintenance_pipeline_run_count_before: int = 0,
    maintenance_pipeline_run_count_after: int = 0,
    maintenance_job_attempt_count_before: int = 0,
    maintenance_job_attempt_count_after: int = 0,
    checks_failed: Sequence[str],
) -> MaintenanceExecutionResult:
    return MaintenanceExecutionResult(
        due_failed_retryable_plan_loaded=due_failed_retryable_plan_loaded,
        latest_failed_retryable_delivery_record_loaded=latest_failed_retryable_delivery_record_loaded,
        due_retry_candidate_valid=due_retry_candidate_valid,
        retry_intent_event_created=retry_intent_event_created,
        retry_intent_payload_matches_plan=retry_intent_payload_matches_plan,
        retry_intent_dedupe_key_stable=retry_intent_dedupe_key_stable,
        maintenance_pipeline_run_recorded=maintenance_pipeline_run_recorded,
        maintenance_job_attempt_recorded=maintenance_job_attempt_recorded,
        replay_request_created=replay_request_created,
        dead_letter_created=dead_letter_created,
        notification_plan_mutated_by_maintenance=notification_plan_mutated_by_maintenance,
        notification_delivery_record_mutated_by_maintenance=notification_delivery_record_mutated_by_maintenance,
        analysis_mutated=analysis_mutated,
        judge_output_mutated=judge_output_mutated,
        candidate_group_mutated=candidate_group_mutated,
        retry_intent_event_count_before=retry_intent_event_count_before,
        retry_intent_event_count_after=retry_intent_event_count_after,
        maintenance_pipeline_run_count_before=maintenance_pipeline_run_count_before,
        maintenance_pipeline_run_count_after=maintenance_pipeline_run_count_after,
        maintenance_job_attempt_count_before=maintenance_job_attempt_count_before,
        maintenance_job_attempt_count_after=maintenance_job_attempt_count_after,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _max_attempts_from_env(env: Mapping[str, str]) -> int | None:
    raw = env.get("DELIVERY_RETRY_MAX_ATTEMPTS") or env.get("NOTIFICATION_RETRY_MAX_ATTEMPTS") or str(DEFAULT_MAX_ATTEMPTS)
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return None


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


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
