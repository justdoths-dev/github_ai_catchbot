from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from uuid import UUID

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools import local_db_maintenance_due_failed_retryable_retry_intent_fixture_runner as due_runner


SCHEMA_VERSION = "local_db_maintenance_max_attempts_failed_retryable_dead_letter_fixture_runner_v1"
NOTIFICATION_PLAN_CREATED_EVENT_TYPE = due_runner.NOTIFICATION_PLAN_CREATED_EVENT_TYPE
FAILED_RETRYABLE_STATUS = due_runner.FAILED_RETRYABLE_STATUS
RETRY_REASON = due_runner.RETRY_REASON
FIXTURE_TRANSPORT_ERROR_CODE = due_runner.FIXTURE_TRANSPORT_ERROR_CODE
FIXTURE_TRANSPORT_ERROR_CLASS = due_runner.FIXTURE_TRANSPORT_ERROR_CLASS
MAINTENANCE_TRIGGER_SOURCE = "local_db_maintenance_max_attempts_failed_retryable_dead_letter_fixture_runner"
MAINTENANCE_RUN_KIND = "local_test_retry_ceiling_dead_letter"
MAINTENANCE_STAGE_NAME = "maintenance_delivery_retry"
MAINTENANCE_QUEUE_NAME = "q.maintenance"
DEAD_LETTER_ERROR_CODE = "max_notification_retry_attempts_exceeded"
DEAD_LETTER_LAST_ERROR_SNIPPET = "delivery retry ceiling reached"
DEAD_LETTER_NEXT_MANUAL_ACTION = "request_explicit_delivery_replay"
DEAD_LETTER_REPLAY_HINT = "delivery_replay_from_notification_plan"
DEFAULT_MAX_ATTEMPTS = 5
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "due_failed_retryable_plan_loaded",
    "latest_failed_retryable_delivery_record_loaded",
    "retry_ceiling_candidate_valid",
    "max_attempts_exceeded",
    "dead_letter_created",
    "dead_letter_payload_matches_plan",
    "dead_letter_dedupe_or_uniqueness_stable",
    "maintenance_pipeline_run_recorded",
    "maintenance_job_attempt_recorded",
)
FALSE_RESULT_KEYS = (
    "retry_intent_event_created",
    "replay_request_created",
    "notification_plan_mutated_by_maintenance",
    "notification_delivery_record_mutated_by_maintenance",
    "analysis_mutated",
    "judge_output_mutated",
    "candidate_group_mutated",
    "evidence_bundle_mutated",
    "artifact_mutated",
    "source_message_mutated",
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
    "max_notification_retry_attempts_not_exceeded",
    "retry_intent_event_present",
    "dead_letter_ambiguous",
    "dead_letter_missing_or_invalid",
    "dead_letter_payload_mismatch",
    "maintenance_pipeline_run_ambiguous",
    "maintenance_job_attempt_ambiguous",
}

source_candidate_runner = due_runner.source_candidate_runner
github_snapshot_runner = due_runner.github_snapshot_runner
NotificationPlanRecord = due_runner.NotificationPlanRecord
DeliveryRecord = due_runner.DeliveryRecord


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
class MaintenanceExecutionResult:
    due_failed_retryable_plan_loaded: bool
    latest_failed_retryable_delivery_record_loaded: bool
    retry_ceiling_candidate_valid: bool
    max_attempts_exceeded: bool
    retry_intent_event_created: bool
    dead_letter_created: bool
    dead_letter_payload_matches_plan: bool
    dead_letter_dedupe_or_uniqueness_stable: bool
    maintenance_pipeline_run_recorded: bool
    maintenance_job_attempt_recorded: bool
    replay_request_created: bool = False
    notification_plan_mutated_by_maintenance: bool = False
    notification_delivery_record_mutated_by_maintenance: bool = False
    analysis_mutated: bool = False
    judge_output_mutated: bool = False
    candidate_group_mutated: bool = False
    evidence_bundle_mutated: bool = False
    artifact_mutated: bool = False
    source_message_mutated: bool = False
    retry_intent_event_count_before: int = 0
    retry_intent_event_count_after: int = 0
    dead_letter_count_before: int = 0
    dead_letter_count_after: int = 0
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
        max_attempts: int,
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
    ) -> Any: ...


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
    ) -> Any:
        return due_runner.render_runner.run(
            due_runner.render_runner.build_parser().parse_args(
                [
                    "--database-url",
                    database_url,
                    "--source-fixture",
                    str(source_fixture_path),
                    "--github-snapshot-fixture",
                    str(github_snapshot_fixture_path),
                    "--replay-namespace",
                    replay_namespace,
                    "--confirm-local-test-db",
                ]
            ),
            env=env,
            repo_root=repo_root,
        )


class DefaultFixturePlanResolver:
    def __init__(self, *, render_runner: RenderDryRunRunner | None = None) -> None:
        self._render_runner = render_runner or DefaultRenderDryRunRunner()

    def resolve(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        max_attempts: int,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> FixtureResolutionResult:
        predecessor = self._render_runner.run(
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
        plan_ids = due_runner._find_fixture_notification_plan_ids_by_namespace(  # noqa: SLF001
            database_url=database_url,
            replay_namespace=replay_namespace,
        )
        if len(plan_ids) > 1:
            return FixtureResolutionResult(
                notification_plan_id=None,
                failed_retryable_fixture_prepared=False,
                checks_failed=("fixture_notification_plan_ambiguous",),
            )
        if not plan_ids:
            return FixtureResolutionResult(
                notification_plan_id=None,
                failed_retryable_fixture_prepared=False,
                checks_failed=("fixture_notification_plan_missing_or_invalid",),
            )
        prepared = _prepare_failed_retryable_fixture(
            database_url=database_url,
            notification_plan_id=plan_ids[0],
            max_attempts=max_attempts,
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
        due_runner.render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001
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
            "Prove that a local/test DB due failed_retryable notification plan at the retry ceiling "
            "is dead-lettered without creating retry-intent events or replay requests."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--notification-plan-id")
    parser.add_argument("--source-fixture")
    parser.add_argument("--github-snapshot-fixture")
    parser.add_argument("--replay-namespace")
    parser.add_argument("--prepare-failed-retryable-fixture", action="store_true")
    parser.add_argument("--max-attempts")
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

    max_attempts = _max_attempts_from_args_env(getattr(args, "max_attempts", None), effective_env)
    if max_attempts is None:
        checks_failed.append("max_attempts_invalid")
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
                max_attempts=max_attempts,
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
    except Exception as exc:  # noqa: BLE001 - sanitized report only.
        return _finish(report, [_safe_failure_code(exc)])

    report.update(
        {
            "due_failed_retryable_plan_loaded": execution.due_failed_retryable_plan_loaded,
            "latest_failed_retryable_delivery_record_loaded": (
                execution.latest_failed_retryable_delivery_record_loaded
            ),
            "retry_ceiling_candidate_valid": execution.retry_ceiling_candidate_valid,
            "max_attempts_exceeded": execution.max_attempts_exceeded,
            "retry_intent_event_created": execution.retry_intent_event_created,
            "dead_letter_created": execution.dead_letter_created,
            "dead_letter_payload_matches_plan": execution.dead_letter_payload_matches_plan,
            "dead_letter_dedupe_or_uniqueness_stable": execution.dead_letter_dedupe_or_uniqueness_stable,
            "maintenance_pipeline_run_recorded": execution.maintenance_pipeline_run_recorded,
            "maintenance_job_attempt_recorded": execution.maintenance_job_attempt_recorded,
            "replay_request_created": execution.replay_request_created,
            "notification_plan_mutated_by_maintenance": execution.notification_plan_mutated_by_maintenance,
            "notification_delivery_record_mutated_by_maintenance": (
                execution.notification_delivery_record_mutated_by_maintenance
            ),
            "analysis_mutated": execution.analysis_mutated,
            "judge_output_mutated": execution.judge_output_mutated,
            "candidate_group_mutated": execution.candidate_group_mutated,
            "evidence_bundle_mutated": execution.evidence_bundle_mutated,
            "artifact_mutated": execution.artifact_mutated,
            "source_message_mutated": execution.source_message_mutated,
            "retry_intent_event_count_before": execution.retry_intent_event_count_before,
            "retry_intent_event_count_after": execution.retry_intent_event_count_after,
            "dead_letter_count_before": execution.dead_letter_count_before,
            "dead_letter_count_after": execution.dead_letter_count_after,
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
            checks_failed.append(key)
    return _finish(report, checks_failed)


def validate_database_url(database_url: str | None):
    return due_runner.validate_database_url(database_url)


def evaluate_retry_ceiling(
    *,
    delivery_status: str,
    plan: NotificationPlanRecord,
    latest_attempt_count: int,
    max_attempts: int,
    now: datetime,
) -> dict[str, Any]:
    return due_runner._evaluate_retry_promotion(  # noqa: SLF001 - local proof reuses predecessor policy wrapper.
        delivery_status=delivery_status,
        plan=plan,
        latest_attempt_count=latest_attempt_count,
        max_attempts=max_attempts,
        now=now,
    )


def _execute_maintenance(
    connection: Any,
    *,
    notification_plan_id: UUID,
    max_attempts: int,
) -> MaintenanceExecutionResult:
    plan = due_runner._load_notification_plan(connection, notification_plan_id)  # noqa: SLF001
    if plan is None:
        return _execution_result(
            due_failed_retryable_plan_loaded=False,
            checks_failed=("due_failed_retryable_plan_missing_or_invalid",),
        )

    now = due_runner._database_now(connection)  # noqa: SLF001
    latest = due_runner._load_latest_delivery_record(connection, notification_plan_id)  # noqa: SLF001
    latest_loaded = latest is not None and latest.delivery_status == FAILED_RETRYABLE_STATUS
    if latest is None:
        return _execution_result(
            due_failed_retryable_plan_loaded=due_runner._plan_is_failed_retryable_due(plan, now=now),  # noqa: SLF001
            latest_failed_retryable_delivery_record_loaded=False,
            checks_failed=("latest_failed_retryable_delivery_record_missing_or_invalid",),
        )

    before = _capture_scope(connection, plan=plan, latest=latest)
    plan_due = due_runner._plan_is_failed_retryable_due(plan, now=now)  # noqa: SLF001
    if not plan_due:
        return _execution_result(
            due_failed_retryable_plan_loaded=False,
            latest_failed_retryable_delivery_record_loaded=latest_loaded,
            retry_intent_event_count_before=before["retry_intent_events"],
            retry_intent_event_count_after=before["retry_intent_events"],
            dead_letter_count_before=before["dead_letters"],
            dead_letter_count_after=before["dead_letters"],
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
            dead_letter_count_before=before["dead_letters"],
            dead_letter_count_after=before["dead_letters"],
            maintenance_pipeline_run_count_before=before["maintenance_pipeline_runs"],
            maintenance_pipeline_run_count_after=before["maintenance_pipeline_runs"],
            maintenance_job_attempt_count_before=before["maintenance_job_attempts"],
            maintenance_job_attempt_count_after=before["maintenance_job_attempts"],
            checks_failed=("latest_failed_retryable_delivery_record_missing_or_invalid",),
        )

    decision = evaluate_retry_ceiling(
        delivery_status=latest.delivery_status,
        plan=plan,
        latest_attempt_count=latest.attempt_count,
        max_attempts=max_attempts,
        now=now,
    )
    max_exceeded = latest.attempt_count >= max_attempts
    if decision["action"] != "dead_letter_retry_ceiling" or not max_exceeded:
        reason = (
            "max_notification_retry_attempts_not_exceeded"
            if decision["action"] == "emit_retry_intent"
            else str(decision["reason_code"])
        )
        return _execution_result(
            due_failed_retryable_plan_loaded=True,
            latest_failed_retryable_delivery_record_loaded=True,
            retry_ceiling_candidate_valid=True,
            max_attempts_exceeded=max_exceeded,
            retry_intent_event_count_before=before["retry_intent_events"],
            retry_intent_event_count_after=before["retry_intent_events"],
            dead_letter_count_before=before["dead_letters"],
            dead_letter_count_after=before["dead_letters"],
            maintenance_pipeline_run_count_before=before["maintenance_pipeline_runs"],
            maintenance_pipeline_run_count_after=before["maintenance_pipeline_runs"],
            maintenance_job_attempt_count_before=before["maintenance_job_attempts"],
            maintenance_job_attempt_count_after=before["maintenance_job_attempts"],
            checks_failed=(reason,),
        )

    _insert_or_reuse_dead_letter(
        connection,
        notification_plan_id=plan.notification_plan_id,
        retry_count=latest.attempt_count,
    )
    _insert_or_reuse_pipeline_run(connection, notification_plan_id=plan.notification_plan_id)
    _insert_or_reuse_job_attempt(connection, notification_plan_id=plan.notification_plan_id)
    after = _capture_scope(connection, plan=plan, latest=latest)

    dead_letter = _load_dead_letter(connection, notification_plan_id=plan.notification_plan_id)
    payload_matches = _dead_letter_payload_matches(
        dead_letter,
        notification_plan_id=plan.notification_plan_id,
        retry_count=latest.attempt_count,
    )
    dedupe_stable = (
        before["dead_letters"] <= 1
        and after["dead_letters"] == 1
        and after["dead_letters"] - before["dead_letters"] in {0, 1}
    )

    checks_failed: list[str] = []
    if after["retry_intent_events"] > before["retry_intent_events"]:
        checks_failed.append("retry_intent_event_created")
    if after["dead_letters"] != 1:
        checks_failed.append("dead_letter_missing_or_invalid")
    if not payload_matches:
        checks_failed.append("dead_letter_payload_mismatch")
    if not dedupe_stable:
        checks_failed.append("dead_letter_dedupe_or_uniqueness_unstable")
    if after["maintenance_pipeline_runs"] != 1:
        checks_failed.append("maintenance_pipeline_run_recorded:missing")
    if after["maintenance_job_attempts"] != 1:
        checks_failed.append("maintenance_job_attempt_recorded:missing")
    if after["replay_requests"] != 0:
        checks_failed.append("replay_request_created")
    for key, failure in (
        ("notification_plan_digest", "notification_plan_mutated_by_maintenance"),
        ("delivery_record_digest", "notification_delivery_record_mutated_by_maintenance"),
        ("analysis_digest", "analysis_mutated"),
        ("judge_output_digest", "judge_output_mutated"),
        ("candidate_group_digest", "candidate_group_mutated"),
        ("evidence_bundle_digest", "evidence_bundle_mutated"),
        ("artifact_digest", "artifact_mutated"),
        ("source_message_digest", "source_message_mutated"),
    ):
        if after[key] != before[key]:
            checks_failed.append(failure)

    return MaintenanceExecutionResult(
        due_failed_retryable_plan_loaded=True,
        latest_failed_retryable_delivery_record_loaded=True,
        retry_ceiling_candidate_valid=True,
        max_attempts_exceeded=True,
        retry_intent_event_created=after["retry_intent_events"] > before["retry_intent_events"],
        dead_letter_created=after["dead_letters"] == 1,
        dead_letter_payload_matches_plan=payload_matches,
        dead_letter_dedupe_or_uniqueness_stable=dedupe_stable,
        maintenance_pipeline_run_recorded=after["maintenance_pipeline_runs"] == 1,
        maintenance_job_attempt_recorded=after["maintenance_job_attempts"] == 1,
        replay_request_created=after["replay_requests"] > before["replay_requests"] or after["replay_requests"] != 0,
        notification_plan_mutated_by_maintenance=after["notification_plan_digest"] != before["notification_plan_digest"],
        notification_delivery_record_mutated_by_maintenance=after["delivery_record_digest"] != before["delivery_record_digest"],
        analysis_mutated=after["analysis_digest"] != before["analysis_digest"],
        judge_output_mutated=after["judge_output_digest"] != before["judge_output_digest"],
        candidate_group_mutated=after["candidate_group_digest"] != before["candidate_group_digest"],
        evidence_bundle_mutated=after["evidence_bundle_digest"] != before["evidence_bundle_digest"],
        artifact_mutated=after["artifact_digest"] != before["artifact_digest"],
        source_message_mutated=after["source_message_digest"] != before["source_message_digest"],
        retry_intent_event_count_before=before["retry_intent_events"],
        retry_intent_event_count_after=after["retry_intent_events"],
        dead_letter_count_before=before["dead_letters"],
        dead_letter_count_after=after["dead_letters"],
        maintenance_pipeline_run_count_before=before["maintenance_pipeline_runs"],
        maintenance_pipeline_run_count_after=after["maintenance_pipeline_runs"],
        maintenance_job_attempt_count_before=before["maintenance_job_attempts"],
        maintenance_job_attempt_count_after=after["maintenance_job_attempts"],
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _capture_scope(
    connection: Any,
    *,
    plan: NotificationPlanRecord,
    latest: DeliveryRecord,
) -> dict[str, Any]:
    context = _load_candidate_scope_context(connection, candidate_group_id=plan.candidate_group_id)
    return {
        "notification_plan_digest": due_runner._notification_plan_digest(  # noqa: SLF001
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "delivery_record_digest": due_runner._delivery_record_digest(  # noqa: SLF001
            connection,
            notification_delivery_record_id=latest.notification_delivery_record_id,
        ),
        "analysis_digest": due_runner._analysis_digest(connection, analysis_id=plan.analysis_id),  # noqa: SLF001
        "judge_output_digest": due_runner._judge_output_digest_for_analysis(  # noqa: SLF001
            connection,
            analysis_id=plan.analysis_id,
        ),
        "candidate_group_digest": due_runner._candidate_group_digest(  # noqa: SLF001
            connection,
            candidate_group_id=plan.candidate_group_id,
        ),
        "evidence_bundle_digest": _evidence_bundle_digest(connection, bundle_id=context["current_bundle_id"]),
        "artifact_digest": _artifact_digest(connection, artifact_id=context["current_primary_artifact_id"]),
        "source_message_digest": _source_message_digest(connection, source_message_id=context["source_message_id"]),
        "retry_intent_events": _retry_intent_event_count(connection, notification_plan_id=plan.notification_plan_id),
        "replay_requests": due_runner._replay_request_count(  # noqa: SLF001
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
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


def _insert_or_reuse_dead_letter(connection: Any, *, notification_plan_id: UUID, retry_count: int) -> None:
    import sqlalchemy as sa

    existing = _dead_letter_count(connection, notification_plan_id=notification_plan_id)
    if existing > 1:
        raise ValueError("dead_letter_ambiguous")
    if existing == 1:
        return
    connection.execute(
        sa.text(
            """
            INSERT INTO dead_letter_entries (
                stage_name,
                queue_name,
                root_object_type,
                root_object_id,
                last_error_code,
                last_error_snippet,
                retry_count,
                first_failed_at,
                last_failed_at,
                next_manual_action,
                replay_hint
            ) VALUES (
                :stage_name,
                :queue_name,
                'notification_plan',
                CAST(:notification_plan_id AS uuid),
                :last_error_code,
                :last_error_snippet,
                :retry_count,
                now(),
                now(),
                :next_manual_action,
                :replay_hint
            )
            """
        ),
        {
            "stage_name": MAINTENANCE_STAGE_NAME,
            "queue_name": MAINTENANCE_QUEUE_NAME,
            "notification_plan_id": str(notification_plan_id),
            "last_error_code": DEAD_LETTER_ERROR_CODE,
            "last_error_snippet": DEAD_LETTER_LAST_ERROR_SNIPPET,
            "retry_count": retry_count,
            "next_manual_action": DEAD_LETTER_NEXT_MANUAL_ACTION,
            "replay_hint": DEAD_LETTER_REPLAY_HINT,
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
                'failed_terminal'::job_attempt_status_enum,
                :error_code,
                now()
            )
            """
        ),
        {
            "stage_name": MAINTENANCE_STAGE_NAME,
            "queue_name": MAINTENANCE_QUEUE_NAME,
            "notification_plan_id": str(notification_plan_id),
            "error_code": DEAD_LETTER_ERROR_CODE,
        },
    )


def _retry_intent_event_count(connection: Any, *, notification_plan_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = :event_type
                  AND payload_json ->> 'notification_plan_id' = :notification_plan_id
                  AND payload_json ->> 'retry_reason' = :retry_reason
                """
            ),
            {
                "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                "notification_plan_id": str(notification_plan_id),
                "retry_reason": RETRY_REASON,
            },
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
                WHERE stage_name = :stage_name
                  AND queue_name = :queue_name
                  AND root_object_type = 'notification_plan'
                  AND root_object_id = CAST(:notification_plan_id AS uuid)
                  AND last_error_code = :last_error_code
                  AND replay_hint = :replay_hint
                """
            ),
            {
                "stage_name": MAINTENANCE_STAGE_NAME,
                "queue_name": MAINTENANCE_QUEUE_NAME,
                "notification_plan_id": str(notification_plan_id),
                "last_error_code": DEAD_LETTER_ERROR_CODE,
                "replay_hint": DEAD_LETTER_REPLAY_HINT,
            },
        ).scalar_one()
    )


def _load_dead_letter(connection: Any, *, notification_plan_id: UUID) -> dict[str, Any] | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT stage_name,
                   queue_name,
                   root_object_type,
                   root_object_id::text AS root_object_id,
                   last_error_code,
                   last_error_snippet,
                   retry_count,
                   next_manual_action,
                   replay_hint
            FROM dead_letter_entries
            WHERE stage_name = :stage_name
              AND queue_name = :queue_name
              AND root_object_type = 'notification_plan'
              AND root_object_id = CAST(:notification_plan_id AS uuid)
              AND last_error_code = :last_error_code
              AND replay_hint = :replay_hint
            ORDER BY last_failed_at DESC NULLS LAST, dead_letter_entry_id DESC
            LIMIT 1
            """
        ),
        {
            "stage_name": MAINTENANCE_STAGE_NAME,
            "queue_name": MAINTENANCE_QUEUE_NAME,
            "notification_plan_id": str(notification_plan_id),
            "last_error_code": DEAD_LETTER_ERROR_CODE,
            "replay_hint": DEAD_LETTER_REPLAY_HINT,
        },
    ).mappings().first()
    return dict(row) if row is not None else None


def _dead_letter_payload_matches(
    row: Mapping[str, Any] | None,
    *,
    notification_plan_id: UUID,
    retry_count: int,
) -> bool:
    if row is None:
        return False
    return (
        row.get("stage_name") == MAINTENANCE_STAGE_NAME
        and row.get("queue_name") == MAINTENANCE_QUEUE_NAME
        and row.get("root_object_type") == "notification_plan"
        and row.get("root_object_id") == str(notification_plan_id)
        and row.get("last_error_code") == DEAD_LETTER_ERROR_CODE
        and row.get("last_error_snippet") == DEAD_LETTER_LAST_ERROR_SNIPPET
        and int(row.get("retry_count") or 0) == retry_count
        and row.get("next_manual_action") == DEAD_LETTER_NEXT_MANUAL_ACTION
        and row.get("replay_hint") == DEAD_LETTER_REPLAY_HINT
    )


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
                  AND attempt_status = 'failed_terminal'::job_attempt_status_enum
                  AND error_code = :error_code
                """
            ),
            {
                "stage_name": MAINTENANCE_STAGE_NAME,
                "queue_name": MAINTENANCE_QUEUE_NAME,
                "notification_plan_id": str(notification_plan_id),
                "error_code": DEAD_LETTER_ERROR_CODE,
            },
        ).scalar_one()
    )


def _load_candidate_scope_context(connection: Any, candidate_group_id: UUID) -> dict[str, UUID | None]:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT source_message_id, current_bundle_id, current_primary_artifact_id
            FROM candidate_group_proposals
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    ).mappings().first()
    if row is None:
        return {"source_message_id": None, "current_bundle_id": None, "current_primary_artifact_id": None}
    return {
        "source_message_id": _uuid_or_none(row["source_message_id"]),
        "current_bundle_id": _uuid_or_none(row["current_bundle_id"]),
        "current_primary_artifact_id": _uuid_or_none(row["current_primary_artifact_id"]),
    }


def _evidence_bundle_digest(connection: Any, *, bundle_id: UUID | None) -> str:
    if bundle_id is None:
        return ""
    import sqlalchemy as sa

    return str(
        connection.execute(
            sa.text(
                """
                SELECT COALESCE(md5(to_jsonb(b)::text), '')
                FROM candidate_evidence_bundles AS b
                WHERE b.bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": str(bundle_id)},
        ).scalar_one_or_none()
        or ""
    )


def _artifact_digest(connection: Any, *, artifact_id: UUID | None) -> str:
    if artifact_id is None:
        return ""
    import sqlalchemy as sa

    return str(
        connection.execute(
            sa.text(
                """
                SELECT COALESCE(md5(to_jsonb(ar)::text), '')
                FROM artifact_registry AS ar
                WHERE ar.artifact_id = CAST(:artifact_id AS uuid)
                """
            ),
            {"artifact_id": str(artifact_id)},
        ).scalar_one_or_none()
        or ""
    )


def _source_message_digest(connection: Any, *, source_message_id: UUID | None) -> str:
    if source_message_id is None:
        return ""
    import sqlalchemy as sa

    return str(
        connection.execute(
            sa.text(
                """
                SELECT COALESCE(md5(to_jsonb(sm)::text), '')
                FROM source_messages AS sm
                WHERE sm.source_message_id = CAST(:source_message_id AS uuid)
                """
            ),
            {"source_message_id": str(source_message_id)},
        ).scalar_one_or_none()
        or ""
    )


def _prepare_failed_retryable_fixture(
    *,
    database_url: str,
    notification_plan_id: UUID,
    max_attempts: int,
) -> bool:
    due_runner.render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001
    import sqlalchemy as sa

    response = {
        "local_fixture": True,
        "transport_skipped": True,
        "retryable": True,
        "reason_code": FIXTURE_TRANSPORT_ERROR_CODE,
        "retry_ceiling_fixture": True,
    }
    engine = sa.create_engine(database_url, future=True)
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
                        :attempt_count,
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
                    "attempt_count": max_attempts,
                    "transport_error_code": FIXTURE_TRANSPORT_ERROR_CODE,
                    "transport_error_class": FIXTURE_TRANSPORT_ERROR_CLASS,
                    "telegram_response_json": _json_dumps(response),
                },
            )
            return True
    finally:
        engine.dispose()


def _render_fixture_result_acceptable(predecessor: Any) -> bool:
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
        "retry_ceiling_candidate_valid": False,
        "max_attempts_exceeded": False,
        "retry_intent_event_created": False,
        "dead_letter_created": False,
        "dead_letter_payload_matches_plan": False,
        "dead_letter_dedupe_or_uniqueness_stable": False,
        "maintenance_pipeline_run_recorded": False,
        "maintenance_job_attempt_recorded": False,
        "replay_request_created": False,
        "notification_plan_mutated_by_maintenance": False,
        "notification_delivery_record_mutated_by_maintenance": False,
        "analysis_mutated": False,
        "judge_output_mutated": False,
        "candidate_group_mutated": False,
        "evidence_bundle_mutated": False,
        "artifact_mutated": False,
        "source_message_mutated": False,
        "openai_called": False,
        "telegram_called": False,
        "live_github_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "retry_intent_event_count_before": 0,
        "retry_intent_event_count_after": 0,
        "dead_letter_count_before": 0,
        "dead_letter_count_after": 0,
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
    retry_ceiling_candidate_valid: bool = False,
    max_attempts_exceeded: bool = False,
    retry_intent_event_created: bool = False,
    dead_letter_created: bool = False,
    dead_letter_payload_matches_plan: bool = False,
    dead_letter_dedupe_or_uniqueness_stable: bool = False,
    maintenance_pipeline_run_recorded: bool = False,
    maintenance_job_attempt_recorded: bool = False,
    replay_request_created: bool = False,
    notification_plan_mutated_by_maintenance: bool = False,
    notification_delivery_record_mutated_by_maintenance: bool = False,
    analysis_mutated: bool = False,
    judge_output_mutated: bool = False,
    candidate_group_mutated: bool = False,
    evidence_bundle_mutated: bool = False,
    artifact_mutated: bool = False,
    source_message_mutated: bool = False,
    retry_intent_event_count_before: int = 0,
    retry_intent_event_count_after: int = 0,
    dead_letter_count_before: int = 0,
    dead_letter_count_after: int = 0,
    maintenance_pipeline_run_count_before: int = 0,
    maintenance_pipeline_run_count_after: int = 0,
    maintenance_job_attempt_count_before: int = 0,
    maintenance_job_attempt_count_after: int = 0,
    checks_failed: Sequence[str],
) -> MaintenanceExecutionResult:
    return MaintenanceExecutionResult(
        due_failed_retryable_plan_loaded=due_failed_retryable_plan_loaded,
        latest_failed_retryable_delivery_record_loaded=latest_failed_retryable_delivery_record_loaded,
        retry_ceiling_candidate_valid=retry_ceiling_candidate_valid,
        max_attempts_exceeded=max_attempts_exceeded,
        retry_intent_event_created=retry_intent_event_created,
        dead_letter_created=dead_letter_created,
        dead_letter_payload_matches_plan=dead_letter_payload_matches_plan,
        dead_letter_dedupe_or_uniqueness_stable=dead_letter_dedupe_or_uniqueness_stable,
        maintenance_pipeline_run_recorded=maintenance_pipeline_run_recorded,
        maintenance_job_attempt_recorded=maintenance_job_attempt_recorded,
        replay_request_created=replay_request_created,
        notification_plan_mutated_by_maintenance=notification_plan_mutated_by_maintenance,
        notification_delivery_record_mutated_by_maintenance=notification_delivery_record_mutated_by_maintenance,
        analysis_mutated=analysis_mutated,
        judge_output_mutated=judge_output_mutated,
        candidate_group_mutated=candidate_group_mutated,
        evidence_bundle_mutated=evidence_bundle_mutated,
        artifact_mutated=artifact_mutated,
        source_message_mutated=source_message_mutated,
        retry_intent_event_count_before=retry_intent_event_count_before,
        retry_intent_event_count_after=retry_intent_event_count_after,
        dead_letter_count_before=dead_letter_count_before,
        dead_letter_count_after=dead_letter_count_after,
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


def _max_attempts_from_args_env(value: Any, env: Mapping[str, str]) -> int | None:
    raw = _string_or_none(value)
    if raw is None:
        raw = _string_or_none(env.get("NOTIFICATION_RETRY_MAX_ATTEMPTS"))
    if raw is None:
        raw = _string_or_none(env.get("DELIVERY_RETRY_MAX_ATTEMPTS"))
    if raw is None:
        raw = str(DEFAULT_MAX_ATTEMPTS)
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _json_dumps(value: Any) -> str:
    return due_runner._json_dumps(value)  # noqa: SLF001


def _uuid_or_none(value: Any) -> UUID | None:
    return due_runner._uuid_or_none(value)  # noqa: SLF001


def _path_or_none(value: Any) -> Path | None:
    return due_runner._path_or_none(value)  # noqa: SLF001


def _string_or_none(value: Any) -> str | None:
    return due_runner._string_or_none(value)  # noqa: SLF001


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc).strip()
    if message in SAFE_EXCEPTION_MESSAGES:
        return message
    return "maintenance_retry_ceiling_dead_letter_execution_failed"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main(argv: Sequence[str] | None = None) -> int:
    result = run(build_parser().parse_args(argv))
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
