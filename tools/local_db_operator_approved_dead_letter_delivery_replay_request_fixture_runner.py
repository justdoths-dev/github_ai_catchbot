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

from tools import local_db_maintenance_max_attempts_failed_retryable_dead_letter_fixture_runner as dlq_runner


SCHEMA_VERSION = "local_db_operator_approved_dead_letter_delivery_replay_request_fixture_runner_v1"
OPERATOR_APPROVAL_TOKEN = "operator-approved-delivery-replay-request"
REQUESTED_BY = "operator_approved_delivery_recovery"
REPLAY_REASON = "operator_approved_dead_letter_delivery_replay"
REPLAY_REQUESTED_EVENT_TYPE = "replay.requested.v1"
NOTIFICATION_PLAN_CREATED_EVENT_TYPE = dlq_runner.NOTIFICATION_PLAN_CREATED_EVENT_TYPE
DEAD_LETTER_REPLAY_HINT = dlq_runner.DEAD_LETTER_REPLAY_HINT
DEAD_LETTER_NEXT_MANUAL_ACTION = dlq_runner.DEAD_LETTER_NEXT_MANUAL_ACTION
MAINTENANCE_TRIGGER_SOURCE = "local_db_operator_approved_dead_letter_delivery_replay_request_fixture_runner"
MAINTENANCE_RUN_KIND = "local_test_operator_approved_delivery_replay_request_creation"
MAINTENANCE_STAGE_NAME = "maintenance_operator_approved_delivery_replay_request"
MAINTENANCE_QUEUE_NAME = dlq_runner.MAINTENANCE_QUEUE_NAME
MAINTENANCE_JOB_ERROR_CODE = "operator_approved_delivery_replay_request_created"
OPEN_REPLAY_REQUEST_STATUSES = ("pending", "dispatched", "completed")
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "dead_letter_loaded",
    "dead_letter_replay_hint_valid",
    "notification_plan_loaded",
    "delivery_replay_request_created",
    "delivery_replay_request_payload_matches_dead_letter",
    "replay_request_dedupe_or_uniqueness_stable",
    "replay_requested_event_created",
    "replay_requested_event_payload_matches_request",
    "replay_requested_event_dedupe_key_stable",
    "maintenance_pipeline_run_recorded",
    "maintenance_job_attempt_recorded",
)
FALSE_RESULT_KEYS = (
    "notification_plan_created_replay_intent_created",
    "notifier_render_created",
    "notification_delivery_record_created",
    "dead_letter_mutated",
    "notification_plan_mutated",
    "notification_render_mutated",
    "notification_delivery_record_mutated",
    "state_transition_mutated",
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
    "dead_letter_ambiguous",
    "dead_letter_missing_or_invalid",
    "dead_letter_replay_hint_invalid",
    "dead_letter_root_object_invalid",
    "dead_letter_next_manual_action_invalid",
    "fixture_notification_plan_ambiguous",
    "fixture_notification_plan_missing_or_invalid",
    "operator_approved_dead_letter_fixture_failed",
    "notification_plan_missing_or_invalid",
    "replay_request_ambiguous",
    "replay_request_missing_or_invalid",
    "replay_request_payload_mismatch",
    "replay_requested_event_ambiguous",
    "replay_requested_event_missing_or_invalid",
    "replay_requested_event_payload_mismatch",
    "maintenance_pipeline_run_ambiguous",
    "maintenance_job_attempt_ambiguous",
}

source_candidate_runner = dlq_runner.source_candidate_runner
github_snapshot_runner = dlq_runner.github_snapshot_runner
NotificationPlanRecord = dlq_runner.NotificationPlanRecord


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class FixtureResolutionResult:
    dead_letter_entry_id: UUID | None
    dead_letter_fixture_prepared: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DeadLetterRecord:
    dead_letter_entry_id: UUID
    stage_name: str
    queue_name: str
    root_object_type: str
    root_object_id: UUID | None
    retry_count: int
    next_manual_action: str | None
    replay_hint: str | None


@dataclass(frozen=True, slots=True)
class ReplayRequestRecord:
    replay_request_id: UUID
    replay_type: str
    root_object_type: str
    root_object_id: UUID
    requested_by: str | None
    status: str


@dataclass(frozen=True, slots=True)
class ReplayRequestExecutionResult:
    dead_letter_loaded: bool
    dead_letter_replay_hint_valid: bool
    notification_plan_loaded: bool
    delivery_replay_request_created: bool
    delivery_replay_request_payload_matches_dead_letter: bool
    replay_request_dedupe_or_uniqueness_stable: bool
    replay_requested_event_created: bool
    replay_requested_event_payload_matches_request: bool
    replay_requested_event_dedupe_key_stable: bool
    maintenance_pipeline_run_recorded: bool
    maintenance_job_attempt_recorded: bool
    notification_plan_created_replay_intent_created: bool = False
    notifier_render_created: bool = False
    notification_delivery_record_created: bool = False
    dead_letter_mutated: bool = False
    notification_plan_mutated: bool = False
    notification_render_mutated: bool = False
    notification_delivery_record_mutated: bool = False
    state_transition_mutated: bool = False
    analysis_mutated: bool = False
    judge_output_mutated: bool = False
    candidate_group_mutated: bool = False
    evidence_bundle_mutated: bool = False
    artifact_mutated: bool = False
    source_message_mutated: bool = False
    delivery_replay_request_count_before: int = 0
    delivery_replay_request_count_after: int = 0
    replay_requested_event_count_before: int = 0
    replay_requested_event_count_after: int = 0
    notification_plan_created_event_count_before: int = 0
    notification_plan_created_event_count_after: int = 0
    maintenance_pipeline_run_count_before: int = 0
    maintenance_pipeline_run_count_after: int = 0
    maintenance_job_attempt_count_before: int = 0
    maintenance_job_attempt_count_after: int = 0
    checks_failed: tuple[str, ...] = ()


class DeadLetterFixtureResolver(Protocol):
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


class ReplayRequestExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        dead_letter_entry_id: UUID | None,
        notification_plan_id: UUID | None,
    ) -> ReplayRequestExecutionResult: ...


class DefaultDeadLetterFixtureResolver:
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
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        predecessor = dlq_runner.run(
            argparse.Namespace(
                database_url=database_url,
                notification_plan_id=None,
                source_fixture=str(source_fixture_path),
                github_snapshot_fixture=str(github_snapshot_fixture_path),
                replay_namespace=replay_namespace,
                prepare_failed_retryable_fixture=True,
                max_attempts=str(max_attempts),
                confirm_local_test_db=True,
            ),
            env=predecessor_env,
            repo_root=repo_root,
        )
        if predecessor.exit_code != 0 or predecessor.report.get("status") != "pass":
            return FixtureResolutionResult(
                dead_letter_entry_id=None,
                dead_letter_fixture_prepared=False,
                checks_failed=("operator_approved_dead_letter_fixture_failed",),
            )

        plan_ids = dlq_runner.due_runner._find_fixture_notification_plan_ids_by_namespace(  # noqa: SLF001
            database_url=database_url,
            replay_namespace=replay_namespace,
        )
        if len(plan_ids) > 1:
            return FixtureResolutionResult(
                dead_letter_entry_id=None,
                dead_letter_fixture_prepared=True,
                checks_failed=("fixture_notification_plan_ambiguous",),
            )
        if len(plan_ids) != 1:
            return FixtureResolutionResult(
                dead_letter_entry_id=None,
                dead_letter_fixture_prepared=True,
                checks_failed=("fixture_notification_plan_missing_or_invalid",),
            )

        dead_letter_id, failures = _find_dead_letter_id_for_plan(
            database_url=database_url,
            notification_plan_id=plan_ids[0],
        )
        return FixtureResolutionResult(
            dead_letter_entry_id=dead_letter_id,
            dead_letter_fixture_prepared=True,
            checks_failed=tuple(failures),
        )


class SqlAlchemyReplayRequestExecutor:
    def execute(
        self,
        *,
        database_url: str,
        dead_letter_entry_id: UUID | None,
        notification_plan_id: UUID | None,
    ) -> ReplayRequestExecutionResult:
        dlq_runner.due_runner.render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                return _execute_replay_request_creation(
                    connection,
                    dead_letter_entry_id=dead_letter_entry_id,
                    notification_plan_id=notification_plan_id,
                )
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create a local/test DB operator-approved delivery replay request from one eligible "
            "notification_plan dead-letter row without running notifier replay dispatch."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--operator-approval")
    parser.add_argument("--dead-letter-entry-id")
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
    resolver: DeadLetterFixtureResolver | None = None,
    executor: ReplayRequestExecutor | None = None,
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

    operator_approval = _string_or_none(getattr(args, "operator_approval", None))
    if operator_approval == OPERATOR_APPROVAL_TOKEN:
        report["operator_approval_status"] = "approved"
    elif operator_approval is None:
        report["operator_approval_status"] = "missing"
        checks_failed.append("operator_approval_required")
    else:
        report["operator_approval_status"] = "rejected"
        checks_failed.append("operator_approval_invalid")

    max_attempts = _max_attempts_from_args_env(getattr(args, "max_attempts", None), effective_env)
    if max_attempts is None:
        checks_failed.append("max_attempts_invalid")
        max_attempts = dlq_runner.DEFAULT_MAX_ATTEMPTS

    raw_dead_letter_id = _string_or_none(getattr(args, "dead_letter_entry_id", None))
    raw_plan_id = _string_or_none(getattr(args, "notification_plan_id", None))
    dead_letter_id = _uuid_or_none(raw_dead_letter_id)
    plan_id = _uuid_or_none(raw_plan_id)
    if raw_dead_letter_id is not None and dead_letter_id is None:
        checks_failed.append("dead_letter_entry_id_invalid")
    if raw_plan_id is not None and plan_id is None:
        checks_failed.append("notification_plan_id_invalid")

    source_fixture = _path_or_none(getattr(args, "source_fixture", None))
    github_fixture = _path_or_none(getattr(args, "github_snapshot_fixture", None))
    replay_namespace = _string_or_none(getattr(args, "replay_namespace", None))
    prepare_fixture = bool(getattr(args, "prepare_failed_retryable_fixture", False))
    fixture_selector_supplied = (
        source_fixture is not None or github_fixture is not None or replay_namespace is not None or prepare_fixture
    )
    fixture_selector_complete = (
        source_fixture is not None and github_fixture is not None and replay_namespace is not None and prepare_fixture
    )

    selector_modes = int(raw_dead_letter_id is not None) + int(raw_plan_id is not None) + int(fixture_selector_supplied)
    if selector_modes > 1:
        checks_failed.append("selector_mode_ambiguous")
    elif selector_modes == 0:
        checks_failed.append("selector_mode_required")
    elif fixture_selector_supplied and not fixture_selector_complete:
        checks_failed.append("fixture_selector_incomplete")

    if replay_namespace is not None:
        namespace_ok, namespace_failures = source_candidate_runner.validate_replay_namespace(replay_namespace)
        checks_failed.extend(namespace_failures)
        if not namespace_ok:
            replay_namespace = None

    if fixture_selector_supplied and source_fixture is not None and github_fixture is not None:
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

    resolved_dead_letter_id = dead_letter_id
    resolved_plan_id = plan_id
    if fixture_selector_supplied:
        if source_fixture is None or github_fixture is None or replay_namespace is None:
            return _finish(report, ["fixture_selector_incomplete"])
        active_resolver = resolver or DefaultDeadLetterFixtureResolver()
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
        report["dead_letter_fixture_prepared"] = resolution.dead_letter_fixture_prepared
        checks_failed.extend(resolution.checks_failed)
        resolved_dead_letter_id = resolution.dead_letter_entry_id
        resolved_plan_id = None
        if resolved_dead_letter_id is None:
            checks_failed.append("dead_letter_missing_or_invalid")
        if checks_failed:
            return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyReplayRequestExecutor()
    try:
        execution = active_executor.execute(
            database_url=args.database_url,
            dead_letter_entry_id=resolved_dead_letter_id,
            notification_plan_id=resolved_plan_id,
        )
    except Exception as exc:  # noqa: BLE001 - sanitized report only.
        return _finish(report, [_safe_failure_code(exc)])

    report.update(
        {
            "dead_letter_loaded": execution.dead_letter_loaded,
            "dead_letter_replay_hint_valid": execution.dead_letter_replay_hint_valid,
            "notification_plan_loaded": execution.notification_plan_loaded,
            "delivery_replay_request_created": execution.delivery_replay_request_created,
            "delivery_replay_request_payload_matches_dead_letter": (
                execution.delivery_replay_request_payload_matches_dead_letter
            ),
            "replay_request_dedupe_or_uniqueness_stable": execution.replay_request_dedupe_or_uniqueness_stable,
            "replay_requested_event_created": execution.replay_requested_event_created,
            "replay_requested_event_payload_matches_request": execution.replay_requested_event_payload_matches_request,
            "replay_requested_event_dedupe_key_stable": execution.replay_requested_event_dedupe_key_stable,
            "maintenance_pipeline_run_recorded": execution.maintenance_pipeline_run_recorded,
            "maintenance_job_attempt_recorded": execution.maintenance_job_attempt_recorded,
            "notification_plan_created_replay_intent_created": (
                execution.notification_plan_created_replay_intent_created
            ),
            "notifier_render_created": execution.notifier_render_created,
            "notification_delivery_record_created": execution.notification_delivery_record_created,
            "dead_letter_mutated": execution.dead_letter_mutated,
            "notification_plan_mutated": execution.notification_plan_mutated,
            "notification_render_mutated": execution.notification_render_mutated,
            "notification_delivery_record_mutated": execution.notification_delivery_record_mutated,
            "state_transition_mutated": execution.state_transition_mutated,
            "analysis_mutated": execution.analysis_mutated,
            "judge_output_mutated": execution.judge_output_mutated,
            "candidate_group_mutated": execution.candidate_group_mutated,
            "evidence_bundle_mutated": execution.evidence_bundle_mutated,
            "artifact_mutated": execution.artifact_mutated,
            "source_message_mutated": execution.source_message_mutated,
            "delivery_replay_request_count_before": execution.delivery_replay_request_count_before,
            "delivery_replay_request_count_after": execution.delivery_replay_request_count_after,
            "replay_requested_event_count_before": execution.replay_requested_event_count_before,
            "replay_requested_event_count_after": execution.replay_requested_event_count_after,
            "notification_plan_created_event_count_before": execution.notification_plan_created_event_count_before,
            "notification_plan_created_event_count_after": execution.notification_plan_created_event_count_after,
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
    return dlq_runner.validate_database_url(database_url)


def build_replay_requested_payload(
    *,
    replay_request_id: UUID,
    notification_plan_id: UUID,
    dead_letter_entry_id: UUID,
    operator_approval: str = OPERATOR_APPROVAL_TOKEN,
) -> dict[str, str]:
    return {
        "replay_request_id": str(replay_request_id),
        "replay_type": "delivery",
        "root_object_type": "notification_plan",
        "root_object_id": str(notification_plan_id),
        "operator_approval": operator_approval,
        "replay_reason": REPLAY_REASON,
        "source_dead_letter_entry_id": str(dead_letter_entry_id),
    }


def build_replay_requested_event_dedupe_key(replay_request_id: UUID) -> str:
    return f"notify:operator-approved-delivery-replay-request:{replay_request_id}"


def _find_dead_letter_id_for_plan(
    *,
    database_url: str,
    notification_plan_id: UUID,
) -> tuple[UUID | None, tuple[str, ...]]:
    dlq_runner.due_runner.render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001
    import sqlalchemy as sa

    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.begin() as connection:
            rows = _load_eligible_dead_letters_by_plan(connection, notification_plan_id=notification_plan_id)
            if len(rows) > 1:
                return None, ("dead_letter_ambiguous",)
            if not rows:
                return None, ("dead_letter_missing_or_invalid",)
            return rows[0].dead_letter_entry_id, ()
    finally:
        engine.dispose()


def _execute_replay_request_creation(
    connection: Any,
    *,
    dead_letter_entry_id: UUID | None,
    notification_plan_id: UUID | None,
) -> ReplayRequestExecutionResult:
    if dead_letter_entry_id is not None:
        dead_letter = _load_dead_letter_by_id(connection, dead_letter_entry_id=dead_letter_entry_id)
    elif notification_plan_id is not None:
        rows = _load_eligible_dead_letters_by_plan(connection, notification_plan_id=notification_plan_id)
        if len(rows) > 1:
            return _execution_result(dead_letter_loaded=False, checks_failed=("dead_letter_ambiguous",))
        dead_letter = rows[0] if rows else None
    else:
        return _execution_result(dead_letter_loaded=False, checks_failed=("selector_mode_required",))

    if dead_letter is None:
        return _execution_result(dead_letter_loaded=False, checks_failed=("dead_letter_missing_or_invalid",))

    hint_valid = dead_letter.replay_hint == DEAD_LETTER_REPLAY_HINT
    if not hint_valid:
        return _execution_result(
            dead_letter_loaded=True,
            dead_letter_replay_hint_valid=False,
            checks_failed=("dead_letter_replay_hint_invalid",),
        )
    if dead_letter.root_object_type != "notification_plan" or dead_letter.root_object_id is None:
        return _execution_result(
            dead_letter_loaded=True,
            dead_letter_replay_hint_valid=True,
            checks_failed=("dead_letter_root_object_invalid",),
        )
    if not _next_manual_action_allows_delivery_replay(dead_letter.next_manual_action):
        return _execution_result(
            dead_letter_loaded=True,
            dead_letter_replay_hint_valid=True,
            checks_failed=("dead_letter_next_manual_action_invalid",),
        )

    plan = dlq_runner.due_runner._load_notification_plan(  # noqa: SLF001
        connection,
        dead_letter.root_object_id,
    )
    if plan is None:
        return _execution_result(
            dead_letter_loaded=True,
            dead_letter_replay_hint_valid=True,
            notification_plan_loaded=False,
            checks_failed=("notification_plan_missing_or_invalid",),
        )

    existing_requests = _load_matching_delivery_replay_requests(
        connection,
        notification_plan_id=plan.notification_plan_id,
    )
    if len(existing_requests) > 1:
        return _execution_result(
            dead_letter_loaded=True,
            dead_letter_replay_hint_valid=True,
            notification_plan_loaded=True,
            checks_failed=("replay_request_ambiguous",),
        )
    before_request_id = existing_requests[0].replay_request_id if existing_requests else None
    before = _capture_scope(
        connection,
        plan=plan,
        dead_letter=dead_letter,
        replay_request_id=before_request_id,
    )

    replay_request = existing_requests[0] if existing_requests else _insert_delivery_replay_request(connection, plan=plan)
    _insert_or_reuse_replay_requested_event(connection, replay_request=replay_request, dead_letter=dead_letter)
    _insert_or_reuse_pipeline_run(connection, replay_request_id=replay_request.replay_request_id)
    _insert_or_reuse_job_attempt(connection, replay_request_id=replay_request.replay_request_id)

    after = _capture_scope(
        connection,
        plan=plan,
        dead_letter=dead_letter,
        replay_request_id=replay_request.replay_request_id,
    )
    loaded_request = _load_replay_request_by_id(connection, replay_request_id=replay_request.replay_request_id)
    event = _load_replay_requested_event(
        connection,
        replay_request_id=replay_request.replay_request_id,
    )

    replay_request_matches = _replay_request_matches_dead_letter(loaded_request, dead_letter=dead_letter)
    event_matches = _replay_requested_event_matches(
        event,
        replay_request=replay_request,
        dead_letter=dead_letter,
    )
    request_dedupe_stable = (
        before["delivery_replay_requests"] <= 1
        and after["delivery_replay_requests"] == 1
        and after["delivery_replay_requests"] - before["delivery_replay_requests"] in {0, 1}
    )
    event_dedupe_stable = (
        before["replay_requested_events"] <= 1
        and after["replay_requested_events"] == 1
        and after["replay_requested_events"] - before["replay_requested_events"] in {0, 1}
    )

    checks_failed: list[str] = []
    if not replay_request_matches:
        checks_failed.append("replay_request_payload_mismatch")
    if not request_dedupe_stable:
        checks_failed.append("replay_request_dedupe_or_uniqueness_unstable")
    if after["replay_requested_events"] != 1:
        checks_failed.append("replay_requested_event_missing_or_invalid")
    if not event_matches:
        checks_failed.append("replay_requested_event_payload_mismatch")
    if not event_dedupe_stable:
        checks_failed.append("replay_requested_event_dedupe_key_unstable")
    if after["maintenance_pipeline_runs"] != 1:
        checks_failed.append("maintenance_pipeline_run_recorded:missing")
    if after["maintenance_job_attempts"] != 1:
        checks_failed.append("maintenance_job_attempt_recorded:missing")
    for key, failure in (
        ("dead_letter_digest", "dead_letter_mutated"),
        ("notification_plan_digest", "notification_plan_mutated"),
        ("notification_render_digest", "notification_render_mutated"),
        ("notification_delivery_record_digest", "notification_delivery_record_mutated"),
        ("state_transition_digest", "state_transition_mutated"),
        ("analysis_digest", "analysis_mutated"),
        ("judge_output_digest", "judge_output_mutated"),
        ("candidate_group_digest", "candidate_group_mutated"),
        ("evidence_bundle_digest", "evidence_bundle_mutated"),
        ("artifact_digest", "artifact_mutated"),
        ("source_message_digest", "source_message_mutated"),
    ):
        if after[key] != before[key]:
            checks_failed.append(failure)
    if after["notification_plan_created_events"] > before["notification_plan_created_events"]:
        checks_failed.append("notification_plan_created_replay_intent_created")
    if after["notification_render_count"] > before["notification_render_count"]:
        checks_failed.append("notifier_render_created")
    if after["notification_delivery_record_count"] > before["notification_delivery_record_count"]:
        checks_failed.append("notification_delivery_record_created")

    return ReplayRequestExecutionResult(
        dead_letter_loaded=True,
        dead_letter_replay_hint_valid=True,
        notification_plan_loaded=True,
        delivery_replay_request_created=after["delivery_replay_requests"] == 1,
        delivery_replay_request_payload_matches_dead_letter=replay_request_matches,
        replay_request_dedupe_or_uniqueness_stable=request_dedupe_stable,
        replay_requested_event_created=after["replay_requested_events"] == 1,
        replay_requested_event_payload_matches_request=event_matches,
        replay_requested_event_dedupe_key_stable=event_dedupe_stable,
        maintenance_pipeline_run_recorded=after["maintenance_pipeline_runs"] == 1,
        maintenance_job_attempt_recorded=after["maintenance_job_attempts"] == 1,
        notification_plan_created_replay_intent_created=(
            after["notification_plan_created_events"] > before["notification_plan_created_events"]
        ),
        notifier_render_created=after["notification_render_count"] > before["notification_render_count"],
        notification_delivery_record_created=(
            after["notification_delivery_record_count"] > before["notification_delivery_record_count"]
        ),
        dead_letter_mutated=after["dead_letter_digest"] != before["dead_letter_digest"],
        notification_plan_mutated=after["notification_plan_digest"] != before["notification_plan_digest"],
        notification_render_mutated=after["notification_render_digest"] != before["notification_render_digest"],
        notification_delivery_record_mutated=(
            after["notification_delivery_record_digest"] != before["notification_delivery_record_digest"]
        ),
        state_transition_mutated=after["state_transition_digest"] != before["state_transition_digest"],
        analysis_mutated=after["analysis_digest"] != before["analysis_digest"],
        judge_output_mutated=after["judge_output_digest"] != before["judge_output_digest"],
        candidate_group_mutated=after["candidate_group_digest"] != before["candidate_group_digest"],
        evidence_bundle_mutated=after["evidence_bundle_digest"] != before["evidence_bundle_digest"],
        artifact_mutated=after["artifact_digest"] != before["artifact_digest"],
        source_message_mutated=after["source_message_digest"] != before["source_message_digest"],
        delivery_replay_request_count_before=before["delivery_replay_requests"],
        delivery_replay_request_count_after=after["delivery_replay_requests"],
        replay_requested_event_count_before=before["replay_requested_events"],
        replay_requested_event_count_after=after["replay_requested_events"],
        notification_plan_created_event_count_before=before["notification_plan_created_events"],
        notification_plan_created_event_count_after=after["notification_plan_created_events"],
        maintenance_pipeline_run_count_before=before["maintenance_pipeline_runs"],
        maintenance_pipeline_run_count_after=after["maintenance_pipeline_runs"],
        maintenance_job_attempt_count_before=before["maintenance_job_attempts"],
        maintenance_job_attempt_count_after=after["maintenance_job_attempts"],
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _load_dead_letter_by_id(connection: Any, *, dead_letter_entry_id: UUID) -> DeadLetterRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT dead_letter_entry_id,
                   stage_name,
                   queue_name,
                   root_object_type,
                   root_object_id,
                   retry_count,
                   next_manual_action,
                   replay_hint
            FROM dead_letter_entries
            WHERE dead_letter_entry_id = CAST(:dead_letter_entry_id AS uuid)
            """
        ),
        {"dead_letter_entry_id": str(dead_letter_entry_id)},
    ).mappings().first()
    return _dead_letter_from_row(row) if row is not None else None


def _load_eligible_dead_letters_by_plan(connection: Any, *, notification_plan_id: UUID) -> list[DeadLetterRecord]:
    import sqlalchemy as sa

    rows = connection.execute(
        sa.text(
            """
            SELECT dead_letter_entry_id,
                   stage_name,
                   queue_name,
                   root_object_type,
                   root_object_id,
                   retry_count,
                   next_manual_action,
                   replay_hint
            FROM dead_letter_entries
            WHERE root_object_type = 'notification_plan'
              AND root_object_id = CAST(:notification_plan_id AS uuid)
              AND replay_hint = :replay_hint
              AND next_manual_action = :next_manual_action
            ORDER BY last_failed_at DESC NULLS LAST, dead_letter_entry_id DESC
            """
        ),
        {
            "notification_plan_id": str(notification_plan_id),
            "replay_hint": DEAD_LETTER_REPLAY_HINT,
            "next_manual_action": DEAD_LETTER_NEXT_MANUAL_ACTION,
        },
    ).mappings().all()
    return [_dead_letter_from_row(row) for row in rows]


def _dead_letter_from_row(row: Mapping[str, Any]) -> DeadLetterRecord:
    return DeadLetterRecord(
        dead_letter_entry_id=UUID(str(row["dead_letter_entry_id"])),
        stage_name=str(row["stage_name"]),
        queue_name=str(row["queue_name"]),
        root_object_type=str(row["root_object_type"]),
        root_object_id=_uuid_or_none(row["root_object_id"]),
        retry_count=int(row["retry_count"] or 0),
        next_manual_action=_string_or_none(row["next_manual_action"]),
        replay_hint=_string_or_none(row["replay_hint"]),
    )


def _load_matching_delivery_replay_requests(
    connection: Any,
    *,
    notification_plan_id: UUID,
) -> list[ReplayRequestRecord]:
    import sqlalchemy as sa

    rows = connection.execute(
        sa.text(
            """
            SELECT replay_request_id,
                   replay_type::text AS replay_type,
                   root_object_type,
                   root_object_id,
                   requested_by,
                   status
            FROM replay_requests
            WHERE replay_type = 'delivery'::replay_type_enum
              AND root_object_type = 'notification_plan'
              AND root_object_id = CAST(:notification_plan_id AS uuid)
              AND requested_by = :requested_by
              AND status = ANY(:statuses)
            ORDER BY requested_at ASC, replay_request_id ASC
            """
        ),
        {
            "notification_plan_id": str(notification_plan_id),
            "requested_by": REQUESTED_BY,
            "statuses": list(OPEN_REPLAY_REQUEST_STATUSES),
        },
    ).mappings().all()
    return [_replay_request_from_row(row) for row in rows]


def _load_replay_request_by_id(connection: Any, *, replay_request_id: UUID) -> ReplayRequestRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT replay_request_id,
                   replay_type::text AS replay_type,
                   root_object_type,
                   root_object_id,
                   requested_by,
                   status
            FROM replay_requests
            WHERE replay_request_id = CAST(:replay_request_id AS uuid)
            """
        ),
        {"replay_request_id": str(replay_request_id)},
    ).mappings().first()
    return _replay_request_from_row(row) if row is not None else None


def _replay_request_from_row(row: Mapping[str, Any]) -> ReplayRequestRecord:
    return ReplayRequestRecord(
        replay_request_id=UUID(str(row["replay_request_id"])),
        replay_type=str(row["replay_type"]),
        root_object_type=str(row["root_object_type"]),
        root_object_id=UUID(str(row["root_object_id"])),
        requested_by=_string_or_none(row["requested_by"]),
        status=str(row["status"]),
    )


def _insert_delivery_replay_request(connection: Any, *, plan: NotificationPlanRecord) -> ReplayRequestRecord:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            INSERT INTO replay_requests (
                replay_type,
                root_object_type,
                root_object_id,
                requested_by,
                requested_at,
                status
            ) VALUES (
                'delivery'::replay_type_enum,
                'notification_plan',
                CAST(:notification_plan_id AS uuid),
                :requested_by,
                now(),
                'pending'
            )
            RETURNING replay_request_id,
                      replay_type::text AS replay_type,
                      root_object_type,
                      root_object_id,
                      requested_by,
                      status
            """
        ),
        {"notification_plan_id": str(plan.notification_plan_id), "requested_by": REQUESTED_BY},
    ).mappings().first()
    if row is None:
        raise ValueError("replay_request_missing_or_invalid")
    return _replay_request_from_row(row)


def _insert_or_reuse_replay_requested_event(
    connection: Any,
    *,
    replay_request: ReplayRequestRecord,
    dead_letter: DeadLetterRecord,
) -> None:
    import sqlalchemy as sa

    payload = build_replay_requested_payload(
        replay_request_id=replay_request.replay_request_id,
        notification_plan_id=replay_request.root_object_id,
        dead_letter_entry_id=dead_letter.dead_letter_entry_id,
    )
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
                'replay_request',
                CAST(:replay_request_id AS uuid),
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
            "event_type": REPLAY_REQUESTED_EVENT_TYPE,
            "replay_request_id": str(replay_request.replay_request_id),
            "dedupe_key": build_replay_requested_event_dedupe_key(replay_request.replay_request_id),
            "payload_json": _json_dumps(payload),
        },
    )


def _load_replay_requested_event(connection: Any, *, replay_request_id: UUID) -> dict[str, Any] | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT event_type,
                   aggregate_type,
                   aggregate_id,
                   dedupe_key,
                   payload_json
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_type = 'replay_request'
              AND aggregate_id = CAST(:replay_request_id AS uuid)
              AND dedupe_key = :dedupe_key
            """
        ),
        {
            "event_type": REPLAY_REQUESTED_EVENT_TYPE,
            "replay_request_id": str(replay_request_id),
            "dedupe_key": build_replay_requested_event_dedupe_key(replay_request_id),
        },
    ).mappings().first()
    if row is None:
        return None
    payload = _json_loads(row["payload_json"])
    return {
        "event_type": str(row["event_type"]),
        "aggregate_type": str(row["aggregate_type"]),
        "aggregate_id": str(row["aggregate_id"]),
        "dedupe_key": str(row["dedupe_key"]),
        "payload_json": payload if isinstance(payload, dict) else {},
    }


def _insert_or_reuse_pipeline_run(connection: Any, *, replay_request_id: UUID) -> None:
    import sqlalchemy as sa

    existing = _maintenance_pipeline_run_count(connection, replay_request_id=replay_request_id)
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
                'replay_request',
                CAST(:replay_request_id AS uuid),
                now(),
                now(),
                'succeeded'
            )
            """
        ),
        {
            "trigger_source": MAINTENANCE_TRIGGER_SOURCE,
            "run_kind": MAINTENANCE_RUN_KIND,
            "replay_request_id": str(replay_request_id),
        },
    )


def _insert_or_reuse_job_attempt(connection: Any, *, replay_request_id: UUID) -> None:
    import sqlalchemy as sa

    existing = _maintenance_job_attempt_count(connection, replay_request_id=replay_request_id)
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
                'replay_request',
                CAST(:replay_request_id AS uuid),
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
            "replay_request_id": str(replay_request_id),
            "error_code": MAINTENANCE_JOB_ERROR_CODE,
        },
    )


def _capture_scope(
    connection: Any,
    *,
    plan: NotificationPlanRecord,
    dead_letter: DeadLetterRecord,
    replay_request_id: UUID | None,
) -> dict[str, Any]:
    context = dlq_runner._load_candidate_scope_context(connection, candidate_group_id=plan.candidate_group_id)  # noqa: SLF001
    return {
        "dead_letter_digest": _dead_letter_digest(
            connection,
            dead_letter_entry_id=dead_letter.dead_letter_entry_id,
        ),
        "notification_plan_digest": dlq_runner.due_runner._notification_plan_digest(  # noqa: SLF001
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "notification_render_digest": _notification_render_digest(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "notification_delivery_record_digest": _notification_delivery_record_digest(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "state_transition_digest": _state_transition_digest(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "analysis_digest": dlq_runner.due_runner._analysis_digest(connection, analysis_id=plan.analysis_id),  # noqa: SLF001
        "judge_output_digest": dlq_runner.due_runner._judge_output_digest_for_analysis(  # noqa: SLF001
            connection,
            analysis_id=plan.analysis_id,
        ),
        "candidate_group_digest": dlq_runner.due_runner._candidate_group_digest(  # noqa: SLF001
            connection,
            candidate_group_id=plan.candidate_group_id,
        ),
        "evidence_bundle_digest": dlq_runner._evidence_bundle_digest(  # noqa: SLF001
            connection,
            bundle_id=context["current_bundle_id"],
        ),
        "artifact_digest": dlq_runner._artifact_digest(  # noqa: SLF001
            connection,
            artifact_id=context["current_primary_artifact_id"],
        ),
        "source_message_digest": dlq_runner._source_message_digest(  # noqa: SLF001
            connection,
            source_message_id=context["source_message_id"],
        ),
        "delivery_replay_requests": _delivery_replay_request_count(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "replay_requested_events": _replay_requested_event_count(
            connection,
            replay_request_id=replay_request_id,
        ),
        "notification_plan_created_events": _notification_plan_created_replay_intent_count(
            connection,
            replay_request_id=replay_request_id,
        ),
        "notification_render_count": _notification_render_count(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "notification_delivery_record_count": _notification_delivery_record_count(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "maintenance_pipeline_runs": _maintenance_pipeline_run_count(
            connection,
            replay_request_id=replay_request_id,
        ),
        "maintenance_job_attempts": _maintenance_job_attempt_count(
            connection,
            replay_request_id=replay_request_id,
        ),
    }


def _dead_letter_digest(connection: Any, *, dead_letter_entry_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT dead_letter_entry_id::text AS dead_letter_entry_id,
                   stage_name,
                   queue_name,
                   root_object_type,
                   root_object_id::text AS root_object_id,
                   last_error_code,
                   last_error_snippet,
                   retry_count,
                   first_failed_at::text AS first_failed_at,
                   last_failed_at::text AS last_failed_at,
                   next_manual_action,
                   replay_hint
            FROM dead_letter_entries
            WHERE dead_letter_entry_id = CAST(:dead_letter_entry_id AS uuid)
            """
        ),
        {"dead_letter_entry_id": str(dead_letter_entry_id)},
    ).mappings().first()
    return _stable_digest(dict(row) if row is not None else None)


def _notification_render_digest(connection: Any, *, notification_plan_id: UUID) -> str:
    import sqlalchemy as sa

    return str(
        connection.execute(
            sa.text(
                """
                SELECT COALESCE(md5(COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.notification_render_id), '[]'::jsonb)::text), '')
                FROM (
                    SELECT notification_render_id::text AS notification_render_id,
                           notification_plan_id::text AS notification_plan_id,
                           render_hash,
                           created_at::text AS created_at
                    FROM notification_renders
                    WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                ) AS r
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        ).scalar_one()
        or ""
    )


def _notification_delivery_record_digest(connection: Any, *, notification_plan_id: UUID) -> str:
    import sqlalchemy as sa

    return str(
        connection.execute(
            sa.text(
                """
                SELECT COALESCE(md5(COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.notification_delivery_record_id), '[]'::jsonb)::text), '')
                FROM (
                    SELECT notification_delivery_record_id::text AS notification_delivery_record_id,
                           notification_plan_id::text AS notification_plan_id,
                           delivery_status::text AS delivery_status,
                           attempt_count,
                           transport_error_code,
                           transport_error_class,
                           created_at::text AS created_at
                    FROM notification_delivery_records
                    WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                ) AS r
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        ).scalar_one()
        or ""
    )


def _state_transition_digest(connection: Any, *, notification_plan_id: UUID) -> str:
    import sqlalchemy as sa

    return str(
        connection.execute(
            sa.text(
                """
                SELECT COALESCE(md5(COALESCE(jsonb_agg(to_jsonb(st) ORDER BY st.state_transition_id), '[]'::jsonb)::text), '')
                FROM (
                    SELECT state_transition_id::text AS state_transition_id,
                           object_type,
                           object_id::text AS object_id,
                           from_state,
                           to_state,
                           reason_code,
                           created_at::text AS created_at
                    FROM state_transitions
                    WHERE object_type = 'notification_plan'
                      AND object_id = CAST(:notification_plan_id AS uuid)
                ) AS st
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        ).scalar_one()
        or ""
    )


def _delivery_replay_request_count(connection: Any, *, notification_plan_id: UUID) -> int:
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
                  AND requested_by = :requested_by
                  AND status = ANY(:statuses)
                """
            ),
            {
                "notification_plan_id": str(notification_plan_id),
                "requested_by": REQUESTED_BY,
                "statuses": list(OPEN_REPLAY_REQUEST_STATUSES),
            },
        ).scalar_one()
    )


def _replay_requested_event_count(connection: Any, *, replay_request_id: UUID | None) -> int:
    if replay_request_id is None:
        return 0
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = :event_type
                  AND aggregate_type = 'replay_request'
                  AND aggregate_id = CAST(:replay_request_id AS uuid)
                  AND dedupe_key = :dedupe_key
                """
            ),
            {
                "event_type": REPLAY_REQUESTED_EVENT_TYPE,
                "replay_request_id": str(replay_request_id),
                "dedupe_key": build_replay_requested_event_dedupe_key(replay_request_id),
            },
        ).scalar_one()
    )


def _notification_plan_created_replay_intent_count(connection: Any, *, replay_request_id: UUID | None) -> int:
    if replay_request_id is None:
        return 0
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = :event_type
                  AND payload_json ->> 'replay_request_id' = :replay_request_id
                """
            ),
            {
                "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                "replay_request_id": str(replay_request_id),
            },
        ).scalar_one()
    )


def _notification_render_count(connection: Any, *, notification_plan_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_renders
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        ).scalar_one()
    )


def _notification_delivery_record_count(connection: Any, *, notification_plan_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_delivery_records
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        ).scalar_one()
    )


def _maintenance_pipeline_run_count(connection: Any, *, replay_request_id: UUID | None) -> int:
    if replay_request_id is None:
        return 0
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM pipeline_runs
                WHERE trigger_source = :trigger_source
                  AND run_kind = :run_kind
                  AND root_object_type = 'replay_request'
                  AND root_object_id = CAST(:replay_request_id AS uuid)
                  AND terminal_status = 'succeeded'
                """
            ),
            {
                "trigger_source": MAINTENANCE_TRIGGER_SOURCE,
                "run_kind": MAINTENANCE_RUN_KIND,
                "replay_request_id": str(replay_request_id),
            },
        ).scalar_one()
    )


def _maintenance_job_attempt_count(connection: Any, *, replay_request_id: UUID | None) -> int:
    if replay_request_id is None:
        return 0
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM job_attempts
                WHERE stage_name = :stage_name
                  AND queue_name = :queue_name
                  AND root_object_type = 'replay_request'
                  AND root_object_id = CAST(:replay_request_id AS uuid)
                  AND attempt_status = 'succeeded'::job_attempt_status_enum
                  AND error_code = :error_code
                """
            ),
            {
                "stage_name": MAINTENANCE_STAGE_NAME,
                "queue_name": MAINTENANCE_QUEUE_NAME,
                "replay_request_id": str(replay_request_id),
                "error_code": MAINTENANCE_JOB_ERROR_CODE,
            },
        ).scalar_one()
    )


def _replay_request_matches_dead_letter(
    replay_request: ReplayRequestRecord | None,
    *,
    dead_letter: DeadLetterRecord,
) -> bool:
    return (
        replay_request is not None
        and dead_letter.root_object_id is not None
        and replay_request.replay_type == "delivery"
        and replay_request.root_object_type == "notification_plan"
        and replay_request.root_object_id == dead_letter.root_object_id
        and replay_request.requested_by == REQUESTED_BY
        and replay_request.status in OPEN_REPLAY_REQUEST_STATUSES
    )


def _replay_requested_event_matches(
    event: Mapping[str, Any] | None,
    *,
    replay_request: ReplayRequestRecord,
    dead_letter: DeadLetterRecord,
) -> bool:
    if event is None:
        return False
    expected_payload = build_replay_requested_payload(
        replay_request_id=replay_request.replay_request_id,
        notification_plan_id=replay_request.root_object_id,
        dead_letter_entry_id=dead_letter.dead_letter_entry_id,
    )
    return (
        event.get("event_type") == REPLAY_REQUESTED_EVENT_TYPE
        and event.get("aggregate_type") == "replay_request"
        and event.get("aggregate_id") == str(replay_request.replay_request_id)
        and event.get("dedupe_key") == build_replay_requested_event_dedupe_key(replay_request.replay_request_id)
        and event.get("payload_json") == expected_payload
    )


def _next_manual_action_allows_delivery_replay(value: str | None) -> bool:
    text = (value or "").strip().lower()
    return "delivery" in text and ("replay" in text or "manual" in text or "recovery" in text)


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "operator_approval_status": "missing",
        "dead_letter_fixture_prepared": False,
        "dead_letter_loaded": False,
        "dead_letter_replay_hint_valid": False,
        "notification_plan_loaded": False,
        "delivery_replay_request_created": False,
        "delivery_replay_request_payload_matches_dead_letter": False,
        "replay_request_dedupe_or_uniqueness_stable": False,
        "replay_requested_event_created": False,
        "replay_requested_event_payload_matches_request": False,
        "replay_requested_event_dedupe_key_stable": False,
        "maintenance_pipeline_run_recorded": False,
        "maintenance_job_attempt_recorded": False,
        "notification_plan_created_replay_intent_created": False,
        "notifier_render_created": False,
        "notification_delivery_record_created": False,
        "dead_letter_mutated": False,
        "notification_plan_mutated": False,
        "notification_render_mutated": False,
        "notification_delivery_record_mutated": False,
        "state_transition_mutated": False,
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
        "delivery_replay_request_count_before": 0,
        "delivery_replay_request_count_after": 0,
        "replay_requested_event_count_before": 0,
        "replay_requested_event_count_after": 0,
        "notification_plan_created_event_count_before": 0,
        "notification_plan_created_event_count_after": 0,
        "maintenance_pipeline_run_count_before": 0,
        "maintenance_pipeline_run_count_after": 0,
        "maintenance_job_attempt_count_before": 0,
        "maintenance_job_attempt_count_after": 0,
        "checks_failed": [],
    }


def _execution_result(
    *,
    dead_letter_loaded: bool = False,
    dead_letter_replay_hint_valid: bool = False,
    notification_plan_loaded: bool = False,
    delivery_replay_request_created: bool = False,
    delivery_replay_request_payload_matches_dead_letter: bool = False,
    replay_request_dedupe_or_uniqueness_stable: bool = False,
    replay_requested_event_created: bool = False,
    replay_requested_event_payload_matches_request: bool = False,
    replay_requested_event_dedupe_key_stable: bool = False,
    maintenance_pipeline_run_recorded: bool = False,
    maintenance_job_attempt_recorded: bool = False,
    checks_failed: Sequence[str],
) -> ReplayRequestExecutionResult:
    return ReplayRequestExecutionResult(
        dead_letter_loaded=dead_letter_loaded,
        dead_letter_replay_hint_valid=dead_letter_replay_hint_valid,
        notification_plan_loaded=notification_plan_loaded,
        delivery_replay_request_created=delivery_replay_request_created,
        delivery_replay_request_payload_matches_dead_letter=delivery_replay_request_payload_matches_dead_letter,
        replay_request_dedupe_or_uniqueness_stable=replay_request_dedupe_or_uniqueness_stable,
        replay_requested_event_created=replay_requested_event_created,
        replay_requested_event_payload_matches_request=replay_requested_event_payload_matches_request,
        replay_requested_event_dedupe_key_stable=replay_requested_event_dedupe_key_stable,
        maintenance_pipeline_run_recorded=maintenance_pipeline_run_recorded,
        maintenance_job_attempt_recorded=maintenance_job_attempt_recorded,
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
        raw = str(dlq_runner.DEFAULT_MAX_ATTEMPTS)
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _json_loads(value: Any) -> Any:
    return dlq_runner.due_runner._json_loads(value)  # noqa: SLF001


def _json_dumps(value: Any) -> str:
    return dlq_runner._json_dumps(value)  # noqa: SLF001


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
    return dlq_runner._uuid_or_none(value)  # noqa: SLF001


def _path_or_none(value: Any) -> Path | None:
    return dlq_runner._path_or_none(value)  # noqa: SLF001


def _string_or_none(value: Any) -> str | None:
    return dlq_runner._string_or_none(value)  # noqa: SLF001


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc).strip()
    if message in SAFE_EXCEPTION_MESSAGES:
        return message
    return "operator_approved_delivery_replay_request_execution_failed"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main(argv: Sequence[str] | None = None) -> int:
    result = run(build_parser().parse_args(argv))
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
