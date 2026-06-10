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
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))

from src.services.maintenance.delivery_replay import (
    REPLAY_INTENT_EVENT_TYPE as NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
)
from src.services.maintenance.delivery_replay import build_replay_intent_payload, replay_intent_dedupe_key
from src.services.maintenance.models import NotificationPlanRecord
from tools import local_db_operator_approved_dead_letter_delivery_replay_request_fixture_runner as request_runner


SCHEMA_VERSION = "local_db_delivery_replay_requested_to_notification_plan_created_replay_intent_fixture_runner_v1"
REPLAY_REQUESTED_EVENT_TYPE = request_runner.REPLAY_REQUESTED_EVENT_TYPE
REQUESTED_BY = request_runner.REQUESTED_BY
REPLAY_REASON = "explicit_delivery_replay"
MAINTENANCE_TRIGGER_SOURCE = "local_db_delivery_replay_requested_to_notification_plan_created_replay_intent_fixture_runner"
MAINTENANCE_RUN_KIND = "local_test_delivery_replay_request_dispatch"
MAINTENANCE_STAGE_NAME = "maintenance_delivery_replay_dispatch"
MAINTENANCE_QUEUE_NAME = "q.replay"
MAINTENANCE_JOB_ERROR_CODE = "delivery_replay_request_dispatched"
OPEN_REPLAY_REQUEST_STATUSES = ("pending", "requested", "dispatched")
FINAL_REPLAY_REQUEST_STATUSES = ("completed",)
COMPATIBLE_REPLAY_REQUEST_STATUSES = (*OPEN_REPLAY_REQUEST_STATUSES, *FINAL_REPLAY_REQUEST_STATUSES)
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "replay_request_loaded",
    "replay_requested_event_loaded",
    "notification_plan_loaded",
    "notification_plan_created_replay_intent_created",
    "notification_plan_created_replay_intent_payload_matches_request",
    "notification_plan_created_replay_intent_dedupe_key_stable",
    "replay_request_status_completed_or_reused",
    "maintenance_pipeline_run_recorded",
    "maintenance_job_attempt_recorded",
)
FALSE_RESULT_KEYS = (
    "notifier_render_created",
    "notification_delivery_record_created",
    "telegram_called",
    "openai_called",
    "redis_mutation",
    "live_github_called",
    "workers_started",
    "production_db_write",
    "alembic_or_ddl_ran",
    "notification_plan_mutated",
    "notification_render_mutated",
    "notification_delivery_record_mutated",
    "state_transition_mutated",
    "dead_letter_mutated",
    "analysis_mutated",
    "judge_output_mutated",
    "candidate_group_mutated",
    "evidence_bundle_mutated",
    "artifact_mutated",
    "source_message_mutated",
)
SAFE_EXCEPTION_MESSAGES = {
    "fixture_notification_plan_ambiguous",
    "fixture_notification_plan_missing_or_invalid",
    "operator_approved_dead_letter_fixture_failed",
    "replay_request_ambiguous",
    "replay_request_missing_or_invalid",
    "replay_request_operator_approval_invalid",
    "replay_request_payload_mismatch",
    "replay_request_replay_type_invalid",
    "replay_request_root_object_invalid",
    "replay_request_status_invalid",
    "replay_requested_event_ambiguous",
    "replay_requested_event_missing_or_invalid",
    "replay_requested_event_payload_mismatch",
    "notification_plan_missing_or_invalid",
    "notification_plan_created_replay_intent_ambiguous",
    "notification_plan_created_replay_intent_missing_or_invalid",
    "notification_plan_created_replay_intent_payload_mismatch",
    "notification_plan_created_replay_intent_dedupe_key_unstable",
    "maintenance_pipeline_run_ambiguous",
    "maintenance_job_attempt_ambiguous",
}

source_candidate_runner = request_runner.source_candidate_runner
github_snapshot_runner = request_runner.github_snapshot_runner


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class FixtureResolutionResult:
    replay_request_id: UUID | None
    replay_request_fixture_prepared: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ReplayRequestRecord:
    replay_request_id: UUID
    replay_type: str
    root_object_type: str
    root_object_id: UUID
    requested_by: str | None
    status: str


@dataclass(frozen=True, slots=True)
class ReplayDispatchExecutionResult:
    replay_request_loaded: bool
    replay_requested_event_loaded: bool
    notification_plan_loaded: bool
    notification_plan_created_replay_intent_created: bool
    notification_plan_created_replay_intent_payload_matches_request: bool
    notification_plan_created_replay_intent_dedupe_key_stable: bool
    replay_request_status_completed_or_reused: bool
    maintenance_pipeline_run_recorded: bool
    maintenance_job_attempt_recorded: bool
    notifier_render_created: bool = False
    notification_delivery_record_created: bool = False
    telegram_called: bool = False
    openai_called: bool = False
    redis_mutation: bool = False
    live_github_called: bool = False
    workers_started: bool = False
    production_db_write: bool = False
    alembic_or_ddl_ran: bool = False
    notification_plan_mutated: bool = False
    notification_render_mutated: bool = False
    notification_delivery_record_mutated: bool = False
    state_transition_mutated: bool = False
    dead_letter_mutated: bool = False
    analysis_mutated: bool = False
    judge_output_mutated: bool = False
    candidate_group_mutated: bool = False
    evidence_bundle_mutated: bool = False
    artifact_mutated: bool = False
    source_message_mutated: bool = False
    notification_plan_created_event_count_before: int = 0
    notification_plan_created_event_count_after: int = 0
    maintenance_pipeline_run_count_before: int = 0
    maintenance_pipeline_run_count_after: int = 0
    maintenance_job_attempt_count_before: int = 0
    maintenance_job_attempt_count_after: int = 0
    checks_failed: tuple[str, ...] = ()


class ReplayRequestFixtureResolver(Protocol):
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


class ReplayDispatchExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        replay_request_id: UUID | None,
        notification_plan_id: UUID | None,
    ) -> ReplayDispatchExecutionResult: ...


class DefaultReplayRequestFixtureResolver:
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
        existing_plan_ids = request_runner.dlq_runner.due_runner._find_fixture_notification_plan_ids_by_namespace(  # noqa: SLF001
            database_url=database_url,
            replay_namespace=replay_namespace,
        )
        if len(existing_plan_ids) > 1:
            return FixtureResolutionResult(
                replay_request_id=None,
                replay_request_fixture_prepared=False,
                checks_failed=("fixture_notification_plan_ambiguous",),
            )

        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        predecessor = request_runner.run(
            argparse.Namespace(
                database_url=database_url,
                operator_approval=request_runner.OPERATOR_APPROVAL_TOKEN,
                dead_letter_entry_id=None,
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
                replay_request_id=None,
                replay_request_fixture_prepared=False,
                checks_failed=("operator_approved_dead_letter_fixture_failed",),
            )

        plan_ids = request_runner.dlq_runner.due_runner._find_fixture_notification_plan_ids_by_namespace(  # noqa: SLF001
            database_url=database_url,
            replay_namespace=replay_namespace,
        )
        if len(plan_ids) > 1:
            return FixtureResolutionResult(
                replay_request_id=None,
                replay_request_fixture_prepared=True,
                checks_failed=("fixture_notification_plan_ambiguous",),
            )
        if len(plan_ids) != 1:
            return FixtureResolutionResult(
                replay_request_id=None,
                replay_request_fixture_prepared=True,
                checks_failed=("fixture_notification_plan_missing_or_invalid",),
            )

        replay_request_id, failures = _find_fixture_replay_request_id_for_plan(
            database_url=database_url,
            notification_plan_id=plan_ids[0],
        )
        return FixtureResolutionResult(
            replay_request_id=replay_request_id,
            replay_request_fixture_prepared=True,
            checks_failed=tuple(failures),
        )


class SqlAlchemyReplayDispatchExecutor:
    def execute(
        self,
        *,
        database_url: str,
        replay_request_id: UUID | None,
        notification_plan_id: UUID | None,
    ) -> ReplayDispatchExecutionResult:
        request_runner.dlq_runner.due_runner.render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                return _execute_replay_dispatch(
                    connection,
                    replay_request_id=replay_request_id,
                    notification_plan_id=notification_plan_id,
                )
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Dispatch one local/test DB delivery replay request into a notification.plan.created.v1 "
            "replay-intent without running notifier, Redis, or live transport."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--replay-request-id")
    parser.add_argument("--notification-plan-id")
    parser.add_argument("--source-fixture")
    parser.add_argument("--github-snapshot-fixture")
    parser.add_argument("--replay-namespace")
    parser.add_argument("--prepare-delivery-replay-request-fixture", action="store_true")
    parser.add_argument("--max-attempts")
    parser.add_argument("--confirm-local-test-db", action="store_true")
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    resolver: ReplayRequestFixtureResolver | None = None,
    executor: ReplayDispatchExecutor | None = None,
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

    raw_replay_request_id = _string_or_none(getattr(args, "replay_request_id", None))
    raw_plan_id = _string_or_none(getattr(args, "notification_plan_id", None))
    replay_request_id = _uuid_or_none(raw_replay_request_id)
    plan_id = _uuid_or_none(raw_plan_id)
    if raw_replay_request_id is not None and replay_request_id is None:
        checks_failed.append("replay_request_id_invalid")
    if raw_plan_id is not None and plan_id is None:
        checks_failed.append("notification_plan_id_invalid")

    source_fixture = _path_or_none(getattr(args, "source_fixture", None))
    github_fixture = _path_or_none(getattr(args, "github_snapshot_fixture", None))
    replay_namespace = _string_or_none(getattr(args, "replay_namespace", None))
    prepare_fixture = bool(getattr(args, "prepare_delivery_replay_request_fixture", False))
    fixture_selector_supplied = (
        source_fixture is not None or github_fixture is not None or replay_namespace is not None or prepare_fixture
    )
    fixture_selector_complete = (
        source_fixture is not None and github_fixture is not None and replay_namespace is not None and prepare_fixture
    )

    selector_modes = int(raw_replay_request_id is not None) + int(raw_plan_id is not None) + int(fixture_selector_supplied)
    if selector_modes > 1:
        checks_failed.append("selector_mode_ambiguous")
    elif selector_modes == 0:
        checks_failed.append("selector_mode_required")
    elif fixture_selector_supplied and not fixture_selector_complete:
        checks_failed.append("fixture_selector_incomplete")

    max_attempts = _max_attempts_from_args_env(getattr(args, "max_attempts", None), effective_env)
    if fixture_selector_supplied and max_attempts is None:
        checks_failed.append("max_attempts_invalid")
        max_attempts = request_runner.dlq_runner.DEFAULT_MAX_ATTEMPTS

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

    resolved_replay_request_id = replay_request_id
    resolved_plan_id = plan_id
    if fixture_selector_supplied:
        if source_fixture is None or github_fixture is None or replay_namespace is None:
            return _finish(report, ["fixture_selector_incomplete"])
        active_resolver = resolver or DefaultReplayRequestFixtureResolver()
        try:
            resolution = active_resolver.resolve(
                database_url=args.database_url,
                source_fixture_path=source_fixture,
                github_snapshot_fixture_path=github_fixture,
                replay_namespace=replay_namespace,
                max_attempts=max_attempts or request_runner.dlq_runner.DEFAULT_MAX_ATTEMPTS,
                env=effective_env,
                repo_root=root,
            )
        except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
            return _finish(report, [_safe_failure_code(exc)])
        report["replay_request_fixture_prepared"] = resolution.replay_request_fixture_prepared
        checks_failed.extend(resolution.checks_failed)
        resolved_replay_request_id = resolution.replay_request_id
        resolved_plan_id = None
        if resolved_replay_request_id is None:
            checks_failed.append("replay_request_missing_or_invalid")
        if checks_failed:
            return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyReplayDispatchExecutor()
    try:
        execution = active_executor.execute(
            database_url=args.database_url,
            replay_request_id=resolved_replay_request_id,
            notification_plan_id=resolved_plan_id,
        )
    except Exception as exc:  # noqa: BLE001 - sanitized report only.
        return _finish(report, [_safe_failure_code(exc)])

    report.update(_execution_report(execution))
    checks_failed.extend(execution.checks_failed)

    for key in TRUE_RESULT_KEYS:
        if report.get(key) is not True:
            checks_failed.append(f"{key}:missing")
    for key in FALSE_RESULT_KEYS:
        if report.get(key) is not False:
            checks_failed.append(key)
    return _finish(report, checks_failed)


def validate_database_url(database_url: str | None):
    return request_runner.validate_database_url(database_url)


def build_notification_plan_created_replay_intent_payload(
    *,
    plan: NotificationPlanRecord,
    replay_request_id: UUID,
) -> dict[str, Any]:
    return build_replay_intent_payload(
        plan=plan,
        replay_request_id=replay_request_id,
        replay_reason=REPLAY_REASON,
    )


def build_notification_plan_created_replay_intent_dedupe_key(replay_request_id: UUID) -> str:
    return replay_intent_dedupe_key(replay_request_id)


def _find_fixture_replay_request_id_for_plan(
    *,
    database_url: str,
    notification_plan_id: UUID,
) -> tuple[UUID | None, tuple[str, ...]]:
    request_runner.dlq_runner.due_runner.render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001
    import sqlalchemy as sa

    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.begin() as connection:
            rows = _load_compatible_delivery_replay_requests_by_plan(connection, notification_plan_id=notification_plan_id)
            if len(rows) > 1:
                return None, ("replay_request_ambiguous",)
            if not rows:
                return None, ("replay_request_missing_or_invalid",)
            return rows[0].replay_request_id, ()
    finally:
        engine.dispose()


def _execute_replay_dispatch(
    connection: Any,
    *,
    replay_request_id: UUID | None,
    notification_plan_id: UUID | None,
) -> ReplayDispatchExecutionResult:
    replay_request: ReplayRequestRecord | None
    if replay_request_id is not None:
        replay_request = _load_replay_request_by_id(connection, replay_request_id=replay_request_id)
    elif notification_plan_id is not None:
        rows = _load_open_delivery_replay_requests_by_plan(connection, notification_plan_id=notification_plan_id)
        if len(rows) > 1:
            return _execution_result(replay_request_loaded=False, checks_failed=("replay_request_ambiguous",))
        replay_request = rows[0] if rows else None
    else:
        return _execution_result(replay_request_loaded=False, checks_failed=("selector_mode_required",))

    if replay_request is None:
        return _execution_result(replay_request_loaded=False, checks_failed=("replay_request_missing_or_invalid",))
    if replay_request.replay_type != "delivery":
        return _execution_result(
            replay_request_loaded=True,
            checks_failed=("replay_request_replay_type_invalid",),
        )
    if replay_request.root_object_type != "notification_plan":
        return _execution_result(
            replay_request_loaded=True,
            checks_failed=("replay_request_root_object_invalid",),
        )
    if replay_request.requested_by != REQUESTED_BY:
        return _execution_result(
            replay_request_loaded=True,
            checks_failed=("replay_request_operator_approval_invalid",),
        )
    if replay_request.status not in COMPATIBLE_REPLAY_REQUEST_STATUSES:
        return _execution_result(
            replay_request_loaded=True,
            checks_failed=("replay_request_status_invalid",),
        )

    plan = _load_notification_plan(connection, notification_plan_id=replay_request.root_object_id)
    if plan is None:
        return _execution_result(
            replay_request_loaded=True,
            notification_plan_loaded=False,
            checks_failed=("notification_plan_missing_or_invalid",),
        )

    replay_events = _load_replay_requested_events(connection, replay_request_id=replay_request.replay_request_id)
    if len(replay_events) > 1:
        return _execution_result(
            replay_request_loaded=True,
            notification_plan_loaded=True,
            checks_failed=("replay_requested_event_ambiguous",),
        )
    if not replay_events:
        return _execution_result(
            replay_request_loaded=True,
            notification_plan_loaded=True,
            replay_requested_event_loaded=False,
            checks_failed=("replay_requested_event_missing_or_invalid",),
        )
    replay_event = replay_events[0]
    if not _replay_requested_event_matches(replay_event, replay_request=replay_request):
        return _execution_result(
            replay_request_loaded=True,
            replay_requested_event_loaded=True,
            notification_plan_loaded=True,
            checks_failed=("replay_requested_event_payload_mismatch",),
        )

    before = _capture_scope(connection, plan=plan, replay_request_id=replay_request.replay_request_id)

    if replay_request.status not in FINAL_REPLAY_REQUEST_STATUSES:
        _update_replay_request_status(
            connection,
            replay_request_id=replay_request.replay_request_id,
            status="dispatched",
        )
    _insert_or_reuse_replay_intent(connection, plan=plan, replay_request_id=replay_request.replay_request_id)
    if replay_request.status not in FINAL_REPLAY_REQUEST_STATUSES:
        _update_replay_request_status(
            connection,
            replay_request_id=replay_request.replay_request_id,
            status="completed",
        )
    _insert_or_reuse_pipeline_run(connection, replay_request_id=replay_request.replay_request_id)
    _insert_or_reuse_job_attempt(connection, replay_request_id=replay_request.replay_request_id)

    after = _capture_scope(connection, plan=plan, replay_request_id=replay_request.replay_request_id)
    reloaded = _load_replay_request_by_id(connection, replay_request_id=replay_request.replay_request_id)
    replay_intents = _load_replay_intent_events(connection, replay_request_id=replay_request.replay_request_id)

    checks_failed: list[str] = []
    if len(replay_intents) > 1:
        checks_failed.append("notification_plan_created_replay_intent_ambiguous")
    if len(replay_intents) != 1:
        checks_failed.append("notification_plan_created_replay_intent_missing_or_invalid")
        replay_intent: Mapping[str, Any] | None = None
    else:
        replay_intent = replay_intents[0]

    payload_matches = _replay_intent_payload_matches(replay_intent, plan=plan, replay_request=replay_request)
    if not payload_matches:
        checks_failed.append("notification_plan_created_replay_intent_payload_mismatch")

    dedupe_stable = (
        before["notification_plan_created_events"] <= 1
        and after["notification_plan_created_events"] == 1
        and after["notification_plan_created_events"] - before["notification_plan_created_events"] in {0, 1}
        and replay_intent is not None
        and replay_intent.get("dedupe_key")
        == build_notification_plan_created_replay_intent_dedupe_key(replay_request.replay_request_id)
    )
    if not dedupe_stable:
        checks_failed.append("notification_plan_created_replay_intent_dedupe_key_unstable")
    if reloaded is None or reloaded.status != "completed":
        checks_failed.append("replay_request_status_not_completed")
    if after["maintenance_pipeline_runs"] != 1:
        checks_failed.append("maintenance_pipeline_run_recorded:missing")
    if after["maintenance_job_attempts"] != 1:
        checks_failed.append("maintenance_job_attempt_recorded:missing")

    for key, failure in (
        ("notification_plan_digest", "notification_plan_mutated"),
        ("notification_render_digest", "notification_render_mutated"),
        ("notification_delivery_record_digest", "notification_delivery_record_mutated"),
        ("state_transition_digest", "state_transition_mutated"),
        ("dead_letter_digest", "dead_letter_mutated"),
        ("analysis_digest", "analysis_mutated"),
        ("judge_output_digest", "judge_output_mutated"),
        ("candidate_group_digest", "candidate_group_mutated"),
        ("evidence_bundle_digest", "evidence_bundle_mutated"),
        ("artifact_digest", "artifact_mutated"),
        ("source_message_digest", "source_message_mutated"),
    ):
        if after[key] != before[key]:
            checks_failed.append(failure)
    if after["notification_render_count"] > before["notification_render_count"]:
        checks_failed.append("notifier_render_created")
    if after["notification_delivery_record_count"] > before["notification_delivery_record_count"]:
        checks_failed.append("notification_delivery_record_created")

    return ReplayDispatchExecutionResult(
        replay_request_loaded=True,
        replay_requested_event_loaded=True,
        notification_plan_loaded=True,
        notification_plan_created_replay_intent_created=after["notification_plan_created_events"] == 1,
        notification_plan_created_replay_intent_payload_matches_request=payload_matches,
        notification_plan_created_replay_intent_dedupe_key_stable=dedupe_stable,
        replay_request_status_completed_or_reused=reloaded is not None and reloaded.status == "completed",
        maintenance_pipeline_run_recorded=after["maintenance_pipeline_runs"] == 1,
        maintenance_job_attempt_recorded=after["maintenance_job_attempts"] == 1,
        notifier_render_created=after["notification_render_count"] > before["notification_render_count"],
        notification_delivery_record_created=(
            after["notification_delivery_record_count"] > before["notification_delivery_record_count"]
        ),
        notification_plan_mutated=after["notification_plan_digest"] != before["notification_plan_digest"],
        notification_render_mutated=after["notification_render_digest"] != before["notification_render_digest"],
        notification_delivery_record_mutated=(
            after["notification_delivery_record_digest"] != before["notification_delivery_record_digest"]
        ),
        state_transition_mutated=after["state_transition_digest"] != before["state_transition_digest"],
        dead_letter_mutated=after["dead_letter_digest"] != before["dead_letter_digest"],
        analysis_mutated=after["analysis_digest"] != before["analysis_digest"],
        judge_output_mutated=after["judge_output_digest"] != before["judge_output_digest"],
        candidate_group_mutated=after["candidate_group_digest"] != before["candidate_group_digest"],
        evidence_bundle_mutated=after["evidence_bundle_digest"] != before["evidence_bundle_digest"],
        artifact_mutated=after["artifact_digest"] != before["artifact_digest"],
        source_message_mutated=after["source_message_digest"] != before["source_message_digest"],
        notification_plan_created_event_count_before=before["notification_plan_created_events"],
        notification_plan_created_event_count_after=after["notification_plan_created_events"],
        maintenance_pipeline_run_count_before=before["maintenance_pipeline_runs"],
        maintenance_pipeline_run_count_after=after["maintenance_pipeline_runs"],
        maintenance_job_attempt_count_before=before["maintenance_job_attempts"],
        maintenance_job_attempt_count_after=after["maintenance_job_attempts"],
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


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


def _load_open_delivery_replay_requests_by_plan(
    connection: Any,
    *,
    notification_plan_id: UUID,
) -> list[ReplayRequestRecord]:
    return _load_delivery_replay_requests_by_plan(
        connection,
        notification_plan_id=notification_plan_id,
        statuses=OPEN_REPLAY_REQUEST_STATUSES,
    )


def _load_compatible_delivery_replay_requests_by_plan(
    connection: Any,
    *,
    notification_plan_id: UUID,
) -> list[ReplayRequestRecord]:
    return _load_delivery_replay_requests_by_plan(
        connection,
        notification_plan_id=notification_plan_id,
        statuses=COMPATIBLE_REPLAY_REQUEST_STATUSES,
    )


def _load_delivery_replay_requests_by_plan(
    connection: Any,
    *,
    notification_plan_id: UUID,
    statuses: Sequence[str],
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
            "statuses": list(statuses),
        },
    ).mappings().all()
    return [_replay_request_from_row(row) for row in rows]


def _replay_request_from_row(row: Mapping[str, Any]) -> ReplayRequestRecord:
    return ReplayRequestRecord(
        replay_request_id=UUID(str(row["replay_request_id"])),
        replay_type=str(row["replay_type"]),
        root_object_type=str(row["root_object_type"]),
        root_object_id=UUID(str(row["root_object_id"])),
        requested_by=_string_or_none(row["requested_by"]),
        status=str(row["status"]),
    )


def _load_notification_plan(connection: Any, *, notification_plan_id: UUID) -> NotificationPlanRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT notification_plan_id,
                   analysis_id,
                   candidate_group_id,
                   delivery_decision::text AS delivery_decision,
                   urgency_profile::text AS urgency_profile,
                   target_chat_id,
                   target_thread_id,
                   render_profile,
                   dedupe_subject_key,
                   material_change_hash,
                   send_after,
                   suppress_reason_code,
                   status::text AS status
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
        target_thread_id=int(row["target_thread_id"]) if row["target_thread_id"] is not None else None,
        render_profile=_string_or_none(row["render_profile"]),
        dedupe_subject_key=str(row["dedupe_subject_key"]),
        material_change_hash=str(row["material_change_hash"]),
        send_after=row["send_after"],
        suppress_reason_code=_string_or_none(row["suppress_reason_code"]),
        status=str(row["status"]),
    )


def _load_replay_requested_events(connection: Any, *, replay_request_id: UUID) -> list[dict[str, Any]]:
    import sqlalchemy as sa

    rows = connection.execute(
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
            ORDER BY created_at ASC, event_id ASC
            """
        ),
        {
            "event_type": REPLAY_REQUESTED_EVENT_TYPE,
            "replay_request_id": str(replay_request_id),
        },
    ).mappings().all()
    return [_event_from_row(row) for row in rows]


def _load_replay_intent_events(connection: Any, *, replay_request_id: UUID) -> list[dict[str, Any]]:
    import sqlalchemy as sa

    rows = connection.execute(
        sa.text(
            """
            SELECT event_type,
                   aggregate_type,
                   aggregate_id,
                   dedupe_key,
                   payload_json
            FROM event_outbox
            WHERE event_type = :event_type
              AND payload_json ->> 'replay_request_id' = :replay_request_id
            ORDER BY created_at ASC, event_id ASC
            """
        ),
        {
            "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
            "replay_request_id": str(replay_request_id),
        },
    ).mappings().all()
    return [_event_from_row(row) for row in rows]


def _event_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = _json_loads(row["payload_json"])
    return {
        "event_type": str(row["event_type"]),
        "aggregate_type": str(row["aggregate_type"]),
        "aggregate_id": str(row["aggregate_id"]),
        "dedupe_key": str(row["dedupe_key"]),
        "payload_json": payload if isinstance(payload, dict) else {},
    }


def _update_replay_request_status(connection: Any, *, replay_request_id: UUID, status: str) -> None:
    import sqlalchemy as sa

    connection.execute(
        sa.text(
            """
            UPDATE replay_requests
            SET status = :status
            WHERE replay_request_id = CAST(:replay_request_id AS uuid)
            """
        ),
        {"replay_request_id": str(replay_request_id), "status": status},
    )


def _insert_or_reuse_replay_intent(
    connection: Any,
    *,
    plan: NotificationPlanRecord,
    replay_request_id: UUID,
) -> None:
    import sqlalchemy as sa

    payload = build_notification_plan_created_replay_intent_payload(
        plan=plan,
        replay_request_id=replay_request_id,
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
            "dedupe_key": build_notification_plan_created_replay_intent_dedupe_key(replay_request_id),
            "payload_json": _json_dumps(payload),
        },
    )


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
    replay_request_id: UUID,
) -> dict[str, Any]:
    context = request_runner.dlq_runner._load_candidate_scope_context(  # noqa: SLF001
        connection,
        candidate_group_id=plan.candidate_group_id,
    )
    return {
        "notification_plan_digest": request_runner.dlq_runner.due_runner._notification_plan_digest(  # noqa: SLF001
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "notification_render_digest": request_runner._notification_render_digest(  # noqa: SLF001
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "notification_delivery_record_digest": request_runner._notification_delivery_record_digest(  # noqa: SLF001
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "state_transition_digest": request_runner._state_transition_digest(  # noqa: SLF001
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "dead_letter_digest": _dead_letter_digest(connection, notification_plan_id=plan.notification_plan_id),
        "analysis_digest": request_runner.dlq_runner.due_runner._analysis_digest(  # noqa: SLF001
            connection,
            analysis_id=plan.analysis_id,
        ),
        "judge_output_digest": request_runner.dlq_runner.due_runner._judge_output_digest_for_analysis(  # noqa: SLF001
            connection,
            analysis_id=plan.analysis_id,
        ),
        "candidate_group_digest": request_runner.dlq_runner.due_runner._candidate_group_digest(  # noqa: SLF001
            connection,
            candidate_group_id=plan.candidate_group_id,
        ),
        "evidence_bundle_digest": request_runner.dlq_runner._evidence_bundle_digest(  # noqa: SLF001
            connection,
            bundle_id=context["current_bundle_id"],
        ),
        "artifact_digest": request_runner.dlq_runner._artifact_digest(  # noqa: SLF001
            connection,
            artifact_id=context["current_primary_artifact_id"],
        ),
        "source_message_digest": request_runner.dlq_runner._source_message_digest(  # noqa: SLF001
            connection,
            source_message_id=context["source_message_id"],
        ),
        "notification_plan_created_events": _notification_plan_created_replay_intent_count(
            connection,
            replay_request_id=replay_request_id,
        ),
        "notification_render_count": request_runner._notification_render_count(  # noqa: SLF001
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "notification_delivery_record_count": request_runner._notification_delivery_record_count(  # noqa: SLF001
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


def _dead_letter_digest(connection: Any, *, notification_plan_id: UUID) -> str:
    import sqlalchemy as sa

    return str(
        connection.execute(
            sa.text(
                """
                SELECT COALESCE(md5(COALESCE(jsonb_agg(to_jsonb(d) ORDER BY d.dead_letter_entry_id), '[]'::jsonb)::text), '')
                FROM (
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
                    WHERE root_object_type = 'notification_plan'
                      AND root_object_id = CAST(:notification_plan_id AS uuid)
                ) AS d
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        ).scalar_one()
        or ""
    )


def _notification_plan_created_replay_intent_count(connection: Any, *, replay_request_id: UUID) -> int:
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


def _maintenance_pipeline_run_count(connection: Any, *, replay_request_id: UUID) -> int:
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


def _maintenance_job_attempt_count(connection: Any, *, replay_request_id: UUID) -> int:
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


def _replay_requested_event_matches(
    event: Mapping[str, Any],
    *,
    replay_request: ReplayRequestRecord,
) -> bool:
    payload = event.get("payload_json") if isinstance(event.get("payload_json"), Mapping) else {}
    return (
        event.get("event_type") == REPLAY_REQUESTED_EVENT_TYPE
        and event.get("aggregate_type") == "replay_request"
        and event.get("aggregate_id") == str(replay_request.replay_request_id)
        and payload.get("replay_request_id") == str(replay_request.replay_request_id)
        and payload.get("replay_type") == "delivery"
        and payload.get("root_object_type") == "notification_plan"
        and payload.get("root_object_id") == str(replay_request.root_object_id)
        and payload.get("operator_approval") == request_runner.OPERATOR_APPROVAL_TOKEN
        and payload.get("replay_reason") == request_runner.REPLAY_REASON
        and _uuid_or_none(payload.get("source_dead_letter_entry_id")) is not None
    )


def _replay_intent_payload_matches(
    event: Mapping[str, Any] | None,
    *,
    plan: NotificationPlanRecord,
    replay_request: ReplayRequestRecord,
) -> bool:
    if event is None:
        return False
    expected_payload = build_notification_plan_created_replay_intent_payload(
        plan=plan,
        replay_request_id=replay_request.replay_request_id,
    )
    return (
        event.get("event_type") == NOTIFICATION_PLAN_CREATED_EVENT_TYPE
        and event.get("aggregate_type") == "analysis"
        and event.get("aggregate_id") == str(plan.analysis_id)
        and event.get("dedupe_key")
        == build_notification_plan_created_replay_intent_dedupe_key(replay_request.replay_request_id)
        and event.get("payload_json") == expected_payload
    )


def _execution_report(execution: ReplayDispatchExecutionResult) -> dict[str, Any]:
    return {
        "replay_request_loaded": execution.replay_request_loaded,
        "replay_requested_event_loaded": execution.replay_requested_event_loaded,
        "notification_plan_loaded": execution.notification_plan_loaded,
        "notification_plan_created_replay_intent_created": (
            execution.notification_plan_created_replay_intent_created
        ),
        "notification_plan_created_replay_intent_payload_matches_request": (
            execution.notification_plan_created_replay_intent_payload_matches_request
        ),
        "notification_plan_created_replay_intent_dedupe_key_stable": (
            execution.notification_plan_created_replay_intent_dedupe_key_stable
        ),
        "replay_request_status_completed_or_reused": execution.replay_request_status_completed_or_reused,
        "maintenance_pipeline_run_recorded": execution.maintenance_pipeline_run_recorded,
        "maintenance_job_attempt_recorded": execution.maintenance_job_attempt_recorded,
        "notifier_render_created": execution.notifier_render_created,
        "notification_delivery_record_created": execution.notification_delivery_record_created,
        "telegram_called": execution.telegram_called,
        "openai_called": execution.openai_called,
        "redis_mutation": execution.redis_mutation,
        "live_github_called": execution.live_github_called,
        "workers_started": execution.workers_started,
        "production_db_write": execution.production_db_write,
        "alembic_or_ddl_ran": execution.alembic_or_ddl_ran,
        "notification_plan_mutated": execution.notification_plan_mutated,
        "notification_render_mutated": execution.notification_render_mutated,
        "notification_delivery_record_mutated": execution.notification_delivery_record_mutated,
        "state_transition_mutated": execution.state_transition_mutated,
        "dead_letter_mutated": execution.dead_letter_mutated,
        "analysis_mutated": execution.analysis_mutated,
        "judge_output_mutated": execution.judge_output_mutated,
        "candidate_group_mutated": execution.candidate_group_mutated,
        "evidence_bundle_mutated": execution.evidence_bundle_mutated,
        "artifact_mutated": execution.artifact_mutated,
        "source_message_mutated": execution.source_message_mutated,
        "notification_plan_created_event_count_before": execution.notification_plan_created_event_count_before,
        "notification_plan_created_event_count_after": execution.notification_plan_created_event_count_after,
        "maintenance_pipeline_run_count_before": execution.maintenance_pipeline_run_count_before,
        "maintenance_pipeline_run_count_after": execution.maintenance_pipeline_run_count_after,
        "maintenance_job_attempt_count_before": execution.maintenance_job_attempt_count_before,
        "maintenance_job_attempt_count_after": execution.maintenance_job_attempt_count_after,
    }


def _base_report() -> dict[str, Any]:
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "replay_request_fixture_prepared": False,
        "notification_plan_created_event_count_before": 0,
        "notification_plan_created_event_count_after": 0,
        "maintenance_pipeline_run_count_before": 0,
        "maintenance_pipeline_run_count_after": 0,
        "maintenance_job_attempt_count_before": 0,
        "maintenance_job_attempt_count_after": 0,
        "checks_failed": [],
    }
    for key in TRUE_RESULT_KEYS:
        if key != "database_url_guard_passed":
            report[key] = False
    for key in FALSE_RESULT_KEYS:
        report[key] = False
    return report


def _execution_result(
    *,
    replay_request_loaded: bool = False,
    replay_requested_event_loaded: bool = False,
    notification_plan_loaded: bool = False,
    notification_plan_created_replay_intent_created: bool = False,
    notification_plan_created_replay_intent_payload_matches_request: bool = False,
    notification_plan_created_replay_intent_dedupe_key_stable: bool = False,
    replay_request_status_completed_or_reused: bool = False,
    maintenance_pipeline_run_recorded: bool = False,
    maintenance_job_attempt_recorded: bool = False,
    checks_failed: Sequence[str],
) -> ReplayDispatchExecutionResult:
    return ReplayDispatchExecutionResult(
        replay_request_loaded=replay_request_loaded,
        replay_requested_event_loaded=replay_requested_event_loaded,
        notification_plan_loaded=notification_plan_loaded,
        notification_plan_created_replay_intent_created=notification_plan_created_replay_intent_created,
        notification_plan_created_replay_intent_payload_matches_request=(
            notification_plan_created_replay_intent_payload_matches_request
        ),
        notification_plan_created_replay_intent_dedupe_key_stable=(
            notification_plan_created_replay_intent_dedupe_key_stable
        ),
        replay_request_status_completed_or_reused=replay_request_status_completed_or_reused,
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
        raw = str(request_runner.dlq_runner.DEFAULT_MAX_ATTEMPTS)
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _json_loads(value: Any) -> Any:
    return request_runner._json_loads(value)  # noqa: SLF001


def _json_dumps(value: Any) -> str:
    return request_runner._json_dumps(value)  # noqa: SLF001


def _uuid_or_none(value: Any) -> UUID | None:
    return request_runner._uuid_or_none(value)  # noqa: SLF001


def _path_or_none(value: Any) -> Path | None:
    return request_runner._path_or_none(value)  # noqa: SLF001


def _string_or_none(value: Any) -> str | None:
    return request_runner._string_or_none(value)  # noqa: SLF001


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc)
    if message in SAFE_EXCEPTION_MESSAGES:
        return message
    return exc.__class__.__name__


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def main(argv: Sequence[str] | None = None) -> int:
    result = run(build_parser().parse_args(argv))
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
