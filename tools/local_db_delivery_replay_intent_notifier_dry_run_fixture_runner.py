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

from tools import local_db_delivery_replay_requested_to_notification_plan_created_replay_intent_fixture_runner as dispatch_runner
from tools import local_db_notification_plan_created_render_dry_run_fixture_runner as render_runner


SCHEMA_VERSION = "local_db_delivery_replay_intent_notifier_dry_run_fixture_runner_v1"
NOTIFICATION_PLAN_CREATED_EVENT_TYPE = dispatch_runner.NOTIFICATION_PLAN_CREATED_EVENT_TYPE
NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE = render_runner.NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE
REPLAY_REQUESTED_EVENT_TYPE = dispatch_runner.REPLAY_REQUESTED_EVENT_TYPE
RUNNER_INTERNAL_DRY_RUN_MODE = True
REQUIRED_REPLAY_INTENT_PAYLOAD_KEYS = (
    "notification_plan_id",
    "analysis_id",
    "candidate_group_id",
    "delivery_decision",
    "urgency_profile",
    "target_chat_id",
    "dedupe_subject_key",
    "material_change_hash",
    "replay_request_id",
)
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "dry_run_mode_guard_passed",
    "notification_plan_created_event_loaded",
    "notification_plan_loaded_or_concretized",
    "analysis_loaded",
    "judge_output_loaded",
    "candidate_group_loaded",
    "artifact_loaded",
    "source_message_loaded",
    "notification_render_created_or_reused",
    "dry_run_delivery_record_created_or_reused",
    "notification_delivery_result_event_created_or_reused",
    "notification_render_dedupe_stable",
    "dry_run_delivery_record_dedupe_stable",
    "delivery_result_event_dedupe_stable",
    "delivery_result_matches_dry_run_record",
)
FALSE_RESULT_KEYS = (
    "telegram_called",
    "openai_called",
    "redis_mutation",
    "live_github_called",
    "workers_started",
    "production_db_write",
    "alembic_or_ddl_ran",
    "real_transport_attempted",
    "notification_plan_created_replay_intent_created",
    "replay_request_created",
    "replay_requested_event_created",
    "replay_request_status_mutated",
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
    "notification_plan_created_event_aggregate_mismatch",
    "notification_plan_created_replay_intent_ambiguous",
    "notification_plan_created_replay_intent_missing_or_invalid",
    "notification_plan_created_replay_intent_payload_invalid",
    "notification_plan_created_replay_intent_payload_mismatch",
    "notification_plan_missing_or_invalid",
    "analysis_missing",
    "judge_output_missing",
    "candidate_group_missing",
    "primary_artifact_missing",
    "source_message_missing",
    "notification_render_dry_run_fixture_failed",
    "operator_approved_dead_letter_fixture_failed",
    "replay_request_missing_or_invalid",
    "replay_request_ambiguous",
}

source_candidate_runner = dispatch_runner.source_candidate_runner
github_snapshot_runner = dispatch_runner.github_snapshot_runner


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReplayIntentResolutionResult:
    notification_plan_created_event_id: UUID | None
    notification_plan_created_event_loaded: bool
    replay_intent_fixture_prepared: bool = False
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ReplayIntentEvent:
    event_id: UUID
    aggregate_type: str | None
    aggregate_id: UUID | None
    payload_json: dict[str, Any]
    intent: render_runner.notifier_base.NotificationPlanIntent
    replay_request_id: UUID


@dataclass(frozen=True, slots=True)
class ReplayIntentNotifierDryRunExecutionResult:
    notification_plan_created_event_loaded: bool
    notification_plan_loaded_or_concretized: bool
    analysis_loaded: bool
    judge_output_loaded: bool
    candidate_group_loaded: bool
    artifact_loaded: bool
    source_message_loaded: bool
    notification_render_created_or_reused: bool
    dry_run_delivery_record_created_or_reused: bool
    notification_delivery_result_event_created_or_reused: bool
    notification_render_dedupe_stable: bool
    dry_run_delivery_record_dedupe_stable: bool
    delivery_result_event_dedupe_stable: bool
    delivery_result_matches_dry_run_record: bool
    notification_render_count_before: int = 0
    notification_render_count_after: int = 0
    dry_run_delivery_record_count_before: int = 0
    dry_run_delivery_record_count_after: int = 0
    notification_delivery_result_event_count_before: int = 0
    notification_delivery_result_event_count_after: int = 0
    notification_plan_created_replay_intent_count_before: int = 0
    notification_plan_created_replay_intent_count_after: int = 0
    replay_request_count_before: int = 0
    replay_request_count_after: int = 0
    replay_requested_event_count_before: int = 0
    replay_requested_event_count_after: int = 0
    telegram_called: bool = False
    openai_called: bool = False
    redis_mutation: bool = False
    live_github_called: bool = False
    workers_started: bool = False
    production_db_write: bool = False
    alembic_or_ddl_ran: bool = False
    real_transport_attempted: bool = False
    notification_plan_created_replay_intent_created: bool = False
    replay_request_created: bool = False
    replay_requested_event_created: bool = False
    replay_request_status_mutated: bool = False
    analysis_mutated: bool = False
    judge_output_mutated: bool = False
    candidate_group_mutated: bool = False
    evidence_bundle_mutated: bool = False
    artifact_mutated: bool = False
    source_message_mutated: bool = False
    checks_failed: tuple[str, ...] = ()


class ReplayIntentResolver(Protocol):
    def resolve(
        self,
        *,
        database_url: str,
        selector_mode: str,
        notification_plan_created_event_id: UUID | None,
        replay_request_id: UUID | None,
        notification_plan_id: UUID | None,
        source_fixture_path: Path | None,
        github_snapshot_fixture_path: Path | None,
        replay_namespace: str | None,
        max_attempts: int,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> ReplayIntentResolutionResult: ...


class RenderDryRunRunner(Protocol):
    def run(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
        delivery_dedupe_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> render_runner.RunnerResult: ...


class ReplayIntentNotifierDryRunExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
        delivery_dedupe_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> ReplayIntentNotifierDryRunExecutionResult: ...


class DefaultReplayIntentResolver:
    def resolve(
        self,
        *,
        database_url: str,
        selector_mode: str,
        notification_plan_created_event_id: UUID | None,
        replay_request_id: UUID | None,
        notification_plan_id: UUID | None,
        source_fixture_path: Path | None,
        github_snapshot_fixture_path: Path | None,
        replay_namespace: str | None,
        max_attempts: int,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> ReplayIntentResolutionResult:
        if selector_mode == "notification_plan_created_event":
            if notification_plan_created_event_id is None:
                return _resolution_result(checks_failed=("notification_plan_created_replay_intent_missing_or_invalid",))
            return _resolve_event_by_id(database_url, notification_plan_created_event_id)
        if selector_mode == "replay_request":
            if replay_request_id is None:
                return _resolution_result(checks_failed=("replay_request_missing_or_invalid",))
            return _resolve_event_by_replay_request_id(database_url, replay_request_id)
        if selector_mode == "notification_plan":
            if notification_plan_id is None:
                return _resolution_result(checks_failed=("notification_plan_missing_or_invalid",))
            return _resolve_event_by_notification_plan_id(database_url, notification_plan_id)
        if selector_mode == "fixture_chain":
            if source_fixture_path is None or github_snapshot_fixture_path is None or replay_namespace is None:
                return _resolution_result(checks_failed=("fixture_selector_incomplete",))
            return _resolve_fixture_chain(
                database_url=database_url,
                source_fixture_path=source_fixture_path,
                github_snapshot_fixture_path=github_snapshot_fixture_path,
                replay_namespace=replay_namespace,
                max_attempts=max_attempts,
                env=env,
                repo_root=repo_root,
            )
        return _resolution_result(checks_failed=("selector_mode_required",))


class DefaultRenderDryRunRunner:
    def run(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
        delivery_dedupe_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> render_runner.RunnerResult:
        args = argparse.Namespace(
            database_url=database_url,
            notification_plan_created_event_id=str(notification_plan_created_event_id),
            source_fixture=None,
            github_snapshot_fixture=None,
            replay_namespace=delivery_dedupe_namespace,
            confirm_local_test_db=True,
        )
        dry_run_env = dict(env)
        dry_run_env["APP_ENV"] = "test"
        dry_run_env["NOTIFIER_TELEGRAM_DRY_RUN"] = "true"
        dry_run_env["ENABLE_NOTIFICATION_SEND"] = "false"
        return render_runner.run(args, env=dry_run_env, repo_root=repo_root)


class SqlAlchemyReplayIntentNotifierDryRunExecutor:
    def __init__(self, *, render: RenderDryRunRunner | None = None) -> None:
        self._render = render or DefaultRenderDryRunRunner()

    def execute(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
        delivery_dedupe_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> ReplayIntentNotifierDryRunExecutionResult:
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                event, failures = _load_replay_intent_event_by_id(
                    connection,
                    event_id=notification_plan_created_event_id,
                )
                if event is None:
                    return _execution_result(
                        notification_plan_created_event_loaded=False,
                        checks_failed=failures or ("notification_plan_created_replay_intent_missing_or_invalid",),
                    )
                source_checks = _validate_required_source_rows(connection, event=event)
                if source_checks["checks_failed"]:
                    return _execution_result(
                        notification_plan_created_event_loaded=True,
                        analysis_loaded=source_checks["analysis_loaded"],
                        judge_output_loaded=source_checks["judge_output_loaded"],
                        candidate_group_loaded=source_checks["candidate_group_loaded"],
                        artifact_loaded=source_checks["artifact_loaded"],
                        source_message_loaded=source_checks["source_message_loaded"],
                        checks_failed=source_checks["checks_failed"],
                    )
                before = _capture_scope(
                    connection,
                    event=event,
                    delivery_dedupe_namespace=delivery_dedupe_namespace,
                )
        finally:
            engine.dispose()

        render_result = self._render.run(
            database_url=database_url,
            notification_plan_created_event_id=notification_plan_created_event_id,
            delivery_dedupe_namespace=delivery_dedupe_namespace,
            env=env,
            repo_root=repo_root,
        )

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                event_after, failures_after = _load_replay_intent_event_by_id(
                    connection,
                    event_id=notification_plan_created_event_id,
                )
                if event_after is None:
                    return _execution_result(
                        notification_plan_created_event_loaded=False,
                        checks_failed=failures_after or ("notification_plan_created_replay_intent_missing_or_invalid",),
                    )
                after = _capture_scope(
                    connection,
                    event=event_after,
                    delivery_dedupe_namespace=delivery_dedupe_namespace,
                )
                return _verify_notifier_dry_run_success(
                    connection,
                    event=event_after,
                    before=before,
                    after=after,
                    delivery_dedupe_namespace=delivery_dedupe_namespace,
                    render_result=render_result,
                )
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Consume one local/test DB notification.plan.created.v1 delivery replay-intent "
            "with the existing notifier dry-run render path, without live transport, Redis, "
            "workers, or upstream recomputation."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--notification-plan-created-event-id")
    parser.add_argument("--replay-request-id")
    parser.add_argument("--notification-plan-id")
    parser.add_argument("--source-fixture")
    parser.add_argument("--github-snapshot-fixture")
    parser.add_argument("--replay-namespace")
    parser.add_argument("--prepare-delivery-replay-intent-fixture", action="store_true")
    parser.add_argument(
        "--prepare-delivery-replay-request-fixture",
        action="store_true",
        dest="prepare_delivery_replay_intent_fixture",
    )
    parser.add_argument("--max-attempts")
    parser.add_argument("--confirm-local-test-db", action="store_true")
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    resolver: ReplayIntentResolver | None = None,
    executor: ReplayIntentNotifierDryRunExecutor | None = None,
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

    report["runner_internal_dry_run_mode"] = RUNNER_INTERNAL_DRY_RUN_MODE
    report["notifier_telegram_dry_run_env"] = _truthy(effective_env.get("NOTIFIER_TELEGRAM_DRY_RUN"))
    report["dry_run_mode_guard_passed"] = RUNNER_INTERNAL_DRY_RUN_MODE or report["notifier_telegram_dry_run_env"]
    if not report["dry_run_mode_guard_passed"]:
        checks_failed.append("dry_run_mode_required")

    enable_send = _string_or_none(effective_env.get("ENABLE_NOTIFICATION_SEND"))
    if enable_send is not None and not _falsey(enable_send):
        checks_failed.append("enable_notification_send_must_be_false")
    report["notification_send_disabled_or_unconfigured"] = enable_send is None or _falsey(enable_send)

    raw_event_id = _string_or_none(getattr(args, "notification_plan_created_event_id", None))
    raw_replay_request_id = _string_or_none(getattr(args, "replay_request_id", None))
    raw_plan_id = _string_or_none(getattr(args, "notification_plan_id", None))
    event_id = _uuid_or_none(raw_event_id)
    replay_request_id = _uuid_or_none(raw_replay_request_id)
    notification_plan_id = _uuid_or_none(raw_plan_id)
    if raw_event_id is not None and event_id is None:
        checks_failed.append("notification_plan_created_event_id_invalid")
    if raw_replay_request_id is not None and replay_request_id is None:
        checks_failed.append("replay_request_id_invalid")
    if raw_plan_id is not None and notification_plan_id is None:
        checks_failed.append("notification_plan_id_invalid")

    source_fixture = _path_or_none(getattr(args, "source_fixture", None))
    github_fixture = _path_or_none(getattr(args, "github_snapshot_fixture", None))
    replay_namespace = _string_or_none(getattr(args, "replay_namespace", None))
    prepare_fixture = bool(getattr(args, "prepare_delivery_replay_intent_fixture", False))
    fixture_selector_supplied = (
        source_fixture is not None or github_fixture is not None or replay_namespace is not None or prepare_fixture
    )
    fixture_selector_complete = (
        source_fixture is not None and github_fixture is not None and replay_namespace is not None and prepare_fixture
    )

    selector_modes = (
        int(raw_event_id is not None)
        + int(raw_replay_request_id is not None)
        + int(raw_plan_id is not None)
        + int(fixture_selector_supplied)
    )
    if selector_modes > 1:
        checks_failed.append("selector_mode_ambiguous")
    elif selector_modes == 0:
        checks_failed.append("selector_mode_required")
    elif fixture_selector_supplied and not fixture_selector_complete:
        checks_failed.append("fixture_selector_incomplete")

    selector_mode = _selector_mode(
        event_id=event_id,
        replay_request_id=replay_request_id,
        notification_plan_id=notification_plan_id,
        fixture_selector_supplied=fixture_selector_supplied,
    )

    max_attempts = _max_attempts_from_args_env(getattr(args, "max_attempts", None), effective_env)
    if fixture_selector_supplied and max_attempts is None:
        checks_failed.append("max_attempts_invalid")
        max_attempts = dispatch_runner.request_runner.dlq_runner.DEFAULT_MAX_ATTEMPTS

    if replay_namespace is not None:
        namespace_ok, namespace_failures = source_candidate_runner.validate_replay_namespace(replay_namespace)
        checks_failed.extend(namespace_failures)
        if not namespace_ok:
            replay_namespace = None

    if fixture_selector_supplied and source_fixture is not None and github_fixture is not None:
        try:
            source_candidate_runner.load_source_fixture(source_fixture, repo_root=root)
        except Exception:  # noqa: BLE001 - sanitized report only.
            checks_failed.append("source_fixture_load_failed")
        try:
            github_snapshot_runner.load_github_snapshot_fixture(github_fixture, repo_root=root)
        except Exception:  # noqa: BLE001 - sanitized report only.
            checks_failed.append("github_snapshot_fixture_load_failed")

    if checks_failed:
        return _finish(report, checks_failed)

    active_resolver = resolver or DefaultReplayIntentResolver()
    try:
        resolution = active_resolver.resolve(
            database_url=args.database_url,
            selector_mode=selector_mode,
            notification_plan_created_event_id=event_id,
            replay_request_id=replay_request_id,
            notification_plan_id=notification_plan_id,
            source_fixture_path=source_fixture,
            github_snapshot_fixture_path=github_fixture,
            replay_namespace=replay_namespace,
            max_attempts=max_attempts or dispatch_runner.request_runner.dlq_runner.DEFAULT_MAX_ATTEMPTS,
            env=effective_env,
            repo_root=root,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
        return _finish(report, [_safe_failure_code(exc)])

    report["replay_intent_fixture_prepared"] = resolution.replay_intent_fixture_prepared
    report["notification_plan_created_event_loaded"] = resolution.notification_plan_created_event_loaded
    checks_failed.extend(resolution.checks_failed)
    resolved_event_id = resolution.notification_plan_created_event_id
    if resolved_event_id is None:
        checks_failed.append("notification_plan_created_replay_intent_missing_or_invalid")
    if checks_failed:
        return _finish(report, checks_failed)

    delivery_dedupe_namespace = replay_namespace or _namespace_for_replay_intent_event(resolved_event_id)
    active_executor = executor or SqlAlchemyReplayIntentNotifierDryRunExecutor()
    try:
        execution = active_executor.execute(
            database_url=args.database_url,
            notification_plan_created_event_id=resolved_event_id,
            delivery_dedupe_namespace=delivery_dedupe_namespace,
            env=effective_env,
            repo_root=root,
        )
    except Exception as exc:  # noqa: BLE001 - sanitized operator result only.
        return _finish(report, [_safe_failure_code(exc)])

    _apply_execution(report, execution)
    checks_failed.extend(execution.checks_failed)

    for key in TRUE_RESULT_KEYS:
        if report.get(key) is not True:
            checks_failed.append(f"{key}:missing")
    for key in FALSE_RESULT_KEYS:
        if report.get(key) is not False:
            checks_failed.append(f"{key}:unexpected")

    return _finish(report, checks_failed)


def validate_database_url(database_url: str | None):
    return dispatch_runner.validate_database_url(database_url)


def _resolve_fixture_chain(
    *,
    database_url: str,
    source_fixture_path: Path,
    github_snapshot_fixture_path: Path,
    replay_namespace: str,
    max_attempts: int,
    env: Mapping[str, str],
    repo_root: Path,
) -> ReplayIntentResolutionResult:
    predecessor_env = dict(env)
    predecessor_env["APP_ENV"] = "test"
    predecessor = dispatch_runner.run(
        argparse.Namespace(
            database_url=database_url,
            replay_request_id=None,
            notification_plan_id=None,
            source_fixture=str(source_fixture_path),
            github_snapshot_fixture=str(github_snapshot_fixture_path),
            replay_namespace=replay_namespace,
            prepare_delivery_replay_request_fixture=True,
            max_attempts=str(max_attempts),
            confirm_local_test_db=True,
        ),
        env=predecessor_env,
        repo_root=repo_root,
    )
    if predecessor.exit_code != 0 or predecessor.report.get("status") != "pass":
        return _resolution_result(
            replay_intent_fixture_prepared=False,
            checks_failed=("operator_approved_dead_letter_fixture_failed",),
        )
    replay_request_id, failures = _find_fixture_replay_request_id(
        database_url=database_url,
        replay_namespace=replay_namespace,
    )
    if failures:
        return _resolution_result(replay_intent_fixture_prepared=True, checks_failed=failures)
    if replay_request_id is None:
        return _resolution_result(
            replay_intent_fixture_prepared=True,
            checks_failed=("replay_request_missing_or_invalid",),
        )
    resolved = _resolve_event_by_replay_request_id(database_url, replay_request_id)
    return ReplayIntentResolutionResult(
        notification_plan_created_event_id=resolved.notification_plan_created_event_id,
        notification_plan_created_event_loaded=resolved.notification_plan_created_event_loaded,
        replay_intent_fixture_prepared=True,
        checks_failed=resolved.checks_failed,
    )


def _find_fixture_replay_request_id(
    *,
    database_url: str,
    replay_namespace: str,
) -> tuple[UUID | None, tuple[str, ...]]:
    plan_ids = dispatch_runner.request_runner.dlq_runner.due_runner._find_fixture_notification_plan_ids_by_namespace(  # noqa: SLF001
        database_url=database_url,
        replay_namespace=replay_namespace,
    )
    if len(plan_ids) > 1:
        return None, ("fixture_notification_plan_ambiguous",)
    if len(plan_ids) != 1:
        return None, ("fixture_notification_plan_missing_or_invalid",)
    return dispatch_runner._find_fixture_replay_request_id_for_plan(  # noqa: SLF001
        database_url=database_url,
        notification_plan_id=plan_ids[0],
    )


def _resolve_event_by_id(database_url: str, event_id: UUID) -> ReplayIntentResolutionResult:
    return _resolve_one_event(
        database_url=database_url,
        event_ids=[event_id],
        ambiguous_code="notification_plan_created_replay_intent_ambiguous",
        missing_code="notification_plan_created_replay_intent_missing_or_invalid",
    )


def _resolve_event_by_replay_request_id(database_url: str, replay_request_id: UUID) -> ReplayIntentResolutionResult:
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
                      AND payload_json ->> 'replay_request_id' = :replay_request_id
                    ORDER BY created_at, event_id
                    """
                ),
                {
                    "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                    "replay_request_id": str(replay_request_id),
                },
            ).scalars().all()
    finally:
        engine.dispose()
    return _resolve_one_event(
        database_url=database_url,
        event_ids=[UUID(str(row)) for row in rows],
        ambiguous_code="notification_plan_created_replay_intent_ambiguous",
        missing_code="notification_plan_created_replay_intent_missing_or_invalid",
    )


def _resolve_event_by_notification_plan_id(database_url: str, notification_plan_id: UUID) -> ReplayIntentResolutionResult:
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
                      AND payload_json ->> 'notification_plan_id' = :notification_plan_id
                      AND payload_json ->> 'replay_request_id' IS NOT NULL
                    ORDER BY created_at, event_id
                    """
                ),
                {
                    "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                    "notification_plan_id": str(notification_plan_id),
                },
            ).scalars().all()
    finally:
        engine.dispose()
    return _resolve_one_event(
        database_url=database_url,
        event_ids=[UUID(str(row)) for row in rows],
        ambiguous_code="notification_plan_created_replay_intent_ambiguous",
        missing_code="notification_plan_created_replay_intent_missing_or_invalid",
    )


def _resolve_one_event(
    *,
    database_url: str,
    event_ids: Sequence[UUID],
    ambiguous_code: str,
    missing_code: str,
) -> ReplayIntentResolutionResult:
    if len(event_ids) > 1:
        return _resolution_result(checks_failed=(ambiguous_code,))
    if len(event_ids) != 1:
        return _resolution_result(checks_failed=(missing_code,))

    import sqlalchemy as sa

    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.begin() as connection:
            event, failures = _load_replay_intent_event_by_id(connection, event_id=event_ids[0])
    finally:
        engine.dispose()
    if event is None:
        return _resolution_result(checks_failed=failures or (missing_code,))
    return ReplayIntentResolutionResult(
        notification_plan_created_event_id=event.event_id,
        notification_plan_created_event_loaded=True,
    )


def _load_replay_intent_event_by_id(
    connection: Any,
    *,
    event_id: UUID,
) -> tuple[ReplayIntentEvent | None, tuple[str, ...]]:
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
    if row is None or str(row["event_type"]) != NOTIFICATION_PLAN_CREATED_EVENT_TYPE:
        return None, ("notification_plan_created_replay_intent_missing_or_invalid",)
    payload = _json_loads(row["payload_json"])
    if not isinstance(payload, dict):
        return None, ("notification_plan_created_replay_intent_payload_invalid",)
    if _replay_intent_payload_missing_required_key(payload):
        return None, ("notification_plan_created_replay_intent_payload_invalid",)
    replay_request_id = _uuid_or_none(payload.get("replay_request_id"))
    if replay_request_id is None:
        return None, ("notification_plan_created_replay_intent_payload_invalid",)
    valid, _failures = render_runner.notifier_base.validate_notification_intent_payload(payload)
    if not valid:
        return None, ("notification_plan_created_replay_intent_payload_invalid",)
    intent = render_runner.notifier_base.notification_plan_intent_from_payload(payload)
    aggregate_type = _string_or_none(row["aggregate_type"])
    aggregate_id = _uuid_or_none(row["aggregate_id"])
    if aggregate_type != "analysis" or aggregate_id != intent.analysis_id:
        return None, ("notification_plan_created_event_aggregate_mismatch",)
    return (
        ReplayIntentEvent(
            event_id=UUID(str(row["event_id"])),
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            payload_json=payload,
            intent=intent,
            replay_request_id=replay_request_id,
        ),
        (),
    )


def _validate_required_source_rows(connection: Any, *, event: ReplayIntentEvent) -> dict[str, Any]:
    analysis = render_runner.notifier_base._load_analysis(connection, event.intent.analysis_id)  # noqa: SLF001
    analysis_loaded = analysis is not None
    judge_output = (
        render_runner.notifier_base._load_judge_output(connection, analysis.judge_output_id)  # noqa: SLF001
        if analysis is not None
        else None
    )
    candidate = render_runner.notifier_base._load_candidate_context(  # noqa: SLF001
        connection,
        event.intent.candidate_group_id,
    )
    context = _load_candidate_scope_context(connection, event.intent.candidate_group_id)
    artifact_loaded = _artifact_exists(connection, artifact_id=context["current_primary_artifact_id"])
    source_message_loaded = _source_message_exists(connection, source_message_id=context["source_message_id"])

    failures: list[str] = []
    if analysis is None:
        failures.append("analysis_missing")
    if judge_output is None:
        failures.append("judge_output_missing")
    if candidate is None:
        failures.append("candidate_group_missing")
    if not artifact_loaded:
        failures.append("primary_artifact_missing")
    if not source_message_loaded:
        failures.append("source_message_missing")
    return {
        "analysis_loaded": analysis_loaded,
        "judge_output_loaded": judge_output is not None,
        "candidate_group_loaded": candidate is not None,
        "artifact_loaded": artifact_loaded,
        "source_message_loaded": source_message_loaded,
        "checks_failed": tuple(failures),
    }


def _capture_scope(
    connection: Any,
    *,
    event: ReplayIntentEvent,
    delivery_dedupe_namespace: str,
) -> dict[str, Any]:
    context = _load_candidate_scope_context(connection, event.intent.candidate_group_id)
    return {
        "notification_plan_created_replay_intents": _replay_intent_event_count(
            connection,
            replay_request_id=event.replay_request_id,
        ),
        "notification_renders": _notification_render_count_for_event(
            connection,
            event=event,
        ),
        "dry_run_delivery_records": _dry_run_delivery_record_count(
            connection,
            notification_plan_id=event.intent.notification_plan_id,
        ),
        "delivery_result_events": _delivery_result_event_count(
            connection,
            notification_plan_id=event.intent.notification_plan_id,
            delivery_dedupe_namespace=delivery_dedupe_namespace,
        ),
        "replay_requests": _replay_request_count(connection, replay_request_id=event.replay_request_id),
        "replay_requested_events": _replay_requested_event_count(
            connection,
            replay_request_id=event.replay_request_id,
        ),
        "replay_request_digest": _replay_request_digest(connection, replay_request_id=event.replay_request_id),
        "analysis_digest": render_runner.notifier_base._load_analysis_digest(  # noqa: SLF001
            connection,
            event.intent.analysis_id,
        ),
        "judge_output_digest": render_runner.notifier_base._load_judge_output_digest_by_analysis(  # noqa: SLF001
            connection,
            event.intent.analysis_id,
        ),
        "candidate_group_digest": render_runner.notifier_base._load_candidate_digest(  # noqa: SLF001
            connection,
            event.intent.candidate_group_id,
        ),
        "evidence_bundle_digest": _evidence_bundle_digest(connection, bundle_id=context["current_bundle_id"]),
        "artifact_digest": _artifact_digest(connection, artifact_id=context["current_primary_artifact_id"]),
        "source_message_digest": _source_message_digest(connection, source_message_id=context["source_message_id"]),
    }


def _verify_notifier_dry_run_success(
    connection: Any,
    *,
    event: ReplayIntentEvent,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    delivery_dedupe_namespace: str,
    render_result: render_runner.RunnerResult,
) -> ReplayIntentNotifierDryRunExecutionResult:
    checks_failed: list[str] = []
    render_acceptable = _render_fixture_result_acceptable(render_result)
    if not render_acceptable:
        checks_failed.append("notification_render_dry_run_fixture_failed")

    source_checks = _validate_required_source_rows(connection, event=event)
    render_delta = int(after["notification_renders"]) - int(before["notification_renders"])
    dry_run_delta = int(after["dry_run_delivery_records"]) - int(before["dry_run_delivery_records"])
    delivery_event_delta = int(after["delivery_result_events"]) - int(before["delivery_result_events"])
    replay_intent_delta = (
        int(after["notification_plan_created_replay_intents"])
        - int(before["notification_plan_created_replay_intents"])
    )
    replay_request_delta = int(after["replay_requests"]) - int(before["replay_requests"])
    replay_requested_delta = int(after["replay_requested_events"]) - int(before["replay_requested_events"])

    render_bounded = int(after["notification_renders"]) == 1 and render_delta in {0, 1}
    dry_run_bounded = int(after["dry_run_delivery_records"]) == 1 and dry_run_delta in {0, 1}
    delivery_event_bounded = int(after["delivery_result_events"]) == 1 and delivery_event_delta in {0, 1}
    delivery_result_matches = _delivery_result_matches_dry_run_record(
        connection,
        event=event,
        delivery_dedupe_namespace=delivery_dedupe_namespace,
    )
    notification_plan_loaded = render_result.report.get("notification_plan_concretized") is True

    checks = {
        "notification_plan_created_event_loaded": int(after["notification_plan_created_replay_intents"]) == 1,
        "notification_plan_loaded_or_concretized": notification_plan_loaded,
        "analysis_loaded": source_checks["analysis_loaded"],
        "judge_output_loaded": source_checks["judge_output_loaded"],
        "candidate_group_loaded": source_checks["candidate_group_loaded"],
        "artifact_loaded": source_checks["artifact_loaded"],
        "source_message_loaded": source_checks["source_message_loaded"],
        "notification_render_created_or_reused": render_acceptable and render_bounded,
        "dry_run_delivery_record_created_or_reused": render_acceptable and dry_run_bounded,
        "notification_delivery_result_event_created_or_reused": render_acceptable and delivery_event_bounded,
        "notification_render_dedupe_stable": render_bounded,
        "dry_run_delivery_record_dedupe_stable": dry_run_bounded,
        "delivery_result_event_dedupe_stable": delivery_event_bounded,
        "delivery_result_matches_dry_run_record": delivery_result_matches,
        "notification_plan_created_replay_intent_created": replay_intent_delta > 0,
        "replay_request_created": replay_request_delta > 0,
        "replay_requested_event_created": replay_requested_delta > 0,
        "replay_request_status_mutated": after["replay_request_digest"] != before["replay_request_digest"],
        "analysis_mutated": after["analysis_digest"] != before["analysis_digest"],
        "judge_output_mutated": after["judge_output_digest"] != before["judge_output_digest"],
        "candidate_group_mutated": after["candidate_group_digest"] != before["candidate_group_digest"],
        "evidence_bundle_mutated": after["evidence_bundle_digest"] != before["evidence_bundle_digest"],
        "artifact_mutated": after["artifact_digest"] != before["artifact_digest"],
        "source_message_mutated": after["source_message_digest"] != before["source_message_digest"],
        "openai_called": render_result.report.get("openai_called") is True,
        "telegram_called": render_result.report.get("telegram_called") is True,
        "live_github_called": render_result.report.get("live_github_called") is True,
        "workers_started": render_result.report.get("workers_started") is True,
        "redis_mutation": render_result.report.get("redis_mutation") is True,
        "production_db_write": render_result.report.get("production_db_write") is True,
        "alembic_or_ddl_ran": render_result.report.get("alembic_or_ddl_ran") is True,
    }
    checks["real_transport_attempted"] = checks["telegram_called"] is True

    for key in TRUE_RESULT_KEYS:
        if key != "database_url_guard_passed" and key != "dry_run_mode_guard_passed" and checks.get(key) is not True:
            checks_failed.append(f"{key}:missing")
    for key in FALSE_RESULT_KEYS:
        if checks.get(key) is not False:
            checks_failed.append(f"{key}:unexpected")
    checks_failed.extend(source_checks["checks_failed"])

    return ReplayIntentNotifierDryRunExecutionResult(
        notification_plan_created_event_loaded=checks["notification_plan_created_event_loaded"],
        notification_plan_loaded_or_concretized=checks["notification_plan_loaded_or_concretized"],
        analysis_loaded=checks["analysis_loaded"],
        judge_output_loaded=checks["judge_output_loaded"],
        candidate_group_loaded=checks["candidate_group_loaded"],
        artifact_loaded=checks["artifact_loaded"],
        source_message_loaded=checks["source_message_loaded"],
        notification_render_created_or_reused=checks["notification_render_created_or_reused"],
        dry_run_delivery_record_created_or_reused=checks["dry_run_delivery_record_created_or_reused"],
        notification_delivery_result_event_created_or_reused=checks[
            "notification_delivery_result_event_created_or_reused"
        ],
        notification_render_dedupe_stable=checks["notification_render_dedupe_stable"],
        dry_run_delivery_record_dedupe_stable=checks["dry_run_delivery_record_dedupe_stable"],
        delivery_result_event_dedupe_stable=checks["delivery_result_event_dedupe_stable"],
        delivery_result_matches_dry_run_record=checks["delivery_result_matches_dry_run_record"],
        notification_render_count_before=int(before["notification_renders"]),
        notification_render_count_after=int(after["notification_renders"]),
        dry_run_delivery_record_count_before=int(before["dry_run_delivery_records"]),
        dry_run_delivery_record_count_after=int(after["dry_run_delivery_records"]),
        notification_delivery_result_event_count_before=int(before["delivery_result_events"]),
        notification_delivery_result_event_count_after=int(after["delivery_result_events"]),
        notification_plan_created_replay_intent_count_before=int(before["notification_plan_created_replay_intents"]),
        notification_plan_created_replay_intent_count_after=int(after["notification_plan_created_replay_intents"]),
        replay_request_count_before=int(before["replay_requests"]),
        replay_request_count_after=int(after["replay_requests"]),
        replay_requested_event_count_before=int(before["replay_requested_events"]),
        replay_requested_event_count_after=int(after["replay_requested_events"]),
        telegram_called=checks["telegram_called"],
        openai_called=checks["openai_called"],
        redis_mutation=checks["redis_mutation"],
        live_github_called=checks["live_github_called"],
        workers_started=checks["workers_started"],
        production_db_write=checks["production_db_write"],
        alembic_or_ddl_ran=checks["alembic_or_ddl_ran"],
        real_transport_attempted=checks["real_transport_attempted"],
        notification_plan_created_replay_intent_created=checks["notification_plan_created_replay_intent_created"],
        replay_request_created=checks["replay_request_created"],
        replay_requested_event_created=checks["replay_requested_event_created"],
        replay_request_status_mutated=checks["replay_request_status_mutated"],
        analysis_mutated=checks["analysis_mutated"],
        judge_output_mutated=checks["judge_output_mutated"],
        candidate_group_mutated=checks["candidate_group_mutated"],
        evidence_bundle_mutated=checks["evidence_bundle_mutated"],
        artifact_mutated=checks["artifact_mutated"],
        source_message_mutated=checks["source_message_mutated"],
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _render_fixture_result_acceptable(result: render_runner.RunnerResult) -> bool:
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
    expected_shape = all(result.report.get(key) is True for key in expected_true) and all(
        result.report.get(key) is False for key in expected_false
    )
    if not expected_shape:
        return False
    if result.exit_code == 0 and result.report.get("status") == "pass":
        return True
    allowed_replay_intent_failures = {"notification_plan_intent_mismatch"}
    checks_failed = set(result.report.get("checks_failed") or [])
    return bool(checks_failed) and checks_failed.issubset(allowed_replay_intent_failures)


def _delivery_result_matches_dry_run_record(
    connection: Any,
    *,
    event: ReplayIntentEvent,
    delivery_dedupe_namespace: str,
) -> bool:
    import sqlalchemy as sa

    rows = connection.execute(
        sa.text(
            """
            SELECT payload_json
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_type = 'notification_plan'
              AND aggregate_id = CAST(:notification_plan_id AS uuid)
              AND dedupe_key LIKE :dedupe_prefix
            ORDER BY created_at, event_id
            LIMIT 2
            """
        ),
        {
            "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
            "notification_plan_id": str(event.intent.notification_plan_id),
            "dedupe_prefix": _delivery_result_dedupe_prefix(delivery_dedupe_namespace),
        },
    ).mappings().all()
    if len(rows) != 1:
        return False
    payload = _json_loads(rows[0]["payload_json"]) or {}
    if not isinstance(payload, dict):
        return False
    delivery_record_id = _uuid_or_none(payload.get("notification_delivery_record_id"))
    if delivery_record_id is None:
        return False
    if payload.get("delivery_status") != "suppressed":
        return False
    if payload.get("dry_run") is not True or payload.get("transport_skipped") is not True:
        return False
    attempt_count = payload.get("attempt_count")
    if payload.get("telegram_message_id") is not None or attempt_count is None or int(attempt_count) != 0:
        return False
    return _dry_run_delivery_record_exists(
        connection,
        notification_plan_id=event.intent.notification_plan_id,
        notification_delivery_record_id=delivery_record_id,
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


def _artifact_exists(connection: Any, *, artifact_id: UUID | None) -> bool:
    if artifact_id is None:
        return False
    import sqlalchemy as sa

    return bool(
        connection.execute(
            sa.text(
                """
                SELECT 1
                FROM artifact_registry
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                LIMIT 1
                """
            ),
            {"artifact_id": str(artifact_id)},
        ).scalar_one_or_none()
    )


def _source_message_exists(connection: Any, *, source_message_id: UUID | None) -> bool:
    if source_message_id is None:
        return False
    import sqlalchemy as sa

    return bool(
        connection.execute(
            sa.text(
                """
                SELECT 1
                FROM source_messages
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                LIMIT 1
                """
            ),
            {"source_message_id": str(source_message_id)},
        ).scalar_one_or_none()
    )


def _notification_render_count_for_event(connection: Any, *, event: ReplayIntentEvent) -> int:
    analysis = render_runner.notifier_base._load_analysis(connection, event.intent.analysis_id)  # noqa: SLF001
    if analysis is None:
        return 0
    judge_output = render_runner.notifier_base._load_judge_output(connection, analysis.judge_output_id)  # noqa: SLF001
    candidate = render_runner.notifier_base._load_candidate_context(  # noqa: SLF001
        connection,
        event.intent.candidate_group_id,
    )
    if judge_output is None or candidate is None:
        return 0
    render = render_runner.build_notification_render(
        intent=event.intent,
        analysis=analysis,
        judge_output=judge_output,
        candidate=candidate,
    )
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_renders
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND render_hash = :render_hash
                """
            ),
            {
                "notification_plan_id": str(event.intent.notification_plan_id),
                "render_hash": render.render_hash,
            },
        ).scalar_one()
    )


def _dry_run_delivery_record_count(connection: Any, *, notification_plan_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_delivery_records
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND delivery_status = 'suppressed'::notification_status_enum
                  AND telegram_message_id IS NULL
                  AND attempt_count = 0
                  AND transport_error_code = :transport_error_code
                  AND telegram_response_json @> CAST(:telegram_response_json AS jsonb)
                """
            ),
            {
                "notification_plan_id": str(notification_plan_id),
                "transport_error_code": render_runner.DRY_RUN_REASON_CODE,
                "telegram_response_json": _json_dumps(
                    {"noop": True, "dry_run": True, "reason_code": render_runner.DRY_RUN_REASON_CODE}
                ),
            },
        ).scalar_one()
    )


def _dry_run_delivery_record_exists(
    connection: Any,
    *,
    notification_plan_id: UUID,
    notification_delivery_record_id: UUID,
) -> bool:
    import sqlalchemy as sa

    return bool(
        connection.execute(
            sa.text(
                """
                SELECT 1
                FROM notification_delivery_records
                WHERE notification_delivery_record_id = CAST(:notification_delivery_record_id AS uuid)
                  AND notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND delivery_status = 'suppressed'::notification_status_enum
                  AND telegram_message_id IS NULL
                  AND attempt_count = 0
                  AND transport_error_code = :transport_error_code
                  AND telegram_response_json @> CAST(:telegram_response_json AS jsonb)
                LIMIT 1
                """
            ),
            {
                "notification_delivery_record_id": str(notification_delivery_record_id),
                "notification_plan_id": str(notification_plan_id),
                "transport_error_code": render_runner.DRY_RUN_REASON_CODE,
                "telegram_response_json": _json_dumps(
                    {"noop": True, "dry_run": True, "reason_code": render_runner.DRY_RUN_REASON_CODE}
                ),
            },
        ).scalar_one_or_none()
    )


def _delivery_result_event_count(
    connection: Any,
    *,
    notification_plan_id: UUID,
    delivery_dedupe_namespace: str,
) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = :event_type
                  AND aggregate_type = 'notification_plan'
                  AND aggregate_id = CAST(:notification_plan_id AS uuid)
                  AND dedupe_key LIKE :dedupe_prefix
                """
            ),
            {
                "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
                "notification_plan_id": str(notification_plan_id),
                "dedupe_prefix": _delivery_result_dedupe_prefix(delivery_dedupe_namespace),
            },
        ).scalar_one()
    )


def _delivery_result_dedupe_prefix(delivery_dedupe_namespace: str) -> str:
    return f"local-db-notification-render-dry-run:{delivery_dedupe_namespace}:notification.delivery.result:%"


def _replay_intent_event_count(connection: Any, *, replay_request_id: UUID) -> int:
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


def _replay_request_count(connection: Any, *, replay_request_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM replay_requests
                WHERE replay_request_id = CAST(:replay_request_id AS uuid)
                """
            ),
            {"replay_request_id": str(replay_request_id)},
        ).scalar_one()
    )


def _replay_requested_event_count(connection: Any, *, replay_request_id: UUID) -> int:
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
                """
            ),
            {
                "event_type": REPLAY_REQUESTED_EVENT_TYPE,
                "replay_request_id": str(replay_request_id),
            },
        ).scalar_one()
    )


def _replay_request_digest(connection: Any, *, replay_request_id: UUID) -> str:
    import sqlalchemy as sa

    return str(
        connection.execute(
            sa.text(
                """
                SELECT COALESCE(md5(to_jsonb(rr)::text), '')
                FROM replay_requests AS rr
                WHERE rr.replay_request_id = CAST(:replay_request_id AS uuid)
                """
            ),
            {"replay_request_id": str(replay_request_id)},
        ).scalar_one_or_none()
        or ""
    )


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


def _apply_execution(report: dict[str, Any], execution: ReplayIntentNotifierDryRunExecutionResult) -> None:
    report.update(
        {
            "notification_plan_created_event_loaded": execution.notification_plan_created_event_loaded,
            "notification_plan_loaded_or_concretized": execution.notification_plan_loaded_or_concretized,
            "analysis_loaded": execution.analysis_loaded,
            "judge_output_loaded": execution.judge_output_loaded,
            "candidate_group_loaded": execution.candidate_group_loaded,
            "artifact_loaded": execution.artifact_loaded,
            "source_message_loaded": execution.source_message_loaded,
            "notification_render_created_or_reused": execution.notification_render_created_or_reused,
            "dry_run_delivery_record_created_or_reused": execution.dry_run_delivery_record_created_or_reused,
            "notification_delivery_result_event_created_or_reused": (
                execution.notification_delivery_result_event_created_or_reused
            ),
            "notification_render_dedupe_stable": execution.notification_render_dedupe_stable,
            "dry_run_delivery_record_dedupe_stable": execution.dry_run_delivery_record_dedupe_stable,
            "delivery_result_event_dedupe_stable": execution.delivery_result_event_dedupe_stable,
            "delivery_result_matches_dry_run_record": execution.delivery_result_matches_dry_run_record,
            "notification_render_count_before": execution.notification_render_count_before,
            "notification_render_count_after": execution.notification_render_count_after,
            "dry_run_delivery_record_count_before": execution.dry_run_delivery_record_count_before,
            "dry_run_delivery_record_count_after": execution.dry_run_delivery_record_count_after,
            "notification_delivery_result_event_count_before": execution.notification_delivery_result_event_count_before,
            "notification_delivery_result_event_count_after": execution.notification_delivery_result_event_count_after,
            "notification_plan_created_replay_intent_count_before": (
                execution.notification_plan_created_replay_intent_count_before
            ),
            "notification_plan_created_replay_intent_count_after": (
                execution.notification_plan_created_replay_intent_count_after
            ),
            "replay_request_count_before": execution.replay_request_count_before,
            "replay_request_count_after": execution.replay_request_count_after,
            "replay_requested_event_count_before": execution.replay_requested_event_count_before,
            "replay_requested_event_count_after": execution.replay_requested_event_count_after,
            "telegram_called": execution.telegram_called,
            "openai_called": execution.openai_called,
            "redis_mutation": execution.redis_mutation,
            "live_github_called": execution.live_github_called,
            "workers_started": execution.workers_started,
            "production_db_write": execution.production_db_write,
            "alembic_or_ddl_ran": execution.alembic_or_ddl_ran,
            "real_transport_attempted": execution.real_transport_attempted,
            "notification_plan_created_replay_intent_created": (
                execution.notification_plan_created_replay_intent_created
            ),
            "replay_request_created": execution.replay_request_created,
            "replay_requested_event_created": execution.replay_requested_event_created,
            "replay_request_status_mutated": execution.replay_request_status_mutated,
            "analysis_mutated": execution.analysis_mutated,
            "judge_output_mutated": execution.judge_output_mutated,
            "candidate_group_mutated": execution.candidate_group_mutated,
            "evidence_bundle_mutated": execution.evidence_bundle_mutated,
            "artifact_mutated": execution.artifact_mutated,
            "source_message_mutated": execution.source_message_mutated,
        }
    )


def _base_report() -> dict[str, Any]:
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "dry_run_mode_guard_passed": False,
        "runner_internal_dry_run_mode": RUNNER_INTERNAL_DRY_RUN_MODE,
        "notifier_telegram_dry_run_env": False,
        "notification_send_disabled_or_unconfigured": False,
        "replay_intent_fixture_prepared": False,
        "notification_render_count_before": 0,
        "notification_render_count_after": 0,
        "dry_run_delivery_record_count_before": 0,
        "dry_run_delivery_record_count_after": 0,
        "notification_delivery_result_event_count_before": 0,
        "notification_delivery_result_event_count_after": 0,
        "notification_plan_created_replay_intent_count_before": 0,
        "notification_plan_created_replay_intent_count_after": 0,
        "replay_request_count_before": 0,
        "replay_request_count_after": 0,
        "replay_requested_event_count_before": 0,
        "replay_requested_event_count_after": 0,
        "checks_failed": [],
    }
    for key in TRUE_RESULT_KEYS:
        if key not in report:
            report[key] = False
    for key in FALSE_RESULT_KEYS:
        report[key] = False
    return report


def _execution_result(
    *,
    notification_plan_created_event_loaded: bool = False,
    notification_plan_loaded_or_concretized: bool = False,
    analysis_loaded: bool = False,
    judge_output_loaded: bool = False,
    candidate_group_loaded: bool = False,
    artifact_loaded: bool = False,
    source_message_loaded: bool = False,
    notification_render_created_or_reused: bool = False,
    dry_run_delivery_record_created_or_reused: bool = False,
    notification_delivery_result_event_created_or_reused: bool = False,
    notification_render_dedupe_stable: bool = False,
    dry_run_delivery_record_dedupe_stable: bool = False,
    delivery_result_event_dedupe_stable: bool = False,
    delivery_result_matches_dry_run_record: bool = False,
    checks_failed: Sequence[str],
) -> ReplayIntentNotifierDryRunExecutionResult:
    return ReplayIntentNotifierDryRunExecutionResult(
        notification_plan_created_event_loaded=notification_plan_created_event_loaded,
        notification_plan_loaded_or_concretized=notification_plan_loaded_or_concretized,
        analysis_loaded=analysis_loaded,
        judge_output_loaded=judge_output_loaded,
        candidate_group_loaded=candidate_group_loaded,
        artifact_loaded=artifact_loaded,
        source_message_loaded=source_message_loaded,
        notification_render_created_or_reused=notification_render_created_or_reused,
        dry_run_delivery_record_created_or_reused=dry_run_delivery_record_created_or_reused,
        notification_delivery_result_event_created_or_reused=notification_delivery_result_event_created_or_reused,
        notification_render_dedupe_stable=notification_render_dedupe_stable,
        dry_run_delivery_record_dedupe_stable=dry_run_delivery_record_dedupe_stable,
        delivery_result_event_dedupe_stable=delivery_result_event_dedupe_stable,
        delivery_result_matches_dry_run_record=delivery_result_matches_dry_run_record,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _resolution_result(
    *,
    replay_intent_fixture_prepared: bool = False,
    checks_failed: Sequence[str],
) -> ReplayIntentResolutionResult:
    return ReplayIntentResolutionResult(
        notification_plan_created_event_id=None,
        notification_plan_created_event_loaded=False,
        replay_intent_fixture_prepared=replay_intent_fixture_prepared,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _selector_mode(
    *,
    event_id: UUID | None,
    replay_request_id: UUID | None,
    notification_plan_id: UUID | None,
    fixture_selector_supplied: bool,
) -> str:
    if event_id is not None:
        return "notification_plan_created_event"
    if replay_request_id is not None:
        return "replay_request"
    if notification_plan_id is not None:
        return "notification_plan"
    if fixture_selector_supplied:
        return "fixture_chain"
    return "none"


def _max_attempts_from_args_env(value: Any, env: Mapping[str, str]) -> int | None:
    raw = _string_or_none(value)
    if raw is None:
        raw = _string_or_none(env.get("NOTIFICATION_RETRY_MAX_ATTEMPTS"))
    if raw is None:
        raw = _string_or_none(env.get("DELIVERY_RETRY_MAX_ATTEMPTS"))
    if raw is None:
        raw = str(dispatch_runner.request_runner.dlq_runner.DEFAULT_MAX_ATTEMPTS)
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _replay_intent_payload_missing_required_key(payload: Mapping[str, Any]) -> bool:
    for key in REQUIRED_REPLAY_INTENT_PAYLOAD_KEYS:
        if key not in payload:
            return True
        if payload.get(key) is None:
            return True
        if isinstance(payload.get(key), str) and not str(payload.get(key)).strip():
            return True
    return False


def _namespace_for_replay_intent_event(event_id: UUID) -> str:
    return f"delivery-replay-intent-{event_id}"


def _truthy(value: Any) -> bool:
    text = _string_or_none(value)
    return text is not None and text.lower() in {"1", "true", "yes", "on"}


def _falsey(value: Any) -> bool:
    text = _string_or_none(value)
    return text is not None and text.lower() in {"0", "false", "no", "off"}


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
    return dispatch_runner._uuid_or_none(value)  # noqa: SLF001


def _path_or_none(value: Any) -> Path | None:
    return dispatch_runner._path_or_none(value)  # noqa: SLF001


def _string_or_none(value: Any) -> str | None:
    return dispatch_runner._string_or_none(value)  # noqa: SLF001


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc)
    if message in SAFE_EXCEPTION_MESSAGES:
        return message
    return exc.__class__.__name__


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main(argv: Sequence[str] | None = None) -> int:
    result = run(build_parser().parse_args(argv))
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
