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

from tools import local_db_maintenance_due_failed_retryable_retry_intent_fixture_runner as due_runner
from tools import local_db_notification_plan_created_render_dry_run_fixture_runner as render_runner


SCHEMA_VERSION = "local_db_maintenance_retry_intent_notification_render_dry_run_fixture_runner_v1"
NOTIFICATION_PLAN_CREATED_EVENT_TYPE = due_runner.NOTIFICATION_PLAN_CREATED_EVENT_TYPE
NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE = render_runner.NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE
RETRY_REASON = due_runner.RETRY_REASON
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "retry_intent_event_found",
    "retry_intent_event_is_due_retry_promotion",
    "retry_intent_payload_matches_plan",
    "notifier_retry_intent_rehydrated",
    "same_notification_plan_reused",
    "notification_render_created_or_reused",
    "dry_run_delivery_record_created",
    "notification_delivery_result_event_created",
    "delivery_result_matches_retry_dry_run_record",
)
EXECUTION_TRUE_RESULT_KEYS = tuple(key for key in TRUE_RESULT_KEYS if key != "database_url_guard_passed")
FALSE_RESULT_KEYS = (
    "replay_request_created",
    "dead_letter_created",
    "notification_plan_mutated_by_maintenance",
    "notification_delivery_record_mutated_by_maintenance",
    "analysis_mutated",
    "judge_output_mutated",
    "candidate_group_mutated",
    "evidence_bundle_mutated",
    "artifact_mutated",
    "source_message_mutated",
    "verdict_recomputed",
    "delivery_decision_overridden",
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
    "latest_failed_retryable_delivery_record_missing_or_invalid",
    "retry_intent_event_ambiguous",
    "retry_intent_event_missing_or_invalid",
    "retry_intent_payload_mismatch",
    "notification_render_dry_run_fixture_failed",
    "notification_delivery_result_event_missing_or_invalid",
}

source_candidate_runner = due_runner.source_candidate_runner
github_snapshot_runner = due_runner.github_snapshot_runner


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class RetryIntentResolutionResult:
    notification_plan_created_event_id: UUID | None
    retry_intent_event_found: bool
    retry_intent_event_count_before: int = 0
    retry_intent_event_count_after: int = 0
    replay_request_created: bool = False
    dead_letter_created: bool = False
    notification_plan_mutated_by_maintenance: bool = False
    notification_delivery_record_mutated_by_maintenance: bool = False
    analysis_mutated: bool = False
    judge_output_mutated: bool = False
    candidate_group_mutated: bool = False
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RetryIntentEvent:
    event_id: UUID
    aggregate_type: str | None
    aggregate_id: UUID | None
    payload_json: dict[str, Any]
    intent: render_runner.notifier_base.NotificationPlanIntent


@dataclass(frozen=True, slots=True)
class RetryIntentRenderExecutionResult:
    retry_intent_event_found: bool
    retry_intent_event_is_due_retry_promotion: bool
    retry_intent_payload_matches_plan: bool
    notifier_retry_intent_rehydrated: bool
    same_notification_plan_reused: bool
    notification_render_created_or_reused: bool
    dry_run_delivery_record_created: bool
    notification_delivery_result_event_created: bool
    delivery_result_matches_retry_dry_run_record: bool
    retry_intent_event_count_before: int = 0
    retry_intent_event_count_after: int = 0
    notification_render_count_before: int = 0
    notification_render_count_after: int = 0
    dry_run_delivery_record_count_before: int = 0
    dry_run_delivery_record_count_after: int = 0
    notification_delivery_result_event_count_before: int = 0
    notification_delivery_result_event_count_after: int = 0
    replay_request_created: bool = False
    dead_letter_created: bool = False
    analysis_mutated: bool = False
    judge_output_mutated: bool = False
    candidate_group_mutated: bool = False
    evidence_bundle_mutated: bool = False
    artifact_mutated: bool = False
    source_message_mutated: bool = False
    verdict_recomputed: bool = False
    delivery_decision_overridden: bool = False
    openai_called: bool = False
    telegram_called: bool = False
    live_github_called: bool = False
    workers_started: bool = False
    redis_mutation: bool = False
    production_db_write: bool = False
    alembic_or_ddl_ran: bool = False
    checks_failed: tuple[str, ...] = ()


class RetryIntentResolver(Protocol):
    def resolve(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> RetryIntentResolutionResult: ...


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


class RetryIntentRenderExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
        delivery_dedupe_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> RetryIntentRenderExecutionResult: ...


class DefaultDueRetryIntentResolver:
    def resolve(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> RetryIntentResolutionResult:
        args = argparse.Namespace(
            database_url=database_url,
            notification_plan_id=None,
            source_fixture=str(source_fixture_path),
            github_snapshot_fixture=str(github_snapshot_fixture_path),
            replay_namespace=replay_namespace,
            prepare_failed_retryable_fixture=True,
            confirm_local_test_db=True,
        )
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        predecessor = due_runner.run(args, env=predecessor_env, repo_root=repo_root)
        if predecessor.exit_code != 0 or predecessor.report.get("status") != "pass":
            return RetryIntentResolutionResult(
                notification_plan_created_event_id=None,
                retry_intent_event_found=False,
                checks_failed=("due_retry_intent_fixture_failed",),
            )

        event_id, failures = _find_retry_intent_event_id_for_fixture_chain(
            database_url=database_url,
            replay_namespace=replay_namespace,
        )
        return RetryIntentResolutionResult(
            notification_plan_created_event_id=event_id,
            retry_intent_event_found=event_id is not None,
            retry_intent_event_count_before=int(predecessor.report.get("retry_intent_event_count_before") or 0),
            retry_intent_event_count_after=int(predecessor.report.get("retry_intent_event_count_after") or 0),
            replay_request_created=predecessor.report.get("replay_request_created") is True,
            dead_letter_created=predecessor.report.get("dead_letter_created") is True,
            notification_plan_mutated_by_maintenance=(
                predecessor.report.get("notification_plan_mutated_by_maintenance") is True
            ),
            notification_delivery_record_mutated_by_maintenance=(
                predecessor.report.get("notification_delivery_record_mutated_by_maintenance") is True
            ),
            analysis_mutated=predecessor.report.get("analysis_mutated") is True,
            judge_output_mutated=predecessor.report.get("judge_output_mutated") is True,
            candidate_group_mutated=predecessor.report.get("candidate_group_mutated") is True,
            checks_failed=tuple(failures),
        )


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
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        return render_runner.run(args, env=predecessor_env, repo_root=repo_root)


class SqlAlchemyRetryIntentRenderExecutor:
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
    ) -> RetryIntentRenderExecutionResult:
        render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001 - local tool reuse.
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                event = _load_retry_intent_event_by_id(connection, notification_plan_created_event_id)
                if event is None:
                    return _execution_result(
                        retry_intent_event_found=False,
                        checks_failed=("retry_intent_event_missing_or_invalid",),
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
                event_after = _load_retry_intent_event_by_id(connection, notification_plan_created_event_id)
                if event_after is None:
                    return _execution_result(
                        retry_intent_event_found=False,
                        checks_failed=("retry_intent_event_missing_or_invalid",),
                    )
                after = _capture_scope(
                    connection,
                    event=event_after,
                    delivery_dedupe_namespace=delivery_dedupe_namespace,
                )
                verification = _verify_retry_intent_render_success(
                    connection,
                    event=event_after,
                    before=before,
                    after=after,
                    delivery_dedupe_namespace=delivery_dedupe_namespace,
                    render_result=render_result,
                )
                return verification
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prove that a local/test DB maintenance notification.plan.created.v1 "
            "due retry-intent can be consumed by the existing notifier dry-run "
            "render path without live transport or upstream recomputation."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--notification-plan-created-event-id")
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
    resolver: RetryIntentResolver | None = None,
    executor: RetryIntentRenderExecutor | None = None,
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

    explicit_event_id = _uuid_or_none(getattr(args, "notification_plan_created_event_id", None))
    if getattr(args, "notification_plan_created_event_id", None) and explicit_event_id is None:
        checks_failed.append("notification_plan_created_event_id_invalid")

    source_fixture = _path_or_none(getattr(args, "source_fixture", None))
    github_fixture = _path_or_none(getattr(args, "github_snapshot_fixture", None))
    replay_namespace = _string_or_none(getattr(args, "replay_namespace", None))
    prepare_fixture = bool(getattr(args, "prepare_failed_retryable_fixture", False))
    fixture_selector_supplied = (
        source_fixture is not None or github_fixture is not None or replay_namespace is not None or prepare_fixture
    )

    if explicit_event_id is not None and fixture_selector_supplied:
        checks_failed.append("selector_mode_ambiguous")

    if explicit_event_id is None:
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

    if explicit_event_id is None and source_fixture is not None and github_fixture is not None:
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

    if explicit_event_id is not None:
        resolved_event_id = explicit_event_id
        delivery_dedupe_namespace = _namespace_for_retry_intent_event(explicit_event_id)
    else:
        if source_fixture is None or github_fixture is None or replay_namespace is None:
            return _finish(report, ["fixture_selector_required"])
        active_resolver = resolver or DefaultDueRetryIntentResolver()
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
        _apply_resolution(report, resolution)
        checks_failed.extend(resolution.checks_failed)
        resolved_event_id = resolution.notification_plan_created_event_id
        if resolved_event_id is None:
            checks_failed.append("retry_intent_event_missing_or_invalid")
        if checks_failed:
            return _finish(report, checks_failed)
        delivery_dedupe_namespace = _namespace_for_retry_intent_event(resolved_event_id, prefix=replay_namespace)

    active_executor = executor or SqlAlchemyRetryIntentRenderExecutor()
    try:
        execution = active_executor.execute(
            database_url=args.database_url,
            notification_plan_created_event_id=resolved_event_id,
            delivery_dedupe_namespace=delivery_dedupe_namespace,
            env=effective_env,
            repo_root=root,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
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
    return due_runner.validate_database_url(database_url)


def _apply_resolution(report: dict[str, Any], resolution: RetryIntentResolutionResult) -> None:
    report.update(
        {
            "retry_intent_event_found": resolution.retry_intent_event_found,
            "retry_intent_event_count_before": resolution.retry_intent_event_count_before,
            "retry_intent_event_count_after": resolution.retry_intent_event_count_after,
            "replay_request_created": resolution.replay_request_created,
            "dead_letter_created": resolution.dead_letter_created,
            "notification_plan_mutated_by_maintenance": resolution.notification_plan_mutated_by_maintenance,
            "notification_delivery_record_mutated_by_maintenance": (
                resolution.notification_delivery_record_mutated_by_maintenance
            ),
            "analysis_mutated": resolution.analysis_mutated,
            "judge_output_mutated": resolution.judge_output_mutated,
            "candidate_group_mutated": resolution.candidate_group_mutated,
        }
    )


def _apply_execution(report: dict[str, Any], execution: RetryIntentRenderExecutionResult) -> None:
    if report["retry_intent_event_count_after"]:
        retry_before = report["retry_intent_event_count_before"]
        retry_after = report["retry_intent_event_count_after"]
    else:
        retry_before = execution.retry_intent_event_count_before
        retry_after = execution.retry_intent_event_count_after
    report.update(
        {
            "retry_intent_event_found": execution.retry_intent_event_found,
            "retry_intent_event_is_due_retry_promotion": execution.retry_intent_event_is_due_retry_promotion,
            "retry_intent_payload_matches_plan": execution.retry_intent_payload_matches_plan,
            "notifier_retry_intent_rehydrated": execution.notifier_retry_intent_rehydrated,
            "same_notification_plan_reused": execution.same_notification_plan_reused,
            "notification_render_created_or_reused": execution.notification_render_created_or_reused,
            "dry_run_delivery_record_created": execution.dry_run_delivery_record_created,
            "notification_delivery_result_event_created": execution.notification_delivery_result_event_created,
            "delivery_result_matches_retry_dry_run_record": execution.delivery_result_matches_retry_dry_run_record,
            "retry_intent_event_count_before": retry_before,
            "retry_intent_event_count_after": retry_after,
            "notification_render_count_before": execution.notification_render_count_before,
            "notification_render_count_after": execution.notification_render_count_after,
            "dry_run_delivery_record_count_before": execution.dry_run_delivery_record_count_before,
            "dry_run_delivery_record_count_after": execution.dry_run_delivery_record_count_after,
            "notification_delivery_result_event_count_before": execution.notification_delivery_result_event_count_before,
            "notification_delivery_result_event_count_after": execution.notification_delivery_result_event_count_after,
            "replay_request_created": report["replay_request_created"] or execution.replay_request_created,
            "dead_letter_created": report["dead_letter_created"] or execution.dead_letter_created,
            "analysis_mutated": report["analysis_mutated"] or execution.analysis_mutated,
            "judge_output_mutated": report["judge_output_mutated"] or execution.judge_output_mutated,
            "candidate_group_mutated": report["candidate_group_mutated"] or execution.candidate_group_mutated,
            "evidence_bundle_mutated": execution.evidence_bundle_mutated,
            "artifact_mutated": execution.artifact_mutated,
            "source_message_mutated": execution.source_message_mutated,
            "verdict_recomputed": execution.verdict_recomputed,
            "delivery_decision_overridden": execution.delivery_decision_overridden,
            "openai_called": execution.openai_called,
            "telegram_called": execution.telegram_called,
            "live_github_called": execution.live_github_called,
            "workers_started": execution.workers_started,
            "redis_mutation": execution.redis_mutation,
            "production_db_write": execution.production_db_write,
            "alembic_or_ddl_ran": execution.alembic_or_ddl_ran,
        }
    )


def _find_retry_intent_event_id_for_fixture_chain(
    *,
    database_url: str,
    replay_namespace: str,
) -> tuple[UUID | None, tuple[str, ...]]:
    render_runner.notifier_base._bootstrap_repo_imports()  # noqa: SLF001
    import sqlalchemy as sa

    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.begin() as connection:
            plan_ids = due_runner._find_fixture_notification_plan_ids_by_namespace(  # noqa: SLF001
                database_url=database_url,
                replay_namespace=replay_namespace,
            )
            if len(plan_ids) > 1:
                return None, ("fixture_notification_plan_ambiguous",)
            if len(plan_ids) != 1:
                return None, ("fixture_notification_plan_missing_or_invalid",)
            plan = due_runner._load_notification_plan(connection, plan_ids[0])  # noqa: SLF001
            latest = due_runner._load_latest_delivery_record(connection, plan_ids[0])  # noqa: SLF001
            if plan is None or latest is None or latest.delivery_status != due_runner.FAILED_RETRYABLE_STATUS:
                return None, ("latest_failed_retryable_delivery_record_missing_or_invalid",)
            dedupe_key = due_runner.build_retry_intent_dedupe_key(
                notification_plan_id=plan.notification_plan_id,
                latest_attempt_count=latest.attempt_count,
                send_after=plan.send_after,
            )
            rows = connection.execute(
                sa.text(
                    """
                    SELECT event_id
                    FROM event_outbox
                    WHERE event_type = :event_type
                      AND dedupe_key = :dedupe_key
                      AND payload_json ->> 'retry_reason' = :retry_reason
                    ORDER BY created_at, event_id
                    """
                ),
                {
                    "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                    "dedupe_key": dedupe_key,
                    "retry_reason": RETRY_REASON,
                },
            ).scalars().all()
            if len(rows) > 1:
                return None, ("retry_intent_event_ambiguous",)
            if len(rows) != 1:
                return None, ("retry_intent_event_missing_or_invalid",)
            return UUID(str(rows[0])), ()
    finally:
        engine.dispose()


def _load_retry_intent_event_by_id(connection: Any, event_id: UUID) -> RetryIntentEvent | None:
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
        return None
    payload = _json_loads(row["payload_json"]) or {}
    if not isinstance(payload, dict):
        return None
    valid, _failures = render_runner.notifier_base.validate_notification_intent_payload(payload)
    if not valid:
        return None
    return RetryIntentEvent(
        event_id=UUID(str(row["event_id"])),
        aggregate_type=_string_or_none(row["aggregate_type"]),
        aggregate_id=_uuid_or_none(row["aggregate_id"]),
        payload_json=payload,
        intent=render_runner.notifier_base.notification_plan_intent_from_payload(payload),
    )


def _capture_scope(
    connection: Any,
    *,
    event: RetryIntentEvent,
    delivery_dedupe_namespace: str,
) -> dict[str, Any]:
    context = _load_candidate_scope_context(connection, event.intent.candidate_group_id)
    return {
        "retry_intent_events": _retry_intent_event_count(connection, event_id=event.event_id),
        "notification_plan_exists": _notification_plan_exists(
            connection,
            notification_plan_id=event.intent.notification_plan_id,
        ),
        "notification_renders": _notification_render_count(
            connection,
            notification_plan_id=event.intent.notification_plan_id,
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
        "replay_requests": due_runner._replay_request_count(  # noqa: SLF001
            connection,
            notification_plan_id=event.intent.notification_plan_id,
        ),
        "dead_letters": due_runner._dead_letter_count(  # noqa: SLF001
            connection,
            notification_plan_id=event.intent.notification_plan_id,
        ),
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


def _verify_retry_intent_render_success(
    connection: Any,
    *,
    event: RetryIntentEvent,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    delivery_dedupe_namespace: str,
    render_result: render_runner.RunnerResult,
) -> RetryIntentRenderExecutionResult:
    checks_failed: list[str] = []
    retry_reason_ok = event.payload_json.get("retry_reason") == RETRY_REASON
    payload_matches_plan = _retry_intent_payload_matches_plan(connection, event=event)
    render_acceptable = _render_fixture_result_acceptable(render_result)
    delivery_result_matches = _delivery_result_matches_retry_dry_run_record(
        connection,
        event=event,
        delivery_dedupe_namespace=delivery_dedupe_namespace,
    )
    same_plan_reused = bool(after["notification_plan_exists"])
    render_delta = int(after["notification_renders"]) - int(before["notification_renders"])
    dry_run_delta = int(after["dry_run_delivery_records"]) - int(before["dry_run_delivery_records"])
    delivery_event_delta = int(after["delivery_result_events"]) - int(before["delivery_result_events"])
    render_bounded = int(after["notification_renders"]) >= 1 and render_delta in {0, 1}
    dry_run_bounded = int(after["dry_run_delivery_records"]) >= 1 and dry_run_delta in {0, 1}
    delivery_event_bounded = int(after["delivery_result_events"]) == 1 and delivery_event_delta in {0, 1}

    checks = {
        "retry_intent_event_found": int(after["retry_intent_events"]) == 1,
        "retry_intent_event_is_due_retry_promotion": retry_reason_ok,
        "retry_intent_payload_matches_plan": payload_matches_plan,
        "notifier_retry_intent_rehydrated": render_acceptable,
        "same_notification_plan_reused": same_plan_reused,
        "notification_render_created_or_reused": render_bounded,
        "dry_run_delivery_record_created": dry_run_bounded,
        "notification_delivery_result_event_created": delivery_event_bounded,
        "delivery_result_matches_retry_dry_run_record": delivery_result_matches,
        "replay_request_created": int(after["replay_requests"]) > int(before["replay_requests"]),
        "dead_letter_created": int(after["dead_letters"]) > int(before["dead_letters"]),
        "analysis_mutated": after["analysis_digest"] != before["analysis_digest"],
        "judge_output_mutated": after["judge_output_digest"] != before["judge_output_digest"],
        "candidate_group_mutated": after["candidate_group_digest"] != before["candidate_group_digest"],
        "evidence_bundle_mutated": after["evidence_bundle_digest"] != before["evidence_bundle_digest"],
        "artifact_mutated": after["artifact_digest"] != before["artifact_digest"],
        "source_message_mutated": after["source_message_digest"] != before["source_message_digest"],
        "verdict_recomputed": render_result.report.get("verdict_recomputed") is True,
        "delivery_decision_overridden": render_result.report.get("delivery_decision_overridden") is True,
        "openai_called": render_result.report.get("openai_called") is True,
        "telegram_called": render_result.report.get("telegram_called") is True,
        "live_github_called": render_result.report.get("live_github_called") is True,
        "workers_started": render_result.report.get("workers_started") is True,
        "redis_mutation": render_result.report.get("redis_mutation") is True,
        "production_db_write": render_result.report.get("production_db_write") is True,
        "alembic_or_ddl_ran": render_result.report.get("alembic_or_ddl_ran") is True,
    }
    for key in EXECUTION_TRUE_RESULT_KEYS:
        if checks[key] is not True:
            checks_failed.append(f"{key}:missing")
    for key in (
        "replay_request_created",
        "dead_letter_created",
        "analysis_mutated",
        "judge_output_mutated",
        "candidate_group_mutated",
        "evidence_bundle_mutated",
        "artifact_mutated",
        "source_message_mutated",
        "verdict_recomputed",
        "delivery_decision_overridden",
        "openai_called",
        "telegram_called",
        "live_github_called",
        "workers_started",
        "redis_mutation",
        "production_db_write",
        "alembic_or_ddl_ran",
    ):
        if checks[key] is not False:
            checks_failed.append(f"{key}:unexpected")

    return RetryIntentRenderExecutionResult(
        retry_intent_event_found=checks["retry_intent_event_found"],
        retry_intent_event_is_due_retry_promotion=checks["retry_intent_event_is_due_retry_promotion"],
        retry_intent_payload_matches_plan=checks["retry_intent_payload_matches_plan"],
        notifier_retry_intent_rehydrated=checks["notifier_retry_intent_rehydrated"],
        same_notification_plan_reused=checks["same_notification_plan_reused"],
        notification_render_created_or_reused=checks["notification_render_created_or_reused"],
        dry_run_delivery_record_created=checks["dry_run_delivery_record_created"],
        notification_delivery_result_event_created=checks["notification_delivery_result_event_created"],
        delivery_result_matches_retry_dry_run_record=checks["delivery_result_matches_retry_dry_run_record"],
        retry_intent_event_count_before=int(before["retry_intent_events"]),
        retry_intent_event_count_after=int(after["retry_intent_events"]),
        notification_render_count_before=int(before["notification_renders"]),
        notification_render_count_after=int(after["notification_renders"]),
        dry_run_delivery_record_count_before=int(before["dry_run_delivery_records"]),
        dry_run_delivery_record_count_after=int(after["dry_run_delivery_records"]),
        notification_delivery_result_event_count_before=int(before["delivery_result_events"]),
        notification_delivery_result_event_count_after=int(after["delivery_result_events"]),
        replay_request_created=checks["replay_request_created"],
        dead_letter_created=checks["dead_letter_created"],
        analysis_mutated=checks["analysis_mutated"],
        judge_output_mutated=checks["judge_output_mutated"],
        candidate_group_mutated=checks["candidate_group_mutated"],
        evidence_bundle_mutated=checks["evidence_bundle_mutated"],
        artifact_mutated=checks["artifact_mutated"],
        source_message_mutated=checks["source_message_mutated"],
        verdict_recomputed=checks["verdict_recomputed"],
        delivery_decision_overridden=checks["delivery_decision_overridden"],
        openai_called=checks["openai_called"],
        telegram_called=checks["telegram_called"],
        live_github_called=checks["live_github_called"],
        workers_started=checks["workers_started"],
        redis_mutation=checks["redis_mutation"],
        production_db_write=checks["production_db_write"],
        alembic_or_ddl_ran=checks["alembic_or_ddl_ran"],
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _retry_intent_payload_matches_plan(connection: Any, *, event: RetryIntentEvent) -> bool:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT notification_plan_id, analysis_id, candidate_group_id, delivery_decision,
                   urgency_profile, target_chat_id, target_thread_id, render_profile,
                   dedupe_subject_key, material_change_hash
            FROM notification_plans
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
            """
        ),
        {"notification_plan_id": str(event.intent.notification_plan_id)},
    ).mappings().first()
    if row is None:
        return False
    payload = event.payload_json
    return {
        "notification_plan_id": str(row["notification_plan_id"]),
        "analysis_id": str(row["analysis_id"]),
        "candidate_group_id": str(row["candidate_group_id"]),
        "delivery_decision": str(row["delivery_decision"]),
        "urgency_profile": str(row["urgency_profile"]),
        "target_chat_id": int(row["target_chat_id"]),
        "target_thread_id": _int_or_none(row["target_thread_id"]),
        "render_profile": _string_or_none(row["render_profile"]),
        "dedupe_subject_key": str(row["dedupe_subject_key"]),
        "material_change_hash": str(row["material_change_hash"]),
    } == {
        "notification_plan_id": str(payload.get("notification_plan_id")),
        "analysis_id": str(payload.get("analysis_id")),
        "candidate_group_id": str(payload.get("candidate_group_id")),
        "delivery_decision": str(payload.get("delivery_decision")),
        "urgency_profile": str(payload.get("urgency_profile")),
        "target_chat_id": _int_or_none(payload.get("target_chat_id")),
        "target_thread_id": _int_or_none(payload.get("target_thread_id")),
        "render_profile": _string_or_none(payload.get("render_profile")),
        "dedupe_subject_key": str(payload.get("dedupe_subject_key")),
        "material_change_hash": str(payload.get("material_change_hash")),
    }


def _delivery_result_matches_retry_dry_run_record(
    connection: Any,
    *,
    event: RetryIntentEvent,
    delivery_dedupe_namespace: str,
) -> bool:
    import sqlalchemy as sa

    row = connection.execute(
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
    if len(row) != 1:
        return False
    payload = _json_loads(row[0]["payload_json"]) or {}
    delivery_record_id = _uuid_or_none(payload.get("notification_delivery_record_id"))
    if delivery_record_id is None:
        return False
    if str(payload.get("notification_plan_id")) != str(event.intent.notification_plan_id):
        return False
    return _dry_run_delivery_record_exists(
        connection,
        notification_plan_id=event.intent.notification_plan_id,
        notification_delivery_record_id=delivery_record_id,
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
    allowed_retry_intent_failures = {"notification_plan_intent_mismatch"}
    checks_failed = set(result.report.get("checks_failed") or [])
    return bool(checks_failed) and checks_failed.issubset(allowed_retry_intent_failures)


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


def _retry_intent_event_count(connection: Any, *, event_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                  AND event_type = :event_type
                  AND payload_json ->> 'retry_reason' = :retry_reason
                """
            ),
            {
                "event_id": str(event_id),
                "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                "retry_reason": RETRY_REASON,
            },
        ).scalar_one()
    )


def _notification_plan_exists(connection: Any, *, notification_plan_id: UUID) -> bool:
    import sqlalchemy as sa

    return bool(
        connection.execute(
            sa.text(
                """
                SELECT 1
                FROM notification_plans
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                LIMIT 1
                """
            ),
            {"notification_plan_id": str(notification_plan_id)},
        ).scalar_one_or_none()
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


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "retry_intent_event_found": False,
        "retry_intent_event_is_due_retry_promotion": False,
        "retry_intent_payload_matches_plan": False,
        "notifier_retry_intent_rehydrated": False,
        "same_notification_plan_reused": False,
        "notification_render_created_or_reused": False,
        "dry_run_delivery_record_created": False,
        "notification_delivery_result_event_created": False,
        "delivery_result_matches_retry_dry_run_record": False,
        "retry_intent_event_count_before": 0,
        "retry_intent_event_count_after": 0,
        "notification_render_count_before": 0,
        "notification_render_count_after": 0,
        "dry_run_delivery_record_count_before": 0,
        "dry_run_delivery_record_count_after": 0,
        "notification_delivery_result_event_count_before": 0,
        "notification_delivery_result_event_count_after": 0,
        "replay_request_created": False,
        "dead_letter_created": False,
        "notification_plan_mutated_by_maintenance": False,
        "notification_delivery_record_mutated_by_maintenance": False,
        "analysis_mutated": False,
        "judge_output_mutated": False,
        "candidate_group_mutated": False,
        "evidence_bundle_mutated": False,
        "artifact_mutated": False,
        "source_message_mutated": False,
        "verdict_recomputed": False,
        "delivery_decision_overridden": False,
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
    retry_intent_event_found: bool = False,
    retry_intent_event_is_due_retry_promotion: bool = False,
    retry_intent_payload_matches_plan: bool = False,
    notifier_retry_intent_rehydrated: bool = False,
    same_notification_plan_reused: bool = False,
    notification_render_created_or_reused: bool = False,
    dry_run_delivery_record_created: bool = False,
    notification_delivery_result_event_created: bool = False,
    delivery_result_matches_retry_dry_run_record: bool = False,
    retry_intent_event_count_before: int = 0,
    retry_intent_event_count_after: int = 0,
    notification_render_count_before: int = 0,
    notification_render_count_after: int = 0,
    dry_run_delivery_record_count_before: int = 0,
    dry_run_delivery_record_count_after: int = 0,
    notification_delivery_result_event_count_before: int = 0,
    notification_delivery_result_event_count_after: int = 0,
    checks_failed: Sequence[str],
) -> RetryIntentRenderExecutionResult:
    return RetryIntentRenderExecutionResult(
        retry_intent_event_found=retry_intent_event_found,
        retry_intent_event_is_due_retry_promotion=retry_intent_event_is_due_retry_promotion,
        retry_intent_payload_matches_plan=retry_intent_payload_matches_plan,
        notifier_retry_intent_rehydrated=notifier_retry_intent_rehydrated,
        same_notification_plan_reused=same_notification_plan_reused,
        notification_render_created_or_reused=notification_render_created_or_reused,
        dry_run_delivery_record_created=dry_run_delivery_record_created,
        notification_delivery_result_event_created=notification_delivery_result_event_created,
        delivery_result_matches_retry_dry_run_record=delivery_result_matches_retry_dry_run_record,
        retry_intent_event_count_before=retry_intent_event_count_before,
        retry_intent_event_count_after=retry_intent_event_count_after,
        notification_render_count_before=notification_render_count_before,
        notification_render_count_after=notification_render_count_after,
        dry_run_delivery_record_count_before=dry_run_delivery_record_count_before,
        dry_run_delivery_record_count_after=dry_run_delivery_record_count_after,
        notification_delivery_result_event_count_before=notification_delivery_result_event_count_before,
        notification_delivery_result_event_count_after=notification_delivery_result_event_count_after,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _namespace_for_retry_intent_event(event_id: UUID, *, prefix: str | None = None) -> str:
    base = _string_or_none(prefix) or "explicit"
    return f"{base}-retry-{event_id}"


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
        return int(str(value))
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
