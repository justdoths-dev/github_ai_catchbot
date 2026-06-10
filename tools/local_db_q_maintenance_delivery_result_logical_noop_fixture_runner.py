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

from src.services.maintenance.retry_policy import classify_delivery_result_dry_run_noop
from src.services.outbox_relay.models import OutboxEventRow, RedisQueuedMessage
from src.services.outbox_relay.routing import OutboxRouteResolver
from tools import local_db_q_notification_send_thin_consumer_notifier_dry_run_fixture_runner as q_send_runner


SCHEMA_VERSION = "local_db_q_maintenance_delivery_result_logical_noop_fixture_runner_v1"
NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE = q_send_runner.NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE
NOTIFICATION_PLAN_CREATED_EVENT_TYPE = q_send_runner.NOTIFICATION_PLAN_CREATED_EVENT_TYPE
REPLAY_REQUESTED_EVENT_TYPE = q_send_runner.REPLAY_REQUESTED_EVENT_TYPE
DRY_RUN_REASON_CODE = q_send_runner.render_runner.DRY_RUN_REASON_CODE
EXPECTED_QUEUE_NAME = "q.maintenance"
EXPECTED_STAGE_NAME = "maintenance"
EXPECTED_ROOT_OBJECT_TYPE = "notification_plan"
REQUIRED_THIN_QUEUE_FIELDS = q_send_runner.REQUIRED_THIN_QUEUE_FIELDS
REQUIRED_EVENT_PAYLOAD_FIELDS = (
    "notification_plan_id",
    "delivery_status",
    "telegram_chat_id",
    "telegram_message_id",
)
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "thin_queue_message_built",
    "thin_queue_message_validated",
    "notification_delivery_result_event_rehydrated",
    "notification_plan_loaded",
    "notification_delivery_record_loaded",
    "delivery_result_classified",
    "maintenance_logical_noop",
)
FALSE_RESULT_KEYS = (
    "retry_intent_created",
    "dead_letter_created",
    "replay_request_created",
    "replay_requested_event_created",
    "notification_plan_created_event_created",
    "notification_plan_mutated",
    "notification_render_mutated",
    "notification_delivery_record_mutated",
    "analysis_mutated",
    "judge_output_mutated",
    "candidate_group_mutated",
    "evidence_bundle_mutated",
    "artifact_mutated",
    "source_message_mutated",
    "telegram_called",
    "openai_called",
    "redis_mutation",
    "workers_started",
    "production_db_write",
    "alembic_or_ddl_ran",
    "real_transport_attempted",
)
SAFE_EXCEPTION_MESSAGES = {
    "delivery_record_missing_or_invalid",
    "delivery_result_fixture_failed",
    "delivery_result_not_logical_noop_target",
    "delivery_result_retryable_not_noop",
    "fixture_selector_incomplete",
    "notification_delivery_record_ambiguous",
    "notification_delivery_record_id_invalid",
    "notification_delivery_record_missing_or_invalid",
    "notification_delivery_record_payload_mismatch",
    "notification_delivery_result_event_aggregate_mismatch",
    "notification_delivery_result_event_ambiguous",
    "notification_delivery_result_event_missing_or_invalid",
    "notification_delivery_result_event_payload_invalid",
    "notification_delivery_result_event_type_invalid",
    "notification_plan_missing_or_invalid",
    "q_maintenance_queue_route_invalid",
    "thin_queue_message_invalid",
    "thin_queue_message_missing_trigger_event_id",
    "thin_queue_message_payload_forbidden",
    "thin_queue_message_root_mismatch",
    "thin_queue_message_stage_invalid",
}

notifier_runner = q_send_runner.notifier_runner
render_runner = q_send_runner.render_runner
source_candidate_runner = q_send_runner.source_candidate_runner
github_snapshot_runner = q_send_runner.github_snapshot_runner


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class DeliveryResultResolutionResult:
    notification_delivery_result_event_id: UUID | None
    notification_delivery_result_event_found: bool
    delivery_result_fixture_prepared: bool = False
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ThinQueueBuildResult:
    message: RedisQueuedMessage | None
    thin_queue_message_built: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DeliveryResultEvent:
    event_id: UUID
    aggregate_type: str | None
    aggregate_id: UUID | None
    dedupe_key: str
    status: str
    fail_count: int
    created_at: datetime
    notification_plan_id: UUID
    notification_delivery_record_id: UUID | None
    delivery_status: str
    telegram_chat_id: int | None
    telegram_message_id: int | None
    attempt_count: int | None
    transport_error_code: str | None
    transport_error_class: str | None
    dry_run: bool
    transport_skipped: bool
    noop: bool
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
    telegram_chat_id: int | None
    telegram_message_id: int | None
    delivery_status: str
    attempt_count: int | None
    transport_error_code: str | None
    transport_error_class: str | None
    telegram_response_json: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class DeliveryResultClassification:
    delivery_result_classified: bool
    maintenance_logical_noop: bool
    reason_code: str
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ThinConsumerExecutionResult:
    thin_queue_message_validated: bool
    notification_delivery_result_event_rehydrated: bool
    notification_plan_loaded: bool
    notification_delivery_record_loaded: bool
    delivery_result_classified: bool
    maintenance_logical_noop: bool
    retry_intent_count_before: int = 0
    retry_intent_count_after: int = 0
    dead_letter_count_before: int = 0
    dead_letter_count_after: int = 0
    replay_request_count_before: int = 0
    replay_request_count_after: int = 0
    replay_requested_event_count_before: int = 0
    replay_requested_event_count_after: int = 0
    notification_plan_created_event_count_before: int = 0
    notification_plan_created_event_count_after: int = 0
    retry_intent_created: bool = False
    dead_letter_created: bool = False
    replay_request_created: bool = False
    replay_requested_event_created: bool = False
    notification_plan_created_event_created: bool = False
    notification_plan_mutated: bool = False
    notification_render_mutated: bool = False
    notification_delivery_record_mutated: bool = False
    analysis_mutated: bool = False
    judge_output_mutated: bool = False
    candidate_group_mutated: bool = False
    evidence_bundle_mutated: bool = False
    artifact_mutated: bool = False
    source_message_mutated: bool = False
    telegram_called: bool = False
    openai_called: bool = False
    live_github_called: bool = False
    redis_mutation: bool = False
    workers_started: bool = False
    production_db_write: bool = False
    alembic_or_ddl_ran: bool = False
    real_transport_attempted: bool = False
    checks_failed: tuple[str, ...] = ()


class NotificationSendPredecessor(Protocol):
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        max_attempts: int,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> q_send_runner.RunnerResult: ...


class DeliveryResultResolver(Protocol):
    def resolve(
        self,
        *,
        database_url: str,
        selector_mode: str,
        notification_delivery_result_event_id: UUID | None,
        notification_plan_id: UUID | None,
        notification_delivery_record_id: UUID | None,
        source_fixture_path: Path | None,
        github_snapshot_fixture_path: Path | None,
        replay_namespace: str | None,
        prepare_delivery_result_fixture: bool,
        max_attempts: int,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> DeliveryResultResolutionResult: ...


class ThinQueueMessageBuilder(Protocol):
    def build(
        self,
        *,
        database_url: str,
        notification_delivery_result_event_id: UUID,
    ) -> ThinQueueBuildResult: ...


class ThinQueueConsumerExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        message: RedisQueuedMessage,
    ) -> ThinConsumerExecutionResult: ...


class DefaultNotificationSendPredecessor:
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        max_attempts: int,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> q_send_runner.RunnerResult:
        args = argparse.Namespace(
            database_url=database_url,
            notification_plan_created_event_id=None,
            notification_plan_id=None,
            replay_request_id=None,
            source_fixture=str(source_fixture_path),
            github_snapshot_fixture=str(github_snapshot_fixture_path),
            replay_namespace=replay_namespace,
            prepare_delivery_replay_intent_fixture=True,
            max_attempts=str(max_attempts),
            confirm_local_test_db=True,
        )
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        predecessor_env.setdefault("ENABLE_NOTIFICATION_SEND", "false")
        return q_send_runner.run(args, env=predecessor_env, repo_root=repo_root)


class DefaultDeliveryResultResolver:
    def __init__(self, *, predecessor: NotificationSendPredecessor | None = None) -> None:
        self._predecessor = predecessor or DefaultNotificationSendPredecessor()

    def resolve(
        self,
        *,
        database_url: str,
        selector_mode: str,
        notification_delivery_result_event_id: UUID | None,
        notification_plan_id: UUID | None,
        notification_delivery_record_id: UUID | None,
        source_fixture_path: Path | None,
        github_snapshot_fixture_path: Path | None,
        replay_namespace: str | None,
        prepare_delivery_result_fixture: bool,
        max_attempts: int,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> DeliveryResultResolutionResult:
        if selector_mode == "notification_delivery_result_event":
            return DeliveryResultResolutionResult(
                notification_delivery_result_event_id=notification_delivery_result_event_id,
                notification_delivery_result_event_found=notification_delivery_result_event_id is not None,
            )
        if selector_mode == "notification_plan":
            if notification_plan_id is None:
                return _resolution_result(checks_failed=("notification_plan_missing_or_invalid",))
            return _resolve_event_by_notification_plan_id(database_url, notification_plan_id)
        if selector_mode == "notification_delivery_record":
            if notification_delivery_record_id is None:
                return _resolution_result(checks_failed=("notification_delivery_record_missing_or_invalid",))
            return _resolve_event_by_delivery_record_id(database_url, notification_delivery_record_id)
        if selector_mode == "fixture_chain":
            if (
                source_fixture_path is None
                or github_snapshot_fixture_path is None
                or replay_namespace is None
                or not prepare_delivery_result_fixture
            ):
                return _resolution_result(checks_failed=("fixture_selector_incomplete",))
            return _resolve_fixture_chain(
                database_url=database_url,
                source_fixture_path=source_fixture_path,
                github_snapshot_fixture_path=github_snapshot_fixture_path,
                replay_namespace=replay_namespace,
                max_attempts=max_attempts,
                env=env,
                repo_root=repo_root,
                predecessor=self._predecessor,
            )
        return _resolution_result(checks_failed=("selector_mode_required",))


class SqlAlchemyThinQueueMessageBuilder:
    def __init__(self, *, route_resolver: OutboxRouteResolver | None = None) -> None:
        self._route_resolver = route_resolver or OutboxRouteResolver()

    def build(
        self,
        *,
        database_url: str,
        notification_delivery_result_event_id: UUID,
    ) -> ThinQueueBuildResult:
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                event, failures = _load_delivery_result_event(
                    connection,
                    event_id=notification_delivery_result_event_id,
                )
                if event is None:
                    return ThinQueueBuildResult(
                        message=None,
                        thin_queue_message_built=False,
                        checks_failed=failures or ("notification_delivery_result_event_missing_or_invalid",),
                    )
                route = self._route_resolver.resolve(_to_outbox_row(event))
                if route.queue_name != EXPECTED_QUEUE_NAME or route.stage_name != EXPECTED_STAGE_NAME:
                    return ThinQueueBuildResult(
                        message=None,
                        thin_queue_message_built=False,
                        checks_failed=("q_maintenance_queue_route_invalid",),
                    )
                return ThinQueueBuildResult(
                    message=RedisQueuedMessage(
                        job_id=str(event.event_id),
                        stage_name=route.stage_name,
                        root_object_type=EXPECTED_ROOT_OBJECT_TYPE,
                        root_object_id=str(event.notification_plan_id),
                        idempotency_key=event.dedupe_key,
                        pipeline_run_id=None,
                        not_before=None,
                        trigger_event_id=str(event.event_id),
                    ),
                    thin_queue_message_built=True,
                )
        finally:
            engine.dispose()


class SqlAlchemyThinQueueConsumerExecutor:
    def __init__(self, *, route_resolver: OutboxRouteResolver | None = None) -> None:
        self._route_resolver = route_resolver or OutboxRouteResolver()

    def execute(
        self,
        *,
        database_url: str,
        message: RedisQueuedMessage,
    ) -> ThinConsumerExecutionResult:
        message_failures = _validate_thin_queue_message(message)
        if message_failures:
            return _consumer_result(checks_failed=message_failures)

        trigger_event_id = _uuid_or_none(message.trigger_event_id)
        if trigger_event_id is None:
            return _consumer_result(checks_failed=("thin_queue_message_missing_trigger_event_id",))

        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                event, event_failures = _load_delivery_result_event(connection, event_id=trigger_event_id)
                if event is None:
                    return _consumer_result(
                        thin_queue_message_validated=True,
                        checks_failed=event_failures or ("notification_delivery_result_event_missing_or_invalid",),
                    )
                route = self._route_resolver.resolve(_to_outbox_row(event))
                if route.queue_name != EXPECTED_QUEUE_NAME or route.stage_name != EXPECTED_STAGE_NAME:
                    return _consumer_result(
                        thin_queue_message_validated=True,
                        notification_delivery_result_event_rehydrated=True,
                        checks_failed=("q_maintenance_queue_route_invalid",),
                    )
                root_object_id = _uuid_or_none(message.root_object_id)
                if (
                    message.stage_name != route.stage_name
                    or message.root_object_type != EXPECTED_ROOT_OBJECT_TYPE
                    or root_object_id is None
                    or root_object_id != event.notification_plan_id
                    or message.idempotency_key != event.dedupe_key
                    or message.job_id != message.trigger_event_id
                ):
                    return _consumer_result(
                        thin_queue_message_validated=True,
                        notification_delivery_result_event_rehydrated=True,
                        checks_failed=("thin_queue_message_root_mismatch",),
                    )

                plan = _load_notification_plan(connection, event.notification_plan_id)
                if plan is None:
                    return _consumer_result(
                        thin_queue_message_validated=True,
                        notification_delivery_result_event_rehydrated=True,
                        notification_plan_loaded=False,
                        checks_failed=("notification_plan_missing_or_invalid",),
                    )
                record = _load_delivery_record_for_event(connection, event)
                if record is None:
                    return _consumer_result(
                        thin_queue_message_validated=True,
                        notification_delivery_result_event_rehydrated=True,
                        notification_plan_loaded=True,
                        notification_delivery_record_loaded=False,
                        checks_failed=("notification_delivery_record_missing_or_invalid",),
                    )

                before = _capture_scope(connection, event=event, plan=plan, record=record)
                classification = classify_delivery_result_logical_noop(event=event, record=record)
                after = _capture_scope(connection, event=event, plan=plan, record=record)
                return _execution_from_scope(
                    before=before,
                    after=after,
                    event=event,
                    record=record,
                    classification=classification,
                    thin_queue_message_validated=True,
                    notification_delivery_result_event_rehydrated=True,
                    notification_plan_loaded=True,
                    notification_delivery_record_loaded=True,
                )
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Model a local/test DB q.maintenance thin queue consumer for one "
            "notification.delivery.result.v1 event, rehydrate by trigger_event_id, "
            "and classify suppressed dry-run delivery results as logical no-ops without "
            "creating retry, dead-letter, replay, Redis, worker, or transport effects."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--notification-delivery-result-event-id")
    parser.add_argument("--notification-plan-id")
    parser.add_argument("--notification-delivery-record-id")
    parser.add_argument("--source-fixture")
    parser.add_argument("--github-snapshot-fixture")
    parser.add_argument("--replay-namespace")
    parser.add_argument("--prepare-delivery-result-fixture", action="store_true")
    parser.add_argument("--max-attempts")
    parser.add_argument("--confirm-local-test-db", action="store_true")
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    resolver: DeliveryResultResolver | None = None,
    queue_builder: ThinQueueMessageBuilder | None = None,
    consumer: ThinQueueConsumerExecutor | None = None,
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

    enable_send = _string_or_none(effective_env.get("ENABLE_NOTIFICATION_SEND"))
    if enable_send is not None and not _falsey(enable_send):
        checks_failed.append("enable_notification_send_must_be_false")
    report["notification_send_disabled_or_unconfigured"] = enable_send is None or _falsey(enable_send)

    raw_event_id = _string_or_none(getattr(args, "notification_delivery_result_event_id", None))
    raw_plan_id = _string_or_none(getattr(args, "notification_plan_id", None))
    raw_record_id = _string_or_none(getattr(args, "notification_delivery_record_id", None))
    event_id = _uuid_or_none(raw_event_id)
    notification_plan_id = _uuid_or_none(raw_plan_id)
    notification_delivery_record_id = _uuid_or_none(raw_record_id)
    if raw_event_id is not None and event_id is None:
        checks_failed.append("notification_delivery_result_event_id_invalid")
    if raw_plan_id is not None and notification_plan_id is None:
        checks_failed.append("notification_plan_id_invalid")
    if raw_record_id is not None and notification_delivery_record_id is None:
        checks_failed.append("notification_delivery_record_id_invalid")

    source_fixture = _path_or_none(getattr(args, "source_fixture", None))
    github_fixture = _path_or_none(getattr(args, "github_snapshot_fixture", None))
    replay_namespace = _string_or_none(getattr(args, "replay_namespace", None))
    prepare_fixture = bool(getattr(args, "prepare_delivery_result_fixture", False))
    fixture_selector_supplied = (
        source_fixture is not None or github_fixture is not None or replay_namespace is not None or prepare_fixture
    )
    fixture_selector_complete = (
        source_fixture is not None and github_fixture is not None and replay_namespace is not None and prepare_fixture
    )

    selector_modes = (
        int(raw_event_id is not None)
        + int(raw_plan_id is not None)
        + int(raw_record_id is not None)
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
        notification_plan_id=notification_plan_id,
        notification_delivery_record_id=notification_delivery_record_id,
        fixture_selector_supplied=fixture_selector_supplied,
    )

    max_attempts = _max_attempts_from_args_env(getattr(args, "max_attempts", None), effective_env)
    if fixture_selector_supplied and max_attempts is None:
        checks_failed.append("max_attempts_invalid")
        max_attempts = q_send_runner.dispatch_runner.request_runner.dlq_runner.DEFAULT_MAX_ATTEMPTS

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

    active_resolver = resolver or DefaultDeliveryResultResolver()
    try:
        resolution = active_resolver.resolve(
            database_url=args.database_url,
            selector_mode=selector_mode,
            notification_delivery_result_event_id=event_id,
            notification_plan_id=notification_plan_id,
            notification_delivery_record_id=notification_delivery_record_id,
            source_fixture_path=source_fixture,
            github_snapshot_fixture_path=github_fixture,
            replay_namespace=replay_namespace,
            prepare_delivery_result_fixture=prepare_fixture,
            max_attempts=max_attempts or q_send_runner.dispatch_runner.request_runner.dlq_runner.DEFAULT_MAX_ATTEMPTS,
            env=effective_env,
            repo_root=root,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
        return _finish(report, [_safe_failure_code(exc)])

    report["delivery_result_fixture_prepared"] = resolution.delivery_result_fixture_prepared
    report["notification_delivery_result_event_resolved"] = resolution.notification_delivery_result_event_found
    checks_failed.extend(resolution.checks_failed)
    resolved_event_id = resolution.notification_delivery_result_event_id
    if resolved_event_id is None:
        checks_failed.append("notification_delivery_result_event_missing_or_invalid")
    if checks_failed:
        return _finish(report, checks_failed)

    active_builder = queue_builder or SqlAlchemyThinQueueMessageBuilder()
    try:
        build_result = active_builder.build(
            database_url=args.database_url,
            notification_delivery_result_event_id=resolved_event_id,
        )
    except Exception as exc:  # noqa: BLE001 - sanitized operator result only.
        return _finish(report, [_safe_failure_code(exc)])
    report["thin_queue_message_built"] = build_result.thin_queue_message_built
    checks_failed.extend(build_result.checks_failed)
    if build_result.message is None:
        checks_failed.append("thin_queue_message_invalid")
    if checks_failed:
        return _finish(report, checks_failed)

    message = build_result.message
    message_failures = _validate_thin_queue_message(message)
    if message_failures:
        return _finish(report, message_failures)
    _apply_safe_queue_shape(report, message)

    active_consumer = consumer or SqlAlchemyThinQueueConsumerExecutor()
    try:
        execution = active_consumer.execute(
            database_url=args.database_url,
            message=message,
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
    return q_send_runner.validate_database_url(database_url)


def classify_delivery_result_logical_noop(
    *,
    event: DeliveryResultEvent,
    record: DeliveryRecord,
) -> DeliveryResultClassification:
    if not _delivery_result_matches_record(event=event, record=record):
        return DeliveryResultClassification(
            delivery_result_classified=False,
            maintenance_logical_noop=False,
            reason_code="notification_delivery_record_payload_mismatch",
            checks_failed=("notification_delivery_record_payload_mismatch",),
        )
    if _retryable_transport_failure_present(event=event, record=record):
        return DeliveryResultClassification(
            delivery_result_classified=True,
            maintenance_logical_noop=False,
            reason_code="delivery_result_retryable_not_noop",
            checks_failed=("delivery_result_retryable_not_noop",),
        )
    if not _dry_run_noop_metadata_present(event=event, record=record):
        return DeliveryResultClassification(
            delivery_result_classified=True,
            maintenance_logical_noop=False,
            reason_code="delivery_result_not_logical_noop_target",
            checks_failed=("delivery_result_not_logical_noop_target",),
        )
    decision = classify_delivery_result_dry_run_noop(
        delivery_status=record.delivery_status,
        delivery_reason=record.transport_error_code,
    )
    maintenance_logical_noop = (
        decision.action == "mark_logical_noop_success"
        and decision.retry_intent_allowed is False
        and decision.dead_letter_allowed is False
        and decision.replay_dispatch_allowed is False
    )
    checks_failed = () if maintenance_logical_noop else ("delivery_result_not_logical_noop_target",)
    return DeliveryResultClassification(
        delivery_result_classified=True,
        maintenance_logical_noop=maintenance_logical_noop,
        reason_code=decision.reason_code,
        checks_failed=checks_failed,
    )


def _resolve_event_by_notification_plan_id(
    database_url: str,
    notification_plan_id: UUID,
) -> DeliveryResultResolutionResult:
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
                      AND (
                        aggregate_id = CAST(:notification_plan_id AS uuid)
                        OR payload_json ->> 'notification_plan_id' = :notification_plan_id
                      )
                    ORDER BY created_at, event_id
                    LIMIT 2
                    """
                ),
                {
                    "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
                    "notification_plan_id": str(notification_plan_id),
                },
            ).scalars().all()
    finally:
        engine.dispose()
    if len(rows) > 1:
        return _resolution_result(checks_failed=("notification_delivery_result_event_ambiguous",))
    if len(rows) != 1:
        return _resolution_result(checks_failed=("notification_delivery_result_event_missing_or_invalid",))
    return DeliveryResultResolutionResult(
        notification_delivery_result_event_id=UUID(str(rows[0])),
        notification_delivery_result_event_found=True,
    )


def _resolve_event_by_delivery_record_id(
    database_url: str,
    notification_delivery_record_id: UUID,
) -> DeliveryResultResolutionResult:
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
                      AND payload_json ->> 'notification_delivery_record_id' = :notification_delivery_record_id
                    ORDER BY created_at, event_id
                    LIMIT 2
                    """
                ),
                {
                    "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
                    "notification_delivery_record_id": str(notification_delivery_record_id),
                },
            ).scalars().all()
    finally:
        engine.dispose()
    if len(rows) > 1:
        return _resolution_result(checks_failed=("notification_delivery_record_ambiguous",))
    if len(rows) != 1:
        return _resolution_result(checks_failed=("notification_delivery_record_missing_or_invalid",))
    return DeliveryResultResolutionResult(
        notification_delivery_result_event_id=UUID(str(rows[0])),
        notification_delivery_result_event_found=True,
    )


def _resolve_fixture_chain(
    *,
    database_url: str,
    source_fixture_path: Path,
    github_snapshot_fixture_path: Path,
    replay_namespace: str,
    max_attempts: int,
    env: Mapping[str, str],
    repo_root: Path,
    predecessor: NotificationSendPredecessor,
) -> DeliveryResultResolutionResult:
    before = _find_delivery_result_event_ids_by_namespace(
        database_url=database_url,
        replay_namespace=replay_namespace,
    )
    if len(before) > 1:
        return _resolution_result(checks_failed=("notification_delivery_result_event_ambiguous",))
    if len(before) == 1:
        return DeliveryResultResolutionResult(
            notification_delivery_result_event_id=before[0],
            notification_delivery_result_event_found=True,
        )

    predecessor_result = predecessor.run(
        database_url=database_url,
        source_fixture_path=source_fixture_path,
        github_snapshot_fixture_path=github_snapshot_fixture_path,
        replay_namespace=replay_namespace,
        max_attempts=max_attempts,
        env=env,
        repo_root=repo_root,
    )
    if not _q_notification_send_result_acceptable(predecessor_result):
        return _resolution_result(
            delivery_result_fixture_prepared=True,
            checks_failed=("delivery_result_fixture_failed",),
        )

    after = _find_delivery_result_event_ids_by_namespace(
        database_url=database_url,
        replay_namespace=replay_namespace,
    )
    if len(after) > 1:
        return _resolution_result(
            delivery_result_fixture_prepared=True,
            checks_failed=("notification_delivery_result_event_ambiguous",),
        )
    if len(after) != 1:
        return _resolution_result(
            delivery_result_fixture_prepared=True,
            checks_failed=("notification_delivery_result_event_missing_or_invalid",),
        )
    return DeliveryResultResolutionResult(
        notification_delivery_result_event_id=after[0],
        notification_delivery_result_event_found=True,
        delivery_result_fixture_prepared=True,
    )


def _find_delivery_result_event_ids_by_namespace(
    *,
    database_url: str,
    replay_namespace: str,
) -> list[UUID]:
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
                    LIMIT 2
                    """
                ),
                {
                    "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
                    "dedupe_prefix": _delivery_result_dedupe_prefix(replay_namespace),
                },
            ).scalars().all()
            return [UUID(str(row)) for row in rows]
    finally:
        engine.dispose()


def _q_notification_send_result_acceptable(result: q_send_runner.RunnerResult) -> bool:
    if result.exit_code != 0 or result.report.get("status") != "pass":
        return False
    expected_true = (
        "database_url_guard_passed",
        "thin_queue_message_built",
        "thin_queue_message_validated",
        "notification_plan_created_event_rehydrated",
        "notification_delivery_result_event_created_or_reused",
    )
    expected_false = (
        "telegram_called",
        "openai_called",
        "redis_mutation",
        "workers_started",
        "production_db_write",
        "alembic_or_ddl_ran",
        "real_transport_attempted",
    )
    return all(result.report.get(key) is True for key in expected_true) and all(
        result.report.get(key) is False for key in expected_false
    )


def _load_delivery_result_event(
    connection: Any,
    *,
    event_id: UUID,
) -> tuple[DeliveryResultEvent | None, tuple[str, ...]]:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT event_id, event_type, aggregate_type, aggregate_id, dedupe_key, payload_json, status,
                   fail_count, created_at
            FROM event_outbox
            WHERE event_id = CAST(:event_id AS uuid)
            """
        ),
        {"event_id": str(event_id)},
    ).mappings().first()
    if row is None:
        return None, ("notification_delivery_result_event_missing_or_invalid",)
    if str(row["event_type"]) != NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE:
        return None, ("notification_delivery_result_event_type_invalid",)

    payload = _json_loads(row["payload_json"])
    if not isinstance(payload, dict):
        return None, ("notification_delivery_result_event_payload_invalid",)
    if any(field not in payload for field in REQUIRED_EVENT_PAYLOAD_FIELDS):
        return None, ("notification_delivery_result_event_payload_invalid",)
    notification_plan_id = _uuid_or_none(payload.get("notification_plan_id"))
    delivery_status = _string_or_none(payload.get("delivery_status"))
    if notification_plan_id is None or delivery_status is None:
        return None, ("notification_delivery_result_event_payload_invalid",)
    notification_delivery_record_id = _uuid_or_none(payload.get("notification_delivery_record_id"))
    if payload.get("notification_delivery_record_id") is not None and notification_delivery_record_id is None:
        return None, ("notification_delivery_result_event_payload_invalid",)
    attempt_count = _int_or_none(payload.get("attempt_count"))
    if payload.get("attempt_count") is not None and attempt_count is None:
        return None, ("notification_delivery_result_event_payload_invalid",)

    aggregate_type = _string_or_none(row["aggregate_type"])
    aggregate_id = _uuid_or_none(row["aggregate_id"])
    if aggregate_type != EXPECTED_ROOT_OBJECT_TYPE or aggregate_id != notification_plan_id:
        return None, ("notification_delivery_result_event_aggregate_mismatch",)

    created_at = row["created_at"]
    if not isinstance(created_at, datetime):
        created_at = datetime.now(timezone.utc)
    return (
        DeliveryResultEvent(
            event_id=UUID(str(row["event_id"])),
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            dedupe_key=str(row["dedupe_key"]),
            status=str(row["status"]),
            fail_count=int(row["fail_count"] or 0),
            created_at=created_at,
            notification_plan_id=notification_plan_id,
            notification_delivery_record_id=notification_delivery_record_id,
            delivery_status=delivery_status,
            telegram_chat_id=_int_or_none(payload.get("telegram_chat_id")),
            telegram_message_id=_int_or_none(payload.get("telegram_message_id")),
            attempt_count=attempt_count,
            transport_error_code=_string_or_none(payload.get("transport_error_code")),
            transport_error_class=_string_or_none(payload.get("transport_error_class")),
            dry_run=payload.get("dry_run") is True,
            transport_skipped=payload.get("transport_skipped") is True,
            noop=payload.get("noop") is True,
            payload_json=payload,
        ),
        (),
    )


def _to_outbox_row(event: DeliveryResultEvent) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=event.event_id,
        event_type=NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
        aggregate_type=event.aggregate_type or EXPECTED_ROOT_OBJECT_TYPE,
        aggregate_id=event.aggregate_id or event.notification_plan_id,
        dedupe_key=event.dedupe_key,
        payload_json=event.payload_json,
        status=event.status,
        fail_count=event.fail_count,
        created_at=event.created_at,
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


def _load_delivery_record_for_event(connection: Any, event: DeliveryResultEvent) -> DeliveryRecord | None:
    if event.notification_delivery_record_id is not None:
        return _load_delivery_record_by_id(
            connection,
            notification_delivery_record_id=event.notification_delivery_record_id,
        )
    return _load_latest_delivery_record(connection, notification_plan_id=event.notification_plan_id)


def _load_delivery_record_by_id(
    connection: Any,
    *,
    notification_delivery_record_id: UUID,
) -> DeliveryRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT notification_delivery_record_id, notification_plan_id, telegram_chat_id,
                   telegram_message_id, delivery_status, attempt_count, transport_error_code,
                   transport_error_class, telegram_response_json
            FROM notification_delivery_records
            WHERE notification_delivery_record_id = CAST(:notification_delivery_record_id AS uuid)
            """
        ),
        {"notification_delivery_record_id": str(notification_delivery_record_id)},
    ).mappings().first()
    return _delivery_record_from_row(row)


def _load_latest_delivery_record(connection: Any, *, notification_plan_id: UUID) -> DeliveryRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT notification_delivery_record_id, notification_plan_id, telegram_chat_id,
                   telegram_message_id, delivery_status, attempt_count, transport_error_code,
                   transport_error_class, telegram_response_json
            FROM notification_delivery_records
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
            ORDER BY created_at DESC, notification_delivery_record_id DESC
            LIMIT 1
            """
        ),
        {"notification_plan_id": str(notification_plan_id)},
    ).mappings().first()
    return _delivery_record_from_row(row)


def _delivery_record_from_row(row: Mapping[str, Any] | None) -> DeliveryRecord | None:
    if row is None:
        return None
    response = _json_loads(row["telegram_response_json"])
    return DeliveryRecord(
        notification_delivery_record_id=UUID(str(row["notification_delivery_record_id"])),
        notification_plan_id=UUID(str(row["notification_plan_id"])),
        telegram_chat_id=_int_or_none(row["telegram_chat_id"]),
        telegram_message_id=_int_or_none(row["telegram_message_id"]),
        delivery_status=str(row["delivery_status"]),
        attempt_count=_int_or_none(row["attempt_count"]),
        transport_error_code=_string_or_none(row["transport_error_code"]),
        transport_error_class=_string_or_none(row["transport_error_class"]),
        telegram_response_json=response if isinstance(response, dict) else None,
    )


def _delivery_result_matches_record(*, event: DeliveryResultEvent, record: DeliveryRecord) -> bool:
    return (
        event.notification_plan_id == record.notification_plan_id
        and (
            event.notification_delivery_record_id is None
            or event.notification_delivery_record_id == record.notification_delivery_record_id
        )
        and event.delivery_status == record.delivery_status
        and event.telegram_chat_id == record.telegram_chat_id
        and event.telegram_message_id == record.telegram_message_id
        and (event.attempt_count is None or event.attempt_count == record.attempt_count)
        and (
            event.transport_error_code is None
            or event.transport_error_code == record.transport_error_code
        )
        and (
            event.transport_error_class is None
            or event.transport_error_class == record.transport_error_class
        )
    )


def _dry_run_noop_metadata_present(*, event: DeliveryResultEvent, record: DeliveryRecord) -> bool:
    response = record.telegram_response_json or {}
    return (
        event.delivery_status == "suppressed"
        and record.delivery_status == "suppressed"
        and record.telegram_message_id is None
        and record.attempt_count == 0
        and record.transport_error_code == DRY_RUN_REASON_CODE
        and record.transport_error_class is None
        and (
            event.dry_run
            or event.transport_skipped
            or event.noop
            or response.get("dry_run") is True
            or response.get("transport_skipped") is True
            or response.get("noop") is True
        )
        and (event.transport_error_code in {None, DRY_RUN_REASON_CODE})
        and response.get("reason_code") == DRY_RUN_REASON_CODE
    )


def _retryable_transport_failure_present(*, event: DeliveryResultEvent, record: DeliveryRecord) -> bool:
    values = (
        event.delivery_status,
        event.transport_error_code,
        event.transport_error_class,
        record.delivery_status,
        record.transport_error_code,
        record.transport_error_class,
    )
    return any("retryable" in str(value).lower() for value in values if value is not None)


def _capture_scope(
    connection: Any,
    *,
    event: DeliveryResultEvent,
    plan: NotificationPlanRecord,
    record: DeliveryRecord,
) -> dict[str, Any]:
    context = notifier_runner._load_candidate_scope_context(  # noqa: SLF001
        connection,
        plan.candidate_group_id,
    )
    return {
        "retry_intents": _retry_intent_event_count(connection, notification_plan_id=plan.notification_plan_id),
        "dead_letters": _dead_letter_count(connection, notification_plan_id=plan.notification_plan_id),
        "replay_requests": _replay_request_count(connection, notification_plan_id=plan.notification_plan_id),
        "replay_requested_events": _replay_requested_event_count(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "notification_plan_created_events": _notification_plan_created_event_count(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "notification_plan_digest": _notification_plan_digest(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "notification_render_digest": _notification_render_digest(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
        "notification_delivery_record_digest": _delivery_record_digest(
            connection,
            notification_delivery_record_id=record.notification_delivery_record_id,
        ),
        "analysis_digest": render_runner.notifier_base._load_analysis_digest(  # noqa: SLF001
            connection,
            plan.analysis_id,
        ),
        "judge_output_digest": render_runner.notifier_base._load_judge_output_digest_by_analysis(  # noqa: SLF001
            connection,
            plan.analysis_id,
        ),
        "candidate_group_digest": render_runner.notifier_base._load_candidate_digest(  # noqa: SLF001
            connection,
            plan.candidate_group_id,
        ),
        "evidence_bundle_digest": notifier_runner._evidence_bundle_digest(  # noqa: SLF001
            connection,
            bundle_id=context["current_bundle_id"],
        ),
        "artifact_digest": notifier_runner._artifact_digest(  # noqa: SLF001
            connection,
            artifact_id=context["current_primary_artifact_id"],
        ),
        "source_message_digest": notifier_runner._source_message_digest(  # noqa: SLF001
            connection,
            source_message_id=context["source_message_id"],
        ),
        "event_digest": _stable_digest(event.payload_json),
    }


def _execution_from_scope(
    *,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    event: DeliveryResultEvent,
    record: DeliveryRecord,
    classification: DeliveryResultClassification,
    thin_queue_message_validated: bool,
    notification_delivery_result_event_rehydrated: bool,
    notification_plan_loaded: bool,
    notification_delivery_record_loaded: bool,
) -> ThinConsumerExecutionResult:
    checks_failed: list[str] = list(classification.checks_failed)
    retry_intent_created = int(after["retry_intents"]) > int(before["retry_intents"])
    dead_letter_created = int(after["dead_letters"]) > int(before["dead_letters"])
    replay_request_created = int(after["replay_requests"]) > int(before["replay_requests"])
    replay_requested_event_created = int(after["replay_requested_events"]) > int(before["replay_requested_events"])
    notification_plan_created_event_created = (
        int(after["notification_plan_created_events"]) > int(before["notification_plan_created_events"])
    )
    mutation_checks = {
        "notification_plan_mutated": after["notification_plan_digest"] != before["notification_plan_digest"],
        "notification_render_mutated": after["notification_render_digest"] != before["notification_render_digest"],
        "notification_delivery_record_mutated": (
            after["notification_delivery_record_digest"] != before["notification_delivery_record_digest"]
        ),
        "analysis_mutated": after["analysis_digest"] != before["analysis_digest"],
        "judge_output_mutated": after["judge_output_digest"] != before["judge_output_digest"],
        "candidate_group_mutated": after["candidate_group_digest"] != before["candidate_group_digest"],
        "evidence_bundle_mutated": after["evidence_bundle_digest"] != before["evidence_bundle_digest"],
        "artifact_mutated": after["artifact_digest"] != before["artifact_digest"],
        "source_message_mutated": after["source_message_digest"] != before["source_message_digest"],
    }
    for flag, value in (
        ("retry_intent_created", retry_intent_created),
        ("dead_letter_created", dead_letter_created),
        ("replay_request_created", replay_request_created),
        ("replay_requested_event_created", replay_requested_event_created),
        ("notification_plan_created_event_created", notification_plan_created_event_created),
        *mutation_checks.items(),
    ):
        if value:
            checks_failed.append(flag)
    if event.notification_delivery_record_id is not None and event.notification_delivery_record_id != record.notification_delivery_record_id:
        checks_failed.append("notification_delivery_record_payload_mismatch")

    return ThinConsumerExecutionResult(
        thin_queue_message_validated=thin_queue_message_validated,
        notification_delivery_result_event_rehydrated=notification_delivery_result_event_rehydrated,
        notification_plan_loaded=notification_plan_loaded,
        notification_delivery_record_loaded=notification_delivery_record_loaded,
        delivery_result_classified=classification.delivery_result_classified,
        maintenance_logical_noop=classification.maintenance_logical_noop,
        retry_intent_count_before=int(before["retry_intents"]),
        retry_intent_count_after=int(after["retry_intents"]),
        dead_letter_count_before=int(before["dead_letters"]),
        dead_letter_count_after=int(after["dead_letters"]),
        replay_request_count_before=int(before["replay_requests"]),
        replay_request_count_after=int(after["replay_requests"]),
        replay_requested_event_count_before=int(before["replay_requested_events"]),
        replay_requested_event_count_after=int(after["replay_requested_events"]),
        notification_plan_created_event_count_before=int(before["notification_plan_created_events"]),
        notification_plan_created_event_count_after=int(after["notification_plan_created_events"]),
        retry_intent_created=retry_intent_created,
        dead_letter_created=dead_letter_created,
        replay_request_created=replay_request_created,
        replay_requested_event_created=replay_requested_event_created,
        notification_plan_created_event_created=notification_plan_created_event_created,
        notification_plan_mutated=mutation_checks["notification_plan_mutated"],
        notification_render_mutated=mutation_checks["notification_render_mutated"],
        notification_delivery_record_mutated=mutation_checks["notification_delivery_record_mutated"],
        analysis_mutated=mutation_checks["analysis_mutated"],
        judge_output_mutated=mutation_checks["judge_output_mutated"],
        candidate_group_mutated=mutation_checks["candidate_group_mutated"],
        evidence_bundle_mutated=mutation_checks["evidence_bundle_mutated"],
        artifact_mutated=mutation_checks["artifact_mutated"],
        source_message_mutated=mutation_checks["source_message_mutated"],
        checks_failed=tuple(dict.fromkeys(checks_failed)),
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
                  AND payload_json ? 'retry_reason'
                """
            ),
            {"event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE, "notification_plan_id": str(notification_plan_id)},
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


def _replay_requested_event_count(connection: Any, *, notification_plan_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM event_outbox AS eo
                JOIN replay_requests AS rr ON rr.replay_request_id = eo.aggregate_id
                WHERE eo.event_type = :event_type
                  AND eo.aggregate_type = 'replay_request'
                  AND rr.replay_type = 'delivery'::replay_type_enum
                  AND rr.root_object_type = 'notification_plan'
                  AND rr.root_object_id = CAST(:notification_plan_id AS uuid)
                """
            ),
            {"event_type": REPLAY_REQUESTED_EVENT_TYPE, "notification_plan_id": str(notification_plan_id)},
        ).scalar_one()
    )


def _notification_plan_created_event_count(connection: Any, *, notification_plan_id: UUID) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = :event_type
                  AND payload_json ->> 'notification_plan_id' = :notification_plan_id
                """
            ),
            {"event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE, "notification_plan_id": str(notification_plan_id)},
        ).scalar_one()
    )


def _notification_plan_digest(connection: Any, *, notification_plan_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT COALESCE(md5(to_jsonb(np)::text), '')
            FROM notification_plans AS np
            WHERE np.notification_plan_id = CAST(:notification_plan_id AS uuid)
            """
        ),
        {"notification_plan_id": str(notification_plan_id)},
    ).scalar_one_or_none()
    return str(row or "")


def _notification_render_digest(connection: Any, *, notification_plan_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT COALESCE(md5(to_jsonb(nr)::text), '')
            FROM notification_renders AS nr
            WHERE nr.notification_plan_id = CAST(:notification_plan_id AS uuid)
            ORDER BY nr.created_at DESC, nr.notification_render_id DESC
            LIMIT 1
            """
        ),
        {"notification_plan_id": str(notification_plan_id)},
    ).scalar_one_or_none()
    return str(row or "")


def _delivery_record_digest(connection: Any, *, notification_delivery_record_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT COALESCE(md5(to_jsonb(ndr)::text), '')
            FROM notification_delivery_records AS ndr
            WHERE ndr.notification_delivery_record_id = CAST(:notification_delivery_record_id AS uuid)
            """
        ),
        {"notification_delivery_record_id": str(notification_delivery_record_id)},
    ).scalar_one_or_none()
    return str(row or "")


def _validate_thin_queue_message(message: RedisQueuedMessage) -> tuple[str, ...]:
    failures: list[str] = []
    fields = message.as_stream_fields()
    if set(fields) != set(REQUIRED_THIN_QUEUE_FIELDS):
        failures.append("thin_queue_message_invalid")
    if "payload_json" in fields:
        failures.append("thin_queue_message_payload_forbidden")
    if not _string_or_none(fields.get("trigger_event_id")):
        failures.append("thin_queue_message_missing_trigger_event_id")
    if _uuid_or_none(fields.get("trigger_event_id")) is None:
        failures.append("thin_queue_message_missing_trigger_event_id")
    if message.stage_name != EXPECTED_STAGE_NAME:
        failures.append("thin_queue_message_stage_invalid")
    if message.root_object_type != EXPECTED_ROOT_OBJECT_TYPE:
        failures.append("thin_queue_message_root_mismatch")
    if _uuid_or_none(message.root_object_id) is None:
        failures.append("thin_queue_message_root_mismatch")
    for key in ("job_id", "idempotency_key"):
        if not _string_or_none(fields.get(key)):
            failures.append("thin_queue_message_invalid")
    return tuple(dict.fromkeys(failures))


def _apply_safe_queue_shape(report: dict[str, Any], message: RedisQueuedMessage) -> None:
    report["thin_queue_message_fields"] = list(REQUIRED_THIN_QUEUE_FIELDS)
    report["thin_queue_message_payload_json_present"] = False
    report["thin_queue_queue_name"] = EXPECTED_QUEUE_NAME
    report["thin_queue_stage_name"] = message.stage_name
    report["thin_queue_root_object_type"] = message.root_object_type
    report["thin_queue_trigger_field"] = "trigger_event_id"
    report["event_payload_used_as_queue_payload"] = False


def _apply_execution(report: dict[str, Any], execution: ThinConsumerExecutionResult) -> None:
    report.update(
        {
            "thin_queue_message_validated": execution.thin_queue_message_validated,
            "notification_delivery_result_event_rehydrated": (
                execution.notification_delivery_result_event_rehydrated
            ),
            "notification_plan_loaded": execution.notification_plan_loaded,
            "notification_delivery_record_loaded": execution.notification_delivery_record_loaded,
            "delivery_result_classified": execution.delivery_result_classified,
            "maintenance_logical_noop": execution.maintenance_logical_noop,
            "retry_intent_count_before": execution.retry_intent_count_before,
            "retry_intent_count_after": execution.retry_intent_count_after,
            "dead_letter_count_before": execution.dead_letter_count_before,
            "dead_letter_count_after": execution.dead_letter_count_after,
            "replay_request_count_before": execution.replay_request_count_before,
            "replay_request_count_after": execution.replay_request_count_after,
            "replay_requested_event_count_before": execution.replay_requested_event_count_before,
            "replay_requested_event_count_after": execution.replay_requested_event_count_after,
            "notification_plan_created_event_count_before": execution.notification_plan_created_event_count_before,
            "notification_plan_created_event_count_after": execution.notification_plan_created_event_count_after,
            "retry_intent_created": execution.retry_intent_created,
            "dead_letter_created": execution.dead_letter_created,
            "replay_request_created": execution.replay_request_created,
            "replay_requested_event_created": execution.replay_requested_event_created,
            "notification_plan_created_event_created": execution.notification_plan_created_event_created,
            "notification_plan_mutated": execution.notification_plan_mutated,
            "notification_render_mutated": execution.notification_render_mutated,
            "notification_delivery_record_mutated": execution.notification_delivery_record_mutated,
            "analysis_mutated": execution.analysis_mutated,
            "judge_output_mutated": execution.judge_output_mutated,
            "candidate_group_mutated": execution.candidate_group_mutated,
            "evidence_bundle_mutated": execution.evidence_bundle_mutated,
            "artifact_mutated": execution.artifact_mutated,
            "source_message_mutated": execution.source_message_mutated,
            "telegram_called": execution.telegram_called,
            "openai_called": execution.openai_called,
            "live_github_called": execution.live_github_called,
            "redis_mutation": execution.redis_mutation,
            "workers_started": execution.workers_started,
            "production_db_write": execution.production_db_write,
            "alembic_or_ddl_ran": execution.alembic_or_ddl_ran,
            "real_transport_attempted": execution.real_transport_attempted,
        }
    )


def _consumer_result(
    *,
    thin_queue_message_validated: bool = False,
    notification_delivery_result_event_rehydrated: bool = False,
    notification_plan_loaded: bool = False,
    notification_delivery_record_loaded: bool = False,
    delivery_result_classified: bool = False,
    maintenance_logical_noop: bool = False,
    checks_failed: Sequence[str],
) -> ThinConsumerExecutionResult:
    return ThinConsumerExecutionResult(
        thin_queue_message_validated=thin_queue_message_validated,
        notification_delivery_result_event_rehydrated=notification_delivery_result_event_rehydrated,
        notification_plan_loaded=notification_plan_loaded,
        notification_delivery_record_loaded=notification_delivery_record_loaded,
        delivery_result_classified=delivery_result_classified,
        maintenance_logical_noop=maintenance_logical_noop,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _base_report() -> dict[str, Any]:
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "notification_send_disabled_or_unconfigured": False,
        "delivery_result_fixture_prepared": False,
        "notification_delivery_result_event_resolved": False,
        "thin_queue_message_fields": list(REQUIRED_THIN_QUEUE_FIELDS),
        "thin_queue_message_payload_json_present": False,
        "thin_queue_queue_name": EXPECTED_QUEUE_NAME,
        "thin_queue_stage_name": EXPECTED_STAGE_NAME,
        "thin_queue_root_object_type": EXPECTED_ROOT_OBJECT_TYPE,
        "thin_queue_trigger_field": "trigger_event_id",
        "event_payload_used_as_queue_payload": False,
        "retry_intent_count_before": 0,
        "retry_intent_count_after": 0,
        "dead_letter_count_before": 0,
        "dead_letter_count_after": 0,
        "replay_request_count_before": 0,
        "replay_request_count_after": 0,
        "replay_requested_event_count_before": 0,
        "replay_requested_event_count_after": 0,
        "notification_plan_created_event_count_before": 0,
        "notification_plan_created_event_count_after": 0,
        "live_github_called": False,
        "checks_failed": [],
    }
    for key in TRUE_RESULT_KEYS:
        if key not in report:
            report[key] = False
    for key in FALSE_RESULT_KEYS:
        report[key] = False
    return report


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _resolution_result(
    *,
    delivery_result_fixture_prepared: bool = False,
    checks_failed: Sequence[str],
) -> DeliveryResultResolutionResult:
    return DeliveryResultResolutionResult(
        notification_delivery_result_event_id=None,
        notification_delivery_result_event_found=False,
        delivery_result_fixture_prepared=delivery_result_fixture_prepared,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _selector_mode(
    *,
    event_id: UUID | None,
    notification_plan_id: UUID | None,
    notification_delivery_record_id: UUID | None,
    fixture_selector_supplied: bool,
) -> str:
    if event_id is not None:
        return "notification_delivery_result_event"
    if notification_plan_id is not None:
        return "notification_plan"
    if notification_delivery_record_id is not None:
        return "notification_delivery_record"
    if fixture_selector_supplied:
        return "fixture_chain"
    return "none"


def _max_attempts_from_args_env(value: Any, env: Mapping[str, str]) -> int | None:
    return q_send_runner._max_attempts_from_args_env(value, env)  # noqa: SLF001


def _delivery_result_dedupe_prefix(replay_namespace: str) -> str:
    return f"local-db-notification-render-dry-run:{replay_namespace}:notification.delivery.result:%"


def _falsey(value: Any) -> bool:
    return q_send_runner._falsey(value)  # noqa: SLF001


def _json_loads(value: Any) -> Any:
    return q_send_runner._json_loads(value)  # noqa: SLF001


def _uuid_or_none(value: Any) -> UUID | None:
    return q_send_runner._uuid_or_none(value)  # noqa: SLF001


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _path_or_none(value: Any) -> Path | None:
    return q_send_runner._path_or_none(value)  # noqa: SLF001


def _string_or_none(value: Any) -> str | None:
    return q_send_runner._string_or_none(value)  # noqa: SLF001


def _stable_digest(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


def _json_default(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


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
