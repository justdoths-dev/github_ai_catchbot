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

from src.services.maintenance.retry_policy import classify_delivery_result_send_disabled_noop
from src.services.outbox_relay.models import RedisQueuedMessage
from tools import local_db_q_maintenance_delivery_result_logical_noop_fixture_runner as q_maintenance_runner
from tools import local_db_q_notification_send_thin_consumer_notifier_dry_run_fixture_runner as q_send_runner


SCHEMA_VERSION = "local_db_send_disabled_delivery_e2e_acceptance_runner_v1"
NOTIFICATION_PLAN_CREATED_EVENT_TYPE = q_send_runner.NOTIFICATION_PLAN_CREATED_EVENT_TYPE
NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE = q_send_runner.NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE
SEND_DISABLED_REASON_CODE = "notification_send_flag_disabled"
SEND_DISABLED_RECOVERY_MODE = "explicit_delivery_replay_only"
EXPECTED_Q_NOTIFICATION_STAGE = "notify"
EXPECTED_Q_MAINTENANCE_STAGE = "maintenance"
EXPECTED_Q_NOTIFICATION_ROOT = "analysis"
EXPECTED_Q_MAINTENANCE_ROOT = "notification_plan"
DELIVERY_FROM_STATE = "rendered"
DELIVERY_TO_STATE = "suppressed"
SEND_DISABLED_RESPONSE = {
    "dry_run": False,
    "send_disabled": True,
    "send_enabled": False,
    "transport_skipped": True,
    "reason_code": SEND_DISABLED_REASON_CODE,
    "delivery_action": "send",
}
REQUIRED_INTENT_PAYLOAD_FIELDS = (
    "notification_plan_id",
    "analysis_id",
    "candidate_group_id",
    "delivery_decision",
    "urgency_profile",
    "target_chat_id",
    "dedupe_subject_key",
    "material_change_hash",
)
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "enable_notification_send_false_guard_passed",
    "q_notification_send_message_built",
    "q_notification_send_message_validated",
    "notification_plan_created_event_rehydrated",
    "notification_plan_created_or_reused",
    "notification_render_created_or_reused",
    "send_disabled_delivery_record_created_or_reused",
    "send_disabled_metadata_present",
    "send_disabled_transport_skipped",
    "notification_delivery_result_event_created_or_reused",
    "q_maintenance_message_built",
    "q_maintenance_message_validated",
    "notification_delivery_result_event_rehydrated",
    "delivery_result_classified",
    "maintenance_logical_noop",
    "analysis_loaded",
    "judge_output_loaded",
    "candidate_group_loaded",
    "artifact_loaded",
    "source_message_loaded",
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
    "retry_intent_created",
    "dead_letter_created",
    "replay_request_created",
    "replay_requested_event_created",
    "notification_plan_created_retry_event_created",
    "analysis_mutated",
    "judge_output_mutated",
    "candidate_group_mutated",
    "evidence_bundle_mutated",
    "artifact_mutated",
    "source_message_mutated",
)
SAFE_EXCEPTION_MESSAGES = {
    "analysis_missing",
    "candidate_group_missing",
    "delivery_result_not_send_disabled_noop_target",
    "fixture_selector_incomplete",
    "github_snapshot_fixture_load_failed",
    "invalid_uuid",
    "notification_delivery_record_missing_or_invalid",
    "notification_delivery_record_payload_mismatch",
    "notification_delivery_result_event_missing_or_invalid",
    "notification_delivery_result_event_payload_invalid",
    "notification_delivery_result_event_type_invalid",
    "notification_intent_payload_invalid",
    "notification_plan_created_event_aggregate_mismatch",
    "notification_plan_created_event_missing_or_invalid",
    "notification_plan_created_event_payload_invalid",
    "notification_plan_created_event_type_invalid",
    "notification_plan_material_conflict",
    "notification_plan_missing_or_invalid",
    "notification_plan_intent_mismatch",
    "primary_artifact_missing",
    "q_maintenance_queue_route_invalid",
    "q_notification_send_queue_route_invalid",
    "source_fixture_load_failed",
    "source_message_missing",
    "thin_queue_message_invalid",
    "thin_queue_message_missing_trigger_event_id",
    "thin_queue_message_payload_forbidden",
    "thin_queue_message_root_mismatch",
    "thin_queue_message_stage_invalid",
}

notifier_runner = q_send_runner.notifier_runner
render_runner = q_send_runner.render_runner
notifier_base = render_runner.notifier_base
source_candidate_runner = q_send_runner.source_candidate_runner
github_snapshot_runner = q_send_runner.github_snapshot_runner


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PlanEventResolutionResult:
    notification_plan_created_event_id: UUID | None
    notification_plan_created_event_found: bool
    send_disabled_e2e_fixture_prepared: bool = False
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class NotificationConsumerExecutionResult:
    q_notification_send_message_validated: bool
    notification_plan_created_event_rehydrated: bool
    notification_plan_created_or_reused: bool
    notification_render_created_or_reused: bool
    send_disabled_delivery_record_created_or_reused: bool
    notification_delivery_result_event_created_or_reused: bool
    analysis_loaded: bool
    judge_output_loaded: bool
    candidate_group_loaded: bool
    artifact_loaded: bool
    source_message_loaded: bool
    notification_plan_count_before: int = 0
    notification_plan_count_after: int = 0
    notification_render_count_before: int = 0
    notification_render_count_after: int = 0
    send_disabled_delivery_record_count_before: int = 0
    send_disabled_delivery_record_count_after: int = 0
    notification_delivery_result_event_count_before: int = 0
    notification_delivery_result_event_count_after: int = 0
    notification_delivery_result_event_id: UUID | None = None
    telegram_called: bool = False
    openai_called: bool = False
    redis_mutation: bool = False
    live_github_called: bool = False
    workers_started: bool = False
    production_db_write: bool = False
    alembic_or_ddl_ran: bool = False
    real_transport_attempted: bool = False
    analysis_mutated: bool = False
    judge_output_mutated: bool = False
    candidate_group_mutated: bool = False
    evidence_bundle_mutated: bool = False
    artifact_mutated: bool = False
    source_message_mutated: bool = False
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MaintenanceConsumerExecutionResult:
    q_maintenance_message_validated: bool
    notification_delivery_result_event_rehydrated: bool
    delivery_result_classified: bool
    maintenance_logical_noop: bool
    send_disabled_recovery_mode: str | None
    retry_intent_count_before: int = 0
    retry_intent_count_after: int = 0
    dead_letter_count_before: int = 0
    dead_letter_count_after: int = 0
    replay_request_count_before: int = 0
    replay_request_count_after: int = 0
    replay_requested_event_count_before: int = 0
    replay_requested_event_count_after: int = 0
    notification_plan_created_retry_event_count_before: int = 0
    notification_plan_created_retry_event_count_after: int = 0
    retry_intent_created: bool = False
    dead_letter_created: bool = False
    replay_request_created: bool = False
    replay_requested_event_created: bool = False
    notification_plan_created_retry_event_created: bool = False
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
    redis_mutation: bool = False
    live_github_called: bool = False
    workers_started: bool = False
    production_db_write: bool = False
    alembic_or_ddl_ran: bool = False
    real_transport_attempted: bool = False
    checks_failed: tuple[str, ...] = ()


class PlanEventResolver(Protocol):
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
    ) -> PlanEventResolutionResult: ...


class QueueMessageBuilder(Protocol):
    def build(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
    ) -> q_send_runner.ThinQueueBuildResult: ...


class NotificationConsumer(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        message: RedisQueuedMessage,
        delivery_dedupe_namespace: str,
    ) -> NotificationConsumerExecutionResult: ...


class MaintenanceQueueMessageBuilder(Protocol):
    def build(
        self,
        *,
        database_url: str,
        notification_delivery_result_event_id: UUID,
    ) -> q_maintenance_runner.ThinQueueBuildResult: ...


class MaintenanceConsumer(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        message: RedisQueuedMessage,
    ) -> MaintenanceConsumerExecutionResult: ...


class DefaultPlanEventResolver:
    def __init__(self, *, delegate: q_send_runner.ReplayIntentResolver | None = None) -> None:
        self._delegate = delegate or q_send_runner.DefaultReplayIntentResolver()

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
    ) -> PlanEventResolutionResult:
        delegated = self._delegate.resolve(
            database_url=database_url,
            selector_mode=selector_mode,
            notification_plan_created_event_id=notification_plan_created_event_id,
            replay_request_id=replay_request_id,
            notification_plan_id=notification_plan_id,
            source_fixture_path=source_fixture_path,
            github_snapshot_fixture_path=github_snapshot_fixture_path,
            replay_namespace=replay_namespace,
            max_attempts=max_attempts,
            env=env,
            repo_root=repo_root,
        )
        return PlanEventResolutionResult(
            notification_plan_created_event_id=delegated.notification_plan_created_event_id,
            notification_plan_created_event_found=delegated.notification_plan_created_event_loaded,
            send_disabled_e2e_fixture_prepared=delegated.replay_intent_fixture_prepared,
            checks_failed=delegated.checks_failed,
        )


class SqlAlchemyNotificationConsumer:
    def execute(
        self,
        *,
        database_url: str,
        message: RedisQueuedMessage,
        delivery_dedupe_namespace: str,
    ) -> NotificationConsumerExecutionResult:
        message_failures = q_send_runner._validate_thin_queue_message(message)  # noqa: SLF001
        if message_failures:
            return _notification_consumer_result(checks_failed=message_failures)

        trigger_event_id = _uuid_or_none(message.trigger_event_id)
        if trigger_event_id is None:
            return _notification_consumer_result(checks_failed=("thin_queue_message_missing_trigger_event_id",))

        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                event, event_failures = q_send_runner._load_queue_event(connection, event_id=trigger_event_id)  # noqa: SLF001
                if event is None:
                    return _notification_consumer_result(
                        q_notification_send_message_validated=True,
                        checks_failed=event_failures or ("notification_plan_created_event_missing_or_invalid",),
                    )
                root_object_id = _uuid_or_none(message.root_object_id)
                if (
                    message.stage_name != EXPECTED_Q_NOTIFICATION_STAGE
                    or message.root_object_type != event.aggregate_type
                    or root_object_id is None
                    or root_object_id != event.aggregate_id
                    or message.idempotency_key != event.dedupe_key
                    or message.job_id != message.trigger_event_id
                ):
                    return _notification_consumer_result(
                        q_notification_send_message_validated=True,
                        notification_plan_created_event_rehydrated=True,
                        checks_failed=("thin_queue_message_root_mismatch",),
                    )
                return _execute_send_disabled_notifier(
                    connection,
                    notification_plan_created_event_id=trigger_event_id,
                    delivery_dedupe_namespace=delivery_dedupe_namespace,
                    q_notification_send_message_validated=True,
                )
        finally:
            engine.dispose()


class SqlAlchemyMaintenanceConsumer:
    def execute(
        self,
        *,
        database_url: str,
        message: RedisQueuedMessage,
    ) -> MaintenanceConsumerExecutionResult:
        message_failures = q_maintenance_runner._validate_thin_queue_message(message)  # noqa: SLF001
        if message_failures:
            return _maintenance_consumer_result(checks_failed=message_failures)

        trigger_event_id = _uuid_or_none(message.trigger_event_id)
        if trigger_event_id is None:
            return _maintenance_consumer_result(checks_failed=("thin_queue_message_missing_trigger_event_id",))

        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                event, event_failures = q_maintenance_runner._load_delivery_result_event(  # noqa: SLF001
                    connection,
                    event_id=trigger_event_id,
                )
                if event is None:
                    return _maintenance_consumer_result(
                        q_maintenance_message_validated=True,
                        checks_failed=event_failures or ("notification_delivery_result_event_missing_or_invalid",),
                    )
                root_object_id = _uuid_or_none(message.root_object_id)
                if (
                    message.stage_name != EXPECTED_Q_MAINTENANCE_STAGE
                    or message.root_object_type != EXPECTED_Q_MAINTENANCE_ROOT
                    or root_object_id is None
                    or root_object_id != event.notification_plan_id
                    or message.idempotency_key != event.dedupe_key
                    or message.job_id != message.trigger_event_id
                ):
                    return _maintenance_consumer_result(
                        q_maintenance_message_validated=True,
                        notification_delivery_result_event_rehydrated=True,
                        checks_failed=("thin_queue_message_root_mismatch",),
                    )
                plan = q_maintenance_runner._load_notification_plan(connection, event.notification_plan_id)  # noqa: SLF001
                if plan is None:
                    return _maintenance_consumer_result(
                        q_maintenance_message_validated=True,
                        notification_delivery_result_event_rehydrated=True,
                        checks_failed=("notification_plan_missing_or_invalid",),
                    )
                record = q_maintenance_runner._load_delivery_record_for_event(connection, event)  # noqa: SLF001
                if record is None:
                    return _maintenance_consumer_result(
                        q_maintenance_message_validated=True,
                        notification_delivery_result_event_rehydrated=True,
                        checks_failed=("notification_delivery_record_missing_or_invalid",),
                    )
                before = _capture_maintenance_scope(connection, event=event, plan=plan, record=record)
                classification = _classify_send_disabled_result(event=event, record=record)
                after = _capture_maintenance_scope(connection, event=event, plan=plan, record=record)
                return _maintenance_execution_from_scope(
                    before=before,
                    after=after,
                    event=event,
                    record=record,
                    classification=classification,
                    q_maintenance_message_validated=True,
                    notification_delivery_result_event_rehydrated=True,
                )
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a local/test DB send-disabled delivery E2E acceptance path from "
            "notification.plan.created.v1 through q.notification.send, a send-disabled "
            "suppressed delivery result, q.maintenance, and logical-noop classification."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--notification-plan-created-event-id")
    parser.add_argument("--notification-plan-id")
    parser.add_argument("--replay-request-id")
    parser.add_argument("--source-fixture")
    parser.add_argument("--github-snapshot-fixture")
    parser.add_argument("--replay-namespace")
    parser.add_argument("--prepare-send-disabled-e2e-fixture", action="store_true")
    parser.add_argument("--max-attempts")
    parser.add_argument("--confirm-local-test-db", action="store_true")
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    resolver: PlanEventResolver | None = None,
    notification_queue_builder: QueueMessageBuilder | None = None,
    notification_consumer: NotificationConsumer | None = None,
    maintenance_queue_builder: MaintenanceQueueMessageBuilder | None = None,
    maintenance_consumer: MaintenanceConsumer | None = None,
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
    report["enable_notification_send_false_guard_passed"] = enable_send is not None and _falsey(enable_send)
    if enable_send is None or not _falsey(enable_send):
        checks_failed.append("enable_notification_send_must_be_false")

    report["notifier_telegram_dry_run_env"] = _truthy(effective_env.get("NOTIFIER_TELEGRAM_DRY_RUN"))
    if report["notifier_telegram_dry_run_env"]:
        checks_failed.append("notifier_telegram_dry_run_must_be_false_for_send_disabled")

    raw_event_id = _string_or_none(getattr(args, "notification_plan_created_event_id", None))
    raw_plan_id = _string_or_none(getattr(args, "notification_plan_id", None))
    raw_replay_request_id = _string_or_none(getattr(args, "replay_request_id", None))
    event_id = _uuid_or_none(raw_event_id)
    notification_plan_id = _uuid_or_none(raw_plan_id)
    replay_request_id = _uuid_or_none(raw_replay_request_id)
    if raw_event_id is not None and event_id is None:
        checks_failed.append("notification_plan_created_event_id_invalid")
    if raw_plan_id is not None and notification_plan_id is None:
        checks_failed.append("notification_plan_id_invalid")
    if raw_replay_request_id is not None and replay_request_id is None:
        checks_failed.append("replay_request_id_invalid")

    source_fixture = _path_or_none(getattr(args, "source_fixture", None))
    github_fixture = _path_or_none(getattr(args, "github_snapshot_fixture", None))
    replay_namespace = _string_or_none(getattr(args, "replay_namespace", None))
    prepare_fixture = bool(getattr(args, "prepare_send_disabled_e2e_fixture", False))
    fixture_selector_supplied = (
        source_fixture is not None or github_fixture is not None or replay_namespace is not None or prepare_fixture
    )
    fixture_selector_complete = (
        source_fixture is not None and github_fixture is not None and replay_namespace is not None and prepare_fixture
    )

    selector_modes = (
        int(raw_event_id is not None)
        + int(raw_plan_id is not None)
        + int(raw_replay_request_id is not None)
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
        replay_request_id=replay_request_id,
        fixture_selector_supplied=fixture_selector_supplied,
    )

    max_attempts = q_send_runner._max_attempts_from_args_env(  # noqa: SLF001
        getattr(args, "max_attempts", None),
        effective_env,
    )
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
        except Exception:  # noqa: BLE001
            checks_failed.append("source_fixture_load_failed")
        try:
            github_snapshot_runner.load_github_snapshot_fixture(github_fixture, repo_root=root)
        except Exception:  # noqa: BLE001
            checks_failed.append("github_snapshot_fixture_load_failed")

    if checks_failed:
        return _finish(report, checks_failed)

    active_resolver = resolver or DefaultPlanEventResolver()
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
            max_attempts=max_attempts or q_send_runner.dispatch_runner.request_runner.dlq_runner.DEFAULT_MAX_ATTEMPTS,
            env=effective_env,
            repo_root=root,
        )
    except Exception as exc:  # noqa: BLE001
        return _finish(report, [_safe_failure_code(exc)])

    report["send_disabled_e2e_fixture_prepared"] = resolution.send_disabled_e2e_fixture_prepared
    report["notification_plan_created_event_found"] = resolution.notification_plan_created_event_found
    checks_failed.extend(resolution.checks_failed)
    resolved_event_id = resolution.notification_plan_created_event_id
    if resolved_event_id is None:
        checks_failed.append("notification_plan_created_event_missing_or_invalid")
    if checks_failed:
        return _finish(report, checks_failed)

    active_notification_builder = notification_queue_builder or q_send_runner.SqlAlchemyThinQueueMessageBuilder()
    try:
        notification_build = active_notification_builder.build(
            database_url=args.database_url,
            notification_plan_created_event_id=resolved_event_id,
        )
    except Exception as exc:  # noqa: BLE001
        return _finish(report, [_safe_failure_code(exc)])
    report["q_notification_send_message_built"] = notification_build.thin_queue_message_built
    checks_failed.extend(notification_build.checks_failed)
    if notification_build.message is None:
        checks_failed.append("thin_queue_message_invalid")
    if checks_failed:
        return _finish(report, checks_failed)

    notification_message = notification_build.message
    notification_message_failures = q_send_runner._validate_thin_queue_message(notification_message)  # noqa: SLF001
    if notification_message_failures:
        return _finish(report, notification_message_failures)
    _apply_q_notification_shape(report, notification_message)

    delivery_dedupe_namespace = replay_namespace or _namespace_for_event(resolved_event_id)
    active_notification_consumer = notification_consumer or SqlAlchemyNotificationConsumer()
    try:
        notification_execution = active_notification_consumer.execute(
            database_url=args.database_url,
            message=notification_message,
            delivery_dedupe_namespace=delivery_dedupe_namespace,
        )
    except Exception as exc:  # noqa: BLE001
        return _finish(report, [_safe_failure_code(exc)])

    _apply_notification_execution(report, notification_execution)
    checks_failed.extend(notification_execution.checks_failed)
    delivery_result_event_id = notification_execution.notification_delivery_result_event_id
    if delivery_result_event_id is None:
        checks_failed.append("notification_delivery_result_event_missing_or_invalid")
    if checks_failed:
        return _finish(report, checks_failed)

    active_maintenance_builder = maintenance_queue_builder or q_maintenance_runner.SqlAlchemyThinQueueMessageBuilder()
    try:
        maintenance_build = active_maintenance_builder.build(
            database_url=args.database_url,
            notification_delivery_result_event_id=delivery_result_event_id,
        )
    except Exception as exc:  # noqa: BLE001
        return _finish(report, [_safe_failure_code(exc)])
    report["q_maintenance_message_built"] = maintenance_build.thin_queue_message_built
    checks_failed.extend(maintenance_build.checks_failed)
    if maintenance_build.message is None:
        checks_failed.append("thin_queue_message_invalid")
    if checks_failed:
        return _finish(report, checks_failed)

    maintenance_message = maintenance_build.message
    maintenance_message_failures = q_maintenance_runner._validate_thin_queue_message(maintenance_message)  # noqa: SLF001
    if maintenance_message_failures:
        return _finish(report, maintenance_message_failures)
    _apply_q_maintenance_shape(report, maintenance_message)

    active_maintenance_consumer = maintenance_consumer or SqlAlchemyMaintenanceConsumer()
    try:
        maintenance_execution = active_maintenance_consumer.execute(
            database_url=args.database_url,
            message=maintenance_message,
        )
    except Exception as exc:  # noqa: BLE001
        return _finish(report, [_safe_failure_code(exc)])

    _apply_maintenance_execution(report, maintenance_execution)
    checks_failed.extend(maintenance_execution.checks_failed)

    for key in TRUE_RESULT_KEYS:
        if report.get(key) is not True:
            checks_failed.append(f"{key}:missing")
    for key in FALSE_RESULT_KEYS:
        if report.get(key) is not False:
            checks_failed.append(f"{key}:unexpected")

    return _finish(report, checks_failed)


def validate_database_url(database_url: str | None):
    return q_send_runner.validate_database_url(database_url)


def _execute_send_disabled_notifier(
    connection: Any,
    *,
    notification_plan_created_event_id: UUID,
    delivery_dedupe_namespace: str,
    q_notification_send_message_validated: bool,
) -> NotificationConsumerExecutionResult:
    event = render_runner._load_plan_event_by_id(connection, notification_plan_created_event_id)  # noqa: SLF001
    if event is None:
        return _notification_consumer_result(
            q_notification_send_message_validated=q_notification_send_message_validated,
            checks_failed=("notification_plan_created_event_missing_or_invalid",),
        )
    intent = event.payload
    payload_failures = _validate_intent_payload(event.payload)
    if payload_failures:
        return _notification_consumer_result(
            q_notification_send_message_validated=q_notification_send_message_validated,
            notification_plan_created_event_rehydrated=True,
            checks_failed=payload_failures,
        )
    if event.aggregate_type != EXPECTED_Q_NOTIFICATION_ROOT or event.aggregate_id != intent.analysis_id:
        return _notification_consumer_result(
            q_notification_send_message_validated=q_notification_send_message_validated,
            notification_plan_created_event_rehydrated=True,
            checks_failed=("notification_plan_created_event_aggregate_mismatch",),
        )
    if intent.delivery_decision != "send_now":
        return _notification_consumer_result(
            q_notification_send_message_validated=q_notification_send_message_validated,
            notification_plan_created_event_rehydrated=True,
            checks_failed=("notification_intent_payload_invalid",),
        )

    analysis = notifier_base._load_analysis(connection, intent.analysis_id)  # noqa: SLF001
    if analysis is None:
        return _notification_consumer_result(
            q_notification_send_message_validated=q_notification_send_message_validated,
            notification_plan_created_event_rehydrated=True,
            analysis_loaded=False,
            checks_failed=("analysis_missing",),
        )
    judge_output = notifier_base._load_judge_output(connection, analysis.judge_output_id)  # noqa: SLF001
    candidate = notifier_base._load_candidate_context(connection, intent.candidate_group_id)  # noqa: SLF001
    checks_failed: list[str] = []
    if judge_output is None:
        checks_failed.append("judge_output_missing")
    if candidate is None:
        checks_failed.append("candidate_group_missing")
    context = notifier_runner._load_candidate_scope_context(connection, intent.candidate_group_id)  # noqa: SLF001
    artifact_loaded = notifier_runner._artifact_exists(  # noqa: SLF001
        connection,
        artifact_id=context["current_primary_artifact_id"],
    )
    source_message_loaded = notifier_runner._source_message_exists(  # noqa: SLF001
        connection,
        source_message_id=context["source_message_id"],
    )
    if not artifact_loaded:
        checks_failed.append("primary_artifact_missing")
    if not source_message_loaded:
        checks_failed.append("source_message_missing")
    if checks_failed:
        return _notification_consumer_result(
            q_notification_send_message_validated=q_notification_send_message_validated,
            notification_plan_created_event_rehydrated=True,
            analysis_loaded=True,
            judge_output_loaded=judge_output is not None,
            candidate_group_loaded=candidate is not None,
            artifact_loaded=artifact_loaded,
            source_message_loaded=source_message_loaded,
            checks_failed=checks_failed,
        )
    assert judge_output is not None
    assert candidate is not None

    context_failures = render_runner.context_failure_codes(  # noqa: SLF001
        intent=intent,
        analysis=analysis,
        judge_output=judge_output,
    )
    if context_failures:
        return _notification_consumer_result(
            q_notification_send_message_validated=q_notification_send_message_validated,
            notification_plan_created_event_rehydrated=True,
            analysis_loaded=True,
            judge_output_loaded=True,
            candidate_group_loaded=True,
            artifact_loaded=artifact_loaded,
            source_message_loaded=source_message_loaded,
            checks_failed=context_failures,
        )

    render = render_runner.build_notification_render(  # noqa: SLF001
        intent=intent,
        analysis=analysis,
        judge_output=judge_output,
        candidate=candidate,
    )
    before = _capture_notification_scope(connection, intent=intent, render=render, context=context)

    plan_id = notifier_base._insert_or_reuse_notification_plan(connection, intent=intent)  # noqa: SLF001
    if plan_id != intent.notification_plan_id:
        return _notification_consumer_result(
            q_notification_send_message_validated=q_notification_send_message_validated,
            notification_plan_created_event_rehydrated=True,
            analysis_loaded=True,
            judge_output_loaded=True,
            candidate_group_loaded=True,
            artifact_loaded=artifact_loaded,
            source_message_loaded=source_message_loaded,
            checks_failed=("notification_plan_material_conflict",),
        )
    if not _notification_plan_matches_send_disabled_intent(connection, intent=intent):
        checks_failed.append("notification_plan_intent_mismatch")
    render_id = notifier_base._insert_or_reuse_notification_render(connection, render=render)  # noqa: SLF001
    _mark_plan_status(connection, notification_plan_id=intent.notification_plan_id, status=DELIVERY_FROM_STATE)
    delivery_record_id = _insert_or_reuse_send_disabled_delivery_record(connection, intent=intent)
    _mark_plan_status(connection, notification_plan_id=intent.notification_plan_id, status=DELIVERY_TO_STATE)
    _insert_or_reuse_send_disabled_state_transition(connection, notification_plan_id=intent.notification_plan_id)
    _insert_or_reuse_send_disabled_delivery_result_event(
        connection,
        delivery_dedupe_namespace=delivery_dedupe_namespace,
        notification_plan_id=intent.notification_plan_id,
        notification_delivery_record_id=delivery_record_id,
        telegram_chat_id=intent.target_chat_id,
    )
    after = _capture_notification_scope(connection, intent=intent, render=render, context=context)
    delivery_result_event_id = _load_send_disabled_delivery_result_event_id(
        connection,
        delivery_dedupe_namespace=delivery_dedupe_namespace,
        notification_plan_id=intent.notification_plan_id,
        notification_delivery_record_id=delivery_record_id,
    )

    checks_failed.extend(
        _verify_send_disabled_notification_scope(
            connection,
            intent=intent,
            render=render,
            render_id=render_id,
            delivery_record_id=delivery_record_id,
            delivery_dedupe_namespace=delivery_dedupe_namespace,
            before=before,
            after=after,
        )
    )
    return NotificationConsumerExecutionResult(
        q_notification_send_message_validated=q_notification_send_message_validated,
        notification_plan_created_event_rehydrated=True,
        notification_plan_created_or_reused=after["notification_plans"] == 1,
        notification_render_created_or_reused=after["notification_renders"] == 1,
        send_disabled_delivery_record_created_or_reused=after["send_disabled_delivery_records"] == 1,
        notification_delivery_result_event_created_or_reused=after["delivery_result_events"] == 1,
        analysis_loaded=True,
        judge_output_loaded=True,
        candidate_group_loaded=True,
        artifact_loaded=artifact_loaded,
        source_message_loaded=source_message_loaded,
        notification_plan_count_before=int(before["notification_plans"]),
        notification_plan_count_after=int(after["notification_plans"]),
        notification_render_count_before=int(before["notification_renders"]),
        notification_render_count_after=int(after["notification_renders"]),
        send_disabled_delivery_record_count_before=int(before["send_disabled_delivery_records"]),
        send_disabled_delivery_record_count_after=int(after["send_disabled_delivery_records"]),
        notification_delivery_result_event_count_before=int(before["delivery_result_events"]),
        notification_delivery_result_event_count_after=int(after["delivery_result_events"]),
        notification_delivery_result_event_id=delivery_result_event_id,
        analysis_mutated=after["analysis_digest"] != before["analysis_digest"],
        judge_output_mutated=after["judge_output_digest"] != before["judge_output_digest"],
        candidate_group_mutated=after["candidate_group_digest"] != before["candidate_group_digest"],
        evidence_bundle_mutated=after["evidence_bundle_digest"] != before["evidence_bundle_digest"],
        artifact_mutated=after["artifact_digest"] != before["artifact_digest"],
        source_message_mutated=after["source_message_digest"] != before["source_message_digest"],
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _validate_intent_payload(intent: notifier_base.NotificationPlanIntent) -> tuple[str, ...]:
    raw = {
        "notification_plan_id": intent.notification_plan_id,
        "analysis_id": intent.analysis_id,
        "candidate_group_id": intent.candidate_group_id,
        "delivery_decision": intent.delivery_decision,
        "urgency_profile": intent.urgency_profile,
        "target_chat_id": intent.target_chat_id,
        "dedupe_subject_key": intent.dedupe_subject_key,
        "material_change_hash": intent.material_change_hash,
    }
    failures = [f"{key}:missing" for key in REQUIRED_INTENT_PAYLOAD_FIELDS if raw.get(key) in {None, ""}]
    return tuple(failures)


def _insert_or_reuse_send_disabled_delivery_record(
    connection: Any,
    *,
    intent: notifier_base.NotificationPlanIntent,
) -> UUID:
    import sqlalchemy as sa

    existing = _load_send_disabled_delivery_record_id(connection, notification_plan_id=intent.notification_plan_id)
    if existing is not None:
        return existing
    result = connection.execute(
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
            ) VALUES (
                CAST(:notification_plan_id AS uuid),
                :telegram_chat_id,
                NULL,
                'suppressed'::notification_status_enum,
                NULL,
                NULL,
                0,
                :transport_error_code,
                NULL,
                CAST(:telegram_response_json AS jsonb),
                now()
            )
            RETURNING notification_delivery_record_id
            """
        ),
        {
            "notification_plan_id": str(intent.notification_plan_id),
            "telegram_chat_id": intent.target_chat_id,
            "transport_error_code": SEND_DISABLED_REASON_CODE,
            "telegram_response_json": _json_dumps(SEND_DISABLED_RESPONSE),
        },
    )
    return UUID(str(result.scalar_one()))


def _load_send_disabled_delivery_record_id(connection: Any, *, notification_plan_id: UUID) -> UUID | None:
    import sqlalchemy as sa

    value = connection.execute(
        sa.text(
            """
            SELECT notification_delivery_record_id
            FROM notification_delivery_records
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
              AND delivery_status = 'suppressed'::notification_status_enum
              AND telegram_message_id IS NULL
              AND attempt_count = 0
              AND transport_error_code = :transport_error_code
              AND transport_error_class IS NULL
              AND telegram_response_json @> CAST(:telegram_response_json AS jsonb)
            ORDER BY created_at, notification_delivery_record_id
            LIMIT 1
            """
        ),
        {
            "notification_plan_id": str(notification_plan_id),
            "transport_error_code": SEND_DISABLED_REASON_CODE,
            "telegram_response_json": _json_dumps(
                {
                    "send_disabled": True,
                    "transport_skipped": True,
                    "reason_code": SEND_DISABLED_REASON_CODE,
                }
            ),
        },
    ).scalar_one_or_none()
    return UUID(str(value)) if value is not None else None


def _insert_or_reuse_send_disabled_state_transition(connection: Any, *, notification_plan_id: UUID) -> None:
    import sqlalchemy as sa

    existing = connection.execute(
        sa.text(
            """
            SELECT 1
            FROM state_transitions
            WHERE object_type = 'notification_plan'
              AND object_id = CAST(:notification_plan_id AS uuid)
              AND from_state = :from_state
              AND to_state = :to_state
              AND reason_code = :reason_code
            LIMIT 1
            """
        ),
        {
            "notification_plan_id": str(notification_plan_id),
            "from_state": DELIVERY_FROM_STATE,
            "to_state": DELIVERY_TO_STATE,
            "reason_code": SEND_DISABLED_REASON_CODE,
        },
    ).scalar_one_or_none()
    if existing:
        return
    connection.execute(
        sa.text(
            """
            INSERT INTO state_transitions (
                state_transition_id,
                object_type,
                object_id,
                from_state,
                to_state,
                reason_code,
                created_at
            ) VALUES (
                gen_random_uuid(),
                'notification_plan',
                CAST(:notification_plan_id AS uuid),
                :from_state,
                :to_state,
                :reason_code,
                now()
            )
            """
        ),
        {
            "notification_plan_id": str(notification_plan_id),
            "from_state": DELIVERY_FROM_STATE,
            "to_state": DELIVERY_TO_STATE,
            "reason_code": SEND_DISABLED_REASON_CODE,
        },
    )


def _insert_or_reuse_send_disabled_delivery_result_event(
    connection: Any,
    *,
    delivery_dedupe_namespace: str,
    notification_plan_id: UUID,
    notification_delivery_record_id: UUID,
    telegram_chat_id: int | None,
) -> None:
    import sqlalchemy as sa

    payload = build_send_disabled_delivery_result_payload(
        notification_plan_id=notification_plan_id,
        notification_delivery_record_id=notification_delivery_record_id,
        telegram_chat_id=telegram_chat_id,
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
                'notification_plan',
                CAST(:notification_plan_id AS uuid),
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
            "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
            "notification_plan_id": str(notification_plan_id),
            "dedupe_key": build_send_disabled_delivery_result_event_dedupe_key(
                delivery_dedupe_namespace=delivery_dedupe_namespace,
                notification_plan_id=notification_plan_id,
                notification_delivery_record_id=notification_delivery_record_id,
            ),
            "payload_json": _json_dumps(payload),
        },
    )


def build_send_disabled_delivery_result_payload(
    *,
    notification_plan_id: UUID,
    notification_delivery_record_id: UUID,
    telegram_chat_id: int | None,
) -> dict[str, Any]:
    return {
        "notification_plan_id": str(notification_plan_id),
        "notification_delivery_record_id": str(notification_delivery_record_id),
        "delivery_status": DELIVERY_TO_STATE,
        "telegram_chat_id": telegram_chat_id,
        "telegram_message_id": None,
        "attempt_count": 0,
        "transport_error_code": SEND_DISABLED_REASON_CODE,
        "transport_error_class": None,
        "edited": False,
        **SEND_DISABLED_RESPONSE,
    }


def build_send_disabled_delivery_result_event_dedupe_key(
    *,
    delivery_dedupe_namespace: str,
    notification_plan_id: UUID | str,
    notification_delivery_record_id: UUID | str,
) -> str:
    return (
        "local-db-send-disabled-delivery-e2e:"
        f"{delivery_dedupe_namespace}:notification.delivery.result:"
        f"{notification_plan_id}:{notification_delivery_record_id}"
    )


def _load_send_disabled_delivery_result_event_id(
    connection: Any,
    *,
    delivery_dedupe_namespace: str,
    notification_plan_id: UUID,
    notification_delivery_record_id: UUID,
) -> UUID | None:
    import sqlalchemy as sa

    value = connection.execute(
        sa.text(
            """
            SELECT event_id
            FROM event_outbox
            WHERE event_type = :event_type
              AND dedupe_key = :dedupe_key
            LIMIT 1
            """
        ),
        {
            "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
            "dedupe_key": build_send_disabled_delivery_result_event_dedupe_key(
                delivery_dedupe_namespace=delivery_dedupe_namespace,
                notification_plan_id=notification_plan_id,
                notification_delivery_record_id=notification_delivery_record_id,
            ),
        },
    ).scalar_one_or_none()
    return UUID(str(value)) if value is not None else None


def _mark_plan_status(connection: Any, *, notification_plan_id: UUID, status: str) -> None:
    render_runner._mark_plan_status(connection, notification_plan_id=notification_plan_id, status=status)  # noqa: SLF001


def _capture_notification_scope(
    connection: Any,
    *,
    intent: notifier_base.NotificationPlanIntent,
    render: notifier_base.NotificationRender,
    context: Mapping[str, UUID | None],
) -> dict[str, Any]:
    return {
        "notification_plans": _notification_plan_count(connection, intent=intent),
        "notification_renders": _notification_render_count(connection, render=render),
        "send_disabled_delivery_records": _send_disabled_delivery_record_count(
            connection,
            notification_plan_id=intent.notification_plan_id,
        ),
        "delivery_result_events": _send_disabled_delivery_result_event_count_for_plan(
            connection,
            notification_plan_id=intent.notification_plan_id,
        ),
        "analysis_digest": notifier_base._load_analysis_digest(connection, intent.analysis_id),  # noqa: SLF001
        "judge_output_digest": notifier_base._load_judge_output_digest_by_analysis(  # noqa: SLF001
            connection,
            intent.analysis_id,
        ),
        "candidate_group_digest": notifier_base._load_candidate_digest(  # noqa: SLF001
            connection,
            intent.candidate_group_id,
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
    }


def _notification_plan_count(connection: Any, *, intent: notifier_base.NotificationPlanIntent) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_plans
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND analysis_id = CAST(:analysis_id AS uuid)
                  AND candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND delivery_decision = CAST(:delivery_decision AS delivery_decision_enum)
                  AND urgency_profile = CAST(:urgency_profile AS urgency_profile_enum)
                  AND target_chat_id = :target_chat_id
                  AND material_change_hash = :material_change_hash
                """
            ),
            {
                "notification_plan_id": str(intent.notification_plan_id),
                "analysis_id": str(intent.analysis_id),
                "candidate_group_id": str(intent.candidate_group_id),
                "delivery_decision": intent.delivery_decision,
                "urgency_profile": intent.urgency_profile,
                "target_chat_id": intent.target_chat_id,
                "material_change_hash": intent.material_change_hash,
            },
        ).scalar_one()
    )


def _notification_plan_matches_send_disabled_intent(
    connection: Any,
    *,
    intent: notifier_base.NotificationPlanIntent,
) -> bool:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT notification_plan_id, analysis_id, candidate_group_id, delivery_decision,
                   urgency_profile, target_chat_id, target_thread_id, render_profile,
                   dedupe_subject_key, material_change_hash, suppress_reason_code
            FROM notification_plans
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
            """
        ),
        {"notification_plan_id": str(intent.notification_plan_id)},
    ).mappings().first()
    if row is None:
        return False
    return {
        "notification_plan_id": str(row["notification_plan_id"]),
        "analysis_id": str(row["analysis_id"]),
        "candidate_group_id": str(row["candidate_group_id"]),
        "delivery_decision": str(row["delivery_decision"]),
        "urgency_profile": str(row["urgency_profile"]),
        "target_chat_id": int(row["target_chat_id"]),
        "target_thread_id": notifier_base._int_or_none(row["target_thread_id"]),  # noqa: SLF001
        "render_profile": row["render_profile"],
        "dedupe_subject_key": row["dedupe_subject_key"],
        "material_change_hash": row["material_change_hash"],
        "suppress_reason_code": row["suppress_reason_code"],
    } == {
        "notification_plan_id": str(intent.notification_plan_id),
        "analysis_id": str(intent.analysis_id),
        "candidate_group_id": str(intent.candidate_group_id),
        "delivery_decision": intent.delivery_decision,
        "urgency_profile": intent.urgency_profile,
        "target_chat_id": intent.target_chat_id,
        "target_thread_id": intent.target_thread_id,
        "render_profile": intent.render_profile,
        "dedupe_subject_key": intent.dedupe_subject_key,
        "material_change_hash": intent.material_change_hash,
        "suppress_reason_code": intent.suppress_reason_code,
    }


def _notification_render_count(connection: Any, *, render: notifier_base.NotificationRender) -> int:
    import sqlalchemy as sa

    return int(
        connection.execute(
            sa.text(
                """
                SELECT count(*)
                FROM notification_renders
                WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND render_hash = :render_hash
                  AND message_text = :message_text
                """
            ),
            {
                "notification_plan_id": str(render.notification_plan_id),
                "render_hash": render.render_hash,
                "message_text": render.message_text,
            },
        ).scalar_one()
    )


def _send_disabled_delivery_record_count(connection: Any, *, notification_plan_id: UUID) -> int:
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
                  AND transport_error_class IS NULL
                  AND telegram_response_json @> CAST(:telegram_response_json AS jsonb)
                """
            ),
            {
                "notification_plan_id": str(notification_plan_id),
                "transport_error_code": SEND_DISABLED_REASON_CODE,
                "telegram_response_json": _json_dumps(
                    {
                        "send_disabled": True,
                        "transport_skipped": True,
                        "reason_code": SEND_DISABLED_REASON_CODE,
                    }
                ),
            },
        ).scalar_one()
    )


def _send_disabled_delivery_result_event_count_for_plan(connection: Any, *, notification_plan_id: UUID) -> int:
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
                  AND payload_json ->> 'transport_error_code' = :transport_error_code
                  AND payload_json ->> 'send_disabled' = 'true'
                  AND payload_json ->> 'transport_skipped' = 'true'
                """
            ),
            {
                "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
                "notification_plan_id": str(notification_plan_id),
                "transport_error_code": SEND_DISABLED_REASON_CODE,
            },
        ).scalar_one()
    )


def _verify_send_disabled_notification_scope(
    connection: Any,
    *,
    intent: notifier_base.NotificationPlanIntent,
    render: notifier_base.NotificationRender,
    render_id: UUID,
    delivery_record_id: UUID,
    delivery_dedupe_namespace: str,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> tuple[str, ...]:
    import sqlalchemy as sa

    expected_event_payload = build_send_disabled_delivery_result_payload(
        notification_plan_id=intent.notification_plan_id,
        notification_delivery_record_id=delivery_record_id,
        telegram_chat_id=intent.target_chat_id,
    )
    counts = connection.execute(
        sa.text(
            """
            SELECT
              (SELECT count(*) FROM notification_renders
               WHERE notification_render_id = CAST(:notification_render_id AS uuid)
                 AND notification_plan_id = CAST(:notification_plan_id AS uuid)
                 AND render_hash = :render_hash) AS render_by_id,
              (SELECT count(*) FROM notification_delivery_records
               WHERE notification_delivery_record_id = CAST(:notification_delivery_record_id AS uuid)
                 AND notification_plan_id = CAST(:notification_plan_id AS uuid)
                 AND delivery_status = 'suppressed'::notification_status_enum
                 AND telegram_chat_id = :telegram_chat_id
                 AND telegram_message_id IS NULL
                 AND attempt_count = 0
                 AND transport_error_code = :transport_error_code
                 AND transport_error_class IS NULL
                 AND telegram_response_json @> CAST(:telegram_response_json AS jsonb)) AS delivery_record_by_id,
              (SELECT count(*) FROM event_outbox
               WHERE event_type = :event_type
                 AND aggregate_type = 'notification_plan'
                 AND aggregate_id = CAST(:notification_plan_id AS uuid)
                 AND dedupe_key = :dedupe_key
                 AND payload_json = CAST(:payload_json AS jsonb)) AS delivery_result_event
            """
        ),
        {
            "notification_render_id": str(render_id),
            "notification_plan_id": str(intent.notification_plan_id),
            "render_hash": render.render_hash,
            "notification_delivery_record_id": str(delivery_record_id),
            "telegram_chat_id": intent.target_chat_id,
            "transport_error_code": SEND_DISABLED_REASON_CODE,
            "telegram_response_json": _json_dumps(SEND_DISABLED_RESPONSE),
            "event_type": NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE,
            "dedupe_key": build_send_disabled_delivery_result_event_dedupe_key(
                delivery_dedupe_namespace=delivery_dedupe_namespace,
                notification_plan_id=intent.notification_plan_id,
                notification_delivery_record_id=delivery_record_id,
            ),
            "payload_json": _json_dumps(expected_event_payload),
        },
    ).mappings().one()
    checks = {
        "notification_plan_created_or_reused": after["notification_plans"] == 1,
        "notification_render_created_or_reused": after["notification_renders"] == 1 and int(counts["render_by_id"]) == 1,
        "send_disabled_delivery_record_created_or_reused": (
            after["send_disabled_delivery_records"] == 1 and int(counts["delivery_record_by_id"]) == 1
        ),
        "notification_delivery_result_event_created_or_reused": (
            after["delivery_result_events"] == 1 and int(counts["delivery_result_event"]) == 1
        ),
        "analysis_mutated": after["analysis_digest"] != before["analysis_digest"],
        "judge_output_mutated": after["judge_output_digest"] != before["judge_output_digest"],
        "candidate_group_mutated": after["candidate_group_digest"] != before["candidate_group_digest"],
        "evidence_bundle_mutated": after["evidence_bundle_digest"] != before["evidence_bundle_digest"],
        "artifact_mutated": after["artifact_digest"] != before["artifact_digest"],
        "source_message_mutated": after["source_message_digest"] != before["source_message_digest"],
    }
    failures: list[str] = []
    for key in (
        "notification_plan_created_or_reused",
        "notification_render_created_or_reused",
        "send_disabled_delivery_record_created_or_reused",
        "notification_delivery_result_event_created_or_reused",
    ):
        if checks[key] is not True:
            failures.append(f"{key}:missing")
    for key in (
        "analysis_mutated",
        "judge_output_mutated",
        "candidate_group_mutated",
        "evidence_bundle_mutated",
        "artifact_mutated",
        "source_message_mutated",
    ):
        if checks[key] is not False:
            failures.append(f"{key}:unexpected")
    return tuple(failures)


def _classify_send_disabled_result(
    *,
    event: q_maintenance_runner.DeliveryResultEvent,
    record: q_maintenance_runner.DeliveryRecord,
) -> dict[str, Any]:
    if not q_maintenance_runner._delivery_result_matches_record(event=event, record=record):  # noqa: SLF001
        return {
            "delivery_result_classified": False,
            "maintenance_logical_noop": False,
            "send_disabled_recovery_mode": None,
            "reason_code": "notification_delivery_record_payload_mismatch",
            "checks_failed": ("notification_delivery_record_payload_mismatch",),
        }
    response = record.telegram_response_json or {}
    send_disabled_metadata_present = (
        event.delivery_status == "suppressed"
        and record.delivery_status == "suppressed"
        and record.telegram_message_id is None
        and record.attempt_count == 0
        and record.transport_error_code == SEND_DISABLED_REASON_CODE
        and record.transport_error_class is None
        and response.get("send_disabled") is True
        and response.get("transport_skipped") is True
        and response.get("reason_code") == SEND_DISABLED_REASON_CODE
        and response.get("dry_run") is not True
        and event.payload_json.get("send_disabled") is True
        and event.payload_json.get("transport_skipped") is True
    )
    if not send_disabled_metadata_present:
        return {
            "delivery_result_classified": True,
            "maintenance_logical_noop": False,
            "send_disabled_recovery_mode": None,
            "reason_code": "delivery_result_not_send_disabled_noop_target",
            "checks_failed": ("delivery_result_not_send_disabled_noop_target",),
        }
    decision = classify_delivery_result_send_disabled_noop(
        delivery_status=record.delivery_status,
        delivery_reason=record.transport_error_code,
    )
    maintenance_logical_noop = (
        decision.action == "mark_logical_noop_success"
        and decision.retry_intent_allowed is False
        and decision.dead_letter_allowed is False
        and decision.replay_dispatch_allowed is False
        and decision.replay_recovery_mode == SEND_DISABLED_RECOVERY_MODE
    )
    return {
        "delivery_result_classified": True,
        "maintenance_logical_noop": maintenance_logical_noop,
        "send_disabled_recovery_mode": decision.replay_recovery_mode,
        "reason_code": decision.reason_code,
        "checks_failed": () if maintenance_logical_noop else ("delivery_result_not_send_disabled_noop_target",),
    }


def _capture_maintenance_scope(
    connection: Any,
    *,
    event: q_maintenance_runner.DeliveryResultEvent,
    plan: q_maintenance_runner.NotificationPlanRecord,
    record: q_maintenance_runner.DeliveryRecord,
) -> dict[str, Any]:
    base = q_maintenance_runner._capture_scope(connection, event=event, plan=plan, record=record)  # noqa: SLF001
    return {
        **base,
        "notification_plan_created_retry_events": _retry_intent_event_count(
            connection,
            notification_plan_id=plan.notification_plan_id,
        ),
    }


def _maintenance_execution_from_scope(
    *,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    event: q_maintenance_runner.DeliveryResultEvent,
    record: q_maintenance_runner.DeliveryRecord,
    classification: Mapping[str, Any],
    q_maintenance_message_validated: bool,
    notification_delivery_result_event_rehydrated: bool,
) -> MaintenanceConsumerExecutionResult:
    checks_failed: list[str] = list(classification["checks_failed"])
    retry_intent_created = int(after["retry_intents"]) > int(before["retry_intents"])
    dead_letter_created = int(after["dead_letters"]) > int(before["dead_letters"])
    replay_request_created = int(after["replay_requests"]) > int(before["replay_requests"])
    replay_requested_event_created = int(after["replay_requested_events"]) > int(before["replay_requested_events"])
    notification_plan_created_retry_event_created = (
        int(after["notification_plan_created_retry_events"]) > int(before["notification_plan_created_retry_events"])
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
        ("notification_plan_created_retry_event_created", notification_plan_created_retry_event_created),
        *mutation_checks.items(),
    ):
        if value:
            checks_failed.append(flag)
    if event.notification_delivery_record_id is not None and event.notification_delivery_record_id != record.notification_delivery_record_id:
        checks_failed.append("notification_delivery_record_payload_mismatch")
    return MaintenanceConsumerExecutionResult(
        q_maintenance_message_validated=q_maintenance_message_validated,
        notification_delivery_result_event_rehydrated=notification_delivery_result_event_rehydrated,
        delivery_result_classified=bool(classification["delivery_result_classified"]),
        maintenance_logical_noop=bool(classification["maintenance_logical_noop"]),
        send_disabled_recovery_mode=_string_or_none(classification["send_disabled_recovery_mode"]),
        retry_intent_count_before=int(before["retry_intents"]),
        retry_intent_count_after=int(after["retry_intents"]),
        dead_letter_count_before=int(before["dead_letters"]),
        dead_letter_count_after=int(after["dead_letters"]),
        replay_request_count_before=int(before["replay_requests"]),
        replay_request_count_after=int(after["replay_requests"]),
        replay_requested_event_count_before=int(before["replay_requested_events"]),
        replay_requested_event_count_after=int(after["replay_requested_events"]),
        notification_plan_created_retry_event_count_before=int(before["notification_plan_created_retry_events"]),
        notification_plan_created_retry_event_count_after=int(after["notification_plan_created_retry_events"]),
        retry_intent_created=retry_intent_created,
        dead_letter_created=dead_letter_created,
        replay_request_created=replay_request_created,
        replay_requested_event_created=replay_requested_event_created,
        notification_plan_created_retry_event_created=notification_plan_created_retry_event_created,
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
            {
                "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                "notification_plan_id": str(notification_plan_id),
            },
        ).scalar_one()
    )


def _apply_q_notification_shape(report: dict[str, Any], message: RedisQueuedMessage) -> None:
    report["q_notification_send_message_fields"] = list(q_send_runner.REQUIRED_THIN_QUEUE_FIELDS)
    report["q_notification_send_payload_json_present"] = False
    report["q_notification_send_stage_name"] = message.stage_name
    report["q_notification_send_root_object_type"] = message.root_object_type
    report["q_notification_send_trigger_field"] = "trigger_event_id"


def _apply_q_maintenance_shape(report: dict[str, Any], message: RedisQueuedMessage) -> None:
    report["q_maintenance_message_fields"] = list(q_maintenance_runner.REQUIRED_THIN_QUEUE_FIELDS)
    report["q_maintenance_payload_json_present"] = False
    report["q_maintenance_stage_name"] = message.stage_name
    report["q_maintenance_root_object_type"] = message.root_object_type
    report["q_maintenance_trigger_field"] = "trigger_event_id"


def _apply_notification_execution(report: dict[str, Any], execution: NotificationConsumerExecutionResult) -> None:
    report.update(
        {
            "q_notification_send_message_validated": execution.q_notification_send_message_validated,
            "notification_plan_created_event_rehydrated": execution.notification_plan_created_event_rehydrated,
            "notification_plan_created_or_reused": execution.notification_plan_created_or_reused,
            "notification_render_created_or_reused": execution.notification_render_created_or_reused,
            "send_disabled_delivery_record_created_or_reused": (
                execution.send_disabled_delivery_record_created_or_reused
            ),
            "send_disabled_transport_error_code": (
                SEND_DISABLED_REASON_CODE if execution.send_disabled_delivery_record_created_or_reused else None
            ),
            "send_disabled_metadata_present": execution.send_disabled_delivery_record_created_or_reused,
            "send_disabled_transport_skipped": execution.send_disabled_delivery_record_created_or_reused,
            "dry_run_metadata_present": False,
            "notification_delivery_result_event_created_or_reused": (
                execution.notification_delivery_result_event_created_or_reused
            ),
            "analysis_loaded": execution.analysis_loaded,
            "judge_output_loaded": execution.judge_output_loaded,
            "candidate_group_loaded": execution.candidate_group_loaded,
            "artifact_loaded": execution.artifact_loaded,
            "source_message_loaded": execution.source_message_loaded,
            "notification_plan_count_before": execution.notification_plan_count_before,
            "notification_plan_count_after": execution.notification_plan_count_after,
            "notification_render_count_before": execution.notification_render_count_before,
            "notification_render_count_after": execution.notification_render_count_after,
            "send_disabled_delivery_record_count_before": execution.send_disabled_delivery_record_count_before,
            "send_disabled_delivery_record_count_after": execution.send_disabled_delivery_record_count_after,
            "notification_delivery_result_event_count_before": execution.notification_delivery_result_event_count_before,
            "notification_delivery_result_event_count_after": execution.notification_delivery_result_event_count_after,
            "notification_delivery_result_event_id_present": execution.notification_delivery_result_event_id is not None,
            "telegram_called": execution.telegram_called,
            "openai_called": execution.openai_called,
            "redis_mutation": execution.redis_mutation,
            "live_github_called": execution.live_github_called,
            "workers_started": execution.workers_started,
            "production_db_write": execution.production_db_write,
            "alembic_or_ddl_ran": execution.alembic_or_ddl_ran,
            "real_transport_attempted": execution.real_transport_attempted,
            "analysis_mutated": execution.analysis_mutated,
            "judge_output_mutated": execution.judge_output_mutated,
            "candidate_group_mutated": execution.candidate_group_mutated,
            "evidence_bundle_mutated": execution.evidence_bundle_mutated,
            "artifact_mutated": execution.artifact_mutated,
            "source_message_mutated": execution.source_message_mutated,
        }
    )


def _apply_maintenance_execution(report: dict[str, Any], execution: MaintenanceConsumerExecutionResult) -> None:
    report.update(
        {
            "q_maintenance_message_validated": execution.q_maintenance_message_validated,
            "notification_delivery_result_event_rehydrated": (
                execution.notification_delivery_result_event_rehydrated
            ),
            "delivery_result_classified": execution.delivery_result_classified,
            "maintenance_logical_noop": execution.maintenance_logical_noop,
            "send_disabled_recovery_mode": execution.send_disabled_recovery_mode,
            "retry_intent_count_before": execution.retry_intent_count_before,
            "retry_intent_count_after": execution.retry_intent_count_after,
            "dead_letter_count_before": execution.dead_letter_count_before,
            "dead_letter_count_after": execution.dead_letter_count_after,
            "replay_request_count_before": execution.replay_request_count_before,
            "replay_request_count_after": execution.replay_request_count_after,
            "replay_requested_event_count_before": execution.replay_requested_event_count_before,
            "replay_requested_event_count_after": execution.replay_requested_event_count_after,
            "notification_plan_created_retry_event_count_before": (
                execution.notification_plan_created_retry_event_count_before
            ),
            "notification_plan_created_retry_event_count_after": (
                execution.notification_plan_created_retry_event_count_after
            ),
            "retry_intent_created": execution.retry_intent_created,
            "dead_letter_created": execution.dead_letter_created,
            "replay_request_created": execution.replay_request_created,
            "replay_requested_event_created": execution.replay_requested_event_created,
            "notification_plan_created_retry_event_created": (
                execution.notification_plan_created_retry_event_created
            ),
            "notification_plan_mutated_by_maintenance": execution.notification_plan_mutated,
            "notification_render_mutated_by_maintenance": execution.notification_render_mutated,
            "notification_delivery_record_mutated_by_maintenance": execution.notification_delivery_record_mutated,
            "analysis_mutated": report["analysis_mutated"] or execution.analysis_mutated,
            "judge_output_mutated": report["judge_output_mutated"] or execution.judge_output_mutated,
            "candidate_group_mutated": report["candidate_group_mutated"] or execution.candidate_group_mutated,
            "evidence_bundle_mutated": report["evidence_bundle_mutated"] or execution.evidence_bundle_mutated,
            "artifact_mutated": report["artifact_mutated"] or execution.artifact_mutated,
            "source_message_mutated": report["source_message_mutated"] or execution.source_message_mutated,
            "telegram_called": report["telegram_called"] or execution.telegram_called,
            "openai_called": report["openai_called"] or execution.openai_called,
            "redis_mutation": report["redis_mutation"] or execution.redis_mutation,
            "live_github_called": report["live_github_called"] or execution.live_github_called,
            "workers_started": report["workers_started"] or execution.workers_started,
            "production_db_write": report["production_db_write"] or execution.production_db_write,
            "alembic_or_ddl_ran": report["alembic_or_ddl_ran"] or execution.alembic_or_ddl_ran,
            "real_transport_attempted": report["real_transport_attempted"] or execution.real_transport_attempted,
        }
    )


def _notification_consumer_result(
    *,
    q_notification_send_message_validated: bool = False,
    notification_plan_created_event_rehydrated: bool = False,
    notification_plan_created_or_reused: bool = False,
    notification_render_created_or_reused: bool = False,
    send_disabled_delivery_record_created_or_reused: bool = False,
    notification_delivery_result_event_created_or_reused: bool = False,
    analysis_loaded: bool = False,
    judge_output_loaded: bool = False,
    candidate_group_loaded: bool = False,
    artifact_loaded: bool = False,
    source_message_loaded: bool = False,
    checks_failed: Sequence[str],
) -> NotificationConsumerExecutionResult:
    return NotificationConsumerExecutionResult(
        q_notification_send_message_validated=q_notification_send_message_validated,
        notification_plan_created_event_rehydrated=notification_plan_created_event_rehydrated,
        notification_plan_created_or_reused=notification_plan_created_or_reused,
        notification_render_created_or_reused=notification_render_created_or_reused,
        send_disabled_delivery_record_created_or_reused=send_disabled_delivery_record_created_or_reused,
        notification_delivery_result_event_created_or_reused=notification_delivery_result_event_created_or_reused,
        analysis_loaded=analysis_loaded,
        judge_output_loaded=judge_output_loaded,
        candidate_group_loaded=candidate_group_loaded,
        artifact_loaded=artifact_loaded,
        source_message_loaded=source_message_loaded,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _maintenance_consumer_result(
    *,
    q_maintenance_message_validated: bool = False,
    notification_delivery_result_event_rehydrated: bool = False,
    delivery_result_classified: bool = False,
    maintenance_logical_noop: bool = False,
    send_disabled_recovery_mode: str | None = None,
    checks_failed: Sequence[str],
) -> MaintenanceConsumerExecutionResult:
    return MaintenanceConsumerExecutionResult(
        q_maintenance_message_validated=q_maintenance_message_validated,
        notification_delivery_result_event_rehydrated=notification_delivery_result_event_rehydrated,
        delivery_result_classified=delivery_result_classified,
        maintenance_logical_noop=maintenance_logical_noop,
        send_disabled_recovery_mode=send_disabled_recovery_mode,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "enable_notification_send_false_guard_passed": False,
        "notifier_telegram_dry_run_env": False,
        "send_disabled_e2e_fixture_prepared": False,
        "notification_plan_created_event_found": False,
        "q_notification_send_message_built": False,
        "q_notification_send_message_validated": False,
        "q_notification_send_payload_json_present": False,
        "notification_plan_created_event_rehydrated": False,
        "notification_plan_created_or_reused": False,
        "notification_render_created_or_reused": False,
        "send_disabled_delivery_record_created_or_reused": False,
        "send_disabled_transport_error_code": None,
        "send_disabled_metadata_present": False,
        "send_disabled_transport_skipped": False,
        "dry_run_metadata_present": False,
        "notification_delivery_result_event_created_or_reused": False,
        "notification_delivery_result_event_id_present": False,
        "q_maintenance_message_built": False,
        "q_maintenance_message_validated": False,
        "q_maintenance_payload_json_present": False,
        "notification_delivery_result_event_rehydrated": False,
        "delivery_result_classified": False,
        "maintenance_logical_noop": False,
        "send_disabled_recovery_mode": None,
        "analysis_loaded": False,
        "judge_output_loaded": False,
        "candidate_group_loaded": False,
        "artifact_loaded": False,
        "source_message_loaded": False,
        "notification_plan_count_before": 0,
        "notification_plan_count_after": 0,
        "notification_render_count_before": 0,
        "notification_render_count_after": 0,
        "send_disabled_delivery_record_count_before": 0,
        "send_disabled_delivery_record_count_after": 0,
        "notification_delivery_result_event_count_before": 0,
        "notification_delivery_result_event_count_after": 0,
        "retry_intent_count_before": 0,
        "retry_intent_count_after": 0,
        "dead_letter_count_before": 0,
        "dead_letter_count_after": 0,
        "replay_request_count_before": 0,
        "replay_request_count_after": 0,
        "replay_requested_event_count_before": 0,
        "replay_requested_event_count_after": 0,
        "notification_plan_created_retry_event_count_before": 0,
        "notification_plan_created_retry_event_count_after": 0,
        "retry_intent_created": False,
        "dead_letter_created": False,
        "replay_request_created": False,
        "replay_requested_event_created": False,
        "notification_plan_created_retry_event_created": False,
        "notification_plan_mutated_by_maintenance": False,
        "notification_render_mutated_by_maintenance": False,
        "notification_delivery_record_mutated_by_maintenance": False,
        "telegram_called": False,
        "openai_called": False,
        "live_github_called": False,
        "redis_mutation": False,
        "workers_started": False,
        "production_db_write": False,
        "alembic_or_ddl_ran": False,
        "real_transport_attempted": False,
        "analysis_mutated": False,
        "judge_output_mutated": False,
        "candidate_group_mutated": False,
        "evidence_bundle_mutated": False,
        "artifact_mutated": False,
        "source_message_mutated": False,
        "checks_failed": [],
    }


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=1 if normalized_failures else 0, report=report)


def _selector_mode(
    *,
    event_id: UUID | None,
    notification_plan_id: UUID | None,
    replay_request_id: UUID | None,
    fixture_selector_supplied: bool,
) -> str:
    if event_id is not None:
        return "notification_plan_created_event"
    if notification_plan_id is not None:
        return "notification_plan"
    if replay_request_id is not None:
        return "replay_request"
    if fixture_selector_supplied:
        return "fixture_chain"
    return "none"


def _namespace_for_event(event_id: UUID) -> str:
    return f"send-disabled-e2e-{event_id}"


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _falsey(value: Any) -> bool:
    return str(value or "").strip().lower() in {"0", "false", "no", "off"}


def _uuid_or_none(value: Any) -> UUID | None:
    if value in {None, ""}:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _path_or_none(value: Any) -> Path | None:
    text = _string_or_none(value)
    return Path(text) if text else None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


def _json_default(value: Any) -> str | None:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"unsupported json type: {type(value)!r}")


def _safe_failure_code(exc: Exception) -> str:
    text = str(exc).strip()
    if text in SAFE_EXCEPTION_MESSAGES:
        return text
    return type(exc).__name__


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main(argv: Sequence[str] | None = None) -> int:
    result = run(build_parser().parse_args(argv))
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
