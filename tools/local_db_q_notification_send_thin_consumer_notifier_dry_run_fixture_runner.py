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

from src.services.outbox_relay.models import OutboxEventRow, RedisQueuedMessage
from src.services.outbox_relay.routing import OutboxRouteResolver
from tools import local_db_delivery_replay_intent_notifier_dry_run_fixture_runner as notifier_runner


SCHEMA_VERSION = "local_db_q_notification_send_thin_consumer_notifier_dry_run_fixture_runner_v1"
NOTIFICATION_PLAN_CREATED_EVENT_TYPE = notifier_runner.NOTIFICATION_PLAN_CREATED_EVENT_TYPE
NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE = notifier_runner.NOTIFICATION_DELIVERY_RESULT_EVENT_TYPE
REPLAY_REQUESTED_EVENT_TYPE = notifier_runner.REPLAY_REQUESTED_EVENT_TYPE
RUNNER_INTERNAL_DRY_RUN_MODE = True
EXPECTED_QUEUE_NAME = "q.notification.send"
EXPECTED_STAGE_NAME = "notify"
EXPECTED_ROOT_OBJECT_TYPE = "analysis"
REQUIRED_THIN_QUEUE_FIELDS = (
    "job_id",
    "stage_name",
    "root_object_type",
    "root_object_id",
    "idempotency_key",
    "pipeline_run_id",
    "not_before",
    "trigger_event_id",
)
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "dry_run_mode_guard_passed",
    "thin_queue_message_built",
    "thin_queue_message_validated",
    "notification_plan_created_event_rehydrated",
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
    "replay_request_created",
    "replay_request_status_mutated",
    "replay_requested_event_created",
    "notification_plan_created_event_created",
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
    "fixture_selector_incomplete",
    "notification_plan_created_event_aggregate_mismatch",
    "notification_plan_created_event_missing_or_invalid",
    "notification_plan_created_event_payload_invalid",
    "notification_plan_created_event_type_invalid",
    "notification_plan_created_replay_intent_ambiguous",
    "notification_plan_created_replay_intent_missing_or_invalid",
    "notification_plan_created_replay_intent_payload_invalid",
    "notification_plan_missing_or_invalid",
    "q_notification_send_queue_route_invalid",
    "thin_queue_message_invalid",
    "thin_queue_message_missing_trigger_event_id",
    "thin_queue_message_payload_forbidden",
    "thin_queue_message_root_mismatch",
    "thin_queue_message_stage_invalid",
    "operator_approved_dead_letter_fixture_failed",
    "replay_request_ambiguous",
    "replay_request_missing_or_invalid",
}

dispatch_runner = notifier_runner.dispatch_runner
render_runner = notifier_runner.render_runner
source_candidate_runner = notifier_runner.source_candidate_runner
github_snapshot_runner = notifier_runner.github_snapshot_runner


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ThinQueueBuildResult:
    message: RedisQueuedMessage | None
    thin_queue_message_built: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ThinConsumerExecutionResult:
    thin_queue_message_validated: bool
    notification_plan_created_event_rehydrated: bool
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
    notification_render_count_before: int = 0
    notification_render_count_after: int = 0
    dry_run_delivery_record_count_before: int = 0
    dry_run_delivery_record_count_after: int = 0
    notification_delivery_result_event_count_before: int = 0
    notification_delivery_result_event_count_after: int = 0
    replay_request_count_before: int = 0
    replay_request_count_after: int = 0
    replay_requested_event_count_before: int = 0
    replay_requested_event_count_after: int = 0
    notification_plan_created_event_count_before: int = 0
    notification_plan_created_event_count_after: int = 0
    telegram_called: bool = False
    openai_called: bool = False
    redis_mutation: bool = False
    live_github_called: bool = False
    workers_started: bool = False
    production_db_write: bool = False
    alembic_or_ddl_ran: bool = False
    real_transport_attempted: bool = False
    replay_request_created: bool = False
    replay_request_status_mutated: bool = False
    replay_requested_event_created: bool = False
    notification_plan_created_event_created: bool = False
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
    ) -> notifier_runner.ReplayIntentResolutionResult: ...


class ThinQueueMessageBuilder(Protocol):
    def build(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
    ) -> ThinQueueBuildResult: ...


class NotifierDryRunExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
        delivery_dedupe_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> notifier_runner.ReplayIntentNotifierDryRunExecutionResult: ...


class ThinQueueConsumerExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        message: RedisQueuedMessage,
        delivery_dedupe_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> ThinConsumerExecutionResult: ...


class DefaultReplayIntentResolver:
    def __init__(self, *, delegate: ReplayIntentResolver | None = None) -> None:
        self._delegate = delegate or notifier_runner.DefaultReplayIntentResolver()

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
    ) -> notifier_runner.ReplayIntentResolutionResult:
        return self._delegate.resolve(
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


class SqlAlchemyThinQueueMessageBuilder:
    def __init__(self, *, route_resolver: OutboxRouteResolver | None = None) -> None:
        self._route_resolver = route_resolver or OutboxRouteResolver()

    def build(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
    ) -> ThinQueueBuildResult:
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                event, failures = _load_queue_event(
                    connection,
                    event_id=notification_plan_created_event_id,
                )
                if event is None:
                    return ThinQueueBuildResult(
                        message=None,
                        thin_queue_message_built=False,
                        checks_failed=failures or ("notification_plan_created_event_missing_or_invalid",),
                    )
                route = self._route_resolver.resolve(event)
                if route.queue_name != EXPECTED_QUEUE_NAME or route.stage_name != EXPECTED_STAGE_NAME:
                    return ThinQueueBuildResult(
                        message=None,
                        thin_queue_message_built=False,
                        checks_failed=("q_notification_send_queue_route_invalid",),
                    )
                return ThinQueueBuildResult(
                    message=RedisQueuedMessage(
                        job_id=str(event.event_id),
                        stage_name=route.stage_name,
                        root_object_type=event.aggregate_type,
                        root_object_id=str(event.aggregate_id),
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
    def __init__(self, *, notifier: NotifierDryRunExecutor | None = None) -> None:
        self._notifier = notifier or notifier_runner.SqlAlchemyReplayIntentNotifierDryRunExecutor()

    def execute(
        self,
        *,
        database_url: str,
        message: RedisQueuedMessage,
        delivery_dedupe_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
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
                event, event_failures = _load_queue_event(connection, event_id=trigger_event_id)
                if event is None:
                    return _consumer_result(
                        thin_queue_message_validated=True,
                        checks_failed=event_failures or ("notification_plan_created_event_missing_or_invalid",),
                    )
                root_object_id = _uuid_or_none(message.root_object_id)
                if (
                    message.root_object_type != event.aggregate_type
                    or root_object_id is None
                    or root_object_id != event.aggregate_id
                    or message.idempotency_key != event.dedupe_key
                    or message.job_id != message.trigger_event_id
                ):
                    return _consumer_result(
                        thin_queue_message_validated=True,
                        notification_plan_created_event_rehydrated=True,
                        checks_failed=("thin_queue_message_root_mismatch",),
                    )
        finally:
            engine.dispose()

        delegated = self._notifier.execute(
            database_url=database_url,
            notification_plan_created_event_id=trigger_event_id,
            delivery_dedupe_namespace=delivery_dedupe_namespace,
            env=env,
            repo_root=repo_root,
        )
        return _consumer_result_from_delegated(
            delegated,
            thin_queue_message_validated=True,
            notification_plan_created_event_rehydrated=True,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Model a local/test DB q.notification.send thin queue consumer for one "
            "notification.plan.created.v1 replay-intent event, rehydrate by trigger_event_id, "
            "and delegate to the existing notifier dry-run path without Redis, workers, or transport."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--notification-plan-created-event-id")
    parser.add_argument("--notification-plan-id")
    parser.add_argument("--replay-request-id")
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
    prepare_fixture = bool(getattr(args, "prepare_delivery_replay_intent_fixture", False))
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
    checks_failed.extend(resolution.checks_failed)
    resolved_event_id = resolution.notification_plan_created_event_id
    if resolved_event_id is None:
        checks_failed.append("notification_plan_created_replay_intent_missing_or_invalid")
    if checks_failed:
        return _finish(report, checks_failed)

    active_builder = queue_builder or SqlAlchemyThinQueueMessageBuilder()
    try:
        build_result = active_builder.build(
            database_url=args.database_url,
            notification_plan_created_event_id=resolved_event_id,
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
    delivery_dedupe_namespace = replay_namespace or _namespace_for_event(resolved_event_id)
    active_consumer = consumer or SqlAlchemyThinQueueConsumerExecutor()
    try:
        execution = active_consumer.execute(
            database_url=args.database_url,
            message=message,
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
    return notifier_runner.validate_database_url(database_url)


def _load_queue_event(connection: Any, *, event_id: UUID) -> tuple[OutboxEventRow | None, tuple[str, ...]]:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT event_id, event_type, aggregate_type, aggregate_id, dedupe_key, payload_json, status, fail_count,
                   created_at
            FROM event_outbox
            WHERE event_id = CAST(:event_id AS uuid)
            """
        ),
        {"event_id": str(event_id)},
    ).mappings().first()
    if row is None:
        return None, ("notification_plan_created_event_missing_or_invalid",)
    if str(row["event_type"]) != NOTIFICATION_PLAN_CREATED_EVENT_TYPE:
        return None, ("notification_plan_created_event_type_invalid",)

    payload = _json_loads(row["payload_json"])
    if not isinstance(payload, dict):
        return None, ("notification_plan_created_event_payload_invalid",)
    if notifier_runner._replay_intent_payload_missing_required_key(payload):  # noqa: SLF001
        return None, ("notification_plan_created_event_payload_invalid",)

    replay_event, failures = notifier_runner._load_replay_intent_event_by_id(connection, event_id=event_id)  # noqa: SLF001
    if replay_event is None:
        return None, tuple(
            "notification_plan_created_event_payload_invalid"
            if failure == "notification_plan_created_replay_intent_payload_invalid"
            else failure
            for failure in (failures or ("notification_plan_created_event_missing_or_invalid",))
        )

    aggregate_type = _string_or_none(row["aggregate_type"])
    aggregate_id = _uuid_or_none(row["aggregate_id"])
    if aggregate_type != EXPECTED_ROOT_OBJECT_TYPE or aggregate_id != replay_event.intent.analysis_id:
        return None, ("notification_plan_created_event_aggregate_mismatch",)

    created_at = row["created_at"]
    if not isinstance(created_at, datetime):
        created_at = datetime.now(timezone.utc)
    return (
        OutboxEventRow(
            event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            dedupe_key=str(row["dedupe_key"]),
            payload_json=payload,
            status=str(row["status"]),
            fail_count=int(row["fail_count"] or 0),
            created_at=created_at,
        ),
        (),
    )


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
    for key in ("job_id", "root_object_id", "idempotency_key"):
        if not _string_or_none(fields.get(key)):
            failures.append("thin_queue_message_invalid")
    return tuple(dict.fromkeys(failures))


def _apply_safe_queue_shape(report: dict[str, Any], message: RedisQueuedMessage) -> None:
    report["thin_queue_message_fields"] = list(REQUIRED_THIN_QUEUE_FIELDS)
    report["thin_queue_message_payload_json_present"] = False
    report["thin_queue_stage_name"] = message.stage_name
    report["thin_queue_root_object_type"] = message.root_object_type
    report["thin_queue_trigger_field"] = "trigger_event_id"
    report["event_payload_used_as_queue_payload"] = False


def _apply_execution(report: dict[str, Any], execution: ThinConsumerExecutionResult) -> None:
    report.update(
        {
            "thin_queue_message_validated": execution.thin_queue_message_validated,
            "notification_plan_created_event_rehydrated": execution.notification_plan_created_event_rehydrated,
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
            "notification_render_count_before": execution.notification_render_count_before,
            "notification_render_count_after": execution.notification_render_count_after,
            "dry_run_delivery_record_count_before": execution.dry_run_delivery_record_count_before,
            "dry_run_delivery_record_count_after": execution.dry_run_delivery_record_count_after,
            "notification_delivery_result_event_count_before": execution.notification_delivery_result_event_count_before,
            "notification_delivery_result_event_count_after": execution.notification_delivery_result_event_count_after,
            "replay_request_count_before": execution.replay_request_count_before,
            "replay_request_count_after": execution.replay_request_count_after,
            "replay_requested_event_count_before": execution.replay_requested_event_count_before,
            "replay_requested_event_count_after": execution.replay_requested_event_count_after,
            "notification_plan_created_event_count_before": execution.notification_plan_created_event_count_before,
            "notification_plan_created_event_count_after": execution.notification_plan_created_event_count_after,
            "telegram_called": execution.telegram_called,
            "openai_called": execution.openai_called,
            "redis_mutation": execution.redis_mutation,
            "live_github_called": execution.live_github_called,
            "workers_started": execution.workers_started,
            "production_db_write": execution.production_db_write,
            "alembic_or_ddl_ran": execution.alembic_or_ddl_ran,
            "real_transport_attempted": execution.real_transport_attempted,
            "replay_request_created": execution.replay_request_created,
            "replay_request_status_mutated": execution.replay_request_status_mutated,
            "replay_requested_event_created": execution.replay_requested_event_created,
            "notification_plan_created_event_created": execution.notification_plan_created_event_created,
            "analysis_mutated": execution.analysis_mutated,
            "judge_output_mutated": execution.judge_output_mutated,
            "candidate_group_mutated": execution.candidate_group_mutated,
            "evidence_bundle_mutated": execution.evidence_bundle_mutated,
            "artifact_mutated": execution.artifact_mutated,
            "source_message_mutated": execution.source_message_mutated,
        }
    )


def _consumer_result_from_delegated(
    delegated: notifier_runner.ReplayIntentNotifierDryRunExecutionResult,
    *,
    thin_queue_message_validated: bool,
    notification_plan_created_event_rehydrated: bool,
) -> ThinConsumerExecutionResult:
    return ThinConsumerExecutionResult(
        thin_queue_message_validated=thin_queue_message_validated,
        notification_plan_created_event_rehydrated=notification_plan_created_event_rehydrated,
        notification_plan_loaded_or_concretized=delegated.notification_plan_loaded_or_concretized,
        analysis_loaded=delegated.analysis_loaded,
        judge_output_loaded=delegated.judge_output_loaded,
        candidate_group_loaded=delegated.candidate_group_loaded,
        artifact_loaded=delegated.artifact_loaded,
        source_message_loaded=delegated.source_message_loaded,
        notification_render_created_or_reused=delegated.notification_render_created_or_reused,
        dry_run_delivery_record_created_or_reused=delegated.dry_run_delivery_record_created_or_reused,
        notification_delivery_result_event_created_or_reused=(
            delegated.notification_delivery_result_event_created_or_reused
        ),
        notification_render_dedupe_stable=delegated.notification_render_dedupe_stable,
        dry_run_delivery_record_dedupe_stable=delegated.dry_run_delivery_record_dedupe_stable,
        delivery_result_event_dedupe_stable=delegated.delivery_result_event_dedupe_stable,
        notification_render_count_before=delegated.notification_render_count_before,
        notification_render_count_after=delegated.notification_render_count_after,
        dry_run_delivery_record_count_before=delegated.dry_run_delivery_record_count_before,
        dry_run_delivery_record_count_after=delegated.dry_run_delivery_record_count_after,
        notification_delivery_result_event_count_before=delegated.notification_delivery_result_event_count_before,
        notification_delivery_result_event_count_after=delegated.notification_delivery_result_event_count_after,
        replay_request_count_before=delegated.replay_request_count_before,
        replay_request_count_after=delegated.replay_request_count_after,
        replay_requested_event_count_before=delegated.replay_requested_event_count_before,
        replay_requested_event_count_after=delegated.replay_requested_event_count_after,
        notification_plan_created_event_count_before=delegated.notification_plan_created_replay_intent_count_before,
        notification_plan_created_event_count_after=delegated.notification_plan_created_replay_intent_count_after,
        telegram_called=delegated.telegram_called,
        openai_called=delegated.openai_called,
        redis_mutation=delegated.redis_mutation,
        live_github_called=delegated.live_github_called,
        workers_started=delegated.workers_started,
        production_db_write=delegated.production_db_write,
        alembic_or_ddl_ran=delegated.alembic_or_ddl_ran,
        real_transport_attempted=delegated.real_transport_attempted,
        replay_request_created=delegated.replay_request_created,
        replay_request_status_mutated=delegated.replay_request_status_mutated,
        replay_requested_event_created=delegated.replay_requested_event_created,
        notification_plan_created_event_created=delegated.notification_plan_created_replay_intent_created,
        analysis_mutated=delegated.analysis_mutated,
        judge_output_mutated=delegated.judge_output_mutated,
        candidate_group_mutated=delegated.candidate_group_mutated,
        evidence_bundle_mutated=delegated.evidence_bundle_mutated,
        artifact_mutated=delegated.artifact_mutated,
        source_message_mutated=delegated.source_message_mutated,
        checks_failed=delegated.checks_failed,
    )


def _consumer_result(
    *,
    thin_queue_message_validated: bool = False,
    notification_plan_created_event_rehydrated: bool = False,
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
    checks_failed: Sequence[str],
) -> ThinConsumerExecutionResult:
    return ThinConsumerExecutionResult(
        thin_queue_message_validated=thin_queue_message_validated,
        notification_plan_created_event_rehydrated=notification_plan_created_event_rehydrated,
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
        checks_failed=tuple(dict.fromkeys(checks_failed)),
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
        "thin_queue_message_fields": list(REQUIRED_THIN_QUEUE_FIELDS),
        "thin_queue_message_payload_json_present": False,
        "thin_queue_stage_name": EXPECTED_STAGE_NAME,
        "thin_queue_root_object_type": EXPECTED_ROOT_OBJECT_TYPE,
        "thin_queue_trigger_field": "trigger_event_id",
        "event_payload_used_as_queue_payload": False,
        "notification_render_count_before": 0,
        "notification_render_count_after": 0,
        "dry_run_delivery_record_count_before": 0,
        "dry_run_delivery_record_count_after": 0,
        "notification_delivery_result_event_count_before": 0,
        "notification_delivery_result_event_count_after": 0,
        "replay_request_count_before": 0,
        "replay_request_count_after": 0,
        "replay_requested_event_count_before": 0,
        "replay_requested_event_count_after": 0,
        "notification_plan_created_event_count_before": 0,
        "notification_plan_created_event_count_after": 0,
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


def _max_attempts_from_args_env(value: Any, env: Mapping[str, str]) -> int | None:
    return notifier_runner._max_attempts_from_args_env(value, env)  # noqa: SLF001


def _namespace_for_event(event_id: UUID) -> str:
    return f"q-notification-send-{event_id}"


def _truthy(value: Any) -> bool:
    return notifier_runner._truthy(value)  # noqa: SLF001


def _falsey(value: Any) -> bool:
    return notifier_runner._falsey(value)  # noqa: SLF001


def _json_loads(value: Any) -> Any:
    return notifier_runner._json_loads(value)  # noqa: SLF001


def _uuid_or_none(value: Any) -> UUID | None:
    return notifier_runner._uuid_or_none(value)  # noqa: SLF001


def _path_or_none(value: Any) -> Path | None:
    return notifier_runner._path_or_none(value)  # noqa: SLF001


def _string_or_none(value: Any) -> str | None:
    return notifier_runner._string_or_none(value)  # noqa: SLF001


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
