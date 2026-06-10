from __future__ import annotations

import ast
from pathlib import Path
from uuid import UUID

from src.services.outbox_relay.models import RedisQueuedMessage
from tools import local_db_send_disabled_delivery_e2e_acceptance_runner as runner


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = "tests/fixtures/upstream/source_message_github_repo_signal.json"
GITHUB_FIXTURE = "tests/fixtures/upstream/github_repo_snapshot_example_tool.json"
PG_SCHEME = "postgresql+psycopg"
SAFE_DATABASE_NAME = "github_ai_catchbot_test"
SOCKET_HOST = "/var/run/postgresql"
SAFE_SOCKET_URL = f"{PG_SCHEME}:///{SAFE_DATABASE_NAME}?host={SOCKET_HOST}"
SECRET_VALUE = "local" + "_" + "secret"
AUTHORITY_WITH_PASSWORD = "local_user" + ":" + SECRET_VALUE + "@" + "127.0.0.1:5432"
PASSWORD_URL = f"{PG_SCHEME}://" + AUTHORITY_WITH_PASSWORD + f"/{SAFE_DATABASE_NAME}"
REMOTE_UNSAFE_URL = f"{PG_SCHEME}://db.example.invalid/prod"
PLAN_EVENT_ID = UUID("10000000-0000-4000-8000-000000000001")
DELIVERY_RESULT_EVENT_ID = UUID("20000000-0000-4000-8000-000000000002")
REPLAY_REQUEST_ID = UUID("30000000-0000-4000-8000-000000000003")
PLAN_ID = UUID("40000000-0000-4000-8000-000000000004")


class FakeResolver:
    def __init__(
        self,
        *,
        event_id: UUID | None = PLAN_EVENT_ID,
        found: bool = True,
        prepared: bool = False,
        checks_failed: tuple[str, ...] = (),
        order: list[str] | None = None,
    ) -> None:
        self.calls = []
        self.event_id = event_id
        self.found = found
        self.prepared = prepared
        self.checks_failed = checks_failed
        self.order = order

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
        env,
        repo_root: Path,
    ) -> runner.PlanEventResolutionResult:
        if self.order is not None:
            self.order.append("resolver")
        self.calls.append(
            {
                "database_url": database_url,
                "selector_mode": selector_mode,
                "notification_plan_created_event_id": notification_plan_created_event_id,
                "replay_request_id": replay_request_id,
                "notification_plan_id": notification_plan_id,
                "source_fixture_path": source_fixture_path,
                "github_snapshot_fixture_path": github_snapshot_fixture_path,
                "replay_namespace": replay_namespace,
                "max_attempts": max_attempts,
                "env": dict(env),
                "repo_root": repo_root,
            }
        )
        return runner.PlanEventResolutionResult(
            notification_plan_created_event_id=self.event_id,
            notification_plan_created_event_found=self.found,
            send_disabled_e2e_fixture_prepared=self.prepared,
            checks_failed=self.checks_failed,
        )


class FakeNotificationQueueBuilder:
    def __init__(
        self,
        *,
        message: RedisQueuedMessage | None = None,
        built: bool = True,
        checks_failed: tuple[str, ...] = (),
        order: list[str] | None = None,
    ) -> None:
        self.calls = []
        self.message = message if message is not None else _notification_message()
        self.built = built
        self.checks_failed = checks_failed
        self.order = order

    def build(
        self,
        *,
        database_url: str,
        notification_plan_created_event_id: UUID,
    ) -> runner.q_send_runner.ThinQueueBuildResult:
        if self.order is not None:
            self.order.append("q_notification_build")
        self.calls.append(
            {
                "database_url": database_url,
                "notification_plan_created_event_id": notification_plan_created_event_id,
            }
        )
        return runner.q_send_runner.ThinQueueBuildResult(
            message=self.message,
            thin_queue_message_built=self.built,
            checks_failed=self.checks_failed,
        )


class FakeNotificationConsumer:
    def __init__(
        self,
        *,
        execution: runner.NotificationConsumerExecutionResult | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.calls = []
        self.execution = execution or _successful_notification_execution()
        self.order = order

    def execute(
        self,
        *,
        database_url: str,
        message: RedisQueuedMessage,
        delivery_dedupe_namespace: str,
    ) -> runner.NotificationConsumerExecutionResult:
        if self.order is not None:
            self.order.append("q_notification_consume")
        self.calls.append(
            {
                "database_url": database_url,
                "message": message,
                "delivery_dedupe_namespace": delivery_dedupe_namespace,
            }
        )
        return self.execution


class FakeMaintenanceQueueBuilder:
    def __init__(
        self,
        *,
        message: RedisQueuedMessage | None = None,
        built: bool = True,
        checks_failed: tuple[str, ...] = (),
        order: list[str] | None = None,
    ) -> None:
        self.calls = []
        self.message = message if message is not None else _maintenance_message()
        self.built = built
        self.checks_failed = checks_failed
        self.order = order

    def build(
        self,
        *,
        database_url: str,
        notification_delivery_result_event_id: UUID,
    ) -> runner.q_maintenance_runner.ThinQueueBuildResult:
        if self.order is not None:
            self.order.append("q_maintenance_build")
        self.calls.append(
            {
                "database_url": database_url,
                "notification_delivery_result_event_id": notification_delivery_result_event_id,
            }
        )
        return runner.q_maintenance_runner.ThinQueueBuildResult(
            message=self.message,
            thin_queue_message_built=self.built,
            checks_failed=self.checks_failed,
        )


class FakeMaintenanceConsumer:
    def __init__(
        self,
        *,
        execution: runner.MaintenanceConsumerExecutionResult | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.calls = []
        self.execution = execution or _successful_maintenance_execution()
        self.order = order

    def execute(
        self,
        *,
        database_url: str,
        message: RedisQueuedMessage,
    ) -> runner.MaintenanceConsumerExecutionResult:
        if self.order is not None:
            self.order.append("q_maintenance_consume")
        self.calls.append({"database_url": database_url, "message": message})
        return self.execution


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def _env(**overrides: str) -> dict[str, str]:
    env = {
        "APP_ENV": "test",
        "ENABLE_NOTIFICATION_SEND": "false",
        "NOTIFIER_TELEGRAM_DRY_RUN": "false",
    }
    env.update(overrides)
    return env


def _run(
    *args: str,
    env=None,
    resolver=None,
    notification_queue_builder=None,
    notification_consumer=None,
    maintenance_queue_builder=None,
    maintenance_consumer=None,
) -> runner.RunnerResult:
    return runner.run(
        _parse_args(*args),
        env=env or _env(),
        resolver=resolver,
        notification_queue_builder=notification_queue_builder,
        notification_consumer=notification_consumer,
        maintenance_queue_builder=maintenance_queue_builder,
        maintenance_consumer=maintenance_consumer,
        repo_root=ROOT,
    )


def _notification_message(
    *,
    stage_name: str = "notify",
    root_object_type: str = "analysis",
    root_object_id: str = str(PLAN_ID),
    trigger_event_id: str = str(PLAN_EVENT_ID),
) -> RedisQueuedMessage:
    return RedisQueuedMessage(
        job_id=trigger_event_id or str(PLAN_EVENT_ID),
        stage_name=stage_name,
        root_object_type=root_object_type,
        root_object_id=root_object_id,
        idempotency_key="unit-plan-created",
        pipeline_run_id=None,
        not_before=None,
        trigger_event_id=trigger_event_id,
    )


def _maintenance_message(
    *,
    stage_name: str = "maintenance",
    root_object_type: str = "notification_plan",
    root_object_id: str = str(PLAN_ID),
    trigger_event_id: str = str(DELIVERY_RESULT_EVENT_ID),
) -> RedisQueuedMessage:
    return RedisQueuedMessage(
        job_id=trigger_event_id or str(DELIVERY_RESULT_EVENT_ID),
        stage_name=stage_name,
        root_object_type=root_object_type,
        root_object_id=root_object_id,
        idempotency_key="unit-delivery-result",
        pipeline_run_id=None,
        not_before=None,
        trigger_event_id=trigger_event_id,
    )


def _successful_notification_execution(
    *,
    plan_before: int = 0,
    plan_after: int = 1,
    render_before: int = 0,
    render_after: int = 1,
    record_before: int = 0,
    record_after: int = 1,
    event_before: int = 0,
    event_after: int = 1,
    delivery_result_event_id: UUID | None = DELIVERY_RESULT_EVENT_ID,
) -> runner.NotificationConsumerExecutionResult:
    return runner.NotificationConsumerExecutionResult(
        q_notification_send_message_validated=True,
        notification_plan_created_event_rehydrated=True,
        notification_plan_created_or_reused=True,
        notification_render_created_or_reused=True,
        send_disabled_delivery_record_created_or_reused=True,
        notification_delivery_result_event_created_or_reused=True,
        analysis_loaded=True,
        judge_output_loaded=True,
        candidate_group_loaded=True,
        artifact_loaded=True,
        source_message_loaded=True,
        notification_plan_count_before=plan_before,
        notification_plan_count_after=plan_after,
        notification_render_count_before=render_before,
        notification_render_count_after=render_after,
        send_disabled_delivery_record_count_before=record_before,
        send_disabled_delivery_record_count_after=record_after,
        notification_delivery_result_event_count_before=event_before,
        notification_delivery_result_event_count_after=event_after,
        notification_delivery_result_event_id=delivery_result_event_id,
    )


def _successful_maintenance_execution(
    *,
    retry_before: int = 0,
    retry_after: int = 0,
    dead_before: int = 0,
    dead_after: int = 0,
    replay_before: int = 0,
    replay_after: int = 0,
    replay_event_before: int = 0,
    replay_event_after: int = 0,
    retry_event_before: int = 0,
    retry_event_after: int = 0,
) -> runner.MaintenanceConsumerExecutionResult:
    return runner.MaintenanceConsumerExecutionResult(
        q_maintenance_message_validated=True,
        notification_delivery_result_event_rehydrated=True,
        delivery_result_classified=True,
        maintenance_logical_noop=True,
        send_disabled_recovery_mode="explicit_delivery_replay_only",
        retry_intent_count_before=retry_before,
        retry_intent_count_after=retry_after,
        dead_letter_count_before=dead_before,
        dead_letter_count_after=dead_after,
        replay_request_count_before=replay_before,
        replay_request_count_after=replay_after,
        replay_requested_event_count_before=replay_event_before,
        replay_requested_event_count_after=replay_event_after,
        notification_plan_created_retry_event_count_before=retry_event_before,
        notification_plan_created_retry_event_count_after=retry_event_after,
    )


def _full_success(**kwargs) -> runner.RunnerResult:
    return _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(PLAN_EVENT_ID),
        "--confirm-local-test-db",
        resolver=kwargs.get("resolver", FakeResolver()),
        notification_queue_builder=kwargs.get("notification_queue_builder", FakeNotificationQueueBuilder()),
        notification_consumer=kwargs.get("notification_consumer", FakeNotificationConsumer()),
        maintenance_queue_builder=kwargs.get("maintenance_queue_builder", FakeMaintenanceQueueBuilder()),
        maintenance_consumer=kwargs.get("maintenance_consumer", FakeMaintenanceConsumer()),
    )


def test_app_env_test_required() -> None:
    consumer = FakeNotificationConsumer()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(PLAN_EVENT_ID),
        "--confirm-local-test-db",
        env=_env(APP_ENV="dev"),
        notification_consumer=consumer,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["app_env_test_required"]
    assert consumer.calls == []


def test_confirm_local_test_db_required() -> None:
    consumer = FakeNotificationConsumer()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(PLAN_EVENT_ID),
        notification_consumer=consumer,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is True
    assert result.report["checks_failed"] == ["confirm_local_test_db_required"]
    assert consumer.calls == []


def test_enable_notification_send_false_required() -> None:
    consumer = FakeNotificationConsumer()
    env = _env()
    del env["ENABLE_NOTIFICATION_SEND"]

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(PLAN_EVENT_ID),
        "--confirm-local-test-db",
        env=env,
        notification_consumer=consumer,
    )

    assert result.exit_code == 1
    assert result.report["enable_notification_send_false_guard_passed"] is False
    assert result.report["checks_failed"] == ["enable_notification_send_must_be_false"]
    assert consumer.calls == []


def test_unsafe_database_url_rejected() -> None:
    consumer = FakeNotificationConsumer()

    result = _run(
        "--database-url",
        REMOTE_UNSAFE_URL,
        "--notification-plan-created-event-id",
        str(PLAN_EVENT_ID),
        "--confirm-local-test-db",
        notification_consumer=consumer,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is False
    assert "database_url_remote_host_rejected" in result.report["checks_failed"]
    assert consumer.calls == []


def test_selector_ambiguity_rejected_before_mutation() -> None:
    notification_consumer = FakeNotificationConsumer()
    maintenance_consumer = FakeMaintenanceConsumer()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(PLAN_EVENT_ID),
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        notification_consumer=notification_consumer,
        maintenance_consumer=maintenance_consumer,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["selector_mode_ambiguous"]
    assert notification_consumer.calls == []
    assert maintenance_consumer.calls == []


def test_no_selector_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        notification_queue_builder=FakeNotificationQueueBuilder(),
        notification_consumer=FakeNotificationConsumer(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["selector_mode_required"]


def test_invalid_uuid_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        "not-a-uuid",
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        notification_queue_builder=FakeNotificationQueueBuilder(),
        notification_consumer=FakeNotificationConsumer(),
    )

    assert result.exit_code == 1
    assert "notification_plan_created_event_id_invalid" in result.report["checks_failed"]


def test_explicit_notification_plan_created_event_id_mode_builds_q_notification_send_thin_message() -> None:
    resolver = FakeResolver()
    notification_builder = FakeNotificationQueueBuilder()
    notification_consumer = FakeNotificationConsumer()
    maintenance_builder = FakeMaintenanceQueueBuilder()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-created-event-id",
        str(PLAN_EVENT_ID),
        "--confirm-local-test-db",
        resolver=resolver,
        notification_queue_builder=notification_builder,
        notification_consumer=notification_consumer,
        maintenance_queue_builder=maintenance_builder,
        maintenance_consumer=FakeMaintenanceConsumer(),
    )

    assert result.exit_code == 0
    assert resolver.calls[0]["selector_mode"] == "notification_plan_created_event"
    assert resolver.calls[0]["notification_plan_created_event_id"] == PLAN_EVENT_ID
    assert notification_builder.calls[0]["notification_plan_created_event_id"] == PLAN_EVENT_ID
    assert notification_consumer.calls[0]["message"].stage_name == "notify"
    assert notification_consumer.calls[0]["message"].trigger_event_id == str(PLAN_EVENT_ID)
    assert result.report["q_notification_send_message_built"] is True
    assert result.report["q_notification_send_message_validated"] is True


def test_q_notification_send_message_has_no_payload_json() -> None:
    result = _full_success()

    assert result.exit_code == 0
    assert result.report["q_notification_send_payload_json_present"] is False
    assert "payload_json" not in result.report["q_notification_send_message_fields"]


def test_selected_notification_plan_event_is_rehydrated_by_trigger_event_id() -> None:
    result = _full_success()

    assert result.exit_code == 0
    assert result.report["notification_plan_created_event_rehydrated"] is True


def test_plan_and_render_are_created_or_reused() -> None:
    result = _full_success()

    assert result.exit_code == 0
    assert result.report["notification_plan_created_or_reused"] is True
    assert result.report["notification_render_created_or_reused"] is True


def test_send_disabled_transport_skip_creates_or_reuses_suppressed_delivery_record() -> None:
    result = _full_success()

    assert result.exit_code == 0
    assert result.report["send_disabled_delivery_record_created_or_reused"] is True


def test_send_disabled_delivery_record_uses_notification_send_flag_disabled() -> None:
    payload = runner.build_send_disabled_delivery_result_payload(
        notification_plan_id=PLAN_ID,
        notification_delivery_record_id=DELIVERY_RESULT_EVENT_ID,
        telegram_chat_id=12345,
    )

    assert payload["delivery_status"] == "suppressed"
    assert payload["transport_error_code"] == "notification_send_flag_disabled"
    assert payload["attempt_count"] == 0
    assert payload["telegram_message_id"] is None


def test_send_disabled_metadata_is_distinct_from_dry_run_metadata() -> None:
    payload = runner.build_send_disabled_delivery_result_payload(
        notification_plan_id=PLAN_ID,
        notification_delivery_record_id=DELIVERY_RESULT_EVENT_ID,
        telegram_chat_id=12345,
    )

    assert payload["send_disabled"] is True
    assert payload["transport_skipped"] is True
    assert payload["reason_code"] == "notification_send_flag_disabled"
    assert payload["dry_run"] is False
    assert payload["transport_error_code"] != "dry_run_skip_transport"


def test_delivery_result_event_is_created_or_reused() -> None:
    result = _full_success()

    assert result.exit_code == 0
    assert result.report["notification_delivery_result_event_created_or_reused"] is True
    assert result.report["notification_delivery_result_event_id_present"] is True


def test_q_maintenance_thin_message_is_built_from_delivery_result_event() -> None:
    maintenance_builder = FakeMaintenanceQueueBuilder()

    result = _full_success(maintenance_queue_builder=maintenance_builder)

    assert result.exit_code == 0
    assert maintenance_builder.calls[0]["notification_delivery_result_event_id"] == DELIVERY_RESULT_EVENT_ID
    assert result.report["q_maintenance_message_built"] is True
    assert result.report["q_maintenance_message_validated"] is True


def test_q_maintenance_message_has_no_payload_json() -> None:
    result = _full_success()

    assert result.exit_code == 0
    assert result.report["q_maintenance_payload_json_present"] is False
    assert "payload_json" not in result.report["q_maintenance_message_fields"]


def test_maintenance_classifies_send_disabled_suppression_as_logical_noop() -> None:
    result = _full_success()

    assert result.exit_code == 0
    assert result.report["delivery_result_classified"] is True
    assert result.report["maintenance_logical_noop"] is True


def test_send_disabled_suppression_is_explicit_replay_only_not_auto_retry() -> None:
    result = _full_success()

    assert result.exit_code == 0
    assert result.report["send_disabled_recovery_mode"] == "explicit_delivery_replay_only"
    assert result.report["retry_intent_created"] is False


def test_no_retry_dlq_replay_request_or_replay_requested_event_created() -> None:
    result = _full_success()

    assert result.exit_code == 0
    assert result.report["retry_intent_created"] is False
    assert result.report["dead_letter_created"] is False
    assert result.report["replay_request_created"] is False
    assert result.report["replay_requested_event_created"] is False
    assert result.report["notification_plan_created_retry_event_created"] is False


def test_idempotent_rerun_keeps_counts_stable() -> None:
    result = _full_success(
        notification_consumer=FakeNotificationConsumer(
            execution=_successful_notification_execution(
                plan_before=1,
                plan_after=1,
                render_before=1,
                render_after=1,
                record_before=1,
                record_after=1,
                event_before=1,
                event_after=1,
            )
        ),
        maintenance_consumer=FakeMaintenanceConsumer(
            execution=_successful_maintenance_execution(
                retry_before=1,
                retry_after=1,
                dead_before=2,
                dead_after=2,
                replay_before=3,
                replay_after=3,
                replay_event_before=4,
                replay_event_after=4,
                retry_event_before=5,
                retry_event_after=5,
            )
        ),
    )

    assert result.exit_code == 0
    assert result.report["notification_plan_count_before"] == 1
    assert result.report["notification_plan_count_after"] == 1
    assert result.report["notification_render_count_before"] == 1
    assert result.report["notification_render_count_after"] == 1
    assert result.report["send_disabled_delivery_record_count_before"] == 1
    assert result.report["send_disabled_delivery_record_count_after"] == 1
    assert result.report["notification_delivery_result_event_count_before"] == 1
    assert result.report["notification_delivery_result_event_count_after"] == 1
    assert result.report["retry_intent_count_before"] == 1
    assert result.report["retry_intent_count_after"] == 1
    assert result.report["dead_letter_count_before"] == 2
    assert result.report["dead_letter_count_after"] == 2
    assert result.report["replay_request_count_before"] == 3
    assert result.report["replay_request_count_after"] == 3
    assert result.report["replay_requested_event_count_before"] == 4
    assert result.report["replay_requested_event_count_after"] == 4


def test_no_telegram_openai_redis_network_worker_or_ddl_authority() -> None:
    result = _full_success()

    assert result.exit_code == 0
    for key in (
        "telegram_called",
        "openai_called",
        "redis_mutation",
        "live_github_called",
        "workers_started",
        "production_db_write",
        "alembic_or_ddl_ran",
        "real_transport_attempted",
    ):
        assert result.report[key] is False


def test_no_upstream_mutation_booleans() -> None:
    result = _full_success()

    assert result.exit_code == 0
    for key in (
        "analysis_mutated",
        "judge_output_mutated",
        "candidate_group_mutated",
        "evidence_bundle_mutated",
        "artifact_mutated",
        "source_message_mutated",
    ):
        assert result.report[key] is False


def test_sanitized_output_omits_db_url_password_and_traceback() -> None:
    result = _run(
        "--database-url",
        PASSWORD_URL,
        "--notification-plan-created-event-id",
        str(PLAN_EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
    )

    rendered = runner.render_json(result.report)
    assert result.exit_code == 1
    assert PASSWORD_URL not in rendered
    assert SECRET_VALUE not in rendered
    assert "Traceback" not in rendered


def test_source_import_check_has_no_forbidden_runtime_clients() -> None:
    source = (ROOT / "tools/local_db_send_disabled_delivery_e2e_acceptance_runner.py").read_text()
    tree = ast.parse(source)
    forbidden = {"redis", "openai", "telegram", "docker", "systemd", "requests", "httpx", "aiohttp"}
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert not (imported & forbidden)


def test_source_contains_no_ddl_sql_statements() -> None:
    source = (ROOT / "tools/local_db_send_disabled_delivery_e2e_acceptance_runner.py").read_text().lower()
    forbidden = ("create table", "alter table", "drop table", "truncate table")
    assert all(token not in source for token in forbidden)


def test_fixture_chain_mode_uses_existing_predecessor_then_send_disabled_e2e_path() -> None:
    order: list[str] = []
    resolver = FakeResolver(prepared=True, order=order)
    notification_builder = FakeNotificationQueueBuilder(order=order)
    notification_consumer = FakeNotificationConsumer(order=order)
    maintenance_builder = FakeMaintenanceQueueBuilder(order=order)
    maintenance_consumer = FakeMaintenanceConsumer(order=order)

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-send-disabled-e2e",
        "--prepare-send-disabled-e2e-fixture",
        "--max-attempts",
        "3",
        "--confirm-local-test-db",
        resolver=resolver,
        notification_queue_builder=notification_builder,
        notification_consumer=notification_consumer,
        maintenance_queue_builder=maintenance_builder,
        maintenance_consumer=maintenance_consumer,
    )

    assert result.exit_code == 0
    assert order == [
        "resolver",
        "q_notification_build",
        "q_notification_consume",
        "q_maintenance_build",
        "q_maintenance_consume",
    ]
    assert resolver.calls[0]["selector_mode"] == "fixture_chain"
    assert result.report["send_disabled_e2e_fixture_prepared"] is True
