from __future__ import annotations

import ast
from pathlib import Path
from uuid import UUID

from src.services.outbox_relay.models import RedisQueuedMessage
from tools import local_db_q_maintenance_delivery_result_logical_noop_fixture_runner as runner


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
REMOTE_PASSWORD_URL = f"{PG_SCHEME}://" + AUTHORITY_WITH_PASSWORD + "/prod"
EVENT_ID = UUID("10000000-0000-4000-8000-000000000001")
PLAN_ID = UUID("20000000-0000-4000-8000-000000000002")
RECORD_ID = UUID("30000000-0000-4000-8000-000000000003")


class FakeResolver:
    def __init__(
        self,
        *,
        event_id: UUID | None = EVENT_ID,
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
        notification_delivery_result_event_id: UUID | None,
        notification_plan_id: UUID | None,
        notification_delivery_record_id: UUID | None,
        source_fixture_path: Path | None,
        github_snapshot_fixture_path: Path | None,
        replay_namespace: str | None,
        prepare_delivery_result_fixture: bool,
        max_attempts: int,
        env,
        repo_root: Path,
    ) -> runner.DeliveryResultResolutionResult:
        if self.order is not None:
            self.order.append("predecessor")
        self.calls.append(
            {
                "database_url": database_url,
                "selector_mode": selector_mode,
                "notification_delivery_result_event_id": notification_delivery_result_event_id,
                "notification_plan_id": notification_plan_id,
                "notification_delivery_record_id": notification_delivery_record_id,
                "source_fixture_path": source_fixture_path,
                "github_snapshot_fixture_path": github_snapshot_fixture_path,
                "replay_namespace": replay_namespace,
                "prepare_delivery_result_fixture": prepare_delivery_result_fixture,
                "max_attempts": max_attempts,
                "env": dict(env),
                "repo_root": repo_root,
            }
        )
        return runner.DeliveryResultResolutionResult(
            notification_delivery_result_event_id=self.event_id,
            notification_delivery_result_event_found=self.found,
            delivery_result_fixture_prepared=self.prepared,
            checks_failed=self.checks_failed,
        )


class FakeQueueBuilder:
    def __init__(
        self,
        *,
        message: RedisQueuedMessage | None = None,
        built: bool = True,
        checks_failed: tuple[str, ...] = (),
        order: list[str] | None = None,
    ) -> None:
        self.calls = []
        self.message = message if message is not None else _message()
        self.built = built
        self.checks_failed = checks_failed
        self.order = order

    def build(
        self,
        *,
        database_url: str,
        notification_delivery_result_event_id: UUID,
    ) -> runner.ThinQueueBuildResult:
        if self.order is not None:
            self.order.append("queue")
        self.calls.append(
            {
                "database_url": database_url,
                "notification_delivery_result_event_id": notification_delivery_result_event_id,
            }
        )
        return runner.ThinQueueBuildResult(
            message=self.message,
            thin_queue_message_built=self.built,
            checks_failed=self.checks_failed,
        )


class FakeConsumer:
    def __init__(
        self,
        *,
        execution: runner.ThinConsumerExecutionResult | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.calls = []
        self.execution = execution or _successful_execution()
        self.order = order

    def execute(
        self,
        *,
        database_url: str,
        message: RedisQueuedMessage,
    ) -> runner.ThinConsumerExecutionResult:
        if self.order is not None:
            self.order.append("consumer")
        self.calls.append({"database_url": database_url, "message": message})
        return self.execution


def _parse_args(*args: str):
    return runner.build_parser().parse_args(args)


def _run(*args: str, env=None, resolver=None, queue_builder=None, consumer=None) -> runner.RunnerResult:
    return runner.run(
        _parse_args(*args),
        env=env or {"APP_ENV": "test"},
        resolver=resolver,
        queue_builder=queue_builder,
        consumer=consumer,
        repo_root=ROOT,
    )


def _message(
    *,
    stage_name: str = "maintenance",
    root_object_type: str = "notification_plan",
    root_object_id: str = str(PLAN_ID),
    trigger_event_id: str = str(EVENT_ID),
) -> RedisQueuedMessage:
    return RedisQueuedMessage(
        job_id=trigger_event_id or str(EVENT_ID),
        stage_name=stage_name,
        root_object_type=root_object_type,
        root_object_id=root_object_id,
        idempotency_key="unit-maintenance-delivery-result",
        pipeline_run_id=None,
        not_before=None,
        trigger_event_id=trigger_event_id,
    )


def _successful_execution() -> runner.ThinConsumerExecutionResult:
    return runner.ThinConsumerExecutionResult(
        thin_queue_message_validated=True,
        notification_delivery_result_event_rehydrated=True,
        notification_plan_loaded=True,
        notification_delivery_record_loaded=True,
        delivery_result_classified=True,
        maintenance_logical_noop=True,
        retry_intent_count_before=0,
        retry_intent_count_after=0,
        dead_letter_count_before=0,
        dead_letter_count_after=0,
        replay_request_count_before=2,
        replay_request_count_after=2,
        replay_requested_event_count_before=1,
        replay_requested_event_count_after=1,
        notification_plan_created_event_count_before=3,
        notification_plan_created_event_count_after=3,
    )


def _event_record_pair(
    *,
    delivery_status: str = "suppressed",
    transport_error_code: str | None = "dry_run_skip_transport",
    transport_error_class: str | None = None,
) -> tuple[runner.DeliveryResultEvent, runner.DeliveryRecord]:
    event = runner.DeliveryResultEvent(
        event_id=EVENT_ID,
        aggregate_type="notification_plan",
        aggregate_id=PLAN_ID,
        dedupe_key="unit-key",
        status="pending",
        fail_count=0,
        created_at=runner.datetime.now(runner.timezone.utc),
        notification_plan_id=PLAN_ID,
        notification_delivery_record_id=RECORD_ID,
        delivery_status=delivery_status,
        telegram_chat_id=12345,
        telegram_message_id=None,
        attempt_count=0,
        transport_error_code=transport_error_code,
        transport_error_class=transport_error_class,
        dry_run=True,
        transport_skipped=True,
        noop=True,
        payload_json={
            "notification_plan_id": str(PLAN_ID),
            "notification_delivery_record_id": str(RECORD_ID),
            "delivery_status": delivery_status,
            "telegram_chat_id": 12345,
            "telegram_message_id": None,
            "attempt_count": 0,
            "transport_error_code": transport_error_code,
            "transport_error_class": transport_error_class,
            "dry_run": True,
            "transport_skipped": True,
            "noop": True,
        },
    )
    record = runner.DeliveryRecord(
        notification_delivery_record_id=RECORD_ID,
        notification_plan_id=PLAN_ID,
        telegram_chat_id=12345,
        telegram_message_id=None,
        delivery_status=delivery_status,
        attempt_count=0,
        transport_error_code=transport_error_code,
        transport_error_class=transport_error_class,
        telegram_response_json={
            "dry_run": True,
            "transport_skipped": True,
            "noop": True,
            "reason_code": transport_error_code,
        },
    )
    return event, record


def test_app_env_test_required() -> None:
    consumer = FakeConsumer()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        env={"APP_ENV": "dev"},
        consumer=consumer,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["app_env_test_required"]
    assert consumer.calls == []


def test_confirm_local_test_db_required() -> None:
    consumer = FakeConsumer()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        consumer=consumer,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is True
    assert result.report["checks_failed"] == ["confirm_local_test_db_required"]
    assert consumer.calls == []


def test_unsafe_database_url_rejected() -> None:
    consumer = FakeConsumer()

    result = _run(
        "--database-url",
        REMOTE_UNSAFE_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        consumer=consumer,
    )

    assert result.exit_code == 1
    assert result.report["database_url_guard_passed"] is False
    assert "database_url_remote_host_rejected" in result.report["checks_failed"]
    assert consumer.calls == []


def test_enable_notification_send_true_rejected_before_mutation() -> None:
    consumer = FakeConsumer()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        env={"APP_ENV": "test", "ENABLE_NOTIFICATION_SEND": "true"},
        consumer=consumer,
    )

    assert result.exit_code == 1
    assert result.report["notification_send_disabled_or_unconfigured"] is False
    assert result.report["checks_failed"] == ["enable_notification_send_must_be_false"]
    assert consumer.calls == []


def test_selector_ambiguity_rejected_before_mutation() -> None:
    queue_builder = FakeQueueBuilder()
    consumer = FakeConsumer()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        queue_builder=queue_builder,
        consumer=consumer,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["selector_mode_ambiguous"]
    assert queue_builder.calls == []
    assert consumer.calls == []


def test_no_selector_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        queue_builder=FakeQueueBuilder(),
        consumer=FakeConsumer(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["selector_mode_required"]


def test_invalid_uuid_rejected() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        "not-a-uuid",
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        queue_builder=FakeQueueBuilder(),
        consumer=FakeConsumer(),
    )

    assert result.exit_code == 1
    assert "notification_delivery_result_event_id_invalid" in result.report["checks_failed"]


def test_explicit_notification_delivery_result_event_id_mode_builds_and_validates_thin_q_maintenance_message() -> None:
    resolver = FakeResolver()
    queue_builder = FakeQueueBuilder()
    consumer = FakeConsumer()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=resolver,
        queue_builder=queue_builder,
        consumer=consumer,
    )

    assert result.exit_code == 0
    assert resolver.calls[0]["selector_mode"] == "notification_delivery_result_event"
    assert queue_builder.calls[0]["notification_delivery_result_event_id"] == EVENT_ID
    assert consumer.calls[0]["message"].stage_name == "maintenance"
    assert consumer.calls[0]["message"].root_object_type == "notification_plan"
    assert consumer.calls[0]["message"].trigger_event_id == str(EVENT_ID)
    assert result.report["thin_queue_message_built"] is True
    assert result.report["thin_queue_message_validated"] is True
    assert result.report["notification_delivery_result_event_rehydrated"] is True
    assert result.report["thin_queue_message_payload_json_present"] is False


def test_notification_plan_id_mode_resolves_exactly_one_delivery_result_event() -> None:
    resolver = FakeResolver()
    queue_builder = FakeQueueBuilder()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        resolver=resolver,
        queue_builder=queue_builder,
        consumer=FakeConsumer(),
    )

    assert result.exit_code == 0
    assert resolver.calls[0]["selector_mode"] == "notification_plan"
    assert resolver.calls[0]["notification_plan_id"] == PLAN_ID
    assert queue_builder.calls[0]["notification_delivery_result_event_id"] == EVENT_ID


def test_notification_plan_id_mode_fails_ambiguity_safely() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-plan-id",
        str(PLAN_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(event_id=None, found=False, checks_failed=("notification_delivery_result_event_ambiguous",)),
        queue_builder=FakeQueueBuilder(),
        consumer=FakeConsumer(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == [
        "notification_delivery_result_event_ambiguous",
        "notification_delivery_result_event_missing_or_invalid",
    ]


def test_notification_delivery_record_id_mode_resolves_exactly_one_delivery_result_event() -> None:
    resolver = FakeResolver()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-record-id",
        str(RECORD_ID),
        "--confirm-local-test-db",
        resolver=resolver,
        queue_builder=FakeQueueBuilder(),
        consumer=FakeConsumer(),
    )

    assert result.exit_code == 0
    assert resolver.calls[0]["selector_mode"] == "notification_delivery_record"
    assert resolver.calls[0]["notification_delivery_record_id"] == RECORD_ID


def test_fixture_chain_mode_delegates_notification_send_predecessor_then_maintenance_consumer_path_using_fakes() -> None:
    order: list[str] = []
    resolver = FakeResolver(prepared=True, order=order)
    queue_builder = FakeQueueBuilder(order=order)
    consumer = FakeConsumer(order=order)

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--source-fixture",
        SOURCE_FIXTURE,
        "--github-snapshot-fixture",
        GITHUB_FIXTURE,
        "--replay-namespace",
        "unit-q-maintenance-noop",
        "--prepare-delivery-result-fixture",
        "--max-attempts",
        "3",
        "--confirm-local-test-db",
        resolver=resolver,
        queue_builder=queue_builder,
        consumer=consumer,
    )

    assert result.exit_code == 0
    assert order == ["predecessor", "queue", "consumer"]
    assert resolver.calls[0]["prepare_delivery_result_fixture"] is True
    assert resolver.calls[0]["replay_namespace"] == "unit-q-maintenance-noop"
    assert result.report["delivery_result_fixture_prepared"] is True


def test_malformed_queue_payload_fails_before_maintenance_classification() -> None:
    consumer = FakeConsumer()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        queue_builder=FakeQueueBuilder(message=_message(stage_name="notify")),
        consumer=consumer,
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["thin_queue_message_stage_invalid"]
    assert result.report["delivery_result_classified"] is False
    assert consumer.calls == []


def test_missing_trigger_event_id_fails_before_classification() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        queue_builder=FakeQueueBuilder(message=_message(trigger_event_id="")),
        consumer=FakeConsumer(),
    )

    assert result.exit_code == 1
    assert result.report["checks_failed"] == ["thin_queue_message_missing_trigger_event_id"]
    assert result.report["delivery_result_classified"] is False


def test_wrong_event_type_fails_before_classification() -> None:
    consumer = FakeConsumer()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        queue_builder=FakeQueueBuilder(
            message=None,
            built=False,
            checks_failed=("notification_delivery_result_event_type_invalid",),
        ),
        consumer=consumer,
    )

    assert result.exit_code == 1
    assert "notification_delivery_result_event_type_invalid" in result.report["checks_failed"]
    assert result.report["delivery_result_classified"] is False
    assert consumer.calls == []


def test_wrong_route_fails_before_classification() -> None:
    consumer = FakeConsumer()

    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        queue_builder=FakeQueueBuilder(
            message=None,
            built=False,
            checks_failed=("q_maintenance_queue_route_invalid",),
        ),
        consumer=consumer,
    )

    assert result.exit_code == 1
    assert "q_maintenance_queue_route_invalid" in result.report["checks_failed"]
    assert result.report["delivery_result_classified"] is False
    assert consumer.calls == []


def test_dry_run_suppressed_noop_delivery_result_is_classified_as_maintenance_logical_noop() -> None:
    event, record = _event_record_pair()

    classification = runner.classify_delivery_result_logical_noop(event=event, record=record)

    assert classification.delivery_result_classified is True
    assert classification.maintenance_logical_noop is True
    assert classification.checks_failed == ()


def test_retryable_delivery_result_is_not_treated_as_noop() -> None:
    event, record = _event_record_pair(
        delivery_status="failed_retryable",
        transport_error_code="telegram_retryable",
        transport_error_class="server_error_retryable",
    )

    classification = runner.classify_delivery_result_logical_noop(event=event, record=record)

    assert classification.delivery_result_classified is True
    assert classification.maintenance_logical_noop is False
    assert classification.checks_failed == ("delivery_result_retryable_not_noop",)


def test_no_retry_intent_dlq_or_replay_request_created_for_noop() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        queue_builder=FakeQueueBuilder(),
        consumer=FakeConsumer(),
    )

    assert result.exit_code == 0
    assert result.report["retry_intent_created"] is False
    assert result.report["dead_letter_created"] is False
    assert result.report["replay_request_created"] is False
    assert result.report["replay_requested_event_created"] is False


def test_idempotent_zero_to_zero_retry_and_dlq_counts() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        queue_builder=FakeQueueBuilder(),
        consumer=FakeConsumer(),
    )

    assert result.exit_code == 0
    assert result.report["retry_intent_count_before"] == 0
    assert result.report["retry_intent_count_after"] == 0
    assert result.report["dead_letter_count_before"] == 0
    assert result.report["dead_letter_count_after"] == 0
    assert result.report["replay_request_count_before"] == 2
    assert result.report["replay_request_count_after"] == 2
    assert result.report["notification_plan_created_event_count_before"] == 3
    assert result.report["notification_plan_created_event_count_after"] == 3


def test_no_telegram_openai_redis_network_worker_or_ddl_authority() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        queue_builder=FakeQueueBuilder(),
        consumer=FakeConsumer(),
    )

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


def test_no_upstream_or_notifier_source_row_mutation_booleans() -> None:
    result = _run(
        "--database-url",
        SAFE_SOCKET_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        queue_builder=FakeQueueBuilder(),
        consumer=FakeConsumer(),
    )

    for key in (
        "notification_plan_mutated",
        "notification_render_mutated",
        "notification_delivery_record_mutated",
        "analysis_mutated",
        "judge_output_mutated",
        "candidate_group_mutated",
        "evidence_bundle_mutated",
        "artifact_mutated",
        "source_message_mutated",
    ):
        assert result.report[key] is False


def test_sanitized_output_contains_no_database_url_password_or_traceback() -> None:
    result = _run(
        "--database-url",
        REMOTE_PASSWORD_URL,
        "--notification-delivery-result-event-id",
        str(EVENT_ID),
        "--confirm-local-test-db",
        resolver=FakeResolver(),
        queue_builder=FakeQueueBuilder(),
        consumer=FakeConsumer(),
    )

    rendered = runner.render_json(result.report)

    assert result.exit_code == 1
    assert SECRET_VALUE not in rendered
    assert REMOTE_PASSWORD_URL not in rendered
    assert "Traceback" not in rendered


def test_source_imports_do_not_open_forbidden_runtime_clients() -> None:
    source_path = ROOT / "tools/local_db_q_maintenance_delivery_result_logical_noop_fixture_runner.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    forbidden = {"redis", "openai", "telegram", "docker", "systemd", "requests", "httpx", "aiohttp"}

    assert not any(module.split(".")[0] in forbidden for module in imported_modules)


def test_source_contains_no_ddl_strings() -> None:
    source_path = ROOT / "tools/local_db_q_maintenance_delivery_result_logical_noop_fixture_runner.py"
    upper_source = source_path.read_text(encoding="utf-8").upper()

    for ddl in ("CREATE TABLE", "ALTER TABLE", "DROP TABLE", "TRUNCATE TABLE"):
        assert ddl not in upper_source
