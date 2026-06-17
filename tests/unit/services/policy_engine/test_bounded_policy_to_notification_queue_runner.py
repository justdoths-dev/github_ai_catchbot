from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.services.outbox_relay.models import OutboxEventRow
from src.services.policy_engine.bounded_policy_apply_runner import _build_analysis
from src.services.policy_engine.bounded_policy_to_notification_queue_runner import (
    BoundedPolicyToNotificationQueueConfig,
    NotificationQueuePublishRepositoryHandle,
    NotificationQueueRedisPublisherHandle,
    run_bounded_policy_to_notification_queue_sync,
)
from src.services.policy_engine.notification_intent import NotificationIntentBuilder
from tests.unit.services.policy_engine.test_bounded_policy_apply_runner import (
    ANALYSIS_ID,
    BUNDLE_ID,
    CANDIDATE_GROUP_ID,
    CHAT_ID,
    DB_LOCATOR,
    IDEMPOTENCY_SENTINEL,
    JUDGE_OUTPUT_ID,
    JUDGE_RUN_ID,
    NOTIFICATION_EVENT_ID,
    POLICY_APPLY_EVENT_ID,
    RAW_EXCEPTION_SENTINEL,
    RAW_PAYLOAD_SENTINEL,
    REDIS_LOCATOR,
    REDIS_MESSAGE_ID,
    FakeRedisBuilder,
    FakeRedisClient,
    FakeRepository,
    FakeRepositoryBuilder,
    _bundle,
    _config,
    _existing_analysis,
    _inspect_scores,
    _judge_output,
    _judge_run,
    _notification_outbox,
    _redis_message,
    _runtime_config,
    _skip_scores,
)


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/policy_engine/bounded_policy_to_notification_queue_runner.py"
TOOL_PATH = ROOT / "tools/bounded_policy_to_notification_queue_runner.py"
NOTIFY_REDIS_MESSAGE_ID = "1800000333444-0"
FORBIDDEN_REPORT_TEXT = "private notification failure detail must not print"


class FakeNotificationRepository:
    def __init__(
        self,
        source_rows: list[OutboxEventRow],
        *,
        order: list[str] | None = None,
        fail_mark_published: bool = False,
    ) -> None:
        self.source_rows = source_rows
        self.order = order
        self.fail_mark_published = fail_mark_published
        self.load_calls: list[Any] = []
        self.mark_published_calls: list[Any] = []
        self.job_attempt_calls: list[dict[str, Any]] = []

    async def load_event(self, event_id):
        if self.order is not None:
            self.order.append("notify:load")
        self.load_calls.append(event_id)
        for row in self.source_rows:
            if row.event_id == event_id:
                return row
        return None

    async def mark_published(self, *, event_id, published_at=None) -> None:
        del published_at
        if self.order is not None:
            self.order.append("notify:mark_published")
        if self.fail_mark_published:
            raise RuntimeError(FORBIDDEN_REPORT_TEXT)
        self.mark_published_calls.append(event_id)

    async def insert_job_attempt(self, **kwargs) -> None:
        if self.order is not None:
            self.order.append("notify:insert_job_attempt")
        self.job_attempt_calls.append(kwargs)


class FakeNotificationRepositoryBuilder:
    def __init__(self, repository: FakeNotificationRepository, *, order: list[str] | None = None) -> None:
        self.repository = repository
        self.order = order
        self.calls = 0
        self.close_commits: list[bool] = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.notification_repository_opened = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            if commit:
                state.notification_outbox_commit_attempted = True
                if self.order is not None:
                    self.order.append("notify:commit")
            elif self.order is not None:
                self.order.append("notify:rollback")

        return NotificationQueuePublishRepositoryHandle(repository=self.repository, close=close)


class FakeNotificationPublisher:
    def __init__(self, *, order: list[str] | None = None, failure: BaseException | None = None) -> None:
        self.order = order
        self.failure = failure
        self.publish_calls: list[tuple[Any, Any]] = []

    async def publish(self, route, message) -> str:
        if self.order is not None:
            self.order.append("notify:publish")
        self.publish_calls.append((route, message))
        if self.failure is not None:
            raise self.failure
        return NOTIFY_REDIS_MESSAGE_ID


class FakeNotificationPublisherBuilder:
    def __init__(self, publisher: FakeNotificationPublisher) -> None:
        self.publisher = publisher
        self.calls = 0
        self.close_calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.redis_publisher_created = True

        async def close() -> None:
            self.close_calls += 1

        return NotificationQueueRedisPublisherHandle(publisher=self.publisher, close=close)


def _macro_config(*, mode: str = "execute", **overrides) -> BoundedPolicyToNotificationQueueConfig:
    values = {
        "mode": mode,
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_redis_read": True,
        "allow_database_read": True,
        "allow_redis_consume": mode == "execute",
        "allow_database_write": mode == "execute",
        "allow_redis_ack": mode == "execute",
        "allow_notification_outbox_publish": mode == "execute",
        "allow_notification_redis_publish": mode == "execute",
        "allow_redis_group_create": False,
        "trigger_event_suffix": "3d5b3290",
        "judge_run_suffix": "7a111d13",
        "judge_output_suffix": "c7d7ef5e",
        "notification_plan_event_suffix": None,
        "scan_limit": 25,
    }
    values.update(overrides)
    return BoundedPolicyToNotificationQueueConfig(**values)


def _run_macro(
    policy_repository: FakeRepository,
    *,
    client: FakeRedisClient | None = None,
    config: BoundedPolicyToNotificationQueueConfig | None = None,
    notification_repository: FakeNotificationRepository | None = None,
    notification_publisher: FakeNotificationPublisher | None = None,
):
    client = client or FakeRedisClient([_redis_message()])
    notification_repository = notification_repository or FakeNotificationRepository(policy_repository.notification_rows)
    notification_publisher = notification_publisher or FakeNotificationPublisher()
    result = run_bounded_policy_to_notification_queue_sync(
        config or _macro_config(),
        runtime_config_loader=lambda: _runtime_config(),
        policy_redis_builder=FakeRedisBuilder(client),
        policy_repository_builder=FakeRepositoryBuilder(policy_repository),
        notification_repository_builder=FakeNotificationRepositoryBuilder(notification_repository),
        notification_redis_publisher_builder=FakeNotificationPublisherBuilder(notification_publisher),
    )
    return result, client, notification_repository, notification_publisher


def _matching_existing_notification_row() -> OutboxEventRow:
    analysis, evaluation = _build_analysis(
        policy_config=_runtime_config().to_policy_config(),
        judge_run=_judge_run(),
        judge_output=_judge_output(_inspect_scores()),
        bundle=_bundle(),
    )
    intent = NotificationIntentBuilder(config=_runtime_config().to_policy_config()).build(
        analysis_id=ANALYSIS_ID,
        analysis=analysis,
        evaluation=evaluation,
    )
    assert intent is not None
    return _notification_outbox(intent=intent)


def test_preview_suppress_target_reports_no_notification_and_no_side_effects() -> None:
    repository = FakeRepository(judge_output=_judge_output(_skip_scores(), model_proposed_verdict="skip"))
    client = FakeRedisClient([_redis_message()])

    result, client, notification_repository, notification_publisher = _run_macro(
        repository,
        client=client,
        config=_macro_config(mode="preview"),
    )
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["status"] == "policy_suppressed_no_notification"
    assert report["policy_apply_status"] == "preview"
    assert report["delivery_decision"] == "suppress"
    assert report["notification_outbox_found"] is False
    assert report["q_notification_send_published"] is False
    assert client.xreadgroup_calls == 0
    assert client.acked == []
    assert repository.inserted_analyses == []
    assert repository.notification_rows == []
    assert notification_repository.load_calls == []
    assert notification_publisher.publish_calls == []
    assert report["side_effects"]["policy_db_write"] is False
    assert report["side_effects"]["notification_redis_publish_called"] is False


def test_execute_suppress_target_acks_after_safe_policy_completion_without_notification() -> None:
    order: list[str] = []
    repository = FakeRepository(
        judge_output=_judge_output(_skip_scores(), model_proposed_verdict="skip"),
        order=order,
    )
    client = FakeRedisClient([_redis_message()], order=order)

    result, client, notification_repository, notification_publisher = _run_macro(repository, client=client)
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["status"] == "policy_suppressed_no_notification"
    assert report["policy_apply_status"] == "applied"
    assert report["delivery_decision"] == "suppress"
    assert report["notification_outbox_found"] is False
    assert report["q_notification_send_published"] is False
    assert repository.inserted_analyses
    assert repository.notification_rows == []
    assert client.acked == [REDIS_MESSAGE_ID]
    assert notification_repository.load_calls == []
    assert notification_publisher.publish_calls == []
    assert order == ["db:commit", "redis:ack"]


def test_preview_non_suppress_target_reports_would_create_intent_and_publish_without_mutation() -> None:
    repository = FakeRepository()
    client = FakeRedisClient([_redis_message()])

    result, client, notification_repository, notification_publisher = _run_macro(
        repository,
        client=client,
        config=_macro_config(mode="preview"),
    )
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["status"] == "preview_non_suppress_would_publish_notification_queue"
    assert report["policy_apply_status"] == "preview"
    assert report["verdict"] == "inspect_now"
    assert report["delivery_decision"] == "send_now"
    assert report["planned_action"]["expected_notification_intent_action"] == "would_create_notification_plan_intent"
    assert report["planned_action"]["expected_outbox_relay_action"] == (
        "would_publish_exact_notification_plan_event_to_q_notification_send"
    )
    assert report["planned_action"]["q_notification_send_would_receive_thin_message"] is True
    assert report["notification_outbox_written"] is False
    assert report["q_notification_send_published"] is False
    assert client.xreadgroup_calls == 0
    assert client.acked == []
    assert repository.inserted_analyses == []
    assert repository.notification_rows == []
    assert notification_repository.load_calls == []
    assert notification_publisher.publish_calls == []


def test_execute_non_suppress_creates_intent_publishes_exact_thin_message_marks_published_then_acks() -> None:
    order: list[str] = []
    repository = FakeRepository(order=order)
    client = FakeRedisClient([_redis_message()], order=order)
    notification_repository = FakeNotificationRepository(repository.notification_rows, order=order)
    notification_publisher = FakeNotificationPublisher(order=order)

    result = run_bounded_policy_to_notification_queue_sync(
        _macro_config(),
        runtime_config_loader=lambda: _runtime_config(),
        policy_redis_builder=FakeRedisBuilder(client),
        policy_repository_builder=FakeRepositoryBuilder(repository),
        notification_repository_builder=FakeNotificationRepositoryBuilder(notification_repository, order=order),
        notification_redis_publisher_builder=FakeNotificationPublisherBuilder(notification_publisher),
    )
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["status"] == "notification_queue_handoff_published"
    assert report["notification_outbox_written"] is True
    assert report["notification_outbox_published"] is True
    assert report["q_notification_send_published"] is True
    assert report["q_notification_send_message_suffix"] == "333444-0"
    assert report["notification_plan_event_suffix"] == "b7b2b7b2"
    assert repository.notification_rows[0].event_id == NOTIFICATION_EVENT_ID
    assert notification_repository.mark_published_calls == [NOTIFICATION_EVENT_ID]
    assert notification_repository.job_attempt_calls == [
        {
            "stage_name": "notify",
            "queue_name": "q.notification.send",
            "root_object_type": "analysis",
            "root_object_id": ANALYSIS_ID,
            "attempt_status": "succeeded",
            "error_code": None,
        }
    ]
    assert client.acked == [REDIS_MESSAGE_ID]
    assert order == [
        "db:commit",
        "notify:load",
        "notify:publish",
        "notify:mark_published",
        "notify:insert_job_attempt",
        "notify:commit",
        "redis:ack",
    ]
    route, message = notification_publisher.publish_calls[0]
    fields = message.as_stream_fields()
    assert route.queue_name == "q.notification.send"
    assert route.stage_name == "notify"
    assert fields == {
        "job_id": str(NOTIFICATION_EVENT_ID),
        "stage_name": "notify",
        "root_object_type": "analysis",
        "root_object_id": str(ANALYSIS_ID),
        "idempotency_key": repository.notification_rows[0].dedupe_key,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(NOTIFICATION_EVENT_ID),
    }


def test_existing_notification_intent_reuse_does_not_duplicate_and_publishes_existing_once() -> None:
    existing_row = _matching_existing_notification_row()
    repository = FakeRepository(existing_analysis=_existing_analysis(), notification_rows=[existing_row])
    client = FakeRedisClient([_redis_message()])
    notification_repository = FakeNotificationRepository(repository.notification_rows)
    notification_publisher = FakeNotificationPublisher()

    result = run_bounded_policy_to_notification_queue_sync(
        _macro_config(notification_plan_event_suffix=str(existing_row.event_id)[-8:]),
        runtime_config_loader=lambda: _runtime_config(),
        policy_redis_builder=FakeRedisBuilder(client),
        policy_repository_builder=FakeRepositoryBuilder(repository),
        notification_repository_builder=FakeNotificationRepositoryBuilder(notification_repository),
        notification_redis_publisher_builder=FakeNotificationPublisherBuilder(notification_publisher),
    )
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    assert report["planned_action"]["policy_action"] == "reuse_existing_analysis_and_notification_intent"
    assert report["planned_action"]["expected_notification_intent_action"] == "reuse_notification_plan_intent"
    assert report["notification_outbox_written"] is False
    assert report["notification_outbox_published"] is True
    assert len(repository.notification_rows) == 1
    assert notification_repository.load_calls == [existing_row.event_id]
    assert len(notification_publisher.publish_calls) == 1
    assert client.acked == [REDIS_MESSAGE_ID]


def test_duplicate_notification_intent_rows_fail_closed_without_publish_or_ack() -> None:
    row = _matching_existing_notification_row()
    repository = FakeRepository(existing_analysis=_existing_analysis(), notification_rows=[row, row])
    client = FakeRedisClient([_redis_message()])
    notification_repository = FakeNotificationRepository(repository.notification_rows)
    notification_publisher = FakeNotificationPublisher()

    result = run_bounded_policy_to_notification_queue_sync(
        _macro_config(),
        runtime_config_loader=lambda: _runtime_config(),
        policy_redis_builder=FakeRedisBuilder(client),
        policy_repository_builder=FakeRepositoryBuilder(repository),
        notification_repository_builder=FakeNotificationRepositoryBuilder(notification_repository),
        notification_redis_publisher_builder=FakeNotificationPublisherBuilder(notification_publisher),
    )
    report = result.to_sanitized_dict()

    assert report["ok"] is False
    assert report["status"] == "policy_apply_failed"
    assert report["error_code"] == "duplicate_notification_plan_intent_outbox"
    assert notification_repository.load_calls == []
    assert notification_publisher.publish_calls == []
    assert client.acked == []


def test_outbox_publish_failure_leaves_policy_target_unacked_and_sanitizes_failure() -> None:
    repository = FakeRepository()
    client = FakeRedisClient([_redis_message()])
    notification_repository = FakeNotificationRepository(repository.notification_rows)
    notification_publisher = FakeNotificationPublisher(failure=RuntimeError(RAW_EXCEPTION_SENTINEL))

    result = run_bounded_policy_to_notification_queue_sync(
        _macro_config(),
        runtime_config_loader=lambda: _runtime_config(),
        policy_redis_builder=FakeRedisBuilder(client),
        policy_repository_builder=FakeRepositoryBuilder(repository),
        notification_repository_builder=FakeNotificationRepositoryBuilder(notification_repository),
        notification_redis_publisher_builder=FakeNotificationPublisherBuilder(notification_publisher),
    )
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, ensure_ascii=False, sort_keys=True)

    assert report["ok"] is False
    assert report["status"] == "notification_queue_handoff_failed"
    assert report["error_code"] == "notification_redis_xadd_failed"
    assert repository.commits == 1
    assert notification_repository.mark_published_calls == []
    assert notification_repository.job_attempt_calls == []
    assert client.acked == []
    assert RAW_EXCEPTION_SENTINEL not in rendered
    assert FORBIDDEN_REPORT_TEXT not in rendered


def test_forbidden_payload_fields_never_enter_q_notification_send_message() -> None:
    repository = FakeRepository()
    notification_publisher = FakeNotificationPublisher()

    result, _, _, notification_publisher = _run_macro(
        repository,
        notification_publisher=notification_publisher,
    )
    report = result.to_sanitized_dict()

    assert report["ok"] is True
    route, message = notification_publisher.publish_calls[0]
    fields = message.as_stream_fields()
    assert route.queue_name == "q.notification.send"
    assert set(fields) == {
        "job_id",
        "stage_name",
        "root_object_type",
        "root_object_id",
        "idempotency_key",
        "pipeline_run_id",
        "not_before",
        "trigger_event_id",
    }
    for forbidden in (
        "message_text",
        "payload_json",
        "scores",
        "judge_output",
        "target_chat_id",
        "raw_text",
        "database_url",
        "redis_url",
    ):
        assert forbidden not in fields


def test_redaction_omits_full_ids_urls_chat_payload_dedupe_and_exception_detail() -> None:
    repository = FakeRepository()
    result, _, _, _ = _run_macro(repository)
    rendered = json.dumps(result.to_sanitized_dict(), ensure_ascii=False, sort_keys=True)

    for forbidden in (
        REDIS_MESSAGE_ID,
        NOTIFY_REDIS_MESSAGE_ID,
        str(POLICY_APPLY_EVENT_ID),
        str(JUDGE_RUN_ID),
        str(JUDGE_OUTPUT_ID),
        str(BUNDLE_ID),
        str(CANDIDATE_GROUP_ID),
        str(ANALYSIS_ID),
        str(NOTIFICATION_EVENT_ID),
        DB_LOCATOR,
        REDIS_LOCATOR,
        str(CHAT_ID),
        RAW_PAYLOAD_SENTINEL,
        RAW_EXCEPTION_SENTINEL,
        IDEMPOTENCY_SENTINEL,
        repository.notification_rows[0].dedupe_key,
    ):
        assert forbidden not in rendered
    assert "3d5b3290" in rendered
    assert "b7b2b7b2" in rendered
    assert "333444-0" in rendered


def test_ast_guards_no_notifier_external_or_broad_worker_authority() -> None:
    forbidden_roots = {
        "subprocess",
        "requests",
        "httpx",
        "aiohttp",
        "telegram",
        "openai",
        "docker",
    }
    forbidden_import_fragments = {
        "notifier_telegram",
        "judge_openai",
        "gh_enricher",
        "x_enricher",
        "web_enricher",
    }
    forbidden_call_attrs = {
        "run_forever",
        "system",
        "popen",
        "call",
        "check_call",
        "check_output",
    }
    for path in (SOURCE_PATH, TOOL_PATH):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_roots: set[str] = set()
        imported_modules: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
                imported_modules.add(node.module)
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in forbidden_call_attrs

        assert forbidden_roots.isdisjoint(imported_roots)
        assert not any(fragment in module for fragment in forbidden_import_fragments for module in imported_modules)
        assert "run_forever(" not in source
        assert "worker_once" not in source
