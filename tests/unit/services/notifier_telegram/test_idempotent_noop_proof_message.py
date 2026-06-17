from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID, uuid4

import pytest

from src.services.notifier_telegram.idempotency import classify_notifier_idempotency_state
from src.services.notifier_telegram.idempotent_noop_proof_message import (
    BoundedNotifierIdempotentNoopProofMessageConfig,
    BoundedNotifierIdempotentNoopProofMessageRuntimeConfig,
    RedisProofQueueInspection,
    run_bounded_notifier_idempotent_noop_proof_message,
)
from src.services.notifier_telegram.models import (
    NotificationIntentJob,
    NotifierPlanIdempotencySnapshot,
)
from tests.unit.services.notifier_telegram._service_fakes import config as notifier_config


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/notifier_telegram/idempotent_noop_proof_message.py"
QUEUE_NAME = "q.notification.send"


class FakeRuntime:
    def __init__(
        self,
        *,
        intents: list[NotificationIntentJob],
        snapshots: list[NotifierPlanIdempotencySnapshot] | None = None,
        inspection: RedisProofQueueInspection | None = None,
        inspect_error: BaseException | None = None,
        publish_message_id: str = "1718000011048-0",
    ) -> None:
        self.intents = intents
        self.snapshots = snapshots if snapshots is not None else _sent_snapshot(intents[0])
        self.inspection = inspection or _safe_inspection()
        self.inspect_error = inspect_error
        self.publish_message_id = publish_message_id
        self.state = None
        self.loaded_suffixes: list[str] = []
        self.readback_intents: list[UUID] = []
        self.inspect_calls = 0
        self.publish_calls: list[dict[str, str]] = []
        self.closed = False

    async def load_intents_by_event_suffix(self, *, event_suffix: str, limit: int):
        assert limit == 2
        self.state.database_read_attempted = True
        self.loaded_suffixes.append(event_suffix)
        return self.intents

    async def load_readback(self, intent: NotificationIntentJob):
        self.state.database_read_attempted = True
        self.readback_intents.append(intent.trigger_event_id)
        return classify_notifier_idempotency_state(self.snapshots)

    async def inspect_redis_state(self, config):
        del config
        self.state.redis_read_attempted = True
        self.inspect_calls += 1
        if self.inspect_error is not None:
            raise self.inspect_error
        return self.inspection

    async def publish_proof_message(self, fields: Mapping[str, str]) -> str:
        self.state.redis_publish_attempted = True
        self.publish_calls.append(dict(fields))
        return self.publish_message_id

    async def close(self) -> None:
        self.closed = True


class FakeRuntimeBuilder:
    def __init__(self, runtime: FakeRuntime) -> None:
        self.runtime = runtime
        self.calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True
        state.redis_client_created = True
        self.runtime.state = state
        return self.runtime


def _runtime_config_loader(cfg) -> BoundedNotifierIdempotentNoopProofMessageRuntimeConfig:
    return BoundedNotifierIdempotentNoopProofMessageRuntimeConfig(
        notifier_config=notifier_config(dry_run=True, enable_notification_send=False, allow_edits=False),
        redis_url="redis://unit/0",
    )


def _raising_runtime_config_loader(cfg) -> BoundedNotifierIdempotentNoopProofMessageRuntimeConfig:
    del cfg
    raise AssertionError("runtime config must not be loaded")


def _base_config(
    intent: NotificationIntentJob,
    *,
    mode: str = "preview",
    analysis_suffix: str | None = None,
) -> BoundedNotifierIdempotentNoopProofMessageConfig:
    return BoundedNotifierIdempotentNoopProofMessageConfig(
        operator_approved=True,
        allow_runtime_config=True,
        allow_database_read=True,
        allow_redis_read=True,
        allow_redis_publish=mode == "publish",
        allow_proof_message_publish=mode == "publish",
        require_telegram_disabled=True,
        mode=mode,
        queue_name=QUEUE_NAME,
        trigger_event_suffix=str(intent.trigger_event_id)[-8:],
        analysis_suffix=analysis_suffix or str(intent.analysis_id)[-8:],
    )


def _intent(*, event_type: str = "notification.plan.created.v1") -> NotificationIntentJob:
    return NotificationIntentJob(
        trigger_event_id=uuid4(),
        event_type=event_type,
        notification_plan_id=uuid4(),
        analysis_id=uuid4(),
        candidate_group_id=uuid4(),
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=12345,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key="subject",
        material_change_hash="material",
        send_after=None,
        suppress_reason_code=None,
    )


def _safe_inspection() -> RedisProofQueueInspection:
    return RedisProofQueueInspection(
        status="matched",
        error_code=None,
        queue_type="stream",
        consumer_group_present=True,
        group_lag=0,
        group_pending=0,
        stream_tail_checked=True,
        stream_tail_count=1,
    )


def _sent_snapshot(intent: NotificationIntentJob) -> list[NotifierPlanIdempotencySnapshot]:
    return [
        NotifierPlanIdempotencySnapshot(
            notification_plan_id=intent.notification_plan_id,
            status="sent",
            render_count=1,
            delivery_record_count=2,
            sent_delivery_count=1,
            suppressed_delivery_count=1,
            terminal_delivery_count=2,
            sent_delivery_chat_id_present_count=1,
            sent_delivery_message_id_present_count=1,
        )
    ]


def _pending_duplicate_snapshot(intent: NotificationIntentJob) -> list[NotifierPlanIdempotencySnapshot]:
    return [
        NotifierPlanIdempotencySnapshot(notification_plan_id=intent.notification_plan_id, status="planned"),
        NotifierPlanIdempotencySnapshot(notification_plan_id=uuid4(), status="queued"),
    ]


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_runtime_config_db_redis_or_telegram() -> None:
    result = await run_bounded_notifier_idempotent_noop_proof_message(
        BoundedNotifierIdempotentNoopProofMessageConfig(),
        runtime_config_loader=_raising_runtime_config_loader,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "operator_approval_missing"
    assert report["runtime_config_loaded"] is False
    assert report["database_read_attempted"] is False
    assert report["redis_read_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert report["redis_consume_called"] is False
    assert report["redis_ack_called"] is False
    assert report["notifier_called"] is False
    assert report["telegram_send_called"] is False
    assert report["telegram_edit_called"] is False


@pytest.mark.asyncio
async def test_preview_safe_existing_plan_sent_reports_publish_safe_without_publish_or_ack() -> None:
    intent = _intent()
    runtime = FakeRuntime(intents=[intent])

    result = await run_bounded_notifier_idempotent_noop_proof_message(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["proof_publish_safe"] is True
    assert report["proof_message_published"] is False
    assert report["pre_idempotency_classification"] == "existing_plan_sent"
    assert report["pre_notification_plan_count"] == 1
    assert report["pre_notification_render_count"] == 1
    assert report["pre_notification_delivery_record_count"] == 2
    assert report["pre_sent_delivery_count"] == 1
    assert report["pre_suppressed_delivery_count"] == 1
    assert report["redis_group_lag"] == 0
    assert report["redis_group_pending"] == 0
    assert report["database_write_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert report["redis_consume_called"] is False
    assert report["redis_ack_called"] is False
    assert report["notifier_called"] is False
    assert report["telegram_send_called"] is False
    assert report["telegram_edit_called"] is False
    assert runtime.loaded_suffixes == [str(intent.trigger_event_id)[-8:]]
    assert runtime.readback_intents == [intent.trigger_event_id]
    assert runtime.inspect_calls == 1
    assert runtime.publish_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "snapshots, expected_classification",
    [
        ([], "no_existing_plan"),
        (None, "existing_duplicate_plans"),
    ],
)
async def test_preview_unsafe_classification_blocks_without_publish(
    snapshots: list[NotifierPlanIdempotencySnapshot] | None,
    expected_classification: str,
) -> None:
    intent = _intent()
    runtime = FakeRuntime(
        intents=[intent],
        snapshots=_pending_duplicate_snapshot(intent) if snapshots is None else snapshots,
    )

    result = await run_bounded_notifier_idempotent_noop_proof_message(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "pre_readback_not_proof_publish_safe"
    assert report["pre_idempotency_classification"] == expected_classification
    assert report["redis_read_attempted"] is True
    assert report["redis_publish_attempted"] is False
    assert report["proof_message_published"] is False
    assert runtime.publish_calls == []


@pytest.mark.asyncio
async def test_publish_safe_emits_exactly_one_thin_xadd_message_without_db_write_or_ack() -> None:
    intent = _intent()
    runtime = FakeRuntime(intents=[intent])

    result = await run_bounded_notifier_idempotent_noop_proof_message(
        _base_config(intent, mode="publish"),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["proof_publish_safe"] is True
    assert report["proof_message_published"] is True
    assert report["proof_message_id_suffix"] == "011048-0"
    assert report["redis_publish_attempted"] is True
    assert report["database_write_attempted"] is False
    assert report["redis_consume_called"] is False
    assert report["redis_ack_called"] is False
    assert report["notifier_called"] is False
    assert report["telegram_send_called"] is False
    assert report["telegram_edit_called"] is False
    assert len(runtime.publish_calls) == 1
    fields = runtime.publish_calls[0]
    assert fields["stage_name"] == "notify"
    assert fields["root_object_type"] == "analysis"
    assert fields["root_object_id"] == str(intent.analysis_id)
    assert fields["trigger_event_id"] == str(intent.trigger_event_id)
    assert fields["proof_kind"] == "idempotent_noop_reprocess_v1"
    assert "payload_json" not in fields
    assert "message_text" not in fields
    assert not any("chat_id" in key.lower() for key in fields)


@pytest.mark.asyncio
async def test_event_analysis_mismatch_blocks_before_redis_or_publish() -> None:
    intent = _intent()
    runtime = FakeRuntime(intents=[intent])

    result = await run_bounded_notifier_idempotent_noop_proof_message(
        _base_config(intent, analysis_suffix="deadbeef"),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "analysis_mismatch"
    assert report["redis_read_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert runtime.publish_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "inspection, inspect_error, expected_status, expected_error",
    [
        (
            RedisProofQueueInspection(
                status="blocked",
                error_code="queue_key_wrong_type",
                queue_type="string",
            ),
            None,
            "blocked",
            "queue_key_wrong_type",
        ),
        (_safe_inspection(), TimeoutError("redis unavailable"), "failed", "redis_read_failed"),
    ],
)
async def test_redis_wrong_type_or_unavailable_fails_safe_without_publish_db_write_or_telegram(
    inspection: RedisProofQueueInspection,
    inspect_error: BaseException | None,
    expected_status: str,
    expected_error: str,
) -> None:
    intent = _intent()
    runtime = FakeRuntime(intents=[intent], inspection=inspection, inspect_error=inspect_error)

    result = await run_bounded_notifier_idempotent_noop_proof_message(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == expected_status
    assert report["error_code"] == expected_error
    assert report["database_write_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert report["notifier_called"] is False
    assert report["telegram_send_called"] is False
    assert report["telegram_edit_called"] is False
    assert runtime.publish_calls == []


def test_source_has_no_consume_ack_db_write_notifier_telegram_or_external_authority() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    forbidden_attrs = {"xreadgroup", "xack", "xgroup", "xclaim", "xautoclaim", "run_forever"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr.lower() not in forbidden_attrs

    assert {"openai", "github", "requests", "httpx", "aiohttp", "subprocess", "alembic"}.isdisjoint(
        imported_roots
    )
    lowered = source.lower()
    for forbidden in (
        "xreadgroup",
        "xack",
        "xgroup",
        "xclaim",
        "xautoclaim",
        "delete ",
        "update ",
        "insert ",
        "runtime.env",
        "send_message(",
        "edit_message_text(",
        "run_forever(",
    ):
        assert forbidden not in lowered
