from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID, uuid4

import pytest

from src.services.notifier_telegram.idempotency import classify_notifier_idempotency_state
from src.services.notifier_telegram.idempotent_noop_proof_message import (
    BoundedNotifierIdempotentNoopProofMessageConfig,
    BoundedNotifierIdempotentNoopProofMessageRuntimeConfig,
    BoundedNotifierIdempotentNoopProofMessageState,
    DefaultBoundedNotifierIdempotentNoopProofMessageRuntime,
    ProofMessagePublishOutcome,
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


class FakeAtomicPipeline:
    def __init__(self, responses: object) -> None:
        self.responses = responses
        self.calls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        del exc_type, exc, traceback

    async def watch(self, queue_name):
        assert queue_name == QUEUE_NAME
        self.calls.append("watch")

    async def type(self, queue_name):
        assert queue_name == QUEUE_NAME
        self.calls.append("type")
        return "stream"

    async def xinfo_groups(self, queue_name):
        assert queue_name == QUEUE_NAME
        self.calls.append("xinfo_groups")
        return [{"name": "notifier-telegram", "pending": 0, "lag": 0}]

    async def xinfo_stream(self, queue_name):
        assert queue_name == QUEUE_NAME
        self.calls.append("xinfo_stream")
        return {"last-generated-id": "1718000011047-0"}

    def multi(self):
        self.calls.append("multi")

    def xadd(self, queue_name, fields, *, id, **kwargs):
        assert queue_name == QUEUE_NAME
        assert fields["proof_kind"] == "idempotent_noop_reprocess_v1"
        assert id == "1718000011048-0"
        assert kwargs == {"maxlen": 10000, "approximate": True}
        self.calls.append("xadd")

    def xreadgroup(self, group_name, consumer_name, streams, *, count):
        assert group_name == "notifier-telegram"
        assert consumer_name == "bounded-notifier-idempotent-noop-proof"
        assert streams == {QUEUE_NAME: ">"}
        assert count == 1
        self.calls.append("xreadgroup")

    async def execute(self, *, raise_on_error):
        assert raise_on_error is False
        self.calls.append("execute")
        if isinstance(self.responses, BaseException):
            raise self.responses
        return self.responses


class FakeAtomicRedis:
    def __init__(self, pipeline: FakeAtomicPipeline) -> None:
        self._pipeline = pipeline

    def pipeline(self, *, transaction):
        assert transaction is True
        return self._pipeline


def _atomic_runtime(pipeline: FakeAtomicPipeline):
    state = BoundedNotifierIdempotentNoopProofMessageState()
    runtime = DefaultBoundedNotifierIdempotentNoopProofMessageRuntime(
        redis_client=FakeAtomicRedis(pipeline),
        queue_name=QUEUE_NAME,
        consumer_group="notifier-telegram",
        xadd_maxlen=10000,
        repository=object(),  # type: ignore[arg-type]
        session=object(),
        engine=object(),
        state=state,
    )
    return runtime, state


class FakeRuntime:
    def __init__(
        self,
        *,
        intents: list[NotificationIntentJob],
        snapshots: list[NotifierPlanIdempotencySnapshot] | None = None,
        inspection: RedisProofQueueInspection | None = None,
        inspect_error: BaseException | None = None,
        publish_message_id: str | None = "1718000011048-0",
        publish_claimed_for_worker_once: bool = False,
        publish_error_code: str | None = None,
        publish_outcome_unknown: bool = False,
        publish_message_id_hint: str | None = None,
    ) -> None:
        self.intents = intents
        self.snapshots = snapshots if snapshots is not None else _sent_snapshot(intents[0])
        self.inspection = inspection or _safe_inspection()
        self.inspect_error = inspect_error
        self.publish_message_id = publish_message_id
        self.publish_claimed_for_worker_once = publish_claimed_for_worker_once
        self.publish_error_code = publish_error_code
        self.publish_outcome_unknown = publish_outcome_unknown
        self.publish_message_id_hint = publish_message_id_hint
        self.state = None
        self.loaded_suffixes: list[str] = []
        self.readback_intents: list[UUID] = []
        self.inspect_calls = 0
        self.publish_calls: list[dict[str, str]] = []
        self.atomic_claim_requests: list[bool] = []
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

    async def publish_proof_message(
        self,
        fields: Mapping[str, str],
        *,
        atomic_claim_for_worker_once: bool,
    ) -> ProofMessagePublishOutcome:
        self.state.redis_publish_attempted = True
        self.state.redis_consume_called = atomic_claim_for_worker_once
        self.publish_calls.append(dict(fields))
        self.atomic_claim_requests.append(atomic_claim_for_worker_once)
        return ProofMessagePublishOutcome(
            message_id=self.publish_message_id,
            claimed_for_worker_once=self.publish_claimed_for_worker_once,
            error_code=self.publish_error_code,
            outcome_unknown=self.publish_outcome_unknown,
            message_id_hint=self.publish_message_id_hint,
        )

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
    atomic_claim: bool = False,
) -> BoundedNotifierIdempotentNoopProofMessageConfig:
    return BoundedNotifierIdempotentNoopProofMessageConfig(
        operator_approved=True,
        allow_runtime_config=True,
        allow_database_read=True,
        allow_redis_read=True,
        allow_redis_publish=mode == "publish",
        allow_proof_message_publish=mode == "publish",
        allow_atomic_redis_claim=mode == "publish" and atomic_claim,
        atomic_claim_for_worker_once=atomic_claim,
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
async def test_atomic_claim_request_without_explicit_authority_fails_before_runtime() -> None:
    intent = _intent()
    config = replace(
        _base_config(intent, mode="publish", atomic_claim=True),
        allow_atomic_redis_claim=False,
    )

    result = await run_bounded_notifier_idempotent_noop_proof_message(
        config,
        runtime_config_loader=_raising_runtime_config_loader,
    )

    assert result.status == "blocked"
    assert result.error_code == "atomic_redis_claim_not_allowed"
    assert result.state.runtime_config_loaded is False


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
async def test_publish_atomic_claim_reserves_exact_proof_for_worker_once() -> None:
    intent = _intent()
    runtime = FakeRuntime(intents=[intent], publish_claimed_for_worker_once=True)

    result = await run_bounded_notifier_idempotent_noop_proof_message(
        _base_config(intent, mode="publish", atomic_claim=True),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "pass"
    assert report["proof_message_published"] is True
    assert report["proof_message_claimed_for_worker_once"] is True
    assert report["redis_consume_called"] is True
    assert report["redis_ack_called"] is False
    assert runtime.atomic_claim_requests == [True]
    assert len(runtime.publish_calls) == 1


@pytest.mark.asyncio
async def test_default_runtime_atomic_claim_orders_xadd_and_exact_claim_in_one_exec() -> None:
    message_id = "1718000011048-0"
    pipeline = FakeAtomicPipeline(
        [
            message_id,
            [[QUEUE_NAME, [[message_id, {"proof_kind": "idempotent_noop_reprocess_v1"}]]]],
        ]
    )
    runtime, state = _atomic_runtime(pipeline)

    outcome = await runtime.publish_proof_message(
        {"proof_kind": "idempotent_noop_reprocess_v1"},
        atomic_claim_for_worker_once=True,
    )

    assert outcome == ProofMessagePublishOutcome(
        message_id=message_id,
        claimed_for_worker_once=True,
    )
    assert pipeline.calls == [
        "watch",
        "type",
        "xinfo_groups",
        "xinfo_stream",
        "multi",
        "xadd",
        "xreadgroup",
        "execute",
    ]
    assert state.redis_publish_attempted is True
    assert state.redis_consume_called is True


@pytest.mark.asyncio
async def test_default_runtime_atomic_claim_mismatch_retains_published_suffix_for_recovery() -> None:
    pipeline = FakeAtomicPipeline(
        [
            "1718000011048-0",
            [[QUEUE_NAME, [["1718000011049-0", {}]]]],
        ]
    )
    runtime, _ = _atomic_runtime(pipeline)

    outcome = await runtime.publish_proof_message(
        {"proof_kind": "idempotent_noop_reprocess_v1"},
        atomic_claim_for_worker_once=True,
    )

    assert outcome.message_id == "1718000011048-0"
    assert outcome.claimed_for_worker_once is False
    assert outcome.error_code == "proof_message_atomic_claim_failed"
    assert outcome.outcome_unknown is False


@pytest.mark.asyncio
async def test_default_runtime_watch_race_fails_without_retry_or_unknown_outcome() -> None:
    class WatchError(Exception):
        pass

    pipeline = FakeAtomicPipeline(WatchError("concurrent stream change"))
    runtime, _ = _atomic_runtime(pipeline)

    outcome = await runtime.publish_proof_message(
        {"proof_kind": "idempotent_noop_reprocess_v1"},
        atomic_claim_for_worker_once=True,
    )

    assert outcome == ProofMessagePublishOutcome(
        message_id=None,
        error_code="redis_atomic_claim_race",
    )
    assert pipeline.calls.count("execute") == 1


@pytest.mark.asyncio
async def test_default_runtime_lost_exec_response_is_unknown_and_never_retried() -> None:
    pipeline = FakeAtomicPipeline(ConnectionError("lost EXEC response"))
    runtime, _ = _atomic_runtime(pipeline)

    outcome = await runtime.publish_proof_message(
        {"proof_kind": "idempotent_noop_reprocess_v1"},
        atomic_claim_for_worker_once=True,
    )

    assert outcome == ProofMessagePublishOutcome(
        message_id=None,
        error_code="redis_atomic_claim_outcome_unknown",
        outcome_unknown=True,
        message_id_hint="1718000011048-0",
    )
    assert pipeline.calls.count("execute") == 1


@pytest.mark.asyncio
async def test_unknown_atomic_outcome_reports_precomputed_suffix_without_claiming_publish() -> None:
    intent = _intent()
    runtime = FakeRuntime(
        intents=[intent],
        publish_message_id=None,
        publish_error_code="redis_atomic_claim_outcome_unknown",
        publish_outcome_unknown=True,
        publish_message_id_hint="1718000011048-0",
    )

    result = await run_bounded_notifier_idempotent_noop_proof_message(
        _base_config(intent, mode="publish", atomic_claim=True),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "redis_atomic_claim_outcome_unknown"
    assert report["atomic_claim_outcome_unknown"] is True
    assert report["proof_message_id_suffix"] == "011048-0"
    assert report["proof_message_published"] is False
    assert report["proof_message_claimed_for_worker_once"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "inspection, expected_error",
    [
        (
            RedisProofQueueInspection(
                status="matched",
                error_code=None,
                queue_type="stream",
                consumer_group_present=True,
                group_lag=0,
                group_pending=None,
            ),
            "redis_group_pending_unavailable",
        ),
        (
            RedisProofQueueInspection(
                status="matched",
                error_code=None,
                queue_type="stream",
                consumer_group_present=True,
                group_lag=None,
                group_pending=0,
            ),
            "redis_group_lag_unavailable",
        ),
    ],
)
async def test_preview_unavailable_group_metrics_fail_closed(
    inspection: RedisProofQueueInspection,
    expected_error: str,
) -> None:
    intent = _intent()
    runtime = FakeRuntime(intents=[intent], inspection=inspection)

    result = await run_bounded_notifier_idempotent_noop_proof_message(
        _base_config(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )

    assert result.error_code == expected_error
    assert result.proof_message_published is False
    assert runtime.publish_calls == []


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
    forbidden_attrs = {"xack", "xgroup_create", "xclaim", "xautoclaim", "run_forever"}
    xreadgroup_calls = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr.lower() not in forbidden_attrs
            if node.func.attr.lower() == "xreadgroup":
                xreadgroup_calls += 1

    assert {"openai", "github", "requests", "httpx", "aiohttp", "subprocess", "alembic"}.isdisjoint(
        imported_roots
    )
    lowered = source.lower()
    assert xreadgroup_calls == 1
    for forbidden in (
        "xack",
        "xgroup_create",
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
