from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from os import getenv
from urllib.parse import urlparse
from uuid import UUID, uuid4

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError, TimeoutError as RedisTimeoutError

from services.notifier_telegram.redis_streams import RedisStreamConsumer
from services.notifier_telegram.worker import NotifierTelegramWorker
from services.outbox_relay.models import OutboxEventRow
from services.outbox_relay.routing import OutboxRouteResolver
from tests.component.services.notifier_telegram._fakes import (
    RaisingTelegramClient,
    config,
    repo_with_valid_case,
    service,
)


FORBIDDEN_STREAM_FIELDS = {
    "payload_json",
    "notification_plan_id",
    "notification_delivery_record_id",
    "delivery_status",
    "telegram_bot_token",
    "telegram_token",
    "openai_api_key",
    "secret",
    "token",
}


@pytest.mark.asyncio
async def test_notifier_worker_consumes_redis_stream_message_and_acknowledges_send_disabled_result() -> None:
    redis_url = _explicit_test_redis_url()
    queue_name = f"q.notification.send.test.{uuid4().hex}"
    consumer_group = f"notifier-telegram-test-{uuid4().hex}"
    consumer_name = f"worker-{uuid4().hex}"
    redis = Redis.from_url(redis_url, decode_responses=True)

    notifier_repository, intent = repo_with_valid_case()
    cfg = replace(
        config(dry_run=False, enable_notification_send=False),
        redis_url=redis_url,
        queue_name=queue_name,
        consumer_group=consumer_group,
        consumer_name=consumer_name,
        batch_size=1,
        block_ms=50,
    )
    consumer = RedisStreamConsumer(
        redis,
        queue_name=queue_name,
        consumer_group=consumer_group,
        consumer_name=consumer_name,
        block_ms=cfg.block_ms,
        batch_size=cfg.batch_size,
    )

    try:
        await _assert_redis_available(redis)
        await consumer.ensure_group()

        production_route = OutboxRouteResolver().resolve(_notification_plan_created_row(intent))
        assert production_route.queue_name == "q.notification.send"
        assert production_route.stage_name == "notify"

        ignored_root_object_id = uuid4()
        assert ignored_root_object_id != intent.analysis_id
        fields = _stream_fields(
            trigger_event_id=intent.trigger_event_id,
            root_object_id=ignored_root_object_id,
        )
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
        assert FORBIDDEN_STREAM_FIELDS.isdisjoint(fields)
        assert all("telegram" not in key.lower() for key in fields)
        assert all("openai" not in key.lower() for key in fields)
        assert all("secret" not in key.lower() for key in fields)
        assert all("token" not in key.lower() for key in fields)

        message_id = await redis.xadd(queue_name, fields)
        raw_entry = await redis.xrange(queue_name, min=message_id, max=message_id, count=1)
        assert raw_entry == [(message_id, fields)]

        original_analysis = notifier_repository.analyses[intent.analysis_id]
        original_judge_output = notifier_repository.judge_outputs[original_analysis.judge_output_id]
        original_candidate = notifier_repository.candidates[intent.candidate_group_id]
        client = RaisingTelegramClient()
        worker = NotifierTelegramWorker(
            cfg,
            consumer=consumer,
            service=service(notifier_repository, cfg=cfg, client=client),
        )

        result = await worker.run_once()

        assert result.processed == 1
        assert result.acked == 1
        assert await redis.xpending_range(queue_name, consumer_group, min="-", max="+", count=10) == []
        assert client.calls == 0
        assert notifier_repository.loaded_trigger_ids == [intent.trigger_event_id]
        assert len(notifier_repository.plans) == 1
        assert notifier_repository.plans[intent.notification_plan_id].analysis_id == intent.analysis_id
        assert str(ignored_root_object_id) != str(notifier_repository.plans[intent.notification_plan_id].analysis_id)
        assert notifier_repository.plans[intent.notification_plan_id].status == "suppressed"
        assert len(notifier_repository.renders) == 1
        assert len(notifier_repository.delivery_records) == 1
        assert notifier_repository.delivery_records[0]["result_status"] == "suppressed"
        assert notifier_repository.delivery_records[0]["transport_error_code"] == "notification_send_flag_disabled"
        assert notifier_repository.delivery_records[0]["telegram_response_json"]["send_disabled"] is True
        assert notifier_repository.delivery_records[0]["telegram_response_json"]["transport_skipped"] is True
        assert [transition["to_state"] for transition in notifier_repository.state_transitions] == [
            "rendered",
            "suppressed",
        ]
        assert len(notifier_repository.delivery_outbox) == 1
        assert notifier_repository.delivery_outbox[0]["delivery_status"] == "suppressed"
        assert notifier_repository.analyses[intent.analysis_id] == original_analysis
        assert notifier_repository.judge_outputs[original_analysis.judge_output_id] == original_judge_output
        assert notifier_repository.candidates[intent.candidate_group_id] == original_candidate
    finally:
        await _cleanup(redis, queue_name=queue_name, consumer_group=consumer_group)


def _explicit_test_redis_url() -> str:
    redis_url = getenv("TEST_REDIS_URL", "").strip()
    if not redis_url:
        pytest.skip("requires explicit TEST_REDIS_URL for Redis-backed acceptance")
    parsed = urlparse(redis_url)
    if parsed.scheme != "redis":
        pytest.fail("TEST_REDIS_URL must use a redis:// test URL")
    if parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        pytest.fail("TEST_REDIS_URL must point at local test Redis")
    if (parsed.path or "").lstrip("/") != "15":
        pytest.fail("TEST_REDIS_URL must use isolated Redis DB 15")
    return redis_url


async def _assert_redis_available(redis: Redis) -> None:
    try:
        assert await redis.ping() is True
    except (RedisConnectionError, RedisTimeoutError) as exc:
        pytest.fail(f"explicit TEST_REDIS_URL was not reachable: {type(exc).__name__}")


async def _cleanup(redis: Redis, *, queue_name: str, consumer_group: str) -> None:
    try:
        await redis.xgroup_destroy(queue_name, consumer_group)
    except ResponseError:
        pass
    await redis.delete(queue_name)
    await redis.aclose()


def _stream_fields(*, trigger_event_id: UUID, root_object_id: UUID) -> dict[str, str]:
    return {
        "job_id": str(trigger_event_id),
        "stage_name": "notify",
        "root_object_type": "analysis",
        "root_object_id": str(root_object_id),
        "idempotency_key": f"q-notification-send-redis-backed:{trigger_event_id}",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(trigger_event_id),
    }


def _notification_plan_created_row(intent) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=intent.trigger_event_id,
        event_type="notification.plan.created.v1",
        aggregate_type="analysis",
        aggregate_id=intent.analysis_id,
        dedupe_key=f"notification-plan-created:{intent.trigger_event_id}",
        payload_json={
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
            "send_after": intent.send_after,
            "suppress_reason_code": intent.suppress_reason_code,
        },
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )
