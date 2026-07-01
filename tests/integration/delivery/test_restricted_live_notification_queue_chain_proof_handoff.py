from __future__ import annotations

import pytest

from services.maintenance.restricted_live_notification_queue_chain_proof_runner import (
    REASON_PASSED,
    run_restricted_live_notification_queue_chain_proof,
)
from tests.unit.services.maintenance.test_restricted_live_notification_queue_chain_proof_runner import (
    FakePublisherRepositoryBuilder,
    FakeRedisPublisher,
    FakeRedisPublisherBuilder,
    _add_stale_suppressed_pending_plan_created_event,
    _config,
    _event_id_from_suffix,
    _fake_session_factory_builder,
    _queue_chain_repository,
    _runtime_config,
)


@pytest.mark.asyncio
async def test_restricted_live_queue_chain_proof_creates_exact_target_and_ignores_stale_suppressed_pending_event() -> None:
    repository, source_plan_id, _ = _queue_chain_repository()
    stale_event_id = _add_stale_suppressed_pending_plan_created_event(repository)
    redis = FakeRedisPublisher()

    report = await run_restricted_live_notification_queue_chain_proof(
        _config(source_plan_id),
        runtime_config_loader=_runtime_config,
        session_factory_builder=_fake_session_factory_builder,
        proof_repository_builder=lambda session: repository,
        publisher_repository_builder=FakePublisherRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(redis),
    )
    target_event_id = _event_id_from_suffix(repository, report["target"]["event_id_suffix"])

    assert report["status"] == "pass"
    assert report["reason_code"] == REASON_PASSED
    assert report["target"]["outbox_status_before_publish"] == "pending"
    assert report["target"]["outbox_status_after_publish"] == "published"
    assert report["publisher"]["redis_xadd_count"] == 1
    assert report["publisher"]["event_outbox_marked_published"] is True
    assert report["publisher"]["job_attempt_inserted"] is True
    assert repository.event_outbox[target_event_id]["status"] == "published"
    assert repository.event_outbox[stale_event_id]["status"] == "pending"
    assert len(redis.publish_calls) == 1
    assert redis.publish_calls[0][0].queue_name == "q.notification.send"
    assert redis.publish_calls[0][1].trigger_event_id == str(target_event_id)
    assert report["authority"]["redis_consume_attempted"] is False
    assert report["authority"]["telegram_transport_attempted"] is False
    assert report["authority"]["openai_called"] is False
