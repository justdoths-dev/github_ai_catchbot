from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest

from services.maintenance.restricted_live_notification_queue_chain_proof_runner import (
    REASON_PASSED,
    RestrictedLiveNotificationQueueChainProofConfig,
    run_restricted_live_notification_queue_chain_proof,
)
from services.notifier_telegram.models import NotificationPlanDraft
from services.outbox_relay.bounded_notification_plan_publish import (
    BoundedNotificationPlanPublishRuntimeConfig,
    BoundedNotificationPlanRedisPublisherHandle,
    BoundedNotificationPlanRepositoryHandle,
    NotificationPlanSendabilityRow,
)
from services.outbox_relay.models import OutboxEventRow
from tests.unit.services.notifier_telegram.test_restricted_live_worker_once_proof_cli import (
    ProofRepository,
    _proof_repository,
)


DB_URL = "postgresql+psycopg://unit:secret@db.internal/catchbot"
REDIS_URL = "redis://:secret@redis.internal/0"


@pytest.mark.asyncio
async def test_queue_chain_runner_creates_pending_target_then_exact_target_publisher_queues_only_fresh_event() -> None:
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
    assert report["source"]["source_notification_plan_id_suffix"] == str(source_plan_id)[-8:]
    assert report["target"]["created"] is True
    assert report["target"]["existing"] is False
    assert report["target"]["plan_status"] == "planned"
    assert report["target"]["delivery_decision"] == "send_now"
    assert report["target"]["outbox_status_before_publish"] == "pending"
    assert report["target"]["outbox_status_after_publish"] == "published"
    assert report["publisher"]["selected_event_present"] is True
    assert report["publisher"]["selected_event_id_suffix"] == str(target_event_id)[-8:]
    assert report["publisher"]["selected_aggregate_type"] == "notification_plan"
    assert report["publisher"]["redis_xadd_count"] == 1
    assert report["publisher"]["redis_message_id_present"] is True
    assert report["publisher"]["event_outbox_marked_published"] is True
    assert report["publisher"]["job_attempt_inserted"] is True
    assert report["authority"] == {
        "db_read_attempted": True,
        "db_write_attempted": True,
        "redis_write_attempted": True,
        "redis_consume_attempted": False,
        "redis_ack_attempted": False,
        "telegram_transport_attempted": False,
        "openai_called": False,
        "github_called": False,
        "x_called": False,
        "web_called": False,
        "workers_started": False,
        "run_forever_started": False,
        "docker_or_systemd_called": False,
        "alembic_or_ddl_ran": False,
        "runtime_values_printed": False,
        "runtime_paths_printed": False,
        "raw_ids_printed": False,
        "raw_payload_printed": False,
    }

    assert repository.event_outbox[target_event_id]["status"] == "published"
    assert repository.event_outbox[stale_event_id]["status"] == "pending"
    assert len(redis.publish_calls) == 1
    route, message = redis.publish_calls[0]
    assert route.queue_name == "q.notification.send"
    assert route.stage_name == "notify"
    assert message.as_stream_fields() == {
        "job_id": str(target_event_id),
        "stage_name": "notify",
        "root_object_type": "notification_plan",
        "root_object_id": str(repository.event_outbox[target_event_id]["aggregate_id"]),
        "idempotency_key": repository.event_outbox[target_event_id]["dedupe_key"],
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(target_event_id),
    }
    assert repository.job_attempts == [
        {
            "stage_name": "notify",
            "queue_name": "q.notification.send",
            "root_object_type": "notification_plan",
            "root_object_id": repository.event_outbox[target_event_id]["aggregate_id"],
            "attempt_status": "succeeded",
            "error_code": None,
        }
    ]

    rendered = json.dumps(report, sort_keys=True)
    for raw in [
        str(source_plan_id),
        str(target_event_id),
        str(repository.event_outbox[target_event_id]["aggregate_id"]),
        DB_URL,
        REDIS_URL,
        '"payload_json":',
        "secret",
        "source text",
        "TELEGRAM_BOT_TOKEN",
    ]:
        assert raw not in rendered


@pytest.mark.asyncio
async def test_queue_chain_runner_reports_existing_published_target_without_republishing() -> None:
    repository, source_plan_id, _ = _queue_chain_repository()
    first = await run_restricted_live_notification_queue_chain_proof(
        _config(source_plan_id),
        runtime_config_loader=_runtime_config,
        session_factory_builder=_fake_session_factory_builder,
        proof_repository_builder=lambda session: repository,
        publisher_repository_builder=FakePublisherRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )
    assert first["status"] == "pass"
    redis = FakeRedisPublisher()

    second = await run_restricted_live_notification_queue_chain_proof(
        _config(source_plan_id),
        runtime_config_loader=_runtime_config,
        session_factory_builder=_fake_session_factory_builder,
        proof_repository_builder=lambda session: repository,
        publisher_repository_builder=FakePublisherRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(redis),
    )

    assert second["status"] == "blocked"
    assert second["reason_code"] == "existing_already_published"
    assert second["target"]["existing"] is True
    assert second["target"]["outbox_status_before_publish"] == "published"
    assert second["publisher"]["redis_xadd_count"] == 0
    assert redis.publish_calls == []


@pytest.mark.asyncio
async def test_queue_chain_runner_blocks_before_runtime_without_authority_flags() -> None:
    repository, source_plan_id, _ = _queue_chain_repository()

    report = await run_restricted_live_notification_queue_chain_proof(
        replace(_config(source_plan_id), allow_database_write=False),
        runtime_config_loader=lambda: (_ for _ in ()).throw(AssertionError("runtime config must not load")),
        session_factory_builder=_fake_session_factory_builder,
        proof_repository_builder=lambda session: repository,
        publisher_repository_builder=FakePublisherRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(FakeRedisPublisher()),
    )

    assert report["status"] == "blocked"
    assert report["reason_code"] == "database_write_not_allowed"
    assert report["authority"]["db_read_attempted"] is False
    assert report["authority"]["redis_write_attempted"] is False


def _queue_chain_repository() -> tuple["QueueChainRepository", UUID, UUID]:
    base, source_plan_id, source_event_id = _proof_repository()
    repository = QueueChainRepository()
    repository.jobs.update(base.jobs)
    repository.analyses.update(base.analyses)
    repository.judge_outputs.update(base.judge_outputs)
    repository.candidates.update(base.candidates)
    repository.plans.update(base.plans)
    repository.event_outbox.update(base.event_outbox)
    return repository, source_plan_id, source_event_id


def _config(source_plan_id: UUID) -> RestrictedLiveNotificationQueueChainProofConfig:
    return RestrictedLiveNotificationQueueChainProofConfig(
        operator_confirmed=True,
        source_notification_plan_id=source_plan_id,
        proof_key="proof-key-01",
        allow_database_write=True,
        allow_redis_write=True,
        allow_outbox_status_update=True,
        expected_target_pending_count=1,
    )


def _runtime_config() -> BoundedNotificationPlanPublishRuntimeConfig:
    return BoundedNotificationPlanPublishRuntimeConfig(database_url=DB_URL, redis_url=REDIS_URL)


def _add_stale_suppressed_pending_plan_created_event(repository: "QueueChainRepository") -> UUID:
    plan_id = uuid4()
    analysis_id = next(iter(repository.analyses))
    candidate_group_id = repository.analyses[analysis_id].candidate_group_id
    repository.plans[plan_id] = NotificationPlanDraft(
        notification_plan_id=plan_id,
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=12345,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key="stale-suppressed-dedupe",
        material_change_hash="stale-suppressed-material",
        send_after=None,
        suppress_reason_code=None,
        status="suppressed",
    )
    event_id = uuid4()
    repository.event_outbox[event_id] = {
        "event_id": event_id,
        "event_type": "notification.plan.created.v1",
        "aggregate_type": "notification_plan",
        "aggregate_id": plan_id,
        "dedupe_key": "notification-plan-created:stale-suppressed",
        "payload_json": {
            "notification_plan_id": str(plan_id),
            "analysis_id": str(analysis_id),
            "candidate_group_id": str(candidate_group_id),
            "delivery_decision": "send_now",
            "urgency_profile": "high",
            "target_chat_id": 12345,
            "target_thread_id": None,
            "render_profile": "telegram_single_alert_high_v1",
            "dedupe_subject_key": "stale-suppressed-dedupe",
            "material_change_hash": "stale-suppressed-material",
            "send_after": None,
            "suppress_reason_code": None,
        },
        "status": "pending",
        "fail_count": 0,
        "created_at": datetime.now(timezone.utc),
    }
    return event_id


def _event_id_from_suffix(repository: "QueueChainRepository", suffix: str) -> UUID:
    matches = [event_id for event_id in repository.event_outbox if str(event_id).endswith(suffix)]
    assert len(matches) == 1
    return matches[0]


class QueueChainRepository(ProofRepository):
    def __init__(self) -> None:
        super().__init__()
        self.job_attempts: list[dict[str, Any]] = []

    async def count_pending_events(self, *, event_type: str) -> int:
        return sum(
            1
            for row in self.event_outbox.values()
            if row["event_type"] == event_type and row["status"] == "pending"
        )

    async def fetch_oldest_pending_event(self, *, event_type: str) -> OutboxEventRow | None:
        rows = [
            self._outbox_row(row)
            for row in self.event_outbox.values()
            if row["event_type"] == event_type and row["status"] == "pending"
        ]
        rows.sort(key=lambda row: (row.created_at, row.event_id))
        return rows[0] if rows else None

    async def fetch_event_by_id(self, *, event_id: UUID) -> OutboxEventRow | None:
        row = self.event_outbox.get(event_id)
        return self._outbox_row(row) if row is not None else None

    async def load_notification_plan_sendability(
        self,
        *,
        notification_plan_id: UUID,
    ) -> NotificationPlanSendabilityRow | None:
        plan = self.plans.get(notification_plan_id)
        if plan is None:
            return None
        return NotificationPlanSendabilityRow(
            notification_plan_id=notification_plan_id,
            delivery_decision=plan.delivery_decision,
            status=plan.status,
        )

    async def mark_published(self, *, event_id: UUID, published_at: datetime | None = None) -> None:
        self.event_outbox[event_id]["status"] = "published"
        self.event_outbox[event_id]["published_at"] = published_at or datetime.now(timezone.utc)

    async def insert_job_attempt(self, **kwargs) -> None:
        self.job_attempts.append(dict(kwargs))

    @staticmethod
    def _outbox_row(row: dict[str, Any] | None) -> OutboxEventRow:
        assert row is not None
        return OutboxEventRow(
            event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            aggregate_type=str(row["aggregate_type"]),
            aggregate_id=UUID(str(row["aggregate_id"])),
            dedupe_key=str(row["dedupe_key"]),
            payload_json=dict(row["payload_json"]),
            status=str(row["status"]),
            fail_count=int(row.get("fail_count") or 0),
            created_at=row.get("created_at") or datetime.now(timezone.utc),
        )


class FakePublisherRepositoryBuilder:
    def __init__(self, repository: QueueChainRepository) -> None:
        self.repository = repository

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            del commit

        return BoundedNotificationPlanRepositoryHandle(repository=self.repository, close=close)


class FakeRedisPublisher:
    def __init__(self) -> None:
        self.publish_calls: list[tuple[object, object]] = []

    async def publish(self, route, message) -> str:
        self.publish_calls.append((route, message))
        return "1-0"


class FakeRedisPublisherBuilder:
    def __init__(self, publisher: FakeRedisPublisher) -> None:
        self.publisher = publisher

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.redis_publisher_created = True

        async def close() -> None:
            return None

        return BoundedNotificationPlanRedisPublisherHandle(publisher=self.publisher, close=close)


class _FakeSessionContext:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSessionFactory:
    def begin(self):
        return _FakeSessionContext()


def _fake_session_factory_builder(database_url: str):
    del database_url

    async def dispose() -> None:
        return None

    return _FakeSessionFactory(), dispose
