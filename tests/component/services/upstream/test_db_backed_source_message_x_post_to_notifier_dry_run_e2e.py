from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from services.analysis_router.repositories import AnalysisRouterRepository
from services.analysis_router.service import AnalysisRouterService
from services.analysis_validator.repositories import AnalysisValidatorRepository
from services.analysis_validator.service import AnalysisValidatorService
from services.evidence_assembler.repositories import EvidenceAssemblerRepository
from services.evidence_assembler.service import EvidenceAssemblerService
from services.judge_openai.repositories import JudgeOpenAIRepository
from services.judge_openai.service import JudgeOpenAIService
from services.notifier_telegram.models import StreamMessage as NotifierStreamMessage
from services.notifier_telegram.repositories import NotifierTelegramRepository
from services.notifier_telegram.service import NotifierTelegramService
from services.notifier_telegram.worker import NotifierTelegramWorker
from services.outbox_relay.repositories import OutboxRelayRepository
from services.outbox_relay.routing import OutboxRouteResolver
from services.outbox_relay.service import OutboxRelayService
from services.policy_engine.repositories import PolicyEngineRepository
from services.policy_engine.service import PolicyEngineService
from services.router_normalizer.repositories import RouterNormalizerRepository
from services.router_normalizer.service import RouterNormalizerService
from services.router_normalizer.worker import RouterNormalizerWorker
from services.x_enricher.config import XEnricherConfig
from services.x_enricher.redis_streams import StreamMessage as XStreamMessage
from services.x_enricher.repositories import XEnricherRepository
from services.x_enricher.response_mapper import XResponseMapper
from services.x_enricher.service import XEnricherService
from services.x_enricher.url_discovery import XUrlDiscovery
from services.x_enricher.worker import XEnricherWorker
from tests.component.services.upstream.test_db_backed_artifact_snapshot_updated_to_notification_queue_e2e import (
    THIN_REDIS_FIELDS,
    RecordingOpenAIClient,
    RecordingRedisPublisher,
    _analyses_for_judge_output,
    _candidate_bundle_members,
    _candidate_bundles,
    _count,
    _events,
    _evidence_config,
    _judge_openai_config,
    _judge_outputs_for_run,
    _judge_runs_for_bundle,
    _jsonb,
    _json_obj,
    _local_test_database_url,
    _mark_events_published,
    _move_event_to_front,
    _notifier_owned_counts,
    _outbox_config,
    _policy_config,
    _router_config,
    _validator_config,
)
from tests.component.services.upstream.test_db_backed_github_enrich_requested_to_notification_queue_e2e import (
    _artifact_current_snapshot,
    _artifact_snapshots_for_artifact,
    _candidate_group_members,
    _discovered_url_observations,
)
from tests.component.services.upstream.test_db_backed_source_message_created_to_github_notification_queue_e2e import (
    RecordingNormalizeConsumer,
    _artifact_observations_for_source,
    _candidate_groups_for_source,
    _normalization_runs_for_source,
    _normalizer_config,
    _router_downstream_owned_counts,
    _source_message_version_count,
)
from tests.component.services.upstream.test_db_backed_source_message_github_to_notifier_dry_run_e2e import (
    RecordingNotifierConsumer,
    RaisingTelegramClient,
    _notification_rows,
    _notifier_config,
    _upstream_boundary_snapshot,
)


pytestmark = pytest.mark.skipif(
    not os.environ.get("LOCAL_TEST_DATABASE_URL"),
    reason="LOCAL_TEST_DATABASE_URL is required for the DB-backed source-message X to notifier e2e test",
)


@dataclass(frozen=True, slots=True)
class SeedXIds:
    source_message_id: UUID
    source_message_created_event_id: UUID
    post_id: str
    referenced_post_id: str
    canonical_id: str
    canonical_url: str


class RecordingXRedisConsumer:
    def __init__(self, message: XStreamMessage) -> None:
        self._messages = [message]
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        pass

    async def read_batch(self) -> list[XStreamMessage]:
        messages = self._messages
        self._messages = []
        return messages

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


class RecordingXClient:
    def __init__(self, *, post_id: str, referenced_post_id: str) -> None:
        self._post_id = post_id
        self._referenced_post_id = referenced_post_id
        self.calls: list[str] = []
        self.requested_post_ids: list[str] = []

    def default_request_profile(self) -> str:
        self.calls.append("default_request_profile")
        return "offline-x-profile"

    async def get_posts_by_ids(self, *, post_ids: list[str], profile: Any) -> dict[str, Any]:
        self.calls.append("get_posts_by_ids")
        self.requested_post_ids.extend(post_ids)
        assert post_ids == [self._post_id]
        assert profile == "offline-x-profile"
        return {
            "data": [
                {
                    "id": self._post_id,
                    "text": (
                        "Offline X post about an AI developer workflow tool "
                        "https://t.co/root"
                    ),
                    "author_id": "42",
                    "conversation_id": self._post_id,
                    "edit_history_tweet_ids": [self._post_id],
                    "public_metrics": {"like_count": 27, "repost_count": 5},
                    "entities": {
                        "urls": [
                            {
                                "url": "https://t.co/root",
                                "expanded_url": "https://github.com/example/offline-x-tool",
                            }
                        ]
                    },
                    "referenced_tweets": [{"type": "quoted", "id": self._referenced_post_id}],
                },
                {
                    "id": self._referenced_post_id,
                    "text": "Referenced context with implementation notes https://t.co/ref",
                    "author_id": "43",
                    "conversation_id": self._post_id,
                    "edit_history_tweet_ids": [self._referenced_post_id],
                    "entities": {
                        "urls": [
                            {
                                "url": "https://t.co/ref",
                                "expanded_url": "https://example.com/offline-x-tool-notes",
                            }
                        ]
                    },
                },
            ],
            "includes": {
                "users": [
                    {
                        "id": "42",
                        "username": "example",
                        "name": "Example Dev",
                        "verified": False,
                        "created_at": "2025-01-01T00:00:00Z",
                        "public_metrics": {"followers_count": 1234},
                    },
                    {
                        "id": "43",
                        "username": "contextdev",
                        "name": "Context Dev",
                        "verified": False,
                    },
                ]
            },
        }


@pytest.mark.asyncio
async def test_db_backed_source_message_x_path_creates_notifier_dry_run_records() -> None:
    database_url = _local_test_database_url()
    engine = create_async_engine(database_url, future=True)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            ids = await _seed_source_message_x_created_case(session)
            await session.commit()

            assert await _source_message_version_count(session, ids.source_message_id) == 1
            source_events = await _events(
                session,
                event_type="source_message.created.v1",
                aggregate_id=ids.source_message_id,
            )
            assert len(source_events) == 1
            assert source_events[0]["event_id"] == ids.source_message_created_event_id
            assert source_events[0]["payload_json"] == {
                "source_message_id": str(ids.source_message_id),
                "current_version_no": 1,
            }

            source_publisher = RecordingRedisPublisher()
            source_relay = OutboxRelayService(
                _outbox_config(database_url),
                repository=OutboxRelayRepository(session),
                publisher=source_publisher,
                route_resolver=OutboxRouteResolver(),
            )
            assert await source_relay.run_once() == 1
            await session.commit()

            assert len(source_publisher.published) == 1
            source_route, source_message = source_publisher.published[0]
            assert source_route.queue_name == "q.source.normalize"
            assert source_route.stage_name == "normalize"
            source_fields = source_message.as_stream_fields()
            assert source_fields == {
                "job_id": str(source_events[0]["event_id"]),
                "stage_name": "normalize",
                "root_object_type": "source_message",
                "root_object_id": str(ids.source_message_id),
                "idempotency_key": source_events[0]["dedupe_key"],
                "pipeline_run_id": "",
                "not_before": "",
                "trigger_event_id": str(source_events[0]["event_id"]),
            }
            assert set(source_fields) == THIN_REDIS_FIELDS
            assert "payload_json" not in source_fields

            normalizer = RouterNormalizerService(
                _normalizer_config(database_url),
                repository=RouterNormalizerRepository(session),
            )
            normalize_consumer = RecordingNormalizeConsumer("1-0", source_fields)
            normalize_worker = RouterNormalizerWorker(
                _normalizer_config(database_url),
                consumer=normalize_consumer,
                service=normalizer,
            )
            normalize_result = await normalize_worker.run_once()
            await session.commit()

            assert normalize_result.processed == 1
            assert normalize_result.acked == 1
            assert normalize_result.failed == 0
            assert normalize_result.skipped == 0
            assert normalize_consumer.acked == ["1-0"]

            normalization_runs = await _normalization_runs_for_source(session, ids.source_message_id)
            assert len(normalization_runs) == 1
            assert normalization_runs[0]["signal_detected"] is True
            assert normalization_runs[0]["candidate_eligible"] is True
            assert normalization_runs[0]["trigger_strength"] == "strong"

            artifacts = await _x_artifacts_for_source(session, ids.source_message_id)
            assert len(artifacts) == 1
            artifact = artifacts[0]
            artifact_id = artifact["artifact_id"]
            assert artifact["artifact_type"] == "x_post"
            assert artifact["canonical_id"] == ids.canonical_id
            assert artifact["canonical_url"] == ids.canonical_url
            assert artifact["normalized_host"] == "x.com"
            assert artifact["artifact_key_json"] == {"author": "example", "post_id": ids.post_id}

            observations = await _artifact_observations_for_source(session, ids.source_message_id)
            assert len(observations) == 1
            assert observations[0]["artifact_id"] == artifact_id
            assert observations[0]["observed_url"] == ids.canonical_url
            assert observations[0]["source_kind"] == "regex"

            candidate_groups = await _candidate_groups_for_source(session, ids.source_message_id)
            assert len(candidate_groups) == 1
            candidate_group_id = candidate_groups[0]["candidate_group_id"]
            fake_openai = RecordingOpenAIClient(candidate_group_id=candidate_group_id)
            assert candidate_groups[0]["initial_primary_artifact_id"] == artifact_id
            assert candidate_groups[0]["current_primary_artifact_id"] == artifact_id
            assert candidate_groups[0]["dedupe_subject_key"] == ids.canonical_id
            candidate_members_before_x = await _candidate_group_members(session, candidate_group_id)
            assert candidate_members_before_x == [
                {
                    "candidate_group_id": candidate_group_id,
                    "artifact_id": artifact_id,
                    "member_role": "primary",
                    "member_order": 0,
                }
            ]
            assert await _router_downstream_owned_counts(session, candidate_group_id) == {
                "candidate_evidence_bundles": 0,
                "judge_runs": 0,
                "judge_outputs": 0,
                "analyses": 0,
                "notification_plans": 0,
                "notification_renders": 0,
                "notification_delivery_records": 0,
            }

            enrich_events = await _events(
                session,
                event_type="artifact.enrich.requested.v1",
                aggregate_id=artifact_id,
            )
            assert len(enrich_events) == 1
            assert enrich_events[0]["payload_json"] == {
                "candidate_group_id": str(candidate_group_id),
                "artifact_id": str(artifact_id),
                "artifact_type": "x_post",
                "canonical_id": ids.canonical_id,
                "provider_route": "x",
                "refresh_mode": "standard",
                "depth_budget": 1,
                "source_message_id": str(ids.source_message_id),
                "source_version_no": 1,
            }
            await _move_event_to_front(session, enrich_events[0]["event_id"])
            await session.commit()

            artifact_count_after_router = await _count(session, "SELECT count(*) FROM artifact_registry", {})

            enrich_publisher = RecordingRedisPublisher()
            enrich_relay = OutboxRelayService(
                _outbox_config(database_url),
                repository=OutboxRelayRepository(session),
                publisher=enrich_publisher,
                route_resolver=OutboxRouteResolver(),
            )
            assert await enrich_relay.run_once() == 1
            await session.commit()

            assert len(enrich_publisher.published) == 1
            enrich_route, enrich_message = enrich_publisher.published[0]
            assert enrich_route.queue_name == "q.artifact.enrich.x"
            assert enrich_route.stage_name == "enrich_x"
            enrich_fields = enrich_message.as_stream_fields()
            assert enrich_fields == {
                "job_id": str(enrich_events[0]["event_id"]),
                "stage_name": "enrich_x",
                "root_object_type": "artifact",
                "root_object_id": str(artifact_id),
                "idempotency_key": enrich_events[0]["dedupe_key"],
                "pipeline_run_id": "",
                "not_before": "",
                "trigger_event_id": str(enrich_events[0]["event_id"]),
            }
            assert set(enrich_fields) == THIN_REDIS_FIELDS
            assert "payload_json" not in enrich_fields

            fake_x = RecordingXClient(post_id=ids.post_id, referenced_post_id=ids.referenced_post_id)
            x_service = XEnricherService(
                _x_config(database_url),
                repository=XEnricherRepository(session),
                x_api_client=fake_x,
                response_mapper=XResponseMapper(),
                url_discovery=XUrlDiscovery(),
            )
            x_consumer = RecordingXRedisConsumer(
                XStreamMessage(
                    stream=enrich_route.queue_name,
                    message_id="1-0",
                    fields=enrich_fields,
                )
            )
            x_worker = XEnricherWorker(
                _x_config(database_url),
                consumer=x_consumer,
                service=x_service,
            )
            x_result = await x_worker.run_once()
            await session.commit()

            assert x_result.processed == 1
            assert x_result.acked == 1
            assert x_consumer.acked == ["1-0"]
            assert fake_x.calls == ["default_request_profile", "get_posts_by_ids"]
            assert fake_x.requested_post_ids == [ids.post_id]
            assert fake_openai.calls == []

            enrichment_runs = await _x_artifact_enrichment_runs(session, artifact_id)
            assert len(enrichment_runs) == 1
            assert enrichment_runs[0]["provider"] == "x"
            assert enrichment_runs[0]["status"] == "ready"
            assert enrichment_runs[0]["content_anchor"] == f"xpost:{ids.post_id}:{ids.post_id}"

            snapshots = await _artifact_snapshots_for_artifact(session, artifact_id)
            assert len(snapshots) == 1
            snapshot_id = snapshots[0]["snapshot_id"]
            assert snapshots[0]["provider"] == "x"
            assert snapshots[0]["snapshot_type"] == "x_post"
            assert snapshots[0]["status"] == "ready"
            assert snapshots[0]["content_anchor"] == f"xpost:{ids.post_id}:{ids.post_id}"
            assert await _artifact_current_snapshot(session, artifact_id) == {
                "current_snapshot_id": snapshot_id,
                "current_status": "ready",
            }

            x_rows = await _x_post_rows(session, snapshot_id)
            assert len(x_rows) == 1
            assert x_rows[0]["post_id"] == ids.post_id
            assert x_rows[0]["content_anchor_post_version"] == f"xpost:{ids.post_id}:{ids.post_id}"
            assert x_rows[0]["author_summary_json"]["username"] == "example"
            assert "Offline X post about an AI developer workflow tool" in x_rows[0]["text_full"]
            assert x_rows[0]["referenced_post_ids_json"] == [ids.referenced_post_id]
            discovered_link_ids = {item["canonical_id"] for item in x_rows[0]["discovered_links_json"]}
            assert "github:repo:example/offline-x-tool" in discovered_link_ids
            assert any(item.startswith("web_article:") for item in discovered_link_ids)

            discovered_urls = await _discovered_url_observations(session, snapshot_id)
            assert len(discovered_urls) == 2
            assert {row["context_path"] for row in discovered_urls} == {
                "root_post.entities.urls[0]",
                "referenced_posts[0].entities.urls[0]",
            }
            assert await _count(session, "SELECT count(*) FROM artifact_registry", {}) == artifact_count_after_router
            assert await _candidate_group_members(session, candidate_group_id) == candidate_members_before_x
            assert await _events(
                session,
                event_type="analysis.requested.v1",
                aggregate_id=candidate_group_id,
            ) == []

            artifact_snapshot_events = await _events(
                session,
                event_type="artifact.snapshot.updated.v1",
                aggregate_id=artifact_id,
            )
            assert len(artifact_snapshot_events) == 1
            assert artifact_snapshot_events[0]["payload_json"] == {
                "artifact_id": str(artifact_id),
                "candidate_group_id": str(candidate_group_id),
                "snapshot_id": str(snapshot_id),
                "provider": "x",
                "provider_route": "x",
                "snapshot_type": "x_post",
                "status": "ready",
                "content_anchor": f"xpost:{ids.post_id}:{ids.post_id}",
            }

            evidence_assembler = EvidenceAssemblerService(
                _evidence_config(database_url),
                repository=EvidenceAssemblerRepository(session),
            )
            first_results = await evidence_assembler.handle_trigger_event(artifact_snapshot_events[0]["event_id"])
            second_results = await evidence_assembler.handle_trigger_event(artifact_snapshot_events[0]["event_id"])
            await _mark_events_published(session, [artifact_snapshot_events[0]["event_id"]])
            await session.commit()

            assert len(first_results) == 1
            assert first_results[0].candidate_group_id == candidate_group_id
            assert first_results[0].reused_existing_bundle is False
            assert first_results[0].ready_for_analysis is True
            assert first_results[0].emitted_analysis_requested is True
            assert len(second_results) == 1
            assert second_results[0].bundle_id == first_results[0].bundle_id
            assert second_results[0].reused_existing_bundle is True
            assert second_results[0].ready_for_analysis is True
            assert second_results[0].emitted_analysis_requested is False

            bundles = await _candidate_bundles(session, candidate_group_id)
            assert len(bundles) == 1
            bundle_id = bundles[0]["bundle_id"]
            assert await _candidate_bundle_members(session, bundle_id) == [
                {
                    "artifact_id": artifact_id,
                    "snapshot_id": snapshot_id,
                    "member_role": "primary",
                    "member_order": 0,
                }
            ]

            analysis_requested_events = await _events(
                session,
                event_type="analysis.requested.v1",
                aggregate_id=candidate_group_id,
            )
            assert len(analysis_requested_events) == 1
            assert analysis_requested_events[0]["payload_json"] == {
                "candidate_group_id": str(candidate_group_id),
                "bundle_id": str(bundle_id),
                "judge_profile": "x_primary",
                "escalation_allowed": True,
            }

            router = AnalysisRouterService(
                _router_config(database_url),
                repository=AnalysisRouterRepository(session),
            )
            await router.handle_trigger_event(analysis_requested_events[0]["event_id"])
            await router.handle_trigger_event(analysis_requested_events[0]["event_id"])
            await _mark_events_published(session, [analysis_requested_events[0]["event_id"]])
            await session.commit()

            judge_runs = await _judge_runs_for_bundle(session, bundle_id)
            assert len(judge_runs) == 1
            judge_run_id = judge_runs[0]["judge_run_id"]
            judge_call_events = await _events(
                session,
                event_type="judge.call.requested.v1",
                aggregate_id=judge_run_id,
            )
            assert len(judge_call_events) == 1
            assert judge_call_events[0]["payload_json"] == {
                "judge_run_id": str(judge_run_id),
                "candidate_group_id": str(candidate_group_id),
                "bundle_id": str(bundle_id),
                "judge_profile": "x_primary",
                "model": "gpt-5.4-mini",
                "reasoning_effort": "low",
                "prompt_version": "judge_x_primary_v1",
                "prompt_cache_key": "judge:x_primary:judge_x_primary_v1:judge_output_v1:verdict_policy_v1",
            }

            judge_openai = JudgeOpenAIService(
                _judge_openai_config(database_url),
                repository=JudgeOpenAIRepository(session),
                openai_client=fake_openai,
            )
            await judge_openai.handle_trigger_event(judge_call_events[0]["event_id"])
            await _mark_events_published(session, [judge_call_events[0]["event_id"]])
            await session.commit()

            assert len(fake_openai.calls) == 1
            assert fake_openai.calls[0]["prompt_cache_key"] == (
                "judge:x_primary:judge_x_primary_v1:judge_output_v1:verdict_policy_v1"
            )
            judge_outputs = await _judge_outputs_for_run(session, judge_run_id)
            assert len(judge_outputs) == 1
            judge_output_id = judge_outputs[0]["judge_output_id"]
            judge_output_ready_events = await _events(
                session,
                event_type="judge.output.ready.v1",
                aggregate_id=judge_run_id,
            )
            assert len(judge_output_ready_events) == 1

            validator = AnalysisValidatorService(
                _validator_config(database_url),
                repository=AnalysisValidatorRepository(session),
            )
            await validator.handle_trigger_event(judge_output_ready_events[0]["event_id"])
            await _mark_events_published(session, [judge_output_ready_events[0]["event_id"]])
            await session.commit()

            policy_events = await _events(
                session,
                event_type="analysis.policy.apply.v1",
                aggregate_id=judge_run_id,
            )
            assert len(policy_events) == 1
            policy = PolicyEngineService(
                _policy_config(database_url, enable_notification_send=True),
                repository=PolicyEngineRepository(session),
            )
            await policy.handle_trigger_event(policy_events[0]["event_id"])
            await _mark_events_published(session, [policy_events[0]["event_id"]])
            await session.commit()

            analyses = await _analyses_for_judge_output(session, judge_output_id)
            assert len(analyses) == 1
            analysis_id = analyses[0]["analysis_id"]
            assert analyses[0]["verdict"] == "inspect_now"
            assert analyses[0]["delivery_decision"] == "send_now"

            notification_events = await _events(
                session,
                event_type="notification.plan.created.v1",
                aggregate_id=analysis_id,
            )
            assert len(notification_events) == 1
            await _move_event_to_front(session, notification_events[0]["event_id"])
            await session.commit()

            assert await _notifier_owned_counts(session, candidate_group_id) == {
                "notification_plans": 0,
                "notification_renders": 0,
                "notification_delivery_records": 0,
            }

            notify_publisher = RecordingRedisPublisher()
            notify_relay = OutboxRelayService(
                _outbox_config(database_url),
                repository=OutboxRelayRepository(session),
                publisher=notify_publisher,
                route_resolver=OutboxRouteResolver(),
            )
            assert await notify_relay.run_once() == 1
            await session.commit()

            assert len(notify_publisher.published) == 1
            notify_route, notify_message = notify_publisher.published[0]
            assert notify_route.queue_name == "q.notification.send"
            assert notify_route.stage_name == "notify"
            notify_fields = notify_message.as_stream_fields()
            assert notify_fields == {
                "job_id": str(notification_events[0]["event_id"]),
                "stage_name": "notify",
                "root_object_type": "analysis",
                "root_object_id": str(analysis_id),
                "idempotency_key": notification_events[0]["dedupe_key"],
                "pipeline_run_id": "",
                "not_before": "",
                "trigger_event_id": str(notification_events[0]["event_id"]),
            }
            assert set(notify_fields) == THIN_REDIS_FIELDS
            assert "payload_json" not in notify_fields

            before_notifier_counts = await _notifier_owned_counts(session, candidate_group_id)
            before_notifier_snapshot = await _upstream_boundary_snapshot(
                session,
                source_message_id=ids.source_message_id,
                artifact_id=artifact_id,
                candidate_group_id=candidate_group_id,
                bundle_id=bundle_id,
                judge_output_id=judge_output_id,
                analysis_id=analysis_id,
            )
            assert before_notifier_counts == {
                "notification_plans": 0,
                "notification_renders": 0,
                "notification_delivery_records": 0,
            }

            telegram_client = RaisingTelegramClient()
            notifier_consumer = RecordingNotifierConsumer(
                NotifierStreamMessage(
                    stream=notify_route.queue_name,
                    message_id="1-0",
                    fields=notify_fields,
                )
            )
            notifier_worker = NotifierTelegramWorker(
                _notifier_config(database_url),
                consumer=notifier_consumer,
                service=NotifierTelegramService(
                    _notifier_config(database_url),
                    repository=NotifierTelegramRepository(session),
                    telegram_client=telegram_client,
                ),
            )
            notifier_result = await notifier_worker.run_once()
            await session.commit()

            assert notifier_result.processed == 1
            assert notifier_result.acked == 1
            assert notifier_consumer.acked == ["1-0"]
            assert telegram_client.calls == []
            assert await _upstream_boundary_snapshot(
                session,
                source_message_id=ids.source_message_id,
                artifact_id=artifact_id,
                candidate_group_id=candidate_group_id,
                bundle_id=bundle_id,
                judge_output_id=judge_output_id,
                analysis_id=analysis_id,
            ) == before_notifier_snapshot

            after_notifier_counts = await _notifier_owned_counts(session, candidate_group_id)
            assert after_notifier_counts == {
                "notification_plans": 1,
                "notification_renders": 1,
                "notification_delivery_records": 1,
            }

            notification_rows = await _notification_rows(session, candidate_group_id)
            assert len(notification_rows["plans"]) == 1
            plan = notification_rows["plans"][0]
            notification_plan_id = plan["notification_plan_id"]
            assert plan["analysis_id"] == analysis_id
            assert plan["candidate_group_id"] == candidate_group_id
            assert plan["delivery_decision"] == "send_now"
            assert plan["urgency_profile"] == "high"
            assert plan["target_chat_id"] == 12345
            assert plan["render_profile"] == "telegram_single_alert_high_v1"
            assert plan["status"] == "suppressed"

            assert len(notification_rows["renders"]) == 1
            assert notification_rows["renders"][0]["notification_plan_id"] == notification_plan_id
            assert notification_rows["renders"][0]["disable_notification"] is False

            assert len(notification_rows["delivery_records"]) == 1
            delivery_record = notification_rows["delivery_records"][0]
            assert delivery_record["notification_plan_id"] == notification_plan_id
            assert delivery_record["delivery_status"] == "suppressed"
            assert delivery_record["attempt_count"] == 0
            assert delivery_record["transport_error_code"] == "dry_run_skip_transport"
            assert delivery_record["transport_error_class"] is None
            assert delivery_record["telegram_chat_id"] == 12345
            assert delivery_record["telegram_message_id"] is None
            assert delivery_record["telegram_response_json"] == {
                "delivery_action": "send",
                "dry_run": True,
                "reason_code": "dry_run_skip_transport",
                "send_disabled": False,
                "send_enabled": True,
                "transport_skipped": True,
            }
            assert sorted(notification_rows["state_transition_reason_codes"]) == [
                "dry_run_skip_transport",
                "notification_rendered",
            ]
            assert notification_rows["delivery_result_events"] == [
                {
                    "aggregate_id": notification_plan_id,
                    "payload_json": {
                        "attempt_count": 0,
                        "delivery_status": "suppressed",
                        "edited": False,
                        "notification_delivery_record_id": str(
                            delivery_record["notification_delivery_record_id"]
                        ),
                        "notification_plan_id": str(notification_plan_id),
                        "telegram_chat_id": 12345,
                        "telegram_message_id": None,
                        "transport_error_class": None,
                        "transport_error_code": "dry_run_skip_transport",
                    },
                    "status": "pending",
                }
            ]

            replay_consumer = RecordingNotifierConsumer(
                NotifierStreamMessage(
                    stream=notify_route.queue_name,
                    message_id="2-0",
                    fields=notify_fields,
                )
            )
            replay_worker = NotifierTelegramWorker(
                _notifier_config(database_url),
                consumer=replay_consumer,
                service=NotifierTelegramService(
                    _notifier_config(database_url),
                    repository=NotifierTelegramRepository(session),
                    telegram_client=telegram_client,
                ),
            )
            replay_result = await replay_worker.run_once()
            await session.commit()

            assert replay_result.processed == 1
            assert replay_result.acked == 1
            assert replay_consumer.acked == ["2-0"]
            assert telegram_client.calls == []
            assert await _notifier_owned_counts(session, candidate_group_id) == after_notifier_counts
            replay_rows = await _notification_rows(session, candidate_group_id)
            assert sorted(replay_rows["state_transition_reason_codes"]) == [
                "dry_run_skip_transport",
                "notification_duplicate_terminal_noop",
                "notification_rendered",
            ]
            assert replay_rows["delivery_result_events"] == notification_rows["delivery_result_events"]
            assert await _upstream_boundary_snapshot(
                session,
                source_message_id=ids.source_message_id,
                artifact_id=artifact_id,
                candidate_group_id=candidate_group_id,
                bundle_id=bundle_id,
                judge_output_id=judge_output_id,
                analysis_id=analysis_id,
            ) == before_notifier_snapshot
    finally:
        await engine.dispose()


async def _seed_source_message_x_created_case(session: AsyncSession) -> SeedXIds:
    source_message_id = uuid4()
    event_id = uuid4()
    suffix = uuid4().hex
    now = datetime.now(timezone.utc)
    post_id = str(1_880_000_000_000_000_000 + int(suffix[:10], 16))
    referenced_post_id = str(int(post_id) + 1)
    canonical_id = f"x:post:{post_id}"
    canonical_url = f"https://x.com/example/status/{post_id}"
    source_text = (
        "AI developer workflow tool launch for source-message X E2E: "
        f"{canonical_url}"
    )
    raw_message_json = {
        "local_test": True,
        "content": {
            "@type": "messageText",
            "text": {"text": source_text},
        },
    }

    await session.execute(
        sa.text(
            """
            INSERT INTO source_messages (
                source_message_id, chat_id, message_id, logical_post_key,
                is_channel_post, posted_at, content_type, text_body, text_surface,
                entities_json, url_surface_json, raw_message_json, current_version_no
            ) VALUES (
                CAST(:source_message_id AS uuid), :chat_id, :message_id, :logical_post_key,
                true, :posted_at, 'text', :text_body, :text_body,
                CAST(:entities_json AS jsonb), CAST(:url_surface_json AS jsonb),
                CAST(:raw_message_json AS jsonb), 1
            )
            """
        ),
        {
            "source_message_id": str(source_message_id),
            "chat_id": 9500000000 + int(suffix[:8], 16),
            "message_id": int(suffix[8:16], 16),
            "logical_post_key": f"db-source-message-x-to-notifier-e2e:{suffix}",
            "posted_at": now,
            "text_body": source_text,
            "entities_json": _jsonb([]),
            "url_surface_json": _jsonb([]),
            "raw_message_json": _jsonb(raw_message_json),
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO source_message_versions (
                source_message_id, version_no, version_reason, observed_at,
                text_surface, entities_json, raw_message_json, content_hash
            ) VALUES (
                CAST(:source_message_id AS uuid), 1, 'new', :observed_at,
                :text_surface, CAST(:entities_json AS jsonb),
                CAST(:raw_message_json AS jsonb), :content_hash
            )
            """
        ),
        {
            "source_message_id": str(source_message_id),
            "observed_at": now,
            "text_surface": source_text,
            "entities_json": _jsonb([]),
            "raw_message_json": _jsonb(raw_message_json),
            "content_hash": hashlib.sha256(source_text.encode("utf-8")).hexdigest(),
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO event_outbox (
                event_id, event_type, aggregate_type, aggregate_id, dedupe_key,
                payload_json, status, created_at
            ) VALUES (
                CAST(:event_id AS uuid), 'source_message.created.v1', 'source_message',
                CAST(:source_message_id AS uuid), :dedupe_key,
                CAST(:payload_json AS jsonb), 'pending'::outbox_status_enum,
                TIMESTAMPTZ '0001-01-01 00:00:00+00'
            )
            """
        ),
        {
            "event_id": str(event_id),
            "source_message_id": str(source_message_id),
            "dedupe_key": f"db-source-message-x-to-notifier-e2e:{suffix}:source_message.created",
            "payload_json": _jsonb(
                {
                    "source_message_id": str(source_message_id),
                    "current_version_no": 1,
                }
            ),
        },
    )
    return SeedXIds(
        source_message_id=source_message_id,
        source_message_created_event_id=event_id,
        post_id=post_id,
        referenced_post_id=referenced_post_id,
        canonical_id=canonical_id,
        canonical_url=canonical_url,
    )


async def _x_artifacts_for_source(session: AsyncSession, source_message_id: UUID) -> list[dict[str, Any]]:
    return await _rows(
        session,
        """
        SELECT DISTINCT ar.artifact_id, ar.artifact_type, ar.canonical_id,
                        ar.canonical_url, ar.normalized_host, ar.artifact_key_json
        FROM artifact_registry ar
        JOIN artifact_observations ao
          ON ao.artifact_id = ar.artifact_id
        WHERE ao.source_message_id = CAST(:source_message_id AS uuid)
          AND ar.artifact_type = 'x_post'::artifact_type_enum
        ORDER BY ar.canonical_id
        """,
        {"source_message_id": str(source_message_id)},
    )


async def _x_artifact_enrichment_runs(session: AsyncSession, artifact_id: UUID) -> list[dict[str, Any]]:
    return await _rows(
        session,
        """
        SELECT artifact_enrichment_run_id, artifact_id, provider, refresh_mode,
               depth_budget, status, content_anchor, job_idempotency_key
        FROM artifact_enrichment_runs
        WHERE artifact_id = CAST(:artifact_id AS uuid)
          AND provider = 'x'
        ORDER BY requested_at, artifact_enrichment_run_id
        """,
        {"artifact_id": str(artifact_id)},
    )


async def _x_post_rows(session: AsyncSession, snapshot_id: UUID) -> list[dict[str, Any]]:
    return await _rows(
        session,
        """
        SELECT snapshot_id, post_id, content_anchor_post_version,
               author_summary_json, text_full, text_excerpt, conversation_id,
               referenced_post_ids_json, discovered_links_json,
               media_summary_json, metrics_summary_json
        FROM artifact_snapshot_x_post
        WHERE snapshot_id = CAST(:snapshot_id AS uuid)
        ORDER BY snapshot_id
        """,
        {"snapshot_id": str(snapshot_id)},
    )


async def _rows(session: AsyncSession, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    result = await session.execute(sa.text(sql), params)
    return [_row_dict(row) for row in result.mappings().all()]


def _row_dict(row: Any) -> dict[str, Any]:
    converted = dict(row)
    for key, value in list(converted.items()):
        if key.endswith("_json") or key == "payload_json":
            converted[key] = _json_obj(value)
    return converted


def _x_config(database_url: str) -> XEnricherConfig:
    return XEnricherConfig(
        app_env="test",
        database_url=database_url,
        redis_url="unused",
        queue_name="q.artifact.enrich.x",
        consumer_group="x-enricher",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        x_api_base_url="https://api.x.invalid",
        x_bearer_token="unused-test-token",
        request_timeout_sec=1.0,
        request_max_ids=100,
        depth_budget_default=1,
        log_level="INFO",
    )
