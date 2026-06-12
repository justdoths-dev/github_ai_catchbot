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
from services.gh_enricher.fetch_planner import GitHubFetchPlanner
from services.gh_enricher.file_sampler import GitHubFileSampler
from services.gh_enricher.redis_streams import StreamMessage as GhStreamMessage
from services.gh_enricher.repositories import GhEnricherRepository
from services.gh_enricher.service import GhEnricherService
from services.gh_enricher.url_discovery import GitHubUrlDiscovery
from services.gh_enricher.worker import GhEnricherWorker
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
    RecordingGitHubClient,
    RecordingRedisConsumer as RecordingGhRedisConsumer,
    _artifact_current_snapshot,
    _artifact_enrichment_runs as _github_artifact_enrichment_runs,
    _artifact_snapshots_for_artifact,
    _candidate_group_members,
    _discovered_url_observations,
    _gh_config,
    _github_file_samples,
    _github_repo_rows,
)
from tests.component.services.upstream.test_db_backed_source_message_created_to_github_notification_queue_e2e import (
    _router_downstream_owned_counts,
    _source_message_version_count,
)
from tests.component.services.upstream.test_db_backed_source_message_github_to_notifier_dry_run_e2e import (
    RecordingNotifierConsumer,
    RaisingTelegramClient,
    _notification_rows,
    _notifier_config,
)
from tests.component.services.upstream.test_db_backed_source_message_x_post_to_notifier_dry_run_e2e import (
    RecordingXRedisConsumer,
    _x_artifact_enrichment_runs,
    _x_config,
    _x_post_rows,
)


pytestmark = pytest.mark.skipif(
    not os.environ.get("LOCAL_TEST_DATABASE_URL"),
    reason="LOCAL_TEST_DATABASE_URL is required for the DB-backed X-discovered GitHub promotion reroot e2e test",
)


@dataclass(frozen=True, slots=True)
class SeedPromotionIds:
    source_message_id: UUID
    candidate_group_id: UUID
    x_artifact_id: UUID
    x_enrich_requested_event_id: UUID
    x_post_id: str
    x_canonical_id: str
    x_canonical_url: str
    github_canonical_id: str
    github_canonical_url: str
    repo_full_name: str


@dataclass(frozen=True, slots=True)
class UpstreamBoundarySnapshot:
    source_messages: list[dict[str, Any]]
    artifact_registry: list[dict[str, Any]]
    candidate_group_members: list[dict[str, Any]]
    candidate_evidence_bundles: list[dict[str, Any]]
    judge_outputs: list[dict[str, Any]]
    analyses: list[dict[str, Any]]


class RecordingRerootXClient:
    def __init__(self, *, post_id: str, github_canonical_url: str) -> None:
        self._post_id = post_id
        self._github_canonical_url = github_canonical_url
        self.calls: list[str] = []
        self.requested_post_ids: list[str] = []

    def default_request_profile(self) -> str:
        self.calls.append("default_request_profile")
        return "offline-x-reroot-profile"

    async def get_posts_by_ids(self, *, post_ids: list[str], profile: Any) -> dict[str, Any]:
        self.calls.append("get_posts_by_ids")
        self.requested_post_ids.extend(post_ids)
        assert post_ids == [self._post_id]
        assert profile == "offline-x-reroot-profile"
        return {
            "data": [
                {
                    "id": self._post_id,
                    "text": (
                        "Offline X post describing the implementation details for "
                        f"{self._github_canonical_url}"
                    ),
                    "author_id": "4242",
                    "conversation_id": self._post_id,
                    "edit_history_tweet_ids": [self._post_id],
                    "public_metrics": {"like_count": 31, "repost_count": 7},
                    "entities": {
                        "urls": [
                            {
                                "url": "https://t.co/reroot",
                                "expanded_url": self._github_canonical_url,
                            }
                        ]
                    },
                }
            ],
            "includes": {
                "users": [
                    {
                        "id": "4242",
                        "username": "rerootdev",
                        "name": "Reroot Dev",
                        "verified": False,
                        "created_at": "2025-01-01T00:00:00Z",
                        "public_metrics": {"followers_count": 2048},
                    }
                ]
            },
        }


@pytest.mark.asyncio
async def test_db_backed_x_discovered_github_promotion_reroot_creates_notifier_dry_run_records() -> None:
    database_url = _local_test_database_url()
    engine = create_async_engine(database_url, future=True)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            ids = await _seed_x_discovered_github_promotion_case(session)
            await session.commit()

            assert await _count(
                session,
                "SELECT count(*) FROM source_messages WHERE source_message_id = CAST(:source_message_id AS uuid)",
                {"source_message_id": str(ids.source_message_id)},
            ) == 1
            assert await _source_message_version_count(session, ids.source_message_id) == 1
            assert await _candidate_group_current_primary(session, ids.candidate_group_id) == ids.x_artifact_id
            candidate_members_before = await _candidate_group_members(session, ids.candidate_group_id)
            assert candidate_members_before == [
                {
                    "candidate_group_id": ids.candidate_group_id,
                    "artifact_id": ids.x_artifact_id,
                    "member_role": "primary",
                    "member_order": 0,
                }
            ]
            assert await _artifact_by_canonical_id(session, ids.github_canonical_id) is None
            assert await _router_downstream_owned_counts(session, ids.candidate_group_id) == {
                "candidate_evidence_bundles": 0,
                "judge_runs": 0,
                "judge_outputs": 0,
                "analyses": 0,
                "notification_plans": 0,
                "notification_renders": 0,
                "notification_delivery_records": 0,
            }

            x_enrich_events = await _events(
                session,
                event_type="artifact.enrich.requested.v1",
                aggregate_id=ids.x_artifact_id,
            )
            assert len(x_enrich_events) == 1
            assert x_enrich_events[0]["event_id"] == ids.x_enrich_requested_event_id
            assert x_enrich_events[0]["payload_json"] == {
                "candidate_group_id": str(ids.candidate_group_id),
                "artifact_id": str(ids.x_artifact_id),
                "artifact_type": "x_post",
                "canonical_id": ids.x_canonical_id,
                "provider_route": "x",
                "refresh_mode": "standard",
                "depth_budget": 1,
                "source_message_id": str(ids.source_message_id),
                "source_version_no": 1,
            }
            assert await _events(
                session,
                event_type="artifact.enrich.requested.v1",
                aggregate_id=ids.candidate_group_id,
            ) == []

            await _move_event_to_front(session, ids.x_enrich_requested_event_id)
            await session.commit()
            artifact_count_after_seed = await _count(session, "SELECT count(*) FROM artifact_registry", {})

            x_publisher = RecordingRedisPublisher()
            x_relay = OutboxRelayService(
                _outbox_config(database_url),
                repository=OutboxRelayRepository(session),
                publisher=x_publisher,
                route_resolver=OutboxRouteResolver(),
            )
            assert await x_relay.run_once() == 1
            await session.commit()

            assert len(x_publisher.published) == 1
            x_route, x_message = x_publisher.published[0]
            assert x_route.queue_name == "q.artifact.enrich.x"
            assert x_route.stage_name == "enrich_x"
            x_fields = x_message.as_stream_fields()
            assert x_fields == {
                "job_id": str(ids.x_enrich_requested_event_id),
                "stage_name": "enrich_x",
                "root_object_type": "artifact",
                "root_object_id": str(ids.x_artifact_id),
                "idempotency_key": x_enrich_events[0]["dedupe_key"],
                "pipeline_run_id": "",
                "not_before": "",
                "trigger_event_id": str(ids.x_enrich_requested_event_id),
            }
            assert set(x_fields) == THIN_REDIS_FIELDS
            assert "payload_json" not in x_fields

            fake_x = RecordingRerootXClient(
                post_id=ids.x_post_id,
                github_canonical_url=ids.github_canonical_url,
            )
            fake_openai = RecordingOpenAIClient(candidate_group_id=ids.candidate_group_id)
            x_service = XEnricherService(
                _x_config(database_url),
                repository=XEnricherRepository(session),
                x_api_client=fake_x,
                response_mapper=XResponseMapper(),
                url_discovery=XUrlDiscovery(),
            )
            x_consumer = RecordingXRedisConsumer(
                XStreamMessage(stream=x_route.queue_name, message_id="1-0", fields=x_fields)
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
            assert fake_x.requested_post_ids == [ids.x_post_id]
            assert fake_openai.calls == []

            x_runs = await _x_artifact_enrichment_runs(session, ids.x_artifact_id)
            assert len(x_runs) == 1
            assert x_runs[0]["provider"] == "x"
            assert x_runs[0]["status"] == "ready"

            x_snapshots = await _artifact_snapshots_for_artifact(session, ids.x_artifact_id)
            assert len(x_snapshots) == 1
            x_snapshot_id = x_snapshots[0]["snapshot_id"]
            assert x_snapshots[0]["provider"] == "x"
            assert x_snapshots[0]["snapshot_type"] == "x_post"
            assert x_snapshots[0]["status"] == "ready"
            assert await _artifact_current_snapshot(session, ids.x_artifact_id) == {
                "current_snapshot_id": x_snapshot_id,
                "current_status": "ready",
            }

            x_rows = await _x_post_rows(session, x_snapshot_id)
            assert len(x_rows) == 1
            assert x_rows[0]["post_id"] == ids.x_post_id
            assert x_rows[0]["author_summary_json"]["username"] == "rerootdev"
            assert ids.github_canonical_url in x_rows[0]["text_full"]
            discovered_link_ids = {item["canonical_id"] for item in x_rows[0]["discovered_links_json"]}
            discovered_link_urls = {item["canonical_url"] for item in x_rows[0]["discovered_links_json"]}
            assert ids.github_canonical_id in discovered_link_ids
            assert ids.github_canonical_url in discovered_link_urls

            x_discovered_urls = await _discovered_url_observations(session, x_snapshot_id)
            assert len(x_discovered_urls) == 1
            assert x_discovered_urls[0]["observed_url"] == ids.github_canonical_url
            assert x_discovered_urls[0]["context_path"] == "root_post.entities.urls[0]"
            assert x_discovered_urls[0]["discovery_reason"] == "x_post_embedded_link"
            assert await _count(session, "SELECT count(*) FROM artifact_registry", {}) == artifact_count_after_seed
            assert await _candidate_group_members(session, ids.candidate_group_id) == candidate_members_before
            assert await _events(
                session,
                event_type="analysis.requested.v1",
                aggregate_id=ids.candidate_group_id,
            ) == []

            x_snapshot_events = await _events(
                session,
                event_type="artifact.snapshot.updated.v1",
                aggregate_id=ids.x_artifact_id,
            )
            assert len(x_snapshot_events) == 1
            assert x_snapshot_events[0]["payload_json"] == {
                "artifact_id": str(ids.x_artifact_id),
                "candidate_group_id": str(ids.candidate_group_id),
                "snapshot_id": str(x_snapshot_id),
                "provider": "x",
                "provider_route": "x",
                "snapshot_type": "x_post",
                "status": "ready",
                "content_anchor": f"xpost:{ids.x_post_id}:{ids.x_post_id}",
            }

            evidence_assembler = EvidenceAssemblerService(
                _evidence_config(database_url),
                repository=EvidenceAssemblerRepository(session),
            )
            promotion_results = await evidence_assembler.handle_trigger_event(x_snapshot_events[0]["event_id"])
            promotion_replay_results = await evidence_assembler.handle_trigger_event(x_snapshot_events[0]["event_id"])
            await session.commit()

            assert len(promotion_results) == 1
            assert promotion_results[0].candidate_group_id == ids.candidate_group_id
            assert promotion_results[0].bundle_id is None
            assert promotion_results[0].reused_existing_bundle is False
            assert promotion_results[0].ready_for_analysis is False
            assert promotion_results[0].emitted_analysis_requested is False
            assert len(promotion_replay_results) == 1
            assert promotion_replay_results[0].candidate_group_id == ids.candidate_group_id
            assert promotion_replay_results[0].bundle_id is None
            assert promotion_replay_results[0].ready_for_analysis is False
            assert promotion_replay_results[0].emitted_analysis_requested is False

            github_artifact = await _artifact_by_canonical_id(session, ids.github_canonical_id)
            assert github_artifact is not None
            github_artifact_id = github_artifact["artifact_id"]
            assert github_artifact == {
                "artifact_id": github_artifact_id,
                "artifact_type": "github_repo",
                "canonical_id": ids.github_canonical_id,
                "canonical_url": ids.github_canonical_url,
                "normalized_host": "github.com",
                "current_snapshot_id": None,
                "current_status": None,
            }
            candidate_members_after_promotion = await _candidate_group_members(session, ids.candidate_group_id)
            assert candidate_members_after_promotion == [
                {
                    "candidate_group_id": ids.candidate_group_id,
                    "artifact_id": ids.x_artifact_id,
                    "member_role": "primary",
                    "member_order": 0,
                },
                {
                    "candidate_group_id": ids.candidate_group_id,
                    "artifact_id": github_artifact_id,
                    "member_role": "supporting",
                    "member_order": 1,
                },
            ]
            github_enrich_events = await _events(
                session,
                event_type="artifact.enrich.requested.v1",
                aggregate_id=github_artifact_id,
            )
            assert len(github_enrich_events) == 1
            assert github_enrich_events[0]["payload_json"] == {
                "candidate_group_id": str(ids.candidate_group_id),
                "artifact_id": str(github_artifact_id),
                "artifact_type": "github_repo",
                "canonical_id": ids.github_canonical_id,
                "provider_route": "github",
                "refresh_mode": "standard",
                "depth_budget": 0,
                "source_message_id": str(ids.source_message_id),
                "source_version_no": 1,
            }
            assert await _candidate_reroot_events(session, ids.candidate_group_id) == []
            assert await _candidate_group_current_primary(session, ids.candidate_group_id) == ids.x_artifact_id
            assert await _candidate_bundles(session, ids.candidate_group_id) == []
            assert await _events(
                session,
                event_type="analysis.requested.v1",
                aggregate_id=ids.candidate_group_id,
            ) == []
            assert await _count(session, "SELECT count(*) FROM artifact_registry", {}) == artifact_count_after_seed + 1

            await _move_event_to_front(session, github_enrich_events[0]["event_id"])
            await session.commit()
            github_publisher = RecordingRedisPublisher()
            github_relay = OutboxRelayService(
                _outbox_config(database_url),
                repository=OutboxRelayRepository(session),
                publisher=github_publisher,
                route_resolver=OutboxRouteResolver(),
            )
            assert await github_relay.run_once() == 1
            await _mark_events_published(session, [x_snapshot_events[0]["event_id"]])
            await session.commit()

            assert len(github_publisher.published) == 1
            github_route, github_message = github_publisher.published[0]
            assert github_route.queue_name == "q.artifact.enrich.github"
            assert github_route.stage_name == "enrich_github"
            github_fields = github_message.as_stream_fields()
            assert github_fields == {
                "job_id": str(github_enrich_events[0]["event_id"]),
                "stage_name": "enrich_github",
                "root_object_type": "artifact",
                "root_object_id": str(github_artifact_id),
                "idempotency_key": github_enrich_events[0]["dedupe_key"],
                "pipeline_run_id": "",
                "not_before": "",
                "trigger_event_id": str(github_enrich_events[0]["event_id"]),
            }
            assert set(github_fields) == THIN_REDIS_FIELDS
            assert "payload_json" not in github_fields

            owner, repo = ids.repo_full_name.split("/", 1)
            fake_github = RecordingGitHubClient(owner=owner, repo=repo)
            gh_service = GhEnricherService(
                _gh_config(database_url),
                repository=GhEnricherRepository(session),
                github_client=fake_github,
                fetch_planner=GitHubFetchPlanner(),
                file_sampler=GitHubFileSampler(),
                url_discovery=GitHubUrlDiscovery(),
            )
            gh_consumer = RecordingGhRedisConsumer(
                GhStreamMessage(stream=github_route.queue_name, message_id="1-0", fields=github_fields)
            )
            gh_worker = GhEnricherWorker(
                _gh_config(database_url),
                consumer=gh_consumer,
                service=gh_service,
            )
            gh_result = await gh_worker.run_once()
            await session.commit()

            assert gh_result.processed == 1
            assert gh_result.acked == 1
            assert gh_consumer.acked == ["1-0"]
            assert fake_github.calls == [
                "repo",
                "head",
                "tree",
                "contents:README.md",
                "contents:pyproject.toml",
                "contents:.github/workflows/ci.yml",
                "contents:tests/test_feature.py",
                "releases",
            ]
            assert fake_openai.calls == []

            github_runs = await _github_artifact_enrichment_runs(session, github_artifact_id)
            assert len(github_runs) == 1
            assert github_runs[0]["provider"] == "github"
            assert github_runs[0]["status"] in {"ready", "partial_ready"}

            github_snapshots = await _artifact_snapshots_for_artifact(session, github_artifact_id)
            assert len(github_snapshots) == 1
            github_snapshot_id = github_snapshots[0]["snapshot_id"]
            assert github_snapshots[0]["provider"] == "github"
            assert github_snapshots[0]["snapshot_type"] == "github_repo"
            assert github_snapshots[0]["status"] in {"ready", "partial_ready"}
            assert github_snapshots[0]["content_anchor"] == "commit:abc123def456"
            assert await _artifact_current_snapshot(session, github_artifact_id) == {
                "current_snapshot_id": github_snapshot_id,
                "current_status": github_snapshots[0]["status"],
            }
            assert await _github_repo_rows(session, github_snapshot_id) == [
                {
                    "snapshot_id": github_snapshot_id,
                    "repo_full_name": ids.repo_full_name,
                    "default_branch": "main",
                    "resolved_ref": "abc123def456",
                    "content_anchor_commit_sha": "abc123def456",
                }
            ]
            assert len(await _github_file_samples(session, github_snapshot_id)) >= 1
            assert len(await _discovered_url_observations(session, github_snapshot_id)) >= 1
            assert await _count(session, "SELECT count(*) FROM artifact_registry", {}) == artifact_count_after_seed + 1
            assert await _candidate_group_members(session, ids.candidate_group_id) == candidate_members_after_promotion
            assert await _events(
                session,
                event_type="analysis.requested.v1",
                aggregate_id=ids.candidate_group_id,
            ) == []

            github_snapshot_events = await _events(
                session,
                event_type="artifact.snapshot.updated.v1",
                aggregate_id=github_artifact_id,
            )
            assert len(github_snapshot_events) == 1
            assert github_snapshot_events[0]["payload_json"] == {
                "artifact_id": str(github_artifact_id),
                "snapshot_id": str(github_snapshot_id),
                "provider": "github",
                "status": github_snapshots[0]["status"],
                "content_anchor": "commit:abc123def456",
            }
            assert "candidate_group_id" not in github_snapshot_events[0]["payload_json"]

            evidence_assembler = EvidenceAssemblerService(
                _evidence_config(database_url),
                repository=EvidenceAssemblerRepository(session),
            )
            first_results = await evidence_assembler.handle_trigger_event(github_snapshot_events[0]["event_id"])
            second_results = await evidence_assembler.handle_trigger_event(github_snapshot_events[0]["event_id"])
            await _mark_events_published(session, [github_snapshot_events[0]["event_id"]])
            await session.commit()

            assert len(first_results) == 1
            assert first_results[0].candidate_group_id == ids.candidate_group_id
            assert first_results[0].reused_existing_bundle is False
            assert first_results[0].ready_for_analysis is True
            assert first_results[0].emitted_analysis_requested is True
            assert len(second_results) == 1
            assert second_results[0].candidate_group_id == ids.candidate_group_id
            assert second_results[0].bundle_id == first_results[0].bundle_id
            assert second_results[0].reused_existing_bundle is True
            assert second_results[0].ready_for_analysis is True
            assert second_results[0].emitted_analysis_requested is False

            reroot_events = await _candidate_reroot_events(session, ids.candidate_group_id)
            assert reroot_events == [
                {
                    "from_artifact_id": ids.x_artifact_id,
                    "to_artifact_id": github_artifact_id,
                    "reason_code": "x_post_discovered_github_repo_supporting_reroot",
                    "trigger_snapshot_id": github_snapshot_id,
                }
            ]
            assert await _candidate_group_current_primary(session, ids.candidate_group_id) == github_artifact_id
            assert await _candidate_group_members(session, ids.candidate_group_id) == candidate_members_after_promotion

            bundles = await _candidate_bundles(session, ids.candidate_group_id)
            assert len(bundles) == 1
            bundle_id = bundles[0]["bundle_id"]
            assert bundle_id == first_results[0].bundle_id
            assert bundles[0]["ready_for_analysis"] is True
            assert await _candidate_bundle_members(session, bundle_id) == [
                {
                    "artifact_id": github_artifact_id,
                    "snapshot_id": github_snapshot_id,
                    "member_role": "primary",
                    "member_order": 0,
                },
                {
                    "artifact_id": ids.x_artifact_id,
                    "snapshot_id": x_snapshot_id,
                    "member_role": "supporting",
                    "member_order": 0,
                },
            ]
            bundle_details = await _candidate_bundle_details(session, bundle_id)
            assert bundle_details == {
                "bundle_id": bundle_id,
                "candidate_group_id": ids.candidate_group_id,
                "initial_primary_artifact_id": ids.x_artifact_id,
                "current_primary_artifact_id": github_artifact_id,
                "reroot_count": 1,
                "ready_for_analysis": True,
            }

            analysis_requested_events = await _events(
                session,
                event_type="analysis.requested.v1",
                aggregate_id=ids.candidate_group_id,
            )
            assert len(analysis_requested_events) == 1
            assert analysis_requested_events[0]["payload_json"] == {
                "candidate_group_id": str(ids.candidate_group_id),
                "bundle_id": str(bundle_id),
                "judge_profile": "github_primary",
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
                "candidate_group_id": str(ids.candidate_group_id),
                "bundle_id": str(bundle_id),
                "judge_profile": "github_primary",
                "model": "gpt-5.4-mini",
                "reasoning_effort": "low",
                "prompt_version": "judge_github_primary_v1",
                "prompt_cache_key": "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1",
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
                "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1"
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

            assert await _notifier_owned_counts(session, ids.candidate_group_id) == {
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

            before_notifier_counts = await _notifier_owned_counts(session, ids.candidate_group_id)
            before_notifier_snapshot = await _upstream_boundary_snapshot(
                session,
                source_message_id=ids.source_message_id,
                artifact_ids=[ids.x_artifact_id, github_artifact_id],
                candidate_group_id=ids.candidate_group_id,
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
                NotifierStreamMessage(stream=notify_route.queue_name, message_id="1-0", fields=notify_fields)
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
                artifact_ids=[ids.x_artifact_id, github_artifact_id],
                candidate_group_id=ids.candidate_group_id,
                bundle_id=bundle_id,
                judge_output_id=judge_output_id,
                analysis_id=analysis_id,
            ) == before_notifier_snapshot

            after_notifier_counts = await _notifier_owned_counts(session, ids.candidate_group_id)
            assert after_notifier_counts == {
                "notification_plans": 1,
                "notification_renders": 1,
                "notification_delivery_records": 1,
            }

            notification_rows = await _notification_rows(session, ids.candidate_group_id)
            assert len(notification_rows["plans"]) == 1
            plan = notification_rows["plans"][0]
            notification_plan_id = plan["notification_plan_id"]
            assert plan["analysis_id"] == analysis_id
            assert plan["candidate_group_id"] == ids.candidate_group_id
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
                NotifierStreamMessage(stream=notify_route.queue_name, message_id="2-0", fields=notify_fields)
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
            assert await _notifier_owned_counts(session, ids.candidate_group_id) == after_notifier_counts
            replay_rows = await _notification_rows(session, ids.candidate_group_id)
            assert sorted(replay_rows["state_transition_reason_codes"]) == [
                "dry_run_skip_transport",
                "notification_duplicate_terminal_noop",
                "notification_rendered",
            ]
            assert replay_rows["delivery_result_events"] == notification_rows["delivery_result_events"]
            assert await _upstream_boundary_snapshot(
                session,
                source_message_id=ids.source_message_id,
                artifact_ids=[ids.x_artifact_id, github_artifact_id],
                candidate_group_id=ids.candidate_group_id,
                bundle_id=bundle_id,
                judge_output_id=judge_output_id,
                analysis_id=analysis_id,
            ) == before_notifier_snapshot
    finally:
        await engine.dispose()


async def _seed_x_discovered_github_promotion_case(session: AsyncSession) -> SeedPromotionIds:
    source_message_id = uuid4()
    candidate_group_id = uuid4()
    x_artifact_id = uuid4()
    x_event_id = uuid4()
    suffix = uuid4().hex
    now = datetime.now(timezone.utc)
    x_post_id = str(1_890_000_000_000_000_000 + int(suffix[:10], 16))
    owner = "example"
    repo = f"x-discovered-promotion-{suffix[:12]}"
    repo_full_name = f"{owner}/{repo}"
    github_canonical_id = f"github:repo:{repo_full_name}"
    github_canonical_url = f"https://github.com/{repo_full_name}"
    x_canonical_id = f"x:post:{x_post_id}"
    x_canonical_url = f"https://x.com/rerootdev/status/{x_post_id}"
    source_text = (
        "X primary with later-discovered GitHub repo for promotion reroot E2E: "
        f"{x_canonical_url}"
    )
    raw_message_json = {
        "local_test": True,
        "content": {"@type": "messageText", "text": {"text": source_text}},
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
            "chat_id": 9700000000 + int(suffix[:8], 16),
            "message_id": int(suffix[8:16], 16),
            "logical_post_key": f"db-x-discovered-github-promotion-e2e:{suffix}",
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
            INSERT INTO artifact_registry (
                artifact_id, artifact_type, canonical_id, canonical_url,
                normalized_host, artifact_key_json
            ) VALUES
              (
                CAST(:x_artifact_id AS uuid), 'x_post'::artifact_type_enum,
                :x_canonical_id, :x_canonical_url, 'x.com',
                CAST(:x_artifact_key_json AS jsonb)
              )
            """
        ),
        {
            "x_artifact_id": str(x_artifact_id),
            "x_canonical_id": x_canonical_id,
            "x_canonical_url": x_canonical_url,
            "x_artifact_key_json": _jsonb({"author": "rerootdev", "post_id": x_post_id}),
        },
    )
    for artifact_id, observed_url, source_kind, canonical_url, classification, context_path in [
        (x_artifact_id, x_canonical_url, "regex", x_canonical_url, "x_post", "urls[0]"),
    ]:
        await session.execute(
            sa.text(
                """
                INSERT INTO artifact_observations (
                    artifact_id, source_message_id, source_version_no, observed_url,
                    source_kind, normalized_url, resolved_url, canonical_url,
                    classification, context_path
                ) VALUES (
                    CAST(:artifact_id AS uuid), CAST(:source_message_id AS uuid), 1,
                    :observed_url, :source_kind, :observed_url, :observed_url,
                    :canonical_url, :classification, :context_path
                )
                """
            ),
            {
                "artifact_id": str(artifact_id),
                "source_message_id": str(source_message_id),
                "observed_url": observed_url,
                "source_kind": source_kind,
                "canonical_url": canonical_url,
                "classification": classification,
                "context_path": context_path,
            },
        )
    await session.execute(
        sa.text(
            """
            INSERT INTO candidate_group_proposals (
                candidate_group_id, source_message_id, source_version_no,
                initial_primary_artifact_id, current_primary_artifact_id,
                proposal_status, normalizer_version, dedupe_subject_key
            ) VALUES (
                CAST(:candidate_group_id AS uuid), CAST(:source_message_id AS uuid), 1,
                CAST(:x_artifact_id AS uuid), CAST(:x_artifact_id AS uuid),
                'ready_for_enrich', :normalizer_version, :dedupe_subject_key
            )
            """
        ),
        {
            "candidate_group_id": str(candidate_group_id),
            "source_message_id": str(source_message_id),
            "x_artifact_id": str(x_artifact_id),
            "normalizer_version": "db-x-discovered-github-promotion-e2e-test-v1",
            "dedupe_subject_key": f"db-x-discovered-github-promotion-e2e:{suffix}",
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO candidate_group_members (
                candidate_group_id, artifact_id, member_role, member_order
            ) VALUES
              (CAST(:candidate_group_id AS uuid), CAST(:x_artifact_id AS uuid), 'primary', 0)
            """
        ),
        {
            "candidate_group_id": str(candidate_group_id),
            "x_artifact_id": str(x_artifact_id),
        },
    )
    await _insert_enrich_requested_event(
        session,
        event_id=x_event_id,
        artifact_id=x_artifact_id,
        candidate_group_id=candidate_group_id,
        source_message_id=source_message_id,
        artifact_type="x_post",
        canonical_id=x_canonical_id,
        provider_route="x",
        dedupe_key=f"db-x-discovered-github-promotion-e2e:{suffix}:x.enrich.requested",
    )
    return SeedPromotionIds(
        source_message_id=source_message_id,
        candidate_group_id=candidate_group_id,
        x_artifact_id=x_artifact_id,
        x_enrich_requested_event_id=x_event_id,
        x_post_id=x_post_id,
        x_canonical_id=x_canonical_id,
        x_canonical_url=x_canonical_url,
        github_canonical_id=github_canonical_id,
        github_canonical_url=github_canonical_url,
        repo_full_name=repo_full_name,
    )


async def _insert_enrich_requested_event(
    session: AsyncSession,
    *,
    event_id: UUID,
    artifact_id: UUID,
    candidate_group_id: UUID,
    source_message_id: UUID,
    artifact_type: str,
    canonical_id: str,
    provider_route: str,
    dedupe_key: str,
) -> None:
    await session.execute(
        sa.text(
            """
            INSERT INTO event_outbox (
                event_id, event_type, aggregate_type, aggregate_id, dedupe_key,
                payload_json, status, created_at
            ) VALUES (
                CAST(:event_id AS uuid), 'artifact.enrich.requested.v1', 'artifact',
                CAST(:artifact_id AS uuid), :dedupe_key,
                CAST(:payload_json AS jsonb), 'pending'::outbox_status_enum, now()
            )
            """
        ),
        {
            "event_id": str(event_id),
            "artifact_id": str(artifact_id),
            "dedupe_key": dedupe_key,
            "payload_json": _jsonb(
                {
                    "candidate_group_id": str(candidate_group_id),
                    "artifact_id": str(artifact_id),
                    "artifact_type": artifact_type,
                    "canonical_id": canonical_id,
                    "provider_route": provider_route,
                    "refresh_mode": "standard",
                    "depth_budget": 1,
                    "source_message_id": str(source_message_id),
                    "source_version_no": 1,
                }
            ),
        },
    )


async def _artifact_by_canonical_id(session: AsyncSession, canonical_id: str) -> dict[str, Any] | None:
    result = await session.execute(
        sa.text(
            """
            SELECT artifact_id, artifact_type, canonical_id, canonical_url,
                   normalized_host, current_snapshot_id, current_status
            FROM artifact_registry
            WHERE canonical_id = :canonical_id
            """
        ),
        {"canonical_id": canonical_id},
    )
    row = result.mappings().first()
    return _row_dict(row) if row is not None else None


async def _candidate_group_current_primary(session: AsyncSession, candidate_group_id: UUID) -> UUID:
    result = await session.execute(
        sa.text(
            """
            SELECT current_primary_artifact_id
            FROM candidate_group_proposals
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    )
    return UUID(str(result.scalar_one()))


async def _candidate_reroot_events(session: AsyncSession, candidate_group_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT from_artifact_id, to_artifact_id, reason_code, trigger_snapshot_id
            FROM candidate_reroot_events
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            ORDER BY created_at, candidate_reroot_event_id
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _candidate_bundle_details(session: AsyncSession, bundle_id: UUID) -> dict[str, Any]:
    result = await session.execute(
        sa.text(
            """
            SELECT bundle_id, candidate_group_id, initial_primary_artifact_id,
                   current_primary_artifact_id, reroot_count, ready_for_analysis
            FROM candidate_evidence_bundles
            WHERE bundle_id = CAST(:bundle_id AS uuid)
            """
        ),
        {"bundle_id": str(bundle_id)},
    )
    return _row_dict(result.mappings().one())


async def _upstream_boundary_snapshot(
    session: AsyncSession,
    *,
    source_message_id: UUID,
    artifact_ids: list[UUID],
    candidate_group_id: UUID,
    bundle_id: UUID,
    judge_output_id: UUID,
    analysis_id: UUID,
) -> UpstreamBoundarySnapshot:
    return UpstreamBoundarySnapshot(
        source_messages=await _rows(
            session,
            """
            SELECT source_message_id, current_version_no, content_type, text_surface
            FROM source_messages
            WHERE source_message_id = CAST(:source_message_id AS uuid)
            """,
            {"source_message_id": str(source_message_id)},
        ),
        artifact_registry=await _rows(
            session,
            """
            SELECT artifact_id, artifact_type, canonical_id, canonical_url,
                   normalized_host, current_snapshot_id, current_status
            FROM artifact_registry
            WHERE artifact_id = ANY(CAST(:artifact_ids AS uuid[]))
            ORDER BY artifact_id
            """,
            {"artifact_ids": [str(artifact_id) for artifact_id in artifact_ids]},
        ),
        candidate_group_members=await _candidate_group_members(session, candidate_group_id),
        candidate_evidence_bundles=await _rows(
            session,
            """
            SELECT bundle_id, candidate_group_id, initial_primary_artifact_id,
                   current_primary_artifact_id, ready_for_analysis, token_budget_profile
            FROM candidate_evidence_bundles
            WHERE bundle_id = CAST(:bundle_id AS uuid)
            """,
            {"bundle_id": str(bundle_id)},
        ),
        judge_outputs=await _rows(
            session,
            """
            SELECT judge_output_id, judge_run_id, candidate_group_id,
                   model_proposed_verdict, model_confidence_band, payload_json
            FROM judge_outputs
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
            """,
            {"judge_output_id": str(judge_output_id)},
        ),
        analyses=await _rows(
            session,
            """
            SELECT analysis_id, candidate_group_id, judge_output_id, verdict,
                   delivery_decision, reason_codes_json, policy_reconciled_flag
            FROM analyses
            WHERE analysis_id = CAST(:analysis_id AS uuid)
            """,
            {"analysis_id": str(analysis_id)},
        ),
    )


async def _rows(session: AsyncSession, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    result = await session.execute(sa.text(sql), params)
    return [_row_dict(row) for row in result.mappings().all()]


def _row_dict(row: Any) -> dict[str, Any]:
    converted = dict(row)
    for key, value in list(converted.items()):
        if key.endswith("_json") or key == "payload_json" or key == "telegram_response_json":
            converted[key] = _json_obj(value)
    return converted
