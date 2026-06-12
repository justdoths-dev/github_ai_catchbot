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
from services.gh_enricher.redis_streams import StreamMessage
from services.gh_enricher.repositories import GhEnricherRepository
from services.gh_enricher.service import GhEnricherService
from services.gh_enricher.url_discovery import GitHubUrlDiscovery
from services.gh_enricher.worker import GhEnricherWorker
from services.judge_openai.repositories import JudgeOpenAIRepository
from services.judge_openai.service import JudgeOpenAIService
from services.outbox_relay.repositories import OutboxRelayRepository
from services.outbox_relay.routing import OutboxRouteResolver
from services.outbox_relay.service import OutboxRelayService
from services.policy_engine.repositories import PolicyEngineRepository
from services.policy_engine.service import PolicyEngineService
from services.router_normalizer.config import RouterNormalizerConfig
from services.router_normalizer.models import RedisNormalizeMessage
from services.router_normalizer.repositories import RouterNormalizerRepository
from services.router_normalizer.service import RouterNormalizerService
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
    RecordingRedisConsumer,
    _artifact_current_snapshot,
    _artifact_enrichment_runs,
    _artifact_snapshots_for_artifact,
    _candidate_group_members,
    _discovered_url_observations,
    _gh_config,
    _github_file_samples,
    _github_repo_rows,
)


pytestmark = pytest.mark.skipif(
    not os.environ.get("LOCAL_TEST_DATABASE_URL"),
    reason="LOCAL_TEST_DATABASE_URL is required for the DB-backed source-message upstream e2e test",
)


@dataclass(frozen=True, slots=True)
class SeedIds:
    source_message_id: UUID
    source_message_created_event_id: UUID
    repo_full_name: str
    canonical_id: str
    canonical_url: str


@pytest.mark.asyncio
async def test_db_backed_source_message_created_routes_to_github_notification_queue() -> None:
    database_url = _local_test_database_url()
    engine = create_async_engine(database_url, future=True)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            ids = await _seed_source_message_created_case(session)
            await session.commit()

            assert await _source_message_version_count(session, ids.source_message_id) == 1
            source_events = await _events(
                session,
                event_type="source_message.created.v1",
                aggregate_id=ids.source_message_id,
            )
            assert len(source_events) == 1
            assert source_events[0]["event_id"] == ids.source_message_created_event_id
            assert source_events[0]["status"] == "pending"
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
                "job_id": str(ids.source_message_created_event_id),
                "stage_name": "normalize",
                "root_object_type": "source_message",
                "root_object_id": str(ids.source_message_id),
                "idempotency_key": source_events[0]["dedupe_key"],
                "pipeline_run_id": "",
                "not_before": "",
                "trigger_event_id": str(ids.source_message_created_event_id),
            }
            assert set(source_fields) == THIN_REDIS_FIELDS
            assert "payload_json" not in source_fields

            normalizer = RouterNormalizerService(
                _normalizer_config(database_url),
                repository=RouterNormalizerRepository(session),
            )
            normalization = await normalizer.process_stream_message(
                RedisNormalizeMessage.from_stream_fields(source_fields)
            )
            await session.commit()

            assert normalization.signal_detected is True
            assert normalization.candidate_eligible is True
            assert normalization.trigger_strength == "strong"
            assert normalization.artifact_count == 1
            assert normalization.candidate_group_count == 1

            normalization_runs = await _normalization_runs_for_source(session, ids.source_message_id)
            assert len(normalization_runs) == 1
            assert normalization_runs[0]["source_version_no"] == 1
            assert normalization_runs[0]["signal_detected"] is True
            assert normalization_runs[0]["candidate_eligible"] is True
            assert normalization_runs[0]["trigger_strength"] == "strong"

            artifacts = await _github_artifacts_for_source(session, ids.source_message_id)
            assert len(artifacts) == 1
            artifact = artifacts[0]
            artifact_id = artifact["artifact_id"]
            assert artifact["artifact_type"] == "github_repo"
            assert artifact["canonical_id"] == ids.canonical_id
            assert artifact["canonical_url"] == ids.canonical_url
            assert artifact["normalized_host"] == "github.com"

            observations = await _artifact_observations_for_source(session, ids.source_message_id)
            assert len(observations) == 1
            assert observations[0]["artifact_id"] == artifact_id
            assert observations[0]["observed_url"] == ids.canonical_url
            assert observations[0]["source_kind"] == "regex"

            candidate_groups = await _candidate_groups_for_source(session, ids.source_message_id)
            assert len(candidate_groups) == 1
            candidate_group_id = candidate_groups[0]["candidate_group_id"]
            fake_openai = RecordingOpenAIClient(candidate_group_id=candidate_group_id)
            assert candidate_groups[0]["source_version_no"] == 1
            assert candidate_groups[0]["initial_primary_artifact_id"] == artifact_id
            assert candidate_groups[0]["current_primary_artifact_id"] == artifact_id
            assert candidate_groups[0]["dedupe_subject_key"] == ids.canonical_id
            assert await _candidate_group_members(session, candidate_group_id) == [
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
                "artifact_type": "github_repo",
                "canonical_id": ids.canonical_id,
                "provider_route": "github",
                "refresh_mode": "standard",
                "depth_budget": 1,
                "source_message_id": str(ids.source_message_id),
                "source_version_no": 1,
            }
            assert enrich_events[0]["payload_json"]["provider_route"] == "github"
            await _move_event_to_front(session, enrich_events[0]["event_id"])
            await session.commit()

            artifact_count_after_router = await _count(session, "SELECT count(*) FROM artifact_registry", {})
            candidate_members_before = await _candidate_group_members(session, candidate_group_id)

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
            assert enrich_route.queue_name == "q.artifact.enrich.github"
            assert enrich_route.stage_name == "enrich_github"
            enrich_fields = enrich_message.as_stream_fields()
            assert enrich_fields == {
                "job_id": str(enrich_events[0]["event_id"]),
                "stage_name": "enrich_github",
                "root_object_type": "artifact",
                "root_object_id": str(artifact_id),
                "idempotency_key": enrich_events[0]["dedupe_key"],
                "pipeline_run_id": "",
                "not_before": "",
                "trigger_event_id": str(enrich_events[0]["event_id"]),
            }
            assert set(enrich_fields) == THIN_REDIS_FIELDS
            assert "payload_json" not in enrich_fields

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
            gh_consumer = RecordingRedisConsumer(
                StreamMessage(
                    stream=enrich_route.queue_name,
                    message_id="1-0",
                    fields=enrich_fields,
                )
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

            enrichment_runs = await _artifact_enrichment_runs(session, artifact_id)
            assert len(enrichment_runs) == 1
            assert enrichment_runs[0]["provider"] == "github"
            assert enrichment_runs[0]["status"] in {"ready", "partial_ready"}

            snapshots = await _artifact_snapshots_for_artifact(session, artifact_id)
            assert len(snapshots) == 1
            snapshot_id = snapshots[0]["snapshot_id"]
            assert snapshots[0]["content_anchor"] == "commit:abc123def456"
            assert await _artifact_current_snapshot(session, artifact_id) == {
                "current_snapshot_id": snapshot_id,
                "current_status": snapshots[0]["status"],
            }
            assert await _github_repo_rows(session, snapshot_id) == [
                {
                    "snapshot_id": snapshot_id,
                    "repo_full_name": ids.repo_full_name,
                    "default_branch": "main",
                    "resolved_ref": "abc123def456",
                    "content_anchor_commit_sha": "abc123def456",
                }
            ]
            assert len(await _github_file_samples(session, snapshot_id)) >= 1
            assert len(await _discovered_url_observations(session, snapshot_id)) >= 1
            assert await _count(session, "SELECT count(*) FROM artifact_registry", {}) == artifact_count_after_router
            assert await _candidate_group_members(session, candidate_group_id) == candidate_members_before
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
                "snapshot_id": str(snapshot_id),
                "provider": "github",
                "status": snapshots[0]["status"],
                "content_anchor": "commit:abc123def456",
            }
            assert "candidate_group_id" not in artifact_snapshot_events[0]["payload_json"]

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
            assert second_results[0].candidate_group_id == candidate_group_id
            assert second_results[0].bundle_id == first_results[0].bundle_id
            assert second_results[0].reused_existing_bundle is True
            assert second_results[0].ready_for_analysis is True
            assert second_results[0].emitted_analysis_requested is False

            bundles = await _candidate_bundles(session, candidate_group_id)
            assert len(bundles) == 1
            bundle_id = bundles[0]["bundle_id"]
            assert bundle_id == first_results[0].bundle_id
            assert bundles[0]["ready_for_analysis"] is True
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

            judge_openai = JudgeOpenAIService(
                _judge_openai_config(database_url),
                repository=JudgeOpenAIRepository(session),
                openai_client=fake_openai,
            )
            await judge_openai.handle_trigger_event(judge_call_events[0]["event_id"])
            await _mark_events_published(session, [judge_call_events[0]["event_id"]])
            await session.commit()

            assert len(fake_openai.calls) == 1
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
            assert analyses[0]["verdict"] == "inspect_now"
            assert analyses[0]["delivery_decision"] == "send_now"

            notification_events = await _events(
                session,
                event_type="notification.plan.created.v1",
                aggregate_id=analyses[0]["analysis_id"],
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
                "root_object_id": str(analyses[0]["analysis_id"]),
                "idempotency_key": notification_events[0]["dedupe_key"],
                "pipeline_run_id": "",
                "not_before": "",
                "trigger_event_id": str(notification_events[0]["event_id"]),
            }
            assert set(notify_fields) == THIN_REDIS_FIELDS
            assert "payload_json" not in notify_fields
            assert await _notifier_owned_counts(session, candidate_group_id) == {
                "notification_plans": 0,
                "notification_renders": 0,
                "notification_delivery_records": 0,
            }
    finally:
        await engine.dispose()


async def _seed_source_message_created_case(session: AsyncSession) -> SeedIds:
    source_message_id = uuid4()
    event_id = uuid4()
    suffix = uuid4().hex
    now = datetime.now(timezone.utc)
    owner = "example"
    repo = f"source-message-router-e2e-{suffix[:12]}"
    repo_full_name = f"{owner}/{repo}"
    canonical_id = f"github:repo:{repo_full_name}"
    canonical_url = f"https://github.com/{repo_full_name}"
    source_text = (
        "AI developer repository signal for source-message router E2E: "
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
            "chat_id": 9400000000 + int(suffix[:8], 16),
            "message_id": int(suffix[8:16], 16),
            "logical_post_key": f"db-source-message-created-e2e:{suffix}",
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
            "dedupe_key": f"db-source-message-created-e2e:{suffix}:source_message.created",
            "payload_json": _jsonb(
                {
                    "source_message_id": str(source_message_id),
                    "current_version_no": 1,
                }
            ),
        },
    )
    return SeedIds(
        source_message_id=source_message_id,
        source_message_created_event_id=event_id,
        repo_full_name=repo_full_name,
        canonical_id=canonical_id,
        canonical_url=canonical_url,
    )


def _normalizer_config(database_url: str) -> RouterNormalizerConfig:
    return RouterNormalizerConfig(
        app_env="test",
        database_url=database_url,
        redis_url="unused",
        queue_name="q.source.normalize",
        consumer_group="router-normalizer",
        consumer_name="test",
        block_ms=100,
        batch_size=20,
        normalizer_version="db-source-message-created-e2e-test-v1",
        short_url_allowlist=(),
        short_url_hop_limit=1,
        short_url_timeout_seconds=0.1,
        log_level="INFO",
    )


async def _source_message_version_count(session: AsyncSession, source_message_id: UUID) -> int:
    return await _count(
        session,
        """
        SELECT count(*)
        FROM source_message_versions
        WHERE source_message_id = CAST(:source_message_id AS uuid)
        """,
        {"source_message_id": str(source_message_id)},
    )


async def _normalization_runs_for_source(session: AsyncSession, source_message_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT normalization_run_id, source_message_id, source_version_no,
                   normalizer_version, signal_detected, candidate_eligible,
                   trigger_strength, result_hash
            FROM normalization_runs
            WHERE source_message_id = CAST(:source_message_id AS uuid)
            ORDER BY completed_at, normalization_run_id
            """
        ),
        {"source_message_id": str(source_message_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _github_artifacts_for_source(session: AsyncSession, source_message_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT DISTINCT ar.artifact_id, ar.artifact_type, ar.canonical_id,
                            ar.canonical_url, ar.normalized_host, ar.artifact_key_json
            FROM artifact_registry ar
            JOIN artifact_observations ao
              ON ao.artifact_id = ar.artifact_id
            WHERE ao.source_message_id = CAST(:source_message_id AS uuid)
              AND ar.artifact_type = 'github_repo'::artifact_type_enum
            ORDER BY ar.canonical_id
            """
        ),
        {"source_message_id": str(source_message_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _artifact_observations_for_source(session: AsyncSession, source_message_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT artifact_id, source_message_id, source_version_no, observed_url,
                   source_kind, normalized_url, resolved_url, canonical_url,
                   classification, context_path
            FROM artifact_observations
            WHERE source_message_id = CAST(:source_message_id AS uuid)
            ORDER BY created_at, artifact_observation_id
            """
        ),
        {"source_message_id": str(source_message_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _candidate_groups_for_source(session: AsyncSession, source_message_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT candidate_group_id, source_message_id, source_version_no,
                   initial_primary_artifact_id, current_primary_artifact_id,
                   proposal_status, normalizer_version, dedupe_subject_key,
                   current_bundle_id, current_analysis_id
            FROM candidate_group_proposals
            WHERE source_message_id = CAST(:source_message_id AS uuid)
            ORDER BY created_at, candidate_group_id
            """
        ),
        {"source_message_id": str(source_message_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _router_downstream_owned_counts(session: AsyncSession, candidate_group_id: UUID) -> dict[str, int]:
    candidate_evidence_bundles = await _count(
        session,
        """
        SELECT count(*)
        FROM candidate_evidence_bundles
        WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
        """,
        {"candidate_group_id": str(candidate_group_id)},
    )
    judge_runs = await _count(
        session,
        """
        SELECT count(*)
        FROM judge_runs jr
        JOIN candidate_evidence_bundles ceb
          ON ceb.bundle_id = jr.bundle_id
        WHERE ceb.candidate_group_id = CAST(:candidate_group_id AS uuid)
        """,
        {"candidate_group_id": str(candidate_group_id)},
    )
    judge_outputs = await _count(
        session,
        """
        SELECT count(*)
        FROM judge_outputs
        WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
        """,
        {"candidate_group_id": str(candidate_group_id)},
    )
    analyses = await _count(
        session,
        """
        SELECT count(*)
        FROM analyses
        WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
        """,
        {"candidate_group_id": str(candidate_group_id)},
    )
    notification_counts = await _notifier_owned_counts(session, candidate_group_id)
    return {
        "candidate_evidence_bundles": candidate_evidence_bundles,
        "judge_runs": judge_runs,
        "judge_outputs": judge_outputs,
        "analyses": analyses,
        **notification_counts,
    }


def _row_dict(row: Any) -> dict[str, Any]:
    converted = dict(row)
    for key, value in list(converted.items()):
        if key.endswith("_json") or key == "payload_json":
            converted[key] = _json_obj(value)
    return converted
