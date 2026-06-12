from __future__ import annotations

import base64
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
from services.gh_enricher.config import GhEnricherConfig
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


pytestmark = pytest.mark.skipif(
    not os.environ.get("LOCAL_TEST_DATABASE_URL"),
    reason="LOCAL_TEST_DATABASE_URL is required for the DB-backed GitHub enrich upstream e2e test",
)


@dataclass(frozen=True, slots=True)
class SeedIds:
    source_message_id: UUID
    artifact_id: UUID
    candidate_group_id: UUID
    artifact_enrich_requested_event_id: UUID
    canonical_id: str
    repo_full_name: str


class RecordingGitHubClient:
    def __init__(self, *, owner: str, repo: str) -> None:
        self._owner = owner
        self._repo = repo
        self.calls: list[str] = []
        self._contents = {
            "README.md": (
                "# Local DB GitHub enrich E2E\n"
                "Offline README fixture with https://docs.example.test/sdk and "
                "https://ci.example.test/build links.\n"
            ),
            "pyproject.toml": (
                "[project]\n"
                'name = "local-db-github-enrich-e2e"\n'
                'homepage = "https://package.example.test/local-db-github-enrich-e2e"\n'
            ),
            ".github/workflows/ci.yml": "name: ci\non: [push]\n",
            "tests/test_feature.py": "def test_feature():\n    assert True\n",
        }

    async def get_repo(self, owner: str, repo: str, *, auth_mode: str) -> dict[str, Any]:
        self.calls.append("repo")
        assert owner == self._owner
        assert repo == self._repo
        assert auth_mode == "anonymous_degraded"
        return {
            "full_name": f"{owner}/{repo}",
            "default_branch": "main",
            "description": "Offline component-test repository fixture",
            "homepage": "https://repo-home.example.test/local-db-github-enrich-e2e",
            "license": {"spdx_id": "MIT"},
            "topics": ["ai", "automation"],
            "language": "Python",
            "stargazers_count": 144,
            "subscribers_count": 17,
            "forks_count": 4,
            "open_issues_count": 2,
            "archived": False,
            "fork": False,
            "is_template": False,
            "pushed_at": "2026-06-01T00:00:00Z",
        }

    async def get_default_branch_head(
        self,
        owner: str,
        repo: str,
        default_branch: str,
        *,
        auth_mode: str,
    ) -> dict[str, Any]:
        self.calls.append("head")
        assert owner == self._owner
        assert repo == self._repo
        assert default_branch == "main"
        assert auth_mode == "anonymous_degraded"
        return {"sha": "abc123def456"}

    async def get_tree(
        self,
        owner: str,
        repo: str,
        ref: str,
        *,
        recursive: bool,
        auth_mode: str,
    ) -> dict[str, Any]:
        self.calls.append("tree")
        assert owner == self._owner
        assert repo == self._repo
        assert ref == "abc123def456"
        assert recursive is True
        assert auth_mode == "anonymous_degraded"
        return {
            "truncated": False,
            "tree": [
                {"type": "blob", "path": "README.md"},
                {"type": "blob", "path": "pyproject.toml"},
                {"type": "blob", "path": ".github/workflows/ci.yml"},
                {"type": "blob", "path": "tests/test_feature.py"},
            ],
        }

    async def get_contents(
        self,
        owner: str,
        repo: str,
        path: str,
        *,
        ref: str,
        auth_mode: str,
    ) -> dict[str, Any]:
        self.calls.append(f"contents:{path}")
        assert owner == self._owner
        assert repo == self._repo
        assert ref == "abc123def456"
        assert auth_mode == "anonymous_degraded"
        text = self._contents[path]
        return {
            "encoding": "base64",
            "content": base64.b64encode(text.encode("utf-8")).decode("ascii"),
            "size": len(text.encode("utf-8")),
        }

    async def get_releases(self, owner: str, repo: str, *, auth_mode: str) -> list[dict[str, Any]]:
        self.calls.append("releases")
        assert owner == self._owner
        assert repo == self._repo
        assert auth_mode == "anonymous_degraded"
        return [
            {
                "published_at": "2026-05-20T00:00:00Z",
                "assets": [{"download_count": 11}],
                "prerelease": False,
            }
        ]


class RecordingRedisConsumer:
    def __init__(self, message: StreamMessage) -> None:
        self._messages = [message]
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        pass

    async def read_batch(self) -> list[StreamMessage]:
        messages = self._messages
        self._messages = []
        return messages

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


@pytest.mark.asyncio
async def test_db_backed_github_enrich_requested_routes_to_notification_queue_with_fake_boundaries() -> None:
    database_url = _local_test_database_url()
    engine = create_async_engine(database_url, future=True)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            ids = await _seed_github_enrich_requested_case(session)
            await session.commit()

            artifact_count_after_seed = await _count(session, "SELECT count(*) FROM artifact_registry", {})
            candidate_members_before = await _candidate_group_members(session, ids.candidate_group_id)
            assert len(candidate_members_before) == 1
            assert await _count(
                session,
                """
                SELECT count(*)
                FROM artifact_snapshots
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                """,
                {"artifact_id": str(ids.artifact_id)},
            ) == 0

            enrich_events = await _events(
                session,
                event_type="artifact.enrich.requested.v1",
                aggregate_id=ids.artifact_id,
            )
            assert len(enrich_events) == 1
            assert enrich_events[0]["event_id"] == ids.artifact_enrich_requested_event_id
            assert enrich_events[0]["payload_json"] == {
                "candidate_group_id": str(ids.candidate_group_id),
                "artifact_id": str(ids.artifact_id),
                "artifact_type": "github_repo",
                "provider_route": "github",
                "refresh_mode": "standard",
                "depth_budget": 1,
            }

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
                "job_id": str(ids.artifact_enrich_requested_event_id),
                "stage_name": "enrich_github",
                "root_object_type": "artifact",
                "root_object_id": str(ids.artifact_id),
                "idempotency_key": enrich_events[0]["dedupe_key"],
                "pipeline_run_id": "",
                "not_before": "",
                "trigger_event_id": str(ids.artifact_enrich_requested_event_id),
            }
            assert set(enrich_fields) == THIN_REDIS_FIELDS
            assert "payload_json" not in enrich_fields

            owner, repo = ids.repo_full_name.split("/", 1)
            fake_github = RecordingGitHubClient(owner=owner, repo=repo)
            fake_openai = RecordingOpenAIClient(candidate_group_id=ids.candidate_group_id)
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

            enrichment_runs = await _artifact_enrichment_runs(session, ids.artifact_id)
            assert len(enrichment_runs) == 1
            assert enrichment_runs[0]["provider"] == "github"
            assert enrichment_runs[0]["status"] in {"ready", "partial_ready"}

            snapshots = await _artifact_snapshots_for_artifact(session, ids.artifact_id)
            assert len(snapshots) == 1
            snapshot_id = snapshots[0]["snapshot_id"]
            assert snapshots[0]["status"] in {"ready", "partial_ready"}
            assert snapshots[0]["content_anchor"] == "commit:abc123def456"

            current_artifact = await _artifact_current_snapshot(session, ids.artifact_id)
            assert current_artifact == {
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
            assert await _count(session, "SELECT count(*) FROM artifact_registry", {}) == artifact_count_after_seed
            assert await _candidate_group_members(session, ids.candidate_group_id) == candidate_members_before
            assert await _events(
                session,
                event_type="analysis.requested.v1",
                aggregate_id=ids.candidate_group_id,
            ) == []

            artifact_snapshot_events = await _events(
                session,
                event_type="artifact.snapshot.updated.v1",
                aggregate_id=ids.artifact_id,
            )
            assert len(artifact_snapshot_events) == 1
            assert artifact_snapshot_events[0]["payload_json"] == {
                "artifact_id": str(ids.artifact_id),
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

            bundles = await _candidate_bundles(session, ids.candidate_group_id)
            assert len(bundles) == 1
            bundle_id = bundles[0]["bundle_id"]
            assert bundle_id == first_results[0].bundle_id
            assert bundles[0]["ready_for_analysis"] is True
            assert await _candidate_bundle_members(session, bundle_id) == [
                {
                    "artifact_id": ids.artifact_id,
                    "snapshot_id": snapshot_id,
                    "member_role": "primary",
                    "member_order": 0,
                }
            ]

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
            judge_outputs = await _judge_outputs_for_run(session, judge_run_id)
            assert len(judge_outputs) == 1
            judge_output_id = judge_outputs[0]["judge_output_id"]

            judge_output_ready_events = await _events(
                session,
                event_type="judge.output.ready.v1",
                aggregate_id=judge_run_id,
            )
            assert len(judge_output_ready_events) == 1
            assert judge_output_ready_events[0]["payload_json"] == {
                "judge_run_id": str(judge_run_id),
                "judge_output_id": str(judge_output_id),
                "finish_reason": "completed",
                "refusal_detected": False,
            }

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
            assert policy_events[0]["payload_json"] == {
                "judge_run_id": str(judge_run_id),
                "judge_output_id": str(judge_output_id),
                "candidate_group_id": str(ids.candidate_group_id),
                "bundle_id": str(bundle_id),
            }

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
                "root_object_id": str(analyses[0]["analysis_id"]),
                "idempotency_key": notification_events[0]["dedupe_key"],
                "pipeline_run_id": "",
                "not_before": "",
                "trigger_event_id": str(notification_events[0]["event_id"]),
            }
            assert set(notify_fields) == THIN_REDIS_FIELDS
            assert "payload_json" not in notify_fields
            assert await _notifier_owned_counts(session, ids.candidate_group_id) == {
                "notification_plans": 0,
                "notification_renders": 0,
                "notification_delivery_records": 0,
            }
    finally:
        await engine.dispose()


async def _seed_github_enrich_requested_case(session: AsyncSession) -> SeedIds:
    source_message_id = uuid4()
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    artifact_enrich_requested_event_id = uuid4()
    suffix = uuid4().hex
    now = datetime.now(timezone.utc)
    owner = "example"
    repo = f"github-enrich-e2e-{suffix[:12]}"
    repo_full_name = f"{owner}/{repo}"
    canonical_id = f"github:local-db-github-enrich-requested-e2e:{suffix}"
    canonical_url = f"https://github.com/{repo_full_name}"

    await session.execute(
        sa.text(
            """
            INSERT INTO source_messages (
                source_message_id, chat_id, message_id, logical_post_key,
                is_channel_post, posted_at, content_type, text_body, text_surface,
                raw_message_json
            ) VALUES (
                CAST(:source_message_id AS uuid), :chat_id, :message_id, :logical_post_key,
                true, :posted_at, 'text', :text_body, :text_body,
                CAST(:raw_message_json AS jsonb)
            )
            """
        ),
        {
            "source_message_id": str(source_message_id),
            "chat_id": 9300000000 + int(suffix[:8], 16),
            "message_id": int(suffix[8:16], 16),
            "logical_post_key": f"db-github-enrich-requested-e2e:{suffix}",
            "posted_at": now,
            "text_body": "Repository signal for local DB GitHub enrich requested notification queue test.",
            "raw_message_json": _jsonb({"local_test": True}),
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO artifact_registry (
                artifact_id, artifact_type, canonical_id, canonical_url,
                normalized_host, artifact_key_json
            ) VALUES (
                CAST(:artifact_id AS uuid), 'github_repo'::artifact_type_enum,
                :canonical_id, :canonical_url, 'github.com',
                CAST(:artifact_key_json AS jsonb)
            )
            """
        ),
        {
            "artifact_id": str(artifact_id),
            "canonical_id": canonical_id,
            "canonical_url": canonical_url,
            "artifact_key_json": _jsonb({"owner": owner, "repo": repo}),
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
                CAST(:artifact_id AS uuid), CAST(:artifact_id AS uuid),
                'ready_for_enrich', :normalizer_version, :dedupe_subject_key
            )
            """
        ),
        {
            "candidate_group_id": str(candidate_group_id),
            "source_message_id": str(source_message_id),
            "artifact_id": str(artifact_id),
            "normalizer_version": "db-github-enrich-requested-e2e-test-v1",
            "dedupe_subject_key": f"db-github-enrich-requested-e2e:{suffix}",
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO candidate_group_members (
                candidate_group_id, artifact_id, member_role, member_order
            ) VALUES (
                CAST(:candidate_group_id AS uuid), CAST(:artifact_id AS uuid), 'primary', 0
            )
            """
        ),
        {"candidate_group_id": str(candidate_group_id), "artifact_id": str(artifact_id)},
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO event_outbox (
                event_id, event_type, aggregate_type, aggregate_id, dedupe_key,
                payload_json, status, created_at
            ) VALUES (
                CAST(:event_id AS uuid), 'artifact.enrich.requested.v1', 'artifact',
                CAST(:artifact_id AS uuid), :dedupe_key,
                CAST(:payload_json AS jsonb), 'pending'::outbox_status_enum,
                TIMESTAMPTZ '0001-01-01 00:00:00+00'
            )
            """
        ),
        {
            "event_id": str(artifact_enrich_requested_event_id),
            "artifact_id": str(artifact_id),
            "dedupe_key": f"db-github-enrich-requested-e2e:{suffix}:artifact.enrich.requested",
            "payload_json": _jsonb(
                {
                    "candidate_group_id": str(candidate_group_id),
                    "artifact_id": str(artifact_id),
                    "artifact_type": "github_repo",
                    "provider_route": "github",
                    "refresh_mode": "standard",
                    "depth_budget": 1,
                }
            ),
        },
    )
    return SeedIds(
        source_message_id=source_message_id,
        artifact_id=artifact_id,
        candidate_group_id=candidate_group_id,
        artifact_enrich_requested_event_id=artifact_enrich_requested_event_id,
        canonical_id=canonical_id,
        repo_full_name=repo_full_name,
    )


async def _candidate_group_members(session: AsyncSession, candidate_group_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT candidate_group_id, artifact_id, member_role, member_order
            FROM candidate_group_members
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            ORDER BY member_role, member_order, artifact_id
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _artifact_enrichment_runs(session: AsyncSession, artifact_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT artifact_enrichment_run_id, artifact_id, provider, refresh_mode,
                   depth_budget, status, content_anchor, job_idempotency_key
            FROM artifact_enrichment_runs
            WHERE artifact_id = CAST(:artifact_id AS uuid)
              AND provider = 'github'
            ORDER BY requested_at, artifact_enrichment_run_id
            """
        ),
        {"artifact_id": str(artifact_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _artifact_snapshots_for_artifact(session: AsyncSession, artifact_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT snapshot_id, artifact_id, provider, snapshot_type, status, content_anchor
            FROM artifact_snapshots
            WHERE artifact_id = CAST(:artifact_id AS uuid)
            ORDER BY fetched_at, snapshot_id
            """
        ),
        {"artifact_id": str(artifact_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _artifact_current_snapshot(session: AsyncSession, artifact_id: UUID) -> dict[str, Any]:
    result = await session.execute(
        sa.text(
            """
            SELECT current_snapshot_id, current_status
            FROM artifact_registry
            WHERE artifact_id = CAST(:artifact_id AS uuid)
            """
        ),
        {"artifact_id": str(artifact_id)},
    )
    return _row_dict(result.mappings().one())


async def _github_repo_rows(session: AsyncSession, snapshot_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT snapshot_id, repo_full_name, default_branch, resolved_ref,
                   content_anchor_commit_sha
            FROM artifact_snapshot_github_repo
            WHERE snapshot_id = CAST(:snapshot_id AS uuid)
            ORDER BY snapshot_id
            """
        ),
        {"snapshot_id": str(snapshot_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _github_file_samples(session: AsyncSession, snapshot_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT snapshot_id, path, role, size_bytes, content_hash, excerpt
            FROM artifact_snapshot_github_file_samples
            WHERE snapshot_id = CAST(:snapshot_id AS uuid)
            ORDER BY role, path
            """
        ),
        {"snapshot_id": str(snapshot_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _discovered_url_observations(session: AsyncSession, snapshot_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT parent_candidate_group_id, parent_artifact_id, parent_snapshot_id,
                   observed_url, context_path, discovery_reason, depth_remaining
            FROM discovered_url_observations
            WHERE parent_snapshot_id = CAST(:snapshot_id AS uuid)
            ORDER BY observed_url, context_path
            """
        ),
        {"snapshot_id": str(snapshot_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


def _row_dict(row: Any) -> dict[str, Any]:
    converted = dict(row)
    if "payload_json" in converted:
        converted["payload_json"] = _json_obj(converted["payload_json"])
    return converted


def _gh_config(database_url: str) -> GhEnricherConfig:
    return GhEnricherConfig(
        app_env="test",
        database_url=database_url,
        redis_url="unused",
        queue_name="q.artifact.enrich.github",
        consumer_group="gh-enricher",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        github_api_base_url="https://api.github.invalid",
        github_app_id=None,
        github_installation_id=None,
        github_private_key=None,
        request_timeout_sec=1.0,
        sample_max_files=20,
        sample_excerpt_chars=1200,
        max_file_bytes=131072,
        stale_after_sec=21600,
        log_level="INFO",
    )
