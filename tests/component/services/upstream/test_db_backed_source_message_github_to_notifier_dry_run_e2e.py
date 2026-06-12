from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from uuid import UUID

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
from services.notifier_telegram.config import NotifierTelegramConfig
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
from tests.component.services.upstream.test_db_backed_artifact_snapshot_updated_to_notification_queue_e2e import (
    THIN_REDIS_FIELDS,
    RecordingOpenAIClient,
    RecordingRedisPublisher,
    _analyses_for_judge_output,
    _candidate_bundle_members,
    _candidate_bundles,
    _events,
    _evidence_config,
    _judge_openai_config,
    _judge_outputs_for_run,
    _judge_runs_for_bundle,
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
    _artifact_enrichment_runs,
    _artifact_snapshots_for_artifact,
    _candidate_group_members,
    _discovered_url_observations,
    _gh_config,
    _github_file_samples,
    _github_repo_rows,
)
from tests.component.services.upstream.test_db_backed_source_message_created_to_github_notification_queue_e2e import (
    RecordingNormalizeConsumer,
    _artifact_observations_for_source,
    _candidate_groups_for_source,
    _github_artifacts_for_source,
    _normalization_runs_for_source,
    _normalizer_config,
    _router_downstream_owned_counts,
    _seed_source_message_created_case,
    _source_message_version_count,
)


pytestmark = pytest.mark.skipif(
    not os.environ.get("LOCAL_TEST_DATABASE_URL"),
    reason="LOCAL_TEST_DATABASE_URL is required for the DB-backed source-message to notifier e2e test",
)


@dataclass(frozen=True, slots=True)
class UpstreamBoundarySnapshot:
    source_messages: list[dict[str, Any]]
    artifact_registry: list[dict[str, Any]]
    candidate_group_members: list[dict[str, Any]]
    candidate_evidence_bundles: list[dict[str, Any]]
    judge_outputs: list[dict[str, Any]]
    analyses: list[dict[str, Any]]


class RecordingNotifierConsumer:
    def __init__(self, message: NotifierStreamMessage) -> None:
        self._messages = [message]
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        pass

    async def read_batch(self) -> list[NotifierStreamMessage]:
        messages = self._messages
        self._messages = []
        return messages

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


class RaisingTelegramClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def send_message(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("send_message")
        raise AssertionError("dry-run notifier must not call Telegram send_message")

    async def edit_message_text(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("edit_message_text")
        raise AssertionError("dry-run notifier must not call Telegram edit_message_text")


@pytest.mark.asyncio
async def test_db_backed_source_message_github_path_creates_notifier_dry_run_records() -> None:
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

            source_publisher = RecordingRedisPublisher()
            source_relay = OutboxRelayService(
                _outbox_config(database_url),
                repository=OutboxRelayRepository(session),
                publisher=source_publisher,
                route_resolver=OutboxRouteResolver(),
            )
            assert await source_relay.run_once() == 1
            await session.commit()

            source_route, source_message = source_publisher.published[0]
            assert source_route.queue_name == "q.source.normalize"
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
            assert normalize_consumer.acked == ["1-0"]

            normalization_runs = await _normalization_runs_for_source(session, ids.source_message_id)
            assert len(normalization_runs) == 1
            artifacts = await _github_artifacts_for_source(session, ids.source_message_id)
            assert len(artifacts) == 1
            artifact_id = artifacts[0]["artifact_id"]
            assert artifacts[0]["canonical_id"] == ids.canonical_id
            assert await _artifact_observations_for_source(session, ids.source_message_id)

            candidate_groups = await _candidate_groups_for_source(session, ids.source_message_id)
            assert len(candidate_groups) == 1
            candidate_group_id = candidate_groups[0]["candidate_group_id"]
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
            await _move_event_to_front(session, enrich_events[0]["event_id"])
            await session.commit()

            enrich_publisher = RecordingRedisPublisher()
            enrich_relay = OutboxRelayService(
                _outbox_config(database_url),
                repository=OutboxRelayRepository(session),
                publisher=enrich_publisher,
                route_resolver=OutboxRouteResolver(),
            )
            assert await enrich_relay.run_once() == 1
            await session.commit()

            enrich_route, enrich_message = enrich_publisher.published[0]
            assert enrich_route.queue_name == "q.artifact.enrich.github"
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
            fake_openai = RecordingOpenAIClient(candidate_group_id=candidate_group_id)
            gh_service = GhEnricherService(
                _gh_config(database_url),
                repository=GhEnricherRepository(session),
                github_client=fake_github,
                fetch_planner=GitHubFetchPlanner(),
                file_sampler=GitHubFileSampler(),
                url_discovery=GitHubUrlDiscovery(),
            )
            gh_consumer = RecordingGhRedisConsumer(
                GhStreamMessage(
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
            snapshots = await _artifact_snapshots_for_artifact(session, artifact_id)
            assert len(snapshots) == 1
            snapshot_id = snapshots[0]["snapshot_id"]
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

            artifact_snapshot_events = await _events(
                session,
                event_type="artifact.snapshot.updated.v1",
                aggregate_id=artifact_id,
            )
            assert len(artifact_snapshot_events) == 1

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

            assert notification_rows["state_transition_reason_codes"] == [
                "notification_rendered",
                "dry_run_skip_transport",
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
            assert replay_rows["state_transition_reason_codes"] == [
                "notification_rendered",
                "dry_run_skip_transport",
                "notification_duplicate_terminal_noop",
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


def _notifier_config(database_url: str) -> NotifierTelegramConfig:
    return NotifierTelegramConfig(
        app_env="test",
        database_url=database_url,
        redis_url="unused",
        telegram_bot_token="unused-dry-run-token",
        queue_name="q.notification.send",
        consumer_group="notifier-telegram",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        dry_run=True,
        allow_edits=False,
        enable_notification_send=True,
        enable_digest_runtime=False,
        max_message_chars=3800,
        edit_window_minutes=180,
        telegram_api_base_url="https://api.telegram.invalid",
        request_timeout_sec=1.0,
        log_level="INFO",
    )


async def _upstream_boundary_snapshot(
    session: AsyncSession,
    *,
    source_message_id: UUID,
    artifact_id: UUID,
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
            WHERE artifact_id = CAST(:artifact_id AS uuid)
            """,
            {"artifact_id": str(artifact_id)},
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


async def _notification_rows(session: AsyncSession, candidate_group_id: UUID) -> dict[str, Any]:
    plans = await _rows(
        session,
        """
        SELECT notification_plan_id, analysis_id, candidate_group_id, delivery_decision,
               urgency_profile, target_chat_id, target_thread_id, render_profile,
               dedupe_subject_key, material_change_hash, suppress_reason_code, status
        FROM notification_plans
        WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
        ORDER BY created_at, notification_plan_id
        """,
        {"candidate_group_id": str(candidate_group_id)},
    )
    renders = await _rows(
        session,
        """
        SELECT nr.notification_plan_id, nr.disable_notification, nr.protect_content,
               nr.parse_strategy, nr.render_hash
        FROM notification_renders nr
        JOIN notification_plans np
          ON np.notification_plan_id = nr.notification_plan_id
        WHERE np.candidate_group_id = CAST(:candidate_group_id AS uuid)
        ORDER BY nr.created_at, nr.notification_render_id
        """,
        {"candidate_group_id": str(candidate_group_id)},
    )
    delivery_records = await _rows(
        session,
        """
        SELECT ndr.notification_delivery_record_id, ndr.notification_plan_id,
               ndr.telegram_chat_id, ndr.telegram_message_id, ndr.delivery_status,
               ndr.attempt_count, ndr.transport_error_code, ndr.transport_error_class,
               ndr.telegram_response_json
        FROM notification_delivery_records ndr
        JOIN notification_plans np
          ON np.notification_plan_id = ndr.notification_plan_id
        WHERE np.candidate_group_id = CAST(:candidate_group_id AS uuid)
        ORDER BY ndr.created_at, ndr.notification_delivery_record_id
        """,
        {"candidate_group_id": str(candidate_group_id)},
    )
    state_transition_reason_codes_result = await session.execute(
        sa.text(
            """
            SELECT st.reason_code
            FROM state_transitions st
            JOIN notification_plans np
              ON np.notification_plan_id = st.object_id
            WHERE st.object_type = 'notification_plan'
              AND np.candidate_group_id = CAST(:candidate_group_id AS uuid)
            ORDER BY st.created_at, st.state_transition_id
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    )
    delivery_result_events = await _rows(
        session,
        """
        SELECT eo.aggregate_id, eo.payload_json, eo.status
        FROM event_outbox eo
        JOIN notification_plans np
          ON np.notification_plan_id = eo.aggregate_id
        WHERE eo.event_type = 'notification.delivery.result.v1'
          AND np.candidate_group_id = CAST(:candidate_group_id AS uuid)
        ORDER BY eo.created_at, eo.event_id
        """,
        {"candidate_group_id": str(candidate_group_id)},
    )
    return {
        "plans": plans,
        "renders": renders,
        "delivery_records": delivery_records,
        "state_transition_reason_codes": [
            str(row["reason_code"]) for row in state_transition_reason_codes_result.mappings().all()
        ],
        "delivery_result_events": delivery_result_events,
    }


async def _rows(session: AsyncSession, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    result = await session.execute(sa.text(sql), params)
    return [_normalize_row(row) for row in result.mappings().all()]


def _normalize_row(row: Any) -> dict[str, Any]:
    normalized = dict(row)
    for key, value in list(normalized.items()):
        if key.endswith("_json") or key == "payload_json" or key == "telegram_response_json":
            normalized[key] = value if not isinstance(value, str) else json.loads(value)
    return normalized
