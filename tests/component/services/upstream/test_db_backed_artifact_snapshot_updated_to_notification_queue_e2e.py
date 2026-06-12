from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from services.analysis_router.config import AnalysisRouterConfig
from services.analysis_router.repositories import AnalysisRouterRepository
from services.analysis_router.service import AnalysisRouterService
from services.analysis_validator.config import AnalysisValidatorConfig
from services.analysis_validator.repositories import AnalysisValidatorRepository
from services.analysis_validator.service import AnalysisValidatorService
from services.evidence_assembler.config import EvidenceAssemblerConfig
from services.evidence_assembler.repositories import EvidenceAssemblerRepository
from services.evidence_assembler.service import EvidenceAssemblerService
from services.judge_openai.config import JudgeOpenAIConfig
from services.judge_openai.repositories import JudgeOpenAIRepository
from services.judge_openai.service import JudgeOpenAIService
from services.outbox_relay.config import OutboxRelayConfig
from services.outbox_relay.models import QueueRoute, RedisQueuedMessage
from services.outbox_relay.repositories import OutboxRelayRepository
from services.outbox_relay.routing import OutboxRouteResolver
from services.outbox_relay.service import OutboxRelayService
from services.policy_engine.config import PolicyEngineConfig
from services.policy_engine.repositories import PolicyEngineRepository
from services.policy_engine.service import PolicyEngineService


THIN_REDIS_FIELDS = {
    "job_id",
    "stage_name",
    "root_object_type",
    "root_object_id",
    "idempotency_key",
    "pipeline_run_id",
    "not_before",
    "trigger_event_id",
}


pytestmark = pytest.mark.skipif(
    not os.environ.get("LOCAL_TEST_DATABASE_URL"),
    reason="LOCAL_TEST_DATABASE_URL is required for the DB-backed artifact snapshot upstream e2e test",
)


@dataclass(frozen=True, slots=True)
class SeedIds:
    source_message_id: UUID
    artifact_id: UUID
    snapshot_id: UUID
    candidate_group_id: UUID
    artifact_snapshot_updated_event_id: UUID
    content_anchor: str
    canonical_id: str


class RecordingOpenAIClient:
    def __init__(self, *, candidate_group_id: UUID) -> None:
        self._candidate_group_id = candidate_group_id
        self.calls: list[dict[str, Any]] = []

    async def create_structured_response(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {
            "id": "local-db-snapshot-upstream-e2e-fake-response",
            "status": "completed",
            "output_text": json.dumps(
                _judge_output_payload(
                    candidate_group_id=self._candidate_group_id,
                    scores=_send_worthy_scores(),
                    model_proposed_verdict="inspect_now",
                ),
                sort_keys=True,
            ),
            "usage": {
                "input_tokens": 90,
                "input_tokens_details": {"cached_tokens": 70},
                "output_tokens": 20,
                "output_tokens_details": {"reasoning_tokens": 6},
            },
        }


class RecordingRedisPublisher:
    def __init__(self) -> None:
        self.published: list[tuple[QueueRoute, RedisQueuedMessage]] = []

    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str:
        self.published.append((route, message))
        return "0-1"


@pytest.mark.asyncio
async def test_db_backed_artifact_snapshot_updated_routes_to_notification_queue_with_fake_judge_openai() -> None:
    database_url = _local_test_database_url()
    engine = create_async_engine(database_url, future=True)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            ids = await _seed_artifact_snapshot_updated_case(session)
            await session.commit()

            artifact_snapshot_events = await _events(
                session,
                event_type="artifact.snapshot.updated.v1",
                aggregate_id=ids.artifact_id,
            )
            assert len(artifact_snapshot_events) == 1
            assert artifact_snapshot_events[0]["event_id"] == ids.artifact_snapshot_updated_event_id
            assert artifact_snapshot_events[0]["payload_json"] == {
                "artifact_id": str(ids.artifact_id),
                "snapshot_id": str(ids.snapshot_id),
                "provider": "github",
                "status": "ready",
                "content_anchor": ids.content_anchor,
            }
            assert "candidate_group_id" not in artifact_snapshot_events[0]["payload_json"]

            artifact_count_after_seed = await _count(session, "SELECT count(*) FROM artifact_registry", {})
            fake_openai = RecordingOpenAIClient(candidate_group_id=ids.candidate_group_id)

            evidence_assembler = EvidenceAssemblerService(
                _evidence_config(database_url),
                repository=EvidenceAssemblerRepository(session),
            )
            first_results = await evidence_assembler.handle_trigger_event(ids.artifact_snapshot_updated_event_id)
            second_results = await evidence_assembler.handle_trigger_event(ids.artifact_snapshot_updated_event_id)
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
            assert await _count(session, "SELECT count(*) FROM artifact_registry", {}) == artifact_count_after_seed
            assert await _artifact_enrich_events_for_artifact(session, ids.artifact_id) == []
            assert fake_openai.calls == []

            bundles = await _candidate_bundles(session, ids.candidate_group_id)
            assert len(bundles) == 1
            bundle_id = bundles[0]["bundle_id"]
            assert bundle_id == first_results[0].bundle_id
            assert bundles[0]["ready_for_analysis"] is True
            assert await _candidate_bundle_members(session, bundle_id) == [
                {
                    "artifact_id": ids.artifact_id,
                    "snapshot_id": ids.snapshot_id,
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

            publisher = RecordingRedisPublisher()
            relay = OutboxRelayService(
                _outbox_config(database_url),
                repository=OutboxRelayRepository(session),
                publisher=publisher,
                route_resolver=OutboxRouteResolver(),
            )
            assert await relay.run_once() == 1
            await session.commit()

            assert len(publisher.published) == 1
            route, message = publisher.published[0]
            assert route.queue_name == "q.notification.send"
            assert route.stage_name == "notify"
            fields = message.as_stream_fields()
            assert fields == {
                "job_id": str(notification_events[0]["event_id"]),
                "stage_name": "notify",
                "root_object_type": "analysis",
                "root_object_id": str(analyses[0]["analysis_id"]),
                "idempotency_key": notification_events[0]["dedupe_key"],
                "pipeline_run_id": "",
                "not_before": "",
                "trigger_event_id": str(notification_events[0]["event_id"]),
            }
            assert set(fields) == THIN_REDIS_FIELDS
            assert "payload_json" not in fields
            assert await _notifier_owned_counts(session, ids.candidate_group_id) == {
                "notification_plans": 0,
                "notification_renders": 0,
                "notification_delivery_records": 0,
            }
    finally:
        await engine.dispose()


async def _seed_artifact_snapshot_updated_case(session: AsyncSession) -> SeedIds:
    source_message_id = uuid4()
    artifact_id = uuid4()
    snapshot_id = uuid4()
    candidate_group_id = uuid4()
    artifact_snapshot_updated_event_id = uuid4()
    suffix = uuid4().hex
    now = datetime.now(timezone.utc)
    canonical_id = f"github:local-db-snapshot-upstream-e2e:{suffix}"
    canonical_url = "https://github.com/example/local-db-snapshot-upstream-e2e"
    content_anchor = f"local-db-snapshot-upstream-e2e:{suffix}"

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
            "chat_id": 9200000000 + int(suffix[:8], 16),
            "message_id": int(suffix[8:16], 16),
            "logical_post_key": f"db-snapshot-upstream-e2e:{suffix}",
            "posted_at": now,
            "text_body": "Repository signal for local DB artifact snapshot notification queue test.",
            "raw_message_json": _jsonb({"local_test": True}),
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO artifact_registry (
                artifact_id, artifact_type, canonical_id, canonical_url,
                normalized_host, artifact_key_json, current_status
            ) VALUES (
                CAST(:artifact_id AS uuid), 'github_repo'::artifact_type_enum,
                :canonical_id, :canonical_url, 'github.com',
                CAST(:artifact_key_json AS jsonb), 'ready'::snapshot_status_enum
            )
            """
        ),
        {
            "artifact_id": str(artifact_id),
            "canonical_id": canonical_id,
            "canonical_url": canonical_url,
            "artifact_key_json": _jsonb({"owner": "example", "repo": f"snapshot-{suffix}"}),
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO artifact_snapshots (
                snapshot_id, artifact_id, provider, snapshot_type, status, fetched_at,
                content_anchor, auth_mode, normalized_projection, raw_payload_ref,
                evidence_limitations, fetch_anomalies
            ) VALUES (
                CAST(:snapshot_id AS uuid), CAST(:artifact_id AS uuid), 'github',
                'github_repo', 'ready'::snapshot_status_enum, :fetched_at,
                :content_anchor, 'local_db_component_test',
                CAST(:normalized_projection AS jsonb), NULL,
                CAST(:evidence_limitations AS jsonb), CAST(:fetch_anomalies AS jsonb)
            )
            """
        ),
        {
            "snapshot_id": str(snapshot_id),
            "artifact_id": str(artifact_id),
            "fetched_at": now,
            "content_anchor": content_anchor,
            "normalized_projection": _jsonb(
                {
                    "title": "local-db-snapshot-upstream-e2e",
                    "description": "Deterministic component-test repository fixture",
                    "language": "Python",
                    "stars": 84,
                }
            ),
            "evidence_limitations": _jsonb(["local DB component fixture"]),
            "fetch_anomalies": _jsonb([]),
        },
    )
    await session.execute(
        sa.text(
            """
            UPDATE artifact_registry
            SET current_snapshot_id = CAST(:snapshot_id AS uuid),
                current_status = 'ready'::snapshot_status_enum
            WHERE artifact_id = CAST(:artifact_id AS uuid)
            """
        ),
        {"artifact_id": str(artifact_id), "snapshot_id": str(snapshot_id)},
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
            "normalizer_version": "db-snapshot-upstream-e2e-test-v1",
            "dedupe_subject_key": f"db-snapshot-upstream-e2e:{suffix}",
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
                CAST(:event_id AS uuid), 'artifact.snapshot.updated.v1', 'artifact',
                CAST(:artifact_id AS uuid), :dedupe_key,
                CAST(:payload_json AS jsonb), 'published'::outbox_status_enum, :created_at
            )
            """
        ),
        {
            "event_id": str(artifact_snapshot_updated_event_id),
            "artifact_id": str(artifact_id),
            "dedupe_key": f"db-snapshot-upstream-e2e:{suffix}:artifact.snapshot.updated",
            "payload_json": _jsonb(
                {
                    "artifact_id": str(artifact_id),
                    "snapshot_id": str(snapshot_id),
                    "provider": "github",
                    "status": "ready",
                    "content_anchor": content_anchor,
                }
            ),
            "created_at": now,
        },
    )
    return SeedIds(
        source_message_id=source_message_id,
        artifact_id=artifact_id,
        snapshot_id=snapshot_id,
        candidate_group_id=candidate_group_id,
        artifact_snapshot_updated_event_id=artifact_snapshot_updated_event_id,
        content_anchor=content_anchor,
        canonical_id=canonical_id,
    )


async def _candidate_bundles(session: AsyncSession, candidate_group_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT bundle_id, candidate_group_id, ready_for_analysis, bundle_profile_version,
                   token_budget_profile
            FROM candidate_evidence_bundles
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            ORDER BY created_at, bundle_id
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _candidate_bundle_members(session: AsyncSession, bundle_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT artifact_id, snapshot_id, member_role, member_order
            FROM candidate_evidence_members
            WHERE bundle_id = CAST(:bundle_id AS uuid)
            ORDER BY member_role, member_order, artifact_id
            """
        ),
        {"bundle_id": str(bundle_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _artifact_enrich_events_for_artifact(session: AsyncSession, artifact_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT event_id, dedupe_key, payload_json, aggregate_type, aggregate_id, status
            FROM event_outbox
            WHERE event_type = 'artifact.enrich.requested.v1'
              AND aggregate_id = CAST(:artifact_id AS uuid)
            ORDER BY created_at, event_id
            """
        ),
        {"artifact_id": str(artifact_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _events(session: AsyncSession, *, event_type: str, aggregate_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT event_id, dedupe_key, payload_json, aggregate_type, aggregate_id, status
            FROM event_outbox
            WHERE event_type = :event_type
              AND aggregate_id = CAST(:aggregate_id AS uuid)
            ORDER BY created_at, event_id
            """
        ),
        {"event_type": event_type, "aggregate_id": str(aggregate_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _judge_runs_for_bundle(session: AsyncSession, bundle_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT judge_run_id, bundle_id, judge_profile, model, reasoning_effort,
                   prompt_version, schema_version, policy_version, prompt_cache_key, status
            FROM judge_runs
            WHERE bundle_id = CAST(:bundle_id AS uuid)
            ORDER BY judge_run_id
            """
        ),
        {"bundle_id": str(bundle_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _judge_outputs_for_run(session: AsyncSession, judge_run_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT judge_output_id, judge_run_id, candidate_group_id, payload_json,
                   model_proposed_verdict, model_confidence_band
            FROM judge_outputs
            WHERE judge_run_id = CAST(:judge_run_id AS uuid)
            ORDER BY created_at, judge_output_id
            """
        ),
        {"judge_run_id": str(judge_run_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _analyses_for_judge_output(session: AsyncSession, judge_output_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT analysis_id, candidate_group_id, judge_output_id, verdict, delivery_decision
            FROM analyses
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
            ORDER BY created_at, analysis_id
            """
        ),
        {"judge_output_id": str(judge_output_id)},
    )
    return [_row_dict(row) for row in result.mappings().all()]


async def _notifier_owned_counts(session: AsyncSession, candidate_group_id: UUID) -> dict[str, int]:
    notification_plans = await _count(
        session,
        """
        SELECT count(*)
        FROM notification_plans
        WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
        """,
        {"candidate_group_id": str(candidate_group_id)},
    )
    notification_renders = await _count(
        session,
        """
        SELECT count(*)
        FROM notification_renders nr
        JOIN notification_plans np
          ON np.notification_plan_id = nr.notification_plan_id
        WHERE np.candidate_group_id = CAST(:candidate_group_id AS uuid)
        """,
        {"candidate_group_id": str(candidate_group_id)},
    )
    notification_delivery_records = await _count(
        session,
        """
        SELECT count(*)
        FROM notification_delivery_records ndr
        JOIN notification_plans np
          ON np.notification_plan_id = ndr.notification_plan_id
        WHERE np.candidate_group_id = CAST(:candidate_group_id AS uuid)
        """,
        {"candidate_group_id": str(candidate_group_id)},
    )
    return {
        "notification_plans": notification_plans,
        "notification_renders": notification_renders,
        "notification_delivery_records": notification_delivery_records,
    }


async def _mark_events_published(session: AsyncSession, event_ids: list[UUID]) -> None:
    await session.execute(
        sa.text(
            """
            UPDATE event_outbox
            SET status = 'published'::outbox_status_enum
            WHERE event_id = ANY(CAST(:event_ids AS uuid[]))
            """
        ),
        {"event_ids": [str(event_id) for event_id in event_ids]},
    )


async def _move_event_to_front(session: AsyncSession, event_id: UUID) -> None:
    await session.execute(
        sa.text(
            """
            UPDATE event_outbox
            SET created_at = TIMESTAMPTZ '1900-01-01 00:00:00+00'
            WHERE event_id = CAST(:event_id AS uuid)
            """
        ),
        {"event_id": str(event_id)},
    )


async def _count(session: AsyncSession, sql: str, params: dict[str, Any]) -> int:
    result = await session.execute(sa.text(sql), params)
    return int(result.scalar_one())


def _row_dict(row: Any) -> dict[str, Any]:
    converted = dict(row)
    if "payload_json" in converted:
        converted["payload_json"] = _json_obj(converted["payload_json"])
    return converted


def _judge_output_payload(
    *,
    candidate_group_id: UUID,
    scores: dict[str, int | None],
    model_proposed_verdict: str,
) -> dict[str, Any]:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate_group_id),
        "headline": "Useful repository",
        "summary_one_line_ko": "short summary",
        "skeptical_take_ko": "needs more evidence before acting",
        "why_it_might_matter_ko": "could help workflow automation",
        "comparables": ["existing-tool"],
        "scores": scores,
        "reason_codes": ["judge_output_validated"],
        "red_flags_ko": ["production use is still unclear"],
        "evidence_limitations_ko": ["only public docs were checked"],
        "recommended_action_ko": "inspect repository",
        "freshness_note_ko": "recent activity needs verification",
        "model_proposed_verdict": model_proposed_verdict,
        "model_confidence_band": "high",
    }


def _send_worthy_scores() -> dict[str, int | None]:
    return {
        "novelty": 82,
        "practical_usefulness": 90,
        "evidence_strength": 80,
        "hype_penalty": 10,
        "confidence": 85,
        "code_quality": 80,
        "maintenance_signal": 75,
        "specificity": 80,
        "reproducibility_signal": 70,
    }


def _evidence_config(database_url: str) -> EvidenceAssemblerConfig:
    return EvidenceAssemblerConfig(
        app_env="test",
        database_url=database_url,
        redis_url="unused",
        queue_name="q.candidate.bundle",
        consumer_group="evidence-assembler",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        bundle_profile_version="bundle_profile_v1",
        enable_text_idea=True,
        enable_reroot=True,
        log_level="INFO",
    )


def _router_config(database_url: str) -> AnalysisRouterConfig:
    return AnalysisRouterConfig(
        app_env="test",
        database_url=database_url,
        redis_url="unused",
        queue_name="q.analysis.route",
        consumer_group="analysis-router",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        enable_model_escalation=False,
        default_model="gpt-5.4-mini",
        escalation_model="gpt-5.4",
        default_reasoning_effort="low",
        escalation_reasoning_effort="medium",
        github_prompt_version="judge_github_primary_v1",
        x_prompt_version="judge_x_primary_v1",
        text_idea_prompt_version="judge_text_idea_primary_v1",
        judge_schema_version="judge_output_v1",
        policy_version="verdict_policy_v1",
        log_level="INFO",
    )


def _judge_openai_config(database_url: str) -> JudgeOpenAIConfig:
    return JudgeOpenAIConfig(
        app_env="test",
        database_url=database_url,
        redis_url="unused",
        queue_name="q.analysis.judge",
        consumer_group="judge-openai",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        openai_api_key="unused-test-key",
        openai_project=None,
        request_timeout_sec=1.0,
        max_output_tokens=800,
        enable_prompt_guard_preflight=False,
        log_level="INFO",
    )


def _validator_config(database_url: str) -> AnalysisValidatorConfig:
    return AnalysisValidatorConfig(
        app_env="test",
        database_url=database_url,
        redis_url="unused",
        queue_name="q.analysis.validate",
        consumer_group="analysis-validator",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        max_headline_chars=200,
        max_summary_chars=1200,
        max_text_items=10,
        log_level="INFO",
    )


def _policy_config(database_url: str, *, enable_notification_send: bool) -> PolicyEngineConfig:
    return PolicyEngineConfig(
        app_env="test",
        database_url=database_url,
        redis_url="unused",
        queue_name="q.analysis.policy",
        consumer_group="policy-engine",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
        operator_chat_id=12345,
        enable_later_delivery=True,
        enable_silent_later=True,
        enable_notification_send=enable_notification_send,
        render_profile_high="telegram_single_alert_high_v1",
        render_profile_normal="telegram_single_alert_normal_v1",
        log_level="INFO",
    )


def _outbox_config(database_url: str) -> OutboxRelayConfig:
    return OutboxRelayConfig(
        app_env="test",
        database_url=database_url,
        redis_url="unused",
        poll_interval_ms=1000,
        batch_size=1,
        xadd_maxlen=10000,
        log_level="INFO",
    )


def _local_test_database_url() -> str:
    database_url = os.environ["LOCAL_TEST_DATABASE_URL"]
    assert "test" in database_url.lower(), "LOCAL_TEST_DATABASE_URL must target a test database"
    return database_url


def _jsonb(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _json_obj(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value
