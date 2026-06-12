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

from services.analysis_validator.config import AnalysisValidatorConfig
from services.analysis_validator.repositories import AnalysisValidatorRepository
from services.analysis_validator.service import AnalysisValidatorService
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
    reason="LOCAL_TEST_DATABASE_URL is required for the DB-backed validator/policy/notification queue test",
)


@dataclass(frozen=True, slots=True)
class SeedIds:
    source_message_id: UUID
    artifact_id: UUID
    candidate_group_id: UUID
    bundle_id: UUID
    judge_run_id: UUID
    judge_output_id: UUID
    judge_output_ready_event_id: UUID


class RecordingRedisPublisher:
    def __init__(self) -> None:
        self.published: list[tuple[QueueRoute, RedisQueuedMessage]] = []

    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str:
        self.published.append((route, message))
        return "0-1"


@pytest.mark.asyncio
async def test_db_backed_validator_policy_notification_queue_send_worthy_path_and_dedupe() -> None:
    database_url = _local_test_database_url()
    engine = create_async_engine(database_url, future=True)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            ids = await _seed_case(session, scores=_send_worthy_scores(), model_proposed_verdict="inspect_now")
            await session.commit()

            validator = AnalysisValidatorService(
                _validator_config(database_url),
                repository=AnalysisValidatorRepository(session),
            )
            await validator.handle_trigger_event(ids.judge_output_ready_event_id)
            await validator.handle_trigger_event(ids.judge_output_ready_event_id)
            await session.commit()

            policy_events = await _events(
                session,
                event_type="analysis.policy.apply.v1",
                aggregate_id=ids.judge_run_id,
            )
            assert len(policy_events) == 1
            assert policy_events[0]["payload_json"] == {
                "judge_run_id": str(ids.judge_run_id),
                "judge_output_id": str(ids.judge_output_id),
                "candidate_group_id": str(ids.candidate_group_id),
                "bundle_id": str(ids.bundle_id),
            }

            await _mark_events_published(session, [policy_events[0]["event_id"]])
            await session.commit()

            policy = PolicyEngineService(
                _policy_config(database_url, enable_notification_send=True),
                repository=PolicyEngineRepository(session),
            )
            await policy.handle_trigger_event(policy_events[0]["event_id"])
            await policy.handle_trigger_event(policy_events[0]["event_id"])
            await session.commit()

            analyses = await _analyses_for_judge_output(session, ids.judge_output_id)
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
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_db_backed_suppress_path_writes_analysis_without_notification_queue() -> None:
    database_url = _local_test_database_url()
    engine = create_async_engine(database_url, future=True)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            ids = await _seed_case(session, scores=_suppress_scores(), model_proposed_verdict="skip")
            await session.commit()

            validator = AnalysisValidatorService(
                _validator_config(database_url),
                repository=AnalysisValidatorRepository(session),
            )
            await validator.handle_trigger_event(ids.judge_output_ready_event_id)
            await session.commit()

            policy_events = await _events(
                session,
                event_type="analysis.policy.apply.v1",
                aggregate_id=ids.judge_run_id,
            )
            assert len(policy_events) == 1
            await _mark_events_published(session, [policy_events[0]["event_id"]])
            await session.commit()

            policy = PolicyEngineService(
                _policy_config(database_url, enable_notification_send=True),
                repository=PolicyEngineRepository(session),
            )
            await policy.handle_trigger_event(policy_events[0]["event_id"])
            await session.commit()

            analyses = await _analyses_for_judge_output(session, ids.judge_output_id)
            assert len(analyses) == 1
            assert analyses[0]["verdict"] == "skip"
            assert analyses[0]["delivery_decision"] == "suppress"
            assert await _events(
                session,
                event_type="notification.plan.created.v1",
                aggregate_id=analyses[0]["analysis_id"],
            ) == []
            assert await _notifier_owned_counts(session, ids.candidate_group_id) == {
                "notification_plans": 0,
                "notification_renders": 0,
                "notification_delivery_records": 0,
            }

            publisher = RecordingRedisPublisher()
            assert publisher.published == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_db_backed_invalid_judge_output_fails_closed_before_policy() -> None:
    database_url = _local_test_database_url()
    engine = create_async_engine(database_url, future=True)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            ids = await _seed_case(
                session,
                scores=_send_worthy_scores(),
                model_proposed_verdict="inspect_now",
                mutate_payload=lambda payload: payload.pop("headline"),
            )
            await session.commit()

            validator = AnalysisValidatorService(
                _validator_config(database_url),
                repository=AnalysisValidatorRepository(session),
            )
            await validator.handle_trigger_event(ids.judge_output_ready_event_id)
            await session.commit()

            assert await _events(session, event_type="analysis.policy.apply.v1", aggregate_id=ids.judge_run_id) == []
            assert await _analyses_for_judge_output(session, ids.judge_output_id) == []
            assert await _events(
                session,
                event_type="notification.plan.created.v1",
                aggregate_id=ids.judge_run_id,
            ) == []
            assert await _judge_run_status(session, ids.judge_run_id) == {
                "status": "failed_terminal",
                "finish_reason": "validator_schema_invalid",
            }
            assert await _notifier_owned_counts(session, ids.candidate_group_id) == {
                "notification_plans": 0,
                "notification_renders": 0,
                "notification_delivery_records": 0,
            }
    finally:
        await engine.dispose()


async def _seed_case(
    session: AsyncSession,
    *,
    scores: dict[str, int | None],
    model_proposed_verdict: str,
    mutate_payload: Any | None = None,
) -> SeedIds:
    source_message_id = uuid4()
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    judge_run_id = uuid4()
    judge_output_id = uuid4()
    ready_event_id = uuid4()
    suffix = uuid4().hex
    now = datetime.now(timezone.utc)
    payload = _judge_output_payload(
        candidate_group_id=candidate_group_id,
        scores=scores,
        model_proposed_verdict=model_proposed_verdict,
    )
    if mutate_payload is not None:
        mutate_payload(payload)

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
            "chat_id": 9000000000 + int(suffix[:8], 16),
            "message_id": int(suffix[8:16], 16),
            "logical_post_key": f"db-validator-policy-notify:{suffix}",
            "posted_at": now,
            "text_body": "Repository signal for local DB validator policy notification queue test.",
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
            "canonical_id": f"github:local-db-validator-policy-notify:{suffix}",
            "canonical_url": "https://github.com/example/local-db-validator-policy-notify",
            "artifact_key_json": _jsonb({"owner": "example", "repo": f"local-{suffix}"}),
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
                'proposed', :normalizer_version, :dedupe_subject_key
            )
            """
        ),
        {
            "candidate_group_id": str(candidate_group_id),
            "source_message_id": str(source_message_id),
            "artifact_id": str(artifact_id),
            "normalizer_version": "db-validator-policy-notify-test-v1",
            "dedupe_subject_key": f"db-validator-policy-notify:{suffix}",
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO candidate_evidence_bundles (
                bundle_id, candidate_group_id, initial_primary_artifact_id,
                current_primary_artifact_id, bundle_profile_version, bundle_input_hash,
                primary_summary, evidence_limitations, ready_for_analysis,
                token_budget_profile
            ) VALUES (
                CAST(:bundle_id AS uuid), CAST(:candidate_group_id AS uuid),
                CAST(:artifact_id AS uuid), CAST(:artifact_id AS uuid),
                'db_validator_policy_notify_bundle_v1', :bundle_input_hash,
                CAST(:primary_summary AS jsonb), CAST(:evidence_limitations AS jsonb),
                true, 'test'
            )
            """
        ),
        {
            "bundle_id": str(bundle_id),
            "candidate_group_id": str(candidate_group_id),
            "artifact_id": str(artifact_id),
            "bundle_input_hash": f"db-validator-policy-notify:{suffix}",
            "primary_summary": _jsonb({"title": "Useful repository", "artifact_type": "github_repo"}),
            "evidence_limitations": _jsonb(["local DB component fixture"]),
        },
    )
    await session.execute(
        sa.text(
            """
            UPDATE candidate_group_proposals
            SET current_bundle_id = CAST(:bundle_id AS uuid)
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """
        ),
        {"bundle_id": str(bundle_id), "candidate_group_id": str(candidate_group_id)},
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO judge_runs (
                judge_run_id, bundle_id, judge_profile, model, reasoning_effort,
                prompt_version, schema_version, policy_version, prompt_cache_key,
                status, schema_retry_count, finish_reason, refusal_detected,
                started_at, finished_at
            ) VALUES (
                CAST(:judge_run_id AS uuid), CAST(:bundle_id AS uuid),
                'github_primary', 'gpt-5.4-mini', 'low',
                'judge_github_primary_v1', 'judge_output_v1', 'verdict_policy_v1',
                :prompt_cache_key, 'succeeded', 0, 'completed', false, :now, :now
            )
            """
        ),
        {
            "judge_run_id": str(judge_run_id),
            "bundle_id": str(bundle_id),
            "prompt_cache_key": f"judge:db-validator-policy-notify:{suffix}",
            "now": now,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO judge_outputs (
                judge_output_id, judge_run_id, candidate_group_id, judge_schema_version,
                payload_json, model_proposed_verdict, model_confidence_band
            ) VALUES (
                CAST(:judge_output_id AS uuid), CAST(:judge_run_id AS uuid),
                CAST(:candidate_group_id AS uuid), 'judge_output_v1',
                CAST(:payload_json AS jsonb), CAST(:model_proposed_verdict AS verdict_enum),
                'high'
            )
            """
        ),
        {
            "judge_output_id": str(judge_output_id),
            "judge_run_id": str(judge_run_id),
            "candidate_group_id": str(candidate_group_id),
            "payload_json": _jsonb(payload),
            "model_proposed_verdict": model_proposed_verdict,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO event_outbox (
                event_id, event_type, aggregate_type, aggregate_id, dedupe_key,
                payload_json, status, created_at
            ) VALUES (
                CAST(:event_id AS uuid), 'judge.output.ready.v1', 'judge_run',
                CAST(:judge_run_id AS uuid), :dedupe_key,
                CAST(:payload_json AS jsonb), 'published'::outbox_status_enum, :created_at
            )
            """
        ),
        {
            "event_id": str(ready_event_id),
            "judge_run_id": str(judge_run_id),
            "dedupe_key": f"db-validator-policy-notify:{suffix}:judge.output.ready",
            "payload_json": _jsonb(
                {
                    "judge_run_id": str(judge_run_id),
                    "judge_output_id": str(judge_output_id),
                    "finish_reason": "completed",
                    "refusal_detected": False,
                }
            ),
            "created_at": now,
        },
    )
    return SeedIds(
        source_message_id=source_message_id,
        artifact_id=artifact_id,
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
        judge_output_ready_event_id=ready_event_id,
    )


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


async def _judge_run_status(session: AsyncSession, judge_run_id: UUID) -> dict[str, str | None]:
    result = await session.execute(
        sa.text(
            """
            SELECT status, finish_reason
            FROM judge_runs
            WHERE judge_run_id = CAST(:judge_run_id AS uuid)
            """
        ),
        {"judge_run_id": str(judge_run_id)},
    )
    row = result.mappings().one()
    return {"status": row["status"], "finish_reason": row["finish_reason"]}


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
            SET created_at = TIMESTAMPTZ '1970-01-01 00:00:00+00'
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


def _suppress_scores() -> dict[str, int | None]:
    return {
        "novelty": 20,
        "practical_usefulness": 10,
        "evidence_strength": 10,
        "hype_penalty": 90,
        "confidence": 10,
        "code_quality": 10,
        "maintenance_signal": 10,
        "specificity": 10,
        "reproducibility_signal": 10,
    }


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
