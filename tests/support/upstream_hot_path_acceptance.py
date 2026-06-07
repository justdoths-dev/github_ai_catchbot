from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

from services.router_normalizer.config import RouterNormalizerConfig
from services.router_normalizer.models import (
    CanonicalArtifact,
    OutboxEventRow,
    RedisNormalizeMessage,
    SourceMessageSnapshot,
)
from services.router_normalizer.service import RouterNormalizerService
from tests.integration.upstream import (
    test_analysis_requested_to_notification_delivery_result_fake_offline_hot_path as predecessor,
)


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "upstream"
SOURCE_FIXTURE_PATH = FIXTURE_ROOT / "source_message_github_repo_signal.json"

AcceptanceOutcome = Literal["send_now", "suppress"]


@dataclass(slots=True, frozen=True)
class UpstreamHotPathAcceptance:
    result: dict[str, Any]
    ledger: predecessor.AnalysisToDeliveryLedger
    source_repository: "SourceMessageRouterRepository"
    fake_openai_client: predecessor.FakeOpenAIClient
    telegram_client: predecessor.TripwireTelegramClient


class SourceMessageRouterRepository:
    def __init__(
        self,
        *,
        snapshot: SourceMessageSnapshot,
        candidate_group_id: UUID,
        primary_artifact_id: UUID,
    ) -> None:
        self.trigger_event_id = uuid4()
        self.event = OutboxEventRow(
            event_id=self.trigger_event_id,
            event_type="source_message.created.v1",
            aggregate_type="source_message",
            aggregate_id=snapshot.source_message_id,
            dedupe_key=f"source-message:{snapshot.source_message_id}:{snapshot.source_version_no}",
            payload_json={
                "source_message_id": str(snapshot.source_message_id),
                "current_version_no": snapshot.source_version_no,
            },
            status="published",
            created_at=datetime.now(timezone.utc),
        )
        self.snapshot = snapshot
        self.candidate_group_id = candidate_group_id
        self.primary_artifact_id = primary_artifact_id
        self.normalization_runs: list[dict[str, Any]] = []
        self.suppression_traces: list[dict[str, Any]] = []
        self.artifacts_by_id: dict[str, UUID] = {}
        self.artifacts: dict[str, CanonicalArtifact] = {}
        self.observations: list[dict[str, Any]] = []
        self.candidate_groups: list[dict[str, Any]] = []
        self.members: list[dict[str, Any]] = []
        self.enrich_events: list[dict[str, Any]] = []

    async def get_outbox_event(self, event_id: UUID):
        return self.event if event_id == self.event.event_id else None

    async def get_current_source_message(self, source_message_id: UUID):
        return self.snapshot if source_message_id == self.snapshot.source_message_id else None

    async def get_source_message_version(self, *, source_message_id: UUID, version_no: int):
        raise AssertionError("acceptance harness uses the current deterministic source-message fixture")

    async def upsert_normalization_run(self, **kwargs):
        normalization_run_id = uuid4()
        self.normalization_runs.append({"normalization_run_id": normalization_run_id, **kwargs})
        return normalization_run_id

    async def insert_suppression_trace(self, **kwargs):
        self.suppression_traces.append(kwargs)

    async def upsert_artifact_registry(self, artifact: CanonicalArtifact):
        artifact_id = self.artifacts_by_id.get(artifact.canonical_id)
        if artifact_id is None:
            artifact_id = self.primary_artifact_id if not self.artifacts_by_id else uuid4()
            self.artifacts_by_id[artifact.canonical_id] = artifact_id
        self.artifacts[artifact.canonical_id] = artifact
        return artifact_id

    async def insert_artifact_observation_if_absent(self, **kwargs):
        if kwargs not in self.observations:
            self.observations.append(kwargs)

    async def upsert_candidate_group(self, **kwargs):
        if not self.candidate_groups:
            self.candidate_groups.append({"candidate_group_id": self.candidate_group_id, **kwargs})
        return self.candidate_groups[0]["candidate_group_id"]

    async def upsert_candidate_member(self, **kwargs):
        if kwargs not in self.members:
            self.members.append(kwargs)

    async def insert_enrichment_requested_outbox(self, **kwargs):
        self.enrich_events.append(kwargs)


def install_runtime_tripwires(monkeypatch: Any) -> None:
    predecessor._install_tripwires(monkeypatch)


async def run_upstream_hot_path_acceptance(
    *,
    outcome: AcceptanceOutcome = "send_now",
    repeat_terminal_delivery: bool = False,
) -> UpstreamHotPathAcceptance:
    source_snapshot = _load_source_snapshot()
    downstream_fixture = _downstream_fixture_for_source(source_snapshot)
    source_repository = SourceMessageRouterRepository(
        snapshot=source_snapshot,
        candidate_group_id=UUID(downstream_fixture["candidate_group_id"]),
        primary_artifact_id=UUID(downstream_fixture["current_primary_artifact_id"]),
    )
    await _run_source_router(source_repository)
    downstream_fixture = _with_source_router_outputs(
        downstream_fixture,
        source_snapshot=source_snapshot,
        source_repository=source_repository,
    )

    ledger = predecessor.AnalysisToDeliveryLedger(downstream_fixture)
    fake_openai_client = predecessor.FakeOpenAIClient(_openai_response(ledger.candidate_group_id, outcome=outcome))
    telegram_client = predecessor.TripwireTelegramClient()

    await _run_through_policy(ledger=ledger, fake_openai_client=fake_openai_client)
    if outcome == "send_now":
        await predecessor._run_notifier_stage(
            ledger,
            ledger.event_id_for_type("notification.plan.created.v1"),
            telegram_client,
        )
        if repeat_terminal_delivery:
            await predecessor._run_notifier_stage(
                ledger,
                ledger.event_id_for_type("notification.plan.created.v1"),
                telegram_client,
            )

    return UpstreamHotPathAcceptance(
        result=_acceptance_result(
            ledger=ledger,
            source_repository=source_repository,
            fake_openai_client=fake_openai_client,
            telegram_client=telegram_client,
        ),
        ledger=ledger,
        source_repository=source_repository,
        fake_openai_client=fake_openai_client,
        telegram_client=telegram_client,
    )


async def rerun_same_fixture_chain(acceptance: UpstreamHotPathAcceptance) -> None:
    ledger = acceptance.ledger
    await predecessor._run_analysis_router_stage(ledger, ledger.event_id_for_type("analysis.requested.v1"))
    await predecessor._run_judge_stage(
        ledger,
        ledger.event_id_for_type("judge.call.requested.v1"),
        predecessor.FakeOpenAIClient(_openai_response(ledger.candidate_group_id, outcome="send_now")),
    )
    await predecessor._run_validator_stage(ledger, ledger.event_id_for_type("judge.output.ready.v1"))
    await predecessor._run_policy_stage(ledger, ledger.event_id_for_type("analysis.policy.apply.v1"))
    await predecessor._run_notifier_stage(
        ledger,
        ledger.event_id_for_type("notification.plan.created.v1"),
        acceptance.telegram_client,
    )


def event_type_sequence(ledger: predecessor.AnalysisToDeliveryLedger) -> list[str]:
    return [row["event_type"] for row in ledger.event_outbox]


def terminal_counts(ledger: predecessor.AnalysisToDeliveryLedger) -> dict[str, int]:
    return {
        "judge_runs": len(ledger.judge_runs),
        "judge_outputs": len(ledger.judge_outputs),
        "analyses": len(ledger.analyses),
        "notification_plans": len(ledger.notification_plans),
        "notification_renders": len(ledger.notification_renders),
        "notification_delivery_records": len(ledger.notification_delivery_records),
        "notification_delivery_result_events": len(ledger.notification_delivery_result_events),
    }


async def _run_source_router(repository: SourceMessageRouterRepository) -> None:
    result = await RouterNormalizerService(_router_config(), repository=repository).process_stream_message(
        RedisNormalizeMessage(
            job_id=str(repository.trigger_event_id),
            stage_name="normalize",
            root_object_type="source_message",
            root_object_id=str(repository.snapshot.source_message_id),
            idempotency_key="fixture",
            trigger_event_id=str(repository.trigger_event_id),
        )
    )
    assert result.candidate_eligible is True
    assert result.candidate_group_count == 1
    assert repository.candidate_groups
    assert repository.members


async def _run_through_policy(
    *,
    ledger: predecessor.AnalysisToDeliveryLedger,
    fake_openai_client: predecessor.FakeOpenAIClient,
) -> None:
    assert event_type_sequence(ledger) == ["analysis.requested.v1"]
    await predecessor._run_analysis_router_stage(ledger, ledger.trigger_event_id)
    await predecessor._run_judge_stage(
        ledger,
        ledger.event_id_for_type("judge.call.requested.v1"),
        fake_openai_client,
    )
    await predecessor._run_validator_stage(ledger, ledger.event_id_for_type("judge.output.ready.v1"))
    await predecessor._run_policy_stage(ledger, ledger.event_id_for_type("analysis.policy.apply.v1"))


def _acceptance_result(
    *,
    ledger: predecessor.AnalysisToDeliveryLedger,
    source_repository: SourceMessageRouterRepository,
    fake_openai_client: predecessor.FakeOpenAIClient,
    telegram_client: predecessor.TripwireTelegramClient,
) -> dict[str, Any]:
    analysis = next(iter(ledger.analyses.values()), None)
    notification_plan = next(iter(ledger.notification_plans.values()), None)
    delivery_decision = analysis.delivery_decision if analysis is not None else None
    passed = bool(
        source_repository.snapshot
        and source_repository.artifacts_by_id
        and source_repository.candidate_groups
        and ledger.candidate_evidence_bundles
        and ledger.judge_outputs
        and ledger.analyses
        and delivery_decision in {"send_now", "suppress"}
    )
    if delivery_decision == "send_now":
        passed = passed and bool(notification_plan) and bool(ledger.notification_delivery_records)
    else:
        passed = passed and not ledger.notification_plans and not ledger.notification_delivery_records

    return {
        "schema_version": "upstream_hot_path_acceptance_v1",
        "status": "pass" if passed else "fail",
        "source_message_created": True,
        "artifact_created": bool(source_repository.artifacts_by_id),
        "candidate_group_created": bool(source_repository.candidate_groups),
        "evidence_bundle_ready": any(bundle.ready_for_analysis for bundle in ledger.candidate_evidence_bundles.values()),
        "judge_output_ready": bool(ledger.judge_outputs),
        "analysis_created": bool(ledger.analyses),
        "notification_plan_created": bool(ledger.notification_plans),
        "delivery_decision": delivery_decision,
        "notifier_boundary_reached": bool(ledger.notification_delivery_records),
        "live_telegram_called": telegram_client.send_calls > 0 or telegram_client.edit_calls > 0,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": bool(ledger.redis_dispatches),
        "target_chat_id_present": notification_plan.target_chat_id is not None if notification_plan else False,
        "fake_judge_client_calls": len(fake_openai_client.calls),
        "event_sequence": event_type_sequence(ledger),
    }


def _router_config() -> RouterNormalizerConfig:
    return RouterNormalizerConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        queue_name="q.source.normalize",
        consumer_group="router-normalizer",
        consumer_name="upstream-acceptance",
        block_ms=100,
        batch_size=1,
        normalizer_version="test-normalizer",
        short_url_allowlist=(),
        short_url_hop_limit=1,
        short_url_timeout_seconds=0.1,
        log_level="INFO",
    )


def _load_source_snapshot() -> SourceMessageSnapshot:
    payload = json.loads(SOURCE_FIXTURE_PATH.read_text(encoding="utf-8"))
    return SourceMessageSnapshot(
        source_message_id=UUID(payload["source_message_id"]),
        source_version_no=int(payload["source_version_no"]),
        text_body=payload.get("text_body"),
        caption_text=payload.get("caption_text"),
        text_surface=payload.get("text_surface"),
        entities_json=payload.get("entities_json"),
        url_surface_json=payload.get("url_surface_json"),
        raw_message_json=payload.get("raw_message_json") or {},
    )


def _downstream_fixture_for_source(source_snapshot: SourceMessageSnapshot) -> dict[str, Any]:
    fixture = deepcopy(predecessor._load_fixture())
    fixture["source_message_id"] = str(source_snapshot.source_message_id)
    fixture["source_text_surface"] = source_snapshot.text_surface or ""
    return fixture


def _with_source_router_outputs(
    fixture: dict[str, Any],
    *,
    source_snapshot: SourceMessageSnapshot,
    source_repository: SourceMessageRouterRepository,
) -> dict[str, Any]:
    output = deepcopy(fixture)
    candidate_group = source_repository.candidate_groups[0]
    primary_member = next(member for member in source_repository.members if member["member_role"] == "primary")
    artifact = next(
        artifact
        for artifact_id, artifact in (
            (source_repository.artifacts_by_id[key], value)
            for key, value in source_repository.artifacts.items()
        )
        if artifact_id == primary_member["artifact_id"]
    )
    output.update(
        {
            "source_message_id": str(source_snapshot.source_message_id),
            "source_text_surface": source_snapshot.text_surface or "",
            "candidate_group_id": str(candidate_group["candidate_group_id"]),
            "current_primary_artifact_id": str(primary_member["artifact_id"]),
            "current_primary_artifact_type": artifact.artifact_type,
            "primary_canonical_url": artifact.canonical_url,
            "primary_canonical_id": artifact.canonical_id,
        }
    )
    return output


def _openai_response(candidate_group_id: UUID, *, outcome: AcceptanceOutcome) -> dict[str, Any]:
    payload = predecessor._judge_payload(candidate_group_id)
    if outcome == "suppress":
        payload = deepcopy(payload)
        payload["scores"] = {
            "novelty": 20,
            "practical_usefulness": 20,
            "evidence_strength": 20,
            "hype_penalty": 20,
            "confidence": 20,
            "code_quality": 20,
            "maintenance_signal": 20,
            "specificity": 20,
            "reproducibility_signal": 20,
        }
        payload["reason_codes"] = ["low_value_fixture"]
        payload["model_proposed_verdict"] = "skip"
        payload["model_confidence_band"] = "low"
    return {
        "id": "fake-response-id",
        "status": "completed",
        "output_text": json.dumps(payload),
        "usage": {
            "input_tokens": 90,
            "input_tokens_details": {"cached_tokens": 70},
            "output_tokens": 20,
            "output_tokens_details": {"reasoning_tokens": 6},
        },
    }
