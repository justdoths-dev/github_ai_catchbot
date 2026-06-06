from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from services.evidence_assembler.config import EvidenceAssemblerConfig
from services.evidence_assembler.models import (
    BundleRefreshTarget,
    CandidateGroupRecord,
    CandidateMemberRecord,
    EvidenceBundleDraft,
    ExistingBundleRecord,
    SnapshotRecord,
)
from services.evidence_assembler.service import EvidenceAssemblerService
from services.router_normalizer.config import RouterNormalizerConfig
from services.router_normalizer.models import CanonicalArtifact, OutboxEventRow, RedisNormalizeMessage, SourceMessageSnapshot
from services.router_normalizer.service import RouterNormalizerService


FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "upstream"


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class UpstreamHotPathLedger:
    def __init__(self, snapshot: SourceMessageSnapshot) -> None:
        self.snapshot = snapshot
        self.source_messages = {snapshot.source_message_id: snapshot}
        self.source_message_versions: dict[tuple[UUID, int], SourceMessageSnapshot] = {}
        self.normalization_runs: dict[tuple[UUID, int, str], dict] = {}
        self.normalization_suppression_traces: list[dict] = []
        self.artifact_registry: dict[UUID, dict] = {}
        self.artifact_by_canonical_id: dict[str, UUID] = {}
        self.artifact_observations: dict[tuple[UUID, UUID, int, str], dict] = {}
        self.candidate_group_proposals: dict[UUID, dict] = {}
        self.candidate_group_by_key: dict[tuple[UUID, int, str], UUID] = {}
        self.candidate_group_members: dict[tuple[UUID, UUID, str], dict] = {}
        self.artifact_snapshots: dict[UUID, SnapshotRecord] = {}
        self.candidate_reroot_events: list[dict] = []
        self.candidate_evidence_bundles: dict[UUID, dict] = {}
        self.candidate_evidence_members: dict[UUID, list[dict]] = {}
        self.event_outbox: list[dict] = []
        self._event_by_id: dict[UUID, dict] = {}
        self._outbox_dedupe_keys: set[str] = set()
        self.judge_runs: list[dict] = []
        self.judge_outputs: list[dict] = []
        self.analyses: list[dict] = []
        self.notification_plans: list[dict] = []
        self.notification_renders: list[dict] = []
        self.notification_delivery_records: list[dict] = []
        self.replay_requests: list[dict] = []
        self.dead_letter_entries: list[dict] = []
        self.redis_dispatches: list[dict] = []
        self.telegram_calls: list[dict] = []
        self.openai_calls: list[dict] = []
        self.maintenance_calls: list[dict] = []
        self.source_event_id = self.append_event(
            event_type="source_message.created.v1",
            aggregate_type="source_message",
            aggregate_id=snapshot.source_message_id,
            dedupe_key=f"source-message:{snapshot.source_message_id}:{snapshot.source_version_no}",
            payload_json={
                "source_message_id": str(snapshot.source_message_id),
                "current_version_no": snapshot.source_version_no,
            },
        )

    def append_event(
        self,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: UUID,
        dedupe_key: str,
        payload_json: dict,
    ) -> UUID:
        if dedupe_key in self._outbox_dedupe_keys:
            return next(row["event_id"] for row in self.event_outbox if row["dedupe_key"] == dedupe_key)
        event_id = uuid4()
        row = {
            "event_id": event_id,
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "dedupe_key": dedupe_key,
            "payload_json": payload_json,
            "status": "pending",
            "created_at": datetime.now(timezone.utc),
        }
        self.event_outbox.append(row)
        self._event_by_id[event_id] = row
        self._outbox_dedupe_keys.add(dedupe_key)
        return event_id

    def transaction(self):
        return Tx()

    async def get_outbox_event(self, event_id: UUID):
        row = self._event_by_id.get(event_id)
        if row is None:
            return None
        return OutboxEventRow(
            event_id=row["event_id"],
            event_type=row["event_type"],
            aggregate_type=row["aggregate_type"],
            aggregate_id=row["aggregate_id"],
            dedupe_key=row["dedupe_key"],
            payload_json=row["payload_json"],
            status=row["status"],
            created_at=row["created_at"],
        )

    async def get_current_source_message(self, source_message_id: UUID):
        return self.source_messages.get(source_message_id)

    async def get_source_message_version(self, *, source_message_id: UUID, version_no: int):
        return self.source_message_versions.get((source_message_id, version_no))

    async def upsert_normalization_run(self, **kwargs):
        key = (kwargs["source_message_id"], kwargs["source_version_no"], kwargs["normalizer_version"])
        row = self.normalization_runs.get(key)
        if row is None:
            row = {"normalization_run_id": uuid4(), **kwargs}
            self.normalization_runs[key] = row
        else:
            row.update(kwargs)
        return row["normalization_run_id"]

    async def insert_suppression_trace(self, **kwargs):
        self.normalization_suppression_traces.append(kwargs)

    async def upsert_artifact_registry(self, artifact: CanonicalArtifact):
        artifact_id = self.artifact_by_canonical_id.get(artifact.canonical_id)
        if artifact_id is None:
            artifact_id = uuid4()
            self.artifact_by_canonical_id[artifact.canonical_id] = artifact_id
        self.artifact_registry[artifact_id] = {"artifact_id": artifact_id, "artifact": artifact}
        return artifact_id

    async def insert_artifact_observation_if_absent(self, **kwargs):
        observed_url = kwargs["artifact"].observed_url or ""
        key = (kwargs["artifact_id"], kwargs["source_message_id"], kwargs["source_version_no"], observed_url)
        self.artifact_observations.setdefault(key, kwargs)

    async def upsert_candidate_group(self, **kwargs):
        key = (kwargs["source_message_id"], kwargs["source_version_no"], kwargs["dedupe_subject_key"])
        candidate_group_id = self.candidate_group_by_key.get(key)
        if candidate_group_id is None:
            candidate_group_id = uuid4()
            self.candidate_group_by_key[key] = candidate_group_id
        self.candidate_group_proposals[candidate_group_id] = {
            "candidate_group_id": candidate_group_id,
            "source_message_id": kwargs["source_message_id"],
            "source_version_no": kwargs["source_version_no"],
            "initial_primary_artifact_id": kwargs["primary_artifact_id"],
            "current_primary_artifact_id": kwargs["primary_artifact_id"],
            "proposal_status": "proposed",
            "normalizer_version": kwargs["normalizer_version"],
            "dedupe_subject_key": kwargs["dedupe_subject_key"],
            "current_bundle_id": self.candidate_group_proposals.get(candidate_group_id, {}).get("current_bundle_id"),
        }
        return candidate_group_id

    async def upsert_candidate_member(self, **kwargs):
        key = (kwargs["candidate_group_id"], kwargs["artifact_id"], kwargs["member_role"])
        self.candidate_group_members[key] = kwargs

    async def insert_enrichment_requested_outbox(self, **kwargs):
        artifact = kwargs["artifact"]
        if artifact.provider_route is None:
            return
        self.append_event(
            event_type="artifact.enrich.requested.v1",
            aggregate_type="artifact",
            aggregate_id=kwargs["artifact_id"],
            dedupe_key=(
                f"artifact:enrich:{kwargs['candidate_group_id']}:{artifact.canonical_id}:"
                f"{kwargs['source_message_id']}:{kwargs['source_version_no']}"
            ),
            payload_json={
                "candidate_group_id": str(kwargs["candidate_group_id"]),
                "artifact_id": str(kwargs["artifact_id"]),
                "artifact_type": artifact.artifact_type,
                "canonical_id": artifact.canonical_id,
                "provider_route": artifact.provider_route,
                "refresh_mode": "standard",
                "depth_budget": 1,
                "source_message_id": str(kwargs["source_message_id"]),
                "source_version_no": kwargs["source_version_no"],
            },
        )

    def seed_ready_snapshots_for_candidate(self, candidate_group_id: UUID) -> None:
        for row in self.candidate_group_members.values():
            if row["candidate_group_id"] != candidate_group_id:
                continue
            artifact_id = row["artifact_id"]
            artifact = self.artifact_registry[artifact_id]["artifact"]
            self.artifact_snapshots[artifact_id] = SnapshotRecord(
                snapshot_id=uuid4(),
                artifact_id=artifact_id,
                provider="fixture",
                snapshot_type=artifact.artifact_type,
                status="ready",
                fetched_at=datetime.now(timezone.utc),
                content_anchor=f"fixture:{artifact.canonical_id}:v1",
                normalized_projection={"title": "Fixture-backed upstream candidate"},
                evidence_limitations=[],
                fetch_anomalies=[],
            )

    def append_candidate_bundle_refresh_event(self, candidate_group_id: UUID) -> UUID:
        return self.append_event(
            event_type="candidate.bundle.refresh.v1",
            aggregate_type="candidate_group",
            aggregate_id=candidate_group_id,
            dedupe_key=f"candidate-bundle-refresh:{candidate_group_id}:fixture",
            payload_json={"candidate_group_id": str(candidate_group_id), "refresh_mode": "fixture"},
        )

    async def resolve_refresh_targets(self, trigger_event_id: UUID):
        row = self._event_by_id.get(trigger_event_id)
        if row is None or row["event_type"] != "candidate.bundle.refresh.v1":
            return []
        candidate_group_id = UUID(row["payload_json"]["candidate_group_id"])
        return [BundleRefreshTarget(candidate_group_id, trigger_event_id, row["event_type"])]

    async def load_candidate_group(self, candidate_group_id: UUID):
        row = self.candidate_group_proposals.get(candidate_group_id)
        if row is None:
            return None
        return CandidateGroupRecord(
            candidate_group_id=row["candidate_group_id"],
            source_message_id=row["source_message_id"],
            source_version_no=row["source_version_no"],
            initial_primary_artifact_id=row["initial_primary_artifact_id"],
            current_primary_artifact_id=row["current_primary_artifact_id"],
            proposal_status=row["proposal_status"],
            current_bundle_id=row["current_bundle_id"],
        )

    async def load_candidate_members(self, candidate_group_id: UUID):
        members = [
            row
            for row in self.candidate_group_members.values()
            if row["candidate_group_id"] == candidate_group_id
        ]
        records = []
        for row in sorted(members, key=lambda item: (item["member_order"], str(item["artifact_id"]))):
            artifact = self.artifact_registry[row["artifact_id"]]["artifact"]
            records.append(
                CandidateMemberRecord(
                    artifact_id=row["artifact_id"],
                    artifact_type=artifact.artifact_type,
                    member_role=row["member_role"],
                    member_order=row["member_order"],
                )
            )
        return records

    async def load_current_snapshots(self, artifact_ids):
        wanted = set(artifact_ids)
        return {artifact_id: snapshot for artifact_id, snapshot in self.artifact_snapshots.items() if artifact_id in wanted}

    async def load_source_message_text_surface(self, **kwargs):
        return self.snapshot.text_surface

    async def ensure_text_idea_snapshot(self, draft):
        raise AssertionError("GitHub fixture path must not materialize text_idea snapshots")

    async def load_discovered_links(self, **kwargs):
        return []

    async def count_reroot_events(self, candidate_group_id: UUID):
        return len([row for row in self.candidate_reroot_events if row["candidate_group_id"] == candidate_group_id])

    async def append_reroot_event(self, **kwargs):
        self.candidate_reroot_events.append(kwargs)

    async def update_current_primary(self, **kwargs):
        self.candidate_group_proposals[kwargs["candidate_group_id"]]["current_primary_artifact_id"] = kwargs["artifact_id"]

    async def load_existing_bundle(self, **kwargs):
        for bundle_id, row in self.candidate_evidence_bundles.items():
            draft = row["draft"]
            if (
                draft.candidate_group_id == kwargs["candidate_group_id"]
                and draft.bundle_profile_version == kwargs["bundle_profile_version"]
                and draft.bundle_input_hash == kwargs["bundle_input_hash"]
            ):
                return ExistingBundleRecord(
                    bundle_id=bundle_id,
                    candidate_group_id=draft.candidate_group_id,
                    bundle_version=row["bundle_version"],
                    bundle_profile_version=draft.bundle_profile_version,
                    bundle_input_hash=draft.bundle_input_hash,
                    ready_for_analysis=draft.ready_for_analysis,
                )
        return None

    async def next_bundle_version(self, candidate_group_id: UUID):
        return 1 + sum(
            1 for row in self.candidate_evidence_bundles.values() if row["draft"].candidate_group_id == candidate_group_id
        )

    async def append_bundle(self, *, draft: EvidenceBundleDraft, bundle_version: int):
        bundle_id = uuid4()
        self.candidate_evidence_bundles[bundle_id] = {"bundle_id": bundle_id, "bundle_version": bundle_version, "draft": draft}
        self.candidate_evidence_members[bundle_id] = [
            {
                "artifact_id": member.artifact_id,
                "snapshot_id": member.snapshot_id,
                "member_role": member.member_role,
                "member_order": member.member_order,
            }
            for member in draft.members
        ]
        return bundle_id

    async def update_current_bundle(self, *, candidate_group_id: UUID, bundle_id: UUID):
        self.candidate_group_proposals[candidate_group_id]["current_bundle_id"] = bundle_id

    async def insert_analysis_requested_outbox(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
        judge_profile: str,
        escalation_allowed: bool,
    ):
        self.append_event(
            event_type="analysis.requested.v1",
            aggregate_type="candidate_group",
            aggregate_id=candidate_group_id,
            dedupe_key=f"analysis-request:{candidate_group_id}:{bundle_id}",
            payload_json={
                "candidate_group_id": str(candidate_group_id),
                "bundle_id": str(bundle_id),
                "judge_profile": judge_profile,
                "escalation_allowed": escalation_allowed,
            },
        )


def _router_config() -> RouterNormalizerConfig:
    return RouterNormalizerConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        queue_name="q.source.normalize",
        consumer_group="router-normalizer",
        consumer_name="test",
        block_ms=100,
        batch_size=10,
        normalizer_version="test-normalizer",
        short_url_allowlist=(),
        short_url_hop_limit=1,
        short_url_timeout_seconds=0.1,
        log_level="INFO",
    )


def _assembler_config() -> EvidenceAssemblerConfig:
    return EvidenceAssemblerConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        queue_name="q.candidate.bundle",
        consumer_group="evidence-assembler",
        consumer_name="test",
        batch_size=1,
        block_ms=1,
        bundle_profile_version="bundle_profile_v1",
        enable_text_idea=True,
        enable_reroot=True,
        log_level="INFO",
    )


def _load_fixture(name: str) -> SourceMessageSnapshot:
    payload = json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))
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


def _forbidden_state(ledger: UpstreamHotPathLedger) -> dict:
    return {
        "judge_runs": ledger.judge_runs,
        "judge_outputs": ledger.judge_outputs,
        "analyses": ledger.analyses,
        "notification_plans": ledger.notification_plans,
        "notification_renders": ledger.notification_renders,
        "notification_delivery_records": ledger.notification_delivery_records,
        "replay_requests": ledger.replay_requests,
        "dead_letter_entries": ledger.dead_letter_entries,
        "redis_dispatches": ledger.redis_dispatches,
        "telegram_calls": ledger.telegram_calls,
        "openai_calls": ledger.openai_calls,
        "maintenance_calls": ledger.maintenance_calls,
    }


def _install_downstream_tripwires(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.analysis_router import worker as analysis_router_worker
    from services.analysis_validator import worker as analysis_validator_worker
    from services.judge_openai import openai_client
    from services.maintenance import batch_recovery_tool, worker as maintenance_worker
    from services.notifier_telegram import telegram_client, worker as notifier_worker
    from services.outbox_relay import redis_streams as outbox_redis_streams
    from services.policy_engine import worker as policy_worker

    def fail_downstream(*args, **kwargs):
        raise AssertionError("upstream hot-path acceptance must stop at analysis.requested.v1")

    monkeypatch.setattr(openai_client, "OpenAIJudgeClient", fail_downstream)
    monkeypatch.setattr(analysis_router_worker, "AnalysisRouterWorker", fail_downstream)
    monkeypatch.setattr(analysis_validator_worker, "AnalysisValidatorWorker", fail_downstream)
    monkeypatch.setattr(policy_worker, "PolicyEngineWorker", fail_downstream)
    monkeypatch.setattr(notifier_worker, "NotifierTelegramWorker", fail_downstream)
    monkeypatch.setattr(telegram_client, "TelegramBotClient", fail_downstream)
    monkeypatch.setattr(outbox_redis_streams, "RedisStreamsPublisher", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "MaintenanceQueueWorker", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "ReplayQueueWorker", fail_downstream)
    monkeypatch.setattr(maintenance_worker, "DueRetryPromotionWorker", fail_downstream)
    monkeypatch.setattr(batch_recovery_tool, "DeliveryBatchRecoveryTool", fail_downstream)


@pytest.mark.asyncio
async def test_github_source_message_reaches_analysis_requested_without_downstream_execution(monkeypatch) -> None:
    _install_downstream_tripwires(monkeypatch)
    ledger = UpstreamHotPathLedger(_load_fixture("source_message_github_repo_signal.json"))
    before_forbidden = deepcopy(_forbidden_state(ledger))

    router_result = await RouterNormalizerService(_router_config(), repository=ledger).process_stream_message(
        RedisNormalizeMessage(
            job_id=str(ledger.source_event_id),
            stage_name="normalize",
            root_object_type="source_message",
            root_object_id=str(ledger.snapshot.source_message_id),
            idempotency_key="fixture-source-message",
            trigger_event_id=str(ledger.source_event_id),
        )
    )

    assert router_result.candidate_eligible is True
    assert router_result.candidate_group_count == 1
    candidate_group_id = next(iter(ledger.candidate_group_proposals))
    ledger.seed_ready_snapshots_for_candidate(candidate_group_id)
    bundle_refresh_event_id = ledger.append_candidate_bundle_refresh_event(candidate_group_id)

    first_results = await EvidenceAssemblerService(_assembler_config(), repository=ledger).handle_trigger_event(
        bundle_refresh_event_id
    )
    second_results = await EvidenceAssemblerService(_assembler_config(), repository=ledger).handle_trigger_event(
        bundle_refresh_event_id
    )

    analysis_events = [row for row in ledger.event_outbox if row["event_type"] == "analysis.requested.v1"]
    assert len(ledger.normalization_runs) == 1
    assert len(ledger.artifact_registry) == 1
    assert len(ledger.artifact_observations) == 1
    assert len(ledger.candidate_group_proposals) == 1
    assert len(ledger.candidate_group_members) == 1
    assert len(ledger.artifact_snapshots) == 1
    assert len(ledger.candidate_evidence_bundles) == 1
    assert len(next(iter(ledger.candidate_evidence_members.values()))) == 1
    assert len(analysis_events) == 1
    assert first_results[0].ready_for_analysis is True
    assert first_results[0].emitted_analysis_requested is True
    assert second_results[0].reused_existing_bundle is True
    assert second_results[0].emitted_analysis_requested is False
    payload = analysis_events[0]["payload_json"]
    bundle_id = next(iter(ledger.candidate_evidence_bundles))
    assert analysis_events[0]["aggregate_type"] == "candidate_group"
    assert analysis_events[0]["aggregate_id"] == candidate_group_id
    assert payload["candidate_group_id"] == str(candidate_group_id)
    assert payload["bundle_id"] == str(bundle_id)
    assert payload["judge_profile"] == "github_primary"
    assert payload["escalation_allowed"] is True
    assert {row["event_type"] for row in ledger.event_outbox} == {
        "source_message.created.v1",
        "artifact.enrich.requested.v1",
        "candidate.bundle.refresh.v1",
        "analysis.requested.v1",
    }
    assert _forbidden_state(ledger) == before_forbidden
