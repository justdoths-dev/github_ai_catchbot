from __future__ import annotations

from uuid import uuid4

from services.evidence_assembler.config import EvidenceAssemblerConfig
from services.evidence_assembler.models import (
    BundleRefreshTarget,
    CandidateGroupRecord,
    CandidateMemberRecord,
    EvidenceBundleDraft,
    ExistingBundleRecord,
    SnapshotRecord,
)
from services.evidence_assembler.text_idea_builder import TextIdeaBuilder


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


def config(*, enable_reroot: bool = True) -> EvidenceAssemblerConfig:
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
        enable_reroot=enable_reroot,
        log_level="INFO",
    )


def snapshot(artifact_id, *, snapshot_type="github_repo", status="ready") -> SnapshotRecord:
    return SnapshotRecord(
        snapshot_id=uuid4(),
        artifact_id=artifact_id,
        provider="test",
        snapshot_type=snapshot_type,
        status=status,
        fetched_at=None,
        content_anchor=f"anchor:{artifact_id}",
    )


class FakeRepository:
    def __init__(self) -> None:
        self.targets: list[BundleRefreshTarget] = []
        self.candidates: dict[object, CandidateGroupRecord] = {}
        self.members: dict[object, list[CandidateMemberRecord]] = {}
        self.snapshots: dict[object, SnapshotRecord] = {}
        self.source_text = "Build a GitHub automation tool with Python."
        self.reroot_count = 0
        self.current_primary_updates: list[dict] = []
        self.current_bundle_updates: list[dict] = []
        self.bundles: list[tuple[object, EvidenceBundleDraft]] = []
        self.outbox: list[dict] = []
        self.text_idea_snapshots_created = 0
        self._text_idea_by_anchor: dict[tuple[object, str], SnapshotRecord] = {}

    def transaction(self):
        return Tx()

    async def resolve_refresh_targets(self, trigger_event_id):
        return self.targets

    async def load_candidate_group(self, candidate_group_id):
        return self.candidates.get(candidate_group_id)

    async def load_candidate_members(self, candidate_group_id):
        return self.members.get(candidate_group_id, [])

    async def load_current_snapshots(self, artifact_ids):
        wanted = set(artifact_ids)
        return {artifact_id: snap for artifact_id, snap in self.snapshots.items() if artifact_id in wanted}

    async def load_source_message_text_surface(self, **kwargs):
        return self.source_text

    async def ensure_text_idea_snapshot(self, draft):
        content_anchor = TextIdeaBuilder.input_hash(draft)
        key = (draft.artifact_id, content_anchor)
        if key not in self._text_idea_by_anchor:
            self.text_idea_snapshots_created += 1
            self._text_idea_by_anchor[key] = SnapshotRecord(
                snapshot_id=uuid4(),
                artifact_id=draft.artifact_id,
                provider="local_text_idea",
                snapshot_type="text_idea",
                status=draft.status,
                fetched_at=None,
                content_anchor=content_anchor,
                normalized_projection={"display_surface": draft.display_surface},
                evidence_limitations=draft.evidence_limitations,
            )
        return self._text_idea_by_anchor[key]

    async def load_discovered_links(self, **kwargs):
        return []

    async def count_reroot_events(self, candidate_group_id):
        return self.reroot_count

    async def append_reroot_event(self, **kwargs):
        self.reroot_count += 1

    async def update_current_primary(self, **kwargs):
        self.current_primary_updates.append(kwargs)
        candidate = self.candidates[kwargs["candidate_group_id"]]
        self.candidates[kwargs["candidate_group_id"]] = CandidateGroupRecord(
            candidate.candidate_group_id,
            candidate.source_message_id,
            candidate.source_version_no,
            candidate.initial_primary_artifact_id,
            kwargs["artifact_id"],
            candidate.proposal_status,
            candidate.current_bundle_id,
        )

    async def load_existing_bundle(self, **kwargs):
        for bundle_id, draft in self.bundles:
            if (
                draft.candidate_group_id == kwargs["candidate_group_id"]
                and draft.bundle_profile_version == kwargs["bundle_profile_version"]
                and draft.bundle_input_hash == kwargs["bundle_input_hash"]
            ):
                return ExistingBundleRecord(
                    bundle_id=bundle_id,
                    candidate_group_id=draft.candidate_group_id,
                    bundle_version=1,
                    bundle_profile_version=draft.bundle_profile_version,
                    bundle_input_hash=draft.bundle_input_hash,
                    ready_for_analysis=draft.ready_for_analysis,
                )
        return None

    async def next_bundle_version(self, candidate_group_id):
        return 1 + sum(1 for _, draft in self.bundles if draft.candidate_group_id == candidate_group_id)

    async def append_bundle(self, **kwargs):
        bundle_id = uuid4()
        self.bundles.append((bundle_id, kwargs["draft"]))
        return bundle_id

    async def update_current_bundle(self, **kwargs):
        self.current_bundle_updates.append(kwargs)

    async def insert_analysis_requested_outbox(self, **kwargs):
        self.outbox.append(kwargs)


def add_candidate(repository: FakeRepository, *, candidate_group_id, primary_artifact_id, artifact_type):
    candidate = CandidateGroupRecord(
        candidate_group_id=candidate_group_id,
        source_message_id=uuid4(),
        source_version_no=1,
        initial_primary_artifact_id=primary_artifact_id,
        current_primary_artifact_id=primary_artifact_id,
        proposal_status="ready_for_enrich",
        current_bundle_id=None,
    )
    repository.candidates[candidate_group_id] = candidate
    repository.members[candidate_group_id] = [
        CandidateMemberRecord(primary_artifact_id, artifact_type, "primary", 0),
    ]
    return candidate
