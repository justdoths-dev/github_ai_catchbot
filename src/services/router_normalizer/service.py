from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import replace
from uuid import UUID

from .canonicalizer import build_text_idea_artifact, canonicalize_resolved_urls
from .config import RouterNormalizerConfig
from .models import (
    TriggerEvaluation,
    CanonicalArtifact,
    NormalizationResult,
    RedisNormalizeMessage,
    SourceMessageSnapshot,
)
from .repositories import RouterNormalizerRepository
from .short_url_resolver import ShortUrlResolver
from .text_surfaces import build_text_surfaces
from .trigger_rules import evaluate_triggers
from .url_extraction import extract_urls


class RouterNormalizerService:
    """Deterministic source-message router-normalizer."""

    def __init__(
        self,
        config: RouterNormalizerConfig,
        *,
        repository: RouterNormalizerRepository,
        short_url_resolver: ShortUrlResolver | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._short_url_resolver = short_url_resolver or ShortUrlResolver(
            allowlist=config.short_url_allowlist,
            hop_limit=config.short_url_hop_limit,
            timeout_seconds=config.short_url_timeout_seconds,
        )
        self._logger = logger or logging.getLogger(__name__)

    async def process_stream_message(self, message: RedisNormalizeMessage) -> NormalizationResult:
        trigger_event_id = UUID(message.trigger_event_id)
        outbox_event = await self._repository.get_outbox_event(trigger_event_id)
        if outbox_event is None:
            raise ValueError(f"trigger event not found: {trigger_event_id}")
        source_message_id = _source_message_id_from_event(outbox_event.aggregate_id, outbox_event.payload_json)
        requested_version = _requested_version_from_event(outbox_event.payload_json)

        current_snapshot = await self._repository.get_current_source_message(source_message_id)
        if current_snapshot is None:
            raise ValueError(f"source message not found: {source_message_id}")
        if current_snapshot.deleted_at is not None:
            snapshot = _snapshot_with_requested_version(current_snapshot, requested_version)
            return await self._persist_deleted_suppression(snapshot)
        snapshot = await self._snapshot_for_requested_version(current_snapshot, requested_version)

        surfaces = build_text_surfaces(snapshot)
        extracted_urls = extract_urls(snapshot, surfaces)
        resolved_urls = [await self._short_url_resolver.resolve(url) for url in extracted_urls]
        artifacts = _with_inferred_repo_anchors(canonicalize_resolved_urls(resolved_urls))
        evaluation = evaluate_triggers(surfaces, artifacts)
        if evaluation.candidate_eligible and not artifacts:
            artifacts = [build_text_idea_artifact(surfaces)]
        result_hash = _result_hash(
            {
                "normalizer_version": self._config.normalizer_version,
                "source_message_id": str(snapshot.source_message_id),
                "source_version_no": snapshot.source_version_no,
                "hash_surface": surfaces.hash_surface,
                "canonical_ids": [artifact.canonical_id for artifact in artifacts],
                "evaluation": {
                    "signal_detected": evaluation.signal_detected,
                    "candidate_eligible": evaluation.candidate_eligible,
                    "trigger_strength": evaluation.trigger_strength,
                    "reason_codes": evaluation.reason_codes,
                },
            }
        )
        normalization_run_id = await self._repository.upsert_normalization_run(
            source_message_id=snapshot.source_message_id,
            source_version_no=snapshot.source_version_no,
            normalizer_version=self._config.normalizer_version,
            evaluation=evaluation,
            result_hash=result_hash,
        )
        artifact_ids = await self._persist_artifacts(snapshot=snapshot, artifacts=artifacts)
        if not evaluation.candidate_eligible:
            for reason_code in evaluation.reason_codes:
                await self._repository.insert_suppression_trace(
                    normalization_run_id=normalization_run_id,
                    reason_code=reason_code,
                    trigger_strength=evaluation.trigger_strength,
                    notes_json=evaluation.notes,
                )
            return NormalizationResult(
                normalization_run_id=normalization_run_id,
                signal_detected=evaluation.signal_detected,
                candidate_eligible=False,
                trigger_strength=evaluation.trigger_strength,
                artifact_count=len(artifacts),
                candidate_group_count=0,
                suppression_reason_codes=evaluation.reason_codes,
            )

        candidate_group_count = await self._persist_candidate_flow(
            snapshot=snapshot,
            artifacts=artifacts,
            artifact_ids=artifact_ids,
        )
        return NormalizationResult(
            normalization_run_id=normalization_run_id,
            signal_detected=evaluation.signal_detected,
            candidate_eligible=True,
            trigger_strength=evaluation.trigger_strength,
            artifact_count=len(artifacts),
            candidate_group_count=candidate_group_count,
            suppression_reason_codes=[],
        )

    async def _snapshot_for_requested_version(
        self,
        current_snapshot: SourceMessageSnapshot,
        requested_version: int | None,
    ) -> SourceMessageSnapshot:
        if requested_version is None or requested_version == current_snapshot.source_version_no:
            return current_snapshot
        version_snapshot = await self._repository.get_source_message_version(
            source_message_id=current_snapshot.source_message_id,
            version_no=requested_version,
        )
        if version_snapshot is None:
            raise ValueError(
                f"source message version not found: {current_snapshot.source_message_id} v{requested_version}"
            )
        return version_snapshot

    async def _persist_deleted_suppression(self, snapshot: SourceMessageSnapshot) -> NormalizationResult:
        evaluation = TriggerEvaluation(
            signal_detected=False,
            candidate_eligible=False,
            trigger_strength=None,
            reason_codes=["source_message_deleted_current"],
            notes={"deleted_at": snapshot.deleted_at.isoformat() if snapshot.deleted_at else None},
        )
        result_hash = _result_hash(
            {
                "normalizer_version": self._config.normalizer_version,
                "source_message_id": str(snapshot.source_message_id),
                "source_version_no": snapshot.source_version_no,
                "deleted_at": evaluation.notes["deleted_at"],
                "evaluation": {
                    "signal_detected": evaluation.signal_detected,
                    "candidate_eligible": evaluation.candidate_eligible,
                    "trigger_strength": evaluation.trigger_strength,
                    "reason_codes": evaluation.reason_codes,
                },
            }
        )
        normalization_run_id = await self._repository.upsert_normalization_run(
            source_message_id=snapshot.source_message_id,
            source_version_no=snapshot.source_version_no,
            normalizer_version=self._config.normalizer_version,
            evaluation=evaluation,
            result_hash=result_hash,
        )
        await self._repository.insert_suppression_trace(
            normalization_run_id=normalization_run_id,
            reason_code="source_message_deleted_current",
            trigger_strength=None,
            notes_json=evaluation.notes,
        )
        return NormalizationResult(
            normalization_run_id=normalization_run_id,
            signal_detected=False,
            candidate_eligible=False,
            trigger_strength=None,
            artifact_count=0,
            candidate_group_count=0,
            suppression_reason_codes=["source_message_deleted_current"],
        )

    async def _persist_artifacts(
        self,
        *,
        snapshot: SourceMessageSnapshot,
        artifacts: list[CanonicalArtifact],
    ) -> dict[str, UUID]:
        artifact_ids: dict[str, UUID] = {}
        for artifact in artifacts:
            artifact_id = await self._repository.upsert_artifact_registry(artifact)
            artifact_ids[artifact.canonical_id] = artifact_id
            await self._repository.insert_artifact_observation_if_absent(
                artifact_id=artifact_id,
                source_message_id=snapshot.source_message_id,
                source_version_no=snapshot.source_version_no,
                artifact=artifact,
            )
        return artifact_ids

    async def _persist_candidate_flow(
        self,
        *,
        snapshot: SourceMessageSnapshot,
        artifacts: list[CanonicalArtifact],
        artifact_ids: dict[str, UUID],
    ) -> int:
        group_count = 0
        for primary, related in _candidate_group_plan(artifacts):
            primary_artifact_id = artifact_ids[primary.canonical_id]
            group_id = await self._repository.upsert_candidate_group(
                source_message_id=snapshot.source_message_id,
                source_version_no=snapshot.source_version_no,
                primary_artifact_id=primary_artifact_id,
                normalizer_version=self._config.normalizer_version,
                dedupe_subject_key=primary.canonical_id,
            )
            await self._repository.upsert_candidate_member(
                candidate_group_id=group_id,
                artifact_id=primary_artifact_id,
                member_role="primary",
                member_order=0,
            )
            for order, member in enumerate(related, start=1):
                await self._repository.upsert_candidate_member(
                    candidate_group_id=group_id,
                    artifact_id=artifact_ids[member.canonical_id],
                    member_role="supporting",
                    member_order=order,
                )
            for artifact in [primary, *related]:
                await self._repository.insert_enrichment_requested_outbox(
                    candidate_group_id=group_id,
                    artifact_id=artifact_ids[artifact.canonical_id],
                    artifact=artifact,
                    source_message_id=snapshot.source_message_id,
                    source_version_no=snapshot.source_version_no,
                )
            group_count += 1
        return group_count


def _candidate_group_plan(artifacts: list[CanonicalArtifact]) -> list[tuple[CanonicalArtifact, list[CanonicalArtifact]]]:
    """Plan provisional candidate groups without crossing into enrichment."""
    github_primaries = [artifact for artifact in artifacts if artifact.artifact_type in {"github_repo", "github_gist"}]
    if github_primaries:
        return [
            (
                primary,
                [
                    artifact
                    for artifact in artifacts
                    if artifact.canonical_id != primary.canonical_id
                    and not _is_other_github_primary(artifact, primary)
                    and _supports_github_primary(artifact, primary)
                ],
            )
            for primary in github_primaries
        ]

    for primary_type in ("x_post", "web_article", "text_idea", "unknown_link", "short_url_unresolved"):
        primaries = [artifact for artifact in artifacts if artifact.artifact_type == primary_type]
        if primaries:
            return [
                (
                    primary,
                    [
                        artifact
                        for artifact in artifacts
                        if artifact.canonical_id != primary.canonical_id and artifact.artifact_type != primary_type
                    ],
                )
                for primary in primaries
            ]
    return []


def _is_other_github_primary(artifact: CanonicalArtifact, primary: CanonicalArtifact) -> bool:
    return artifact.artifact_type in {"github_repo", "github_gist"} and artifact.canonical_id != primary.canonical_id


def _supports_github_primary(artifact: CanonicalArtifact, primary: CanonicalArtifact) -> bool:
    if artifact.artifact_type in {"github_subpath", "github_repo_page"}:
        return artifact.inferred_repo is not None and artifact.inferred_repo.canonical_id == primary.canonical_id
    return True


def _with_inferred_repo_anchors(artifacts: list[CanonicalArtifact]) -> list[CanonicalArtifact]:
    output: list[CanonicalArtifact] = []
    seen: set[str] = set()
    for artifact in artifacts:
        if artifact.inferred_repo is not None and artifact.inferred_repo.canonical_id not in seen:
            output.append(artifact.inferred_repo)
            seen.add(artifact.inferred_repo.canonical_id)
        if artifact.canonical_id not in seen:
            output.append(artifact)
            seen.add(artifact.canonical_id)
    return output


def _snapshot_with_requested_version(
    current_snapshot: SourceMessageSnapshot,
    requested_version: int | None,
) -> SourceMessageSnapshot:
    if requested_version is None or requested_version == current_snapshot.source_version_no:
        return current_snapshot
    return replace(current_snapshot, source_version_no=requested_version)


def _source_message_id_from_event(aggregate_id: UUID, payload_json: dict) -> UUID:
    payload_source_id = payload_json.get("source_message_id")
    if isinstance(payload_source_id, str) and payload_source_id.strip():
        return UUID(payload_source_id)
    return aggregate_id


def _requested_version_from_event(payload_json: dict) -> int | None:
    raw = payload_json.get("current_version_no")
    if raw is None:
        raw = payload_json.get("source_version_no")
    if raw is None:
        return None
    return int(raw)


def _result_hash(payload: dict) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
