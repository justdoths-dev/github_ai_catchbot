from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, AsyncIterator, Protocol
from urllib.parse import urlparse
from uuid import UUID

import sqlalchemy as sa

from ..collector_telegram.operator_supplied_source import (
    OperatorSourceIngestResult,
    OperatorSuppliedSourceAdapter,
    OperatorSuppliedSourceError,
    OperatorSuppliedTelegramSourcePacket,
    TelegramRegistryTarget,
    build_source_projection,
    fingerprint_value,
    load_operator_source_packet,
)
from ..evidence_assembler.config import EvidenceAssemblerConfig
from ..router_normalizer.canonicalizer import build_text_idea_artifact, canonicalize_resolved_urls
from ..router_normalizer.config import RouterNormalizerConfig
from ..router_normalizer.models import RedisNormalizeMessage, ResolvedUrl, SourceMessageSnapshot
from ..router_normalizer.text_surfaces import build_text_surfaces
from ..router_normalizer.trigger_rules import evaluate_triggers
from ..router_normalizer.url_extraction import extract_urls


SCHEMA_VERSION = "exact_target_source_to_analysis_materializer_report_v2"
CONFIRM_TOKEN = "materialize-source-analysis"
PROVIDER_LIVE_CONFIRM_TOKEN = "live-github-provider-evidence"
PROVIDER_RESUME_CONFIRM_TOKEN = "resume-live-github-provider-evidence"
PLACEHOLDER_REDIS_URL = "redis_locator_not_attempted"
REPO_ROOT = Path(__file__).resolve().parents[3]

RUNTIME_VALUE_KEYS = {
    "APP_ENV",
    "DATABASE_URL",
    "REDIS_URL",
    "ROUTER_NORMALIZER_QUEUE",
    "ROUTER_NORMALIZER_CONSUMER_GROUP",
    "ROUTER_NORMALIZER_CONSUMER_NAME",
    "ROUTER_NORMALIZER_BLOCK_MS",
    "ROUTER_NORMALIZER_BATCH_SIZE",
    "ROUTER_NORMALIZER_VERSION",
    "ROUTER_NORMALIZER_SHORT_URL_ALLOWLIST",
    "ROUTER_NORMALIZER_SHORT_URL_HOP_LIMIT",
    "ROUTER_NORMALIZER_SHORT_URL_TIMEOUT_SECONDS",
    "EVIDENCE_ASSEMBLER_QUEUE_NAME",
    "EVIDENCE_ASSEMBLER_CONSUMER_GROUP",
    "EVIDENCE_ASSEMBLER_CONSUMER_NAME",
    "EVIDENCE_ASSEMBLER_BATCH_SIZE",
    "EVIDENCE_ASSEMBLER_BLOCK_MS",
    "EVIDENCE_ASSEMBLER_BUNDLE_PROFILE_VERSION",
    "EVIDENCE_ASSEMBLER_ENABLE_TEXT_IDEA",
    "EVIDENCE_ASSEMBLER_ENABLE_REROOT",
    "GH_ENRICHER_QUEUE_NAME",
    "GH_ENRICHER_CONSUMER_GROUP",
    "GH_ENRICHER_CONSUMER_NAME",
    "GH_ENRICHER_BATCH_SIZE",
    "GH_ENRICHER_BLOCK_MS",
    "GITHUB_API_BASE_URL",
    "GH_ENRICHER_REQUEST_TIMEOUT_SEC",
    "GH_ENRICHER_SAMPLE_MAX_FILES",
    "GH_ENRICHER_SAMPLE_EXCERPT_CHARS",
    "GH_ENRICHER_MAX_FILE_BYTES",
    "GH_ENRICHER_STALE_AFTER_SEC",
    "LOG_LEVEL",
}
RUNTIME_FILE_KEYS = {"DATABASE_URL_FILE"}
RUNTIME_ENV_KEYS = RUNTIME_VALUE_KEYS | RUNTIME_FILE_KEYS


class ExactTargetSourceToAnalysisConfigError(ValueError):
    pass


class SilentArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # pragma: no cover - argparse calls this
        del message
        raise ExactTargetSourceToAnalysisConfigError("invalid_cli_arguments")


@dataclass(slots=True, frozen=True)
class ProviderLiveAuthority:
    allow_live_github_provider_read: bool = False
    allow_provider_snapshot_write: bool = False
    provider_live_confirm: str | None = None

    @property
    def github_live_opened(self) -> bool:
        return (
            self.allow_live_github_provider_read
            and self.allow_provider_snapshot_write
            and self.provider_live_confirm == PROVIDER_LIVE_CONFIRM_TOKEN
        )


@dataclass(slots=True, frozen=True)
class ExistingSourceProviderResumeAuthority:
    allow_existing_source_provider_resume: bool = False
    provider_resume_confirm: str | None = None

    @property
    def args_present(self) -> bool:
        return (
            self.allow_existing_source_provider_resume
            or self.provider_resume_confirm is not None
        )

    @property
    def opened(self) -> bool:
        return (
            self.allow_existing_source_provider_resume
            and self.provider_resume_confirm == PROVIDER_RESUME_CONFIRM_TOKEN
        )


@dataclass(slots=True, frozen=True)
class ExactTargetSourceToAnalysisReport:
    schema_version: str
    mode: str
    status: str
    reason_code: str
    source_packet_fingerprint: str | None
    source_ref_fingerprint: str | None
    source_message_fingerprint: str | None
    source_event_fingerprint: str | None
    artifact_fingerprint: str | None
    candidate_group_fingerprint: str | None
    refresh_event_fingerprint: str | None
    artifact_enrichment_request_fingerprint: str | None
    provider_snapshot_update_fingerprint: str | None
    bundle_fingerprint: str | None
    analysis_request_fingerprint: str | None
    preflight_passed: bool
    source_ingest_attempted: bool
    normalization_attempted: bool
    provider_enrichment_attempted: bool
    bundle_refresh_attempted: bool
    assembler_attempted: bool
    source_message_created: bool
    source_version_created: bool
    candidate_created: bool
    provider_snapshot_created: bool
    text_idea_snapshot_created: bool
    bundle_created: bool
    analysis_request_created: bool
    artifact_enrichment_request_created: bool
    openai_attempted: bool
    redis_attempted: bool
    telegram_live_read_attempted: bool
    telegram_send_attempted: bool
    external_network_attempted: bool
    redactions_applied: bool
    bounded_counts: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ExactTargetSourceToAnalysisRequest:
    mode: str
    packet: OperatorSuppliedTelegramSourcePacket
    provider_authority: ProviderLiveAuthority = field(default_factory=ProviderLiveAuthority)
    provider_resume_authority: ExistingSourceProviderResumeAuthority = field(
        default_factory=ExistingSourceProviderResumeAuthority
    )


@dataclass(slots=True, frozen=True)
class RuntimeConfigBundle:
    database_url: str
    values: Mapping[str, str]
    router_config: RouterNormalizerConfig
    assembler_config: EvidenceAssemblerConfig


@dataclass(slots=True, frozen=True)
class LocalSourceRoutingPlan:
    signal_detected: bool
    candidate_eligible: bool
    predicted_candidate_count: int
    primary_artifact_type: str | None
    artifact_fingerprint: str | None
    external_url_count: int
    enrichment_request_count: int
    provider_route_counts: dict[str, int] = field(default_factory=dict)
    reason_code: str | None = None

    @property
    def passed(self) -> bool:
        return self.reason_code is None


@dataclass(slots=True, frozen=True)
class NormalizationReadback:
    normalization_runs: int
    candidate_groups: int
    primary_members: int
    primary_artifact_type: str | None
    primary_artifact_id: UUID | None
    candidate_group_id: UUID | None
    enrichment_requests: int
    enrichment_request_event_id: UUID | None = None
    provider_route: str | None = None
    refresh_mode: str | None = None
    depth_budget: int | None = None
    provider_route_counts: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class RefreshEventRecord:
    event_id: UUID
    created: bool


@dataclass(slots=True, frozen=True)
class ProviderEnrichmentRequest:
    trigger_event_id: UUID
    candidate_group_id: UUID
    artifact_id: UUID
    artifact_type: str
    provider_route: str
    refresh_mode: str
    depth_budget: int
    provider_authority: ProviderLiveAuthority = field(default_factory=ProviderLiveAuthority)


@dataclass(slots=True, frozen=True)
class ProviderEnrichmentResult:
    provider_route: str
    status: str
    emitted_snapshot_updated: bool
    snapshot_id: UUID | None = None
    snapshot_updated_event_id: UUID | None = None
    snapshot_created: bool = False
    external_network_attempted: bool = False
    github_request_count: int = 0
    error_code: str | None = None


@dataclass(slots=True, frozen=True)
class FinalReadback:
    source_messages: int = 0
    source_message_versions: int = 0
    source_created_events: int = 0
    telegram_raw_updates: int = 0
    normalization_runs: int = 0
    candidate_groups: int = 0
    primary_text_idea_members: int = 0
    external_enrichment_requests: int = 0
    provider_snapshots: int = 0
    artifact_snapshot_updated_events: int = 0
    text_idea_snapshots: int = 0
    ready_current_bundles: int = 0
    candidate_evidence_members: int = 0
    analysis_requested_events: int = 0
    judge_runs: int = 0
    judge_call_requested_events: int = 0
    provider_snapshot_updated_event_id: UUID | None = None
    bundle_id: UUID | None = None
    analysis_request_event_id: UUID | None = None

    def to_counts(self) -> dict[str, int]:
        return {
            "source_messages": self.source_messages,
            "source_message_versions": self.source_message_versions,
            "source_created_events": self.source_created_events,
            "telegram_raw_updates": self.telegram_raw_updates,
            "normalization_runs": self.normalization_runs,
            "candidate_groups": self.candidate_groups,
            "primary_text_idea_members": self.primary_text_idea_members,
            "external_enrichment_requests": self.external_enrichment_requests,
            "provider_snapshots": self.provider_snapshots,
            "artifact_snapshot_updated_events": self.artifact_snapshot_updated_events,
            "text_idea_snapshots": self.text_idea_snapshots,
            "ready_current_bundles": self.ready_current_bundles,
            "candidate_evidence_members": self.candidate_evidence_members,
            "analysis_requested_events": self.analysis_requested_events,
            "judge_runs": self.judge_runs,
            "judge_call_requested_events": self.judge_call_requested_events,
        }


@dataclass(slots=True, frozen=True)
class PreflightSnapshot:
    registry_target: TelegramRegistryTarget | None = None
    source_content_hash: str | None = None
    source_message_id: str | None = None
    source_version_no: int | None = None
    local_plan: LocalSourceRoutingPlan | None = None
    reason_code: str | None = None

    @property
    def passed(self) -> bool:
        return self.reason_code is None


class MaterializerRepositoryProtocol(Protocol):
    async def load_normalization_readback(
        self,
        *,
        source_message_id: UUID,
        source_version_no: int,
    ) -> NormalizationReadback: ...

    async def insert_candidate_bundle_refresh_event(
        self,
        *,
        candidate_group_id: UUID,
        source_message_id: UUID,
        source_version_no: int,
        packet_fingerprint: str,
    ) -> RefreshEventRecord: ...

    async def load_final_readback(
        self,
        *,
        source_message_id: UUID,
        source_version_no: int,
        source_content_hash: str,
        chat_id: int,
        message_id: int,
        candidate_group_id: UUID,
    ) -> FinalReadback: ...


class ProviderEnrichmentServiceProtocol(Protocol):
    async def materialize_provider_request(
        self,
        request: ProviderEnrichmentRequest,
    ) -> ProviderEnrichmentResult: ...


class StageComponentsProtocol(Protocol):
    collector_repository: Any
    source_adapter: OperatorSuppliedSourceAdapter
    materializer_repository: MaterializerRepositoryProtocol
    normalizer_service: Any
    provider_enrichment_service: ProviderEnrichmentServiceProtocol
    assembler_service: Any

    async def commit(self) -> None: ...


class StageFactoryProtocol(Protocol):
    def stage(self, stage_name: str) -> AsyncIterator[StageComponentsProtocol]: ...


class NoNetworkShortUrlResolver:
    def __init__(self, allowlist: Sequence[str] = ()) -> None:
        self._allowlist = {host.lower().removeprefix("www.") for host in allowlist}

    async def resolve(self, url: Any) -> Any:
        normalized = _strip_url_fragment(str(url.observed_url))
        resolution_status = "short_url_unresolved" if _url_host(normalized) in self._allowlist else "not_short_url"
        return ResolvedUrl(
            observed_url=str(url.observed_url),
            normalized_url=normalized,
            resolved_url=None,
            source_kind=str(url.source_kind),
            context_path=getattr(url, "context_path", None),
            resolution_status=resolution_status,
        )


class SqlExactTargetSourceToAnalysisRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def load_normalization_readback(
        self,
        *,
        source_message_id: UUID,
        source_version_no: int,
    ) -> NormalizationReadback:
        normalization_runs = await self._count(
            """
            SELECT count(*)
            FROM normalization_runs
            WHERE source_message_id = CAST(:source_message_id AS uuid)
              AND source_version_no = :source_version_no
              AND signal_detected IS TRUE
              AND candidate_eligible IS TRUE
            """,
            {
                "source_message_id": str(source_message_id),
                "source_version_no": source_version_no,
            },
        )
        candidate_groups = await self._count(
            """
            SELECT count(*)
            FROM candidate_group_proposals
            WHERE source_message_id = CAST(:source_message_id AS uuid)
              AND source_version_no = :source_version_no
            """,
            {
                "source_message_id": str(source_message_id),
                "source_version_no": source_version_no,
            },
        )
        primary_rows = await self._rows(
            """
            SELECT cgp.candidate_group_id, ar.artifact_id, ar.artifact_type
            FROM candidate_group_proposals cgp
            JOIN candidate_group_members cgm
              ON cgm.candidate_group_id = cgp.candidate_group_id
             AND cgm.member_role = 'primary'
            JOIN artifact_registry ar ON ar.artifact_id = cgm.artifact_id
            WHERE cgp.source_message_id = CAST(:source_message_id AS uuid)
              AND cgp.source_version_no = :source_version_no
            ORDER BY cgp.created_at ASC, cgp.candidate_group_id ASC
            LIMIT 2
            """,
            {
                "source_message_id": str(source_message_id),
                "source_version_no": source_version_no,
            },
        )
        enrichment_rows = await self._rows(
            """
            SELECT
                event_id,
                payload_json->>'provider_route' AS provider_route,
                payload_json->>'refresh_mode' AS refresh_mode,
                payload_json->>'depth_budget' AS depth_budget
            FROM event_outbox
            WHERE event_type = 'artifact.enrich.requested.v1'
              AND payload_json->>'source_message_id' = :source_message_id
              AND payload_json->>'source_version_no' = :source_version_no_text
            ORDER BY created_at ASC, event_id ASC
            LIMIT 10
            """,
            {
                "source_message_id": str(source_message_id),
                "source_version_no_text": str(source_version_no),
            },
        )
        provider_route_counts: dict[str, int] = {}
        for row in enrichment_rows:
            route = str(row.get("provider_route") or "")
            if route in {"github", "x", "web"}:
                provider_route_counts[route] = provider_route_counts.get(route, 0) + 1
        first = primary_rows[0] if primary_rows else None
        enrichment = enrichment_rows[0] if enrichment_rows else None
        return NormalizationReadback(
            normalization_runs=normalization_runs,
            candidate_groups=candidate_groups,
            primary_members=len(primary_rows),
            primary_artifact_type=None if first is None else str(first["artifact_type"]),
            primary_artifact_id=None if first is None else UUID(str(first["artifact_id"])),
            candidate_group_id=None if first is None else UUID(str(first["candidate_group_id"])),
            enrichment_requests=len(enrichment_rows),
            enrichment_request_event_id=(
                None if not enrichment_rows else UUID(str(enrichment_rows[0]["event_id"]))
            ),
            provider_route=None if enrichment is None else str(enrichment.get("provider_route") or ""),
            refresh_mode=None if enrichment is None else str(enrichment.get("refresh_mode") or ""),
            depth_budget=_int_or_none(None if enrichment is None else enrichment.get("depth_budget")),
            provider_route_counts=provider_route_counts,
        )

    async def insert_candidate_bundle_refresh_event(
        self,
        *,
        candidate_group_id: UUID,
        source_message_id: UUID,
        source_version_no: int,
        packet_fingerprint: str,
    ) -> RefreshEventRecord:
        dedupe_key = (
            "bundle-refresh:operator-source:"
            f"{candidate_group_id}:{source_message_id}:{source_version_no}:{packet_fingerprint}"
        )
        payload = {
            "candidate_group_id": str(candidate_group_id),
            "trigger_kind": "operator_supplied_source_canary",
            "trigger_object_type": "source_message",
            "trigger_object_id": str(source_message_id),
            "source_version_no": source_version_no,
            "refresh_reason": "operator_supplied_canary",
        }
        rows = await self._rows(
            """
            WITH inserted AS (
                INSERT INTO event_outbox (
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    dedupe_key,
                    payload_json,
                    status,
                    created_at
                ) VALUES (
                    'candidate.bundle.refresh.v1',
                    'candidate_group',
                    CAST(:candidate_group_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                RETURNING event_id, TRUE AS created
            )
            SELECT event_id, created FROM inserted
            UNION ALL
            SELECT event_id, FALSE AS created
            FROM event_outbox
            WHERE dedupe_key = :dedupe_key
            LIMIT 1
            """,
            {
                "candidate_group_id": str(candidate_group_id),
                "dedupe_key": dedupe_key,
                "payload_json": _jsonb_dumps(payload),
            },
        )
        if not rows:
            raise OperatorSuppliedSourceError("refresh_event_not_created")
        return RefreshEventRecord(
            event_id=UUID(str(rows[0]["event_id"])),
            created=bool(rows[0]["created"]),
        )

    async def load_final_readback(
        self,
        *,
        source_message_id: UUID,
        source_version_no: int,
        source_content_hash: str,
        chat_id: int,
        message_id: int,
        candidate_group_id: UUID,
    ) -> FinalReadback:
        bundle_id = await self._uuid_or_none(
            """
            SELECT ceb.bundle_id
            FROM candidate_group_proposals cgp
            JOIN candidate_evidence_bundles ceb
              ON ceb.bundle_id = cgp.current_bundle_id
            WHERE cgp.candidate_group_id = CAST(:candidate_group_id AS uuid)
              AND ceb.ready_for_analysis IS TRUE
            LIMIT 2
            """,
            {"candidate_group_id": str(candidate_group_id)},
        )
        analysis_rows: list[Mapping[str, Any]] = []
        if bundle_id is not None:
            analysis_rows = await self._rows(
                """
                SELECT event_id
                FROM event_outbox
                WHERE event_type = 'analysis.requested.v1'
                  AND aggregate_type = 'candidate_group'
                  AND aggregate_id = CAST(:candidate_group_id AS uuid)
                  AND payload_json->>'bundle_id' = :bundle_id
                ORDER BY created_at ASC, event_id ASC
                LIMIT 2
                """,
                {
                    "candidate_group_id": str(candidate_group_id),
                    "bundle_id": str(bundle_id),
                },
            )
        provider_snapshot_updated_event_id = await self._uuid_or_none(
            """
            SELECT eo.event_id
            FROM event_outbox eo
            JOIN candidate_group_members cgm
              ON cgm.artifact_id = eo.aggregate_id
            JOIN artifact_registry ar
              ON ar.artifact_id = cgm.artifact_id
            JOIN artifact_snapshots aps
              ON aps.snapshot_id = ar.current_snapshot_id
            WHERE cgm.candidate_group_id = CAST(:candidate_group_id AS uuid)
              AND eo.event_type = 'artifact.snapshot.updated.v1'
              AND eo.aggregate_type = 'artifact'
              AND aps.provider IN ('github', 'x', 'web')
              AND aps.status IN ('ready', 'partial_ready')
              AND eo.dedupe_key = concat(
                    'artifact:snapshot_updated:',
                    ar.artifact_id::text,
                    ':',
                    aps.snapshot_id::text
                  )
            ORDER BY eo.created_at ASC, eo.event_id ASC
            LIMIT 2
            """,
            {"candidate_group_id": str(candidate_group_id)},
        )
        return FinalReadback(
            source_messages=await self._count(
                "SELECT count(*) FROM source_messages WHERE source_message_id = CAST(:source_message_id AS uuid)",
                {"source_message_id": str(source_message_id)},
            ),
            source_message_versions=await self._count(
                """
                SELECT count(*)
                FROM source_message_versions
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                  AND version_no = :source_version_no
                  AND content_hash = :source_content_hash
                """,
                {
                    "source_message_id": str(source_message_id),
                    "source_version_no": source_version_no,
                    "source_content_hash": source_content_hash,
                },
            ),
            source_created_events=await self._count(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = 'source_message.created.v1'
                  AND aggregate_type = 'source_message'
                  AND aggregate_id = CAST(:source_message_id AS uuid)
                """,
                {"source_message_id": str(source_message_id)},
            ),
            telegram_raw_updates=await self._count(
                """
                SELECT count(*)
                FROM telegram_raw_updates
                WHERE chat_id = :chat_id AND message_id = :message_id
                """,
                {"chat_id": chat_id, "message_id": message_id},
            ),
            normalization_runs=await self._count(
                """
                SELECT count(*)
                FROM normalization_runs
                WHERE source_message_id = CAST(:source_message_id AS uuid)
                  AND source_version_no = :source_version_no
                  AND signal_detected IS TRUE
                  AND candidate_eligible IS TRUE
                """,
                {
                    "source_message_id": str(source_message_id),
                    "source_version_no": source_version_no,
                },
            ),
            candidate_groups=await self._count(
                """
                SELECT count(*)
                FROM candidate_group_proposals
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND source_message_id = CAST(:source_message_id AS uuid)
                  AND source_version_no = :source_version_no
                """,
                {
                    "candidate_group_id": str(candidate_group_id),
                    "source_message_id": str(source_message_id),
                    "source_version_no": source_version_no,
                },
            ),
            primary_text_idea_members=await self._count(
                """
                SELECT count(*)
                FROM candidate_group_members cgm
                JOIN artifact_registry ar ON ar.artifact_id = cgm.artifact_id
                WHERE cgm.candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND cgm.member_role = 'primary'
                  AND ar.artifact_type = 'text_idea'
                """,
                {"candidate_group_id": str(candidate_group_id)},
            ),
            external_enrichment_requests=await self._count(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = 'artifact.enrich.requested.v1'
                  AND payload_json->>'source_message_id' = :source_message_id
                  AND payload_json->>'source_version_no' = :source_version_no_text
                """,
                {
                    "source_message_id": str(source_message_id),
                    "source_version_no_text": str(source_version_no),
                },
            ),
            provider_snapshots=await self._count(
                """
                SELECT count(*)
                FROM candidate_group_members cgm
                JOIN artifact_registry ar ON ar.artifact_id = cgm.artifact_id
                JOIN artifact_snapshots aps ON aps.snapshot_id = ar.current_snapshot_id
                WHERE cgm.candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND aps.provider IN ('github', 'x', 'web')
                """,
                {"candidate_group_id": str(candidate_group_id)},
            ),
            artifact_snapshot_updated_events=await self._count(
                """
                SELECT count(*)
                FROM event_outbox eo
                JOIN candidate_group_members cgm
                  ON cgm.artifact_id = eo.aggregate_id
                WHERE cgm.candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND eo.event_type = 'artifact.snapshot.updated.v1'
                  AND eo.aggregate_type = 'artifact'
                """,
                {"candidate_group_id": str(candidate_group_id)},
            ),
            text_idea_snapshots=await self._count(
                """
                SELECT count(*)
                FROM artifact_snapshot_text_idea asti
                JOIN artifact_snapshots aps ON aps.snapshot_id = asti.snapshot_id
                JOIN candidate_group_members cgm ON cgm.artifact_id = aps.artifact_id
                WHERE cgm.candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND asti.source_message_id = CAST(:source_message_id AS uuid)
                  AND asti.source_version_no = :source_version_no
                  AND aps.provider = 'local_text_idea'
                """,
                {
                    "candidate_group_id": str(candidate_group_id),
                    "source_message_id": str(source_message_id),
                    "source_version_no": source_version_no,
                },
            ),
            ready_current_bundles=await self._count(
                """
                SELECT count(*)
                FROM candidate_group_proposals cgp
                JOIN candidate_evidence_bundles ceb
                  ON ceb.bundle_id = cgp.current_bundle_id
                WHERE cgp.candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND ceb.ready_for_analysis IS TRUE
                """,
                {"candidate_group_id": str(candidate_group_id)},
            ),
            candidate_evidence_members=(
                0
                if bundle_id is None
                else await self._count(
                    """
                    SELECT count(*)
                    FROM candidate_evidence_members
                    WHERE bundle_id = CAST(:bundle_id AS uuid)
                    """,
                    {"bundle_id": str(bundle_id)},
                )
            ),
            analysis_requested_events=len(analysis_rows),
            judge_runs=(
                0
                if bundle_id is None
                else await self._count(
                    "SELECT count(*) FROM judge_runs WHERE bundle_id = CAST(:bundle_id AS uuid)",
                    {"bundle_id": str(bundle_id)},
                )
            ),
            judge_call_requested_events=(
                0
                if bundle_id is None
                else await self._count(
                    """
                    SELECT count(*)
                    FROM event_outbox
                    WHERE event_type = 'judge.call.requested.v1'
                      AND payload_json->>'bundle_id' = :bundle_id
                    """,
                    {"bundle_id": str(bundle_id)},
                )
            ),
            provider_snapshot_updated_event_id=provider_snapshot_updated_event_id,
            bundle_id=bundle_id,
            analysis_request_event_id=(
                None if not analysis_rows else UUID(str(analysis_rows[0]["event_id"]))
            ),
        )

    async def _uuid_or_none(self, query: str, params: Mapping[str, Any]) -> UUID | None:
        rows = await self._rows(query, params)
        if len(rows) != 1:
            return None
        return UUID(str(next(iter(rows[0].values()))))

    async def _count(self, query: str, params: Mapping[str, Any]) -> int:
        result = await self._session.execute(sa.text(query), dict(params))
        return int(result.scalar_one())

    async def _rows(self, query: str, params: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        result = await self._session.execute(sa.text(query), dict(params))
        return list(result.mappings().all())


class SqlStageComponents:
    def __init__(self, session: Any, runtime: RuntimeConfigBundle) -> None:
        from ..collector_telegram.repositories import CollectorRepository
        from ..evidence_assembler.repositories import EvidenceAssemblerRepository
        from ..evidence_assembler.service import EvidenceAssemblerService
        from ..router_normalizer.repositories import RouterNormalizerRepository
        from ..router_normalizer.service import RouterNormalizerService

        self._session = session
        self.collector_repository = CollectorRepository(session)
        self.source_adapter = OperatorSuppliedSourceAdapter()
        self.materializer_repository = SqlExactTargetSourceToAnalysisRepository(session)
        self.normalizer_service = RouterNormalizerService(
            runtime.router_config,
            repository=RouterNormalizerRepository(session),
            short_url_resolver=NoNetworkShortUrlResolver(runtime.router_config.short_url_allowlist),
        )
        self.provider_enrichment_service = SqlProviderEnrichmentService(session, runtime)
        self.assembler_service = EvidenceAssemblerService(
            runtime.assembler_config,
            repository=EvidenceAssemblerRepository(session),
        )

    async def commit(self) -> None:
        await self._session.commit()


class SqlStageFactory:
    def __init__(self, runtime: RuntimeConfigBundle) -> None:
        self._runtime = runtime
        self._engine: Any = None
        self._session_factory: Any = None

    async def __aenter__(self) -> "SqlStageFactory":
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

        self._engine = create_async_engine(self._runtime.database_url, future=True)
        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._engine is not None:
            await self._engine.dispose()

    @asynccontextmanager
    async def stage(self, stage_name: str) -> AsyncIterator[SqlStageComponents]:
        del stage_name
        async with self._session_factory() as session:
            components = SqlStageComponents(session, self._runtime)
            try:
                yield components
            except Exception:
                if session.in_transaction():
                    await session.rollback()
                raise


class SqlProviderEnrichmentService:
    def __init__(
        self,
        session: Any,
        runtime: RuntimeConfigBundle,
        *,
        github_client_factory: Callable[[Any], Any] | None = None,
        repository_factory: Callable[[Any], Any] | None = None,
        track_external_network: bool = True,
    ) -> None:
        self._session = session
        self._runtime = runtime
        self._github_client_factory = github_client_factory
        self._repository_factory = repository_factory
        self._track_external_network = track_external_network

    async def materialize_provider_request(
        self,
        request: ProviderEnrichmentRequest,
    ) -> ProviderEnrichmentResult:
        if not request.provider_authority.github_live_opened:
            return ProviderEnrichmentResult(
                provider_route=request.provider_route,
                status="blocked",
                emitted_snapshot_updated=False,
                error_code="provider_live_authority_required",
            )
        if request.provider_route != "github":
            return ProviderEnrichmentResult(
                provider_route=request.provider_route,
                status="blocked",
                emitted_snapshot_updated=False,
                error_code="provider_route_not_supported_by_live_exact_materializer",
            )
        try:
            gh_config = _build_gh_enricher_config(self._runtime)
            _validate_gh_enricher_caps(gh_config)
        except ExactTargetSourceToAnalysisConfigError as exc:
            return ProviderEnrichmentResult(
                provider_route=request.provider_route,
                status="blocked",
                emitted_snapshot_updated=False,
                error_code=_safe_reason_code(exc),
            )

        from ..gh_enricher.fetch_planner import GitHubFetchPlanner
        from ..gh_enricher.file_sampler import GitHubFileSampler
        from ..gh_enricher.github_client import (
            GitHubAccessDeniedError,
            GitHubClient,
            GitHubClientError,
            GitHubNotFoundError,
            GitHubRateLimitedError,
        )
        from ..gh_enricher.models import ArtifactEnrichmentJob
        from ..gh_enricher.repositories import GhEnricherRepository
        from ..gh_enricher.service import GhEnricherService
        from ..gh_enricher.url_discovery import GitHubUrlDiscovery

        state = _GitHubProviderAttemptState()
        client_factory = self._github_client_factory or (
            lambda config: GitHubClient(
                api_base_url=config.github_api_base_url,
                timeout_sec=config.request_timeout_sec,
                token_provider=None,
            )
        )
        repository_factory = self._repository_factory or (lambda session: GhEnricherRepository(session))
        repository = _SnapshotUpdatedEventCapturingGhRepository(
            repository_factory(self._session),
            session=self._session,
            state=state,
        )
        github_client = _TrackedGitHubClient(
            client_factory(gh_config),
            state,
            request_limit=_github_request_limit(),
            track_external_network=self._track_external_network,
        )
        service = GhEnricherService(
            gh_config,
            repository=repository,  # type: ignore[arg-type]
            github_client=github_client,  # type: ignore[arg-type]
            fetch_planner=GitHubFetchPlanner(),
            file_sampler=GitHubFileSampler(),
            url_discovery=GitHubUrlDiscovery(),
        )
        try:
            result = await service.handle_job(
                ArtifactEnrichmentJob(
                    trigger_event_id=request.trigger_event_id,
                    event_type="artifact.enrich.requested.v1",
                    candidate_group_id=request.candidate_group_id,
                    artifact_id=request.artifact_id,
                    artifact_type=request.artifact_type,  # type: ignore[arg-type]
                    provider_route=request.provider_route,
                    refresh_mode=request.refresh_mode,
                    depth_budget=request.depth_budget,
                )
            )
        except _GitHubRequestCapExceeded:
            return ProviderEnrichmentResult(
                provider_route=request.provider_route,
                status="blocked",
                emitted_snapshot_updated=False,
                external_network_attempted=state.external_network_attempted,
                github_request_count=state.github_request_count,
                error_code="github_request_cap_exceeded",
            )
        except (
            GitHubRateLimitedError,
            GitHubAccessDeniedError,
            GitHubNotFoundError,
            GitHubClientError,
        ) as exc:
            return ProviderEnrichmentResult(
                provider_route=request.provider_route,
                status="failed_transient",
                emitted_snapshot_updated=False,
                snapshot_created=repository.snapshot_created,
                external_network_attempted=state.external_network_attempted,
                github_request_count=state.github_request_count,
                error_code=_github_client_exception_reason(exc) or "provider_github_client_error",
            )
        except Exception:
            return ProviderEnrichmentResult(
                provider_route=request.provider_route,
                status="failed_transient",
                emitted_snapshot_updated=False,
                snapshot_created=repository.snapshot_created,
                external_network_attempted=state.external_network_attempted,
                github_request_count=state.github_request_count,
                error_code=(
                    state.repository_error_code
                    or state.github_error_code
                    or "provider_live_enrichment_failed"
                ),
            )

        if result.error_code is not None:
            return ProviderEnrichmentResult(
                provider_route=request.provider_route,
                status=result.status,
                emitted_snapshot_updated=result.emitted_snapshot_updated,
                snapshot_id=result.snapshot_id,
                snapshot_updated_event_id=repository.snapshot_updated_event_id,
                snapshot_created=repository.snapshot_created,
                external_network_attempted=state.external_network_attempted,
                github_request_count=state.github_request_count,
                error_code=result.error_code,
            )

        if result.status in {"rate_limited", "access_denied", "failed_permanent", "failed_transient"}:
            if state.github_error_code is not None:
                return ProviderEnrichmentResult(
                    provider_route=request.provider_route,
                    status=result.status,
                    emitted_snapshot_updated=result.emitted_snapshot_updated,
                    snapshot_id=result.snapshot_id,
                    snapshot_updated_event_id=repository.snapshot_updated_event_id,
                    snapshot_created=repository.snapshot_created,
                    external_network_attempted=state.external_network_attempted,
                    github_request_count=state.github_request_count,
                    error_code=state.github_error_code,
                )

        return ProviderEnrichmentResult(
            provider_route=request.provider_route,
            status=result.status,
            emitted_snapshot_updated=result.emitted_snapshot_updated,
            snapshot_id=result.snapshot_id,
            snapshot_updated_event_id=repository.snapshot_updated_event_id,
            snapshot_created=repository.snapshot_created,
            external_network_attempted=state.external_network_attempted,
            github_request_count=state.github_request_count,
        )


@dataclass(slots=True)
class _GitHubProviderAttemptState:
    external_network_attempted: bool = False
    github_request_count: int = 0
    github_error_code: str | None = None
    repository_error_code: str | None = None


class _GitHubRequestCapExceeded(Exception):
    pass


class _TrackedGitHubClient:
    def __init__(
        self,
        github_client: Any,
        state: _GitHubProviderAttemptState,
        *,
        request_limit: int,
        track_external_network: bool,
    ) -> None:
        self._github_client = github_client
        self._state = state
        self._request_limit = request_limit
        self._track_external_network = track_external_network

    async def get_repo(self, owner: str, repo: str, *, auth_mode: str) -> dict[str, Any]:
        self._count_request()
        try:
            return await self._github_client.get_repo(owner, repo, auth_mode=auth_mode)
        except Exception as exc:
            self._record_github_error(exc)
            raise

    async def get_tree(
        self,
        owner: str,
        repo: str,
        ref: str,
        *,
        recursive: bool,
        auth_mode: str,
    ) -> dict[str, Any]:
        self._count_request()
        try:
            return await self._github_client.get_tree(
                owner,
                repo,
                ref,
                recursive=recursive,
                auth_mode=auth_mode,
            )
        except Exception as exc:
            self._record_github_error(exc)
            raise

    async def get_contents(
        self,
        owner: str,
        repo: str,
        path: str,
        *,
        ref: str | None,
        auth_mode: str,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        self._count_request()
        try:
            return await self._github_client.get_contents(owner, repo, path, ref=ref, auth_mode=auth_mode)
        except Exception as exc:
            self._record_github_error(exc)
            raise

    async def get_releases(self, owner: str, repo: str, *, auth_mode: str) -> list[dict[str, Any]]:
        self._count_request()
        try:
            return await self._github_client.get_releases(owner, repo, auth_mode=auth_mode)
        except Exception as exc:
            self._record_github_error(exc)
            raise

    async def get_default_branch_head(
        self,
        owner: str,
        repo: str,
        default_branch: str,
        *,
        auth_mode: str,
    ) -> dict[str, Any]:
        self._count_request()
        try:
            return await self._github_client.get_default_branch_head(
                owner,
                repo,
                default_branch,
                auth_mode=auth_mode,
            )
        except Exception as exc:
            self._record_github_error(exc)
            raise

    async def get_gist(self, gist_id: str, *, auth_mode: str) -> dict[str, Any]:
        self._count_request()
        try:
            return await self._github_client.get_gist(gist_id, auth_mode=auth_mode)
        except Exception as exc:
            self._record_github_error(exc)
            raise

    def _count_request(self) -> None:
        self._state.github_request_count += 1
        if self._state.github_request_count > self._request_limit:
            raise _GitHubRequestCapExceeded
        if self._track_external_network:
            self._state.external_network_attempted = True

    def _record_github_error(self, exc: Exception) -> None:
        reason_code = _github_client_exception_reason(exc)
        if reason_code is not None:
            self._state.github_error_code = reason_code


class _SnapshotUpdatedEventCapturingGhRepository:
    def __init__(self, repository: Any, *, session: Any, state: _GitHubProviderAttemptState) -> None:
        self._repository = repository
        self._session = session
        self._state = state
        self.snapshot_updated_event_id: UUID | None = None
        self.snapshot_write_attempted = False
        self.snapshot_created = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._repository, name)

    async def insert_enrichment_run_if_absent(self, **kwargs: Any) -> UUID | None:
        return await self._repository_write("provider_repository_write_failed", "insert_enrichment_run_if_absent", **kwargs)

    async def mark_enrichment_run_started(self, run_id: UUID) -> None:
        await self._repository_write("provider_repository_write_failed", "mark_enrichment_run_started", run_id)

    async def claim_failed_transient_enrichment_run_for_retry(self, **kwargs: Any) -> UUID | None:
        return await self._repository_write(
            "provider_repository_write_failed",
            "claim_failed_transient_enrichment_run_for_retry",
            **kwargs,
        )

    async def load_enrichment_run_status_by_job_idempotency_key(self, **kwargs: Any) -> str | None:
        return await self._repository.load_enrichment_run_status_by_job_idempotency_key(**kwargs)

    async def mark_enrichment_run_finished(self, **kwargs: Any) -> None:
        await self._repository_write("provider_repository_write_failed", "mark_enrichment_run_finished", **kwargs)

    async def insert_snapshot(self, **kwargs: Any) -> UUID:
        self.snapshot_write_attempted = True
        snapshot_id = await self._repository_write(
            "provider_snapshot_write_failed",
            "insert_snapshot",
            **kwargs,
        )
        self.snapshot_created = True
        return snapshot_id

    async def insert_github_repo_child(self, **kwargs: Any) -> None:
        await self._repository_write("provider_snapshot_write_failed", "insert_github_repo_child", **kwargs)

    async def insert_github_file_sample(self, **kwargs: Any) -> None:
        await self._repository_write("provider_snapshot_write_failed", "insert_github_file_sample", **kwargs)

    async def insert_discovered_url(self, **kwargs: Any) -> None:
        await self._repository_write("provider_snapshot_write_failed", "insert_discovered_url", **kwargs)

    async def update_artifact_current_snapshot(self, **kwargs: Any) -> None:
        await self._repository_write("provider_snapshot_write_failed", "update_artifact_current_snapshot", **kwargs)

    async def insert_snapshot_updated_outbox(self, **kwargs: Any) -> UUID | None:
        try:
            event_id = await self._repository.insert_snapshot_updated_outbox(**kwargs)
            if event_id is None:
                event_id = await self._load_existing_snapshot_updated_event_id(
                    artifact_id=kwargs["artifact_id"],
                    snapshot_id=kwargs["snapshot_id"],
                )
        except Exception:
            self._state.repository_error_code = "provider_snapshot_outbox_write_failed"
            raise
        self.snapshot_updated_event_id = event_id
        return event_id

    async def _repository_write(self, reason_code: str, method_name: str, *args: Any, **kwargs: Any) -> Any:
        try:
            return await getattr(self._repository, method_name)(*args, **kwargs)
        except Exception:
            self._state.repository_error_code = reason_code
            raise

    async def _load_existing_snapshot_updated_event_id(
        self,
        *,
        artifact_id: UUID,
        snapshot_id: UUID,
    ) -> UUID | None:
        execute = getattr(self._session, "execute", None)
        if execute is None:
            return None
        result = await execute(
            sa.text(
                """
                SELECT event_id
                FROM event_outbox
                WHERE event_type = 'artifact.snapshot.updated.v1'
                  AND dedupe_key = :dedupe_key
                ORDER BY created_at ASC, event_id ASC
                LIMIT 1
                """
            ),
            {"dedupe_key": f"artifact:snapshot_updated:{artifact_id}:{snapshot_id}"},
        )
        scalar = getattr(result, "scalar_one_or_none", None)
        if scalar is None:
            return None
        row = scalar()
        return UUID(str(row)) if row else None


def _build_gh_enricher_config(runtime: RuntimeConfigBundle) -> Any:
    from ..gh_enricher.config import GhEnricherConfig

    values = runtime.values
    try:
        config = GhEnricherConfig(
            app_env=_read(values, "APP_ENV", runtime.router_config.app_env).lower(),
            database_url=runtime.database_url,
            redis_url=runtime.router_config.redis_url or PLACEHOLDER_REDIS_URL,
            queue_name=_read(values, "GH_ENRICHER_QUEUE_NAME", "q.artifact.enrich.github"),
            consumer_group=_read(values, "GH_ENRICHER_CONSUMER_GROUP", "gh-enricher"),
            consumer_name=_read(values, "GH_ENRICHER_CONSUMER_NAME", "gh-enricher-1"),
            batch_size=int(_read(values, "GH_ENRICHER_BATCH_SIZE", "1")),
            block_ms=int(_read(values, "GH_ENRICHER_BLOCK_MS", "5000")),
            github_api_base_url=_read(values, "GITHUB_API_BASE_URL", "https://api.github.com"),
            github_app_id=None,
            github_installation_id=None,
            github_private_key=None,
            request_timeout_sec=float(_read(values, "GH_ENRICHER_REQUEST_TIMEOUT_SEC", "10")),
            sample_max_files=int(_read(values, "GH_ENRICHER_SAMPLE_MAX_FILES", "20")),
            sample_excerpt_chars=int(_read(values, "GH_ENRICHER_SAMPLE_EXCERPT_CHARS", "1200")),
            max_file_bytes=int(_read(values, "GH_ENRICHER_MAX_FILE_BYTES", "131072")),
            stale_after_sec=int(_read(values, "GH_ENRICHER_STALE_AFTER_SEC", "21600")),
            log_level=_read(values, "LOG_LEVEL", "INFO").upper(),
        )
        config.validate()
        return config
    except Exception:
        raise ExactTargetSourceToAnalysisConfigError("github_provider_runtime_config_invalid") from None


def _validate_gh_enricher_caps(config: Any) -> None:
    from ..gh_enricher.bounded_github_enrich_runner import (
        ALLOWED_GITHUB_API_BASE_URL,
        HARD_MAX_FILE_BYTES,
        HARD_REQUEST_TIMEOUT_SEC,
        HARD_SAMPLE_EXCERPT_CHARS,
        HARD_SAMPLE_MAX_FILES,
        QUEUE_NAME,
    )

    if config.queue_name != QUEUE_NAME:
        raise ExactTargetSourceToAnalysisConfigError("github_provider_queue_name_mismatch")
    if config.github_api_base_url.rstrip("/") != ALLOWED_GITHUB_API_BASE_URL:
        raise ExactTargetSourceToAnalysisConfigError("github_api_base_url_not_allowed")
    if config.request_timeout_sec <= 0 or config.request_timeout_sec > HARD_REQUEST_TIMEOUT_SEC:
        raise ExactTargetSourceToAnalysisConfigError("github_timeout_cap_out_of_range")
    if config.sample_max_files <= 0 or config.sample_max_files > HARD_SAMPLE_MAX_FILES:
        raise ExactTargetSourceToAnalysisConfigError("github_sample_max_files_cap_out_of_range")
    if config.sample_excerpt_chars <= 0 or config.sample_excerpt_chars > HARD_SAMPLE_EXCERPT_CHARS:
        raise ExactTargetSourceToAnalysisConfigError("github_sample_excerpt_cap_out_of_range")
    if config.max_file_bytes <= 0 or config.max_file_bytes > HARD_MAX_FILE_BYTES:
        raise ExactTargetSourceToAnalysisConfigError("github_max_file_bytes_cap_out_of_range")


def _github_request_limit() -> int:
    from ..gh_enricher.bounded_github_enrich_runner import HARD_GITHUB_REQUEST_LIMIT

    return HARD_GITHUB_REQUEST_LIMIT


def build_parser() -> argparse.ArgumentParser:
    parser = SilentArgumentParser(prog="exact-target-source-to-analysis-materializer")
    parser.add_argument("--mode")
    parser.add_argument("--source-packet-json", action="append", default=[])
    parser.add_argument("--env-file")
    parser.add_argument("--confirm", default=None)
    parser.add_argument("--allow-live-github-provider-read", action="store_true")
    parser.add_argument("--allow-provider-snapshot-write", action="store_true")
    parser.add_argument("--provider-live-confirm", default=None)
    parser.add_argument("--allow-existing-source-provider-resume", action="store_true")
    parser.add_argument("--provider-resume-confirm", default=None)
    return parser


async def run_cli(
    argv: Sequence[str] | None = None,
    *,
    emit_json: Callable[[str], None] = print,
    runtime_config_loader: Callable[[str], RuntimeConfigBundle] | None = None,
    stage_factory_builder: Callable[[RuntimeConfigBundle], Any] | None = None,
    repo_root: Path = REPO_ROOT,
) -> int:
    try:
        args = build_parser().parse_args(list(argv) if argv is not None else None)
    except ExactTargetSourceToAnalysisConfigError as exc:
        emit_json(_compact_json(asdict(_report(mode="unknown", status="blocked", reason_code=str(exc)))))
        return 2

    validation_error = _cli_request_error(args)
    mode = str(args.mode) if args.mode in {"plan", "execute"} else "unknown"
    if validation_error is not None:
        emit_json(_compact_json(asdict(_report(mode=mode, status="blocked", reason_code=validation_error))))
        return 2

    if args.mode == "execute" and args.confirm != CONFIRM_TOKEN:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=args.mode,
                        status="blocked",
                        reason_code="materialize_source_analysis_confirm_missing",
                    )
                )
            )
        )
        return 2
    if args.mode == "plan" and args.confirm is not None:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=args.mode,
                        status="blocked",
                        reason_code="confirm_not_allowed_for_plan",
                    )
                )
            )
        )
        return 2
    if args.mode == "plan" and _provider_authority_args_present(args):
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=args.mode,
                        status="blocked",
                        reason_code="provider_live_authority_not_allowed_for_plan",
                    )
                )
            )
        )
        return 2
    if args.mode == "plan" and _provider_resume_authority_args_present(args):
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=args.mode,
                        status="blocked",
                        reason_code="provider_resume_authority_not_allowed_for_plan",
                    )
                )
            )
        )
        return 2

    try:
        packet = load_operator_source_packet(args.source_packet_json[0], repo_root=repo_root)
    except OperatorSuppliedSourceError as exc:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=args.mode,
                        status="blocked",
                        reason_code=exc.reason_code,
                    )
                )
            )
        )
        return 2

    try:
        runtime = (runtime_config_loader or load_runtime_config)(str(args.env_file))
    except ExactTargetSourceToAnalysisConfigError as exc:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=args.mode,
                        status="blocked",
                        reason_code=_safe_reason_code(exc),
                        packet=packet,
                    )
                )
            )
        )
        return 2

    builder = stage_factory_builder or (lambda runtime_config: SqlStageFactory(runtime_config))
    async with builder(runtime) as stage_factory:
        report = await run_exact_target_source_to_analysis_materializer(
            ExactTargetSourceToAnalysisRequest(
                mode=args.mode,
                packet=packet,
                provider_authority=_provider_authority_from_args(args),
                provider_resume_authority=_provider_resume_authority_from_args(args),
            ),
            stage_factory=stage_factory,
        )
    emit_json(_compact_json(asdict(report)))
    return 0 if report.status == "pass" else 2


async def run_exact_target_source_to_analysis_materializer(
    request: ExactTargetSourceToAnalysisRequest,
    *,
    stage_factory: StageFactoryProtocol,
) -> ExactTargetSourceToAnalysisReport:
    report = _report(
        mode=request.mode,
        status="failed",
        reason_code="unhandled_error",
        packet=request.packet,
    )
    try:
        async with stage_factory.stage("preflight") as components:
            preflight = await _load_preflight(
                components,
                request.packet,
                provider_resume_authority=request.provider_resume_authority,
            )
        report = _apply_preflight(report, preflight)
        if not preflight.passed:
            return replace(
                report,
                status="blocked",
                reason_code=preflight.reason_code or "preflight_blocked",
            )
        if request.mode == "plan":
            return replace(report, status="pass", reason_code="plan_ready", preflight_passed=True)

        assert preflight.registry_target is not None
        assert preflight.source_content_hash is not None

        if preflight.source_message_id is not None and request.provider_resume_authority.opened:
            resume = await _load_existing_provider_resume(
                request=request,
                stage_factory=stage_factory,
                report=report,
                preflight=preflight,
            )
            if resume is not None:
                return resume

        report = replace(report, source_ingest_attempted=True)
        async with stage_factory.stage("source_ingest") as components:
            ingest = await components.source_adapter.ingest_source(
                components.collector_repository,
                packet=request.packet,
                registry_target=preflight.registry_target,
            )
            if ingest.duplicate:
                return replace(
                    _apply_ingest(report, ingest),
                    status="blocked",
                    reason_code="source_packet_already_materialized",
                )
            await components.commit()
        report = _apply_ingest(report, ingest)
        if (
            ingest.source_message_id is None
            or ingest.source_version_no is None
            or ingest.source_event_id is None
        ):
            return replace(report, status="failed", reason_code="source_ingest_readback_invalid")

        source_message_id = UUID(ingest.source_message_id)
        source_version_no = int(ingest.source_version_no)
        source_event_id = UUID(ingest.source_event_id)

        report = replace(report, normalization_attempted=True)
        async with stage_factory.stage("normalization") as components:
            await components.normalizer_service.process_stream_message(
                RedisNormalizeMessage(
                    job_id=str(source_event_id),
                    stage_name="normalize",
                    root_object_type="source_message",
                    root_object_id=str(source_message_id),
                    idempotency_key=f"operator-source:{request.packet.packet_fingerprint}",
                    trigger_event_id=str(source_event_id),
                )
            )
            await components.commit()

        async with stage_factory.stage("normalization_readback") as components:
            normalization = await components.materializer_repository.load_normalization_readback(
                source_message_id=source_message_id,
                source_version_no=source_version_no,
            )
        report = _apply_normalization_readback(report, normalization)
        normalization_error = _normalization_error(normalization)
        if normalization_error is not None:
            return replace(report, status="failed", reason_code=normalization_error)
        assert normalization.candidate_group_id is not None

        if normalization.enrichment_requests >= 1:
            return await _run_provider_enrichment_to_analysis(
                request=request,
                stage_factory=stage_factory,
                report=report,
                preflight=preflight,
                source_message_id=source_message_id,
                source_version_no=source_version_no,
                normalization=normalization,
            )

        report = replace(report, bundle_refresh_attempted=True)
        async with stage_factory.stage("refresh_event") as components:
            refresh = await components.materializer_repository.insert_candidate_bundle_refresh_event(
                candidate_group_id=normalization.candidate_group_id,
                source_message_id=source_message_id,
                source_version_no=source_version_no,
                packet_fingerprint=request.packet.packet_fingerprint,
            )
            if not refresh.created:
                return replace(report, status="blocked", reason_code="refresh_event_already_exists")
            await components.commit()
        report = replace(report, refresh_event_fingerprint=_fingerprint(refresh.event_id))

        report = replace(report, assembler_attempted=True)
        async with stage_factory.stage("assembler") as components:
            await components.assembler_service.handle_trigger_event(refresh.event_id)
            await components.commit()

        async with stage_factory.stage("final_readback") as components:
            final = await components.materializer_repository.load_final_readback(
                source_message_id=source_message_id,
                source_version_no=source_version_no,
                source_content_hash=preflight.source_content_hash,
                chat_id=preflight.registry_target.chat_id,
                message_id=request.packet.parsed_ref.message_id,
                candidate_group_id=normalization.candidate_group_id,
            )
        report = _apply_final_readback(report, final)
        final_error = _final_readback_error(final)
        if final_error is not None:
            return replace(report, status="failed", reason_code=final_error)

        return replace(
            report,
            status="pass",
            reason_code="analysis_request_materialized",
            preflight_passed=True,
            source_message_created=True,
            source_version_created=True,
            candidate_created=True,
            text_idea_snapshot_created=True,
            bundle_created=True,
            analysis_request_created=True,
        )
    except OperatorSuppliedSourceError as exc:
        return replace(report, status="blocked", reason_code=exc.reason_code)
    except ExactTargetSourceToAnalysisConfigError as exc:
        return replace(report, status="blocked", reason_code=_safe_reason_code(exc))
    except Exception:
        return replace(report, status="failed", reason_code="unhandled_error")


async def _load_existing_provider_resume(
    *,
    request: ExactTargetSourceToAnalysisRequest,
    stage_factory: StageFactoryProtocol,
    report: ExactTargetSourceToAnalysisReport,
    preflight: PreflightSnapshot,
) -> ExactTargetSourceToAnalysisReport | None:
    if preflight.source_message_id is None:
        return None
    if preflight.source_version_no is None:
        return replace(
            report,
            status="blocked",
            reason_code="provider_resume_source_version_missing",
            preflight_passed=True,
        )
    if preflight.source_version_no != 1:
        return replace(
            report,
            status="blocked",
            reason_code="provider_resume_source_version_cardinality_invalid",
            preflight_passed=True,
        )
    assert preflight.registry_target is not None
    assert preflight.source_content_hash is not None

    source_message_id = UUID(preflight.source_message_id)
    source_version_no = int(preflight.source_version_no)
    async with stage_factory.stage("normalization_readback") as components:
        normalization = await components.materializer_repository.load_normalization_readback(
            source_message_id=source_message_id,
            source_version_no=source_version_no,
        )
    report = _apply_normalization_readback(report, normalization)
    normalization_error = _normalization_error(normalization)
    if normalization_error is not None:
        return replace(report, status="blocked", reason_code=normalization_error, preflight_passed=True)
    resume_request_error = _provider_resume_request_error(normalization)
    if resume_request_error is not None:
        return replace(report, status="blocked", reason_code=resume_request_error, preflight_passed=True)
    assert normalization.candidate_group_id is not None

    async with stage_factory.stage("final_readback") as components:
        final = await components.materializer_repository.load_final_readback(
            source_message_id=source_message_id,
            source_version_no=source_version_no,
            source_content_hash=preflight.source_content_hash,
            chat_id=preflight.registry_target.chat_id,
            message_id=request.packet.parsed_ref.message_id,
            candidate_group_id=normalization.candidate_group_id,
        )
    report = _apply_final_readback(report, final)
    resume_readback_error = _provider_resume_pre_provider_readback_error(final)
    if resume_readback_error == "existing_source_analysis_already_materialized":
        return replace(
            report,
            status="pass",
            reason_code=resume_readback_error,
            preflight_passed=True,
        )
    if resume_readback_error is not None:
        return replace(report, status="blocked", reason_code=resume_readback_error, preflight_passed=True)

    if _provider_resume_snapshot_ready_for_assembly(final):
        return await _run_existing_provider_snapshot_to_analysis(
            request=request,
            stage_factory=stage_factory,
            report=report,
            preflight=preflight,
            source_message_id=source_message_id,
            source_version_no=source_version_no,
            normalization=normalization,
            snapshot_updated_event_id=final.provider_snapshot_updated_event_id,
        )

    return await _run_provider_enrichment_to_analysis(
        request=request,
        stage_factory=stage_factory,
        report=report,
        preflight=preflight,
        source_message_id=source_message_id,
        source_version_no=source_version_no,
        normalization=normalization,
    )


async def _run_existing_provider_snapshot_to_analysis(
    *,
    request: ExactTargetSourceToAnalysisRequest,
    stage_factory: StageFactoryProtocol,
    report: ExactTargetSourceToAnalysisReport,
    preflight: PreflightSnapshot,
    source_message_id: UUID,
    source_version_no: int,
    normalization: NormalizationReadback,
    snapshot_updated_event_id: UUID | None,
) -> ExactTargetSourceToAnalysisReport:
    if snapshot_updated_event_id is None:
        return replace(
            report,
            status="blocked",
            reason_code="provider_resume_snapshot_event_ambiguous",
            preflight_passed=True,
        )
    assert preflight.registry_target is not None
    assert preflight.source_content_hash is not None
    assert normalization.candidate_group_id is not None

    report = replace(
        report,
        assembler_attempted=True,
        provider_snapshot_update_fingerprint=_fingerprint(snapshot_updated_event_id),
    )
    async with stage_factory.stage("assembler") as components:
        await components.assembler_service.handle_trigger_event(snapshot_updated_event_id)
        await components.commit()

    async with stage_factory.stage("final_readback") as components:
        final = await components.materializer_repository.load_final_readback(
            source_message_id=source_message_id,
            source_version_no=source_version_no,
            source_content_hash=preflight.source_content_hash,
            chat_id=preflight.registry_target.chat_id,
            message_id=request.packet.parsed_ref.message_id,
            candidate_group_id=normalization.candidate_group_id,
        )
    report = _apply_final_readback(report, final)
    provider_final_error = _provider_final_readback_error(final)
    if provider_final_error is not None:
        return replace(report, status="failed", reason_code=provider_final_error)
    return replace(
        report,
        status="pass",
        reason_code="source_url_provider_evidence_analysis_requested",
        preflight_passed=True,
        bundle_created=True,
        analysis_request_created=True,
    )


async def _run_provider_enrichment_to_analysis(
    *,
    request: ExactTargetSourceToAnalysisRequest,
    stage_factory: StageFactoryProtocol,
    report: ExactTargetSourceToAnalysisReport,
    preflight: PreflightSnapshot,
    source_message_id: UUID,
    source_version_no: int,
    normalization: NormalizationReadback,
) -> ExactTargetSourceToAnalysisReport:
    provider_request_error = _provider_request_error(normalization)
    if provider_request_error is not None:
        return replace(report, status="failed", reason_code=provider_request_error)
    assert preflight.registry_target is not None
    assert preflight.source_content_hash is not None
    assert normalization.enrichment_request_event_id is not None
    assert normalization.candidate_group_id is not None
    assert normalization.primary_artifact_id is not None
    assert normalization.primary_artifact_type is not None
    assert normalization.provider_route is not None
    assert normalization.refresh_mode is not None
    assert normalization.depth_budget is not None

    provider_request = ProviderEnrichmentRequest(
        trigger_event_id=normalization.enrichment_request_event_id,
        candidate_group_id=normalization.candidate_group_id,
        artifact_id=normalization.primary_artifact_id,
        artifact_type=normalization.primary_artifact_type,
        provider_route=normalization.provider_route,
        refresh_mode=normalization.refresh_mode,
        depth_budget=normalization.depth_budget,
        provider_authority=request.provider_authority,
    )
    report = replace(report, provider_enrichment_attempted=True)
    async with stage_factory.stage("provider_enrichment") as components:
        provider = await components.provider_enrichment_service.materialize_provider_request(
            provider_request
        )
        await components.commit()
    report = _apply_provider_enrichment_result(report, provider)

    async with stage_factory.stage("final_readback") as components:
        final = await components.materializer_repository.load_final_readback(
            source_message_id=source_message_id,
            source_version_no=source_version_no,
            source_content_hash=preflight.source_content_hash,
            chat_id=preflight.registry_target.chat_id,
            message_id=request.packet.parsed_ref.message_id,
            candidate_group_id=normalization.candidate_group_id,
        )
    report = _apply_final_readback(report, final)
    if provider.error_code is not None:
        provider_error = _provider_not_ready_readback_error(final)
        if provider_error is not None:
            return replace(report, status="failed", reason_code=provider_error)
        return replace(
            report,
            status="blocked",
            reason_code=provider.error_code,
            preflight_passed=True,
            analysis_request_created=False,
        )
    if not _provider_result_ready_for_assembly(provider):
        provider_error = _provider_not_ready_readback_error(final)
        if provider_error is not None:
            return replace(report, status="failed", reason_code=provider_error)
        return replace(
            report,
            status="pass",
            reason_code=_provider_not_ready_reason(provider),
            preflight_passed=True,
            analysis_request_created=False,
        )
    if provider.snapshot_updated_event_id is None:
        return replace(
            report,
            status="failed",
            reason_code="provider_snapshot_updated_event_missing",
        )

    report = replace(report, assembler_attempted=True)
    async with stage_factory.stage("assembler") as components:
        await components.assembler_service.handle_trigger_event(
            provider.snapshot_updated_event_id
        )
        await components.commit()

    async with stage_factory.stage("final_readback") as components:
        final = await components.materializer_repository.load_final_readback(
            source_message_id=source_message_id,
            source_version_no=source_version_no,
            source_content_hash=preflight.source_content_hash,
            chat_id=preflight.registry_target.chat_id,
            message_id=request.packet.parsed_ref.message_id,
            candidate_group_id=normalization.candidate_group_id,
        )
    report = _apply_final_readback(report, final)
    provider_final_error = _provider_final_readback_error(final)
    if provider_final_error is not None:
        return replace(report, status="failed", reason_code=provider_final_error)
    return replace(
        report,
        status="pass",
        reason_code="source_url_provider_evidence_analysis_requested",
        preflight_passed=True,
        bundle_created=True,
        analysis_request_created=True,
    )


def load_runtime_config(env_file: str) -> RuntimeConfigBundle:
    values = _read_runtime_env_file(env_file)
    resolved_values = dict(values)
    database_url = _resolve_file_indirection(
        resolved_values,
        value_key="DATABASE_URL",
        file_key="DATABASE_URL_FILE",
        missing_reason_code="database_url_missing",
        file_missing_reason_code="database_url_file_missing",
        file_empty_reason_code="database_url_file_empty",
    )
    redis_url = _read(resolved_values, "REDIS_URL", PLACEHOLDER_REDIS_URL) or PLACEHOLDER_REDIS_URL
    try:
        router_config = RouterNormalizerConfig(
            app_env=_read(resolved_values, "APP_ENV", "dev").lower(),
            database_url=database_url,
            redis_url=redis_url,
            queue_name=_read(resolved_values, "ROUTER_NORMALIZER_QUEUE", "q.source.normalize"),
            consumer_group=_read(
                resolved_values,
                "ROUTER_NORMALIZER_CONSUMER_GROUP",
                "router-normalizer",
            ),
            consumer_name=_read(
                resolved_values,
                "ROUTER_NORMALIZER_CONSUMER_NAME",
                "router-normalizer-1",
            ),
            block_ms=int(_read(resolved_values, "ROUTER_NORMALIZER_BLOCK_MS", "5000")),
            batch_size=int(_read(resolved_values, "ROUTER_NORMALIZER_BATCH_SIZE", "10")),
            normalizer_version=_read(
                resolved_values,
                "ROUTER_NORMALIZER_VERSION",
                "router-normalizer-v1",
            ),
            short_url_allowlist=tuple(
                host.strip().lower()
                for host in _read(
                    resolved_values,
                    "ROUTER_NORMALIZER_SHORT_URL_ALLOWLIST",
                    "",
                ).split(",")
                if host.strip()
            ),
            short_url_hop_limit=int(
                _read(resolved_values, "ROUTER_NORMALIZER_SHORT_URL_HOP_LIMIT", "1")
            ),
            short_url_timeout_seconds=float(
                _read(resolved_values, "ROUTER_NORMALIZER_SHORT_URL_TIMEOUT_SECONDS", "0.1")
            ),
            log_level=_read(resolved_values, "LOG_LEVEL", "INFO").upper(),
        )
        router_config.validate()
        assembler_config = EvidenceAssemblerConfig(
            app_env=_read(resolved_values, "APP_ENV", "dev").lower(),
            database_url=database_url,
            redis_url=redis_url,
            queue_name=_read(
                resolved_values,
                "EVIDENCE_ASSEMBLER_QUEUE_NAME",
                "q.candidate.bundle",
            ),
            consumer_group=_read(
                resolved_values,
                "EVIDENCE_ASSEMBLER_CONSUMER_GROUP",
                "evidence-assembler",
            ),
            consumer_name=_read(
                resolved_values,
                "EVIDENCE_ASSEMBLER_CONSUMER_NAME",
                "evidence-assembler-1",
            ),
            batch_size=int(_read(resolved_values, "EVIDENCE_ASSEMBLER_BATCH_SIZE", "10")),
            block_ms=int(_read(resolved_values, "EVIDENCE_ASSEMBLER_BLOCK_MS", "5000")),
            bundle_profile_version=_read(
                resolved_values,
                "EVIDENCE_ASSEMBLER_BUNDLE_PROFILE_VERSION",
                "bundle_profile_v1",
            ),
            enable_text_idea=_bool_value(
                _read(resolved_values, "EVIDENCE_ASSEMBLER_ENABLE_TEXT_IDEA", "true")
            ),
            enable_reroot=_bool_value(
                _read(resolved_values, "EVIDENCE_ASSEMBLER_ENABLE_REROOT", "true")
            ),
            log_level=_read(resolved_values, "LOG_LEVEL", "INFO").upper(),
        )
        assembler_config.validate()
    except (TypeError, ValueError):
        raise ExactTargetSourceToAnalysisConfigError("runtime_config_invalid") from None
    return RuntimeConfigBundle(
        database_url=database_url,
        values=resolved_values,
        router_config=router_config,
        assembler_config=assembler_config,
    )


async def _load_preflight(
    components: StageComponentsProtocol,
    packet: OperatorSuppliedTelegramSourcePacket,
    *,
    provider_resume_authority: ExistingSourceProviderResumeAuthority,
) -> PreflightSnapshot:
    registry_target = await components.source_adapter.resolve_registry_target(
        components.collector_repository,
        packet,
    )
    projection = build_source_projection(packet=packet, registry_target=registry_target)
    local_plan = _plan_source_routing(packet=packet, registry_target=registry_target)
    if not local_plan.passed:
        return PreflightSnapshot(
            registry_target=registry_target,
            source_content_hash=projection.content_hash,
            local_plan=local_plan,
            reason_code=local_plan.reason_code,
        )
    existing = await components.source_adapter.inspect_existing_source(
        components.collector_repository,
        packet=packet,
        registry_target=registry_target,
    )
    if existing is not None:
        if provider_resume_authority.opened:
            return PreflightSnapshot(
                registry_target=registry_target,
                source_content_hash=projection.content_hash,
                source_message_id=existing.source_message_id,
                source_version_no=existing.current_version_no,
                local_plan=local_plan,
            )
        reason_code = (
            "provider_resume_authority_required"
            if provider_resume_authority.args_present
            else "source_packet_already_materialized"
        )
        return PreflightSnapshot(
            registry_target=registry_target,
            source_content_hash=projection.content_hash,
            source_message_id=existing.source_message_id,
            source_version_no=existing.current_version_no,
            local_plan=local_plan,
            reason_code=reason_code,
        )
    return PreflightSnapshot(
        registry_target=registry_target,
        source_content_hash=projection.content_hash,
        local_plan=local_plan,
    )


def _plan_source_routing(
    *,
    packet: OperatorSuppliedTelegramSourcePacket,
    registry_target: TelegramRegistryTarget,
) -> LocalSourceRoutingPlan:
    projection = build_source_projection(packet=packet, registry_target=registry_target)
    snapshot = SourceMessageSnapshot(
        source_message_id=UUID("00000000-0000-0000-0000-000000000000"),
        source_version_no=1,
        text_body=projection.text_body,
        caption_text=projection.caption_text,
        text_surface=projection.text_surface,
        entities_json=projection.entities_json,
        url_surface_json=projection.url_surface_json,
        raw_message_json=projection.raw_message_json,
    )
    surfaces = build_text_surfaces(snapshot)
    extracted_urls = extract_urls(snapshot, surfaces)
    if extracted_urls:
        artifacts = canonicalize_resolved_urls(
            [
                ResolvedUrl(
                    observed_url=url.observed_url,
                    normalized_url=url.observed_url,
                    resolved_url=None,
                    source_kind=url.source_kind,
                    context_path=url.context_path,
                )
                for url in extracted_urls
            ]
        )
        evaluation = evaluate_triggers(surfaces, artifacts)
        provider_request_count = sum(1 for artifact in artifacts if artifact.provider_route is not None)
        provider_route_counts = _provider_route_counts(artifacts)
        primary_artifact = artifacts[0] if artifacts else None
        if not evaluation.signal_detected:
            return LocalSourceRoutingPlan(
                signal_detected=False,
                candidate_eligible=False,
                predicted_candidate_count=0,
                primary_artifact_type=None,
                artifact_fingerprint=None,
                external_url_count=len(extracted_urls),
                enrichment_request_count=provider_request_count,
                provider_route_counts=provider_route_counts,
                reason_code="signal_not_detected",
            )
        if not evaluation.candidate_eligible:
            return LocalSourceRoutingPlan(
                signal_detected=True,
                candidate_eligible=False,
                predicted_candidate_count=0,
                primary_artifact_type=None if primary_artifact is None else primary_artifact.artifact_type,
                artifact_fingerprint=(
                    None if primary_artifact is None else fingerprint_value(primary_artifact.canonical_id)
                ),
                external_url_count=len(extracted_urls),
                enrichment_request_count=provider_request_count,
                provider_route_counts=provider_route_counts,
                reason_code="candidate_not_eligible",
            )
        return LocalSourceRoutingPlan(
            signal_detected=True,
            candidate_eligible=True,
            predicted_candidate_count=1,
            primary_artifact_type=None if primary_artifact is None else primary_artifact.artifact_type,
            artifact_fingerprint=(
                None if primary_artifact is None else fingerprint_value(primary_artifact.canonical_id)
            ),
            external_url_count=len(extracted_urls),
            enrichment_request_count=provider_request_count,
            provider_route_counts=provider_route_counts,
        )
    evaluation = evaluate_triggers(surfaces, [])
    if not evaluation.signal_detected:
        return LocalSourceRoutingPlan(
            signal_detected=False,
            candidate_eligible=False,
            predicted_candidate_count=0,
            primary_artifact_type=None,
            artifact_fingerprint=None,
            external_url_count=0,
            enrichment_request_count=0,
            reason_code="signal_not_detected",
        )
    if not evaluation.candidate_eligible:
        return LocalSourceRoutingPlan(
            signal_detected=True,
            candidate_eligible=False,
            predicted_candidate_count=0,
            primary_artifact_type=None,
            artifact_fingerprint=None,
            external_url_count=0,
            enrichment_request_count=0,
            reason_code="candidate_not_eligible",
        )
    artifact = build_text_idea_artifact(surfaces)
    return LocalSourceRoutingPlan(
        signal_detected=True,
        candidate_eligible=True,
        predicted_candidate_count=1,
        primary_artifact_type=artifact.artifact_type,
        artifact_fingerprint=fingerprint_value(artifact.canonical_id),
        external_url_count=0,
        enrichment_request_count=0,
    )


def _provider_route_counts(artifacts: Sequence[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for artifact in artifacts:
        route = getattr(artifact, "provider_route", None)
        if route in {"github", "x", "web"}:
            counts[route] = counts.get(route, 0) + 1
    return counts


def _strip_url_fragment(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl()


def _url_host(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    return host.removeprefix("www.")


def _apply_preflight(
    report: ExactTargetSourceToAnalysisReport,
    preflight: PreflightSnapshot,
) -> ExactTargetSourceToAnalysisReport:
    counts: dict[str, int] = {}
    if preflight.local_plan is not None:
        counts.update(
            {
                "predicted_candidates": preflight.local_plan.predicted_candidate_count,
                "predicted_external_urls": preflight.local_plan.external_url_count,
                "predicted_enrichment_requests": preflight.local_plan.enrichment_request_count,
                **{
                    f"predicted_provider_route_{route}": count
                    for route, count in preflight.local_plan.provider_route_counts.items()
                },
            }
        )
    return replace(
        _with_counts(report, counts),
        source_message_fingerprint=(
            _fingerprint(preflight.source_message_id)
            if preflight.source_message_id is not None
            else report.source_message_fingerprint
        ),
        artifact_fingerprint=(
            preflight.local_plan.artifact_fingerprint
            if preflight.local_plan and preflight.local_plan.artifact_fingerprint
            else report.artifact_fingerprint
        ),
        preflight_passed=preflight.passed,
    )


def _apply_ingest(
    report: ExactTargetSourceToAnalysisReport,
    ingest: OperatorSourceIngestResult,
) -> ExactTargetSourceToAnalysisReport:
    return replace(
        report,
        source_message_fingerprint=_fingerprint(ingest.source_message_id),
        source_event_fingerprint=_fingerprint(ingest.source_event_id),
        source_message_created=ingest.source_message_created,
        source_version_created=ingest.source_version_created,
    )


def _apply_normalization_readback(
    report: ExactTargetSourceToAnalysisReport,
    readback: NormalizationReadback,
) -> ExactTargetSourceToAnalysisReport:
    return replace(
        _with_counts(
            report,
            {
                "normalization_runs": readback.normalization_runs,
                "candidate_groups": readback.candidate_groups,
                "primary_members": readback.primary_members,
                "external_enrichment_requests": readback.enrichment_requests,
                **{
                    f"provider_route_{route}": count
                    for route, count in readback.provider_route_counts.items()
                },
            },
        ),
        candidate_group_fingerprint=_fingerprint(readback.candidate_group_id),
        artifact_fingerprint=_fingerprint(readback.primary_artifact_id),
        candidate_created=readback.candidate_groups == 1,
        artifact_enrichment_request_created=readback.enrichment_requests >= 1,
        artifact_enrichment_request_fingerprint=_fingerprint(readback.enrichment_request_event_id),
    )


def _apply_provider_enrichment_result(
    report: ExactTargetSourceToAnalysisReport,
    result: ProviderEnrichmentResult,
) -> ExactTargetSourceToAnalysisReport:
    return replace(
        _with_counts(
            report,
            {
                "provider_snapshot_results": 1 if result.snapshot_id is not None else 0,
                "provider_snapshot_updated_events": (
                    1 if result.snapshot_updated_event_id is not None else 0
                ),
                "github_request_count": result.github_request_count,
            },
        ),
        provider_snapshot_update_fingerprint=_fingerprint(result.snapshot_updated_event_id),
        provider_snapshot_created=result.snapshot_created,
        external_network_attempted=report.external_network_attempted
        or result.external_network_attempted,
    )


def _apply_final_readback(
    report: ExactTargetSourceToAnalysisReport,
    readback: FinalReadback,
) -> ExactTargetSourceToAnalysisReport:
    return replace(
        _with_counts(report, readback.to_counts()),
        bundle_fingerprint=_fingerprint(readback.bundle_id),
        analysis_request_fingerprint=_fingerprint(readback.analysis_request_event_id),
        provider_snapshot_update_fingerprint=(
            report.provider_snapshot_update_fingerprint
            or _fingerprint(readback.provider_snapshot_updated_event_id)
        ),
        text_idea_snapshot_created=readback.text_idea_snapshots == 1,
        bundle_created=readback.ready_current_bundles == 1,
        analysis_request_created=readback.analysis_requested_events == 1,
    )


def _normalization_error(readback: NormalizationReadback) -> str | None:
    if readback.normalization_runs != 1:
        return "normalization_run_cardinality_invalid"
    if readback.candidate_groups != 1:
        return "candidate_group_cardinality_invalid"
    if readback.primary_members != 1:
        return "primary_member_cardinality_invalid"
    if readback.enrichment_requests >= 1:
        if readback.enrichment_requests != 1:
            return "artifact_enrichment_request_cardinality_invalid"
        if readback.primary_artifact_type == "text_idea":
            return "primary_artifact_type_invalid_for_enrichment"
        if not readback.provider_route_counts:
            return "provider_route_missing"
        if readback.enrichment_request_event_id is None:
            return "artifact_enrichment_request_missing"
        if not readback.provider_route:
            return "provider_route_missing"
        if not readback.refresh_mode:
            return "refresh_mode_missing"
        if readback.depth_budget is None:
            return "depth_budget_missing"
        return None
    if readback.primary_artifact_type != "text_idea":
        return "primary_artifact_type_not_text_idea"
    return None


def _provider_request_error(readback: NormalizationReadback) -> str | None:
    if readback.enrichment_requests != 1:
        return "artifact_enrichment_request_cardinality_invalid"
    if readback.enrichment_request_event_id is None:
        return "artifact_enrichment_request_missing"
    if readback.candidate_group_id is None:
        return "candidate_group_missing"
    if readback.primary_artifact_id is None:
        return "primary_artifact_missing"
    if not readback.primary_artifact_type:
        return "primary_artifact_type_missing"
    if not readback.provider_route:
        return "provider_route_missing"
    if readback.provider_route not in {"github", "x", "web"}:
        return "provider_route_not_allowed"
    if not readback.refresh_mode:
        return "refresh_mode_missing"
    if readback.depth_budget is None:
        return "depth_budget_missing"
    return None


def _provider_resume_request_error(readback: NormalizationReadback) -> str | None:
    request_error = _provider_request_error(readback)
    if request_error is not None:
        return request_error
    if readback.provider_route != "github":
        return "provider_resume_provider_route_not_github"
    return None


def _provider_resume_pre_provider_readback_error(readback: FinalReadback) -> str | None:
    expected_one = {
        "source_messages": readback.source_messages,
        "source_message_versions": readback.source_message_versions,
        "source_created_events": readback.source_created_events,
        "normalization_runs": readback.normalization_runs,
        "candidate_groups": readback.candidate_groups,
        "external_enrichment_requests": readback.external_enrichment_requests,
    }
    for name, count in expected_one.items():
        if count != 1:
            return f"provider_resume_{name}_cardinality_invalid"
    completed = (
        readback.provider_snapshots == 1
        and readback.artifact_snapshot_updated_events == 1
        and readback.ready_current_bundles == 1
        and readback.analysis_requested_events == 1
        and readback.candidate_evidence_members >= 1
        and readback.bundle_id is not None
        and readback.analysis_request_event_id is not None
        and readback.judge_runs == 0
        and readback.judge_call_requested_events == 0
    )
    if completed:
        return "existing_source_analysis_already_materialized"
    if readback.telegram_raw_updates != 0:
        return "provider_resume_telegram_raw_updates_unexpected"
    if readback.primary_text_idea_members != 0:
        return "provider_resume_primary_text_idea_members_unexpected"
    if readback.text_idea_snapshots != 0:
        return "provider_resume_text_idea_snapshots_unexpected"
    if readback.analysis_requested_events != 0:
        return "provider_resume_analysis_already_present"
    if readback.judge_runs != 0 or readback.judge_call_requested_events != 0:
        return "provider_resume_judge_state_unexpected"
    if readback.ready_current_bundles != 0 or readback.candidate_evidence_members != 0:
        return "provider_resume_bundle_already_present"
    if readback.provider_snapshots == 1 and readback.artifact_snapshot_updated_events == 1:
        if readback.provider_snapshot_updated_event_id is None:
            return "provider_resume_snapshot_event_ambiguous"
        return None
    if readback.provider_snapshots != 0 or readback.artifact_snapshot_updated_events != 0:
        return "provider_resume_snapshot_state_ambiguous"
    return None


def _provider_resume_snapshot_ready_for_assembly(readback: FinalReadback) -> bool:
    return (
        readback.provider_snapshots == 1
        and readback.artifact_snapshot_updated_events == 1
        and readback.provider_snapshot_updated_event_id is not None
        and readback.ready_current_bundles == 0
        and readback.candidate_evidence_members == 0
        and readback.analysis_requested_events == 0
        and readback.judge_runs == 0
        and readback.judge_call_requested_events == 0
    )


def _provider_not_ready_readback_error(readback: FinalReadback) -> str | None:
    expected_one = {
        "source_messages": readback.source_messages,
        "source_message_versions": readback.source_message_versions,
        "source_created_events": readback.source_created_events,
        "normalization_runs": readback.normalization_runs,
        "candidate_groups": readback.candidate_groups,
        "external_enrichment_requests": readback.external_enrichment_requests,
    }
    for name, count in expected_one.items():
        if count != 1:
            return f"{name}_cardinality_invalid"
    expected_zero = {
        "telegram_raw_updates": readback.telegram_raw_updates,
        "primary_text_idea_members": readback.primary_text_idea_members,
        "text_idea_snapshots": readback.text_idea_snapshots,
        "ready_current_bundles": readback.ready_current_bundles,
        "candidate_evidence_members": readback.candidate_evidence_members,
        "analysis_requested_events": readback.analysis_requested_events,
        "judge_runs": readback.judge_runs,
        "judge_call_requested_events": readback.judge_call_requested_events,
    }
    for name, count in expected_zero.items():
        if count != 0:
            return f"{name}_unexpected"
    return None


def _provider_final_readback_error(readback: FinalReadback) -> str | None:
    expected_one = {
        "source_messages": readback.source_messages,
        "source_message_versions": readback.source_message_versions,
        "source_created_events": readback.source_created_events,
        "normalization_runs": readback.normalization_runs,
        "candidate_groups": readback.candidate_groups,
        "external_enrichment_requests": readback.external_enrichment_requests,
        "provider_snapshots": readback.provider_snapshots,
        "artifact_snapshot_updated_events": readback.artifact_snapshot_updated_events,
        "ready_current_bundles": readback.ready_current_bundles,
        "analysis_requested_events": readback.analysis_requested_events,
    }
    for key, value in expected_one.items():
        if value != 1:
            return f"{key}_cardinality_invalid"
    expected_zero = {
        "telegram_raw_updates": readback.telegram_raw_updates,
        "primary_text_idea_members": readback.primary_text_idea_members,
        "text_idea_snapshots": readback.text_idea_snapshots,
        "judge_runs": readback.judge_runs,
        "judge_call_requested_events": readback.judge_call_requested_events,
    }
    for key, value in expected_zero.items():
        if value != 0:
            return f"{key}_unexpected"
    if readback.candidate_evidence_members < 1:
        return "candidate_evidence_members_missing"
    if readback.bundle_id is None:
        return "bundle_missing"
    if readback.analysis_request_event_id is None:
        return "analysis_request_missing"
    return None


def _final_readback_error(readback: FinalReadback) -> str | None:
    expected_one = {
        "source_messages": readback.source_messages,
        "source_message_versions": readback.source_message_versions,
        "source_created_events": readback.source_created_events,
        "normalization_runs": readback.normalization_runs,
        "candidate_groups": readback.candidate_groups,
        "primary_text_idea_members": readback.primary_text_idea_members,
        "text_idea_snapshots": readback.text_idea_snapshots,
        "ready_current_bundles": readback.ready_current_bundles,
        "analysis_requested_events": readback.analysis_requested_events,
    }
    for key, value in expected_one.items():
        if value != 1:
            return f"{key}_cardinality_invalid"
    expected_zero = {
        "telegram_raw_updates": readback.telegram_raw_updates,
        "external_enrichment_requests": readback.external_enrichment_requests,
        "provider_snapshots": readback.provider_snapshots,
        "artifact_snapshot_updated_events": readback.artifact_snapshot_updated_events,
        "judge_runs": readback.judge_runs,
        "judge_call_requested_events": readback.judge_call_requested_events,
    }
    for key, value in expected_zero.items():
        if value != 0:
            return f"{key}_unexpected"
    if readback.candidate_evidence_members < 1:
        return "candidate_evidence_members_missing"
    if readback.bundle_id is None:
        return "bundle_missing"
    if readback.analysis_request_event_id is None:
        return "analysis_request_missing"
    return None


def _provider_result_ready_for_assembly(result: ProviderEnrichmentResult) -> bool:
    if result.error_code is not None:
        return False
    if result.snapshot_updated_event_id is None:
        return False
    if not result.emitted_snapshot_updated:
        return False
    return result.status in {"ready", "partial_ready"}


def _provider_not_ready_reason(result: ProviderEnrichmentResult) -> str:
    if result.status in {"pending", "fetching"}:
        return "provider_enrichment_run_in_progress"
    if result.status == "low_evidence":
        return "provider_evidence_low_evidence"
    if result.status in {"failed_transient", "failed_permanent", "rate_limited", "access_denied", "unsupported"}:
        return "provider_evidence_unusable"
    if not result.emitted_snapshot_updated:
        return "provider_snapshot_not_updated"
    return "provider_evidence_not_ready"


def _github_client_exception_reason(exc: Exception) -> str | None:
    from ..gh_enricher.github_client import (
        GitHubAccessDeniedError,
        GitHubClientError,
        GitHubNotFoundError,
        GitHubRateLimitedError,
    )

    if isinstance(exc, GitHubRateLimitedError):
        return "provider_github_rate_limited"
    if isinstance(exc, GitHubAccessDeniedError):
        return "provider_github_access_denied"
    if isinstance(exc, GitHubNotFoundError):
        return "provider_github_not_found"
    if isinstance(exc, GitHubClientError):
        return "provider_github_client_error"
    return None


def _report(
    *,
    mode: str,
    status: str,
    reason_code: str,
    packet: OperatorSuppliedTelegramSourcePacket | None = None,
) -> ExactTargetSourceToAnalysisReport:
    return ExactTargetSourceToAnalysisReport(
        schema_version=SCHEMA_VERSION,
        mode=mode,
        status=status,
        reason_code=reason_code,
        source_packet_fingerprint=None if packet is None else packet.packet_fingerprint,
        source_ref_fingerprint=None if packet is None else packet.source_ref_fingerprint,
        source_message_fingerprint=None,
        source_event_fingerprint=None,
        artifact_fingerprint=None,
        candidate_group_fingerprint=None,
        refresh_event_fingerprint=None,
        artifact_enrichment_request_fingerprint=None,
        provider_snapshot_update_fingerprint=None,
        bundle_fingerprint=None,
        analysis_request_fingerprint=None,
        preflight_passed=False,
        source_ingest_attempted=False,
        normalization_attempted=False,
        provider_enrichment_attempted=False,
        bundle_refresh_attempted=False,
        assembler_attempted=False,
        source_message_created=False,
        source_version_created=False,
        candidate_created=False,
        provider_snapshot_created=False,
        text_idea_snapshot_created=False,
        bundle_created=False,
        analysis_request_created=False,
        artifact_enrichment_request_created=False,
        openai_attempted=False,
        redis_attempted=False,
        telegram_live_read_attempted=False,
        telegram_send_attempted=False,
        external_network_attempted=False,
        redactions_applied=True,
        bounded_counts={},
    )


def _cli_request_error(args: argparse.Namespace) -> str | None:
    if args.mode not in {"plan", "execute"}:
        return "mode_required"
    if len(args.source_packet_json) != 1:
        return "exactly_one_source_packet_json_required"
    if not args.env_file:
        return "env_file_required"
    return None


def _provider_authority_args_present(args: argparse.Namespace) -> bool:
    return (
        bool(args.allow_live_github_provider_read)
        or bool(args.allow_provider_snapshot_write)
        or args.provider_live_confirm is not None
    )


def _provider_resume_authority_args_present(args: argparse.Namespace) -> bool:
    return (
        bool(args.allow_existing_source_provider_resume)
        or args.provider_resume_confirm is not None
    )


def _provider_authority_from_args(args: argparse.Namespace) -> ProviderLiveAuthority:
    return ProviderLiveAuthority(
        allow_live_github_provider_read=bool(args.allow_live_github_provider_read),
        allow_provider_snapshot_write=bool(args.allow_provider_snapshot_write),
        provider_live_confirm=args.provider_live_confirm,
    )


def _provider_resume_authority_from_args(args: argparse.Namespace) -> ExistingSourceProviderResumeAuthority:
    return ExistingSourceProviderResumeAuthority(
        allow_existing_source_provider_resume=bool(args.allow_existing_source_provider_resume),
        provider_resume_confirm=args.provider_resume_confirm,
    )


def _read_runtime_env_file(env_file: str) -> dict[str, str]:
    path = Path(env_file)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        raise ExactTargetSourceToAnalysisConfigError("env_file_missing") from None
    except OSError:
        raise ExactTargetSourceToAnalysisConfigError("env_file_unreadable") from None

    values: dict[str, str] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in RUNTIME_ENV_KEYS:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    if not values:
        raise ExactTargetSourceToAnalysisConfigError("env_file_no_runtime_config")
    return values


def _resolve_file_indirection(
    values: dict[str, str],
    *,
    value_key: str,
    file_key: str,
    missing_reason_code: str,
    file_missing_reason_code: str,
    file_empty_reason_code: str,
) -> str:
    direct = values.get(value_key, "").strip()
    if direct:
        return direct
    file_path = values.get(file_key, "").strip()
    if not file_path:
        raise ExactTargetSourceToAnalysisConfigError(missing_reason_code)
    path = Path(file_path)
    if not path.is_file():
        raise ExactTargetSourceToAnalysisConfigError(file_missing_reason_code)
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError:
        raise ExactTargetSourceToAnalysisConfigError(file_missing_reason_code) from None
    if not value:
        raise ExactTargetSourceToAnalysisConfigError(file_empty_reason_code)
    return value


def _read(values: Mapping[str, str], key: str, default: str) -> str:
    value = values.get(key, default)
    return value.strip() if isinstance(value, str) else default


def _bool_value(value: str) -> bool:
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _safe_reason_code(exc: Exception) -> str:
    value = str(exc)
    if not value:
        return "configuration_error"
    if re_match := re.fullmatch(r"[a-z0-9_]{1,80}", value):
        return re_match.group(0)
    return "configuration_error"


def _with_counts(
    report: ExactTargetSourceToAnalysisReport,
    counts: Mapping[str, int],
) -> ExactTargetSourceToAnalysisReport:
    merged = dict(report.bounded_counts)
    merged.update({key: _bounded_count(value) for key, value in counts.items()})
    return replace(report, bounded_counts=merged)


def _bounded_count(value: int | None) -> int:
    if value is None or value <= 0:
        return 0
    if value == 1:
        return 1
    return 2


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _fingerprint(value: Any) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]


def _json_default(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"unsupported json type: {type(value)!r}")


def _jsonb_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


def _compact_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run_cli(argv))


if __name__ == "__main__":
    raise SystemExit(main())
