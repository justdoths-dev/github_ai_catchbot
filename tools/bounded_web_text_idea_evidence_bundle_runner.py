from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, Sequence
from uuid import UUID


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.evidence_assembler.config import (  # noqa: E402
    EvidenceAssemblerConfig,
    EvidenceAssemblerConfigurationError,
)
from src.services.evidence_assembler.models import AssemblyResult, BundleRefreshTarget  # noqa: E402
from src.services.evidence_assembler.repositories import EvidenceAssemblerRepository  # noqa: E402
from src.services.evidence_assembler.service import EvidenceAssemblerService  # noqa: E402
from src.services.evidence_assembler.text_idea_builder import TextIdeaBuilder  # noqa: E402
from src.services.web_enricher.article_parser import ArticleParser  # noqa: E402
from src.services.web_enricher.config import WebEnricherConfig, WebEnricherConfigurationError  # noqa: E402
from src.services.web_enricher.models import (  # noqa: E402
    ArtifactEnrichmentJob,
    EnrichmentResult,
    FetchedDocument,
)
from src.services.web_enricher.repositories import WebEnricherRepository  # noqa: E402
from src.services.web_enricher.service import WebEnricherService  # noqa: E402
from src.services.web_enricher.url_discovery import WebUrlDiscovery  # noqa: E402
from src.services.web_enricher.web_fetch_client import WebFetchClient, WebFetchClientError  # noqa: E402


SCHEMA_VERSION = "web_text_idea_supporting_evidence_bundle_proof_v1"
RUNNER_NAME = "bounded_web_text_idea_evidence_bundle_runner"
PLAN_MODE = "plan"
EXECUTE_MODE = "execute"
CONFIRM_TOKEN = "LIVE_WEB_TEXT_IDEA_EVIDENCE_BUNDLE_EXECUTE"
WEB_READY_STATUSES = frozenset({"ready", "partial_ready", "low_evidence"})
TEXT_IDEA_READY_STATUSES = frozenset({"ready", "low_evidence"})
ALLOWED_PROVIDER_STATUSES = frozenset(
    {
        "ready",
        "partial_ready",
        "low_evidence",
        "rate_limited",
        "access_denied",
        "failed_permanent",
        "failed_transient",
        "unsupported",
    }
)
ALLOWED_WEB_CONTENT_TYPES = frozenset(
    {
        "text/html",
        "application/xhtml+xml",
        "text/plain",
        "text/markdown",
    }
)
HARD_WEB_FETCH_LIMIT = 1
HARD_WEB_TIMEOUT_SEC = 10.0
HARD_WEB_MAX_REDIRECTS = 10
HARD_WEB_MAX_BYTES = 1_048_576
HARD_WEB_MAX_OUTBOUND_LINKS = 100


class CliArgumentError(ValueError):
    pass


class ProofRunnerError(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


@dataclass(frozen=True, slots=True)
class RunnerConfig:
    mode: str
    web_event_id_suffix: str | None = None
    text_idea_event_id_suffix: str | None = None
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    confirm_token: str | None = None
    allow_web_fetch: bool = False
    allow_database_write: bool = False
    allow_artifact_snapshot_write: bool = False
    allow_text_idea_snapshot_write: bool = False
    allow_evidence_bundle_write: bool = False


@dataclass(slots=True)
class RunnerState:
    runtime_config_loaded: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    web_fetch_attempted: bool = False
    web_fetch_count: int = 0
    artifact_snapshot_write_attempted: bool = False
    text_idea_snapshot_write_attempted: bool = False
    evidence_bundle_write_attempted: bool = False


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    web_config: WebEnricherConfig
    assembler_config: EvidenceAssemblerConfig

    @property
    def database_url(self) -> str:
        return self.web_config.database_url


@dataclass(frozen=True, slots=True)
class TargetEventRow:
    event_id: UUID
    event_type: str
    status: str
    aggregate_type: str
    aggregate_id: UUID
    payload_json: dict[str, Any]


@dataclass(frozen=True, slots=True)
class WebTargetResolutionReadback:
    artifact_count: int
    candidate_group_count: int
    candidate_member_count: int
    canonical_url_valid: bool


@dataclass(frozen=True, slots=True)
class TextIdeaTargetReadback:
    target_count: int
    candidate_group_id: UUID | None
    candidate_group_count: int
    source_identity_present: bool
    text_idea_member_count: int
    source_text_present: bool
    current_primary_is_text_idea: bool
    usable_external_snapshot_count: int
    existing_text_idea_snapshot_count: int


@dataclass(frozen=True, slots=True)
class EnrichmentRunReadback:
    artifact_id: UUID
    provider: str
    status: str
    content_anchor: str | None


@dataclass(frozen=True, slots=True)
class WebSnapshotReadback:
    snapshot_id: UUID
    artifact_id: UUID
    provider: str
    snapshot_type: str
    status: str
    content_anchor: str | None
    normalized_projection_present: bool
    web_article_child_count: int
    discovered_url_count: int
    discovered_url_fingerprints: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TextIdeaSnapshotReadback:
    snapshot_id: UUID
    artifact_id: UUID
    provider: str
    snapshot_type: str
    status: str
    content_anchor: str | None
    normalized_projection_present: bool
    text_idea_child_count: int
    matching_snapshot_count: int


@dataclass(frozen=True, slots=True)
class OutboxReadback:
    event_id: UUID
    event_type: str
    status: str


@dataclass(frozen=True, slots=True)
class BundleReadback:
    bundle_id: UUID
    candidate_group_id: UUID
    ready_for_analysis: bool
    current_bundle_consistent: bool


@dataclass(frozen=True, slots=True)
class WebBranchProof:
    status: str = "not_requested"
    reason_code: str = "not_requested"
    target_event_id: UUID | None = None
    artifact_id: UUID | None = None
    candidate_group_id: UUID | None = None
    artifact_type: str | None = None
    provider_route: str | None = None
    snapshot_id: UUID | None = None
    snapshot_status: str | None = None
    content_anchor: str | None = None
    enrichment_run_readback: EnrichmentRunReadback | None = None
    snapshot_readback: WebSnapshotReadback | None = None
    snapshot_updated_outbox: OutboxReadback | None = None
    bundle_id: UUID | None = None
    ready_for_analysis: bool | None = None
    analysis_requested_outbox: OutboxReadback | None = None
    evidence_bundle_written_or_reused: bool = False
    reused_existing_bundle: bool | None = None


@dataclass(frozen=True, slots=True)
class TextIdeaBranchProof:
    status: str = "not_requested"
    reason_code: str = "not_requested"
    target_event_id: UUID | None = None
    candidate_group_id: UUID | None = None
    snapshot_id: UUID | None = None
    artifact_id: UUID | None = None
    snapshot_status: str | None = None
    snapshot_readback: TextIdeaSnapshotReadback | None = None
    bundle_id: UUID | None = None
    ready_for_analysis: bool | None = None
    analysis_requested_outbox: OutboxReadback | None = None
    evidence_bundle_written_or_reused: bool = False
    reused_existing_bundle: bool | None = None
    reused_existing_text_idea_snapshot: bool = False


@dataclass(frozen=True, slots=True)
class RunnerResult:
    status: str
    reason_code: str
    config: RunnerConfig
    state: RunnerState = field(default_factory=RunnerState)
    web: WebBranchProof = field(default_factory=WebBranchProof)
    text_idea: TextIdeaBranchProof = field(default_factory=TextIdeaBranchProof)

    @property
    def ok(self) -> bool:
        return self.status == "pass"

    def to_sanitized_dict(self) -> dict[str, Any]:
        web_snapshot = self.web.snapshot_readback
        text_snapshot = self.text_idea.snapshot_readback
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": self.config.mode,
            "status": self.status,
            "reason_code": self.reason_code,
            "web_branch_status": self.web.status,
            "web_branch_reason_code": self.web.reason_code,
            "text_idea_branch_status": self.text_idea.status,
            "text_idea_branch_reason_code": self.text_idea.reason_code,
            "web_event_fingerprint": _fingerprint_uuid(self.web.target_event_id),
            "text_idea_event_fingerprint": _fingerprint_uuid(self.text_idea.target_event_id),
            "web_artifact_fingerprint": _fingerprint_uuid(self.web.artifact_id),
            "text_idea_artifact_fingerprint": _fingerprint_uuid(self.text_idea.artifact_id),
            "web_candidate_group_fingerprint": _fingerprint_uuid(self.web.candidate_group_id),
            "text_idea_candidate_group_fingerprint": _fingerprint_uuid(self.text_idea.candidate_group_id),
            "web_snapshot_fingerprint": _fingerprint_uuid(self.web.snapshot_id),
            "text_idea_snapshot_fingerprint": _fingerprint_uuid(self.text_idea.snapshot_id),
            "web_bundle_fingerprint": _fingerprint_uuid(self.web.bundle_id),
            "text_idea_bundle_fingerprint": _fingerprint_uuid(self.text_idea.bundle_id),
            "web_snapshot_updated_outbox_fingerprint": _fingerprint_uuid(
                self.web.snapshot_updated_outbox.event_id if self.web.snapshot_updated_outbox else None
            ),
            "web_artifact_type": self.web.artifact_type,
            "web_provider_route": self.web.provider_route,
            "web_fetch_attempted": self.state.web_fetch_attempted,
            "web_fetch_count_bucket": _count_bucket(self.state.web_fetch_count),
            "web_snapshot_status": self.web.snapshot_status,
            "web_content_anchor_fingerprint": _fingerprint_text(self.web.content_anchor),
            "web_article_child_readback": _web_article_child_readback(web_snapshot),
            "web_discovered_url_count_bucket": _count_bucket(
                web_snapshot.discovered_url_count if web_snapshot else 0
            ),
            "web_discovered_url_fingerprints": list(web_snapshot.discovered_url_fingerprints)
            if web_snapshot
            else [],
            "text_idea_snapshot_status": self.text_idea.snapshot_status,
            "text_idea_child_readback": _text_idea_child_readback(text_snapshot),
            "reused_existing_text_idea_snapshot": self.text_idea.reused_existing_text_idea_snapshot,
            "evidence_bundle_written_or_reused": (
                self.web.evidence_bundle_written_or_reused
                or self.text_idea.evidence_bundle_written_or_reused
            ),
            "web_evidence_bundle_written_or_reused": self.web.evidence_bundle_written_or_reused,
            "text_idea_evidence_bundle_written_or_reused": self.text_idea.evidence_bundle_written_or_reused,
            "ready_for_analysis": _combined_ready_for_analysis(self.web, self.text_idea),
            "web_ready_for_analysis": self.web.ready_for_analysis,
            "text_idea_ready_for_analysis": self.text_idea.ready_for_analysis,
            "analysis_requested_outbox_fingerprint": _first_present(
                _fingerprint_uuid(
                    self.web.analysis_requested_outbox.event_id if self.web.analysis_requested_outbox else None
                ),
                _fingerprint_uuid(
                    self.text_idea.analysis_requested_outbox.event_id
                    if self.text_idea.analysis_requested_outbox
                    else None
                ),
            ),
            "web_analysis_requested_outbox_fingerprint": _fingerprint_uuid(
                self.web.analysis_requested_outbox.event_id if self.web.analysis_requested_outbox else None
            ),
            "text_idea_analysis_requested_outbox_fingerprint": _fingerprint_uuid(
                self.text_idea.analysis_requested_outbox.event_id
                if self.text_idea.analysis_requested_outbox
                else None
            ),
            "authority": _authority(self.state),
            "side_effects": _side_effects(self.state),
            "redactions_applied": _redactions_applied(),
            "raw_values_printed": _raw_values_printed(),
            "runtime_config_loaded": self.state.runtime_config_loaded,
            "database_read_attempted": self.state.database_read_attempted,
        }


class ProofDatabase(Protocol):
    async def select_events_by_suffix(self, suffix: str) -> list[TargetEventRow]: ...

    async def load_web_job_by_trigger_event_id(self, event_id: UUID) -> ArtifactEnrichmentJob | None: ...

    async def verify_web_target_resolution(self, job: ArtifactEnrichmentJob) -> WebTargetResolutionReadback: ...

    async def run_web_enricher(self, job: ArtifactEnrichmentJob) -> EnrichmentResult: ...

    async def read_latest_web_enrichment_run(
        self,
        *,
        artifact_id: UUID,
        status: str,
        content_anchor: str | None,
    ) -> EnrichmentRunReadback | None: ...

    async def read_web_snapshot(self, snapshot_id: UUID) -> WebSnapshotReadback | None: ...

    async def read_snapshot_updated_outbox(
        self,
        *,
        artifact_id: UUID,
        snapshot_id: UUID,
    ) -> OutboxReadback | None: ...

    async def resolve_text_idea_target(self, event_id: UUID) -> TextIdeaTargetReadback: ...

    async def run_evidence_assembler(self, trigger_event_id: UUID, *, branch: str) -> list[AssemblyResult]: ...

    async def read_text_idea_snapshot(self, candidate_group_id: UUID) -> TextIdeaSnapshotReadback | None: ...

    async def read_bundle(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
    ) -> BundleReadback | None: ...

    async def read_analysis_requested_outbox(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
    ) -> OutboxReadback | None: ...

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class DatabaseHandle:
    database: ProofDatabase
    close: Callable[[], Awaitable[None]]


class DatabaseBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: RuntimeConfig,
        state: RunnerState,
        logger: logging.Logger,
    ) -> DatabaseHandle: ...


class TrackedWebFetchClient:
    def __init__(self, fetch_client: WebFetchClient, state: RunnerState) -> None:
        self._fetch_client = fetch_client
        self._state = state

    async def fetch(self, url: str) -> FetchedDocument:
        self._state.web_fetch_attempted = True
        self._state.web_fetch_count += 1
        if self._state.web_fetch_count > HARD_WEB_FETCH_LIMIT:
            raise ProofRunnerError("web_fetch_request_cap_exceeded")
        return await self._fetch_client.fetch(url)

    async def close(self) -> None:
        await self._fetch_client.close()


class SessionBackedProofDatabase:
    def __init__(
        self,
        *,
        runtime_config: RuntimeConfig,
        state: RunnerState,
        logger: logging.Logger,
    ) -> None:
        self._runtime_config = runtime_config
        self._state = state
        self._logger = logger
        self._engine: Any | None = None
        self._session_factory: Any | None = None

    async def _ensure_session_factory(self) -> Any:
        if self._session_factory is not None:
            return self._session_factory

        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

        self._engine = create_async_engine(self._runtime_config.database_url, future=True)
        self._session_factory = async_sessionmaker(self._engine, class_=AsyncSession, expire_on_commit=False)
        return self._session_factory

    async def select_events_by_suffix(self, suffix: str) -> list[TargetEventRow]:
        import sqlalchemy as sa

        self._state.database_read_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    """
                    SELECT event_id, event_type, status, aggregate_type, aggregate_id, payload_json
                    FROM event_outbox
                    WHERE lower(event_id::text) LIKE :pattern
                    ORDER BY created_at ASC, event_id ASC
                    LIMIT 2
                    """
                ),
                {"pattern": f"%{suffix.lower()}"},
            )
            return [
                TargetEventRow(
                    event_id=UUID(str(row["event_id"])),
                    event_type=str(row["event_type"]),
                    status=str(row["status"]),
                    aggregate_type=str(row["aggregate_type"]),
                    aggregate_id=UUID(str(row["aggregate_id"])),
                    payload_json=_json_loads(row["payload_json"]) or {},
                )
                for row in result.mappings().all()
            ]

    async def load_web_job_by_trigger_event_id(self, event_id: UUID) -> ArtifactEnrichmentJob | None:
        self._state.database_read_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            return await WebEnricherRepository(session).load_job_by_trigger_event_id(event_id)

    async def verify_web_target_resolution(self, job: ArtifactEnrichmentJob) -> WebTargetResolutionReadback:
        import sqlalchemy as sa

        self._state.database_read_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    """
                    SELECT
                        (
                            SELECT COUNT(*)::int
                            FROM artifact_registry
                            WHERE artifact_id = CAST(:artifact_id AS uuid)
                        ) AS artifact_count,
                        (
                            SELECT COUNT(*)::int
                            FROM candidate_group_proposals
                            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                        ) AS candidate_group_count,
                        (
                            SELECT COUNT(*)::int
                            FROM candidate_group_members
                            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                              AND artifact_id = CAST(:artifact_id AS uuid)
                        ) AS candidate_member_count,
                        COALESCE(
                            (
                                SELECT canonical_url LIKE 'http://%' OR canonical_url LIKE 'https://%'
                                FROM artifact_registry
                                WHERE artifact_id = CAST(:artifact_id AS uuid)
                                LIMIT 1
                            ),
                            FALSE
                        ) AS canonical_url_valid
                    """
                ),
                {"artifact_id": str(job.artifact_id), "candidate_group_id": str(job.candidate_group_id)},
            )
            row = result.mappings().one()
            return WebTargetResolutionReadback(
                artifact_count=int(row["artifact_count"]),
                candidate_group_count=int(row["candidate_group_count"]),
                candidate_member_count=int(row["candidate_member_count"]),
                canonical_url_valid=bool(row["canonical_url_valid"]),
            )

    async def run_web_enricher(self, job: ArtifactEnrichmentJob) -> EnrichmentResult:
        self._state.database_write_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            tracked_fetch_client = TrackedWebFetchClient(
                WebFetchClient(
                    timeout_sec=self._runtime_config.web_config.request_timeout_sec,
                    max_redirects=self._runtime_config.web_config.max_redirects,
                    max_bytes=self._runtime_config.web_config.max_bytes,
                    user_agent=self._runtime_config.web_config.user_agent,
                    content_type_allowlist=self._runtime_config.web_config.content_type_allowlist,
                ),
                self._state,
            )
            try:
                service = WebEnricherService(
                    self._runtime_config.web_config,
                    repository=WebEnricherRepository(session),
                    fetch_client=tracked_fetch_client,  # type: ignore[arg-type]
                    article_parser=ArticleParser(
                        excerpt_chars=self._runtime_config.web_config.excerpt_chars,
                        max_outbound_links=self._runtime_config.web_config.max_outbound_links,
                    ),
                    url_discovery=WebUrlDiscovery(),
                    logger=self._logger,
                )
                result = await service.handle_job(job)
                self._state.artifact_snapshot_write_attempted = result.snapshot_id is not None
                return result
            finally:
                await tracked_fetch_client.close()

    async def read_latest_web_enrichment_run(
        self,
        *,
        artifact_id: UUID,
        status: str,
        content_anchor: str | None,
    ) -> EnrichmentRunReadback | None:
        import sqlalchemy as sa

        self._state.database_read_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    """
                    SELECT artifact_id, provider, status, content_anchor
                    FROM artifact_enrichment_runs
                    WHERE artifact_id = CAST(:artifact_id AS uuid)
                      AND provider = 'web'
                      AND status = CAST(:status AS snapshot_status_enum)
                      AND content_anchor IS NOT DISTINCT FROM :content_anchor
                    ORDER BY COALESCE(finished_at, started_at, requested_at) DESC
                    LIMIT 1
                    """
                ),
                {"artifact_id": str(artifact_id), "status": status, "content_anchor": content_anchor},
            )
            row = result.mappings().first()
            if row is None:
                return None
            return EnrichmentRunReadback(
                artifact_id=UUID(str(row["artifact_id"])),
                provider=str(row["provider"]),
                status=str(row["status"]),
                content_anchor=str(row["content_anchor"]) if row["content_anchor"] else None,
            )

    async def read_web_snapshot(self, snapshot_id: UUID) -> WebSnapshotReadback | None:
        import sqlalchemy as sa

        self._state.database_read_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    """
                    SELECT s.snapshot_id, s.artifact_id, s.provider, s.snapshot_type, s.status,
                           s.content_anchor, s.normalized_projection,
                           (
                               SELECT COUNT(*)::int
                               FROM artifact_snapshot_web_article wa
                               WHERE wa.snapshot_id = s.snapshot_id
                           ) AS web_article_child_count,
                           (
                               SELECT COUNT(*)::int
                               FROM discovered_url_observations du
                               WHERE du.parent_snapshot_id = s.snapshot_id
                           ) AS discovered_url_count,
                           (
                               SELECT jsonb_agg(du.observed_url ORDER BY du.context_path, du.observed_url)
                               FROM discovered_url_observations du
                               WHERE du.parent_snapshot_id = s.snapshot_id
                           ) AS discovered_urls_json
                    FROM artifact_snapshots s
                    WHERE s.snapshot_id = CAST(:snapshot_id AS uuid)
                    """
                ),
                {"snapshot_id": str(snapshot_id)},
            )
            row = result.mappings().first()
            if row is None:
                return None
            discovered_urls = _json_loads(row["discovered_urls_json"]) or []
            return WebSnapshotReadback(
                snapshot_id=UUID(str(row["snapshot_id"])),
                artifact_id=UUID(str(row["artifact_id"])),
                provider=str(row["provider"]),
                snapshot_type=str(row["snapshot_type"]),
                status=str(row["status"]),
                content_anchor=str(row["content_anchor"]) if row["content_anchor"] else None,
                normalized_projection_present=bool(_json_loads(row["normalized_projection"])),
                web_article_child_count=int(row["web_article_child_count"]),
                discovered_url_count=int(row["discovered_url_count"]),
                discovered_url_fingerprints=tuple(
                    _fingerprint_text(str(value)) for value in discovered_urls if value
                ),
            )

    async def read_snapshot_updated_outbox(
        self,
        *,
        artifact_id: UUID,
        snapshot_id: UUID,
    ) -> OutboxReadback | None:
        import sqlalchemy as sa

        self._state.database_read_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    """
                    SELECT event_id, event_type, status
                    FROM event_outbox
                    WHERE event_type = 'artifact.snapshot.updated.v1'
                      AND aggregate_type = 'artifact'
                      AND aggregate_id = CAST(:artifact_id AS uuid)
                      AND dedupe_key = :dedupe_key
                    ORDER BY created_at ASC, event_id ASC
                    LIMIT 1
                    """
                ),
                {
                    "artifact_id": str(artifact_id),
                    "dedupe_key": f"artifact:snapshot_updated:{artifact_id}:{snapshot_id}",
                },
            )
            return _outbox_readback(result.mappings().first())

    async def resolve_text_idea_target(self, event_id: UUID) -> TextIdeaTargetReadback:
        self._state.database_read_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            repository = EvidenceAssemblerRepository(session)
            targets = await repository.resolve_refresh_targets(event_id)
            if len(targets) != 1:
                return TextIdeaTargetReadback(
                    target_count=len(targets),
                    candidate_group_id=None,
                    candidate_group_count=0,
                    source_identity_present=False,
                    text_idea_member_count=0,
                    source_text_present=False,
                    current_primary_is_text_idea=False,
                    usable_external_snapshot_count=0,
                    existing_text_idea_snapshot_count=0,
                )
            target = targets[0]
            candidate = await repository.load_candidate_group(target.candidate_group_id)
            if candidate is None:
                return TextIdeaTargetReadback(
                    target_count=1,
                    candidate_group_id=target.candidate_group_id,
                    candidate_group_count=0,
                    source_identity_present=False,
                    text_idea_member_count=0,
                    source_text_present=False,
                    current_primary_is_text_idea=False,
                    usable_external_snapshot_count=0,
                    existing_text_idea_snapshot_count=0,
                )
            members = await repository.load_candidate_members(candidate.candidate_group_id)
            snapshots = await repository.load_current_snapshots(member.artifact_id for member in members)
            source_text = await repository.load_source_message_text_surface(
                source_message_id=candidate.source_message_id,
                source_version_no=candidate.source_version_no,
            )
            text_idea_member_ids = [member.artifact_id for member in members if member.artifact_type == "text_idea"]
            current_primary_is_text_idea = candidate.current_primary_artifact_id in set(text_idea_member_ids)
            usable_external_snapshot_count = sum(
                1
                for member in members
                if member.artifact_type != "text_idea"
                and member.artifact_id in snapshots
                and snapshots[member.artifact_id].status in {"ready", "partial_ready", "low_evidence"}
            )
            existing_text_idea_snapshot_count = await self._count_existing_text_idea_snapshots(
                text_idea_member_ids
            )
            return TextIdeaTargetReadback(
                target_count=1,
                candidate_group_id=candidate.candidate_group_id,
                candidate_group_count=1,
                source_identity_present=bool(candidate.source_message_id and candidate.source_version_no is not None),
                text_idea_member_count=len(text_idea_member_ids),
                source_text_present=bool(source_text),
                current_primary_is_text_idea=current_primary_is_text_idea,
                usable_external_snapshot_count=usable_external_snapshot_count,
                existing_text_idea_snapshot_count=existing_text_idea_snapshot_count,
            )

    async def run_evidence_assembler(self, trigger_event_id: UUID, *, branch: str) -> list[AssemblyResult]:
        self._state.database_write_attempted = True
        self._state.evidence_bundle_write_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            async with session.begin():
                service = EvidenceAssemblerService(
                    self._runtime_config.assembler_config,
                    repository=EvidenceAssemblerRepository(session),
                    text_idea_builder=TextIdeaBuilder(),
                    logger=self._logger,
                )
                result = await service.handle_trigger_event(trigger_event_id)
        if branch == "text_idea":
            self._state.text_idea_snapshot_write_attempted = True
        return result

    async def read_text_idea_snapshot(self, candidate_group_id: UUID) -> TextIdeaSnapshotReadback | None:
        import sqlalchemy as sa

        self._state.database_read_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    """
                    WITH matching AS (
                        SELECT s.snapshot_id, s.artifact_id, s.provider, s.snapshot_type, s.status,
                               s.content_anchor, s.normalized_projection, s.fetched_at
                        FROM candidate_group_members cgm
                        JOIN artifact_registry ar ON ar.artifact_id = cgm.artifact_id
                        JOIN artifact_snapshots s ON s.artifact_id = ar.artifact_id
                        WHERE cgm.candidate_group_id = CAST(:candidate_group_id AS uuid)
                          AND ar.artifact_type = 'text_idea'
                          AND s.provider = 'local_text_idea'
                          AND s.snapshot_type = 'text_idea'
                    )
                    SELECT m.snapshot_id, m.artifact_id, m.provider, m.snapshot_type, m.status,
                           m.content_anchor, m.normalized_projection,
                           (
                               SELECT COUNT(*)::int
                               FROM artifact_snapshot_text_idea ti
                               WHERE ti.snapshot_id = m.snapshot_id
                           ) AS text_idea_child_count,
                           (SELECT COUNT(*)::int FROM matching) AS matching_snapshot_count
                    FROM matching m
                    ORDER BY m.fetched_at DESC, m.snapshot_id DESC
                    LIMIT 1
                    """
                ),
                {"candidate_group_id": str(candidate_group_id)},
            )
            row = result.mappings().first()
            if row is None:
                return None
            return TextIdeaSnapshotReadback(
                snapshot_id=UUID(str(row["snapshot_id"])),
                artifact_id=UUID(str(row["artifact_id"])),
                provider=str(row["provider"]),
                snapshot_type=str(row["snapshot_type"]),
                status=str(row["status"]),
                content_anchor=str(row["content_anchor"]) if row["content_anchor"] else None,
                normalized_projection_present=bool(_json_loads(row["normalized_projection"])),
                text_idea_child_count=int(row["text_idea_child_count"]),
                matching_snapshot_count=int(row["matching_snapshot_count"]),
            )

    async def read_bundle(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
    ) -> BundleReadback | None:
        import sqlalchemy as sa

        self._state.database_read_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    """
                    SELECT b.bundle_id, b.candidate_group_id, b.ready_for_analysis,
                           (c.current_bundle_id = b.bundle_id) AS current_bundle_consistent
                    FROM candidate_evidence_bundles b
                    JOIN candidate_group_proposals c
                      ON c.candidate_group_id = b.candidate_group_id
                    WHERE b.candidate_group_id = CAST(:candidate_group_id AS uuid)
                      AND b.bundle_id = CAST(:bundle_id AS uuid)
                    LIMIT 1
                    """
                ),
                {"candidate_group_id": str(candidate_group_id), "bundle_id": str(bundle_id)},
            )
            row = result.mappings().first()
            if row is None:
                return None
            return BundleReadback(
                bundle_id=UUID(str(row["bundle_id"])),
                candidate_group_id=UUID(str(row["candidate_group_id"])),
                ready_for_analysis=bool(row["ready_for_analysis"]),
                current_bundle_consistent=bool(row["current_bundle_consistent"]),
            )

    async def read_analysis_requested_outbox(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
    ) -> OutboxReadback | None:
        import sqlalchemy as sa

        self._state.database_read_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    """
                    SELECT event_id, event_type, status
                    FROM event_outbox
                    WHERE event_type = 'analysis.requested.v1'
                      AND aggregate_type = 'candidate_group'
                      AND aggregate_id = CAST(:candidate_group_id AS uuid)
                      AND dedupe_key = :dedupe_key
                    ORDER BY created_at ASC, event_id ASC
                    LIMIT 1
                    """
                ),
                {
                    "candidate_group_id": str(candidate_group_id),
                    "dedupe_key": f"analysis-request:{candidate_group_id}:{bundle_id}",
                },
            )
            return _outbox_readback(result.mappings().first())

    async def _count_existing_text_idea_snapshots(self, artifact_ids: list[UUID]) -> int:
        if not artifact_ids:
            return 0
        import sqlalchemy as sa

        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    """
                    SELECT COUNT(*)::int
                    FROM artifact_snapshots
                    WHERE artifact_id = ANY(CAST(:artifact_ids AS uuid[]))
                      AND provider = 'local_text_idea'
                      AND snapshot_type = 'text_idea'
                    """
                ),
                {"artifact_ids": [str(artifact_id) for artifact_id in artifact_ids]},
            )
            return int(result.scalar_one())

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()


async def build_default_database(
    runtime_config: RuntimeConfig,
    state: RunnerState,
    logger: logging.Logger,
) -> DatabaseHandle:
    database = SessionBackedProofDatabase(runtime_config=runtime_config, state=state, logger=logger)
    return DatabaseHandle(database=database, close=database.close)


def load_runtime_config() -> RuntimeConfig:
    try:
        web_config = WebEnricherConfig.from_env()
        assembler_config = EvidenceAssemblerConfig.from_env()
        _validate_web_runtime_caps(web_config)
    except WebEnricherConfigurationError as exc:
        raise ProofRunnerError(_config_error_code(str(exc))) from exc
    except EvidenceAssemblerConfigurationError as exc:
        raise ProofRunnerError(_config_error_code(str(exc))) from exc
    except ProofRunnerError:
        raise
    except Exception as exc:
        raise ProofRunnerError("runtime_config_error") from exc
    return RuntimeConfig(web_config=web_config, assembler_config=assembler_config)


async def run_bounded_web_text_idea_evidence_bundle(
    config: RunnerConfig,
    *,
    runtime_config_loader: Callable[[], RuntimeConfig] = load_runtime_config,
    database_builder: DatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> RunnerResult:
    state = RunnerState()
    preflight = _pre_runtime_gate_error(config)
    if preflight is not None:
        return RunnerResult(status="blocked", reason_code=preflight, config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except ProofRunnerError as exc:
        return RunnerResult(status="blocked", reason_code=_safe_reason_code(exc.reason_code), config=config, state=state)
    except Exception:
        return RunnerResult(status="blocked", reason_code="runtime_config_error", config=config, state=state)

    database_handle: DatabaseHandle | None = None
    web = WebBranchProof()
    text = TextIdeaBranchProof()
    try:
        database_handle = await (database_builder or build_default_database)(runtime_config, state, effective_logger)
        database = database_handle.database

        web_context: tuple[TargetEventRow, ArtifactEnrichmentJob] | None = None
        text_context: tuple[TargetEventRow, TextIdeaTargetReadback] | None = None

        if config.web_event_id_suffix:
            web_preflight = await _preflight_web_branch(database, config.web_event_id_suffix)
            web = web_preflight.branch
            if web_preflight.reason_code is not None:
                return _finalize(config=config, state=state, web=web, text=text)
            web_context = web_preflight.context

        if config.text_idea_event_id_suffix:
            text_preflight = await _preflight_text_idea_branch(database, config.text_idea_event_id_suffix)
            text = text_preflight.branch
            if text_preflight.reason_code is not None:
                return _finalize(config=config, state=state, web=web, text=text)
            text_context = text_preflight.context

        if config.mode == PLAN_MODE:
            return _finalize(config=config, state=state, web=web, text=text)

        if web_context is not None:
            web = await _execute_web_branch(database, web_context[1], base=web)
            if web.status != "pass":
                if text_context is not None and text.status == "pass":
                    text = TextIdeaBranchProof(
                        status="blocked",
                        reason_code="not_run_after_web_branch_failure",
                        target_event_id=text.target_event_id,
                        candidate_group_id=text.candidate_group_id,
                    )
                return _finalize(config=config, state=state, web=web, text=text)

        if text_context is not None:
            text = await _execute_text_idea_branch(
                database,
                text_context[0],
                text_context[1],
                base=text,
            )

        return _finalize(config=config, state=state, web=web, text=text)
    except WebFetchClientError:
        failed_web = WebBranchProof(
            status="failed",
            reason_code="web_fetch_client_error",
            target_event_id=web.target_event_id,
            artifact_id=web.artifact_id,
            candidate_group_id=web.candidate_group_id,
            artifact_type=web.artifact_type,
            provider_route=web.provider_route,
        )
        return _finalize(config=config, state=state, web=failed_web, text=text)
    except ProofRunnerError as exc:
        reason_code = _safe_reason_code(exc.reason_code)
        failed_web = web
        if config.web_event_id_suffix and web.status in {"pass", "not_requested"}:
            failed_web = WebBranchProof(
                status="failed",
                reason_code=reason_code,
                target_event_id=web.target_event_id,
                artifact_id=web.artifact_id,
                candidate_group_id=web.candidate_group_id,
                artifact_type=web.artifact_type,
                provider_route=web.provider_route,
            )
        else:
            text = TextIdeaBranchProof(
                status="failed",
                reason_code=reason_code,
                target_event_id=text.target_event_id,
                candidate_group_id=text.candidate_group_id,
            )
        return _finalize(config=config, state=state, web=failed_web, text=text)
    except Exception:
        return RunnerResult(
            status="failed",
            reason_code="unexpected_runner_error",
            config=config,
            state=state,
            web=web,
            text_idea=text,
        )
    finally:
        if database_handle is not None:
            try:
                await database_handle.close()
            except Exception:
                pass


@dataclass(frozen=True, slots=True)
class _WebPreflight:
    branch: WebBranchProof
    context: tuple[TargetEventRow, ArtifactEnrichmentJob] | None = None
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class _TextIdeaPreflight:
    branch: TextIdeaBranchProof
    context: tuple[TargetEventRow, TextIdeaTargetReadback] | None = None
    reason_code: str | None = None


async def _preflight_web_branch(database: ProofDatabase, suffix: str) -> _WebPreflight:
    rows = await database.select_events_by_suffix(suffix)
    rows_error = _target_rows_error(rows)
    if rows_error is not None:
        return _WebPreflight(WebBranchProof(status="blocked", reason_code=rows_error), reason_code=rows_error)
    target = rows[0]
    payload = target.payload_json
    base = WebBranchProof(
        status="blocked",
        reason_code="preflight_pending",
        target_event_id=target.event_id,
        artifact_id=_uuid_or_none(payload.get("artifact_id")),
        candidate_group_id=_uuid_or_none(payload.get("candidate_group_id")),
        artifact_type=_str_or_none(payload.get("artifact_type")),
        provider_route=_str_or_none(payload.get("provider_route")),
    )
    contract_error = _web_target_contract_error(target)
    if contract_error is not None:
        return _WebPreflight(_replace_web(base, reason_code=contract_error), reason_code=contract_error)

    job = await database.load_web_job_by_trigger_event_id(target.event_id)
    if job is None:
        return _WebPreflight(_replace_web(base, reason_code="web_job_rehydrate_failed"), reason_code="web_job_rehydrate_failed")
    job_error = _web_job_contract_error(job, target)
    if job_error is not None:
        return _WebPreflight(_replace_web(base, reason_code=job_error), reason_code=job_error)
    resolution = await database.verify_web_target_resolution(job)
    resolution_error = _web_resolution_error(resolution)
    if resolution_error is not None:
        return _WebPreflight(_replace_web(base, reason_code=resolution_error), reason_code=resolution_error)
    return _WebPreflight(_replace_web(base, status="pass", reason_code="plan_exact_web_target_ready"), (target, job))


async def _preflight_text_idea_branch(database: ProofDatabase, suffix: str) -> _TextIdeaPreflight:
    rows = await database.select_events_by_suffix(suffix)
    rows_error = _target_rows_error(rows)
    if rows_error is not None:
        return _TextIdeaPreflight(
            TextIdeaBranchProof(status="blocked", reason_code=rows_error),
            reason_code=rows_error,
        )
    target = rows[0]
    base = TextIdeaBranchProof(
        status="blocked",
        reason_code="preflight_pending",
        target_event_id=target.event_id,
    )
    contract_error = _text_idea_target_contract_error(target)
    if contract_error is not None:
        return _TextIdeaPreflight(_replace_text(base, reason_code=contract_error), reason_code=contract_error)
    readback = await database.resolve_text_idea_target(target.event_id)
    base = _replace_text(base, candidate_group_id=readback.candidate_group_id)
    readback_error = _text_idea_readback_error(readback)
    if readback_error is not None:
        return _TextIdeaPreflight(_replace_text(base, reason_code=readback_error), reason_code=readback_error)
    return _TextIdeaPreflight(
        _replace_text(
            base,
            status="pass",
            reason_code="plan_exact_text_idea_target_ready",
            reused_existing_text_idea_snapshot=readback.existing_text_idea_snapshot_count > 0,
        ),
        (target, readback),
    )


async def _execute_web_branch(
    database: ProofDatabase,
    job: ArtifactEnrichmentJob,
    *,
    base: WebBranchProof,
) -> WebBranchProof:
    service_result = await database.run_web_enricher(job)
    result_fields = {
        "snapshot_id": service_result.snapshot_id,
        "snapshot_status": service_result.status,
        "content_anchor": service_result.content_anchor,
    }
    run_readback = await database.read_latest_web_enrichment_run(
        artifact_id=job.artifact_id,
        status=service_result.status,
        content_anchor=service_result.content_anchor,
    )
    if run_readback is None:
        return _replace_web(base, status="failed", reason_code="enrichment_run_readback_missing", **result_fields)
    run_error = _web_enrichment_run_readback_error(run_readback, job, service_result.status, service_result.content_anchor)
    if run_error is not None:
        return _replace_web(
            base,
            status="failed",
            reason_code=run_error,
            enrichment_run_readback=run_readback,
            **result_fields,
        )
    if service_result.status not in ALLOWED_PROVIDER_STATUSES:
        return _replace_web(
            base,
            status="blocked",
            reason_code="provider_status_not_terminal_or_ready",
            enrichment_run_readback=run_readback,
            **result_fields,
        )
    if service_result.status not in WEB_READY_STATUSES:
        return _replace_web(
            base,
            status="blocked",
            reason_code=f"provider_status_{service_result.status}",
            enrichment_run_readback=run_readback,
            **result_fields,
        )
    if service_result.snapshot_id is None:
        return _replace_web(
            base,
            status="failed",
            reason_code="snapshot_id_missing_after_ready_provider",
            enrichment_run_readback=run_readback,
            **result_fields,
        )

    snapshot = await database.read_web_snapshot(service_result.snapshot_id)
    snapshot_error = _web_snapshot_readback_error(snapshot, job)
    if snapshot_error is not None:
        return _replace_web(
            base,
            status="failed",
            reason_code=snapshot_error,
            enrichment_run_readback=run_readback,
            snapshot_readback=snapshot,
            **result_fields,
        )
    if not service_result.emitted_snapshot_updated:
        return _replace_web(
            base,
            status="failed",
            reason_code="snapshot_updated_outbox_not_emitted",
            enrichment_run_readback=run_readback,
            snapshot_readback=snapshot,
            **result_fields,
        )
    snapshot_updated = await database.read_snapshot_updated_outbox(
        artifact_id=job.artifact_id,
        snapshot_id=service_result.snapshot_id,
    )
    if snapshot_updated is None:
        return _replace_web(
            base,
            status="failed",
            reason_code="snapshot_updated_outbox_missing",
            enrichment_run_readback=run_readback,
            snapshot_readback=snapshot,
            **result_fields,
        )

    assembly = await database.run_evidence_assembler(snapshot_updated.event_id, branch="web")
    assembly_result = _select_assembly_result(assembly, job.candidate_group_id)
    if assembly_result is None or assembly_result.bundle_id is None:
        return _replace_web(
            base,
            status="failed",
            reason_code="evidence_bundle_not_materialized",
            enrichment_run_readback=run_readback,
            snapshot_readback=snapshot,
            snapshot_updated_outbox=snapshot_updated,
            **result_fields,
        )
    bundle_result = await _read_and_verify_bundle(
        database,
        candidate_group_id=job.candidate_group_id,
        assembly_result=assembly_result,
    )
    if isinstance(bundle_result, str):
        return _replace_web(
            base,
            status="failed",
            reason_code=bundle_result,
            enrichment_run_readback=run_readback,
            snapshot_readback=snapshot,
            snapshot_updated_outbox=snapshot_updated,
            evidence_bundle_written_or_reused=True,
            bundle_id=assembly_result.bundle_id,
            ready_for_analysis=assembly_result.ready_for_analysis,
            reused_existing_bundle=assembly_result.reused_existing_bundle,
            **result_fields,
        )
    bundle, analysis_outbox = bundle_result
    return _replace_web(
        base,
        status="pass",
        reason_code="web_provider_evidence_bundle_proof_complete",
        enrichment_run_readback=run_readback,
        snapshot_readback=snapshot,
        snapshot_updated_outbox=snapshot_updated,
        evidence_bundle_written_or_reused=True,
        bundle_id=bundle.bundle_id,
        ready_for_analysis=bundle.ready_for_analysis,
        analysis_requested_outbox=analysis_outbox,
        reused_existing_bundle=assembly_result.reused_existing_bundle,
        **result_fields,
    )


async def _execute_text_idea_branch(
    database: ProofDatabase,
    target: TargetEventRow,
    readback: TextIdeaTargetReadback,
    *,
    base: TextIdeaBranchProof,
) -> TextIdeaBranchProof:
    if readback.candidate_group_id is None:
        return _replace_text(base, status="failed", reason_code="text_idea_candidate_group_missing")
    assembly = await database.run_evidence_assembler(target.event_id, branch="text_idea")
    assembly_result = _select_assembly_result(assembly, readback.candidate_group_id)
    if assembly_result is None or assembly_result.bundle_id is None:
        return _replace_text(base, status="failed", reason_code="evidence_bundle_not_materialized")
    snapshot = await database.read_text_idea_snapshot(readback.candidate_group_id)
    snapshot_error = _text_idea_snapshot_readback_error(snapshot)
    if snapshot_error is not None:
        return _replace_text(
            base,
            status="failed",
            reason_code=snapshot_error,
            snapshot_readback=snapshot,
            evidence_bundle_written_or_reused=True,
            bundle_id=assembly_result.bundle_id,
            ready_for_analysis=assembly_result.ready_for_analysis,
            reused_existing_bundle=assembly_result.reused_existing_bundle,
        )
    bundle_result = await _read_and_verify_bundle(
        database,
        candidate_group_id=readback.candidate_group_id,
        assembly_result=assembly_result,
    )
    if isinstance(bundle_result, str):
        return _replace_text(
            base,
            status="failed",
            reason_code=bundle_result,
            snapshot_id=snapshot.snapshot_id if snapshot else None,
            artifact_id=snapshot.artifact_id if snapshot else None,
            snapshot_status=snapshot.status if snapshot else None,
            snapshot_readback=snapshot,
            evidence_bundle_written_or_reused=True,
            bundle_id=assembly_result.bundle_id,
            ready_for_analysis=assembly_result.ready_for_analysis,
            reused_existing_bundle=assembly_result.reused_existing_bundle,
        )
    bundle, analysis_outbox = bundle_result
    return _replace_text(
        base,
        status="pass",
        reason_code="text_idea_evidence_bundle_proof_complete",
        snapshot_id=snapshot.snapshot_id if snapshot else None,
        artifact_id=snapshot.artifact_id if snapshot else None,
        snapshot_status=snapshot.status if snapshot else None,
        snapshot_readback=snapshot,
        evidence_bundle_written_or_reused=True,
        bundle_id=bundle.bundle_id,
        ready_for_analysis=bundle.ready_for_analysis,
        analysis_requested_outbox=analysis_outbox,
        reused_existing_bundle=assembly_result.reused_existing_bundle,
        reused_existing_text_idea_snapshot=readback.existing_text_idea_snapshot_count > 0,
    )


async def _read_and_verify_bundle(
    database: ProofDatabase,
    *,
    candidate_group_id: UUID,
    assembly_result: AssemblyResult,
) -> tuple[BundleReadback, OutboxReadback | None] | str:
    if assembly_result.bundle_id is None:
        return "bundle_id_missing"
    bundle = await database.read_bundle(
        candidate_group_id=candidate_group_id,
        bundle_id=assembly_result.bundle_id,
    )
    if bundle is None:
        return "bundle_readback_missing"
    if not bundle.current_bundle_consistent:
        return "current_bundle_readback_inconsistent"
    analysis_outbox = None
    if bundle.ready_for_analysis:
        analysis_outbox = await database.read_analysis_requested_outbox(
            candidate_group_id=candidate_group_id,
            bundle_id=bundle.bundle_id,
        )
        if analysis_outbox is None:
            return "analysis_requested_outbox_missing"
    return bundle, analysis_outbox


def run_bounded_web_text_idea_evidence_bundle_sync(
    config: RunnerConfig,
    *,
    runtime_config_loader: Callable[[], RuntimeConfig] = load_runtime_config,
    database_builder: DatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> RunnerResult:
    return asyncio.run(
        run_bounded_web_text_idea_evidence_bundle(
            config,
            runtime_config_loader=runtime_config_loader,
            database_builder=database_builder,
            logger=logger,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Prove exact web_article and/or text_idea targets through EvidenceBundle assembly.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=[PLAN_MODE, EXECUTE_MODE], required=True)
    parser.add_argument("--web-event-id-suffix")
    parser.add_argument("--text-idea-event-id-suffix")
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--confirm-token")
    parser.add_argument("--allow-web-fetch", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-artifact-snapshot-write", action="store_true")
    parser.add_argument("--allow-text-idea-snapshot-write", action="store_true")
    parser.add_argument("--allow-evidence-bundle-write", action="store_true")
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader: Callable[[], RuntimeConfig] = load_runtime_config,
    database_builder: DatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> RunnerResult:
    return run_bounded_web_text_idea_evidence_bundle_sync(
        RunnerConfig(
            mode=str(args.mode),
            web_event_id_suffix=args.web_event_id_suffix,
            text_idea_event_id_suffix=args.text_idea_event_id_suffix,
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            confirm_token=args.confirm_token,
            allow_web_fetch=bool(args.allow_web_fetch),
            allow_database_write=bool(args.allow_database_write),
            allow_artifact_snapshot_write=bool(args.allow_artifact_snapshot_write),
            allow_text_idea_snapshot_write=bool(args.allow_text_idea_snapshot_write),
            allow_evidence_bundle_write=bool(args.allow_evidence_bundle_write),
        ),
        runtime_config_loader=runtime_config_loader,
        database_builder=database_builder,
        logger=logger,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader: Callable[[], RuntimeConfig] = load_runtime_config,
    database_builder: DatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        report = RunnerResult(
            status="blocked",
            reason_code=str(exc),
            config=RunnerConfig(mode="invalid"),
        ).to_sanitized_dict()
        sys.stdout.write(render_sanitized_json(report))
        return 1
    result = run(
        args,
        runtime_config_loader=runtime_config_loader,
        database_builder=database_builder,
        logger=logger,
    )
    sys.stdout.write(render_sanitized_json(result.to_sanitized_dict()))
    return 0 if result.ok else 1


def render_sanitized_json(report: dict[str, Any]) -> str:
    return json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"


def _pre_runtime_gate_error(config: RunnerConfig) -> str | None:
    if config.mode not in {PLAN_MODE, EXECUTE_MODE}:
        return "invalid_mode"
    web_suffix = _normalized_suffix(config.web_event_id_suffix)
    text_suffix = _normalized_suffix(config.text_idea_event_id_suffix)
    if web_suffix is None and text_suffix is None:
        return "no_branch_requested"
    if config.web_event_id_suffix is not None and web_suffix is None:
        return "invalid_or_missing_web_event_id_suffix"
    if config.text_idea_event_id_suffix is not None and text_suffix is None:
        return "invalid_or_missing_text_idea_event_id_suffix"
    if config.mode == PLAN_MODE and (
        config.confirm_token
        or config.allow_web_fetch
        or config.allow_database_write
        or config.allow_artifact_snapshot_write
        or config.allow_text_idea_snapshot_write
        or config.allow_evidence_bundle_write
    ):
        return "execute_authority_not_allowed_for_plan"
    if config.mode == EXECUTE_MODE:
        if not config.operator_approved:
            return "operator_approval_missing"
        if config.confirm_token != CONFIRM_TOKEN:
            return "confirm_token_missing_or_invalid"
        if config.web_event_id_suffix and not config.allow_web_fetch:
            return "web_fetch_not_allowed"
        if not config.allow_database_write:
            return "database_write_not_allowed"
        if config.web_event_id_suffix and not config.allow_artifact_snapshot_write:
            return "artifact_snapshot_write_not_allowed"
        if config.text_idea_event_id_suffix and not config.allow_text_idea_snapshot_write:
            return "text_idea_snapshot_write_not_allowed"
        if not config.allow_evidence_bundle_write:
            return "evidence_bundle_write_not_allowed"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    return None


def _target_rows_error(rows: list[TargetEventRow]) -> str | None:
    if not rows:
        return "target_event_not_found"
    if len(rows) != 1:
        return "target_event_suffix_ambiguous"
    return None


def _web_target_contract_error(row: TargetEventRow) -> str | None:
    if row.event_type != "artifact.enrich.requested.v1":
        return "web_target_event_type_not_artifact_enrich_requested"
    if row.status != "published":
        return "web_target_event_status_not_published"
    if row.aggregate_type != "artifact":
        return "web_target_event_aggregate_type_not_artifact"
    payload = row.payload_json
    missing = [
        key
        for key in ("candidate_group_id", "artifact_id", "artifact_type", "provider_route", "refresh_mode", "depth_budget")
        if key not in payload
    ]
    if missing:
        return "web_target_event_payload_missing_required_field"
    artifact_id = _uuid_or_none(payload.get("artifact_id"))
    candidate_group_id = _uuid_or_none(payload.get("candidate_group_id"))
    if artifact_id is None or candidate_group_id is None:
        return "web_target_event_payload_invalid_uuid"
    if artifact_id != row.aggregate_id:
        return "web_target_event_artifact_id_aggregate_mismatch"
    if payload.get("provider_route") != "web":
        return "web_target_event_provider_route_not_web"
    if payload.get("artifact_type") != "web_article":
        return "web_target_event_artifact_type_not_web_article"
    return None


def _web_job_contract_error(job: ArtifactEnrichmentJob, row: TargetEventRow) -> str | None:
    payload = row.payload_json
    if job.trigger_event_id != row.event_id:
        return "web_job_trigger_event_mismatch"
    if job.event_type != "artifact.enrich.requested.v1":
        return "web_job_event_type_mismatch"
    if job.provider_route != "web":
        return "web_job_provider_route_not_web"
    if job.artifact_type != "web_article":
        return "web_job_artifact_type_not_web_article"
    if job.artifact_id != _uuid_or_none(payload.get("artifact_id")):
        return "web_job_artifact_id_mismatch"
    if job.candidate_group_id != _uuid_or_none(payload.get("candidate_group_id")):
        return "web_job_candidate_group_id_mismatch"
    return None


def _web_resolution_error(readback: WebTargetResolutionReadback) -> str | None:
    if readback.artifact_count != 1:
        return "web_target_artifact_resolution_not_exactly_one"
    if readback.candidate_group_count != 1:
        return "web_target_candidate_group_resolution_not_exactly_one"
    if readback.candidate_member_count != 1:
        return "web_target_candidate_member_resolution_not_exactly_one"
    if not readback.canonical_url_valid:
        return "web_target_canonical_url_not_http_https"
    return None


def _web_enrichment_run_readback_error(
    readback: EnrichmentRunReadback,
    job: ArtifactEnrichmentJob,
    status: str,
    content_anchor: str | None,
) -> str | None:
    if readback.artifact_id != job.artifact_id:
        return "web_enrichment_run_artifact_mismatch"
    if readback.provider != "web":
        return "web_enrichment_run_provider_not_web"
    if readback.status != status:
        return "web_enrichment_run_status_mismatch"
    if readback.content_anchor != content_anchor:
        return "web_enrichment_run_content_anchor_mismatch"
    return None


def _web_snapshot_readback_error(snapshot: WebSnapshotReadback | None, job: ArtifactEnrichmentJob) -> str | None:
    if snapshot is None:
        return "web_snapshot_readback_missing"
    if snapshot.artifact_id != job.artifact_id:
        return "web_snapshot_artifact_mismatch"
    if snapshot.provider != "web":
        return "web_snapshot_provider_not_web"
    if snapshot.snapshot_type != "web_article":
        return "web_snapshot_type_not_web_article"
    if snapshot.status not in ALLOWED_PROVIDER_STATUSES:
        return "web_snapshot_status_not_allowed"
    if snapshot.status in WEB_READY_STATUSES and not snapshot.content_anchor:
        return "web_snapshot_content_anchor_missing"
    if snapshot.status in WEB_READY_STATUSES and not snapshot.normalized_projection_present:
        return "web_projection_readback_missing"
    if snapshot.status in WEB_READY_STATUSES and snapshot.web_article_child_count < 1:
        return "web_article_child_readback_missing"
    return None


def _text_idea_target_contract_error(row: TargetEventRow) -> str | None:
    if row.event_type not in {"candidate.bundle.refresh.v1", "artifact.snapshot.updated.v1"}:
        return "text_idea_target_event_type_not_supported"
    return None


def _text_idea_readback_error(readback: TextIdeaTargetReadback) -> str | None:
    if readback.target_count != 1:
        return "text_idea_target_resolution_not_exactly_one"
    if readback.candidate_group_count != 1:
        return "text_idea_candidate_group_resolution_not_exactly_one"
    if not readback.source_identity_present:
        return "text_idea_candidate_source_identity_missing"
    if readback.text_idea_member_count < 1:
        return "text_idea_member_missing"
    if not readback.source_text_present:
        return "text_idea_source_text_missing"
    if (
        not readback.current_primary_is_text_idea
        and readback.usable_external_snapshot_count > 0
        and readback.existing_text_idea_snapshot_count < 1
    ):
        return "text_idea_not_primary_and_usable_external_exists"
    return None


def _text_idea_snapshot_readback_error(snapshot: TextIdeaSnapshotReadback | None) -> str | None:
    if snapshot is None:
        return "text_idea_snapshot_readback_missing"
    if snapshot.provider != "local_text_idea":
        return "text_idea_snapshot_provider_not_local_text_idea"
    if snapshot.snapshot_type != "text_idea":
        return "text_idea_snapshot_type_not_text_idea"
    if snapshot.status not in TEXT_IDEA_READY_STATUSES:
        return "text_idea_snapshot_status_not_allowed"
    if not snapshot.content_anchor:
        return "text_idea_snapshot_content_anchor_missing"
    if not snapshot.normalized_projection_present:
        return "text_idea_projection_readback_missing"
    if snapshot.text_idea_child_count < 1:
        return "text_idea_child_readback_missing"
    return None


def _select_assembly_result(results: list[AssemblyResult], candidate_group_id: UUID) -> AssemblyResult | None:
    matches = [result for result in results if result.candidate_group_id == candidate_group_id]
    if len(matches) != 1:
        return None
    return matches[0]


def _validate_web_runtime_caps(config: WebEnricherConfig) -> None:
    if config.request_timeout_sec <= 0 or config.request_timeout_sec > HARD_WEB_TIMEOUT_SEC:
        raise ProofRunnerError("web_fetch_timeout_out_of_range")
    if config.max_redirects < 0 or config.max_redirects > HARD_WEB_MAX_REDIRECTS:
        raise ProofRunnerError("web_fetch_max_redirects_out_of_range")
    if config.max_bytes <= 0 or config.max_bytes > HARD_WEB_MAX_BYTES:
        raise ProofRunnerError("web_fetch_max_bytes_out_of_range")
    if config.max_outbound_links <= 0 or config.max_outbound_links > HARD_WEB_MAX_OUTBOUND_LINKS:
        raise ProofRunnerError("web_fetch_max_outbound_links_out_of_range")
    if not set(config.content_type_allowlist).issubset(ALLOWED_WEB_CONTENT_TYPES):
        raise ProofRunnerError("web_fetch_content_type_allowlist_not_allowed")


def _config_error_code(text: str) -> str:
    if "DATABASE_URL" in text:
        return "database_url_missing"
    if "REDIS_URL" in text:
        return "redis_url_missing"
    return "runtime_config_error"


def _finalize(
    *,
    config: RunnerConfig,
    state: RunnerState,
    web: WebBranchProof,
    text: TextIdeaBranchProof,
) -> RunnerResult:
    requested = []
    if config.web_event_id_suffix:
        requested.append(web)
    if config.text_idea_event_id_suffix:
        requested.append(text)
    if requested and all(branch.status == "pass" for branch in requested):
        reason = (
            "plan_exact_requested_targets_ready"
            if config.mode == PLAN_MODE
            else "web_text_idea_evidence_bundle_proof_complete"
        )
        return RunnerResult(
            status="pass",
            reason_code=reason,
            config=config,
            state=state,
            web=web,
            text_idea=text,
        )
    failed = next((branch for branch in requested if branch.status == "failed"), None)
    if failed is not None:
        return RunnerResult(
            status="failed",
            reason_code=failed.reason_code,
            config=config,
            state=state,
            web=web,
            text_idea=text,
        )
    blocked = next((branch for branch in requested if branch.status == "blocked"), None)
    reason = blocked.reason_code if blocked is not None else "no_branch_requested"
    return RunnerResult(
        status="blocked",
        reason_code=reason,
        config=config,
        state=state,
        web=web,
        text_idea=text,
    )


def _replace_web(base: WebBranchProof, **kwargs: Any) -> WebBranchProof:
    values = {
        "status": base.status,
        "reason_code": base.reason_code,
        "target_event_id": base.target_event_id,
        "artifact_id": base.artifact_id,
        "candidate_group_id": base.candidate_group_id,
        "artifact_type": base.artifact_type,
        "provider_route": base.provider_route,
        "snapshot_id": base.snapshot_id,
        "snapshot_status": base.snapshot_status,
        "content_anchor": base.content_anchor,
        "enrichment_run_readback": base.enrichment_run_readback,
        "snapshot_readback": base.snapshot_readback,
        "snapshot_updated_outbox": base.snapshot_updated_outbox,
        "bundle_id": base.bundle_id,
        "ready_for_analysis": base.ready_for_analysis,
        "analysis_requested_outbox": base.analysis_requested_outbox,
        "evidence_bundle_written_or_reused": base.evidence_bundle_written_or_reused,
        "reused_existing_bundle": base.reused_existing_bundle,
    }
    values.update(kwargs)
    return WebBranchProof(**values)


def _replace_text(base: TextIdeaBranchProof, **kwargs: Any) -> TextIdeaBranchProof:
    values = {
        "status": base.status,
        "reason_code": base.reason_code,
        "target_event_id": base.target_event_id,
        "candidate_group_id": base.candidate_group_id,
        "snapshot_id": base.snapshot_id,
        "artifact_id": base.artifact_id,
        "snapshot_status": base.snapshot_status,
        "snapshot_readback": base.snapshot_readback,
        "bundle_id": base.bundle_id,
        "ready_for_analysis": base.ready_for_analysis,
        "analysis_requested_outbox": base.analysis_requested_outbox,
        "evidence_bundle_written_or_reused": base.evidence_bundle_written_or_reused,
        "reused_existing_bundle": base.reused_existing_bundle,
        "reused_existing_text_idea_snapshot": base.reused_existing_text_idea_snapshot,
    }
    values.update(kwargs)
    return TextIdeaBranchProof(**values)


def _normalized_suffix(value: str | None) -> str | None:
    if value is None:
        return None
    suffix = value.strip().lower()
    if len(suffix) < 8 or len(suffix) > 36:
        return None
    if any(char not in "0123456789abcdef-" for char in suffix):
        return None
    return suffix


def _safe_reason_code(value: str) -> str:
    if 1 <= len(value) <= 80 and all(char.islower() or char.isdigit() or char == "_" for char in value):
        return value
    return "runner_error"


def _outbox_readback(row: Any) -> OutboxReadback | None:
    if row is None:
        return None
    return OutboxReadback(
        event_id=UUID(str(row["event_id"])),
        event_type=str(row["event_type"]),
        status=str(row["status"]),
    )


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _fingerprint_uuid(value: UUID | None) -> str | None:
    return _fingerprint_text(str(value)) if value is not None else None


def _fingerprint_text(value: str | None) -> str | None:
    if not value:
        return None
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _count_bucket(value: int | None) -> str:
    if value is None:
        return "unknown"
    if value <= 0:
        return "zero"
    if value == 1:
        return "one"
    if value <= 5:
        return "few"
    if value <= 20:
        return "several"
    return "many"


def _first_present(*values: str | None) -> str | None:
    for value in values:
        if value:
            return value
    return None


def _combined_ready_for_analysis(web: WebBranchProof, text: TextIdeaBranchProof) -> bool | None:
    values = [
        branch.ready_for_analysis
        for branch in (web, text)
        if branch.status != "not_requested" and branch.ready_for_analysis is not None
    ]
    if not values:
        return None
    return any(values)


def _web_article_child_readback(snapshot: WebSnapshotReadback | None) -> dict[str, Any]:
    return {
        "expected": True,
        "present": bool(snapshot and snapshot.web_article_child_count > 0),
        "projection_present": bool(snapshot and snapshot.normalized_projection_present),
    }


def _text_idea_child_readback(snapshot: TextIdeaSnapshotReadback | None) -> dict[str, Any]:
    return {
        "expected": True,
        "present": bool(snapshot and snapshot.text_idea_child_count > 0),
        "projection_present": bool(snapshot and snapshot.normalized_projection_present),
    }


def _redactions_applied() -> dict[str, bool]:
    return {
        "full_event_ids_omitted": True,
        "full_artifact_ids_omitted": True,
        "full_candidate_group_ids_omitted": True,
        "raw_urls_omitted": True,
        "raw_final_urls_omitted": True,
        "raw_article_text_omitted": True,
        "raw_body_or_html_omitted": True,
        "source_text_omitted": True,
        "header_values_omitted": True,
        "secret_values_omitted": True,
        "env_values_omitted": True,
        "database_url_omitted": True,
        "redis_url_omitted": True,
        "exception_bodies_omitted": True,
        "stderr_omitted": True,
        "traceback_omitted": True,
    }


def _raw_values_printed() -> dict[str, bool]:
    return {
        "raw_ids": False,
        "raw_urls": False,
        "raw_final_urls": False,
        "raw_article_text": False,
        "raw_body_or_html": False,
        "raw_source_text": False,
        "headers": False,
        "secrets": False,
        "env_values": False,
        "database_url": False,
        "redis_url": False,
        "exception_bodies": False,
        "stderr": False,
        "traceback": False,
        "raw_remote_url": False,
    }


def _authority(state: RunnerState) -> dict[str, bool]:
    return {
        "runtime_config_loaded": state.runtime_config_loaded,
        "database_read_attempted": state.database_read_attempted,
        "database_write_attempted": state.database_write_attempted,
        "web_fetch_attempted": state.web_fetch_attempted,
        "artifact_snapshot_write_attempted": state.artifact_snapshot_write_attempted,
        "text_idea_snapshot_write_attempted": state.text_idea_snapshot_write_attempted,
        "evidence_bundle_write_attempted": state.evidence_bundle_write_attempted,
        "redis_consume_or_ack": False,
        "redis_group_create": False,
        "github_attempted": False,
        "x_attempted": False,
        "openai_attempted": False,
        "telegram_read_or_send_attempted": False,
        "notifier_attempted": False,
        "policy_attempted": False,
        "collector_attempted": False,
        "worker_loop_started": False,
        "systemd_called": False,
        "docker_called": False,
        "alembic_called": False,
    }


def _side_effects(state: RunnerState) -> dict[str, bool]:
    return {
        "db_read": state.database_read_attempted,
        "db_write": state.database_write_attempted,
        "web_fetch": state.web_fetch_attempted,
        "artifact_snapshot_write": state.artifact_snapshot_write_attempted,
        "text_idea_snapshot_write": state.text_idea_snapshot_write_attempted,
        "evidence_bundle_write": state.evidence_bundle_write_attempted,
        "redis_publish": False,
        "redis_consume": False,
        "redis_ack": False,
        "redis_group_create": False,
        "github_call": False,
        "x_call": False,
        "openai_call": False,
        "telegram_read": False,
        "telegram_send": False,
        "notifier": False,
        "policy": False,
        "collector": False,
        "worker_loop": False,
        "systemd": False,
        "docker": False,
        "alembic": False,
    }


__all__ = [
    "CONFIRM_TOKEN",
    "DatabaseHandle",
    "RuntimeConfig",
    "RunnerConfig",
    "RunnerResult",
    "TextIdeaSnapshotReadback",
    "TextIdeaTargetReadback",
    "WebSnapshotReadback",
    "WebTargetResolutionReadback",
    "build_parser",
    "main",
    "render_sanitized_json",
    "run",
    "run_bounded_web_text_idea_evidence_bundle",
    "run_bounded_web_text_idea_evidence_bundle_sync",
]


if __name__ == "__main__":
    raise SystemExit(main())
