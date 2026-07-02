from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
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
from src.services.evidence_assembler.models import AssemblyResult  # noqa: E402
from src.services.evidence_assembler.repositories import EvidenceAssemblerRepository  # noqa: E402
from src.services.evidence_assembler.service import EvidenceAssemblerService  # noqa: E402
from src.services.gh_enricher.config import GhEnricherConfig, GhEnricherConfigurationError  # noqa: E402
from src.services.gh_enricher.fetch_planner import GitHubFetchPlanner  # noqa: E402
from src.services.gh_enricher.file_sampler import GitHubFileSampler  # noqa: E402
from src.services.gh_enricher.github_app_auth import GitHubAppTokenProvider  # noqa: E402
from src.services.gh_enricher.github_client import GitHubClient, GitHubClientError  # noqa: E402
from src.services.gh_enricher.models import ArtifactEnrichmentJob, EnrichmentResult  # noqa: E402
from src.services.gh_enricher.repositories import GhEnricherRepository  # noqa: E402
from src.services.gh_enricher.service import (  # noqa: E402
    SUPPORTED_GITHUB_ARTIFACT_TYPES,
    GhEnricherService,
)
from src.services.gh_enricher.url_discovery import GitHubUrlDiscovery  # noqa: E402


SCHEMA_VERSION = "github_live_provider_to_evidence_bundle_proof_v1"
RUNNER_NAME = "bounded_github_provider_evidence_bundle_runner"
PLAN_MODE = "plan"
EXECUTE_MODE = "execute"
CONFIRM_TOKEN = "LIVE_GITHUB_PROVIDER_EVIDENCE_BUNDLE_EXECUTE"
READY_PROVIDER_STATUSES = frozenset({"ready", "partial_ready"})
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
HARD_GITHUB_REQUEST_LIMIT = 32
HARD_REQUEST_TIMEOUT_SEC = 10.0
HARD_SAMPLE_MAX_FILES = 20
HARD_SAMPLE_EXCERPT_CHARS = 1200
HARD_MAX_FILE_BYTES = 131_072
ALLOWED_GITHUB_API_BASE_URL = "https://api.github.com"


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
    event_id_suffix: str | None = None
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    confirm_token: str | None = None
    allow_github_live_read: bool = False
    allow_database_write: bool = False
    allow_artifact_snapshot_write: bool = False
    allow_evidence_bundle_write: bool = False


@dataclass(slots=True)
class RunnerState:
    runtime_config_loaded: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    github_live_read_attempted: bool = False
    github_request_count: int = 0
    snapshot_write_attempted: bool = False
    evidence_bundle_write_attempted: bool = False


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    gh_config: GhEnricherConfig
    assembler_config: EvidenceAssemblerConfig

    @property
    def database_url(self) -> str:
        return self.gh_config.database_url


@dataclass(frozen=True, slots=True)
class TargetEventRow:
    event_id: UUID
    event_type: str
    status: str
    aggregate_type: str
    aggregate_id: UUID
    payload_json: dict[str, Any]


@dataclass(frozen=True, slots=True)
class TargetResolutionReadback:
    artifact_count: int
    candidate_group_count: int
    candidate_member_count: int


@dataclass(frozen=True, slots=True)
class SnapshotReadback:
    snapshot_id: UUID
    artifact_id: UUID
    provider: str
    snapshot_type: str
    status: str
    content_anchor: str | None
    normalized_projection_present: bool
    github_child_count: int
    file_sample_count: int
    discovered_url_count: int


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


class ProofDatabase(Protocol):
    async def select_events_by_suffix(self, suffix: str) -> list[TargetEventRow]: ...

    async def load_job_by_trigger_event_id(self, event_id: UUID) -> ArtifactEnrichmentJob | None: ...

    async def verify_target_resolution(self, job: ArtifactEnrichmentJob) -> TargetResolutionReadback: ...

    async def run_gh_enricher(self, job: ArtifactEnrichmentJob) -> EnrichmentResult: ...

    async def read_snapshot(self, snapshot_id: UUID) -> SnapshotReadback | None: ...

    async def read_snapshot_updated_outbox(
        self,
        *,
        artifact_id: UUID,
        snapshot_id: UUID,
    ) -> OutboxReadback | None: ...

    async def run_evidence_assembler(self, snapshot_updated_event_id: UUID) -> list[AssemblyResult]: ...

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


@dataclass(frozen=True, slots=True)
class RunnerResult:
    status: str
    reason_code: str
    config: RunnerConfig
    state: RunnerState = field(default_factory=RunnerState)
    target_event_id: UUID | None = None
    artifact_id: UUID | None = None
    candidate_group_id: UUID | None = None
    artifact_type: str | None = None
    provider_route: str | None = None
    snapshot_id: UUID | None = None
    snapshot_status: str | None = None
    content_anchor: str | None = None
    snapshot_readback: SnapshotReadback | None = None
    snapshot_updated_outbox: OutboxReadback | None = None
    evidence_bundle_written_or_reused: bool = False
    bundle_id: UUID | None = None
    ready_for_analysis: bool | None = None
    analysis_requested_outbox: OutboxReadback | None = None
    reused_existing_bundle: bool | None = None

    @property
    def ok(self) -> bool:
        return self.status == "pass"

    def to_sanitized_dict(self) -> dict[str, Any]:
        snapshot = self.snapshot_readback
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": self.config.mode,
            "status": self.status,
            "reason_code": self.reason_code,
            "target_event_fingerprint": _fingerprint_uuid(self.target_event_id),
            "artifact_fingerprint": _fingerprint_uuid(self.artifact_id),
            "candidate_group_fingerprint": _fingerprint_uuid(self.candidate_group_id),
            "artifact_type": self.artifact_type,
            "provider_route": self.provider_route,
            "github_live_read_attempted": self.state.github_live_read_attempted,
            "github_request_count_bucket": _count_bucket(self.state.github_request_count),
            "db_write_attempted": self.state.database_write_attempted,
            "snapshot_written": snapshot is not None,
            "snapshot_status": self.snapshot_status,
            "snapshot_fingerprint": _fingerprint_uuid(self.snapshot_id),
            "content_anchor_fingerprint": _fingerprint_text(self.content_anchor),
            "github_child_readback": _github_child_readback(snapshot, self.artifact_type),
            "file_sample_count_bucket": _count_bucket(snapshot.file_sample_count if snapshot else 0),
            "discovered_url_count_bucket": _count_bucket(snapshot.discovered_url_count if snapshot else 0),
            "snapshot_updated_outbox_fingerprint": _fingerprint_uuid(
                self.snapshot_updated_outbox.event_id if self.snapshot_updated_outbox else None
            ),
            "evidence_bundle_written_or_reused": self.evidence_bundle_written_or_reused,
            "bundle_fingerprint": _fingerprint_uuid(self.bundle_id),
            "ready_for_analysis": self.ready_for_analysis,
            "analysis_requested_outbox_fingerprint": _fingerprint_uuid(
                self.analysis_requested_outbox.event_id if self.analysis_requested_outbox else None
            ),
            "redactions_applied": _redactions_applied(),
            "raw_values_printed": _raw_values_printed(),
            "authority": _authority(self.state),
            "side_effects": _side_effects(self.state),
            "reused_existing_bundle": self.reused_existing_bundle,
            "runtime_config_loaded": self.state.runtime_config_loaded,
            "database_read_attempted": self.state.database_read_attempted,
        }


class TrackedGitHubClient:
    def __init__(self, github_client: GitHubClient, state: RunnerState) -> None:
        self._github_client = github_client
        self._state = state

    async def get_repo(self, owner: str, repo: str, *, auth_mode: str) -> dict[str, Any]:
        self._count_request()
        return await self._github_client.get_repo(owner, repo, auth_mode=auth_mode)

    async def get_tree(self, owner: str, repo: str, ref: str, *, recursive: bool, auth_mode: str) -> dict[str, Any]:
        self._count_request()
        return await self._github_client.get_tree(owner, repo, ref, recursive=recursive, auth_mode=auth_mode)

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
        return await self._github_client.get_contents(owner, repo, path, ref=ref, auth_mode=auth_mode)

    async def get_releases(self, owner: str, repo: str, *, auth_mode: str) -> list[dict[str, Any]]:
        self._count_request()
        return await self._github_client.get_releases(owner, repo, auth_mode=auth_mode)

    async def get_default_branch_head(self, owner: str, repo: str, default_branch: str, *, auth_mode: str) -> dict[str, Any]:
        self._count_request()
        return await self._github_client.get_default_branch_head(owner, repo, default_branch, auth_mode=auth_mode)

    async def get_gist(self, gist_id: str, *, auth_mode: str) -> dict[str, Any]:
        self._count_request()
        return await self._github_client.get_gist(gist_id, auth_mode=auth_mode)

    def _count_request(self) -> None:
        self._state.github_live_read_attempted = True
        self._state.github_request_count += 1
        if self._state.github_request_count > HARD_GITHUB_REQUEST_LIMIT:
            raise ProofRunnerError("github_request_cap_exceeded")


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

    async def load_job_by_trigger_event_id(self, event_id: UUID) -> ArtifactEnrichmentJob | None:
        self._state.database_read_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            return await GhEnricherRepository(session).load_job_by_trigger_event_id(event_id)

    async def verify_target_resolution(self, job: ArtifactEnrichmentJob) -> TargetResolutionReadback:
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
                        ) AS candidate_member_count
                    """
                ),
                {"artifact_id": str(job.artifact_id), "candidate_group_id": str(job.candidate_group_id)},
            )
            row = result.mappings().one()
            return TargetResolutionReadback(
                artifact_count=int(row["artifact_count"]),
                candidate_group_count=int(row["candidate_group_count"]),
                candidate_member_count=int(row["candidate_member_count"]),
            )

    async def run_gh_enricher(self, job: ArtifactEnrichmentJob) -> EnrichmentResult:
        self._state.database_write_attempted = True
        self._state.snapshot_write_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            async with session.begin():
                service = GhEnricherService(
                    self._runtime_config.gh_config,
                    repository=GhEnricherRepository(session),
                    github_client=self._build_github_client(),
                    fetch_planner=GitHubFetchPlanner(),
                    file_sampler=GitHubFileSampler(),
                    url_discovery=GitHubUrlDiscovery(),
                    logger=self._logger,
                )
                return await service.handle_job(job)

    async def read_snapshot(self, snapshot_id: UUID) -> SnapshotReadback | None:
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
                               FROM artifact_snapshot_github_repo gr
                               WHERE gr.snapshot_id = s.snapshot_id
                           ) AS github_child_count,
                           (
                               SELECT COUNT(*)::int
                               FROM artifact_snapshot_github_file_samples fs
                               WHERE fs.snapshot_id = s.snapshot_id
                           ) AS file_sample_count,
                           (
                               SELECT COUNT(*)::int
                               FROM discovered_url_observations du
                               WHERE du.parent_snapshot_id = s.snapshot_id
                           ) AS discovered_url_count
                    FROM artifact_snapshots s
                    WHERE s.snapshot_id = CAST(:snapshot_id AS uuid)
                    """
                ),
                {"snapshot_id": str(snapshot_id)},
            )
            row = result.mappings().first()
            if row is None:
                return None
            projection = _json_loads(row["normalized_projection"])
            return SnapshotReadback(
                snapshot_id=UUID(str(row["snapshot_id"])),
                artifact_id=UUID(str(row["artifact_id"])),
                provider=str(row["provider"]),
                snapshot_type=str(row["snapshot_type"]),
                status=str(row["status"]),
                content_anchor=str(row["content_anchor"]) if row["content_anchor"] else None,
                normalized_projection_present=bool(projection),
                github_child_count=int(row["github_child_count"]),
                file_sample_count=int(row["file_sample_count"]),
                discovered_url_count=int(row["discovered_url_count"]),
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

    async def run_evidence_assembler(self, snapshot_updated_event_id: UUID) -> list[AssemblyResult]:
        self._state.database_write_attempted = True
        self._state.evidence_bundle_write_attempted = True
        session_factory = await self._ensure_session_factory()
        async with session_factory() as session:
            async with session.begin():
                service = EvidenceAssemblerService(
                    self._runtime_config.assembler_config,
                    repository=EvidenceAssemblerRepository(session),
                    logger=self._logger,
                )
                return await service.handle_trigger_event(snapshot_updated_event_id)

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

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()

    def _build_github_client(self) -> TrackedGitHubClient:
        config = self._runtime_config.gh_config
        token_provider = None
        client_config = config
        if config.github_app_id and config.github_installation_id and config.github_private_key:
            token_provider = GitHubAppTokenProvider(
                app_id=config.github_app_id,
                installation_id=config.github_installation_id,
                private_key_pem=config.github_private_key,
                api_base_url=config.github_api_base_url,
                timeout_sec=config.request_timeout_sec,
            )
        else:
            client_config = replace(config, github_app_id=None, github_installation_id=None, github_private_key=None)
        return TrackedGitHubClient(
            GitHubClient(
                api_base_url=client_config.github_api_base_url,
                timeout_sec=client_config.request_timeout_sec,
                token_provider=token_provider,
            ),
            self._state,
        )


async def build_default_database(
    runtime_config: RuntimeConfig,
    state: RunnerState,
    logger: logging.Logger,
) -> DatabaseHandle:
    database = SessionBackedProofDatabase(runtime_config=runtime_config, state=state, logger=logger)
    return DatabaseHandle(database=database, close=database.close)


def load_runtime_config() -> RuntimeConfig:
    try:
        gh_config = GhEnricherConfig.from_env()
        assembler_config = EvidenceAssemblerConfig.from_env()
        _validate_runtime_caps(gh_config)
    except GhEnricherConfigurationError as exc:
        raise ProofRunnerError(_config_error_code(str(exc))) from exc
    except EvidenceAssemblerConfigurationError as exc:
        raise ProofRunnerError(_config_error_code(str(exc))) from exc
    except ProofRunnerError:
        raise
    except Exception as exc:
        raise ProofRunnerError("runtime_config_error") from exc
    return RuntimeConfig(gh_config=gh_config, assembler_config=assembler_config)


async def run_bounded_github_provider_evidence_bundle(
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
        return RunnerResult(status="blocked", reason_code=exc.reason_code, config=config, state=state)
    except Exception:
        return RunnerResult(status="blocked", reason_code="runtime_config_error", config=config, state=state)

    database_handle: DatabaseHandle | None = None
    try:
        database_handle = await (database_builder or build_default_database)(runtime_config, state, effective_logger)
        database = database_handle.database
        target_rows = await database.select_events_by_suffix(config.event_id_suffix or "")
        target_error = _target_rows_error(target_rows)
        if target_error is not None:
            return RunnerResult(status="blocked", reason_code=target_error, config=config, state=state)

        target = target_rows[0]
        target_contract_error = _target_contract_error(target)
        target_payload = target.payload_json
        artifact_id = _uuid_or_none(target_payload.get("artifact_id"))
        candidate_group_id = _uuid_or_none(target_payload.get("candidate_group_id"))
        artifact_type = _str_or_none(target_payload.get("artifact_type"))
        provider_route = _str_or_none(target_payload.get("provider_route"))
        base = {
            "target_event_id": target.event_id,
            "artifact_id": artifact_id,
            "candidate_group_id": candidate_group_id,
            "artifact_type": artifact_type,
            "provider_route": provider_route,
        }
        if target_contract_error is not None:
            return RunnerResult(status="blocked", reason_code=target_contract_error, config=config, state=state, **base)

        job = await database.load_job_by_trigger_event_id(target.event_id)
        if job is None:
            return RunnerResult(status="blocked", reason_code="job_rehydrate_failed", config=config, state=state, **base)
        job_error = _job_contract_error(job, target)
        if job_error is not None:
            return RunnerResult(status="blocked", reason_code=job_error, config=config, state=state, **base)

        resolution = await database.verify_target_resolution(job)
        resolution_error = _resolution_error(resolution)
        if resolution_error is not None:
            return RunnerResult(status="blocked", reason_code=resolution_error, config=config, state=state, **base)

        if config.mode == PLAN_MODE:
            return RunnerResult(status="pass", reason_code="plan_exact_target_ready", config=config, state=state, **base)

        service_result = await database.run_gh_enricher(job)
        state.database_write_attempted = True
        snapshot_status = service_result.status
        result_fields = dict(base)
        result_fields.update(
            snapshot_id=service_result.snapshot_id,
            snapshot_status=snapshot_status,
            content_anchor=service_result.content_anchor,
        )

        if snapshot_status not in ALLOWED_PROVIDER_STATUSES:
            return RunnerResult(
                status="blocked",
                reason_code="provider_status_not_terminal_or_ready",
                config=config,
                state=state,
                **result_fields,
            )
        if snapshot_status not in READY_PROVIDER_STATUSES:
            return RunnerResult(
                status="blocked",
                reason_code=f"provider_status_{snapshot_status}",
                config=config,
                state=state,
                **result_fields,
            )
        if not state.github_live_read_attempted:
            return RunnerResult(
                status="blocked",
                reason_code="github_live_read_not_attempted",
                config=config,
                state=state,
                **result_fields,
            )
        if service_result.snapshot_id is None:
            return RunnerResult(
                status="failed",
                reason_code="snapshot_id_missing_after_ready_provider",
                config=config,
                state=state,
                **result_fields,
            )

        snapshot = await database.read_snapshot(service_result.snapshot_id)
        snapshot_error = _snapshot_readback_error(snapshot, job)
        if snapshot_error is not None:
            return RunnerResult(
                status="failed",
                reason_code=snapshot_error,
                config=config,
                state=state,
                snapshot_readback=snapshot,
                **result_fields,
            )

        snapshot_updated = None
        if service_result.emitted_snapshot_updated:
            snapshot_updated = await database.read_snapshot_updated_outbox(
                artifact_id=job.artifact_id,
                snapshot_id=service_result.snapshot_id,
            )
            if snapshot_updated is None:
                return RunnerResult(
                    status="failed",
                    reason_code="snapshot_updated_outbox_missing",
                    config=config,
                    state=state,
                    snapshot_readback=snapshot,
                    **result_fields,
                )
        else:
            return RunnerResult(
                status="failed",
                reason_code="snapshot_updated_outbox_not_emitted",
                config=config,
                state=state,
                snapshot_readback=snapshot,
                **result_fields,
            )

        assembly = await database.run_evidence_assembler(snapshot_updated.event_id)
        assembly_result = _select_assembly_result(assembly, job.candidate_group_id)
        if assembly_result is None or assembly_result.bundle_id is None:
            return RunnerResult(
                status="failed",
                reason_code="evidence_bundle_not_materialized",
                config=config,
                state=state,
                snapshot_readback=snapshot,
                snapshot_updated_outbox=snapshot_updated,
                **result_fields,
            )

        bundle = await database.read_bundle(
            candidate_group_id=job.candidate_group_id,
            bundle_id=assembly_result.bundle_id,
        )
        if bundle is None:
            return RunnerResult(
                status="failed",
                reason_code="bundle_readback_missing",
                config=config,
                state=state,
                snapshot_readback=snapshot,
                snapshot_updated_outbox=snapshot_updated,
                evidence_bundle_written_or_reused=True,
                bundle_id=assembly_result.bundle_id,
                ready_for_analysis=assembly_result.ready_for_analysis,
                reused_existing_bundle=assembly_result.reused_existing_bundle,
                **result_fields,
            )
        if not bundle.current_bundle_consistent:
            return RunnerResult(
                status="failed",
                reason_code="current_bundle_readback_inconsistent",
                config=config,
                state=state,
                snapshot_readback=snapshot,
                snapshot_updated_outbox=snapshot_updated,
                evidence_bundle_written_or_reused=True,
                bundle_id=bundle.bundle_id,
                ready_for_analysis=bundle.ready_for_analysis,
                reused_existing_bundle=assembly_result.reused_existing_bundle,
                **result_fields,
            )

        analysis_outbox = None
        if bundle.ready_for_analysis:
            analysis_outbox = await database.read_analysis_requested_outbox(
                candidate_group_id=job.candidate_group_id,
                bundle_id=bundle.bundle_id,
            )
            if analysis_outbox is None:
                return RunnerResult(
                    status="failed",
                    reason_code="analysis_requested_outbox_missing",
                    config=config,
                    state=state,
                    snapshot_readback=snapshot,
                    snapshot_updated_outbox=snapshot_updated,
                    evidence_bundle_written_or_reused=True,
                    bundle_id=bundle.bundle_id,
                    ready_for_analysis=bundle.ready_for_analysis,
                    reused_existing_bundle=assembly_result.reused_existing_bundle,
                    **result_fields,
                )

        return RunnerResult(
            status="pass",
            reason_code="github_provider_evidence_bundle_proof_complete",
            config=config,
            state=state,
            snapshot_readback=snapshot,
            snapshot_updated_outbox=snapshot_updated,
            evidence_bundle_written_or_reused=True,
            bundle_id=bundle.bundle_id,
            ready_for_analysis=bundle.ready_for_analysis,
            analysis_requested_outbox=analysis_outbox,
            reused_existing_bundle=assembly_result.reused_existing_bundle,
            **result_fields,
        )
    except GitHubClientError:
        return RunnerResult(status="failed", reason_code="github_client_error", config=config, state=state)
    except ProofRunnerError as exc:
        return RunnerResult(status="failed", reason_code=exc.reason_code, config=config, state=state)
    except Exception:
        return RunnerResult(status="failed", reason_code="unexpected_runner_error", config=config, state=state)
    finally:
        if database_handle is not None:
            try:
                await database_handle.close()
            except Exception:
                pass


def run_bounded_github_provider_evidence_bundle_sync(
    config: RunnerConfig,
    *,
    runtime_config_loader: Callable[[], RuntimeConfig] = load_runtime_config,
    database_builder: DatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> RunnerResult:
    return asyncio.run(
        run_bounded_github_provider_evidence_bundle(
            config,
            runtime_config_loader=runtime_config_loader,
            database_builder=database_builder,
            logger=logger,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Prove one GitHub provider artifact enrichment through EvidenceBundle assembly.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=[PLAN_MODE, EXECUTE_MODE], required=True)
    parser.add_argument("--event-id-suffix")
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--confirm-token")
    parser.add_argument("--allow-github-live-read", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-artifact-snapshot-write", action="store_true")
    parser.add_argument("--allow-evidence-bundle-write", action="store_true")
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader: Callable[[], RuntimeConfig] = load_runtime_config,
    database_builder: DatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> RunnerResult:
    return run_bounded_github_provider_evidence_bundle_sync(
        RunnerConfig(
            mode=str(args.mode),
            event_id_suffix=args.event_id_suffix,
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            confirm_token=args.confirm_token,
            allow_github_live_read=bool(args.allow_github_live_read),
            allow_database_write=bool(args.allow_database_write),
            allow_artifact_snapshot_write=bool(args.allow_artifact_snapshot_write),
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
    if not config.operator_approved:
        return "operator_approval_missing"
    suffix = (config.event_id_suffix or "").strip().lower()
    if len(suffix) < 8 or len(suffix) > 36 or any(char not in "0123456789abcdef-" for char in suffix):
        return "invalid_or_missing_event_id_suffix"
    if config.mode == PLAN_MODE and (
        config.confirm_token
        or config.allow_github_live_read
        or config.allow_database_write
        or config.allow_artifact_snapshot_write
        or config.allow_evidence_bundle_write
    ):
        return "execute_authority_not_allowed_for_plan"
    if config.mode == EXECUTE_MODE:
        if config.confirm_token != CONFIRM_TOKEN:
            return "confirm_token_missing_or_invalid"
        if not config.allow_github_live_read:
            return "github_live_read_not_allowed"
        if not config.allow_database_write:
            return "database_write_not_allowed"
        if not config.allow_artifact_snapshot_write:
            return "artifact_snapshot_write_not_allowed"
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


def _target_contract_error(row: TargetEventRow) -> str | None:
    if row.event_type != "artifact.enrich.requested.v1":
        return "target_event_type_not_artifact_enrich_requested"
    if row.status != "published":
        return "target_event_status_not_published"
    if row.aggregate_type != "artifact":
        return "target_event_aggregate_type_not_artifact"
    payload = row.payload_json
    missing = [
        key
        for key in ("candidate_group_id", "artifact_id", "artifact_type", "provider_route", "refresh_mode", "depth_budget")
        if key not in payload
    ]
    if missing:
        return "target_event_payload_missing_required_field"
    artifact_id = _uuid_or_none(payload.get("artifact_id"))
    candidate_group_id = _uuid_or_none(payload.get("candidate_group_id"))
    if artifact_id is None or candidate_group_id is None:
        return "target_event_payload_invalid_uuid"
    if artifact_id != row.aggregate_id:
        return "target_event_artifact_id_aggregate_mismatch"
    if payload.get("provider_route") != "github":
        return "target_event_provider_route_not_github"
    if payload.get("artifact_type") not in SUPPORTED_GITHUB_ARTIFACT_TYPES:
        return "target_event_artifact_type_not_supported_github"
    return None


def _job_contract_error(job: ArtifactEnrichmentJob, row: TargetEventRow) -> str | None:
    payload = row.payload_json
    if job.trigger_event_id != row.event_id:
        return "job_trigger_event_mismatch"
    if job.event_type != "artifact.enrich.requested.v1":
        return "job_event_type_mismatch"
    if job.provider_route != "github":
        return "job_provider_route_not_github"
    if job.artifact_type not in SUPPORTED_GITHUB_ARTIFACT_TYPES:
        return "job_artifact_type_not_supported_github"
    if job.artifact_id != _uuid_or_none(payload.get("artifact_id")):
        return "job_artifact_id_mismatch"
    if job.candidate_group_id != _uuid_or_none(payload.get("candidate_group_id")):
        return "job_candidate_group_id_mismatch"
    return None


def _resolution_error(readback: TargetResolutionReadback) -> str | None:
    if readback.artifact_count != 1:
        return "target_artifact_resolution_not_exactly_one"
    if readback.candidate_group_count != 1:
        return "target_candidate_group_resolution_not_exactly_one"
    if readback.candidate_member_count != 1:
        return "target_candidate_member_resolution_not_exactly_one"
    return None


def _snapshot_readback_error(snapshot: SnapshotReadback | None, job: ArtifactEnrichmentJob) -> str | None:
    if snapshot is None:
        return "snapshot_readback_missing"
    if snapshot.artifact_id != job.artifact_id:
        return "snapshot_artifact_mismatch"
    if snapshot.provider != "github":
        return "snapshot_provider_not_github"
    if snapshot.status not in ALLOWED_PROVIDER_STATUSES:
        return "snapshot_status_not_allowed"
    if snapshot.status in READY_PROVIDER_STATUSES and not snapshot.content_anchor:
        return "ready_snapshot_content_anchor_missing"
    if job.artifact_type != "github_gist":
        if snapshot.github_child_count < 1:
            return "github_child_readback_missing"
        if not snapshot.normalized_projection_present:
            return "github_projection_readback_missing"
        if snapshot.file_sample_count < 1:
            return "github_file_sample_readback_missing"
    return None


def _select_assembly_result(results: list[AssemblyResult], candidate_group_id: UUID) -> AssemblyResult | None:
    matches = [result for result in results if result.candidate_group_id == candidate_group_id]
    if len(matches) != 1:
        return None
    return matches[0]


def _validate_runtime_caps(config: GhEnricherConfig) -> None:
    if config.github_api_base_url.rstrip("/") != ALLOWED_GITHUB_API_BASE_URL:
        raise ProofRunnerError("github_api_base_url_not_allowed")
    if config.request_timeout_sec <= 0 or config.request_timeout_sec > HARD_REQUEST_TIMEOUT_SEC:
        raise ProofRunnerError("github_request_timeout_out_of_range")
    if config.sample_max_files <= 0 or config.sample_max_files > HARD_SAMPLE_MAX_FILES:
        raise ProofRunnerError("github_sample_max_files_out_of_range")
    if config.sample_excerpt_chars <= 0 or config.sample_excerpt_chars > HARD_SAMPLE_EXCERPT_CHARS:
        raise ProofRunnerError("github_sample_excerpt_chars_out_of_range")
    if config.max_file_bytes <= 0 or config.max_file_bytes > HARD_MAX_FILE_BYTES:
        raise ProofRunnerError("github_max_file_bytes_out_of_range")


def _config_error_code(text: str) -> str:
    if "DATABASE_URL" in text:
        return "database_url_missing"
    if "REDIS_URL" in text:
        return "redis_url_missing"
    return "runtime_config_error"


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


def _github_child_readback(snapshot: SnapshotReadback | None, artifact_type: str | None) -> dict[str, Any]:
    expected = artifact_type in {"github_repo", "github_subpath", "github_repo_page"}
    return {
        "expected": expected,
        "present": bool(snapshot and snapshot.github_child_count > 0),
        "projection_present": bool(snapshot and snapshot.normalized_projection_present),
    }


def _redactions_applied() -> dict[str, bool]:
    return {
        "full_event_ids_omitted": True,
        "full_artifact_ids_omitted": True,
        "full_candidate_group_ids_omitted": True,
        "raw_urls_omitted": True,
        "source_text_omitted": True,
        "secret_values_omitted": True,
        "env_values_omitted": True,
        "database_url_omitted": True,
        "redis_url_omitted": True,
        "exception_bodies_omitted": True,
        "stderr_omitted": True,
        "traceback_omitted": True,
        "github_response_body_omitted": True,
    }


def _raw_values_printed() -> dict[str, bool]:
    return {
        "raw_ids": False,
        "raw_urls": False,
        "source_text": False,
        "secrets": False,
        "env_values": False,
        "database_url": False,
        "redis_url": False,
        "exception_bodies": False,
        "stderr": False,
        "traceback": False,
        "github_response_body": False,
    }


def _authority(state: RunnerState) -> dict[str, bool]:
    return {
        "github_live_read_attempted": state.github_live_read_attempted,
        "database_read_attempted": state.database_read_attempted,
        "database_write_attempted": state.database_write_attempted,
        "artifact_snapshot_write_attempted": state.snapshot_write_attempted,
        "evidence_bundle_write_attempted": state.evidence_bundle_write_attempted,
        "telegram_send_attempted": False,
        "openai_attempted": False,
        "x_attempted": False,
        "web_attempted": False,
        "redis_consume_or_ack": False,
        "worker_loop_started": False,
        "systemd_called": False,
        "docker_called": False,
        "alembic_called": False,
    }


def _side_effects(state: RunnerState) -> dict[str, bool]:
    return {
        "db_read": state.database_read_attempted,
        "db_write": state.database_write_attempted,
        "github_live_read": state.github_live_read_attempted,
        "artifact_snapshot_write": state.snapshot_write_attempted,
        "evidence_bundle_write": state.evidence_bundle_write_attempted,
        "redis_publish": False,
        "redis_consume": False,
        "redis_ack": False,
        "telegram_send": False,
        "openai_call": False,
        "x_call": False,
        "web_call": False,
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
    "SnapshotReadback",
    "TargetEventRow",
    "TargetResolutionReadback",
    "build_parser",
    "main",
    "render_sanitized_json",
    "run",
    "run_bounded_github_provider_evidence_bundle",
    "run_bounded_github_provider_evidence_bundle_sync",
]


if __name__ == "__main__":
    raise SystemExit(main())
