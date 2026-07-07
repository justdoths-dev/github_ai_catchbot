from __future__ import annotations

import ast
import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from src.services.collector_telegram.operator_supplied_source import (
    OperatorSuppliedSourceAdapter,
    parse_operator_source_packet,
)
from src.services.evidence_assembler.config import EvidenceAssemblerConfig
from src.services.gh_enricher.github_client import (
    GitHubAccessDeniedError,
    GitHubClientError,
    GitHubNotFoundError,
    GitHubRateLimitedError,
)
from src.services.gh_enricher.models import CurrentSnapshotRef, EnrichmentRunRef
from src.services.maintenance.exact_target_source_to_analysis_materializer import (
    ExistingSourceProviderResumeAuthority,
    ExactTargetSourceToAnalysisConfigError,
    ExactTargetSourceToAnalysisRequest,
    FinalReadback,
    M1NotificationUxReadbackAcceptance,
    NormalizationReadback,
    ProviderEnrichmentRequest,
    ProviderEnrichmentResult,
    ProviderLiveAuthority,
    RefreshEventRecord,
    RuntimeConfigBundle,
    SqlProviderEnrichmentService,
    SqlStageComponents,
    load_m1_notification_ux_readback_acceptance,
    run_cli,
    run_exact_target_source_to_analysis_materializer,
)
from src.services.router_normalizer.config import RouterNormalizerConfig


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/maintenance/exact_target_source_to_analysis_materializer.py"
RAW_SECRET = "private exception body with raw packet text"
KOREAN_LLM_WORKFLOW_TEXT = (
    "회사에서 llm 사용 권한 받은김에 이것저것 작업 중인데.. "
    "머 보안때문에 되는게 없네요. cli는 쓸수도 없고.. 자동화를 할수가 없네"
)
GITHUB_URL_TEXT = (
    "https://github.com/DietrichGebert/ponytail\n\n"
    "AI가 코드를 작성하기 전에 다음 6단계의 사다리를 거치도록 통제합니다..."
)


class RegistryLookupResult:
    def mappings(self) -> "RegistryLookupResult":
        return self

    def all(self) -> list[dict[str, Any]]:
        return [
            {
                "registry_id": "11111111-1111-1111-1111-111111111111",
                "chat_id": 9001,
            }
        ]


class RegistryLookupSession:
    def __init__(self) -> None:
        self.sql_calls: list[str] = []

    async def execute(self, statement, params=None) -> RegistryLookupResult:
        del params
        self.sql_calls.append(str(statement))
        return RegistryLookupResult()


class NoProviderWriteSession:
    def __init__(self) -> None:
        self.sql_calls: list[str] = []

    async def execute(self, statement, params=None):
        del params
        self.sql_calls.append(str(statement))
        raise AssertionError("SQL provider enrichment path must fail before provider writes")


class SqlProviderTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class SqlProviderFakeRepository:
    def __init__(
        self,
        *,
        artifact_id: UUID,
        fail_insert_snapshot: bool = False,
        fail_insert_snapshot_updated_outbox: bool = False,
    ) -> None:
        from src.services.gh_enricher.models import ArtifactRecord

        self.fail_insert_snapshot = fail_insert_snapshot
        self.fail_insert_snapshot_updated_outbox = fail_insert_snapshot_updated_outbox
        self.artifact = ArtifactRecord(
            artifact_id=artifact_id,
            artifact_type="github_repo",
            canonical_id="github_repo:example/project",
            canonical_url="https://github.com/example/project",
            normalized_host="github.com",
            artifact_key_json={"owner": "example", "repo": "project"},
            current_snapshot_id=None,
            current_status=None,
        )
        self.runs: list[dict[str, Any]] = []
        self.snapshots: list[dict[str, Any]] = []
        self.outbox_event_id = uuid4()
        self.outbox: list[dict[str, Any]] = []

    def transaction(self):
        return SqlProviderTx()

    async def load_artifact(self, artifact_id):
        assert artifact_id == self.artifact.artifact_id
        return self.artifact

    async def load_current_snapshot(self, snapshot_id):
        assert snapshot_id is None
        return None

    async def load_enrichment_run_by_job_idempotency_key(self, *, job_idempotency_key: str):
        del job_idempotency_key
        return None

    async def load_valid_orphan_provider_snapshots(self, **kwargs):
        del kwargs
        return []

    async def insert_enrichment_run_if_absent(self, **kwargs):
        self.runs.append(kwargs)
        return uuid4()

    async def mark_enrichment_run_started(self, run_id):
        del run_id

    async def mark_enrichment_run_finished(self, **kwargs):
        del kwargs

    async def insert_snapshot(self, **kwargs):
        if self.fail_insert_snapshot:
            raise RuntimeError(RAW_SECRET)
        self.snapshots.append(kwargs)
        return uuid4()

    async def insert_github_repo_child(self, **kwargs):
        del kwargs

    async def insert_github_file_sample(self, **kwargs):
        del kwargs

    async def insert_discovered_url(self, **kwargs):
        del kwargs

    async def update_artifact_current_snapshot(self, **kwargs):
        del kwargs

    async def insert_snapshot_updated_outbox(self, **kwargs):
        if self.fail_insert_snapshot_updated_outbox:
            raise RuntimeError(RAW_SECRET)
        self.outbox.append(kwargs)
        return self.outbox_event_id


class SqlProviderConflictRetryFakeRepository(SqlProviderFakeRepository):
    def __init__(self, *, artifact_id: UUID, existing_status: str = "failed_transient") -> None:
        super().__init__(artifact_id=artifact_id)
        self.existing_status = existing_status
        self.status_by_key: dict[str, str] = {}
        self.run_key_by_id: dict[UUID, str] = {}
        self.claim_calls = 0
        self.status_loads = 0

    async def insert_enrichment_run_if_absent(self, **kwargs):
        self.runs.append(kwargs)
        self.status_by_key[kwargs["job_idempotency_key"]] = self.existing_status
        return None

    async def claim_failed_transient_enrichment_run_for_retry(self, *, job_idempotency_key: str):
        self.claim_calls += 1
        if self.status_by_key.get(job_idempotency_key) != "failed_transient":
            return None
        run_id = uuid4()
        self.status_by_key[job_idempotency_key] = "fetching"
        self.run_key_by_id[run_id] = job_idempotency_key
        return run_id

    async def load_enrichment_run_status_by_job_idempotency_key(self, *, job_idempotency_key: str):
        run = await self.load_enrichment_run_by_job_idempotency_key(
            job_idempotency_key=job_idempotency_key,
        )
        return None if run is None else run.status

    async def load_enrichment_run_by_job_idempotency_key(self, *, job_idempotency_key: str):
        self.status_loads += 1
        status = self.status_by_key.get(job_idempotency_key)
        if status is None:
            return None
        run_id = next(
            (
                existing_run_id
                for existing_run_id, key in self.run_key_by_id.items()
                if key == job_idempotency_key
            ),
            None,
        )
        if run_id is None:
            run_id = uuid4()
            self.run_key_by_id[run_id] = job_idempotency_key
        return EnrichmentRunRef(run_id=run_id, status=status)

    async def mark_enrichment_run_finished(self, **kwargs):
        key = self.run_key_by_id[kwargs["run_id"]]
        self.status_by_key[key] = kwargs["status"]


class SqlProviderOrphanSnapshotFakeRepository(SqlProviderConflictRetryFakeRepository):
    def __init__(
        self,
        *,
        artifact_id: UUID,
        existing_status: str = "fetching",
        orphan_snapshots: list[CurrentSnapshotRef] | None = None,
    ) -> None:
        super().__init__(artifact_id=artifact_id, existing_status=existing_status)
        self.orphan_snapshots = orphan_snapshots or [
            CurrentSnapshotRef(
                snapshot_id=uuid4(),
                status="ready",
                fetched_at=datetime.now(timezone.utc),
                content_anchor="commit:abc123",
                normalized_projection={"repo_full_name": "example/project"},
            )
        ]
        self.current_updates: list[dict[str, Any]] = []
        self.orphan_loads = 0

    async def load_valid_orphan_provider_snapshots(self, **kwargs):
        self.orphan_loads += 1
        assert kwargs["artifact_id"] == self.artifact.artifact_id
        assert kwargs["provider"] == "github"
        return self.orphan_snapshots[: kwargs.get("limit", 2)]

    async def update_artifact_current_snapshot(self, **kwargs):
        self.current_updates.append(kwargs)


class SqlProviderFakeGitHubClient:
    def __init__(self, *, repo_error: Exception | None = None) -> None:
        self.repo_error = repo_error
        self.calls: list[str] = []

    async def get_repo(self, owner, repo, *, auth_mode):
        del auth_mode
        self.calls.append("repo")
        if self.repo_error is not None:
            raise self.repo_error
        return {
            "full_name": f"{owner}/{repo}",
            "default_branch": "main",
            "description": "offline injected provider fixture",
            "homepage": None,
            "license": {"spdx_id": "MIT"},
            "topics": ["ai"],
            "language": "Python",
            "stargazers_count": 3,
            "subscribers_count": 1,
            "forks_count": 0,
            "open_issues_count": 0,
            "archived": False,
            "fork": False,
            "is_template": False,
            "pushed_at": "2026-06-01T00:00:00Z",
        }

    async def get_default_branch_head(self, owner, repo, default_branch, *, auth_mode):
        del owner, repo, default_branch, auth_mode
        self.calls.append("head")
        return {"sha": "abc123"}

    async def get_tree(self, owner, repo, ref, *, recursive, auth_mode):
        del owner, repo, ref, auth_mode
        self.calls.append("tree")
        return {
            "truncated": False,
            "tree": [
                {"type": "blob", "path": "README.md"},
                {"type": "blob", "path": "pyproject.toml"},
            ],
        } if recursive else {"truncated": False, "tree": []}

    async def get_contents(self, owner, repo, path, *, ref, auth_mode):
        del owner, repo, ref, auth_mode
        self.calls.append(f"contents:{path}")
        return {
            "encoding": "base64",
            "content": "IyBGYWtlIHByb3ZpZGVyIGV2aWRlbmNlCg==",
            "size": 25,
        }

    async def get_releases(self, owner, repo, *, auth_mode):
        del owner, repo, auth_mode
        self.calls.append("releases")
        return []


class Ledger:
    def __init__(self) -> None:
        self.registry_rows = [{"registry_id": str(uuid4()), "chat_id": 9001}]
        self.current: dict[tuple[str, int, int], dict[str, Any]] = {}
        self.versions: dict[str, list[dict[str, Any]]] = {}
        self.outbox: list[dict[str, Any]] = []
        self.normalization_runs: list[dict[str, Any]] = []
        self.candidate_group_id: UUID | None = None
        self.primary_artifact_id: UUID | None = None
        self.primary_artifact_type: str | None = None
        self.enrichment_requests = 0
        self.enrichment_request_event_id: UUID | None = None
        self.provider_route_counts: dict[str, int] = {}
        self.provider_route: str | None = None
        self.refresh_mode: str | None = None
        self.depth_budget: int | None = None
        self.provider_snapshot_id: UUID | None = None
        self.provider_snapshot_status = "ready"
        self.provider_snapshot_updated_event_id: UUID | None = None
        self.provider_error_code: str | None = None
        self.provider_orphan_snapshot_recovered = False
        self.provider_requires_live_authority = False
        self.refresh_event_id: UUID | None = None
        self.text_idea_snapshot_count = 0
        self.bundle_id: UUID | None = None
        self.evidence_member_count = 0
        self.analysis_request_event_id: UUID | None = None
        self.judge_runs = 0
        self.judge_call_events = 0
        self.raw_updates = 0
        self.call_order: list[str] = []
        self.normalizer_failure = False
        self.assembler_failure = False
        self.provider_authority: ProviderLiveAuthority | None = None


class FakeCollectorRepository:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    async def find_public_username_registry_targets(self, normalized_source_value: str):
        self.ledger.call_order.append(f"registry:{normalized_source_value}")
        return self.ledger.registry_rows

    async def get_source_message(self, *, platform: str, chat_id: int, message_id: int):
        self.ledger.call_order.append("collector.get_source_message")
        return self.ledger.current.get((platform, chat_id, message_id))

    async def get_latest_version(self, source_message_id: str):
        self.ledger.call_order.append("collector.get_latest_version")
        rows = self.ledger.versions.get(source_message_id, [])
        return rows[-1] if rows else None

    async def upsert_source_message(self, projection, *, platform: str = "telegram"):
        self.ledger.call_order.append("collector.upsert_source_message")
        key = (platform, projection.chat_id, projection.message_id)
        row = self.ledger.current.get(key)
        if row is None:
            row = {
                "source_message_id": str(uuid4()),
                "current_version_no": 0,
            }
            self.ledger.current[key] = row
        row.update(
            {
                "text_body": projection.text_body,
                "caption_text": projection.caption_text,
                "text_surface": projection.text_surface,
                "url_surface_json": projection.url_surface_json,
                "raw_message_json": projection.raw_message_json,
            }
        )
        return row

    async def append_source_message_version(
        self,
        *,
        source_message_id: str,
        projection,
        version_reason: str,
        observed_at=None,
        telegram_edit_date=None,
    ):
        self.ledger.call_order.append("collector.append_source_message_version")
        row = {
            "version_no": len(self.ledger.versions.get(source_message_id, [])) + 1,
            "content_hash": projection.content_hash,
            "version_reason": version_reason,
        }
        self.ledger.versions.setdefault(source_message_id, []).append(row)
        for current in self.ledger.current.values():
            if current["source_message_id"] == source_message_id:
                current["current_version_no"] = row["version_no"]
        return row

    async def insert_outbox_event(self, event):
        self.ledger.call_order.append("collector.insert_outbox_event")
        self.ledger.outbox.append(
            {
                "event_id": uuid4(),
                "event_type": event.event_type,
                "aggregate_type": event.aggregate_type,
                "aggregate_id": event.aggregate_id,
                "dedupe_key": event.dedupe_key,
                "payload_json": event.payload_json,
            }
        )
        return True

    async def get_outbox_event_by_dedupe_key(self, dedupe_key: str):
        self.ledger.call_order.append("collector.get_outbox_event_by_dedupe_key")
        for row in self.ledger.outbox:
            if row["dedupe_key"] == dedupe_key:
                return row
        return None


class FakeMaterializerRepository:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    async def load_normalization_readback(self, *, source_message_id: UUID, source_version_no: int):
        self.ledger.call_order.append("materializer.load_normalization_readback")
        del source_message_id, source_version_no
        return NormalizationReadback(
            normalization_runs=len(self.ledger.normalization_runs),
            candidate_groups=1 if self.ledger.candidate_group_id else 0,
            primary_members=1 if self.ledger.primary_artifact_id else 0,
            primary_artifact_type=self.ledger.primary_artifact_type,
            primary_artifact_id=self.ledger.primary_artifact_id,
            candidate_group_id=self.ledger.candidate_group_id,
            enrichment_requests=self.ledger.enrichment_requests,
            enrichment_request_event_id=self.ledger.enrichment_request_event_id,
            provider_route=self.ledger.provider_route,
            refresh_mode=self.ledger.refresh_mode,
            depth_budget=self.ledger.depth_budget,
            provider_route_counts=self.ledger.provider_route_counts,
        )

    async def insert_candidate_bundle_refresh_event(
        self,
        *,
        candidate_group_id: UUID,
        source_message_id: UUID,
        source_version_no: int,
        packet_fingerprint: str,
    ):
        self.ledger.call_order.append("materializer.insert_candidate_bundle_refresh_event")
        del candidate_group_id, source_message_id, source_version_no, packet_fingerprint
        if self.ledger.refresh_event_id is not None:
            return RefreshEventRecord(event_id=self.ledger.refresh_event_id, created=False)
        self.ledger.refresh_event_id = uuid4()
        self.ledger.outbox.append(
            {
                "event_id": self.ledger.refresh_event_id,
                "event_type": "candidate.bundle.refresh.v1",
            }
        )
        return RefreshEventRecord(event_id=self.ledger.refresh_event_id, created=True)

    async def load_final_readback(
        self,
        *,
        source_message_id: UUID,
        source_version_no: int,
        source_content_hash: str,
        chat_id: int,
        message_id: int,
        candidate_group_id: UUID,
    ):
        self.ledger.call_order.append("materializer.load_final_readback")
        source_id = str(source_message_id)
        del source_version_no, source_content_hash, chat_id, message_id, candidate_group_id
        return FinalReadback(
            source_messages=sum(
                1 for row in self.ledger.current.values() if row["source_message_id"] == source_id
            ),
            source_message_versions=len(self.ledger.versions.get(source_id, [])),
            source_created_events=sum(
                1 for row in self.ledger.outbox if row["event_type"] == "source_message.created.v1"
            ),
            telegram_raw_updates=self.ledger.raw_updates,
            normalization_runs=len(self.ledger.normalization_runs),
            candidate_groups=1 if self.ledger.candidate_group_id else 0,
            primary_text_idea_members=(
                1
                if self.ledger.primary_artifact_id and self.ledger.primary_artifact_type == "text_idea"
                else 0
            ),
            external_enrichment_requests=self.ledger.enrichment_requests,
            provider_snapshots=1 if self.ledger.provider_snapshot_id else 0,
            artifact_snapshot_updated_events=1 if self.ledger.provider_snapshot_updated_event_id else 0,
            text_idea_snapshots=self.ledger.text_idea_snapshot_count,
            ready_current_bundles=1 if self.ledger.bundle_id else 0,
            candidate_evidence_members=self.ledger.evidence_member_count,
            analysis_requested_events=1 if self.ledger.analysis_request_event_id else 0,
            judge_runs=self.ledger.judge_runs,
            judge_call_requested_events=self.ledger.judge_call_events,
            provider_snapshot_updated_event_id=self.ledger.provider_snapshot_updated_event_id,
            bundle_id=self.ledger.bundle_id,
            analysis_request_event_id=self.ledger.analysis_request_event_id,
        )


class FakeNormalizerService:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    async def process_stream_message(self, message) -> None:
        self.ledger.call_order.append("normalizer.process_stream_message")
        if self.ledger.normalizer_failure:
            raise RuntimeError(RAW_SECRET)
        if not self.ledger.normalization_runs:
            self.ledger.normalization_runs.append({"trigger_event_id": message.trigger_event_id})
            self.ledger.candidate_group_id = uuid4()
            self.ledger.primary_artifact_id = uuid4()
            current = next(iter(self.ledger.current.values()))
            if current.get("url_surface_json"):
                self.ledger.primary_artifact_type = "github_repo"
                self.ledger.enrichment_requests = 1
                self.ledger.enrichment_request_event_id = uuid4()
                self.ledger.provider_route = "github"
                self.ledger.refresh_mode = "standard"
                self.ledger.depth_budget = 1
                self.ledger.provider_route_counts = {"github": 1}
                self.ledger.outbox.append(
                    {
                        "event_id": self.ledger.enrichment_request_event_id,
                        "event_type": "artifact.enrich.requested.v1",
                    }
                )
            else:
                self.ledger.primary_artifact_type = "text_idea"


class FakeProviderEnrichmentService:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    async def materialize_provider_request(self, request) -> ProviderEnrichmentResult:
        self.ledger.call_order.append("provider.materialize_provider_request")
        self.ledger.provider_authority = request.provider_authority
        assert request.trigger_event_id == self.ledger.enrichment_request_event_id
        assert request.artifact_id == self.ledger.primary_artifact_id
        assert request.candidate_group_id == self.ledger.candidate_group_id
        assert request.provider_route == "github"
        if self.ledger.provider_requires_live_authority and not request.provider_authority.github_live_opened:
            return ProviderEnrichmentResult(
                provider_route="github",
                status="blocked",
                emitted_snapshot_updated=False,
                error_code="provider_live_authority_required",
            )
        if self.ledger.provider_error_code is not None:
            return ProviderEnrichmentResult(
                provider_route="github",
                status="unsupported",
                emitted_snapshot_updated=False,
                error_code=self.ledger.provider_error_code,
            )
        if self.ledger.provider_snapshot_status == "pending":
            return ProviderEnrichmentResult(
                provider_route="github",
                status="pending",
                emitted_snapshot_updated=False,
            )
        if self.ledger.provider_orphan_snapshot_recovered:
            self.ledger.provider_snapshot_id = self.ledger.provider_snapshot_id or uuid4()
            self.ledger.provider_snapshot_updated_event_id = (
                self.ledger.provider_snapshot_updated_event_id or uuid4()
            )
            return ProviderEnrichmentResult(
                provider_route="github",
                status="ready",
                emitted_snapshot_updated=True,
                snapshot_id=self.ledger.provider_snapshot_id,
                snapshot_updated_event_id=self.ledger.provider_snapshot_updated_event_id,
                snapshot_created=False,
                github_request_count=0,
            )
        self.ledger.provider_snapshot_id = self.ledger.provider_snapshot_id or uuid4()
        self.ledger.provider_snapshot_updated_event_id = (
            self.ledger.provider_snapshot_updated_event_id or uuid4()
        )
        return ProviderEnrichmentResult(
            provider_route="github",
            status=self.ledger.provider_snapshot_status,
            emitted_snapshot_updated=True,
            snapshot_id=self.ledger.provider_snapshot_id,
            snapshot_updated_event_id=self.ledger.provider_snapshot_updated_event_id,
            snapshot_created=True,
        )


class FakeAssemblerService:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    async def handle_trigger_event(self, trigger_event_id) -> None:
        self.ledger.call_order.append("assembler.handle_trigger_event")
        if self.ledger.assembler_failure:
            raise RuntimeError(RAW_SECRET)
        parsed_trigger_event_id = UUID(str(trigger_event_id))
        assert parsed_trigger_event_id in {
            self.ledger.refresh_event_id,
            self.ledger.provider_snapshot_updated_event_id,
        }
        if not self.ledger.bundle_id:
            if parsed_trigger_event_id == self.ledger.refresh_event_id:
                self.ledger.text_idea_snapshot_count = 1
            self.ledger.bundle_id = uuid4()
            self.ledger.evidence_member_count = 1
            self.ledger.analysis_request_event_id = uuid4()


class FakeStageComponents:
    def __init__(self, ledger: Ledger, stage_name: str) -> None:
        self.ledger = ledger
        self.stage_name = stage_name
        self.collector_repository = FakeCollectorRepository(ledger)
        self.source_adapter = OperatorSuppliedSourceAdapter()
        self.materializer_repository = FakeMaterializerRepository(ledger)
        self.normalizer_service = FakeNormalizerService(ledger)
        self.provider_enrichment_service = FakeProviderEnrichmentService(ledger)
        self.assembler_service = FakeAssemblerService(ledger)

    async def commit(self) -> None:
        self.ledger.call_order.append(f"commit:{self.stage_name}")


class FakeStageFactory:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    @asynccontextmanager
    async def stage(self, stage_name: str):
        self.ledger.call_order.append(f"enter:{stage_name}")
        yield FakeStageComponents(self.ledger, stage_name)
        self.ledger.call_order.append(f"exit:{stage_name}")


class FakeStageFactoryContext:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    async def __aenter__(self):
        return FakeStageFactory(self.ledger)

    async def __aexit__(self, exc_type, exc, tb):
        return False


def packet(text: str = KOREAN_LLM_WORKFLOW_TEXT):
    return parse_operator_source_packet(
        {
            "schema_version": "operator_supplied_telegram_source_v1",
            "source_ref": "https://t.me/SynthChannel/12345",
            "posted_at": "2026-06-23T01:02:03Z",
            "message_text": text,
        }
    )


def packet_json(text: str = KOREAN_LLM_WORKFLOW_TEXT) -> str:
    return json.dumps(
        {
            "schema_version": "operator_supplied_telegram_source_v1",
            "source_ref": "https://t.me/SynthChannel/12345",
            "posted_at": "2026-06-23T01:02:03Z",
            "message_text": text,
        },
        ensure_ascii=False,
    )


def m1_notification_ux_readback_payload(
    *,
    status: str = "pass",
    authority_overrides: dict[str, bool] | None = None,
    surface_overrides: dict[str, Any] | None = None,
    quality_overrides: dict[str, Any] | None = None,
    omit_delivery_quality: bool = False,
    stale_schema_status_only: bool = False,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    authority = {
        "live_telegram_transport_attempted": False,
        "live_openai_called": False,
        "live_github_called": False,
        "live_x_called": False,
        "live_web_called": False,
        "docker_or_systemd_called": False,
        "alembic_or_ddl_ran": False,
        "runtime_values_printed": False,
        "raw_payload_printed": False,
        "raw_ids_printed": False,
    }
    if authority_overrides:
        authority.update(authority_overrides)
    delivery_quality: dict[str, Any] = {
        "operator_actionability": "pass",
        "missing_sections": [],
        "visible_first_lines": [
            "[MID] [GitHub]",
            "판정: later | confidence 64",
            "제목: Useful repo",
        ],
        "button_count": 1,
        "message_char_count": 256,
        "notifier_reinterpreted_policy": False,
    }
    if quality_overrides:
        delivery_quality.update(quality_overrides)
    ux_surface: dict[str, Any] = {
        "status": "pass",
        "reason_code": "ok",
        "schema_valid": True,
        "checks_failed_count": 0,
        "verdict_first_section": True,
        "source_type_first_section": True,
        "severity_first_section": True,
        "urgency_first_section": True,
        "confidence_visible_or_not_applicable": True,
        "skeptical_or_risk_visible": True,
        "risk_visible": True,
        "recommended_action_visible": True,
        "evidence_limitations_visible": True,
        "primary_link_surface_visible": True,
        "link_buttons_present": True,
        "github_primary_expectations_preserved": True,
        "later_or_low_urgency_not_misleading": True,
        "high_urgency_not_silent": True,
        "message_under_limit": True,
        "link_preview_disabled": True,
        "protect_content_false": True,
        "raw_leak_checks_passed": True,
        "message_char_count": 256,
        "configured_message_char_limit": 3800,
        "button_count": 1,
        "button_labels": ["GitHub 열기"],
        "disable_notification": True,
    }
    if not omit_delivery_quality:
        ux_surface["delivery_quality_summary"] = delivery_quality
    if surface_overrides:
        ux_surface.update(surface_overrides)
    surfaces = (
        {"fake_backed_notification_operator_acceptance": {"status": "pass"}}
        if stale_schema_status_only
        else {"notification_ux_render_preview": ux_surface}
    )
    payload: dict[str, Any] = {
        "schema_version": "notification_operator_acceptance_readback_consolidation_v1",
        "status": status,
        "surfaces": surfaces,
        "authority": authority,
    }
    if extra:
        payload.update(extra)
    return payload


def write_json(path: Path, payload: Any) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return path


def load_m1_acceptance(tmp_path: Path, payload: dict[str, Any]) -> M1NotificationUxReadbackAcceptance:
    return load_m1_notification_ux_readback_acceptance(
        str(write_json(tmp_path / "m1-readback.json", payload))
    )


def test_load_m1_notification_ux_readback_acceptance_accepts_delivery_quality_pass(
    tmp_path: Path,
) -> None:
    acceptance = load_m1_acceptance(tmp_path, m1_notification_ux_readback_payload())

    assert acceptance.schema_version == "notification_operator_acceptance_readback_consolidation_v1"
    assert acceptance.delivery_quality_operator_actionability == "pass"
    assert acceptance.delivery_quality_missing_sections_count == 0
    assert acceptance.delivery_quality_button_count == 1
    assert acceptance.delivery_quality_message_char_count == 256
    assert acceptance.notifier_reinterpreted_policy is False


def test_load_m1_notification_ux_readback_acceptance_rejects_stale_readback(
    tmp_path: Path,
) -> None:
    path = write_json(
        tmp_path / "m1-readback.json",
        m1_notification_ux_readback_payload(omit_delivery_quality=True),
    )

    with pytest.raises(ExactTargetSourceToAnalysisConfigError) as excinfo:
        load_m1_notification_ux_readback_acceptance(str(path))

    assert str(excinfo.value) == "m1_notification_ux_readback_delivery_quality_missing"


def test_load_m1_notification_ux_readback_acceptance_rejects_schema_status_only_readback(
    tmp_path: Path,
) -> None:
    path = write_json(
        tmp_path / "m1-readback.json",
        m1_notification_ux_readback_payload(stale_schema_status_only=True),
    )

    with pytest.raises(ExactTargetSourceToAnalysisConfigError) as excinfo:
        load_m1_notification_ux_readback_acceptance(str(path))

    assert str(excinfo.value) == "m1_notification_ux_surface_missing"


def test_load_m1_notification_ux_readback_acceptance_rejects_missing_sections(
    tmp_path: Path,
) -> None:
    path = write_json(
        tmp_path / "m1-readback.json",
        m1_notification_ux_readback_payload(
            quality_overrides={"missing_sections": ["risk_marker"]}
        ),
    )

    with pytest.raises(ExactTargetSourceToAnalysisConfigError) as excinfo:
        load_m1_notification_ux_readback_acceptance(str(path))

    assert str(excinfo.value) == "m1_notification_ux_readback_delivery_quality_failed"


def test_load_m1_notification_ux_readback_acceptance_rejects_notifier_reinterpretation(
    tmp_path: Path,
) -> None:
    path = write_json(
        tmp_path / "m1-readback.json",
        m1_notification_ux_readback_payload(
            quality_overrides={"notifier_reinterpreted_policy": True}
        ),
    )

    with pytest.raises(ExactTargetSourceToAnalysisConfigError) as excinfo:
        load_m1_notification_ux_readback_acceptance(str(path))

    assert str(excinfo.value) == "m1_notification_ux_readback_notifier_reinterpreted_policy"


def test_load_m1_notification_ux_readback_acceptance_rejects_missing_required_check(
    tmp_path: Path,
) -> None:
    path = write_json(
        tmp_path / "m1-readback.json",
        m1_notification_ux_readback_payload(
            surface_overrides={"recommended_action_visible": False}
        ),
    )

    with pytest.raises(ExactTargetSourceToAnalysisConfigError) as excinfo:
        load_m1_notification_ux_readback_acceptance(str(path))

    assert str(excinfo.value) == "m1_notification_ux_readback_missing_required_check"


def runtime_bundle() -> RuntimeConfigBundle:
    return RuntimeConfigBundle(
        database_url="db_locator_not_used",
        values={},
        router_config=RouterNormalizerConfig(
            app_env="test",
            database_url="db_locator_not_used",
            redis_url="redis_locator_not_used",
            queue_name="q.source.normalize",
            consumer_group="router-normalizer",
            consumer_name="unit",
            block_ms=1,
            batch_size=1,
            normalizer_version="unit-normalizer",
            short_url_allowlist=(),
            short_url_hop_limit=1,
            short_url_timeout_seconds=0.1,
            log_level="INFO",
        ),
        assembler_config=EvidenceAssemblerConfig(
            app_env="test",
            database_url="db_locator_not_used",
            redis_url="redis_locator_not_used",
            queue_name="q.candidate.bundle",
            consumer_group="evidence-assembler",
            consumer_name="unit",
            batch_size=1,
            block_ms=1,
            bundle_profile_version="bundle_profile_v1",
            enable_text_idea=True,
            enable_reroot=True,
            log_level="INFO",
        ),
    )


async def run(
    ledger: Ledger,
    *,
    mode: str = "plan",
    text: str | None = None,
    provider_authority: ProviderLiveAuthority | None = None,
    provider_resume_authority: ExistingSourceProviderResumeAuthority | None = None,
    m1_notification_ux_readback: M1NotificationUxReadbackAcceptance | None = None,
):
    return await run_exact_target_source_to_analysis_materializer(
        ExactTargetSourceToAnalysisRequest(
            mode=mode,
            packet=packet(text or KOREAN_LLM_WORKFLOW_TEXT),
            provider_authority=provider_authority or ProviderLiveAuthority(),
            provider_resume_authority=(
                provider_resume_authority or ExistingSourceProviderResumeAuthority()
            ),
            m1_notification_ux_readback=m1_notification_ux_readback,
        ),
        stage_factory=FakeStageFactory(ledger),
    )


@pytest.mark.asyncio
async def test_sql_stage_components_wire_adapter_to_real_collector_registry_lookup() -> None:
    session = RegistryLookupSession()
    components = SqlStageComponents(session, runtime_bundle())

    target = await components.source_adapter.resolve_registry_target(
        components.collector_repository,
        packet(),
    )

    assert target.registry_id == "11111111-1111-1111-1111-111111111111"
    assert target.chat_id == 9001
    assert len(session.sql_calls) == 1
    normalized_sql = " ".join(session.sql_calls[0].split()).lower()
    assert "from telegram_channel_registry" in normalized_sql
    assert "source_messages" not in normalized_sql
    assert "event_outbox" not in normalized_sql


@pytest.mark.asyncio
async def test_sql_provider_enrichment_service_requires_live_authority_without_fake_writes() -> None:
    session = NoProviderWriteSession()
    service = SqlProviderEnrichmentService(session, runtime_bundle())

    result = await service.materialize_provider_request(
        ProviderEnrichmentRequest(
            trigger_event_id=uuid4(),
            candidate_group_id=uuid4(),
            artifact_id=uuid4(),
            artifact_type="github_repo",
            provider_route="github",
            refresh_mode="standard",
            depth_budget=1,
        )
    )

    assert result.provider_route == "github"
    assert result.status == "blocked"
    assert result.error_code == "provider_live_authority_required"
    assert result.emitted_snapshot_updated is False
    assert result.snapshot_id is None
    assert result.snapshot_updated_event_id is None
    assert result.snapshot_created is False
    assert result.external_network_attempted is False
    assert result.github_request_count == 0
    assert session.sql_calls == []


@pytest.mark.asyncio
async def test_sql_provider_partial_live_authority_blocks_before_network_or_snapshot_write() -> None:
    artifact_id = uuid4()
    fake_repository = SqlProviderFakeRepository(artifact_id=artifact_id)
    fake_client = SqlProviderFakeGitHubClient()
    service = SqlProviderEnrichmentService(
        object(),
        runtime_bundle(),
        github_client_factory=lambda config: fake_client,
        repository_factory=lambda session: fake_repository,
        track_external_network=False,
    )

    result = await service.materialize_provider_request(
        ProviderEnrichmentRequest(
            trigger_event_id=uuid4(),
            candidate_group_id=uuid4(),
            artifact_id=artifact_id,
            artifact_type="github_repo",
            provider_route="github",
            refresh_mode="standard",
            depth_budget=1,
            provider_authority=ProviderLiveAuthority(
                allow_live_github_provider_read=True,
                allow_provider_snapshot_write=False,
                provider_live_confirm="live-github-provider-evidence",
            ),
        )
    )

    assert result.status == "blocked"
    assert result.error_code == "provider_live_authority_required"
    assert result.github_request_count == 0
    assert result.external_network_attempted is False
    assert fake_client.calls == []
    assert fake_repository.snapshots == []
    assert fake_repository.outbox == []


@pytest.mark.asyncio
async def test_sql_provider_live_authority_rejects_non_github_route_without_network() -> None:
    fake_client = SqlProviderFakeGitHubClient()
    service = SqlProviderEnrichmentService(
        object(),
        runtime_bundle(),
        github_client_factory=lambda config: fake_client,
        repository_factory=lambda session: SqlProviderFakeRepository(artifact_id=uuid4()),
        track_external_network=False,
    )

    result = await service.materialize_provider_request(
        ProviderEnrichmentRequest(
            trigger_event_id=uuid4(),
            candidate_group_id=uuid4(),
            artifact_id=uuid4(),
            artifact_type="web_article",
            provider_route="web",
            refresh_mode="standard",
            depth_budget=1,
            provider_authority=ProviderLiveAuthority(
                allow_live_github_provider_read=True,
                allow_provider_snapshot_write=True,
                provider_live_confirm="live-github-provider-evidence",
            ),
        )
    )

    assert result.status == "blocked"
    assert result.error_code == "provider_route_not_supported_by_live_exact_materializer"
    assert result.github_request_count == 0
    assert result.external_network_attempted is False
    assert fake_client.calls == []


@pytest.mark.asyncio
async def test_sql_provider_live_authority_uses_real_gh_service_with_injected_client() -> None:
    artifact_id = uuid4()
    fake_repository = SqlProviderFakeRepository(artifact_id=artifact_id)
    fake_client = SqlProviderFakeGitHubClient()
    service = SqlProviderEnrichmentService(
        object(),
        runtime_bundle(),
        github_client_factory=lambda config: fake_client,
        repository_factory=lambda session: fake_repository,
        track_external_network=False,
    )

    result = await service.materialize_provider_request(
        ProviderEnrichmentRequest(
            trigger_event_id=uuid4(),
            candidate_group_id=uuid4(),
            artifact_id=artifact_id,
            artifact_type="github_repo",
            provider_route="github",
            refresh_mode="standard",
            depth_budget=1,
            provider_authority=ProviderLiveAuthority(
                allow_live_github_provider_read=True,
                allow_provider_snapshot_write=True,
                provider_live_confirm="live-github-provider-evidence",
            ),
        )
    )

    assert result.error_code is None
    assert result.status == "ready"
    assert result.snapshot_id is not None
    assert result.snapshot_updated_event_id == fake_repository.outbox_event_id
    assert result.snapshot_created is True
    assert result.emitted_snapshot_updated is True
    assert result.github_request_count == len(fake_client.calls)
    assert result.github_request_count >= 5
    assert result.external_network_attempted is False
    assert fake_repository.snapshots
    assert fake_repository.outbox


@pytest.mark.asyncio
async def test_sql_provider_failed_transient_conflict_retries_with_injected_github_client() -> None:
    artifact_id = uuid4()
    fake_repository = SqlProviderConflictRetryFakeRepository(artifact_id=artifact_id)
    fake_client = SqlProviderFakeGitHubClient()
    service = SqlProviderEnrichmentService(
        object(),
        runtime_bundle(),
        github_client_factory=lambda config: fake_client,
        repository_factory=lambda session: fake_repository,
        track_external_network=False,
    )

    result = await service.materialize_provider_request(
        ProviderEnrichmentRequest(
            trigger_event_id=uuid4(),
            candidate_group_id=uuid4(),
            artifact_id=artifact_id,
            artifact_type="github_repo",
            provider_route="github",
            refresh_mode="standard",
            depth_budget=1,
            provider_authority=ProviderLiveAuthority(
                allow_live_github_provider_read=True,
                allow_provider_snapshot_write=True,
                provider_live_confirm="live-github-provider-evidence",
            ),
        )
    )

    assert result.error_code is None
    assert result.status == "ready"
    assert result.snapshot_id is not None
    assert result.snapshot_updated_event_id == fake_repository.outbox_event_id
    assert result.snapshot_created is True
    assert result.emitted_snapshot_updated is True
    assert result.github_request_count == len(fake_client.calls)
    assert result.github_request_count >= 5
    assert result.external_network_attempted is False
    assert fake_repository.claim_calls == 1
    assert fake_repository.status_loads == 0
    assert len(fake_repository.runs) == 1
    assert set(fake_repository.status_by_key.values()) == {"ready"}
    assert fake_repository.snapshots
    assert fake_repository.outbox


@pytest.mark.asyncio
async def test_sql_provider_fetching_conflict_recovers_orphan_snapshot_without_github_read() -> None:
    artifact_id = uuid4()
    fake_repository = SqlProviderOrphanSnapshotFakeRepository(
        artifact_id=artifact_id,
        existing_status="fetching",
    )
    fake_client = SqlProviderFakeGitHubClient()
    service = SqlProviderEnrichmentService(
        object(),
        runtime_bundle(),
        github_client_factory=lambda config: fake_client,
        repository_factory=lambda session: fake_repository,
        track_external_network=False,
    )

    result = await service.materialize_provider_request(
        ProviderEnrichmentRequest(
            trigger_event_id=uuid4(),
            candidate_group_id=uuid4(),
            artifact_id=artifact_id,
            artifact_type="github_repo",
            provider_route="github",
            refresh_mode="standard",
            depth_budget=1,
            provider_authority=ProviderLiveAuthority(
                allow_live_github_provider_read=True,
                allow_provider_snapshot_write=True,
                provider_live_confirm="live-github-provider-evidence",
            ),
        )
    )

    orphan = fake_repository.orphan_snapshots[0]
    assert result.error_code is None
    assert result.status == "ready"
    assert result.snapshot_id == orphan.snapshot_id
    assert result.snapshot_updated_event_id == fake_repository.outbox_event_id
    assert result.snapshot_created is False
    assert result.emitted_snapshot_updated is True
    assert result.github_request_count == 0
    assert result.external_network_attempted is False
    assert fake_client.calls == []
    assert fake_repository.claim_calls == 1
    assert fake_repository.status_loads == 1
    assert fake_repository.orphan_loads == 1
    assert fake_repository.current_updates == [
        {
            "artifact_id": artifact_id,
            "snapshot_id": orphan.snapshot_id,
            "status": "ready",
        }
    ]
    assert fake_repository.status_by_key[fake_repository.runs[0]["job_idempotency_key"]] == "ready"
    assert fake_repository.snapshots == []
    assert fake_repository.outbox


@pytest.mark.asyncio
async def test_sql_provider_multiple_orphan_snapshots_returns_precise_error_without_github_read() -> None:
    artifact_id = uuid4()
    fake_repository = SqlProviderOrphanSnapshotFakeRepository(
        artifact_id=artifact_id,
        existing_status="fetching",
        orphan_snapshots=[
            CurrentSnapshotRef(
                snapshot_id=uuid4(),
                status="ready",
                fetched_at=datetime.now(timezone.utc),
                content_anchor="commit:abc123",
                normalized_projection={"repo_full_name": "example/project"},
            ),
            CurrentSnapshotRef(
                snapshot_id=uuid4(),
                status="partial_ready",
                fetched_at=datetime.now(timezone.utc),
                content_anchor="commit:def456",
                normalized_projection={"repo_full_name": "example/project"},
            ),
        ],
    )
    fake_client = SqlProviderFakeGitHubClient()
    service = SqlProviderEnrichmentService(
        object(),
        runtime_bundle(),
        github_client_factory=lambda config: fake_client,
        repository_factory=lambda session: fake_repository,
        track_external_network=False,
    )

    result = await service.materialize_provider_request(
        ProviderEnrichmentRequest(
            trigger_event_id=uuid4(),
            candidate_group_id=uuid4(),
            artifact_id=artifact_id,
            artifact_type="github_repo",
            provider_route="github",
            refresh_mode="standard",
            depth_budget=1,
            provider_authority=ProviderLiveAuthority(
                allow_live_github_provider_read=True,
                allow_provider_snapshot_write=True,
                provider_live_confirm="live-github-provider-evidence",
            ),
        )
    )

    assert result.status == "failed_permanent"
    assert result.error_code == "multiple_orphan_github_provider_snapshots"
    assert result.snapshot_id is None
    assert result.snapshot_updated_event_id is None
    assert result.snapshot_created is False
    assert result.github_request_count == 0
    assert result.external_network_attempted is False
    assert fake_client.calls == []
    assert fake_repository.current_updates == []
    assert fake_repository.snapshots == []
    assert fake_repository.outbox == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "reason_code"),
    [
        (GitHubRateLimitedError(f"rate limited {RAW_SECRET}"), "provider_github_rate_limited"),
        (GitHubAccessDeniedError(f"access denied {RAW_SECRET}"), "provider_github_access_denied"),
        (GitHubNotFoundError(f"not found {RAW_SECRET}"), "provider_github_not_found"),
        (GitHubClientError(f"client failed {RAW_SECRET}"), "provider_github_client_error"),
    ],
)
async def test_sql_provider_classifies_github_client_exceptions_without_raw_body(
    error: Exception,
    reason_code: str,
) -> None:
    artifact_id = uuid4()
    fake_repository = SqlProviderFakeRepository(artifact_id=artifact_id)
    fake_client = SqlProviderFakeGitHubClient(repo_error=error)
    service = SqlProviderEnrichmentService(
        object(),
        runtime_bundle(),
        github_client_factory=lambda config: fake_client,
        repository_factory=lambda session: fake_repository,
        track_external_network=False,
    )

    result = await service.materialize_provider_request(
        ProviderEnrichmentRequest(
            trigger_event_id=uuid4(),
            candidate_group_id=uuid4(),
            artifact_id=artifact_id,
            artifact_type="github_repo",
            provider_route="github",
            refresh_mode="standard",
            depth_budget=1,
            provider_authority=ProviderLiveAuthority(
                allow_live_github_provider_read=True,
                allow_provider_snapshot_write=True,
                provider_live_confirm="live-github-provider-evidence",
            ),
        )
    )
    rendered = json.dumps(asdict(result), sort_keys=True)

    assert result.error_code == reason_code
    assert result.github_request_count == 1
    assert result.external_network_attempted is False
    assert result.snapshot_created is False
    assert RAW_SECRET not in rendered
    assert "Traceback" not in rendered
    assert "example/project" not in rendered
    assert "https://github.com/example/project" not in rendered


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("repository_kwargs", "reason_code"),
    [
        ({"fail_insert_snapshot": True}, "provider_snapshot_write_failed"),
        ({"fail_insert_snapshot_updated_outbox": True}, "provider_snapshot_outbox_write_failed"),
    ],
)
async def test_sql_provider_classifies_repository_write_failures(
    repository_kwargs: dict[str, bool],
    reason_code: str,
) -> None:
    artifact_id = uuid4()
    fake_repository = SqlProviderFakeRepository(artifact_id=artifact_id, **repository_kwargs)
    fake_client = SqlProviderFakeGitHubClient()
    service = SqlProviderEnrichmentService(
        object(),
        runtime_bundle(),
        github_client_factory=lambda config: fake_client,
        repository_factory=lambda session: fake_repository,
        track_external_network=False,
    )

    result = await service.materialize_provider_request(
        ProviderEnrichmentRequest(
            trigger_event_id=uuid4(),
            candidate_group_id=uuid4(),
            artifact_id=artifact_id,
            artifact_type="github_repo",
            provider_route="github",
            refresh_mode="standard",
            depth_budget=1,
            provider_authority=ProviderLiveAuthority(
                allow_live_github_provider_read=True,
                allow_provider_snapshot_write=True,
                provider_live_confirm="live-github-provider-evidence",
            ),
        )
    )
    rendered = json.dumps(asdict(result), sort_keys=True)

    assert result.error_code == reason_code
    assert result.error_code != "provider_live_read_failed"
    assert RAW_SECRET not in rendered
    assert "Traceback" not in rendered


@pytest.mark.asyncio
async def test_plan_is_read_only_and_reports_local_text_idea_candidate() -> None:
    ledger = Ledger()

    report = await run(ledger, mode="plan")

    assert report.status == "pass"
    assert report.reason_code == "plan_ready"
    assert report.preflight_passed is True
    assert report.source_ingest_attempted is False
    assert report.normalization_attempted is False
    assert report.bundle_refresh_attempted is False
    assert report.assembler_attempted is False
    assert report.bounded_counts["predicted_candidates"] == 1
    assert report.bounded_counts["predicted_external_urls"] == 0
    assert ledger.current == {}
    assert ledger.outbox == []


@pytest.mark.asyncio
async def test_plan_predicts_provider_enrichment_for_url_before_writes() -> None:
    ledger = Ledger()

    report = await run(
        ledger,
        mode="plan",
        text=GITHUB_URL_TEXT,
    )

    assert report.status == "pass"
    assert report.reason_code == "plan_ready"
    assert report.bounded_counts["predicted_candidates"] == 1
    assert report.bounded_counts["predicted_external_urls"] == 1
    assert report.bounded_counts["predicted_enrichment_requests"] == 1
    assert report.bounded_counts["predicted_provider_route_github"] == 1
    assert ledger.current == {}
    assert ledger.outbox == []


@pytest.mark.asyncio
async def test_plan_blocks_weak_non_candidate_message() -> None:
    ledger = Ledger()

    report = await run(ledger, mode="plan", text="AI")

    assert report.status == "blocked"
    assert report.reason_code == "candidate_not_eligible"
    assert ledger.current == {}
    assert ledger.outbox == []


@pytest.mark.asyncio
async def test_execute_stage_order_materializes_exactly_one_analysis_request() -> None:
    ledger = Ledger()

    report = await run(ledger, mode="execute")

    assert report.status == "pass"
    assert report.reason_code == "analysis_request_materialized"
    assert report.source_ingest_attempted is True
    assert report.normalization_attempted is True
    assert report.bundle_refresh_attempted is True
    assert report.assembler_attempted is True
    assert report.source_message_created is True
    assert report.source_version_created is True
    assert report.candidate_created is True
    assert report.text_idea_snapshot_created is True
    assert report.bundle_created is True
    assert report.analysis_request_created is True
    assert report.artifact_enrichment_request_created is False
    assert report.openai_attempted is False
    assert report.redis_attempted is False
    assert report.telegram_live_read_attempted is False
    assert report.telegram_send_attempted is False
    assert report.external_network_attempted is False
    assert ledger.judge_runs == 0
    assert ledger.judge_call_events == 0
    assert report.bounded_counts["analysis_requested_events"] == 1
    assert report.bounded_counts["judge_runs"] == 0
    assert report.bounded_counts["judge_call_requested_events"] == 0
    assert ledger.call_order.index("commit:source_ingest") < ledger.call_order.index("enter:normalization")
    assert ledger.call_order.index("commit:normalization") < ledger.call_order.index("enter:normalization_readback")
    assert ledger.call_order.index("commit:refresh_event") < ledger.call_order.index("enter:assembler")
    assert ledger.call_order.index("commit:assembler") < ledger.call_order.index("enter:final_readback")


@pytest.mark.asyncio
async def test_execute_report_includes_restricted_source_channel_proof_without_raw_leaks() -> None:
    ledger = Ledger()

    report = await run(ledger, mode="execute")
    proof = report.restricted_source_channel_proof
    closure = report.mvp_closure_packet
    rendered = json.dumps(asdict(report), ensure_ascii=False, sort_keys=True)

    assert proof["schema_version"] == "github_ai_catchbot_restricted_source_channel_proof_v1"
    assert proof["status"] == "pass"
    assert proof["reason_code"] == "restricted_source_channel_proof_closed"
    assert proof["exact_source_channel_bounded"] is True
    assert proof["registry_lookup"] == {
        "public_username_scoped": True,
        "exact_single_target_required": True,
        "missing_target_fails_closed": True,
        "ambiguous_target_fails_closed": True,
    }
    assert proof["provenance"] == {
        "operator_supplied": True,
        "live_telegram_read": False,
        "operator_path_distinct_from_live_collector": True,
    }
    assert proof["source_to_analysis_boundary"] == {
        "source_packet_fingerprint_present": True,
        "source_ref_fingerprint_present": True,
        "source_message_fingerprint_present": True,
        "source_outbox_event_fingerprint_present": True,
        "normalization_run_visible": True,
        "candidate_group_fingerprint_present": True,
        "candidate_group_visible": True,
        "artifact_fingerprint_present": True,
        "evidence_bundle_fingerprint_present": True,
        "ready_evidence_bundle_visible": True,
        "analysis_request_fingerprint_present": True,
        "analysis_requested_visible": True,
        "judge_call_not_requested": True,
    }
    assert proof["bounded_counts"]["telegram_raw_updates"] == 0
    assert proof["authority"] == {
        "full_live_collector_opened": False,
        "tdlib_live_history_read_opened": False,
        "broad_registry_ingest_opened": False,
        "telegram_raw_updates_write_required": False,
        "telegram_send_opened": False,
        "openai_called": False,
        "github_called": False,
        "x_called": False,
        "web_called": False,
        "redis_consume_or_ack": False,
        "docker_or_systemd_called": False,
        "alembic_or_ddl_ran": False,
        "production_db_mutation": False,
    }
    assert all(proof["raw_values_printed"][name] is False for name in proof["raw_values_printed"])
    assert closure["schema_version"] == "github_ai_catchbot_mvp_closure_packet_v1"
    assert closure["status"] == "blocked"
    assert closure["reason_code"] == "mvp_closure_inputs_incomplete"
    assert closure["m1_notification_ux_acceptance_closed"] is False
    assert closure["m1_notification_ux_readback_schema_version"] is None
    assert closure["m2_restricted_source_channel_proof_closed"] is True
    assert closure["mvp_closure_packet_ready"] is False
    assert "AUTHORITY_OPEN" in closure["open_gates"]
    assert "ROLLOUT_OPEN" in closure["open_gates"]
    assert closure["completion_claims"]["mvp_code_proof_ux_packet_ready"] is False
    assert closure["completion_claims"]["final_function_complete"] is False
    assert closure["completion_claims"]["production_complete"] is False
    assert closure["completion_claims"]["bot_complete"] is False
    assert closure["completion_claims"]["one_hundred_percent_complete"] is False
    for raw_value in (
        "https://t.me/SynthChannel/12345",
        "SynthChannel/12345",
        KOREAN_LLM_WORKFLOW_TEXT,
        "db_locator_not_used",
        "redis_locator_not_used",
        RAW_SECRET,
        "Traceback",
    ):
        assert raw_value not in rendered


@pytest.mark.asyncio
async def test_execute_report_closes_mvp_packet_with_sanitized_m1_quality_summary(
    tmp_path: Path,
) -> None:
    ledger = Ledger()
    m1_acceptance = load_m1_acceptance(tmp_path, m1_notification_ux_readback_payload())

    report = await run(
        ledger,
        mode="execute",
        m1_notification_ux_readback=m1_acceptance,
    )

    closure = report.mvp_closure_packet
    rendered_report = json.dumps(asdict(report), ensure_ascii=False, sort_keys=True)
    rendered_closure = json.dumps(closure, ensure_ascii=False, sort_keys=True)

    assert report.status == "pass"
    assert report.restricted_source_channel_proof["status"] == "pass"
    assert closure["status"] == "pass"
    assert closure["reason_code"] == "mvp_code_proof_ux_packet_ready"
    assert closure["m1_notification_ux_acceptance_closed"] is True
    assert closure["m1_notification_ux_readback_schema_version"] == (
        "notification_operator_acceptance_readback_consolidation_v1"
    )
    assert closure["m2_restricted_source_channel_proof_closed"] is True
    assert closure["mvp_closure_packet_ready"] is True
    assert closure["completion_claims"]["mvp_code_proof_ux_packet_ready"] is True
    assert closure["completion_claims"]["final_function_complete"] is False
    assert closure["completion_claims"]["production_complete"] is False
    assert closure["completion_claims"]["bot_complete"] is False
    assert closure["completion_claims"]["one_hundred_percent_complete"] is False
    assert closure["m1_delivery_quality_operator_actionability"] == "pass"
    assert closure["m1_delivery_quality_missing_sections_count"] == 0
    assert closure["m1_delivery_quality_button_count"] == 1
    assert closure["m1_delivery_quality_message_char_count"] == 256
    assert closure["m1_notifier_reinterpreted_policy"] is False
    assert "visible_first_lines" not in closure
    assert "visible_first_lines" not in rendered_closure
    for forbidden in (
        "[MID] [GitHub]",
        "판정: later | confidence 64",
        "제목: Useful repo",
        "https://t.me/SynthChannel/12345",
        "SynthChannel/12345",
        KOREAN_LLM_WORKFLOW_TEXT,
        "db_locator_not_used",
        "redis_locator_not_used",
        "postgresql+psycopg" + "://",
        "redis" + "://",
        "TELEGRAM_BOT_TOKEN",
        "OPENAI_API_KEY",
        "tok" + "en=",
        "pass" + "word=",
        RAW_SECRET,
        "runtime.env",
        "payload_json",
        "telegram_response_json",
        "Traceback",
        "11111111-1111-1111-1111-111111111111",
    ):
        assert forbidden not in rendered_report


@pytest.mark.asyncio
async def test_execute_url_path_materializes_provider_snapshot_and_analysis_request() -> None:
    ledger = Ledger()

    report = await run(ledger, mode="execute", text=GITHUB_URL_TEXT)

    assert report.status == "pass"
    assert report.reason_code == "source_url_provider_evidence_analysis_requested"
    assert report.source_ingest_attempted is True
    assert report.normalization_attempted is True
    assert report.provider_enrichment_attempted is True
    assert report.bundle_refresh_attempted is False
    assert report.assembler_attempted is True
    assert report.source_message_created is True
    assert report.source_version_created is True
    assert report.candidate_created is True
    assert report.artifact_enrichment_request_created is True
    assert report.artifact_enrichment_request_fingerprint is not None
    assert report.provider_snapshot_created is True
    assert report.provider_snapshot_update_fingerprint is not None
    assert report.text_idea_snapshot_created is False
    assert report.bundle_created is True
    assert report.analysis_request_created is True
    assert report.openai_attempted is False
    assert report.redis_attempted is False
    assert report.telegram_live_read_attempted is False
    assert report.telegram_send_attempted is False
    assert report.external_network_attempted is False
    rendered_report = json.dumps(asdict(report), sort_keys=True)
    assert "https://github.com/DietrichGebert/ponytail" not in rendered_report
    assert "DietrichGebert" not in rendered_report
    assert "ponytail" not in rendered_report
    assert "AI가 코드를 작성" not in rendered_report
    assert report.bounded_counts["external_enrichment_requests"] == 1
    assert report.bounded_counts["provider_route_github"] == 1
    assert report.bounded_counts["provider_snapshots"] == 1
    assert report.bounded_counts["artifact_snapshot_updated_events"] == 1
    assert report.bounded_counts["analysis_requested_events"] == 1
    assert ledger.enrichment_requests == 1
    assert ledger.provider_snapshot_id is not None
    assert ledger.provider_snapshot_updated_event_id is not None
    assert ledger.bundle_id is not None
    assert ledger.analysis_request_event_id is not None
    assert sum(1 for row in ledger.outbox if row["event_type"] == "artifact.enrich.requested.v1") == 1
    assert "enter:refresh_event" not in ledger.call_order
    assert ledger.call_order.index("commit:provider_enrichment") < ledger.call_order.index("enter:assembler")


@pytest.mark.asyncio
async def test_execute_url_path_provider_pending_does_not_emit_analysis_request() -> None:
    ledger = Ledger()
    ledger.provider_snapshot_status = "pending"

    report = await run(ledger, mode="execute", text=GITHUB_URL_TEXT)

    assert report.status == "pass"
    assert report.reason_code == "provider_enrichment_run_in_progress"
    assert report.provider_enrichment_attempted is True
    assert report.assembler_attempted is False
    assert report.provider_snapshot_created is False
    assert report.bundle_created is False
    assert report.analysis_request_created is False
    assert report.bounded_counts["analysis_requested_events"] == 0
    assert ledger.provider_snapshot_id is None
    assert ledger.bundle_id is None
    assert ledger.analysis_request_event_id is None
    assert "enter:assembler" not in ledger.call_order


@pytest.mark.asyncio
async def test_execute_url_path_orphan_provider_snapshot_recovery_continues_to_analysis_request() -> None:
    ledger = Ledger()
    ledger.provider_orphan_snapshot_recovered = True

    report = await run(ledger, mode="execute", text=GITHUB_URL_TEXT)

    assert report.status == "pass"
    assert report.reason_code == "source_url_provider_evidence_analysis_requested"
    assert report.provider_enrichment_attempted is True
    assert report.assembler_attempted is True
    assert report.provider_snapshot_created is False
    assert report.provider_snapshot_update_fingerprint is not None
    assert report.bundle_created is True
    assert report.analysis_request_created is True
    assert report.bounded_counts["provider_snapshot_results"] == 1
    assert report.bounded_counts["provider_snapshot_updated_events"] == 1
    assert report.bounded_counts["github_request_count"] == 0
    assert report.bounded_counts["provider_snapshots"] == 1
    assert report.bounded_counts["artifact_snapshot_updated_events"] == 1
    assert report.bounded_counts["analysis_requested_events"] == 1
    assert ledger.provider_snapshot_id is not None
    assert ledger.provider_snapshot_updated_event_id is not None
    assert ledger.bundle_id is not None
    assert ledger.analysis_request_event_id is not None
    assert ledger.call_order.index("commit:provider_enrichment") < ledger.call_order.index("enter:assembler")


@pytest.mark.asyncio
async def test_execute_url_path_provider_low_evidence_does_not_emit_analysis_request() -> None:
    ledger = Ledger()
    ledger.provider_snapshot_status = "low_evidence"

    report = await run(ledger, mode="execute", text=GITHUB_URL_TEXT)

    assert report.status == "pass"
    assert report.reason_code == "provider_evidence_low_evidence"
    assert report.provider_enrichment_attempted is True
    assert report.assembler_attempted is False
    assert report.provider_snapshot_created is True
    assert report.bundle_created is False
    assert report.analysis_request_created is False
    assert report.bounded_counts["provider_snapshots"] == 1
    assert report.bounded_counts["artifact_snapshot_updated_events"] == 1
    assert report.bounded_counts["analysis_requested_events"] == 0
    assert ledger.provider_snapshot_id is not None
    assert ledger.bundle_id is None
    assert ledger.analysis_request_event_id is None
    assert "enter:assembler" not in ledger.call_order


@pytest.mark.asyncio
async def test_duplicate_execute_blocks_before_second_stage_write() -> None:
    ledger = Ledger()

    first = await run(ledger, mode="execute")
    second = await run(ledger, mode="execute")

    assert first.status == "pass"
    assert second.status == "blocked"
    assert second.reason_code == "source_packet_already_materialized"
    assert len(ledger.current) == 1
    source_id = next(iter(ledger.versions))
    assert len(ledger.versions[source_id]) == 1
    assert sum(1 for row in ledger.outbox if row["event_type"] == "source_message.created.v1") == 1
    assert sum(1 for row in ledger.outbox if row["event_type"] == "candidate.bundle.refresh.v1") == 1


@pytest.mark.asyncio
async def test_duplicate_url_execute_without_resume_gate_blocks_after_prior_provider_failure() -> None:
    ledger = Ledger()
    ledger.provider_error_code = "provider_github_client_error"

    first = await run(ledger, mode="execute", text=GITHUB_URL_TEXT)
    second = await run(ledger, mode="execute", text=GITHUB_URL_TEXT)

    assert first.status == "blocked"
    assert first.reason_code == "provider_github_client_error"
    assert second.status == "blocked"
    assert second.reason_code == "source_packet_already_materialized"
    assert second.source_ingest_attempted is False
    assert second.normalization_attempted is False
    assert second.provider_enrichment_attempted is False
    assert sum(len(rows) for rows in ledger.versions.values()) == 1
    assert sum(1 for row in ledger.outbox if row["event_type"] == "source_message.created.v1") == 1
    assert sum(1 for row in ledger.outbox if row["event_type"] == "artifact.enrich.requested.v1") == 1


@pytest.mark.asyncio
async def test_duplicate_url_resume_gate_without_live_provider_gate_blocks_before_snapshot() -> None:
    ledger = Ledger()
    ledger.provider_error_code = "provider_github_client_error"
    first = await run(ledger, mode="execute", text=GITHUB_URL_TEXT)
    ledger.provider_error_code = None
    ledger.provider_requires_live_authority = True
    second_call_start = len(ledger.call_order)

    second = await run(
        ledger,
        mode="execute",
        text=GITHUB_URL_TEXT,
        provider_resume_authority=ExistingSourceProviderResumeAuthority(
            allow_existing_source_provider_resume=True,
            provider_resume_confirm="resume-live-github-provider-evidence",
        ),
    )

    assert first.reason_code == "provider_github_client_error"
    assert second.status == "blocked"
    assert second.reason_code == "provider_live_authority_required"
    assert second.source_ingest_attempted is False
    assert second.normalization_attempted is False
    assert second.provider_enrichment_attempted is True
    assert second.assembler_attempted is False
    assert ledger.provider_snapshot_id is None
    assert ledger.provider_snapshot_updated_event_id is None
    assert ledger.analysis_request_event_id is None
    assert "normalizer.process_stream_message" not in ledger.call_order[second_call_start:]
    assert sum(len(rows) for rows in ledger.versions.values()) == 1
    assert sum(1 for row in ledger.outbox if row["event_type"] == "artifact.enrich.requested.v1") == 1


@pytest.mark.asyncio
async def test_duplicate_url_resume_gate_retries_provider_without_duplicate_source_or_candidate() -> None:
    ledger = Ledger()
    ledger.provider_error_code = "provider_github_client_error"
    first = await run(ledger, mode="execute", text=GITHUB_URL_TEXT)
    ledger.provider_error_code = None
    second_call_start = len(ledger.call_order)

    second = await run(
        ledger,
        mode="execute",
        text=GITHUB_URL_TEXT,
        provider_authority=ProviderLiveAuthority(
            allow_live_github_provider_read=True,
            allow_provider_snapshot_write=True,
            provider_live_confirm="live-github-provider-evidence",
        ),
        provider_resume_authority=ExistingSourceProviderResumeAuthority(
            allow_existing_source_provider_resume=True,
            provider_resume_confirm="resume-live-github-provider-evidence",
        ),
    )

    assert first.status == "blocked"
    assert first.reason_code == "provider_github_client_error"
    assert second.status == "pass"
    assert second.reason_code == "source_url_provider_evidence_analysis_requested"
    assert second.source_ingest_attempted is False
    assert second.normalization_attempted is False
    assert second.provider_enrichment_attempted is True
    assert second.assembler_attempted is True
    assert second.source_message_created is False
    assert second.source_version_created is False
    assert second.candidate_created is True
    assert second.artifact_enrichment_request_created is True
    assert second.provider_snapshot_created is True
    assert second.analysis_request_created is True
    assert second.bounded_counts["provider_snapshots"] == 1
    assert second.bounded_counts["artifact_snapshot_updated_events"] == 1
    assert second.bounded_counts["analysis_requested_events"] == 1
    assert len(ledger.current) == 1
    assert sum(len(rows) for rows in ledger.versions.values()) == 1
    assert sum(1 for row in ledger.outbox if row["event_type"] == "source_message.created.v1") == 1
    assert sum(1 for row in ledger.outbox if row["event_type"] == "artifact.enrich.requested.v1") == 1
    assert "collector.upsert_source_message" not in ledger.call_order[second_call_start:]
    assert "normalizer.process_stream_message" not in ledger.call_order[second_call_start:]


@pytest.mark.asyncio
async def test_duplicate_url_resume_reuses_existing_snapshot_event_without_provider_network() -> None:
    ledger = Ledger()
    ledger.provider_error_code = "provider_github_client_error"
    first = await run(ledger, mode="execute", text=GITHUB_URL_TEXT)
    ledger.provider_error_code = None
    ledger.provider_snapshot_id = uuid4()
    ledger.provider_snapshot_updated_event_id = uuid4()
    second_call_start = len(ledger.call_order)

    second = await run(
        ledger,
        mode="execute",
        text=GITHUB_URL_TEXT,
        provider_authority=ProviderLiveAuthority(
            allow_live_github_provider_read=True,
            allow_provider_snapshot_write=True,
            provider_live_confirm="live-github-provider-evidence",
        ),
        provider_resume_authority=ExistingSourceProviderResumeAuthority(
            allow_existing_source_provider_resume=True,
            provider_resume_confirm="resume-live-github-provider-evidence",
        ),
    )

    assert first.status == "blocked"
    assert first.reason_code == "provider_github_client_error"
    assert second.status == "pass"
    assert second.reason_code == "source_url_provider_evidence_analysis_requested"
    assert second.source_ingest_attempted is False
    assert second.normalization_attempted is False
    assert second.provider_enrichment_attempted is False
    assert second.assembler_attempted is True
    assert second.provider_snapshot_created is False
    assert second.provider_snapshot_update_fingerprint is not None
    assert second.bundle_created is True
    assert second.analysis_request_created is True
    assert second.openai_attempted is False
    assert second.redis_attempted is False
    assert second.telegram_send_attempted is False
    assert second.external_network_attempted is False
    assert second.bounded_counts["provider_snapshots"] == 1
    assert second.bounded_counts["artifact_snapshot_updated_events"] == 1
    assert second.bounded_counts["ready_current_bundles"] == 1
    assert second.bounded_counts["candidate_evidence_members"] == 1
    assert second.bounded_counts["analysis_requested_events"] == 1
    assert "provider.materialize_provider_request" not in ledger.call_order[second_call_start:]
    assert "normalizer.process_stream_message" not in ledger.call_order[second_call_start:]
    assembler_commit_index = ledger.call_order.index("commit:assembler", second_call_start)
    assert assembler_commit_index < ledger.call_order.index(
        "enter:final_readback",
        assembler_commit_index,
    )


@pytest.mark.asyncio
async def test_duplicate_url_resume_snapshot_without_update_event_fails_closed() -> None:
    ledger = Ledger()
    ledger.provider_error_code = "provider_github_client_error"
    first = await run(ledger, mode="execute", text=GITHUB_URL_TEXT)
    ledger.provider_error_code = None
    ledger.provider_snapshot_id = uuid4()
    second_call_start = len(ledger.call_order)

    second = await run(
        ledger,
        mode="execute",
        text=GITHUB_URL_TEXT,
        provider_authority=ProviderLiveAuthority(
            allow_live_github_provider_read=True,
            allow_provider_snapshot_write=True,
            provider_live_confirm="live-github-provider-evidence",
        ),
        provider_resume_authority=ExistingSourceProviderResumeAuthority(
            allow_existing_source_provider_resume=True,
            provider_resume_confirm="resume-live-github-provider-evidence",
        ),
    )

    assert first.status == "blocked"
    assert second.status == "blocked"
    assert second.reason_code == "provider_resume_snapshot_state_ambiguous"
    assert second.provider_enrichment_attempted is False
    assert second.assembler_attempted is False
    assert "provider.materialize_provider_request" not in ledger.call_order[second_call_start:]
    assert "assembler.handle_trigger_event" not in ledger.call_order[second_call_start:]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutate", "reason_code"),
    [
        (
            lambda ledger: setattr(ledger, "enrichment_requests", 2),
            "artifact_enrichment_request_cardinality_invalid",
        ),
        (
            lambda ledger: (
                setattr(ledger, "provider_route", "web"),
                setattr(ledger, "provider_route_counts", {"web": 1}),
            ),
            "provider_resume_provider_route_not_github",
        ),
        (
            lambda ledger: setattr(ledger, "analysis_request_event_id", uuid4()),
            "provider_resume_analysis_already_present",
        ),
        (
            lambda ledger: setattr(ledger, "candidate_group_id", None),
            "candidate_group_cardinality_invalid",
        ),
    ],
)
async def test_duplicate_url_resume_refuses_ambiguous_existing_state(mutate, reason_code: str) -> None:
    ledger = Ledger()
    ledger.provider_error_code = "provider_github_client_error"
    first = await run(ledger, mode="execute", text=GITHUB_URL_TEXT)
    ledger.provider_error_code = None
    mutate(ledger)

    second = await run(
        ledger,
        mode="execute",
        text=GITHUB_URL_TEXT,
        provider_authority=ProviderLiveAuthority(
            allow_live_github_provider_read=True,
            allow_provider_snapshot_write=True,
            provider_live_confirm="live-github-provider-evidence",
        ),
        provider_resume_authority=ExistingSourceProviderResumeAuthority(
            allow_existing_source_provider_resume=True,
            provider_resume_confirm="resume-live-github-provider-evidence",
        ),
    )

    assert first.reason_code == "provider_github_client_error"
    assert second.status == "blocked"
    assert second.reason_code == reason_code
    assert second.provider_enrichment_attempted is False
    assert second.assembler_attempted is False


@pytest.mark.asyncio
async def test_cli_live_provider_flags_reach_provider_request_authority(tmp_path: Path) -> None:
    ledger = Ledger()
    emitted: list[str] = []
    packet_path = tmp_path / "source-packet.json"
    packet_path.write_text(packet_json(GITHUB_URL_TEXT), encoding="utf-8")

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--source-packet-json",
            str(packet_path),
            "--env-file",
            "/tmp/not-read-by-test.env",
            "--confirm",
            "materialize-source-analysis",
            "--allow-live-github-provider-read",
            "--allow-provider-snapshot-write",
            "--provider-live-confirm",
            "live-github-provider-evidence",
        ],
        emit_json=emitted.append,
        runtime_config_loader=lambda env_file: runtime_bundle(),
        stage_factory_builder=lambda runtime_config: FakeStageFactoryContext(ledger),
    )

    assert exit_code == 0
    assert ledger.provider_authority is not None
    assert ledger.provider_authority.github_live_opened is True
    payload = json.loads(emitted[0])
    assert payload["status"] == "pass"
    assert payload["reason_code"] == "source_url_provider_evidence_analysis_requested"


@pytest.mark.asyncio
async def test_cli_provider_live_flags_are_blocked_in_plan_mode(tmp_path: Path) -> None:
    emitted: list[str] = []
    packet_path = tmp_path / "source-packet.json"
    packet_path.write_text(packet_json(GITHUB_URL_TEXT), encoding="utf-8")

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--source-packet-json",
            str(packet_path),
            "--env-file",
            "/tmp/not-read-by-test.env",
            "--allow-live-github-provider-read",
        ],
        emit_json=emitted.append,
        runtime_config_loader=lambda env_file: runtime_bundle(),
        stage_factory_builder=lambda runtime_config: FakeStageFactoryContext(Ledger()),
    )

    assert exit_code == 2
    payload = json.loads(emitted[0])
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == "provider_live_authority_not_allowed_for_plan"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("readback_payload", "reason_code"),
    [
        (
            m1_notification_ux_readback_payload(omit_delivery_quality=True),
            "m1_notification_ux_readback_delivery_quality_missing",
        ),
        (
            m1_notification_ux_readback_payload(stale_schema_status_only=True),
            "m1_notification_ux_surface_missing",
        ),
        (
            m1_notification_ux_readback_payload(status="blocked"),
            "m1_notification_ux_readback_not_pass",
        ),
        (
            m1_notification_ux_readback_payload(
                quality_overrides={"missing_sections": ["risk_marker"]}
            ),
            "m1_notification_ux_readback_delivery_quality_failed",
        ),
        (
            m1_notification_ux_readback_payload(
                quality_overrides={"notifier_reinterpreted_policy": True}
            ),
            "m1_notification_ux_readback_notifier_reinterpreted_policy",
        ),
        (
            m1_notification_ux_readback_payload(
                authority_overrides={"live_openai_called": True}
            ),
            "m1_notification_ux_readback_authority_open",
        ),
        (
            m1_notification_ux_readback_payload(
                extra={"unsafe_marker": "DATABASE_URL=postgresql+psycopg://private"}
            ),
            "m1_notification_ux_readback_not_sanitized",
        ),
    ],
)
async def test_cli_blocks_invalid_m1_readback_before_runtime_or_stage(
    tmp_path: Path,
    readback_payload: dict[str, Any],
    reason_code: str,
) -> None:
    emitted: list[str] = []
    packet_path = tmp_path / "source-packet.json"
    packet_path.write_text(packet_json(), encoding="utf-8")
    readback_path = write_json(tmp_path / "m1-readback.json", readback_payload)
    runtime_loads: list[str] = []

    def runtime_loader(env_file: str) -> RuntimeConfigBundle:
        runtime_loads.append(env_file)
        raise AssertionError("runtime config must not be loaded for invalid M1 readback")

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--source-packet-json",
            str(packet_path),
            "--env-file",
            "/tmp/not-read-by-test.env",
            "--confirm",
            "materialize-source-analysis",
            "--m1-notification-ux-readback-json",
            str(readback_path),
        ],
        emit_json=emitted.append,
        runtime_config_loader=runtime_loader,
        stage_factory_builder=lambda runtime_config: FakeStageFactoryContext(Ledger()),
    )

    payload = json.loads(emitted[0])
    assert exit_code == 2
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == reason_code
    assert payload["mvp_closure_packet"]["status"] == "blocked"
    assert payload["mvp_closure_packet"]["m1_notification_ux_acceptance_closed"] is False
    assert runtime_loads == []
    for forbidden in (
        str(packet_path),
        str(readback_path),
        "/tmp/not-read-by-test.env",
        "DATABASE_URL",
        "postgresql+psycopg" + "://",
        "redis" + "://",
        "TELEGRAM_BOT_TOKEN",
        "OPENAI_API_KEY",
        "payload_json",
        "telegram_response_json",
        "runtime.env",
        "Traceback",
        RAW_SECRET,
        KOREAN_LLM_WORKFLOW_TEXT,
        "llm",
        "https://t.me/SynthChannel/12345",
        "SynthChannel/12345",
        "TARGET_EVENT_ID",
    ):
        assert forbidden not in emitted[0]


@pytest.mark.asyncio
async def test_partial_stage_failure_returns_sanitized_failure_without_false_pass() -> None:
    ledger = Ledger()
    ledger.normalizer_failure = True

    report = await run(ledger, mode="execute")
    rendered = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "failed"
    assert report.reason_code == "unhandled_error"
    assert report.normalization_attempted is True
    assert report.bundle_refresh_attempted is False
    assert report.assembler_attempted is False
    assert RAW_SECRET not in rendered
    assert "Traceback" not in rendered


def test_module_does_not_import_openai_redis_telegram_notifier_or_judge_boundaries() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    forbidden = {
        "base64",
        "redis",
        "redis.asyncio",
        "openai",
        "telegram",
        "docker",
        "systemd",
        "alembic",
        "requests",
        "httpx",
        "aiohttp",
        "src.services.judge_openai",
        "..judge_openai",
        "src.services.notifier_telegram",
        "..notifier_telegram",
    }
    assert imported_modules.isdisjoint(forbidden)
    assert "LocalFakeGitHubClient" not in source
