from __future__ import annotations

import asyncio
import ast
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.evidence_assembler.config import EvidenceAssemblerConfig
from src.services.evidence_assembler.models import AssemblyResult
from src.services.web_enricher.config import WebEnricherConfig
from src.services.web_enricher.models import ArtifactEnrichmentJob, EnrichmentResult, FetchedDocument
from tools import bounded_web_text_idea_evidence_bundle_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_web_text_idea_evidence_bundle_runner.py"
DB_URL = "database-url-sentinel-redacted"
REDIS_URL = "redis-url-sentinel-redacted"
RAW_URL = "https://private.example.invalid/raw-url-sentinel"
RAW_FINAL_URL = "https://private.example.invalid/final-url-sentinel"
RAW_ARTICLE_TEXT = "article-text-sentinel-private-body"
RAW_SOURCE_TEXT = "source-text-sentinel-private-body"
RAW_EXCEPTION = "exception-detail-sentinel-redacted"
RAW_HEADER = "authorization-header-sentinel-redacted"
RAW_BODY = "<html>body-sentinel-private</html>"


WEB_EVENT_ID = UUID("11111111-1111-4111-8111-1111feedbeef")
TEXT_EVENT_ID = UUID("12121212-1212-4121-8121-1212feedb0b0")
WEB_ARTIFACT_ID = UUID("22222222-2222-4222-8222-2222cafed00d")
TEXT_ARTIFACT_ID = UUID("23232323-2323-4232-8232-2323cafed00d")
CANDIDATE_GROUP_ID = UUID("33333333-3333-4333-8333-3333deadbeef")
WEB_SNAPSHOT_ID = UUID("44444444-4444-4444-8444-4444aaaabbbb")
TEXT_SNAPSHOT_ID = UUID("45454545-4545-4545-8545-4545aaaabbbb")
SNAPSHOT_EVENT_ID = UUID("55555555-5555-4555-8555-5555aaaabbbb")
BUNDLE_ID = UUID("66666666-6666-4666-8666-6666aaaabbbb")
TEXT_BUNDLE_ID = UUID("67676767-6767-4676-8676-6767aaaabbbb")
ANALYSIS_EVENT_ID = UUID("77777777-7777-4777-8777-7777aaaabbbb")
TEXT_ANALYSIS_EVENT_ID = UUID("78787878-7878-4787-8787-7878aaaabbbb")


class RuntimeLoader:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> runner.RuntimeConfig:
        self.calls += 1
        return runner.RuntimeConfig(web_config=_web_config(), assembler_config=_assembler_config())


class RaisingRuntimeLoader:
    def __call__(self) -> runner.RuntimeConfig:
        raise AssertionError("runtime_config_loader_should_not_be_called")


class FakeDatabase:
    def __init__(
        self,
        *,
        state: runner.RunnerState,
        rows_by_suffix: dict[str, list[runner.TargetEventRow]] | None = None,
        web_job: ArtifactEnrichmentJob | None = None,
        web_resolution: runner.WebTargetResolutionReadback | None = None,
        web_enrichment_result: EnrichmentResult | None = None,
        web_enrichment_run: runner.EnrichmentRunReadback | None = None,
        web_snapshot: runner.WebSnapshotReadback | None = None,
        snapshot_outbox: runner.OutboxReadback | None = None,
        web_assembly_results: list[AssemblyResult] | None = None,
        text_readback: runner.TextIdeaTargetReadback | None = None,
        text_assembly_results: list[AssemblyResult] | None = None,
        text_snapshot: runner.TextIdeaSnapshotReadback | None = None,
        web_bundle: runner.BundleReadback | None = None,
        text_bundle: runner.BundleReadback | None = None,
        web_analysis_outbox: runner.OutboxReadback | None = None,
        text_analysis_outbox: runner.OutboxReadback | None = None,
        web_error: BaseException | None = None,
    ) -> None:
        self.state = state
        self.rows_by_suffix = rows_by_suffix or {
            WEB_EVENT_ID.hex[-8:]: [_web_target_row()],
            TEXT_EVENT_ID.hex[-8:]: [_text_target_row()],
        }
        self.web_job = web_job if web_job is not None else _web_job()
        self.web_resolution = web_resolution or runner.WebTargetResolutionReadback(1, 1, 1, True)
        self.web_enrichment_result = web_enrichment_result or EnrichmentResult(
            artifact_id=WEB_ARTIFACT_ID,
            snapshot_id=WEB_SNAPSHOT_ID,
            status="ready",
            content_anchor="web:" + "a" * 64,
            emitted_snapshot_updated=True,
        )
        self.web_enrichment_run = web_enrichment_run
        self.web_snapshot = web_snapshot if web_snapshot is not None else _web_snapshot()
        self.snapshot_outbox = snapshot_outbox if snapshot_outbox is not None else runner.OutboxReadback(
            event_id=SNAPSHOT_EVENT_ID,
            event_type="artifact.snapshot.updated.v1",
            status="pending",
        )
        self.web_assembly_results = web_assembly_results if web_assembly_results is not None else [
            AssemblyResult(
                candidate_group_id=CANDIDATE_GROUP_ID,
                bundle_id=BUNDLE_ID,
                reused_existing_bundle=False,
                ready_for_analysis=True,
                emitted_analysis_requested=True,
                analysis_requested_event_id=ANALYSIS_EVENT_ID,
            )
        ]
        self.text_readback = text_readback if text_readback is not None else _text_readback()
        self.text_assembly_results = text_assembly_results if text_assembly_results is not None else [
            AssemblyResult(
                candidate_group_id=CANDIDATE_GROUP_ID,
                bundle_id=TEXT_BUNDLE_ID,
                reused_existing_bundle=False,
                ready_for_analysis=True,
                emitted_analysis_requested=True,
                analysis_requested_event_id=TEXT_ANALYSIS_EVENT_ID,
            )
        ]
        self.text_snapshot = text_snapshot if text_snapshot is not None else _text_snapshot()
        self.web_bundle = web_bundle if web_bundle is not None else runner.BundleReadback(
            bundle_id=BUNDLE_ID,
            candidate_group_id=CANDIDATE_GROUP_ID,
            ready_for_analysis=True,
            current_bundle_consistent=True,
        )
        self.text_bundle = text_bundle if text_bundle is not None else runner.BundleReadback(
            bundle_id=TEXT_BUNDLE_ID,
            candidate_group_id=CANDIDATE_GROUP_ID,
            ready_for_analysis=True,
            current_bundle_consistent=True,
        )
        self.web_analysis_outbox = web_analysis_outbox
        self.text_analysis_outbox = text_analysis_outbox
        self.web_error = web_error
        self.closed = False
        self.calls: list[str] = []

    async def select_events_by_suffix(self, suffix: str) -> list[runner.TargetEventRow]:
        self.calls.append(f"select:{suffix}")
        self.state.database_read_attempted = True
        return self.rows_by_suffix.get(suffix, [])

    async def load_web_job_by_trigger_event_id(self, event_id: UUID) -> ArtifactEnrichmentJob | None:
        self.calls.append(f"load_web_job:{event_id.hex[-8:]}")
        self.state.database_read_attempted = True
        return self.web_job

    async def verify_web_target_resolution(self, job: ArtifactEnrichmentJob) -> runner.WebTargetResolutionReadback:
        del job
        self.calls.append("verify_web_resolution")
        self.state.database_read_attempted = True
        return self.web_resolution

    async def run_web_enricher(self, job: ArtifactEnrichmentJob) -> EnrichmentResult:
        del job
        self.calls.append("run_web_enricher")
        self.state.database_write_attempted = True
        self.state.web_fetch_attempted = True
        self.state.web_fetch_count = 1
        if self.web_error is not None:
            raise self.web_error
        self.state.artifact_snapshot_write_attempted = self.web_enrichment_result.snapshot_id is not None
        return self.web_enrichment_result

    async def read_latest_web_enrichment_run(
        self,
        *,
        artifact_id: UUID,
        status: str,
        content_anchor: str | None,
    ) -> runner.EnrichmentRunReadback | None:
        self.calls.append("read_latest_web_enrichment_run")
        self.state.database_read_attempted = True
        if self.web_enrichment_run is not None:
            return self.web_enrichment_run
        return runner.EnrichmentRunReadback(
            artifact_id=artifact_id,
            provider="web",
            status=status,
            content_anchor=content_anchor,
        )

    async def read_web_snapshot(self, snapshot_id: UUID) -> runner.WebSnapshotReadback | None:
        del snapshot_id
        self.calls.append("read_web_snapshot")
        self.state.database_read_attempted = True
        return self.web_snapshot

    async def read_snapshot_updated_outbox(
        self,
        *,
        artifact_id: UUID,
        snapshot_id: UUID,
    ) -> runner.OutboxReadback | None:
        del artifact_id, snapshot_id
        self.calls.append("read_snapshot_updated_outbox")
        self.state.database_read_attempted = True
        return self.snapshot_outbox

    async def resolve_text_idea_target(self, event_id: UUID) -> runner.TextIdeaTargetReadback:
        del event_id
        self.calls.append("resolve_text_idea_target")
        self.state.database_read_attempted = True
        return self.text_readback

    async def run_evidence_assembler(self, trigger_event_id: UUID, *, branch: str) -> list[AssemblyResult]:
        del trigger_event_id
        self.calls.append(f"run_evidence_assembler:{branch}")
        self.state.database_write_attempted = True
        self.state.evidence_bundle_write_attempted = True
        if branch == "text_idea":
            self.state.text_idea_snapshot_write_attempted = self.text_readback.existing_text_idea_snapshot_count == 0
            return self.text_assembly_results
        return self.web_assembly_results

    async def read_text_idea_snapshot(self, candidate_group_id: UUID) -> runner.TextIdeaSnapshotReadback | None:
        del candidate_group_id
        self.calls.append("read_text_idea_snapshot")
        self.state.database_read_attempted = True
        return self.text_snapshot

    async def read_bundle(self, *, candidate_group_id: UUID, bundle_id: UUID) -> runner.BundleReadback | None:
        del candidate_group_id
        self.calls.append(f"read_bundle:{bundle_id.hex[-8:]}")
        self.state.database_read_attempted = True
        if bundle_id == TEXT_BUNDLE_ID:
            return self.text_bundle
        return self.web_bundle

    async def read_analysis_requested_outbox(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
    ) -> runner.OutboxReadback | None:
        del candidate_group_id
        self.calls.append(f"read_analysis_requested_outbox:{bundle_id.hex[-8:]}")
        self.state.database_read_attempted = True
        if bundle_id == TEXT_BUNDLE_ID:
            if self.text_analysis_outbox is not None:
                return self.text_analysis_outbox
            return runner.OutboxReadback(
                event_id=TEXT_ANALYSIS_EVENT_ID,
                event_type="analysis.requested.v1",
                status="pending",
            )
        if self.web_analysis_outbox is not None:
            return self.web_analysis_outbox
        return runner.OutboxReadback(
            event_id=ANALYSIS_EVENT_ID,
            event_type="analysis.requested.v1",
            status="pending",
        )

    async def close(self) -> None:
        self.closed = True


class FakeDatabaseBuilder:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.calls = 0
        self.database: FakeDatabase | None = None

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        self.database = FakeDatabase(state=state, **self.kwargs)
        return runner.DatabaseHandle(database=self.database, close=self.database.close)


class FakeFetchClient:
    def __init__(self) -> None:
        self.calls = 0

    async def fetch(self, url: str) -> FetchedDocument:
        self.calls += 1
        return FetchedDocument(
            requested_url=url,
            final_url=RAW_FINAL_URL,
            status_code=200,
            content_type="text/html",
            body_bytes=b"body",
            body_text=RAW_BODY,
            response_headers_subset={"authorization": RAW_HEADER},
            content_hash="a" * 64,
            fetch_anomalies=[],
        )

    async def close(self) -> None:
        return None


def _run(config: runner.RunnerConfig, builder: FakeDatabaseBuilder, loader=None) -> runner.RunnerResult:
    return runner.run_bounded_web_text_idea_evidence_bundle_sync(
        config,
        runtime_config_loader=loader or RuntimeLoader(),
        database_builder=builder,
    )


def _plan_config(**overrides) -> runner.RunnerConfig:
    config = runner.RunnerConfig(
        mode="plan",
        web_event_id_suffix=WEB_EVENT_ID.hex[-8:],
        text_idea_event_id_suffix=TEXT_EVENT_ID.hex[-8:],
        allow_runtime_config=True,
        allow_database_read=True,
    )
    return replace(config, **overrides)


def _execute_config(**overrides) -> runner.RunnerConfig:
    config = runner.RunnerConfig(
        mode="execute",
        web_event_id_suffix=WEB_EVENT_ID.hex[-8:],
        text_idea_event_id_suffix=TEXT_EVENT_ID.hex[-8:],
        operator_approved=True,
        allow_runtime_config=True,
        allow_database_read=True,
        confirm_token=runner.CONFIRM_TOKEN,
        allow_web_fetch=True,
        allow_database_write=True,
        allow_artifact_snapshot_write=True,
        allow_text_idea_snapshot_write=True,
        allow_evidence_bundle_write=True,
    )
    return replace(config, **overrides)


def _web_target_row(**overrides) -> runner.TargetEventRow:
    payload = {
        "candidate_group_id": str(CANDIDATE_GROUP_ID),
        "artifact_id": str(WEB_ARTIFACT_ID),
        "artifact_type": "web_article",
        "provider_route": "web",
        "refresh_mode": "standard",
        "depth_budget": 1,
        "canonical_url": RAW_URL,
        "source_text": RAW_SOURCE_TEXT,
    }
    payload.update(overrides.pop("payload_json", {}))
    row = runner.TargetEventRow(
        event_id=WEB_EVENT_ID,
        event_type="artifact.enrich.requested.v1",
        status="published",
        aggregate_type="artifact",
        aggregate_id=WEB_ARTIFACT_ID,
        payload_json=payload,
    )
    return replace(row, **overrides)


def _text_target_row(**overrides) -> runner.TargetEventRow:
    payload = {
        "candidate_group_id": str(CANDIDATE_GROUP_ID),
        "artifact_id": str(TEXT_ARTIFACT_ID),
        "snapshot_id": str(TEXT_SNAPSHOT_ID),
        "source_text": RAW_SOURCE_TEXT,
    }
    payload.update(overrides.pop("payload_json", {}))
    row = runner.TargetEventRow(
        event_id=TEXT_EVENT_ID,
        event_type="candidate.bundle.refresh.v1",
        status="pending",
        aggregate_type="candidate_group",
        aggregate_id=CANDIDATE_GROUP_ID,
        payload_json=payload,
    )
    return replace(row, **overrides)


def _web_job(**overrides) -> ArtifactEnrichmentJob:
    job = ArtifactEnrichmentJob(
        trigger_event_id=WEB_EVENT_ID,
        event_type="artifact.enrich.requested.v1",
        candidate_group_id=CANDIDATE_GROUP_ID,
        artifact_id=WEB_ARTIFACT_ID,
        artifact_type="web_article",
        provider_route="web",
        refresh_mode="standard",
        depth_budget=1,
        requested_at=datetime.now(timezone.utc),
    )
    return replace(job, **overrides)


def _web_snapshot(**overrides) -> runner.WebSnapshotReadback:
    snapshot = runner.WebSnapshotReadback(
        snapshot_id=WEB_SNAPSHOT_ID,
        artifact_id=WEB_ARTIFACT_ID,
        provider="web",
        snapshot_type="web_article",
        status="ready",
        content_anchor="web:" + "a" * 64,
        normalized_projection_present=True,
        web_article_child_count=1,
        discovered_url_count=1,
        discovered_url_fingerprints=(runner._fingerprint_text(RAW_URL),),
    )
    return replace(snapshot, **overrides)


def _text_snapshot(**overrides) -> runner.TextIdeaSnapshotReadback:
    snapshot = runner.TextIdeaSnapshotReadback(
        snapshot_id=TEXT_SNAPSHOT_ID,
        artifact_id=TEXT_ARTIFACT_ID,
        provider="local_text_idea",
        snapshot_type="text_idea",
        status="ready",
        content_anchor="text_idea:" + "b" * 64,
        normalized_projection_present=True,
        text_idea_child_count=1,
        matching_snapshot_count=1,
    )
    return replace(snapshot, **overrides)


def _text_readback(**overrides) -> runner.TextIdeaTargetReadback:
    readback = runner.TextIdeaTargetReadback(
        target_count=1,
        candidate_group_id=CANDIDATE_GROUP_ID,
        candidate_group_count=1,
        source_identity_present=True,
        text_idea_member_count=1,
        source_text_present=True,
        current_primary_is_text_idea=True,
        usable_external_snapshot_count=0,
        existing_text_idea_snapshot_count=0,
    )
    return replace(readback, **overrides)


def _web_config() -> WebEnricherConfig:
    return WebEnricherConfig(
        app_env="test",
        database_url=DB_URL,
        redis_url=REDIS_URL,
        queue_name="q.artifact.enrich.web",
        consumer_group="web-enricher",
        consumer_name="bounded-proof-test",
        batch_size=1,
        block_ms=100,
        request_timeout_sec=1,
        max_redirects=1,
        max_bytes=4096,
        excerpt_chars=200,
        max_outbound_links=10,
        user_agent="test",
        content_type_allowlist=("text/html", "text/plain"),
        log_level="INFO",
    )


def _assembler_config() -> EvidenceAssemblerConfig:
    return EvidenceAssemblerConfig(
        app_env="test",
        database_url=DB_URL,
        redis_url=REDIS_URL,
        queue_name="q.candidate.bundle",
        consumer_group="evidence-assembler",
        consumer_name="bounded-proof-test",
        batch_size=1,
        block_ms=100,
        bundle_profile_version="bundle_profile_v1",
        enable_text_idea=True,
        enable_reroot=True,
        log_level="INFO",
    )


def test_plan_mode_reads_exact_targets_and_does_not_fetch_web_or_write_db() -> None:
    builder = FakeDatabaseBuilder()

    result = _run(_plan_config(), builder)
    report = result.to_sanitized_dict()

    assert result.status == "pass"
    assert result.reason_code == "plan_exact_requested_targets_ready"
    assert report["web_branch_status"] == "pass"
    assert report["text_idea_branch_status"] == "pass"
    assert report["web_fetch_attempted"] is False
    assert report["side_effects"]["db_write"] is False
    assert builder.database is not None
    assert "run_web_enricher" not in builder.database.calls
    assert not any(call.startswith("run_evidence_assembler") for call in builder.database.calls)


def test_execute_requires_confirm_token_before_runtime_config() -> None:
    result = runner.run_bounded_web_text_idea_evidence_bundle_sync(
        _execute_config(confirm_token="wrong-token"),
        runtime_config_loader=RaisingRuntimeLoader(),
        database_builder=FakeDatabaseBuilder(),
    )

    assert result.status == "blocked"
    assert result.reason_code == "confirm_token_missing_or_invalid"
    assert result.state.runtime_config_loaded is False
    assert result.state.database_read_attempted is False


@pytest.mark.parametrize("suffix", [None, "feedbee"])
def test_invalid_or_short_suffix_blocks_before_runtime_config(suffix: str | None) -> None:
    result = runner.run_bounded_web_text_idea_evidence_bundle_sync(
        _plan_config(web_event_id_suffix=suffix, text_idea_event_id_suffix=None),
        runtime_config_loader=RaisingRuntimeLoader(),
        database_builder=FakeDatabaseBuilder(),
    )

    assert result.status == "blocked"
    assert result.reason_code in {"no_branch_requested", "invalid_or_missing_web_event_id_suffix"}
    assert result.state.runtime_config_loaded is False
    assert result.state.database_read_attempted is False


def test_ambiguous_web_suffix_blocks_before_web_fetch_and_db_write() -> None:
    builder = FakeDatabaseBuilder(
        rows_by_suffix={WEB_EVENT_ID.hex[-8:]: [_web_target_row(), _web_target_row(event_id=uuid4())]}
    )

    result = _run(
        _execute_config(text_idea_event_id_suffix=None, allow_text_idea_snapshot_write=False),
        builder,
    )

    assert result.status == "blocked"
    assert result.reason_code == "target_event_suffix_ambiguous"
    assert result.state.web_fetch_attempted is False
    assert result.state.database_write_attempted is False
    assert builder.database is not None
    assert "run_web_enricher" not in builder.database.calls


@pytest.mark.parametrize(
    ("row", "reason_code"),
    [
        (
            _web_target_row(event_type="artifact.snapshot.updated.v1"),
            "web_target_event_type_not_artifact_enrich_requested",
        ),
        (_web_target_row(status="pending"), "web_target_event_status_not_published"),
        (
            _web_target_row(payload_json={"provider_route": "github"}),
            "web_target_event_provider_route_not_web",
        ),
        (
            _web_target_row(payload_json={"artifact_type": "x_post"}),
            "web_target_event_artifact_type_not_web_article",
        ),
    ],
)
def test_wrong_web_event_provider_or_artifact_type_blocks_before_fetch_or_write(row, reason_code) -> None:
    builder = FakeDatabaseBuilder(rows_by_suffix={WEB_EVENT_ID.hex[-8:]: [row]})

    result = _run(
        _execute_config(text_idea_event_id_suffix=None, allow_text_idea_snapshot_write=False),
        builder,
    )

    assert result.status == "blocked"
    assert result.reason_code == reason_code
    assert result.state.web_fetch_attempted is False
    assert result.state.database_write_attempted is False
    assert builder.database is not None
    assert "run_web_enricher" not in builder.database.calls


def test_web_execute_uses_web_enricher_path_and_reads_snapshot_child_urls_outbox_bundle() -> None:
    builder = FakeDatabaseBuilder()

    result = _run(
        _execute_config(text_idea_event_id_suffix=None, allow_text_idea_snapshot_write=False),
        builder,
    )
    report = result.to_sanitized_dict()

    assert result.status == "pass"
    assert report["web_branch_status"] == "pass"
    assert report["web_fetch_attempted"] is True
    assert report["web_fetch_count_bucket"] == "one"
    assert report["web_snapshot_status"] == "ready"
    assert report["web_article_child_readback"] == {
        "expected": True,
        "present": True,
        "projection_present": True,
    }
    assert report["web_discovered_url_count_bucket"] == "one"
    assert report["web_discovered_url_fingerprints"] == [runner._fingerprint_text(RAW_URL)]
    assert report["web_snapshot_updated_outbox_fingerprint"] is not None
    assert report["web_evidence_bundle_written_or_reused"] is True
    assert builder.database is not None
    assert "run_web_enricher" in builder.database.calls
    assert "read_web_snapshot" in builder.database.calls
    assert "run_evidence_assembler:web" in builder.database.calls


def test_web_fetch_count_cap_enforces_exactly_one_fetch() -> None:
    state = runner.RunnerState()
    client = runner.TrackedWebFetchClient(FakeFetchClient(), state)  # type: ignore[arg-type]

    asyncio.run(client.fetch(RAW_URL))

    assert state.web_fetch_attempted is True
    assert state.web_fetch_count == 1
    with pytest.raises(runner.ProofRunnerError) as exc_info:
        asyncio.run(client.fetch(RAW_URL))
    assert exc_info.value.reason_code == "web_fetch_request_cap_exceeded"


@pytest.mark.parametrize("status", ["rate_limited", "access_denied", "failed_permanent", "failed_transient", "unsupported"])
def test_web_failure_statuses_are_sanitized_and_not_false_pass(status: str) -> None:
    builder = FakeDatabaseBuilder(
        web_enrichment_result=EnrichmentResult(
            artifact_id=WEB_ARTIFACT_ID,
            snapshot_id=None,
            status=status,
            content_anchor=None,
            emitted_snapshot_updated=False,
        )
    )

    result = _run(
        _execute_config(text_idea_event_id_suffix=None, allow_text_idea_snapshot_write=False),
        builder,
    )
    report_text = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.status == "blocked"
    assert result.reason_code == f"provider_status_{status}"
    assert '"status": "pass"' not in report_text
    assert RAW_URL not in report_text
    assert RAW_FINAL_URL not in report_text
    assert RAW_ARTICLE_TEXT not in report_text
    assert RAW_SOURCE_TEXT not in report_text
    assert RAW_EXCEPTION not in report_text
    assert RAW_HEADER not in report_text
    assert RAW_BODY not in report_text
    assert DB_URL not in report_text
    assert REDIS_URL not in report_text


def test_low_evidence_web_snapshot_can_materialize_bundle_without_analysis_outbox() -> None:
    builder = FakeDatabaseBuilder(
        web_enrichment_result=EnrichmentResult(
            artifact_id=WEB_ARTIFACT_ID,
            snapshot_id=WEB_SNAPSHOT_ID,
            status="low_evidence",
            content_anchor="web:" + "c" * 64,
            emitted_snapshot_updated=True,
        ),
        web_snapshot=_web_snapshot(status="low_evidence", content_anchor="web:" + "c" * 64),
        web_assembly_results=[
            AssemblyResult(
                candidate_group_id=CANDIDATE_GROUP_ID,
                bundle_id=BUNDLE_ID,
                reused_existing_bundle=False,
                ready_for_analysis=False,
                emitted_analysis_requested=False,
                analysis_requested_event_id=None,
            )
        ],
        web_bundle=runner.BundleReadback(
            bundle_id=BUNDLE_ID,
            candidate_group_id=CANDIDATE_GROUP_ID,
            ready_for_analysis=False,
            current_bundle_consistent=True,
        ),
    )

    result = _run(
        _execute_config(text_idea_event_id_suffix=None, allow_text_idea_snapshot_write=False),
        builder,
    )
    report = result.to_sanitized_dict()

    assert result.status == "pass"
    assert report["web_snapshot_status"] == "low_evidence"
    assert report["web_ready_for_analysis"] is False
    assert report["web_analysis_requested_outbox_fingerprint"] is None


def test_text_idea_execute_uses_evidence_assembler_text_idea_path_and_reads_child_bundle() -> None:
    builder = FakeDatabaseBuilder()

    result = _run(
        _execute_config(web_event_id_suffix=None, allow_web_fetch=False, allow_artifact_snapshot_write=False),
        builder,
    )
    report = result.to_sanitized_dict()

    assert result.status == "pass"
    assert report["web_branch_status"] == "not_requested"
    assert report["text_idea_branch_status"] == "pass"
    assert report["text_idea_snapshot_status"] == "ready"
    assert report["text_idea_child_readback"] == {
        "expected": True,
        "present": True,
        "projection_present": True,
    }
    assert report["text_idea_evidence_bundle_written_or_reused"] is True
    assert report["text_idea_analysis_requested_outbox_fingerprint"] is not None
    assert builder.database is not None
    assert "resolve_text_idea_target" in builder.database.calls
    assert "run_evidence_assembler:text_idea" in builder.database.calls
    assert "read_text_idea_snapshot" in builder.database.calls


def test_missing_source_text_blocks_text_idea_branch_without_false_pass() -> None:
    builder = FakeDatabaseBuilder(text_readback=_text_readback(source_text_present=False))

    result = _run(
        _execute_config(web_event_id_suffix=None, allow_web_fetch=False, allow_artifact_snapshot_write=False),
        builder,
    )

    assert result.status == "blocked"
    assert result.reason_code == "text_idea_source_text_missing"
    assert result.text_idea.status == "blocked"
    assert builder.database is not None
    assert "run_evidence_assembler:text_idea" not in builder.database.calls


def test_existing_text_idea_snapshot_reuse_is_reported() -> None:
    builder = FakeDatabaseBuilder(text_readback=_text_readback(existing_text_idea_snapshot_count=1))

    result = _run(
        _execute_config(web_event_id_suffix=None, allow_web_fetch=False, allow_artifact_snapshot_write=False),
        builder,
    )
    report = result.to_sanitized_dict()

    assert result.status == "pass"
    assert report["reused_existing_text_idea_snapshot"] is True
    assert result.state.text_idea_snapshot_write_attempted is False


def test_ready_for_analysis_true_requires_analysis_requested_outbox() -> None:
    class MissingAnalysisOutboxDatabase(FakeDatabase):
        async def read_analysis_requested_outbox(self, *, candidate_group_id: UUID, bundle_id: UUID):
            del candidate_group_id, bundle_id
            self.calls.append("read_analysis_requested_outbox:missing")
            self.state.database_read_attempted = True
            return None

    class MissingAnalysisOutboxBuilder(FakeDatabaseBuilder):
        async def __call__(self, runtime_config, state, logger):
            del runtime_config, logger
            self.calls += 1
            self.database = MissingAnalysisOutboxDatabase(state=state, **self.kwargs)
            return runner.DatabaseHandle(database=self.database, close=self.database.close)

    builder = MissingAnalysisOutboxBuilder()

    result = _run(
        _execute_config(web_event_id_suffix=None, allow_web_fetch=False, allow_artifact_snapshot_write=False),
        builder,
    )

    assert result.status == "failed"
    assert result.reason_code == "analysis_requested_outbox_missing"


def test_no_raw_url_article_source_env_db_redis_exception_header_body_in_success_or_failure_json() -> None:
    success = _run(_execute_config(), FakeDatabaseBuilder()).to_sanitized_dict()
    failure = _run(
        _execute_config(text_idea_event_id_suffix=None, allow_text_idea_snapshot_write=False),
        FakeDatabaseBuilder(web_error=runner.ProofRunnerError(RAW_EXCEPTION)),
    ).to_sanitized_dict()

    text = json.dumps({"success": success, "failure": failure}, sort_keys=True)

    assert RAW_URL not in text
    assert RAW_FINAL_URL not in text
    assert RAW_ARTICLE_TEXT not in text
    assert RAW_SOURCE_TEXT not in text
    assert RAW_EXCEPTION not in text
    assert RAW_HEADER not in text
    assert RAW_BODY not in text
    assert DB_URL not in text
    assert REDIS_URL not in text
    assert "database-url-sentinel" not in text
    assert "redis-url-sentinel" not in text
    assert success["raw_values_printed"] == {
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


def test_static_imports_and_calls_do_not_open_forbidden_surfaces() -> None:
    tree = ast.parse(TOOL_PATH.read_text(encoding="utf-8"))
    imported_modules: set[str] = set()
    called_names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                called_names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                called_names.add(node.func.attr)

    forbidden_import_fragments = {
        "gh_enricher",
        "x_enricher",
        "judge_openai",
        "notifier",
        "policy_engine",
        "collector",
        "subprocess",
        "redis",
    }
    assert not any(
        fragment in module for fragment in forbidden_import_fragments for module in imported_modules
    )
    for forbidden_call in {
        "systemctl",
        "docker",
        "alembic",
        "run_forever",
        "xreadgroup",
        "xack",
        "xgroup_create",
        "xclaim",
        "xautoclaim",
    }:
        assert forbidden_call not in called_names


def test_static_default_database_path_uses_existing_services_and_builders() -> None:
    tree = ast.parse(TOOL_PATH.read_text(encoding="utf-8"))
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}

    assert "WebEnricherService" in names
    assert "WebEnricherRepository" in names
    assert "WebFetchClient" in names
    assert "ArticleParser" in names
    assert "WebUrlDiscovery" in names
    assert "EvidenceAssemblerService" in names
    assert "EvidenceAssemblerRepository" in names
    assert "TextIdeaBuilder" in names
