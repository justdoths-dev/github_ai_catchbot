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
from src.services.x_enricher.config import XEnricherConfig
from src.services.x_enricher.models import ArtifactEnrichmentJob, EnrichmentResult
from src.services.x_enricher.x_api_client import XRateLimitedError
from tools import bounded_x_provider_evidence_bundle_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_x_provider_evidence_bundle_runner.py"
DB_URL = "database-url-sentinel-redacted"
REDIS_URL = "redis-url-sentinel-redacted"
X_BEARER_TOKEN = "x-bearer-token-sentinel-redacted"
RAW_URL = "https://private.example.invalid/raw-url-sentinel"
RAW_POST_TEXT = "post-text-sentinel-private-body"
RAW_SOURCE_TEXT = "source-text-sentinel-private-body"
RAW_EXCEPTION = "exception-detail-sentinel-redacted"
RAW_X_RESPONSE = "x-response-body-sentinel-redacted"
_MISSING = object()


EVENT_ID = UUID("11111111-1111-4111-8111-1111feedbeef")
ARTIFACT_ID = UUID("22222222-2222-4222-8222-2222cafed00d")
CANDIDATE_GROUP_ID = UUID("33333333-3333-4333-8333-3333deadbeef")
SNAPSHOT_ID = UUID("44444444-4444-4444-8444-4444aaaabbbb")
SNAPSHOT_EVENT_ID = UUID("55555555-5555-4555-8555-5555aaaabbbb")
BUNDLE_ID = UUID("66666666-6666-4666-8666-6666aaaabbbb")
ANALYSIS_EVENT_ID = UUID("77777777-7777-4777-8777-7777aaaabbbb")


class RuntimeLoader:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> runner.RuntimeConfig:
        self.calls += 1
        return runner.RuntimeConfig(x_config=_x_config(), assembler_config=_assembler_config())


class RaisingRuntimeLoader:
    def __call__(self) -> runner.RuntimeConfig:
        raise AssertionError("runtime_config_loader_should_not_be_called")


class FakeDatabase:
    def __init__(
        self,
        *,
        state: runner.RunnerState,
        rows: list[runner.TargetEventRow] | None = None,
        job: ArtifactEnrichmentJob | None = None,
        resolution: runner.TargetResolutionReadback | None = None,
        enrichment_result: EnrichmentResult | None = None,
        enrichment_run: runner.EnrichmentRunReadback | None = None,
        snapshot: runner.SnapshotReadback | None = None,
        snapshot_outbox: runner.OutboxReadback | None = None,
        assembly_results: list[AssemblyResult] | None = None,
        bundle: runner.BundleReadback | None = None,
        analysis_outbox: runner.OutboxReadback | None | object = _MISSING,
        x_error: BaseException | None = None,
    ) -> None:
        self.state = state
        self.rows = rows if rows is not None else [_target_row()]
        self.job = job if job is not None else _job()
        self.resolution = resolution or runner.TargetResolutionReadback(1, 1, 1, True)
        self.enrichment_result = enrichment_result or EnrichmentResult(
            artifact_id=ARTIFACT_ID,
            snapshot_id=SNAPSHOT_ID,
            status="ready",
            content_anchor="xpost:1881234567890123456:1881234567890123456",
            emitted_snapshot_updated=True,
        )
        self.enrichment_run = enrichment_run
        self.snapshot = snapshot if snapshot is not None else _snapshot()
        self.snapshot_outbox = snapshot_outbox if snapshot_outbox is not None else runner.OutboxReadback(
            event_id=SNAPSHOT_EVENT_ID,
            event_type="artifact.snapshot.updated.v1",
            status="pending",
        )
        self.assembly_results = assembly_results if assembly_results is not None else [
            AssemblyResult(
                candidate_group_id=CANDIDATE_GROUP_ID,
                bundle_id=BUNDLE_ID,
                reused_existing_bundle=False,
                ready_for_analysis=True,
                emitted_analysis_requested=True,
                analysis_requested_event_id=ANALYSIS_EVENT_ID,
            )
        ]
        self.bundle = bundle if bundle is not None else runner.BundleReadback(
            bundle_id=BUNDLE_ID,
            candidate_group_id=CANDIDATE_GROUP_ID,
            ready_for_analysis=True,
            current_bundle_consistent=True,
        )
        if analysis_outbox is _MISSING:
            self.analysis_outbox = runner.OutboxReadback(
                event_id=ANALYSIS_EVENT_ID,
                event_type="analysis.requested.v1",
                status="pending",
            )
        else:
            self.analysis_outbox = analysis_outbox
        self.x_error = x_error
        self.closed = False
        self.calls: list[str] = []

    async def select_events_by_suffix(self, suffix: str) -> list[runner.TargetEventRow]:
        self.calls.append(f"select:{suffix}")
        self.state.database_read_attempted = True
        return self.rows

    async def load_job_by_trigger_event_id(self, event_id: UUID) -> ArtifactEnrichmentJob | None:
        self.calls.append(f"load_job:{event_id.hex[-8:]}")
        self.state.database_read_attempted = True
        return self.job

    async def verify_target_resolution(self, job: ArtifactEnrichmentJob) -> runner.TargetResolutionReadback:
        del job
        self.calls.append("verify_resolution")
        self.state.database_read_attempted = True
        return self.resolution

    async def run_x_enricher(self, job: ArtifactEnrichmentJob) -> EnrichmentResult:
        del job
        self.calls.append("run_x_enricher")
        self.state.database_write_attempted = True
        self.state.snapshot_write_attempted = True
        if self.x_error is not None:
            raise self.x_error
        self.state.x_live_read_attempted = True
        self.state.x_request_count = 1
        if self.enrichment_result.status == "rate_limited":
            self.state.x_rate_limit_status_bucket = "rate_limited"
        else:
            self.state.x_rate_limit_status_bucket = "not_rate_limited"
        return self.enrichment_result

    async def read_latest_enrichment_run(
        self,
        *,
        artifact_id: UUID,
        status: str,
        content_anchor: str | None,
    ) -> runner.EnrichmentRunReadback | None:
        self.calls.append("read_latest_enrichment_run")
        self.state.database_read_attempted = True
        if self.enrichment_run is not None:
            return self.enrichment_run
        return runner.EnrichmentRunReadback(
            artifact_id=artifact_id,
            provider="x",
            status=status,
            content_anchor=content_anchor,
        )

    async def read_snapshot(self, snapshot_id: UUID) -> runner.SnapshotReadback | None:
        del snapshot_id
        self.calls.append("read_snapshot")
        self.state.database_read_attempted = True
        return self.snapshot

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

    async def run_evidence_assembler(self, snapshot_updated_event_id: UUID) -> list[AssemblyResult]:
        del snapshot_updated_event_id
        self.calls.append("run_evidence_assembler")
        self.state.database_write_attempted = True
        self.state.evidence_bundle_write_attempted = True
        return self.assembly_results

    async def read_bundle(self, *, candidate_group_id: UUID, bundle_id: UUID) -> runner.BundleReadback | None:
        del candidate_group_id, bundle_id
        self.calls.append("read_bundle")
        self.state.database_read_attempted = True
        return self.bundle

    async def read_analysis_requested_outbox(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
    ) -> runner.OutboxReadback | None:
        del candidate_group_id, bundle_id
        self.calls.append("read_analysis_requested_outbox")
        self.state.database_read_attempted = True
        return self.analysis_outbox

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


class FakeLiveXClient:
    def __init__(self, *, error: BaseException | None = None) -> None:
        self.error = error
        self.calls: list[list[str]] = []

    def default_request_profile(self):
        return object()

    async def get_posts_by_ids(self, *, post_ids, profile):
        del profile
        self.calls.append(post_ids)
        if self.error is not None:
            raise self.error
        return {"data": [{"id": post_ids[0], "text": RAW_POST_TEXT}]}

    async def close(self) -> None:
        return None


def _run(config: runner.RunnerConfig, builder: FakeDatabaseBuilder, loader=None) -> runner.RunnerResult:
    return runner.run_bounded_x_provider_evidence_bundle_sync(
        config,
        runtime_config_loader=loader or RuntimeLoader(),
        database_builder=builder,
    )


def _plan_config(**overrides) -> runner.RunnerConfig:
    config = runner.RunnerConfig(
        mode="plan",
        event_id_suffix=EVENT_ID.hex[-8:],
        operator_approved=True,
        allow_runtime_config=True,
        allow_database_read=True,
    )
    return replace(config, **overrides)


def _execute_config(**overrides) -> runner.RunnerConfig:
    config = runner.RunnerConfig(
        mode="execute",
        event_id_suffix=EVENT_ID.hex[-8:],
        operator_approved=True,
        allow_runtime_config=True,
        allow_database_read=True,
        confirm_token=runner.CONFIRM_TOKEN,
        allow_x_live_read=True,
        allow_database_write=True,
        allow_artifact_snapshot_write=True,
        allow_evidence_bundle_write=True,
    )
    return replace(config, **overrides)


def _target_row(**overrides) -> runner.TargetEventRow:
    payload = {
        "candidate_group_id": str(CANDIDATE_GROUP_ID),
        "artifact_id": str(ARTIFACT_ID),
        "artifact_type": "x_post",
        "provider_route": "x",
        "refresh_mode": "standard",
        "depth_budget": 1,
        "canonical_url": RAW_URL,
        "source_text": RAW_SOURCE_TEXT,
    }
    payload.update(overrides.pop("payload_json", {}))
    row = runner.TargetEventRow(
        event_id=EVENT_ID,
        event_type="artifact.enrich.requested.v1",
        status="published",
        aggregate_type="artifact",
        aggregate_id=ARTIFACT_ID,
        payload_json=payload,
    )
    return replace(row, **overrides)


def _job(**overrides) -> ArtifactEnrichmentJob:
    job = ArtifactEnrichmentJob(
        trigger_event_id=EVENT_ID,
        event_type="artifact.enrich.requested.v1",
        candidate_group_id=CANDIDATE_GROUP_ID,
        artifact_id=ARTIFACT_ID,
        artifact_type="x_post",
        provider_route="x",
        refresh_mode="standard",
        depth_budget=1,
        requested_at=datetime.now(timezone.utc),
    )
    return replace(job, **overrides)


def _snapshot(**overrides) -> runner.SnapshotReadback:
    snapshot = runner.SnapshotReadback(
        snapshot_id=SNAPSHOT_ID,
        artifact_id=ARTIFACT_ID,
        provider="x",
        snapshot_type="x_post",
        status="ready",
        content_anchor="xpost:1881234567890123456:1881234567890123456",
        normalized_projection_present=True,
        x_post_child_count=1,
        discovered_url_count=1,
        discovered_url_fingerprints=(runner._fingerprint_text(RAW_URL),),
    )
    return replace(snapshot, **overrides)


def _x_config() -> XEnricherConfig:
    return XEnricherConfig(
        app_env="test",
        database_url=DB_URL,
        redis_url=REDIS_URL,
        queue_name="q.artifact.enrich.x",
        consumer_group="x-enricher",
        consumer_name="bounded-proof-test",
        batch_size=1,
        block_ms=100,
        x_api_base_url="https://api.x.com",
        x_bearer_token=X_BEARER_TOKEN,
        request_timeout_sec=1,
        request_max_ids=1,
        depth_budget_default=1,
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


def test_plan_mode_exact_target_readback_has_no_x_call_and_no_db_write() -> None:
    builder = FakeDatabaseBuilder()

    result = _run(_plan_config(), builder)
    report = result.to_sanitized_dict()

    assert result.status == "pass"
    assert result.reason_code == "plan_exact_target_ready"
    assert report["x_live_read_attempted"] is False
    assert report["db_write_attempted"] is False
    assert report["snapshot_written"] is False
    assert report["authority"]["redis_consume_or_ack"] is False
    assert builder.database is not None
    assert "run_x_enricher" not in builder.database.calls
    assert "run_evidence_assembler" not in builder.database.calls


@pytest.mark.parametrize("suffix", [None, "feedbee"])
def test_missing_or_short_event_suffix_blocks_before_runtime_config(suffix: str | None) -> None:
    result = runner.run_bounded_x_provider_evidence_bundle_sync(
        _plan_config(event_id_suffix=suffix),
        runtime_config_loader=RaisingRuntimeLoader(),
        database_builder=FakeDatabaseBuilder(),
    )

    assert result.status == "blocked"
    assert result.reason_code == "invalid_or_missing_event_id_suffix"
    assert result.state.runtime_config_loaded is False
    assert result.state.database_read_attempted is False


def test_execute_requires_confirm_token_before_runtime_config() -> None:
    result = runner.run_bounded_x_provider_evidence_bundle_sync(
        _execute_config(confirm_token="wrong-token"),
        runtime_config_loader=RaisingRuntimeLoader(),
        database_builder=FakeDatabaseBuilder(),
    )

    assert result.status == "blocked"
    assert result.reason_code == "confirm_token_missing_or_invalid"
    assert result.state.runtime_config_loaded is False
    assert result.state.database_read_attempted is False


def test_ambiguous_event_suffix_blocks_before_live_authority() -> None:
    builder = FakeDatabaseBuilder(rows=[_target_row(), _target_row(event_id=uuid4())])

    result = _run(_execute_config(), builder)

    assert result.status == "blocked"
    assert result.reason_code == "target_event_suffix_ambiguous"
    assert result.state.x_live_read_attempted is False
    assert result.state.database_write_attempted is False
    assert builder.database is not None
    assert "run_x_enricher" not in builder.database.calls


@pytest.mark.parametrize(
    ("row", "reason_code"),
    [
        (
            _target_row(event_type="artifact.snapshot.updated.v1"),
            "target_event_type_not_artifact_enrich_requested",
        ),
        (_target_row(status="pending"), "target_event_status_not_published"),
        (
            _target_row(payload_json={"provider_route": "github"}),
            "target_event_provider_route_not_x",
        ),
        (
            _target_row(payload_json={"artifact_type": "web_article"}),
            "target_event_artifact_type_not_x_post",
        ),
    ],
)
def test_wrong_target_contract_blocks_before_live_authority(row, reason_code) -> None:
    builder = FakeDatabaseBuilder(rows=[row])

    result = _run(_execute_config(), builder)

    assert result.status == "blocked"
    assert result.reason_code == reason_code
    assert result.state.x_live_read_attempted is False
    assert result.state.database_write_attempted is False
    assert builder.database is not None
    assert "run_x_enricher" not in builder.database.calls


def test_non_extractable_post_id_blocks_before_live_authority() -> None:
    builder = FakeDatabaseBuilder(resolution=runner.TargetResolutionReadback(1, 1, 1, False))

    result = _run(_execute_config(), builder)

    assert result.status == "blocked"
    assert result.reason_code == "target_artifact_post_id_not_extractable"
    assert result.state.x_live_read_attempted is False
    assert result.state.database_write_attempted is False


def test_execute_uses_x_enricher_path_and_reads_successful_x_snapshot() -> None:
    builder = FakeDatabaseBuilder()

    result = _run(_execute_config(), builder)
    report = result.to_sanitized_dict()

    assert result.status == "pass"
    assert result.reason_code == "x_provider_evidence_bundle_proof_complete"
    assert report["x_live_read_attempted"] is True
    assert report["x_request_count_bucket"] == "one"
    assert report["x_rate_limit_status_bucket"] == "not_rate_limited"
    assert report["snapshot_written"] is True
    assert report["snapshot_status"] == "ready"
    assert report["x_post_child_readback"] == {
        "expected": True,
        "present": True,
        "projection_present": True,
    }
    assert report["discovered_url_count_bucket"] == "one"
    assert report["discovered_url_fingerprints"] == [runner._fingerprint_text(RAW_URL)]
    assert report["snapshot_updated_outbox_fingerprint"] is not None
    assert builder.database is not None
    assert "run_x_enricher" in builder.database.calls
    assert "read_latest_enrichment_run" in builder.database.calls


def test_tracked_x_client_caps_request_count_and_exact_id_count() -> None:
    state = runner.RunnerState()
    client = runner.TrackedXApiClient(FakeLiveXClient(), state)  # type: ignore[arg-type]

    asyncio.run(client.get_posts_by_ids(post_ids=["1881234567890123456"], profile=object()))  # type: ignore[arg-type]

    assert state.x_live_read_attempted is True
    assert state.x_request_count == 1
    assert state.x_rate_limit_status_bucket == "not_rate_limited"
    with pytest.raises(runner.ProofRunnerError) as too_many_ids:
        asyncio.run(client.get_posts_by_ids(post_ids=["1", "2"], profile=object()))  # type: ignore[arg-type]
    assert too_many_ids.value.reason_code == "x_request_ids_not_exactly_one"
    with pytest.raises(runner.ProofRunnerError) as too_many_calls:
        asyncio.run(client.get_posts_by_ids(post_ids=["1881234567890123457"], profile=object()))  # type: ignore[arg-type]
    assert too_many_calls.value.reason_code == "x_request_cap_exceeded"


def test_tracked_x_client_reports_rate_limit_bucket_without_body_leakage() -> None:
    state = runner.RunnerState()
    client = runner.TrackedXApiClient(FakeLiveXClient(error=XRateLimitedError(RAW_X_RESPONSE)), state)  # type: ignore[arg-type]

    with pytest.raises(XRateLimitedError):
        asyncio.run(client.get_posts_by_ids(post_ids=["1881234567890123456"], profile=object()))  # type: ignore[arg-type]

    report = runner.RunnerResult(
        status="blocked",
        reason_code="provider_status_rate_limited",
        config=_execute_config(),
        state=state,
    ).to_sanitized_dict()
    text = json.dumps(report, sort_keys=True)
    assert state.x_rate_limit_status_bucket == "rate_limited"
    assert RAW_X_RESPONSE not in text
    assert report["raw_values_printed"]["x_response_body"] is False


def test_evidence_assembler_consumption_creates_bundle_and_analysis_outbox_readback() -> None:
    builder = FakeDatabaseBuilder()

    result = _run(_execute_config(), builder)
    report = result.to_sanitized_dict()

    assert result.status == "pass"
    assert report["evidence_bundle_written_or_reused"] is True
    assert report["bundle_fingerprint"] is not None
    assert report["ready_for_analysis"] is True
    assert report["analysis_requested_outbox_fingerprint"] is not None
    assert report["reused_existing_bundle"] is False
    assert builder.database is not None
    assert "run_evidence_assembler" in builder.database.calls
    assert "read_bundle" in builder.database.calls


def test_low_evidence_snapshot_can_materialize_bundle_without_analysis_outbox() -> None:
    builder = FakeDatabaseBuilder(
        enrichment_result=EnrichmentResult(
            artifact_id=ARTIFACT_ID,
            snapshot_id=SNAPSHOT_ID,
            status="low_evidence",
            content_anchor="xpost:1881234567890123456:1881234567890123456",
            emitted_snapshot_updated=True,
        ),
        snapshot=_snapshot(status="low_evidence"),
        assembly_results=[
            AssemblyResult(
                candidate_group_id=CANDIDATE_GROUP_ID,
                bundle_id=BUNDLE_ID,
                reused_existing_bundle=False,
                ready_for_analysis=False,
                emitted_analysis_requested=False,
                analysis_requested_event_id=None,
            )
        ],
        bundle=runner.BundleReadback(
            bundle_id=BUNDLE_ID,
            candidate_group_id=CANDIDATE_GROUP_ID,
            ready_for_analysis=False,
            current_bundle_consistent=True,
        ),
        analysis_outbox=None,
    )

    result = _run(_execute_config(), builder)
    report = result.to_sanitized_dict()

    assert result.status == "pass"
    assert report["snapshot_status"] == "low_evidence"
    assert report["ready_for_analysis"] is False
    assert report["analysis_requested_outbox_fingerprint"] is None
    assert builder.database is not None
    assert "run_evidence_assembler" in builder.database.calls


def test_evidence_assembler_existing_bundle_reuse_is_reported() -> None:
    builder = FakeDatabaseBuilder(
        assembly_results=[
            AssemblyResult(
                candidate_group_id=CANDIDATE_GROUP_ID,
                bundle_id=BUNDLE_ID,
                reused_existing_bundle=True,
                ready_for_analysis=True,
                emitted_analysis_requested=False,
                analysis_requested_event_id=ANALYSIS_EVENT_ID,
            )
        ],
    )

    result = _run(_execute_config(), builder)
    report = result.to_sanitized_dict()

    assert result.status == "pass"
    assert report["evidence_bundle_written_or_reused"] is True
    assert report["reused_existing_bundle"] is True
    assert report["analysis_requested_outbox_fingerprint"] is not None


@pytest.mark.parametrize(
    ("status", "rate_bucket"),
    [
        ("rate_limited", "rate_limited"),
        ("access_denied", "not_rate_limited"),
        ("failed_permanent", "not_rate_limited"),
        ("failed_transient", "not_rate_limited"),
    ],
)
def test_provider_failure_statuses_are_sanitized_and_not_false_pass(status: str, rate_bucket: str) -> None:
    builder = FakeDatabaseBuilder(
        enrichment_result=EnrichmentResult(
            artifact_id=ARTIFACT_ID,
            snapshot_id=None,
            status=status,
            content_anchor=None,
            emitted_snapshot_updated=False,
        )
    )

    result = _run(_execute_config(), builder)
    report = result.to_sanitized_dict()
    report_text = json.dumps(report, sort_keys=True)

    assert result.status == "blocked"
    assert result.reason_code == f"provider_status_{status}"
    assert report["x_rate_limit_status_bucket"] == rate_bucket
    assert '"status": "pass"' not in report_text
    assert RAW_URL not in report_text
    assert RAW_POST_TEXT not in report_text
    assert RAW_SOURCE_TEXT not in report_text
    assert RAW_EXCEPTION not in report_text
    assert RAW_X_RESPONSE not in report_text
    assert DB_URL not in report_text
    assert REDIS_URL not in report_text
    assert X_BEARER_TOKEN not in report_text
    assert report["raw_values_printed"]["exception_bodies"] is False
    assert report["raw_values_printed"]["traceback"] is False


def test_transient_x_exception_is_sanitized_and_not_false_pass() -> None:
    builder = FakeDatabaseBuilder(x_error=runner.XApiClientError(RAW_EXCEPTION))

    result = _run(_execute_config(), builder)
    report = result.to_sanitized_dict()
    text = json.dumps(report, sort_keys=True)

    assert result.status == "failed"
    assert result.reason_code == "x_client_error"
    assert RAW_EXCEPTION not in text
    assert report["raw_values_printed"]["exception_bodies"] is False
    assert report["raw_values_printed"]["traceback"] is False


def test_snapshot_child_missing_fails_readback() -> None:
    builder = FakeDatabaseBuilder(snapshot=_snapshot(x_post_child_count=0))

    result = _run(_execute_config(), builder)

    assert result.status == "failed"
    assert result.reason_code == "x_post_child_readback_missing"


def test_analysis_requested_outbox_missing_fails_only_when_ready_for_analysis() -> None:
    builder = FakeDatabaseBuilder(analysis_outbox=None)

    result = _run(_execute_config(), builder)

    assert result.status == "failed"
    assert result.reason_code == "analysis_requested_outbox_missing"


def test_no_raw_bearer_token_url_post_text_source_text_db_redis_exception_body_in_success_report() -> None:
    builder = FakeDatabaseBuilder()

    result = _run(_execute_config(), builder)
    report = result.to_sanitized_dict()
    text = json.dumps(report, sort_keys=True)

    assert "database-url-sentinel" not in text
    assert "redis-url-sentinel" not in text
    assert "x-bearer-token-sentinel" not in text
    assert "exception-detail-sentinel" not in text
    assert RAW_URL not in text
    assert RAW_POST_TEXT not in text
    assert RAW_SOURCE_TEXT not in text
    assert RAW_X_RESPONSE not in text
    assert DB_URL not in text
    assert REDIS_URL not in text
    assert X_BEARER_TOKEN not in text
    assert report["raw_values_printed"] == {
        "raw_ids": False,
        "raw_urls": False,
        "raw_post_text": False,
        "raw_source_text": False,
        "secrets": False,
        "env_values": False,
        "database_url": False,
        "redis_url": False,
        "x_bearer_token": False,
        "exception_bodies": False,
        "stderr": False,
        "traceback": False,
        "x_response_body": False,
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
        "bounded_github_provider",
        "web_enricher",
        "judge_openai",
        "notifier",
        "collector",
        "subprocess",
    }
    assert not any(
        fragment in module for fragment in forbidden_import_fragments for module in imported_modules
    )
    assert "subprocess" not in imported_modules
    assert "systemctl" not in called_names
    assert "docker" not in called_names
    assert "alembic" not in called_names
    assert "run_forever" not in called_names
    assert "xreadgroup" not in called_names
    assert "xack" not in called_names
    assert "xgroup_create" not in called_names
    assert "xclaim" not in called_names
    assert "xautoclaim" not in called_names


def test_default_database_path_uses_existing_services_not_duplicate_fetcher_or_assembler() -> None:
    tree = ast.parse(TOOL_PATH.read_text(encoding="utf-8"))
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}

    assert "XEnricherService" in names
    assert "XEnricherRepository" in names
    assert "XApiClient" in names
    assert "XResponseMapper" in names
    assert "XUrlDiscovery" in names
    assert "EvidenceAssemblerService" in names
    assert "EvidenceAssemblerRepository" in names
    assert "extract_post_id_from_canonical_id" in names
