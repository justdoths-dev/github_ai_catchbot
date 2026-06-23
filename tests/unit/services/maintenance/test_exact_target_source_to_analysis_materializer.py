from __future__ import annotations

import ast
import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from src.services.collector_telegram.operator_supplied_source import (
    OperatorSuppliedSourceAdapter,
    parse_operator_source_packet,
)
from src.services.evidence_assembler.config import EvidenceAssemblerConfig
from src.services.maintenance.exact_target_source_to_analysis_materializer import (
    ExactTargetSourceToAnalysisRequest,
    FinalReadback,
    NormalizationReadback,
    RefreshEventRecord,
    RuntimeConfigBundle,
    SqlStageComponents,
    run_exact_target_source_to_analysis_materializer,
)
from src.services.router_normalizer.config import RouterNormalizerConfig


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/maintenance/exact_target_source_to_analysis_materializer.py"
RAW_SECRET = "private exception body with raw packet text"


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


class Ledger:
    def __init__(self) -> None:
        self.registry_rows = [{"registry_id": str(uuid4()), "chat_id": 9001}]
        self.current: dict[tuple[str, int, int], dict[str, Any]] = {}
        self.versions: dict[str, list[dict[str, Any]]] = {}
        self.outbox: list[dict[str, Any]] = []
        self.normalization_runs: list[dict[str, Any]] = []
        self.candidate_group_id: UUID | None = None
        self.primary_artifact_id: UUID | None = None
        self.enrichment_requests = 0
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
        row.update({"raw_message_json": projection.raw_message_json})
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
            primary_artifact_type="text_idea" if self.ledger.primary_artifact_id else None,
            primary_artifact_id=self.ledger.primary_artifact_id,
            candidate_group_id=self.ledger.candidate_group_id,
            enrichment_requests=self.ledger.enrichment_requests,
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
            primary_text_idea_members=1 if self.ledger.primary_artifact_id else 0,
            external_enrichment_requests=self.ledger.enrichment_requests,
            text_idea_snapshots=self.ledger.text_idea_snapshot_count,
            ready_current_bundles=1 if self.ledger.bundle_id else 0,
            candidate_evidence_members=self.ledger.evidence_member_count,
            analysis_requested_events=1 if self.ledger.analysis_request_event_id else 0,
            judge_runs=self.ledger.judge_runs,
            judge_call_requested_events=self.ledger.judge_call_events,
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


class FakeAssemblerService:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    async def handle_trigger_event(self, trigger_event_id) -> None:
        self.ledger.call_order.append("assembler.handle_trigger_event")
        if self.ledger.assembler_failure:
            raise RuntimeError(RAW_SECRET)
        assert self.ledger.refresh_event_id == UUID(str(trigger_event_id))
        if not self.ledger.bundle_id:
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


def packet(text: str = "AI developer workflow automation for repository tests."):
    return parse_operator_source_packet(
        {
            "schema_version": "operator_supplied_telegram_source_v1",
            "source_ref": "https://t.me/SynthChannel/12345",
            "posted_at": "2026-06-23T01:02:03Z",
            "message_text": text,
        }
    )


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


async def run(ledger: Ledger, *, mode: str = "plan", text: str | None = None):
    return await run_exact_target_source_to_analysis_materializer(
        ExactTargetSourceToAnalysisRequest(mode=mode, packet=packet(text or "AI developer workflow automation for repository tests.")),
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
async def test_plan_blocks_external_url_before_writes() -> None:
    ledger = Ledger()

    report = await run(
        ledger,
        mode="plan",
        text="AI developer workflow automation https://example.com/project",
    )

    assert report.status == "blocked"
    assert report.reason_code == "external_enrichment_required"
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
        "redis",
        "redis.asyncio",
        "openai",
        "telegram",
        "src.services.judge_openai",
        "..judge_openai",
        "src.services.notifier_telegram",
        "..notifier_telegram",
    }
    assert imported_modules.isdisjoint(forbidden)
