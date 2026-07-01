from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from src.services.collector_telegram.models import SourceMessageProjection
from src.services.collector_telegram.operator_supplied_source import (
    OperatorSuppliedSourceAdapter,
    OperatorSuppliedSourceError,
    TelegramRegistryTarget,
    build_source_projection,
    parse_operator_source_packet,
)
from src.services.collector_telegram.repositories import CollectorRepository


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/collector_telegram/operator_supplied_source.py"
RAW_REF = "https://t.me/SynthChannel/12345"
RAW_TEXT = "AI developer workflow automation for repository tests."


class FakeCollectorRepository:
    def __init__(self) -> None:
        self.registry_rows: list[dict[str, Any]] = [
            {"registry_id": "reg-1", "chat_id": 9001},
        ]
        self.current: dict[tuple[str, int, int], dict[str, Any]] = {}
        self.versions: dict[str, list[dict[str, Any]]] = {}
        self.outbox: list[Any] = []
        self.raw_updates: list[Any] = []
        self.calls: list[str] = []

    async def find_public_username_registry_targets(self, normalized_source_value: str):
        self.calls.append(f"registry:{normalized_source_value}")
        return self.registry_rows

    async def get_source_message(self, *, platform: str, chat_id: int, message_id: int):
        self.calls.append("get_source_message")
        return self.current.get((platform, chat_id, message_id))

    async def get_latest_version(self, source_message_id: str):
        self.calls.append("get_latest_version")
        rows = self.versions.get(source_message_id, [])
        return rows[-1] if rows else None

    async def upsert_source_message(self, projection, *, platform: str = "telegram"):
        self.calls.append("upsert_source_message")
        key = (platform, projection.chat_id, projection.message_id)
        row = self.current.get(key)
        if row is None:
            row = {
                "source_message_id": f"src-{projection.chat_id}-{projection.message_id}",
                "current_version_no": 0,
            }
            self.current[key] = row
        row.update(
            {
                "logical_post_key": projection.logical_post_key,
                "raw_message_json": projection.raw_message_json,
                "forward_info_json": projection.forward_info_json,
                "text_surface": projection.text_surface,
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
        self.calls.append("append_source_message_version")
        row = {
            "version_no": len(self.versions.get(source_message_id, [])) + 1,
            "content_hash": projection.content_hash,
            "version_reason": version_reason,
        }
        self.versions.setdefault(source_message_id, []).append(row)
        for current in self.current.values():
            if current["source_message_id"] == source_message_id:
                current["current_version_no"] = row["version_no"]
        return row

    async def insert_outbox_event(self, event):
        self.calls.append("insert_outbox_event")
        self.outbox.append(event)
        return True

    async def get_outbox_event_by_dedupe_key(self, dedupe_key: str):
        self.calls.append("get_outbox_event_by_dedupe_key")
        for index, event in enumerate(self.outbox, start=1):
            if event.dedupe_key == dedupe_key:
                return {"event_id": f"00000000-0000-0000-0000-00000000000{index}"}
        return None


def packet(text: str = "AI developer workflow automation for repository tests."):
    return parse_operator_source_packet(
        {
            "schema_version": "operator_supplied_telegram_source_v1",
            "source_ref": RAW_REF,
            "posted_at": "2026-06-23T01:02:03Z",
            "message_text": text,
        }
    )


class RegistryOnlyResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> "RegistryOnlyResult":
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class RegistryOnlySession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, statement, params=None) -> RegistryOnlyResult:
        sql = str(statement)
        call_params = dict(params or {})
        self.calls.append((sql, call_params))
        return RegistryOnlyResult(
            [
                {
                    "registry_id": "11111111-1111-1111-1111-111111111111",
                    "chat_id": 9001,
                }
            ]
        )


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.split()).lower()


def test_real_collector_repository_exposes_operator_adapter_contract_methods() -> None:
    required_methods = {
        "get_source_message",
        "get_latest_version",
        "upsert_source_message",
        "append_source_message_version",
        "insert_outbox_event",
        "get_outbox_event_by_dedupe_key",
        "find_public_username_registry_targets",
    }

    for method_name in required_methods:
        assert callable(getattr(CollectorRepository, method_name, None))

    assert SourceMessageProjection.__name__ == "SourceMessageProjection"
    assert OperatorSuppliedSourceAdapter.__name__ == "OperatorSuppliedSourceAdapter"


@pytest.mark.asyncio
async def test_real_collector_registry_lookup_is_read_only_and_registry_scoped() -> None:
    session = RegistryOnlySession()
    repository = CollectorRepository(session)  # type: ignore[arg-type]

    rows = await repository.find_public_username_registry_targets("synthchannel")

    assert rows == [
        {
            "registry_id": "11111111-1111-1111-1111-111111111111",
            "chat_id": 9001,
        }
    ]
    assert len(session.calls) == 1
    sql, params = session.calls[0]
    normalized_sql = _normalize_sql(sql)
    assert "from telegram_channel_registry" in normalized_sql
    assert "source_messages" not in normalized_sql
    assert "event_outbox" not in normalized_sql
    assert all(keyword not in normalized_sql for keyword in (" insert ", " update ", " delete "))
    assert params == {"source_value": "synthchannel"}
    rendered_call = f"{sql} {params} {rows}"
    assert RAW_REF not in rendered_call
    assert RAW_TEXT not in rendered_call


@pytest.mark.asyncio
async def test_ingest_writes_current_version_and_outbox_without_raw_update() -> None:
    repository = FakeCollectorRepository()
    adapter = OperatorSuppliedSourceAdapter()
    target = await adapter.resolve_registry_target(repository, packet())

    result = await adapter.ingest_source(repository, packet=packet(), registry_target=target)

    assert result.source_message_created is True
    assert result.source_version_created is True
    assert result.source_outbox_created is True
    assert len(repository.current) == 1
    assert len(repository.versions[result.source_message_id]) == 1  # type: ignore[index]
    assert repository.versions[result.source_message_id][0]["version_reason"] == "operator_supplied_canary"  # type: ignore[index]
    assert len(repository.outbox) == 1
    assert repository.outbox[0].event_type == "source_message.created.v1"
    assert repository.raw_updates == []
    row = next(iter(repository.current.values()))
    assert row["raw_message_json"]["operator_supplied"] is True
    assert row["raw_message_json"]["live_telegram_read"] is False
    assert row["forward_info_json"]["operator_supplied"] is True
    assert row["forward_info_json"]["live_telegram_read"] is False


@pytest.mark.asyncio
async def test_same_semantic_packet_is_idempotent_before_second_write() -> None:
    repository = FakeCollectorRepository()
    adapter = OperatorSuppliedSourceAdapter()
    target = await adapter.resolve_registry_target(repository, packet())

    first = await adapter.ingest_source(repository, packet=packet(), registry_target=target)
    second = await adapter.ingest_source(repository, packet=packet(), registry_target=target)

    assert first.source_message_created is True
    assert second.duplicate is True
    assert second.reason_code == "source_packet_already_materialized"
    assert len(repository.current) == 1
    assert len(repository.versions[first.source_message_id]) == 1  # type: ignore[index]
    assert len(repository.outbox) == 1


@pytest.mark.asyncio
async def test_conflicting_content_for_same_telegram_identity_fails_closed() -> None:
    repository = FakeCollectorRepository()
    adapter = OperatorSuppliedSourceAdapter()
    original = packet("AI developer workflow automation for repository tests.")
    changed = packet("AI developer workflow automation for SDK tests with changed content.")
    target = await adapter.resolve_registry_target(repository, original)

    await adapter.ingest_source(repository, packet=original, registry_target=target)

    with pytest.raises(OperatorSuppliedSourceError) as exc:
        await adapter.ingest_source(repository, packet=changed, registry_target=target)

    assert exc.value.reason_code == "source_identity_content_conflict"
    assert len(repository.current) == 1
    assert len(repository.versions[next(iter(repository.versions))]) == 1
    assert len(repository.outbox) == 1


@pytest.mark.asyncio
async def test_registry_identity_requires_exact_non_null_match() -> None:
    repository = FakeCollectorRepository()
    adapter = OperatorSuppliedSourceAdapter()
    repository.registry_rows = []

    with pytest.raises(OperatorSuppliedSourceError) as missing:
        await adapter.resolve_registry_target(repository, packet())

    assert missing.value.reason_code == "telegram_channel_registry_target_missing"

    repository.registry_rows = [
        {"registry_id": "reg-1", "chat_id": 9001},
        {"registry_id": "reg-2", "chat_id": 9002},
    ]
    with pytest.raises(OperatorSuppliedSourceError) as ambiguous:
        await adapter.resolve_registry_target(repository, packet())

    assert ambiguous.value.reason_code == "telegram_channel_registry_target_ambiguous"

    repository.registry_rows = [
        {"registry_id": "reg-1", "chat_id": None},
    ]
    with pytest.raises(OperatorSuppliedSourceError) as null_chat_id:
        await adapter.resolve_registry_target(repository, packet())

    assert null_chat_id.value.reason_code == "telegram_channel_registry_target_missing"


def test_projection_uses_operator_provenance_and_no_raw_source_ref_storage() -> None:
    item = packet()
    projection = build_source_projection(
        packet=item,
        registry_target=TelegramRegistryTarget(registry_id="reg-1", chat_id=9001),
    )

    assert projection.posted_at == datetime(2026, 6, 23, 1, 2, 3, tzinfo=timezone.utc)
    assert projection.message_link is None
    assert projection.text_surface == item.message_text
    rendered = str(projection.raw_message_json)
    assert item.source_ref not in rendered
    assert "SynthChannel" not in rendered
    assert "12345" not in rendered
    assert projection.raw_message_json["source_ref_fingerprint"] == item.source_ref_fingerprint


def test_collector_adapter_does_not_import_adjacent_candidate_llm_or_notification_boundaries() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    forbidden = {
        "src.services.router_normalizer",
        "..router_normalizer",
        "src.services.evidence_assembler",
        "..evidence_assembler",
        "src.services.judge_openai",
        "..judge_openai",
        "src.services.notifier_telegram",
        "..notifier_telegram",
    }
    assert imported_modules.isdisjoint(forbidden)
