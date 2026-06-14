from __future__ import annotations

import ast
import json
from copy import deepcopy
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import pytest

from src.services.collector_telegram.bounded_history_ingest_runner import (
    BoundedTelegramCollectorHistoryIngestConfig,
    BoundedTelegramCollectorHistoryIngestRuntimeHandle,
    run_bounded_telegram_collector_history_ingest,
)
from src.services.collector_telegram.config import CollectorTelegramConfig
from src.services.collector_telegram.models import CollectorEnvironment, CollectorMode, TrackedChat


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/collector_telegram/bounded_history_ingest_runner.py"
TOOL_PATH = ROOT / "tools/bounded_telegram_collector_history_ingest_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_db_password@127.0.0.1/db"
RAW_CHAT_ID = 9876543210123
RAW_MESSAGE_ID = 444555666
RAW_MESSAGE_TEXT = "sentinel bounded history ingest raw message text"
RAW_SECRET = "sentinel_telegram_api_hash_history_ingest"
EXCEPTION_DETAIL = "private exception detail with sentinel bounded history ingest raw message text"


class FakeTransaction:
    def __init__(self, repository: "FakeRepository") -> None:
        self.repository = repository
        self.snapshot: Any = None
        self.committed = False
        self.rolled_back = False

    async def __aenter__(self) -> "FakeRepository":
        self.snapshot = self.repository.snapshot()
        self.repository.transactions.append(self)
        return self.repository

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is not None:
            self.repository.restore(self.snapshot)
            self.rolled_back = True
            return None
        self.committed = True
        return None


class FakeRepository:
    def __init__(
        self,
        *,
        tracked: TrackedChat | None = None,
        fail_upsert: Exception | None = None,
    ) -> None:
        self.tracked = tracked
        self.fail_upsert = fail_upsert
        self.messages: dict[tuple[int, int], dict[str, Any]] = {}
        self.versions: dict[str, list[dict[str, Any]]] = {}
        self.outbox: list[Any] = []
        self.dedupe_keys: set[str] = set()
        self.transactions: list[FakeTransaction] = []
        self.registry_lookups: list[str] = []
        self.upsert_calls = 0

    def snapshot(self) -> Any:
        return (
            deepcopy(self.messages),
            deepcopy(self.versions),
            list(self.outbox),
            set(self.dedupe_keys),
            self.upsert_calls,
        )

    def restore(self, snapshot: Any) -> None:
        self.messages, self.versions, self.outbox, self.dedupe_keys, self.upsert_calls = snapshot

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)

    async def get_active_joined_tracked_chat_by_registry_id(self, registry_id: str) -> TrackedChat | None:
        self.registry_lookups.append(registry_id)
        if self.tracked is None:
            return None
        if self.tracked.registry_id != registry_id:
            return None
        if self.tracked.desired_state != "active" or self.tracked.access_state != "joined":
            return None
        return self.tracked

    async def get_source_message(self, *, platform: str, chat_id: int, message_id: int) -> dict[str, Any] | None:
        assert platform == "telegram"
        return self.messages.get((chat_id, message_id))

    async def get_latest_version(self, source_message_id: str) -> dict[str, Any] | None:
        versions = self.versions.get(source_message_id, [])
        return versions[-1] if versions else None

    async def upsert_source_message(self, projection: Any, *, platform: str = "telegram") -> dict[str, Any]:
        if self.fail_upsert is not None:
            raise self.fail_upsert
        assert platform == "telegram"
        self.upsert_calls += 1
        key = (projection.chat_id, projection.message_id)
        row = self.messages.get(key)
        if row is None:
            source_message_id = str(uuid5(NAMESPACE_URL, f"telegram:{projection.chat_id}:{projection.message_id}"))
            row = {
                "source_message_id": source_message_id,
                "chat_id": projection.chat_id,
                "message_id": projection.message_id,
                "logical_post_key": projection.logical_post_key,
                "current_version_no": 0,
            }
            self.messages[key] = row
            self.versions[source_message_id] = []
        row["logical_post_key"] = projection.logical_post_key
        return row

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: Any,
        version_reason: str,
        observed_at: Any = None,
        telegram_edit_date: Any = None,
    ) -> tuple[bool, dict[str, Any] | None]:
        del observed_at, telegram_edit_date
        versions = self.versions.setdefault(source_message_id, [])
        if versions and versions[-1]["content_hash"] == projection.content_hash:
            return False, None
        row = {
            "source_message_id": source_message_id,
            "version_no": len(versions) + 1,
            "version_reason": version_reason,
            "content_hash": projection.content_hash,
        }
        versions.append(row)
        return True, row

    async def insert_outbox_event(self, event: Any) -> bool:
        if event.dedupe_key in self.dedupe_keys:
            return False
        self.dedupe_keys.add(event.dedupe_key)
        self.outbox.append(event)
        return True


class FakeHistoryClient:
    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self.messages = messages
        self.calls: list[dict[str, int]] = []
        self.closed = False

    async def fetch_newest_history_messages(self, *, chat_id: int, limit: int):
        self.calls.append({"chat_id": chat_id, "limit": limit})
        return tuple(deepcopy(self.messages))

    async def close(self) -> None:
        self.closed = True


class FakeRuntimeBuilder:
    def __init__(self, repository: FakeRepository, history_client: FakeHistoryClient) -> None:
        self.repository = repository
        self.history_client = history_client
        self.calls = 0
        self.close_commits: list[bool] = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, state, logger
        self.calls += 1

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            await self.history_client.close()

        return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
            repository=self.repository,
            history_client=self.history_client,
            close=close,
        )


class Loader:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> CollectorTelegramConfig:
        self.calls += 1
        return _runtime_config()


def _runtime_config() -> CollectorTelegramConfig:
    return CollectorTelegramConfig(
        app_env=CollectorEnvironment.TEST,
        database_url=DB_URL,
        redis_url=None,
        collector_mode=CollectorMode.REPLAY,
        telegram_api_id=12345,
        telegram_api_hash=RAW_SECRET,
        telegram_phone_number="+15555550123",
        telegram_2fa_password=None,
        tdlib_state_dir="/tmp/sentinel-history-ingest-tdlib-state",
        tdlib_files_dir="/tmp/sentinel-history-ingest-tdlib-files",
        tdlib_db_encryption_key="sentinel-tdlib-encryption-key",
        reconcile_interval_sec=300,
        reconcile_backfill_limit=3,
        warm_backfill_limit=1,
        history_page_limit=3,
    )


def _message(*, text: str = RAW_MESSAGE_TEXT, message_id: int = RAW_MESSAGE_ID) -> dict[str, Any]:
    return {
        "@type": "message",
        "chat_id": RAW_CHAT_ID,
        "id": message_id,
        "date": 1713550000,
        "is_channel_post": True,
        "content": {
            "@type": "messageText",
            "text": {"text": text, "entities": []},
        },
        "raw_nested_secret": "sentinel raw message json payload value",
    }


def _approved_config(**overrides: Any) -> BoundedTelegramCollectorHistoryIngestConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_telegram_read": True,
        "allow_database_write": True,
        "allow_outbox_write": True,
        "max_messages": 1,
        "chat_id": RAW_CHAT_ID,
    }
    values.update(overrides)
    return BoundedTelegramCollectorHistoryIngestConfig(**values)


async def _run(
    config: BoundedTelegramCollectorHistoryIngestConfig,
    *,
    repository: FakeRepository | None = None,
    history: FakeHistoryClient | None = None,
    loader: Loader | None = None,
):
    fake_repository = repository or FakeRepository()
    fake_history = history or FakeHistoryClient([_message()])
    fake_loader = loader or Loader()
    builder = FakeRuntimeBuilder(fake_repository, fake_history)
    result = await run_bounded_telegram_collector_history_ingest(
        config,
        runtime_config_loader=fake_loader,
        runtime_builder=builder,
    )
    return result, fake_loader, builder, fake_repository, fake_history


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_runtime_config_or_authority() -> None:
    def forbidden_loader() -> CollectorTelegramConfig:
        raise AssertionError("runtime config must not be loaded")

    result = await run_bounded_telegram_collector_history_ingest(
        BoundedTelegramCollectorHistoryIngestConfig(),
        runtime_config_loader=forbidden_loader,
    )
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == "operator_approval_missing"
    assert report["runtime_config_attempted"] is False
    assert report["runtime_builder_attempted"] is False
    assert report["telegram_read_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["outbox_write_attempted"] is False
    assert report["side_effects"]["db_write"] is False
    assert report["side_effects"]["telegram_read_called"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "error_code", "loader_calls", "builder_calls"),
    [
        ({"allow_runtime_config": False}, "runtime_config_not_allowed", 0, 0),
        ({"allow_telegram_read": False}, "telegram_read_not_allowed", 1, 0),
        ({"allow_database_write": False}, "database_write_not_allowed", 1, 0),
        ({"allow_outbox_write": False}, "outbox_write_not_allowed", 1, 0),
    ],
)
async def test_missing_each_gate_blocks_before_next_authority(
    overrides: dict[str, Any],
    error_code: str,
    loader_calls: int,
    builder_calls: int,
) -> None:
    result, loader, builder, _repository, history = await _run(_approved_config(**overrides))
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == error_code
    assert loader.calls == loader_calls
    assert builder.calls == builder_calls
    assert history.calls == []
    assert report["telegram_read_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["outbox_write_attempted"] is False


@pytest.mark.asyncio
async def test_max_messages_hard_cap_blocks_before_runtime_config() -> None:
    result, loader, builder, _repository, history = await _run(_approved_config(max_messages=4))
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == "max_messages_out_of_bounds"
    assert loader.calls == 0
    assert builder.calls == 0
    assert history.calls == []


@pytest.mark.asyncio
async def test_registry_target_requires_active_joined_single_row_before_read() -> None:
    repository = FakeRepository()
    history = FakeHistoryClient([_message()])
    result, _loader, builder, repository, history = await _run(
        _approved_config(chat_id=None, registry_id="11111111-1111-1111-1111-111111111111"),
        repository=repository,
        history=history,
    )
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == "registry_target_not_active_joined"
    assert report["registry_lookup_attempted"] is True
    assert builder.calls == 1
    assert repository.registry_lookups == ["11111111-1111-1111-1111-111111111111"]
    assert history.calls == []
    assert builder.close_commits == [False]


@pytest.mark.asyncio
async def test_one_fake_message_flows_through_projection_repository_and_outbox() -> None:
    result, _loader, builder, repository, history = await _run(_approved_config())
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, sort_keys=True)

    assert result.ok is True
    assert report["messages_requested"] == 1
    assert report["messages_seen"] == 1
    assert report["source_messages_created_count"] == 1
    assert report["source_versions_appended_count"] == 1
    assert report["outbox_events_inserted_count"] == 1
    assert report["idempotent_noop_count"] == 0
    assert report["database_write_attempted"] is True
    assert report["outbox_write_attempted"] is True
    assert report["side_effects"]["db_write"] is True
    assert report["side_effects"]["redis_mutation"] is False
    assert report["side_effects"]["telegram_send_called"] is False
    assert report["side_effects"]["telegram_edit_called"] is False
    assert report["side_effects"]["openai_called"] is False
    assert report["side_effects"]["github_called"] is False
    assert report["side_effects"]["x_called"] is False
    assert report["side_effects"]["web_called"] is False
    assert report["side_effects"]["notification_table_write"] is False
    assert report["side_effects"]["worker_started"] is False
    assert report["side_effects"]["run_forever_called"] is False
    assert report["side_effects"]["systemd_called"] is False
    assert report["side_effects"]["docker_called"] is False
    assert report["side_effects"]["alembic_called"] is False
    assert history.calls == [{"chat_id": RAW_CHAT_ID, "limit": 1}]
    assert len(repository.messages) == 1
    assert len(repository.outbox) == 1
    assert repository.outbox[0].event_type == "source_message.created.v1"
    assert builder.close_commits == [True]
    assert str(RAW_CHAT_ID) not in rendered
    assert RAW_MESSAGE_TEXT not in rendered
    assert DB_URL not in rendered
    assert RAW_SECRET not in rendered


@pytest.mark.asyncio
async def test_duplicate_same_message_is_idempotent_noop_without_new_outbox() -> None:
    repository = FakeRepository()
    first_history = FakeHistoryClient([_message()])
    first_result, _loader, _builder, repository, _history = await _run(
        _approved_config(),
        repository=repository,
        history=first_history,
    )
    second_history = FakeHistoryClient([_message()])
    second_result, _loader, _builder, repository, _history = await _run(
        _approved_config(),
        repository=repository,
        history=second_history,
    )
    second = second_result.to_sanitized_dict()

    assert first_result.ok is True
    assert second_result.ok is True
    assert second["source_messages_created_count"] == 0
    assert second["source_versions_appended_count"] == 0
    assert second["outbox_events_inserted_count"] == 0
    assert second["idempotent_noop_count"] == 1
    assert second["database_write_attempted"] is False
    assert second["outbox_write_attempted"] is False
    assert len(repository.outbox) == 1
    assert repository.upsert_calls == 1


@pytest.mark.asyncio
async def test_failure_report_omits_raw_message_json_secrets_and_exception_detail() -> None:
    repository = FakeRepository(fail_upsert=RuntimeError(EXCEPTION_DETAIL))
    result, _loader, _builder, _repository, _history = await _run(
        _approved_config(),
        repository=repository,
        history=FakeHistoryClient([_message()]),
    )
    output = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is False
    assert result.error_code == "unexpected_failure"
    assert result.error_class == "RuntimeError"
    assert "RuntimeError" in output
    for raw_value in (
        str(RAW_CHAT_ID),
        str(RAW_MESSAGE_ID),
        RAW_MESSAGE_TEXT,
        "sentinel raw message json payload value",
        DB_URL,
        RAW_SECRET,
        EXCEPTION_DETAIL,
    ):
        assert raw_value not in output


def test_runner_and_cli_do_not_import_forbidden_authority_modules() -> None:
    forbidden_import_roots = {
        "subprocess",
        "scripts.ops",
        "src.services.notifier_telegram",
        "src.services.judge_openai",
        "src.services.gh_enricher",
        "src.services.x_enricher",
        "src.services.web_enricher",
    }
    forbidden_call_names = {"run_forever", "systemctl", "docker", "alembic"}

    for path in (SOURCE_PATH, TOOL_PATH):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported: set[str] = set()
        called: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
            elif isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name):
                    called.add(func.id)
                elif isinstance(func, ast.Attribute):
                    called.add(func.attr)

        assert not (imported & forbidden_import_roots)
        assert not (called & forbidden_call_names)
