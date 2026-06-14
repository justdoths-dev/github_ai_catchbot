from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from src.services.collector_telegram.bounded_history_ingest_runner import (
    BoundedTelegramCollectorHistoryIngestRuntimeHandle,
)
from src.services.collector_telegram.config import CollectorTelegramConfig
from src.services.collector_telegram.models import CollectorEnvironment, CollectorMode
from tools import bounded_telegram_collector_history_ingest_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_telegram_collector_history_ingest_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_db_password@127.0.0.1/db"
RAW_CHAT_ID = 9876543210123
RAW_MESSAGE_TEXT = "sentinel cli history ingest message text"
RAW_SECRET = "sentinel_cli_history_ingest_secret"


class FakeTransaction:
    def __init__(self, repository: "FakeRepository") -> None:
        self.repository = repository

    async def __aenter__(self) -> "FakeRepository":
        return self.repository

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class FakeRepository:
    def __init__(self) -> None:
        self.messages: dict[tuple[int, int], dict[str, Any]] = {}
        self.versions: dict[str, list[dict[str, Any]]] = {}
        self.outbox: list[Any] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)

    async def get_active_joined_tracked_chat_by_registry_id(self, registry_id: str):
        del registry_id
        return None

    async def get_source_message(self, *, platform: str, chat_id: int, message_id: int):
        assert platform == "telegram"
        return self.messages.get((chat_id, message_id))

    async def get_latest_version(self, source_message_id: str):
        versions = self.versions.get(source_message_id, [])
        return versions[-1] if versions else None

    async def upsert_source_message(self, projection: Any, *, platform: str = "telegram"):
        assert platform == "telegram"
        source_message_id = str(uuid5(NAMESPACE_URL, f"telegram:{projection.chat_id}:{projection.message_id}"))
        row = {
            "source_message_id": source_message_id,
            "chat_id": projection.chat_id,
            "message_id": projection.message_id,
        }
        self.messages[(projection.chat_id, projection.message_id)] = row
        self.versions.setdefault(source_message_id, [])
        return row

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: Any,
        version_reason: str,
        observed_at: Any = None,
        telegram_edit_date: Any = None,
    ):
        del version_reason, observed_at, telegram_edit_date
        versions = self.versions.setdefault(source_message_id, [])
        row = {"version_no": len(versions) + 1, "content_hash": projection.content_hash}
        versions.append(row)
        return True, row

    async def insert_outbox_event(self, event: Any) -> bool:
        self.outbox.append(event)
        return True


class FakeHistoryClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, int]] = []

    async def fetch_newest_history_messages(self, *, chat_id: int, limit: int):
        self.calls.append({"chat_id": chat_id, "limit": limit})
        return (
            {
                "@type": "message",
                "chat_id": chat_id,
                "id": 123456,
                "date": 1713550000,
                "content": {
                    "@type": "messageText",
                    "text": {"text": RAW_MESSAGE_TEXT, "entities": []},
                },
            },
        )

    async def close(self) -> None:
        return None


class FakeRuntimeBuilder:
    def __init__(self) -> None:
        self.repository = FakeRepository()
        self.history_client = FakeHistoryClient()

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, state, logger

        async def close(commit: bool) -> None:
            del commit

        return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
            repository=self.repository,
            history_client=self.history_client,
            close=close,
        )


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
        tdlib_state_dir="/tmp/sentinel-cli-history-ingest-tdlib-state",
        tdlib_files_dir="/tmp/sentinel-cli-history-ingest-tdlib-files",
        tdlib_db_encryption_key="sentinel-cli-tdlib-encryption-key",
        reconcile_interval_sec=300,
        reconcile_backfill_limit=3,
        warm_backfill_limit=1,
        history_page_limit=3,
    )


def test_main_with_no_flags_returns_fail_closed_json(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_telegram_collector_history_ingest_v1"
    assert parsed["runner_name"] == "bounded_telegram_collector_history_ingest_runner"
    assert parsed["operator_approved"] is False
    assert parsed["runtime_config_attempted"] is False
    assert parsed["tdlib_auth_ready_checked"] is False
    assert parsed["tdlib_auth_ready"] is False
    assert parsed["tdlib_parameters_submitted"] is False
    assert parsed["tdlib_log_suppression_attempted"] is False
    assert parsed["tdlib_log_suppression_confirmed"] is False
    assert parsed["telegram_read_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["outbox_write_attempted"] is False
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["side_effects"]["db_write"] is False
    assert parsed["side_effects"]["telegram_read_called"] is False


def test_parser_exposes_only_approved_bounded_flags() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    parser_flags: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument":
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value.startswith("--"):
                    parser_flags.add(arg.value)

    assert parser_flags == {
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-telegram-read",
        "--allow-database-write",
        "--allow-outbox-write",
        "--max-messages",
        "--chat-id",
        "--registry-id",
    }


def test_unsupported_authority_flags_return_sanitized_json(capsys) -> None:
    for flag in (
        "--allow-network",
        "--database-url",
        "--runtime-env-path",
        "--telegram-api-hash",
        "--tdlib-state-dir",
        "--allow-send",
    ):
        exit_code = runner.main([flag])
        parsed = json.loads(capsys.readouterr().out)

        assert exit_code == 1
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["runtime_config_attempted"] is False
        assert parsed["telegram_read_attempted"] is False
        assert parsed["database_write_attempted"] is False
        assert parsed["outbox_write_attempted"] is False


def test_valid_cli_run_prints_sanitized_json_and_delegates_to_source(capsys) -> None:
    runtime_builder = FakeRuntimeBuilder()
    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-telegram-read",
            "--allow-database-write",
            "--allow-outbox-write",
            "--max-messages",
            "1",
            "--chat-id",
            str(RAW_CHAT_ID),
        ],
        runtime_config_loader=_runtime_config,
        runtime_builder=runtime_builder,
    )
    captured = capsys.readouterr()
    output = captured.out
    parsed = json.loads(output)

    assert exit_code == 0
    assert captured.err == ""
    assert parsed["ok"] is True
    assert parsed["messages_requested"] == 1
    assert parsed["messages_seen"] == 1
    assert parsed["source_messages_created_count"] == 1
    assert parsed["source_versions_appended_count"] == 1
    assert parsed["outbox_events_inserted_count"] == 1
    assert runtime_builder.history_client.calls == [{"chat_id": RAW_CHAT_ID, "limit": 1}]
    for raw in (
        str(RAW_CHAT_ID),
        "getChatHistory",
        RAW_MESSAGE_TEXT,
        DB_URL,
        RAW_SECRET,
        "+15555550123",
        "/tmp/sentinel-cli-history-ingest-tdlib-state",
        "/tmp/sentinel-cli-history-ingest-tdlib-files",
    ):
        assert raw not in output
