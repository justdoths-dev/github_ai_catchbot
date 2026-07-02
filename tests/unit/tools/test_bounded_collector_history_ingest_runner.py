from __future__ import annotations

import ast
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from src.services.collector_telegram.bounded_history_ingest_runner import (
    BoundedTelegramCollectorHistoryIngestRuntimeHandle,
)
from src.services.collector_telegram.config import CollectorTelegramConfig
from src.services.collector_telegram.models import CollectorEnvironment, CollectorMode, TrackedChat
from src.services.outbox_relay.models import OutboxEventRow
from tools import bounded_collector_history_ingest_runner as runner
from tools import bounded_telegram_collector_history_ingest_runner as legacy_runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_collector_history_ingest_runner.py"
LEGACY_TOOL_PATH = ROOT / "tools/bounded_telegram_collector_history_ingest_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_db_password@127.0.0.1/db"
REDIS_URL = "redis://sentinel:secret@127.0.0.1/0"
RAW_CHAT_ID = 9876543210123
RAW_MESSAGE_TEXT = "sentinel cli history ingest message text"
RAW_SECRET = "sentinel_cli_history_ingest_secret"
CLOSE_EXCEPTION_DETAIL = "private cli close failure with sentinel cli history ingest message text"


class FakeTransaction:
    def __init__(self, repository: "FakeRepository") -> None:
        self.repository = repository
        self.snapshot: Any = None

    async def __aenter__(self) -> "FakeRepository":
        self.snapshot = self.repository.snapshot()
        return self.repository

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is not None:
            self.repository.restore(self.snapshot)
        return None


class FakeRepository:
    def __init__(self, targets: list[TrackedChat] | None = None) -> None:
        self.targets = targets or [
            TrackedChat(
                registry_id="11111111-1111-1111-1111-111111111111",
                chat_id=RAW_CHAT_ID,
                desired_state="active",
                access_state="joined",
                source_kind="public_username",
                source_value="trendingrepo",
                priority_weight=100,
            )
        ]
        self.messages: dict[tuple[int, int], dict[str, Any]] = {}
        self.versions: dict[str, list[dict[str, Any]]] = {}
        self.outbox: list[Any] = []
        self.registry_lookups: list[str] = []
        self.mark_published_calls: list[UUID] = []
        self.cursor_updates: list[dict[str, Any]] = []

    def snapshot(self) -> Any:
        return deepcopy((self.messages, self.versions, self.outbox, self.cursor_updates))

    def restore(self, snapshot: Any) -> None:
        self.messages, self.versions, self.outbox, self.cursor_updates = snapshot

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)

    async def find_public_username_registry_targets(self, normalized_source_value: str):
        self.registry_lookups.append(normalized_source_value)
        rows = []
        for target in self.targets:
            source_value = target.source_value.strip().lstrip("@").lower()
            if source_value != normalized_source_value:
                continue
            rows.append(
                {
                    "registry_id": target.registry_id,
                    "chat_id": target.chat_id,
                    "desired_state": target.desired_state,
                    "access_state": target.access_state,
                    "source_kind": target.source_kind,
                    "source_value": target.source_value,
                    "username_snapshot": f"@{target.source_value}",
                    "priority_weight": target.priority_weight,
                    "last_seen_message_id": None,
                    "last_seen_message_date": None,
                }
            )
        return rows

    async def list_reconcile_targets(self, limit: int) -> list[TrackedChat]:
        self.registry_lookups.append(f"list_reconcile_targets:{limit}")
        return self.targets[:limit]

    async def get_source_message(self, *, platform: str, chat_id: int, message_id: int):
        assert platform == "telegram"
        return self.messages.get((chat_id, message_id))

    async def get_latest_version(self, source_message_id: str):
        versions = self.versions.get(source_message_id, [])
        return versions[-1] if versions else None

    async def upsert_source_message(self, projection: Any, *, platform: str = "telegram"):
        assert platform == "telegram"
        source_message_uuid = uuid5(NAMESPACE_URL, f"telegram:{projection.chat_id}:{projection.message_id}")
        source_message_id = str(source_message_uuid)
        row = {
            "source_message_id": source_message_uuid,
            "chat_id": projection.chat_id,
            "message_id": projection.message_id,
            "logical_post_key": projection.logical_post_key,
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
        self.outbox.append(event)
        return True

    async def get_outbox_event_by_dedupe_key(self, dedupe_key: str):
        for event in self.outbox:
            if event.dedupe_key == dedupe_key:
                return OutboxEventRow(
                    event_id=uuid5(NAMESPACE_URL, event.dedupe_key),
                    event_type=event.event_type,
                    aggregate_type=event.aggregate_type,
                    aggregate_id=UUID(str(event.aggregate_id)),
                    dedupe_key=event.dedupe_key,
                    payload_json=dict(event.payload_json),
                    status="pending",
                    fail_count=0,
                    created_at=datetime.now(timezone.utc),
                )
        return None

    async def mark_outbox_published(self, *, event_id: UUID, published_at: Any = None) -> bool:
        del published_at
        self.mark_published_calls.append(event_id)
        return True

    async def update_channel_sync_cursor(self, **kwargs: Any) -> None:
        self.cursor_updates.append(kwargs)

    async def count_source_message_versions(self, source_message_id: str) -> int:
        return len(self.versions.get(str(source_message_id), []))

    async def count_source_created_events(self, source_message_id: str) -> int:
        return sum(
            1
            for event in self.outbox
            if str(event.aggregate_id) == str(source_message_id)
            and event.event_type == "source_message.created.v1"
        )

    async def count_source_outbox_events(self, source_message_id: str) -> int:
        return sum(
            1
            for event in self.outbox
            if str(event.aggregate_id) == str(source_message_id)
            and event.event_type
            in {
                "source_message.created.v1",
                "source_message.edited.v1",
                "source_message.deleted.v1",
                "source_message.reconciled.v1",
            }
        )


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


class FakeRedisPublisher:
    def __init__(self) -> None:
        self.publish_calls: list[tuple[Any, Any]] = []

    async def publish(self, route: Any, message: Any) -> str:
        self.publish_calls.append((route, message))
        return "1234567890-0"


class FakeRuntimeBuilder:
    def __init__(
        self,
        *,
        close_error: Exception | None = None,
        redis_publisher: FakeRedisPublisher | None = None,
        repository: FakeRepository | None = None,
    ) -> None:
        self.repository = repository or FakeRepository()
        self.history_client = FakeHistoryClient()
        self.redis_publisher = redis_publisher
        self.close_error = close_error
        self.commit_calls = 0
        self.close_commits: list[bool] = []

    async def __call__(self, runtime_config: Any, state: Any, logger: Any):
        del runtime_config, state, logger

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            if self.close_error is not None:
                raise self.close_error

        async def commit() -> None:
            self.commit_calls += 1

        return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
            repository=self.repository,
            history_client=self.history_client,
            redis_publisher=self.redis_publisher,
            close=close,
            commit=commit,
        )


def _runtime_config() -> CollectorTelegramConfig:
    return CollectorTelegramConfig(
        app_env=CollectorEnvironment.TEST,
        database_url=DB_URL,
        redis_url=REDIS_URL,
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


def _three_cli_targets() -> list[TrackedChat]:
    return [
        TrackedChat(
            registry_id="11111111-1111-1111-1111-111111111111",
            chat_id=RAW_CHAT_ID,
            desired_state="active",
            access_state="joined",
            source_kind="public_username",
            source_value="alpha_tools",
            priority_weight=100,
        ),
        TrackedChat(
            registry_id="22222222-2222-2222-2222-222222222222",
            chat_id=RAW_CHAT_ID + 1,
            desired_state="active",
            access_state="joined",
            source_kind="public_username",
            source_value="beta_tools",
            priority_weight=100,
        ),
        TrackedChat(
            registry_id="33333333-3333-3333-3333-333333333333",
            chat_id=RAW_CHAT_ID + 2,
            desired_state="active",
            access_state="joined",
            source_kind="public_username",
            source_value="gamma_tools",
            priority_weight=100,
        ),
    ]


def test_main_with_no_flags_returns_fail_closed_json(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert parsed["schema_version"] == "live_collector_one_channel_source_last_rollout_v1"
    assert parsed["runner_name"] == "bounded_collector_history_ingest_runner"
    assert parsed["operator_approved"] is False
    assert parsed["runtime_config_attempted"] is False
    assert parsed["telegram_read_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["source_outbox_write_attempted"] is False
    assert parsed["error_code"] == "operator_approval_missing"


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
        "--mode",
        "--rollout-scope",
        "--source-kind",
        "--source-value",
        "--registry-id-suffix",
        "--max-targets",
        "--history-limit",
        "--operator-approved",
        "--confirm-token",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-telegram-read",
        "--allow-database-write",
        "--allow-source-message-write",
        "--allow-source-version-write",
        "--allow-source-outbox-write",
        "--allow-source-outbox-publish",
        "--allow-redis-publish",
    }


def test_unsupported_authority_flags_return_sanitized_json(capsys) -> None:
    for flag in (
        "--allow-network",
        "--database-url",
        "--runtime-env-path",
        "--telegram-api-hash",
        "--tdlib-state-dir",
        "--allow-send",
        "--all-channels",
        "--live-collector",
        "--chat-id",
        "--registry-id",
    ):
        exit_code = runner.main([flag])
        parsed = json.loads(capsys.readouterr().out)

        assert exit_code == 1
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["runtime_config_attempted"] is False
        assert parsed["telegram_read_attempted"] is False
        assert parsed["database_write_attempted"] is False
        assert parsed["source_outbox_write_attempted"] is False


def test_plan_without_telegram_gate_delegates_to_exact_source_and_writes_nothing(capsys) -> None:
    runtime_builder = FakeRuntimeBuilder()
    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--source-kind",
            "public_username",
            "--source-value",
            "@trendingrepo",
            "--history-limit",
            "5",
        ],
        runtime_config_loader=_runtime_config,
        runtime_builder=runtime_builder,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert parsed["ok"] is True
    assert parsed["mode"] == "plan"
    assert parsed["status"] == "pass"
    assert parsed["source_value_surface"] is None
    assert parsed["exact_channel_target_fingerprint"].startswith("sha256:")
    assert parsed["registry_target_fingerprint"].startswith("sha256:")
    assert parsed["telegram_read_attempted"] is False
    assert parsed["authority"]["live_telegram_read_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["bounded_counts"]["registry_targets"] == 1
    assert runtime_builder.repository.registry_lookups == ["trendingrepo"]
    assert runtime_builder.history_client.calls == []
    assert runtime_builder.close_commits == [False]
    assert "trendingrepo" not in captured.out


def test_three_channel_plan_repeated_source_values_delegate_without_live_read_or_raw_output(capsys) -> None:
    runtime_builder = FakeRuntimeBuilder(repository=FakeRepository(targets=_three_cli_targets()))
    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--source-kind",
            "public_username",
            "--source-value",
            "@alpha_tools",
            "--source-value",
            "beta_tools",
            "--source-value",
            "gamma_tools",
            "--history-limit",
            "5",
        ],
        runtime_config_loader=_runtime_config,
        runtime_builder=runtime_builder,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert parsed["schema_version"] == "live_collector_three_channel_source_last_rollout_v1"
    assert parsed["ok"] is True
    assert parsed["mode"] == "plan"
    assert parsed["target_count"] == 3
    assert len(parsed["target_fingerprints"]) == 3
    assert len(parsed["per_channel_results"]) == 3
    assert parsed["telegram_read_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert runtime_builder.repository.registry_lookups == ["alpha_tools", "beta_tools", "gamma_tools"]
    assert runtime_builder.history_client.calls == []
    assert runtime_builder.close_commits == [False]
    for raw in ("alpha_tools", "beta_tools", "gamma_tools", str(RAW_CHAT_ID), RAW_MESSAGE_TEXT):
        assert raw not in captured.out


def test_three_channel_execute_requires_f2_token_through_cli_before_runtime_config(capsys) -> None:
    runtime_builder = FakeRuntimeBuilder(repository=FakeRepository(targets=_three_cli_targets()))
    exit_code = runner.main(
        [
            "--mode",
            "execute",
            "--operator-approved",
            "--confirm-token",
            runner.EXECUTE_CONFIRM_TOKEN,
            "--allow-runtime-config",
            "--allow-database-read",
            "--allow-telegram-read",
            "--allow-database-write",
            "--allow-source-message-write",
            "--allow-source-version-write",
            "--allow-source-outbox-write",
            "--source-kind",
            "public_username",
            "--source-value",
            "alpha_tools",
            "--source-value",
            "beta_tools",
            "--source-value",
            "gamma_tools",
            "--history-limit",
            "1",
        ],
        runtime_config_loader=_runtime_config,
        runtime_builder=runtime_builder,
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["error_code"] == "confirm_token_invalid"
    assert parsed["runtime_config_attempted"] is False
    assert parsed["runtime_builder_attempted"] is False
    assert runtime_builder.repository.registry_lookups == []
    assert runtime_builder.history_client.calls == []


def test_full_registry_plan_delegates_with_explicit_scope_and_max_targets(capsys) -> None:
    runtime_builder = FakeRuntimeBuilder(repository=FakeRepository(targets=_three_cli_targets()))
    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--rollout-scope",
            "full-tracked-registry",
            "--max-targets",
            "3",
            "--history-limit",
            "5",
        ],
        runtime_config_loader=_runtime_config,
        runtime_builder=runtime_builder,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert parsed["schema_version"] == "live_collector_full_registry_source_last_rollout_v1"
    assert parsed["ok"] is True
    assert parsed["mode"] == "plan"
    assert parsed["rollout_scope"] == "full_tracked_registry"
    assert parsed["target_count"] == 3
    assert parsed["max_targets"] == 3
    assert len(parsed["target_fingerprints"]) == 3
    assert runtime_builder.repository.registry_lookups == ["list_reconcile_targets:4"]
    assert runtime_builder.history_client.calls == []
    assert runtime_builder.close_commits == [False]
    for raw in ("alpha_tools", "beta_tools", "gamma_tools", str(RAW_CHAT_ID), RAW_MESSAGE_TEXT):
        assert raw not in captured.out


def test_full_registry_execute_requires_f3_token_through_cli_before_runtime_config(capsys) -> None:
    runtime_builder = FakeRuntimeBuilder(repository=FakeRepository(targets=_three_cli_targets()))
    exit_code = runner.main(
        [
            "--mode",
            "execute",
            "--operator-approved",
            "--confirm-token",
            runner.THREE_CHANNEL_EXECUTE_CONFIRM_TOKEN,
            "--allow-runtime-config",
            "--allow-database-read",
            "--allow-telegram-read",
            "--allow-database-write",
            "--allow-source-message-write",
            "--allow-source-version-write",
            "--allow-source-outbox-write",
            "--rollout-scope",
            "full-tracked-registry",
            "--max-targets",
            "3",
            "--history-limit",
            "1",
        ],
        runtime_config_loader=_runtime_config,
        runtime_builder=runtime_builder,
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["error_code"] == "confirm_token_invalid"
    assert parsed["runtime_config_attempted"] is False
    assert parsed["runtime_builder_attempted"] is False
    assert runtime_builder.repository.registry_lookups == []
    assert runtime_builder.history_client.calls == []


def test_execute_publish_prints_sanitized_json_and_thin_handoff(capsys) -> None:
    redis_publisher = FakeRedisPublisher()
    runtime_builder = FakeRuntimeBuilder(redis_publisher=redis_publisher)
    exit_code = runner.main(
        [
            "--mode",
            "execute",
            "--operator-approved",
            "--confirm-token",
            runner.EXECUTE_CONFIRM_TOKEN,
            "--allow-runtime-config",
            "--allow-database-read",
            "--allow-telegram-read",
            "--allow-database-write",
            "--allow-source-message-write",
            "--allow-source-version-write",
            "--allow-source-outbox-write",
            "--allow-source-outbox-publish",
            "--allow-redis-publish",
            "--source-kind",
            "public_username",
            "--source-value",
            "trendingrepo",
            "--history-limit",
            "1",
        ],
        runtime_config_loader=_runtime_config,
        runtime_builder=runtime_builder,
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["ok"] is True
    assert parsed["messages_requested"] == 1
    assert parsed["messages_seen"] == 1
    assert parsed["source_messages_created_count"] == 1
    assert parsed["source_versions_appended_count"] == 1
    assert parsed["outbox_events_inserted_count"] == 1
    assert parsed["source_created_events_count"] == 1
    assert parsed["redis_events_published_count"] == 1
    assert parsed["event_outbox_marked_published_count"] == 1
    assert parsed["bounded_counts"]["source_normalize_handoffs"] == 1
    assert parsed["source_message_fingerprints"]
    assert parsed["source_outbox_event_fingerprints"]
    assert parsed["redis_message_fingerprints"]
    assert runtime_builder.history_client.calls == [{"chat_id": RAW_CHAT_ID, "limit": 1}]
    assert runtime_builder.commit_calls == 3
    assert runtime_builder.close_commits == [True]

    route, message = redis_publisher.publish_calls[0]
    fields = message.as_stream_fields()
    assert route.queue_name == "q.source.normalize"
    assert fields["stage_name"] == "normalize"
    assert fields["root_object_type"] == "source_message"
    assert fields["pipeline_run_id"] == ""
    assert fields["not_before"] == ""
    assert "payload_json" not in fields
    assert RAW_MESSAGE_TEXT not in json.dumps(fields, sort_keys=True)
    for raw in (
        str(RAW_CHAT_ID),
        "getChatHistory",
        RAW_MESSAGE_TEXT,
        DB_URL,
        REDIS_URL,
        RAW_SECRET,
        "+15555550123",
        "/tmp/sentinel-cli-history-ingest-tdlib-state",
        "/tmp/sentinel-cli-history-ingest-tdlib-files",
    ):
        assert raw not in output


def test_main_close_failure_returns_sanitized_json_without_stderr(capsys) -> None:
    runtime_builder = FakeRuntimeBuilder(close_error=RuntimeError(CLOSE_EXCEPTION_DETAIL))
    exit_code = runner.main(
        [
            "--mode",
            "execute",
            "--operator-approved",
            "--confirm-token",
            runner.EXECUTE_CONFIRM_TOKEN,
            "--allow-runtime-config",
            "--allow-database-read",
            "--allow-telegram-read",
            "--allow-database-write",
            "--allow-source-message-write",
            "--allow-source-version-write",
            "--allow-source-outbox-write",
            "--source-kind",
            "public_username",
            "--source-value",
            "trendingrepo",
            "--history-limit",
            "1",
        ],
        runtime_config_loader=_runtime_config,
        runtime_builder=runtime_builder,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["ok"] is False
    assert parsed["status"] == "failed"
    assert parsed["error_code"] == "runtime_commit_failed"
    assert parsed["error_class"] == "RuntimeError"
    assert runtime_builder.close_commits == [True]
    assert runtime_builder.history_client.calls == [{"chat_id": RAW_CHAT_ID, "limit": 1}]
    for raw in (
        str(RAW_CHAT_ID),
        "getChatHistory",
        RAW_MESSAGE_TEXT,
        DB_URL,
        REDIS_URL,
        RAW_SECRET,
        CLOSE_EXCEPTION_DETAIL,
        "+15555550123",
        "/tmp/sentinel-cli-history-ingest-tdlib-state",
        "/tmp/sentinel-cli-history-ingest-tdlib-files",
    ):
        assert raw not in captured.out


def test_legacy_tool_delegates_to_new_exact_source_parser() -> None:
    assert legacy_runner.main is runner.main
    assert "chat-id" not in LEGACY_TOOL_PATH.read_text(encoding="utf-8")
