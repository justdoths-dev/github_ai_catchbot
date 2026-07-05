from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.services.collector_telegram.channel_candidate_inventory import (
    ChannelCandidateInventoryConfig,
    ChannelCandidateInventoryRepositoryHandle,
    ChannelCandidateInventoryRuntimeConfig,
    SqlAlchemyChannelCandidateInventoryRepository,
    render_sanitized_json,
    run_channel_candidate_inventory_sync,
)


ROOT = Path(__file__).resolve().parents[4]
SERVICE_PATH = ROOT / "src/services/collector_telegram/channel_candidate_inventory.py"
DB_URL_SENTINEL = "private_database_locator_must_not_print"
RAW_CHAT_ID_SENTINEL = "raw_chat_locator_must_not_print"
RAW_MESSAGE_ID_SENTINEL = "raw_message_locator_must_not_print"
RAW_TEXT_SENTINEL = "private_text_surface_sentinel_must_not_print"
RAW_URL_SENTINEL = "raw_url_locator_must_not_print"


class FakeRepository:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[dict[str, int]] = []

    async def load_channel_candidate_rows(self, *, limit: int, lookback_days: int):
        self.calls.append({"limit": limit, "lookback_days": lookback_days})
        return list(self.rows)


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository
        self.calls = 0
        self.closed = False

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True

        async def close() -> None:
            self.closed = True

        return ChannelCandidateInventoryRepositoryHandle(repository=self.repository, close=close)


def test_inventory_passes_with_three_selectable_public_usernames_and_omits_raw_values() -> None:
    repository = FakeRepository(
        [
            _row(
                source_value="stabledev",
                title_snapshot="Stable Dev",
                recent_messages_7d=20,
                recent_signal_messages_7d=2,
                github_link_seen=True,
                last_history_sync_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
            ),
            _row(
                source_value="@ghsignals",
                title_snapshot="GitHub Signals",
                recent_messages_7d=12,
                recent_signal_messages_7d=6,
                github_link_seen=True,
                ai_dev_context_seen=True,
            ),
            _row(
                source_value="noisytools",
                title_snapshot="Noisy Tools",
                recent_messages_7d=9,
                recent_signal_messages_7d=1,
                vibe_coding_seen=True,
            ),
            _row(
                source_value="inactive",
                title_snapshot="Inactive",
                recent_messages_7d=0,
                recent_signal_messages_7d=0,
            ),
        ]
    )
    builder = FakeRepositoryBuilder(repository)

    result = run_channel_candidate_inventory_sync(
        _approved_config(),
        runtime_config_loader=_runtime_config_loader,
        repository_builder=builder,
    )
    report = result.to_sanitized_dict()
    rendered = render_sanitized_json(report)

    assert result.ok is True
    assert report["schema_version"] == "channel_candidate_inventory_v1"
    assert report["status"] == "pass"
    assert report["reason_code"] == "channel_candidate_inventory_ready"
    assert report["selectable_candidate_count"] == 3
    assert report["authority"]["database_read_allowed"] is True
    assert report["authority"]["database_write_allowed"] is False
    assert report["authority"]["redis_allowed"] is False
    assert report["authority"]["telegram_live_read_allowed"] is False
    assert report["redactions_applied"]["database_url_omitted"] is True
    assert report["redactions_applied"]["raw_chat_ids_omitted"] is True
    assert report["raw_values_printed"] is False
    assert repository.calls == [{"limit": 20, "lookback_days": 7}]
    assert builder.closed is True

    parsed = json.loads(rendered)
    assert parsed["candidates"][0]["rank"] == 1
    assert parsed["candidates"][0]["public_username"].startswith("@")
    assert parsed["candidates"][0]["access_state"] == "joined_active"
    assert parsed["candidates"][0]["recent_messages_7d"] >= parsed["candidates"][0]["recent_signal_messages_7d"]
    assert any(candidate["recommended_bucket"] == "good_f2_candidate" for candidate in parsed["candidates"])
    assert DB_URL_SENTINEL not in rendered
    assert RAW_CHAT_ID_SENTINEL not in rendered
    assert RAW_MESSAGE_ID_SENTINEL not in rendered
    assert RAW_TEXT_SENTINEL not in rendered
    assert RAW_URL_SENTINEL not in rendered


def test_inventory_blocks_when_fewer_than_three_selectable_candidates() -> None:
    repository = FakeRepository(
        [
            _row(source_value="onlyone", recent_messages_7d=5, recent_signal_messages_7d=1),
            _row(source_value="accesslost", access_state="access_lost", recent_messages_7d=9),
            _row(source_value="inactive", recent_messages_7d=0),
        ]
    )

    result = run_channel_candidate_inventory_sync(
        _approved_config(),
        runtime_config_loader=_runtime_config_loader,
        repository_builder=FakeRepositoryBuilder(repository),
    )
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["status"] == "blocked"
    assert report["reason_code"] == "insufficient_selectable_channel_candidates"
    assert report["selectable_candidate_count"] == 1
    assert any(candidate["recommended_bucket"].startswith("avoid_") for candidate in report["candidates"])


def test_authority_gates_block_before_runtime_env_or_database_read() -> None:
    cases = (
        (ChannelCandidateInventoryConfig(), "operator_approval_missing"),
        (
            ChannelCandidateInventoryConfig(operator_approved=True),
            "runtime_env_read_not_allowed",
        ),
        (
            ChannelCandidateInventoryConfig(operator_approved=True, allow_runtime_env_read=True),
            "runtime_env_file_required",
        ),
        (
            ChannelCandidateInventoryConfig(
                operator_approved=True,
                allow_runtime_env_read=True,
                runtime_env_file="/tmp/runtime.env",
            ),
            "database_read_not_allowed",
        ),
        (
            ChannelCandidateInventoryConfig(
                operator_approved=True,
                allow_runtime_env_read=True,
                allow_database_read=True,
                runtime_env_file="/tmp/runtime.env",
                limit=0,
            ),
            "invalid_limit",
        ),
    )

    for config, reason_code in cases:
        result = run_channel_candidate_inventory_sync(
            config,
            runtime_config_loader=_raising_runtime_config_loader,
            repository_builder=FakeRepositoryBuilder(FakeRepository([])),
        )
        report = result.to_sanitized_dict()

        assert result.ok is False
        assert report["reason_code"] == reason_code
        assert report["candidate_count"] == 0


def test_sql_repository_reads_registry_and_source_message_aggregates_without_selecting_raw_values() -> None:
    source = SERVICE_PATH.read_text(encoding="utf-8")

    assert "FROM telegram_channel_registry" in source
    assert "JOIN source_messages" in source
    assert "sm.chat_id = r.chat_id" in source
    assert "raw_message_json" not in source
    assert "sm.message_id" not in source
    assert "last_seen_message_id" not in source
    assert "SELECT *" not in source
    assert "INSERT INTO" not in source
    assert "UPDATE " not in source
    assert "DELETE " not in source
    assert SqlAlchemyChannelCandidateInventoryRepository is not None


def _runtime_config_loader(runtime_env_file: str | None, state):
    assert runtime_env_file == "/tmp/runtime.env"
    state.runtime_env_read_attempted = True
    return ChannelCandidateInventoryRuntimeConfig(database_url=DB_URL_SENTINEL)


def _raising_runtime_config_loader(runtime_env_file: str | None, state):
    del runtime_env_file, state
    raise AssertionError("runtime config must not load before gates pass")


def _approved_config() -> ChannelCandidateInventoryConfig:
    return ChannelCandidateInventoryConfig(
        operator_approved=True,
        allow_runtime_env_read=True,
        allow_database_read=True,
        runtime_env_file="/tmp/runtime.env",
    )


def _row(
    *,
    source_value: str,
    title_snapshot: str = "Example",
    desired_state: str = "active",
    access_state: str = "joined",
    priority_weight: int = 100,
    last_seen_message_date: datetime | None = None,
    last_history_sync_at: datetime | None = None,
    recent_messages_7d: int = 3,
    recent_signal_messages_7d: int = 1,
    github_link_seen: bool = False,
    x_link_seen: bool = False,
    vibe_coding_seen: bool = False,
    ai_dev_context_seen: bool = False,
    generic_ai_noise_only: bool = False,
) -> dict[str, Any]:
    return {
        "source_value": source_value,
        "username_snapshot": None,
        "title_snapshot": title_snapshot,
        "desired_state": desired_state,
        "access_state": access_state,
        "priority_weight": priority_weight,
        "last_seen_message_date": last_seen_message_date or datetime(2026, 7, 1, tzinfo=timezone.utc),
        "last_history_sync_at": last_history_sync_at,
        "recent_messages_7d": recent_messages_7d,
        "recent_signal_messages_7d": recent_signal_messages_7d,
        "github_link_seen": github_link_seen,
        "x_link_seen": x_link_seen,
        "vibe_coding_seen": vibe_coding_seen,
        "ai_dev_context_seen": ai_dev_context_seen,
        "generic_ai_noise_only": generic_ai_noise_only,
        "raw_chat_id": RAW_CHAT_ID_SENTINEL,
        "raw_message_id": RAW_MESSAGE_ID_SENTINEL,
        "text_surface": RAW_TEXT_SENTINEL,
        "url_surface_json": [{"url": RAW_URL_SENTINEL}],
    }
