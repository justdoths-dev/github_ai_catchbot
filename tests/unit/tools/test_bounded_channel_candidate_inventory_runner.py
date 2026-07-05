from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tools import bounded_channel_candidate_inventory_runner as runner
from src.services.collector_telegram.channel_candidate_inventory import (
    ChannelCandidateInventoryRepositoryHandle,
    ChannelCandidateInventoryRuntimeConfig,
)


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_channel_candidate_inventory_runner.py"
DB_URL_SENTINEL = "private_database_locator_must_not_print"
RAW_CHAT_ID_SENTINEL = "raw_chat_locator_must_not_print"
RAW_MESSAGE_ID_SENTINEL = "raw_message_locator_must_not_print"
RAW_TEXT_SENTINEL = "private_text_surface_sentinel_must_not_print"
RAW_URL_SENTINEL = "raw_url_locator_must_not_print"


class FakeRepository:
    async def load_channel_candidate_rows(self, *, limit: int, lookback_days: int):
        assert limit == 10
        assert lookback_days == 7
        return [
            _row("alphaagents", "Alpha Agents", 18, 5, github_link_seen=True, ai_dev_context_seen=True),
            _row("betacoding", "Beta Coding", 10, 3, vibe_coding_seen=True),
            _row("gammatools", "Gamma Tools", 8, 1, x_link_seen=True),
        ]


class FakeRepositoryBuilder:
    def __init__(self) -> None:
        self.closed = False

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True

        async def close() -> None:
            self.closed = True

        return ChannelCandidateInventoryRepositoryHandle(repository=FakeRepository(), close=close)


def test_main_with_required_flags_returns_sanitized_json_only_and_empty_stderr(capsys) -> None:
    builder = FakeRepositoryBuilder()
    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-env-read",
            "--allow-database-read",
            "--runtime-env-file",
            "/tmp/runtime.env",
            "--limit",
            "10",
        ],
        runtime_config_loader=_runtime_config_loader,
        repository_builder=builder,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert parsed["schema_version"] == "channel_candidate_inventory_v1"
    assert parsed["status"] == "pass"
    assert parsed["reason_code"] == "channel_candidate_inventory_ready"
    assert parsed["selection_guidance"] == {
        "recommended_count": 3,
        "max_messages_next_step": 1,
        "avoid": ["removed", "access_lost", "no_recent_activity"],
    }
    assert parsed["selectable_candidate_count"] == 3
    assert [candidate["rank"] for candidate in parsed["candidates"]] == [1, 2, 3]
    assert all(candidate["public_username"].startswith("@") for candidate in parsed["candidates"])
    assert all(candidate["access_state"] == "joined_active" for candidate in parsed["candidates"])
    assert parsed["authority"]["database_read_allowed"] is True
    assert parsed["authority"]["database_write_allowed"] is False
    assert parsed["authority"]["redis_allowed"] is False
    assert parsed["authority"]["telegram_live_read_allowed"] is False
    assert parsed["authority"]["provider_calls_allowed"] is False
    assert parsed["raw_values_printed"] is False
    assert builder.closed is True

    rendered = captured.out
    assert DB_URL_SENTINEL not in rendered
    assert RAW_CHAT_ID_SENTINEL not in rendered
    assert RAW_MESSAGE_ID_SENTINEL not in rendered
    assert RAW_TEXT_SENTINEL not in rendered
    assert RAW_URL_SENTINEL not in rendered


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
        "--allow-runtime-env-read",
        "--allow-database-read",
        "--runtime-env-file",
        "--limit",
    }


def test_unsupported_authority_flags_return_json_without_stderr(capsys) -> None:
    unsupported_flags = (
        "--allow-database-write",
        "--allow-redis-read",
        "--allow-redis-publish",
        "--allow-telegram-read",
        "--allow-telegram-send",
        "--allow-openai",
        "--allow-provider-calls",
        "--database-url",
        "--redis-url",
        "--source-value",
        "--run-worker",
    )
    for flag in unsupported_flags:
        exit_code = runner.main([flag])
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)

        assert exit_code == 1
        assert captured.err == ""
        assert parsed["status"] == "blocked"
        assert parsed["reason_code"] == "unsupported_cli_argument"
        assert parsed["authority"]["database_read_allowed"] is False
        assert parsed["authority"]["database_write_allowed"] is False
        assert parsed["authority"]["redis_allowed"] is False
        assert parsed["authority"]["telegram_live_read_allowed"] is False
        assert parsed["authority"]["openai_allowed"] is False


def test_required_authority_flags_gate_before_runtime_config() -> None:
    cases = (
        ([], "operator_approval_missing"),
        (["--operator-approved"], "runtime_env_read_not_allowed"),
        (
            ["--operator-approved", "--allow-runtime-env-read"],
            "runtime_env_file_required",
        ),
        (
            [
                "--operator-approved",
                "--allow-runtime-env-read",
                "--runtime-env-file",
                "/tmp/runtime.env",
            ],
            "database_read_not_allowed",
        ),
        (
            [
                "--operator-approved",
                "--allow-runtime-env-read",
                "--allow-database-read",
                "--runtime-env-file",
                "/tmp/runtime.env",
                "--limit",
                "0",
            ],
            "invalid_limit",
        ),
    )
    for argv, reason_code in cases:
        result = runner.run(
            runner.build_parser().parse_args(argv),
            runtime_config_loader=_raising_runtime_config_loader,
            repository_builder=FakeRepositoryBuilder(),
        )

        assert result.exit_code == 1
        assert result.report["status"] == "blocked"
        assert result.report["reason_code"] == reason_code


def _runtime_config_loader(runtime_env_file: str | None, state: Any):
    assert runtime_env_file == "/tmp/runtime.env"
    state.runtime_env_read_attempted = True
    return ChannelCandidateInventoryRuntimeConfig(database_url=DB_URL_SENTINEL)


def _raising_runtime_config_loader(runtime_env_file: str | None, state: Any):
    del runtime_env_file, state
    raise AssertionError("runtime config must not be loaded before gates pass")


def _row(
    source_value: str,
    title_snapshot: str,
    recent_messages: int,
    recent_signals: int,
    *,
    github_link_seen: bool = False,
    x_link_seen: bool = False,
    vibe_coding_seen: bool = False,
    ai_dev_context_seen: bool = False,
) -> dict[str, Any]:
    return {
        "source_value": source_value,
        "username_snapshot": None,
        "title_snapshot": title_snapshot,
        "desired_state": "active",
        "access_state": "joined",
        "priority_weight": 100,
        "last_seen_message_date": datetime(2026, 7, 1, tzinfo=timezone.utc),
        "last_history_sync_at": datetime(2026, 7, 2, tzinfo=timezone.utc),
        "recent_messages_7d": recent_messages,
        "recent_signal_messages_7d": recent_signals,
        "github_link_seen": github_link_seen,
        "x_link_seen": x_link_seen,
        "vibe_coding_seen": vibe_coding_seen,
        "ai_dev_context_seen": ai_dev_context_seen,
        "generic_ai_noise_only": False,
        "raw_chat_id": RAW_CHAT_ID_SENTINEL,
        "raw_message_id": RAW_MESSAGE_ID_SENTINEL,
        "text_surface": RAW_TEXT_SENTINEL,
        "url_surface_json": [{"url": RAW_URL_SENTINEL}],
    }
