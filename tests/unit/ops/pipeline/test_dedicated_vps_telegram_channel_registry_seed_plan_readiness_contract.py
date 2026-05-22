from __future__ import annotations

import ast
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_telegram_channel_registry_seed_plan_readiness.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-for-seed-plan@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_DATABASE_PASSWORD = "unit-db-password-for-seed-plan"
FAKE_TELEGRAM_SECRET = "unit-telegram-api-hash-seed-plan"
RAW_PUBLIC_USERNAME = "@private_alpha_channel"
RAW_INVITE_LINK = "https://t.me/+privateInviteToken"
RAW_CHAT_ID = 9876543210123
RAW_UNSUPPORTED_SOURCE_KIND = "custom_private_source_kind_alpha"


class FakeResult:
    def __init__(
        self,
        *,
        scalar: Any = None,
        rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self._scalar = scalar
        self._rows = rows or []

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class FakeTransaction:
    def __init__(self) -> None:
        self.rolled_back = False

    def rollback(self) -> None:
        self.rolled_back = True


class FakeDatabaseConnection:
    def __init__(
        self,
        rows: list[dict[str, Any]],
        *,
        table_available: bool = True,
        fail_select_1: bool = False,
    ) -> None:
        self.rows = rows
        self.table_available = table_available
        self.fail_select_1 = fail_select_1
        self.statements: list[str] = []
        self.closed = False
        self.transaction = FakeTransaction()
        self.mutation_methods_called: list[str] = []

    def begin(self) -> FakeTransaction:
        return self.transaction

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> FakeResult:
        del params
        module = _module()
        normalized = _normalize(statement)
        self.statements.append(normalized)

        if "SOURCE_VALUE" in normalized.upper():
            raise AssertionError("source_value must not be selected by this script")

        if normalized == "SET TRANSACTION READ ONLY":
            return FakeResult()
        if normalized == "SHOW transaction_read_only":
            return FakeResult(scalar="on")
        if normalized == "SELECT 1":
            if self.fail_select_1:
                raise RuntimeError(f"cannot connect to {FAKE_DATABASE_URL}")
            return FakeResult(scalar=1)
        if normalized == _normalize(module.TABLE_AVAILABLE_QUERY):
            return FakeResult(scalar=self.table_available)
        if normalized == _normalize(module.REGISTRY_ROW_COUNT_QUERY):
            return FakeResult(scalar=len(self.rows))
        if normalized == _normalize(module.ACTIVE_JOINED_CHANNEL_COUNT_QUERY):
            return FakeResult(scalar=len([row for row in self.rows if _startup_eligible(row)]))
        if normalized == _normalize(module.CHAT_ID_PRESENT_COUNT_QUERY):
            return FakeResult(scalar=len([row for row in self.rows if row["chat_id"] is not None]))
        if normalized == _normalize(module.UNRESOLVED_OR_NOT_JOINED_COUNT_QUERY):
            return FakeResult(
                scalar=len(
                    [
                        row
                        for row in self.rows
                        if row["desired_state"] == "active"
                        and (row["access_state"] != "joined" or row["chat_id"] is None)
                    ]
                )
            )
        if normalized == _normalize(module.DESIRED_STATE_COUNT_QUERY):
            return FakeResult(
                rows=_counts(
                    self.rows,
                    "desired_state",
                    {"active", "paused", "removed"},
                )
            )
        if normalized == _normalize(module.ACCESS_STATE_COUNT_QUERY):
            return FakeResult(
                rows=_counts(
                    self.rows,
                    "access_state",
                    {
                        "unresolved",
                        "joined",
                        "join_requested",
                        "forbidden",
                        "not_found",
                        "left",
                    },
                )
            )
        if normalized == _normalize(module.SOURCE_KIND_COUNT_QUERY):
            return FakeResult(
                rows=_counts(
                    self.rows,
                    "source_kind",
                    {"public_username", "invite_link", "chat_id"},
                )
            )

        raise AssertionError(f"unexpected SQL: {statement}")

    def close(self) -> None:
        self.closed = True

    def insert(self, *_args: Any, **_kwargs: Any) -> None:
        self.mutation_methods_called.append("insert")

    def update(self, *_args: Any, **_kwargs: Any) -> None:
        self.mutation_methods_called.append("update")

    def delete(self, *_args: Any, **_kwargs: Any) -> None:
        self.mutation_methods_called.append("delete")


def _module():
    from scripts.ops import (
        dedicated_vps_telegram_channel_registry_seed_plan_readiness as module,
    )

    return module


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "TELEGRAM_API_HASH": FAKE_TELEGRAM_SECRET,
    }


def _startup_eligible(row: dict[str, Any]) -> bool:
    return (
        row["desired_state"] == "active"
        and row["access_state"] == "joined"
        and row["chat_id"] is not None
    )


def _counts(
    rows: list[dict[str, Any]],
    key: str,
    allowed: set[str],
) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for row in rows:
        value = str(row[key])
        counter[value if value in allowed else "unsupported"] += 1
    return sorted(counter.items())


def _seed_required_rows() -> list[dict[str, Any]]:
    return [
        {
            "source_kind": "public_username",
            "source_value": RAW_PUBLIC_USERNAME,
            "desired_state": "active",
            "access_state": "unresolved",
            "chat_id": None,
        },
        {
            "source_kind": "invite_link",
            "source_value": RAW_INVITE_LINK,
            "desired_state": "active",
            "access_state": "join_requested",
            "chat_id": None,
        },
        {
            "source_kind": RAW_UNSUPPORTED_SOURCE_KIND,
            "source_value": "opaque-private-source-value",
            "desired_state": "active",
            "access_state": "joined",
            "chat_id": None,
        },
        {
            "source_kind": "chat_id",
            "source_value": str(RAW_CHAT_ID),
            "desired_state": "paused",
            "access_state": "joined",
            "chat_id": RAW_CHAT_ID,
        },
    ]


def _run_with_rows(rows: list[dict[str, Any]]) -> tuple[dict[str, Any], FakeDatabaseConnection]:
    module = _module()
    db = FakeDatabaseConnection(rows)
    result = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        runtime_env_reader=_runtime_env,
        database_connection_factory=lambda _database_url: db,
    )
    return result.report, db


def _render(report: dict[str, Any]) -> str:
    return _module().render_json(report)


def test_no_active_joined_channels_requires_seed_plan() -> None:
    report, db = _run_with_rows(_seed_required_rows())

    assert report["contract_status"] == "seed_required_no_active_joined_channels"
    assert "channel_registry.no_active_joined_channels" in report["checks_failed"]
    assert report["runtime_env_read"] is True
    assert report["database_readiness_checked"] is True
    assert report["database_connected"] is True
    assert report["channel_registry_checked"] is True
    assert report["channel_registry_table_available"] is True
    assert report["active_joined_channels_present"] is False
    assert report["active_joined_channel_count_bucket"] == "zero"
    assert report["seed_plan_required"] is True
    assert db.closed is True
    assert db.transaction.rolled_back is True


def test_one_active_joined_channel_passes_seed_readiness() -> None:
    rows = [
        {
            "source_kind": "chat_id",
            "source_value": str(RAW_CHAT_ID),
            "desired_state": "active",
            "access_state": "joined",
            "chat_id": RAW_CHAT_ID,
        }
    ]

    report, _db = _run_with_rows(rows)

    assert report["contract_status"] == "channel_registry_seed_readiness_passed"
    assert report["checks_failed"] == []
    assert report["active_joined_channels_present"] is True
    assert report["active_joined_channel_count_bucket"] == "one"
    assert report["registry_row_count_bucket"] == "one"
    assert report["seed_plan_required"] is False


def test_seed_plan_input_contract_lists_accepted_shapes() -> None:
    report, _db = _run_with_rows(_seed_required_rows())

    source_kinds = {
        entry["source_kind"] for entry in report["seed_plan_input_contract"]
    }

    assert source_kinds == {"public_username", "invite_link", "chat_id"}
    for entry in report["seed_plan_input_contract"]:
        assert "source_value_description" in entry
        assert "source_value" in entry["required_fields"]


def test_output_does_not_leak_raw_source_values_or_chat_ids() -> None:
    report, _db = _run_with_rows(_seed_required_rows())
    rendered = _render(report)

    forbidden_fragments = (
        RAW_PUBLIC_USERNAME,
        RAW_INVITE_LINK,
        "opaque-private-source-value",
        str(RAW_CHAT_ID),
        FAKE_DATABASE_URL,
        FAKE_DATABASE_PASSWORD,
        FAKE_TELEGRAM_SECRET,
    )
    for fragment in forbidden_fragments:
        assert fragment not in rendered


def test_all_side_effect_flags_are_false() -> None:
    report, _db = _run_with_rows(_seed_required_rows())

    assert set(report["side_effects"]) == set(_module().SIDE_EFFECT_FLAG_NAMES)
    for value in report["side_effects"].values():
        assert value is False


def test_sql_path_is_read_only_and_blocks_mutation() -> None:
    module = _module()
    report, db = _run_with_rows(_seed_required_rows())

    assert report["contract_status"] == "seed_required_no_active_joined_channels"
    assert db.mutation_methods_called == []
    for statement in db.statements:
        upper_statement = statement.upper()
        for verb in module.FORBIDDEN_SQL_VERBS:
            assert verb not in upper_statement
        assert "SOURCE_VALUE" not in upper_statement
    with pytest.raises(ValueError):
        module._assert_read_only_sql(
            "UPDATE telegram_channel_registry SET desired_state = 'active'"
        )


def test_missing_runtime_env_and_database_fail_closed_without_secret_leaks() -> None:
    module = _module()

    unreadable = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        runtime_env_reader=lambda _path: (_ for _ in ()).throw(
            OSError(f"cannot read {FAKE_TELEGRAM_SECRET}")
        ),
        database_connection_factory=lambda _database_url: FakeDatabaseConnection([]),
    ).report
    rendered_unreadable = _render(unreadable)

    assert unreadable["contract_status"] == "blocked_runtime_env_unreadable"
    assert unreadable["runtime_env_read"] is False
    assert FAKE_TELEGRAM_SECRET not in rendered_unreadable

    missing_database = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        runtime_env_reader=lambda _path: {
            "TELEGRAM_API_HASH": FAKE_TELEGRAM_SECRET,
            "DATABASE_URL": "",
        },
        database_connection_factory=lambda _database_url: FakeDatabaseConnection([]),
    ).report
    rendered_missing_database = _render(missing_database)

    assert missing_database["contract_status"] == "blocked_database_unavailable"
    assert "database.url_missing" in missing_database["checks_failed"]
    assert FAKE_TELEGRAM_SECRET not in rendered_missing_database


def test_channel_registry_unavailable_blocks_without_seed_values() -> None:
    module = _module()
    db = FakeDatabaseConnection(_seed_required_rows(), table_available=False)

    report = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        runtime_env_reader=_runtime_env,
        database_connection_factory=lambda _database_url: db,
    ).report
    rendered = _render(report)

    assert report["contract_status"] == "blocked_channel_registry_unavailable"
    assert report["channel_registry_checked"] is True
    assert report["channel_registry_table_available"] is False
    assert RAW_PUBLIC_USERNAME not in rendered
    assert RAW_INVITE_LINK not in rendered


def test_coarse_buckets_are_used_instead_of_exact_counts() -> None:
    report, _db = _run_with_rows(_seed_required_rows())

    bucket_fields = (
        "active_joined_channel_count_bucket",
        "registry_row_count_bucket",
        "unresolved_or_not_joined_count_bucket",
        "chat_id_present_count_bucket",
    )
    for field in bucket_fields:
        assert report[field] in _module().BUCKET_LABELS
        assert isinstance(report[field], str)

    bucket_maps = (
        report["desired_state_count_buckets"],
        report["access_state_count_buckets"],
        report["source_kind_count_buckets"],
    )
    for bucket_map in bucket_maps:
        for value in bucket_map.values():
            assert value in _module().BUCKET_LABELS
            assert isinstance(value, str)


def test_unsupported_source_kind_is_only_reported_as_safe_bucket() -> None:
    report, _db = _run_with_rows(_seed_required_rows())
    rendered = _render(report)

    assert report["source_kind_count_buckets"]["unsupported"] == "one"
    assert "unsupported" in rendered
    assert RAW_UNSUPPORTED_SOURCE_KIND not in rendered


def test_cli_outputs_json_for_missing_runtime_env(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--format",
            "json",
            "--runtime-env-path",
            str(tmp_path / "missing-runtime.env"),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode != 0
    report = json.loads(result.stdout)
    assert report["script_name"] == _module().SCRIPT_NAME
    assert report["contract_status"] == "blocked_runtime_env_unreadable"


def test_script_static_contract_avoids_tdlib_and_collector_runtime_calls() -> None:
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    imported: list[str] = []
    called_attrs: list[str] = []
    called_names: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                called_attrs.append(node.func.attr)
            elif isinstance(node.func, ast.Name):
                called_names.append(node.func.id)

    forbidden_import_fragments = (
        "src.services.collector_telegram.runtime",
        "src.services.collector_telegram.service",
        "src.services.collector_telegram.main",
        "src.services.collector_telegram.tdlib_client",
        "src.services.collector_telegram.auth_entrypoint",
    )
    assert not [
        name
        for name in imported
        if any(fragment in name for fragment in forbidden_import_fragments)
    ]

    for forbidden_call in (
        "initialize",
        "send",
        "receive",
        "searchPublicChat",
        "joinChat",
        "joinChatByInviteLink",
        "getChatHistory",
    ):
        assert forbidden_call not in called_attrs
    for forbidden_name in (
        "CollectorTelegramService",
        "CollectorRuntime",
        "run_tdlib_auth_only_once",
        "TDLibAuthOnlyRunner",
    ):
        assert forbidden_name not in called_names
