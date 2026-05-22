from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_telegram_channel_registry_seed_operator.py"
)

FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    "unit-db-password-for-seed-operator@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_DATABASE_PASSWORD = "unit-db-password-for-seed-operator"
FAKE_TELEGRAM_SECRET = "unit-telegram-api-hash-seed-operator"
RAW_PUBLIC_USERNAME = "PrivateAlphaChannel"
RAW_INVITE_LINK = "https://t.me/+privateInviteToken"
RAW_CHAT_ID = "9876543210123"


class FakeResult:
    def __init__(
        self,
        *,
        scalar: Any = None,
        rows: list[tuple[Any, ...]] | None = None,
        rowcount: int = 0,
    ) -> None:
        self._scalar = scalar
        self._rows = rows or []
        self.rowcount = rowcount

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class FakeTransaction:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


class FakeDatabaseConnection:
    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        table_available: bool = True,
        fail_select_1: bool = False,
    ) -> None:
        self.rows = rows or []
        self.table_available = table_available
        self.fail_select_1 = fail_select_1
        self.statements: list[str] = []
        self.closed = False
        self.transaction = FakeTransaction()
        self.insert_attempts = 0

    def begin(self) -> FakeTransaction:
        return self.transaction

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(statement)
        self.statements.append(normalized)
        module = _module()

        if normalized == _normalize(module.SELECT_ONE_QUERY):
            if self.fail_select_1:
                raise RuntimeError(f"cannot connect to {FAKE_DATABASE_URL}")
            return FakeResult(scalar=1)

        if normalized == _normalize(module.TABLE_AVAILABLE_QUERY):
            return FakeResult(scalar=self.table_available)

        if normalized == _normalize(module.EXISTING_REGISTRY_ROW_QUERY):
            return FakeResult(
                scalar=1
                if any(
                    row["desired_state"] != "removed"
                    and row["source_kind"] == params["source_kind"]
                    and row["source_value"] == params["source_value"]
                    for row in self.rows
                )
                else None
            )

        if normalized == _normalize(module.INSERT_PUBLIC_USERNAME_QUERY):
            self.insert_attempts += 1
            if any(
                row["desired_state"] != "removed"
                and row["source_kind"] == params["source_kind"]
                and row["source_value"] == params["source_value"]
                for row in self.rows
            ):
                return FakeResult(scalar=None, rowcount=0)

            self.rows.append(
                {
                    "source_kind": params["source_kind"],
                    "source_value": params["source_value"],
                    "desired_state": params["desired_state"],
                    "access_state": "unresolved",
                    "chat_id": None,
                    "username_snapshot": None,
                    "title_snapshot": None,
                    "chat_type": None,
                    "priority_weight": params["priority_weight"],
                    "notes": params["notes"],
                }
            )
            return FakeResult(
                scalar=f"registry-{len(self.rows)}",
                rowcount=1,
            )

        raise AssertionError(f"unexpected SQL: {statement}")

    def close(self) -> None:
        self.closed = True


def _module():
    from scripts.ops import (
        dedicated_vps_telegram_channel_registry_seed_operator as module,
    )

    return module


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "TELEGRAM_API_HASH": FAKE_TELEGRAM_SECRET,
    }


def _write_jsonl(tmp_path: Path, rows: list[dict[str, Any]]) -> Path:
    path = tmp_path / "seed-input.jsonl"
    path.write_text(
        "".join(f"{json.dumps(row, sort_keys=True)}\n" for row in rows),
        encoding="utf-8",
    )
    return path


def _run_report(
    *,
    input_path: Path,
    db: FakeDatabaseConnection | None = None,
    dry_run: bool = True,
    approved: bool = False,
    runtime_env_reader=_runtime_env,
) -> tuple[dict[str, Any], FakeDatabaseConnection]:
    module = _module()
    fake_db = db or FakeDatabaseConnection()
    result = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        input_jsonl_path=input_path,
        dry_run=dry_run,
        approved_registry_seed_mutation=approved,
        runtime_env_reader=runtime_env_reader,
        database_connection_factory=lambda _database_url: fake_db,
    )
    return result.report, fake_db


def _render(report: dict[str, Any]) -> str:
    return _module().render_json(report)


def _public_username_row(value: str = RAW_PUBLIC_USERNAME) -> dict[str, Any]:
    return {
        "source_kind": "public_username",
        "source_value": value,
        "desired_state": "active",
        "priority_weight": 100,
        "notes": "seeded_by_operator",
    }


def _preexisting_registry_row(value: str = RAW_PUBLIC_USERNAME) -> dict[str, Any]:
    return {
        "source_kind": "public_username",
        "source_value": value,
        "desired_state": "active",
        "access_state": "unresolved",
        "chat_id": None,
        "username_snapshot": None,
        "title_snapshot": None,
        "chat_type": None,
        "priority_weight": 100,
        "notes": "existing",
    }


def test_dry_run_validates_public_username_jsonl_and_performs_no_mutation(
    tmp_path: Path,
) -> None:
    input_path = _write_jsonl(tmp_path, [_public_username_row()])

    report, db = _run_report(input_path=input_path)

    assert report["contract_status"] == "dry_run_seed_plan_validated"
    assert report["runtime_env_read"] is True
    assert report["database_connected"] is True
    assert report["input_file_read"] is True
    assert report["input_rows_validated"] is True
    assert report["dry_run"] is True
    assert report["approved_registry_seed_mutation"] is False
    assert report["accepted_source_kind_buckets"]["public_username"] == "one"
    assert report["inserted_row_count_bucket"] == "zero"
    assert db.rows == []
    assert db.insert_attempts == 0
    assert db.transaction.rolled_back is True
    assert db.transaction.committed is False
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["side_effects"]["telegram_channel_registry_inserted"] is False


def test_approved_mode_inserts_public_username_unresolved_with_null_chat_id(
    tmp_path: Path,
) -> None:
    input_path = _write_jsonl(tmp_path, [_public_username_row()])

    report, db = _run_report(input_path=input_path, dry_run=False, approved=True)

    assert report["contract_status"] == "registry_seed_inserted"
    assert report["dry_run"] is False
    assert report["seed_mutation_performed"] is True
    assert report["inserted_row_count_bucket"] == "one"
    assert len(db.rows) == 1
    row = db.rows[0]
    assert row["source_kind"] == "public_username"
    assert row["source_value"] == RAW_PUBLIC_USERNAME
    assert row["desired_state"] == "active"
    assert row["access_state"] == "unresolved"
    assert row["chat_id"] is None
    assert row["username_snapshot"] is None
    assert row["title_snapshot"] is None
    assert row["chat_type"] is None
    assert db.transaction.committed is True
    assert db.transaction.rolled_back is False
    assert report["side_effects"]["database_mutation_performed"] is True
    assert report["side_effects"]["telegram_channel_registry_inserted"] is True


def test_duplicate_input_rows_are_handled_safely(tmp_path: Path) -> None:
    input_path = _write_jsonl(
        tmp_path,
        [
            _public_username_row("DuplicateSeedChannel"),
            _public_username_row("DuplicateSeedChannel"),
        ],
    )

    report, db = _run_report(input_path=input_path, dry_run=False, approved=True)

    assert report["contract_status"] == "registry_seed_inserted"
    assert report["duplicate_input_count_bucket"] == "one"
    assert report["inserted_row_count_bucket"] == "one"
    assert len(db.rows) == 1
    assert db.insert_attempts == 1


def test_existing_active_rows_are_skipped_idempotently(tmp_path: Path) -> None:
    input_path = _write_jsonl(tmp_path, [_public_username_row()])
    db = FakeDatabaseConnection([_preexisting_registry_row()])

    report, db = _run_report(
        input_path=input_path,
        db=db,
        dry_run=False,
        approved=True,
    )

    assert report["contract_status"] == "registry_seed_noop_all_existing"
    assert report["existing_row_count_bucket"] == "one"
    assert report["skipped_existing_count_bucket"] == "one"
    assert report["inserted_row_count_bucket"] == "zero"
    assert len(db.rows) == 1
    assert db.insert_attempts == 0
    assert report["side_effects"]["database_mutation_performed"] is False
    assert report["side_effects"]["telegram_channel_registry_inserted"] is False


def test_raw_source_values_do_not_appear_in_output_json(tmp_path: Path) -> None:
    input_path = _write_jsonl(tmp_path, [_public_username_row()])

    report, _db = _run_report(input_path=input_path)
    rendered = _render(report)

    forbidden_fragments = (
        RAW_PUBLIC_USERNAME,
        RAW_INVITE_LINK,
        RAW_CHAT_ID,
        FAKE_DATABASE_URL,
        FAKE_DATABASE_PASSWORD,
        FAKE_TELEGRAM_SECRET,
    )
    for fragment in forbidden_fragments:
        assert fragment not in rendered


def test_t_me_url_normalization_preserves_username_case(tmp_path: Path) -> None:
    input_path = _write_jsonl(
        tmp_path,
        [_public_username_row("https://t.me/CasePreserved_Channel")],
    )

    report, db = _run_report(input_path=input_path, dry_run=False, approved=True)

    assert report["contract_status"] == "registry_seed_inserted"
    assert db.rows[0]["source_value"] == "CasePreserved_Channel"


def test_invalid_rows_fail_closed_with_row_numbers_only(tmp_path: Path) -> None:
    input_path = _write_jsonl(
        tmp_path,
        [
            {
                "source_kind": "public_username",
                "source_value": "DATABASE_URL=postgresql://secret-value",
            },
            {
                "source_kind": "invite_link",
                "source_value": RAW_INVITE_LINK,
            },
            {
                "source_kind": "chat_id",
                "source_value": RAW_CHAT_ID,
            },
        ],
    )

    report, _db = _run_report(input_path=input_path)
    rendered = _render(report)

    assert report["contract_status"] == "blocked_input_validation_failed"
    assert report["input_rows_validated"] is False
    assert report["rejected_row_count_bucket"] == "two_to_five"
    assert sorted(report["checks_failed"]) == [
        "input.row_1.source_value_suspicious",
        "input.row_2.source_kind_not_supported",
        "input.row_3.source_kind_not_supported",
    ]
    assert "DATABASE_URL=postgresql://secret-value" not in rendered
    assert RAW_INVITE_LINK not in rendered
    assert RAW_CHAT_ID not in rendered


def test_invalid_cli_output_has_no_raw_values_or_stderr(tmp_path: Path) -> None:
    runtime_env = tmp_path / "runtime.env"
    runtime_env.write_text(
        f"DATABASE_URL={FAKE_DATABASE_URL}\nTELEGRAM_API_HASH={FAKE_TELEGRAM_SECRET}\n",
        encoding="utf-8",
    )
    input_path = _write_jsonl(
        tmp_path,
        [
            {
                "source_kind": "public_username",
                "source_value": "https://t.me/private/path",
            }
        ],
    )

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--format",
            "json",
            "--runtime-env-path",
            str(runtime_env),
            "--input-jsonl",
            str(input_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode != 0
    report = json.loads(result.stdout)
    assert report["contract_status"] == "blocked_input_validation_failed"
    assert result.stderr == ""
    assert "private/path" not in result.stdout
    assert FAKE_DATABASE_URL not in result.stdout
    assert FAKE_TELEGRAM_SECRET not in result.stdout


def test_no_update_delete_or_non_registry_insert_sql_is_used(tmp_path: Path) -> None:
    input_path = _write_jsonl(tmp_path, [_public_username_row()])

    report, db = _run_report(input_path=input_path, dry_run=False, approved=True)

    assert report["contract_status"] == "registry_seed_inserted"
    for statement in db.statements:
        upper_statement = statement.upper()
        assert " UPDATE " not in f" {upper_statement} "
        assert " DELETE " not in f" {upper_statement} "
        assert " TRUNCATE " not in f" {upper_statement} "
        if upper_statement.startswith("INSERT "):
            assert "INSERT INTO TELEGRAM_CHANNEL_REGISTRY" in upper_statement
            assert "EVENT_OUTBOX" not in upper_statement
            assert "SOURCE_MESSAGES" not in upper_statement
            assert "SOURCE_MESSAGE_VERSIONS" not in upper_statement


def test_no_external_runtime_or_transport_side_effects_are_possible(tmp_path: Path) -> None:
    input_path = _write_jsonl(tmp_path, [_public_username_row()])
    report, _db = _run_report(input_path=input_path)
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
        "redis",
        "telegram",
        "tdlib",
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
        "publish",
        "ping",
        "upgrade",
        "downgrade",
        "stamp",
    ):
        assert forbidden_call not in called_attrs

    for forbidden_name in (
        "CollectorTelegramService",
        "CollectorRuntime",
        "run_tdlib_auth_only_once",
        "TDLibAuthOnlyRunner",
    ):
        assert forbidden_name not in called_names

    for side_effect_name, value in report["side_effects"].items():
        assert value is False, side_effect_name


def test_all_side_effect_fields_are_present_and_correct(
    tmp_path: Path,
) -> None:
    input_path = _write_jsonl(tmp_path, [_public_username_row()])
    module = _module()

    dry_run_report, _db = _run_report(input_path=input_path)
    approved_report, _db = _run_report(
        input_path=input_path,
        dry_run=False,
        approved=True,
    )

    assert set(dry_run_report["side_effects"]) == set(module.SIDE_EFFECT_FLAG_NAMES)
    assert set(approved_report["side_effects"]) == set(module.SIDE_EFFECT_FLAG_NAMES)

    for value in dry_run_report["side_effects"].values():
        assert value is False

    assert approved_report["side_effects"]["database_mutation_performed"] is True
    assert approved_report["side_effects"]["telegram_channel_registry_inserted"] is True
    for key, value in approved_report["side_effects"].items():
        if key not in {
            "database_mutation_performed",
            "telegram_channel_registry_inserted",
        }:
            assert value is False, key


def test_missing_runtime_env_or_db_access_fails_closed_without_leaks(
    tmp_path: Path,
) -> None:
    input_path = _write_jsonl(tmp_path, [_public_username_row()])
    module = _module()

    unreadable_runtime = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        input_jsonl_path=input_path,
        runtime_env_reader=lambda _path: (_ for _ in ()).throw(
            OSError(f"cannot read {FAKE_TELEGRAM_SECRET}")
        ),
        database_connection_factory=lambda _database_url: FakeDatabaseConnection(),
    ).report
    rendered_unreadable = _render(unreadable_runtime)

    assert unreadable_runtime["contract_status"] == "blocked_runtime_env_unreadable"
    assert unreadable_runtime["runtime_env_read"] is False
    assert FAKE_TELEGRAM_SECRET not in rendered_unreadable
    assert RAW_PUBLIC_USERNAME not in rendered_unreadable

    db = FakeDatabaseConnection(fail_select_1=True)
    database_unavailable = module.generate_report(
        runtime_env_path="/safe/unit/runtime.env",
        input_jsonl_path=input_path,
        runtime_env_reader=_runtime_env,
        database_connection_factory=lambda _database_url: db,
    ).report
    rendered_database_unavailable = _render(database_unavailable)

    assert database_unavailable["contract_status"] == "blocked_database_unavailable"
    assert "database.connection" in database_unavailable["checks_failed"]
    assert FAKE_DATABASE_URL not in rendered_database_unavailable
    assert FAKE_DATABASE_PASSWORD not in rendered_database_unavailable
    assert RAW_PUBLIC_USERNAME not in rendered_database_unavailable


def test_input_file_unreadable_fails_closed_without_raw_value_leak(tmp_path: Path) -> None:
    raw_value_in_path = tmp_path / RAW_PUBLIC_USERNAME / "missing.jsonl"

    report, _db = _run_report(input_path=raw_value_in_path)
    rendered = _render(report)

    assert report["contract_status"] == "blocked_input_file_unreadable"
    assert report["input_file_read"] is False
    assert RAW_PUBLIC_USERNAME not in rendered
