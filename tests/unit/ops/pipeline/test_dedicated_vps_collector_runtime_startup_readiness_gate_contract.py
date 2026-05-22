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
    / "dedicated_vps_collector_runtime_startup_readiness_gate.py"
)

FAKE_DB_PASSWORD = "unit-db-password-for-startup-gate"
FAKE_REDIS_PASSWORD = "unit-redis-password-for-startup-gate"
FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    f"{FAKE_DB_PASSWORD}@127.0.0.1:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = f"redis://:{FAKE_REDIS_PASSWORD}@127.0.0.1:6379/0"
FAKE_API_HASH = "0123456789abcdef0123456789abcdef"
FAKE_PHONE_NUMBER = "+15551234567"
FAKE_TDLIB_KEY = "unit-tdlib-encryption-key"
FAKE_LOGIN_CODE = "12345"
FAKE_2FA_PASSWORD = "unit two factor password"


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


class FakeDatabaseConnection:
    def __init__(
        self,
        *,
        active_joined_count: int = 1,
        fail_select_1: bool = False,
        fail_alembic: bool = False,
        fail_channel_registry: bool = False,
    ) -> None:
        self.active_joined_count = active_joined_count
        self.fail_select_1 = fail_select_1
        self.fail_alembic = fail_alembic
        self.fail_channel_registry = fail_channel_registry
        self.statements: list[str] = []
        self.closed = False
        self.mutation_methods_called: list[str] = []

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> FakeResult:
        del params
        normalized = " ".join(statement.strip().split())
        self.statements.append(normalized)

        if normalized == "SELECT 1":
            if self.fail_select_1:
                raise RuntimeError(f"db unavailable for {FAKE_DATABASE_URL}")
            return FakeResult(scalar=1)

        if normalized == "SELECT version_num FROM alembic_version":
            if self.fail_alembic:
                raise RuntimeError(f"alembic current failed for {FAKE_DATABASE_URL}")
            return FakeResult(rows=[("0004_judge_delivery_obs",)])

        if normalized == _module().ACTIVE_JOINED_CHANNEL_COUNT_QUERY:
            if self.fail_channel_registry:
                raise RuntimeError(f"registry unavailable for {FAKE_DATABASE_URL}")
            return FakeResult(scalar=self.active_joined_count)

        raise AssertionError(f"unexpected SQL: {statement}")

    def close(self) -> None:
        self.closed = True

    def insert(self, *_args: Any, **_kwargs: Any) -> None:
        self.mutation_methods_called.append("insert")

    def update(self, *_args: Any, **_kwargs: Any) -> None:
        self.mutation_methods_called.append("update")

    def delete(self, *_args: Any, **_kwargs: Any) -> None:
        self.mutation_methods_called.append("delete")


class FakeRedisClient:
    def __init__(self, *, fail_ping: bool = False) -> None:
        self.fail_ping = fail_ping
        self.ping_count = 0
        self.closed = False
        self.mutation_methods_called: list[str] = []

    def ping(self) -> bool:
        self.ping_count += 1
        if self.fail_ping:
            raise RuntimeError(f"redis unavailable for {FAKE_REDIS_URL}")
        return True

    def close(self) -> None:
        self.closed = True

    def set(self, *_args: Any, **_kwargs: Any) -> None:
        self.mutation_methods_called.append("set")

    def delete(self, *_args: Any, **_kwargs: Any) -> None:
        self.mutation_methods_called.append("delete")

    def xadd(self, *_args: Any, **_kwargs: Any) -> None:
        self.mutation_methods_called.append("xadd")

    def xread(self, *_args: Any, **_kwargs: Any) -> None:
        self.mutation_methods_called.append("xread")

    def xgroup_create(self, *_args: Any, **_kwargs: Any) -> None:
        self.mutation_methods_called.append("xgroup_create")


def _module():
    from scripts.ops import dedicated_vps_collector_runtime_startup_readiness_gate as module

    return module


def _noop_tdjson(_repo_root: Path, _values: dict[str, str]) -> None:
    return None


def _write_runtime_env(
    tmp_path: Path,
    *,
    create_state_dir: bool = True,
    create_files_dir: bool = True,
    state_entries: int = 1,
    singleton_lock_path: Path | None = None,
    **overrides: str | None,
) -> Path:
    state_dir = tmp_path / "tdlib-state"
    files_dir = tmp_path / "tdlib-files"
    if create_state_dir:
        state_dir.mkdir()
        for index in range(state_entries):
            (state_dir / f"entry-{index}.bin").write_text("fixture", encoding="utf-8")
    if create_files_dir:
        files_dir.mkdir()

    values: dict[str, str] = {
        "APP_ENV": "prod",
        "COLLECTOR_MODE": "live",
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "TELEGRAM_API_ID": "123456",
        "TELEGRAM_API_HASH": FAKE_API_HASH,
        "TELEGRAM_PHONE_NUMBER": FAKE_PHONE_NUMBER,
        "TELEGRAM_2FA_PASSWORD": FAKE_2FA_PASSWORD,
        "TELEGRAM_LOGIN_CODE": FAKE_LOGIN_CODE,
        "TDLIB_DB_ENCRYPTION_KEY": FAKE_TDLIB_KEY,
        "TDLIB_STATE_DIR": str(state_dir),
        "TDLIB_FILES_DIR": str(files_dir),
    }
    if singleton_lock_path is not None:
        values["COLLECTOR_SINGLETON_LOCK_PATH"] = str(singleton_lock_path)
    for key, value in overrides.items():
        if value is None:
            values.pop(key, None)
        else:
            values[key] = value

    runtime_env = tmp_path / "runtime.env"
    runtime_env.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()),
        encoding="utf-8",
    )
    return runtime_env


def _render(report: dict[str, Any]) -> str:
    return _module().render_json(report)


def _run_gate(
    tmp_path: Path,
    *,
    db: FakeDatabaseConnection | None = None,
    redis_client: FakeRedisClient | None = None,
    runtime_env: Path | None = None,
    **runtime_overrides: str | None,
):
    module = _module()
    runtime_env = runtime_env or _write_runtime_env(tmp_path, **runtime_overrides)
    db = db or FakeDatabaseConnection()
    redis_client = redis_client or FakeRedisClient()
    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        tdjson_availability_checker=_noop_tdjson,
        database_connection_factory=lambda _database_url: db,
        redis_client_factory=lambda _redis_url: redis_client,
    )
    return result, db, redis_client


def test_default_missing_runtime_env_fails_closed_without_service_calls(tmp_path: Path) -> None:
    module = _module()
    database_factory_calls: list[str] = []
    redis_factory_calls: list[str] = []

    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=tmp_path / "missing-runtime.env",
        tdjson_availability_checker=_noop_tdjson,
        database_connection_factory=lambda database_url: database_factory_calls.append(database_url),  # type: ignore[arg-type]
        redis_client_factory=lambda redis_url: redis_factory_calls.append(redis_url),  # type: ignore[arg-type]
    )
    report = result.report

    assert result.exit_code != 0
    assert report["report_type"] == module.REPORT_TYPE
    assert report["contract_status"] == "blocked_runtime_env_unreadable"
    assert report["runtime_env_read"] is False
    assert "runtime_env.unreadable" in report["checks_failed"]
    assert database_factory_calls == []
    assert redis_factory_calls == []
    for flag in module.SIDE_EFFECT_FLAGS:
        assert report[flag] is False


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
    assert report["contract_status"] == "blocked_runtime_env_unreadable"
    assert report["runtime_env_values_printed"] is False
    assert report["secret_values_printed"] is False


def test_invalid_collector_config_fails_closed_before_db_or_redis(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _write_runtime_env(tmp_path, COLLECTOR_MODE="replay")
    database_factory_calls: list[str] = []
    redis_factory_calls: list[str] = []

    report = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        tdjson_availability_checker=_noop_tdjson,
        database_connection_factory=lambda database_url: database_factory_calls.append(database_url),  # type: ignore[arg-type]
        redis_client_factory=lambda redis_url: redis_factory_calls.append(redis_url),  # type: ignore[arg-type]
    ).report
    rendered = _render(report)

    assert report["contract_status"] == "blocked_collector_config_invalid"
    assert report["collector_config_built"] is False
    assert database_factory_calls == []
    assert redis_factory_calls == []
    assert FAKE_DATABASE_URL not in rendered
    assert FAKE_API_HASH not in rendered
    assert FAKE_PHONE_NUMBER not in rendered


def test_tdjson_unavailable_fails_closed_before_db_or_redis(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _write_runtime_env(tmp_path)
    database_factory_calls: list[str] = []
    redis_factory_calls: list[str] = []

    def unavailable(_repo_root: Path, _values: dict[str, str]) -> None:
        raise OSError("tdjson unavailable")

    report = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        tdjson_availability_checker=unavailable,
        database_connection_factory=lambda database_url: database_factory_calls.append(database_url),  # type: ignore[arg-type]
        redis_client_factory=lambda redis_url: redis_factory_calls.append(redis_url),  # type: ignore[arg-type]
    ).report

    assert report["contract_status"] == "blocked_tdjson_unavailable"
    assert report["collector_config_built"] is True
    assert report["tdjson_available"] is False
    assert report["database_readiness_checked"] is False
    assert report["redis_readiness_checked"] is False
    assert database_factory_calls == []
    assert redis_factory_calls == []


def test_tdlib_session_reuse_preflight_failure_blocks_before_db_or_redis(
    tmp_path: Path,
) -> None:
    module = _module()
    runtime_env = _write_runtime_env(tmp_path, create_state_dir=False)
    database_factory_calls: list[str] = []
    redis_factory_calls: list[str] = []

    report = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        tdjson_availability_checker=_noop_tdjson,
        database_connection_factory=lambda database_url: database_factory_calls.append(database_url),  # type: ignore[arg-type]
        redis_client_factory=lambda redis_url: redis_factory_calls.append(redis_url),  # type: ignore[arg-type]
    ).report

    assert report["contract_status"] == "blocked_tdlib_session_reuse_preflight_failed"
    assert report["tdlib_session_reuse_preflight_passed"] is False
    assert database_factory_calls == []
    assert redis_factory_calls == []


def test_database_unavailable_fails_closed_without_printing_database_url(
    tmp_path: Path,
) -> None:
    module = _module()
    runtime_env = _write_runtime_env(tmp_path)

    def unavailable(_database_url: str) -> FakeDatabaseConnection:
        raise RuntimeError(f"cannot connect to {FAKE_DATABASE_URL}")

    report = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        tdjson_availability_checker=_noop_tdjson,
        database_connection_factory=unavailable,
        redis_client_factory=lambda _redis_url: FakeRedisClient(),
    ).report
    rendered = _render(report)

    assert report["contract_status"] == "blocked_database_unavailable"
    assert report["database_readiness_checked"] is True
    assert report["database_connected"] is False
    assert FAKE_DATABASE_URL not in rendered
    assert FAKE_DB_PASSWORD not in rendered


def test_alembic_unavailable_fails_closed_without_migration_execution(
    tmp_path: Path,
) -> None:
    db = FakeDatabaseConnection(fail_alembic=True)
    result, db, redis_client = _run_gate(tmp_path, db=db)
    report = result.report
    rendered = _render(report)

    assert report["contract_status"] == "blocked_alembic_current_unavailable"
    assert report["database_connected"] is True
    assert report["alembic_current_checked"] is True
    assert report["alembic_current_available"] is False
    assert report["alembic_upgrade_run"] is False
    assert report["alembic_stamp_run"] is False
    assert redis_client.ping_count == 0
    assert db.closed is True
    assert FAKE_DATABASE_URL not in rendered
    assert FAKE_DB_PASSWORD not in rendered


def test_redis_unavailable_fails_closed_without_printing_redis_url(
    tmp_path: Path,
) -> None:
    redis_client = FakeRedisClient(fail_ping=True)
    result, db, redis_client = _run_gate(tmp_path, redis_client=redis_client)
    report = result.report
    rendered = _render(report)

    assert report["contract_status"] == "blocked_redis_unavailable"
    assert report["redis_readiness_checked"] is True
    assert report["redis_connected"] is False
    assert report["channel_registry_checked"] is False
    assert db.closed is True
    assert redis_client.closed is True
    assert FAKE_REDIS_URL not in rendered
    assert FAKE_REDIS_PASSWORD not in rendered


def test_no_active_joined_channel_blocks_startup_gate(tmp_path: Path) -> None:
    db = FakeDatabaseConnection(active_joined_count=0)
    result, db, redis_client = _run_gate(tmp_path, db=db)
    report = result.report

    assert report["contract_status"] == "blocked_no_active_joined_channels"
    assert report["channel_registry_checked"] is True
    assert report["active_joined_channel_count_bucket"] == "zero"
    assert report["active_joined_channels_present"] is False
    assert redis_client.ping_count == 1
    assert db.closed is True


def test_singleton_lock_parent_unavailable_blocks_after_readiness_checks(
    tmp_path: Path,
) -> None:
    runtime_env = _write_runtime_env(
        tmp_path,
        singleton_lock_path=tmp_path / "missing-lock-parent" / "collector.lock",
    )
    result, db, redis_client = _run_gate(tmp_path, runtime_env=runtime_env)
    report = result.report

    assert report["contract_status"] == "blocked_singleton_lock_path_unavailable"
    assert report["singleton_lock_path_checked"] is True
    assert report["singleton_lock_path_parent_exists"] is False
    assert report["singleton_lock_file_created"] is False
    assert redis_client.ping_count == 1
    assert db.closed is True


def test_valid_runtime_db_redis_channel_and_session_preflight_pass(
    tmp_path: Path,
) -> None:
    result, db, redis_client = _run_gate(tmp_path)
    report = result.report

    assert result.exit_code == 0
    assert report["contract_status"] == "collector_runtime_startup_readiness_gate_passed"
    assert report["runtime_env_read"] is True
    assert report["collector_config_built"] is True
    assert report["tdjson_available"] is True
    assert report["tdlib_session_reuse_preflight_passed"] is True
    assert report["database_readiness_checked"] is True
    assert report["database_connected"] is True
    assert report["alembic_current_checked"] is True
    assert report["alembic_current_available"] is True
    assert report["redis_readiness_checked"] is True
    assert report["redis_connected"] is True
    assert report["channel_registry_checked"] is True
    assert report["active_joined_channel_count_bucket"] == "one_to_five"
    assert report["active_joined_channels_present"] is True
    assert report["singleton_lock_path_checked"] is True
    assert report["singleton_lock_path_parent_exists"] is True
    assert report["singleton_lock_path_parent_is_dir"] is True
    assert report["singleton_lock_path_parent_writable"] is True
    assert db.statements == [
        "SELECT 1",
        "SELECT version_num FROM alembic_version",
        _module().ACTIVE_JOINED_CHANNEL_COUNT_QUERY,
    ]
    assert redis_client.ping_count == 1
    assert db.closed is True
    assert redis_client.closed is True


def test_report_excludes_runtime_db_redis_and_tdlib_secret_values(
    tmp_path: Path,
) -> None:
    report = _run_gate(tmp_path)[0].report
    rendered = _render(report)

    forbidden_fragments = (
        FAKE_DATABASE_URL,
        FAKE_DB_PASSWORD,
        FAKE_REDIS_URL,
        FAKE_REDIS_PASSWORD,
        FAKE_API_HASH,
        FAKE_PHONE_NUMBER,
        FAKE_LOGIN_CODE,
        FAKE_2FA_PASSWORD,
        FAKE_TDLIB_KEY,
    )
    for fragment in forbidden_fragments:
        assert fragment not in rendered
    assert report["runtime_env_values_printed"] is False
    assert report["secret_values_printed"] is False
    assert report["database_values_printed"] is False
    assert report["redis_values_printed"] is False


def test_no_db_or_redis_mutation_methods_are_called(tmp_path: Path) -> None:
    result, db, redis_client = _run_gate(tmp_path)
    report = result.report

    assert report["contract_status"] == "collector_runtime_startup_readiness_gate_passed"
    assert db.mutation_methods_called == []
    assert redis_client.mutation_methods_called == []
    for statement in db.statements:
        upper_statement = statement.upper()
        for verb in _module().FORBIDDEN_SQL_VERBS:
            assert verb not in upper_statement


def test_side_effect_flags_remain_false_on_pass(tmp_path: Path) -> None:
    module = _module()
    report = _run_gate(tmp_path)[0].report

    for flag in module.SIDE_EFFECT_FLAGS:
        assert report[flag] is False


def test_script_static_contract_avoids_tdlib_auth_and_collector_startup_calls() -> None:
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
        "src.services.collector_telegram.auth_entrypoint",
    )
    assert not [
        name
        for name in imported
        if any(fragment in name for fragment in forbidden_import_fragments)
    ]

    for forbidden_call in ("initialize", "send", "receive"):
        assert forbidden_call not in called_attrs
    for forbidden_name in (
        "CollectorTelegramService",
        "CollectorRuntime",
        "run_tdlib_auth_only_once",
        "TDLibAuthOnlyRunner",
    ):
        assert forbidden_name not in called_names
