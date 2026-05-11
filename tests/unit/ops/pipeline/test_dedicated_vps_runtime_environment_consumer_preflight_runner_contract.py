from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_runtime_environment_consumer_preflight_runner.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "dedicated_vps_runtime_environment_consumer_preflight.md"
FAKE_DB_PASSWORD = "fake-db-password-consumer-preflight"
FAKE_REDIS_PASSWORD = "fake-redis-password-consumer-preflight"
FAKE_DATABASE_URL = (
    "postgresql+psycopg://github_ai_catchbot_app:"
    f"{FAKE_DB_PASSWORD}@localhost:5432/github_ai_catchbot"
)
FAKE_REDIS_URL = f"redis://:{FAKE_REDIS_PASSWORD}@localhost:6379/0"


def _module():
    from scripts.ops import dedicated_vps_runtime_environment_consumer_preflight_runner as module

    return module


def _runtime_env(tmp_path: Path, **overrides: str | None) -> Path:
    values: dict[str, str] = {
        "APP_ENV": "prod",
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "ENABLE_NOTIFICATION_SEND": "false",
        "NOTIFIER_TELEGRAM_DRY_RUN": "false",
        "NOTIFIER_TELEGRAM_ALLOW_EDITS": "true",
        "ENABLE_REPLAY_TO_PROD_DB": "false",
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": "false",
        "LOG_LEVEL": "203.0.113.77",
        "OPENAI_API_KEY": "fake-openai-secret",
        "TELEGRAM_BOT_TOKEN": "fake-telegram-secret",
    }
    for key, value in overrides.items():
        if value is None:
            values.pop(key, None)
        else:
            values[key] = value

    path = tmp_path / "runtime.env"
    path.write_text(
        "\n".join(["# fixture only", *(f"{key}={value}" for key, value in values.items())]),
        encoding="utf-8",
    )
    return path


def _approved_report(tmp_path: Path, **overrides: str | None) -> dict[str, object]:
    module = _module()
    runtime_env = _runtime_env(tmp_path, **overrides)
    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        approved_runtime_env_consumer_preflight=True,
    )
    return result.report


def test_runner_imports() -> None:
    module = _module()

    assert module.REPORT_TYPE == "dedicated_vps_runtime_environment_consumer_preflight_result_v1"
    assert callable(module.main)


def test_no_approval_json_is_parseable_and_does_not_read_runtime_env(tmp_path: Path) -> None:
    missing_runtime_env = tmp_path / "missing-runtime.env"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--format",
            "json",
            "--runtime-env-path",
            str(missing_runtime_env),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode != 0
    report = json.loads(result.stdout)
    assert report["contract_status"] == "approval_required"
    assert report["runtime_env_read"] is False
    assert "approval.required" in report["checks_failed"]


def test_no_approval_path_does_not_expose_unread_fixture_values(tmp_path: Path) -> None:
    runtime_env = _runtime_env(tmp_path)

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json", "--runtime-env-path", str(runtime_env)],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode != 0
    assert FAKE_DATABASE_URL not in result.stdout
    assert FAKE_DB_PASSWORD not in result.stdout
    assert FAKE_REDIS_URL not in result.stdout
    assert FAKE_REDIS_PASSWORD not in result.stdout
    assert "fake-openai-secret" not in result.stdout
    assert json.loads(result.stdout)["runtime_env_read"] is False


def test_no_approval_side_effect_flags_are_false(tmp_path: Path) -> None:
    report = _module().generate_report(runtime_env_path=tmp_path / "missing-runtime.env").report

    for key in (
        "runtime_env_read",
        "runtime_env_values_printed",
        "database_url_printed",
        "redis_url_printed",
        "secret_values_printed",
        "process_env_inspected",
        "database_connected",
        "redis_connected",
        "db_write_performed",
        "redis_mutation_performed",
        "alembic_run",
        "app_runtime_started",
        "tdlib_auth_performed",
        "telegram_connected",
        "live_collector_started",
        "notifier_transport_enabled",
        "production_rollout_performed",
        "docker_used",
        "systemd_modified",
        "migration_files_modified",
    ):
        assert report[key] is False


def test_approved_fake_fixture_passes_from_temp_file_only_and_redacts_values(tmp_path: Path) -> None:
    module = _module()
    runtime_env = _runtime_env(tmp_path)

    result = module.generate_report(
        repo_root=ROOT,
        runtime_env_path=runtime_env,
        approved_runtime_env_consumer_preflight=True,
    )

    report = result.report
    rendered = module.render_json(report)
    assert result.exit_code == 0
    assert report["contract_status"] == "passed"
    assert report["runtime_env_read"] is True
    assert report["runtime_env_path"] == str(runtime_env)
    assert FAKE_DATABASE_URL not in rendered
    assert FAKE_DB_PASSWORD not in rendered
    assert FAKE_REDIS_URL not in rendered
    assert FAKE_REDIS_PASSWORD not in rendered
    assert "fake-openai-secret" not in rendered
    assert "fake-telegram-secret" not in rendered
    assert "203.0.113.77" not in rendered
    assert report["runtime_env_values_printed"] is False
    assert report["secret_values_printed"] is False
    assert report["optional_sensitive_keys_present"] == ["OPENAI_API_KEY", "TELEGRAM_BOT_TOKEN"]
    assert report["warnings"]


def test_required_keys_pass_when_present(tmp_path: Path) -> None:
    report = _approved_report(tmp_path)

    assert report["required_keys_missing"] == []
    assert set(report["required_keys_present"]) == set(_module().REQUIRED_KEYS)


def test_required_keys_fail_when_missing(tmp_path: Path) -> None:
    report = _approved_report(tmp_path, DATABASE_URL=None)

    assert report["contract_status"] == "failed"
    assert "DATABASE_URL" in report["required_keys_missing"]
    assert "runtime_env.required_keys" in report["checks_failed"]
    assert "database_url.present" in report["checks_failed"]


def test_app_env_must_be_prod(tmp_path: Path) -> None:
    report = _approved_report(tmp_path, APP_ENV="dev")

    assert report["contract_status"] == "failed"
    assert "safety_profile.app_env" in report["checks_failed"]
    assert report["app_env_seen"] == "dev"


def test_enable_notification_send_must_be_false(tmp_path: Path) -> None:
    report = _approved_report(tmp_path, ENABLE_NOTIFICATION_SEND="true")

    assert "safety_profile.enable_notification_send" in report["checks_failed"]
    assert report["feature_flags"]["ENABLE_NOTIFICATION_SEND"] is True


def test_missing_enable_notification_send_fails_safety_profile(tmp_path: Path) -> None:
    report = _approved_report(tmp_path, ENABLE_NOTIFICATION_SEND=None)

    assert report["contract_status"] == "failed"
    assert "runtime_env.required_keys" in report["checks_failed"]
    assert "safety_profile.enable_notification_send.parse" in report["checks_failed"]
    assert report["safety_profile_passed"] is False


def test_notifier_telegram_dry_run_must_be_false_for_prod_pre_runtime(tmp_path: Path) -> None:
    report = _approved_report(tmp_path, NOTIFIER_TELEGRAM_DRY_RUN="true")

    assert "safety_profile.notifier_telegram_dry_run" in report["checks_failed"]
    assert report["feature_flags"]["NOTIFIER_TELEGRAM_DRY_RUN"] is True


def test_notifier_telegram_allow_edits_must_be_true_for_prod_pre_runtime(tmp_path: Path) -> None:
    report = _approved_report(tmp_path, NOTIFIER_TELEGRAM_ALLOW_EDITS="false")

    assert "safety_profile.notifier_telegram_allow_edits" in report["checks_failed"]
    assert report["feature_flags"]["NOTIFIER_TELEGRAM_ALLOW_EDITS"] is False


def test_replay_to_prod_must_be_false(tmp_path: Path) -> None:
    report = _approved_report(tmp_path, ENABLE_REPLAY_TO_PROD_DB="true")

    assert "safety_profile.enable_replay_to_prod_db" in report["checks_failed"]
    assert report["feature_flags"]["ENABLE_REPLAY_TO_PROD_DB"] is True


def test_missing_enable_replay_to_prod_db_fails_safety_profile(tmp_path: Path) -> None:
    report = _approved_report(tmp_path, ENABLE_REPLAY_TO_PROD_DB=None)

    assert report["contract_status"] == "failed"
    assert "runtime_env.required_keys" in report["checks_failed"]
    assert "safety_profile.enable_replay_to_prod_db.parse" in report["checks_failed"]
    assert report["safety_profile_passed"] is False


def test_maintenance_retry_promotion_must_be_false(tmp_path: Path) -> None:
    report = _approved_report(tmp_path, MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION="true")

    assert "safety_profile.maintenance_enable_notification_retry_promotion" in report["checks_failed"]
    assert report["feature_flags"]["MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION"] is True


def test_database_url_shape_parser_validates_expected_shape_without_password_output() -> None:
    module = _module()
    shape = module.database_url_shape(
        f"postgresql+psycopg://github_ai_catchbot_app:{FAKE_DB_PASSWORD}@127.0.0.1:5432/github_ai_catchbot"
    )

    assert shape == {
        "present": True,
        "scheme": "postgresql+psycopg",
        "has_credentials": True,
        "username": "github_ai_catchbot_app",
        "host": "127.0.0.1",
        "port": 5432,
        "database": "github_ai_catchbot",
        "loopback_only": True,
    }
    assert FAKE_DB_PASSWORD not in json.dumps(shape)


def test_database_url_shape_failures_are_reported(tmp_path: Path) -> None:
    report = _approved_report(
        tmp_path,
        DATABASE_URL=f"postgresql://wrong:{FAKE_DB_PASSWORD}@203.0.113.10:15432/wrongdb",
    )

    assert "database_url.scheme" in report["checks_failed"]
    assert "database_url.username" in report["checks_failed"]
    assert "database_url.port" in report["checks_failed"]
    assert "database_url.database" in report["checks_failed"]
    assert "database_url.loopback_only" in report["checks_failed"]
    assert "203.0.113.10" not in json.dumps(report)
    assert report["database_url_shape"]["host"] == "<non_loopback_redacted>"


def test_redis_url_shape_parser_validates_expected_shape_without_value_output() -> None:
    module = _module()
    shape = module.redis_url_shape(f"redis://:{FAKE_REDIS_PASSWORD}@127.0.0.1:6379/2")

    assert shape == {
        "present": True,
        "scheme": "redis",
        "host": "127.0.0.1",
        "port": 6379,
        "database_index": 2,
        "loopback_only": True,
    }
    assert FAKE_REDIS_PASSWORD not in json.dumps(shape)


def test_redis_url_shape_failures_are_reported(tmp_path: Path) -> None:
    report = _approved_report(tmp_path, REDIS_URL=f"rediss://:{FAKE_REDIS_PASSWORD}@203.0.113.11:6380/0")

    assert "redis_url.scheme" in report["checks_failed"]
    assert "redis_url.loopback_only" in report["checks_failed"]
    assert "redis_url.port" in report["checks_failed"]
    assert "203.0.113.11" not in json.dumps(report)
    assert report["redis_url_shape"]["host"] == "<non_loopback_redacted>"


def test_consumer_summary_and_side_effect_flags_remain_safe_in_approved_fake_path(tmp_path: Path) -> None:
    report = _approved_report(tmp_path)
    summary = report["consumer_profile_summary"]

    assert summary["database_consumers_ready"] is True
    assert summary["redis_consumers_ready"] is True
    assert summary["notification_transport_disabled"] is True
    assert summary["replay_to_prod_disabled"] is True
    assert summary["maintenance_retry_promotion_disabled"] is True
    assert summary["runtime_start_authorized"] is False
    assert summary["tdlib_authorized"] is False
    assert summary["telegram_authorized"] is False
    assert summary["live_collector_authorized"] is False
    assert summary["notifier_transport_authorized"] is False
    assert summary["production_rollout_authorized"] is False
    assert report["safety_profile"] == "prod_pre_runtime"
    assert report["safety_profile_passed"] is True

    for key in (
        "database_connected",
        "redis_connected",
        "db_write_performed",
        "redis_mutation_performed",
        "alembic_run",
        "app_runtime_started",
        "tdlib_auth_performed",
        "telegram_connected",
        "live_collector_started",
        "notifier_transport_enabled",
        "production_rollout_performed",
    ):
        assert report[key] is False


def test_runner_static_ast_safety_contract() -> None:
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    forbidden_import_roots = {
        "subprocess",
        "redis",
        "httpx",
        "requests",
        "urllib",
        "socket",
        "dotenv",
        "os",
    }
    imported_roots: set[str] = set()
    forbidden_write_calls: set[str] = set()
    open_write_modes: list[str] = []
    os_environ_references: list[str] = []
    shell_execution_calls: set[str] = set()
    connection_calls: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        if isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id == "os" and node.attr == "environ":
                os_environ_references.append("os.environ")
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr in {
                "write_text",
                "write_bytes",
                "touch",
                "unlink",
                "mkdir",
                "rename",
                "replace",
            }:
                forbidden_write_calls.add(func.attr)
            if isinstance(func, ast.Name) and func.id == "open" and len(node.args) >= 2:
                mode_arg = node.args[1]
                if isinstance(mode_arg, ast.Constant) and isinstance(mode_arg.value, str):
                    if any(flag in mode_arg.value for flag in ("w", "a", "+")):
                        open_write_modes.append(mode_arg.value)
            if isinstance(func, ast.Attribute) and func.attr in {"run", "Popen", "call", "check_call", "check_output"}:
                shell_execution_calls.add(func.attr)
            if isinstance(func, ast.Attribute) and func.attr in {"connect", "create_engine", "from_url"}:
                connection_calls.add(func.attr)
            if isinstance(func, ast.Name) and func.id in {"Redis", "StrictRedis"}:
                connection_calls.add(func.id)

    assert imported_roots.isdisjoint(forbidden_import_roots)
    assert os_environ_references == []
    assert forbidden_write_calls == set()
    assert open_write_modes == []
    assert shell_execution_calls == set()
    assert connection_calls == set()


def test_runbook_contains_future_only_approval_flag_and_non_authorization_language() -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")

    for phrase in (
        "Purpose",
        "Scope",
        "Source-of-truth handling",
        "DB/Redis provisioning passed",
        "runtime secret placement passed",
        "Alembic upgrade passed",
        "post-migration DB acceptance smoke passed",
        "does not start app runtime",
        "does not authorize TDLib, Telegram, live collector, notifier transport, or production rollout",
        "--approved-runtime-env-consumer-preflight",
        "Without approval, the runner reads no runtime.env and connects nowhere",
        "reads runtime.env inside Python only and prints redacted JSON only",
        "validates key presence and value shapes only",
        "validates safety flag posture for the prod pre-runtime baseline",
        "does not connect DB/Redis",
        "does not run Alembic",
        "does not mutate runtime.env",
        "does not print `DATABASE_URL`, `REDIS_URL`, DB password, Redis credentials, secret values, raw server IP, or raw operator IP",
        "does not inspect process env vars",
        "does not use `cat`, `source`, dot-source, or `export`",
        "python scripts/ops/dedicated_vps_runtime_environment_consumer_preflight_runner.py",
        "If preflight fails, stop and bring the redacted JSON back to ChatGPT.",
        "Passing this preflight does not authorize runtime start.",
        "separately reviewed app/runtime import/config preflight or TDLib auth package",
    ):
        assert phrase in runbook
