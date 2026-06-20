from __future__ import annotations

import json

import pytest

from services.maintenance import main as maintenance_main


RAW_DATABASE_URL = "postgresql+psycopg://sentinel-db-user:sentinel-db-pass@sentinel-db-host/sentinel-db-name"
RAW_REDIS_URL = "redis://:sentinel-redis-token@sentinel-redis-host:6379/0"
TELEGRAM_TOKEN_KEY = "TELEGRAM_" + "BOT_" + "TOKEN"
OPENAI_KEY = "OPENAI_" + "API_" + "KEY"


def _clear_runtime_env(monkeypatch) -> None:
    for key in maintenance_main.ONE_SHOT_RUNTIME_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _assert_no_secret_leaks(output: str, *extra_values: object) -> None:
    forbidden = [
        RAW_DATABASE_URL,
        RAW_REDIS_URL,
        "sentinel-db-user",
        "sentinel-db-pass",
        "sentinel-db-host",
        "sentinel-db-name",
        "sentinel-redis-token",
        "sentinel-redis-host",
        "sentinel-telegram-secret-fragment",
        "sentinel-openai-secret-fragment",
        TELEGRAM_TOKEN_KEY,
        OPENAI_KEY,
        *[str(value) for value in extra_values],
    ]
    for value in forbidden:
        if value:
            assert value not in output


def test_parser_accepts_worker_startup_probe_execute_shape() -> None:
    args = maintenance_main.build_parser().parse_args(
        [
            "worker-startup-probe",
            "--mode",
            "execute",
            "--confirm",
            "run",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert args.command == "worker-startup-probe"
    assert args.mode == "execute"
    assert args.confirm == "run"
    assert args.env_file == "/tmp/runtime.env"
    assert maintenance_main._worker_startup_probe_request_error(args) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("confirm_args", [[], ["--confirm", "nope"]])
async def test_missing_or_invalid_confirmation_blocks_before_env_or_dependency_calls(
    monkeypatch,
    tmp_path,
    capsys,
    confirm_args: list[str],
) -> None:
    _clear_runtime_env(monkeypatch)
    env_file = tmp_path / "missing-runtime.env"

    def fail_overlay(*args, **kwargs):
        raise AssertionError("confirmation failure must block before env-file read")

    async def fail_probe_operation(config, args):
        raise AssertionError("confirmation failure must block before startup probe operation")

    monkeypatch.setattr(maintenance_main, "_resolve_one_shot_runtime_env_file_overlay", fail_overlay)
    monkeypatch.setattr(maintenance_main, "_run_worker_startup_probe_operation", fail_probe_operation)

    exit_code = await maintenance_main._run(
        [
            "worker-startup-probe",
            "--mode",
            "execute",
            "--env-file",
            str(env_file),
            *confirm_args,
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 2
    assert payload["schema_version"] == "maintenance_worker_startup_probe_report_v1"
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == "probe_request_not_confirmed"
    assert payload["config_loaded"] is False
    assert payload["worker_dependencies_constructed"] is False
    assert payload["broad_worker_run_started"] is False
    assert payload["redis_consume_attempted"] is False
    assert payload["redis_ack_attempted"] is False
    assert payload["redis_group_create_attempted"] is False
    assert payload["redis_write_attempted"] is False
    assert payload["db_write_attempted"] is False
    assert payload["systemd_attempted"] is False
    assert payload["docker_attempted"] is False
    assert payload["external_api_attempted"] is False
    _assert_no_secret_leaks(output, env_file)


@pytest.mark.asyncio
async def test_missing_env_file_returns_probe_schema_without_path_leak(monkeypatch, tmp_path, capsys) -> None:
    _clear_runtime_env(monkeypatch)
    missing_env_file = tmp_path / "missing-runtime.env"

    exit_code = await maintenance_main._run(
        [
            "worker-startup-probe",
            "--mode",
            "execute",
            "--confirm",
            "run",
            "--env-file",
            str(missing_env_file),
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 2
    assert payload["schema_version"] == "maintenance_worker_startup_probe_report_v1"
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == "env_file_missing"
    assert payload["config_loaded"] is False
    _assert_no_secret_leaks(output, missing_env_file)


@pytest.mark.asyncio
async def test_env_file_without_runtime_config_returns_probe_schema_without_secret_leak(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    _clear_runtime_env(monkeypatch)
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "# no accepted runtime config keys",
                f"{TELEGRAM_TOKEN_KEY}=sentinel-telegram-secret-fragment",
                f"{OPENAI_KEY}=sentinel-openai-secret-fragment",
                "NOT_A_RUNTIME_KEY=sentinel-db-host",
            ]
        ),
        encoding="utf-8",
    )

    exit_code = await maintenance_main._run(
        [
            "worker-startup-probe",
            "--mode",
            "execute",
            "--confirm",
            "run",
            "--env-file",
            str(env_file),
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 2
    assert payload["schema_version"] == "maintenance_worker_startup_probe_report_v1"
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == "env_file_no_runtime_config"
    assert payload["config_loaded"] is False
    _assert_no_secret_leaks(output, env_file)


@pytest.mark.asyncio
async def test_runtime_config_error_returns_probe_schema_without_raw_value_or_exception_leak(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    _clear_runtime_env(monkeypatch)
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                f"DATABASE_URL={RAW_DATABASE_URL}",
                f"REDIS_URL={RAW_REDIS_URL}",
                "MAINTENANCE_BATCH_SIZE=0",
            ]
        ),
        encoding="utf-8",
    )

    exit_code = await maintenance_main._run(
        [
            "worker-startup-probe",
            "--mode",
            "execute",
            "--confirm",
            "run",
            "--env-file",
            str(env_file),
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 1
    assert payload["schema_version"] == "maintenance_worker_startup_probe_report_v1"
    assert payload["status"] == "failed"
    assert payload["reason_code"] == "runtime_config_error"
    assert payload["config_loaded"] is False
    _assert_no_secret_leaks(output, env_file, "MAINTENANCE_BATCH_SIZE must be between 1 and 500")

