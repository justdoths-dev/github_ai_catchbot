from __future__ import annotations

import json
import os

import pytest

from services.maintenance import main as maintenance_main


RAW_DATABASE_URL = "opaque-database-source-sentinel-db-user-sentinel-db-pass-sentinel-db-host-sentinel-db-name"
RAW_REDIS_URL = "opaque-redis-source-sentinel-redis-token-sentinel-redis-host"
PROCESS_DATABASE_URL = "opaque-process-database-source-process-db-user-process-db-host-process-db-name"
PROCESS_REDIS_URL = "opaque-process-redis-source-process-redis-token-process-redis-host"
TELEGRAM_TOKEN_KEY = "TELEGRAM_" + "BOT_" + "TOKEN"
OPENAI_KEY = "OPENAI_" + "API_" + "KEY"


def _clear_runtime_env(monkeypatch) -> None:
    for key in maintenance_main.ONE_SHOT_RUNTIME_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _runtime_env_text(*, database_url: str = RAW_DATABASE_URL, redis_url: str = RAW_REDIS_URL) -> str:
    return "\n".join(
        [
            "# runtime config for one-shot maintenance command",
            f'DATABASE_URL="{database_url}"',
            f"REDIS_URL='{redis_url}'",
            "APP_ENV=test",
            "ENABLE_NOTIFICATION_SEND=true",
            "NOTIFIER_TELEGRAM_DRY_RUN=false",
            "NOTIFIER_TELEGRAM_ALLOW_EDITS=false",
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=true",
            "ENABLE_REPLAY_TO_PROD_DB=false",
            "MAINTENANCE_BATCH_SIZE=5",
            "MAINTENANCE_BLOCK_MS=25",
            "MAINTENANCE_NOTIFICATION_RETRY_POLL_SEC=3",
            "DELIVERY_RETRY_MAX_ATTEMPTS=2",
            "DELIVERY_GATE_MIN_SUCCESS_RATE_1H=0.75",
            "DELIVERY_GATE_MIN_SUCCESS_RATE_24H=0.70",
            "DELIVERY_GATE_MAX_HIGH_SOURCE_TO_DELIVERY_P95_SEC=90",
            "DELIVERY_GATE_MAX_PLAN_TO_TRANSPORT_P95_SEC=80",
            "DELIVERY_GATE_MAX_DUE_RETRY_LAG_SEC=70",
            "DELIVERY_GATE_MAX_OPEN_DLQ_COUNT=1",
            "DELIVERY_GATE_MAX_SEND_DISABLED_COUNT=2",
            "DELIVERY_GATE_MAX_REPLAY_GUARD_REJECT_COUNT=3",
            "DELIVERY_GATE_REQUIRE_OPERATOR_REVIEW_FOR_FULL=false",
            "LOG_LEVEL=warning",
            f"{TELEGRAM_TOKEN_KEY}=sentinel-telegram-secret-fragment",
            f"{OPENAI_KEY}=sentinel-openai-secret-fragment",
        ]
    )


def _assert_runtime_config_failure(output: str, reason_code: str) -> None:
    payload = json.loads(output)
    assert payload == {
        "schema_version": "maintenance_one_shot_runtime_config_v1",
        "status": "fail",
        "reason_code": reason_code,
        "warnings": [reason_code],
    }


def _assert_no_secret_leaks(output: str, *extra_values: object) -> None:
    forbidden = [
        RAW_DATABASE_URL,
        RAW_REDIS_URL,
        PROCESS_DATABASE_URL,
        PROCESS_REDIS_URL,
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


def _fail_from_env(cls):
    raise AssertionError("source failure must not load MaintenanceConfig.from_env")


@pytest.mark.asyncio
async def test_delivery_gate_env_file_indirection_loads_config_and_restores_environment(
    monkeypatch,
    tmp_path,
) -> None:
    _clear_runtime_env(monkeypatch)
    database_url_file = tmp_path / "database-url.secret"
    redis_url_file = tmp_path / "redis-url.secret"
    env_file = tmp_path / "runtime.env"
    database_url_file.write_text(f" {RAW_DATABASE_URL}\n", encoding="utf-8")
    redis_url_file.write_text(f"{RAW_REDIS_URL}\n", encoding="utf-8")
    env_file.write_text(
        "\n".join(
            [
                f'DATABASE_URL_FILE="{database_url_file}"',
                f"REDIS_URL_FILE='{redis_url_file}'",
                "APP_ENV=test",
                "ENABLE_NOTIFICATION_SEND=true",
                "NOTIFIER_TELEGRAM_DRY_RUN=false",
                "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=true",
                "ENABLE_REPLAY_TO_PROD_DB=false",
            ]
        ),
        encoding="utf-8",
    )
    calls: list[str] = []

    async def fake_delivery_gate(config, args):
        calls.append(args.env_file)
        assert config.database_url == RAW_DATABASE_URL
        assert config.redis_url == RAW_REDIS_URL
        assert config.app_env == "test"
        assert config.enable_notification_send is True
        assert config.notifier_telegram_dry_run is False
        assert config.enable_delivery_retry_promotion is True
        assert config.enable_replay_to_prod_db is False
        return 0

    monkeypatch.setattr(maintenance_main, "_run_delivery_gate", fake_delivery_gate)

    exit_code = await maintenance_main._run(
        [
            "delivery-gate",
            "--mode",
            "restricted",
            "--format",
            "json",
            "--env-file",
            str(env_file),
        ]
    )

    assert exit_code == 0
    assert calls == [str(env_file)]
    assert "DATABASE_URL" not in os.environ
    assert "REDIS_URL" not in os.environ


@pytest.mark.asyncio
async def test_mvp_readiness_env_file_respects_process_env_precedence(monkeypatch, tmp_path) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("DATABASE_URL", PROCESS_DATABASE_URL)
    monkeypatch.setenv("REDIS_URL", PROCESS_REDIS_URL)
    env_file = tmp_path / "runtime.env"
    env_file.write_text(_runtime_env_text(), encoding="utf-8")
    calls: list[str] = []

    async def fake_mvp_readiness(config, args):
        calls.append(args.env_file)
        assert config.database_url == PROCESS_DATABASE_URL
        assert config.redis_url == PROCESS_REDIS_URL
        assert config.app_env == "test"
        assert config.enable_notification_send is True
        assert config.log_level == "WARNING"
        return 3

    monkeypatch.setattr(maintenance_main, "_run_mvp_readiness", fake_mvp_readiness)

    exit_code = await maintenance_main._run(
        [
            "mvp-readiness",
            "--mode",
            "restricted",
            "--format",
            "json",
            "--env-file",
            str(env_file),
        ]
    )

    assert exit_code == 3
    assert calls == [str(env_file)]
    assert os.environ["DATABASE_URL"] == PROCESS_DATABASE_URL
    assert os.environ["REDIS_URL"] == PROCESS_REDIS_URL
    assert "APP_ENV" not in os.environ


@pytest.mark.asyncio
async def test_missing_env_file_returns_sanitized_failure_without_config_load(monkeypatch, tmp_path, capsys) -> None:
    _clear_runtime_env(monkeypatch)
    missing_env_file = tmp_path / "missing-runtime.env"
    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(_fail_from_env))

    exit_code = await maintenance_main._run(
        [
            "mvp-readiness",
            "--mode",
            "restricted",
            "--format",
            "json",
            "--env-file",
            str(missing_env_file),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    _assert_runtime_config_failure(output, "env_file_missing")
    _assert_no_secret_leaks(output, missing_env_file)


@pytest.mark.asyncio
async def test_env_file_without_runtime_keys_returns_sanitized_failure(monkeypatch, tmp_path, capsys) -> None:
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
    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(_fail_from_env))

    exit_code = await maintenance_main._run(
        [
            "delivery-gate",
            "--mode",
            "restricted",
            "--format",
            "json",
            "--env-file",
            str(env_file),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    _assert_runtime_config_failure(output, "env_file_no_runtime_config")
    _assert_no_secret_leaks(output, env_file)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("file_key", "inline_key", "inline_value", "reason_code", "write_pointed_file"),
    [
        (
            "DATABASE_URL_FILE",
            "REDIS_URL",
            RAW_REDIS_URL,
            "env_file_database_url_file_missing",
            False,
        ),
        (
            "DATABASE_URL_FILE",
            "REDIS_URL",
            RAW_REDIS_URL,
            "env_file_database_url_file_empty",
            True,
        ),
        (
            "REDIS_URL_FILE",
            "DATABASE_URL",
            RAW_DATABASE_URL,
            "env_file_redis_url_file_missing",
            False,
        ),
        (
            "REDIS_URL_FILE",
            "DATABASE_URL",
            RAW_DATABASE_URL,
            "env_file_redis_url_file_empty",
            True,
        ),
    ],
)
async def test_missing_or_empty_pointed_runtime_secret_file_returns_sanitized_failure(
    monkeypatch,
    tmp_path,
    capsys,
    file_key: str,
    inline_key: str,
    inline_value: str,
    reason_code: str,
    write_pointed_file: bool,
) -> None:
    _clear_runtime_env(monkeypatch)
    env_file = tmp_path / "runtime.env"
    pointed_file = tmp_path / f"{file_key.lower()}.secret"
    if write_pointed_file:
        pointed_file.write_text(" \n", encoding="utf-8")
    env_file.write_text(
        "\n".join(
            [
                f"{file_key}={pointed_file}",
                f"{inline_key}={inline_value}",
                "APP_ENV=test",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(_fail_from_env))

    exit_code = await maintenance_main._run(
        [
            "delivery-gate",
            "--mode",
            "restricted",
            "--format",
            "json",
            "--env-file",
            str(env_file),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    _assert_runtime_config_failure(output, reason_code)
    _assert_no_secret_leaks(output, env_file, pointed_file)


@pytest.mark.asyncio
async def test_env_file_config_validation_error_is_sanitized_and_restores_environment(
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

    async def fail_delivery_gate(config, args):
        raise AssertionError("invalid runtime config must not run delivery gate")

    monkeypatch.setattr(maintenance_main, "_run_delivery_gate", fail_delivery_gate)

    exit_code = await maintenance_main._run(
        [
            "delivery-gate",
            "--mode",
            "restricted",
            "--format",
            "json",
            "--env-file",
            str(env_file),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    _assert_runtime_config_failure(output, "maintenance_runtime_config_error")
    _assert_no_secret_leaks(output, env_file)
    assert "DATABASE_URL" not in os.environ
    assert "REDIS_URL" not in os.environ


@pytest.mark.asyncio
async def test_env_file_malformed_numeric_config_error_is_sanitized_and_restores_environment(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    _clear_runtime_env(monkeypatch)
    malformed_value = "not-an-int"
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                f"DATABASE_URL={RAW_DATABASE_URL}",
                f"REDIS_URL={RAW_REDIS_URL}",
                f"MAINTENANCE_BATCH_SIZE={malformed_value}",
            ]
        ),
        encoding="utf-8",
    )
    calls: list[str] = []

    def fail_from_env(cls):
        assert os.environ["DATABASE_URL"] == RAW_DATABASE_URL
        assert os.environ["REDIS_URL"] == RAW_REDIS_URL
        assert os.environ["MAINTENANCE_BATCH_SIZE"] == malformed_value
        raise ValueError(f"malformed numeric config: {malformed_value}")

    async def fail_delivery_gate(config, args):
        calls.append("delivery-gate")
        raise AssertionError("malformed runtime config must not run delivery gate")

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(fail_from_env))
    monkeypatch.setattr(maintenance_main, "_run_delivery_gate", fail_delivery_gate)

    exit_code = await maintenance_main._run(
        [
            "delivery-gate",
            "--mode",
            "restricted",
            "--format",
            "json",
            "--env-file",
            str(env_file),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    _assert_runtime_config_failure(output, "maintenance_runtime_config_error")
    _assert_no_secret_leaks(output, env_file, malformed_value, "malformed numeric config")
    assert "DATABASE_URL" not in os.environ
    assert "REDIS_URL" not in os.environ
    assert calls == []


@pytest.mark.asyncio
async def test_replay_selected_without_confirmation_keeps_existing_no_config_guard(monkeypatch, tmp_path, capsys) -> None:
    _clear_runtime_env(monkeypatch)
    env_file = tmp_path / "runtime.env"
    env_file.write_text(_runtime_env_text(), encoding="utf-8")
    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(_fail_from_env))

    exit_code = await maintenance_main._run(
        [
            "batch-recovery",
            "replay-selected",
            "--plan-id",
            "00000000-0000-0000-0000-000000000001",
            "--requested-by",
            "test/operator",
            "--env-file",
            str(env_file),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "batch_recovery_operator_confirmation_required"


@pytest.mark.asyncio
async def test_replay_selected_missing_env_file_fails_before_no_confirm_guard(monkeypatch, tmp_path, capsys) -> None:
    _clear_runtime_env(monkeypatch)
    missing_env_file = tmp_path / "missing-runtime.env"
    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(_fail_from_env))

    exit_code = await maintenance_main._run(
        [
            "batch-recovery",
            "replay-selected",
            "--plan-id",
            "00000000-0000-0000-0000-000000000001",
            "--requested-by",
            "test/operator",
            "--env-file",
            str(missing_env_file),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    _assert_runtime_config_failure(output, "env_file_missing")
    _assert_no_secret_leaks(output, missing_env_file)


@pytest.mark.asyncio
async def test_retry_selected_due_invalid_id_keeps_existing_no_config_guard(monkeypatch, tmp_path, capsys) -> None:
    _clear_runtime_env(monkeypatch)
    env_file = tmp_path / "runtime.env"
    env_file.write_text(_runtime_env_text(), encoding="utf-8")
    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(_fail_from_env))

    exit_code = await maintenance_main._run(
        [
            "batch-recovery",
            "retry-selected-due",
            "--plan-id",
            "not-a-uuid",
            "--requested-by",
            "test/operator",
            "--confirm",
            "write",
            "--env-file",
            str(env_file),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "invalid_notification_plan_id"
    assert payload["emitted_count"] == 0
