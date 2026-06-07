from __future__ import annotations

import argparse
import json
import os
from uuid import UUID

import pytest

from services.notifier_telegram import main as notifier_main
from services.notifier_telegram.models import NotificationPlanDraft

from ._fakes import FakeRepository, config, repo_with_valid_case

RAW_DATABASE_URL = "postgresql+psycopg://sentinel-db-user:sentinel-db-pass@db.example/app"
RAW_REDIS_URL = "redis://:sentinel-redis-pass@redis.example:6379/0"
RAW_TELEGRAM_TOKEN = "123456:sentinel-telegram-token"
PROCESS_DATABASE_URL = "postgresql+psycopg://process-db-user:process-db-pass@db.example/app"
PROCESS_REDIS_URL = "redis://:process-redis-pass@redis.example:6379/0"


def _send_canary_argv(*extra: str) -> list[str]:
    return [
        "send-canary",
        "--notification-plan-id",
        "00000000-0000-0000-0000-000000000001",
        *extra,
    ]


def _clear_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in notifier_main.ONE_SHOT_RUNTIME_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _fail_from_env(cls, **kwargs):
    raise AssertionError("early canary rejection must not load NotifierTelegramConfig.from_env")


def _assert_no_secret_leaks(output: str, *extra_values: object) -> None:
    forbidden = [RAW_DATABASE_URL, RAW_REDIS_URL, RAW_TELEGRAM_TOKEN, PROCESS_DATABASE_URL, PROCESS_REDIS_URL]
    forbidden.extend(str(value) for value in extra_values)
    for value in forbidden:
        assert value not in output


def _runtime_env_text() -> str:
    return "\n".join(
        [
            f"DATABASE_URL={RAW_DATABASE_URL}",
            f"REDIS_URL={RAW_REDIS_URL}",
            f"TELEGRAM_BOT_TOKEN={RAW_TELEGRAM_TOKEN}",
            "APP_ENV=test",
            "ENABLE_NOTIFICATION_SEND=true",
            "NOTIFIER_TELEGRAM_DRY_RUN=false",
            "LOG_LEVEL=WARNING",
        ]
    )


def _store_existing_plan(repository: FakeRepository, intent, *, status: str = "planned", send_after=None) -> None:
    repository.plans[intent.notification_plan_id] = NotificationPlanDraft(
        notification_plan_id=intent.notification_plan_id,
        analysis_id=intent.analysis_id,
        candidate_group_id=intent.candidate_group_id,
        delivery_decision=intent.delivery_decision,
        urgency_profile=intent.urgency_profile,
        target_chat_id=intent.target_chat_id,
        target_thread_id=intent.target_thread_id,
        render_profile=intent.render_profile,
        dedupe_subject_key=intent.dedupe_subject_key,
        material_change_hash=intent.material_change_hash,
        send_after=send_after,
        suppress_reason_code=intent.suppress_reason_code,
        status=status,
    )


class FailingTelegramClientBuilder:
    def __call__(self, **kwargs):
        raise AssertionError("blocked canary must not create Telegram transport")


class RecordingTelegramClient:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.sent: list[dict] = []

    async def send_message(self, **kwargs):
        self.sent.append(kwargs)
        return {
            "ok": True,
            "result": {
                "message_id": 9876,
                "chat": {"id": kwargs["chat_id"]},
            },
        }

    async def edit_message_text(self, **kwargs):
        raise AssertionError("send canary must not edit by default")


@pytest.mark.asyncio
async def test_missing_operator_confirmation_rejects_before_config_db_client_transport(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setattr(notifier_main.NotifierTelegramConfig, "from_env", classmethod(_fail_from_env))
    monkeypatch.setattr(notifier_main, "_build_send_canary_session_factory", lambda database_url: (_ for _ in ()).throw(AssertionError))

    exit_code = await notifier_main._run(_send_canary_argv("--env-file", "/tmp/must-not-be-read.env"))
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload == {
        "schema_version": "notifier_one_shot_canary_v1",
        "status": "rejected",
        "reason_code": "operator_confirmation_required",
    }


@pytest.mark.asyncio
async def test_invalid_notification_plan_id_rejects_before_config_db_client_transport(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setattr(notifier_main.NotifierTelegramConfig, "from_env", classmethod(_fail_from_env))
    monkeypatch.setattr(notifier_main, "_build_send_canary_session_factory", lambda database_url: (_ for _ in ()).throw(AssertionError))

    exit_code = await notifier_main._run(
        [
            "send-canary",
            "--notification-plan-id",
            "not-a-uuid",
            "--operator-confirmed",
            "--env-file",
            "/tmp/must-not-be-read.env",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "invalid_notification_plan_id"


@pytest.mark.asyncio
async def test_missing_env_file_returns_sanitized_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _clear_runtime_env(monkeypatch)
    missing_env_file = tmp_path / "missing-runtime.env"
    monkeypatch.setattr(notifier_main, "_run_send_canary", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError))

    exit_code = await notifier_main._run(
        _send_canary_argv("--operator-confirmed", "--env-file", str(missing_env_file))
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 1
    assert payload["status"] == "fail"
    assert payload["reason_code"] == "env_file_missing"
    assert payload["warnings"] == ["env_file_missing"]
    _assert_no_secret_leaks(output, missing_env_file)


@pytest.mark.asyncio
async def test_env_file_secret_files_resolve_without_leaking_values_and_restore_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _clear_runtime_env(monkeypatch)
    database_url_file = tmp_path / "database-url.secret"
    redis_url_file = tmp_path / "redis-url.secret"
    telegram_token_file = tmp_path / "telegram-token.secret"
    env_file = tmp_path / "runtime.env"
    database_url_file.write_text(f" {RAW_DATABASE_URL}\n", encoding="utf-8")
    redis_url_file.write_text(f"{RAW_REDIS_URL}\n", encoding="utf-8")
    telegram_token_file.write_text(f"{RAW_TELEGRAM_TOKEN}\n", encoding="utf-8")
    env_file.write_text(
        "\n".join(
            [
                f'DATABASE_URL_FILE="{database_url_file}"',
                f"REDIS_URL_FILE='{redis_url_file}'",
                f"TELEGRAM_BOT_TOKEN_FILE={telegram_token_file}",
                "APP_ENV=test",
                "ENABLE_NOTIFICATION_SEND=true",
                "NOTIFIER_TELEGRAM_DRY_RUN=false",
            ]
        ),
        encoding="utf-8",
    )

    async def fake_run(config, args, notification_plan_id, *, emit_json=print, **kwargs):
        assert config.database_url == RAW_DATABASE_URL
        assert config.redis_url == RAW_REDIS_URL
        assert config.telegram_bot_token == RAW_TELEGRAM_TOKEN
        assert config.enable_notification_send is True
        assert config.dry_run is False
        emit_json(notifier_main._to_json({"schema_version": notifier_main.CANARY_SCHEMA_VERSION, "status": "sent"}))
        return 0

    monkeypatch.setattr(notifier_main, "_run_send_canary", fake_run)

    exit_code = await notifier_main._run(
        _send_canary_argv("--operator-confirmed", "--env-file", str(env_file))
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "DATABASE_URL" not in os.environ
    assert "REDIS_URL" not in os.environ
    assert "TELEGRAM_BOT_TOKEN" not in os.environ
    _assert_no_secret_leaks(output, env_file, database_url_file, redis_url_file, telegram_token_file)


@pytest.mark.asyncio
async def test_process_env_precedence_wins_over_env_file_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("DATABASE_URL", PROCESS_DATABASE_URL)
    monkeypatch.setenv("REDIS_URL", PROCESS_REDIS_URL)
    env_file = tmp_path / "runtime.env"
    env_file.write_text(_runtime_env_text(), encoding="utf-8")

    async def fake_run(config, args, notification_plan_id, *, emit_json=print, **kwargs):
        assert config.database_url == PROCESS_DATABASE_URL
        assert config.redis_url == PROCESS_REDIS_URL
        assert config.telegram_bot_token == RAW_TELEGRAM_TOKEN
        assert config.enable_notification_send is True
        assert config.log_level == "WARNING"
        emit_json(notifier_main._to_json({"schema_version": notifier_main.CANARY_SCHEMA_VERSION, "status": "sent"}))
        return 0

    monkeypatch.setattr(notifier_main, "_run_send_canary", fake_run)

    exit_code = await notifier_main._run(
        _send_canary_argv("--operator-confirmed", "--env-file", str(env_file))
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert os.environ["DATABASE_URL"] == PROCESS_DATABASE_URL
    assert os.environ["REDIS_URL"] == PROCESS_REDIS_URL
    assert "TELEGRAM_BOT_TOKEN" not in os.environ
    _assert_no_secret_leaks(output, env_file)


@pytest.mark.asyncio
async def test_enable_notification_send_false_blocks_before_session_or_transport(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = await notifier_main._run_send_canary(
        config(dry_run=False, enable_notification_send=False),
        argparse.Namespace(),
        UUID("00000000-0000-0000-0000-000000000001"),
        session_factory_builder=lambda database_url: (_ for _ in ()).throw(AssertionError("must not create DB session")),
        telegram_client_builder=FailingTelegramClientBuilder(),
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "notification_send_disabled"


@pytest.mark.asyncio
async def test_notifier_dry_run_true_blocks_before_session_or_transport(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = await notifier_main._run_send_canary(
        config(dry_run=True, enable_notification_send=True),
        argparse.Namespace(),
        UUID("00000000-0000-0000-0000-000000000001"),
        session_factory_builder=lambda database_url: (_ for _ in ()).throw(AssertionError("must not create DB session")),
        telegram_client_builder=FailingTelegramClientBuilder(),
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "notifier_dry_run_enabled"


@pytest.mark.asyncio
async def test_already_delivered_plan_rejected_before_telegram_client(capsys: pytest.CaptureFixture[str]) -> None:
    repository, intent = repo_with_valid_case()
    _store_existing_plan(repository, intent, status="sent")

    exit_code = await notifier_main.run_send_canary_with_repository(
        config(dry_run=False, enable_notification_send=True),
        intent.notification_plan_id,
        repository,
        telegram_client_builder=FailingTelegramClientBuilder(),
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "notification_plan_already_delivered"
    assert repository.renders == []
    assert repository.delivery_records == []


@pytest.mark.asyncio
async def test_happy_path_fake_telegram_records_sent_result_through_service_boundary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    repository, intent = repo_with_valid_case()
    _store_existing_plan(repository, intent)
    clients: list[RecordingTelegramClient] = []

    def client_builder(**kwargs):
        client = RecordingTelegramClient(**kwargs)
        clients.append(client)
        return client

    exit_code = await notifier_main.run_send_canary_with_repository(
        config(dry_run=False, enable_notification_send=True),
        intent.notification_plan_id,
        repository,
        telegram_client_builder=client_builder,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload == {
        "schema_version": "notifier_one_shot_canary_v1",
        "status": "sent",
        "notification_plan_id": str(intent.notification_plan_id),
        "delivery_status": "sent",
        "telegram_chat_id_present": True,
        "telegram_message_id_present": True,
    }
    assert len(clients) == 1
    assert clients[0].kwargs["bot_token"] == "token"
    assert repository.plans[intent.notification_plan_id].status == "sent"
    assert len(repository.renders) == 1
    assert repository.delivery_records[0]["result_status"] == "sent"
    assert repository.delivery_records[0]["telegram_message_id"] == 9876
    assert repository.state_transitions[-1]["to_state"] == "sent"
    assert repository.delivery_outbox[0]["delivery_status"] == "sent"
