from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import datetime, timezone
from uuid import UUID

import pytest

from services.notifier_telegram import main as notifier_main
from services.notifier_telegram.models import NotificationPlanDraft

from ._fakes import FakeRepository, config, repo_with_valid_case, service as fake_service


RAW_DATABASE_URL = "postgresql+psycopg://sentinel-db-user:sentinel-db-pass@db.example/app"
RAW_REDIS_URL = "redis://:sentinel-redis-pass@redis.example:6379/0"
SOURCE_PLAN_ID = UUID("00000000-0000-0000-0000-000000000001")
CANARY_KEY = "stable-canary_01"


def _create_canary_argv(*extra: str) -> list[str]:
    return [
        "create-canary-plan",
        "--source-notification-plan-id",
        str(SOURCE_PLAN_ID),
        *extra,
    ]


def _clear_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in notifier_main.ONE_SHOT_RUNTIME_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _fail_from_env(cls, **kwargs):
    raise AssertionError("early create-canary-plan rejection must not load runtime config")


def _store_source_plan(
    repository: FakeRepository,
    intent,
    *,
    status: str = "sent",
    delivery_decision: str | None = None,
    target_chat_id: int | None = 12345,
) -> None:
    repository.plans[intent.notification_plan_id] = NotificationPlanDraft(
        notification_plan_id=intent.notification_plan_id,
        analysis_id=intent.analysis_id,
        candidate_group_id=intent.candidate_group_id,
        delivery_decision=delivery_decision or intent.delivery_decision,
        urgency_profile=intent.urgency_profile,
        target_chat_id=target_chat_id,  # type: ignore[arg-type]
        target_thread_id=intent.target_thread_id,
        render_profile=intent.render_profile,
        dedupe_subject_key="source-dedupe",
        material_change_hash="source-material",
        send_after=None,
        suppress_reason_code=intent.suppress_reason_code,
        status=status,
    )


async def _run_create(repository: FakeRepository, source_plan_id: UUID, canary_key: str = CANARY_KEY) -> tuple[int, dict]:
    outputs: list[str] = []
    exit_code = await notifier_main.run_create_canary_plan_with_repository(
        source_plan_id,
        canary_key,
        repository,
        emit_json=outputs.append,
    )
    assert len(outputs) == 1
    return exit_code, json.loads(outputs[0])


def _assert_no_delivery_side_effects(repository: FakeRepository) -> None:
    assert repository.renders == []
    assert repository.delivery_records == []
    assert repository.delivery_outbox == []
    assert "render" not in repository.operations
    assert "delivery_record" not in repository.operations
    assert "delivery_outbox" not in repository.operations


@pytest.mark.asyncio
async def test_missing_operator_confirmation_rejects_before_config_session_or_transport(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setattr(notifier_main.NotifierTelegramConfig, "from_env", classmethod(_fail_from_env))
    monkeypatch.setattr(
        notifier_main,
        "_build_send_canary_session_factory",
        lambda database_url: (_ for _ in ()).throw(AssertionError),
    )

    exit_code = await notifier_main._run(
        _create_canary_argv("--canary-key", CANARY_KEY, "--env-file", "/tmp/must-not-be-read.env")
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload == {
        "schema_version": "notifier_canary_plan_created_v1",
        "status": "rejected",
        "reason_code": "operator_confirmation_required",
    }


@pytest.mark.asyncio
async def test_malformed_source_uuid_invalid_canary_key_and_missing_env_file_reject_before_config(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setattr(notifier_main.NotifierTelegramConfig, "from_env", classmethod(_fail_from_env))
    monkeypatch.setattr(
        notifier_main,
        "_build_send_canary_session_factory",
        lambda database_url: (_ for _ in ()).throw(AssertionError),
    )

    exit_code = await notifier_main._run(
        [
            "create-canary-plan",
            "--source-notification-plan-id",
            "not-a-uuid",
            "--canary-key",
            CANARY_KEY,
            "--operator-confirmed",
            "--env-file",
            "/tmp/must-not-be-read.env",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload["reason_code"] == "invalid_source_notification_plan_id"

    exit_code = await notifier_main._run(
        _create_canary_argv("--canary-key", "bad key", "--operator-confirmed", "--env-file", "/tmp/must-not-be-read.env")
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload["reason_code"] == "invalid_canary_key"

    exit_code = await notifier_main._run(_create_canary_argv("--canary-key", CANARY_KEY, "--operator-confirmed"))
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload["reason_code"] == "env_file_required"


@pytest.mark.asyncio
async def test_env_file_db_only_config_loads_without_telegram_token_and_restores_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _clear_runtime_env(monkeypatch)
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                f"DATABASE_URL={RAW_DATABASE_URL}",
                f"REDIS_URL={RAW_REDIS_URL}",
                "APP_ENV=test",
                "ENABLE_NOTIFICATION_SEND=false",
                "NOTIFIER_TELEGRAM_DRY_RUN=true",
            ]
        ),
        encoding="utf-8",
    )

    async def fake_run(config, source_notification_plan_id, canary_key, *, emit_json=print, **kwargs):
        assert config.database_url == RAW_DATABASE_URL
        assert config.redis_url == RAW_REDIS_URL
        assert config.telegram_bot_token == ""
        emit_json(notifier_main._to_json({"schema_version": notifier_main.CANARY_PLAN_SCHEMA_VERSION, "status": "created"}))
        return 0

    monkeypatch.setattr(notifier_main, "_run_create_canary_plan", fake_run)

    exit_code = await notifier_main._run(
        _create_canary_argv("--canary-key", CANARY_KEY, "--operator-confirmed", "--env-file", str(env_file))
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert RAW_DATABASE_URL not in output
    assert RAW_REDIS_URL not in output
    assert "DATABASE_URL" not in os.environ
    assert "REDIS_URL" not in os.environ
    assert "TELEGRAM_BOT_TOKEN" not in os.environ


@pytest.mark.asyncio
async def test_creates_canary_plan_from_existing_sent_send_now_source_plan_and_is_idempotent() -> None:
    repository, intent = repo_with_valid_case()
    _store_source_plan(repository, intent, status="sent")

    exit_code, payload = await _run_create(repository, intent.notification_plan_id)

    assert exit_code == 0
    assert payload["schema_version"] == "notifier_canary_plan_created_v1"
    assert payload["status"] == "created"
    assert payload["source_notification_plan_id"] == str(intent.notification_plan_id)
    assert payload["source_plan_status"] == "sent"
    assert payload["delivery_decision"] == "send_now"
    assert payload["plan_status"] == "planned"
    assert payload["send_after_due"] is True
    assert payload["target_chat_id_present"] is True
    assert payload["ready_for_send_canary"] is True

    canary_plan_id = UUID(payload["notification_plan_id"])
    source_plan = repository.plans[intent.notification_plan_id]
    canary_plan = repository.plans[canary_plan_id]
    assert source_plan.status == "sent"
    assert canary_plan.analysis_id == intent.analysis_id
    assert canary_plan.candidate_group_id == intent.candidate_group_id
    assert canary_plan.delivery_decision == "send_now"
    assert canary_plan.status == "planned"
    assert canary_plan.send_after is not None
    assert canary_plan.dedupe_subject_key == f"operator-canary:{intent.notification_plan_id}:{CANARY_KEY}"
    assert canary_plan.dedupe_subject_key != source_plan.dedupe_subject_key
    assert canary_plan.material_change_hash != source_plan.material_change_hash
    assert canary_plan.suppress_reason_code is None
    _assert_no_delivery_side_effects(repository)

    second_exit_code, second_payload = await _run_create(repository, intent.notification_plan_id)

    assert second_exit_code == 0
    assert second_payload["status"] == "existing"
    assert second_payload["notification_plan_id"] == str(canary_plan_id)
    assert list(repository.plans).count(canary_plan_id) == 1
    assert repository.operations.count("plan") == 1
    _assert_no_delivery_side_effects(repository)


@pytest.mark.asyncio
async def test_existing_already_delivered_canary_plan_returns_not_ready_and_does_not_reset_status() -> None:
    repository, intent = repo_with_valid_case()
    _store_source_plan(repository, intent, status="sent")
    _, created_payload = await _run_create(repository, intent.notification_plan_id)
    canary_plan_id = UUID(created_payload["notification_plan_id"])
    repository.plans[canary_plan_id] = replace(repository.plans[canary_plan_id], status="sent")
    repository.delivery_records.append(
        {
            "notification_plan_id": canary_plan_id,
            "result_status": "sent",
            "telegram_chat_id": repository.plans[canary_plan_id].target_chat_id,
            "telegram_message_id": 9876,
            "created_at": datetime.now(timezone.utc),
        }
    )
    repository.operations.clear()

    exit_code, payload = await _run_create(repository, intent.notification_plan_id)

    assert exit_code == 0
    assert payload["status"] == "existing_already_delivered"
    assert payload["notification_plan_id"] == str(canary_plan_id)
    assert payload["plan_status"] == "sent"
    assert payload["ready_for_send_canary"] is False
    assert repository.plans[canary_plan_id].status == "sent"
    assert repository.operations == []


@pytest.mark.asyncio
async def test_rejects_invalid_source_plan_and_analysis_contexts() -> None:
    repository, intent = repo_with_valid_case()
    exit_code, payload = await _run_create(repository, intent.notification_plan_id)
    assert exit_code == 1
    assert payload["reason_code"] == "source_notification_plan_missing"

    _store_source_plan(repository, intent, delivery_decision="suppress")
    exit_code, payload = await _run_create(repository, intent.notification_plan_id)
    assert exit_code == 1
    assert payload["reason_code"] == "source_plan_not_send_now"

    repository, intent = repo_with_valid_case()
    _store_source_plan(repository, intent, target_chat_id=None)
    exit_code, payload = await _run_create(repository, intent.notification_plan_id)
    assert exit_code == 1
    assert payload["reason_code"] == "source_plan_target_chat_missing"

    repository, intent = repo_with_valid_case()
    _store_source_plan(repository, intent)
    del repository.analyses[intent.analysis_id]
    exit_code, payload = await _run_create(repository, intent.notification_plan_id)
    assert exit_code == 1
    assert payload["reason_code"] == "source_analysis_missing"

    repository, intent = repo_with_valid_case()
    _store_source_plan(repository, intent)
    repository.analyses[intent.analysis_id] = replace(repository.analyses[intent.analysis_id], delivery_decision="suppress")
    exit_code, payload = await _run_create(repository, intent.notification_plan_id)
    assert exit_code == 1
    assert payload["reason_code"] == "source_analysis_not_send_now"


@pytest.mark.asyncio
async def test_conflicting_deterministic_canary_plan_rejects_without_overwrite() -> None:
    repository, intent = repo_with_valid_case()
    _store_source_plan(repository, intent)
    identity = notifier_main._canary_identity(
        source_notification_plan_id=intent.notification_plan_id,
        canary_key=CANARY_KEY,
        analysis_id=intent.analysis_id,
        candidate_group_id=intent.candidate_group_id,
        target_chat_id=intent.target_chat_id,
    )
    repository.plans[identity.notification_plan_id] = NotificationPlanDraft(
        notification_plan_id=identity.notification_plan_id,
        analysis_id=intent.analysis_id,
        candidate_group_id=intent.candidate_group_id,
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=intent.target_chat_id,
        target_thread_id=None,
        render_profile=intent.render_profile,
        dedupe_subject_key="wrong-canary-dedupe",
        material_change_hash=identity.material_change_hash,
        send_after=None,
        suppress_reason_code=None,
        status="planned",
    )

    exit_code, payload = await _run_create(repository, intent.notification_plan_id)

    assert exit_code == 1
    assert payload["reason_code"] == "canary_plan_conflict"
    assert repository.plans[identity.notification_plan_id].dedupe_subject_key == "wrong-canary-dedupe"


@pytest.mark.asyncio
async def test_created_plan_is_eligible_for_existing_send_canary_preflight_and_send_action() -> None:
    repository, intent = repo_with_valid_case()
    _store_source_plan(repository, intent, status="sent")

    exit_code, payload = await _run_create(repository, intent.notification_plan_id)

    assert exit_code == 0
    canary_plan_id = UUID(payload["notification_plan_id"])
    plan_row = await repository.load_notification_plan(canary_plan_id)
    canary_intent = await repository.load_notification_plan_intent(canary_plan_id)
    assert canary_intent is not None
    service = fake_service(repository, cfg=config(dry_run=False, enable_notification_send=True))
    action = await service.decide_delivery_action(canary_intent, candidate=repository.candidates[intent.candidate_group_id])

    assert notifier_main._plan_guard_reason(plan_row) is None
    assert action.mode == "send"
    assert action.reason_code == "notification_no_recent_delivery"
    _assert_no_delivery_side_effects(repository)
