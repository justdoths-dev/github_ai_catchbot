from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest

from services.notifier_telegram.main import (
    SEND_DISABLED_PROOF_REASON_CODE,
    SEND_DISABLED_PROOF_SCHEMA_VERSION,
    _run_send_disabled_worker_once_proof,
    _run_send_disabled_worker_once_proof_command,
    build_parser,
    create_send_disabled_worker_once_proof_with_repository,
)
from services.notifier_telegram.models import NotificationIntentJob, NotificationPlanDraft, StreamMessage
from services.notifier_telegram.service import NotifierTelegramService
from services.notifier_telegram.worker_once import EXPECTED_QUEUE_NAME, WorkerOnceRuntime, run_worker_once_invocation
from tests.component.services.notifier_telegram._fakes import FakeRepository, RaisingTelegramClient, repo_with_valid_case


NOTIFIER_RUNTIME_ENV_KEYS = (
    "APP_ENV",
    "DATABASE_URL",
    "DATABASE_URL_FILE",
    "REDIS_URL",
    "REDIS_URL_FILE",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_BOT_TOKEN_FILE",
    "TELEGRAM_API_BASE_URL",
    "ENABLE_NOTIFICATION_SEND",
    "NOTIFIER_TELEGRAM_DRY_RUN",
    "NOTIFIER_TELEGRAM_ALLOW_EDITS",
    "NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS",
    "NOTIFIER_TELEGRAM_EDIT_WINDOW_MINUTES",
    "NOTIFIER_TELEGRAM_REQUEST_TIMEOUT_SEC",
    "ENABLE_DIGEST_RUNTIME",
    "LOG_LEVEL",
    "NOTIFIER_TELEGRAM_QUEUE_NAME",
    "NOTIFIER_TELEGRAM_CONSUMER_GROUP",
    "NOTIFIER_TELEGRAM_CONSUMER_NAME",
    "NOTIFIER_TELEGRAM_BATCH_SIZE",
    "NOTIFIER_TELEGRAM_BLOCK_MS",
)


def _clear_notifier_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in NOTIFIER_RUNTIME_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


@pytest.mark.asyncio
async def test_cli_rejects_without_operator_confirmation() -> None:
    emitted: list[str] = []
    args = build_parser().parse_args(
        [
            "send-disabled-worker-once-proof",
            "--source-notification-plan-id",
            str(uuid4()),
            "--proof-key",
            "proof-key-01",
            "--env-file",
            "/tmp/prod.env",
            "--format",
            "json",
        ]
    )

    code = await _run_send_disabled_worker_once_proof_command(args, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload == {
        "schema_version": SEND_DISABLED_PROOF_SCHEMA_VERSION,
        "status": "rejected",
        "reason_code": "operator_confirmation_required",
    }


@pytest.mark.asyncio
async def test_cli_rejects_when_transport_enabled(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_NOTIFICATION_SEND", "false")
    _clear_notifier_runtime_env(monkeypatch)
    env_file = tmp_path / "prod.env"
    env_file.write_text(
        "\n".join(
            [
                "APP_ENV=prod",
                "DATABASE_URL=postgresql+psycopg" + "://unit/db",
                "REDIS_URL=redis" + "://unit/0",
                "ENABLE_NOTIFICATION_SEND=true",
                "NOTIFIER_TELEGRAM_DRY_RUN=false",
                "NOTIFIER_TELEGRAM_ALLOW_EDITS=false",
            ]
        ),
        encoding="utf-8",
    )
    emitted: list[str] = []
    args = build_parser().parse_args(
        [
            "send-disabled-worker-once-proof",
            "--source-notification-plan-id",
            str(uuid4()),
            "--proof-key",
            "proof-key-01",
            "--operator-confirmed",
            "--env-file",
            str(env_file),
            "--format",
            "json",
        ]
    )

    code = await _run_send_disabled_worker_once_proof_command(args, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "telegram_transport_enabled"


@pytest.mark.asyncio
async def test_cli_rejects_source_plan_with_zero_target_chat_id() -> None:
    repository, source_plan_id, _ = _proof_repository()
    source_plan = repository.plans[source_plan_id]
    repository.plans[source_plan_id] = replace(source_plan, target_chat_id=0)
    redis = FakeRedis()
    emitted: list[str] = []

    code = await _run_send_disabled_worker_once_proof(
        _proof_config(),
        source_plan_id,
        "proof-key-01",
        emit_json=emitted.append,
        session_factory_builder=_fake_session_factory_builder,
        redis_client_builder=lambda redis_url: redis,
        repository_builder=lambda session: repository,
        worker_once_runner=_unused_worker_once_runner,
    )
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "source_plan_target_chat_missing"
    assert redis.xadd_calls == []


@pytest.mark.asyncio
async def test_cli_rejects_when_redis_queue_is_not_idle_before_db_mutation() -> None:
    repository, source_plan_id, _ = _proof_repository()
    redis = FakeRedis(lag=1)
    emitted: list[str] = []

    code = await _run_send_disabled_worker_once_proof(
        _proof_config(),
        source_plan_id,
        "proof-key-01",
        emit_json=emitted.append,
        session_factory_builder=_fake_session_factory_builder,
        redis_client_builder=lambda redis_url: redis,
        repository_builder=lambda session: repository,
        worker_once_runner=_unused_worker_once_runner,
    )
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "redis_queue_not_idle"
    assert redis.xadd_calls == []
    assert len(repository.plans) == 1
    assert len(repository.event_outbox) == 1


@pytest.mark.asyncio
async def test_cli_creates_deterministic_proof_plan_event_without_overwriting_source() -> None:
    repository, source_plan_id, source_event_id = _proof_repository()

    first = await create_send_disabled_worker_once_proof_with_repository(
        source_plan_id,
        "proof-key-01",
        repository,
    )
    second = await create_send_disabled_worker_once_proof_with_repository(
        source_plan_id,
        "proof-key-01",
        repository,
    )

    assert first.reason_code is None
    assert first.notification_plan_id is not None
    assert first.trigger_event_id is not None
    assert repository.plans[source_plan_id].status == "sent"
    assert repository.event_outbox[source_event_id]["event_type"] == "source.existing.v1"
    assert repository.event_outbox[first.trigger_event_id]["event_type"] == "notification.plan.created.v1"
    assert repository.event_outbox[first.trigger_event_id]["aggregate_type"] == "notification_plan"
    assert repository.event_outbox[first.trigger_event_id]["aggregate_id"] == first.notification_plan_id
    assert repository.event_outbox[first.trigger_event_id]["status"] == "published"
    assert repository.event_outbox[first.trigger_event_id]["payload_json"]["target_chat_id"] == 12345
    assert repository.plans[first.notification_plan_id].dedupe_subject_key == (
        f"proof/send-disabled/{source_plan_id}/proof-key-01"
    )
    assert second.reason_code == "proof_notification_plan_exists"


@pytest.mark.asyncio
async def test_cli_invokes_worker_once_and_records_send_disabled_suppressed_delivery_result() -> None:
    repository, source_plan_id, _ = _proof_repository()
    redis = FakeRedis()
    telegram_client = RaisingTelegramClient()
    emitted: list[str] = []

    async def worker_once_runner(config, emit_json):
        return await _run_fake_worker_once(config, emit_json, repository=repository, redis=redis, client=telegram_client)

    code = await _run_send_disabled_worker_once_proof(
        _proof_config(),
        source_plan_id,
        "proof-key-01",
        emit_json=emitted.append,
        session_factory_builder=_fake_session_factory_builder,
        redis_client_builder=lambda redis_url: redis,
        repository_builder=lambda session: repository,
        worker_once_runner=worker_once_runner,
    )
    payload = json.loads(emitted[0])
    proof_plan_id = UUID(payload["proof_notification_plan_id"])
    delivery_record = repository.delivery_records[0]
    response_json = delivery_record["telegram_response_json"]

    assert code == 0
    assert payload["status"] == "pass"
    assert payload["worker_once"]["status"] == "processed"
    assert payload["worker_once"]["acked"] is True
    assert payload["authority"]["telegram_transport_possible"] is False
    assert payload["authority"]["database_session_opened"] is True
    assert payload["authority"]["workers_started"] is False
    assert payload["authority"]["run_forever_started"] is False
    assert payload["authority"]["openai_called"] is False
    assert payload["authority"]["github_called"] is False
    assert payload["authority"]["docker_or_systemd_called"] is False
    assert payload["authority"]["alembic_or_ddl_ran"] is False
    assert len(redis.xadd_calls) == 1
    assert redis.xadd_calls[0]["name"] == EXPECTED_QUEUE_NAME
    assert redis.xadd_calls[0]["fields"]["root_object_type"] == "notification_plan"
    assert repository.plans[proof_plan_id].status == "suppressed"
    assert len(repository.renders) == 1
    assert len(repository.delivery_records) == 1
    assert delivery_record["result_status"] == "suppressed"
    assert delivery_record["attempt_count"] == 0
    assert delivery_record["transport_error_code"] == SEND_DISABLED_PROOF_REASON_CODE
    assert response_json["send_disabled"] is True
    assert response_json["dry_run"] is False
    assert response_json["reason_code"] == SEND_DISABLED_PROOF_REASON_CODE
    assert response_json["transport_skipped"] is True
    assert repository.state_transitions[-1]["reason_code"] == SEND_DISABLED_PROOF_REASON_CODE
    assert repository.delivery_outbox[0]["delivery_status"] == "suppressed"
    assert telegram_client.calls == 0


@pytest.mark.asyncio
async def test_cli_output_is_sanitized_without_env_contents_traceback_or_raw_exception_text() -> None:
    repository, source_plan_id, _ = _proof_repository()
    redis = FakeRedis()
    emitted: list[str] = []
    secret_database_url = "postgresql+psycopg" + "://" + "user:pass" + "word@example/db"
    secret_redis_url = "redis" + "://" + ":pass" + "word@example/0"
    secret_token = "telegram" + "-credential-value"

    async def worker_once_runner(config, emit_json):
        del config, emit_json
        raise RuntimeError(
            "RAW_EXCEPTION_SENTINEL Traceback DATABASE_URL REDIS_URL TELEGRAM_BOT_TOKEN "
            + secret_database_url
            + secret_redis_url
            + secret_token
        )

    code = await _run_send_disabled_worker_once_proof(
        replace(_proof_config(), database_url=secret_database_url, redis_url=secret_redis_url),
        source_plan_id,
        "proof-key-01",
        emit_json=emitted.append,
        session_factory_builder=_fake_session_factory_builder,
        redis_client_builder=lambda redis_url: redis,
        repository_builder=lambda session: repository,
        worker_once_runner=worker_once_runner,
    )
    output = emitted[0]
    payload = json.loads(output)

    assert code == 1
    assert payload["status"] == "fail"
    assert payload["reason_code"] == "send_disabled_proof_failed"
    for forbidden in [
        secret_database_url,
        secret_redis_url,
        secret_token,
        "DATABASE_URL",
        "REDIS_URL",
        "TELEGRAM_BOT_TOKEN",
        "prod.env",
        "Traceback",
        "RAW_EXCEPTION_SENTINEL",
        "pass" + "word",
    ]:
        assert forbidden not in output


def _proof_repository() -> tuple[ProofRepository, UUID, UUID]:
    repository = ProofRepository()
    base_repository, intent = repo_with_valid_case()
    repository.analyses.update(base_repository.analyses)
    repository.judge_outputs.update(base_repository.judge_outputs)
    repository.candidates.update(base_repository.candidates)

    source_plan_id = uuid4()
    source_plan = NotificationPlanDraft(
        notification_plan_id=source_plan_id,
        analysis_id=intent.analysis_id,
        candidate_group_id=intent.candidate_group_id,
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=12345,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key="source-dedupe-subject",
        material_change_hash="source-material-hash",
        send_after=None,
        suppress_reason_code=None,
        status="sent",
    )
    repository.plans[source_plan_id] = source_plan
    source_event_id = uuid4()
    repository.event_outbox[source_event_id] = {
        "event_id": source_event_id,
        "event_type": "source.existing.v1",
        "aggregate_type": "notification_plan",
        "aggregate_id": source_plan_id,
        "dedupe_key": "source-existing",
        "payload_json": {},
        "status": "published",
    }
    return repository, source_plan_id, source_event_id


def _proof_config():
    from tests.component.services.notifier_telegram._fakes import config

    return replace(
        config(dry_run=False, enable_notification_send=False, allow_edits=False),
        app_env="prod",
        database_url="postgresql+psycopg" + "://unit/db",
        redis_url="redis" + "://unit/0",
        telegram_bot_token="",
    )


async def _unused_worker_once_runner(config, emit_json):
    del config, emit_json
    raise AssertionError("worker-once must not run")


async def _run_fake_worker_once(config, emit_json, *, repository: "ProofRepository", redis: "FakeRedis", client):
    fields = redis.xadd_calls[-1]["fields"]
    consumer = _OneMessageConsumer(StreamMessage(stream=EXPECTED_QUEUE_NAME, message_id="1-0", fields=fields))

    async def runtime_builder(cfg, state, logger):
        del logger

        class StateTrackingService:
            async def handle_trigger_event(self, trigger_event_id: str):
                state.database_session_opened = True
                service = NotifierTelegramService(cfg, repository=repository, telegram_client=client)
                return await service.handle_trigger_event(trigger_event_id)

        async def dispose() -> None:
            return None

        return WorkerOnceRuntime(consumer=consumer, service=StateTrackingService(), dispose=dispose)

    return await run_worker_once_invocation(
        queue=EXPECTED_QUEUE_NAME,
        confirm_worker_once=True,
        output_format="json",
        emit_json=emit_json,
        config_loader=lambda: config,
        runtime_builder=runtime_builder,
    )


class _FakeSessionContext:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSessionFactory:
    def begin(self):
        return _FakeSessionContext()


def _fake_session_factory_builder(database_url: str):
    del database_url

    async def dispose() -> None:
        return None

    return _FakeSessionFactory(), dispose


class FakeRedis:
    def __init__(self, *, pending: int = 0, lag: int = 0) -> None:
        self.pending = pending
        self.lag = lag
        self.xadd_calls: list[dict[str, Any]] = []
        self.closed = False

    async def xinfo_groups(self, name: str):
        assert name == EXPECTED_QUEUE_NAME
        return [{"name": "notifier-telegram", "pending": self.pending, "lag": self.lag}]

    async def xadd(self, name: str, fields: dict[str, str]):
        self.xadd_calls.append({"name": name, "fields": dict(fields)})
        return "1-0"

    async def aclose(self) -> None:
        self.closed = True


class _OneMessageConsumer:
    def __init__(self, message: StreamMessage) -> None:
        self._message = message
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        return None

    async def read_batch(self) -> list[StreamMessage]:
        return [self._message]

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


class ProofRepository(FakeRepository):
    def __init__(self) -> None:
        super().__init__()
        self.event_outbox: dict[UUID, dict[str, Any]] = {}

    async def load_event_outbox(self, event_id: UUID):
        return self.event_outbox.get(event_id)

    async def insert_published_notification_plan_created_outbox(
        self,
        *,
        event_id: UUID,
        notification_plan_id: UUID,
        dedupe_key: str,
        payload_json: dict[str, Any],
    ):
        if event_id in self.event_outbox:
            return None
        if any(row["dedupe_key"] == dedupe_key for row in self.event_outbox.values()):
            return None
        self.event_outbox[event_id] = {
            "event_id": event_id,
            "event_type": "notification.plan.created.v1",
            "aggregate_type": "notification_plan",
            "aggregate_id": notification_plan_id,
            "dedupe_key": dedupe_key,
            "payload_json": payload_json,
            "status": "published",
            "published_at": datetime.now(timezone.utc),
        }
        self.jobs[event_id] = NotificationIntentJob(
            trigger_event_id=event_id,
            event_type="notification.plan.created.v1",
            notification_plan_id=notification_plan_id,
            analysis_id=UUID(str(payload_json["analysis_id"])),
            candidate_group_id=UUID(str(payload_json["candidate_group_id"])),
            delivery_decision=payload_json["delivery_decision"],
            urgency_profile=payload_json["urgency_profile"],
            target_chat_id=int(payload_json["target_chat_id"]),
            target_thread_id=payload_json["target_thread_id"],
            render_profile=payload_json["render_profile"],
            dedupe_subject_key=payload_json["dedupe_subject_key"],
            material_change_hash=payload_json["material_change_hash"],
            send_after=None,
            suppress_reason_code=payload_json["suppress_reason_code"],
        )
        return event_id

    async def load_send_disabled_worker_once_proof_verification(self, *, notification_plan_id: UUID):
        plan = self.plans.get(notification_plan_id)
        deliveries = [
            record for record in self.delivery_records if record["notification_plan_id"] == notification_plan_id
        ]
        latest_delivery = deliveries[-1] if deliveries else {}
        transitions = [
            row
            for row in self.state_transitions
            if row.get("object_type") == "notification_plan" and row.get("object_id") == notification_plan_id
        ]
        return {
            "proof_plan_final_status": plan.status if plan else None,
            "notification_render_count": sum(1 for render in self.renders if render.notification_plan_id == notification_plan_id),
            "notification_delivery_record_count": len(deliveries),
            "delivery_status": latest_delivery.get("result_status"),
            "attempt_count": latest_delivery.get("attempt_count"),
            "transport_error_code": latest_delivery.get("transport_error_code"),
            "telegram_response_json": latest_delivery.get("telegram_response_json"),
            "latest_state_transition_reason_code": transitions[-1]["reason_code"] if transitions else None,
            "delivery_result_outbox_exists": any(
                row.get("notification_plan_id") == notification_plan_id for row in self.delivery_outbox
            ),
        }
