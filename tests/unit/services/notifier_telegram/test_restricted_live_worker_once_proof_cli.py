from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest

from services.notifier_telegram.main import (
    RESTRICTED_LIVE_PROOF_SCHEMA_VERSION,
    _run_restricted_live_worker_once_proof,
    _run_restricted_live_worker_once_proof_command,
    build_parser,
    create_restricted_live_worker_once_proof_with_repository,
)
from services.notifier_telegram.models import NotificationIntentJob, NotificationPlanDraft, StreamMessage
from services.notifier_telegram.service import NotifierTelegramService
from services.notifier_telegram.worker_once import (
    EXPECTED_QUEUE_NAME,
    REQUIRED_THIN_QUEUE_FIELDS,
    WorkerOnceRuntime,
    run_worker_once_invocation,
)
from tests.component.services.notifier_telegram._fakes import FakeRepository, repo_with_valid_case


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
            "restricted-live-worker-once-proof",
            "--source-notification-plan-id",
            str(uuid4()),
            "--proof-key",
            "proof-key-01",
            "--env-file",
            "/tmp/notifier-runtime.env",
            "--format",
            "json",
        ]
    )

    code = await _run_restricted_live_worker_once_proof_command(args, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload == {
        "schema_version": RESTRICTED_LIVE_PROOF_SCHEMA_VERSION,
        "status": "rejected",
        "reason_code": "operator_confirmation_required",
    }


@pytest.mark.asyncio
async def test_cli_rejects_when_send_disabled_transport_disabled(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_notifier_runtime_env(monkeypatch)
    env_file = tmp_path / "notifier-runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "APP_ENV=prod",
                "DATABASE_URL=postgresql+psycopg" + "://unit/db",
                "REDIS_URL=redis" + "://unit/0",
                "TELEGRAM_BOT_TOKEN=unit-token",
                "ENABLE_NOTIFICATION_SEND=false",
                "NOTIFIER_TELEGRAM_DRY_RUN=false",
                "NOTIFIER_TELEGRAM_ALLOW_EDITS=false",
            ]
        ),
        encoding="utf-8",
    )
    emitted: list[str] = []
    args = build_parser().parse_args(
        [
            "restricted-live-worker-once-proof",
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

    code = await _run_restricted_live_worker_once_proof_command(args, emit_json=emitted.append)
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "notification_send_disabled"


@pytest.mark.asyncio
async def test_cli_rejects_when_dry_run_enabled() -> None:
    repository, source_plan_id, _ = _proof_repository()
    emitted: list[str] = []

    code = await _run_restricted_live_worker_once_proof(
        replace(_proof_config(), dry_run=True),
        source_plan_id,
        "proof-key-01",
        emit_json=emitted.append,
        session_factory_builder=_fake_session_factory_builder,
        redis_client_builder=lambda redis_url: FakeRedis(),
        repository_builder=lambda session: repository,
        worker_once_runner=_unused_worker_once_runner,
    )
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "notifier_dry_run_enabled"
    assert len(repository.plans) == 1


@pytest.mark.asyncio
async def test_cli_rejects_when_edits_enabled() -> None:
    repository, source_plan_id, _ = _proof_repository()
    emitted: list[str] = []

    code = await _run_restricted_live_worker_once_proof(
        replace(_proof_config(), allow_edits=True),
        source_plan_id,
        "proof-key-01",
        emit_json=emitted.append,
        session_factory_builder=_fake_session_factory_builder,
        redis_client_builder=lambda redis_url: FakeRedis(),
        repository_builder=lambda session: repository,
        worker_once_runner=_unused_worker_once_runner,
    )
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "notifier_edits_enabled"
    assert len(repository.plans) == 1


@pytest.mark.asyncio
async def test_cli_rejects_when_telegram_bot_token_missing() -> None:
    repository, source_plan_id, _ = _proof_repository()
    emitted: list[str] = []

    code = await _run_restricted_live_worker_once_proof(
        replace(_proof_config(), telegram_bot_token=""),
        source_plan_id,
        "proof-key-01",
        emit_json=emitted.append,
        session_factory_builder=_fake_session_factory_builder,
        redis_client_builder=lambda redis_url: FakeRedis(),
        repository_builder=lambda session: repository,
        worker_once_runner=_unused_worker_once_runner,
    )
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "telegram_bot_token_missing"
    assert len(repository.plans) == 1


@pytest.mark.asyncio
async def test_cli_rejects_local_telegram_api_base_url() -> None:
    repository, source_plan_id, _ = _proof_repository()
    emitted: list[str] = []

    code = await _run_restricted_live_worker_once_proof(
        replace(_proof_config(), telegram_api_base_url="http://127.0.0.1:8081"),
        source_plan_id,
        "proof-key-01",
        emit_json=emitted.append,
        session_factory_builder=_fake_session_factory_builder,
        redis_client_builder=lambda redis_url: FakeRedis(),
        repository_builder=lambda session: repository,
        worker_once_runner=_unused_worker_once_runner,
    )
    payload = json.loads(emitted[0])

    assert code == 2
    assert payload["status"] == "rejected"
    assert payload["reason_code"] == "telegram_api_base_url_blackhole"
    assert len(repository.plans) == 1


@pytest.mark.asyncio
async def test_cli_rejects_source_plan_with_zero_target_chat_id() -> None:
    repository, source_plan_id, _ = _proof_repository()
    source_plan = repository.plans[source_plan_id]
    repository.plans[source_plan_id] = replace(source_plan, target_chat_id=0)
    redis = FakeRedis()
    emitted: list[str] = []

    code = await _run_restricted_live_worker_once_proof(
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
    redis = FakeRedis(pending=1)
    emitted: list[str] = []

    code = await _run_restricted_live_worker_once_proof(
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
async def test_cli_rejects_when_redis_group_metrics_unavailable_before_db_mutation() -> None:
    repository, source_plan_id, _ = _proof_repository()
    redis = FakeRedis(metrics_unavailable=True)
    emitted: list[str] = []

    code = await _run_restricted_live_worker_once_proof(
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
    assert payload["reason_code"] == "redis_group_metrics_unavailable"
    assert redis.xadd_calls == []
    assert len(repository.plans) == 1
    assert len(repository.event_outbox) == 1


@pytest.mark.asyncio
async def test_cli_creates_deterministic_proof_plan_event_without_overwriting_source() -> None:
    repository, source_plan_id, source_event_id = _proof_repository()

    first = await create_restricted_live_worker_once_proof_with_repository(
        source_plan_id,
        "proof-key-01",
        repository,
    )
    second = await create_restricted_live_worker_once_proof_with_repository(
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
        f"proof/restricted-live/{source_plan_id}/proof-key-01"
    )
    assert second.reason_code == "proof_notification_plan_exists"


@pytest.mark.asyncio
async def test_cli_xadds_once_invokes_worker_once_once_and_records_live_send() -> None:
    repository, source_plan_id, _ = _proof_repository()
    redis = FakeRedis()
    telegram_client = RecordingTelegramClient()
    worker_once_calls = 0
    emitted: list[str] = []

    async def worker_once_runner(config, emit_json):
        nonlocal worker_once_calls
        worker_once_calls += 1
        return await _run_fake_worker_once(config, emit_json, repository=repository, redis=redis, client=telegram_client)

    code = await _run_restricted_live_worker_once_proof(
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

    assert code == 0
    assert payload["status"] == "pass"
    assert payload["reason_code"] == "restricted_live_worker_once_proof_passed"
    assert payload["worker_once"]["status"] == "processed"
    assert payload["worker_once"]["acked"] is True
    assert payload["worker_once"]["handler_called"] is True
    assert payload["authority"]["telegram_transport_possible"] is True
    assert payload["authority"]["database_session_opened"] is True
    assert payload["authority"]["workers_started"] is False
    assert payload["authority"]["run_forever_started"] is False
    assert payload["authority"]["openai_called"] is False
    assert payload["authority"]["github_called"] is False
    assert payload["authority"]["docker_or_systemd_called"] is False
    assert payload["authority"]["alembic_or_ddl_ran"] is False
    assert worker_once_calls == 1
    assert len(redis.xadd_calls) == 1
    assert redis.xadd_calls[0]["name"] == EXPECTED_QUEUE_NAME
    assert set(redis.xadd_calls[0]["fields"]) == set(REQUIRED_THIN_QUEUE_FIELDS)
    assert redis.xadd_calls[0]["fields"]["root_object_type"] == "notification_plan"
    assert "payload_json" not in redis.xadd_calls[0]["fields"]
    assert repository.plans[proof_plan_id].status == "sent"
    assert len(repository.renders) == 1
    assert len(repository.delivery_records) == 1
    assert delivery_record["result_status"] == "sent"
    assert delivery_record["attempt_count"] == 1
    assert delivery_record["transport_error_code"] is None
    assert delivery_record["telegram_message_id"] == 9001
    assert delivery_record["telegram_chat_id"] == 12345
    assert repository.state_transitions[-1]["to_state"] == "sent"
    assert repository.state_transitions[-1]["reason_code"] == "notification_no_recent_delivery"
    assert repository.delivery_outbox[0]["delivery_status"] == "sent"
    assert telegram_client.send_message_calls == 1
    assert telegram_client.edit_message_text_calls == 0


@pytest.mark.asyncio
async def test_cli_output_is_sanitized_for_success_and_failure() -> None:
    repository, source_plan_id, _ = _proof_repository()
    redis = FakeRedis()
    telegram_client = RecordingTelegramClient()
    emitted: list[str] = []

    async def worker_once_runner(config, emit_json):
        return await _run_fake_worker_once(config, emit_json, repository=repository, redis=redis, client=telegram_client)

    code = await _run_restricted_live_worker_once_proof(
        _proof_config(),
        source_plan_id,
        "proof-key-01",
        emit_json=emitted.append,
        session_factory_builder=_fake_session_factory_builder,
        redis_client_builder=lambda redis_url: redis,
        repository_builder=lambda session: repository,
        worker_once_runner=worker_once_runner,
    )

    assert code == 0
    output = emitted[0]
    payload = json.loads(output)
    assert "telegram_response_json" not in payload["db_verification"]
    for forbidden in [
        "RAW_TELEGRAM_RESPONSE_BODY",
        "Useful repo",
        "clear utility",
        "source text",
        "DATABASE_URL",
        "REDIS_URL",
        "TELEGRAM_BOT_TOKEN",
        "notifier-runtime.env",
        "Traceback",
        "password",
        "token",
        "credential",
    ]:
        assert forbidden not in output

    failing_repository, failing_source_plan_id, _ = _proof_repository()
    failing_redis = FakeRedis()
    failure_emitted: list[str] = []
    secret_database_url = "postgresql+psycopg" + "://" + "user:pass" + "word@example/db"
    secret_redis_url = "redis" + "://" + ":pass" + "word@example/0"
    secret_token = "telegram" + "-credential-value"

    async def failing_worker_once_runner(config, emit_json):
        del config, emit_json
        raise RuntimeError(
            "RAW_EXCEPTION_SENTINEL Traceback DATABASE_URL REDIS_URL TELEGRAM_BOT_TOKEN "
            + secret_database_url
            + secret_redis_url
            + secret_token
        )

    failure_code = await _run_restricted_live_worker_once_proof(
        replace(_proof_config(), database_url=secret_database_url, redis_url=secret_redis_url),
        failing_source_plan_id,
        "proof-key-01",
        emit_json=failure_emitted.append,
        session_factory_builder=_fake_session_factory_builder,
        redis_client_builder=lambda redis_url: failing_redis,
        repository_builder=lambda session: failing_repository,
        worker_once_runner=failing_worker_once_runner,
    )
    failure_output = failure_emitted[0]
    failure_payload = json.loads(failure_output)

    assert failure_code == 1
    assert failure_payload["status"] == "fail"
    assert failure_payload["reason_code"] == "restricted_live_worker_once_proof_failed"
    for forbidden in [
        secret_database_url,
        secret_redis_url,
        secret_token,
        "DATABASE_URL",
        "REDIS_URL",
        "TELEGRAM_BOT_TOKEN",
        "Traceback",
        "RAW_EXCEPTION_SENTINEL",
        "pass" + "word",
    ]:
        assert forbidden not in failure_output


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
        config(dry_run=False, enable_notification_send=True, allow_edits=False),
        app_env="prod",
        database_url="postgresql+psycopg" + "://unit/db",
        redis_url="redis" + "://unit/0",
        telegram_bot_token="unit-live-send-token",
        telegram_api_base_url="https://api.telegram.org",
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
    def __init__(self, *, pending: int = 0, lag: int = 0, metrics_unavailable: bool = False) -> None:
        self.pending = pending
        self.lag = lag
        self.metrics_unavailable = metrics_unavailable
        self.xadd_calls: list[dict[str, Any]] = []
        self.closed = False

    async def xinfo_groups(self, name: str):
        assert name == EXPECTED_QUEUE_NAME
        if self.metrics_unavailable:
            raise RuntimeError("metrics unavailable")
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


class RecordingTelegramClient:
    def __init__(self) -> None:
        self.send_message_calls = 0
        self.edit_message_text_calls = 0
        self.sent_texts: list[str] = []

    async def send_message(self, **kwargs):
        self.send_message_calls += 1
        self.sent_texts.append(str(kwargs.get("text") or ""))
        return {
            "ok": True,
            "description": "RAW_TELEGRAM_RESPONSE_BODY",
            "result": {
                "message_id": 9001,
                "text": "RAW_TELEGRAM_RESPONSE_BODY",
                "chat": {"id": kwargs["chat_id"]},
            },
        }

    async def edit_message_text(self, **kwargs):
        del kwargs
        self.edit_message_text_calls += 1
        raise AssertionError("edit_message_text must not be called")


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

    async def load_restricted_live_worker_once_proof_verification(self, *, notification_plan_id: UUID):
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
            "notification_render_count": sum(
                1 for render in self.renders if render.notification_plan_id == notification_plan_id
            ),
            "notification_delivery_record_count": len(deliveries),
            "delivery_status": latest_delivery.get("result_status"),
            "attempt_count": latest_delivery.get("attempt_count"),
            "transport_error_code": latest_delivery.get("transport_error_code"),
            "telegram_chat_id_present": latest_delivery.get("telegram_chat_id") is not None,
            "telegram_message_id_present": latest_delivery.get("telegram_message_id") is not None,
            "latest_state_transition_to_state": transitions[-1]["to_state"] if transitions else None,
            "latest_state_transition_reason_code": transitions[-1]["reason_code"] if transitions else None,
            "delivery_result_outbox_exists": any(
                row.get("notification_plan_id") == notification_plan_id for row in self.delivery_outbox
            ),
        }
