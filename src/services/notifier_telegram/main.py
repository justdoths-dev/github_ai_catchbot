from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from .config import NotifierTelegramConfig, NotifierTelegramConfigurationError
from .models import NotificationPlanDraft
from .redis_streams import RedisStreamConsumer
from .repositories import NotifierTelegramRepository
from .service import NotifierTelegramService
from .telegram_client import TelegramBotClient
from .worker import NotifierTelegramWorker

CANARY_SCHEMA_VERSION = "notifier_one_shot_canary_v1"
CANARY_PLAN_SCHEMA_VERSION = "notifier_canary_plan_created_v1"
CANARY_PLAN_SEED_VERSION = "operator-canary-plan-v1"
CANARY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._-]{6,80}$")

ONE_SHOT_RUNTIME_CONFIG_REASON_CODES = {
    "env_file_missing",
    "env_file_no_runtime_config",
    "env_file_database_url_file_missing",
    "env_file_database_url_file_empty",
    "env_file_redis_url_file_missing",
    "env_file_redis_url_file_empty",
    "env_file_telegram_bot_token_file_missing",
    "env_file_telegram_bot_token_file_empty",
    "notifier_runtime_config_error",
}

ONE_SHOT_RUNTIME_ENV_VALUE_KEYS = {
    "APP_ENV",
    "DATABASE_URL",
    "REDIS_URL",
    "TELEGRAM_BOT_TOKEN",
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
}
ONE_SHOT_RUNTIME_ENV_FILE_KEYS = {"DATABASE_URL_FILE", "REDIS_URL_FILE", "TELEGRAM_BOT_TOKEN_FILE"}
ONE_SHOT_RUNTIME_ENV_KEYS = ONE_SHOT_RUNTIME_ENV_VALUE_KEYS | ONE_SHOT_RUNTIME_ENV_FILE_KEYS


class _NotifierOneShotRuntimeConfigError(ValueError):
    pass


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("notifier-telegram")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="notifier-telegram")
    subcommands = parser.add_subparsers(dest="command")

    subcommands.add_parser("worker")

    send_canary = subcommands.add_parser("send-canary")
    send_canary.add_argument("--notification-plan-id", required=True)
    send_canary.add_argument("--operator-confirmed", action="store_true")
    send_canary.add_argument("--env-file")
    send_canary.add_argument("--format", choices=["json"], default="json")

    create_canary_plan = subcommands.add_parser("create-canary-plan")
    create_canary_plan.add_argument("--source-notification-plan-id")
    create_canary_plan.add_argument("--canary-key")
    create_canary_plan.add_argument("--operator-confirmed", action="store_true")
    create_canary_plan.add_argument("--env-file")
    create_canary_plan.add_argument("--format", choices=["json"], default="json")
    return parser


async def _run_worker(config: NotifierTelegramConfig) -> int:
    logger = _build_logger(config.log_level)

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)
    consumer = RedisStreamConsumer(
        redis_client,
        queue_name=config.queue_name,
        consumer_group=config.consumer_group,
        consumer_name=config.consumer_name,
        block_ms=config.block_ms,
        batch_size=config.batch_size,
    )
    telegram_client = TelegramBotClient(
        bot_token=config.telegram_bot_token,
        base_url=config.telegram_api_base_url,
        timeout_sec=config.request_timeout_sec,
    )

    class SessionBackedService:
        async def handle_trigger_event(self, trigger_event_id: str):
            async with session_factory.begin() as session:
                repository = NotifierTelegramRepository(session)
                service = NotifierTelegramService(
                    config,
                    repository=repository,
                    telegram_client=telegram_client,
                    logger=logger,
                )
                return await service.handle_trigger_event(trigger_event_id)

    worker = NotifierTelegramWorker(config, consumer=consumer, service=SessionBackedService(), logger=logger)  # type: ignore[arg-type]
    try:
        await worker.run_forever()
    except asyncio.CancelledError:
        logger.info("notifier_telegram_cancelled", extra={"service": "notifier-telegram", "event": "cancelled"})
        return 0
    finally:
        close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close is not None:
            maybe_awaitable = close()
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable
        await engine.dispose()
    return 0


async def _run_send_canary_command(args: argparse.Namespace, *, emit_json=print) -> int:
    if not args.operator_confirmed:
        emit_json(_to_json(_rejected_payload("operator_confirmation_required")))
        return 2

    try:
        notification_plan_id = UUID(str(args.notification_plan_id))
    except (TypeError, ValueError, AttributeError):
        emit_json(_to_json(_rejected_payload("invalid_notification_plan_id")))
        return 2

    try:
        config = _load_notifier_one_shot_runtime_config(args)
    except _NotifierOneShotRuntimeConfigError as exc:
        reason_code = _one_shot_runtime_config_reason_code(exc)
        emit_json(_to_json(_source_or_config_failure_payload(reason_code)))
        return 1

    return await _run_send_canary(config, args, notification_plan_id, emit_json=emit_json)


async def _run_create_canary_plan_command(args: argparse.Namespace, *, emit_json=print) -> int:
    if not args.operator_confirmed:
        emit_json(_to_json(_create_canary_rejected_payload("operator_confirmation_required")))
        return 2

    try:
        source_notification_plan_id = UUID(str(args.source_notification_plan_id))
    except (TypeError, ValueError, AttributeError):
        emit_json(_to_json(_create_canary_rejected_payload("invalid_source_notification_plan_id")))
        return 2

    canary_key = str(args.canary_key or "")
    if not _valid_canary_key(canary_key):
        emit_json(_to_json(_create_canary_rejected_payload("invalid_canary_key")))
        return 2

    if not getattr(args, "env_file", None):
        emit_json(_to_json(_create_canary_rejected_payload("env_file_required")))
        return 2

    try:
        config = _load_notifier_one_shot_runtime_config(args)
    except _NotifierOneShotRuntimeConfigError as exc:
        reason_code = _one_shot_runtime_config_reason_code(exc)
        emit_json(_to_json(_create_canary_failure_payload(reason_code)))
        return 1

    return await _run_create_canary_plan(
        config,
        source_notification_plan_id,
        canary_key,
        emit_json=emit_json,
    )


async def _run_create_canary_plan(
    config: NotifierTelegramConfig,
    source_notification_plan_id: UUID,
    canary_key: str,
    *,
    emit_json=print,
    session_factory_builder=None,
) -> int:
    if session_factory_builder is None:
        session_factory_builder = _build_send_canary_session_factory

    try:
        session_factory, dispose = session_factory_builder(config.database_url)
        try:
            async with session_factory.begin() as session:
                repository = NotifierTelegramRepository(session)
                return await run_create_canary_plan_with_repository(
                    source_notification_plan_id,
                    canary_key,
                    repository,
                    emit_json=emit_json,
                )
        finally:
            await dispose()
    except Exception:
        emit_json(_to_json(_create_canary_failure_payload("notifier_runtime_config_error")))
        return 1


async def run_create_canary_plan_with_repository(
    source_notification_plan_id: UUID,
    canary_key: str,
    repository,
    *,
    emit_json=print,
) -> int:
    result = await create_canary_plan_with_repository(source_notification_plan_id, canary_key, repository)
    if result.reason_code is not None:
        emit_json(_to_json(_create_canary_failure_payload(result.reason_code)))
        return 1
    emit_json(_to_json(_create_canary_success_payload(result)))
    return 0


async def create_canary_plan_with_repository(
    source_notification_plan_id: UUID,
    canary_key: str,
    repository,
) -> "_CreateCanaryPlanResult":
    source_plan = await repository.load_notification_plan(source_notification_plan_id)
    if source_plan is None:
        return _CreateCanaryPlanResult(reason_code="source_notification_plan_missing")
    if str(source_plan.get("delivery_decision") or "") != "send_now":
        return _CreateCanaryPlanResult(reason_code="source_plan_not_send_now")

    source_analysis_id = _uuid_from_row(source_plan.get("analysis_id"))
    source_candidate_group_id = _uuid_from_row(source_plan.get("candidate_group_id"))
    target_chat_id = _int_from_row(source_plan.get("target_chat_id"))
    if source_analysis_id is None:
        return _CreateCanaryPlanResult(reason_code="source_analysis_missing")
    if source_candidate_group_id is None:
        return _CreateCanaryPlanResult(reason_code="canary_plan_conflict")
    if target_chat_id is None:
        return _CreateCanaryPlanResult(reason_code="source_plan_target_chat_missing")

    analysis = await repository.load_analysis(source_analysis_id)
    if analysis is None:
        return _CreateCanaryPlanResult(reason_code="source_analysis_missing")
    if analysis.delivery_decision != "send_now":
        return _CreateCanaryPlanResult(reason_code="source_analysis_not_send_now")
    if analysis.candidate_group_id != source_candidate_group_id:
        return _CreateCanaryPlanResult(reason_code="source_candidate_group_mismatch")

    identity = _canary_identity(
        source_notification_plan_id=source_notification_plan_id,
        canary_key=canary_key,
        analysis_id=source_analysis_id,
        candidate_group_id=source_candidate_group_id,
        target_chat_id=target_chat_id,
    )
    draft = NotificationPlanDraft(
        notification_plan_id=identity.notification_plan_id,
        analysis_id=source_analysis_id,
        candidate_group_id=source_candidate_group_id,
        delivery_decision="send_now",
        urgency_profile=str(source_plan.get("urgency_profile") or "high"),
        target_chat_id=target_chat_id,
        target_thread_id=_int_from_row(source_plan.get("target_thread_id")),
        render_profile=_string_from_row(source_plan.get("render_profile")),
        dedupe_subject_key=identity.dedupe_subject_key,
        material_change_hash=identity.material_change_hash,
        send_after=datetime.now(timezone.utc),
        suppress_reason_code=None,
        status="planned",
    )

    existing_plan = await repository.load_notification_plan(identity.notification_plan_id)
    if existing_plan is not None:
        return await _existing_canary_plan_result(
            repository,
            source_notification_plan_id=source_notification_plan_id,
            source_plan=source_plan,
            expected_draft=draft,
            existing_plan=existing_plan,
        )

    material_existing = await repository.load_existing_plan_by_material(
        analysis_id=draft.analysis_id,
        target_chat_id=draft.target_chat_id,
        material_change_hash=draft.material_change_hash,
    )
    if material_existing is not None:
        return _CreateCanaryPlanResult(reason_code="canary_plan_conflict")

    async with repository.transaction():
        inserted_plan_id = await repository.insert_notification_plan(draft)
    if inserted_plan_id != identity.notification_plan_id:
        return _CreateCanaryPlanResult(reason_code="canary_plan_conflict")

    created_plan = await repository.load_notification_plan(identity.notification_plan_id)
    if created_plan is None:
        return _CreateCanaryPlanResult(reason_code="canary_plan_conflict")
    return await _created_or_existing_canary_plan_result(
        repository,
        status="created",
        source_notification_plan_id=source_notification_plan_id,
        source_plan=source_plan,
        plan=created_plan,
    )


async def _run_send_canary(
    config: NotifierTelegramConfig,
    args: argparse.Namespace,
    notification_plan_id: UUID,
    *,
    emit_json=print,
    session_factory_builder=None,
    telegram_client_builder=TelegramBotClient,
    service_builder=NotifierTelegramService,
) -> int:
    del args
    guard_reason = _config_guard_reason(config)
    if guard_reason is not None:
        emit_json(_to_json(_rejected_payload(guard_reason, notification_plan_id=notification_plan_id)))
        return 2

    if session_factory_builder is None:
        session_factory_builder = _build_send_canary_session_factory

    try:
        session_factory, dispose = session_factory_builder(config.database_url)
        try:
            async with session_factory.begin() as session:
                repository = NotifierTelegramRepository(session)
                return await run_send_canary_with_repository(
                    config,
                    notification_plan_id,
                    repository,
                    emit_json=emit_json,
                    telegram_client_builder=telegram_client_builder,
                    service_builder=service_builder,
                )
        finally:
            await dispose()
    except Exception:
        emit_json(_to_json(_source_or_config_failure_payload("notifier_canary_failed")))
        return 1


async def run_send_canary_with_repository(
    config: NotifierTelegramConfig,
    notification_plan_id: UUID,
    repository,
    *,
    emit_json=print,
    telegram_client_builder=TelegramBotClient,
    service_builder=NotifierTelegramService,
) -> int:
    plan_row = await repository.load_notification_plan(notification_plan_id)
    plan_guard_reason = _plan_guard_reason(plan_row)
    if plan_guard_reason is not None:
        emit_json(_to_json(_rejected_payload(plan_guard_reason, notification_plan_id=notification_plan_id)))
        return 2

    telegram_client = telegram_client_builder(
        bot_token=config.telegram_bot_token,
        base_url=config.telegram_api_base_url,
        timeout_sec=config.request_timeout_sec,
    )
    service = service_builder(
        config,
        repository=repository,
        telegram_client=telegram_client,
        logger=_build_logger(config.log_level),
    )
    try:
        result = await service.handle_notification_plan_canary(notification_plan_id)
    except Exception:
        emit_json(_to_json(_source_or_config_failure_payload("notifier_canary_failed")))
        return 1
    if result is None:
        emit_json(_to_json(_source_or_config_failure_payload("notifier_canary_failed")))
        return 1

    payload = _delivery_result_payload(notification_plan_id, result)
    emit_json(_to_json(payload))
    if result.delivery_status in {"sent", "edited"}:
        return 0
    if result.delivery_status in {"failed_retryable", "failed_terminal"}:
        return 1
    return 2


def _config_guard_reason(config: NotifierTelegramConfig) -> str | None:
    if not config.enable_notification_send:
        return "notification_send_disabled"
    if config.dry_run:
        return "notifier_dry_run_enabled"
    if not config.telegram_bot_token:
        return "telegram_bot_token_missing"
    return None


def _plan_guard_reason(plan_row: Mapping[str, object] | None) -> str | None:
    if plan_row is None:
        return "notification_plan_not_found"
    if str(plan_row.get("delivery_decision") or "") != "send_now":
        return "notification_plan_not_send_now"
    if str(plan_row.get("status") or "") in {"sent", "edited"}:
        return "notification_plan_already_delivered"
    send_after = plan_row.get("send_after")
    if send_after is not None:
        from datetime import datetime, timezone

        if isinstance(send_after, datetime):
            due_at = send_after if send_after.tzinfo else send_after.replace(tzinfo=timezone.utc)
            if due_at.astimezone(timezone.utc) > datetime.now(timezone.utc):
                return "notification_plan_not_due"
    return None


def _delivery_result_payload(notification_plan_id: UUID, result) -> dict:
    if result.delivery_status in {"sent", "edited"}:
        return {
            "schema_version": CANARY_SCHEMA_VERSION,
            "status": "sent",
            "notification_plan_id": str(notification_plan_id),
            "delivery_status": result.delivery_status,
            "telegram_chat_id_present": result.telegram_chat_id is not None,
            "telegram_message_id_present": result.telegram_message_id is not None,
        }
    if result.delivery_status == "failed_retryable":
        return _source_or_config_failure_payload(
            "telegram_retryable",
            status="failed_retryable",
            notification_plan_id=notification_plan_id,
        )
    if result.delivery_status == "failed_terminal":
        return _source_or_config_failure_payload(
            "telegram_terminal",
            status="failed_terminal",
            notification_plan_id=notification_plan_id,
        )
    return _source_or_config_failure_payload(
        str(result.transport_error_code or "notifier_canary_failed"),
        notification_plan_id=notification_plan_id,
    )


@dataclass(slots=True, frozen=True)
class _CanaryIdentity:
    notification_plan_id: UUID
    dedupe_subject_key: str
    material_change_hash: str


@dataclass(slots=True, frozen=True)
class _CreateCanaryPlanResult:
    status: str | None = None
    notification_plan_id: UUID | None = None
    source_notification_plan_id: UUID | None = None
    source_plan_status: str | None = None
    delivery_decision: str | None = None
    plan_status: str | None = None
    send_after_due: bool = False
    target_chat_id_present: bool = False
    ready_for_send_canary: bool = False
    reason_code: str | None = None


def _valid_canary_key(canary_key: str) -> bool:
    return bool(CANARY_KEY_PATTERN.fullmatch(canary_key))


def _canary_identity(
    *,
    source_notification_plan_id: UUID,
    canary_key: str,
    analysis_id: UUID,
    candidate_group_id: UUID,
    target_chat_id: int,
) -> _CanaryIdentity:
    seed = (
        f"{CANARY_PLAN_SEED_VERSION}|{source_notification_plan_id}|{canary_key}|"
        f"{analysis_id}|{candidate_group_id}|{target_chat_id}"
    )
    return _CanaryIdentity(
        notification_plan_id=uuid5(NAMESPACE_URL, seed),
        dedupe_subject_key=f"operator-canary:{source_notification_plan_id}:{canary_key}",
        material_change_hash=hashlib.sha256(seed.encode("utf-8")).hexdigest(),
    )


async def _existing_canary_plan_result(
    repository,
    *,
    source_notification_plan_id: UUID,
    source_plan: Mapping[str, object],
    expected_draft: NotificationPlanDraft,
    existing_plan: Mapping[str, object],
) -> _CreateCanaryPlanResult:
    if not _plan_matches_expected_canary(existing_plan, expected_draft):
        return _CreateCanaryPlanResult(reason_code="canary_plan_conflict")
    return await _created_or_existing_canary_plan_result(
        repository,
        status="existing",
        source_notification_plan_id=source_notification_plan_id,
        source_plan=source_plan,
        plan=existing_plan,
    )


async def _created_or_existing_canary_plan_result(
    repository,
    *,
    status: str,
    source_notification_plan_id: UUID,
    source_plan: Mapping[str, object],
    plan: Mapping[str, object],
) -> _CreateCanaryPlanResult:
    notification_plan_id = _uuid_from_row(plan.get("notification_plan_id"))
    target_chat_id = _int_from_row(plan.get("target_chat_id"))
    dedupe_subject_key = str(plan.get("dedupe_subject_key") or "")
    material_change_hash = str(plan.get("material_change_hash") or "")
    plan_status = str(plan.get("status") or "")
    delivery_decision = str(plan.get("delivery_decision") or "")
    send_after_due = _send_after_due(plan.get("send_after"))
    target_chat_id_present = target_chat_id is not None
    already_delivered = plan_status in {"sent", "edited"}

    if target_chat_id is not None and dedupe_subject_key and material_change_hash:
        successful_delivery = await repository.load_successful_delivery_for_material(
            dedupe_subject_key=dedupe_subject_key,
            target_chat_id=target_chat_id,
            material_change_hash=material_change_hash,
        )
        already_delivered = already_delivered or successful_delivery is not None

    ready_for_send_canary = (
        not already_delivered
        and notification_plan_id is not None
        and delivery_decision == "send_now"
        and plan_status == "planned"
        and send_after_due
        and target_chat_id_present
        and _plan_guard_reason(plan) is None
    )
    result_status = "existing_already_delivered" if already_delivered else status
    return _CreateCanaryPlanResult(
        status=result_status,
        notification_plan_id=notification_plan_id,
        source_notification_plan_id=source_notification_plan_id,
        source_plan_status=_string_from_row(source_plan.get("status")),
        delivery_decision=delivery_decision,
        plan_status=plan_status,
        send_after_due=send_after_due,
        target_chat_id_present=target_chat_id_present,
        ready_for_send_canary=ready_for_send_canary,
    )


def _plan_matches_expected_canary(plan: Mapping[str, object], draft: NotificationPlanDraft) -> bool:
    return (
        _uuid_from_row(plan.get("notification_plan_id")) == draft.notification_plan_id
        and _uuid_from_row(plan.get("analysis_id")) == draft.analysis_id
        and _uuid_from_row(plan.get("candidate_group_id")) == draft.candidate_group_id
        and str(plan.get("delivery_decision") or "") == draft.delivery_decision
        and str(plan.get("urgency_profile") or "") == draft.urgency_profile
        and _int_from_row(plan.get("target_chat_id")) == draft.target_chat_id
        and _int_from_row(plan.get("target_thread_id")) == draft.target_thread_id
        and _string_from_row(plan.get("render_profile")) == draft.render_profile
        and str(plan.get("dedupe_subject_key") or "") == draft.dedupe_subject_key
        and str(plan.get("material_change_hash") or "") == draft.material_change_hash
        and _string_from_row(plan.get("suppress_reason_code")) is None
    )


def _send_after_due(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, datetime):
        due_at = value
    else:
        try:
            due_at = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return False
    if due_at.tzinfo is None:
        due_at = due_at.replace(tzinfo=timezone.utc)
    return due_at.astimezone(timezone.utc) <= datetime.now(timezone.utc)


def _uuid_from_row(value: object) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _int_from_row(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _string_from_row(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _create_canary_success_payload(result: _CreateCanaryPlanResult) -> dict:
    payload = {
        "schema_version": CANARY_PLAN_SCHEMA_VERSION,
        "status": result.status,
        "notification_plan_id": str(result.notification_plan_id),
        "source_notification_plan_id": str(result.source_notification_plan_id),
        "delivery_decision": result.delivery_decision,
        "plan_status": result.plan_status,
        "send_after_due": result.send_after_due,
        "target_chat_id_present": result.target_chat_id_present,
        "ready_for_send_canary": result.ready_for_send_canary,
    }
    if result.status == "created":
        payload["source_plan_status"] = result.source_plan_status
    return payload


def _create_canary_rejected_payload(reason_code: str) -> dict:
    return {
        "schema_version": CANARY_PLAN_SCHEMA_VERSION,
        "status": "rejected",
        "reason_code": reason_code,
    }


def _create_canary_failure_payload(reason_code: str) -> dict:
    return {
        "schema_version": CANARY_PLAN_SCHEMA_VERSION,
        "status": "fail",
        "reason_code": reason_code,
        "warnings": [reason_code],
    }


def _load_notifier_one_shot_runtime_config(
    args: argparse.Namespace,
    *,
    env_file_overlay: dict[str, str] | None = None,
) -> NotifierTelegramConfig:
    env_file = getattr(args, "env_file", None)
    if not env_file:
        raise _NotifierOneShotRuntimeConfigError("env_file_no_runtime_config")

    overlay = env_file_overlay if env_file_overlay is not None else _resolve_one_shot_runtime_env_file_overlay(env_file)
    try:
        with _temporary_environment_defaults(overlay):
            return NotifierTelegramConfig.from_env(require_transport_token=False)
    except (NotifierTelegramConfigurationError, ValueError, TypeError):
        raise _NotifierOneShotRuntimeConfigError("notifier_runtime_config_error") from None


def _resolve_one_shot_runtime_env_file_overlay(
    env_file: str,
    *,
    process_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    try:
        values = _read_minimal_env_file(env_file, allowed_keys=ONE_SHOT_RUNTIME_ENV_KEYS)
    except ValueError as exc:
        raise _NotifierOneShotRuntimeConfigError(_one_shot_runtime_config_reason_code(exc)) from None

    if not values:
        raise _NotifierOneShotRuntimeConfigError("env_file_no_runtime_config")

    source = os.environ if process_env is None else process_env
    overlay: dict[str, str] = {}
    for key in ONE_SHOT_RUNTIME_ENV_VALUE_KEYS:
        value = values.get(key, "").strip()
        if value and key not in source:
            overlay[key] = value

    _resolve_one_shot_runtime_file_value(
        values,
        source=source,
        overlay=overlay,
        file_key="DATABASE_URL_FILE",
        target_key="DATABASE_URL",
        missing_reason_code="env_file_database_url_file_missing",
        empty_reason_code="env_file_database_url_file_empty",
    )
    _resolve_one_shot_runtime_file_value(
        values,
        source=source,
        overlay=overlay,
        file_key="REDIS_URL_FILE",
        target_key="REDIS_URL",
        missing_reason_code="env_file_redis_url_file_missing",
        empty_reason_code="env_file_redis_url_file_empty",
    )
    _resolve_one_shot_runtime_file_value(
        values,
        source=source,
        overlay=overlay,
        file_key="TELEGRAM_BOT_TOKEN_FILE",
        target_key="TELEGRAM_BOT_TOKEN",
        missing_reason_code="env_file_telegram_bot_token_file_missing",
        empty_reason_code="env_file_telegram_bot_token_file_empty",
    )
    return overlay


def _resolve_one_shot_runtime_file_value(
    values: Mapping[str, str],
    *,
    source: Mapping[str, str],
    overlay: dict[str, str],
    file_key: str,
    target_key: str,
    missing_reason_code: str,
    empty_reason_code: str,
) -> None:
    if target_key in source or target_key in overlay or file_key not in values:
        return
    try:
        overlay[target_key] = _read_runtime_secret_file(
            values.get(file_key, "").strip(),
            missing_reason_code=missing_reason_code,
            empty_reason_code=empty_reason_code,
        )
    except ValueError as exc:
        raise _NotifierOneShotRuntimeConfigError(_one_shot_runtime_config_reason_code(exc)) from None


def _read_minimal_env_file(env_file: str, *, allowed_keys: set[str]) -> dict[str, str]:
    env_path = Path(env_file)
    if not env_path.is_file():
        raise ValueError("env_file_missing")
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        raise ValueError("env_file_missing") from None

    values: dict[str, str] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in allowed_keys:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def _read_runtime_secret_file(
    path: str,
    *,
    missing_reason_code: str,
    empty_reason_code: str,
) -> str:
    value_path = Path(path)
    if not value_path.is_file():
        raise ValueError(missing_reason_code)
    try:
        value = value_path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        raise ValueError(missing_reason_code) from None
    if not value:
        raise ValueError(empty_reason_code)
    return value


@contextmanager
def _temporary_environment_defaults(values: Mapping[str, str]):
    added: list[str] = []
    try:
        for key, value in values.items():
            if key in os.environ:
                continue
            os.environ[key] = value
            added.append(key)
        yield
    finally:
        for key in reversed(added):
            os.environ.pop(key, None)


def _one_shot_runtime_config_reason_code(exc: ValueError) -> str:
    reason_code = str(exc)
    if reason_code in ONE_SHOT_RUNTIME_CONFIG_REASON_CODES:
        return reason_code
    return "notifier_runtime_config_error"


def _build_send_canary_session_factory(database_url: str):
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def dispose() -> None:
        await engine.dispose()

    return session_factory, dispose


def _rejected_payload(reason_code: str, *, notification_plan_id: UUID | None = None) -> dict:
    payload = {
        "schema_version": CANARY_SCHEMA_VERSION,
        "status": "rejected",
        "reason_code": reason_code,
    }
    if notification_plan_id is not None:
        payload["notification_plan_id"] = str(notification_plan_id)
    return payload


def _source_or_config_failure_payload(
    reason_code: str,
    *,
    status: str = "fail",
    notification_plan_id: UUID | None = None,
) -> dict:
    payload = {
        "schema_version": CANARY_SCHEMA_VERSION,
        "status": status,
        "reason_code": reason_code,
        "warnings": [reason_code],
    }
    if notification_plan_id is not None:
        payload["notification_plan_id"] = str(notification_plan_id)
    return payload


def _to_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, indent=2, sort_keys=True)


async def _run(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command or "worker"
    if command == "create-canary-plan":
        return await _run_create_canary_plan_command(args)
    if command == "send-canary":
        return await _run_send_canary_command(args)
    if command == "worker":
        config = NotifierTelegramConfig.from_env()
        return await _run_worker(config)
    parser.error(f"unsupported command: {command}")
    return 2


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
