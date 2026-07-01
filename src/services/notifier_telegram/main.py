from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
from collections.abc import Awaitable, Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import NAMESPACE_URL, UUID, uuid5

from .config import NotifierTelegramConfig, NotifierTelegramConfigurationError
from .models import NotificationPlanDraft
from .redis_streams import RedisStreamConsumer
from .repositories import NotifierTelegramRepository
from .restricted_transport_canary import run_restricted_transport_canary
from .renderer import NotificationRenderer, RenderInput
from .service import NotifierTelegramService
from .telegram_client import TelegramBotClient
from .worker import NotifierTelegramWorker
from .worker_once import EXPECTED_QUEUE_NAME, EXPECTED_STAGE_NAME, run_worker_once_invocation

CANARY_SCHEMA_VERSION = "notifier_one_shot_canary_v1"
CANARY_PLAN_SCHEMA_VERSION = "notifier_canary_plan_created_v1"
NOTIFICATION_UX_RENDER_PREVIEW_SCHEMA_VERSION = "notification_ux_render_preview_v1"
CANARY_PLAN_SEED_VERSION = "operator-canary-plan-v1"
CANARY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._-]{6,80}$")
SEND_DISABLED_PROOF_SCHEMA_VERSION = "notifier_send_disabled_worker_once_proof_v1"
SEND_DISABLED_PROOF_SEED_VERSION = "notifier-send-disabled-worker-once-proof-v1"
SEND_DISABLED_PROOF_KEY_PATTERN = CANARY_KEY_PATTERN
SEND_DISABLED_PROOF_REASON_CODE = "notification_send_flag_disabled"
RESTRICTED_LIVE_PROOF_SCHEMA_VERSION = "notifier_restricted_live_worker_once_proof_v1"
RESTRICTED_LIVE_PROOF_SEED_VERSION = "notifier-restricted-live-worker-once-proof-v1"
RESTRICTED_LIVE_PROOF_KEY_PATTERN = CANARY_KEY_PATTERN
RESTRICTED_LIVE_QUEUED_WORKER_ONCE_SCHEMA_VERSION = "notifier_restricted_live_queued_worker_once_v1"
RESTRICTED_LIVE_QUEUED_WORKER_ONCE_DEFAULT_MAX_LAG = 1
NOTIFICATION_UX_SECRET_WORDS = (
    "secret",
    "runtime.env",
    "DATABASE_URL",
    "REDIS_URL",
    "OPENAI_API_KEY",
    "TELEGRAM_BOT_TOKEN",
)
NOTIFICATION_UX_URL_RE = re.compile(r"https?://[^\s<>)\"']+")
NOTIFICATION_UX_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
SEND_DISABLED_PROOF_SETUP_REJECTION_REASONS = {
    "source_notification_plan_missing",
    "source_plan_not_send_now",
    "source_analysis_missing",
    "source_candidate_group_missing",
    "source_plan_target_chat_missing",
    "proof_notification_plan_exists",
    "proof_plan_created_event_exists",
    "proof_notification_plan_conflict",
    "proof_plan_created_event_conflict",
}
RESTRICTED_LIVE_PROOF_SETUP_REJECTION_REASONS = SEND_DISABLED_PROOF_SETUP_REJECTION_REASONS | {
    "source_analysis_not_send_now",
    "source_candidate_group_mismatch",
}

ONE_SHOT_RUNTIME_CONFIG_REASON_CODES = {
    "env_file_missing",
    "env_file_no_runtime_config",
    "env_file_database_url_file_missing",
    "env_file_database_url_file_empty",
    "env_file_redis_url_file_missing",
    "env_file_redis_url_file_empty",
    "env_file_telegram_bot_token_file_missing",
    "env_file_telegram_bot_token_file_empty",
    "runtime_config_not_found",
    "runtime_database_config_not_found",
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
NOTIFICATION_UX_DB_RUNTIME_ENV_KEYS = {
    "DATABASE_URL",
    "DATABASE_URL_FILE",
    "NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS",
}
NOTIFICATION_UX_DB_RUNTIME_CANDIDATE_FILE_NAMES = {
    "runtime.env",
    ".env",
    ".env.prod",
    "prod.env",
    "production.env",
    "catchbot.env",
    "github_ai_catchbot.env",
    "github_ai_catchbot.runtime.env",
}
NOTIFICATION_UX_DB_RUNTIME_DISCOVERY_SKIP_DIRS = {
    ".git",
    "venv",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    "node_modules",
    "logs",
    "log",
}
NOTIFICATION_UX_DB_RUNTIME_DISCOVERY_MAX_DEPTH = 7
NOTIFICATION_UX_DB_RUNTIME_DISCOVERY_MAX_FILES = 80
NOTIFICATION_UX_PREVIEW_DEFAULT_MAX_MESSAGE_CHARS = 3800


class _NotifierOneShotRuntimeConfigError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class _NotificationUxPreviewRuntimeConfig:
    database_url: str
    max_message_chars: int


@dataclass(slots=True, frozen=True)
class _NotificationUxPreviewRuntimeConfigResult:
    config: _NotificationUxPreviewRuntimeConfig
    locator: dict[str, Any]


@dataclass(slots=True, frozen=True)
class _RestrictedLiveQueuedRuntimeConfigResult:
    config: NotifierTelegramConfig
    locator: dict[str, Any]


class _RestrictedLiveQueuedRuntimeConfigDiscoveryError(_NotifierOneShotRuntimeConfigError):
    def __init__(self, reason_code: str, locator: Mapping[str, Any]) -> None:
        super().__init__(reason_code)
        self.locator = dict(locator)


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("notifier-telegram")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="notifier-telegram")
    subcommands = parser.add_subparsers(dest="command")

    subcommands.add_parser("worker")

    worker_once = subcommands.add_parser("worker-once")
    worker_once.add_argument("--queue")
    worker_once.add_argument("--confirm-worker-once", action="store_true")
    worker_once.add_argument("--format", default="json")

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

    notification_ux_preview = subcommands.add_parser("notification-ux-render-preview")
    notification_ux_selector = notification_ux_preview.add_mutually_exclusive_group(required=True)
    notification_ux_selector.add_argument("--notification-plan-id")
    notification_ux_selector.add_argument("--select-latest-eligible", action="store_true")
    notification_ux_preview.add_argument("--env-file")
    notification_ux_preview.add_argument("--discover-db-config", action="store_true")
    notification_ux_preview.add_argument("--format", choices=["json"], default="json")

    send_disabled_proof = subcommands.add_parser("send-disabled-worker-once-proof")
    send_disabled_proof.add_argument("--source-notification-plan-id", required=True)
    send_disabled_proof.add_argument("--proof-key", required=True)
    send_disabled_proof.add_argument("--operator-confirmed", action="store_true")
    send_disabled_proof.add_argument("--env-file")
    send_disabled_proof.add_argument("--format", choices=["json"], default="json")

    restricted_live_proof = subcommands.add_parser("restricted-live-worker-once-proof")
    restricted_live_proof.add_argument("--source-notification-plan-id", required=True)
    restricted_live_proof.add_argument("--proof-key", required=True)
    restricted_live_proof.add_argument("--operator-confirmed", action="store_true")
    restricted_live_proof.add_argument("--env-file")
    restricted_live_proof.add_argument("--format", choices=["json"], default="json")

    restricted_live_queued = subcommands.add_parser("restricted-live-queued-worker-once")
    restricted_live_queued.add_argument("--operator-confirmed", action="store_true")
    restricted_live_queued.add_argument("--env-file")
    restricted_live_queued.add_argument("--discover-runtime-config", action="store_true")
    restricted_live_queued.add_argument("--send-only-runtime-projection", action="store_true")
    restricted_live_queued.add_argument(
        "--max-lag",
        type=int,
        default=RESTRICTED_LIVE_QUEUED_WORKER_ONCE_DEFAULT_MAX_LAG,
    )
    restricted_live_queued.add_argument("--format", choices=["json"], default="json")

    restricted_transport_canary = subcommands.add_parser("restricted-transport-canary")
    restricted_transport_canary.add_argument("--target-chat-id", required=True)
    restricted_transport_canary.add_argument("--message", required=True)
    restricted_transport_canary.add_argument("--confirm-send", action="store_true")
    restricted_transport_canary.add_argument("--format", choices=["json"], default="json")
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


async def _run_send_disabled_worker_once_proof_command(args: argparse.Namespace, *, emit_json=print) -> int:
    if not args.operator_confirmed:
        emit_json(_to_json(_send_disabled_proof_rejected_payload("operator_confirmation_required")))
        return 2

    try:
        source_notification_plan_id = UUID(str(args.source_notification_plan_id))
    except (TypeError, ValueError, AttributeError):
        emit_json(_to_json(_send_disabled_proof_rejected_payload("invalid_source_notification_plan_id")))
        return 2

    proof_key = str(args.proof_key or "")
    if not _valid_send_disabled_proof_key(proof_key):
        emit_json(_to_json(_send_disabled_proof_rejected_payload("invalid_proof_key")))
        return 2

    if not getattr(args, "env_file", None):
        emit_json(_to_json(_send_disabled_proof_rejected_payload("env_file_required")))
        return 2

    try:
        config = _load_notifier_one_shot_runtime_config(args)
    except _NotifierOneShotRuntimeConfigError as exc:
        reason_code = _one_shot_runtime_config_reason_code(exc)
        emit_json(_to_json(_send_disabled_proof_failure_payload(reason_code)))
        return 1

    return await _run_send_disabled_worker_once_proof(
        config,
        source_notification_plan_id,
        proof_key,
        emit_json=emit_json,
    )


async def _run_restricted_live_worker_once_proof_command(args: argparse.Namespace, *, emit_json=print) -> int:
    if not args.operator_confirmed:
        emit_json(_to_json(_restricted_live_proof_rejected_payload("operator_confirmation_required")))
        return 2

    try:
        source_notification_plan_id = UUID(str(args.source_notification_plan_id))
    except (TypeError, ValueError, AttributeError):
        emit_json(_to_json(_restricted_live_proof_rejected_payload("invalid_source_notification_plan_id")))
        return 2

    proof_key = str(args.proof_key or "")
    if not _valid_restricted_live_proof_key(proof_key):
        emit_json(_to_json(_restricted_live_proof_rejected_payload("invalid_proof_key")))
        return 2

    if not getattr(args, "env_file", None):
        emit_json(_to_json(_restricted_live_proof_rejected_payload("env_file_required")))
        return 2

    try:
        config = _load_notifier_one_shot_runtime_config(args)
    except _NotifierOneShotRuntimeConfigError as exc:
        reason_code = _one_shot_runtime_config_reason_code(exc)
        emit_json(_to_json(_restricted_live_proof_failure_payload(reason_code)))
        return 1

    return await _run_restricted_live_worker_once_proof(
        config,
        source_notification_plan_id,
        proof_key,
        emit_json=emit_json,
    )


async def _run_restricted_live_queued_worker_once_command(args: argparse.Namespace, *, emit_json=print) -> int:
    if not args.operator_confirmed:
        emit_json(_to_json(_restricted_live_queued_worker_once_rejected_payload("operator_confirmation_required")))
        return 2

    discover_runtime_config = bool(getattr(args, "discover_runtime_config", False))
    if not getattr(args, "env_file", None) and not discover_runtime_config:
        emit_json(_to_json(_restricted_live_queued_worker_once_rejected_payload("env_file_required")))
        return 2

    max_lag = getattr(args, "max_lag", RESTRICTED_LIVE_QUEUED_WORKER_ONCE_DEFAULT_MAX_LAG)
    if not isinstance(max_lag, int) or max_lag < 1:
        emit_json(_to_json(_restricted_live_queued_worker_once_rejected_payload("invalid_max_lag")))
        return 2

    runtime_config_locator: dict[str, Any] | None = None
    try:
        if discover_runtime_config:
            config_result = _load_restricted_live_queued_runtime_config(args)
            config = config_result.config
            runtime_config_locator = config_result.locator
        else:
            config = _load_notifier_one_shot_runtime_config(args)
    except _RestrictedLiveQueuedRuntimeConfigDiscoveryError as exc:
        reason_code = _one_shot_runtime_config_reason_code(exc)
        emit_json(
            _to_json(
                _restricted_live_queued_worker_once_rejected_payload(
                    reason_code,
                    runtime_config_locator=exc.locator,
                )
            )
        )
        return 2
    except _NotifierOneShotRuntimeConfigError as exc:
        reason_code = _one_shot_runtime_config_reason_code(exc)
        emit_json(_to_json(_restricted_live_queued_worker_once_rejected_payload(reason_code)))
        return 2

    runtime_projection: dict[str, Any] | None = None
    if getattr(args, "send_only_runtime_projection", False):
        config, runtime_projection = _restricted_live_queued_send_only_projection(config)

    return await _run_restricted_live_queued_worker_once(
        config,
        max_lag=max_lag,
        emit_json=emit_json,
        runtime_config_locator=runtime_config_locator,
        runtime_projection=runtime_projection,
    )


async def _run_restricted_transport_canary_command(args: argparse.Namespace, *, emit_json=print) -> int:
    return await run_restricted_transport_canary(
        target_chat_id=args.target_chat_id,
        message=args.message,
        confirm_send=args.confirm_send,
        emit_json=emit_json,
    )


async def _run_notification_ux_render_preview_command(
    args: argparse.Namespace,
    *,
    emit_json=print,
    session_factory_builder=None,
    repository_builder=NotifierTelegramRepository,
) -> int:
    select_latest_eligible = bool(getattr(args, "select_latest_eligible", False))
    notification_plan_id: UUID | None = None
    if not select_latest_eligible:
        try:
            notification_plan_id = UUID(str(args.notification_plan_id))
        except (TypeError, ValueError, AttributeError):
            emit_json(
                _to_json(
                    _notification_ux_preview_payload(
                        "invalid_notification_plan_id",
                        db_read_attempted=False,
                    )
                )
            )
            return 2

    try:
        config_result = _load_notification_ux_preview_config(args)
    except _NotifierOneShotRuntimeConfigError as exc:
        reason_code = _one_shot_runtime_config_reason_code(exc)
        emit_json(
            _to_json(
                _notification_ux_preview_payload(
                    reason_code,
                    db_read_attempted=False,
                    selection=_notification_ux_preview_selection_payload(
                        latest=select_latest_eligible,
                        selected=False,
                    ),
                    runtime_config_locator=_notification_ux_preview_runtime_config_locator_from_args(args),
                )
            )
        )
        return 1

    return await _run_notification_ux_render_preview(
        config_result.config,
        notification_plan_id,
        emit_json=emit_json,
        session_factory_builder=session_factory_builder,
        repository_builder=repository_builder,
        select_latest_eligible=select_latest_eligible,
        runtime_config_locator=config_result.locator,
    )


async def _run_notification_ux_render_preview(
    config: Any,
    notification_plan_id: UUID | None,
    *,
    emit_json=print,
    session_factory_builder=None,
    repository_builder=NotifierTelegramRepository,
    select_latest_eligible: bool = False,
    runtime_config_locator: Mapping[str, Any] | None = None,
) -> int:
    if session_factory_builder is None:
        session_factory_builder = _build_send_canary_session_factory

    try:
        session_factory, dispose = session_factory_builder(config.database_url)
        try:
            async with session_factory.begin() as session:
                repository = repository_builder(session)
                if select_latest_eligible:
                    return await run_latest_eligible_notification_ux_render_preview_with_repository(
                        repository,
                        renderer=NotificationRenderer(max_message_chars=config.max_message_chars),
                        emit_json=emit_json,
                        runtime_config_locator=runtime_config_locator,
                    )
                if notification_plan_id is None:
                    emit_json(
                        _to_json(
                            _notification_ux_preview_payload(
                                "invalid_notification_plan_id",
                                db_read_attempted=False,
                                runtime_config_locator=runtime_config_locator,
                            )
                        )
                    )
                    return 2
                return await run_notification_ux_render_preview_with_repository(
                    notification_plan_id,
                    repository,
                    renderer=NotificationRenderer(max_message_chars=config.max_message_chars),
                    emit_json=emit_json,
                    runtime_config_locator=runtime_config_locator,
                )
        finally:
            await dispose()
    except Exception:
        emit_json(
            _to_json(
                _notification_ux_preview_payload(
                    "db_read_failed",
                    selection=_notification_ux_preview_selection_payload(
                        latest=select_latest_eligible,
                        selected=False,
                    ),
                    runtime_config_locator=runtime_config_locator,
                )
            )
        )
        return 1


async def run_notification_ux_render_preview_with_repository(
    notification_plan_id: UUID,
    repository,
    *,
    renderer: NotificationRenderer | None = None,
    emit_json=print,
    runtime_config_locator: Mapping[str, Any] | None = None,
) -> int:
    payload = await build_notification_ux_render_preview_payload(
        notification_plan_id,
        repository,
        renderer=renderer,
        runtime_config_locator=runtime_config_locator,
    )
    emit_json(_to_json(payload))
    return 0 if payload["status"] == "pass" else 1


async def run_latest_eligible_notification_ux_render_preview_with_repository(
    repository,
    *,
    renderer: NotificationRenderer | None = None,
    emit_json=print,
    runtime_config_locator: Mapping[str, Any] | None = None,
) -> int:
    plan = await _load_latest_eligible_notification_plan(repository)
    if plan is None:
        payload = _notification_ux_preview_payload(
            "no_eligible_notification_plan_found",
            selection=_notification_ux_preview_selection_payload(latest=True, selected=False),
            runtime_config_locator=runtime_config_locator,
        )
        emit_json(_to_json(payload))
        return 1
    notification_plan_id = _uuid_from_row(plan.get("notification_plan_id"))
    if notification_plan_id is None:
        payload = _notification_ux_preview_payload(
            "no_eligible_notification_plan_found",
            selection=_notification_ux_preview_selection_payload(latest=True, selected=False),
            runtime_config_locator=runtime_config_locator,
        )
        emit_json(_to_json(payload))
        return 1
    payload = await build_notification_ux_render_preview_payload(
        notification_plan_id,
        repository,
        renderer=renderer,
        preloaded_plan=plan,
        selection=_notification_ux_preview_selection_payload(latest=True, selected=True),
        runtime_config_locator=runtime_config_locator,
    )
    emit_json(_to_json(payload))
    return 0 if payload["status"] == "pass" else 1


async def build_notification_ux_render_preview_payload(
    notification_plan_id: UUID,
    repository,
    *,
    renderer: NotificationRenderer | None = None,
    preloaded_plan: Mapping[str, Any] | None = None,
    selection: Mapping[str, Any] | None = None,
    runtime_config_locator: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    renderer = renderer or NotificationRenderer()
    plan = dict(preloaded_plan) if preloaded_plan is not None else await repository.load_notification_plan(notification_plan_id)
    if plan is None:
        return _notification_ux_preview_payload(
            "notification_plan_missing",
            checks=_notification_ux_preview_base_checks(plan_found=False),
            selection=selection,
            runtime_config_locator=runtime_config_locator,
        )

    analysis_id = _uuid_from_row(plan.get("analysis_id"))
    candidate_group_id = _uuid_from_row(plan.get("candidate_group_id"))
    urgency_profile = str(plan.get("urgency_profile") or "")
    if analysis_id is None:
        return _notification_ux_preview_payload(
            "analysis_id_missing",
            selection=selection,
            runtime_config_locator=runtime_config_locator,
        )
    if candidate_group_id is None:
        return _notification_ux_preview_payload(
            "candidate_group_id_missing",
            selection=selection,
            runtime_config_locator=runtime_config_locator,
        )

    analysis = await repository.load_analysis(analysis_id)
    if analysis is None:
        return _notification_ux_preview_payload(
            "analysis_missing",
            checks=_notification_ux_preview_base_checks(analysis_found=False),
            selection=selection,
            runtime_config_locator=runtime_config_locator,
        )
    if analysis.candidate_group_id != candidate_group_id:
        return _notification_ux_preview_payload(
            "analysis_candidate_group_mismatch",
            selection=selection,
            runtime_config_locator=runtime_config_locator,
        )

    judge_output = await repository.load_judge_output_render_fields(analysis.judge_output_id)
    if judge_output is None:
        return _notification_ux_preview_payload(
            "judge_output_missing",
            checks=_notification_ux_preview_base_checks(judge_output_found=False),
            selection=selection,
            runtime_config_locator=runtime_config_locator,
        )

    candidate = await repository.load_candidate_render_context(candidate_group_id)
    if candidate is None:
        return _notification_ux_preview_payload(
            "candidate_missing",
            checks=_notification_ux_preview_base_checks(candidate_found=False),
            selection=selection,
            runtime_config_locator=runtime_config_locator,
        )

    render = renderer.render(
        notification_plan_id=notification_plan_id,
        payload=RenderInput(
            analysis=analysis,
            judge_output=judge_output,
            candidate=candidate,
            urgency_profile=urgency_profile,
        ),
    )
    checks = _notification_ux_render_checks(
        message_text=render.message_text,
        render=render,
        verdict=analysis.verdict,
        urgency_profile=urgency_profile,
        candidate=candidate,
    )
    failed = [name for name, passed in checks.items() if not passed]
    payload = {
        "schema_version": NOTIFICATION_UX_RENDER_PREVIEW_SCHEMA_VERSION,
        "status": "pass" if not failed else "fail",
        "reason_code": "ok" if not failed else "render_preview_checks_failed",
        "checks_failed": failed,
        "checks": checks,
        "render_summary": _notification_ux_render_summary(render),
        "authority": _notification_ux_preview_authority(),
    }
    if selection is not None:
        payload["selection"] = dict(selection)
    if runtime_config_locator is not None:
        payload["runtime_config_locator"] = dict(runtime_config_locator)
    return payload


async def _run_worker_once_command(args: argparse.Namespace, *, emit_json=print) -> int:
    return await run_worker_once_invocation(
        queue=args.queue,
        confirm_worker_once=args.confirm_worker_once,
        output_format=args.format,
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
        await repository.insert_notification_plan(draft)

    created_plan = await repository.load_notification_plan(identity.notification_plan_id)
    if created_plan is None or not _plan_matches_expected_canary(created_plan, draft):
        return _CreateCanaryPlanResult(reason_code="canary_plan_conflict")
    return await _created_or_existing_canary_plan_result(
        repository,
        status="created",
        source_notification_plan_id=source_notification_plan_id,
        source_plan=source_plan,
        plan=created_plan,
    )


async def _run_send_disabled_worker_once_proof(
    config: NotifierTelegramConfig,
    source_notification_plan_id: UUID,
    proof_key: str,
    *,
    emit_json=print,
    session_factory_builder=None,
    redis_client_builder=None,
    repository_builder=NotifierTelegramRepository,
    worker_once_runner: Callable[[NotifierTelegramConfig, Callable[[str], None]], Awaitable[int]] | None = None,
) -> int:
    guard_reason = _send_disabled_proof_config_guard_reason(config)
    if guard_reason is not None:
        emit_json(_to_json(_send_disabled_proof_rejected_payload(guard_reason)))
        return 2

    if session_factory_builder is None:
        session_factory_builder = _build_send_canary_session_factory
    if redis_client_builder is None:
        redis_client_builder = _build_send_disabled_proof_redis_client
    if worker_once_runner is None:
        worker_once_runner = _run_default_send_disabled_proof_worker_once

    session_factory = None
    dispose_session_factory = None
    redis_client = None
    try:
        session_factory, dispose_session_factory = session_factory_builder(config.database_url)
        redis_client = redis_client_builder(config.redis_url)

        initial_redis = await _load_send_disabled_proof_redis_metrics(
            redis_client,
            queue_name=EXPECTED_QUEUE_NAME,
            consumer_group=config.consumer_group,
        )
        if initial_redis.reason_code is not None:
            emit_json(_to_json(_send_disabled_proof_failure_payload(initial_redis.reason_code)))
            return 1
        if initial_redis.pending != 0 or initial_redis.lag != 0:
            emit_json(_to_json(_send_disabled_proof_rejected_payload("redis_queue_not_idle")))
            return 2

        async with session_factory.begin() as session:
            repository = repository_builder(session)
            setup_result = await create_send_disabled_worker_once_proof_with_repository(
                source_notification_plan_id,
                proof_key,
                repository,
            )
        if setup_result.reason_code is not None:
            if setup_result.reason_code in SEND_DISABLED_PROOF_SETUP_REJECTION_REASONS:
                emit_json(_to_json(_send_disabled_proof_rejected_payload(setup_result.reason_code)))
                return 2
            emit_json(_to_json(_send_disabled_proof_failure_payload(setup_result.reason_code)))
            return 1

        stream_fields = _send_disabled_proof_stream_fields(setup_result)
        await redis_client.xadd(EXPECTED_QUEUE_NAME, stream_fields)

        worker_emitted: list[str] = []
        worker_code = await worker_once_runner(config, worker_emitted.append)
        worker_payload = _safe_json_object(worker_emitted[0] if worker_emitted else "")

        async with session_factory.begin() as session:
            repository = repository_builder(session)
            db_verification = await repository.load_send_disabled_worker_once_proof_verification(
                notification_plan_id=setup_result.notification_plan_id,
            )

        final_redis = await _load_send_disabled_proof_redis_metrics(
            redis_client,
            queue_name=EXPECTED_QUEUE_NAME,
            consumer_group=config.consumer_group,
        )
        payload = _send_disabled_proof_result_payload(
            setup_result=setup_result,
            worker_code=worker_code,
            worker_payload=worker_payload,
            db_verification=db_verification,
            redis_metrics=final_redis,
        )
        emit_json(_to_json(payload))
        return 0 if payload["status"] == "pass" else 1
    except Exception:
        emit_json(_to_json(_send_disabled_proof_failure_payload("send_disabled_proof_failed")))
        return 1
    finally:
        if redis_client is not None:
            await _close_maybe_async(redis_client)
        if dispose_session_factory is not None:
            await dispose_session_factory()


async def create_send_disabled_worker_once_proof_with_repository(
    source_notification_plan_id: UUID,
    proof_key: str,
    repository,
) -> "_SendDisabledProofSetupResult":
    source_plan = await repository.load_notification_plan(source_notification_plan_id)
    if source_plan is None:
        return _SendDisabledProofSetupResult(reason_code="source_notification_plan_missing")
    if str(source_plan.get("delivery_decision") or "") != "send_now":
        return _SendDisabledProofSetupResult(reason_code="source_plan_not_send_now")

    source_analysis_id = _uuid_from_row(source_plan.get("analysis_id"))
    source_candidate_group_id = _uuid_from_row(source_plan.get("candidate_group_id"))
    target_chat_id = _int_from_row(source_plan.get("target_chat_id"))
    if source_analysis_id is None:
        return _SendDisabledProofSetupResult(reason_code="source_analysis_missing")
    if source_candidate_group_id is None:
        return _SendDisabledProofSetupResult(reason_code="source_candidate_group_missing")
    if target_chat_id is None or target_chat_id == 0:
        return _SendDisabledProofSetupResult(reason_code="source_plan_target_chat_missing")

    identity = _send_disabled_proof_identity(
        source_notification_plan_id=source_notification_plan_id,
        proof_key=proof_key,
    )
    if await repository.load_notification_plan(identity.notification_plan_id) is not None:
        return _SendDisabledProofSetupResult(reason_code="proof_notification_plan_exists")
    if await repository.load_event_outbox(identity.event_id) is not None:
        return _SendDisabledProofSetupResult(reason_code="proof_plan_created_event_exists")

    material_existing = await repository.load_existing_plan_by_material(
        analysis_id=source_analysis_id,
        target_chat_id=target_chat_id,
        material_change_hash=identity.material_change_hash,
    )
    if material_existing is not None:
        return _SendDisabledProofSetupResult(reason_code="proof_notification_plan_conflict")

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
        send_after=None,
        suppress_reason_code=None,
        status="planned",
    )
    payload_json = _send_disabled_proof_plan_created_payload(draft)

    async with repository.transaction():
        inserted_plan_id = await repository.insert_notification_plan(draft)
        if inserted_plan_id != identity.notification_plan_id:
            return _SendDisabledProofSetupResult(reason_code="proof_notification_plan_conflict")
        inserted_event_id = await repository.insert_published_notification_plan_created_outbox(
            event_id=identity.event_id,
            notification_plan_id=identity.notification_plan_id,
            dedupe_key=identity.event_dedupe_key,
            payload_json=payload_json,
        )
        if inserted_event_id != identity.event_id:
            return _SendDisabledProofSetupResult(reason_code="proof_plan_created_event_conflict")

    return _SendDisabledProofSetupResult(
        notification_plan_id=identity.notification_plan_id,
        source_notification_plan_id=source_notification_plan_id,
        trigger_event_id=identity.event_id,
        event_dedupe_key=identity.event_dedupe_key,
        event_payload_json=payload_json,
    )


async def _run_restricted_live_worker_once_proof(
    config: NotifierTelegramConfig,
    source_notification_plan_id: UUID,
    proof_key: str,
    *,
    emit_json=print,
    session_factory_builder=None,
    redis_client_builder=None,
    repository_builder=NotifierTelegramRepository,
    worker_once_runner: Callable[[NotifierTelegramConfig, Callable[[str], None]], Awaitable[int]] | None = None,
) -> int:
    guard_reason = _restricted_live_proof_config_guard_reason(config)
    if guard_reason is not None:
        emit_json(_to_json(_restricted_live_proof_rejected_payload(guard_reason)))
        return 2

    if session_factory_builder is None:
        session_factory_builder = _build_send_canary_session_factory
    if redis_client_builder is None:
        redis_client_builder = _build_send_disabled_proof_redis_client
    if worker_once_runner is None:
        worker_once_runner = _run_default_restricted_live_proof_worker_once

    session_factory = None
    dispose_session_factory = None
    redis_client = None
    try:
        session_factory, dispose_session_factory = session_factory_builder(config.database_url)
        redis_client = redis_client_builder(config.redis_url)

        initial_redis = await _load_send_disabled_proof_redis_metrics(
            redis_client,
            queue_name=EXPECTED_QUEUE_NAME,
            consumer_group=config.consumer_group,
        )
        if initial_redis.reason_code is not None:
            emit_json(_to_json(_restricted_live_proof_rejected_payload(initial_redis.reason_code)))
            return 2
        if initial_redis.pending != 0 or initial_redis.lag != 0:
            emit_json(_to_json(_restricted_live_proof_rejected_payload("redis_queue_not_idle")))
            return 2

        async with session_factory.begin() as session:
            repository = repository_builder(session)
            setup_result = await create_restricted_live_worker_once_proof_with_repository(
                source_notification_plan_id,
                proof_key,
                repository,
            )
        if setup_result.reason_code is not None:
            if setup_result.reason_code in RESTRICTED_LIVE_PROOF_SETUP_REJECTION_REASONS:
                emit_json(_to_json(_restricted_live_proof_rejected_payload(setup_result.reason_code)))
                return 2
            emit_json(_to_json(_restricted_live_proof_failure_payload(setup_result.reason_code)))
            return 1

        stream_fields = _restricted_live_proof_stream_fields(setup_result)
        await redis_client.xadd(EXPECTED_QUEUE_NAME, stream_fields)

        worker_emitted: list[str] = []
        worker_code = await worker_once_runner(config, worker_emitted.append)
        worker_payload = _safe_json_object(worker_emitted[0] if worker_emitted else "")

        async with session_factory.begin() as session:
            repository = repository_builder(session)
            db_verification = await repository.load_restricted_live_worker_once_proof_verification(
                notification_plan_id=setup_result.notification_plan_id,
            )

        final_redis = await _load_send_disabled_proof_redis_metrics(
            redis_client,
            queue_name=EXPECTED_QUEUE_NAME,
            consumer_group=config.consumer_group,
        )
        payload = _restricted_live_proof_result_payload(
            setup_result=setup_result,
            worker_code=worker_code,
            worker_payload=worker_payload,
            db_verification=db_verification,
            redis_metrics=final_redis,
        )
        emit_json(_to_json(payload))
        return 0 if payload["status"] == "pass" else 1
    except Exception:
        emit_json(_to_json(_restricted_live_proof_failure_payload("restricted_live_worker_once_proof_failed")))
        return 1
    finally:
        if redis_client is not None:
            await _close_maybe_async(redis_client)
        if dispose_session_factory is not None:
            await dispose_session_factory()


async def _run_restricted_live_queued_worker_once(
    config: NotifierTelegramConfig,
    *,
    max_lag: int = RESTRICTED_LIVE_QUEUED_WORKER_ONCE_DEFAULT_MAX_LAG,
    emit_json=print,
    runtime_config_locator: Mapping[str, Any] | None = None,
    runtime_projection: Mapping[str, Any] | None = None,
    redis_client_builder=None,
    worker_once_runner: Callable[[NotifierTelegramConfig, Callable[[str], None]], Awaitable[int]] | None = None,
) -> int:
    guard_reason = _restricted_live_queued_worker_once_config_guard_reason(config)
    if guard_reason is not None:
        emit_json(
            _to_json(
                _restricted_live_queued_worker_once_rejected_payload(
                    guard_reason,
                    config=config,
                    runtime_config_locator=runtime_config_locator,
                    runtime_projection=runtime_projection,
                )
            )
        )
        return 2
    if max_lag < 1:
        emit_json(
            _to_json(
                _restricted_live_queued_worker_once_rejected_payload(
                    "invalid_max_lag",
                    config=config,
                    runtime_config_locator=runtime_config_locator,
                    runtime_projection=runtime_projection,
                )
            )
        )
        return 2

    if redis_client_builder is None:
        redis_client_builder = _build_send_disabled_proof_redis_client
    if worker_once_runner is None:
        worker_once_runner = _run_default_restricted_live_queued_worker_once

    redis_client = None
    redis_metrics: _RedisGroupMetrics | None = None
    try:
        try:
            redis_client = redis_client_builder(config.redis_url)
        except Exception:
            redis_metrics = _RedisGroupMetrics(reason_code="redis_group_metrics_unavailable")
            emit_json(
                _to_json(
                    _restricted_live_queued_worker_once_rejected_payload(
                        redis_metrics.reason_code,
                        config=config,
                        redis_metrics=redis_metrics,
                        runtime_config_locator=runtime_config_locator,
                        runtime_projection=runtime_projection,
                    )
                )
            )
            return 2

        redis_metrics = await _load_send_disabled_proof_redis_metrics(
            redis_client,
            queue_name=EXPECTED_QUEUE_NAME,
            consumer_group=config.consumer_group,
        )
        if redis_metrics.reason_code is not None:
            emit_json(
                _to_json(
                    _restricted_live_queued_worker_once_rejected_payload(
                        redis_metrics.reason_code,
                        config=config,
                        redis_metrics=redis_metrics,
                        runtime_config_locator=runtime_config_locator,
                        runtime_projection=runtime_projection,
                    )
                )
            )
            return 2
        if (redis_metrics.pending or 0) > 0:
            emit_json(
                _to_json(
                    _restricted_live_queued_worker_once_rejected_payload(
                        "redis_pending_messages_present",
                        config=config,
                        redis_metrics=redis_metrics,
                        runtime_config_locator=runtime_config_locator,
                        runtime_projection=runtime_projection,
                    )
                )
            )
            return 2
        if redis_metrics.lag == 0:
            emit_json(
                _to_json(
                    _restricted_live_queued_worker_once_noop_payload(
                        "no_queued_message",
                        config=config,
                        redis_metrics=redis_metrics,
                        runtime_config_locator=runtime_config_locator,
                        runtime_projection=runtime_projection,
                    )
                )
            )
            return 0
        if (redis_metrics.lag or 0) > max_lag:
            emit_json(
                _to_json(
                    _restricted_live_queued_worker_once_rejected_payload(
                        "queue_lag_exceeds_restricted_worker_once_limit",
                        config=config,
                        redis_metrics=redis_metrics,
                        runtime_config_locator=runtime_config_locator,
                        runtime_projection=runtime_projection,
                    )
                )
            )
            return 2

        worker_emitted: list[str] = []
        try:
            worker_code = await worker_once_runner(config, worker_emitted.append)
        except Exception:
            emit_json(
                _to_json(
                    _restricted_live_queued_worker_once_failure_payload(
                        "worker_once_exception",
                        config=config,
                        redis_metrics=redis_metrics,
                        runtime_config_locator=runtime_config_locator,
                        runtime_projection=runtime_projection,
                    )
                )
            )
            return 1

        worker_payload = _safe_json_object(worker_emitted[0] if worker_emitted else "")
        payload = _restricted_live_queued_worker_once_result_payload(
            config=config,
            redis_metrics=redis_metrics,
            worker_code=worker_code,
            worker_payload=worker_payload,
            runtime_config_locator=runtime_config_locator,
            runtime_projection=runtime_projection,
        )
        emit_json(_to_json(payload))
        if payload["status"] in {"pass", "noop"}:
            return 0
        return 1
    except Exception:
        emit_json(
            _to_json(
                _restricted_live_queued_worker_once_failure_payload(
                    "restricted_live_queued_worker_once_failed",
                    config=config,
                    redis_metrics=redis_metrics,
                    runtime_config_locator=runtime_config_locator,
                    runtime_projection=runtime_projection,
                )
            )
        )
        return 1
    finally:
        if redis_client is not None:
            await _close_maybe_async(redis_client)


async def create_restricted_live_worker_once_proof_with_repository(
    source_notification_plan_id: UUID,
    proof_key: str,
    repository,
) -> "_RestrictedLiveProofSetupResult":
    source_context, reason_code = await _load_restricted_live_proof_source_context(
        source_notification_plan_id,
        repository,
    )
    if reason_code is not None or source_context is None:
        return _RestrictedLiveProofSetupResult(reason_code=reason_code or "source_notification_plan_missing")

    identity = _restricted_live_proof_identity(
        source_notification_plan_id=source_notification_plan_id,
        proof_key=proof_key,
    )
    if await repository.load_notification_plan(identity.notification_plan_id) is not None:
        return _RestrictedLiveProofSetupResult(reason_code="proof_notification_plan_exists")
    if await repository.load_event_outbox(identity.event_id) is not None:
        return _RestrictedLiveProofSetupResult(reason_code="proof_plan_created_event_exists")

    material_existing = await repository.load_existing_plan_by_material(
        analysis_id=source_context.source_analysis_id,
        target_chat_id=source_context.target_chat_id,
        material_change_hash=identity.material_change_hash,
    )
    if material_existing is not None:
        return _RestrictedLiveProofSetupResult(reason_code="proof_notification_plan_conflict")

    draft = _restricted_live_proof_draft(identity, source_context)
    payload_json = _restricted_live_proof_plan_created_payload(draft)

    async with repository.transaction():
        inserted_plan_id = await repository.insert_notification_plan(draft)
        if inserted_plan_id != identity.notification_plan_id:
            return _RestrictedLiveProofSetupResult(reason_code="proof_notification_plan_conflict")
        inserted_event_id = await repository.insert_published_notification_plan_created_outbox(
            event_id=identity.event_id,
            notification_plan_id=identity.notification_plan_id,
            dedupe_key=identity.event_dedupe_key,
            payload_json=payload_json,
        )
        if inserted_event_id != identity.event_id:
            return _RestrictedLiveProofSetupResult(reason_code="proof_plan_created_event_conflict")

    return _RestrictedLiveProofSetupResult(
        notification_plan_id=identity.notification_plan_id,
        source_notification_plan_id=source_notification_plan_id,
        trigger_event_id=identity.event_id,
        event_dedupe_key=identity.event_dedupe_key,
        event_payload_json=payload_json,
    )


async def create_restricted_live_queue_chain_proof_target_with_repository(
    source_notification_plan_id: UUID,
    proof_key: str,
    repository,
) -> "_RestrictedLiveQueueChainProofTargetResult":
    source_context, reason_code = await _load_restricted_live_proof_source_context(
        source_notification_plan_id,
        repository,
    )
    if reason_code is not None or source_context is None:
        return _RestrictedLiveQueueChainProofTargetResult(reason_code=reason_code or "source_notification_plan_missing")

    identity = _restricted_live_proof_identity(
        source_notification_plan_id=source_notification_plan_id,
        proof_key=proof_key,
    )
    draft = _restricted_live_proof_draft(identity, source_context)
    payload_json = _restricted_live_proof_plan_created_payload(draft)

    existing_plan = await repository.load_notification_plan(identity.notification_plan_id)
    existing_event = await repository.load_event_outbox(identity.event_id)
    material_existing = await repository.load_existing_plan_by_material(
        analysis_id=source_context.source_analysis_id,
        target_chat_id=source_context.target_chat_id,
        material_change_hash=identity.material_change_hash,
    )
    if material_existing is not None and _uuid_from_row(material_existing.get("notification_plan_id")) != (
        identity.notification_plan_id
    ):
        return _RestrictedLiveQueueChainProofTargetResult(reason_code="proof_notification_plan_conflict")
    if existing_plan is not None and not _notification_plan_matches_draft(existing_plan, draft):
        return _RestrictedLiveQueueChainProofTargetResult(reason_code="proof_notification_plan_conflict")
    if existing_event is not None and not _notification_plan_created_event_matches(
        existing_event,
        identity=identity,
        payload_json=payload_json,
    ):
        return _RestrictedLiveQueueChainProofTargetResult(reason_code="proof_plan_created_event_conflict")

    if existing_event is not None:
        event_status = str(existing_event.get("status") or "")
        if event_status == "pending":
            return _restricted_live_queue_chain_proof_target_result(
                status="existing_pending",
                source_notification_plan_id=source_notification_plan_id,
                source_context=source_context,
                draft=draft,
                identity=identity,
                payload_json=payload_json,
                created=False,
                existing=True,
                outbox_status_before_publish="pending",
            )
        if event_status == "published":
            return _restricted_live_queue_chain_proof_target_result(
                status="existing_already_published",
                source_notification_plan_id=source_notification_plan_id,
                source_context=source_context,
                draft=draft,
                identity=identity,
                payload_json=payload_json,
                created=False,
                existing=True,
                outbox_status_before_publish="published",
            )
        return _RestrictedLiveQueueChainProofTargetResult(reason_code="proof_plan_created_event_conflict")

    async with repository.transaction():
        if existing_plan is None:
            inserted_plan_id = await repository.insert_notification_plan(draft)
            if inserted_plan_id != identity.notification_plan_id:
                return _RestrictedLiveQueueChainProofTargetResult(reason_code="proof_notification_plan_conflict")
        inserted_event_id = await repository.insert_pending_notification_plan_created_outbox(
            event_id=identity.event_id,
            notification_plan_id=identity.notification_plan_id,
            dedupe_key=identity.event_dedupe_key,
            payload_json=payload_json,
        )
        if inserted_event_id != identity.event_id:
            return _RestrictedLiveQueueChainProofTargetResult(reason_code="proof_plan_created_event_conflict")

    return _restricted_live_queue_chain_proof_target_result(
        status="created",
        source_notification_plan_id=source_notification_plan_id,
        source_context=source_context,
        draft=draft,
        identity=identity,
        payload_json=payload_json,
        created=True,
        existing=existing_plan is not None,
        outbox_status_before_publish="pending",
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


@dataclass(slots=True, frozen=True)
class _SendDisabledProofIdentity:
    notification_plan_id: UUID
    event_id: UUID
    dedupe_subject_key: str
    material_change_hash: str
    event_dedupe_key: str


@dataclass(slots=True, frozen=True)
class _SendDisabledProofSetupResult:
    notification_plan_id: UUID | None = None
    source_notification_plan_id: UUID | None = None
    trigger_event_id: UUID | None = None
    event_dedupe_key: str | None = None
    event_payload_json: dict[str, Any] | None = None
    reason_code: str | None = None


@dataclass(slots=True, frozen=True)
class _RestrictedLiveProofIdentity:
    notification_plan_id: UUID
    event_id: UUID
    dedupe_subject_key: str
    material_change_hash: str
    event_dedupe_key: str


@dataclass(slots=True, frozen=True)
class _RestrictedLiveProofSourceContext:
    source_plan: Mapping[str, object]
    source_analysis_id: UUID
    source_candidate_group_id: UUID
    target_chat_id: int


@dataclass(slots=True, frozen=True)
class _RestrictedLiveProofSetupResult:
    notification_plan_id: UUID | None = None
    source_notification_plan_id: UUID | None = None
    trigger_event_id: UUID | None = None
    event_dedupe_key: str | None = None
    event_payload_json: dict[str, Any] | None = None
    reason_code: str | None = None


@dataclass(slots=True, frozen=True)
class _RestrictedLiveQueueChainProofTargetResult:
    status: str | None = None
    notification_plan_id: UUID | None = None
    source_notification_plan_id: UUID | None = None
    source_analysis_id: UUID | None = None
    source_candidate_group_id: UUID | None = None
    trigger_event_id: UUID | None = None
    event_dedupe_key: str | None = None
    event_payload_json: dict[str, Any] | None = None
    created: bool = False
    existing: bool = False
    plan_status: str | None = None
    delivery_decision: str | None = None
    urgency_profile: str | None = None
    outbox_status_before_publish: str | None = None
    reason_code: str | None = None


@dataclass(slots=True, frozen=True)
class _RedisGroupMetrics:
    pending: int | None = None
    lag: int | None = None
    reason_code: str | None = None


def _valid_canary_key(canary_key: str) -> bool:
    return bool(CANARY_KEY_PATTERN.fullmatch(canary_key))


def _valid_send_disabled_proof_key(proof_key: str) -> bool:
    return bool(SEND_DISABLED_PROOF_KEY_PATTERN.fullmatch(proof_key))


def _valid_restricted_live_proof_key(proof_key: str) -> bool:
    return bool(RESTRICTED_LIVE_PROOF_KEY_PATTERN.fullmatch(proof_key))


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


def _send_disabled_proof_identity(*, source_notification_plan_id: UUID, proof_key: str) -> _SendDisabledProofIdentity:
    seed = f"{SEND_DISABLED_PROOF_SEED_VERSION}|{source_notification_plan_id}|{proof_key}"
    notification_plan_id = uuid5(NAMESPACE_URL, seed)
    event_id = uuid5(NAMESPACE_URL, f"{seed}|notification.plan.created.v1")
    return _SendDisabledProofIdentity(
        notification_plan_id=notification_plan_id,
        event_id=event_id,
        dedupe_subject_key=f"proof/send-disabled/{source_notification_plan_id}/{proof_key}",
        material_change_hash=hashlib.sha256(seed.encode("utf-8")).hexdigest(),
        event_dedupe_key=f"notification-plan-created:send-disabled-proof:{notification_plan_id}",
    )


def _restricted_live_proof_identity(
    *,
    source_notification_plan_id: UUID,
    proof_key: str,
) -> _RestrictedLiveProofIdentity:
    seed = f"{RESTRICTED_LIVE_PROOF_SEED_VERSION}|{source_notification_plan_id}|{proof_key}"
    notification_plan_id = uuid5(NAMESPACE_URL, seed)
    event_id = uuid5(NAMESPACE_URL, f"{seed}|notification.plan.created.v1")
    return _RestrictedLiveProofIdentity(
        notification_plan_id=notification_plan_id,
        event_id=event_id,
        dedupe_subject_key=f"proof/restricted-live/{source_notification_plan_id}/{proof_key}",
        material_change_hash=hashlib.sha256(seed.encode("utf-8")).hexdigest(),
        event_dedupe_key=f"notification-plan-created:restricted-live-proof:{notification_plan_id}",
    )


async def _load_restricted_live_proof_source_context(
    source_notification_plan_id: UUID,
    repository,
) -> tuple[_RestrictedLiveProofSourceContext | None, str | None]:
    source_plan = await repository.load_notification_plan(source_notification_plan_id)
    if source_plan is None:
        return None, "source_notification_plan_missing"
    if str(source_plan.get("delivery_decision") or "") != "send_now":
        return None, "source_plan_not_send_now"

    source_analysis_id = _uuid_from_row(source_plan.get("analysis_id"))
    source_candidate_group_id = _uuid_from_row(source_plan.get("candidate_group_id"))
    target_chat_id = _int_from_row(source_plan.get("target_chat_id"))
    if source_analysis_id is None:
        return None, "source_analysis_missing"
    if source_candidate_group_id is None:
        return None, "source_candidate_group_missing"
    if target_chat_id is None or target_chat_id == 0:
        return None, "source_plan_target_chat_missing"

    analysis = await repository.load_analysis(source_analysis_id)
    if analysis is None:
        return None, "source_analysis_missing"
    if analysis.delivery_decision != "send_now":
        return None, "source_analysis_not_send_now"
    if analysis.candidate_group_id != source_candidate_group_id:
        return None, "source_candidate_group_mismatch"

    return (
        _RestrictedLiveProofSourceContext(
            source_plan=source_plan,
            source_analysis_id=source_analysis_id,
            source_candidate_group_id=source_candidate_group_id,
            target_chat_id=target_chat_id,
        ),
        None,
    )


def _restricted_live_proof_draft(
    identity: _RestrictedLiveProofIdentity,
    source_context: _RestrictedLiveProofSourceContext,
) -> NotificationPlanDraft:
    source_plan = source_context.source_plan
    return NotificationPlanDraft(
        notification_plan_id=identity.notification_plan_id,
        analysis_id=source_context.source_analysis_id,
        candidate_group_id=source_context.source_candidate_group_id,
        delivery_decision="send_now",
        urgency_profile=str(source_plan.get("urgency_profile") or "high"),
        target_chat_id=source_context.target_chat_id,
        target_thread_id=_int_from_row(source_plan.get("target_thread_id")),
        render_profile=_string_from_row(source_plan.get("render_profile")),
        dedupe_subject_key=identity.dedupe_subject_key,
        material_change_hash=identity.material_change_hash,
        send_after=None,
        suppress_reason_code=None,
        status="planned",
    )


def _restricted_live_queue_chain_proof_target_result(
    *,
    status: str,
    source_notification_plan_id: UUID,
    source_context: _RestrictedLiveProofSourceContext,
    draft: NotificationPlanDraft,
    identity: _RestrictedLiveProofIdentity,
    payload_json: dict[str, Any],
    created: bool,
    existing: bool,
    outbox_status_before_publish: str,
) -> _RestrictedLiveQueueChainProofTargetResult:
    return _RestrictedLiveQueueChainProofTargetResult(
        status=status,
        notification_plan_id=draft.notification_plan_id,
        source_notification_plan_id=source_notification_plan_id,
        source_analysis_id=source_context.source_analysis_id,
        source_candidate_group_id=source_context.source_candidate_group_id,
        trigger_event_id=identity.event_id,
        event_dedupe_key=identity.event_dedupe_key,
        event_payload_json=payload_json,
        created=created,
        existing=existing,
        plan_status=draft.status,
        delivery_decision=draft.delivery_decision,
        urgency_profile=draft.urgency_profile,
        outbox_status_before_publish=outbox_status_before_publish,
    )


def _notification_plan_matches_draft(plan: Mapping[str, object], draft: NotificationPlanDraft) -> bool:
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
        and _string_from_row(plan.get("send_after")) is None
        and _string_from_row(plan.get("suppress_reason_code")) is None
        and str(plan.get("status") or "") == draft.status
    )


def _notification_plan_created_event_matches(
    row: Mapping[str, object],
    *,
    identity: _RestrictedLiveProofIdentity,
    payload_json: dict[str, Any],
) -> bool:
    return (
        _uuid_from_row(row.get("event_id")) == identity.event_id
        and str(row.get("event_type") or "") == "notification.plan.created.v1"
        and str(row.get("aggregate_type") or "") == "notification_plan"
        and _uuid_from_row(row.get("aggregate_id")) == identity.notification_plan_id
        and str(row.get("dedupe_key") or "") == identity.event_dedupe_key
        and _event_payload_from_row(row) == payload_json
    )


def _event_payload_from_row(row: Mapping[str, object]) -> dict[str, Any]:
    payload = row.get("payload_json")
    if isinstance(payload, Mapping):
        return dict(payload)
    if isinstance(payload, str):
        parsed = _safe_json_object(payload)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _send_disabled_proof_plan_created_payload(draft: NotificationPlanDraft) -> dict[str, Any]:
    return {
        "notification_plan_id": str(draft.notification_plan_id),
        "analysis_id": str(draft.analysis_id),
        "candidate_group_id": str(draft.candidate_group_id),
        "delivery_decision": draft.delivery_decision,
        "urgency_profile": draft.urgency_profile,
        "target_chat_id": draft.target_chat_id,
        "target_thread_id": draft.target_thread_id,
        "render_profile": draft.render_profile,
        "dedupe_subject_key": draft.dedupe_subject_key,
        "material_change_hash": draft.material_change_hash,
        "send_after": draft.send_after,
        "suppress_reason_code": draft.suppress_reason_code,
    }


def _restricted_live_proof_plan_created_payload(draft: NotificationPlanDraft) -> dict[str, Any]:
    return _send_disabled_proof_plan_created_payload(draft)


def _send_disabled_proof_stream_fields(setup_result: _SendDisabledProofSetupResult) -> dict[str, str]:
    return {
        "job_id": str(setup_result.trigger_event_id),
        "stage_name": EXPECTED_STAGE_NAME,
        "root_object_type": "notification_plan",
        "root_object_id": str(setup_result.notification_plan_id),
        "idempotency_key": str(setup_result.event_dedupe_key or ""),
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(setup_result.trigger_event_id),
    }


def _restricted_live_proof_stream_fields(setup_result: _RestrictedLiveProofSetupResult) -> dict[str, str]:
    return {
        "job_id": str(setup_result.trigger_event_id),
        "stage_name": EXPECTED_STAGE_NAME,
        "root_object_type": "notification_plan",
        "root_object_id": str(setup_result.notification_plan_id),
        "idempotency_key": str(setup_result.event_dedupe_key or ""),
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(setup_result.trigger_event_id),
    }


def _send_disabled_proof_config_guard_reason(config: NotifierTelegramConfig) -> str | None:
    if config.app_env not in {"prod", "production"}:
        return "app_env_not_prod"
    if config.queue_name != EXPECTED_QUEUE_NAME:
        return "notifier_queue_mismatch"
    if config.transport_enabled:
        return "telegram_transport_enabled"
    if config.enable_notification_send:
        return "notification_send_not_disabled"
    if config.dry_run:
        return "notifier_dry_run_enabled"
    if config.allow_edits:
        return "notifier_edits_enabled"
    return None


def _restricted_live_proof_config_guard_reason(config: NotifierTelegramConfig) -> str | None:
    if config.app_env not in {"prod", "production"}:
        return "app_env_not_prod"
    if config.queue_name != EXPECTED_QUEUE_NAME:
        return "notifier_queue_mismatch"
    if not config.enable_notification_send:
        return "notification_send_disabled"
    if config.dry_run:
        return "notifier_dry_run_enabled"
    if config.allow_edits:
        return "notifier_edits_enabled"
    if not config.transport_enabled:
        return "telegram_transport_disabled"
    if not config.telegram_bot_token:
        return "telegram_bot_token_missing"
    if _telegram_api_base_url_is_blackhole(config.telegram_api_base_url):
        return "telegram_api_base_url_blackhole"
    return None


def _restricted_live_queued_worker_once_config_guard_reason(config: NotifierTelegramConfig) -> str | None:
    if config.app_env not in {"prod", "production"}:
        return "app_env_not_prod"
    if config.queue_name != EXPECTED_QUEUE_NAME:
        return "notifier_queue_mismatch"
    if not config.enable_notification_send:
        return "notification_send_disabled"
    if config.dry_run:
        return "notifier_dry_run_enabled"
    if config.allow_edits:
        return "notifier_edits_enabled"
    if not config.transport_enabled:
        return "telegram_transport_disabled"
    if not config.telegram_bot_token:
        return "telegram_bot_token_missing"
    if _telegram_api_base_url_is_blackhole(config.telegram_api_base_url):
        return "telegram_api_base_url_blackhole"
    if not _telegram_api_base_url_is_official(config.telegram_api_base_url):
        return "telegram_api_base_url_unofficial"
    return None


def _telegram_api_base_url_is_blackhole(base_url: str) -> bool:
    try:
        hostname = (urlparse(base_url).hostname or "").strip().lower()
    except ValueError:
        return False
    return hostname == "localhost" or hostname == "::1" or hostname.startswith("127.")


def _telegram_api_base_url_is_official(base_url: str) -> bool:
    try:
        parsed = urlparse(base_url)
    except ValueError:
        return False
    return parsed.scheme == "https" and (parsed.hostname or "").strip().lower() == "api.telegram.org"


async def _run_default_send_disabled_proof_worker_once(
    config: NotifierTelegramConfig,
    emit_json: Callable[[str], None],
) -> int:
    return await run_worker_once_invocation(
        queue=EXPECTED_QUEUE_NAME,
        confirm_worker_once=True,
        output_format="json",
        emit_json=emit_json,
        config_loader=lambda: config,
    )


async def _run_default_restricted_live_proof_worker_once(
    config: NotifierTelegramConfig,
    emit_json: Callable[[str], None],
) -> int:
    return await run_worker_once_invocation(
        queue=EXPECTED_QUEUE_NAME,
        confirm_worker_once=True,
        output_format="json",
        emit_json=emit_json,
        config_loader=lambda: config,
    )


async def _run_default_restricted_live_queued_worker_once(
    config: NotifierTelegramConfig,
    emit_json: Callable[[str], None],
) -> int:
    return await run_worker_once_invocation(
        queue=EXPECTED_QUEUE_NAME,
        confirm_worker_once=True,
        output_format="json",
        emit_json=emit_json,
        config_loader=lambda: config,
    )


def _build_send_disabled_proof_redis_client(redis_url: str):
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    return Redis.from_url(redis_url, decode_responses=True)


async def _load_send_disabled_proof_redis_metrics(
    redis_client,
    *,
    queue_name: str,
    consumer_group: str,
) -> _RedisGroupMetrics:
    try:
        groups = await redis_client.xinfo_groups(queue_name)
    except Exception:
        return _RedisGroupMetrics(reason_code="redis_group_metrics_unavailable")
    for group in groups or []:
        group_name = _redis_group_value(group, "name")
        if group_name != consumer_group:
            continue
        pending = _redis_group_int(group, "pending")
        lag = _redis_group_int(group, "lag")
        if pending is None:
            return _RedisGroupMetrics(reason_code="redis_pending_unavailable")
        if lag is None:
            return _RedisGroupMetrics(reason_code="redis_lag_unavailable")
        return _RedisGroupMetrics(pending=pending, lag=lag)
    return _RedisGroupMetrics(reason_code="redis_consumer_group_missing")


def _redis_group_value(group: object, key: str) -> str | None:
    if not isinstance(group, Mapping):
        return None
    value = group.get(key)
    if value is None:
        value = group.get(key.encode("utf-8"))
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value) if value is not None else None


def _redis_group_int(group: object, key: str) -> int | None:
    value = _redis_group_value(group, key)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


async def _close_maybe_async(resource) -> None:
    close = getattr(resource, "aclose", None) or getattr(resource, "close", None)
    if close is None:
        return
    maybe_awaitable = close()
    if asyncio.iscoroutine(maybe_awaitable):
        await maybe_awaitable


def _safe_json_object(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _send_disabled_proof_result_payload(
    *,
    setup_result: _SendDisabledProofSetupResult,
    worker_code: int,
    worker_payload: dict[str, Any],
    db_verification: dict[str, Any],
    redis_metrics: _RedisGroupMetrics,
) -> dict[str, Any]:
    checks = _send_disabled_proof_checks(
        worker_code=worker_code,
        worker_payload=worker_payload,
        db_verification=db_verification,
        redis_metrics=redis_metrics,
    )
    failed = [name for name, passed in checks.items() if not passed]
    status = "pass" if not failed else "fail"
    reason_code = "send_disabled_worker_once_proof_passed" if not failed else "proof_verification_failed"
    worker_authority = worker_payload.get("authority") if isinstance(worker_payload.get("authority"), dict) else {}
    return {
        "schema_version": SEND_DISABLED_PROOF_SCHEMA_VERSION,
        "status": status,
        "reason_code": reason_code,
        "source_notification_plan_id": str(setup_result.source_notification_plan_id),
        "proof_notification_plan_id": str(setup_result.notification_plan_id),
        "trigger_event_id_present": setup_result.trigger_event_id is not None,
        "event_outbox_published": True,
        "redis_xadd_count": 1,
        "worker_once": {
            "exit_code": worker_code,
            "status": worker_payload.get("status"),
            "reason_code": worker_payload.get("reason_code"),
            "acked": worker_payload.get("acked"),
            "handler_called": worker_payload.get("handler_called"),
            "authority": worker_authority,
        },
        "db_verification": db_verification,
        "redis_verification": {
            "pending": redis_metrics.pending,
            "lag": redis_metrics.lag,
            "reason_code": redis_metrics.reason_code,
        },
        "authority": {
            "telegram_transport_possible": worker_authority.get("telegram_transport_possible"),
            "database_session_opened": worker_authority.get("database_session_opened"),
            "workers_started": worker_authority.get("workers_started"),
            "run_forever_started": worker_authority.get("run_forever_started"),
            "openai_called": worker_authority.get("openai_called"),
            "github_called": worker_authority.get("github_called"),
            "docker_or_systemd_called": worker_authority.get("docker_or_systemd_called"),
            "alembic_or_ddl_ran": worker_authority.get("alembic_or_ddl_ran"),
            "telegram_send_or_edit_called": False,
        },
        "checks": checks,
        "checks_failed": failed,
    }


def _send_disabled_proof_checks(
    *,
    worker_code: int,
    worker_payload: dict[str, Any],
    db_verification: dict[str, Any],
    redis_metrics: _RedisGroupMetrics,
) -> dict[str, bool]:
    response_json = db_verification.get("telegram_response_json")
    response = response_json if isinstance(response_json, dict) else {}
    authority = worker_payload.get("authority") if isinstance(worker_payload.get("authority"), dict) else {}
    return {
        "worker_exit_zero": worker_code == 0,
        "worker_processed": worker_payload.get("status") == "processed",
        "worker_acked": worker_payload.get("acked") is True,
        "telegram_transport_possible_false": authority.get("telegram_transport_possible") is False,
        "database_session_opened": authority.get("database_session_opened") is True,
        "workers_started_false": authority.get("workers_started") is False,
        "run_forever_started_false": authority.get("run_forever_started") is False,
        "openai_called_false": authority.get("openai_called") is False,
        "github_called_false": authority.get("github_called") is False,
        "docker_or_systemd_called_false": authority.get("docker_or_systemd_called") is False,
        "alembic_or_ddl_ran_false": authority.get("alembic_or_ddl_ran") is False,
        "proof_plan_suppressed": str(db_verification.get("proof_plan_final_status") or "") == "suppressed",
        "notification_render_created": int(db_verification.get("notification_render_count") or 0) >= 1,
        "exactly_one_delivery_record": int(db_verification.get("notification_delivery_record_count") or 0) == 1,
        "delivery_status_suppressed": db_verification.get("delivery_status") == "suppressed",
        "attempt_count_zero": db_verification.get("attempt_count") == 0,
        "transport_error_code_send_disabled": db_verification.get("transport_error_code")
        == SEND_DISABLED_PROOF_REASON_CODE,
        "response_send_disabled_true": response.get("send_disabled") is True,
        "response_dry_run_false": response.get("dry_run") is False,
        "response_reason_code_send_disabled": response.get("reason_code") == SEND_DISABLED_PROOF_REASON_CODE,
        "response_transport_skipped_true": response.get("transport_skipped") is True,
        "latest_transition_send_disabled": db_verification.get("latest_state_transition_reason_code")
        == SEND_DISABLED_PROOF_REASON_CODE,
        "delivery_result_outbox_exists": db_verification.get("delivery_result_outbox_exists") is True,
        "redis_pending_zero": redis_metrics.pending == 0,
        "redis_lag_zero": redis_metrics.lag == 0,
    }


def _send_disabled_proof_rejected_payload(
    reason_code: str,
    *,
    proof_notification_plan_id: UUID | None = None,
) -> dict:
    payload = {
        "schema_version": SEND_DISABLED_PROOF_SCHEMA_VERSION,
        "status": "rejected",
        "reason_code": reason_code,
    }
    if proof_notification_plan_id is not None:
        payload["proof_notification_plan_id"] = str(proof_notification_plan_id)
    return payload


def _send_disabled_proof_failure_payload(reason_code: str) -> dict:
    return {
        "schema_version": SEND_DISABLED_PROOF_SCHEMA_VERSION,
        "status": "fail",
        "reason_code": reason_code,
        "warnings": [reason_code],
    }


def _restricted_live_proof_result_payload(
    *,
    setup_result: _RestrictedLiveProofSetupResult,
    worker_code: int,
    worker_payload: dict[str, Any],
    db_verification: dict[str, Any],
    redis_metrics: _RedisGroupMetrics,
) -> dict[str, Any]:
    checks = _restricted_live_proof_checks(
        worker_code=worker_code,
        worker_payload=worker_payload,
        db_verification=db_verification,
        redis_metrics=redis_metrics,
    )
    failed = [name for name, passed in checks.items() if not passed]
    status = "pass" if not failed else "fail"
    reason_code = "restricted_live_worker_once_proof_passed" if not failed else "proof_verification_failed"
    worker_authority = worker_payload.get("authority") if isinstance(worker_payload.get("authority"), dict) else {}
    return {
        "schema_version": RESTRICTED_LIVE_PROOF_SCHEMA_VERSION,
        "status": status,
        "reason_code": reason_code,
        "source_notification_plan_id": str(setup_result.source_notification_plan_id),
        "proof_notification_plan_id": str(setup_result.notification_plan_id),
        "trigger_event_id_present": setup_result.trigger_event_id is not None,
        "event_outbox_published": True,
        "redis_xadd_count": 1,
        "worker_once": {
            "exit_code": worker_code,
            "status": worker_payload.get("status"),
            "reason_code": worker_payload.get("reason_code"),
            "acked": worker_payload.get("acked"),
            "handler_called": worker_payload.get("handler_called"),
            "authority": worker_authority,
        },
        "db_verification": db_verification,
        "redis_verification": {
            "pending": redis_metrics.pending,
            "lag": redis_metrics.lag,
            "reason_code": redis_metrics.reason_code,
        },
        "authority": {
            "telegram_transport_possible": worker_authority.get("telegram_transport_possible"),
            "database_session_opened": worker_authority.get("database_session_opened"),
            "workers_started": worker_authority.get("workers_started"),
            "run_forever_started": worker_authority.get("run_forever_started"),
            "openai_called": worker_authority.get("openai_called"),
            "github_called": worker_authority.get("github_called"),
            "docker_or_systemd_called": worker_authority.get("docker_or_systemd_called"),
            "alembic_or_ddl_ran": worker_authority.get("alembic_or_ddl_ran"),
        },
        "checks": checks,
        "checks_failed": failed,
    }


def _restricted_live_proof_checks(
    *,
    worker_code: int,
    worker_payload: dict[str, Any],
    db_verification: dict[str, Any],
    redis_metrics: _RedisGroupMetrics,
) -> dict[str, bool]:
    authority = worker_payload.get("authority") if isinstance(worker_payload.get("authority"), dict) else {}
    latest_reason_code = db_verification.get("latest_state_transition_reason_code")
    return {
        "worker_exit_zero": worker_code == 0,
        "worker_processed": worker_payload.get("status") == "processed",
        "worker_acked": worker_payload.get("acked") is True,
        "handler_called": worker_payload.get("handler_called") is True,
        "telegram_transport_possible_true": authority.get("telegram_transport_possible") is True,
        "database_session_opened": authority.get("database_session_opened") is True,
        "workers_started_false": authority.get("workers_started") is False,
        "run_forever_started_false": authority.get("run_forever_started") is False,
        "openai_called_false": authority.get("openai_called") is False,
        "github_called_false": authority.get("github_called") is False,
        "docker_or_systemd_called_false": authority.get("docker_or_systemd_called") is False,
        "alembic_or_ddl_ran_false": authority.get("alembic_or_ddl_ran") is False,
        "proof_plan_sent": str(db_verification.get("proof_plan_final_status") or "") == "sent",
        "notification_render_created": int(db_verification.get("notification_render_count") or 0) >= 1,
        "exactly_one_delivery_record": int(db_verification.get("notification_delivery_record_count") or 0) == 1,
        "delivery_status_sent": db_verification.get("delivery_status") == "sent",
        "attempt_count_one": db_verification.get("attempt_count") == 1,
        "transport_error_code_null": db_verification.get("transport_error_code") is None,
        "telegram_chat_id_present": db_verification.get("telegram_chat_id_present") is True,
        "telegram_message_id_present": db_verification.get("telegram_message_id_present") is True,
        "latest_transition_sent": db_verification.get("latest_state_transition_to_state") == "sent",
        "latest_transition_reason_allowed": latest_reason_code
        in {"notification_no_recent_delivery", "notification_delivery_result"},
        "delivery_result_outbox_exists": db_verification.get("delivery_result_outbox_exists") is True,
        "redis_pending_zero": redis_metrics.pending == 0,
        "redis_lag_zero": redis_metrics.lag == 0,
    }


def _restricted_live_proof_rejected_payload(
    reason_code: str,
    *,
    proof_notification_plan_id: UUID | None = None,
) -> dict:
    payload = {
        "schema_version": RESTRICTED_LIVE_PROOF_SCHEMA_VERSION,
        "status": "rejected",
        "reason_code": reason_code,
    }
    if proof_notification_plan_id is not None:
        payload["proof_notification_plan_id"] = str(proof_notification_plan_id)
    return payload


def _restricted_live_proof_failure_payload(reason_code: str) -> dict:
    return {
        "schema_version": RESTRICTED_LIVE_PROOF_SCHEMA_VERSION,
        "status": "fail",
        "reason_code": reason_code,
        "warnings": [reason_code],
    }


def _restricted_live_queued_worker_once_rejected_payload(
    reason_code: str | None,
    *,
    config: NotifierTelegramConfig | None = None,
    redis_metrics: _RedisGroupMetrics | None = None,
    runtime_config_locator: Mapping[str, Any] | None = None,
    runtime_projection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return _restricted_live_queued_worker_once_base_payload(
        status="rejected",
        reason_code=reason_code or "restricted_live_queued_worker_once_rejected",
        config=config,
        redis_metrics=redis_metrics,
        runtime_config_locator=runtime_config_locator,
        runtime_projection=runtime_projection,
    )


def _restricted_live_queued_worker_once_noop_payload(
    reason_code: str,
    *,
    config: NotifierTelegramConfig,
    redis_metrics: _RedisGroupMetrics,
    runtime_config_locator: Mapping[str, Any] | None = None,
    runtime_projection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return _restricted_live_queued_worker_once_base_payload(
        status="noop",
        reason_code=reason_code,
        config=config,
        redis_metrics=redis_metrics,
        runtime_config_locator=runtime_config_locator,
        runtime_projection=runtime_projection,
    )


def _restricted_live_queued_worker_once_failure_payload(
    reason_code: str,
    *,
    config: NotifierTelegramConfig | None,
    redis_metrics: _RedisGroupMetrics | None = None,
    runtime_config_locator: Mapping[str, Any] | None = None,
    runtime_projection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return _restricted_live_queued_worker_once_base_payload(
        status="fail",
        reason_code=reason_code,
        config=config,
        redis_metrics=redis_metrics,
        runtime_config_locator=runtime_config_locator,
        runtime_projection=runtime_projection,
    )


def _restricted_live_queued_worker_once_result_payload(
    *,
    config: NotifierTelegramConfig,
    redis_metrics: _RedisGroupMetrics,
    worker_code: int,
    worker_payload: dict[str, Any],
    runtime_config_locator: Mapping[str, Any] | None = None,
    runtime_projection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    worker_status = _safe_output_token(worker_payload.get("status"), "unknown")
    worker_reason = _safe_output_token(worker_payload.get("reason_code"), "unknown")
    worker_once = {
        "exit_code": worker_code,
        "status": worker_status,
        "reason_code": worker_reason,
        "acked": worker_payload.get("acked") is True,
        "handler_called": worker_payload.get("handler_called") is True,
    }
    if worker_code == 0 and worker_status == "processed" and worker_once["acked"] and worker_once["handler_called"]:
        status = "pass"
        reason_code = "restricted_live_queued_worker_once_processed"
    elif worker_code == 0 and worker_status == "empty":
        status = "noop"
        reason_code = "worker_once_no_message_available"
    elif worker_status == "rejected" or worker_code == 2:
        status = "fail"
        reason_code = "worker_once_rejected"
    else:
        status = "fail"
        reason_code = "worker_once_failed"

    payload = _restricted_live_queued_worker_once_base_payload(
        status=status,
        reason_code=reason_code,
        config=config,
        redis_metrics=redis_metrics,
        worker_payload=worker_payload,
        runtime_config_locator=runtime_config_locator,
        runtime_projection=runtime_projection,
    )
    payload["worker_once"] = worker_once
    if status == "pass":
        delivery_result_summary = _restricted_live_queued_delivery_result_summary(
            worker_payload.get("delivery_result_summary")
        )
        if delivery_result_summary is not None:
            payload["delivery_result_summary"] = delivery_result_summary
    return payload


def _restricted_live_queued_worker_once_base_payload(
    *,
    status: str,
    reason_code: str,
    config: NotifierTelegramConfig | None = None,
    redis_metrics: _RedisGroupMetrics | None = None,
    worker_payload: dict[str, Any] | None = None,
    runtime_config_locator: Mapping[str, Any] | None = None,
    runtime_projection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_version": RESTRICTED_LIVE_QUEUED_WORKER_ONCE_SCHEMA_VERSION,
        "status": status,
        "reason_code": reason_code,
        "redis_precheck": {
            "pending": redis_metrics.pending if redis_metrics is not None else None,
            "lag": redis_metrics.lag if redis_metrics is not None else None,
            "reason_code": redis_metrics.reason_code if redis_metrics is not None else None,
        },
        "authority": _restricted_live_queued_worker_once_authority(config, worker_payload=worker_payload),
    }
    if runtime_config_locator is not None:
        payload["runtime_config_locator"] = dict(runtime_config_locator)
    if runtime_projection is not None:
        payload["runtime_projection"] = dict(runtime_projection)
    return payload


def _restricted_live_queued_send_only_projection(
    config: NotifierTelegramConfig,
) -> tuple[NotifierTelegramConfig, dict[str, Any]]:
    projected = replace(config, allow_edits=False)
    return projected, {
        "mode": "send_only",
        "requested": True,
        "applied": True,
        "source_allow_edits_enabled": bool(config.allow_edits),
        "effective_allow_edits": bool(projected.allow_edits),
        "env_mutated": False,
        "values_printed": False,
        "paths_printed": False,
    }


def _restricted_live_queued_worker_once_authority(
    config: NotifierTelegramConfig | None,
    *,
    worker_payload: dict[str, Any] | None = None,
) -> dict[str, bool]:
    worker_authority = worker_payload.get("authority") if isinstance(worker_payload, dict) else None
    if not isinstance(worker_authority, dict):
        worker_authority = {}
    return {
        "telegram_transport_possible": _worker_authority_bool(
            worker_authority,
            "telegram_transport_possible",
            default=_restricted_live_queued_transport_possible(config),
        ),
        "database_session_opened": _worker_authority_bool(worker_authority, "database_session_opened"),
        "workers_started": _worker_authority_bool(worker_authority, "workers_started"),
        "run_forever_started": _worker_authority_bool(worker_authority, "run_forever_started"),
        "openai_called": _worker_authority_bool(worker_authority, "openai_called"),
        "github_called": _worker_authority_bool(worker_authority, "github_called"),
        "docker_or_systemd_called": _worker_authority_bool(worker_authority, "docker_or_systemd_called"),
        "alembic_or_ddl_ran": _worker_authority_bool(worker_authority, "alembic_or_ddl_ran"),
        "subprocess_started": _worker_authority_bool(worker_authority, "subprocess_started"),
        "shell_invoked": _worker_authority_bool(worker_authority, "shell_invoked"),
    }


def _worker_authority_bool(authority: Mapping[str, Any], name: str, *, default: bool = False) -> bool:
    value = authority.get(name)
    if isinstance(value, bool):
        return value
    return default


def _restricted_live_queued_transport_possible(config: NotifierTelegramConfig | None) -> bool:
    return bool(
        config
        and config.transport_enabled
        and config.telegram_bot_token
        and config.app_env in {"prod", "production"}
        and config.queue_name == EXPECTED_QUEUE_NAME
        and not config.allow_edits
        and _telegram_api_base_url_is_official(config.telegram_api_base_url)
    )


def _restricted_live_queued_delivery_result_summary(value: object) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    return {
        "delivery_status": _safe_output_token(value.get("delivery_status"), "unknown"),
        "attempt_count": _safe_output_int(value.get("attempt_count")),
        "transport_error_code": _safe_optional_output_token(value.get("transport_error_code")),
        "transport_error_class": _safe_optional_output_token(value.get("transport_error_class")),
        "telegram_chat_id_present": value.get("telegram_chat_id_present") is True,
        "telegram_message_id_present": value.get("telegram_message_id_present") is True,
        "retry_after_seconds_present": value.get("retry_after_seconds_present") is True,
        "edited": value.get("edited") is True,
    }


_SAFE_OUTPUT_TOKEN = re.compile(r"^[A-Za-z0-9_]{1,120}$")
_SENSITIVE_OUTPUT_MARKERS = ("password", "secret", "token", "credential", "api_key", "database_url", "redis_url")


def _safe_output_token(value: object, default: str) -> str:
    if value is None:
        return default
    text = str(value)
    lowered = text.lower()
    if not _SAFE_OUTPUT_TOKEN.fullmatch(text):
        return default
    if any(marker in lowered for marker in _SENSITIVE_OUTPUT_MARKERS):
        return default
    return text


def _safe_optional_output_token(value: object) -> str | None:
    if value is None:
        return None
    return _safe_output_token(value, "redacted")


def _safe_output_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


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


def _notification_ux_preview_payload(
    reason_code: str,
    *,
    checks: dict[str, bool] | None = None,
    db_read_attempted: bool = True,
    selection: Mapping[str, Any] | None = None,
    runtime_config_locator: Mapping[str, Any] | None = None,
) -> dict:
    checks = checks or _notification_ux_preview_base_checks()
    payload = {
        "schema_version": NOTIFICATION_UX_RENDER_PREVIEW_SCHEMA_VERSION,
        "status": "fail",
        "reason_code": reason_code,
        "checks_failed": [name for name, passed in checks.items() if not passed],
        "checks": checks,
        "authority": _notification_ux_preview_authority(db_read_attempted=db_read_attempted),
    }
    if selection is not None:
        payload["selection"] = dict(selection)
    if runtime_config_locator is not None:
        payload["runtime_config_locator"] = dict(runtime_config_locator)
    return payload


def _notification_ux_preview_base_checks(
    *,
    plan_found: bool = True,
    analysis_found: bool = True,
    judge_output_found: bool = True,
    candidate_found: bool = True,
) -> dict[str, bool]:
    return {
        "plan_found": plan_found,
        "analysis_found": analysis_found,
        "judge_output_found": judge_output_found,
        "candidate_found": candidate_found,
        "message_nonempty": False,
        "message_under_telegram_limit": False,
        "verdict_visible_in_first_three_lines": False,
        "korean_summary_marker_present": False,
        "skeptical_or_risk_marker_present": False,
        "recommended_action_marker_present": False,
        "link_preview_disabled": False,
        "protect_content_false": False,
        "silent_later_or_normal_profile": False,
        "high_profile_not_silent": False,
        "url_button_present": False,
        "github_primary_button_label": False,
        "primary_url_not_in_message_text_when_button_exists": False,
        "source_url_not_in_message_text_when_button_exists": False,
        "no_url_in_message_text": False,
        "no_uuid_in_message_text": False,
        "no_sensitive_markers_in_message_text": False,
    }


def _notification_ux_render_checks(
    *,
    message_text: str,
    render,
    verdict: str,
    urgency_profile: str,
    candidate,
) -> dict[str, bool]:
    first_lines = _first_nonempty_lines(message_text)
    buttons = _notification_ux_url_buttons(render.reply_markup_json)
    button_urls = {button["url"] for button in buttons}
    primary_url = candidate.primary_canonical_url
    source_url = candidate.source_message_link
    primary_button_exists = bool(primary_url and primary_url in button_urls)
    source_button_exists = bool(source_url and source_url in button_urls)
    github_primary = bool(primary_url and _is_github_url(primary_url)) or candidate.primary_artifact_type == "github_repo"
    sensitive_text = message_text.lower()
    return {
        "plan_found": True,
        "analysis_found": True,
        "judge_output_found": True,
        "candidate_found": True,
        "message_nonempty": bool(message_text.strip()),
        "message_under_telegram_limit": len(message_text) <= 4096,
        "verdict_visible_in_first_three_lines": any(verdict and verdict in line for line in first_lines[:3]),
        "korean_summary_marker_present": "한줄 요약:" in message_text or "요약:" in message_text,
        "skeptical_or_risk_marker_present": "냉정 평가:" in message_text or "리스크:" in message_text,
        "recommended_action_marker_present": "추천 행동:" in message_text,
        "link_preview_disabled": render.link_preview_options_json == {"is_disabled": True},
        "protect_content_false": render.protect_content is False,
        "silent_later_or_normal_profile": (
            render.disable_notification is True
            if urgency_profile == "normal_silent" or verdict == "later"
            else True
        ),
        "high_profile_not_silent": render.disable_notification is False if urgency_profile == "high" else True,
        "url_button_present": bool(buttons) if primary_url or source_url else True,
        "github_primary_button_label": (
            any(button["text"] == "GitHub 열기" and button["url"] == primary_url for button in buttons)
            if github_primary and primary_url
            else True
        ),
        "primary_url_not_in_message_text_when_button_exists": (
            primary_url not in message_text if primary_button_exists and primary_url else True
        ),
        "source_url_not_in_message_text_when_button_exists": (
            source_url not in message_text if source_button_exists and source_url else True
        ),
        "no_url_in_message_text": NOTIFICATION_UX_URL_RE.search(message_text) is None,
        "no_uuid_in_message_text": NOTIFICATION_UX_UUID_RE.search(message_text) is None,
        "no_sensitive_markers_in_message_text": not any(
            word.lower() in sensitive_text for word in NOTIFICATION_UX_SECRET_WORDS
        ),
    }


def _notification_ux_render_summary(render) -> dict[str, Any]:
    buttons = _notification_ux_url_buttons(render.reply_markup_json)
    return {
        "render_hash_suffix": render.render_hash[-8:],
        "message_char_count": len(render.message_text),
        "first_nonempty_lines": [_sanitize_notification_ux_line(line) for line in _first_nonempty_lines(render.message_text)[:5]],
        "button_count": len(buttons),
        "button_labels": [button["text"] for button in buttons],
        "disable_notification": render.disable_notification,
        "protect_content": render.protect_content,
        "link_preview_disabled": render.link_preview_options_json == {"is_disabled": True},
    }


def _notification_ux_url_buttons(reply_markup_json: Any) -> list[dict[str, str]]:
    if not isinstance(reply_markup_json, dict):
        return []
    rows = reply_markup_json.get("inline_keyboard")
    if not isinstance(rows, list):
        return []
    buttons: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, list):
            continue
        for button in row:
            if not isinstance(button, dict):
                continue
            text = _string_from_row(button.get("text"))
            url = _string_from_row(button.get("url"))
            if text and url:
                buttons.append({"text": text, "url": url})
    return buttons


def _first_nonempty_lines(message_text: str) -> list[str]:
    return [line.strip() for line in message_text.splitlines() if line.strip()]


def _sanitize_notification_ux_line(line: str) -> str:
    sanitized = NOTIFICATION_UX_UUID_RE.sub("[redacted-id]", line)
    sanitized = NOTIFICATION_UX_URL_RE.sub("[redacted-url]", sanitized)
    for word in NOTIFICATION_UX_SECRET_WORDS:
        sanitized = re.sub(re.escape(word), "[redacted-sensitive-marker]", sanitized, flags=re.IGNORECASE)
    return sanitized[:180]


def _is_github_url(value: str) -> bool:
    host = urlparse(value).netloc.lower()
    return host == "github.com" or host.endswith(".github.com")


def _notification_ux_preview_authority(*, db_read_attempted: bool = True) -> dict[str, bool]:
    return {
        "db_read_attempted": db_read_attempted,
        "db_write_attempted": False,
        "redis_attempted": False,
        "telegram_transport_attempted": False,
        "openai_called": False,
        "github_called": False,
        "x_called": False,
        "web_called": False,
        "docker_or_systemd_called": False,
        "alembic_or_ddl_ran": False,
        "runtime_env_values_printed": False,
    }


def _notification_ux_preview_selection_payload(*, latest: bool, selected: bool) -> dict[str, Any] | None:
    if not latest:
        return None
    return {
        "mode": "latest_eligible",
        "selected": selected,
        "raw_id_printed": False,
    }


async def _load_latest_eligible_notification_plan(repository) -> dict[str, Any] | None:
    loader = getattr(repository, "load_latest_eligible_notification_plan", None)
    if callable(loader):
        row = await loader()
        return dict(row) if row is not None else None

    session = getattr(repository, "_session", None)
    if session is None:
        return None
    import sqlalchemy as sa

    result = await session.execute(
        sa.text(
            """
            SELECT p.notification_plan_id, p.analysis_id, p.candidate_group_id, p.delivery_decision,
                   p.urgency_profile, p.target_chat_id, p.target_thread_id, p.render_profile,
                   p.dedupe_subject_key, p.material_change_hash, p.send_after,
                   p.suppress_reason_code, p.status
            FROM notification_plans p
            JOIN analyses a
              ON a.analysis_id = p.analysis_id
             AND a.candidate_group_id = p.candidate_group_id
            JOIN judge_outputs jo
              ON jo.judge_output_id = a.judge_output_id
            JOIN candidate_group_proposals cgp
              ON cgp.candidate_group_id = p.candidate_group_id
            WHERE p.delivery_decision::text = 'send_now'
              AND p.urgency_profile::text IN ('normal_silent', 'high')
              AND a.verdict::text IN ('later', 'inspect_now')
            ORDER BY
              CASE
                WHEN p.status::text IN ('sent', 'edited') THEN 0
                WHEN p.status::text IN ('rendered', 'queued') THEN 1
                WHEN p.status::text = 'planned' THEN 2
                ELSE 3
              END ASC,
              p.created_at DESC,
              p.notification_plan_id DESC
            LIMIT 1
            """
        )
    )
    row = result.mappings().first()
    return dict(row) if row else None


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
    if env_file_overlay is None and not env_file:
        raise _NotifierOneShotRuntimeConfigError("env_file_no_runtime_config")

    overlay = env_file_overlay if env_file_overlay is not None else _resolve_one_shot_runtime_env_file_overlay(env_file)
    return _load_notifier_one_shot_runtime_config_from_overlay(overlay)


def _load_notifier_one_shot_runtime_config_from_overlay(overlay: Mapping[str, str]) -> NotifierTelegramConfig:
    try:
        with _temporary_environment_defaults(overlay):
            return NotifierTelegramConfig.from_env(require_transport_token=False)
    except (NotifierTelegramConfigurationError, ValueError, TypeError):
        raise _NotifierOneShotRuntimeConfigError("notifier_runtime_config_error") from None


def _load_restricted_live_queued_runtime_config(
    args: argparse.Namespace,
) -> _RestrictedLiveQueuedRuntimeConfigResult:
    process_env = getattr(args, "_restricted_live_queued_worker_once_process_env", None)
    if process_env is None:
        process_env = os.environ
    discovery_roots = getattr(args, "_restricted_live_queued_worker_once_discovery_roots", None)

    process_values = _one_shot_runtime_values_from_mapping(process_env)
    process_config = _notifier_one_shot_runtime_config_from_values(process_values)
    if process_config is not None:
        return _RestrictedLiveQueuedRuntimeConfigResult(
            config=process_config,
            locator=_restricted_live_queued_runtime_config_locator(
                process_env_used=True,
                bounded_candidate_file_count=0,
                bounded_candidate_files_with_runtime_config_key_count=0,
                values=process_values,
                config=process_config,
            ),
        )

    env_file = getattr(args, "env_file", None)
    if env_file:
        try:
            env_values = _read_minimal_env_file(str(env_file), allowed_keys=ONE_SHOT_RUNTIME_ENV_KEYS)
        except ValueError:
            env_values = {}
        merged_values = _merge_one_shot_runtime_values(env_values, process_values)
        env_config = _notifier_one_shot_runtime_config_from_values(merged_values)
        if env_config is not None:
            return _RestrictedLiveQueuedRuntimeConfigResult(
                config=env_config,
                locator=_restricted_live_queued_runtime_config_locator(
                    process_env_used=False,
                    bounded_candidate_file_count=0,
                    bounded_candidate_files_with_runtime_config_key_count=0,
                    values=merged_values,
                    config=env_config,
                ),
            )

    discovered = _discover_restricted_live_queued_runtime_config(
        process_values=process_values,
        roots=discovery_roots,
    )
    if discovered is not None:
        return discovered

    candidate_count, runtime_key_count, discovered_values = _count_restricted_live_runtime_candidate_files(
        roots=discovery_roots,
    )
    raise _RestrictedLiveQueuedRuntimeConfigDiscoveryError(
        "runtime_config_not_found",
        _restricted_live_queued_runtime_config_locator(
            process_env_used=False,
            bounded_candidate_file_count=candidate_count,
            bounded_candidate_files_with_runtime_config_key_count=runtime_key_count,
            values=_merge_one_shot_runtime_values(discovered_values, process_values),
        ),
    )


def _notifier_one_shot_runtime_config_from_values(values: Mapping[str, str]) -> NotifierTelegramConfig | None:
    try:
        overlay = _resolve_one_shot_runtime_values_overlay(values, source={})
        return _load_notifier_one_shot_runtime_config_from_overlay(overlay)
    except _NotifierOneShotRuntimeConfigError:
        return None


def _one_shot_runtime_values_from_mapping(values: Mapping[str, str]) -> dict[str, str]:
    return {
        key: str(values.get(key, "") or "").strip()
        for key in ONE_SHOT_RUNTIME_ENV_KEYS
        if str(values.get(key, "") or "").strip()
    }


def _merge_one_shot_runtime_values(
    values: Mapping[str, str],
    fallback_values: Mapping[str, str],
) -> dict[str, str]:
    merged = _one_shot_runtime_values_from_mapping(values)
    for key, value in _one_shot_runtime_values_from_mapping(fallback_values).items():
        merged[key] = value
    return merged


def _discover_restricted_live_queued_runtime_config(
    *,
    process_values: Mapping[str, str],
    roots: list[Path] | None = None,
) -> _RestrictedLiveQueuedRuntimeConfigResult | None:
    candidate_count = 0
    runtime_key_count = 0
    for candidate in _iter_restricted_live_runtime_candidate_files(roots=roots):
        candidate_count += 1
        try:
            values = _read_minimal_env_file(str(candidate), allowed_keys=ONE_SHOT_RUNTIME_ENV_KEYS)
        except ValueError:
            values = {}
        runtime_values = _one_shot_runtime_values_from_mapping(values)
        if runtime_values:
            runtime_key_count += 1
        config_values = _merge_one_shot_runtime_values(runtime_values, process_values)
        config = _notifier_one_shot_runtime_config_from_values(config_values)
        if config is not None:
            return _RestrictedLiveQueuedRuntimeConfigResult(
                config=config,
                locator=_restricted_live_queued_runtime_config_locator(
                    process_env_used=False,
                    bounded_candidate_file_count=candidate_count,
                    bounded_candidate_files_with_runtime_config_key_count=runtime_key_count,
                    values=config_values,
                    config=config,
                ),
            )
    return None


def _count_restricted_live_runtime_candidate_files(
    *,
    roots: list[Path] | None = None,
) -> tuple[int, int, dict[str, str]]:
    candidate_count = 0
    runtime_key_count = 0
    discovered_values: dict[str, str] = {}
    for candidate in _iter_restricted_live_runtime_candidate_files(roots=roots):
        candidate_count += 1
        try:
            values = _read_minimal_env_file(str(candidate), allowed_keys=ONE_SHOT_RUNTIME_ENV_KEYS)
        except ValueError:
            values = {}
        runtime_values = _one_shot_runtime_values_from_mapping(values)
        if runtime_values:
            runtime_key_count += 1
            discovered_values.update(runtime_values)
    return candidate_count, runtime_key_count, discovered_values


def _restricted_live_queued_runtime_config_locator(
    *,
    process_env_used: bool,
    bounded_candidate_file_count: int,
    bounded_candidate_files_with_runtime_config_key_count: int,
    values: Mapping[str, str],
    config: NotifierTelegramConfig | None = None,
) -> dict[str, Any]:
    return {
        "process_env_checked": True,
        "process_env_used": process_env_used,
        "bounded_candidate_file_count": bounded_candidate_file_count,
        "bounded_candidate_files_with_runtime_config_key_count": bounded_candidate_files_with_runtime_config_key_count,
        "database_config_present": bool(config.database_url)
        if config is not None
        else _runtime_config_key_present(values, "DATABASE_URL"),
        "redis_config_present": bool(config.redis_url)
        if config is not None
        else _runtime_config_key_present(values, "REDIS_URL"),
        "telegram_bot_token_present": bool(config.telegram_bot_token)
        if config is not None
        else _runtime_config_key_present(values, "TELEGRAM_BOT_TOKEN"),
        "paths_printed": False,
        "values_printed": False,
    }


def _runtime_config_key_present(values: Mapping[str, str], key: str) -> bool:
    if str(values.get(key, "") or "").strip():
        return True
    return bool(str(values.get(f"{key}_FILE", "") or "").strip())


def _iter_restricted_live_runtime_candidate_files(
    *,
    roots: list[Path] | None = None,
):
    candidate_roots = roots if roots is not None else _restricted_live_runtime_candidate_roots()
    yield from _iter_notification_ux_preview_db_runtime_candidate_files(roots=candidate_roots)


def _restricted_live_runtime_candidate_roots() -> list[Path]:
    repo_root = Path(__file__).resolve().parents[3]
    roots = [
        repo_root,
        Path("/home/deploy"),
        Path("/srv/catchbot"),
        Path("/srv/github_ai_catchbot"),
        Path("/opt/catchbot"),
        Path("/opt/github_ai_catchbot"),
        Path("/etc/catchbot"),
        Path("/etc/github_ai_catchbot"),
    ]
    deduped: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        try:
            resolved = root.resolve()
        except OSError:
            resolved = root
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(root)
    return deduped


def _load_notification_ux_preview_config(args: argparse.Namespace) -> _NotificationUxPreviewRuntimeConfigResult:
    process_env = getattr(args, "_notification_ux_preview_process_env", None)
    if process_env is None:
        process_env = os.environ
    discovery_roots = getattr(args, "_notification_ux_preview_discovery_roots", None)

    process_config = _notification_ux_preview_config_from_values(process_env)
    if process_config is not None:
        return _NotificationUxPreviewRuntimeConfigResult(
            config=process_config,
            locator=_notification_ux_preview_runtime_config_locator(
                process_env_used=True,
                bounded_candidate_file_count=0,
                bounded_candidate_files_with_database_key_count=0,
            ),
        )

    env_file = getattr(args, "env_file", None)
    if env_file:
        try:
            env_values = _read_minimal_env_file(
                str(env_file),
                allowed_keys=NOTIFICATION_UX_DB_RUNTIME_ENV_KEYS,
            )
        except ValueError:
            env_values = {}
        env_config = _notification_ux_preview_config_from_values(
            env_values,
            fallback_values=process_env,
        )
        if env_config is not None:
            return _NotificationUxPreviewRuntimeConfigResult(
                config=env_config,
                locator=_notification_ux_preview_runtime_config_locator(
                    process_env_used=False,
                    bounded_candidate_file_count=0,
                    bounded_candidate_files_with_database_key_count=0,
                ),
            )

    if getattr(args, "discover_db_config", False):
        discovered = _discover_notification_ux_preview_db_runtime_config(
            process_env=process_env,
            roots=discovery_roots,
        )
        if discovered is not None:
            return discovered

    raise _NotifierOneShotRuntimeConfigError("runtime_database_config_not_found")


def _notification_ux_preview_runtime_config_locator_from_args(args: argparse.Namespace) -> dict[str, Any]:
    candidate_count = 0
    database_key_count = 0
    if getattr(args, "discover_db_config", False):
        roots = getattr(args, "_notification_ux_preview_discovery_roots", None)
        candidate_count, database_key_count = _count_notification_ux_preview_db_runtime_candidate_files(roots=roots)
    return _notification_ux_preview_runtime_config_locator(
        process_env_used=False,
        bounded_candidate_file_count=candidate_count,
        bounded_candidate_files_with_database_key_count=database_key_count,
    )


def _notification_ux_preview_runtime_config_locator(
    *,
    process_env_used: bool,
    bounded_candidate_file_count: int,
    bounded_candidate_files_with_database_key_count: int,
) -> dict[str, Any]:
    return {
        "process_env_checked": True,
        "process_env_used": process_env_used,
        "bounded_candidate_file_count": bounded_candidate_file_count,
        "bounded_candidate_files_with_database_key_count": bounded_candidate_files_with_database_key_count,
        "paths_printed": False,
        "values_printed": False,
    }


def _notification_ux_preview_config_from_values(
    values: Mapping[str, str],
    *,
    fallback_values: Mapping[str, str] | None = None,
) -> _NotificationUxPreviewRuntimeConfig | None:
    database_url = str(values.get("DATABASE_URL", "") or "").strip()
    if not database_url:
        database_url_file = str(values.get("DATABASE_URL_FILE", "") or "").strip()
        if database_url_file:
            try:
                database_url = _read_runtime_secret_file(
                    database_url_file,
                    missing_reason_code="runtime_database_config_not_found",
                    empty_reason_code="runtime_database_config_not_found",
                )
            except ValueError:
                database_url = ""
    if not database_url:
        return None

    max_chars_value = str(values.get("NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS", "") or "").strip()
    if not max_chars_value and fallback_values is not None:
        max_chars_value = str(fallback_values.get("NOTIFIER_TELEGRAM_MAX_MESSAGE_CHARS", "") or "").strip()
    max_message_chars = _notification_ux_preview_max_message_chars(max_chars_value)
    return _NotificationUxPreviewRuntimeConfig(
        database_url=database_url,
        max_message_chars=max_message_chars,
    )


def _notification_ux_preview_max_message_chars(value: str) -> int:
    if not value:
        return NOTIFICATION_UX_PREVIEW_DEFAULT_MAX_MESSAGE_CHARS
    try:
        parsed = int(value)
    except ValueError:
        raise _NotifierOneShotRuntimeConfigError("notifier_runtime_config_error") from None
    if parsed < 500 or parsed > 4096:
        raise _NotifierOneShotRuntimeConfigError("notifier_runtime_config_error")
    return parsed


def _discover_notification_ux_preview_db_runtime_config(
    *,
    process_env: Mapping[str, str],
    roots: list[Path] | None = None,
) -> _NotificationUxPreviewRuntimeConfigResult | None:
    candidate_count = 0
    database_key_count = 0
    for candidate in _iter_notification_ux_preview_db_runtime_candidate_files(roots=roots):
        candidate_count += 1
        try:
            values = _read_minimal_env_file(str(candidate), allowed_keys=NOTIFICATION_UX_DB_RUNTIME_ENV_KEYS)
        except ValueError:
            values = {}
        if "DATABASE_URL" in values or "DATABASE_URL_FILE" in values:
            database_key_count += 1
        config = _notification_ux_preview_config_from_values(values, fallback_values=process_env)
        if config is not None:
            return _NotificationUxPreviewRuntimeConfigResult(
                config=config,
                locator=_notification_ux_preview_runtime_config_locator(
                    process_env_used=False,
                    bounded_candidate_file_count=candidate_count,
                    bounded_candidate_files_with_database_key_count=database_key_count,
                ),
            )
    return None


def _count_notification_ux_preview_db_runtime_candidate_files(*, roots: list[Path] | None = None) -> tuple[int, int]:
    candidate_count = 0
    database_key_count = 0
    for candidate in _iter_notification_ux_preview_db_runtime_candidate_files(roots=roots):
        candidate_count += 1
        try:
            values = _read_minimal_env_file(str(candidate), allowed_keys=NOTIFICATION_UX_DB_RUNTIME_ENV_KEYS)
        except ValueError:
            values = {}
        if "DATABASE_URL" in values or "DATABASE_URL_FILE" in values:
            database_key_count += 1
    return candidate_count, database_key_count


def _iter_notification_ux_preview_db_runtime_candidate_files(
    *,
    roots: list[Path] | None = None,
):
    candidate_roots = roots if roots is not None else _notification_ux_preview_db_runtime_candidate_roots()
    seen: set[Path] = set()
    yielded = 0
    for root in candidate_roots:
        if yielded >= NOTIFICATION_UX_DB_RUNTIME_DISCOVERY_MAX_FILES:
            return
        root_path = Path(root)
        if not root_path.exists():
            continue
        try:
            resolved_root = root_path.resolve()
        except OSError:
            continue
        if resolved_root in seen:
            continue
        seen.add(resolved_root)
        for candidate in _walk_notification_ux_preview_db_runtime_candidate_files(resolved_root):
            if yielded >= NOTIFICATION_UX_DB_RUNTIME_DISCOVERY_MAX_FILES:
                return
            yielded += 1
            yield candidate


def _notification_ux_preview_db_runtime_candidate_roots() -> list[Path]:
    repo_root = Path(__file__).resolve().parents[3]
    roots = [
        repo_root,
        repo_root.parent,
        Path("/home/deploy"),
        Path("/srv/catchbot"),
        Path("/srv/github_ai_catchbot"),
        Path("/opt/catchbot"),
        Path("/opt/github_ai_catchbot"),
        Path("/etc/catchbot"),
        Path("/etc/github_ai_catchbot"),
    ]
    deduped: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        try:
            resolved = root.resolve()
        except OSError:
            resolved = root
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(root)
    return deduped


def _walk_notification_ux_preview_db_runtime_candidate_files(root: Path):
    stack: list[tuple[Path, int]] = [(root, 0)]
    while stack:
        current, depth = stack.pop()
        try:
            entries = sorted(os.scandir(current), key=lambda entry: entry.name)
        except OSError:
            continue
        child_dirs: list[Path] = []
        for entry in entries:
            name = entry.name
            try:
                if entry.is_file(follow_symlinks=False) and name in NOTIFICATION_UX_DB_RUNTIME_CANDIDATE_FILE_NAMES:
                    yield Path(entry.path)
                elif (
                    depth < NOTIFICATION_UX_DB_RUNTIME_DISCOVERY_MAX_DEPTH
                    and entry.is_dir(follow_symlinks=False)
                    and name not in NOTIFICATION_UX_DB_RUNTIME_DISCOVERY_SKIP_DIRS
                ):
                    child_dirs.append(Path(entry.path))
            except OSError:
                continue
        for child in reversed(child_dirs):
            stack.append((child, depth + 1))


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
    return _resolve_one_shot_runtime_values_overlay(values, source=source)


def _resolve_one_shot_runtime_values_overlay(
    values: Mapping[str, str],
    *,
    source: Mapping[str, str],
) -> dict[str, str]:
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
    if command == "restricted-transport-canary":
        return await _run_restricted_transport_canary_command(args)
    if command == "worker-once":
        return await _run_worker_once_command(args)
    if command == "create-canary-plan":
        return await _run_create_canary_plan_command(args)
    if command == "send-disabled-worker-once-proof":
        return await _run_send_disabled_worker_once_proof_command(args)
    if command == "restricted-live-worker-once-proof":
        return await _run_restricted_live_worker_once_proof_command(args)
    if command == "restricted-live-queued-worker-once":
        return await _run_restricted_live_queued_worker_once_command(args)
    if command == "send-canary":
        return await _run_send_canary_command(args)
    if command == "notification-ux-render-preview":
        return await _run_notification_ux_render_preview_command(args)
    if command == "worker":
        config = NotifierTelegramConfig.from_env()
        return await _run_worker(config)
    parser.error(f"unsupported command: {command}")
    return 2


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
