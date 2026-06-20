from __future__ import annotations

import asyncio
import argparse
import json
import logging
import os
import sys
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from .batch_recovery import prepare_delivery_replay_requests_for_selected_plans
from .batch_recovery_tool import DeliveryBatchRecoveryTool
from .config import MaintenanceConfig, MaintenanceConfigurationError
from .controlled_worker_activation import (
    ControlledWorkerActivationReport,
    ControlledWorkerActivationRequest,
    controlled_worker_activation_request_error,
    run_controlled_worker_activation,
)
from .db_shape_preflight import (
    SCHEMA_VERSION as DB_SHAPE_PREFLIGHT_SCHEMA_VERSION,
    SqlAlchemyDbShapeIntrospectionRepository,
    run_db_shape_preflight,
)
from .delivery_gate_runner import DeliveryGateRunner
from .delivery_gate_preflight import (
    load_delivery_gate_preflight_report,
    run_delivery_gate_preflight,
)
from .delivery_gate_preflight_invocation import run_delivery_gate_preflight_invocation_proof
from .delivery_replay import REPLAY_REQUESTED_EVENT_TYPE
from .delivery_retry import DELIVERY_RESULT_EVENT_TYPE
from .foreground_smoke import (
    ForegroundSmokeReport,
    ForegroundSmokeRequest,
    foreground_smoke_request_error,
    run_foreground_smoke,
)
from .mvp_readiness import run_restricted_live_mvp_readiness
from .redis_streams import RedisStreamConsumer
from .restricted_queue_activation import (
    RestrictedQueueActivationReport,
    RestrictedQueueActivationRequest,
    run_restricted_queue_activation,
)
from .restricted_queue_group_bootstrap import (
    RestrictedQueueGroupBootstrapReport,
    RestrictedQueueGroupBootstrapRequest,
    run_restricted_queue_group_bootstrap,
)
from .repositories import MaintenanceRepository
from .service import MaintenanceService
from .systemd_rollout import (
    SERVICE_NAME as SYSTEMD_ROLLOUT_SERVICE_NAME,
    SystemdDiagnosticRequest,
    SystemdRolloutRequest,
    run_systemd_diagnostic,
    run_systemd_rollout,
)
from .worker import DueRetryPromotionWorker, MaintenanceQueueWorker, ReplayQueueWorker
from .worker_once import WorkerOnceReport, WorkerOnceRequest, run_worker_once
from .worker_startup_probe import (
    WorkerStartupProbeReport,
    WorkerStartupProbeRequest,
    build_worker_startup_probe_blocked_report,
    run_worker_startup_probe,
    worker_startup_probe_request_error,
)

DB_SHAPE_PREFLIGHT_SOURCE_REASON_CODES = {
    "database_url_required",
    "database_url_file_missing",
    "database_url_file_empty",
    "env_file_missing",
    "env_file_no_database_url",
    "env_file_database_url_file_missing",
    "env_file_database_url_file_empty",
    "ambiguous_database_url_source",
}

ONE_SHOT_RUNTIME_CONFIG_SCHEMA_VERSION = "maintenance_one_shot_runtime_config_v1"
ONE_SHOT_RUNTIME_CONFIG_REASON_CODES = {
    "env_file_missing",
    "env_file_no_runtime_config",
    "env_file_database_url_file_missing",
    "env_file_database_url_file_empty",
    "env_file_redis_url_file_missing",
    "env_file_redis_url_file_empty",
    "maintenance_runtime_config_error",
}

ONE_SHOT_RUNTIME_ENV_VALUE_KEYS = {
    "APP_ENV",
    "DATABASE_URL",
    "REDIS_URL",
    "MAINTENANCE_QUEUE_NAME",
    "MAINTENANCE_CONSUMER_GROUP",
    "MAINTENANCE_CONSUMER_NAME",
    "REPLAY_QUEUE_NAME",
    "REPLAY_CONSUMER_GROUP",
    "REPLAY_CONSUMER_NAME",
    "MAINTENANCE_BATCH_SIZE",
    "MAINTENANCE_BLOCK_MS",
    "MAINTENANCE_NOTIFICATION_RETRY_POLL_SEC",
    "DELIVERY_RETRY_MAX_ATTEMPTS",
    "NOTIFICATION_RETRY_MAX_ATTEMPTS",
    "ENABLE_NOTIFICATION_SEND",
    "NOTIFIER_TELEGRAM_DRY_RUN",
    "NOTIFIER_TELEGRAM_ALLOW_EDITS",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
    "ENABLE_REPLAY_TO_PROD_DB",
    "DELIVERY_GATE_MIN_SUCCESS_RATE_1H",
    "DELIVERY_GATE_MIN_SUCCESS_RATE_24H",
    "DELIVERY_GATE_MAX_HIGH_SOURCE_TO_DELIVERY_P95_SEC",
    "DELIVERY_GATE_MAX_PLAN_TO_TRANSPORT_P95_SEC",
    "DELIVERY_GATE_MAX_DUE_RETRY_LAG_SEC",
    "DELIVERY_GATE_MAX_OPEN_DLQ_COUNT",
    "DELIVERY_GATE_MAX_SEND_DISABLED_COUNT",
    "DELIVERY_GATE_MAX_REPLAY_GUARD_REJECT_COUNT",
    "DELIVERY_GATE_REQUIRE_OPERATOR_REVIEW_FOR_FULL",
    "LOG_LEVEL",
}
ONE_SHOT_RUNTIME_ENV_FILE_KEYS = {"DATABASE_URL_FILE", "REDIS_URL_FILE"}
ONE_SHOT_RUNTIME_ENV_KEYS = ONE_SHOT_RUNTIME_ENV_VALUE_KEYS | ONE_SHOT_RUNTIME_ENV_FILE_KEYS


class _MaintenanceOneShotRuntimeConfigError(ValueError):
    pass


def _build_logger(level: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
    return logging.getLogger("maintenance")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="maintenance")
    subcommands = parser.add_subparsers(dest="command")

    subcommands.add_parser("worker")

    worker_startup_probe = subcommands.add_parser("worker-startup-probe")
    worker_startup_probe.add_argument("--mode", required=True)
    worker_startup_probe.add_argument("--confirm", default=None)
    worker_startup_probe.add_argument("--env-file", required=True)

    worker_once = subcommands.add_parser("worker-once")
    worker_once_subcommands = worker_once.add_subparsers(dest="worker_type", required=True)
    for worker_type in ("maintenance", "replay"):
        once = worker_once_subcommands.add_parser(worker_type)
        once.add_argument("--mode", choices=["execute"], required=True)
        once.add_argument("--max-messages", type=int, default=1)
        once.add_argument("--confirm", choices=["ack"], default=None)
        once.add_argument("--env-file", required=True)

    foreground_smoke = subcommands.add_parser("foreground-smoke")
    foreground_smoke.add_argument("--mode", choices=["execute"], required=True)
    foreground_smoke.add_argument("--ticks", type=int, default=1)
    foreground_smoke.add_argument("--max-messages", type=int, default=1)
    foreground_smoke.add_argument("--confirm", choices=["run"], default=None)
    foreground_smoke.add_argument("--env-file", required=True)

    controlled_worker = subcommands.add_parser("controlled-worker")
    controlled_worker.add_argument("--mode", required=True)
    controlled_worker.add_argument("--max-ticks", type=int, default=3)
    controlled_worker.add_argument("--max-runtime-sec", type=int, default=30)
    controlled_worker.add_argument("--max-messages", type=int, default=1)
    controlled_worker.add_argument("--idle-sleep-ms", type=int, default=100)
    controlled_worker.add_argument("--confirm", choices=["run"], default=None)
    controlled_worker.add_argument("--env-file", required=True)

    systemd_rollout = subcommands.add_parser("systemd-rollout")
    systemd_rollout.add_argument(
        "--mode",
        choices=["plan", "install", "start", "proof", "rollback", "diagnose"],
        required=True,
    )
    systemd_rollout.add_argument("--target", required=True)
    systemd_rollout.add_argument("--confirm", choices=["install", "start", "rollback"], default=None)
    systemd_rollout.add_argument("--env-file", required=True)
    systemd_rollout.add_argument("--repo-root")
    systemd_rollout.add_argument("--python-executable")
    systemd_rollout.add_argument("--systemd-user-dir")

    gate = subcommands.add_parser("delivery-gate")
    gate.add_argument("--mode", choices=["restricted", "full"], required=True)
    gate.add_argument("--format", choices=["json"], default="json")
    gate.add_argument("--operator-review-passed", choices=["true", "false"], default=None)
    gate.add_argument("--env-file")

    gate_preflight = subcommands.add_parser("delivery-gate-preflight")
    gate_preflight.add_argument("--mode", required=True)
    gate_preflight.add_argument("--operator-review-passed", action="store_true")
    gate_preflight.add_argument("--output", default="json")
    gate_preflight.add_argument("--env-file")

    gate_preflight_invocation = subcommands.add_parser("delivery-gate-preflight-invocation-proof")
    gate_preflight_invocation.add_argument("--mode", required=True)
    gate_preflight_invocation.add_argument("--operator-review-passed", action="store_true")
    gate_preflight_invocation.add_argument("--output", default="json")
    gate_preflight_invocation.add_argument(
        "--require-gate-status",
        choices=["pass", "warn", "fail"],
    )

    mvp = subcommands.add_parser("mvp-readiness")
    mvp.add_argument("--mode", choices=["restricted"], required=True)
    mvp.add_argument("--format", choices=["json"], default="json")
    mvp.add_argument("--operator-review-passed", choices=["true", "false"], default=None)
    mvp.add_argument("--env-file")

    db_shape = subcommands.add_parser("db-shape-preflight")
    db_shape.add_argument("--format", choices=["json"], default="json")
    db_shape.add_argument("--database-url-file")
    db_shape.add_argument("--env-file")

    delivery_result = subcommands.add_parser("delivery-result")
    delivery_result.add_argument("--mode", choices=["plan", "execute", "proof"], required=True)
    delivery_result.add_argument("--event-id-suffix", required=True)
    delivery_result.add_argument("--confirm", choices=["write"], default=None)
    delivery_result.add_argument("--env-file")

    due_retry = subcommands.add_parser("due-retry")
    due_retry.add_argument("--mode", choices=["plan", "execute", "proof"], required=True)
    due_retry.add_argument("--limit", type=int, default=50)
    due_retry.add_argument("--now-utc")
    due_retry.add_argument("--confirm", choices=["write"], default=None)
    due_retry.add_argument("--env-file")

    delivery_replay = subcommands.add_parser("delivery-replay")
    delivery_replay.add_argument("--mode", choices=["plan", "execute", "proof"], required=True)
    delivery_replay.add_argument("--replay-request-id", required=True)
    delivery_replay.add_argument("--confirm", choices=["write"], default=None)
    delivery_replay.add_argument("--env-file")

    queue_activate = subcommands.add_parser("queue-activate")
    queue_activate_subcommands = queue_activate.add_subparsers(dest="activation_queue", required=True)
    for queue_name in ("maintenance", "replay"):
        activate = queue_activate_subcommands.add_parser(queue_name)
        activate.add_argument("--mode", choices=["plan", "execute", "proof"], required=True)
        activate.add_argument("--max-messages", type=int, default=1)
        activate.add_argument("--confirm", choices=["ack"], default=None)
        activate.add_argument("--allow-create-group", action="store_true")
        activate.add_argument("--exact-trigger-event-id")
        activate.add_argument("--env-file", required=True)

    queue_group_bootstrap = subcommands.add_parser("queue-group-bootstrap")
    queue_group_bootstrap_subcommands = queue_group_bootstrap.add_subparsers(
        dest="bootstrap_queue",
        required=True,
    )
    for queue_name in ("maintenance", "replay"):
        bootstrap = queue_group_bootstrap_subcommands.add_parser(queue_name)
        bootstrap.add_argument("--mode", choices=["plan", "execute", "proof"], required=True)
        bootstrap.add_argument("--confirm", choices=["create-group"], default=None)
        bootstrap.add_argument("--env-file", required=True)

    recovery = subcommands.add_parser("batch-recovery")
    recovery_subcommands = recovery.add_subparsers(dest="recovery_mode", required=True)

    replay = recovery_subcommands.add_parser("replay-selected")
    replay.add_argument("--plan-id", action="append", required=True)
    replay.add_argument("--requested-by", required=True)
    replay.add_argument("--operator-confirmed", action="store_true")
    replay.add_argument("--env-file")

    retry = recovery_subcommands.add_parser("retry-selected-due")
    retry.add_argument("--plan-id", action="append", required=True)
    retry.add_argument("--requested-by", required=True)
    retry.add_argument("--confirm", choices=["write"], required=True)
    retry.add_argument("--env-file")
    return parser


class _NoWriteSelectedReplayRepository:
    async def load_selected_recovery_rows(self, notification_plan_ids: list[UUID]):
        raise AssertionError("unconfirmed or invalid replay-selected command must not load selected rows")

    async def insert_replay_requests_for_selected_plans(self, *, plan_ids: list[UUID], requested_by: str) -> int:
        raise AssertionError("unconfirmed or invalid replay-selected command must not write replay requests")


async def run_replay_selected_batch_recovery(args: argparse.Namespace, repository, *, emit_json=print) -> int:
    selected_plan_ids, invalid_plan_ids = _normalize_notification_plan_ids(args.plan_id)
    if invalid_plan_ids:
        emit_json(_to_json(_invalid_plan_id_result(args.plan_id, invalid_plan_ids)))
        return 2

    result = await prepare_delivery_replay_requests_for_selected_plans(
        repository=repository,
        selected_plan_ids=selected_plan_ids,
        requested_by=args.requested_by,
        operator_confirmed=bool(args.operator_confirmed),
    )
    emit_json(_to_json(asdict(result)))
    return 2 if result.status == "rejected" else 0


async def run_retry_selected_due_batch_recovery(
    config: MaintenanceConfig,
    args: argparse.Namespace,
    repository,
    *,
    emit_json=print,
) -> int:
    selected_plan_ids, invalid_plan_ids = _normalize_notification_plan_ids(args.plan_id)
    if invalid_plan_ids:
        emit_json(_to_json(_invalid_retry_plan_id_result(args.plan_id, invalid_plan_ids)))
        return 2

    tool = DeliveryBatchRecoveryTool(config, repository=repository)
    result = await tool.retry_selected_due(plan_ids=selected_plan_ids, requested_by=args.requested_by)
    emit_json(_to_json(asdict(result)))
    return 0


async def run_mvp_readiness(
    config: MaintenanceConfig,
    args: argparse.Namespace,
    delivery_gate_runner,
    *,
    recovery_cli_surface: dict[str, bool],
    upstream_component_statuses: dict[str, str] | None = None,
    emit_json=print,
) -> int:
    del args
    report = await run_restricted_live_mvp_readiness(
        config=config,
        delivery_gate_runner=delivery_gate_runner,
        recovery_cli_surface=recovery_cli_surface,
        upstream_component_statuses=upstream_component_statuses,
    )
    emit_json(_to_json(asdict(report)))
    if report.readiness_status == "pass":
        return 0
    if report.readiness_status == "warn":
        return 3
    return 2


async def run_db_shape_preflight_command(
    repository,
    *,
    emit_json=print,
    report_runner=run_db_shape_preflight,
    database_url_source: str | None = None,
) -> int:
    report = await report_runner(repository)
    payload = asdict(report)
    if database_url_source is not None:
        payload["database_url_source"] = database_url_source
    emit_json(_to_json(payload))
    return 0 if report.status == "pass" else 2


def _normalize_notification_plan_ids(raw_plan_ids: list[str]) -> tuple[list[UUID], list[str]]:
    selected_plan_ids: list[UUID] = []
    invalid_plan_ids: list[str] = []
    for raw_plan_id in raw_plan_ids:
        try:
            selected_plan_ids.append(UUID(str(raw_plan_id)))
        except (TypeError, ValueError, AttributeError):
            invalid_plan_ids.append(str(raw_plan_id))
    return selected_plan_ids, invalid_plan_ids


def _has_invalid_notification_plan_id(raw_plan_ids: list[str]) -> bool:
    return bool(_normalize_notification_plan_ids(raw_plan_ids)[1])


def _invalid_plan_id_result(raw_plan_ids: list[str], invalid_plan_ids: list[str]) -> dict:
    invalid = set(invalid_plan_ids)
    return {
        "status": "rejected",
        "reason_code": "invalid_notification_plan_id",
        "requested_count": len(raw_plan_ids),
        "created_count": 0,
        "skipped_count": len(raw_plan_ids),
        "results": [
            {
                "notification_plan_id": str(raw_plan_id),
                "action": "skipped",
                "reason_code": (
                    "invalid_notification_plan_id"
                    if str(raw_plan_id) in invalid
                    else "batch_recovery_input_rejected"
                ),
                "replay_request_created": False,
            }
            for raw_plan_id in raw_plan_ids
        ],
    }


def _invalid_retry_plan_id_result(raw_plan_ids: list[str], invalid_plan_ids: list[str]) -> dict:
    invalid = set(invalid_plan_ids)
    valid_rejected_count = len(raw_plan_ids) - len(invalid_plan_ids)
    skipped_reason_codes = {"invalid_notification_plan_id": len(invalid_plan_ids)}
    if valid_rejected_count:
        skipped_reason_codes["batch_recovery_input_rejected"] = valid_rejected_count
    return {
        "status": "rejected",
        "reason_code": "invalid_notification_plan_id",
        "requested_count": len(raw_plan_ids),
        "created_count": 0,
        "emitted_count": 0,
        "skipped_count": len(raw_plan_ids),
        "skipped_reason_codes": skipped_reason_codes,
        "results": [
            {
                "notification_plan_id": str(raw_plan_id),
                "action": "skipped",
                "reason_code": (
                    "invalid_notification_plan_id"
                    if str(raw_plan_id) in invalid
                    else "batch_recovery_input_rejected"
                ),
                "manual_retry_intent_emitted": False,
            }
            for raw_plan_id in raw_plan_ids
        ],
    }


def _to_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, indent=2, sort_keys=True)


async def _run_worker(config: MaintenanceConfig) -> int:
    logger = _build_logger(config.log_level)

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)

    maintenance_consumer = RedisStreamConsumer(
        redis_client,
        queue_name=config.maintenance_queue_name,
        consumer_group=config.maintenance_consumer_group,
        consumer_name=config.maintenance_consumer_name,
        block_ms=config.block_ms,
        batch_size=config.batch_size,
    )
    replay_consumer = RedisStreamConsumer(
        redis_client,
        queue_name=config.replay_queue_name,
        consumer_group=config.replay_consumer_group,
        consumer_name=config.replay_consumer_name,
        block_ms=config.block_ms,
        batch_size=config.batch_size,
    )

    class SessionBackedService:
        async def handle_maintenance_trigger_event(self, trigger_event_id: str) -> None:
            async with session_factory.begin() as session:
                service = MaintenanceService(config, repository=MaintenanceRepository(session), logger=logger)
                await service.handle_maintenance_trigger_event(trigger_event_id)

        async def handle_replay_trigger_event(self, trigger_event_id: str) -> None:
            async with session_factory.begin() as session:
                service = MaintenanceService(config, repository=MaintenanceRepository(session), logger=logger)
                await service.handle_replay_trigger_event(trigger_event_id)

        async def promote_due_retries_once(self, limit: int | None = None) -> int:
            async with session_factory.begin() as session:
                service = MaintenanceService(config, repository=MaintenanceRepository(session), logger=logger)
                return await service.promote_due_retries_once(limit=limit)

    service = SessionBackedService()
    maintenance_worker = MaintenanceQueueWorker(config, consumer=maintenance_consumer, service=service, logger=logger)
    replay_worker = ReplayQueueWorker(config, consumer=replay_consumer, service=service, logger=logger)
    due_retry_worker = DueRetryPromotionWorker(config, service=service, logger=logger)
    try:
        await asyncio.gather(
            maintenance_worker.run_forever(),
            replay_worker.run_forever(),
            due_retry_worker.run_forever(),
        )
    except asyncio.CancelledError:
        logger.info("maintenance_cancelled", extra={"service": "maintenance", "event": "cancelled"})
        return 0
    finally:
        close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close is not None:
            maybe_awaitable = close()
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable
        await engine.dispose()
    return 0


async def _run_delivery_gate(config: MaintenanceConfig, args: argparse.Namespace) -> int:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    operator_review_passed = None
    if args.operator_review_passed == "true":
        operator_review_passed = True
    elif args.operator_review_passed == "false":
        operator_review_passed = False

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with session_factory() as session:
            runner = DeliveryGateRunner(config, repository=MaintenanceRepository(session))
            report = await runner.run(mode=args.mode, operator_review_passed=operator_review_passed)
            print(json.dumps(asdict(report), ensure_ascii=False, default=str, indent=2, sort_keys=False))
            if report.gate_status == "pass":
                return 0
            if report.gate_status == "warn":
                return 3
            return 2
    finally:
        await engine.dispose()


def _load_delivery_gate_preflight_config(args: argparse.Namespace) -> MaintenanceConfig:
    overlay: dict[str, str] = {}
    if getattr(args, "env_file", None):
        overlay.update(_resolve_one_shot_runtime_env_file_overlay(args.env_file))
    if "REDIS_URL" not in os.environ and "REDIS_URL" not in overlay:
        overlay["REDIS_URL"] = "redis://127.0.0.1:6379/0"

    if getattr(args, "env_file", None):
        return _load_maintenance_one_shot_runtime_config(args, env_file_overlay=overlay)
    with _temporary_environment_defaults(overlay):
        return MaintenanceConfig.from_env()


async def _run_delivery_gate_preflight(args: argparse.Namespace) -> int:
    operator_review_passed = True if args.operator_review_passed else None

    async def load_report(config: MaintenanceConfig, mode, review_passed):
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

        engine = create_async_engine(config.database_url, future=True)
        session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with session_factory() as session:
                repository = MaintenanceRepository(session)
                return await load_delivery_gate_preflight_report(
                    config,
                    repository,
                    mode=mode,
                    operator_review_passed=review_passed,
                )
        finally:
            await engine.dispose()

    return await run_delivery_gate_preflight(
        mode=args.mode,
        output=args.output,
        operator_review_passed=operator_review_passed,
        load_config=lambda: _load_delivery_gate_preflight_config(args),
        load_report=load_report,
    )


async def _run_mvp_readiness(config: MaintenanceConfig, args: argparse.Namespace) -> int:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with session_factory() as session:
            runner = DeliveryGateRunner(config, repository=MaintenanceRepository(session))
            return await run_mvp_readiness(
                config,
                args,
                runner,
                recovery_cli_surface=_detect_recovery_cli_surface(build_parser()),
            )
    finally:
        await engine.dispose()


async def _run_db_shape_preflight(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    emit_json=print,
    session_factory_builder=None,
    report_runner=run_db_shape_preflight,
) -> int:
    try:
        database_url, database_url_source = _resolve_database_url_source(args, env=env)
    except ValueError as exc:
        reason_code = _db_shape_preflight_source_reason_code(exc)
        emit_json(_to_json(_db_shape_preflight_error_payload(reason_code)))
        return 1

    if session_factory_builder is None:
        session_factory_builder = _build_db_shape_preflight_session_factory

    try:
        session_factory, dispose = session_factory_builder(database_url)
        try:
            async with session_factory() as session:
                repository = SqlAlchemyDbShapeIntrospectionRepository(session)
                return await run_db_shape_preflight_command(
                    repository,
                    database_url_source=database_url_source,
                    emit_json=emit_json,
                    report_runner=report_runner,
                )
        finally:
            await dispose()
    except Exception:
        emit_json(_to_json(_db_shape_preflight_error_payload("db_shape_preflight_runtime_error")))
        return 1


def _read_database_url_from_env(env: Mapping[str, str] | None = None) -> str:
    source = os.environ if env is None else env
    database_url = source.get("DATABASE_URL", "").strip()
    if not database_url:
        raise ValueError("database_url_required")
    return database_url


def _db_shape_preflight_source_reason_code(exc: ValueError) -> str:
    reason_code = str(exc)
    if reason_code in DB_SHAPE_PREFLIGHT_SOURCE_REASON_CODES:
        return reason_code
    return "database_url_required"


def _resolve_database_url_source(args: argparse.Namespace, *, env: Mapping[str, str] | None = None) -> tuple[str, str]:
    database_url_file = getattr(args, "database_url_file", None)
    env_file = getattr(args, "env_file", None)
    if database_url_file and env_file:
        raise ValueError("ambiguous_database_url_source")

    source = os.environ if env is None else env
    database_url = source.get("DATABASE_URL", "").strip()
    if database_url:
        return database_url, "env"

    if database_url_file:
        return _read_database_url_from_file(
            database_url_file,
            missing_reason_code="database_url_file_missing",
            empty_reason_code="database_url_file_empty",
        ), "database_url_file"

    if env_file:
        return _resolve_database_url_from_env_file(env_file)

    raise ValueError("database_url_required")


def _resolve_database_url_from_env_file(env_file: str) -> tuple[str, str]:
    values = _read_minimal_env_file(env_file)
    database_url = values.get("DATABASE_URL", "").strip()
    if database_url:
        return database_url, "env_file_database_url"

    database_url_file = values.get("DATABASE_URL_FILE", "").strip()
    if not database_url_file:
        raise ValueError("env_file_no_database_url")

    return _read_database_url_from_file(
        database_url_file,
        missing_reason_code="env_file_database_url_file_missing",
        empty_reason_code="env_file_database_url_file_empty",
    ), "env_file_database_url_file"


def _read_database_url_from_file(
    path: str,
    *,
    missing_reason_code: str,
    empty_reason_code: str,
) -> str:
    database_url_path = Path(path)
    if not database_url_path.is_file():
        raise ValueError(missing_reason_code)
    try:
        database_url = database_url_path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        raise ValueError(missing_reason_code) from None
    if not database_url:
        raise ValueError(empty_reason_code)
    return database_url


def _read_minimal_env_file(env_file: str, *, allowed_keys: set[str] | None = None) -> dict[str, str]:
    env_path = Path(env_file)
    if not env_path.is_file():
        raise ValueError("env_file_missing")
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        raise ValueError("env_file_missing") from None

    values: dict[str, str] = {}
    if allowed_keys is None:
        allowed_keys = {"DATABASE_URL", "DATABASE_URL_FILE"}
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


def _load_maintenance_one_shot_runtime_config(
    args: argparse.Namespace,
    *,
    env_file_overlay: dict[str, str] | None = None,
) -> MaintenanceConfig:
    env_file = getattr(args, "env_file", None)
    if not env_file:
        return MaintenanceConfig.from_env()

    overlay = env_file_overlay if env_file_overlay is not None else _resolve_one_shot_runtime_env_file_overlay(env_file)
    try:
        with _temporary_environment_defaults(overlay):
            return MaintenanceConfig.from_env()
    except (MaintenanceConfigurationError, ValueError, TypeError) as exc:
        raise _MaintenanceOneShotRuntimeConfigError("maintenance_runtime_config_error") from exc


def _resolve_one_shot_runtime_env_file_overlay(
    env_file: str,
    *,
    process_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    try:
        values = _read_minimal_env_file(env_file, allowed_keys=ONE_SHOT_RUNTIME_ENV_KEYS)
    except ValueError as exc:
        raise _MaintenanceOneShotRuntimeConfigError(_one_shot_runtime_config_reason_code(exc)) from None

    if not values:
        raise _MaintenanceOneShotRuntimeConfigError("env_file_no_runtime_config")

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
        overlay[target_key] = _read_database_url_from_file(
            values.get(file_key, "").strip(),
            missing_reason_code=missing_reason_code,
            empty_reason_code=empty_reason_code,
        )
    except ValueError as exc:
        raise _MaintenanceOneShotRuntimeConfigError(_one_shot_runtime_config_reason_code(exc)) from None


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
    return "maintenance_runtime_config_error"


def _maintenance_one_shot_runtime_config_error_payload(reason_code: str) -> dict:
    return {
        "schema_version": ONE_SHOT_RUNTIME_CONFIG_SCHEMA_VERSION,
        "status": "fail",
        "reason_code": reason_code,
        "warnings": [reason_code],
    }


def _one_shot_command_uses_explicit_env_file(command: str, args: argparse.Namespace) -> bool:
    return command in {
        "delivery-gate",
        "mvp-readiness",
        "batch-recovery",
        "delivery-result",
        "due-retry",
        "delivery-replay",
        "queue-activate",
        "queue-group-bootstrap",
        "worker-once",
        "worker-startup-probe",
        "foreground-smoke",
        "controlled-worker",
    } and bool(getattr(args, "env_file", None))


def _build_db_shape_preflight_session_factory(database_url: str):
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def dispose() -> None:
        await engine.dispose()

    return session_factory, dispose


def _db_shape_preflight_error_payload(reason_code: str) -> dict:
    return {
        "schema_version": DB_SHAPE_PREFLIGHT_SCHEMA_VERSION,
        "status": "fail",
        "missing_tables": [],
        "missing_columns": [],
        "missing_enum_labels": [],
        "warnings": [reason_code],
        "checks": [
            {
                "check_name": "runtime_configuration",
                "status": "fail",
                "check_type": "runtime_configuration",
                "expected": "valid_db_shape_preflight_runtime",
                "observed": reason_code,
            }
        ],
    }


async def _run_batch_recovery(config: MaintenanceConfig, args: argparse.Namespace) -> int:
    if args.recovery_mode == "replay-selected" and (
        not args.operator_confirmed or _has_invalid_notification_plan_id(args.plan_id)
    ):
        return await run_replay_selected_batch_recovery(args, _NoWriteSelectedReplayRepository())
    if args.recovery_mode == "retry-selected-due" and _has_invalid_notification_plan_id(args.plan_id):
        _, invalid_plan_ids = _normalize_notification_plan_ids(args.plan_id)
        print(_to_json(_invalid_retry_plan_id_result(args.plan_id, invalid_plan_ids)))
        return 2

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with session_factory.begin() as session:
            repository = MaintenanceRepository(session)
            if args.recovery_mode == "replay-selected":
                return await run_replay_selected_batch_recovery(args, repository)
            else:
                return await run_retry_selected_due_batch_recovery(config, args, repository)
    finally:
        await engine.dispose()


async def _run_delivery_result_operation(config: MaintenanceConfig, args: argparse.Namespace) -> int:
    if args.mode == "execute" and args.confirm != "write":
        print(_to_json(_operator_report("delivery-result", args.mode, "blocked", "db_write_approval_missing")))
        return 2

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        if args.mode == "execute":
            event_id: UUID | None = None
            service_result = None
            async with session_factory() as session:
                async with session.begin():
                    repository = MaintenanceRepository(session)
                    events = await repository.load_delivery_result_event_by_suffix(args.event_id_suffix, for_update=True)
                    target_error = _exact_target_error(events, "target_event_suffix")
                    if target_error is not None:
                        print(_to_json(_operator_report("delivery-result", args.mode, "blocked", target_error)))
                        return 2
                    event_id = events[0].event_id
                    service_result = await MaintenanceService(config, repository=repository).handle_maintenance_trigger_event(
                        event_id
                    )
            async with session_factory() as read_session:
                read_repository = MaintenanceRepository(read_session)
                receipt_exists = await read_repository.has_delivery_result_receipt(event_id)
                relay_eligible = await read_repository.is_canonically_relay_eligible(event_id)
            status = "pass" if service_result and service_result.processed and receipt_exists and not relay_eligible else "blocked"
            reason_code = None if status == "pass" else "canonical_relay_exclusion_missing"
            print(
                _to_json(
                    _operator_report(
                        "delivery-result",
                        args.mode,
                        status,
                        reason_code,
                        event_id=event_id,
                        service_result=service_result,
                        receipt_exists=receipt_exists,
                        relay_eligible=relay_eligible,
                    )
                )
            )
            return 0 if status == "pass" else 2

        async with session_factory() as session:
            repository = MaintenanceRepository(session)
            events = await repository.load_delivery_result_event_by_suffix(args.event_id_suffix, for_update=False)
            target_error = _exact_target_error(events, "target_event_suffix")
            if target_error is not None:
                print(_to_json(_operator_report("delivery-result", args.mode, "blocked", target_error)))
                return 2
            event_id = events[0].event_id
            receipt_exists = await repository.has_delivery_result_receipt(event_id)
            relay_eligible = await repository.is_canonically_relay_eligible(event_id)
            if args.mode == "proof":
                status = "pass" if receipt_exists and not relay_eligible else "blocked"
                reason_code = None if status == "pass" else "canonical_relay_exclusion_missing"
            else:
                status = "pass"
                reason_code = None
            print(
                _to_json(
                    _operator_report(
                        "delivery-result",
                        args.mode,
                        status,
                        reason_code,
                        event_id=event_id,
                        receipt_exists=receipt_exists,
                        relay_eligible=relay_eligible,
                    )
                )
            )
            return 0 if status == "pass" else 2
    finally:
        await engine.dispose()


async def _run_due_retry_operation(config: MaintenanceConfig, args: argparse.Namespace) -> int:
    if args.limit < 1 or args.limit > 500:
        print(_to_json(_operator_report("due-retry", args.mode, "blocked", "limit_not_allowed")))
        return 2
    if args.mode == "execute" and args.confirm != "write":
        print(_to_json(_operator_report("due-retry", args.mode, "blocked", "db_write_approval_missing")))
        return 2
    now = _parse_now_utc(args.now_utc)
    if now is None and args.now_utc:
        print(_to_json(_operator_report("due-retry", args.mode, "blocked", "now_utc_invalid")))
        return 2
    now = now or datetime.now(timezone.utc)

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        if args.mode == "execute":
            async with session_factory() as session:
                async with session.begin():
                    repository = MaintenanceRepository(session)
                    service = MaintenanceService(config, repository=repository, now_fn=lambda: now)
                    action_count = await service.promote_due_retries_once(limit=args.limit)
            print(
                _to_json(
                    _operator_report(
                        "due-retry",
                        args.mode,
                        "pass",
                        None,
                        action_count=action_count,
                    )
                )
            )
            return 0

        async with session_factory() as session:
            repository = MaintenanceRepository(session)
            candidates = await repository.load_due_retry_candidates(limit=args.limit, now=now)
            print(
                _to_json(
                    _operator_report(
                        "due-retry",
                        args.mode,
                        "pass",
                        None,
                        candidate_count=len(candidates),
                        plan_suffixes=[_safe_uuid_suffix(candidate.plan.notification_plan_id) for candidate in candidates],
                        latest_delivery_suffixes=[
                            _safe_uuid_suffix(candidate.latest_delivery.notification_delivery_record_id)
                            for candidate in candidates
                            if candidate.latest_delivery is not None
                        ],
                    )
                )
            )
            return 0
    finally:
        await engine.dispose()


async def _run_delivery_replay_operation(config: MaintenanceConfig, args: argparse.Namespace) -> int:
    replay_request_id = _parse_uuid(args.replay_request_id)
    if replay_request_id is None:
        print(_to_json(_operator_report("delivery-replay", args.mode, "blocked", "replay_request_id_invalid")))
        return 2
    if args.mode == "execute" and args.confirm != "write":
        print(_to_json(_operator_report("delivery-replay", args.mode, "blocked", "db_write_approval_missing")))
        return 2

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        if args.mode == "execute":
            decision = None
            async with session_factory() as session:
                async with session.begin():
                    repository = MaintenanceRepository(session)
                    decision = await MaintenanceService(config, repository=repository).dispatch_delivery_replay_request(
                        replay_request_id
                    )
            async with session_factory() as read_session:
                read_repository = MaintenanceRepository(read_session)
                replay_request = await read_repository.load_replay_request(replay_request_id)
            status = "pass" if decision is not None and replay_request is not None else "blocked"
            reason_code = None if status == "pass" else "post_commit_readback_failed"
            print(
                _to_json(
                    _operator_report(
                        "delivery-replay",
                        args.mode,
                        status,
                        reason_code,
                        replay_request_id=replay_request_id,
                        replay_request_status=replay_request.status if replay_request else None,
                        replay_action=decision.action if decision else None,
                        service_reason_code=decision.reason_code if decision else None,
                    )
                )
            )
            return 0 if status == "pass" else 2

        async with session_factory() as session:
            repository = MaintenanceRepository(session)
            replay_request = await repository.load_replay_request(replay_request_id)
            status = "pass" if replay_request is not None else "blocked"
            reason_code = None if replay_request is not None else "replay_request_missing"
            print(
                _to_json(
                    _operator_report(
                        "delivery-replay",
                        args.mode,
                        status,
                        reason_code,
                        replay_request_id=replay_request_id,
                        replay_request_status=replay_request.status if replay_request else None,
                    )
                )
            )
            return 0 if status == "pass" else 2
    finally:
        await engine.dispose()


async def _run_queue_activate_operation(config: MaintenanceConfig, args: argparse.Namespace) -> int:
    request_error = _queue_activate_request_error(args)
    request = _queue_activate_request(config, args)
    if request_error is not None:
        print(_to_json(asdict(_queue_activate_blocked_report(request, args.mode, request_error))))
        return 2

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)
    consumer = RedisStreamConsumer(
        redis_client,
        queue_name=request.queue_name,
        consumer_group=request.consumer_group,
        consumer_name=request.consumer_name,
        block_ms=config.block_ms,
        batch_size=request.max_messages,
    )

    class SessionBackedRestrictedService:
        async def load_outbox_event(self, trigger_event_id: UUID):
            async with session_factory() as session:
                return await MaintenanceRepository(session).load_outbox_event(trigger_event_id)

        async def handle_maintenance_trigger_event(self, trigger_event_id: UUID):
            async with session_factory.begin() as session:
                return await MaintenanceService(config, repository=MaintenanceRepository(session)).handle_maintenance_trigger_event(
                    trigger_event_id
                )

        async def handle_replay_trigger_event(self, trigger_event_id: UUID):
            async with session_factory.begin() as session:
                return await MaintenanceService(config, repository=MaintenanceRepository(session)).handle_replay_trigger_event(
                    trigger_event_id
                )

    try:
        report = await run_restricted_queue_activation(
            request,
            consumer=consumer,
            service=SessionBackedRestrictedService(),
            mode=args.mode,
        )
        print(_to_json(asdict(report)))
        return 0 if report.status == "pass" else 2
    finally:
        close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close is not None:
            maybe_awaitable = close()
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable
        await engine.dispose()


async def _run_queue_group_bootstrap_operation(config: MaintenanceConfig, args: argparse.Namespace) -> int:
    request_error = _queue_group_bootstrap_request_error(args)
    request = _queue_group_bootstrap_request(config, args)
    if request_error is not None:
        print(_to_json(asdict(_queue_group_bootstrap_blocked_report(request, request_error))))
        return 2

    from redis.asyncio import Redis  # type: ignore[import-not-found]

    redis_client = Redis.from_url(config.redis_url, decode_responses=True)
    consumer = RedisStreamConsumer(
        redis_client,
        queue_name=request.queue_name,
        consumer_group=request.consumer_group,
        consumer_name=request.consumer_name,
        block_ms=config.block_ms,
        batch_size=1,
    )
    try:
        report = await run_restricted_queue_group_bootstrap(request, consumer=consumer)
        print(_to_json(asdict(report)))
        return 0 if report.status == "pass" else 2
    finally:
        close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close is not None:
            maybe_awaitable = close()
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable


async def _run_worker_once_operation(config: MaintenanceConfig, args: argparse.Namespace) -> int:
    request = _worker_once_request(config, args)

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)
    consumer = RedisStreamConsumer(
        redis_client,
        queue_name=request.queue_name,
        consumer_group=request.consumer_group,
        consumer_name=_worker_once_consumer_name(config, args),
        block_ms=config.block_ms,
        batch_size=request.max_messages,
    )

    class SessionBackedWorkerOnceService:
        async def handle_maintenance_trigger_event(self, trigger_event_id: UUID):
            async with session_factory.begin() as session:
                return await MaintenanceService(config, repository=MaintenanceRepository(session)).handle_maintenance_trigger_event(
                    trigger_event_id
                )

        async def handle_replay_trigger_event(self, trigger_event_id: UUID):
            async with session_factory.begin() as session:
                return await MaintenanceService(config, repository=MaintenanceRepository(session)).handle_replay_trigger_event(
                    trigger_event_id
                )

        async def promote_due_retries_once(self, limit: int | None = None) -> int:
            async with session_factory.begin() as session:
                return await MaintenanceService(config, repository=MaintenanceRepository(session)).promote_due_retries_once(
                    limit=limit
                )

    try:
        report = await run_worker_once(
            request,
            config=config,
            consumer=consumer,
            service=SessionBackedWorkerOnceService(),
        )
        print(_to_json(asdict(report)))
        if report.status == "pass":
            return 0
        if report.status == "failed":
            return 1
        return 2
    finally:
        close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close is not None:
            maybe_awaitable = close()
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable
        await engine.dispose()


async def _run_worker_startup_probe_operation(config: MaintenanceConfig, args: argparse.Namespace) -> int:
    request = _worker_startup_probe_request(args)
    report = await run_worker_startup_probe(
        request,
        config=config,
        logger=_build_logger(config.log_level),
    )
    print(_to_json(asdict(report)))
    if report.status == "pass":
        return 0
    if report.status == "failed":
        return 1
    return 2


async def _run_foreground_smoke_operation(config: MaintenanceConfig, args: argparse.Namespace) -> int:
    request = _foreground_smoke_request(config, args)

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    logger = _build_logger(config.log_level)
    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)
    maintenance_consumer = RedisStreamConsumer(
        redis_client,
        queue_name=config.maintenance_queue_name,
        consumer_group=config.maintenance_consumer_group,
        consumer_name=config.maintenance_consumer_name,
        block_ms=config.block_ms,
        batch_size=request.max_messages,
    )
    replay_consumer = RedisStreamConsumer(
        redis_client,
        queue_name=config.replay_queue_name,
        consumer_group=config.replay_consumer_group,
        consumer_name=config.replay_consumer_name,
        block_ms=config.block_ms,
        batch_size=request.max_messages,
    )

    class SessionBackedForegroundSmokeService:
        async def handle_maintenance_trigger_event(self, trigger_event_id: UUID):
            async with session_factory.begin() as session:
                service = MaintenanceService(config, repository=MaintenanceRepository(session), logger=logger)
                return await service.handle_maintenance_trigger_event(trigger_event_id)

        async def handle_replay_trigger_event(self, trigger_event_id: UUID):
            async with session_factory.begin() as session:
                service = MaintenanceService(config, repository=MaintenanceRepository(session), logger=logger)
                return await service.handle_replay_trigger_event(trigger_event_id)

        async def promote_due_retries_once(self, limit: int | None = None) -> int:
            async with session_factory.begin() as session:
                service = MaintenanceService(config, repository=MaintenanceRepository(session), logger=logger)
                return await service.promote_due_retries_once(limit=limit)

    try:
        report = await run_foreground_smoke(
            request,
            config=config,
            maintenance_consumer=maintenance_consumer,
            replay_consumer=replay_consumer,
            service=SessionBackedForegroundSmokeService(),
            logger=logger,
        )
        print(_to_json(asdict(report)))
        if report.status == "pass":
            return 0
        if report.status == "failed":
            return 1
        return 2
    finally:
        close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close is not None:
            maybe_awaitable = close()
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable
        await engine.dispose()


async def _run_controlled_worker_operation(config: MaintenanceConfig, args: argparse.Namespace) -> int:
    request = _controlled_worker_request(config, args)

    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    logger = _build_logger(config.log_level)
    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(config.redis_url, decode_responses=True)
    maintenance_consumer = RedisStreamConsumer(
        redis_client,
        queue_name=config.maintenance_queue_name,
        consumer_group=config.maintenance_consumer_group,
        consumer_name=config.maintenance_consumer_name,
        block_ms=config.block_ms,
        batch_size=request.max_messages,
    )
    replay_consumer = RedisStreamConsumer(
        redis_client,
        queue_name=config.replay_queue_name,
        consumer_group=config.replay_consumer_group,
        consumer_name=config.replay_consumer_name,
        block_ms=config.block_ms,
        batch_size=request.max_messages,
    )

    class SessionBackedControlledWorkerService:
        async def handle_maintenance_trigger_event(self, trigger_event_id: UUID):
            async with session_factory.begin() as session:
                service = MaintenanceService(config, repository=MaintenanceRepository(session), logger=logger)
                return await service.handle_maintenance_trigger_event(trigger_event_id)

        async def handle_replay_trigger_event(self, trigger_event_id: UUID):
            async with session_factory.begin() as session:
                service = MaintenanceService(config, repository=MaintenanceRepository(session), logger=logger)
                return await service.handle_replay_trigger_event(trigger_event_id)

        async def promote_due_retries_once(self, limit: int | None = None) -> int:
            async with session_factory.begin() as session:
                service = MaintenanceService(config, repository=MaintenanceRepository(session), logger=logger)
                return await service.promote_due_retries_once(limit=limit)

    try:
        report = await run_controlled_worker_activation(
            request,
            config=config,
            maintenance_consumer=maintenance_consumer,
            replay_consumer=replay_consumer,
            service=SessionBackedControlledWorkerService(),
            logger=logger,
        )
        print(_to_json(asdict(report)))
        if report.status == "pass":
            return 0
        if report.status == "failed":
            return 1
        return 2
    finally:
        close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close is not None:
            maybe_awaitable = close()
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable
        await engine.dispose()


async def _run_systemd_rollout_operation(args: argparse.Namespace) -> int:
    if args.mode == "diagnose":
        request = _systemd_diagnostic_request(args)
        report = run_systemd_diagnostic(request)
        print(_to_json(asdict(report)))
        if report.status == "pass":
            return 0
        if report.status == "failed":
            return 1
        return 2

    request = _systemd_rollout_request(args)
    report = run_systemd_rollout(request)
    print(_to_json(asdict(report)))
    if report.status == "pass":
        return 0
    if report.status == "failed":
        return 1
    return 2


def _systemd_diagnostic_request(args: argparse.Namespace) -> SystemdDiagnosticRequest:
    systemd_user_dir = _path_arg_or_default(args.systemd_user_dir, Path.home() / ".config/systemd/user")
    runtime_env_file = Path(args.env_file).expanduser().resolve()
    return SystemdDiagnosticRequest(
        target=args.target,
        runtime_env_file=runtime_env_file,
        systemd_user_dir=systemd_user_dir,
        service_name=SYSTEMD_ROLLOUT_SERVICE_NAME,
    )


def _systemd_rollout_request(args: argparse.Namespace) -> SystemdRolloutRequest:
    repo_root = _path_arg_or_default(args.repo_root, _default_repo_root())
    python_executable = _path_arg_or_default(args.python_executable, Path(sys.executable))
    systemd_user_dir = _path_arg_or_default(args.systemd_user_dir, Path.home() / ".config/systemd/user")
    runtime_env_file = Path(args.env_file).expanduser().resolve()
    return SystemdRolloutRequest(
        mode=args.mode,
        target=args.target,
        confirm_install=args.confirm == "install",
        confirm_start=args.confirm == "start",
        confirm_rollback=args.confirm == "rollback",
        repo_root=repo_root,
        python_executable=python_executable,
        runtime_env_file=runtime_env_file,
        systemd_user_dir=systemd_user_dir,
        service_name=SYSTEMD_ROLLOUT_SERVICE_NAME,
        timer_name=None,
        dry_run=args.mode in {"plan", "proof"},
    )


def _path_arg_or_default(value: str | None, default: Path) -> Path:
    if value:
        return Path(value).expanduser().resolve()
    return default.expanduser().resolve()


def _default_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _queue_activate_request(config: MaintenanceConfig, args: argparse.Namespace) -> RestrictedQueueActivationRequest:
    if args.activation_queue == "maintenance":
        queue_name = config.maintenance_queue_name
        consumer_group = config.maintenance_consumer_group
        consumer_name = config.maintenance_consumer_name
        expected_event_type = DELIVERY_RESULT_EVENT_TYPE
    else:
        queue_name = config.replay_queue_name
        consumer_group = config.replay_consumer_group
        consumer_name = config.replay_consumer_name
        expected_event_type = REPLAY_REQUESTED_EVENT_TYPE
    return RestrictedQueueActivationRequest(
        queue_name=queue_name,
        consumer_group=consumer_group,
        consumer_name=consumer_name,
        max_messages=int(args.max_messages),
        ack=args.mode == "execute" and args.confirm == "ack",
        dry_run=args.mode != "execute",
        allow_create_group=bool(args.allow_create_group),
        expected_event_type=expected_event_type,
        exact_trigger_event_id=_parse_uuid(args.exact_trigger_event_id),
    )


def _queue_activate_request_error(args: argparse.Namespace) -> str | None:
    if args.max_messages < 1 or args.max_messages > 10:
        return "max_messages_not_allowed"
    if args.mode == "execute" and args.confirm != "ack":
        return "ack_confirm_missing"
    if args.mode != "execute" and args.confirm == "ack":
        return "ack_confirm_not_allowed_for_dry_run"
    if args.mode != "execute" and args.allow_create_group:
        return "allow_create_group_not_allowed_for_dry_run"
    if args.exact_trigger_event_id and _parse_uuid(args.exact_trigger_event_id) is None:
        return "exact_trigger_event_id_invalid"
    return None


def _worker_once_request(config: MaintenanceConfig, args: argparse.Namespace) -> WorkerOnceRequest:
    if args.worker_type == "maintenance":
        queue_name = config.maintenance_queue_name
        consumer_group = config.maintenance_consumer_group
    else:
        queue_name = config.replay_queue_name
        consumer_group = config.replay_consumer_group
    return WorkerOnceRequest(
        worker_type=args.worker_type,
        queue_name=queue_name,
        consumer_group=consumer_group,
        mode=args.mode,
        max_messages=int(args.max_messages),
        confirm_ack=args.mode == "execute" and args.confirm == "ack",
    )


def _worker_once_consumer_name(config: MaintenanceConfig, args: argparse.Namespace) -> str:
    return config.maintenance_consumer_name if args.worker_type == "maintenance" else config.replay_consumer_name


def _worker_startup_probe_request(args: argparse.Namespace) -> WorkerStartupProbeRequest:
    return WorkerStartupProbeRequest(
        mode=args.mode,
        confirm_run=args.mode == "execute" and args.confirm == "run",
    )


def _worker_startup_probe_request_error(args: argparse.Namespace) -> str | None:
    return worker_startup_probe_request_error(_worker_startup_probe_request(args))


def _worker_startup_probe_config_error_report(args: argparse.Namespace, reason_code: str) -> WorkerStartupProbeReport:
    probe_reason_code = _worker_startup_probe_config_reason_code(reason_code)
    status = "blocked" if probe_reason_code in {"env_file_missing", "env_file_no_runtime_config"} else "failed"
    return build_worker_startup_probe_blocked_report(
        mode=args.mode,
        status=status,
        reason_code=probe_reason_code,
    )


def _worker_startup_probe_config_reason_code(reason_code: str) -> str:
    if reason_code in {"env_file_missing", "env_file_no_runtime_config"}:
        return reason_code
    return "runtime_config_error"


def _worker_once_request_error(args: argparse.Namespace) -> str | None:
    if args.max_messages < 1 or args.max_messages > 10:
        return "max_messages_not_allowed"
    if args.mode == "execute" and args.confirm != "ack":
        return "ack_confirm_missing"
    return None


def _foreground_smoke_request(config: MaintenanceConfig, args: argparse.Namespace) -> ForegroundSmokeRequest:
    return ForegroundSmokeRequest(
        mode=args.mode,
        ticks=int(args.ticks),
        max_messages=int(args.max_messages),
        confirm_run=args.mode == "execute" and args.confirm == "run",
        maintenance_queue_name=config.maintenance_queue_name,
        maintenance_consumer_group=config.maintenance_consumer_group,
        replay_queue_name=config.replay_queue_name,
        replay_consumer_group=config.replay_consumer_group,
    )


def _foreground_smoke_request_error(args: argparse.Namespace) -> str | None:
    request = ForegroundSmokeRequest(
        mode=args.mode,
        ticks=int(args.ticks),
        max_messages=int(args.max_messages),
        confirm_run=args.mode == "execute" and args.confirm == "run",
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="maintenance",
        replay_queue_name="q.replay",
        replay_consumer_group="maintenance-replay",
    )
    return foreground_smoke_request_error(request)


def _foreground_smoke_blocked_report(args: argparse.Namespace, reason_code: str) -> ForegroundSmokeReport:
    request = ForegroundSmokeRequest(
        mode=args.mode,
        ticks=int(args.ticks),
        max_messages=int(args.max_messages),
        confirm_run=args.mode == "execute" and args.confirm == "run",
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="maintenance",
        replay_queue_name="q.replay",
        replay_consumer_group="maintenance-replay",
    )
    return ForegroundSmokeReport(
        schema_version="maintenance_foreground_smoke_report_v1",
        mode=request.mode,
        status="blocked",
        ticks_requested=request.ticks,
        ticks_completed=0,
        maintenance_processed_count=0,
        maintenance_acked_count=0,
        replay_processed_count=0,
        replay_acked_count=0,
        due_retry_action_count=0,
        reason_code=reason_code,
        redactions_applied={
            "full_uuid_omitted": True,
            "full_redis_message_id_omitted": True,
            "payload_json_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "runtime_env_values_omitted": True,
            "exception_body_omitted": True,
        },
    )


def _controlled_worker_request(config: MaintenanceConfig, args: argparse.Namespace) -> ControlledWorkerActivationRequest:
    return ControlledWorkerActivationRequest(
        mode=args.mode,
        max_ticks=int(args.max_ticks),
        max_runtime_sec=int(args.max_runtime_sec),
        max_messages=int(args.max_messages),
        idle_sleep_ms=int(args.idle_sleep_ms),
        confirm_run=args.mode == "execute" and args.confirm == "run",
        maintenance_queue_name=config.maintenance_queue_name,
        maintenance_consumer_group=config.maintenance_consumer_group,
        replay_queue_name=config.replay_queue_name,
        replay_consumer_group=config.replay_consumer_group,
    )


def _controlled_worker_request_error(args: argparse.Namespace) -> str | None:
    request = ControlledWorkerActivationRequest(
        mode=args.mode,
        max_ticks=int(args.max_ticks),
        max_runtime_sec=int(args.max_runtime_sec),
        max_messages=int(args.max_messages),
        idle_sleep_ms=int(args.idle_sleep_ms),
        confirm_run=args.mode == "execute" and args.confirm == "run",
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="maintenance",
        replay_queue_name="q.replay",
        replay_consumer_group="maintenance-replay",
    )
    return controlled_worker_activation_request_error(request)


def _controlled_worker_blocked_report(
    args: argparse.Namespace,
    reason_code: str,
) -> ControlledWorkerActivationReport:
    request = ControlledWorkerActivationRequest(
        mode=args.mode,
        max_ticks=int(args.max_ticks),
        max_runtime_sec=int(args.max_runtime_sec),
        max_messages=int(args.max_messages),
        idle_sleep_ms=int(args.idle_sleep_ms),
        confirm_run=args.mode == "execute" and args.confirm == "run",
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="maintenance",
        replay_queue_name="q.replay",
        replay_consumer_group="maintenance-replay",
    )
    return ControlledWorkerActivationReport(
        schema_version="maintenance_controlled_worker_activation_report_v1",
        mode=request.mode,
        status="blocked",
        reason_code=reason_code,
        ticks_requested=request.max_ticks,
        ticks_completed=0,
        runtime_limit_sec=request.max_runtime_sec,
        elapsed_ms=0,
        maintenance_processed_count=0,
        maintenance_acked_count=0,
        replay_processed_count=0,
        replay_acked_count=0,
        due_retry_action_count=0,
        stop_reason="failed",
        redactions_applied={
            "full_uuid_omitted": True,
            "full_redis_message_id_omitted": True,
            "payload_json_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "runtime_env_path_omitted": True,
            "runtime_env_values_omitted": True,
            "secret_values_omitted": True,
            "raw_source_text_omitted": True,
            "exception_body_omitted": True,
        },
    )


def _worker_once_blocked_report(args: argparse.Namespace, reason_code: str) -> WorkerOnceReport:
    if args.worker_type == "maintenance":
        queue_name = "q.maintenance"
        consumer_group = "maintenance"
    else:
        queue_name = "q.replay"
        consumer_group = "maintenance-replay"
    return WorkerOnceReport(
        schema_version="maintenance_worker_once_report_v1",
        worker_type=args.worker_type,
        queue_name=queue_name,
        consumer_group=consumer_group,
        mode=args.mode,
        status="blocked",
        processed_count=0,
        acked_count=0,
        reason_code=reason_code,
        redactions_applied={
            "full_uuid_omitted": True,
            "full_redis_message_id_omitted": True,
            "payload_json_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "exception_body_omitted": True,
        },
    )


def _queue_group_bootstrap_request(
    config: MaintenanceConfig,
    args: argparse.Namespace,
) -> RestrictedQueueGroupBootstrapRequest:
    if args.bootstrap_queue == "maintenance":
        queue_name = config.maintenance_queue_name
        consumer_group = config.maintenance_consumer_group
        consumer_name = config.maintenance_consumer_name
    else:
        queue_name = config.replay_queue_name
        consumer_group = config.replay_consumer_group
        consumer_name = config.replay_consumer_name
    return RestrictedQueueGroupBootstrapRequest(
        queue_selector=args.bootstrap_queue,
        queue_name=queue_name,
        consumer_group=consumer_group,
        consumer_name=consumer_name,
        mode=args.mode,
        confirm_create_group=args.mode == "execute" and args.confirm == "create-group",
    )


def _queue_group_bootstrap_request_error(args: argparse.Namespace) -> str | None:
    if args.mode == "execute" and args.confirm != "create-group":
        return "create_group_confirm_missing"
    if args.mode != "execute" and args.confirm == "create-group":
        return "create_group_confirm_not_allowed_for_read_only"
    return None


def _queue_group_bootstrap_blocked_report(
    request: RestrictedQueueGroupBootstrapRequest,
    reason_code: str,
) -> RestrictedQueueGroupBootstrapReport:
    return RestrictedQueueGroupBootstrapReport(
        schema_version="restricted_queue_group_bootstrap_report_v1",
        queue_selector=request.queue_selector,
        queue_name=request.queue_name,
        consumer_group=request.consumer_group,
        consumer_name=request.consumer_name,
        mode=request.mode,
        status="blocked",
        group_exists=False,
        created=False,
        already_exists=False,
        xgroup_create_attempted=False,
        stream_messages_read=False,
        ack_attempted=False,
        db_writes_attempted=False,
        destructive_redis_commands_attempted=False,
        reason_code=reason_code,
        redactions_applied={
            "full_redis_message_id_omitted": True,
            "payload_json_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "exception_body_omitted": True,
        },
    )


def _queue_activate_blocked_report(
    request: RestrictedQueueActivationRequest,
    mode: str,
    reason_code: str,
) -> RestrictedQueueActivationReport:
    return RestrictedQueueActivationReport(
        schema_version="restricted_queue_activation_report_v1",
        queue_name=request.queue_name,
        consumer_group=request.consumer_group,
        consumer_name=request.consumer_name,
        mode=mode,
        status="blocked",
        processed_count=0,
        acked_count=0,
        skipped_count=0,
        results=[],
        reason_code=reason_code,
        redactions_applied={
            "full_uuid_omitted": True,
            "full_redis_message_id_omitted": True,
            "payload_json_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "exception_body_omitted": True,
        },
    )


def _parse_uuid(value: object) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _exact_target_error(rows: list, label: str) -> str | None:
    if not rows:
        return f"{label}_missing"
    if len(rows) > 1:
        return f"{label}_not_unique"
    return None


def _operator_report(
    operation: str,
    mode: str,
    status: str,
    reason_code: str | None,
    **extra: object,
) -> dict:
    payload = {
        "schema_version": "maintenance_delivery_operator_v1",
        "operation": operation,
        "mode": mode,
        "status": status,
        "ok": status == "pass" and reason_code is None,
        "reason_code": reason_code,
        "redactions_applied": {
            "full_event_id_omitted": True,
            "full_notification_plan_id_omitted": True,
            "full_replay_request_id_omitted": True,
            "payload_json_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "exception_body_omitted": True,
        },
    }
    if "event_id" in extra:
        payload["event_id_suffix"] = _safe_uuid_suffix(extra.pop("event_id"))
    if "replay_request_id" in extra:
        payload["replay_request_id_suffix"] = _safe_uuid_suffix(extra.pop("replay_request_id"))
    service_result = extra.pop("service_result", None)
    if service_result is not None:
        payload.update(
            {
                "service_result_processed": service_result.processed,
                "service_result_classification": service_result.classification,
                "service_result_action": service_result.action,
                "service_result_reason_code": service_result.reason_code,
            }
        )
    payload.update(extra)
    return payload


def _safe_uuid_suffix(value: object) -> str | None:
    if value is None:
        return None
    return str(value)[-8:]


def _parse_now_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _detect_recovery_cli_surface(parser: argparse.ArgumentParser) -> dict[str, bool]:
    return {
        "batch_recovery_replay_selected_operator_confirmed": _parser_accepts(
            parser,
            [
                "batch-recovery",
                "replay-selected",
                "--plan-id",
                "00000000-0000-0000-0000-000000000001",
                "--requested-by",
                "ops",
                "--operator-confirmed",
            ],
        ),
        "batch_recovery_retry_selected_due_confirm_write": _parser_accepts(
            parser,
            [
                "batch-recovery",
                "retry-selected-due",
                "--plan-id",
                "00000000-0000-0000-0000-000000000001",
                "--requested-by",
                "ops",
                "--confirm",
                "write",
            ],
        ),
        "delivery_gate_restricted_mode": _parser_accepts(
            parser,
            [
                "delivery-gate",
                "--mode",
                "restricted",
                "--format",
                "json",
            ],
        ),
    }


def _parser_accepts(parser: argparse.ArgumentParser, argv: list[str]) -> bool:
    try:
        parser.parse_args(argv)
    except SystemExit:
        return False
    return True


async def _run(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command or "worker"
    if command == "worker-once":
        request_error = _worker_once_request_error(args)
        if request_error is not None:
            print(_to_json(asdict(_worker_once_blocked_report(args, request_error))))
            return 2
    if command == "worker-startup-probe":
        request_error = _worker_startup_probe_request_error(args)
        if request_error is not None:
            print(
                _to_json(
                    asdict(
                        build_worker_startup_probe_blocked_report(
                            mode=args.mode,
                            reason_code=request_error,
                        )
                    )
                )
            )
            return 2
    if command == "foreground-smoke":
        request_error = _foreground_smoke_request_error(args)
        if request_error is not None:
            print(_to_json(asdict(_foreground_smoke_blocked_report(args, request_error))))
            return 2
    if command == "controlled-worker":
        request_error = _controlled_worker_request_error(args)
        if request_error is not None:
            print(_to_json(asdict(_controlled_worker_blocked_report(args, request_error))))
            return 2
    if command == "db-shape-preflight":
        return await _run_db_shape_preflight(args)
    if command == "delivery-gate-preflight":
        return await _run_delivery_gate_preflight(args)
    if command == "delivery-gate-preflight-invocation-proof":
        return await run_delivery_gate_preflight_invocation_proof(
            mode=args.mode,
            output=args.output,
            require_gate_status=args.require_gate_status,
            operator_review_passed=args.operator_review_passed,
        )
    if command == "systemd-rollout":
        return await _run_systemd_rollout_operation(args)
    runtime_env_overlay: dict[str, str] | None = None
    if _one_shot_command_uses_explicit_env_file(command, args):
        try:
            runtime_env_overlay = _resolve_one_shot_runtime_env_file_overlay(args.env_file)
        except _MaintenanceOneShotRuntimeConfigError as exc:
            reason_code = _one_shot_runtime_config_reason_code(exc)
            if command == "worker-startup-probe":
                report = _worker_startup_probe_config_error_report(args, reason_code)
                print(_to_json(asdict(report)))
                return 1 if report.status == "failed" else 2
            print(_to_json(_maintenance_one_shot_runtime_config_error_payload(reason_code)))
            return 1
    if command == "batch-recovery" and args.recovery_mode == "replay-selected" and (
        not args.operator_confirmed or _has_invalid_notification_plan_id(args.plan_id)
    ):
        return await run_replay_selected_batch_recovery(args, _NoWriteSelectedReplayRepository())
    if command == "batch-recovery" and args.recovery_mode == "retry-selected-due" and _has_invalid_notification_plan_id(
        args.plan_id
    ):
        _, invalid_plan_ids = _normalize_notification_plan_ids(args.plan_id)
        print(_to_json(_invalid_retry_plan_id_result(args.plan_id, invalid_plan_ids)))
        return 2
    try:
        config = _load_maintenance_one_shot_runtime_config(args, env_file_overlay=runtime_env_overlay)
    except _MaintenanceOneShotRuntimeConfigError as exc:
        reason_code = _one_shot_runtime_config_reason_code(exc)
        if command == "worker-startup-probe":
            report = _worker_startup_probe_config_error_report(args, reason_code)
            print(_to_json(asdict(report)))
            return 1 if report.status == "failed" else 2
        print(_to_json(_maintenance_one_shot_runtime_config_error_payload(reason_code)))
        return 1
    if command == "worker":
        return await _run_worker(config)
    if command == "worker-once":
        return await _run_worker_once_operation(config, args)
    if command == "worker-startup-probe":
        return await _run_worker_startup_probe_operation(config, args)
    if command == "foreground-smoke":
        return await _run_foreground_smoke_operation(config, args)
    if command == "controlled-worker":
        return await _run_controlled_worker_operation(config, args)
    if command == "delivery-gate":
        return await _run_delivery_gate(config, args)
    if command == "mvp-readiness":
        return await _run_mvp_readiness(config, args)
    if command == "delivery-result":
        return await _run_delivery_result_operation(config, args)
    if command == "due-retry":
        return await _run_due_retry_operation(config, args)
    if command == "delivery-replay":
        return await _run_delivery_replay_operation(config, args)
    if command == "queue-activate":
        return await _run_queue_activate_operation(config, args)
    if command == "queue-group-bootstrap":
        return await _run_queue_group_bootstrap_operation(config, args)
    if command == "batch-recovery":
        return await _run_batch_recovery(config, args)
    parser.error(f"unsupported command: {command}")
    return 2


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
