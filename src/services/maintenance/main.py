from __future__ import annotations

import asyncio
import argparse
import json
import logging
import os
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from uuid import UUID

from .batch_recovery import prepare_delivery_replay_requests_for_selected_plans
from .batch_recovery_tool import DeliveryBatchRecoveryTool
from .config import MaintenanceConfig, MaintenanceConfigurationError
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
from .mvp_readiness import run_restricted_live_mvp_readiness
from .redis_streams import RedisStreamConsumer
from .repositories import MaintenanceRepository
from .service import MaintenanceService
from .worker import DueRetryPromotionWorker, MaintenanceQueueWorker, ReplayQueueWorker

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

    mvp = subcommands.add_parser("mvp-readiness")
    mvp.add_argument("--mode", choices=["restricted"], required=True)
    mvp.add_argument("--format", choices=["json"], default="json")
    mvp.add_argument("--operator-review-passed", choices=["true", "false"], default=None)
    mvp.add_argument("--env-file")

    db_shape = subcommands.add_parser("db-shape-preflight")
    db_shape.add_argument("--format", choices=["json"], default="json")
    db_shape.add_argument("--database-url-file")
    db_shape.add_argument("--env-file")

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
    return command in {"delivery-gate", "mvp-readiness", "batch-recovery"} and bool(getattr(args, "env_file", None))


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
    if command == "db-shape-preflight":
        return await _run_db_shape_preflight(args)
    if command == "delivery-gate-preflight":
        return await _run_delivery_gate_preflight(args)
    runtime_env_overlay: dict[str, str] | None = None
    if _one_shot_command_uses_explicit_env_file(command, args):
        try:
            runtime_env_overlay = _resolve_one_shot_runtime_env_file_overlay(args.env_file)
        except _MaintenanceOneShotRuntimeConfigError as exc:
            reason_code = _one_shot_runtime_config_reason_code(exc)
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
        print(_to_json(_maintenance_one_shot_runtime_config_error_payload(reason_code)))
        return 1
    if command == "worker":
        return await _run_worker(config)
    if command == "delivery-gate":
        return await _run_delivery_gate(config, args)
    if command == "mvp-readiness":
        return await _run_mvp_readiness(config, args)
    if command == "batch-recovery":
        return await _run_batch_recovery(config, args)
    parser.error(f"unsupported command: {command}")
    return 2


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
