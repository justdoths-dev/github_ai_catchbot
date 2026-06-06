from __future__ import annotations

import asyncio
import argparse
import json
import logging
from dataclasses import asdict
from uuid import UUID

from .batch_recovery import prepare_delivery_replay_requests_for_selected_plans
from .batch_recovery_tool import DeliveryBatchRecoveryTool
from .config import MaintenanceConfig
from .delivery_gate_runner import DeliveryGateRunner
from .mvp_readiness import run_restricted_live_mvp_readiness
from .redis_streams import RedisStreamConsumer
from .repositories import MaintenanceRepository
from .service import MaintenanceService
from .worker import DueRetryPromotionWorker, MaintenanceQueueWorker, ReplayQueueWorker


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

    mvp = subcommands.add_parser("mvp-readiness")
    mvp.add_argument("--mode", choices=["restricted"], required=True)
    mvp.add_argument("--format", choices=["json"], default="json")
    mvp.add_argument("--operator-review-passed", choices=["true", "false"], default=None)

    recovery = subcommands.add_parser("batch-recovery")
    recovery_subcommands = recovery.add_subparsers(dest="recovery_mode", required=True)

    replay = recovery_subcommands.add_parser("replay-selected")
    replay.add_argument("--plan-id", action="append", required=True)
    replay.add_argument("--requested-by", required=True)
    replay.add_argument("--operator-confirmed", action="store_true")

    retry = recovery_subcommands.add_parser("retry-selected-due")
    retry.add_argument("--plan-id", action="append", required=True)
    retry.add_argument("--requested-by", required=True)
    retry.add_argument("--confirm", choices=["write"], required=True)
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
    config = MaintenanceConfig.from_env()
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
