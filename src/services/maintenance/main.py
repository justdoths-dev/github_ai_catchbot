from __future__ import annotations

import asyncio
import argparse
import json
import logging
from dataclasses import asdict

from .batch_recovery_tool import DeliveryBatchRecoveryTool
from .config import MaintenanceConfig
from .delivery_gate_runner import DeliveryGateRunner
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

    recovery = subcommands.add_parser("batch-recovery")
    recovery_subcommands = recovery.add_subparsers(dest="recovery_mode", required=True)

    replay = recovery_subcommands.add_parser("replay-selected")
    replay.add_argument("--plan-id", action="append", required=True)
    replay.add_argument("--requested-by", required=True)

    retry = recovery_subcommands.add_parser("retry-selected-due")
    retry.add_argument("--plan-id", action="append", required=True)
    retry.add_argument("--requested-by", required=True)
    return parser


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
            return 2 if report.gate_status == "fail" else 0
    finally:
        await engine.dispose()


async def _run_batch_recovery(config: MaintenanceConfig, args: argparse.Namespace) -> int:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with session_factory.begin() as session:
            repository = MaintenanceRepository(session)
            tool = DeliveryBatchRecoveryTool(config, repository=repository)
            if args.recovery_mode == "replay-selected":
                result = await tool.replay_selected(plan_ids=args.plan_id, requested_by=args.requested_by)
            else:
                result = await tool.retry_selected_due(plan_ids=args.plan_id, requested_by=args.requested_by)
            print(json.dumps(asdict(result), ensure_ascii=False, default=str, indent=2, sort_keys=False))
            return 0
    finally:
        await engine.dispose()


async def _run(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = MaintenanceConfig.from_env()
    command = args.command or "worker"
    if command == "worker":
        return await _run_worker(config)
    if command == "delivery-gate":
        return await _run_delivery_gate(config, args)
    if command == "batch-recovery":
        return await _run_batch_recovery(config, args)
    parser.error(f"unsupported command: {command}")
    return 2


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
