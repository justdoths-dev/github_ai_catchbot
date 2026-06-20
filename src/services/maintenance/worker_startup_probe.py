from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from .config import MaintenanceConfig
from .redis_streams import RedisStreamConsumer
from .repositories import MaintenanceRepository
from .service import MaintenanceService
from .worker import DueRetryPromotionWorker, MaintenanceQueueWorker, ReplayQueueWorker


SCHEMA_VERSION = "maintenance_worker_startup_probe_report_v1"

WorkerStartupProbeStatus = Literal["pass", "blocked", "failed"]


@dataclass(frozen=True, slots=True)
class WorkerStartupProbeRequest:
    mode: str
    confirm_run: bool


@dataclass(frozen=True, slots=True)
class WorkerStartupProbeReport:
    schema_version: str
    mode: str
    status: WorkerStartupProbeStatus
    reason_code: str | None
    config_loaded: bool
    db_url_present_redacted: bool
    redis_url_present_redacted: bool
    db_engine_constructed: bool
    db_connectivity_checked: bool
    db_connectivity_ok: bool
    redis_client_constructed: bool
    redis_connectivity_checked: bool
    redis_connectivity_ok: bool
    maintenance_queue_name_present: bool
    maintenance_consumer_group_present: bool
    maintenance_consumer_name_present: bool
    replay_queue_name_present: bool
    replay_consumer_group_present: bool
    replay_consumer_name_present: bool
    maintenance_group_checked: bool
    maintenance_group_exists: bool
    replay_group_checked: bool
    replay_group_exists: bool
    worker_dependencies_constructed: bool
    broad_worker_run_started: bool
    redis_consume_attempted: bool
    redis_ack_attempted: bool
    redis_group_create_attempted: bool
    redis_write_attempted: bool
    db_write_attempted: bool
    systemd_attempted: bool
    docker_attempted: bool
    external_api_attempted: bool
    redactions_applied: dict[str, bool] = field(default_factory=dict)


def worker_startup_probe_request_error(request: WorkerStartupProbeRequest) -> str | None:
    if request.mode != "execute":
        return "mode_not_allowed"
    if not request.confirm_run:
        return "probe_request_not_confirmed"
    return None


def build_worker_startup_probe_blocked_report(
    *,
    mode: str,
    reason_code: str,
    status: WorkerStartupProbeStatus = "blocked",
) -> WorkerStartupProbeReport:
    return _report(mode=mode, status=status, reason_code=reason_code)


async def run_worker_startup_probe(
    request: WorkerStartupProbeRequest,
    *,
    config: MaintenanceConfig,
    logger: logging.Logger | None = None,
    engine_factory: Callable[[str], Any] | None = None,
    session_factory_builder: Callable[[Any], Any] | None = None,
    redis_client_factory: Callable[[str], Any] | None = None,
    consumer_factory: Callable[..., Any] = RedisStreamConsumer,
    maintenance_worker_factory: Callable[..., Any] = MaintenanceQueueWorker,
    replay_worker_factory: Callable[..., Any] = ReplayQueueWorker,
    due_retry_worker_factory: Callable[..., Any] = DueRetryPromotionWorker,
) -> WorkerStartupProbeReport:
    request_error = worker_startup_probe_request_error(request)
    if request_error is not None:
        return _report(mode=request.mode, status="blocked", reason_code=request_error)

    worker_logger = logger or logging.getLogger(__name__)
    report = _report(
        mode=request.mode,
        status="failed",
        reason_code="probe_runtime_error",
        config_loaded=True,
        db_url_present_redacted=bool(config.database_url),
        redis_url_present_redacted=bool(config.redis_url),
        maintenance_queue_name_present=bool(config.maintenance_queue_name),
        maintenance_consumer_group_present=bool(config.maintenance_consumer_group),
        maintenance_consumer_name_present=bool(config.maintenance_consumer_name),
        replay_queue_name_present=bool(config.replay_queue_name),
        replay_consumer_group_present=bool(config.replay_consumer_group),
        replay_consumer_name_present=bool(config.replay_consumer_name),
    )
    engine: Any | None = None
    redis_client: Any | None = None

    try:
        try:
            engine = (engine_factory or _default_engine_factory)(config.database_url)
            session_factory = (session_factory_builder or _default_session_factory_builder)(engine)
        except Exception:
            return _replace(
                report,
                status="failed",
                reason_code="db_engine_construction_failed",
                db_engine_constructed=False,
            )

        report = _replace(report, db_engine_constructed=True)

        try:
            report = _replace(report, db_connectivity_checked=True)
            await _check_db_connectivity(engine)
            report = _replace(report, db_connectivity_ok=True)
        except Exception:
            return _replace(report, status="failed", reason_code="db_connectivity_failed")

        try:
            redis_client = (redis_client_factory or _default_redis_client_factory)(config.redis_url)
        except Exception:
            return _replace(
                report,
                status="failed",
                reason_code="redis_client_construction_failed",
                redis_client_constructed=False,
            )

        report = _replace(report, redis_client_constructed=True)

        try:
            report = _replace(report, redis_connectivity_checked=True)
            await _check_redis_connectivity(redis_client)
            report = _replace(report, redis_connectivity_ok=True)
        except Exception:
            return _replace(report, status="failed", reason_code="redis_connectivity_failed")

        maintenance_consumer = consumer_factory(
            redis_client,
            queue_name=config.maintenance_queue_name,
            consumer_group=config.maintenance_consumer_group,
            consumer_name=config.maintenance_consumer_name,
            block_ms=config.block_ms,
            batch_size=config.batch_size,
        )
        replay_consumer = consumer_factory(
            redis_client,
            queue_name=config.replay_queue_name,
            consumer_group=config.replay_consumer_group,
            consumer_name=config.replay_consumer_name,
            block_ms=config.block_ms,
            batch_size=config.batch_size,
        )

        try:
            report = _replace(report, maintenance_group_checked=True)
            maintenance_group_exists = await _consumer_group_exists(maintenance_consumer)
            report = _replace(report, maintenance_group_exists=maintenance_group_exists)
            if not maintenance_group_exists:
                return _replace(report, status="blocked", reason_code="maintenance_group_missing")

            report = _replace(report, replay_group_checked=True)
            replay_group_exists = await _consumer_group_exists(replay_consumer)
            report = _replace(report, replay_group_exists=replay_group_exists)
            if not replay_group_exists:
                return _replace(report, status="blocked", reason_code="replay_group_missing")
        except Exception:
            return _replace(report, status="failed", reason_code="redis_connectivity_failed")

        class SessionBackedStartupProbeService:
            async def handle_maintenance_trigger_event(self, trigger_event_id):
                async with session_factory.begin() as session:
                    service = MaintenanceService(
                        config,
                        repository=MaintenanceRepository(session),
                        logger=worker_logger,
                    )
                    return await service.handle_maintenance_trigger_event(trigger_event_id)

            async def handle_replay_trigger_event(self, trigger_event_id):
                async with session_factory.begin() as session:
                    service = MaintenanceService(
                        config,
                        repository=MaintenanceRepository(session),
                        logger=worker_logger,
                    )
                    return await service.handle_replay_trigger_event(trigger_event_id)

            async def promote_due_retries_once(self, limit: int | None = None) -> int:
                async with session_factory.begin() as session:
                    service = MaintenanceService(
                        config,
                        repository=MaintenanceRepository(session),
                        logger=worker_logger,
                    )
                    return await service.promote_due_retries_once(limit=limit)

        try:
            service = SessionBackedStartupProbeService()
            maintenance_worker_factory(config, consumer=maintenance_consumer, service=service, logger=worker_logger)
            replay_worker_factory(config, consumer=replay_consumer, service=service, logger=worker_logger)
            due_retry_worker_factory(config, service=service, logger=worker_logger)
        except Exception:
            return _replace(report, status="failed", reason_code="worker_dependency_construction_failed")

        return _replace(
            report,
            status="pass",
            reason_code=None,
            worker_dependencies_constructed=True,
        )
    finally:
        if redis_client is not None:
            await _close_resource(redis_client, close_names=("aclose", "close"))
        if engine is not None:
            await _close_resource(engine, close_names=("dispose",))


def _default_engine_factory(database_url: str):
    from sqlalchemy.ext.asyncio import create_async_engine  # type: ignore[import-not-found]

    return create_async_engine(database_url, future=True)


def _default_session_factory_builder(engine):
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker  # type: ignore[import-not-found]

    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _default_redis_client_factory(redis_url: str):
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    return Redis.from_url(redis_url, decode_responses=True)


async def _check_db_connectivity(engine: Any) -> None:
    from sqlalchemy import text  # type: ignore[import-not-found]

    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))


async def _check_redis_connectivity(redis_client: Any) -> None:
    await redis_client.ping()


async def _consumer_group_exists(consumer: Any) -> bool:
    group_exists = getattr(consumer, "group_exists", None)
    if group_exists is not None:
        return bool(await group_exists())
    ensure_group = getattr(consumer, "ensure_group", None)
    if ensure_group is not None:
        return bool(await ensure_group(allow_create=False))
    raise RuntimeError("consumer_group_check_unavailable")


async def _close_resource(resource: Any, *, close_names: tuple[str, ...]) -> None:
    for close_name in close_names:
        close = getattr(resource, close_name, None)
        if close is None:
            continue
        result = close()
        if inspect.isawaitable(result):
            await result
        return


def _report(
    *,
    mode: str,
    status: WorkerStartupProbeStatus,
    reason_code: str | None,
    config_loaded: bool = False,
    db_url_present_redacted: bool = False,
    redis_url_present_redacted: bool = False,
    db_engine_constructed: bool = False,
    db_connectivity_checked: bool = False,
    db_connectivity_ok: bool = False,
    redis_client_constructed: bool = False,
    redis_connectivity_checked: bool = False,
    redis_connectivity_ok: bool = False,
    maintenance_queue_name_present: bool = False,
    maintenance_consumer_group_present: bool = False,
    maintenance_consumer_name_present: bool = False,
    replay_queue_name_present: bool = False,
    replay_consumer_group_present: bool = False,
    replay_consumer_name_present: bool = False,
    maintenance_group_checked: bool = False,
    maintenance_group_exists: bool = False,
    replay_group_checked: bool = False,
    replay_group_exists: bool = False,
    worker_dependencies_constructed: bool = False,
) -> WorkerStartupProbeReport:
    return WorkerStartupProbeReport(
        schema_version=SCHEMA_VERSION,
        mode=mode,
        status=status,
        reason_code=reason_code,
        config_loaded=config_loaded,
        db_url_present_redacted=db_url_present_redacted,
        redis_url_present_redacted=redis_url_present_redacted,
        db_engine_constructed=db_engine_constructed,
        db_connectivity_checked=db_connectivity_checked,
        db_connectivity_ok=db_connectivity_ok,
        redis_client_constructed=redis_client_constructed,
        redis_connectivity_checked=redis_connectivity_checked,
        redis_connectivity_ok=redis_connectivity_ok,
        maintenance_queue_name_present=maintenance_queue_name_present,
        maintenance_consumer_group_present=maintenance_consumer_group_present,
        maintenance_consumer_name_present=maintenance_consumer_name_present,
        replay_queue_name_present=replay_queue_name_present,
        replay_consumer_group_present=replay_consumer_group_present,
        replay_consumer_name_present=replay_consumer_name_present,
        maintenance_group_checked=maintenance_group_checked,
        maintenance_group_exists=maintenance_group_exists,
        replay_group_checked=replay_group_checked,
        replay_group_exists=replay_group_exists,
        worker_dependencies_constructed=worker_dependencies_constructed,
        broad_worker_run_started=False,
        redis_consume_attempted=False,
        redis_ack_attempted=False,
        redis_group_create_attempted=False,
        redis_write_attempted=False,
        db_write_attempted=False,
        systemd_attempted=False,
        docker_attempted=False,
        external_api_attempted=False,
        redactions_applied={
            "runtime_env_path_omitted": True,
            "runtime_env_values_omitted": True,
            "secret_values_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "raw_exception_body_omitted": True,
            "redis_message_id_omitted": True,
            "payload_json_omitted": True,
        },
    )


def _replace(report: WorkerStartupProbeReport, **changes: Any) -> WorkerStartupProbeReport:
    values = {
        "schema_version": report.schema_version,
        "mode": report.mode,
        "status": report.status,
        "reason_code": report.reason_code,
        "config_loaded": report.config_loaded,
        "db_url_present_redacted": report.db_url_present_redacted,
        "redis_url_present_redacted": report.redis_url_present_redacted,
        "db_engine_constructed": report.db_engine_constructed,
        "db_connectivity_checked": report.db_connectivity_checked,
        "db_connectivity_ok": report.db_connectivity_ok,
        "redis_client_constructed": report.redis_client_constructed,
        "redis_connectivity_checked": report.redis_connectivity_checked,
        "redis_connectivity_ok": report.redis_connectivity_ok,
        "maintenance_queue_name_present": report.maintenance_queue_name_present,
        "maintenance_consumer_group_present": report.maintenance_consumer_group_present,
        "maintenance_consumer_name_present": report.maintenance_consumer_name_present,
        "replay_queue_name_present": report.replay_queue_name_present,
        "replay_consumer_group_present": report.replay_consumer_group_present,
        "replay_consumer_name_present": report.replay_consumer_name_present,
        "maintenance_group_checked": report.maintenance_group_checked,
        "maintenance_group_exists": report.maintenance_group_exists,
        "replay_group_checked": report.replay_group_checked,
        "replay_group_exists": report.replay_group_exists,
        "worker_dependencies_constructed": report.worker_dependencies_constructed,
        "broad_worker_run_started": report.broad_worker_run_started,
        "redis_consume_attempted": report.redis_consume_attempted,
        "redis_ack_attempted": report.redis_ack_attempted,
        "redis_group_create_attempted": report.redis_group_create_attempted,
        "redis_write_attempted": report.redis_write_attempted,
        "db_write_attempted": report.db_write_attempted,
        "systemd_attempted": report.systemd_attempted,
        "docker_attempted": report.docker_attempted,
        "external_api_attempted": report.external_api_attempted,
        "redactions_applied": report.redactions_applied,
    }
    values.update(changes)
    return WorkerStartupProbeReport(**values)
