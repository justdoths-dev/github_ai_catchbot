from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from .config import MaintenanceConfig
from .redis_streams import RedisStreamConsumer
from .repositories import MaintenanceRepository
from .service import MaintenanceService
from .worker import DueRetryPromotionWorker, MaintenanceQueueWorker, ReplayQueueWorker


SCHEMA_VERSION = "maintenance_worker_runtime_crash_probe_report_v1"

MAINTENANCE_QUEUE_WORKER_LABEL = "maintenance_queue_worker"
REPLAY_QUEUE_WORKER_LABEL = "replay_queue_worker"
DUE_RETRY_PROMOTION_WORKER_LABEL = "due_retry_promotion_worker"
WORKER_TASK_LABELS = (
    MAINTENANCE_QUEUE_WORKER_LABEL,
    REPLAY_QUEUE_WORKER_LABEL,
    DUE_RETRY_PROMOTION_WORKER_LABEL,
)

_FAILED_REASON_CODES = {
    MAINTENANCE_QUEUE_WORKER_LABEL: "maintenance_queue_worker_failed",
    REPLAY_QUEUE_WORKER_LABEL: "replay_queue_worker_failed",
    DUE_RETRY_PROMOTION_WORKER_LABEL: "due_retry_promotion_worker_failed",
}
_RETURNED_REASON_CODES = {
    MAINTENANCE_QUEUE_WORKER_LABEL: "maintenance_queue_worker_returned",
    REPLAY_QUEUE_WORKER_LABEL: "replay_queue_worker_returned",
    DUE_RETRY_PROMOTION_WORKER_LABEL: "due_retry_promotion_worker_returned",
}
_CANCEL_TIMEOUT_SEC = 5.0

WorkerRuntimeCrashProbeStatus = Literal["pass", "blocked", "failed"]


@dataclass(frozen=True, slots=True)
class WorkerRuntimeCrashProbeRequest:
    mode: str
    max_runtime_sec: int
    confirm_run: bool


@dataclass(frozen=True, slots=True)
class WorkerRuntimeCrashProbeReport:
    schema_version: str
    mode: str
    status: WorkerRuntimeCrashProbeStatus
    reason_code: str | None
    max_runtime_sec: int
    elapsed_ms: int
    config_loaded: bool
    db_engine_constructed: bool
    redis_client_constructed: bool
    worker_dependencies_constructed: bool
    tasks_started: list[str]
    crashed_task: str | None
    unexpected_return_task: str | None
    timeout_reached: bool
    cancelled_remaining_tasks: bool
    cleanup_completed: bool
    maintenance_queue_worker_started: bool
    replay_queue_worker_started: bool
    due_retry_promotion_worker_started: bool
    broad_worker_run_started: bool
    redis_consume_possible: bool
    redis_ack_possible: bool
    redis_group_create_attempted: bool
    db_write_possible: bool
    systemd_attempted: bool
    docker_attempted: bool
    external_api_attempted: bool
    redactions_applied: dict[str, bool] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class WorkerRuntimeTaskSpec:
    label: str
    awaitable: Awaitable[Any]


@dataclass(frozen=True, slots=True)
class WorkerRuntimeTaskResult:
    status: WorkerRuntimeCrashProbeStatus
    reason_code: str | None
    elapsed_ms: int
    tasks_started: list[str]
    crashed_task: str | None
    unexpected_return_task: str | None
    timeout_reached: bool
    cancelled_remaining_tasks: bool


@dataclass(slots=True)
class WorkerRuntimeComponents:
    engine: Any
    redis_client: Any
    maintenance_worker: Any
    replay_worker: Any
    due_retry_worker: Any


class WorkerRuntimeSetupError(RuntimeError):
    def __init__(self, *, engine: Any | None, redis_client: Any | None) -> None:
        super().__init__("worker_runtime_setup_failed")
        self.engine = engine
        self.redis_client = redis_client


def worker_runtime_crash_probe_request_error(request: WorkerRuntimeCrashProbeRequest) -> str | None:
    if request.mode != "execute":
        return "mode_not_allowed"
    if not request.confirm_run:
        return "probe_request_not_confirmed"
    if request.max_runtime_sec < 1 or request.max_runtime_sec > 30:
        return "max_runtime_not_allowed"
    return None


def build_worker_runtime_crash_probe_blocked_report(
    *,
    mode: str,
    max_runtime_sec: int,
    reason_code: str,
    status: WorkerRuntimeCrashProbeStatus = "blocked",
) -> WorkerRuntimeCrashProbeReport:
    return _report(
        mode=mode,
        max_runtime_sec=max_runtime_sec,
        status=status,
        reason_code=reason_code,
        cleanup_completed=True,
    )


def worker_task_failed_reason_code(label: str) -> str:
    return _FAILED_REASON_CODES.get(label, "probe_runtime_error")


def worker_task_returned_reason_code(label: str) -> str:
    return _RETURNED_REASON_CODES.get(label, "probe_runtime_error")


def build_worker_runtime_components(
    *,
    config: MaintenanceConfig,
    logger: logging.Logger,
    engine_factory: Callable[[str], Any] | None = None,
    session_factory_builder: Callable[[Any], Any] | None = None,
    redis_client_factory: Callable[[str], Any] | None = None,
    consumer_factory: Callable[..., Any] = RedisStreamConsumer,
    maintenance_worker_factory: Callable[..., Any] = MaintenanceQueueWorker,
    replay_worker_factory: Callable[..., Any] = ReplayQueueWorker,
    due_retry_worker_factory: Callable[..., Any] = DueRetryPromotionWorker,
    disable_group_create: bool = False,
    on_constructed: Callable[[str], None] | None = None,
) -> WorkerRuntimeComponents:
    engine: Any | None = None
    redis_client: Any | None = None
    try:
        engine = (engine_factory or _default_engine_factory)(config.database_url)
        _mark_constructed(on_constructed, "db_engine_constructed")
        session_factory = (session_factory_builder or _default_session_factory_builder)(engine)
        redis_client = (redis_client_factory or _default_redis_client_factory)(config.redis_url)
        _mark_constructed(on_constructed, "redis_client_constructed")

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
        if disable_group_create:
            maintenance_consumer = _NoCreateGroupConsumer(maintenance_consumer)
            replay_consumer = _NoCreateGroupConsumer(replay_consumer)

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
        components = WorkerRuntimeComponents(
            engine=engine,
            redis_client=redis_client,
            maintenance_worker=maintenance_worker_factory(
                config,
                consumer=maintenance_consumer,
                service=service,
                logger=logger,
            ),
            replay_worker=replay_worker_factory(
                config,
                consumer=replay_consumer,
                service=service,
                logger=logger,
            ),
            due_retry_worker=due_retry_worker_factory(config, service=service, logger=logger),
        )
        _mark_constructed(on_constructed, "worker_dependencies_constructed")
        return components
    except Exception:
        raise WorkerRuntimeSetupError(engine=engine, redis_client=redis_client) from None


def worker_runtime_task_specs(components: WorkerRuntimeComponents) -> list[WorkerRuntimeTaskSpec]:
    return [
        WorkerRuntimeTaskSpec(
            label=MAINTENANCE_QUEUE_WORKER_LABEL,
            awaitable=components.maintenance_worker.run_forever(),
        ),
        WorkerRuntimeTaskSpec(
            label=REPLAY_QUEUE_WORKER_LABEL,
            awaitable=components.replay_worker.run_forever(),
        ),
        WorkerRuntimeTaskSpec(
            label=DUE_RETRY_PROMOTION_WORKER_LABEL,
            awaitable=components.due_retry_worker.run_forever(),
        ),
    ]


async def run_labeled_worker_tasks(
    task_specs: list[WorkerRuntimeTaskSpec],
    *,
    max_runtime_sec: int | None = None,
) -> WorkerRuntimeTaskResult:
    start = time.monotonic()
    tasks: list[asyncio.Task[Any]] = []
    labels_by_task: dict[asyncio.Task[Any], str] = {}
    for spec in task_specs:
        task = asyncio.create_task(spec.awaitable, name=spec.label)
        tasks.append(task)
        labels_by_task[task] = spec.label
    tasks_started = [labels_by_task[task] for task in tasks]

    try:
        done, pending = await asyncio.wait(
            tasks,
            timeout=max_runtime_sec,
            return_when=asyncio.FIRST_COMPLETED,
        )
    except asyncio.CancelledError:
        await _cancel_remaining_tasks(tasks)
        raise

    if not done:
        cancelled, cancellation_failed = await _cancel_remaining_tasks(tasks)
        reason_code = "cancellation_failed" if cancellation_failed else "timeout_without_crash"
        status: WorkerRuntimeCrashProbeStatus = "failed" if cancellation_failed else "pass"
        return WorkerRuntimeTaskResult(
            status=status,
            reason_code=reason_code,
            elapsed_ms=_elapsed_ms(start),
            tasks_started=tasks_started,
            crashed_task=None,
            unexpected_return_task=None,
            timeout_reached=True,
            cancelled_remaining_tasks=cancelled,
        )

    del pending
    completed_task = _first_completed_task(tasks, done)
    label = labels_by_task[completed_task]
    crashed_task: str | None = None
    unexpected_return_task: str | None = None
    reason_code: str
    if completed_task.cancelled():
        crashed_task = label
        reason_code = worker_task_failed_reason_code(label)
    else:
        try:
            exc = completed_task.exception()
        except asyncio.CancelledError:
            crashed_task = label
            reason_code = worker_task_failed_reason_code(label)
        else:
            if exc is None:
                unexpected_return_task = label
                reason_code = worker_task_returned_reason_code(label)
            else:
                crashed_task = label
                reason_code = worker_task_failed_reason_code(label)

    cancelled, cancellation_failed = await _cancel_remaining_tasks(tasks)
    if cancellation_failed:
        reason_code = "cancellation_failed"
    return WorkerRuntimeTaskResult(
        status="failed",
        reason_code=reason_code,
        elapsed_ms=_elapsed_ms(start),
        tasks_started=tasks_started,
        crashed_task=crashed_task,
        unexpected_return_task=unexpected_return_task,
        timeout_reached=False,
        cancelled_remaining_tasks=cancelled,
    )


async def run_worker_runtime_crash_probe(
    request: WorkerRuntimeCrashProbeRequest,
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
) -> WorkerRuntimeCrashProbeReport:
    request_error = worker_runtime_crash_probe_request_error(request)
    if request_error is not None:
        return build_worker_runtime_crash_probe_blocked_report(
            mode=request.mode,
            max_runtime_sec=request.max_runtime_sec,
            reason_code=request_error,
        )

    started_at = time.monotonic()
    state = {
        "db_engine_constructed": False,
        "redis_client_constructed": False,
        "worker_dependencies_constructed": False,
    }

    def mark_constructed(stage: str) -> None:
        state[stage] = True

    worker_logger = logger or logging.getLogger(__name__)
    components: WorkerRuntimeComponents | None = None
    report = _report(
        mode=request.mode,
        max_runtime_sec=request.max_runtime_sec,
        status="failed",
        reason_code="probe_runtime_error",
        elapsed_ms=0,
        config_loaded=True,
        cleanup_completed=False,
    )
    try:
        components = build_worker_runtime_components(
            config=config,
            logger=worker_logger,
            engine_factory=engine_factory,
            session_factory_builder=session_factory_builder,
            redis_client_factory=redis_client_factory,
            consumer_factory=consumer_factory,
            maintenance_worker_factory=maintenance_worker_factory,
            replay_worker_factory=replay_worker_factory,
            due_retry_worker_factory=due_retry_worker_factory,
            disable_group_create=True,
            on_constructed=mark_constructed,
        )
        report = _replace(
            report,
            db_engine_constructed=state["db_engine_constructed"],
            redis_client_constructed=state["redis_client_constructed"],
            worker_dependencies_constructed=state["worker_dependencies_constructed"],
        )
    except WorkerRuntimeSetupError as exc:
        cleanup_completed = await close_worker_runtime_resources(redis_client=exc.redis_client, engine=exc.engine)
        return _replace(
            report,
            status="failed",
            reason_code="worker_runtime_setup_failed",
            elapsed_ms=_elapsed_ms(started_at),
            db_engine_constructed=state["db_engine_constructed"],
            redis_client_constructed=state["redis_client_constructed"],
            worker_dependencies_constructed=state["worker_dependencies_constructed"],
            cleanup_completed=cleanup_completed,
        )
    except Exception:
        return _replace(
            report,
            status="failed",
            reason_code="probe_runtime_error",
            elapsed_ms=_elapsed_ms(started_at),
            db_engine_constructed=state["db_engine_constructed"],
            redis_client_constructed=state["redis_client_constructed"],
            worker_dependencies_constructed=state["worker_dependencies_constructed"],
            cleanup_completed=True,
        )

    try:
        task_result = await run_labeled_worker_tasks(
            worker_runtime_task_specs(components),
            max_runtime_sec=request.max_runtime_sec,
        )
        report = _replace(
            report,
            status=task_result.status,
            reason_code=task_result.reason_code,
            elapsed_ms=task_result.elapsed_ms,
            tasks_started=task_result.tasks_started,
            crashed_task=task_result.crashed_task,
            unexpected_return_task=task_result.unexpected_return_task,
            timeout_reached=task_result.timeout_reached,
            cancelled_remaining_tasks=task_result.cancelled_remaining_tasks,
            maintenance_queue_worker_started=MAINTENANCE_QUEUE_WORKER_LABEL in task_result.tasks_started,
            replay_queue_worker_started=REPLAY_QUEUE_WORKER_LABEL in task_result.tasks_started,
            due_retry_promotion_worker_started=DUE_RETRY_PROMOTION_WORKER_LABEL in task_result.tasks_started,
            broad_worker_run_started=bool(task_result.tasks_started),
        )
    except Exception:
        report = _replace(
            report,
            status="failed",
            reason_code="probe_runtime_error",
            elapsed_ms=_elapsed_ms(started_at),
        )

    cleanup_completed = await close_worker_runtime_resources(
        redis_client=components.redis_client,
        engine=components.engine,
    )
    if not cleanup_completed:
        return _replace(
            report,
            status="failed",
            reason_code="cleanup_failed",
            elapsed_ms=_elapsed_ms(started_at),
            cleanup_completed=False,
        )
    return _replace(report, cleanup_completed=True)


async def close_worker_runtime_resources(*, redis_client: Any | None, engine: Any | None) -> bool:
    cleanup_completed = True
    if redis_client is not None:
        cleanup_completed = await _close_resource(redis_client, close_names=("aclose", "close")) and cleanup_completed
    if engine is not None:
        cleanup_completed = await _close_resource(engine, close_names=("dispose",)) and cleanup_completed
    return cleanup_completed


def _default_engine_factory(database_url: str):
    from sqlalchemy.ext.asyncio import create_async_engine  # type: ignore[import-not-found]

    return create_async_engine(database_url, future=True)


def _default_session_factory_builder(engine):
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker  # type: ignore[import-not-found]

    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _default_redis_client_factory(redis_url: str):
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    return Redis.from_url(redis_url, decode_responses=True)


class _NoCreateGroupConsumer:
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    async def ensure_group(self, *args, **kwargs) -> bool:
        del args, kwargs
        return bool(await self._inner.ensure_group(allow_create=False))

    async def group_exists(self) -> bool:
        return bool(await self._inner.group_exists())

    async def read_batch(self):
        return await self._inner.read_batch()

    async def ack(self, message_id: str) -> None:
        await self._inner.ack(message_id)


async def _close_resource(resource: Any, *, close_names: tuple[str, ...]) -> bool:
    for close_name in close_names:
        close = getattr(resource, close_name, None)
        if close is None:
            continue
        try:
            result = close()
            if inspect.isawaitable(result):
                await result
        except Exception:
            return False
        return True
    return True


def _mark_constructed(callback: Callable[[str], None] | None, stage: str) -> None:
    if callback is not None:
        callback(stage)


async def _cancel_remaining_tasks(tasks: list[asyncio.Task[Any]]) -> tuple[bool, bool]:
    pending = [task for task in tasks if not task.done()]
    if not pending:
        return False, False
    for task in pending:
        task.cancel()
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*pending, return_exceptions=True),
            timeout=_CANCEL_TIMEOUT_SEC,
        )
    except Exception:
        return True, True
    cancellation_failed = any(
        result is not None and not isinstance(result, asyncio.CancelledError) for result in results
    )
    return True, cancellation_failed


def _first_completed_task(tasks: list[asyncio.Task[Any]], done: set[asyncio.Task[Any]]) -> asyncio.Task[Any]:
    for task in tasks:
        if task in done:
            return task
    return next(iter(done))


def _elapsed_ms(started_at: float) -> int:
    return max(0, int((time.monotonic() - started_at) * 1000))


def _report(
    *,
    mode: str,
    max_runtime_sec: int,
    status: WorkerRuntimeCrashProbeStatus,
    reason_code: str | None,
    elapsed_ms: int = 0,
    config_loaded: bool = False,
    db_engine_constructed: bool = False,
    redis_client_constructed: bool = False,
    worker_dependencies_constructed: bool = False,
    tasks_started: list[str] | None = None,
    crashed_task: str | None = None,
    unexpected_return_task: str | None = None,
    timeout_reached: bool = False,
    cancelled_remaining_tasks: bool = False,
    cleanup_completed: bool = False,
    maintenance_queue_worker_started: bool = False,
    replay_queue_worker_started: bool = False,
    due_retry_promotion_worker_started: bool = False,
    broad_worker_run_started: bool = False,
) -> WorkerRuntimeCrashProbeReport:
    return WorkerRuntimeCrashProbeReport(
        schema_version=SCHEMA_VERSION,
        mode=mode,
        status=status,
        reason_code=reason_code,
        max_runtime_sec=max_runtime_sec,
        elapsed_ms=elapsed_ms,
        config_loaded=config_loaded,
        db_engine_constructed=db_engine_constructed,
        redis_client_constructed=redis_client_constructed,
        worker_dependencies_constructed=worker_dependencies_constructed,
        tasks_started=list(tasks_started or []),
        crashed_task=crashed_task,
        unexpected_return_task=unexpected_return_task,
        timeout_reached=timeout_reached,
        cancelled_remaining_tasks=cancelled_remaining_tasks,
        cleanup_completed=cleanup_completed,
        maintenance_queue_worker_started=maintenance_queue_worker_started,
        replay_queue_worker_started=replay_queue_worker_started,
        due_retry_promotion_worker_started=due_retry_promotion_worker_started,
        broad_worker_run_started=broad_worker_run_started,
        redis_consume_possible=True,
        redis_ack_possible=True,
        redis_group_create_attempted=False,
        db_write_possible=True,
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
            "traceback_omitted": True,
            "redis_message_id_omitted": True,
            "payload_json_omitted": True,
            "systemd_stdout_stderr_omitted": True,
        },
    )


def _replace(report: WorkerRuntimeCrashProbeReport, **changes: Any) -> WorkerRuntimeCrashProbeReport:
    values = {
        "schema_version": report.schema_version,
        "mode": report.mode,
        "status": report.status,
        "reason_code": report.reason_code,
        "max_runtime_sec": report.max_runtime_sec,
        "elapsed_ms": report.elapsed_ms,
        "config_loaded": report.config_loaded,
        "db_engine_constructed": report.db_engine_constructed,
        "redis_client_constructed": report.redis_client_constructed,
        "worker_dependencies_constructed": report.worker_dependencies_constructed,
        "tasks_started": list(report.tasks_started),
        "crashed_task": report.crashed_task,
        "unexpected_return_task": report.unexpected_return_task,
        "timeout_reached": report.timeout_reached,
        "cancelled_remaining_tasks": report.cancelled_remaining_tasks,
        "cleanup_completed": report.cleanup_completed,
        "maintenance_queue_worker_started": report.maintenance_queue_worker_started,
        "replay_queue_worker_started": report.replay_queue_worker_started,
        "due_retry_promotion_worker_started": report.due_retry_promotion_worker_started,
        "broad_worker_run_started": report.broad_worker_run_started,
        "redis_consume_possible": report.redis_consume_possible,
        "redis_ack_possible": report.redis_ack_possible,
        "redis_group_create_attempted": report.redis_group_create_attempted,
        "db_write_possible": report.db_write_possible,
        "systemd_attempted": report.systemd_attempted,
        "docker_attempted": report.docker_attempted,
        "external_api_attempted": report.external_api_attempted,
        "redactions_applied": report.redactions_applied,
    }
    values.update(changes)
    return WorkerRuntimeCrashProbeReport(**values)
