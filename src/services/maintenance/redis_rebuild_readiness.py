from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from .config import MaintenanceConfig


SCHEMA_VERSION = "redis_rebuild_readiness_report_v1"
RUNNER_NAME = "bounded_redis_rebuild_readiness_runner"

DEFAULT_QUEUE_KEYS = ("maintenance", "replay", "notification_send")

OPEN_GATES = {
    "AUTHORITY_OPEN": True,
    "ROLLOUT_OPEN": True,
    "PRODUCTION_ROLLOUT_OPEN": True,
    "ACTUAL_REDIS_MUTATION_OPEN": True,
    "ACTUAL_REDIS_FLUSH_OPEN": True,
    "ACTUAL_REDIS_CONSUME_ACK_OPEN": True,
    "ACTUAL_TELEGRAM_SEND_OPEN": True,
}

COMPLETION_CLAIMS = {
    "REDIS_REBUILD_CLOSED": False,
    "PRODUCT_COMPLETE_CLOSED": False,
    "final_bot_complete": False,
    "one_hundred_percent_complete": False,
    "production_rollout_complete": False,
}

BLOCKED_ACTIONS = (
    "redis_flush",
    "redis_stream_or_group_create",
    "redis_xadd_rebuild_jobs",
    "redis_consume_ack_or_delete_pending",
    "db_abandoned_job_transition",
    "db_outbox_status_mutation",
)

O3B_AUTHORITY = (
    "operator_approved_exact_redis_target",
    "redis_xgroup_create_for_missing_groups",
    "redis_xadd_for_rebuildable_durable_rows",
    "redis_pending_consume_ack_or_delete_only_if_explicitly_scoped",
    "db_write_for_abandoned_job_transition_if_selected",
    "post_write_read_only_audit",
)


@dataclass(frozen=True, slots=True)
class RedisRebuildReadinessRequest:
    mode: str
    queue_selector: str | None = None
    all_known_queues: bool = False
    include_empty: bool = False
    max_sample: int = 3


@dataclass(frozen=True, slots=True)
class KnownQueueSpec:
    key: str
    stream_name: str
    consumer_groups: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RedisQueueInventory:
    queue_key: str
    stream_present: bool | None
    stream_type_bucket: str
    stream_length: int | None = None
    configured_group_count: int = 0
    present_group_count: int = 0
    missing_group_count: int = 0
    pending_count: int | None = None
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class RedisInventorySnapshot:
    queues: tuple[RedisQueueInventory, ...]
    read_error_code: str | None = None


@dataclass(frozen=True, slots=True)
class DurableCategoryInventory:
    name: str
    state: str
    total_count: int = 0
    status_counts: Mapping[str, int] = field(default_factory=dict)
    queue_counts: Mapping[str, int] = field(default_factory=dict)
    stage_counts: Mapping[str, int] = field(default_factory=dict)
    age_counts: Mapping[str, int] = field(default_factory=dict)
    sample_shape_count: int = 0
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class DurableInventorySnapshot:
    categories: tuple[DurableCategoryInventory, ...]
    read_error_code: str | None = None


class RedisReadOnlyInventoryReader(Protocol):
    async def inspect_queues(self, queues: Sequence[KnownQueueSpec]) -> RedisInventorySnapshot: ...


class DurableReadOnlyInventoryReader(Protocol):
    async def load_rebuild_sources(self, *, max_sample: int) -> DurableInventorySnapshot: ...


class RedisRebuildReadinessError(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class RedisReadOnlyQueueInspector:
    def __init__(self, client: Any) -> None:
        self._client = client

    async def inspect_queues(self, queues: Sequence[KnownQueueSpec]) -> RedisInventorySnapshot:
        rows: list[RedisQueueInventory] = []
        try:
            ping = getattr(self._client, "ping", None)
            if ping is not None:
                await ping()
            for queue in queues:
                rows.append(await self._inspect_queue(queue))
        except Exception:
            return RedisInventorySnapshot(queues=tuple(rows), read_error_code="redis_inventory_read_failed")
        return RedisInventorySnapshot(queues=tuple(rows))

    async def _inspect_queue(self, queue: KnownQueueSpec) -> RedisQueueInventory:
        try:
            exists = await self._client.exists(queue.stream_name)
            if int(exists or 0) <= 0:
                return RedisQueueInventory(
                    queue_key=queue.key,
                    stream_present=False,
                    stream_type_bucket="missing",
                    configured_group_count=len(queue.consumer_groups),
                    missing_group_count=len(queue.consumer_groups),
                    pending_count=0,
                    reason_code="stream_missing",
                )
            stream_type = _decode_value(await self._client.type(queue.stream_name))
            if stream_type != "stream":
                return RedisQueueInventory(
                    queue_key=queue.key,
                    stream_present=False,
                    stream_type_bucket="not_stream",
                    configured_group_count=len(queue.consumer_groups),
                    missing_group_count=len(queue.consumer_groups),
                    pending_count=0,
                    reason_code="stream_type_mismatch",
                )

            stream_length = int(await self._client.xlen(queue.stream_name) or 0)
            xinfo_stream = getattr(self._client, "xinfo_stream", None)
            if xinfo_stream is not None:
                await xinfo_stream(queue.stream_name)
            groups = await self._client.xinfo_groups(queue.stream_name)
            present_names = {_decode_value(_dict_get(group, "name")) for group in groups or [] if isinstance(group, dict)}
            configured_groups = set(queue.consumer_groups)
            present_group_count = len(configured_groups & present_names)
            missing_group_count = len(configured_groups - present_names)
            pending_count = 0
            for group_name in sorted(configured_groups & present_names):
                pending_count += await self._pending_count(queue.stream_name, group_name)
            return RedisQueueInventory(
                queue_key=queue.key,
                stream_present=True,
                stream_type_bucket="stream",
                stream_length=stream_length,
                configured_group_count=len(configured_groups),
                present_group_count=present_group_count,
                missing_group_count=missing_group_count,
                pending_count=pending_count,
                reason_code=None if missing_group_count == 0 else "consumer_group_missing",
            )
        except Exception:
            return RedisQueueInventory(
                queue_key=queue.key,
                stream_present=None,
                stream_type_bucket="unknown",
                configured_group_count=len(queue.consumer_groups),
                missing_group_count=len(queue.consumer_groups),
                pending_count=None,
                reason_code="queue_metadata_read_failed",
            )

    async def _pending_count(self, stream_name: str, group_name: str) -> int:
        try:
            summary = await self._client.xpending(stream_name, group_name)
        except Exception:
            return 0
        if isinstance(summary, Mapping):
            return _safe_int(summary.get("pending")) or _safe_int(summary.get("count")) or 0
        if isinstance(summary, (tuple, list)) and summary:
            return _safe_int(summary[0]) or 0
        return 0


class SqlAlchemyDurableRebuildInventoryRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def load_rebuild_sources(self, *, max_sample: int) -> DurableInventorySnapshot:
        categories: list[DurableCategoryInventory] = []
        try:
            categories.append(await self._event_outbox(max_sample=max_sample))
            categories.append(await self._job_attempts(max_sample=max_sample))
            categories.append(await self._notification_plans(max_sample=max_sample))
            categories.append(await self._replay_requests(max_sample=max_sample))
            categories.append(await self._dead_letter_entries(max_sample=max_sample))
        except Exception:
            return DurableInventorySnapshot(
                categories=tuple(categories),
                read_error_code="durable_inventory_read_failed",
            )
        return DurableInventorySnapshot(categories=tuple(categories))

    async def _table_present(self, table_name: str) -> bool:
        result = await self._session.execute(
            _sql("SELECT to_regclass(:table_name) IS NOT NULL AS present"),
            {"table_name": table_name},
        )
        row = result.mappings().first()
        return bool(row and row["present"])

    async def _event_outbox(self, *, max_sample: int) -> DurableCategoryInventory:
        if not await self._table_present("event_outbox"):
            return _not_present("event_outbox")
        rows = await self._all(
            """
            SELECT status::text AS status,
                   event_type,
                   payload_json->>'provider_route' AS provider_route,
                   CASE
                     WHEN created_at >= now() - interval '15 minutes' THEN 'fresh'
                     WHEN created_at >= now() - interval '1 hour' THEN 'recent'
                     WHEN created_at >= now() - interval '24 hours' THEN 'same_day'
                     ELSE 'older'
                   END AS age_bucket,
                   COUNT(*) AS row_count
            FROM event_outbox
            WHERE status IN ('pending'::outbox_status_enum, 'failed'::outbox_status_enum)
               OR published_at IS NULL
            GROUP BY status, event_type, provider_route, age_bucket
            ORDER BY row_count DESC
            LIMIT :limit
            """,
            {"limit": max(max_sample, 1) * 8},
        )
        status_counts: dict[str, int] = {}
        queue_counts: dict[str, int] = {}
        age_counts: dict[str, int] = {}
        total = 0
        for row in rows:
            count = _safe_int(row["row_count"])
            total += count
            _add(status_counts, _safe_status(row["status"]), count)
            _add(queue_counts, _event_type_queue_bucket(row["event_type"], row["provider_route"]), count)
            _add(age_counts, _safe_age_bucket(row["age_bucket"]), count)
        return DurableCategoryInventory(
            name="event_outbox",
            state="present",
            total_count=total,
            status_counts=status_counts,
            queue_counts=queue_counts,
            age_counts=age_counts,
            sample_shape_count=min(max_sample, len(rows)),
        )

    async def _job_attempts(self, *, max_sample: int) -> DurableCategoryInventory:
        if not await self._table_present("job_attempts"):
            return _not_present("job_attempts")
        rows = await self._all(
            """
            SELECT attempt_status::text AS status,
                   queue_name,
                   stage_name,
                   CASE
                     WHEN created_at >= now() - interval '15 minutes' THEN 'fresh'
                     WHEN created_at >= now() - interval '1 hour' THEN 'recent'
                     WHEN created_at >= now() - interval '24 hours' THEN 'same_day'
                     ELSE 'older'
                   END AS age_bucket,
                   COUNT(*) AS row_count
            FROM job_attempts
            WHERE attempt_status IN (
                'pending'::job_attempt_status_enum,
                'running'::job_attempt_status_enum,
                'failed_retryable'::job_attempt_status_enum,
                'abandoned'::job_attempt_status_enum
            )
            GROUP BY attempt_status, queue_name, stage_name, age_bucket
            ORDER BY row_count DESC
            LIMIT :limit
            """,
            {"limit": max(max_sample, 1) * 8},
        )
        return _category_from_queue_rows("job_attempts", rows, max_sample=max_sample)

    async def _notification_plans(self, *, max_sample: int) -> DurableCategoryInventory:
        if not await self._table_present("notification_plans"):
            return _not_present("notification_plans")
        rows = await self._all(
            """
            SELECT status::text AS status,
                   CASE
                     WHEN created_at >= now() - interval '15 minutes' THEN 'fresh'
                     WHEN created_at >= now() - interval '1 hour' THEN 'recent'
                     WHEN created_at >= now() - interval '24 hours' THEN 'same_day'
                     ELSE 'older'
                   END AS age_bucket,
                   COUNT(*) AS row_count
            FROM notification_plans
            WHERE status IN (
                'planned'::notification_status_enum,
                'rendered'::notification_status_enum,
                'queued'::notification_status_enum,
                'failed_retryable'::notification_status_enum
            )
            GROUP BY status, age_bucket
            ORDER BY row_count DESC
            LIMIT :limit
            """,
            {"limit": max(max_sample, 1) * 8},
        )
        status_counts: dict[str, int] = {}
        queue_counts: dict[str, int] = {}
        age_counts: dict[str, int] = {}
        total = 0
        for row in rows:
            count = _safe_int(row["row_count"])
            total += count
            _add(status_counts, _safe_status(row["status"]), count)
            _add(queue_counts, "notification_send", count)
            _add(age_counts, _safe_age_bucket(row["age_bucket"]), count)
        return DurableCategoryInventory(
            name="notification_plans",
            state="present",
            total_count=total,
            status_counts=status_counts,
            queue_counts=queue_counts,
            age_counts=age_counts,
            sample_shape_count=min(max_sample, len(rows)),
        )

    async def _replay_requests(self, *, max_sample: int) -> DurableCategoryInventory:
        if not await self._table_present("replay_requests"):
            return _not_present("replay_requests")
        rows = await self._all(
            """
            SELECT status,
                   replay_type::text AS stage_name,
                   CASE
                     WHEN requested_at >= now() - interval '15 minutes' THEN 'fresh'
                     WHEN requested_at >= now() - interval '1 hour' THEN 'recent'
                     WHEN requested_at >= now() - interval '24 hours' THEN 'same_day'
                     ELSE 'older'
                   END AS age_bucket,
                   COUNT(*) AS row_count
            FROM replay_requests
            WHERE status IN ('pending', 'requested', 'failed', 'rejected_by_env_guard')
            GROUP BY status, replay_type, age_bucket
            ORDER BY row_count DESC
            LIMIT :limit
            """,
            {"limit": max(max_sample, 1) * 8},
        )
        status_counts: dict[str, int] = {}
        queue_counts: dict[str, int] = {}
        stage_counts: dict[str, int] = {}
        age_counts: dict[str, int] = {}
        total = 0
        for row in rows:
            count = _safe_int(row["row_count"])
            total += count
            _add(status_counts, _safe_status(row["status"]), count)
            _add(queue_counts, "replay", count)
            _add(stage_counts, _safe_stage(row["stage_name"]), count)
            _add(age_counts, _safe_age_bucket(row["age_bucket"]), count)
        return DurableCategoryInventory(
            name="replay_requests",
            state="present",
            total_count=total,
            status_counts=status_counts,
            queue_counts=queue_counts,
            stage_counts=stage_counts,
            age_counts=age_counts,
            sample_shape_count=min(max_sample, len(rows)),
        )

    async def _dead_letter_entries(self, *, max_sample: int) -> DurableCategoryInventory:
        if not await self._table_present("dead_letter_entries"):
            return _not_present("dead_letter_entries")
        rows = await self._all(
            """
            SELECT queue_name,
                   stage_name,
                   CASE
                     WHEN replay_hint IS NOT NULL OR next_manual_action ILIKE '%replay%' THEN 'replayable'
                     ELSE 'manual_review'
                   END AS status,
                   CASE
                     WHEN last_failed_at >= now() - interval '15 minutes' THEN 'fresh'
                     WHEN last_failed_at >= now() - interval '1 hour' THEN 'recent'
                     WHEN last_failed_at >= now() - interval '24 hours' THEN 'same_day'
                     ELSE 'older'
                   END AS age_bucket,
                   COUNT(*) AS row_count
            FROM dead_letter_entries
            WHERE replay_hint IS NOT NULL OR next_manual_action ILIKE '%replay%'
            GROUP BY queue_name, stage_name, status, age_bucket
            ORDER BY row_count DESC
            LIMIT :limit
            """,
            {"limit": max(max_sample, 1) * 8},
        )
        return _category_from_queue_rows("dead_letter_entries", rows, max_sample=max_sample)

    async def _all(self, statement: str, params: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        result = await self._session.execute(_sql(statement), dict(params))
        return list(result.mappings().all())


async def build_redis_rebuild_readiness_report(
    request: RedisRebuildReadinessRequest,
    *,
    config: MaintenanceConfig,
    redis_reader: RedisReadOnlyInventoryReader,
    durable_reader: DurableReadOnlyInventoryReader,
) -> dict[str, Any]:
    request_error = _request_error(request)
    if request_error is not None:
        return blocked_report(request_error, mode=_safe_mode(request.mode))

    try:
        queue_specs = _select_queue_specs(config, request)
    except RedisRebuildReadinessError as exc:
        return blocked_report(exc.reason_code, mode=request.mode)
    redis_attempted = False
    db_attempted = False
    redis_snapshot = RedisInventorySnapshot(queues=())
    durable_snapshot = DurableInventorySnapshot(categories=())
    status = "pass"
    reason_code = "readiness_inventory_pass" if request.mode == "inventory" else "dry_run_plan_pass"

    try:
        redis_attempted = True
        redis_snapshot = await redis_reader.inspect_queues(queue_specs)
    except Exception:
        redis_snapshot = RedisInventorySnapshot(queues=(), read_error_code="redis_inventory_read_failed")
    try:
        db_attempted = True
        durable_snapshot = await durable_reader.load_rebuild_sources(max_sample=request.max_sample)
    except Exception:
        durable_snapshot = DurableInventorySnapshot(categories=(), read_error_code="durable_inventory_read_failed")

    if redis_snapshot.read_error_code or durable_snapshot.read_error_code:
        status = "failed"
        reason_code = redis_snapshot.read_error_code or durable_snapshot.read_error_code or "readiness_inventory_failed"

    redis_inventory = _redis_inventory_to_report(queue_specs, redis_snapshot, include_empty=request.include_empty)
    durable_inventory = _durable_inventory_to_report(durable_snapshot, db_read_attempted=db_attempted)
    plan = _dry_run_plan(redis_inventory, durable_inventory)

    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": request.mode,
        "status": status,
        "reason_code": reason_code,
        "redis_inventory": redis_inventory,
        "durable_inventory": durable_inventory,
        "dry_run_rebuild_plan": plan,
        "authority": _authority(
            redis_read_attempted=redis_attempted,
            db_read_attempted=db_attempted,
        ),
        "redactions_applied": _redactions_applied(),
        "open_gates": dict(OPEN_GATES),
        "completion_claims": dict(COMPLETION_CLAIMS),
        "recommended_next_operator_action": _next_action(status),
    }


async def run_redis_rebuild_readiness_from_runtime_env_file(
    runtime_env_file: str,
    request: RedisRebuildReadinessRequest,
) -> dict[str, Any]:
    path_error = _runtime_env_file_error(runtime_env_file)
    if path_error is not None:
        return blocked_report(path_error, mode=_safe_mode(request.mode))
    request_error = _request_error(request)
    if request_error is not None:
        return blocked_report(request_error, mode=_safe_mode(request.mode))

    redis_client = None
    engine = None
    try:
        config = load_runtime_config_from_env_file(runtime_env_file)
        from redis.asyncio import Redis  # type: ignore[import-not-found]
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

        redis_client = Redis.from_url(config.redis_url, decode_responses=True)
        engine = create_async_engine(config.database_url, future=True)
        session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with session_factory() as session:
            return await build_redis_rebuild_readiness_report(
                request,
                config=config,
                redis_reader=RedisReadOnlyQueueInspector(redis_client),
                durable_reader=SqlAlchemyDurableRebuildInventoryRepository(session),
            )
    except RedisRebuildReadinessError as exc:
        return blocked_report(exc.reason_code, mode=_safe_mode(request.mode))
    except Exception:
        return blocked_report("readiness_runtime_error", mode=_safe_mode(request.mode), status="failed")
    finally:
        if redis_client is not None:
            close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
            if close is not None:
                result = close()
                if hasattr(result, "__await__"):
                    await result
        if engine is not None:
            await engine.dispose()


def load_runtime_config_from_env_file(runtime_env_file: str) -> MaintenanceConfig:
    from .main import (  # Reuse the existing allowlisted runtime env-file loader.
        _MaintenanceOneShotRuntimeConfigError,
        _one_shot_runtime_config_reason_code,
        _resolve_one_shot_runtime_env_file_overlay,
        _temporary_environment_defaults,
    )

    try:
        overlay = _resolve_one_shot_runtime_env_file_overlay(runtime_env_file)
        with _temporary_environment_defaults(overlay):
            return MaintenanceConfig.from_env()
    except _MaintenanceOneShotRuntimeConfigError as exc:
        raise RedisRebuildReadinessError(_one_shot_runtime_config_reason_code(exc)) from None
    except Exception:
        raise RedisRebuildReadinessError("maintenance_runtime_config_error") from None


def blocked_report(reason_code: str, *, mode: str = "inventory", status: str = "blocked") -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": mode,
        "status": status,
        "reason_code": reason_code,
        "redis_inventory": _empty_redis_inventory(),
        "durable_inventory": _empty_durable_inventory(),
        "dry_run_rebuild_plan": _empty_dry_run_plan(),
        "authority": _authority(redis_read_attempted=False, db_read_attempted=False),
        "redactions_applied": _redactions_applied(),
        "open_gates": dict(OPEN_GATES),
        "completion_claims": dict(COMPLETION_CLAIMS),
        "recommended_next_operator_action": "fix_blocker_then_rerun_o3a_readiness",
    }


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def known_queue_specs(config: MaintenanceConfig) -> tuple[KnownQueueSpec, ...]:
    return (
        KnownQueueSpec("source_normalize", "q.source.normalize", ("router-normalizer",)),
        KnownQueueSpec("candidate_bundle", "q.candidate.bundle", ("evidence-assembler",)),
        KnownQueueSpec("analysis_route", "q.analysis.route", ("analysis-router",)),
        KnownQueueSpec("analysis_judge", "q.analysis.judge", ("judge-openai",)),
        KnownQueueSpec("analysis_validate", "q.analysis.validate", ("analysis-validator",)),
        KnownQueueSpec("analysis_policy", "q.analysis.policy", ("policy-engine",)),
        KnownQueueSpec("notification_send", "q.notification.send", ("notifier-telegram",)),
        KnownQueueSpec("replay", config.replay_queue_name, (config.replay_consumer_group,)),
        KnownQueueSpec("maintenance", config.maintenance_queue_name, (config.maintenance_consumer_group,)),
        KnownQueueSpec("artifact_enrich_github", "q.artifact.enrich.github", ("gh-enricher",)),
        KnownQueueSpec("artifact_enrich_x", "q.artifact.enrich.x", ("x-enricher",)),
        KnownQueueSpec("artifact_enrich_web", "q.artifact.enrich.web", ("web-enricher",)),
    )


def _select_queue_specs(
    config: MaintenanceConfig,
    request: RedisRebuildReadinessRequest,
) -> tuple[KnownQueueSpec, ...]:
    specs = known_queue_specs(config)
    if request.queue_selector:
        selector = request.queue_selector.strip()
        selected = tuple(spec for spec in specs if selector in {spec.key, spec.stream_name})
        if not selected:
            raise RedisRebuildReadinessError("queue_not_known")
        return selected
    if request.all_known_queues:
        return specs
    defaults = set(DEFAULT_QUEUE_KEYS)
    return tuple(spec for spec in specs if spec.key in defaults)


def _request_error(request: RedisRebuildReadinessRequest) -> str | None:
    if request.mode not in {"inventory", "plan"}:
        return "mode_not_allowed"
    if request.queue_selector and request.all_known_queues:
        return "queue_selector_conflicts_with_all_known_queues"
    if request.max_sample < 0 or request.max_sample > 10:
        return "max_sample_not_allowed"
    return None


def _runtime_env_file_error(runtime_env_file: str) -> str | None:
    if not runtime_env_file:
        return "runtime_env_file_required"
    try:
        path = Path(runtime_env_file)
    except (TypeError, ValueError):
        return "runtime_env_file_invalid"
    if not path.is_absolute():
        return "runtime_env_file_not_absolute"
    return None


def _redis_inventory_to_report(
    queue_specs: Sequence[KnownQueueSpec],
    snapshot: RedisInventorySnapshot,
    *,
    include_empty: bool,
) -> dict[str, Any]:
    queue_keys = [spec.key for spec in queue_specs]
    queues = list(snapshot.queues)
    if not include_empty:
        queues = [queue for queue in queues if queue.stream_present is not False or queue.missing_group_count > 0]
    stream_presence = {
        queue.queue_key: _presence_bucket(queue.stream_present)
        for queue in queues
    }
    group_presence = {
        queue.queue_key: {
            "configured_group_count_bucket": _count_bucket(queue.configured_group_count),
            "present_group_count_bucket": _count_bucket(queue.present_group_count),
            "missing_group_count_bucket": _count_bucket(queue.missing_group_count),
        }
        for queue in queues
    }
    pending_counts = {
        queue.queue_key: _count_bucket(queue.pending_count)
        for queue in queues
    }
    stream_lengths = {
        queue.queue_key: _count_bucket(queue.stream_length)
        for queue in queues
    }
    return {
        "known_queue_count_bucket": _count_bucket(len(queue_keys)),
        "known_queue_buckets": queue_keys,
        "stream_presence_buckets": stream_presence,
        "group_presence_buckets": group_presence,
        "pending_count_buckets": pending_counts,
        "stream_length_buckets": stream_lengths,
        "missing_stream_buckets": [queue.queue_key for queue in snapshot.queues if queue.stream_present is False],
        "missing_group_buckets": [queue.queue_key for queue in snapshot.queues if queue.missing_group_count > 0],
        "read_error_code": snapshot.read_error_code,
        "raw_stream_ids_omitted": True,
        "raw_group_names_if_sensitive_omitted_or_bucketed": True,
    }


def _durable_inventory_to_report(
    snapshot: DurableInventorySnapshot,
    *,
    db_read_attempted: bool,
) -> dict[str, Any]:
    categories = {
        category.name: _category_to_report(category)
        for category in snapshot.categories
    }
    return {
        "db_read_attempted": db_read_attempted,
        "durable_source_categories": categories,
        "read_error_code": snapshot.read_error_code,
        "raw_ids_omitted": True,
        "raw_payloads_omitted": True,
    }


def _category_to_report(category: DurableCategoryInventory) -> dict[str, Any]:
    if category.state != "present":
        return {
            "state": category.state,
            "count_bucket": "zero",
            "reason_code": category.reason_code,
            "raw_ids_omitted": True,
            "raw_payloads_omitted": True,
        }
    return {
        "state": "present",
        "count_bucket": _count_bucket(category.total_count),
        "status_buckets": _bucketed_counts(category.status_counts),
        "queue_buckets": _bucketed_counts(category.queue_counts),
        "stage_buckets": _bucketed_counts(category.stage_counts),
        "age_buckets": _bucketed_counts(category.age_counts),
        "sample_shape_count_bucket": _count_bucket(category.sample_shape_count),
        "raw_ids_omitted": True,
        "raw_payloads_omitted": True,
    }


def _dry_run_plan(redis_inventory: Mapping[str, Any], durable_inventory: Mapping[str, Any]) -> dict[str, Any]:
    categories = durable_inventory.get("durable_source_categories") or {}
    planned_actions = {
        "outbox_events_to_publish": _category_count_bucket(categories, "event_outbox"),
        "job_attempts_to_requeue": _category_count_bucket(categories, "job_attempts"),
        "notification_plans_to_requeue": _category_count_bucket(categories, "notification_plans"),
        "replay_requests_to_requeue": _category_count_bucket(categories, "replay_requests"),
        "dlq_entries_replayable": _category_count_bucket(categories, "dead_letter_entries"),
        "unsupported_or_not_present": _unsupported_category_buckets(categories),
    }
    missing_streams = list(redis_inventory.get("missing_stream_buckets") or [])
    missing_groups = list(redis_inventory.get("missing_group_buckets") or [])
    return {
        "would_create_streams": False,
        "would_create_groups": False,
        "would_xadd_jobs": False,
        "would_ack_or_delete_pending": False,
        "planned_actions_are_dry_run_only": True,
        "planned_action_buckets": planned_actions,
        "missing_stream_buckets": missing_streams,
        "missing_group_buckets": missing_groups,
        "blocked_in_o3a": list(BLOCKED_ACTIONS),
        "o3b_required_authority": list(O3B_AUTHORITY),
    }


def _empty_redis_inventory() -> dict[str, Any]:
    return {
        "known_queue_count_bucket": "zero",
        "known_queue_buckets": [],
        "stream_presence_buckets": {},
        "group_presence_buckets": {},
        "pending_count_buckets": {},
        "stream_length_buckets": {},
        "missing_stream_buckets": [],
        "missing_group_buckets": [],
        "read_error_code": None,
        "raw_stream_ids_omitted": True,
        "raw_group_names_if_sensitive_omitted_or_bucketed": True,
    }


def _empty_durable_inventory() -> dict[str, Any]:
    return {
        "db_read_attempted": False,
        "durable_source_categories": {},
        "read_error_code": None,
        "raw_ids_omitted": True,
        "raw_payloads_omitted": True,
    }


def _empty_dry_run_plan() -> dict[str, Any]:
    return {
        "would_create_streams": False,
        "would_create_groups": False,
        "would_xadd_jobs": False,
        "would_ack_or_delete_pending": False,
        "planned_actions_are_dry_run_only": True,
        "planned_action_buckets": {
            "outbox_events_to_publish": "zero",
            "job_attempts_to_requeue": "zero",
            "notification_plans_to_requeue": "zero",
            "replay_requests_to_requeue": "zero",
            "dlq_entries_replayable": "zero",
            "unsupported_or_not_present": [],
        },
        "missing_stream_buckets": [],
        "missing_group_buckets": [],
        "blocked_in_o3a": list(BLOCKED_ACTIONS),
        "o3b_required_authority": list(O3B_AUTHORITY),
    }


def _authority(*, redis_read_attempted: bool, db_read_attempted: bool) -> dict[str, bool]:
    return {
        "redis_read_attempted": redis_read_attempted,
        "redis_mutation_attempted": False,
        "redis_flush_attempted": False,
        "redis_xadd_attempted": False,
        "redis_xack_attempted": False,
        "redis_xgroup_mutation_attempted": False,
        "redis_xreadgroup_attempted": False,
        "db_read_attempted": db_read_attempted,
        "db_write_attempted": False,
        "runtime_env_values_output": False,
        "secrets_output": False,
        "telegram_attempted": False,
        "openai_attempted": False,
        "github_attempted": False,
        "x_attempted": False,
        "web_attempted": False,
        "systemd_attempted": False,
        "docker_attempted": False,
        "migration_attempted": False,
    }


def _redactions_applied() -> dict[str, bool]:
    return {
        "runtime_env_path_omitted": True,
        "runtime_env_values_omitted": True,
        "database_url_omitted": True,
        "redis_url_omitted": True,
        "secret_values_omitted": True,
        "raw_stream_ids_omitted": True,
        "raw_consumer_names_omitted_or_bucketed": True,
        "raw_event_ids_omitted": True,
        "raw_job_ids_omitted": True,
        "raw_aggregate_ids_omitted": True,
        "raw_dedupe_keys_omitted": True,
        "raw_payload_json_omitted": True,
        "raw_source_text_omitted": True,
        "raw_urls_omitted": True,
        "raw_exception_bodies_omitted": True,
    }


def _next_action(status: str) -> str:
    if status == "pass":
        return "submit_review_bundle_for_chatgpt_pass_before_o3b"
    if status == "failed":
        return "repair_o3a_read_only_inventory_before_o3b"
    return "fix_blocker_then_rerun_o3a_readiness"


def _category_count_bucket(categories: Mapping[str, Any], name: str) -> str:
    category = categories.get(name)
    if not isinstance(category, Mapping):
        return "zero"
    return str(category.get("count_bucket") or "zero")


def _unsupported_category_buckets(categories: Mapping[str, Any]) -> list[str]:
    unsupported: list[str] = []
    for name in ("event_outbox", "job_attempts", "notification_plans", "replay_requests", "dead_letter_entries"):
        category = categories.get(name)
        if not isinstance(category, Mapping) or category.get("state") != "present":
            unsupported.append(f"{name}:not_present_in_current_head")
    return unsupported


def _category_from_queue_rows(
    name: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    max_sample: int,
) -> DurableCategoryInventory:
    status_counts: dict[str, int] = {}
    queue_counts: dict[str, int] = {}
    stage_counts: dict[str, int] = {}
    age_counts: dict[str, int] = {}
    total = 0
    for row in rows:
        count = _safe_int(row["row_count"])
        total += count
        _add(status_counts, _safe_status(row["status"]), count)
        _add(queue_counts, _queue_bucket(row.get("queue_name")), count)
        _add(stage_counts, _safe_stage(row.get("stage_name")), count)
        _add(age_counts, _safe_age_bucket(row.get("age_bucket")), count)
    return DurableCategoryInventory(
        name=name,
        state="present",
        total_count=total,
        status_counts=status_counts,
        queue_counts=queue_counts,
        stage_counts=stage_counts,
        age_counts=age_counts,
        sample_shape_count=min(max_sample, len(rows)),
    )


def _not_present(name: str) -> DurableCategoryInventory:
    return DurableCategoryInventory(
        name=name,
        state="not_present_in_current_head",
        reason_code="not_present_in_current_head",
    )


def _bucketed_counts(values: Mapping[str, int]) -> dict[str, str]:
    return {str(key): _count_bucket(value) for key, value in sorted(values.items())}


def _count_bucket(value: int | None) -> str:
    if value is None:
        return "unknown"
    if value <= 0:
        return "zero"
    if value == 1:
        return "one"
    if value <= 10:
        return "few"
    if value <= 100:
        return "many"
    return "large"


def _presence_bucket(value: bool | None) -> str:
    if value is True:
        return "present"
    if value is False:
        return "missing"
    return "unknown"


def _queue_bucket(value: object) -> str:
    text = str(value or "").strip()
    known = {
        "q.source.normalize": "source_normalize",
        "q.candidate.bundle": "candidate_bundle",
        "q.analysis.route": "analysis_route",
        "q.analysis.judge": "analysis_judge",
        "q.analysis.validate": "analysis_validate",
        "q.analysis.policy": "analysis_policy",
        "q.notification.send": "notification_send",
        "q.replay": "replay",
        "q.maintenance": "maintenance",
        "q.artifact.enrich.github": "artifact_enrich_github",
        "q.artifact.enrich.x": "artifact_enrich_x",
        "q.artifact.enrich.web": "artifact_enrich_web",
    }
    return known.get(text, "custom_or_unknown_queue")


def _event_type_queue_bucket(event_type: object, provider_route: object) -> str:
    text = str(event_type or "").strip()
    if text in {
        "source_message.created.v1",
        "source_message.edited.v1",
        "source_message.deleted.v1",
        "source_message.reconciled.v1",
    }:
        return "source_normalize"
    if text == "artifact.enrich.requested.v1":
        provider = str(provider_route or "").strip()
        if provider in {"github", "x", "web"}:
            return f"artifact_enrich_{provider}"
        return "artifact_enrich_unknown"
    mapping = {
        "candidate.bundle.refresh.v1": "candidate_bundle",
        "artifact.snapshot.updated.v1": "candidate_bundle",
        "analysis.requested.v1": "analysis_route",
        "judge.call.requested.v1": "analysis_judge",
        "judge.output.ready.v1": "analysis_validate",
        "analysis.policy.apply.v1": "analysis_policy",
        "notification.plan.created.v1": "notification_send",
        "replay.requested.v1": "replay",
        "notification.delivery.result.v1": "maintenance",
    }
    return mapping.get(text, "unsupported_or_unknown")


def _safe_status(value: object) -> str:
    text = str(value or "").strip()
    return text if _safe_token(text) else "other"


def _safe_stage(value: object) -> str:
    text = str(value or "").strip()
    return text if _safe_token(text) else "other"


def _safe_age_bucket(value: object) -> str:
    text = str(value or "").strip()
    return text if text in {"fresh", "recent", "same_day", "older"} else "unknown"


def _safe_token(value: str) -> bool:
    if not value or len(value) > 80:
        return False
    return all(ch.isalnum() or ch in {"_", ".", "-"} for ch in value)


def _safe_mode(mode: str) -> str:
    return mode if mode in {"inventory", "plan"} else "inventory"


def _decode_value(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _dict_get(mapping: Mapping[Any, Any], key: str) -> Any:
    return mapping.get(key, mapping.get(key.encode("utf-8")))


def _safe_int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _add(target: dict[str, int], key: str, value: int) -> None:
    target[key] = target.get(key, 0) + value


def _sql(statement: str) -> Any:
    import sqlalchemy as sa

    return sa.text(statement)


__all__ = [
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "DurableCategoryInventory",
    "DurableInventorySnapshot",
    "KnownQueueSpec",
    "RedisInventorySnapshot",
    "RedisQueueInventory",
    "RedisReadOnlyQueueInspector",
    "RedisRebuildReadinessRequest",
    "SqlAlchemyDurableRebuildInventoryRepository",
    "blocked_report",
    "build_redis_rebuild_readiness_report",
    "known_queue_specs",
    "render_sanitized_json",
    "run_redis_rebuild_readiness_from_runtime_env_file",
]
