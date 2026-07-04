from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from ..outbox_relay.models import OutboxEventRow, redis_queued_message_from_outbox_row
from ..outbox_relay.routing import OutboxRouteResolver, UnsupportedOutboxEventTypeError
from .config import MaintenanceConfig
from .redis_rebuild_readiness import (
    DurableReadOnlyInventoryReader,
    KnownQueueSpec,
    RedisInventorySnapshot,
    RedisQueueInventory,
    RedisReadOnlyInventoryReader,
    RedisReadOnlyQueueInspector,
    RedisRebuildReadinessRequest,
    RedisRebuildReadinessError,
    SqlAlchemyDurableRebuildInventoryRepository,
    build_redis_rebuild_readiness_report,
    known_queue_specs,
    load_runtime_config_from_env_file,
)


SCHEMA_VERSION = "redis_rebuild_execution_report_v1"
RUNNER_NAME = "bounded_redis_rebuild_execution_runner"
EXPECTED_BASELINE_HEAD = "64a584ce10000a5c45d1e2ebcb1722497ef88328"
MAX_REBUILD_JOBS = 25
MAX_DUPLICATE_SCAN = 25
THIN_STREAM_FIELDS = {
    "job_id",
    "stage_name",
    "root_object_type",
    "root_object_id",
    "idempotency_key",
    "pipeline_run_id",
    "not_before",
    "trigger_event_id",
}


@dataclass(frozen=True, slots=True)
class RedisRebuildExecutionRequest:
    mode: str
    queue_selector: str | None = None
    max_rebuild_jobs: int | None = None
    expected_head: str | None = None
    understand_mutates_redis: bool = False
    approve_redis_rebuild_execution: bool = False


class RedisRebuildExecutionClient(Protocol):
    async def xinfo_groups(self, name: str) -> Any: ...
    async def xrange(self, name: str, min: str = "-", max: str = "+", count: int | None = None) -> Any: ...
    async def xadd(self, name: str, fields: dict[str, str]) -> Any: ...
    async def xgroup_create(self, name: str, groupname: str, id: str = "0", mkstream: bool = False) -> Any: ...


class DurableRebuildExecutionReader(DurableReadOnlyInventoryReader, Protocol):
    async def load_rebuildable_event_outbox_rows(
        self,
        *,
        queue_name: str,
        limit: int,
    ) -> Sequence[OutboxEventRow]: ...


async def build_redis_rebuild_execution_report(
    request: RedisRebuildExecutionRequest,
    *,
    config: MaintenanceConfig,
    redis_client: RedisRebuildExecutionClient,
    durable_reader: DurableRebuildExecutionReader,
    redis_reader: RedisReadOnlyInventoryReader | None = None,
    route_resolver: OutboxRouteResolver | None = None,
) -> dict[str, Any]:
    queue_error, queue_spec = _select_exact_queue(config, request.queue_selector)
    request_error = queue_error or _request_error(request)
    if request_error is not None:
        return blocked_report(request_error, mode=_safe_mode(request.mode))

    assert queue_spec is not None
    max_rebuild_jobs = request.max_rebuild_jobs or MAX_REBUILD_JOBS
    redis_reader = redis_reader or RedisReadOnlyQueueInspector(redis_client)
    route_resolver = route_resolver or OutboxRouteResolver()

    readiness_report = await build_redis_rebuild_readiness_report(
        RedisRebuildReadinessRequest(
            mode="plan",
            queue_selector=queue_spec.key,
            include_empty=True,
            max_sample=min(max_rebuild_jobs, 10),
        ),
        config=config,
        redis_reader=redis_reader,
        durable_reader=durable_reader,
    )
    preflight_summary = _preflight_readiness_summary(readiness_report, queue_spec)
    if readiness_report.get("status") != "pass":
        return _report(
            request,
            queue_spec=queue_spec,
            status="blocked",
            reason_code="preflight_readiness_failed",
            preflight_readiness_summary=preflight_summary,
            authority=_authority(redis_read_attempted=True, db_read_attempted=True),
        )

    target_before = await _inspect_target_queue(redis_reader, queue_spec)
    present_groups = await _present_group_names(redis_client, queue_spec, target_before)
    missing_groups = tuple(group for group in queue_spec.consumer_groups if group not in present_groups)

    try:
        rows = tuple(
            await durable_reader.load_rebuildable_event_outbox_rows(
                queue_name=queue_spec.stream_name,
                limit=max_rebuild_jobs,
            )
        )
    except Exception:
        return _report(
            request,
            queue_spec=queue_spec,
            status="failed",
            reason_code="durable_rebuild_rows_read_failed",
            preflight_readiness_summary=preflight_summary,
            target_before=target_before,
            authority=_authority(redis_read_attempted=True, db_read_attempted=True),
        )

    candidate_messages = _candidate_messages(rows, queue_spec=queue_spec, route_resolver=route_resolver)
    if candidate_messages.unsupported_count:
        return _report(
            request,
            queue_spec=queue_spec,
            status="blocked",
            reason_code="unsupported_outbox_route",
            preflight_readiness_summary=preflight_summary,
            target_before=target_before,
            eligible_count=len(rows),
            unsupported_count=candidate_messages.unsupported_count,
            authority=_authority(redis_read_attempted=True, db_read_attempted=True),
        )

    if request.mode == "plan":
        reason = "execution_plan_has_work" if candidate_messages.messages or missing_groups else "execution_plan_noop"
        return _report(
            request,
            queue_spec=queue_spec,
            status="pass",
            reason_code=reason,
            preflight_readiness_summary=preflight_summary,
            target_before=target_before,
            eligible_count=len(candidate_messages.messages),
            missing_group_count=len(missing_groups),
            authority=_authority(redis_read_attempted=True, db_read_attempted=True),
        )

    if not candidate_messages.messages and not missing_groups:
        return _report(
            request,
            queue_spec=queue_spec,
            status="blocked",
            reason_code="no_rebuildable_actions",
            preflight_readiness_summary=preflight_summary,
            target_before=target_before,
            authority=_authority(redis_read_attempted=True, db_read_attempted=True),
        )

    if target_before.stream_present is False and not candidate_messages.messages:
        return _report(
            request,
            queue_spec=queue_spec,
            status="blocked",
            reason_code="no_rebuildable_rows_for_missing_stream",
            preflight_readiness_summary=preflight_summary,
            target_before=target_before,
            missing_group_count=len(missing_groups),
            authority=_authority(redis_read_attempted=True, db_read_attempted=True),
        )

    existing_keys_result = await _load_existing_idempotency_keys(redis_client, queue_spec, target_before)
    if existing_keys_result.reason_code is not None:
        return _report(
            request,
            queue_spec=queue_spec,
            status="blocked",
            reason_code=existing_keys_result.reason_code,
            preflight_readiness_summary=preflight_summary,
            target_before=target_before,
            eligible_count=len(candidate_messages.messages),
            missing_group_count=len(missing_groups),
            authority=_authority(redis_read_attempted=True, db_read_attempted=True),
        )

    inserted_count = 0
    duplicate_count = 0
    xadd_attempted = False
    known_idempotency_keys = set(existing_keys_result.keys)
    for message in candidate_messages.messages:
        fields = message.as_stream_fields()
        if set(fields) != THIN_STREAM_FIELDS:
            return _report(
                request,
                queue_spec=queue_spec,
                status="blocked",
                reason_code="thin_stream_fields_invalid",
                preflight_readiness_summary=preflight_summary,
                target_before=target_before,
                eligible_count=len(candidate_messages.messages),
                missing_group_count=len(missing_groups),
                authority=_authority(redis_read_attempted=True, db_read_attempted=True),
            )
        if message.idempotency_key in known_idempotency_keys:
            duplicate_count += 1
            continue
        try:
            xadd_attempted = True
            await redis_client.xadd(queue_spec.stream_name, fields)
        except Exception:
            return _report(
                request,
                queue_spec=queue_spec,
                status="failed",
                reason_code="redis_xadd_failed",
                preflight_readiness_summary=preflight_summary,
                target_before=target_before,
                eligible_count=len(candidate_messages.messages),
                inserted_count=inserted_count,
                duplicate_count=duplicate_count,
                missing_group_count=len(missing_groups),
                authority=_authority(
                    redis_read_attempted=True,
                    redis_xadd_attempted=True,
                    db_read_attempted=True,
                ),
            )
        inserted_count += 1
        known_idempotency_keys.add(message.idempotency_key)

    created_group_count = 0
    xgroup_attempted = False
    if missing_groups and (target_before.stream_present is True or inserted_count > 0):
        for group_name in missing_groups:
            try:
                xgroup_attempted = True
                await redis_client.xgroup_create(queue_spec.stream_name, group_name, id="0", mkstream=False)
                created_group_count += 1
            except Exception as exc:
                if "BUSYGROUP" in str(exc):
                    continue
                return _report(
                    request,
                    queue_spec=queue_spec,
                    status="failed",
                    reason_code="redis_xgroup_create_failed",
                    preflight_readiness_summary=preflight_summary,
                    target_before=target_before,
                    eligible_count=len(candidate_messages.messages),
                    inserted_count=inserted_count,
                    duplicate_count=duplicate_count,
                    missing_group_count=len(missing_groups),
                    created_group_count=created_group_count,
                    authority=_authority(
                        redis_read_attempted=True,
                        redis_xadd_attempted=xadd_attempted,
                        redis_xgroup_create_attempted=True,
                        db_read_attempted=True,
                    ),
                )

    target_after = await _inspect_target_queue(redis_reader, queue_spec)
    reason_code = (
        "redis_rebuild_execution_pass"
        if inserted_count or created_group_count
        else "idempotent_noop"
    )
    return _report(
        request,
        queue_spec=queue_spec,
        status="pass",
        reason_code=reason_code,
        preflight_readiness_summary=preflight_summary,
        target_before=target_before,
        target_after=target_after,
        eligible_count=len(candidate_messages.messages),
        inserted_count=inserted_count,
        duplicate_count=duplicate_count,
        missing_group_count=len(missing_groups),
        created_group_count=created_group_count,
        authority=_authority(
            redis_read_attempted=True,
            redis_xadd_attempted=xadd_attempted,
            redis_xgroup_create_attempted=xgroup_attempted,
            db_read_attempted=True,
        ),
    )


async def run_redis_rebuild_execution_from_runtime_env_file(
    runtime_env_file: str,
    request: RedisRebuildExecutionRequest,
) -> dict[str, Any]:
    path_error = _runtime_env_file_error(runtime_env_file)
    if path_error is not None:
        return blocked_report(path_error, mode=_safe_mode(request.mode))

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
            durable_reader = SqlAlchemyDurableRebuildInventoryRepository(session)
            return await build_redis_rebuild_execution_report(
                request,
                config=config,
                redis_client=redis_client,
                durable_reader=durable_reader,
            )
    except RedisRebuildReadinessError as exc:
        return blocked_report(exc.reason_code, mode=_safe_mode(request.mode))
    except Exception:
        return blocked_report("execution_runtime_error", mode=_safe_mode(request.mode), status="failed")
    finally:
        if redis_client is not None:
            close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
            if close is not None:
                result = close()
                if hasattr(result, "__await__"):
                    await result
        if engine is not None:
            await engine.dispose()


def blocked_report(reason_code: str, *, mode: str = "plan", status: str = "blocked") -> dict[str, Any]:
    return _report(
        RedisRebuildExecutionRequest(mode=_safe_mode(mode)),
        queue_spec=None,
        status=status,
        reason_code=reason_code,
        preflight_readiness_summary=_empty_preflight_summary(),
        authority=_authority(),
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


@dataclass(frozen=True, slots=True)
class _CandidateMessages:
    messages: tuple[Any, ...]
    unsupported_count: int = 0


@dataclass(frozen=True, slots=True)
class _ExistingIdempotencyKeys:
    keys: frozenset[str]
    reason_code: str | None = None


def _select_exact_queue(
    config: MaintenanceConfig,
    queue_selector: str | None,
) -> tuple[str | None, KnownQueueSpec | None]:
    if not queue_selector or not queue_selector.strip():
        return "queue_required", None
    selector = queue_selector.strip()
    matches = tuple(spec for spec in known_queue_specs(config) if selector in {spec.key, spec.stream_name})
    if not matches:
        return "queue_not_known", None
    if len(matches) != 1:
        return "exactly_one_queue_required", None
    return None, matches[0]


def _request_error(request: RedisRebuildExecutionRequest) -> str | None:
    if request.mode not in {"plan", "execute"}:
        return "mode_not_allowed"
    if request.max_rebuild_jobs is not None and (
        request.max_rebuild_jobs <= 0 or request.max_rebuild_jobs > MAX_REBUILD_JOBS
    ):
        return "max_rebuild_jobs_not_allowed"
    if request.mode == "plan":
        return None
    if not request.understand_mutates_redis:
        return "redis_mutation_understanding_required"
    if not request.approve_redis_rebuild_execution:
        return "redis_rebuild_execution_approval_required"
    if request.expected_head is None or not request.expected_head.strip():
        return "expected_head_required"
    if request.expected_head.strip() != EXPECTED_BASELINE_HEAD:
        return "expected_head_mismatch"
    if request.max_rebuild_jobs is None:
        return "max_rebuild_jobs_required"
    return None


async def _inspect_target_queue(
    redis_reader: RedisReadOnlyInventoryReader,
    queue_spec: KnownQueueSpec,
) -> RedisQueueInventory:
    snapshot = await redis_reader.inspect_queues((queue_spec,))
    if isinstance(snapshot, RedisInventorySnapshot) and snapshot.queues:
        return snapshot.queues[0]
    return RedisQueueInventory(
        queue_key=queue_spec.key,
        stream_present=None,
        stream_type_bucket="unknown",
        configured_group_count=len(queue_spec.consumer_groups),
        missing_group_count=len(queue_spec.consumer_groups),
        pending_count=None,
        reason_code="queue_metadata_read_failed",
    )


async def _present_group_names(
    redis_client: RedisRebuildExecutionClient,
    queue_spec: KnownQueueSpec,
    target: RedisQueueInventory,
) -> frozenset[str]:
    if target.stream_present is not True:
        return frozenset()
    try:
        groups = await redis_client.xinfo_groups(queue_spec.stream_name)
    except Exception:
        return frozenset()
    return frozenset(
        _decode_value(_dict_get(group, "name"))
        for group in groups or []
        if isinstance(group, Mapping)
    )


def _candidate_messages(
    rows: Sequence[OutboxEventRow],
    *,
    queue_spec: KnownQueueSpec,
    route_resolver: OutboxRouteResolver,
) -> _CandidateMessages:
    messages: list[Any] = []
    unsupported_count = 0
    for row in rows:
        try:
            route = route_resolver.resolve(row)
        except UnsupportedOutboxEventTypeError:
            unsupported_count += 1
            continue
        if route.queue_name != queue_spec.stream_name:
            unsupported_count += 1
            continue
        messages.append(redis_queued_message_from_outbox_row(row, route))
    return _CandidateMessages(messages=tuple(messages), unsupported_count=unsupported_count)


async def _load_existing_idempotency_keys(
    redis_client: RedisRebuildExecutionClient,
    queue_spec: KnownQueueSpec,
    target: RedisQueueInventory,
) -> _ExistingIdempotencyKeys:
    if target.stream_present is False:
        return _ExistingIdempotencyKeys(keys=frozenset())
    stream_length = target.stream_length
    if stream_length is None:
        return _ExistingIdempotencyKeys(keys=frozenset(), reason_code="idempotency_guard_missing")
    if stream_length <= 0:
        return _ExistingIdempotencyKeys(keys=frozenset())
    if stream_length > MAX_DUPLICATE_SCAN:
        return _ExistingIdempotencyKeys(keys=frozenset(), reason_code="idempotency_guard_missing")
    try:
        entries = await redis_client.xrange(queue_spec.stream_name, min="-", max="+", count=stream_length)
    except Exception:
        return _ExistingIdempotencyKeys(keys=frozenset(), reason_code="idempotency_guard_missing")
    keys: set[str] = set()
    for entry in entries or []:
        if not isinstance(entry, (tuple, list)) or len(entry) != 2:
            continue
        fields = entry[1]
        if not isinstance(fields, Mapping):
            continue
        value = _dict_get(fields, "idempotency_key")
        if value:
            keys.add(_decode_value(value))
    return _ExistingIdempotencyKeys(keys=frozenset(keys))


def _preflight_readiness_summary(
    readiness_report: Mapping[str, Any],
    queue_spec: KnownQueueSpec,
) -> dict[str, Any]:
    redis_inventory = readiness_report.get("redis_inventory") if isinstance(readiness_report, Mapping) else {}
    durable_inventory = readiness_report.get("durable_inventory") if isinstance(readiness_report, Mapping) else {}
    categories = durable_inventory.get("durable_source_categories") if isinstance(durable_inventory, Mapping) else {}
    plan = readiness_report.get("dry_run_rebuild_plan") if isinstance(readiness_report, Mapping) else {}
    return {
        "schema_version": readiness_report.get("schema_version"),
        "status": readiness_report.get("status"),
        "reason_code": readiness_report.get("reason_code"),
        "target_queue_key": queue_spec.key,
        "target_stream_presence_bucket": _mapping_value(redis_inventory, "stream_presence_buckets", queue_spec.key),
        "target_group_presence_buckets": _mapping_value(redis_inventory, "group_presence_buckets", queue_spec.key),
        "durable_event_outbox_count_bucket": _category_bucket(categories, "event_outbox"),
        "planned_action_buckets": plan.get("planned_action_buckets") if isinstance(plan, Mapping) else {},
        "raw_ids_omitted": True,
        "raw_payloads_omitted": True,
    }


def _report(
    request: RedisRebuildExecutionRequest,
    *,
    queue_spec: KnownQueueSpec | None,
    status: str,
    reason_code: str,
    preflight_readiness_summary: Mapping[str, Any],
    authority: Mapping[str, bool],
    target_before: RedisQueueInventory | None = None,
    target_after: RedisQueueInventory | None = None,
    eligible_count: int = 0,
    unsupported_count: int = 0,
    inserted_count: int = 0,
    duplicate_count: int = 0,
    missing_group_count: int = 0,
    created_group_count: int = 0,
) -> dict[str, Any]:
    mode = _safe_mode(request.mode)
    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": mode,
        "status": status,
        "reason_code": reason_code,
        "preflight_readiness_summary": dict(preflight_readiness_summary),
        "approved_target": _approved_target(request, queue_spec),
        "execution_summary": {
            "rebuild_source": "event_outbox",
            "canonical_outbox_route_resolver_reused": True,
            "thin_redis_message_contract_reused": True,
            "eligible_event_outbox_count_bucket": _count_bucket(eligible_count),
            "unsupported_route_count_bucket": _count_bucket(unsupported_count),
            "missing_group_count_bucket": _count_bucket(missing_group_count),
            "created_group_count_bucket": _count_bucket(created_group_count),
            "xadd_inserted_count_bucket": _count_bucket(inserted_count),
            "skipped_duplicate_count_bucket": _count_bucket(duplicate_count),
            "idempotency_guard": "bounded_exact_stream_scan_or_missing_stream",
            "not_executed_categories": [
                "job_attempts:no_current_thin_trigger_rebuild_contract_without_db_mutation",
                "notification_plans:rebuild_via_existing_event_outbox_only",
                "replay_requests:rebuild_via_existing_event_outbox_only",
                "dead_letter_entries:readiness_only_in_current_head",
            ],
            "raw_ids_omitted": True,
            "raw_payloads_omitted": True,
            "raw_dedupe_keys_omitted": True,
        },
        "post_write_readback": _post_write_readback(target_before, target_after, inserted_count, duplicate_count),
        "authority": dict(authority),
        "runtime_authority_opened_in_this_run": _runtime_authority_opened_in_this_run(request, queue_spec),
        "redactions_applied": _redactions_applied(),
        "open_gate_semantics": "global_project_lifecycle_state_after_report_not_invocation_authority_or_attempts",
        "open_gates": _open_gates(),
        "completion_claims": _completion_claims(status, mode, authority),
        "recommended_next_operator_action": _next_action(status, reason_code, mode),
    }


def _approved_target(
    request: RedisRebuildExecutionRequest,
    queue_spec: KnownQueueSpec | None,
) -> dict[str, Any]:
    return {
        "exactly_one_queue_selected": queue_spec is not None,
        "queue_key": queue_spec.key if queue_spec is not None else None,
        "stream_name": queue_spec.stream_name if queue_spec is not None else None,
        "configured_group_count_bucket": _count_bucket(len(queue_spec.consumer_groups)) if queue_spec else "zero",
        "max_rebuild_jobs": request.max_rebuild_jobs,
        "max_rebuild_jobs_within_bound": (
            request.max_rebuild_jobs is not None and 0 < request.max_rebuild_jobs <= MAX_REBUILD_JOBS
        ),
        "expected_head_matches_task_baseline": request.expected_head == EXPECTED_BASELINE_HEAD,
        "runtime_env_path_omitted": True,
    }


def _post_write_readback(
    target_before: RedisQueueInventory | None,
    target_after: RedisQueueInventory | None,
    inserted_count: int,
    duplicate_count: int,
) -> dict[str, Any]:
    target = target_after or target_before
    return {
        "performed": target_after is not None,
        "stream_presence_bucket": _presence_bucket(target.stream_present) if target else "unknown",
        "group_presence_bucket": _count_bucket(target.present_group_count) if target else "unknown",
        "stream_length_count_bucket": _count_bucket(target.stream_length) if target else "unknown",
        "stream_length_delta_bucket": _delta_bucket(target_before, target_after),
        "inserted_count_bucket": _count_bucket(inserted_count),
        "skipped_duplicate_count_bucket": _count_bucket(duplicate_count),
        "raw_stream_ids_omitted": True,
        "raw_message_ids_omitted": True,
    }


def _authority(
    *,
    redis_read_attempted: bool = False,
    redis_xadd_attempted: bool = False,
    redis_xgroup_create_attempted: bool = False,
    db_read_attempted: bool = False,
) -> dict[str, bool]:
    redis_mutation_attempted = redis_xadd_attempted or redis_xgroup_create_attempted
    return {
        "redis_read_attempted": redis_read_attempted,
        "redis_mutation_attempted": redis_mutation_attempted,
        "redis_xadd_attempted": redis_xadd_attempted,
        "redis_xgroup_create_attempted": redis_xgroup_create_attempted,
        "redis_flush_attempted": False,
        "redis_delete_attempted": False,
        "redis_xack_attempted": False,
        "redis_xdel_attempted": False,
        "redis_xreadgroup_attempted": False,
        "redis_xclaim_attempted": False,
        "redis_xautoclaim_attempted": False,
        "redis_publish_subscribe_attempted": False,
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


def _runtime_authority_opened_in_this_run(
    request: RedisRebuildExecutionRequest,
    queue_spec: KnownQueueSpec | None,
) -> dict[str, bool]:
    redis_rebuild_mutation_authority_opened = (
        request.mode == "execute"
        and queue_spec is not None
        and request.understand_mutates_redis
        and request.approve_redis_rebuild_execution
        and request.expected_head == EXPECTED_BASELINE_HEAD
        and request.max_rebuild_jobs is not None
        and 0 < request.max_rebuild_jobs <= MAX_REBUILD_JOBS
    )
    return {
        "redis_xadd_authority_opened": redis_rebuild_mutation_authority_opened,
        "redis_xgroup_create_authority_opened": redis_rebuild_mutation_authority_opened,
        "redis_flush_authority_opened": False,
        "redis_delete_authority_opened": False,
        "redis_consume_ack_authority_opened": False,
        "redis_pending_claim_authority_opened": False,
        "db_write_authority_opened": False,
        "telegram_send_authority_opened": False,
        "production_rollout_authority_opened": False,
    }


def _redactions_applied() -> dict[str, bool]:
    return {
        "runtime_env_path_omitted": True,
        "runtime_env_values_omitted": True,
        "database_url_omitted": True,
        "redis_url_omitted": True,
        "secret_values_omitted": True,
        "raw_stream_ids_omitted": True,
        "raw_message_ids_omitted": True,
        "raw_event_ids_omitted": True,
        "raw_job_ids_omitted": True,
        "raw_object_ids_omitted": True,
        "raw_aggregate_ids_omitted": True,
        "raw_dedupe_keys_omitted": True,
        "raw_payload_json_omitted": True,
        "raw_source_text_omitted": True,
        "raw_urls_omitted": True,
        "raw_exception_bodies_omitted": True,
    }


def _open_gates() -> dict[str, bool]:
    return {
        "PRODUCTION_ROLLOUT_OPEN": True,
        "PRODUCT_COMPLETE_CLOSED": False,
        "ACTUAL_REDIS_FLUSH_OPEN": False,
        "ACTUAL_REDIS_DELETE_OPEN": False,
        "ACTUAL_REDIS_CONSUME_ACK_OPEN": False,
        "ACTUAL_REDIS_PENDING_CLAIM_OPEN": False,
        "ACTUAL_DB_WRITE_OPEN": False,
        "ACTUAL_TELEGRAM_SEND_OPEN": False,
    }


def _completion_claims(status: str, mode: str, authority: Mapping[str, bool]) -> dict[str, bool]:
    return {
        "REDIS_REBUILD_EXECUTION_CAPABILITY_IMPLEMENTED": True,
        "REDIS_REBUILD_EXECUTION_PLAN_PASSED": status == "pass" and mode == "plan",
        "REDIS_REBUILD_EXECUTION_PROOF_PASSED": status == "pass" and mode == "execute",
        "ACTUAL_REDIS_REBUILD_MUTATION_EXECUTED_IN_THIS_RUN": bool(authority.get("redis_mutation_attempted")),
        "REDIS_REBUILD_CLOSED": False,
        "PRODUCT_COMPLETE_CLOSED": False,
        "PRODUCTION_ROLLOUT_CLOSED": False,
        "final_bot_complete": False,
        "one_hundred_percent_complete": False,
        "production_rollout_complete": False,
    }


def _next_action(status: str, reason_code: str, mode: str) -> str:
    if status == "pass" and mode == "plan":
        return "review_plan_then_rerun_execute_with_all_approval_flags_if_needed"
    if status == "pass":
        return "perform_read_only_operator_audit_before_any_downstream_worker_start"
    if reason_code == "idempotency_guard_missing":
        return "stop_and_request_narrower_duplicate_guard_before_xadd"
    return "fix_blocker_then_rerun_bounded_execution"


def _empty_preflight_summary() -> dict[str, Any]:
    return {
        "schema_version": "redis_rebuild_readiness_report_v1",
        "status": "not_run",
        "reason_code": None,
        "target_queue_key": None,
        "target_stream_presence_bucket": "unknown",
        "target_group_presence_buckets": {},
        "durable_event_outbox_count_bucket": "zero",
        "planned_action_buckets": {},
        "raw_ids_omitted": True,
        "raw_payloads_omitted": True,
    }


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


def _safe_mode(mode: str) -> str:
    return mode if mode in {"plan", "execute"} else "plan"


def _mapping_value(mapping: object, key: str, nested_key: str) -> Any:
    if not isinstance(mapping, Mapping):
        return None
    nested = mapping.get(key)
    if not isinstance(nested, Mapping):
        return None
    return nested.get(nested_key)


def _category_bucket(categories: object, name: str) -> str:
    if not isinstance(categories, Mapping):
        return "zero"
    category = categories.get(name)
    if not isinstance(category, Mapping):
        return "zero"
    return str(category.get("count_bucket") or "zero")


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


def _delta_bucket(
    before: RedisQueueInventory | None,
    after: RedisQueueInventory | None,
) -> str:
    if before is None or after is None or before.stream_length is None or after.stream_length is None:
        return "unknown"
    return _count_bucket(max(after.stream_length - before.stream_length, 0))


def _decode_value(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _dict_get(mapping: Mapping[Any, Any], key: str) -> Any:
    return mapping.get(key, mapping.get(key.encode("utf-8")))


__all__ = [
    "EXPECTED_BASELINE_HEAD",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "RedisRebuildExecutionRequest",
    "blocked_report",
    "build_redis_rebuild_execution_report",
    "render_sanitized_json",
    "run_redis_rebuild_execution_from_runtime_env_file",
]
