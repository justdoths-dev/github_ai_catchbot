from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from ..outbox_relay.bounded_delivery_result_outbox_publish import (
    BoundedDeliveryResultOutboxPublishConfig,
    BoundedDeliveryResultOutboxPublishError,
    BoundedDeliveryResultOutboxPublishResult,
    BoundedDeliveryResultPublishRuntimeConfig,
    BoundedDeliveryResultRedisPublisherBuilder,
    BoundedDeliveryResultRepositoryBuilder,
    load_bounded_delivery_result_publish_runtime_config,
    run_bounded_delivery_result_outbox_publish,
)
from ..outbox_relay.eligibility import (
    DELIVERY_RESULT_RECEIPT_CODES,
    EVENT_OUTBOX_ROOT_OBJECT_TYPE,
    MAINTENANCE_DELIVERY_RESULT_STAGE,
    MAINTENANCE_QUEUE_NAME,
)
from .bounded_runtime import (
    MAINTENANCE_RESULT_COMMAND,
    BoundedMaintenanceQueueOnceConfig,
    BoundedMaintenanceResult,
    BoundedMaintenanceRuntimeConfig,
    BoundedMaintenanceQueueRuntimeBuilder,
    load_bounded_maintenance_runtime_config,
    run_bounded_maintenance_queue_once,
)

SCHEMA_VERSION = "restricted_delivery_result_maintenance_drain_proof_v1"
REASON_PASSED = "delivery_result_maintenance_drain_proof_closed"


@dataclass(frozen=True, slots=True)
class RestrictedDeliveryResultMaintenanceDrainProofConfig:
    operator_confirmed: bool
    target_event_id: UUID | None
    allow_database_read: bool
    allow_redis_write: bool
    allow_outbox_status_update: bool
    allow_redis_consume: bool
    allow_redis_ack: bool
    max_lag: int = 1


@dataclass(frozen=True, slots=True)
class DeliveryResultMaintenanceDrainReadback:
    outbox_status: str | None
    outbox_published_at_present: bool
    target_event_id_suffix: str | None
    target_plan_id_suffix: str | None
    maintenance_receipt_present: bool
    maintenance_receipt_code: str | None
    redis_lag: int | None
    redis_pending: int | None


class DeliveryResultDrainReadbackLoader(Protocol):
    async def __call__(self, target_event_id: UUID) -> DeliveryResultMaintenanceDrainReadback: ...


MaintenanceQueueRunner = Callable[..., Awaitable[BoundedMaintenanceResult]]
PublisherRunner = Callable[..., Awaitable[BoundedDeliveryResultOutboxPublishResult]]


async def run_restricted_delivery_result_maintenance_drain_proof(
    config: RestrictedDeliveryResultMaintenanceDrainProofConfig,
    *,
    publisher_runtime_config_loader: Callable[[], BoundedDeliveryResultPublishRuntimeConfig] = (
        load_bounded_delivery_result_publish_runtime_config
    ),
    maintenance_runtime_config_loader: Callable[[], BoundedMaintenanceRuntimeConfig] = (
        load_bounded_maintenance_runtime_config
    ),
    publisher_repository_builder: BoundedDeliveryResultRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedDeliveryResultRedisPublisherBuilder | None = None,
    maintenance_runtime_builder: BoundedMaintenanceQueueRuntimeBuilder | None = None,
    publisher_runner: PublisherRunner = run_bounded_delivery_result_outbox_publish,
    maintenance_runner: MaintenanceQueueRunner = run_bounded_maintenance_queue_once,
    readback_loader: DeliveryResultDrainReadbackLoader | None = None,
) -> dict[str, Any]:
    gate_error = _gate_error(config)
    if gate_error is not None:
        return _report(status="blocked", reason_code=gate_error, config=config)

    assert config.target_event_id is not None
    publisher_result = await publisher_runner(
        BoundedDeliveryResultOutboxPublishConfig(
            operator_approved=True,
            target_event_id=config.target_event_id,
            allow_database_read=config.allow_database_read,
            allow_redis_write=config.allow_redis_write,
            allow_outbox_status_update=config.allow_outbox_status_update,
        ),
        runtime_config_loader=publisher_runtime_config_loader,
        repository_builder=publisher_repository_builder,
        redis_publisher_builder=redis_publisher_builder,
    )
    if not publisher_result.ok:
        return _report(
            status=_status_from_child(publisher_result.status),
            reason_code=publisher_result.error_code or "publisher_not_passed",
            config=config,
            publisher_result=publisher_result,
        )

    selector_error = _publisher_selector_error(publisher_result)
    if selector_error is not None:
        return _report(
            status="failed",
            reason_code=selector_error,
            config=config,
            publisher_result=publisher_result,
        )

    preview_config = _worker_config(config, publisher_result, mode="preview")
    preview_result = await maintenance_runner(
        preview_config,
        runtime_config_loader=maintenance_runtime_config_loader,
        runtime_builder=maintenance_runtime_builder,
    )
    if not preview_result.ok:
        return _report(
            status=_status_from_child(preview_result.status),
            reason_code=preview_result.error_code or "maintenance_preview_failed",
            config=config,
            publisher_result=publisher_result,
            preview_result=preview_result,
        )

    lag = preview_result.redis_selection.group_lag if preview_result.redis_selection else None
    if lag is None:
        return _report(
            status="blocked",
            reason_code="redis_lag_unavailable",
            config=config,
            publisher_result=publisher_result,
            preview_result=preview_result,
        )
    if lag > config.max_lag:
        return _report(
            status="blocked",
            reason_code="redis_lag_exceeds_max",
            config=config,
            publisher_result=publisher_result,
            preview_result=preview_result,
        )

    pending = preview_result.redis_selection.group_pending if preview_result.redis_selection else None
    if pending is None:
        return _report(
            status="blocked",
            reason_code="redis_pending_unavailable",
            config=config,
            publisher_result=publisher_result,
            preview_result=preview_result,
        )
    if pending != 0:
        return _report(
            status="blocked",
            reason_code="redis_pending_not_zero",
            config=config,
            publisher_result=publisher_result,
            preview_result=preview_result,
        )

    execute_result = await maintenance_runner(
        _worker_config(config, publisher_result, mode="execute"),
        runtime_config_loader=maintenance_runtime_config_loader,
        runtime_builder=maintenance_runtime_builder,
    )
    if not execute_result.ok:
        return _report(
            status=_status_from_child(execute_result.status),
            reason_code=execute_result.error_code or "maintenance_execute_failed",
            config=config,
            publisher_result=publisher_result,
            preview_result=preview_result,
            execute_result=execute_result,
        )

    try:
        readback = await (readback_loader or load_delivery_result_maintenance_drain_readback)(config.target_event_id)
    except Exception:
        return _report(
            status="failed",
            reason_code="durable_readback_failed",
            config=config,
            publisher_result=publisher_result,
            preview_result=preview_result,
            execute_result=execute_result,
        )

    readback_error = _readback_error(readback)
    if readback_error is not None:
        return _report(
            status="failed",
            reason_code=readback_error,
            config=config,
            publisher_result=publisher_result,
            preview_result=preview_result,
            execute_result=execute_result,
            readback=readback,
        )

    return _report(
        status="pass",
        reason_code=REASON_PASSED,
        config=config,
        publisher_result=publisher_result,
        preview_result=preview_result,
        execute_result=execute_result,
        readback=readback,
    )


async def load_delivery_result_maintenance_drain_readback(
    target_event_id: UUID,
) -> DeliveryResultMaintenanceDrainReadback:
    from redis.asyncio import Redis  # type: ignore[import-not-found]
    from sqlalchemy import text  # type: ignore[import-not-found]
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    maintenance_runtime = load_bounded_maintenance_runtime_config()
    cfg = maintenance_runtime.maintenance_config
    engine = create_async_engine(cfg.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    event_status = None
    published_at_present = False
    plan_suffix = None
    receipt_present = False
    receipt_code = None
    try:
        async with session_factory() as session:
            event_result = await session.execute(
                text(
                    """
                    SELECT status::text AS status, published_at, aggregate_id
                    FROM event_outbox
                    WHERE event_id = CAST(:event_id AS uuid)
                    """
                ),
                {"event_id": str(target_event_id)},
            )
            event_row = event_result.mappings().first()
            if event_row is not None:
                event_status = str(event_row["status"])
                published_at_present = event_row["published_at"] is not None
                plan_suffix = _suffix(event_row["aggregate_id"])

            receipt_result = await session.execute(
                text(
                    """
                    SELECT error_code
                    FROM job_attempts
                    WHERE stage_name = :stage_name
                      AND queue_name = :queue_name
                      AND root_object_type = :root_object_type
                      AND root_object_id = CAST(:event_id AS uuid)
                      AND attempt_status = 'succeeded'::job_attempt_status_enum
                      AND error_code = ANY(CAST(:receipt_codes AS text[]))
                    ORDER BY finished_at DESC NULLS LAST, created_at DESC NULLS LAST, job_attempt_id DESC
                    LIMIT 1
                    """
                ),
                {
                    "stage_name": MAINTENANCE_DELIVERY_RESULT_STAGE,
                    "queue_name": MAINTENANCE_QUEUE_NAME,
                    "root_object_type": EVENT_OUTBOX_ROOT_OBJECT_TYPE,
                    "event_id": str(target_event_id),
                    "receipt_codes": sorted(DELIVERY_RESULT_RECEIPT_CODES),
                },
            )
            receipt_code = receipt_result.scalar_one_or_none()
            receipt_present = receipt_code is not None
    finally:
        await engine.dispose()

    redis_client = Redis.from_url(cfg.redis_url, decode_responses=True)
    lag = None
    pending = None
    try:
        groups = await redis_client.xinfo_groups(cfg.maintenance_queue_name)
        for group in groups or []:
            if isinstance(group, dict) and _decode(group.get("name") or group.get(b"name")) == cfg.maintenance_consumer_group:
                lag = _safe_int(group.get("lag") or group.get(b"lag"))
                pending = _safe_int(group.get("pending") or group.get(b"pending"))
                break
    finally:
        close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result

    return DeliveryResultMaintenanceDrainReadback(
        outbox_status=event_status,
        outbox_published_at_present=published_at_present,
        target_event_id_suffix=_suffix(target_event_id),
        target_plan_id_suffix=plan_suffix,
        maintenance_receipt_present=receipt_present,
        maintenance_receipt_code=str(receipt_code) if receipt_code is not None else None,
        redis_lag=lag,
        redis_pending=pending,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="restricted-delivery-result-maintenance-drain-proof")
    parser.add_argument("--operator-confirmed", action="store_true")
    parser.add_argument("--target-event-id", required=True)
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-redis-write", action="store_true")
    parser.add_argument("--allow-outbox-status-update", action="store_true")
    parser.add_argument("--allow-redis-consume", action="store_true")
    parser.add_argument("--allow-redis-ack", action="store_true")
    parser.add_argument("--max-lag", type=int, default=1)
    parser.add_argument("--format", choices=["json"], default="json")
    return parser


async def _run(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        target_event_id = UUID(str(args.target_event_id))
    except (TypeError, ValueError, AttributeError):
        report = argument_error_report("invalid_target_event_id")
        print(_to_json(report), end="")
        return 2

    report = await run_restricted_delivery_result_maintenance_drain_proof(
        RestrictedDeliveryResultMaintenanceDrainProofConfig(
            operator_confirmed=bool(args.operator_confirmed),
            target_event_id=target_event_id,
            allow_database_read=bool(args.allow_database_read),
            allow_redis_write=bool(args.allow_redis_write),
            allow_outbox_status_update=bool(args.allow_outbox_status_update),
            allow_redis_consume=bool(args.allow_redis_consume),
            allow_redis_ack=bool(args.allow_redis_ack),
            max_lag=int(args.max_lag),
        )
    )
    print(_to_json(report), end="")
    if report["status"] == "pass":
        return 0
    if report["status"] == "blocked":
        return 2
    return 1


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


def argument_error_report(reason_code: str) -> dict[str, Any]:
    return _report(
        status="blocked",
        reason_code=reason_code,
        config=RestrictedDeliveryResultMaintenanceDrainProofConfig(
            operator_confirmed=False,
            target_event_id=None,
            allow_database_read=False,
            allow_redis_write=False,
            allow_outbox_status_update=False,
            allow_redis_consume=False,
            allow_redis_ack=False,
        ),
    )


def _gate_error(config: RestrictedDeliveryResultMaintenanceDrainProofConfig) -> str | None:
    if not config.operator_confirmed:
        return "operator_confirmation_required"
    if config.target_event_id is None:
        return "target_event_id_missing"
    if config.max_lag < 0:
        return "max_lag_invalid"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if not config.allow_redis_write:
        return "redis_write_not_allowed"
    if not config.allow_outbox_status_update:
        return "outbox_status_update_not_allowed"
    if not config.allow_redis_consume:
        return "redis_consume_not_allowed"
    if not config.allow_redis_ack:
        return "redis_ack_not_allowed"
    return None


def _publisher_selector_error(result: BoundedDeliveryResultOutboxPublishResult) -> str | None:
    if result.selected_event_id_suffix is None:
        return "published_event_suffix_missing"
    if result.selected_aggregate_id_suffix is None:
        return "published_plan_suffix_missing"
    if result.redis_message_id_suffix is None:
        return "published_redis_message_suffix_missing"
    if result.queue_name != MAINTENANCE_QUEUE_NAME or result.stage_name != "maintenance":
        return "published_route_not_maintenance"
    return None


def _worker_config(
    config: RestrictedDeliveryResultMaintenanceDrainProofConfig,
    publisher_result: BoundedDeliveryResultOutboxPublishResult,
    *,
    mode: str,
) -> BoundedMaintenanceQueueOnceConfig:
    return BoundedMaintenanceQueueOnceConfig(
        command=MAINTENANCE_RESULT_COMMAND,
        operator_approved=True,
        allow_runtime_config=True,
        allow_database_read=mode == "execute",
        allow_database_write=mode == "execute",
        allow_redis_read=True,
        allow_redis_consume=mode == "execute" and config.allow_redis_consume,
        allow_redis_ack=mode == "execute" and config.allow_redis_ack,
        mode=mode,
        trigger_event_suffix=publisher_result.selected_event_id_suffix,
        root_object_id_suffix=publisher_result.selected_aggregate_id_suffix,
        redis_message_id_suffix=publisher_result.redis_message_id_suffix,
    )


def _readback_error(readback: DeliveryResultMaintenanceDrainReadback) -> str | None:
    if readback.outbox_status != "published":
        return "outbox_not_published"
    if not readback.outbox_published_at_present:
        return "outbox_published_at_missing"
    if readback.redis_pending != 0:
        return "redis_pending_not_drained"
    if readback.redis_lag != 0:
        return "redis_lag_not_drained"
    if not readback.maintenance_receipt_present:
        return "maintenance_receipt_missing"
    return None


def _report(
    *,
    status: str,
    reason_code: str,
    config: RestrictedDeliveryResultMaintenanceDrainProofConfig,
    publisher_result: BoundedDeliveryResultOutboxPublishResult | None = None,
    preview_result: BoundedMaintenanceResult | None = None,
    execute_result: BoundedMaintenanceResult | None = None,
    readback: DeliveryResultMaintenanceDrainReadback | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "reason_code": reason_code,
        "target": {
            "event_id_suffix": _suffix(config.target_event_id)
            or _publisher_event_suffix(publisher_result)
            or (readback.target_event_id_suffix if readback else None),
            "plan_id_suffix": _publisher_plan_suffix(publisher_result)
            or (readback.target_plan_id_suffix if readback else None),
            "delivery_record_id_suffix": (
                publisher_result.payload_notification_delivery_record_id_suffix if publisher_result else None
            ),
        },
        "publisher": _publisher_report(publisher_result),
        "redis_precheck": _worker_report(preview_result),
        "worker_once": _worker_report(execute_result),
        "readback": _readback_report(readback),
        "authority": {
            "operator_confirmed": config.operator_confirmed,
            "database_read_allowed": config.allow_database_read,
            "redis_write_allowed": config.allow_redis_write,
            "outbox_status_update_allowed": config.allow_outbox_status_update,
            "redis_consume_allowed": config.allow_redis_consume,
            "redis_ack_allowed": config.allow_redis_ack,
            "max_lag": config.max_lag,
            "telegram_transport_attempted": False,
            "openai_called": False,
            "github_called": False,
            "x_called": False,
            "web_called": False,
            "docker_or_systemd_called": False,
            "alembic_or_ddl_ran": False,
            "q_notification_send_consumed": False,
            "raw_payload_printed": False,
            "raw_ids_printed": False,
            "runtime_values_printed": False,
        },
        "redactions_applied": [
            "full_event_id_omitted",
            "full_notification_plan_id_omitted",
            "full_notification_delivery_record_id_omitted",
            "full_redis_message_id_omitted",
            "payload_json_omitted",
            "telegram_response_json_omitted",
            "telegram_ids_omitted",
            "database_url_omitted",
            "redis_url_omitted",
            "exception_detail_omitted",
        ],
    }


def _publisher_report(result: BoundedDeliveryResultOutboxPublishResult | None) -> dict[str, Any]:
    if result is None:
        return {
            "status": None,
            "ok": False,
            "error_code": None,
            "queue_name": None,
            "stage_name": None,
            "redis_xadd_count": 0,
            "event_outbox_marked_published": False,
            "job_attempt_inserted": False,
            "redis_message_id_suffix": None,
        }
    return {
        "status": result.status,
        "ok": result.ok,
        "error_code": result.error_code,
        "queue_name": result.queue_name,
        "stage_name": result.stage_name,
        "redis_xadd_count": result.redis_xadd_count,
        "event_outbox_marked_published": result.event_outbox_marked_published,
        "job_attempt_inserted": result.job_attempt_inserted,
        "redis_message_id_suffix": result.redis_message_id_suffix,
    }


def _worker_report(result: BoundedMaintenanceResult | None) -> dict[str, Any]:
    if result is None:
        return {
            "status": None,
            "ok": False,
            "error_code": None,
            "pending": None,
            "lag": None,
            "processed": False,
            "acked": False,
            "handler_called": False,
            "reason_code": None,
        }
    selection = result.redis_selection
    service = result.service_result
    return {
        "status": result.status,
        "ok": result.ok,
        "error_code": result.error_code,
        "pending": selection.group_pending if selection else None,
        "lag": selection.group_lag if selection else None,
        "processed": bool(service and service.processed),
        "acked": result.acked,
        "handler_called": result.state.service_called,
        "reason_code": service.reason_code if service else result.error_code,
    }


def _readback_report(readback: DeliveryResultMaintenanceDrainReadback | None) -> dict[str, Any]:
    if readback is None:
        return {
            "outbox_status": None,
            "outbox_published_at_present": False,
            "redis_pending": None,
            "redis_lag": None,
            "maintenance_receipt_present": False,
            "maintenance_receipt_code": None,
        }
    return {
        "outbox_status": readback.outbox_status,
        "outbox_published_at_present": readback.outbox_published_at_present,
        "redis_pending": readback.redis_pending,
        "redis_lag": readback.redis_lag,
        "maintenance_receipt_present": readback.maintenance_receipt_present,
        "maintenance_receipt_code": readback.maintenance_receipt_code,
    }


def _status_from_child(status: str) -> str:
    return "blocked" if status == "blocked" else "failed"


def _publisher_event_suffix(result: BoundedDeliveryResultOutboxPublishResult | None) -> str | None:
    return result.selected_event_id_suffix if result is not None else None


def _publisher_plan_suffix(result: BoundedDeliveryResultOutboxPublishResult | None) -> str | None:
    return result.selected_aggregate_id_suffix if result is not None else None


def _suffix(value: object) -> str | None:
    if value is None:
        return None
    try:
        return str(value).replace("-", "")[-8:]
    except Exception:
        return None


def _safe_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _decode(value: object) -> str:
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)


def _to_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    main()
