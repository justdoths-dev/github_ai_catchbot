from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from ..notifier_telegram.main import (
    _valid_restricted_live_proof_key,
    create_restricted_live_queue_chain_proof_target_with_repository,
)
from ..notifier_telegram.repositories import NotifierTelegramRepository
from ..outbox_relay.bounded_notification_plan_publish import (
    BoundedNotificationPlanOutboxPublishConfig,
    BoundedNotificationPlanOutboxPublishError,
    BoundedNotificationPlanOutboxPublishResult,
    BoundedNotificationPlanPublishRuntimeConfig,
    BoundedNotificationPlanRedisPublisherBuilder,
    BoundedNotificationPlanRepositoryBuilder,
    load_bounded_notification_plan_publish_runtime_config,
    run_bounded_notification_plan_outbox_publish,
)

SCHEMA_VERSION = "restricted_live_notification_queue_chain_proof_runner_v1"
REASON_PASSED = "restricted_live_notification_queue_chain_target_queued"


@dataclass(frozen=True, slots=True)
class RestrictedLiveNotificationQueueChainProofConfig:
    operator_confirmed: bool
    source_notification_plan_id: UUID
    proof_key: str
    allow_database_write: bool
    allow_redis_write: bool
    allow_outbox_status_update: bool
    expected_target_pending_count: int = 1


SessionFactoryBuilder = Callable[
    [str],
    tuple[Any, Callable[[], Awaitable[None]]],
]


async def run_restricted_live_notification_queue_chain_proof(
    config: RestrictedLiveNotificationQueueChainProofConfig,
    *,
    runtime_config_loader: Callable[[], BoundedNotificationPlanPublishRuntimeConfig] = (
        load_bounded_notification_plan_publish_runtime_config
    ),
    session_factory_builder: SessionFactoryBuilder | None = None,
    proof_repository_builder: Callable[[Any], Any] = NotifierTelegramRepository,
    publisher_repository_builder: BoundedNotificationPlanRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedNotificationPlanRedisPublisherBuilder | None = None,
    publisher_runner: Callable[..., Awaitable[BoundedNotificationPlanOutboxPublishResult]] = (
        run_bounded_notification_plan_outbox_publish
    ),
) -> dict[str, Any]:
    if not config.operator_confirmed:
        return _report(
            status="blocked",
            reason_code="operator_confirmation_required",
            config=config,
        )
    if not config.allow_database_write:
        return _report(
            status="blocked",
            reason_code="database_write_not_allowed",
            config=config,
        )
    if not config.allow_redis_write:
        return _report(
            status="blocked",
            reason_code="redis_write_not_allowed",
            config=config,
        )
    if not config.allow_outbox_status_update:
        return _report(
            status="blocked",
            reason_code="outbox_status_update_not_allowed",
            config=config,
        )
    if config.expected_target_pending_count != 1:
        return _report(
            status="blocked",
            reason_code="expected_target_pending_count_invalid",
            config=config,
        )

    try:
        runtime_config = runtime_config_loader()
    except BoundedNotificationPlanOutboxPublishError as exc:
        return _report(status="blocked", reason_code=exc.error_code, config=config)
    except Exception:
        return _report(status="failed", reason_code="runtime_config_error", config=config)

    session_factory = None
    dispose_session_factory: Callable[[], Awaitable[None]] | None = None
    target_result = None
    try:
        builder = session_factory_builder or _build_default_session_factory
        session_factory, dispose_session_factory = builder(runtime_config.database_url)
        async with session_factory.begin() as session:
            repository = proof_repository_builder(session)
            target_result = await create_restricted_live_queue_chain_proof_target_with_repository(
                config.source_notification_plan_id,
                config.proof_key,
                repository,
            )
    except Exception:
        return _report(
            status="failed",
            reason_code="proof_target_creation_failed",
            config=config,
            db_read_attempted=True,
            db_write_attempted=config.allow_database_write,
        )
    finally:
        if dispose_session_factory is not None:
            try:
                await dispose_session_factory()
            except Exception:
                pass

    if target_result.reason_code is not None:
        return _report(
            status="blocked",
            reason_code=target_result.reason_code,
            config=config,
            target_result=target_result,
            db_read_attempted=True,
            db_write_attempted=config.allow_database_write,
        )
    if target_result.status == "existing_already_published":
        return _report(
            status="blocked",
            reason_code="existing_already_published",
            config=config,
            target_result=target_result,
            db_read_attempted=True,
            db_write_attempted=False,
        )
    if target_result.trigger_event_id is None:
        return _report(
            status="failed",
            reason_code="target_event_missing",
            config=config,
            target_result=target_result,
            db_read_attempted=True,
            db_write_attempted=config.allow_database_write,
        )

    publisher_result = await publisher_runner(
        BoundedNotificationPlanOutboxPublishConfig(
            operator_approved=True,
            allow_database_read=True,
            allow_redis_write=config.allow_redis_write,
            allow_outbox_status_update=config.allow_outbox_status_update,
            expected_pending_count=config.expected_target_pending_count,
            target_event_id=target_result.trigger_event_id,
        ),
        runtime_config_loader=lambda: runtime_config,
        repository_builder=publisher_repository_builder,
        redis_publisher_builder=redis_publisher_builder,
    )
    outbox_status_after_publish = "published" if publisher_result.event_outbox_marked_published else None
    readback_status = await _load_event_outbox_status(
        runtime_config.database_url,
        target_result.trigger_event_id,
        session_factory_builder=session_factory_builder,
        repository_builder=proof_repository_builder,
    )
    if readback_status is not None:
        outbox_status_after_publish = readback_status
    if publisher_result.status == "pass" and publisher_result.error_code is None:
        status = "pass"
        reason_code = REASON_PASSED
    elif publisher_result.status == "blocked":
        status = "blocked"
        reason_code = publisher_result.error_code or "publisher_blocked"
    else:
        status = "failed"
        reason_code = publisher_result.error_code or "publisher_failed"

    return _report(
        status=status,
        reason_code=reason_code,
        config=config,
        target_result=target_result,
        publisher_result=publisher_result,
        outbox_status_after_publish=outbox_status_after_publish,
        db_read_attempted=True,
        db_write_attempted=target_result.created or publisher_result.event_outbox_marked_published,
        redis_write_attempted=publisher_result.state.redis_xadd_attempted,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="restricted-live-notification-queue-chain-proof")
    parser.add_argument("--operator-confirmed", action="store_true")
    parser.add_argument("--source-notification-plan-id", required=True)
    parser.add_argument("--proof-key", required=True)
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-redis-write", action="store_true")
    parser.add_argument("--allow-outbox-status-update", action="store_true")
    parser.add_argument("--expected-target-pending-count", type=int, default=1)
    parser.add_argument("--format", choices=["json"], default="json")
    return parser


async def _run(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        source_notification_plan_id = UUID(str(args.source_notification_plan_id))
    except (TypeError, ValueError, AttributeError):
        report = _argument_error_report("invalid_source_notification_plan_id")
        print(_to_json(report), end="")
        return 2
    proof_key = str(args.proof_key or "")
    if not _valid_restricted_live_proof_key(proof_key):
        report = _argument_error_report("invalid_proof_key")
        print(_to_json(report), end="")
        return 2

    report = await run_restricted_live_notification_queue_chain_proof(
        RestrictedLiveNotificationQueueChainProofConfig(
            operator_confirmed=bool(args.operator_confirmed),
            source_notification_plan_id=source_notification_plan_id,
            proof_key=proof_key,
            allow_database_write=bool(args.allow_database_write),
            allow_redis_write=bool(args.allow_redis_write),
            allow_outbox_status_update=bool(args.allow_outbox_status_update),
            expected_target_pending_count=int(args.expected_target_pending_count),
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


def _report(
    *,
    status: str,
    reason_code: str,
    config: RestrictedLiveNotificationQueueChainProofConfig,
    target_result: Any | None = None,
    publisher_result: BoundedNotificationPlanOutboxPublishResult | None = None,
    outbox_status_after_publish: str | None = None,
    db_read_attempted: bool = False,
    db_write_attempted: bool = False,
    redis_write_attempted: bool = False,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "reason_code": reason_code,
        "source": _source_report(config, target_result),
        "target": _target_report(target_result, outbox_status_after_publish=outbox_status_after_publish),
        "publisher": _publisher_report(publisher_result),
        "authority": _authority_report(
            db_read_attempted=db_read_attempted,
            db_write_attempted=db_write_attempted,
            redis_write_attempted=redis_write_attempted,
        ),
        "redactions_applied": [
            "full_source_notification_plan_id_omitted",
            "full_notification_plan_id_omitted",
            "full_event_id_omitted",
            "payload_json_omitted",
            "database_url_omitted",
            "redis_url_omitted",
            "redis_message_id_omitted",
            "telegram_token_omitted",
            "exception_detail_omitted",
        ],
    }


def _argument_error_report(reason_code: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "blocked",
        "reason_code": reason_code,
        "source": {
            "source_notification_plan_id_suffix": None,
            "source_analysis_id_suffix": None,
            "source_candidate_group_id_suffix": None,
        },
        "target": _target_report(None, outbox_status_after_publish=None),
        "publisher": _publisher_report(None),
        "authority": _authority_report(
            db_read_attempted=False,
            db_write_attempted=False,
            redis_write_attempted=False,
        ),
        "redactions_applied": [
            "full_source_notification_plan_id_omitted",
            "full_notification_plan_id_omitted",
            "full_event_id_omitted",
            "payload_json_omitted",
            "database_url_omitted",
            "redis_url_omitted",
            "redis_message_id_omitted",
            "telegram_token_omitted",
            "exception_detail_omitted",
        ],
    }


def _source_report(
    config: RestrictedLiveNotificationQueueChainProofConfig,
    target_result: Any | None,
) -> dict[str, Any]:
    return {
        "source_notification_plan_id_suffix": _suffix(config.source_notification_plan_id),
        "source_analysis_id_suffix": _suffix(getattr(target_result, "source_analysis_id", None)),
        "source_candidate_group_id_suffix": _suffix(getattr(target_result, "source_candidate_group_id", None)),
    }


def _target_report(
    target_result: Any | None,
    *,
    outbox_status_after_publish: str | None,
) -> dict[str, Any]:
    return {
        "notification_plan_id_suffix": _suffix(getattr(target_result, "notification_plan_id", None)),
        "event_id_suffix": _suffix(getattr(target_result, "trigger_event_id", None)),
        "created": bool(getattr(target_result, "created", False)),
        "existing": bool(getattr(target_result, "existing", False)),
        "plan_status": getattr(target_result, "plan_status", None),
        "delivery_decision": getattr(target_result, "delivery_decision", None),
        "urgency_profile": getattr(target_result, "urgency_profile", None),
        "outbox_status_before_publish": getattr(target_result, "outbox_status_before_publish", None),
        "outbox_status_after_publish": outbox_status_after_publish,
    }


def _publisher_report(publisher_result: BoundedNotificationPlanOutboxPublishResult | None) -> dict[str, Any]:
    if publisher_result is None:
        return {
            "selected_event_present": False,
            "selected_event_id_suffix": None,
            "selected_aggregate_type": None,
            "selected_aggregate_id_suffix": None,
            "redis_xadd_count": 0,
            "redis_message_id_present": False,
            "event_outbox_marked_published": False,
            "job_attempt_inserted": False,
        }
    return {
        "selected_event_present": publisher_result.selected_event_present,
        "selected_event_id_suffix": publisher_result.selected_event_id_suffix,
        "selected_aggregate_type": publisher_result.selected_aggregate_type,
        "selected_aggregate_id_suffix": publisher_result.selected_aggregate_id_suffix,
        "redis_xadd_count": publisher_result.redis_xadd_count,
        "redis_message_id_present": publisher_result.redis_message_id_present,
        "event_outbox_marked_published": publisher_result.event_outbox_marked_published,
        "job_attempt_inserted": publisher_result.job_attempt_inserted,
    }


def _authority_report(
    *,
    db_read_attempted: bool,
    db_write_attempted: bool,
    redis_write_attempted: bool,
) -> dict[str, bool]:
    return {
        "db_read_attempted": db_read_attempted,
        "db_write_attempted": db_write_attempted,
        "redis_write_attempted": redis_write_attempted,
        "redis_consume_attempted": False,
        "redis_ack_attempted": False,
        "telegram_transport_attempted": False,
        "openai_called": False,
        "github_called": False,
        "x_called": False,
        "web_called": False,
        "workers_started": False,
        "run_forever_started": False,
        "docker_or_systemd_called": False,
        "alembic_or_ddl_ran": False,
        "runtime_values_printed": False,
        "runtime_paths_printed": False,
        "raw_ids_printed": False,
        "raw_payload_printed": False,
    }


def _suffix(value: object) -> str | None:
    if value is None:
        return None
    try:
        return str(value)[-8:]
    except Exception:
        return None


def _to_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _build_default_session_factory(database_url: str) -> tuple[Any, Callable[[], Awaitable[None]]]:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def dispose() -> None:
        await engine.dispose()

    return session_factory, dispose


async def _load_event_outbox_status(
    database_url: str,
    event_id: UUID,
    *,
    session_factory_builder: SessionFactoryBuilder | None,
    repository_builder: Callable[[Any], Any],
) -> str | None:
    session_factory = None
    dispose: Callable[[], Awaitable[None]] | None = None
    try:
        builder = session_factory_builder or _build_default_session_factory
        session_factory, dispose = builder(database_url)
        async with session_factory.begin() as session:
            repository = repository_builder(session)
            row = await repository.load_event_outbox(event_id)
        if not isinstance(row, Mapping):
            return None
        status = row.get("status")
        return str(status) if status is not None else None
    except Exception:
        return None
    finally:
        if dispose is not None:
            try:
                await dispose()
            except Exception:
                pass


if __name__ == "__main__":
    main()
