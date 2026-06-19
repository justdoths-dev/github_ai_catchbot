from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from .delivery_retry import MAINTENANCE_QUEUE_NAME
from .restricted_queue_activation import REPLAY_QUEUE_NAME


SCHEMA_VERSION = "restricted_queue_group_bootstrap_report_v1"
MAINTENANCE_CONSUMER_GROUP = "maintenance"
REPLAY_CONSUMER_GROUP = "maintenance-replay"

_ALLOWED_TARGETS = {
    "maintenance": (MAINTENANCE_QUEUE_NAME, MAINTENANCE_CONSUMER_GROUP),
    "replay": (REPLAY_QUEUE_NAME, REPLAY_CONSUMER_GROUP),
}


@dataclass(frozen=True, slots=True)
class RestrictedQueueGroupBootstrapRequest:
    queue_selector: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    mode: str
    confirm_create_group: bool


@dataclass(frozen=True, slots=True)
class RestrictedQueueGroupBootstrapReport:
    schema_version: str
    queue_selector: str
    queue_name: str
    consumer_group: str
    consumer_name: str
    mode: str
    status: str
    group_exists: bool
    created: bool
    already_exists: bool
    xgroup_create_attempted: bool
    stream_messages_read: bool
    ack_attempted: bool
    db_writes_attempted: bool
    destructive_redis_commands_attempted: bool
    reason_code: str | None = None
    redactions_applied: dict[str, bool] = field(default_factory=dict)


class RestrictedQueueGroupBootstrapConsumerProtocol(Protocol):
    async def ensure_group(self, *, allow_create: bool = True) -> bool: ...


async def run_restricted_queue_group_bootstrap(
    request: RestrictedQueueGroupBootstrapRequest,
    *,
    consumer: RestrictedQueueGroupBootstrapConsumerProtocol,
) -> RestrictedQueueGroupBootstrapReport:
    gate_error = _request_gate_error(request)
    if gate_error is not None:
        return _report(
            request,
            status="blocked",
            group_exists=False,
            created=False,
            already_exists=False,
            xgroup_create_attempted=False,
            reason_code=gate_error,
        )

    try:
        group_exists = await consumer.ensure_group(allow_create=False)
    except Exception:
        return _report(
            request,
            status="blocked",
            group_exists=False,
            created=False,
            already_exists=False,
            xgroup_create_attempted=False,
            reason_code="group_metadata_read_failed",
        )

    if request.mode == "plan":
        return _report(
            request,
            status="pass",
            group_exists=group_exists,
            created=False,
            already_exists=group_exists,
            xgroup_create_attempted=False,
            reason_code=None if group_exists else "consumer_group_missing",
        )

    if request.mode == "proof":
        return _report(
            request,
            status="pass" if group_exists else "blocked",
            group_exists=group_exists,
            created=False,
            already_exists=group_exists,
            xgroup_create_attempted=False,
            reason_code=None if group_exists else "consumer_group_missing",
        )

    if group_exists:
        return _report(
            request,
            status="pass",
            group_exists=True,
            created=False,
            already_exists=True,
            xgroup_create_attempted=False,
            reason_code=None,
        )

    try:
        create_ok = await consumer.ensure_group(allow_create=True)
    except Exception:
        return _report(
            request,
            status="blocked",
            group_exists=False,
            created=False,
            already_exists=False,
            xgroup_create_attempted=True,
            reason_code="consumer_group_create_failed",
        )
    if not create_ok:
        return _report(
            request,
            status="blocked",
            group_exists=False,
            created=False,
            already_exists=False,
            xgroup_create_attempted=True,
            reason_code="consumer_group_create_failed",
        )

    try:
        readback_exists = await consumer.ensure_group(allow_create=False)
    except Exception:
        return _report(
            request,
            status="blocked",
            group_exists=False,
            created=True,
            already_exists=False,
            xgroup_create_attempted=True,
            reason_code="group_metadata_read_failed",
        )

    return _report(
        request,
        status="pass" if readback_exists else "blocked",
        group_exists=readback_exists,
        created=readback_exists,
        already_exists=False,
        xgroup_create_attempted=True,
        reason_code=None if readback_exists else "consumer_group_readback_missing",
    )


def _request_gate_error(request: RestrictedQueueGroupBootstrapRequest) -> str | None:
    expected = _ALLOWED_TARGETS.get(request.queue_selector)
    if expected is None:
        return "queue_selector_not_allowed"
    expected_queue_name, expected_consumer_group = expected
    if request.queue_name != expected_queue_name:
        return "queue_name_not_allowed"
    if request.consumer_group != expected_consumer_group:
        return "consumer_group_not_allowed"
    if not request.consumer_name:
        return "consumer_identity_missing"
    if request.mode not in {"plan", "execute", "proof"}:
        return "mode_not_allowed"
    if request.mode == "execute" and not request.confirm_create_group:
        return "create_group_confirm_missing"
    if request.mode != "execute" and request.confirm_create_group:
        return "create_group_confirm_not_allowed_for_read_only"
    return None


def _report(
    request: RestrictedQueueGroupBootstrapRequest,
    *,
    status: str,
    group_exists: bool,
    created: bool,
    already_exists: bool,
    xgroup_create_attempted: bool,
    reason_code: str | None,
) -> RestrictedQueueGroupBootstrapReport:
    return RestrictedQueueGroupBootstrapReport(
        schema_version=SCHEMA_VERSION,
        queue_selector=request.queue_selector,
        queue_name=request.queue_name,
        consumer_group=request.consumer_group,
        consumer_name=request.consumer_name,
        mode=request.mode,
        status=status,
        group_exists=group_exists,
        created=created,
        already_exists=already_exists,
        xgroup_create_attempted=xgroup_create_attempted,
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
