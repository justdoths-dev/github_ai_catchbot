from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.outbox_relay.models import OutboxEventRow
from services.outbox_relay.routing import OutboxRouteResolver, UnsupportedOutboxEventTypeError


def _row(event_type: str, *, payload_json: dict | None = None) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=uuid4(),
        event_type=event_type,
        aggregate_type="source_message",
        aggregate_id=uuid4(),
        dedupe_key=f"dedupe:{event_type}",
        payload_json=payload_json or {},
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


@pytest.mark.parametrize(
    ("event_type", "queue_name", "stage_name"),
    [
        ("source_message.created.v1", "q.source.normalize", "normalize"),
        ("source_message.edited.v1", "q.source.normalize", "normalize"),
        ("source_message.deleted.v1", "q.source.normalize", "normalize"),
        ("source_message.reconciled.v1", "q.source.normalize", "normalize"),
        ("candidate.bundle.refresh.v1", "q.candidate.bundle", "bundle"),
        ("artifact.snapshot.updated.v1", "q.candidate.bundle", "bundle"),
        ("analysis.requested.v1", "q.analysis.route", "analysis_route"),
        ("judge.call.requested.v1", "q.analysis.judge", "judge"),
        ("judge.output.ready.v1", "q.analysis.validate", "analysis_validate"),
        ("analysis.policy.apply.v1", "q.analysis.policy", "analysis_policy"),
        ("notification.plan.created.v1", "q.notification.send", "notify"),
        ("replay.requested.v1", "q.replay", "replay"),
        ("notification.delivery.result.v1", "q.maintenance", "maintenance"),
    ],
)
def test_resolve_known_routes(event_type: str, queue_name: str, stage_name: str) -> None:
    resolver = OutboxRouteResolver()

    route = resolver.resolve(_row(event_type))

    assert route.queue_name == queue_name
    assert route.stage_name == stage_name


@pytest.mark.parametrize(
    ("provider_route", "queue_name", "stage_name"),
    [
        ("github", "q.artifact.enrich.github", "enrich_github"),
        ("x", "q.artifact.enrich.x", "enrich_x"),
        ("web", "q.artifact.enrich.web", "enrich_web"),
    ],
)
def test_resolve_provider_routed_enrichment_events(
    provider_route: str,
    queue_name: str,
    stage_name: str,
) -> None:
    resolver = OutboxRouteResolver()

    route = resolver.resolve(
        _row(
            "artifact.enrich.requested.v1",
            payload_json={"provider_route": provider_route},
        )
    )

    assert route.queue_name == queue_name
    assert route.stage_name == stage_name


@pytest.mark.parametrize("payload_json", [{}, {"provider_route": ""}, {"provider_route": "unknown"}])
def test_reject_invalid_provider_route(payload_json: dict) -> None:
    resolver = OutboxRouteResolver()

    with pytest.raises(UnsupportedOutboxEventTypeError):
        resolver.resolve(_row("artifact.enrich.requested.v1", payload_json=payload_json))


def test_reject_unsupported_event_type() -> None:
    resolver = OutboxRouteResolver()

    with pytest.raises(UnsupportedOutboxEventTypeError):
        resolver.resolve(_row("unsupported.event.v1"))
