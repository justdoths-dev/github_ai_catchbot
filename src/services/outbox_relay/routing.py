from __future__ import annotations

from typing import Any

from .models import OutboxEventRow, QueueRoute


class UnsupportedOutboxEventTypeError(ValueError):
    """Raised when an outbox event cannot be deterministically routed."""


class OutboxRouteResolver:
    """Resolve event_outbox rows into queue/stage routes.

    Design rules:
    - routing stays deterministic,
    - no business interpretation happens here,
    - provider-specific enrichment routing is derived from payload_json["provider_route"].
    """

    _SOURCE_ROUTE = QueueRoute(queue_name="q.source.normalize", stage_name="normalize")
    _BUNDLE_ROUTE = QueueRoute(queue_name="q.candidate.bundle", stage_name="bundle")
    _ANALYSIS_ROUTE = QueueRoute(queue_name="q.analysis.route", stage_name="analysis_route")
    _JUDGE_ROUTE = QueueRoute(queue_name="q.analysis.judge", stage_name="judge")
    _VALIDATE_ROUTE = QueueRoute(queue_name="q.analysis.validate", stage_name="analysis_validate")
    _POLICY_ROUTE = QueueRoute(queue_name="q.analysis.policy", stage_name="analysis_policy")
    _NOTIFY_ROUTE = QueueRoute(queue_name="q.notification.send", stage_name="notify")
    _REPLAY_ROUTE = QueueRoute(queue_name="q.replay", stage_name="replay")
    _MAINTENANCE_ROUTE = QueueRoute(queue_name="q.maintenance", stage_name="maintenance")

    def resolve(self, row: OutboxEventRow) -> QueueRoute:
        event_type = row.event_type

        if event_type in {
            "source_message.created.v1",
            "source_message.edited.v1",
            "source_message.deleted.v1",
            "source_message.reconciled.v1",
        }:
            return self._SOURCE_ROUTE

        if event_type == "artifact.enrich.requested.v1":
            provider_route = self._payload_value(row.payload_json, "provider_route")
            if provider_route == "github":
                return QueueRoute("q.artifact.enrich.github", "enrich_github")
            if provider_route == "x":
                return QueueRoute("q.artifact.enrich.x", "enrich_x")
            if provider_route == "web":
                return QueueRoute("q.artifact.enrich.web", "enrich_web")
            raise UnsupportedOutboxEventTypeError(
                f"artifact.enrich.requested.v1 missing/invalid provider_route: {provider_route!r}"
            )

        if event_type in {"candidate.bundle.refresh.v1", "artifact.snapshot.updated.v1"}:
            return self._BUNDLE_ROUTE

        if event_type == "analysis.requested.v1":
            return self._ANALYSIS_ROUTE

        if event_type == "judge.call.requested.v1":
            return self._JUDGE_ROUTE

        if event_type == "judge.output.ready.v1":
            return self._VALIDATE_ROUTE

        if event_type == "analysis.policy.apply.v1":
            return self._POLICY_ROUTE

        if event_type == "notification.plan.created.v1":
            return self._NOTIFY_ROUTE

        if event_type == "replay.requested.v1":
            return self._REPLAY_ROUTE

        if event_type == "notification.delivery.result.v1":
            return self._MAINTENANCE_ROUTE

        raise UnsupportedOutboxEventTypeError(f"unsupported outbox event_type: {event_type}")

    @staticmethod
    def _payload_value(payload: dict[str, Any], key: str) -> str | None:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        return None
