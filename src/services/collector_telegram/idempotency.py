"""Collector idempotency rules for raw/current/version/outbox handling."""

from __future__ import annotations

from dataclasses import dataclass

_EVENT_PREFIX_MAP = {
    "source_message.created.v1": "srcmsg:create",
    "source_message.edited.v1": "srcmsg:edit",
    "source_message.deleted.v1": "srcmsg:delete",
    "source_message.reconciled.v1": "srcmsg:reconcile",
}


@dataclass(slots=True, frozen=True)
class IdempotencyPolicy:
    """Collector idempotency rules.

    The collector operates under at-least-once assumptions.
    Repeated live updates and repeated reconcile reads are normal and must
    collapse into deterministic no-op behavior where possible.
    """

    def should_append_new_version(self, previous_hash: str | None, next_hash: str) -> bool:
        if not next_hash:
            raise ValueError("next_hash must not be empty")
        return previous_hash != next_hash

    def semantic_event_dedupe_key(
        self,
        event_type: str,
        source_message_id: str,
        version_no: int,
        extra: str | None = None,
    ) -> str:
        if not source_message_id:
            raise ValueError("source_message_id must not be empty")
        if version_no <= 0:
            raise ValueError("version_no must be > 0")

        prefix = _EVENT_PREFIX_MAP.get(event_type, event_type.replace('.', ':'))
        suffix = f":{extra}" if extra else ""
        return f"{prefix}:{source_message_id}:{version_no}{suffix}"
