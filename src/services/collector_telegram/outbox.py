"""Collector outbox draft builders."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .idempotency import IdempotencyPolicy
from .models import OutboxEventDraft

JsonDict = dict[str, Any]
_SOURCE_MESSAGE_AGGREGATE = "source_message"


@dataclass(slots=True)
class CollectorOutboxBuilder:
    policy: IdempotencyPolicy

    def build_created(
        self,
        *,
        source_message_id: str,
        current_version_no: int,
        logical_post_key: str,
        occurred_at: datetime,
    ) -> OutboxEventDraft:
        event_type = "source_message.created.v1"
        return OutboxEventDraft(
            event_type=event_type,
            aggregate_type=_SOURCE_MESSAGE_AGGREGATE,
            aggregate_id=source_message_id,
            dedupe_key=self.policy.semantic_event_dedupe_key(
                event_type,
                source_message_id,
                current_version_no,
            ),
            payload_json=self._base_payload(
                source_message_id=source_message_id,
                current_version_no=current_version_no,
                logical_post_key=logical_post_key,
                occurred_at=occurred_at,
            ),
        )

    def build_edited(
        self,
        *,
        source_message_id: str,
        current_version_no: int,
        logical_post_key: str,
        occurred_at: datetime,
    ) -> OutboxEventDraft:
        event_type = "source_message.edited.v1"
        return OutboxEventDraft(
            event_type=event_type,
            aggregate_type=_SOURCE_MESSAGE_AGGREGATE,
            aggregate_id=source_message_id,
            dedupe_key=self.policy.semantic_event_dedupe_key(
                event_type,
                source_message_id,
                current_version_no,
            ),
            payload_json=self._base_payload(
                source_message_id=source_message_id,
                current_version_no=current_version_no,
                logical_post_key=logical_post_key,
                occurred_at=occurred_at,
            ),
        )

    def build_deleted(
        self,
        *,
        source_message_id: str,
        current_version_no: int,
        logical_post_key: str,
        occurred_at: datetime,
        delete_kind: str,
    ) -> OutboxEventDraft:
        event_type = "source_message.deleted.v1"
        payload = self._base_payload(
            source_message_id=source_message_id,
            current_version_no=current_version_no,
            logical_post_key=logical_post_key,
            occurred_at=occurred_at,
        )
        payload["delete_kind"] = delete_kind
        return OutboxEventDraft(
            event_type=event_type,
            aggregate_type=_SOURCE_MESSAGE_AGGREGATE,
            aggregate_id=source_message_id,
            dedupe_key=self.policy.semantic_event_dedupe_key(
                event_type,
                source_message_id,
                current_version_no,
            ),
            payload_json=payload,
        )

    def build_reconciled(
        self,
        *,
        source_message_id: str,
        current_version_no: int,
        logical_post_key: str,
        occurred_at: datetime,
        reconcile_reason: str,
    ) -> OutboxEventDraft:
        event_type = "source_message.reconciled.v1"
        payload = self._base_payload(
            source_message_id=source_message_id,
            current_version_no=current_version_no,
            logical_post_key=logical_post_key,
            occurred_at=occurred_at,
        )
        payload["reconcile_reason"] = reconcile_reason
        return OutboxEventDraft(
            event_type=event_type,
            aggregate_type=_SOURCE_MESSAGE_AGGREGATE,
            aggregate_id=source_message_id,
            dedupe_key=self.policy.semantic_event_dedupe_key(
                event_type,
                source_message_id,
                current_version_no,
                extra=reconcile_reason,
            ),
            payload_json=payload,
        )

    def _base_payload(
        self,
        *,
        source_message_id: str,
        current_version_no: int,
        logical_post_key: str,
        occurred_at: datetime,
    ) -> JsonDict:
        return {
            "source_message_id": source_message_id,
            "current_version_no": current_version_no,
            "logical_post_key": logical_post_key,
            "occurred_at": self._isoformat(occurred_at),
        }

    @staticmethod
    def _isoformat(value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
