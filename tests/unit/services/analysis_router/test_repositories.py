from __future__ import annotations

import json
from uuid import uuid4

import pytest

from src.services.analysis_router.repositories import (
    AnalysisRouterRepository,
    bundle_refresh_outbox_dedupe_key,
)


class FakeResult:
    def __init__(self, row: dict) -> None:
        self.row = row

    def mappings(self):
        return self

    def first(self):
        return self.row


class FakeSession:
    def __init__(self, row: dict) -> None:
        self.row = row
        self.execute_calls: list[dict[str, object]] = []

    def in_transaction(self) -> bool:
        return False

    def begin(self):
        raise AssertionError("transaction is not used by this method")

    async def execute(self, statement, params=None):
        self.execute_calls.append({"statement": str(statement), "params": dict(params or {})})
        return FakeResult(self.row)


@pytest.mark.asyncio
async def test_insert_bundle_refresh_outbox_returns_existing_or_created_identity_and_bundle_payload() -> None:
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    event_id = uuid4()
    refresh_reason = "manual_recheck"
    session = FakeSession({"event_id": event_id, "status": "pending", "created": True})
    repository = AnalysisRouterRepository(session)  # type: ignore[arg-type]

    result = await repository.insert_bundle_refresh_outbox(
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        refresh_reason=refresh_reason,
    )

    assert result.event_id == event_id
    assert result.status == "pending"
    assert result.created is True
    assert len(session.execute_calls) == 1
    call = session.execute_calls[0]
    sql = " ".join(str(call["statement"]).split())
    params = call["params"]
    payload = json.loads(str(params["payload_json"]))
    assert "WITH inserted AS" in sql
    assert "ON CONFLICT (dedupe_key) DO NOTHING" in sql
    assert "RETURNING event_id, status, TRUE AS created" in sql
    assert "ORDER BY created DESC LIMIT 1" in sql
    assert params["dedupe_key"] == bundle_refresh_outbox_dedupe_key(
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        refresh_reason=refresh_reason,
    )
    assert payload == {
        "bundle_id": str(bundle_id),
        "candidate_group_id": str(candidate_group_id),
        "refresh_reason": refresh_reason,
        "trigger_kind": "analysis_router_recheck",
        "trigger_object_id": str(bundle_id),
        "trigger_object_type": "bundle",
    }
