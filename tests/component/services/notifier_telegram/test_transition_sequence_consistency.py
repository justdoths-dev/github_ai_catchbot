from __future__ import annotations

import pytest

from ._fakes import config, repo_with_valid_case, service


class SuccessfulClient:
    async def send_message(self, **kwargs):
        return {"ok": True, "result": {"message_id": 456, "chat": {"id": kwargs["chat_id"]}}}


@pytest.mark.asyncio
async def test_planned_rendered_queued_final_transition_sequence_for_live_send() -> None:
    repository, intent = repo_with_valid_case()

    await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=True),
        client=SuccessfulClient(),
    ).handle_intent(intent)

    transitions = [(row["from_state"], row["to_state"]) for row in repository.state_transitions]
    assert transitions == [("planned", "rendered"), ("rendered", "queued"), ("queued", "sent")]
    assert repository.plans[intent.notification_plan_id].status == "sent"
    assert repository.delivery_records[0]["result_status"] == "sent"
