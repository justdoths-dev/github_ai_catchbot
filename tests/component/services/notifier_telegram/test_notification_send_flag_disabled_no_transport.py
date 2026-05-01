from __future__ import annotations

import pytest

from ._fakes import RaisingTelegramClient, config, repo_with_valid_case, service


@pytest.mark.asyncio
async def test_send_disabled_concretizes_and_records_suppressed_without_transport() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()

    await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=False),
        client=client,
    ).handle_intent(intent)

    plan = repository.plans[intent.notification_plan_id]
    record = repository.delivery_records[0]

    assert client.calls == 0
    assert plan.status == "suppressed"
    assert len(repository.renders) == 1
    assert record["result_status"] == "suppressed"
    assert record["transport_error_code"] == "notification_send_flag_disabled"
    assert record["telegram_response_json"]["send_disabled"] is True
    assert record["telegram_response_json"]["dry_run"] is False
    assert repository.state_transitions[-1]["reason_code"] == "notification_send_flag_disabled"
    assert repository.delivery_outbox[0]["delivery_status"] == "suppressed"
