from __future__ import annotations

import pytest

from tests.component.services.notifier_telegram._fakes import RaisingTelegramClient, config, repo_with_valid_case, service


@pytest.mark.asyncio
async def test_same_material_already_delivered_is_noop_without_transport() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()

    await service(repository, cfg=config(dry_run=True, enable_notification_send=True), client=client).handle_intent(intent)
    repository.delivery_records[0]["result_status"] = "sent"
    repository.delivery_records[0]["telegram_message_id"] = 777

    await service(repository, cfg=config(dry_run=False, enable_notification_send=True), client=client).handle_intent(intent)

    assert client.calls == 0
    assert len(repository.delivery_records) == 1
    assert repository.state_transitions[-1]["reason_code"] == "notification_duplicate_noop"
