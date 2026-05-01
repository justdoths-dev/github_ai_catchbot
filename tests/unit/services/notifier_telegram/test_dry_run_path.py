from __future__ import annotations

import pytest

from tests.component.services.notifier_telegram._fakes import RaisingTelegramClient, config, repo_with_valid_case, service


@pytest.mark.asyncio
async def test_dry_run_creates_render_and_suppressed_delivery_audit_without_transport() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()

    await service(repository, cfg=config(dry_run=True, enable_notification_send=True), client=client).handle_intent(intent)

    assert client.calls == 0
    assert len(repository.renders) == 1
    assert len(repository.delivery_records) == 1
    assert repository.delivery_records[0]["result_status"] == "suppressed"
    assert repository.delivery_records[0]["telegram_response_json"]["dry_run"] is True
