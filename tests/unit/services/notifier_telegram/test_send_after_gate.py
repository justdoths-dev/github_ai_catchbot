from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from tests.component.services.notifier_telegram._fakes import RaisingTelegramClient, config, repo_with_valid_case, service


@pytest.mark.asyncio
async def test_future_send_after_defers_without_transport_or_final_sent_state() -> None:
    repository, intent = repo_with_valid_case()
    future_intent = replace(intent, send_after=datetime.now(timezone.utc) + timedelta(minutes=10))
    client = RaisingTelegramClient()

    await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=True),
        client=client,
    ).handle_intent(future_intent)

    assert client.calls == 0
    assert repository.renders == []
    assert repository.delivery_records == []
    assert repository.plans[future_intent.notification_plan_id].status == "planned"
    assert repository.state_transitions[-1]["reason_code"] == "notification_send_after_deferred"
