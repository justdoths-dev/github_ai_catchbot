from __future__ import annotations

import pytest

from tests.component.services.notifier_telegram._fakes import RaisingTelegramClient, config, repo_with_valid_case, service


def _delivery_replay_root(notification_plan_id: str) -> dict[str, str]:
    return {
        "root_object_type": "notification_plan",
        "root_object_id": notification_plan_id,
        "replay_type": "delivery",
    }


@pytest.mark.asyncio
async def test_send_disabled_rollback_preserves_plan_render_and_replay_boundary() -> None:
    repository, intent = repo_with_valid_case()
    client = RaisingTelegramClient()

    await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=False),
        client=client,
    ).handle_intent(intent)

    assert client.calls == 0
    assert intent.notification_plan_id in repository.plans
    assert repository.plans[intent.notification_plan_id].status == "suppressed"
    assert len(repository.renders) == 1
    assert len(repository.delivery_records) == 1
    assert repository.delivery_records[0]["transport_error_code"] == "notification_send_flag_disabled"

    replay_root = _delivery_replay_root(str(intent.notification_plan_id))
    assert replay_root == {
        "root_object_type": "notification_plan",
        "root_object_id": str(intent.notification_plan_id),
        "replay_type": "delivery",
    }
