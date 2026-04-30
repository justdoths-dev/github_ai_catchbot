from __future__ import annotations

from ._service_fakes import config, intent, render

import pytest

from services.notifier_telegram.models import DeliveryAction
from services.notifier_telegram.service import NotifierTelegramService


class Repo:
    pass


class RaisingClient:
    def __init__(self) -> None:
        self.calls = 0

    async def send_message(self, **kwargs):  # pragma: no cover
        self.calls += 1
        raise AssertionError("telegram client must not be called")


@pytest.mark.asyncio
async def test_transport_disabled_does_not_call_client() -> None:
    client = RaisingClient()
    service = NotifierTelegramService(config(dry_run=True, enable_notification_send=True), repository=Repo(), telegram_client=client)  # type: ignore[arg-type]

    result = await service._perform_delivery(intent=intent(), render=render(), action=DeliveryAction(mode="send"))  # noqa: SLF001

    assert result.delivery_status == "suppressed"
    assert result.attempt_count == 0
    assert result.transport_error_code == "telegram_dry_run"
    assert result.telegram_response_json == {
        "dry_run": True,
        "send_enabled": True,
        "transport_skipped": True,
        "reason_code": "telegram_dry_run",
    }
    assert client.calls == 0


@pytest.mark.asyncio
async def test_send_disabled_non_dry_run_does_not_call_client() -> None:
    client = RaisingClient()
    service = NotifierTelegramService(config(dry_run=False, enable_notification_send=False), repository=Repo(), telegram_client=client)  # type: ignore[arg-type]

    result = await service._perform_delivery(intent=intent(), render=render(), action=DeliveryAction(mode="send"))  # noqa: SLF001

    assert result.delivery_status == "suppressed"
    assert result.attempt_count == 0
    assert result.transport_error_code == "telegram_send_disabled"
    assert result.telegram_response_json == {
        "dry_run": False,
        "send_enabled": False,
        "transport_skipped": True,
        "reason_code": "telegram_send_disabled",
    }
    assert client.calls == 0
