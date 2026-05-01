from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.notifier_telegram.telegram_client import TelegramTransportRetryableError

from ._fakes import config, repo_with_valid_case, service


class RateLimitedClient:
    async def send_message(self, **kwargs):
        raise TelegramTransportRetryableError(
            "too many requests",
            error_code="telegram_rate_limited",
            retry_after_seconds=90,
        )


@pytest.mark.asyncio
async def test_telegram_retry_after_value_is_used_for_send_after() -> None:
    repository, intent = repo_with_valid_case()
    before = datetime.now(timezone.utc)

    await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=True),
        client=RateLimitedClient(),
    ).handle_intent(intent)

    delta = (repository.plans[intent.notification_plan_id].send_after - before).total_seconds()
    assert 89 <= delta <= 95
    assert repository.delivery_records[0]["telegram_response_json"]["retry_after_seconds"] == 90
