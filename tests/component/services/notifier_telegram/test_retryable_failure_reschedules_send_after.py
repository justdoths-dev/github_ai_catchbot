from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.notifier_telegram.telegram_client import TelegramTransportRetryableError

from ._fakes import config, repo_with_valid_case, service


class RetryableClient:
    async def send_message(self, **kwargs):
        raise TelegramTransportRetryableError("temporary", error_code="telegram_5xx_retryable")


@pytest.mark.asyncio
async def test_retryable_transport_failure_updates_plan_and_emits_result() -> None:
    repository, intent = repo_with_valid_case()

    await service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=True),
        client=RetryableClient(),
    ).handle_intent(intent)

    plan = repository.plans[intent.notification_plan_id]
    assert plan.status == "failed_retryable"
    assert plan.send_after is not None
    assert plan.send_after > datetime.now(timezone.utc)
    assert repository.delivery_records[0]["result_status"] == "failed_retryable"
    assert repository.delivery_outbox[0]["delivery_status"] == "failed_retryable"
