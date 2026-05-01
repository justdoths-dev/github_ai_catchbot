from __future__ import annotations

import pytest

from services.notifier_telegram.models import DeliveryAction
from services.notifier_telegram.renderer import RenderInput
from services.notifier_telegram.telegram_client import (
    TelegramTransportNoopError,
    TelegramTransportRetryableError,
    TelegramTransportTerminalError,
)
from tests.component.services.notifier_telegram._fakes import config, repo_with_valid_case, service


class RaisingTransportClient:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    async def send_message(self, **kwargs):
        raise self.exc

    async def edit_message_text(self, **kwargs):
        raise self.exc


@pytest.mark.asyncio
async def test_429_retry_after_is_retryable_with_retry_after() -> None:
    result = await _perform_with(
        TelegramTransportRetryableError("too many requests", error_code="telegram_rate_limited", retry_after_seconds=42)
    )

    assert result.delivery_status == "failed_retryable"
    assert result.retry_after_seconds == 42
    assert result.transport_error_code == "telegram_rate_limited"


@pytest.mark.asyncio
async def test_5xx_is_retryable() -> None:
    result = await _perform_with(TelegramTransportRetryableError("bad gateway", error_code="telegram_5xx_retryable"))

    assert result.delivery_status == "failed_retryable"
    assert result.retry_after_seconds == 30


@pytest.mark.asyncio
async def test_timeout_network_is_retryable() -> None:
    result = await _perform_with(TelegramTransportRetryableError("timeout", error_code="telegram_network_retryable"))

    assert result.delivery_status == "failed_retryable"
    assert result.transport_error_code == "telegram_network_retryable"


@pytest.mark.asyncio
async def test_message_is_not_modified_is_noop_suppressed() -> None:
    result = await _perform_with(
        TelegramTransportNoopError("message is not modified", error_code="telegram_edit_not_modified_noop"),
        action=DeliveryAction(mode="edit", existing_message_id=1001),
    )

    assert result.delivery_status == "suppressed"
    assert result.transport_error_code == "telegram_edit_not_modified_noop"
    assert result.telegram_message_id == 1001


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    ["telegram_invalid_chat", "telegram_bot_blocked", "telegram_message_cannot_be_edited"],
)
async def test_classifiable_terminal_errors(error_code: str) -> None:
    result = await _perform_with(TelegramTransportTerminalError(error_code, error_code=error_code))

    assert result.delivery_status == "failed_terminal"
    assert result.transport_error_code == error_code


async def _perform_with(exc: Exception, *, action: DeliveryAction | None = None):
    repository, intent = repo_with_valid_case()
    notifier = service(
        repository,
        cfg=config(dry_run=False, enable_notification_send=True),
        client=RaisingTransportClient(exc),
    )
    await notifier._concretize_plan(intent, status="planned")  # noqa: SLF001
    render = notifier._renderer.render(  # noqa: SLF001
        notification_plan_id=intent.notification_plan_id,
        payload=RenderInput(
            analysis=repository.analyses[intent.analysis_id],
            judge_output=repository.judge_outputs[repository.analyses[intent.analysis_id].judge_output_id],
            candidate=repository.candidates[intent.candidate_group_id],
            urgency_profile=intent.urgency_profile,
        ),
    )
    return await notifier._perform_delivery(  # noqa: SLF001
        intent=intent,
        render=render,
        action=action or DeliveryAction(mode="send"),
    )
