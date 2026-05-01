from __future__ import annotations

import pytest

from services.notifier_telegram.config import NotifierTelegramConfig, NotifierTelegramConfigurationError


def test_dev_defaults_are_dry_run_and_edits_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://example")
    monkeypatch.setenv("REDIS_URL", "redis://example")
    monkeypatch.delenv("NOTIFIER_TELEGRAM_DRY_RUN", raising=False)
    monkeypatch.delenv("NOTIFIER_TELEGRAM_ALLOW_EDITS", raising=False)
    monkeypatch.delenv("ENABLE_NOTIFICATION_SEND", raising=False)

    cfg = NotifierTelegramConfig.from_env()

    assert cfg.dry_run is True
    assert cfg.allow_edits is False
    assert cfg.enable_notification_send is False
    assert cfg.transport_enabled is False


def test_prod_defaults_disable_dry_run_but_still_require_send_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://example")
    monkeypatch.setenv("REDIS_URL", "redis://example")
    monkeypatch.delenv("NOTIFIER_TELEGRAM_DRY_RUN", raising=False)
    monkeypatch.delenv("NOTIFIER_TELEGRAM_ALLOW_EDITS", raising=False)
    monkeypatch.delenv("ENABLE_NOTIFICATION_SEND", raising=False)

    cfg = NotifierTelegramConfig.from_env()

    assert cfg.dry_run is False
    assert cfg.allow_edits is False
    assert cfg.enable_notification_send is False
    assert cfg.transport_enabled is False


def test_token_required_only_for_actual_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://example")
    monkeypatch.setenv("REDIS_URL", "redis://example")
    monkeypatch.setenv("ENABLE_NOTIFICATION_SEND", "true")
    monkeypatch.setenv("NOTIFIER_TELEGRAM_DRY_RUN", "false")
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)

    with pytest.raises(NotifierTelegramConfigurationError):
        NotifierTelegramConfig.from_env()
