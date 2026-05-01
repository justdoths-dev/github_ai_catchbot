from __future__ import annotations

from pathlib import Path

import pytest

from services.notifier_telegram.config import NotifierTelegramConfig, NotifierTelegramConfigurationError


ROOT = Path(__file__).resolve().parents[4]
ENV_DIR = ROOT / "compose" / "env"


def _read_env_example(name: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in (ENV_DIR / name).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


def test_stage40_env_example_matrix_defaults() -> None:
    prod = _read_env_example("notifier.prod.env.example")
    dev = _read_env_example("notifier.dev.env.example")
    replay = _read_env_example("notifier.replay.env.example")

    assert _contains(prod, {
        "APP_ENV": "prod",
        "ENABLE_NOTIFICATION_SEND": "false",
        "NOTIFIER_TELEGRAM_DRY_RUN": "false",
        "NOTIFIER_TELEGRAM_ALLOW_EDITS": "true",
        "ENABLE_LATER_DELIVERY": "true",
        "ENABLE_SILENT_LATER": "true",
        "ENABLE_REPLAY_TO_PROD_DB": "false",
        "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": "false",
    })
    for values, app_env in [(dev, "dev"), (replay, "replay")]:
        assert _contains(values, {
            "APP_ENV": app_env,
            "ENABLE_NOTIFICATION_SEND": "false",
            "NOTIFIER_TELEGRAM_DRY_RUN": "true",
            "NOTIFIER_TELEGRAM_ALLOW_EDITS": "false",
            "ENABLE_LATER_DELIVERY": "true",
            "ENABLE_SILENT_LATER": "true",
            "ENABLE_REPLAY_TO_PROD_DB": "false",
            "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": "false",
        })


def test_config_allows_prod_baseline_without_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://example")
    monkeypatch.setenv("REDIS_URL", "redis://example")
    monkeypatch.setenv("ENABLE_NOTIFICATION_SEND", "false")
    monkeypatch.setenv("NOTIFIER_TELEGRAM_DRY_RUN", "false")
    monkeypatch.setenv("NOTIFIER_TELEGRAM_ALLOW_EDITS", "true")
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)

    cfg = NotifierTelegramConfig.from_env()

    assert cfg.app_env == "prod"
    assert cfg.allow_edits is True
    assert cfg.transport_enabled is False


def test_config_requires_token_only_when_actual_transport_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://example")
    monkeypatch.setenv("REDIS_URL", "redis://example")
    monkeypatch.setenv("ENABLE_NOTIFICATION_SEND", "true")
    monkeypatch.setenv("NOTIFIER_TELEGRAM_DRY_RUN", "false")
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)

    with pytest.raises(NotifierTelegramConfigurationError):
        NotifierTelegramConfig.from_env()

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    cfg = NotifierTelegramConfig.from_env()
    assert cfg.transport_enabled is True


@pytest.mark.parametrize("app_env", ["dev", "replay"])
def test_dev_and_replay_defaults_prevent_live_transport(monkeypatch: pytest.MonkeyPatch, app_env: str) -> None:
    monkeypatch.setenv("APP_ENV", app_env)
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://example")
    monkeypatch.setenv("REDIS_URL", "redis://example")
    monkeypatch.delenv("ENABLE_NOTIFICATION_SEND", raising=False)
    monkeypatch.delenv("NOTIFIER_TELEGRAM_DRY_RUN", raising=False)
    monkeypatch.delenv("NOTIFIER_TELEGRAM_ALLOW_EDITS", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)

    cfg = NotifierTelegramConfig.from_env()

    assert cfg.enable_notification_send is False
    assert cfg.dry_run is True
    assert cfg.allow_edits is False
    assert cfg.transport_enabled is False


def _contains(values: dict[str, str], expected: dict[str, str]) -> bool:
    return all(values.get(key) == value for key, value in expected.items())
