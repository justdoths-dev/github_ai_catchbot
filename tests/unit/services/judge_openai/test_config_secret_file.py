from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from services.judge_openai.config import JudgeOpenAIConfig, JudgeOpenAIConfigurationError


def _base_env(key_file: Path) -> dict[str, str]:
    return {
        "DATABASE_URL": "db-dsn",
        "REDIS_URL": "redis-dsn",
        "OPENAI_API_KEY_FILE": str(key_file),
    }


def test_config_passes_with_openai_api_key_file(tmp_path: Path) -> None:
    secret_value = f"secret-{uuid4().hex}"
    key_file = tmp_path / "openai-key"
    key_file.write_text(f"  {secret_value}\n", encoding="utf-8")

    config = JudgeOpenAIConfig.from_env(_base_env(key_file))

    assert config.database_url == "db-dsn"
    assert config.redis_url == "redis-dsn"
    assert config.openai_api_key == secret_value
    assert config.queue_name == "q.analysis.judge"
    assert config.consumer_group == "judge-openai"
    assert config.consumer_name == "judge-openai-1"
    assert config.batch_size == 10
    assert config.block_ms == 5000
    assert config.request_timeout_sec == 60
    assert config.max_output_tokens is None
    assert config.enable_prompt_guard_preflight is False
    assert secret_value not in repr(config)


def test_config_rejects_direct_openai_api_key_without_leaking_value(tmp_path: Path) -> None:
    direct_secret = f"direct-{uuid4().hex}"
    key_file = tmp_path / "openai-key"
    key_file.write_text("file-secret", encoding="utf-8")
    env = _base_env(key_file) | {"OPENAI_API_KEY": direct_secret}

    with pytest.raises(JudgeOpenAIConfigurationError) as exc_info:
        JudgeOpenAIConfig.from_env(env)

    message = str(exc_info.value)
    assert "direct OPENAI_API_KEY is not supported" in message
    assert direct_secret not in message


def test_config_fails_when_openai_key_file_is_missing(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing-openai-key"

    with pytest.raises(JudgeOpenAIConfigurationError) as exc_info:
        JudgeOpenAIConfig.from_env(_base_env(missing_file))

    assert str(exc_info.value) == "OPENAI_API_KEY_FILE must reference a regular file"
    assert str(missing_file) not in str(exc_info.value)


def test_config_fails_when_openai_key_file_is_empty(tmp_path: Path) -> None:
    key_file = tmp_path / "openai-key"
    key_file.write_text(" \n", encoding="utf-8")

    with pytest.raises(JudgeOpenAIConfigurationError) as exc_info:
        JudgeOpenAIConfig.from_env(_base_env(key_file))

    assert str(exc_info.value) == "OPENAI_API_KEY_FILE is empty"


def test_config_fails_when_openai_key_file_is_unreadable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret_value = f"secret-{uuid4().hex}"
    key_file = tmp_path / "openai-key"
    key_file.write_text(secret_value, encoding="utf-8")

    def raise_unreadable(self: Path, *, encoding: str | None = None) -> str:
        raise OSError("raw filesystem detail that must not leak")

    monkeypatch.setattr(Path, "read_text", raise_unreadable)

    with pytest.raises(JudgeOpenAIConfigurationError) as exc_info:
        JudgeOpenAIConfig.from_env(_base_env(key_file))

    message = str(exc_info.value)
    assert message == "OPENAI_API_KEY_FILE could not be read"
    assert secret_value not in message
    assert "raw filesystem detail" not in message
