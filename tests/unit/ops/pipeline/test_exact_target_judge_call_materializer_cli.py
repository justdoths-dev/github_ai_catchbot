from __future__ import annotations

import json
import os
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest

from src.services.analysis_router.config import AnalysisRouterConfig
from src.services.maintenance.exact_target_judge_call_materializer import (
    ExactTargetJudgeCallMaterializerComponents,
    ExactTargetJudgeCallMaterializerConfigError,
    RuntimeConfigBundle,
    build_parser,
    run_cli,
)


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "exact_target_judge_call_materializer.py"
RAW_ENV_PATH = "/tmp/private-materializer-env-placeholder"


class CountingRuntimeLoader:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, env_file: str) -> RuntimeConfigBundle:
        self.calls.append(env_file)
        raise AssertionError("runtime config must not be loaded")


@asynccontextmanager
async def raising_components_builder(runtime: RuntimeConfigBundle):
    del runtime
    raise AssertionError("session components must not be opened")
    yield  # pragma: no cover


def _router_config() -> AnalysisRouterConfig:
    return AnalysisRouterConfig(
        app_env="test",
        database_url="db_locator_omitted",
        redis_url="redis_locator_omitted",
        queue_name="q.analysis.route",
        consumer_group="analysis-router",
        consumer_name="analysis-router-test",
        batch_size=1,
        block_ms=100,
        enable_model_escalation=False,
        default_model="gpt-5.4-mini",
        escalation_model="gpt-5.4",
        default_reasoning_effort="low",
        escalation_reasoning_effort="medium",
        github_prompt_version="judge_github_primary_v1",
        x_prompt_version="judge_x_primary_v1",
        text_idea_prompt_version="judge_text_idea_primary_v1",
        judge_schema_version="judge_output_v1",
        policy_version="verdict_policy_v1",
        log_level="INFO",
    )


class EnvMissingRuntimeLoader:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, env_file: str) -> RuntimeConfigBundle:
        self.calls.append(env_file)
        raise ExactTargetJudgeCallMaterializerConfigError("env_file_missing")


@pytest.mark.asyncio
async def test_execute_requires_confirm_before_env_session_or_service_invocation() -> None:
    loader = CountingRuntimeLoader()
    outputs: list[str] = []
    event_id = uuid4()

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--trigger-event-id",
            str(event_id),
            "--env-file",
            RAW_ENV_PATH,
        ],
        emit_json=outputs.append,
        runtime_config_loader=loader,
        session_components_builder=raising_components_builder,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == "materialize_judge_call_confirm_missing"
    assert loader.calls == []
    assert RAW_ENV_PATH not in outputs[0]
    assert str(event_id) not in outputs[0]


@pytest.mark.asyncio
async def test_malformed_uuid_blocks_before_env_load() -> None:
    loader = CountingRuntimeLoader()
    outputs: list[str] = []

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--trigger-event-id",
            "not-a-uuid",
            "--env-file",
            RAW_ENV_PATH,
        ],
        emit_json=outputs.append,
        runtime_config_loader=loader,
        session_components_builder=raising_components_builder,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "invalid_trigger_event_id"
    assert loader.calls == []
    assert "not-a-uuid" not in outputs[0]
    assert RAW_ENV_PATH not in outputs[0]


@pytest.mark.asyncio
async def test_env_errors_are_sanitized_after_valid_request() -> None:
    loader = EnvMissingRuntimeLoader()
    outputs: list[str] = []
    event_id = uuid4()

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--trigger-event-id",
            str(event_id),
            "--env-file",
            RAW_ENV_PATH,
        ],
        emit_json=outputs.append,
        runtime_config_loader=loader,
        session_components_builder=raising_components_builder,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "env_file_missing"
    assert loader.calls == [RAW_ENV_PATH]
    assert RAW_ENV_PATH not in outputs[0]
    assert str(event_id) not in outputs[0]


def test_parser_does_not_accept_latest_selector() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--mode",
                "plan",
                "--latest",
                "--env-file",
                RAW_ENV_PATH,
            ]
        )


def test_script_is_thin_delegate() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    assert "from src.services.maintenance.exact_target_judge_call_materializer import main" in source
    assert "subprocess" not in source
    assert "Redis" not in source
    assert "OpenAI" not in source
    assert "Telegram" not in source


def test_file_path_execution_bootstraps_imports_without_pythonpath() -> None:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--mode",
            "execute",
            "--trigger-event-id",
            "not-a-uuid",
            "--env-file",
            RAW_ENV_PATH,
            "--confirm",
            "materialize-judge-call",
        ],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 2
    payload = json.loads(completed.stdout)
    assert payload["reason_code"] == "invalid_trigger_event_id"
    assert payload["openai_attempted"] is False
    assert payload["redis_attempted"] is False
    assert payload["telegram_attempted"] is False
    assert RAW_ENV_PATH not in completed.stdout
