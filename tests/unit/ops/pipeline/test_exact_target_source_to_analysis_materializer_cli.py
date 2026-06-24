from __future__ import annotations

import json
import os
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from src.services.maintenance.exact_target_source_to_analysis_materializer import (
    ExactTargetSourceToAnalysisConfigError,
    RuntimeConfigBundle,
    build_parser,
    run_cli,
)


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "exact_target_source_to_analysis_materializer.py"
RAW_ENV_PATH = "/tmp/private-source-analysis-env-placeholder"
RAW_TEXT = "AI developer workflow automation for repository tests."
RAW_REF = "https://t.me/SynthChannel/12345"


class CountingRuntimeLoader:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, env_file: str) -> RuntimeConfigBundle:
        self.calls.append(env_file)
        raise AssertionError("runtime config must not be loaded")


class EnvMissingRuntimeLoader:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, env_file: str) -> RuntimeConfigBundle:
        self.calls.append(env_file)
        raise ExactTargetSourceToAnalysisConfigError("env_file_missing")


@asynccontextmanager
async def raising_stage_factory_builder(runtime: RuntimeConfigBundle):
    del runtime
    raise AssertionError("stage factory must not be opened")
    yield  # pragma: no cover


def write_packet(path: Path, *, text: str = RAW_TEXT, source_ref: str = RAW_REF) -> Path:
    path.write_text(
        json.dumps(
            {
                "schema_version": "operator_supplied_telegram_source_v1",
                "source_ref": source_ref,
                "posted_at": "2026-06-23T01:02:03Z",
                "message_text": text,
            }
        ),
        encoding="utf-8",
    )
    return path


@pytest.mark.asyncio
async def test_malformed_packet_blocks_before_env_session_or_service_invocation(tmp_path: Path) -> None:
    packet_path = tmp_path / "packet.json"
    packet_path.write_text("{not json", encoding="utf-8")
    loader = CountingRuntimeLoader()
    outputs: list[str] = []

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--source-packet-json",
            str(packet_path),
            "--env-file",
            RAW_ENV_PATH,
        ],
        emit_json=outputs.append,
        runtime_config_loader=loader,
        stage_factory_builder=raising_stage_factory_builder,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == "source_packet_invalid_json"
    assert loader.calls == []
    assert str(packet_path) not in outputs[0]
    assert RAW_ENV_PATH not in outputs[0]


@pytest.mark.asyncio
async def test_execute_requires_confirm_before_packet_env_or_session(tmp_path: Path) -> None:
    packet_path = write_packet(tmp_path / "packet.json")
    loader = CountingRuntimeLoader()
    outputs: list[str] = []

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--source-packet-json",
            str(packet_path),
            "--env-file",
            RAW_ENV_PATH,
        ],
        emit_json=outputs.append,
        runtime_config_loader=loader,
        stage_factory_builder=raising_stage_factory_builder,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "materialize_source_analysis_confirm_missing"
    assert payload["source_packet_fingerprint"] is None
    assert loader.calls == []
    assert str(packet_path) not in outputs[0]
    assert RAW_ENV_PATH not in outputs[0]


@pytest.mark.asyncio
async def test_plan_rejects_confirmation_before_env_or_session(tmp_path: Path) -> None:
    packet_path = write_packet(tmp_path / "packet.json")
    loader = CountingRuntimeLoader()
    outputs: list[str] = []

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--source-packet-json",
            str(packet_path),
            "--env-file",
            RAW_ENV_PATH,
            "--confirm",
            "materialize-source-analysis",
        ],
        emit_json=outputs.append,
        runtime_config_loader=loader,
        stage_factory_builder=raising_stage_factory_builder,
    )

    assert exit_code == 2
    assert json.loads(outputs[0])["reason_code"] == "confirm_not_allowed_for_plan"
    assert loader.calls == []


@pytest.mark.asyncio
async def test_plan_rejects_resume_authority_before_env_or_session(tmp_path: Path) -> None:
    packet_path = write_packet(tmp_path / "packet.json")
    loader = CountingRuntimeLoader()
    outputs: list[str] = []

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--source-packet-json",
            str(packet_path),
            "--env-file",
            RAW_ENV_PATH,
            "--allow-existing-source-provider-resume",
            "--provider-resume-confirm",
            "resume-live-github-provider-evidence",
        ],
        emit_json=outputs.append,
        runtime_config_loader=loader,
        stage_factory_builder=raising_stage_factory_builder,
    )

    assert exit_code == 2
    assert json.loads(outputs[0])["reason_code"] == "provider_resume_authority_not_allowed_for_plan"
    assert loader.calls == []


@pytest.mark.asyncio
async def test_no_latest_or_multiple_packet_support(tmp_path: Path) -> None:
    loader = CountingRuntimeLoader()
    outputs: list[str] = []

    exit_code = await run_cli(
        ["--mode", "plan", "--latest", "--env-file", RAW_ENV_PATH],
        emit_json=outputs.append,
        runtime_config_loader=loader,
        stage_factory_builder=raising_stage_factory_builder,
    )
    assert exit_code == 2
    assert json.loads(outputs[0])["reason_code"] == "invalid_cli_arguments"

    packet_one = write_packet(tmp_path / "one.json")
    packet_two = write_packet(tmp_path / "two.json")
    outputs.clear()
    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--source-packet-json",
            str(packet_one),
            "--source-packet-json",
            str(packet_two),
            "--env-file",
            RAW_ENV_PATH,
        ],
        emit_json=outputs.append,
        runtime_config_loader=loader,
        stage_factory_builder=raising_stage_factory_builder,
    )
    assert exit_code == 2
    assert json.loads(outputs[0])["reason_code"] == "exactly_one_source_packet_json_required"
    assert loader.calls == []


@pytest.mark.asyncio
async def test_packet_file_must_be_outside_repo_and_not_symlink(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    inside_packet = write_packet(repo_root / "packet.json")
    loader = CountingRuntimeLoader()
    outputs: list[str] = []

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--source-packet-json",
            str(inside_packet),
            "--env-file",
            RAW_ENV_PATH,
        ],
        emit_json=outputs.append,
        runtime_config_loader=loader,
        stage_factory_builder=raising_stage_factory_builder,
        repo_root=repo_root,
    )

    assert exit_code == 2
    assert json.loads(outputs[0])["reason_code"] == "source_packet_path_inside_repo"
    assert loader.calls == []

    outside_packet = write_packet(tmp_path / "outside.json")
    symlink_path = tmp_path / "packet-link.json"
    symlink_path.symlink_to(outside_packet)
    outputs.clear()
    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--source-packet-json",
            str(symlink_path),
            "--env-file",
            RAW_ENV_PATH,
        ],
        emit_json=outputs.append,
        runtime_config_loader=loader,
        stage_factory_builder=raising_stage_factory_builder,
        repo_root=repo_root,
    )

    assert exit_code == 2
    assert json.loads(outputs[0])["reason_code"] == "source_packet_path_symlink"
    assert loader.calls == []


@pytest.mark.asyncio
async def test_env_errors_after_valid_packet_are_sanitized(tmp_path: Path) -> None:
    packet_path = write_packet(tmp_path / "packet.json")
    loader = EnvMissingRuntimeLoader()
    outputs: list[str] = []

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--source-packet-json",
            str(packet_path),
            "--env-file",
            RAW_ENV_PATH,
        ],
        emit_json=outputs.append,
        runtime_config_loader=loader,
        stage_factory_builder=raising_stage_factory_builder,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "env_file_missing"
    assert payload["source_packet_fingerprint"] is not None
    assert loader.calls == [RAW_ENV_PATH]
    for forbidden in (str(packet_path), RAW_ENV_PATH, RAW_REF, RAW_TEXT, "SynthChannel", "12345"):
        assert forbidden not in outputs[0]


def test_parser_surface_has_no_latest_selector() -> None:
    parser = build_parser()

    with pytest.raises(ExactTargetSourceToAnalysisConfigError):
        parser.parse_args(["--mode", "plan", "--latest", "--env-file", RAW_ENV_PATH])

    parsed = parser.parse_args(
        [
            "--mode",
            "execute",
            "--source-packet-json",
            "/tmp/source-packet.json",
            "--env-file",
            RAW_ENV_PATH,
            "--allow-existing-source-provider-resume",
            "--provider-resume-confirm",
            "resume-live-github-provider-evidence",
        ]
    )
    assert parsed.allow_existing_source_provider_resume is True
    assert parsed.provider_resume_confirm == "resume-live-github-provider-evidence"


def test_script_is_thin_delegate() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    assert (
        "from src.services.maintenance.exact_target_source_to_analysis_materializer import main"
        in source
    )
    assert "subprocess" not in source
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
            "--source-packet-json",
            "/tmp/nonexistent-source-analysis-packet.json",
            "--env-file",
            RAW_ENV_PATH,
            "--confirm",
            "materialize-source-analysis",
        ],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 2
    assert completed.stderr == ""
    payload = json.loads(completed.stdout)
    assert payload["reason_code"] == "source_packet_missing"
    assert payload["openai_attempted"] is False
    assert payload["redis_attempted"] is False
    assert payload["telegram_live_read_attempted"] is False
    assert "/tmp/nonexistent-source-analysis-packet.json" not in completed.stdout
    assert RAW_ENV_PATH not in completed.stdout
