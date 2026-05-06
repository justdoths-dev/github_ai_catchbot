from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "collector_live_startup_probe.py"
RUNBOOK = ROOT / "ops" / "pipeline" / "runbooks" / "collector_live_startup_probe.md"


def _module():
    from scripts.ops import collector_live_startup_probe as module

    return module


def test_script_and_runbook_exist() -> None:
    assert SCRIPT.exists()
    assert RUNBOOK.exists()


def test_report_type_and_authorization_are_locked() -> None:
    module = _module()

    assert module.REPORT_TYPE == "collector_live_startup_probe_v1"
    assert module.SUCCESS_NOTE == "Probe success does not authorize live ingest or production rollout."


def test_generate_report_passes_with_expected_shape() -> None:
    module = _module()

    report = module.generate_report()

    assert report["report_type"] == "collector_live_startup_probe_v1"
    assert report["contract_status"] == "passed"
    assert report["checks_failed"] == []
    assert report["failures"] == []
    assert report["authorization"]["live_ingest_authorized"] is False
    assert report["authorization"]["production_rollout_authorized"] is False
    assert report["checks"] == {
        "repo_local_only": "passed",
        "uses_synthetic_environment_only": "passed",
        "default_lock_path_computed": "passed",
        "override_lock_path_computed": "passed",
        "singleton_guard_acquire_release": "passed",
        "replay_mode_skips_live_singleton": "passed",
        "fake_runtime_start_stop": "passed",
    }


def test_side_effect_booleans_are_false_in_passing_report() -> None:
    module = _module()
    report = module.generate_report()

    assert report["side_effects"] == {
        "tdlib_started": False,
        "telegram_called": False,
        "db_connection_attempted": False,
        "redis_connection_attempted": False,
        "external_network_attempted": False,
        "docker_invoked": False,
        "systemd_invoked": False,
        "env_or_feature_flags_mutated": False,
        "secret_values_printed": False,
    }


def test_no_secret_sentinel_appears_in_stdout_or_report() -> None:
    module = _module()

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    report = module.render_json(module.generate_report())
    combined = f"{result.stdout}\n{result.stderr}\n{report}"

    assert result.returncode == 0
    for sentinel in module.SYNTHETIC_SECRET_SENTINELS:
        assert sentinel not in combined


def test_singleton_guard_probe_blocks_duplicate_acquisition() -> None:
    module = _module()
    report = module.generate_report()

    assert report["checks"]["singleton_guard_acquire_release"] == "passed"
    assert "singleton_guard_duplicate_acquired" not in report["checks_failed"]


def test_replay_mode_skips_live_singleton_acquisition() -> None:
    module = _module()
    report = module.generate_report()

    assert report["checks"]["replay_mode_skips_live_singleton"] == "passed"


def test_fake_runtime_start_stop_is_exercised() -> None:
    module = _module()
    report = module.generate_report()

    assert report["checks"]["fake_runtime_start_stop"] == "passed"


def test_no_forbidden_runtime_imports_or_invocations() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "import socket" not in text
    assert "import requests" not in text
    assert "from requests" not in text
    assert "import httpx" not in text
    assert "from httpx" not in text
    assert "subprocess" not in text
    assert "create_async_engine" not in text
    assert "redis.Redis" not in text
    assert "DockerClient" not in text
    assert "systemctl" not in text


def test_runbook_contains_hard_safety_warnings() -> None:
    text = RUNBOOK.read_text(encoding="utf-8").lower()

    assert "probe success does not authorize live ingest or production rollout" in text
    assert "does not connect to postgresql" in text
    assert "does not connect to redis" in text
    assert "does not call telegram, tdlib, openai, github, x, or external network" in text
    assert "does not invoke docker, docker compose, systemd" in text
    assert "does not mutate `.env`, feature flags" in text
    assert "does not read or print real secret values" in text


def test_render_json_is_deterministic_json() -> None:
    module = _module()

    rendered = module.render_json(module.generate_report())

    assert json.loads(rendered)["report_type"] == "collector_live_startup_probe_v1"


@pytest.mark.parametrize("args", [[], ["--format", "json"]])
def test_cli_outputs_json(args) -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    assert json.loads(result.stdout)["report_type"] == "collector_live_startup_probe_v1"
