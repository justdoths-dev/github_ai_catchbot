from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EVENT_NAME_FILES = (
    "analysis.policy.apply.v1",
    "judge.call.requested.v1",
    "judge.output.ready.v1",
    "notification.delivery.result.v1",
    "notification.plan.created.v1",
)


def _run_cli(outcome: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "tools.upstream_hot_path_acceptance_runner", "--outcome", outcome],
        check=False,
        capture_output=True,
        cwd=ROOT,
        text=True,
        timeout=30,
    )


def _assert_no_event_name_files() -> None:
    for name in EVENT_NAME_FILES:
        assert not (ROOT / name).exists(), name


def test_send_now_cli_returns_zero_and_emits_stable_json() -> None:
    result = _run_cli("send_now")

    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert result.stderr == ""
    assert report["schema_version"] == "upstream_hot_path_acceptance_v1"
    assert report["status"] == "pass"
    assert report["delivery_decision"] == "send_now"
    assert report["notifier_boundary_reached"] is True
    assert report["live_telegram_called"] is False
    assert report["openai_called"] is False
    assert report["workers_started"] is False
    assert report["redis_mutation"] is False
    assert report["checks_failed"] == []
    assert report["event_sequence"] == [
        "analysis.requested.v1",
        "judge.call.requested.v1",
        "judge.output.ready.v1",
        "analysis.policy.apply.v1",
        "notification.plan.created.v1",
        "notification.delivery.result.v1",
    ]
    _assert_no_event_name_files()


def test_suppress_cli_returns_zero_and_emits_stable_json() -> None:
    result = _run_cli("suppress")

    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert result.stderr == ""
    assert report["schema_version"] == "upstream_hot_path_acceptance_v1"
    assert report["status"] == "pass"
    assert report["delivery_decision"] == "suppress"
    assert report["notification_plan_created"] is False
    assert report["notifier_boundary_reached"] is False
    assert report["live_telegram_called"] is False
    assert report["openai_called"] is False
    assert report["workers_started"] is False
    assert report["redis_mutation"] is False
    assert report["checks_failed"] == []
    assert report["event_sequence"] == [
        "analysis.requested.v1",
        "judge.call.requested.v1",
        "judge.output.ready.v1",
        "analysis.policy.apply.v1",
    ]
    _assert_no_event_name_files()
