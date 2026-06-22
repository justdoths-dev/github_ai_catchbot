from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "exact_target_live_openai_canary.py"


def test_script_is_thin_delegate() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    assert "from src.services.maintenance.exact_target_live_openai_canary import main" in source
    assert "Redis" not in source
    assert "TelegramBotClient" not in source
    assert "JudgeOpenAIWorker" not in source
    assert "subprocess" not in source


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
            "/tmp/missing-runtime.env",
            "--confirm",
            "live-openai",
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
    assert payload["openai_request_count"] == 0
    assert "missing-runtime" not in completed.stdout
