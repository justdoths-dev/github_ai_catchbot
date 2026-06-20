from __future__ import annotations

import pytest

from services.maintenance.main import build_parser


@pytest.mark.parametrize(
    ("mode", "confirm"),
    [
        ("plan", None),
        ("install", "install"),
        ("start", "start"),
        ("proof", None),
        ("rollback", "rollback"),
        ("diagnose", None),
        ("context-proof", None),
    ],
)
def test_parser_accepts_systemd_rollout_modes(mode: str, confirm: str | None) -> None:
    argv = [
        "systemd-rollout",
        "--mode",
        mode,
        "--target",
        "maintenance-worker",
        "--env-file",
        "/tmp/runtime.env",
    ]
    if confirm is not None:
        argv.extend(["--confirm", confirm])

    args = build_parser().parse_args(argv)

    assert args.command == "systemd-rollout"
    assert args.mode == mode
    assert args.target == "maintenance-worker"
    assert args.confirm == confirm
    assert args.env_file == "/tmp/runtime.env"


def test_parser_accepts_systemd_rollout_override_paths() -> None:
    args = build_parser().parse_args(
        [
            "systemd-rollout",
            "--mode",
            "plan",
            "--target",
            "maintenance-worker",
            "--env-file",
            "/tmp/runtime.env",
            "--repo-root",
            "/tmp/repo",
            "--python-executable",
            "/tmp/repo/venv/bin/python",
            "--systemd-user-dir",
            "/tmp/user-units",
        ]
    )

    assert args.repo_root == "/tmp/repo"
    assert args.python_executable == "/tmp/repo/venv/bin/python"
    assert args.systemd_user_dir == "/tmp/user-units"


def test_parser_accepts_worker_runtime_fatal_report_readback_mode() -> None:
    args = build_parser().parse_args(["worker-runtime-fatal-report", "--mode", "read"])

    assert args.command == "worker-runtime-fatal-report"
    assert args.mode == "read"


def test_worker_runtime_fatal_report_readback_rejects_arbitrary_path_input() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "worker-runtime-fatal-report",
                "--mode",
                "read",
                "--path",
                "/tmp/other-report.json",
            ]
        )
