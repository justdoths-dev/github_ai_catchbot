from __future__ import annotations

from services.maintenance.main import (
    _controlled_worker_request_error,
    build_parser,
)


def _args(*extra: str):
    return build_parser().parse_args(
        [
            "controlled-worker",
            "--mode",
            "execute",
            "--max-ticks",
            "3",
            "--max-runtime-sec",
            "30",
            "--max-messages",
            "1",
            "--idle-sleep-ms",
            "100",
            "--confirm",
            "run",
            "--env-file",
            "/tmp/runtime.env",
            *extra,
        ]
    )


def test_parser_accepts_controlled_worker_execute_shape() -> None:
    args = _args()

    assert args.command == "controlled-worker"
    assert args.mode == "execute"
    assert args.max_ticks == 3
    assert args.max_runtime_sec == 30
    assert args.max_messages == 1
    assert args.idle_sleep_ms == 100
    assert args.confirm == "run"
    assert args.env_file == "/tmp/runtime.env"
    assert _controlled_worker_request_error(args) is None


def test_request_rejects_missing_confirm_run() -> None:
    args = build_parser().parse_args(
        [
            "controlled-worker",
            "--mode",
            "execute",
            "--max-ticks",
            "3",
            "--max-runtime-sec",
            "30",
            "--max-messages",
            "1",
            "--idle-sleep-ms",
            "100",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert _controlled_worker_request_error(args) == "run_confirm_missing"


def test_request_rejects_max_ticks_out_of_range() -> None:
    assert _controlled_worker_request_error(_args("--max-ticks", "21")) == "max_ticks_not_allowed"


def test_request_rejects_max_runtime_sec_out_of_range() -> None:
    assert _controlled_worker_request_error(_args("--max-runtime-sec", "301")) == "max_runtime_sec_not_allowed"


def test_request_rejects_max_messages_out_of_range() -> None:
    assert _controlled_worker_request_error(_args("--max-messages", "11")) == "max_messages_not_allowed"


def test_request_rejects_idle_sleep_ms_out_of_range() -> None:
    assert _controlled_worker_request_error(_args("--idle-sleep-ms", "5001")) == "idle_sleep_ms_not_allowed"
