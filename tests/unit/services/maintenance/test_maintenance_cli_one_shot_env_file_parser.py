from __future__ import annotations

import pytest

from services.maintenance.main import build_parser, _foreground_smoke_request_error, _worker_once_request_error


def test_parser_accepts_delivery_gate_env_file() -> None:
    args = build_parser().parse_args(
        [
            "delivery-gate",
            "--mode",
            "restricted",
            "--format",
            "json",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert args.command == "delivery-gate"
    assert args.mode == "restricted"
    assert args.format == "json"
    assert args.env_file == "/tmp/runtime.env"


def test_parser_accepts_mvp_readiness_env_file() -> None:
    args = build_parser().parse_args(
        [
            "mvp-readiness",
            "--mode",
            "restricted",
            "--format",
            "json",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert args.command == "mvp-readiness"
    assert args.mode == "restricted"
    assert args.format == "json"
    assert args.env_file == "/tmp/runtime.env"


def test_parser_accepts_worker_once_maintenance_execute_env_file() -> None:
    args = build_parser().parse_args(
        [
            "worker-once",
            "maintenance",
            "--mode",
            "execute",
            "--max-messages",
            "1",
            "--confirm",
            "ack",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert args.command == "worker-once"
    assert args.worker_type == "maintenance"
    assert args.mode == "execute"
    assert args.max_messages == 1
    assert args.confirm == "ack"
    assert args.env_file == "/tmp/runtime.env"


def test_parser_accepts_worker_once_replay_execute_env_file() -> None:
    args = build_parser().parse_args(
        [
            "worker-once",
            "replay",
            "--mode",
            "execute",
            "--max-messages",
            "2",
            "--confirm",
            "ack",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert args.command == "worker-once"
    assert args.worker_type == "replay"
    assert args.mode == "execute"
    assert args.max_messages == 2
    assert args.confirm == "ack"
    assert args.env_file == "/tmp/runtime.env"


def test_parser_accepts_foreground_smoke_execute_env_file() -> None:
    args = build_parser().parse_args(
        [
            "foreground-smoke",
            "--mode",
            "execute",
            "--ticks",
            "1",
            "--max-messages",
            "1",
            "--confirm",
            "run",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert args.command == "foreground-smoke"
    assert args.mode == "execute"
    assert args.ticks == 1
    assert args.max_messages == 1
    assert args.confirm == "run"
    assert args.env_file == "/tmp/runtime.env"


def test_worker_once_cli_rejects_execute_without_confirm_ack() -> None:
    args = build_parser().parse_args(
        [
            "worker-once",
            "maintenance",
            "--mode",
            "execute",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert _worker_once_request_error(args) == "ack_confirm_missing"


def test_foreground_smoke_cli_rejects_execute_without_confirm_run() -> None:
    args = build_parser().parse_args(
        [
            "foreground-smoke",
            "--mode",
            "execute",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert _foreground_smoke_request_error(args) == "run_confirm_missing"


@pytest.mark.parametrize("max_messages", [0, 11])
def test_worker_once_cli_rejects_max_messages_outside_bounds(max_messages: int) -> None:
    args = build_parser().parse_args(
        [
            "worker-once",
            "maintenance",
            "--mode",
            "execute",
            "--max-messages",
            str(max_messages),
            "--confirm",
            "ack",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert _worker_once_request_error(args) == "max_messages_not_allowed"


@pytest.mark.parametrize("ticks", [0, 6])
def test_foreground_smoke_cli_rejects_ticks_outside_bounds(ticks: int) -> None:
    args = build_parser().parse_args(
        [
            "foreground-smoke",
            "--mode",
            "execute",
            "--ticks",
            str(ticks),
            "--max-messages",
            "1",
            "--confirm",
            "run",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert _foreground_smoke_request_error(args) == "ticks_not_allowed"


@pytest.mark.parametrize("max_messages", [0, 11])
def test_foreground_smoke_cli_rejects_max_messages_outside_bounds(max_messages: int) -> None:
    args = build_parser().parse_args(
        [
            "foreground-smoke",
            "--mode",
            "execute",
            "--ticks",
            "1",
            "--max-messages",
            str(max_messages),
            "--confirm",
            "run",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert _foreground_smoke_request_error(args) == "max_messages_not_allowed"
