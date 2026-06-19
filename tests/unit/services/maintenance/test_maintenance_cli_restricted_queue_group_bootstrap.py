from __future__ import annotations

import pytest

from services.maintenance.main import (
    _queue_group_bootstrap_request,
    _queue_group_bootstrap_request_error,
    build_parser,
)
from tests.component.services.maintenance._fakes import config


@pytest.mark.parametrize("queue_name", ["maintenance", "replay"])
@pytest.mark.parametrize("mode", ["plan", "execute", "proof"])
def test_parser_accepts_queue_group_bootstrap_modes(queue_name: str, mode: str) -> None:
    argv = [
        "queue-group-bootstrap",
        queue_name,
        "--mode",
        mode,
        "--env-file",
        "/tmp/runtime.env",
    ]
    if mode == "execute":
        argv.extend(["--confirm", "create-group"])

    args = build_parser().parse_args(argv)

    assert args.command == "queue-group-bootstrap"
    assert args.bootstrap_queue == queue_name
    assert args.mode == mode
    assert args.env_file == "/tmp/runtime.env"
    assert args.confirm == ("create-group" if mode == "execute" else None)
    assert _queue_group_bootstrap_request_error(args) is None


def test_queue_group_bootstrap_env_file_is_required() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["queue-group-bootstrap", "maintenance", "--mode", "plan"])


def test_invalid_queue_selector_is_rejected_by_argparse() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "queue-group-bootstrap",
                "not-a-queue",
                "--mode",
                "plan",
                "--env-file",
                "/tmp/runtime.env",
            ]
        )


def test_execute_requires_confirm_create_group() -> None:
    args = build_parser().parse_args(
        [
            "queue-group-bootstrap",
            "maintenance",
            "--mode",
            "execute",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert _queue_group_bootstrap_request_error(args) == "create_group_confirm_missing"


@pytest.mark.parametrize("mode", ["plan", "proof"])
def test_plan_and_proof_reject_confirm_create_group(mode: str) -> None:
    args = build_parser().parse_args(
        [
            "queue-group-bootstrap",
            "replay",
            "--mode",
            mode,
            "--confirm",
            "create-group",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert _queue_group_bootstrap_request_error(args) == "create_group_confirm_not_allowed_for_read_only"


def test_queue_group_bootstrap_request_uses_existing_configured_maintenance_identity() -> None:
    args = build_parser().parse_args(
        [
            "queue-group-bootstrap",
            "maintenance",
            "--mode",
            "execute",
            "--confirm",
            "create-group",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    request = _queue_group_bootstrap_request(config(), args)

    assert request.queue_selector == "maintenance"
    assert request.queue_name == "q.maintenance"
    assert request.consumer_group == "maintenance"
    assert request.consumer_name == config().maintenance_consumer_name
    assert request.mode == "execute"
    assert request.confirm_create_group is True


def test_queue_group_bootstrap_request_uses_existing_configured_replay_identity() -> None:
    args = build_parser().parse_args(
        [
            "queue-group-bootstrap",
            "replay",
            "--mode",
            "proof",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    request = _queue_group_bootstrap_request(config(), args)

    assert request.queue_selector == "replay"
    assert request.queue_name == "q.replay"
    assert request.consumer_group == "maintenance-replay"
    assert request.consumer_name == config().replay_consumer_name
    assert request.mode == "proof"
    assert request.confirm_create_group is False
