from __future__ import annotations

import pytest

from services.maintenance.main import build_parser, _queue_activate_request, _queue_activate_request_error
from tests.component.services.maintenance._fakes import config


def test_parser_accepts_queue_activate_maintenance_plan() -> None:
    args = build_parser().parse_args(
        [
            "queue-activate",
            "maintenance",
            "--mode",
            "plan",
            "--max-messages",
            "3",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert args.command == "queue-activate"
    assert args.activation_queue == "maintenance"
    assert args.mode == "plan"
    assert args.max_messages == 3
    assert args.env_file == "/tmp/runtime.env"
    assert args.confirm is None


def test_parser_accepts_queue_activate_replay_execute_with_ack_confirm() -> None:
    args = build_parser().parse_args(
        [
            "queue-activate",
            "replay",
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

    assert args.command == "queue-activate"
    assert args.activation_queue == "replay"
    assert args.mode == "execute"
    assert args.confirm == "ack"


def test_parser_accepts_queue_activate_replay_proof() -> None:
    args = build_parser().parse_args(
        [
            "queue-activate",
            "replay",
            "--mode",
            "proof",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert args.command == "queue-activate"
    assert args.activation_queue == "replay"
    assert args.mode == "proof"
    assert args.max_messages == 1


def test_queue_activate_env_file_is_required() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["queue-activate", "maintenance", "--mode", "plan"])


def test_execute_requires_confirm_ack() -> None:
    args = build_parser().parse_args(
        [
            "queue-activate",
            "maintenance",
            "--mode",
            "execute",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert _queue_activate_request_error(args) == "ack_confirm_missing"


@pytest.mark.parametrize("max_messages", [0, 11])
def test_max_messages_bounds_enforced(max_messages: int) -> None:
    args = build_parser().parse_args(
        [
            "queue-activate",
            "maintenance",
            "--mode",
            "plan",
            "--max-messages",
            str(max_messages),
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert _queue_activate_request_error(args) == "max_messages_not_allowed"


def test_plan_cannot_confirm_ack_or_create_group() -> None:
    args = build_parser().parse_args(
        [
            "queue-activate",
            "maintenance",
            "--mode",
            "plan",
            "--confirm",
            "ack",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )
    assert _queue_activate_request_error(args) == "ack_confirm_not_allowed_for_dry_run"

    create_args = build_parser().parse_args(
        [
            "queue-activate",
            "maintenance",
            "--mode",
            "proof",
            "--allow-create-group",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )
    assert _queue_activate_request_error(create_args) == "allow_create_group_not_allowed_for_dry_run"


def test_queue_activate_request_uses_existing_configured_queue_identity() -> None:
    args = build_parser().parse_args(
        [
            "queue-activate",
            "replay",
            "--mode",
            "execute",
            "--confirm",
            "ack",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    request = _queue_activate_request(config(), args)

    assert request.queue_name == "q.replay"
    assert request.consumer_group == "maintenance-replay"
    assert request.consumer_name == config().replay_consumer_name
    assert request.max_messages == 1
    assert request.ack is True
    assert request.dry_run is False
