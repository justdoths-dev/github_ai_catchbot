from __future__ import annotations

import pytest

from services.maintenance.main import build_parser


def test_parser_accepts_confirmed_replay_selected_command() -> None:
    args = build_parser().parse_args(
        [
            "batch-recovery",
            "replay-selected",
            "--plan-id",
            "00000000-0000-0000-0000-000000000001",
            "--plan-id",
            "00000000-0000-0000-0000-000000000002",
            "--requested-by",
            "test/operator",
            "--operator-confirmed",
        ]
    )

    assert args.command == "batch-recovery"
    assert args.recovery_mode == "replay-selected"
    assert args.plan_id == [
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
    ]
    assert args.requested_by == "test/operator"
    assert args.operator_confirmed is True


def test_delivery_gate_rejects_recovery_flags() -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(
            [
                "delivery-gate",
                "--mode",
                "restricted",
                "--plan-id",
                "00000000-0000-0000-0000-000000000001",
            ]
        )

    assert exc.value.code == 2


def test_replay_selected_rejects_gate_only_flags() -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(
            [
                "batch-recovery",
                "replay-selected",
                "--plan-id",
                "00000000-0000-0000-0000-000000000001",
                "--requested-by",
                "test/operator",
                "--operator-confirmed",
                "--mode",
                "restricted",
            ]
        )

    assert exc.value.code == 2
