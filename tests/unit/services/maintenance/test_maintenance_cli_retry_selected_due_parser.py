from __future__ import annotations

import pytest

from services.maintenance.main import build_parser


def test_parser_accepts_retry_selected_due_multiple_plan_ids_with_confirm_write() -> None:
    args = build_parser().parse_args(
        [
            "batch-recovery",
            "retry-selected-due",
            "--plan-id",
            "00000000-0000-0000-0000-000000000001",
            "--plan-id",
            "00000000-0000-0000-0000-000000000002",
            "--requested-by",
            "test/operator",
            "--confirm",
            "write",
            "--env-file",
            "/tmp/runtime.env",
        ]
    )

    assert args.command == "batch-recovery"
    assert args.recovery_mode == "retry-selected-due"
    assert args.plan_id == [
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
    ]
    assert args.requested_by == "test/operator"
    assert args.confirm == "write"
    assert args.env_file == "/tmp/runtime.env"


def test_retry_selected_due_missing_confirm_is_parse_error() -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(
            [
                "batch-recovery",
                "retry-selected-due",
                "--plan-id",
                "00000000-0000-0000-0000-000000000001",
                "--requested-by",
                "test/operator",
            ]
        )

    assert exc.value.code == 2


def test_retry_selected_due_invalid_confirm_value_is_parse_error() -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(
            [
                "batch-recovery",
                "retry-selected-due",
                "--plan-id",
                "00000000-0000-0000-0000-000000000001",
                "--requested-by",
                "test/operator",
                "--confirm",
                "yes",
            ]
        )

    assert exc.value.code == 2


def test_retry_selected_due_rejects_replay_selected_only_flags() -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(
            [
                "batch-recovery",
                "retry-selected-due",
                "--plan-id",
                "00000000-0000-0000-0000-000000000001",
                "--requested-by",
                "test/operator",
                "--confirm",
                "write",
                "--operator-confirmed",
            ]
        )

    assert exc.value.code == 2


def test_delivery_gate_rejects_batch_recovery_flags() -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(
            [
                "delivery-gate",
                "--mode",
                "restricted",
                "--format",
                "json",
                "--plan-id",
                "00000000-0000-0000-0000-000000000001",
                "--confirm",
                "write",
            ]
        )

    assert exc.value.code == 2
