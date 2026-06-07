from __future__ import annotations

from services.notifier_telegram.main import build_parser


def test_parser_accepts_send_canary_operator_confirmed_env_file_json() -> None:
    args = build_parser().parse_args(
        [
            "send-canary",
            "--notification-plan-id",
            "00000000-0000-0000-0000-000000000001",
            "--operator-confirmed",
            "--env-file",
            "/tmp/notifier-runtime.env",
            "--format",
            "json",
        ]
    )

    assert args.command == "send-canary"
    assert args.notification_plan_id == "00000000-0000-0000-0000-000000000001"
    assert args.operator_confirmed is True
    assert args.env_file == "/tmp/notifier-runtime.env"
    assert args.format == "json"


def test_parser_accepts_create_canary_plan_operator_confirmed_env_file_json() -> None:
    args = build_parser().parse_args(
        [
            "create-canary-plan",
            "--source-notification-plan-id",
            "00000000-0000-0000-0000-000000000001",
            "--canary-key",
            "stable-canary_01",
            "--operator-confirmed",
            "--env-file",
            "/tmp/notifier-runtime.env",
            "--format",
            "json",
        ]
    )

    assert args.command == "create-canary-plan"
    assert args.source_notification_plan_id == "00000000-0000-0000-0000-000000000001"
    assert args.canary_key == "stable-canary_01"
    assert args.operator_confirmed is True
    assert args.env_file == "/tmp/notifier-runtime.env"
    assert args.format == "json"
