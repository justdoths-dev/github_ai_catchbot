from __future__ import annotations

from services.maintenance.main import build_parser


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

