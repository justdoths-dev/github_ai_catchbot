from __future__ import annotations

import pytest

from services.maintenance.main import build_parser


def test_parser_accepts_db_shape_preflight_json_command() -> None:
    args = build_parser().parse_args(["db-shape-preflight", "--format", "json"])

    assert args.command == "db-shape-preflight"
    assert args.format == "json"


@pytest.mark.parametrize(
    "flag",
    [
        "--apply",
        "--fix",
        "--migrate",
        "--write",
        "--repair",
        "--publish",
        "--send",
    ],
)
def test_parser_rejects_db_shape_preflight_write_like_flags(flag: str) -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["db-shape-preflight", "--format", "json", flag])

    assert exc.value.code == 2
