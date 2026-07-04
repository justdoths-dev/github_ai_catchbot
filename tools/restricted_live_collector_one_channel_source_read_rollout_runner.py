from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.collector_telegram.bounded_history_ingest_runner import render_sanitized_json
from src.services.collector_telegram.restricted_source_read_rollout import (
    RestrictedLiveCollectorOneChannelSourceReadProofRequest,
    build_restricted_live_collector_one_channel_source_read_preflight_packet,
    build_restricted_live_collector_one_channel_source_read_rollout_packet,
    restricted_live_collector_one_channel_source_read_argument_error_report,
)


class CliArgumentError(ValueError):
    pass


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Build a fake-backed restricted one-channel Telegram source-read rollout proof packet.",
        add_help=False,
    )
    parser.add_argument("--source-value", action="append", dest="source_values")
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--emit-live-preflight-command", action="store_true")
    return parser


def run(args: argparse.Namespace) -> RunnerResult:
    request = RestrictedLiveCollectorOneChannelSourceReadProofRequest(
        source_values=tuple(args.source_values or ()),
        requested_max_messages=args.max_messages,
    )
    builder = (
        build_restricted_live_collector_one_channel_source_read_preflight_packet
        if args.emit_live_preflight_command
        else build_restricted_live_collector_one_channel_source_read_rollout_packet
    )
    report = builder(request)
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(
            render_sanitized_json(
                restricted_live_collector_one_channel_source_read_argument_error_report(str(exc))
            )
        )
        return 1
    result = run(args)
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


__all__ = [
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
