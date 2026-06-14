from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.collector_telegram.bounded_history_ingest_runner import (
    BoundedTelegramCollectorHistoryIngestConfig,
    BoundedTelegramCollectorHistoryIngestResult,
    BoundedTelegramCollectorHistoryIngestRuntimeBuilder,
    CollectorTelegramConfig,
    argument_error_report,
    render_sanitized_json,
    run_bounded_telegram_collector_history_ingest_sync,
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
        description="Run one bounded Telegram collector history ingest for one chat.",
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-telegram-read", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-outbox-write", action="store_true")
    parser.add_argument("--max-messages", type=int, default=1)
    parser.add_argument("--chat-id", type=int, default=None)
    parser.add_argument("--registry-id", default=None)
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    runtime_builder: BoundedTelegramCollectorHistoryIngestRuntimeBuilder | None = None,
) -> RunnerResult:
    runner_kwargs: dict[str, Any] = {
        "runtime_builder": runtime_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader
    result = run_bounded_telegram_collector_history_ingest_sync(
        BoundedTelegramCollectorHistoryIngestConfig(
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_telegram_read=bool(args.allow_telegram_read),
            allow_database_write=bool(args.allow_database_write),
            allow_outbox_write=bool(args.allow_outbox_write),
            max_messages=int(args.max_messages),
            chat_id=args.chat_id,
            registry_id=args.registry_id,
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    runtime_builder: BoundedTelegramCollectorHistoryIngestRuntimeBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(argument_error_report(str(exc))))
        return 1
    result = run(
        args,
        runtime_config_loader=runtime_config_loader,
        runtime_builder=runtime_builder,
    )
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


__all__ = [
    "BoundedTelegramCollectorHistoryIngestConfig",
    "BoundedTelegramCollectorHistoryIngestResult",
    "BoundedTelegramCollectorHistoryIngestRuntimeBuilder",
    "CliArgumentError",
    "CollectorTelegramConfig",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
