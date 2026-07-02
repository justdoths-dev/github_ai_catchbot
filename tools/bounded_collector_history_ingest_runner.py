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
    EXECUTE_CONFIRM_TOKEN,
    THREE_CHANNEL_EXECUTE_CONFIRM_TOKEN,
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
        description="Run bounded Telegram collector history ingest for one or exactly three registry targets.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=("plan", "execute"), default="plan")
    parser.add_argument("--source-kind", default="public_username")
    parser.add_argument("--source-value", action="append", dest="source_values")
    parser.add_argument("--registry-id-suffix", default=None)
    parser.add_argument("--history-limit", type=int, default=10)
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--confirm-token", default=None)
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-telegram-read", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-source-message-write", action="store_true")
    parser.add_argument("--allow-source-version-write", action="store_true")
    parser.add_argument("--allow-source-outbox-write", action="store_true")
    parser.add_argument("--allow-source-outbox-publish", action="store_true")
    parser.add_argument("--allow-redis-publish", action="store_true")
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
    source_values = tuple(args.source_values or ())
    result = run_bounded_telegram_collector_history_ingest_sync(
        BoundedTelegramCollectorHistoryIngestConfig(
            mode=str(args.mode),
            source_kind=str(args.source_kind),
            source_value=source_values[0] if len(source_values) == 1 else None,
            source_values=source_values,
            registry_id_suffix=args.registry_id_suffix,
            history_limit=int(args.history_limit),
            operator_approved=bool(args.operator_approved),
            confirm_token=args.confirm_token,
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            allow_telegram_read=bool(args.allow_telegram_read),
            allow_database_write=bool(args.allow_database_write),
            allow_source_message_write=bool(args.allow_source_message_write),
            allow_source_version_write=bool(args.allow_source_version_write),
            allow_source_outbox_write=bool(args.allow_source_outbox_write),
            allow_source_outbox_publish=bool(args.allow_source_outbox_publish),
            allow_redis_publish=bool(args.allow_redis_publish),
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
    "EXECUTE_CONFIRM_TOKEN",
    "RunnerResult",
    "THREE_CHANNEL_EXECUTE_CONFIRM_TOKEN",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
