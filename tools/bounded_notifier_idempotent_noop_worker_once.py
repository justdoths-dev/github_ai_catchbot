from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.notifier_telegram.idempotent_noop_worker_once import (
    BoundedNotifierIdempotentNoopRuntimeBuilder,
    BoundedNotifierIdempotentNoopRuntimeConfig,
    BoundedNotifierIdempotentNoopWorkerOnceConfig,
    argument_error_report,
    load_bounded_notifier_idempotent_noop_runtime_config,
    render_sanitized_json,
    run_bounded_notifier_idempotent_noop_worker_once_sync,
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
        description="Preview or execute one exact q.notification.send idempotent no-op proof.",
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-database-write-for-notifier-noop-only", action="store_true")
    parser.add_argument("--allow-redis-read", action="store_true")
    parser.add_argument("--allow-redis-consume", action="store_true")
    parser.add_argument("--allow-redis-ack", action="store_true")
    parser.add_argument("--require-telegram-disabled", action="store_true")
    parser.add_argument("--mode", choices=("preview", "execute"), default="preview")
    parser.add_argument("--queue-name", default="q.notification.send")
    parser.add_argument("--redis-message-id-suffix")
    parser.add_argument("--trigger-event-suffix")
    parser.add_argument("--analysis-suffix")
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=load_bounded_notifier_idempotent_noop_runtime_config,
    runtime_builder: BoundedNotifierIdempotentNoopRuntimeBuilder | None = None,
) -> RunnerResult:
    result = run_bounded_notifier_idempotent_noop_worker_once_sync(
        BoundedNotifierIdempotentNoopWorkerOnceConfig(
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            allow_database_write_for_notifier_noop_only=bool(
                args.allow_database_write_for_notifier_noop_only
            ),
            allow_redis_read=bool(args.allow_redis_read),
            allow_redis_consume=bool(args.allow_redis_consume),
            allow_redis_ack=bool(args.allow_redis_ack),
            require_telegram_disabled=bool(args.require_telegram_disabled),
            mode=str(args.mode),
            queue_name=str(args.queue_name),
            redis_message_id_suffix=args.redis_message_id_suffix,
            trigger_event_suffix=args.trigger_event_suffix,
            analysis_suffix=args.analysis_suffix,
        ),
        runtime_config_loader=runtime_config_loader,
        runtime_builder=runtime_builder,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=load_bounded_notifier_idempotent_noop_runtime_config,
    runtime_builder: BoundedNotifierIdempotentNoopRuntimeBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(argument_error_report(str(exc))))
        return 1
    result = run(args, runtime_config_loader=runtime_config_loader, runtime_builder=runtime_builder)
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


__all__ = [
    "BoundedNotifierIdempotentNoopRuntimeBuilder",
    "BoundedNotifierIdempotentNoopRuntimeConfig",
    "BoundedNotifierIdempotentNoopWorkerOnceConfig",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
