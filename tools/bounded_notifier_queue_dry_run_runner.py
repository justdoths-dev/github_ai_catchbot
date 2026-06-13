from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.notifier_telegram.bounded_queue_invocation import (
    BoundedInvocationRunner,
    BoundedNotificationQueueConsumerBuilder,
    BoundedNotificationQueueRuntimeConfig,
    BoundedNotifierQueueDryRunConfig,
    BoundedNotifierQueueDryRunResult,
    BoundedNotifierRuntimeBuilder,
    QUEUE_NAME,
    load_bounded_notification_queue_config,
    load_forced_dry_run_notifier_config,
    render_sanitized_json,
    run_bounded_notifier_queue_dry_run_invocation_sync,
)
from src.services.notifier_telegram.config import NotifierTelegramConfig


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
        description="Run one q.notification.send notifier message in forced dry-run/send-disabled mode.",
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-redis-read", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-redis-ack", action="store_true")
    return parser


def run(
    args: argparse.Namespace,
    *,
    queue_config_loader=load_bounded_notification_queue_config,
    consumer_builder: BoundedNotificationQueueConsumerBuilder | None = None,
    bounded_invocation_runner: BoundedInvocationRunner | None = None,
    notifier_config_loader=load_forced_dry_run_notifier_config,
    runtime_builder: BoundedNotifierRuntimeBuilder | None = None,
) -> RunnerResult:
    runner_kwargs: dict[str, Any] = {
        "queue_config_loader": queue_config_loader,
        "consumer_builder": consumer_builder,
        "notifier_config_loader": notifier_config_loader,
        "runtime_builder": runtime_builder,
    }
    if bounded_invocation_runner is not None:
        runner_kwargs["bounded_invocation_runner"] = bounded_invocation_runner
    result = run_bounded_notifier_queue_dry_run_invocation_sync(
        BoundedNotifierQueueDryRunConfig(
            operator_approved=bool(args.operator_approved),
            allow_redis_read=bool(args.allow_redis_read),
            allow_database_write=bool(args.allow_database_write),
            allow_redis_ack=bool(args.allow_redis_ack),
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def _argument_error_report(error_code: str) -> dict[str, Any]:
    result = BoundedNotifierQueueDryRunResult(
        status="blocked",
        ok=False,
        error_code=error_code,
        queue_name=QUEUE_NAME,
        operator_approved=False,
        redis_read_allowed=False,
        database_write_allowed=False,
        redis_ack_allowed=False,
        redis_message_count=0,
        redis_ack_count=0,
        trigger_event_id_present=False,
        processed_event_count=0,
    )
    return result.to_sanitized_dict()


def main(
    argv: Sequence[str] | None = None,
    *,
    queue_config_loader=load_bounded_notification_queue_config,
    consumer_builder: BoundedNotificationQueueConsumerBuilder | None = None,
    bounded_invocation_runner: BoundedInvocationRunner | None = None,
    notifier_config_loader=load_forced_dry_run_notifier_config,
    runtime_builder: BoundedNotifierRuntimeBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(_argument_error_report(str(exc))))
        return 1
    result = run(
        args,
        queue_config_loader=queue_config_loader,
        consumer_builder=consumer_builder,
        bounded_invocation_runner=bounded_invocation_runner,
        notifier_config_loader=notifier_config_loader,
        runtime_builder=runtime_builder,
    )
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


__all__ = [
    "BoundedInvocationRunner",
    "BoundedNotificationQueueConsumerBuilder",
    "BoundedNotificationQueueRuntimeConfig",
    "BoundedNotifierQueueDryRunConfig",
    "BoundedNotifierRuntimeBuilder",
    "CliArgumentError",
    "NotifierTelegramConfig",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
