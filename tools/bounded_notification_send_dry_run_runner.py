from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.notifier_telegram.bounded_notification_send_dry_run_runner import (
    BoundedNotificationSendDryRunConfig,
    BoundedNotificationSendDryRunRuntimeBuilder,
    BoundedNotificationSendDryRunRuntimeConfig,
    argument_error_report,
    load_bounded_notification_send_dry_run_runtime_config,
    render_sanitized_json,
    run_bounded_notification_send_dry_run_sync,
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
        description="Preview or execute one exact q.notification.send dry-run delivery handoff.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=("preview", "execute"), default="preview")
    parser.add_argument("--trigger-event-suffix")
    parser.add_argument("--notification-plan-id-suffix")
    parser.add_argument("--analysis-id-suffix")
    parser.add_argument("--redis-message-suffix")
    parser.add_argument("--scan-limit", type=int, default=10)
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-redis-read", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-redis-consume", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-redis-ack", action="store_true")
    parser.add_argument("--allow-render-write", action="store_true")
    parser.add_argument("--allow-delivery-record-write", action="store_true")
    parser.add_argument("--allow-delivery-result-outbox-write", action="store_true")
    parser.add_argument("--allow-maintenance-outbox-publish", action="store_true")
    parser.add_argument("--allow-maintenance-redis-publish", action="store_true")
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=load_bounded_notification_send_dry_run_runtime_config,
    runtime_builder: BoundedNotificationSendDryRunRuntimeBuilder | None = None,
) -> RunnerResult:
    result = run_bounded_notification_send_dry_run_sync(
        BoundedNotificationSendDryRunConfig(
            mode=str(args.mode),
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_redis_read=bool(args.allow_redis_read),
            allow_database_read=bool(args.allow_database_read),
            allow_redis_consume=bool(args.allow_redis_consume),
            allow_database_write=bool(args.allow_database_write),
            allow_redis_ack=bool(args.allow_redis_ack),
            allow_render_write=bool(args.allow_render_write),
            allow_delivery_record_write=bool(args.allow_delivery_record_write),
            allow_delivery_result_outbox_write=bool(args.allow_delivery_result_outbox_write),
            allow_maintenance_outbox_publish=bool(args.allow_maintenance_outbox_publish),
            allow_maintenance_redis_publish=bool(args.allow_maintenance_redis_publish),
            trigger_event_suffix=args.trigger_event_suffix,
            notification_plan_id_suffix=args.notification_plan_id_suffix,
            analysis_id_suffix=args.analysis_id_suffix,
            redis_message_suffix=args.redis_message_suffix,
            scan_limit=int(args.scan_limit),
        ),
        runtime_config_loader=runtime_config_loader,
        runtime_builder=runtime_builder,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=load_bounded_notification_send_dry_run_runtime_config,
    runtime_builder: BoundedNotificationSendDryRunRuntimeBuilder | None = None,
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
    "BoundedNotificationSendDryRunConfig",
    "BoundedNotificationSendDryRunRuntimeBuilder",
    "BoundedNotificationSendDryRunRuntimeConfig",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
