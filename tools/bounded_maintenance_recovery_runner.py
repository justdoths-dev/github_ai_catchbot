from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.maintenance.bounded_runtime import (
    DUE_RETRY_COMMAND,
    MAINTENANCE_RESULT_COMMAND,
    REPLAY_REQUEST_COMMAND,
    BoundedMaintenanceDueRetryConfig,
    BoundedMaintenanceDueRetryRuntimeBuilder,
    BoundedMaintenanceQueueOnceConfig,
    BoundedMaintenanceQueueRuntimeBuilder,
    BoundedMaintenanceRuntimeConfig,
    BoundedMaintenanceRuntimeError,
    argument_error_report,
    load_bounded_maintenance_runtime_config,
    parse_now_utc,
    render_sanitized_json,
    run_bounded_maintenance_due_retry_sync,
    run_bounded_maintenance_queue_once_sync,
)


ENV_FILE_ALLOWED_KEYS = {
    "APP_ENV",
    "DATABASE_URL",
    "REDIS_URL",
    "MAINTENANCE_QUEUE_NAME",
    "MAINTENANCE_CONSUMER_GROUP",
    "MAINTENANCE_CONSUMER_NAME",
    "REPLAY_QUEUE_NAME",
    "REPLAY_CONSUMER_GROUP",
    "REPLAY_CONSUMER_NAME",
    "MAINTENANCE_BATCH_SIZE",
    "MAINTENANCE_BLOCK_MS",
    "MAINTENANCE_NOTIFICATION_RETRY_POLL_SEC",
    "DELIVERY_RETRY_MAX_ATTEMPTS",
    "NOTIFICATION_RETRY_MAX_ATTEMPTS",
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION",
    "ENABLE_REPLAY_TO_PROD_DB",
}


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
        description="Preview or execute exact bounded maintenance delivery recovery operations.",
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--env-file")
    parser.add_argument("--mode", choices=("preview", "execute"), default="preview")
    subcommands = parser.add_subparsers(dest="command", required=True)

    maintenance = subcommands.add_parser(MAINTENANCE_RESULT_COMMAND, add_help=False)
    _add_redis_gates(maintenance)
    maintenance.add_argument("--trigger-event-suffix", required=True)
    maintenance.add_argument("--notification-plan-id-suffix", required=True)
    maintenance.add_argument("--redis-message-suffix", required=True)

    replay = subcommands.add_parser(REPLAY_REQUEST_COMMAND, add_help=False)
    _add_redis_gates(replay)
    replay.add_argument("--trigger-event-suffix", required=True)
    replay.add_argument("--replay-request-id-suffix", required=True)
    replay.add_argument("--redis-message-suffix", required=True)

    due_retry = subcommands.add_parser(DUE_RETRY_COMMAND, add_help=False)
    due_retry.add_argument("--limit", type=int, required=True)
    due_retry.add_argument("--now-utc")
    return parser


def _add_redis_gates(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--allow-redis-read", action="store_true")
    parser.add_argument("--allow-redis-consume", action="store_true")
    parser.add_argument("--allow-redis-ack", action="store_true")


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    queue_runtime_builder: BoundedMaintenanceQueueRuntimeBuilder | None = None,
    due_retry_runtime_builder: BoundedMaintenanceDueRetryRuntimeBuilder | None = None,
) -> RunnerResult:
    loader = _build_runtime_config_loader(args, runtime_config_loader=runtime_config_loader)
    if args.command == MAINTENANCE_RESULT_COMMAND:
        result = run_bounded_maintenance_queue_once_sync(
            BoundedMaintenanceQueueOnceConfig(
                command=MAINTENANCE_RESULT_COMMAND,
                operator_approved=bool(args.operator_approved),
                allow_runtime_config=bool(args.allow_runtime_config),
                allow_database_read=bool(args.allow_database_read),
                allow_database_write=bool(args.allow_database_write),
                allow_redis_read=bool(args.allow_redis_read),
                allow_redis_consume=bool(args.allow_redis_consume),
                allow_redis_ack=bool(args.allow_redis_ack),
                mode=str(args.mode),
                trigger_event_suffix=args.trigger_event_suffix,
                root_object_id_suffix=args.notification_plan_id_suffix,
                redis_message_id_suffix=args.redis_message_suffix,
            ),
            runtime_config_loader=loader,
            runtime_builder=queue_runtime_builder,
        )
    elif args.command == REPLAY_REQUEST_COMMAND:
        result = run_bounded_maintenance_queue_once_sync(
            BoundedMaintenanceQueueOnceConfig(
                command=REPLAY_REQUEST_COMMAND,
                operator_approved=bool(args.operator_approved),
                allow_runtime_config=bool(args.allow_runtime_config),
                allow_database_read=bool(args.allow_database_read),
                allow_database_write=bool(args.allow_database_write),
                allow_redis_read=bool(args.allow_redis_read),
                allow_redis_consume=bool(args.allow_redis_consume),
                allow_redis_ack=bool(args.allow_redis_ack),
                mode=str(args.mode),
                trigger_event_suffix=args.trigger_event_suffix,
                root_object_id_suffix=args.replay_request_id_suffix,
                redis_message_id_suffix=args.redis_message_suffix,
            ),
            runtime_config_loader=loader,
            runtime_builder=queue_runtime_builder,
        )
    else:
        try:
            now_utc = parse_now_utc(args.now_utc)
        except BoundedMaintenanceRuntimeError as exc:
            report = argument_error_report(exc.error_code, command=DUE_RETRY_COMMAND)
            report["command"] = DUE_RETRY_COMMAND
            return RunnerResult(exit_code=1, report=report)
        result = run_bounded_maintenance_due_retry_sync(
            BoundedMaintenanceDueRetryConfig(
                operator_approved=bool(args.operator_approved),
                allow_runtime_config=bool(args.allow_runtime_config),
                allow_database_read=bool(args.allow_database_read),
                allow_database_write=bool(args.allow_database_write),
                mode=str(args.mode),
                limit=int(args.limit),
                now_utc=now_utc,
            ),
            runtime_config_loader=loader,
            runtime_builder=due_retry_runtime_builder,
        )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    queue_runtime_builder: BoundedMaintenanceQueueRuntimeBuilder | None = None,
    due_retry_runtime_builder: BoundedMaintenanceDueRetryRuntimeBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(argument_error_report(str(exc))))
        return 1
    result = run(
        args,
        runtime_config_loader=runtime_config_loader,
        queue_runtime_builder=queue_runtime_builder,
        due_retry_runtime_builder=due_retry_runtime_builder,
    )
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


def _build_runtime_config_loader(args: argparse.Namespace, *, runtime_config_loader=None):
    if runtime_config_loader is not None and not getattr(args, "env_file", None):
        return runtime_config_loader

    def _load() -> BoundedMaintenanceRuntimeConfig:
        if runtime_config_loader is not None:
            base = runtime_config_loader()
            if not getattr(args, "env_file", None):
                return base
        overlay = _resolve_env_file_overlay(getattr(args, "env_file", None))
        if not overlay:
            return load_bounded_maintenance_runtime_config()
        merged = {key: os.environ[key] for key in ENV_FILE_ALLOWED_KEYS if key in os.environ}
        merged.update(overlay)
        return load_bounded_maintenance_runtime_config(merged)

    return _load


def _resolve_env_file_overlay(env_file: str | None) -> dict[str, str]:
    if not env_file:
        return {}
    path = Path(env_file)
    if not path.is_file():
        raise BoundedMaintenanceRuntimeError("env_file_missing")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise BoundedMaintenanceRuntimeError("env_file_missing") from exc

    values: dict[str, str] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in ENV_FILE_ALLOWED_KEYS:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    if not values:
        raise BoundedMaintenanceRuntimeError("env_file_no_runtime_config")
    return values


__all__ = [
    "CliArgumentError",
    "ENV_FILE_ALLOWED_KEYS",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
