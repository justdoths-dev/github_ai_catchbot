from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence
from uuid import UUID


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.outbox_relay.bounded_source_message_outbox_publish_runner import (
    BoundedSourceMessageOutboxPublishConfig,
    BoundedSourceMessageOutboxPublishResult,
    BoundedSourceMessagePublishRuntimeConfig,
    BoundedSourceMessageRedisPublisherBuilder,
    BoundedSourceMessageRepositoryBuilder,
    argument_error_report,
    render_sanitized_json,
    run_bounded_source_message_outbox_publish_sync,
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
        description="Publish exactly one pending source_message outbox row to q.source.normalize.",
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-redis-publish", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--event-id")
    parser.add_argument("--source-message-id")
    parser.add_argument("--max-events", type=int, default=1)
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    repository_builder: BoundedSourceMessageRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedSourceMessageRedisPublisherBuilder | None = None,
) -> RunnerResult:
    event_id = _parse_optional_uuid(args.event_id, "invalid_event_id")
    source_message_id = _parse_optional_uuid(args.source_message_id, "invalid_source_message_id")
    if isinstance(event_id, dict):
        return RunnerResult(exit_code=1, report=event_id)
    if isinstance(source_message_id, dict):
        return RunnerResult(exit_code=1, report=source_message_id)

    runner_kwargs: dict[str, Any] = {
        "repository_builder": repository_builder,
        "redis_publisher_builder": redis_publisher_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader
    result = run_bounded_source_message_outbox_publish_sync(
        BoundedSourceMessageOutboxPublishConfig(
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_redis_publish=bool(args.allow_redis_publish),
            allow_database_write=bool(args.allow_database_write),
            event_id=event_id,
            source_message_id=source_message_id,
            max_events=int(args.max_events),
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    repository_builder: BoundedSourceMessageRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedSourceMessageRedisPublisherBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(argument_error_report(str(exc))))
        return 1
    result = run(
        args,
        runtime_config_loader=runtime_config_loader,
        repository_builder=repository_builder,
        redis_publisher_builder=redis_publisher_builder,
    )
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


def _parse_optional_uuid(value: str | None, error_code: str) -> UUID | None | dict[str, Any]:
    if value is None:
        return None
    try:
        return UUID(value)
    except ValueError:
        return argument_error_report(error_code)


__all__ = [
    "BoundedSourceMessageOutboxPublishConfig",
    "BoundedSourceMessageOutboxPublishResult",
    "BoundedSourceMessagePublishRuntimeConfig",
    "BoundedSourceMessageRedisPublisherBuilder",
    "BoundedSourceMessageRepositoryBuilder",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
