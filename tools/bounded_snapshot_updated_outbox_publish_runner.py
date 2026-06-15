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

from src.services.outbox_relay.bounded_snapshot_updated_outbox_publish_runner import (
    BoundedSnapshotUpdatedOutboxPublishConfig,
    BoundedSnapshotUpdatedOutboxPublishResult,
    BoundedSnapshotUpdatedPublishRuntimeConfig,
    BoundedSnapshotUpdatedRedisPublisherBuilder,
    BoundedSnapshotUpdatedRepositoryBuilder,
    argument_error_report,
    render_sanitized_json,
    run_bounded_snapshot_updated_outbox_publish_sync,
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
        description="Publish exactly one pending artifact.snapshot.updated.v1 outbox row to q.candidate.bundle.",
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-redis-publish", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--event-id")
    parser.add_argument("--artifact-id")
    parser.add_argument("--event-suffix")
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    repository_builder: BoundedSnapshotUpdatedRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedSnapshotUpdatedRedisPublisherBuilder | None = None,
) -> RunnerResult:
    event_id = _parse_optional_uuid(args.event_id, "invalid_event_id")
    artifact_id = _parse_optional_uuid(args.artifact_id, "invalid_artifact_id")
    event_suffix = _parse_optional_event_suffix(args.event_suffix)
    if isinstance(event_id, dict):
        return RunnerResult(exit_code=1, report=event_id)
    if isinstance(artifact_id, dict):
        return RunnerResult(exit_code=1, report=artifact_id)
    if isinstance(event_suffix, dict):
        return RunnerResult(exit_code=1, report=event_suffix)

    runner_kwargs: dict[str, Any] = {
        "repository_builder": repository_builder,
        "redis_publisher_builder": redis_publisher_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader
    result = run_bounded_snapshot_updated_outbox_publish_sync(
        BoundedSnapshotUpdatedOutboxPublishConfig(
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_redis_publish=bool(args.allow_redis_publish),
            allow_database_write=bool(args.allow_database_write),
            event_id=event_id,
            artifact_id=artifact_id,
            event_suffix=event_suffix,
            max_events=1,
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    repository_builder: BoundedSnapshotUpdatedRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedSnapshotUpdatedRedisPublisherBuilder | None = None,
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


def _parse_optional_event_suffix(value: str | None) -> str | None | dict[str, Any]:
    if value is None:
        return None
    stripped = value.strip().lower()
    if 4 <= len(stripped) <= 36 and all(char in "0123456789abcdef-" for char in stripped):
        return stripped
    return argument_error_report("invalid_event_suffix")


__all__ = [
    "BoundedSnapshotUpdatedOutboxPublishConfig",
    "BoundedSnapshotUpdatedOutboxPublishResult",
    "BoundedSnapshotUpdatedPublishRuntimeConfig",
    "BoundedSnapshotUpdatedRedisPublisherBuilder",
    "BoundedSnapshotUpdatedRepositoryBuilder",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
