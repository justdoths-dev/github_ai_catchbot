from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.outbox_relay.bounded_analysis_requested_outbox_publish_runner import (
    BoundedAnalysisRequestedOutboxPublishConfig,
    BoundedAnalysisRequestedOutboxPublishResult,
    BoundedAnalysisRequestedPublishRuntimeConfig,
    BoundedAnalysisRequestedRedisInspectorBuilder,
    BoundedAnalysisRequestedRedisPublisherBuilder,
    BoundedAnalysisRequestedRepositoryBuilder,
    argument_error_report,
    render_sanitized_json,
    run_bounded_analysis_requested_outbox_publish_sync,
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
        description=(
            "Preview or publish one exact analysis.requested.v1 outbox row "
            "to q.analysis.route through the outbox relay boundary."
        ),
        add_help=False,
    )
    parser.add_argument("--mode", choices=("preview", "publish"), default="preview")
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-redis-read", action="store_true")
    parser.add_argument("--allow-redis-publish", action="store_true")
    parser.add_argument("--allow-outbox-status-update", action="store_true")
    parser.add_argument("--event-type")
    parser.add_argument("--event-suffix")
    parser.add_argument("--aggregate-suffix")
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    repository_builder: BoundedAnalysisRequestedRepositoryBuilder | None = None,
    redis_inspector_builder: BoundedAnalysisRequestedRedisInspectorBuilder | None = None,
    redis_publisher_builder: BoundedAnalysisRequestedRedisPublisherBuilder | None = None,
) -> RunnerResult:
    runner_kwargs: dict[str, Any] = {
        "repository_builder": repository_builder,
        "redis_inspector_builder": redis_inspector_builder,
        "redis_publisher_builder": redis_publisher_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader
    result = run_bounded_analysis_requested_outbox_publish_sync(
        BoundedAnalysisRequestedOutboxPublishConfig(
            mode=str(args.mode),
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            allow_redis_read=bool(args.allow_redis_read),
            allow_redis_publish=bool(args.allow_redis_publish),
            allow_outbox_status_update=bool(args.allow_outbox_status_update),
            event_type=args.event_type,
            event_suffix=_clean_optional_suffix(args.event_suffix),
            aggregate_suffix=_clean_optional_suffix(args.aggregate_suffix),
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
    repository_builder: BoundedAnalysisRequestedRepositoryBuilder | None = None,
    redis_inspector_builder: BoundedAnalysisRequestedRedisInspectorBuilder | None = None,
    redis_publisher_builder: BoundedAnalysisRequestedRedisPublisherBuilder | None = None,
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
        redis_inspector_builder=redis_inspector_builder,
        redis_publisher_builder=redis_publisher_builder,
    )
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


def _clean_optional_suffix(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip().lower()


__all__ = [
    "BoundedAnalysisRequestedOutboxPublishConfig",
    "BoundedAnalysisRequestedOutboxPublishResult",
    "BoundedAnalysisRequestedPublishRuntimeConfig",
    "BoundedAnalysisRequestedRedisInspectorBuilder",
    "BoundedAnalysisRequestedRedisPublisherBuilder",
    "BoundedAnalysisRequestedRepositoryBuilder",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
