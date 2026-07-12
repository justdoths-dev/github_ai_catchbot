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

from src.services.analysis_router.bounded_analysis_router_runner import (
    BoundedAnalysisRouterConfig,
    BoundedAnalysisRouterDatabaseBuilder,
    BoundedAnalysisRouterRedisBuilder,
    BoundedAnalysisRouterResult,
    BoundedAnalysisRouterRuntimeConfig,
    argument_error_report,
    render_sanitized_json,
    run_bounded_analysis_router_sync,
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
        description="Preview or execute exactly one target q.analysis.route analysis-router job.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=("preview", "execute"), default="preview")
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-redis-read", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-redis-consume", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-redis-ack", action="store_true")
    parser.add_argument("--allow-unrelated-pending-preservation", action="store_true")
    parser.add_argument("--trigger-event-id")
    parser.add_argument("--trigger-event-suffix")
    parser.add_argument("--candidate-group-suffix", "--aggregate-suffix", dest="candidate_group_suffix")
    parser.add_argument("--redis-message-id")
    parser.add_argument("--max-messages", type=int, default=1)
    parser.add_argument("--scan-limit", type=int, default=25)
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    redis_builder: BoundedAnalysisRouterRedisBuilder | None = None,
    database_builder: BoundedAnalysisRouterDatabaseBuilder | None = None,
) -> RunnerResult:
    if args.trigger_event_id is not None:
        return RunnerResult(
            exit_code=1,
            report=argument_error_report("full_trigger_event_id_selector_not_allowed"),
        )
    if args.redis_message_id is not None:
        return RunnerResult(
            exit_code=1,
            report=argument_error_report("full_redis_message_id_selector_not_allowed"),
        )

    trigger_event_id = _parse_optional_uuid(args.trigger_event_id, "invalid_trigger_event_id")
    trigger_event_suffix = _parse_optional_event_suffix(args.trigger_event_suffix, "invalid_trigger_event_suffix")
    candidate_group_suffix = _parse_optional_event_suffix(
        args.candidate_group_suffix,
        "invalid_candidate_group_suffix",
    )
    if isinstance(trigger_event_id, dict):
        return RunnerResult(exit_code=1, report=trigger_event_id)
    if isinstance(trigger_event_suffix, dict):
        return RunnerResult(exit_code=1, report=trigger_event_suffix)
    if isinstance(candidate_group_suffix, dict):
        return RunnerResult(exit_code=1, report=candidate_group_suffix)

    runner_kwargs: dict[str, Any] = {
        "redis_builder": redis_builder,
        "database_builder": database_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader
    result = run_bounded_analysis_router_sync(
        BoundedAnalysisRouterConfig(
            mode=str(args.mode),
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_redis_read=bool(args.allow_redis_read),
            allow_database_read=bool(args.allow_database_read),
            allow_redis_consume=bool(args.allow_redis_consume),
            allow_database_write=bool(args.allow_database_write),
            allow_redis_ack=bool(args.allow_redis_ack),
            allow_unrelated_pending_preservation=bool(args.allow_unrelated_pending_preservation),
            trigger_event_id=trigger_event_id,
            trigger_event_suffix=trigger_event_suffix,
            candidate_group_suffix=candidate_group_suffix,
            redis_message_id=args.redis_message_id,
            max_messages=int(args.max_messages),
            scan_limit=int(args.scan_limit),
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    redis_builder: BoundedAnalysisRouterRedisBuilder | None = None,
    database_builder: BoundedAnalysisRouterDatabaseBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(argument_error_report(str(exc))))
        return 1
    result = run(
        args,
        runtime_config_loader=runtime_config_loader,
        redis_builder=redis_builder,
        database_builder=database_builder,
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


def _parse_optional_event_suffix(value: str | None, error_code: str) -> str | None | dict[str, Any]:
    if value is None:
        return None
    stripped = value.strip().lower()
    if 4 <= len(stripped) <= 36 and all(char in "0123456789abcdef-" for char in stripped):
        return stripped
    return argument_error_report(error_code)


__all__ = [
    "BoundedAnalysisRouterConfig",
    "BoundedAnalysisRouterDatabaseBuilder",
    "BoundedAnalysisRouterRedisBuilder",
    "BoundedAnalysisRouterResult",
    "BoundedAnalysisRouterRuntimeConfig",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
