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

from src.services.web_enricher.bounded_web_enrich_runner import (
    BoundedWebEnrichConfig,
    BoundedWebEnrichDatabaseBuilder,
    BoundedWebEnrichRedisBuilder,
    BoundedWebEnrichResult,
    BoundedWebEnrichRuntimeConfig,
    argument_error_report,
    render_sanitized_json,
    run_bounded_web_enrich_sync,
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
        description="Process exactly one target q.artifact.enrich.web job.",
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-redis-consume", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-redis-ack", action="store_true")
    parser.add_argument("--allow-web-fetch", action="store_true")
    parser.add_argument("--trigger-event-id")
    parser.add_argument("--artifact-id")
    parser.add_argument("--redis-message-id")
    parser.add_argument("--max-messages", type=int, default=1)
    parser.add_argument("--scan-limit", type=int, default=25)
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    redis_builder: BoundedWebEnrichRedisBuilder | None = None,
    database_builder: BoundedWebEnrichDatabaseBuilder | None = None,
) -> RunnerResult:
    trigger_event_id = _parse_optional_uuid(args.trigger_event_id, "invalid_trigger_event_id")
    artifact_id = _parse_optional_uuid(args.artifact_id, "invalid_artifact_id")
    if isinstance(trigger_event_id, dict):
        return RunnerResult(exit_code=1, report=trigger_event_id)
    if isinstance(artifact_id, dict):
        return RunnerResult(exit_code=1, report=artifact_id)

    runner_kwargs: dict[str, Any] = {
        "redis_builder": redis_builder,
        "database_builder": database_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader
    result = run_bounded_web_enrich_sync(
        BoundedWebEnrichConfig(
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_redis_consume=bool(args.allow_redis_consume),
            allow_database_write=bool(args.allow_database_write),
            allow_redis_ack=bool(args.allow_redis_ack),
            allow_web_fetch=bool(args.allow_web_fetch),
            trigger_event_id=trigger_event_id,
            artifact_id=artifact_id,
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
    redis_builder: BoundedWebEnrichRedisBuilder | None = None,
    database_builder: BoundedWebEnrichDatabaseBuilder | None = None,
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


__all__ = [
    "BoundedWebEnrichConfig",
    "BoundedWebEnrichDatabaseBuilder",
    "BoundedWebEnrichRedisBuilder",
    "BoundedWebEnrichResult",
    "BoundedWebEnrichRuntimeConfig",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
