from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.evidence_assembler.bounded_bundle_assembler_runner import (
    BoundedBundleAssemblerConfig,
    BoundedBundleAssemblerDatabaseBuilder,
    BoundedBundleAssemblerRedisBuilder,
    BoundedBundleAssemblerResult,
    BoundedBundleAssemblerRuntimeConfig,
    argument_error_report,
    render_sanitized_json,
    run_bounded_bundle_assembler_sync,
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
        description="Assemble exactly one target q.candidate.bundle evidence bundle job.",
        add_help=False,
    )
    parser.add_argument("--mode", default="preview")
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-database-write-for-evidence-bundle-only", action="store_true")
    parser.add_argument("--allow-redis-read", action="store_true")
    parser.add_argument("--allow-redis-consume", action="store_true")
    parser.add_argument("--allow-redis-group-create", action="store_true")
    parser.add_argument("--allow-redis-group-destroy", action="store_true")
    parser.add_argument("--allow-redis-ack", action="store_true")
    parser.add_argument("--queue-name")
    parser.add_argument("--redis-message-id-suffix")
    parser.add_argument("--trigger-event-suffix")
    parser.add_argument("--candidate-group-suffix")
    parser.add_argument("--max-messages", type=int, default=1)
    parser.add_argument("--scan-limit", type=int, default=25)
    parser.add_argument("--candidate-fanout-limit", type=int, default=25)
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    redis_builder: BoundedBundleAssemblerRedisBuilder | None = None,
    database_builder: BoundedBundleAssemblerDatabaseBuilder | None = None,
) -> RunnerResult:
    trigger_event_suffix = _parse_optional_trigger_event_suffix(args.trigger_event_suffix)
    candidate_group_suffix = _parse_optional_candidate_group_suffix(args.candidate_group_suffix)
    redis_message_id_suffix = _parse_optional_redis_message_id_suffix(args.redis_message_id_suffix)
    if isinstance(trigger_event_suffix, dict):
        return RunnerResult(exit_code=1, report=trigger_event_suffix)
    if isinstance(candidate_group_suffix, dict):
        return RunnerResult(exit_code=1, report=candidate_group_suffix)
    if isinstance(redis_message_id_suffix, dict):
        return RunnerResult(exit_code=1, report=redis_message_id_suffix)

    runner_kwargs: dict[str, Any] = {
        "redis_builder": redis_builder,
        "database_builder": database_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader
    result = run_bounded_bundle_assembler_sync(
        BoundedBundleAssemblerConfig(
            run_mode=str(args.mode),
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            allow_database_write_for_evidence_bundle_only=bool(
                args.allow_database_write_for_evidence_bundle_only
            ),
            allow_redis_read=bool(args.allow_redis_read),
            allow_redis_consume=bool(args.allow_redis_consume),
            allow_redis_group_create=bool(args.allow_redis_group_create),
            allow_redis_group_destroy=bool(args.allow_redis_group_destroy),
            allow_redis_ack=bool(args.allow_redis_ack),
            queue_name=args.queue_name,
            redis_message_id_suffix=redis_message_id_suffix,
            trigger_event_suffix=trigger_event_suffix,
            candidate_group_suffix=candidate_group_suffix,
            max_messages=int(args.max_messages),
            scan_limit=int(args.scan_limit),
            candidate_fanout_limit=int(args.candidate_fanout_limit),
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    redis_builder: BoundedBundleAssemblerRedisBuilder | None = None,
    database_builder: BoundedBundleAssemblerDatabaseBuilder | None = None,
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


def _parse_optional_trigger_event_suffix(value: str | None) -> str | None | dict[str, Any]:
    if value is None:
        return None
    stripped = value.strip().lower()
    if 4 <= len(stripped) <= 36 and all(char in "0123456789abcdef-" for char in stripped):
        return stripped
    return argument_error_report("invalid_trigger_event_suffix")


def _parse_optional_candidate_group_suffix(value: str | None) -> str | None | dict[str, Any]:
    if value is None:
        return None
    stripped = value.strip().lower()
    if 4 <= len(stripped) <= 36 and all(char in "0123456789abcdef-" for char in stripped):
        return stripped
    return argument_error_report("invalid_candidate_group_suffix")


def _parse_optional_redis_message_id_suffix(value: str | None) -> str | None | dict[str, Any]:
    if value is None:
        return None
    stripped = value.strip()
    if 3 <= len(stripped) <= 64 and all(char in "0123456789-" for char in stripped):
        return stripped
    return argument_error_report("invalid_redis_message_id_suffix")


__all__ = [
    "BoundedBundleAssemblerConfig",
    "BoundedBundleAssemblerDatabaseBuilder",
    "BoundedBundleAssemblerRedisBuilder",
    "BoundedBundleAssemblerResult",
    "BoundedBundleAssemblerRuntimeConfig",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
