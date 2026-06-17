from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.policy_engine.bounded_policy_apply_inventory import (
    BoundedPolicyApplyInventoryConfig,
    BoundedPolicyApplyInventoryRedisReaderBuilder,
    BoundedPolicyApplyInventoryRepositoryBuilder,
    BoundedPolicyApplyInventoryResult,
    BoundedPolicyApplyInventoryRuntimeConfig,
    argument_error_report,
    render_sanitized_json,
    run_bounded_policy_apply_inventory_sync,
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
            "Read PostgreSQL analysis.policy.apply.v1 outbox inventory and correlate with q.analysis.policy "
            "messages without database writes, Redis publish, Redis consume, or policy-engine execution."
        ),
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-redis-read", action="store_true")
    parser.add_argument("--allow-policy-preview", action="store_true")
    parser.add_argument("--db-limit", type=int, default=100)
    parser.add_argument("--redis-scan-limit", type=int, default=100)
    parser.add_argument("--max-results", type=int, default=10)
    parser.add_argument("--prefer-verdict", default="any")
    parser.add_argument("--include-processed", action="store_true")
    parser.add_argument("--include-suppressed", action="store_true")
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    redis_reader_builder: BoundedPolicyApplyInventoryRedisReaderBuilder | None = None,
    repository_builder: BoundedPolicyApplyInventoryRepositoryBuilder | None = None,
) -> RunnerResult:
    runner_kwargs: dict[str, Any] = {
        "redis_reader_builder": redis_reader_builder,
        "repository_builder": repository_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader

    result = run_bounded_policy_apply_inventory_sync(
        BoundedPolicyApplyInventoryConfig(
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            allow_redis_read=bool(args.allow_redis_read),
            allow_policy_preview=bool(args.allow_policy_preview),
            db_limit=int(args.db_limit),
            redis_scan_limit=int(args.redis_scan_limit),
            max_results=int(args.max_results),
            prefer_verdict=str(args.prefer_verdict),
            include_processed=bool(args.include_processed),
            include_suppressed=bool(args.include_suppressed),
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    redis_reader_builder: BoundedPolicyApplyInventoryRedisReaderBuilder | None = None,
    repository_builder: BoundedPolicyApplyInventoryRepositoryBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(argument_error_report(str(exc))))
        return 1
    result = run(
        args,
        runtime_config_loader=runtime_config_loader,
        redis_reader_builder=redis_reader_builder,
        repository_builder=repository_builder,
    )
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


__all__ = [
    "BoundedPolicyApplyInventoryConfig",
    "BoundedPolicyApplyInventoryRedisReaderBuilder",
    "BoundedPolicyApplyInventoryRepositoryBuilder",
    "BoundedPolicyApplyInventoryResult",
    "BoundedPolicyApplyInventoryRuntimeConfig",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
