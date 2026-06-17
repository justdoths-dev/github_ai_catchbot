from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.notifier_telegram.idempotency_readback import (
    BoundedNotifierIdempotencyReadbackConfig,
    BoundedNotifierIdempotencyReadbackResult,
    BoundedNotifierIdempotencyRepositoryBuilder,
    BoundedNotifierIdempotencyRuntimeConfig,
    argument_error_report,
    load_bounded_notifier_idempotency_runtime_config,
    render_sanitized_json,
    run_bounded_notifier_idempotency_readback_sync,
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
        description="Read notifier idempotency state for one notification intent without Redis or Telegram.",
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--event-suffix")
    parser.add_argument("--analysis-suffix")
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=load_bounded_notifier_idempotency_runtime_config,
    repository_builder: BoundedNotifierIdempotencyRepositoryBuilder | None = None,
) -> RunnerResult:
    result = run_bounded_notifier_idempotency_readback_sync(
        BoundedNotifierIdempotencyReadbackConfig(
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            event_suffix=args.event_suffix,
            analysis_suffix=args.analysis_suffix,
        ),
        runtime_config_loader=runtime_config_loader,
        repository_builder=repository_builder,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=load_bounded_notifier_idempotency_runtime_config,
    repository_builder: BoundedNotifierIdempotencyRepositoryBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(argument_error_report(str(exc))))
        return 1
    result = run(args, runtime_config_loader=runtime_config_loader, repository_builder=repository_builder)
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


__all__ = [
    "BoundedNotifierIdempotencyReadbackConfig",
    "BoundedNotifierIdempotencyReadbackResult",
    "BoundedNotifierIdempotencyRepositoryBuilder",
    "BoundedNotifierIdempotencyRuntimeConfig",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
