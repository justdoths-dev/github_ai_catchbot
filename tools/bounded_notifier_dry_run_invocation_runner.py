from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.notifier_telegram.bounded_invocation import (
    BoundedNotifierDryRunInvocationConfig,
    BoundedNotifierDryRunInvocationResult,
    BoundedNotifierRuntimeBuilder,
    load_forced_dry_run_notifier_config,
    render_sanitized_json,
    run_bounded_notifier_dry_run_invocation_sync,
)
from src.services.notifier_telegram.config import NotifierTelegramConfig


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
        description="Invoke one notifier notification.plan.created.v1 event in forced dry-run/send-disabled mode.",
        add_help=True,
    )
    parser.add_argument("--trigger-event-id")
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    return parser


def run(
    args: argparse.Namespace,
    *,
    notifier_config_loader=load_forced_dry_run_notifier_config,
    runtime_builder: BoundedNotifierRuntimeBuilder | None = None,
) -> RunnerResult:
    result = run_bounded_notifier_dry_run_invocation_sync(
        BoundedNotifierDryRunInvocationConfig(
            trigger_event_id=args.trigger_event_id,
            operator_approved=bool(args.operator_approved),
            allow_database_write=bool(args.allow_database_write),
        ),
        notifier_config_loader=notifier_config_loader,
        runtime_builder=runtime_builder,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def _argument_error_report(error_code: str) -> dict[str, Any]:
    result = BoundedNotifierDryRunInvocationResult(
        status="blocked",
        ok=False,
        error_code=error_code,
        trigger_event_id_present=False,
        operator_approved=False,
        database_write_allowed=False,
        processed_event_count=0,
    )
    return result.to_sanitized_dict()


def main(
    argv: Sequence[str] | None = None,
    *,
    notifier_config_loader=load_forced_dry_run_notifier_config,
    runtime_builder: BoundedNotifierRuntimeBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(_argument_error_report(str(exc))))
        return 1
    result = run(args, notifier_config_loader=notifier_config_loader, runtime_builder=runtime_builder)
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


__all__ = [
    "BoundedNotifierDryRunInvocationConfig",
    "BoundedNotifierRuntimeBuilder",
    "CliArgumentError",
    "NotifierTelegramConfig",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
