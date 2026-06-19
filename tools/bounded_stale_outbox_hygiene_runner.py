from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.maintenance.bounded_stale_outbox_hygiene_runner import (
    ALL_CLASSIFICATIONS,
    BoundedStaleOutboxHygieneConfig,
    BoundedStaleOutboxHygieneRepositoryBuilder,
    BoundedStaleOutboxHygieneRuntimeConfig,
    argument_error_report,
    load_bounded_stale_outbox_hygiene_runtime_config,
    render_sanitized_json,
    run_bounded_stale_outbox_hygiene_sync,
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
        description="Inventory, plan, execute, or prove bounded stale outbox hygiene.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=("inventory", "plan", "execute", "proof"), default="inventory")
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--target-event-suffix", action="append", default=[])
    parser.add_argument("--classification", action="append", default=[], choices=tuple(sorted(ALL_CLASSIFICATIONS)))
    parser.add_argument("--scan-limit", type=int, default=100)
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    repository_builder: BoundedStaleOutboxHygieneRepositoryBuilder | None = None,
) -> RunnerResult:
    loader = runtime_config_loader or load_bounded_stale_outbox_hygiene_runtime_config
    result = run_bounded_stale_outbox_hygiene_sync(
        BoundedStaleOutboxHygieneConfig(
            mode=args.mode,
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            allow_database_write=bool(args.allow_database_write),
            target_event_suffixes=tuple(args.target_event_suffix or ()),
            classifications=tuple(args.classification or ()),
            scan_limit=int(args.scan_limit),
        ),
        runtime_config_loader=loader,
        repository_builder=repository_builder,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    repository_builder: BoundedStaleOutboxHygieneRepositoryBuilder | None = None,
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
    "BoundedStaleOutboxHygieneConfig",
    "BoundedStaleOutboxHygieneRuntimeConfig",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
