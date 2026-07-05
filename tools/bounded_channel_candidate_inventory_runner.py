from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.collector_telegram.channel_candidate_inventory import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    ChannelCandidateInventoryConfig,
    ChannelCandidateInventoryRepositoryBuilder,
    ChannelCandidateInventoryResult,
    ChannelCandidateInventoryRuntimeConfig,
    argument_error_report,
    render_sanitized_json,
    run_channel_candidate_inventory_sync,
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
            "Read PostgreSQL channel registry/source-message aggregates and emit a sanitized "
            "public Telegram channel candidate inventory for F2 operator selection."
        ),
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-env-read", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--runtime-env-file", default=None)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    repository_builder: ChannelCandidateInventoryRepositoryBuilder | None = None,
) -> RunnerResult:
    runner_kwargs: dict[str, Any] = {
        "repository_builder": repository_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader
    result = run_channel_candidate_inventory_sync(
        ChannelCandidateInventoryConfig(
            operator_approved=bool(args.operator_approved),
            allow_runtime_env_read=bool(args.allow_runtime_env_read),
            allow_database_read=bool(args.allow_database_read),
            runtime_env_file=args.runtime_env_file,
            limit=int(args.limit),
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    repository_builder: ChannelCandidateInventoryRepositoryBuilder | None = None,
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
    )
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


__all__ = [
    "ChannelCandidateInventoryConfig",
    "ChannelCandidateInventoryRepositoryBuilder",
    "ChannelCandidateInventoryResult",
    "ChannelCandidateInventoryRuntimeConfig",
    "CliArgumentError",
    "MAX_LIMIT",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
