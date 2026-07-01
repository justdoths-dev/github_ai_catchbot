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

from src.services.maintenance.bounded_candidate_bundle_refresh_event_publish_runner import (
    BoundedCandidateBundleRefreshEventPublishConfig,
    BoundedCandidateBundleRefreshEventPublishResult,
    BoundedCandidateBundleRefreshPublishRuntimeConfig,
    CandidateBundleRefreshEventRepositoryBuilder,
    CONFIRM_TOKEN,
    argument_error_report,
    render_sanitized_json,
    run_bounded_candidate_bundle_refresh_event_publish_sync,
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
        description="Materialize and publish exactly one candidate.bundle.refresh.v1 event.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=("plan", "execute"), required=True)
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-redis-publish", action="store_true")
    parser.add_argument("--candidate-group-id")
    parser.add_argument("--candidate-group-suffix")
    parser.add_argument("--bundle-id")
    parser.add_argument("--refresh-reason", required=True)
    parser.add_argument("--confirm")
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    repository_builder: CandidateBundleRefreshEventRepositoryBuilder | None = None,
    publisher_runner=None,
) -> RunnerResult:
    candidate_group_id = _parse_optional_uuid(args.candidate_group_id, "invalid_candidate_group_id")
    bundle_id = _parse_optional_uuid(args.bundle_id, "invalid_bundle_id")
    candidate_group_suffix = _parse_optional_suffix(args.candidate_group_suffix)
    refresh_reason = _parse_refresh_reason(args.refresh_reason)

    for parsed in (candidate_group_id, bundle_id, candidate_group_suffix, refresh_reason):
        if isinstance(parsed, dict):
            return RunnerResult(exit_code=1, report=parsed)

    runner_kwargs: dict[str, Any] = {"repository_builder": repository_builder}
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader
    if publisher_runner is not None:
        runner_kwargs["publisher_runner"] = publisher_runner

    result = run_bounded_candidate_bundle_refresh_event_publish_sync(
        BoundedCandidateBundleRefreshEventPublishConfig(
            mode=str(args.mode),
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            allow_database_write=bool(args.allow_database_write),
            allow_redis_publish=bool(args.allow_redis_publish),
            candidate_group_id=candidate_group_id,
            candidate_group_suffix=candidate_group_suffix,
            bundle_id=bundle_id,
            refresh_reason=refresh_reason,
            confirm=args.confirm,
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    repository_builder: CandidateBundleRefreshEventRepositoryBuilder | None = None,
    publisher_runner=None,
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
        publisher_runner=publisher_runner,
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


def _parse_optional_suffix(value: str | None) -> str | None | dict[str, Any]:
    if value is None:
        return None
    stripped = value.strip().lower()
    if 4 <= len(stripped) <= 36 and all(char in "0123456789abcdef-" for char in stripped):
        return stripped
    return argument_error_report("invalid_candidate_group_suffix")


def _parse_refresh_reason(value: str | None) -> str | dict[str, Any]:
    if value is None:
        return argument_error_report("invalid_refresh_reason")
    stripped = value.strip()
    if 1 <= len(stripped) <= 80 and all(char.islower() or char.isdigit() or char in "_-" for char in stripped):
        return stripped
    return argument_error_report("invalid_refresh_reason")


__all__ = [
    "BoundedCandidateBundleRefreshEventPublishConfig",
    "BoundedCandidateBundleRefreshEventPublishResult",
    "BoundedCandidateBundleRefreshPublishRuntimeConfig",
    "CandidateBundleRefreshEventRepositoryBuilder",
    "CliArgumentError",
    "CONFIRM_TOKEN",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
