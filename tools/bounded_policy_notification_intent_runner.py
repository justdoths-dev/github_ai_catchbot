from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.policy_engine.bounded_notification_intent import (
    BoundedPolicyNotificationIntentConfig,
    BoundedPolicyNotificationIntentResult,
    BoundedPolicyNotificationIntentRuntimeBuilder,
    PolicyEngineConfig,
    argument_error_report,
    render_sanitized_json,
    run_bounded_policy_notification_intent_sync,
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
        description="Apply exactly one pending analysis.policy.apply.v1 event through policy-engine.",
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-policy-write", action="store_true")
    parser.add_argument("--expected-pending-count", type=int, default=1)
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    runtime_builder: BoundedPolicyNotificationIntentRuntimeBuilder | None = None,
) -> RunnerResult:
    runner_kwargs: dict[str, Any] = {
        "runtime_builder": runtime_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader
    result = run_bounded_policy_notification_intent_sync(
        BoundedPolicyNotificationIntentConfig(
            operator_approved=bool(args.operator_approved),
            allow_database_read=bool(args.allow_database_read),
            allow_policy_write=bool(args.allow_policy_write),
            expected_pending_count=int(args.expected_pending_count),
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    runtime_builder: BoundedPolicyNotificationIntentRuntimeBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(argument_error_report(str(exc))))
        return 1
    result = run(
        args,
        runtime_config_loader=runtime_config_loader,
        runtime_builder=runtime_builder,
    )
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


__all__ = [
    "BoundedPolicyNotificationIntentConfig",
    "BoundedPolicyNotificationIntentResult",
    "BoundedPolicyNotificationIntentRuntimeBuilder",
    "CliArgumentError",
    "PolicyEngineConfig",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
