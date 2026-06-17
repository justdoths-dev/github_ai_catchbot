from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.policy_engine.bounded_notification_intent_recovery import (
    BoundedNotificationIntentRecoveryConfig,
    BoundedNotificationIntentRecoveryRedisPublisherBuilder,
    BoundedNotificationIntentRecoveryRepositoryBuilder,
    BoundedNotificationIntentRecoveryResult,
    BoundedNotificationIntentRecoveryRuntimeConfig,
    argument_error_report,
    render_sanitized_json,
    run_bounded_notification_intent_recovery_sync,
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
        description="Recover a missing notification.plan.created.v1 intent for one existing analysis.",
        add_help=False,
    )
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-policy-preview", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-notification-intent-write", action="store_true")
    parser.add_argument("--require-notification-send-enabled", action="store_true")
    parser.add_argument("--allow-redis-read", action="store_true")
    parser.add_argument("--allow-redis-publish", action="store_true")
    parser.add_argument("--allow-notification-send-queue-publish", action="store_true")
    parser.add_argument("--policy-apply-event-suffix")
    parser.add_argument("--judge-run-suffix")
    parser.add_argument("--judge-output-suffix")
    parser.add_argument("--bundle-suffix")
    parser.add_argument("--candidate-group-suffix")
    parser.add_argument("--analysis-suffix")
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    repository_builder: BoundedNotificationIntentRecoveryRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedNotificationIntentRecoveryRedisPublisherBuilder | None = None,
) -> RunnerResult:
    runner_kwargs: dict[str, Any] = {
        "repository_builder": repository_builder,
        "redis_publisher_builder": redis_publisher_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader

    result = run_bounded_notification_intent_recovery_sync(
        BoundedNotificationIntentRecoveryConfig(
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            allow_policy_preview=bool(args.allow_policy_preview),
            allow_database_write=bool(args.allow_database_write),
            allow_notification_intent_write=bool(args.allow_notification_intent_write),
            require_notification_send_enabled=bool(args.require_notification_send_enabled),
            allow_redis_read=bool(args.allow_redis_read),
            allow_redis_publish=bool(args.allow_redis_publish),
            allow_notification_send_queue_publish=bool(args.allow_notification_send_queue_publish),
            policy_apply_event_suffix=args.policy_apply_event_suffix,
            judge_run_suffix=args.judge_run_suffix,
            judge_output_suffix=args.judge_output_suffix,
            bundle_suffix=args.bundle_suffix,
            candidate_group_suffix=args.candidate_group_suffix,
            analysis_suffix=args.analysis_suffix,
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    repository_builder: BoundedNotificationIntentRecoveryRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedNotificationIntentRecoveryRedisPublisherBuilder | None = None,
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
        redis_publisher_builder=redis_publisher_builder,
    )
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


__all__ = [
    "BoundedNotificationIntentRecoveryConfig",
    "BoundedNotificationIntentRecoveryResult",
    "BoundedNotificationIntentRecoveryRuntimeConfig",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
