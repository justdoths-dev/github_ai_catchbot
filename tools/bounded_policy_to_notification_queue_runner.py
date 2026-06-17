from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.policy_engine.bounded_policy_to_notification_queue_runner import (
    BoundedPolicyToNotificationQueueConfig,
    BoundedPolicyToNotificationQueueResult,
    NotificationQueuePublishRepositoryBuilder,
    NotificationQueueRedisPublisherBuilder,
    argument_error_report,
    render_sanitized_json,
    run_bounded_policy_to_notification_queue_sync,
)
from src.services.policy_engine.bounded_policy_apply_runner import (
    BoundedPolicyApplyRuntimeConfig,
    RedisBuilder,
    RepositoryBuilder,
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
            "Apply one exact analysis.policy.apply.v1 target and, for non-suppress policy output, "
            "publish its exact notification.plan.created.v1 intent to q.notification.send."
        ),
        add_help=False,
    )
    parser.add_argument("--mode", choices=("preview", "execute"), default="preview")
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-redis-read", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-redis-consume", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-redis-ack", action="store_true")
    parser.add_argument("--allow-notification-outbox-publish", action="store_true")
    parser.add_argument("--allow-notification-redis-publish", action="store_true")
    parser.add_argument("--allow-redis-group-create", action="store_true")
    parser.add_argument("--trigger-event-suffix", required=False)
    parser.add_argument("--judge-run-suffix", required=False)
    parser.add_argument("--judge-output-suffix", required=False)
    parser.add_argument("--notification-plan-event-suffix", required=False)
    parser.add_argument("--scan-limit", type=int, default=25)
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    policy_redis_builder: RedisBuilder | None = None,
    policy_repository_builder: RepositoryBuilder | None = None,
    notification_repository_builder: NotificationQueuePublishRepositoryBuilder | None = None,
    notification_redis_publisher_builder: NotificationQueueRedisPublisherBuilder | None = None,
) -> RunnerResult:
    parsed = {
        "trigger_event_suffix": _parse_optional_uuid_suffix(
            args.trigger_event_suffix,
            "invalid_trigger_event_suffix",
        ),
        "judge_run_suffix": _parse_optional_uuid_suffix(args.judge_run_suffix, "invalid_judge_run_suffix"),
        "judge_output_suffix": _parse_optional_uuid_suffix(args.judge_output_suffix, "invalid_judge_output_suffix"),
        "notification_plan_event_suffix": _parse_optional_uuid_suffix(
            args.notification_plan_event_suffix,
            "invalid_notification_plan_event_suffix",
        ),
    }
    for value in parsed.values():
        if isinstance(value, dict):
            return RunnerResult(exit_code=1, report=value)

    runner_kwargs: dict[str, Any] = {
        "policy_redis_builder": policy_redis_builder,
        "policy_repository_builder": policy_repository_builder,
        "notification_repository_builder": notification_repository_builder,
        "notification_redis_publisher_builder": notification_redis_publisher_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader

    result = run_bounded_policy_to_notification_queue_sync(
        BoundedPolicyToNotificationQueueConfig(
            mode=str(args.mode),
            operator_approved=bool(args.operator_approved),
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_redis_read=bool(args.allow_redis_read),
            allow_database_read=bool(args.allow_database_read),
            allow_redis_consume=bool(args.allow_redis_consume),
            allow_database_write=bool(args.allow_database_write),
            allow_redis_ack=bool(args.allow_redis_ack),
            allow_notification_outbox_publish=bool(args.allow_notification_outbox_publish),
            allow_notification_redis_publish=bool(args.allow_notification_redis_publish),
            allow_redis_group_create=bool(args.allow_redis_group_create),
            trigger_event_suffix=parsed["trigger_event_suffix"],
            judge_run_suffix=parsed["judge_run_suffix"],
            judge_output_suffix=parsed["judge_output_suffix"],
            notification_plan_event_suffix=parsed["notification_plan_event_suffix"],
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
    policy_redis_builder: RedisBuilder | None = None,
    policy_repository_builder: RepositoryBuilder | None = None,
    notification_repository_builder: NotificationQueuePublishRepositoryBuilder | None = None,
    notification_redis_publisher_builder: NotificationQueueRedisPublisherBuilder | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(argument_error_report(str(exc))))
        return 1
    result = run(
        args,
        runtime_config_loader=runtime_config_loader,
        policy_redis_builder=policy_redis_builder,
        policy_repository_builder=policy_repository_builder,
        notification_repository_builder=notification_repository_builder,
        notification_redis_publisher_builder=notification_redis_publisher_builder,
    )
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


def _parse_optional_uuid_suffix(value: str | None, error_code: str) -> str | None | dict[str, Any]:
    if value is None:
        return None
    stripped = value.strip().lower()
    if 4 <= len(stripped) <= 12 and "-" not in stripped and all(char in "0123456789abcdef" for char in stripped):
        return stripped
    return argument_error_report(error_code)


__all__ = [
    "BoundedPolicyApplyRuntimeConfig",
    "BoundedPolicyToNotificationQueueConfig",
    "BoundedPolicyToNotificationQueueResult",
    "CliArgumentError",
    "RunnerResult",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
