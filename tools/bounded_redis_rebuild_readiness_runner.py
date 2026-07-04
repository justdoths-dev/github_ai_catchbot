from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.maintenance.redis_rebuild_readiness import (  # noqa: E402
    RUNNER_NAME,
    SCHEMA_VERSION,
    RedisRebuildReadinessRequest,
    blocked_report,
    render_sanitized_json,
    run_redis_rebuild_readiness_from_runtime_env_file,
)


class CliArgumentError(ValueError):
    pass


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


RuntimeRunner = Callable[[str, RedisRebuildReadinessRequest], Awaitable[dict[str, Any]]]


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Emit sanitized Redis rebuild readiness inventory or dry-run plan JSON.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=("inventory", "plan"), default="inventory")
    parser.add_argument("--runtime-env-file")
    parser.add_argument("--queue")
    parser.add_argument("--all-known-queues", action="store_true")
    parser.add_argument("--include-empty", action="store_true")
    parser.add_argument("--max-sample", type=int, default=3)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_runner: RuntimeRunner = run_redis_rebuild_readiness_from_runtime_env_file,
) -> int:
    try:
        args = build_parser().parse_args(argv)
        request = RedisRebuildReadinessRequest(
            mode=str(args.mode),
            queue_selector=str(args.queue).strip() if args.queue else None,
            all_known_queues=bool(args.all_known_queues),
            include_empty=bool(args.include_empty),
            max_sample=int(args.max_sample),
        )
        if not args.runtime_env_file:
            report = blocked_report("runtime_env_file_required", mode=request.mode)
        else:
            report = asyncio.run(runtime_runner(str(args.runtime_env_file), request))
        sys.stdout.write(render_sanitized_json(report))
        return 0 if report.get("status") == "pass" else 1
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(_blocked(str(exc))))
        return 1
    except Exception:
        sys.stdout.write(render_sanitized_json(_blocked("runner_error", status="failed")))
        return 1


def _blocked(reason_code: str, *, status: str = "blocked") -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": "inventory",
        "status": status,
        "reason_code": reason_code,
        "redis_inventory": {
            "known_queue_count_bucket": "zero",
            "known_queue_buckets": [],
            "stream_presence_buckets": {},
            "group_presence_buckets": {},
            "pending_count_buckets": {},
            "stream_length_buckets": {},
            "missing_stream_buckets": [],
            "missing_group_buckets": [],
            "read_error_code": None,
            "raw_stream_ids_omitted": True,
            "raw_group_names_if_sensitive_omitted_or_bucketed": True,
        },
        "durable_inventory": {
            "db_read_attempted": False,
            "durable_source_categories": {},
            "read_error_code": None,
            "raw_ids_omitted": True,
            "raw_payloads_omitted": True,
        },
        "dry_run_rebuild_plan": {
            "would_create_streams": False,
            "would_create_groups": False,
            "would_xadd_jobs": False,
            "would_ack_or_delete_pending": False,
            "planned_actions_are_dry_run_only": True,
            "planned_action_buckets": {
                "outbox_events_to_publish": "zero",
                "job_attempts_to_requeue": "zero",
                "notification_plans_to_requeue": "zero",
                "replay_requests_to_requeue": "zero",
                "dlq_entries_replayable": "zero",
                "unsupported_or_not_present": [],
            },
            "missing_stream_buckets": [],
            "missing_group_buckets": [],
            "blocked_in_o3a": [],
            "o3b_required_authority": [],
        },
        "authority": {
            "redis_read_attempted": False,
            "redis_mutation_attempted": False,
            "redis_flush_attempted": False,
            "redis_xadd_attempted": False,
            "redis_xack_attempted": False,
            "redis_xgroup_mutation_attempted": False,
            "redis_xreadgroup_attempted": False,
            "db_read_attempted": False,
            "db_write_attempted": False,
            "runtime_env_values_output": False,
            "secrets_output": False,
            "telegram_attempted": False,
            "openai_attempted": False,
            "github_attempted": False,
            "x_attempted": False,
            "web_attempted": False,
            "systemd_attempted": False,
            "docker_attempted": False,
            "migration_attempted": False,
        },
        "redactions_applied": {
            "runtime_env_path_omitted": True,
            "runtime_env_values_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "secret_values_omitted": True,
            "raw_stream_ids_omitted": True,
            "raw_consumer_names_omitted_or_bucketed": True,
            "raw_event_ids_omitted": True,
            "raw_job_ids_omitted": True,
            "raw_aggregate_ids_omitted": True,
            "raw_dedupe_keys_omitted": True,
            "raw_payload_json_omitted": True,
            "raw_source_text_omitted": True,
            "raw_urls_omitted": True,
            "raw_exception_bodies_omitted": True,
        },
        "open_gates": {
            "AUTHORITY_OPEN": True,
            "ROLLOUT_OPEN": True,
            "PRODUCTION_ROLLOUT_OPEN": True,
            "ACTUAL_REDIS_MUTATION_OPEN": True,
            "ACTUAL_REDIS_FLUSH_OPEN": True,
            "ACTUAL_REDIS_CONSUME_ACK_OPEN": True,
            "ACTUAL_TELEGRAM_SEND_OPEN": True,
        },
        "completion_claims": {
            "REDIS_REBUILD_CLOSED": False,
            "PRODUCT_COMPLETE_CLOSED": False,
            "final_bot_complete": False,
            "one_hundred_percent_complete": False,
            "production_rollout_complete": False,
        },
        "recommended_next_operator_action": "fix_blocker_then_rerun_o3a_readiness",
    }


if __name__ == "__main__":
    raise SystemExit(main())
