from __future__ import annotations

import ast
import json
from pathlib import Path

from tools import bounded_redis_rebuild_readiness_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_redis_rebuild_readiness_runner.py"
SOURCE_PATH = ROOT / "src/services/maintenance/redis_rebuild_readiness.py"

RAW_ENV_PATH = "/abs/private/runtime.env"
RAW_DB_URL = "postgresql+psycopg://user:secret@db.internal/github_ai_catchbot"
RAW_REDIS_URL = "redis://:secret@redis.internal:6379/0"
RAW_STREAM_ID = "1711111111111-42"
RAW_DEDUPE_KEY = "notify:retry-intent:raw-dedupe"
RAW_PAYLOAD = "payload_json sentinel body"
RAW_SOURCE_TEXT = "raw telegram source text"


async def _fake_runtime(runtime_env_file, request):
    assert runtime_env_file == RAW_ENV_PATH
    return {
        "schema_version": "redis_rebuild_readiness_report_v1",
        "runner_name": "bounded_redis_rebuild_readiness_runner",
        "mode": request.mode,
        "status": "pass",
        "reason_code": "dry_run_plan_pass",
        "redis_inventory": {
            "known_queue_count_bucket": "one",
            "known_queue_buckets": ["maintenance"],
            "stream_presence_buckets": {"maintenance": "present"},
            "group_presence_buckets": {
                "maintenance": {
                    "configured_group_count_bucket": "one",
                    "present_group_count_bucket": "one",
                    "missing_group_count_bucket": "zero",
                }
            },
            "pending_count_buckets": {"maintenance": "zero"},
            "stream_length_buckets": {"maintenance": "one"},
            "missing_stream_buckets": [],
            "missing_group_buckets": [],
            "raw_stream_ids_omitted": True,
            "raw_group_names_if_sensitive_omitted_or_bucketed": True,
        },
        "durable_inventory": {
            "db_read_attempted": True,
            "durable_source_categories": {
                "event_outbox": {
                    "state": "present",
                    "count_bucket": "one",
                    "status_buckets": {"pending": "one"},
                    "queue_buckets": {"maintenance": "one"},
                    "stage_buckets": {},
                    "age_buckets": {"fresh": "one"},
                    "sample_shape_count_bucket": "one",
                    "raw_ids_omitted": True,
                    "raw_payloads_omitted": True,
                }
            },
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
                "outbox_events_to_publish": "one",
                "job_attempts_to_requeue": "zero",
                "notification_plans_to_requeue": "zero",
                "replay_requests_to_requeue": "zero",
                "dlq_entries_replayable": "zero",
                "unsupported_or_not_present": [],
            },
            "missing_stream_buckets": [],
            "missing_group_buckets": [],
            "blocked_in_o3a": ["redis_xadd_rebuild_jobs"],
            "o3b_required_authority": ["redis_xadd_for_rebuildable_durable_rows"],
        },
        "authority": {
            "redis_read_attempted": True,
            "redis_mutation_attempted": False,
            "redis_flush_attempted": False,
            "redis_xadd_attempted": False,
            "redis_xack_attempted": False,
            "redis_xgroup_mutation_attempted": False,
            "redis_xreadgroup_attempted": False,
            "db_read_attempted": True,
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
        "recommended_next_operator_action": "submit_review_bundle_for_chatgpt_pass_before_o3b",
    }


def test_runner_inventory_emits_sanitized_json_only(capsys) -> None:
    exit_code = runner.main(
        ["--mode", "inventory", "--runtime-env-file", RAW_ENV_PATH, "--queue", "q.maintenance"],
        runtime_runner=_fake_runtime,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert captured.out.endswith("\n")
    assert "\n" not in captured.out[:-1]
    assert parsed["schema_version"] == "redis_rebuild_readiness_report_v1"
    assert parsed["mode"] == "inventory"
    assert parsed["status"] == "pass"
    assert parsed["authority"]["redis_read_attempted"] is True
    assert parsed["authority"]["db_read_attempted"] is True
    for raw in (RAW_ENV_PATH, RAW_DB_URL, RAW_REDIS_URL, RAW_STREAM_ID, RAW_DEDUPE_KEY, RAW_PAYLOAD, RAW_SOURCE_TEXT):
        assert raw not in captured.out


def test_runner_plan_has_dry_run_mutation_fields_false(capsys) -> None:
    exit_code = runner.main(
        ["--mode", "plan", "--runtime-env-file", RAW_ENV_PATH, "--all-known-queues"],
        runtime_runner=_fake_runtime,
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert parsed["mode"] == "plan"
    assert parsed["dry_run_rebuild_plan"]["would_create_streams"] is False
    assert parsed["dry_run_rebuild_plan"]["would_create_groups"] is False
    assert parsed["dry_run_rebuild_plan"]["would_xadd_jobs"] is False
    assert parsed["dry_run_rebuild_plan"]["would_ack_or_delete_pending"] is False
    assert parsed["authority"]["redis_mutation_attempted"] is False
    assert parsed["authority"]["db_write_attempted"] is False


def test_cli_parse_failures_emit_json_only_without_argparse_usage(capsys) -> None:
    exit_code = runner.main(["--bad-arg"])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["status"] == "blocked"
    assert parsed["reason_code"] == "unsupported_cli_argument"
    assert "usage:" not in captured.out.lower()
    assert parsed["authority"]["redis_mutation_attempted"] is False
    assert parsed["authority"]["db_write_attempted"] is False


def test_missing_runtime_env_file_arg_is_json_blocked(capsys) -> None:
    exit_code = runner.main(["--mode", "inventory"], runtime_runner=_fake_runtime)
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["status"] == "blocked"
    assert parsed["reason_code"] == "runtime_env_file_required"
    assert parsed["authority"]["redis_read_attempted"] is False
    assert parsed["authority"]["db_read_attempted"] is False


def test_static_ast_prevents_forbidden_redis_mutation_calls() -> None:
    forbidden_call_names = {
        "flushdb",
        "flushall",
        "delete",
        "unlink",
        "xadd",
        "xack",
        "xdel",
        "xgroup_create",
        "xgroup_destroy",
        "xgroup_delconsumer",
        "xclaim",
        "xautoclaim",
        "xreadgroup",
        "publish",
        "subscribe",
        "eval",
    }

    for path in (TOOL_PATH, SOURCE_PATH):
        called = _called_names(path)
        assert called.isdisjoint(forbidden_call_names), path


def test_static_ast_prevents_external_runtime_authority_calls_in_runner() -> None:
    forbidden_import_roots = {
        "telegram",
        "openai",
        "github",
        "requests",
        "httpx",
        "aiohttp",
        "urllib",
        "docker",
        "subprocess",
    }
    forbidden_call_names = {
        "systemctl",
        "run_systemd_rollout",
        "LocalUserSystemdAdapter",
        "send_message",
        "edit_message_text",
        "run_forever",
        "alembic",
    }

    for path in (TOOL_PATH, SOURCE_PATH):
        imported = _import_roots(path)
        called = _called_names(path)
        assert imported.isdisjoint(forbidden_import_roots), path
        assert called.isdisjoint(forbidden_call_names), path
    assert "print(" not in TOOL_PATH.read_text(encoding="utf-8")


def _called_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                names.add(node.func.attr)
    return names


def _import_roots(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".", 1)[0])
    return roots
