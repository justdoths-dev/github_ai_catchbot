from __future__ import annotations

import ast
import json
from pathlib import Path

from services.maintenance.redis_rebuild_execution import EXPECTED_BASELINE_HEAD
from tools import bounded_redis_rebuild_execution_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_redis_rebuild_execution_runner.py"
SOURCE_PATH = ROOT / "src/services/maintenance/redis_rebuild_execution.py"

RAW_ENV_PATH = "/abs/private/runtime.env"
RAW_DB_URL = "raw-db-url-sentinel"
RAW_REDIS_URL = "raw-redis-url-sentinel"
RAW_STREAM_ID = "1711111111111-42"
RAW_EVENT_ID = "11111111-1111-4111-8111-111111111111"
RAW_OBJECT_ID = "22222222-2222-4222-8222-222222222222"
RAW_DEDUPE_KEY = "maintenance:raw-dedupe"
RAW_PAYLOAD = "payload_json sentinel body"
RAW_SOURCE_TEXT = "raw telegram source text"
RAW_URL = "raw-url-sentinel"


def _expected_open_gates() -> dict[str, bool]:
    return {
        "PRODUCTION_ROLLOUT_OPEN": True,
        "PRODUCT_COMPLETE_CLOSED": False,
        "ACTUAL_REDIS_FLUSH_OPEN": False,
        "ACTUAL_REDIS_DELETE_OPEN": False,
        "ACTUAL_REDIS_CONSUME_ACK_OPEN": False,
        "ACTUAL_REDIS_PENDING_CLAIM_OPEN": False,
        "ACTUAL_DB_WRITE_OPEN": False,
        "ACTUAL_TELEGRAM_SEND_OPEN": False,
    }


def _assert_runtime_authority_all_false(report: dict) -> None:
    assert all(value is False for value in report["runtime_authority_opened_in_this_run"].values())


def _assert_final_closure_claims_false(report: dict) -> None:
    claims = report["completion_claims"]
    assert claims["REDIS_REBUILD_CLOSED"] is False
    assert claims["PRODUCT_COMPLETE_CLOSED"] is False
    assert claims["PRODUCTION_ROLLOUT_CLOSED"] is False
    assert claims["final_bot_complete"] is False
    assert claims["one_hundred_percent_complete"] is False
    assert claims["production_rollout_complete"] is False
    assert "ACTUAL_REDIS_REBUILD_EXECUTED_IN_THIS_RUN" not in claims


async def _fake_runtime(runtime_env_file, request):
    assert runtime_env_file == RAW_ENV_PATH
    redis_runtime_authority_opened = (
        request.mode == "execute"
        and request.expected_head == EXPECTED_BASELINE_HEAD
        and request.max_rebuild_jobs is not None
        and request.understand_mutates_redis
        and request.approve_redis_rebuild_execution
    )
    redis_mutation_attempted = request.mode == "execute"
    return {
        "schema_version": "redis_rebuild_execution_report_v1",
        "runner_name": "bounded_redis_rebuild_execution_runner",
        "mode": request.mode,
        "status": "pass",
        "reason_code": "execution_plan_has_work" if request.mode == "plan" else "redis_rebuild_execution_pass",
        "preflight_readiness_summary": {
            "schema_version": "redis_rebuild_readiness_report_v1",
            "status": "pass",
            "reason_code": "dry_run_plan_pass",
            "target_queue_key": request.queue_selector,
            "target_stream_presence_bucket": "missing",
            "target_group_presence_buckets": {
                "configured_group_count_bucket": "one",
                "present_group_count_bucket": "zero",
                "missing_group_count_bucket": "one",
            },
            "durable_event_outbox_count_bucket": "one",
            "planned_action_buckets": {"outbox_events_to_publish": "one"},
            "raw_ids_omitted": True,
            "raw_payloads_omitted": True,
        },
        "approved_target": {
            "exactly_one_queue_selected": True,
            "queue_key": request.queue_selector,
            "stream_name": "q.maintenance",
            "configured_group_count_bucket": "one",
            "max_rebuild_jobs": request.max_rebuild_jobs,
            "max_rebuild_jobs_within_bound": True,
            "expected_head_matches_task_baseline": request.expected_head == EXPECTED_BASELINE_HEAD,
            "runtime_env_path_omitted": True,
        },
        "execution_summary": {
            "rebuild_source": "event_outbox",
            "canonical_outbox_route_resolver_reused": True,
            "thin_redis_message_contract_reused": True,
            "eligible_event_outbox_count_bucket": "one",
            "unsupported_route_count_bucket": "zero",
            "missing_group_count_bucket": "one",
            "created_group_count_bucket": "one" if request.mode == "execute" else "zero",
            "xadd_inserted_count_bucket": "one" if request.mode == "execute" else "zero",
            "skipped_duplicate_count_bucket": "zero",
            "idempotency_guard": "bounded_exact_stream_scan_or_missing_stream",
            "not_executed_categories": [],
            "raw_ids_omitted": True,
            "raw_payloads_omitted": True,
            "raw_dedupe_keys_omitted": True,
        },
        "post_write_readback": {
            "performed": request.mode == "execute",
            "stream_presence_bucket": "present" if request.mode == "execute" else "missing",
            "group_presence_bucket": "one" if request.mode == "execute" else "zero",
            "stream_length_count_bucket": "one" if request.mode == "execute" else "zero",
            "stream_length_delta_bucket": "one" if request.mode == "execute" else "unknown",
            "inserted_count_bucket": "one" if request.mode == "execute" else "zero",
            "skipped_duplicate_count_bucket": "zero",
            "raw_stream_ids_omitted": True,
            "raw_message_ids_omitted": True,
        },
        "authority": {
            "redis_read_attempted": True,
            "redis_mutation_attempted": redis_mutation_attempted,
            "redis_xadd_attempted": redis_mutation_attempted,
            "redis_xgroup_create_attempted": redis_mutation_attempted,
            "redis_flush_attempted": False,
            "redis_delete_attempted": False,
            "redis_xack_attempted": False,
            "redis_xdel_attempted": False,
            "redis_xreadgroup_attempted": False,
            "redis_xclaim_attempted": False,
            "redis_xautoclaim_attempted": False,
            "redis_publish_subscribe_attempted": False,
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
        "runtime_authority_opened_in_this_run": {
            "redis_xadd_authority_opened": redis_runtime_authority_opened,
            "redis_xgroup_create_authority_opened": redis_runtime_authority_opened,
            "redis_flush_authority_opened": False,
            "redis_delete_authority_opened": False,
            "redis_consume_ack_authority_opened": False,
            "redis_pending_claim_authority_opened": False,
            "db_write_authority_opened": False,
            "telegram_send_authority_opened": False,
            "production_rollout_authority_opened": False,
        },
        "redactions_applied": {
            "runtime_env_path_omitted": True,
            "runtime_env_values_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "secret_values_omitted": True,
            "raw_stream_ids_omitted": True,
            "raw_message_ids_omitted": True,
            "raw_event_ids_omitted": True,
            "raw_job_ids_omitted": True,
            "raw_object_ids_omitted": True,
            "raw_aggregate_ids_omitted": True,
            "raw_dedupe_keys_omitted": True,
            "raw_payload_json_omitted": True,
            "raw_source_text_omitted": True,
            "raw_urls_omitted": True,
            "raw_exception_bodies_omitted": True,
        },
        "open_gate_semantics": "global_project_lifecycle_state_after_report_not_invocation_authority_or_attempts",
        "open_gates": _expected_open_gates(),
        "completion_claims": {
            "REDIS_REBUILD_EXECUTION_CAPABILITY_IMPLEMENTED": True,
            "REDIS_REBUILD_EXECUTION_PLAN_PASSED": request.mode == "plan",
            "REDIS_REBUILD_EXECUTION_PROOF_PASSED": request.mode == "execute",
            "ACTUAL_REDIS_REBUILD_MUTATION_EXECUTED_IN_THIS_RUN": redis_mutation_attempted,
            "REDIS_REBUILD_CLOSED": False,
            "PRODUCT_COMPLETE_CLOSED": False,
            "PRODUCTION_ROLLOUT_CLOSED": False,
            "final_bot_complete": False,
            "one_hundred_percent_complete": False,
            "production_rollout_complete": False,
        },
        "recommended_next_operator_action": "review_plan_then_rerun_execute_with_all_approval_flags_if_needed",
    }


def test_runner_plan_emits_sanitized_json_only(capsys) -> None:
    exit_code = runner.main(
        ["--mode", "plan", "--runtime-env-file", RAW_ENV_PATH, "--queue", "maintenance"],
        runtime_runner=_fake_runtime,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert captured.out.endswith("\n")
    assert "\n" not in captured.out[:-1]
    assert parsed["schema_version"] == "redis_rebuild_execution_report_v1"
    assert parsed["mode"] == "plan"
    assert parsed["authority"]["redis_mutation_attempted"] is False
    _assert_runtime_authority_all_false(parsed)
    assert parsed["open_gates"] == _expected_open_gates()
    assert parsed["completion_claims"]["REDIS_REBUILD_CLOSED"] is False
    assert parsed["completion_claims"]["PRODUCTION_ROLLOUT_CLOSED"] is False
    for raw in (
        RAW_ENV_PATH,
        RAW_DB_URL,
        RAW_REDIS_URL,
        RAW_STREAM_ID,
        RAW_EVENT_ID,
        RAW_OBJECT_ID,
        RAW_DEDUPE_KEY,
        RAW_PAYLOAD,
        RAW_SOURCE_TEXT,
        RAW_URL,
    ):
        assert raw not in captured.out


def test_runner_execute_passes_all_approval_flags(capsys) -> None:
    exit_code = runner.main(
        [
            "--mode",
            "execute",
            "--runtime-env-file",
            RAW_ENV_PATH,
            "--queue",
            "maintenance",
            "--max-rebuild-jobs",
            "5",
            "--expected-head",
            EXPECTED_BASELINE_HEAD,
            "--i-understand-this-mutates-redis",
            "--approve-redis-rebuild-execution",
        ],
        runtime_runner=_fake_runtime,
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert parsed["mode"] == "execute"
    assert parsed["approved_target"]["expected_head_matches_task_baseline"] is True
    assert parsed["approved_target"]["max_rebuild_jobs"] == 5
    assert parsed["authority"]["redis_xadd_attempted"] is True
    assert parsed["authority"]["db_write_attempted"] is False
    assert parsed["runtime_authority_opened_in_this_run"]["redis_xadd_authority_opened"] is True
    assert parsed["runtime_authority_opened_in_this_run"]["redis_xgroup_create_authority_opened"] is True
    assert parsed["runtime_authority_opened_in_this_run"]["production_rollout_authority_opened"] is False
    assert parsed["open_gates"]["PRODUCTION_ROLLOUT_OPEN"] is True
    assert parsed["completion_claims"]["ACTUAL_REDIS_REBUILD_MUTATION_EXECUTED_IN_THIS_RUN"] is True
    _assert_final_closure_claims_false(parsed)


def test_missing_or_relative_runtime_env_file_blocks_before_runtime(capsys) -> None:
    for argv, reason_code in (
        (["--mode", "plan", "--queue", "maintenance"], "runtime_env_file_required"),
        (
            ["--mode", "execute", "--runtime-env-file", "relative.env", "--queue", "maintenance"],
            "runtime_env_file_not_absolute",
        ),
    ):
        exit_code = runner.main(argv, runtime_runner=_fake_runtime)
        parsed = json.loads(capsys.readouterr().out)

        assert exit_code == 1
        assert parsed["status"] == "blocked"
        assert parsed["reason_code"] == reason_code
        assert parsed["authority"]["redis_mutation_attempted"] is False
        assert parsed["authority"]["db_write_attempted"] is False
        assert all(value is False for value in parsed["authority"].values())
        _assert_runtime_authority_all_false(parsed)
        assert parsed["open_gates"] == _expected_open_gates()
        _assert_final_closure_claims_false(parsed)


def test_cli_parse_failures_emit_json_only_without_argparse_usage(capsys) -> None:
    exit_code = runner.main(["--bad-arg"])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["status"] == "blocked"
    assert parsed["reason_code"] == "unsupported_cli_argument"
    assert "usage:" not in captured.out.lower()
    assert all(value is False for value in parsed["authority"].values())
    _assert_runtime_authority_all_false(parsed)
    assert parsed["open_gates"] == _expected_open_gates()
    _assert_final_closure_claims_false(parsed)


def test_static_ast_prevents_forbidden_redis_mutation_and_external_authority_calls() -> None:
    forbidden_redis_call_names = {
        "flushdb",
        "flushall",
        "delete",
        "unlink",
        "xack",
        "xdel",
        "xreadgroup",
        "xclaim",
        "xautoclaim",
        "xgroup_destroy",
        "xgroup_delconsumer",
        "publish",
        "subscribe",
        "eval",
    }
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
        "alembic",
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
        called = _called_names(path)
        imported = _import_roots(path)
        assert called.isdisjoint(forbidden_redis_call_names), path
        assert called.isdisjoint(forbidden_call_names), path
        assert imported.isdisjoint(forbidden_import_roots), path
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
