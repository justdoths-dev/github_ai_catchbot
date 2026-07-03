from __future__ import annotations

import ast
import json
from pathlib import Path

from services.maintenance.operational_stabilization_governance_proof import (
    AUTHORITY_FLAG_NAMES,
    build_operational_stabilization_governance_proof,
    render_sanitized_json,
)


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/maintenance/operational_stabilization_governance_proof.py"

RAW_VALUES = (
    "11111111-1111-4111-8111-111111111111",
    "stream-123-0",
    "notify:retry-intent:",
    "notify:replay-intent:",
    "https://" + "private.example.invalid",
    "postgresql" + "://",
    "postgresql+psycopg" + "://",
    "redis" + "://",
    "runtime" + ".env",
    "sk-" + "private",
    "Bearer " + "private",
    "x-" + "ratelimit-reset",
    "x-" + "rate-limit-reset",
    "raw private " + "source text",
    "raw stderr " + "body",
)


def test_delivery_operational_metrics_reuse_existing_delivery_gate_semantics() -> None:
    report = build_operational_stabilization_governance_proof()
    delivery = report["o5_monitoring_rate_limit_cost_guards"]["monitoring_health_guards"][
        "delivery_gate_metrics"
    ]
    source = SOURCE_PATH.read_text(encoding="utf-8")

    assert delivery["consumed_existing_delivery_gate_evaluator"] is True
    assert delivery["snapshot_model"] == "DeliveryGateSnapshot"
    assert delivery["report_model"] == "DeliveryGateReportV1"
    assert delivery["delivery_operations_gate_alias_model"] == "DeliveryOperationsGateReport"
    assert delivery["delivery_operations_gate_alias_consumed"] is True
    assert delivery["gate_status"] == "pass"
    assert delivery["blocking_reason_codes"] == []
    assert delivery["warning_reason_codes"] == []
    assert delivery["all_metrics_passed"] is True
    assert set(delivery["metric_names"]) >= {
        "enable_notification_send",
        "notifier_telegram_dry_run",
        "maintenance_retry_promotion",
        "success_rate_1h",
        "high_source_to_delivery_p95_sec",
        "due_retry_oldest_lag_sec",
        "open_delivery_dlq_count",
        "unexpected_send_disabled_count",
    }
    assert "evaluate_delivery_gate(" in source
    assert "DeliveryGateSnapshot(" in source


def test_delivery_operations_gate_dlq_taxonomy_is_reused_as_sanitized_buckets() -> None:
    report = build_operational_stabilization_governance_proof()
    taxonomy = report["o5_monitoring_rate_limit_cost_guards"]["monitoring_health_guards"][
        "delivery_operations_gate_taxonomy"
    ]

    assert taxonomy["consumed_existing_allowed_dlq_taxonomy"] is True
    assert "max_notification_retry_attempts_exceeded" in taxonomy["allowed_last_error_code_buckets"]
    assert "request_delivery_replay_after_operator_fix" in taxonomy["allowed_next_manual_action_buckets"]
    assert taxonomy["allowed_replay_hint_buckets"] == ["delivery_replay_from_notification_plan"]
    assert taxonomy["raw_error_bodies_omitted"] is True
    assert taxonomy["raw_root_ids_omitted"] is True


def test_monitoring_groups_include_service_pipeline_product_quality_and_cost_token() -> None:
    report = build_operational_stabilization_governance_proof()
    health = report["o5_monitoring_rate_limit_cost_guards"]["monitoring_health_guards"]

    assert set(health["metric_groups"]) == {"service_health", "pipeline", "product_quality", "cost_token"}
    assert "collector_heartbeat_age_bucket" in health["metric_groups"]["service_health"]
    assert "evidence_bundle_ready_bucket" in health["metric_groups"]["pipeline"]
    assert "low_evidence_high_bucket" in health["metric_groups"]["product_quality"]
    assert "daily_hard_cap_bucket" in health["metric_groups"]["cost_token"]
    assert health["collector_heartbeat"]["live_metrics_collection_attempted"] is False
    assert health["queue_depth_lag"]["redis_read_attempted"] is False
    assert health["alert_separation"]["operator_alerts_separate_from_user_telegram_notifications"] is True
    assert health["no_live_metrics_collection_attempted"] is True


def test_external_rate_limit_and_flood_buckets_are_represented_without_provider_calls() -> None:
    report = build_operational_stabilization_governance_proof()
    guards = report["o5_monitoring_rate_limit_cost_guards"]["external_dependency_rate_limit_flood_guards"]

    assert guards["github"]["bucket"] == "github_rate_limit_bucket"
    assert guards["x"]["bucket"] == "x_rate_limit_bucket"
    assert guards["openai"]["bucket"] == "openai_429_503_bucket"
    assert guards["telegram"]["bucket"] == "telegram_flood_retry_after_bucket"
    assert guards["circuit_breaker"]["pause_defer_representation"] is True
    assert guards["no_external_api_call_attempted"] is True
    for provider in ("github", "x", "openai", "telegram"):
        assert guards[provider]["pause_or_defer_represented"] is True
        assert guards[provider]["external_api_call_attempted"] is False


def test_openai_hard_cap_and_offline_replay_cost_separation_are_represented() -> None:
    report = build_operational_stabilization_governance_proof()
    cost = report["o5_monitoring_rate_limit_cost_guards"]["openai_cost_hard_cap_guard"]

    assert cost["daily_hard_cap"]["bucket"] == "daily_hard_cap_bucket"
    assert cost["daily_hard_cap"]["app_internal_guard_represented"] is True
    assert cost["model_mix_escalation_ratio"]["auto_downgrade_or_defer_represented"] is True
    assert cost["offline_replay_cost"]["separate_budget_bucket"] is True
    assert cost["offline_replay_cost"]["live_path_cost_mixed_with_replay"] is False
    assert cost["live_path"]["separate_from_offline_replay"] is True
    assert cost["raw_openai_token_usage_from_real_requests_emitted"] is False
    assert cost["no_openai_call_attempted"] is True


def test_backup_restore_drill_represents_postgres_and_tdlib_without_executing_commands() -> None:
    report = build_operational_stabilization_governance_proof()
    drill = report["o6_backup_restore_recovery_drill"]["backup_restore_drill_representation"]

    assert drill["postgres_backup_availability"]["bucket"] == "postgres_backup_available_bucket"
    assert drill["postgres_backup_availability"]["backup_command_attempted"] is False
    assert drill["postgres_restore_drill"]["bucket"] == "postgres_restore_drill_represented"
    assert drill["postgres_restore_drill"]["restore_command_attempted"] is False
    assert drill["tdlib_state_restore_check"]["bucket"] == "tdlib_state_restore_check_represented"
    assert drill["tdlib_state_restore_check"]["tdlib_state_read_attempted"] is False
    assert drill["runtime_config_presence"]["runtime_env_read_attempted"] is False
    assert drill["secret_rotation_checklist"]["secret_values_output"] is False
    assert drill["startup_sequence_proof"]["represented"] is True
    assert drill["startup_sequence_proof"]["workers_started"] is False


def test_redis_recovery_remains_rebuild_from_postgres_not_redis_restore() -> None:
    report = build_operational_stabilization_governance_proof()
    safety = report["o6_backup_restore_recovery_drill"]["recovery_safety"]

    assert safety["redis_recovery_mode"] == "rebuild_from_postgres"
    assert safety["redis_restore_allowed"] is False
    assert safety["postgres_is_restore_authority"] is True
    assert safety["redis_is_transient_rebuild_target"] is True
    assert safety["startup_sequence_starts_workers"] is False
    assert safety["rollback_executes_systemd"] is False
    assert safety["rollback_executes_docker"] is False


def test_production_rollout_governance_represents_gates_but_keeps_rollout_open() -> None:
    report = build_operational_stabilization_governance_proof()
    governance = report["o7_production_rollout_governance"]

    assert governance["rollout_gate"]["represented"] is True
    assert governance["rollout_gate"]["operator_acceptance_required"] is True
    assert governance["rollout_gate"]["production_rollout_attempted"] is False
    assert governance["rollback_gate"]["represented"] is True
    assert governance["release_decision_record"]["decision_bucket"] == "code_review_pass_only"
    assert governance["release_decision_record"]["operator_acceptance_required"] is True
    assert governance["release_decision_record"]["production_rollout_closed"] is False
    assert governance["channel_registry_governance"]["represented"] is True
    assert governance["change_separation"]["combined_unreviewed_change_allowed"] is False
    assert governance["production_rollout_remains_open"] is True
    assert report["target_exit_states"]["PRODUCTION_ROLLOUT_OPEN"] is True
    assert report["target_exit_states"]["PRODUCT_COMPLETE_CLOSED"] is False
    assert report["target_exit_states"]["final_bot_complete"] is False
    assert report["target_exit_states"]["one_hundred_percent_complete"] is False


def test_authority_booleans_are_complete_and_all_false() -> None:
    report = build_operational_stabilization_governance_proof()

    assert set(report["side_effect_authority"]) == set(AUTHORITY_FLAG_NAMES)
    assert all(value is False for value in report["side_effect_authority"].values())
    assert report["side_effect_authority"] == {
        "db_read_attempted": False,
        "db_write_attempted": False,
        "redis_read_attempted": False,
        "redis_mutation_attempted": False,
        "worker_started": False,
        "telegram_attempted": False,
        "openai_attempted": False,
        "github_attempted": False,
        "x_attempted": False,
        "web_attempted": False,
        "docker_attempted": False,
        "systemd_attempted": False,
        "alembic_attempted": False,
        "migration_attempted": False,
        "backup_command_attempted": False,
        "restore_command_attempted": False,
        "tdlib_state_read_attempted": False,
        "runtime_env_read_attempted": False,
        "secrets_output": False,
        "production_rollout_attempted": False,
        "feature_flag_mutation_attempted": False,
        "raw_log_output_attempted": False,
    }


def test_rendered_output_is_sanitized_and_compact() -> None:
    output = render_sanitized_json(build_operational_stabilization_governance_proof())
    parsed = json.loads(output)

    assert output.endswith("\n")
    assert "\n" not in output[:-1]
    assert parsed["schema_version"] == "operational_stabilization_governance_proof_v1"
    assert parsed["status"] == "pass"
    assert parsed["raw_values_printed"] is False
    assert parsed["redactions_applied"]["raw_headers_omitted"] is True
    for raw in RAW_VALUES:
        assert raw not in output


def test_static_source_has_no_forbidden_live_imports_calls_or_command_tokens() -> None:
    _assert_no_forbidden_imports_calls_or_tokens(SOURCE_PATH)


def _assert_no_forbidden_imports_calls_or_tokens(path: Path) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    source = path.read_text(encoding="utf-8")
    imported_roots: set[str] = set()
    call_names: set[str] = set()
    names: set[str] = set()
    attribute_names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                call_names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                call_names.add(node.func.attr)
        elif isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            attribute_names.add(node.attr)

    assert {
        "redis",
        "openai",
        "telegram",
        "docker",
        "systemd",
        "requests",
        "httpx",
        "aiohttp",
        "urllib",
        "subprocess",
    }.isdisjoint(imported_roots)
    assert {
        "create_async_engine",
        "async_sessionmaker",
        "sessionmaker",
        "from_env",
        "xadd",
        "xack",
        "xgroup_create",
        "xreadgroup",
        "run_forever",
        "systemctl",
        "pg_dump",
        "pg_restore",
        "psql",
        "send_message",
        "edit_message_text",
    }.isdisjoint(call_names | names | attribute_names)
    for forbidden in (
        "systemctl",
        "pg_dump",
        "pg_restore",
        "psql ",
        "send_message",
        "edit_message_text",
    ):
        assert forbidden not in source
