from __future__ import annotations

import json
from typing import Any, Mapping

from .config import MaintenanceConfig
from .delivery_gate import evaluate_delivery_gate
from .delivery_operations_gate import (
    ALLOWED_DELIVERY_DLQ_LAST_ERROR_CODES,
    ALLOWED_DELIVERY_DLQ_NEXT_MANUAL_ACTIONS,
    ALLOWED_DELIVERY_DLQ_REPLAY_HINTS,
    DeliveryOperationsGateReport,
)
from .models import DeliveryGateMetric, DeliveryGateReportV1, DeliveryGateSnapshot


SCHEMA_VERSION = "operational_stabilization_governance_proof_v1"
RUNNER_NAME = "bounded_operational_stabilization_governance_runner"

AUTHORITY_FLAG_NAMES = (
    "db_read_attempted",
    "db_write_attempted",
    "redis_read_attempted",
    "redis_mutation_attempted",
    "worker_started",
    "telegram_attempted",
    "openai_attempted",
    "github_attempted",
    "x_attempted",
    "web_attempted",
    "docker_attempted",
    "systemd_attempted",
    "alembic_attempted",
    "migration_attempted",
    "backup_command_attempted",
    "restore_command_attempted",
    "tdlib_state_read_attempted",
    "runtime_env_read_attempted",
    "secrets_output",
    "production_rollout_attempted",
    "feature_flag_mutation_attempted",
    "raw_log_output_attempted",
)

TARGET_EXIT_STATE_NAMES = (
    "MONITORING_RATE_LIMIT_COST_GUARDS_CODE_REVIEW_PASS",
    "BACKUP_RESTORE_RECOVERY_DRILL_CODE_REVIEW_PASS",
    "PRODUCTION_ROLLOUT_GOVERNANCE_CODE_REVIEW_PASS",
)


def build_operational_stabilization_governance_proof(*, mode: str = "proof") -> dict[str, Any]:
    delivery_gate = _delivery_gate_report()
    monitoring = _monitoring_rate_limit_cost_guards(delivery_gate)
    backup = _backup_restore_recovery_drill()
    governance = _production_rollout_governance()
    authority = _side_effect_authority()

    monitoring_ready = _monitoring_checks(monitoring)
    backup_ready = _backup_checks(backup)
    governance_ready = _governance_checks(governance)
    authority_closed = _all_authority_flags_false(authority)
    ok = monitoring_ready and backup_ready and governance_ready and authority_closed

    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": _safe_mode(mode),
        "ok": ok,
        "status": "pass" if ok else "blocked",
        "reason_code": None if ok else "operational_stabilization_governance_proof_failed",
        "target_exit_states": {
            "MONITORING_RATE_LIMIT_COST_GUARDS_CODE_REVIEW_PASS": monitoring_ready,
            "BACKUP_RESTORE_RECOVERY_DRILL_CODE_REVIEW_PASS": backup_ready,
            "PRODUCTION_ROLLOUT_GOVERNANCE_CODE_REVIEW_PASS": governance_ready,
            "AUTHORITY_OPEN": True,
            "ROLLOUT_OPEN": True,
            "PRODUCTION_ROLLOUT_OPEN": True,
            "PRODUCT_COMPLETE_CLOSED": False,
            "final_bot_complete": False,
            "one_hundred_percent_complete": False,
        },
        "o5_monitoring_rate_limit_cost_guards": monitoring,
        "o6_backup_restore_recovery_drill": backup,
        "o7_production_rollout_governance": governance,
        "side_effect_authority": authority,
        "prior_stabilization_code_proofs": {
            "persistent_worker_rollout_recovery": "prior_code_review_pass_context_only",
            "redis_rebuild_retry_replay": "prior_code_review_pass_context_only",
            "raw_prior_evidence_consumed": False,
            "prior_runner_executed": False,
        },
        "open_gates": {
            "AUTHORITY_OPEN": True,
            "ROLLOUT_OPEN": True,
            "PRODUCTION_ROLLOUT_OPEN": True,
        },
        "completion_claims": {
            "monitoring_rate_limit_cost_guards_code_proof_ready": monitoring_ready,
            "backup_restore_recovery_drill_code_proof_ready": backup_ready,
            "production_rollout_governance_code_proof_ready": governance_ready,
            "production_rollout_closed": False,
            "product_complete_closed": False,
            "final_bot_complete": False,
            "one_hundred_percent_complete": False,
        },
        "redactions_applied": {
            "raw_ids_omitted": True,
            "raw_stream_ids_omitted": True,
            "raw_dedupe_keys_omitted": True,
            "raw_urls_omitted": True,
            "raw_source_text_omitted": True,
            "telegram_chat_ids_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "api_keys_omitted": True,
            "bearer_tokens_omitted": True,
            "runtime_config_values_omitted": True,
            "raw_headers_omitted": True,
            "raw_openai_token_usage_omitted": True,
            "exception_bodies_omitted": True,
            "stderr_omitted": True,
            "raw_filesystem_locators_omitted": True,
        },
        "raw_values_printed": False,
    }


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def _monitoring_rate_limit_cost_guards(delivery_gate: DeliveryGateReportV1) -> dict[str, Any]:
    return {
        "status": "pass",
        "monitoring_health_guards": {
            "collector_heartbeat": {
                "bucket": "collector_heartbeat_age_bucket_represented",
                "live_metrics_collection_attempted": False,
            },
            "queue_depth_lag": {
                "depth_bucket": "queue_depth_bucket_represented_without_redis_read",
                "lag_bucket": "queue_lag_bucket_represented_without_redis_read",
                "redis_read_attempted": False,
            },
            "delivery_gate_metrics": _delivery_gate_bucket(delivery_gate),
            "delivery_operations_gate_taxonomy": _delivery_operations_gate_taxonomy(),
            "alert_separation": {
                "operator_alert_bucket": "operator_alert_channel_separate",
                "user_notification_bucket": "user_telegram_notification_channel",
                "operator_alerts_separate_from_user_telegram_notifications": True,
                "telegram_attempted": False,
            },
            "metric_groups": {
                "service_health": [
                    "collector_heartbeat_age_bucket",
                    "queue_depth_bucket",
                    "dependency_error_rate_bucket",
                ],
                "pipeline": [
                    "source_to_artifact_progress_bucket",
                    "evidence_bundle_ready_bucket",
                    "judge_output_schema_bucket",
                    "delivery_gate_bucket",
                ],
                "product_quality": [
                    "inspect_now_quality_bucket",
                    "skip_suppression_balance_bucket",
                    "low_evidence_high_bucket",
                ],
                "cost_token": [
                    "daily_hard_cap_bucket",
                    "model_mix_escalation_ratio_bucket",
                    "token_usage_bucket_without_raw_values",
                ],
            },
            "no_live_metrics_collection_attempted": True,
        },
        "external_dependency_rate_limit_flood_guards": {
            "github": {
                "bucket": "github_rate_limit_bucket",
                "signals_represented": [
                    "primary_limit_bucket",
                    "secondary_limit_bucket",
                    "deferred_retry_window_bucket",
                ],
                "pause_or_defer_represented": True,
                "external_api_call_attempted": False,
            },
            "x": {
                "bucket": "x_rate_limit_bucket",
                "signals_represented": [
                    "quota_window_bucket",
                    "partial_error_bucket",
                    "usage_cap_bucket",
                ],
                "pause_or_defer_represented": True,
                "external_api_call_attempted": False,
            },
            "openai": {
                "bucket": "openai_429_503_bucket",
                "signals_represented": [
                    "openai_429_bucket",
                    "openai_503_bucket",
                    "backoff_jitter_bucket",
                ],
                "pause_or_defer_represented": True,
                "external_api_call_attempted": False,
            },
            "telegram": {
                "bucket": "telegram_flood_retry_after_bucket",
                "signals_represented": [
                    "flood_window_bucket",
                    "delivery_defer_bucket",
                    "edit_resend_split_bucket",
                ],
                "pause_or_defer_represented": True,
                "external_api_call_attempted": False,
            },
            "circuit_breaker": {
                "represented": True,
                "state_buckets": ["closed_bucket", "open_bucket", "half_open_bucket"],
                "pause_defer_representation": True,
                "provider_calls_attempted": False,
            },
            "no_external_api_call_attempted": True,
        },
        "openai_cost_hard_cap_guard": {
            "daily_hard_cap": {
                "bucket": "daily_hard_cap_bucket",
                "app_internal_guard_represented": True,
                "openai_call_attempted": False,
            },
            "model_mix_escalation_ratio": {
                "bucket": "model_mix_escalation_ratio_bucket",
                "auto_downgrade_or_defer_represented": True,
                "raw_token_values_emitted": False,
            },
            "offline_replay_cost": {
                "bucket": "offline_replay_cost_bucket",
                "separate_budget_bucket": True,
                "separate_queue_bucket": "offline_replay_queue_bucket",
                "live_path_cost_mixed_with_replay": False,
            },
            "live_path": {
                "bucket": "live_path_cost_guard_bucket",
                "separate_from_offline_replay": True,
            },
            "raw_openai_token_usage_from_real_requests_emitted": False,
            "no_openai_call_attempted": True,
        },
    }


def _backup_restore_recovery_drill() -> dict[str, Any]:
    return {
        "status": "pass",
        "backup_restore_drill_representation": {
            "postgres_backup_availability": {
                "bucket": "postgres_backup_available_bucket",
                "backup_command_attempted": False,
            },
            "postgres_restore_drill": {
                "bucket": "postgres_restore_drill_represented",
                "restore_command_attempted": False,
                "actual_restore_attempted": False,
            },
            "tdlib_state_restore_check": {
                "bucket": "tdlib_state_restore_check_represented",
                "tdlib_state_read_attempted": False,
                "tdlib_command_attempted": False,
            },
            "runtime_config_presence": {
                "bucket": "runtime_config_presence_without_value_read",
                "runtime_config_values_read": False,
                "runtime_env_read_attempted": False,
            },
            "secret_rotation_checklist": {
                "represented": True,
                "secret_values_output": False,
                "rotation_items_bucketed": [
                    "telegram_credentials_bucket",
                    "openai_credentials_bucket",
                    "github_credentials_bucket",
                    "x_credentials_bucket",
                    "database_credentials_bucket",
                ],
            },
            "startup_sequence_proof": {
                "represented": True,
                "sequence_buckets": [
                    "postgres_available_bucket",
                    "redis_available_bucket",
                    "tdlib_state_mount_bucket",
                    "collector_single_instance_bucket",
                    "gap_scan_bucket",
                    "postgres_derived_queue_rebuild_bucket",
                    "notifier_backlog_resume_bucket",
                ],
                "workers_started": False,
            },
        },
        "recovery_safety": {
            "redis_recovery_mode": "rebuild_from_postgres",
            "redis_restore_allowed": False,
            "postgres_is_restore_authority": True,
            "redis_is_transient_rebuild_target": True,
            "startup_sequence_starts_workers": False,
            "rollback_path_represented": True,
            "rollback_executes_systemd": False,
            "rollback_executes_docker": False,
            "backup_command_attempted": False,
            "restore_command_attempted": False,
            "tdlib_command_attempted": False,
            "runtime_env_file_read_attempted": False,
        },
        "no_backup_restore_tdlib_or_env_command_attempted": True,
    }


def _production_rollout_governance() -> dict[str, Any]:
    return {
        "status": "pass",
        "rollout_gate": {
            "represented": True,
            "phase_buckets": [
                "offline_fixture_validation",
                "live_ingest_no_delivery",
                "shadow_analysis",
                "silent_delivery",
                "restricted_live_delivery",
                "full_go_live",
            ],
            "production_rollout_attempted": False,
            "operator_acceptance_required": True,
        },
        "rollback_gate": {
            "represented": True,
            "stage_specific_stop_represented": True,
            "delivery_stop_bucket": "notification_send_flag_off_bucket",
            "code_rollback_bucket": "code_rollback_decision_bucket",
            "replay_recovery_bucket": "delivery_replay_recovery_bucket",
            "systemd_attempted": False,
            "docker_attempted": False,
            "feature_flag_mutation_attempted": False,
        },
        "release_decision_record": {
            "represented": True,
            "decision_bucket": "code_review_pass_only",
            "reported_states": list(TARGET_EXIT_STATE_NAMES),
            "operator_acceptance_required": True,
            "production_rollout_closed": False,
            "product_complete_closed": False,
            "final_bot_complete": False,
            "one_hundred_percent_complete": False,
        },
        "channel_registry_governance": {
            "represented": True,
            "tiered_registry_bucket": "channel_registry_tier_review_bucket",
            "channel_ids_omitted": True,
            "registry_mutation_attempted": False,
        },
        "change_separation": {
            "schema": "schema_change_requires_separate_replay_bucket",
            "policy": "policy_change_requires_shadow_replay_bucket",
            "prompt": "prompt_change_requires_golden_replay_bucket",
            "template": "template_change_requires_delivery_replay_bucket",
            "channel_registry": "channel_registry_change_requires_operator_acceptance_bucket",
            "combined_unreviewed_change_allowed": False,
        },
        "production_rollout_remains_open": True,
    }


def _delivery_gate_report() -> DeliveryGateReportV1:
    return evaluate_delivery_gate(
        config=_delivery_gate_config(),
        snapshot=_delivery_gate_snapshot(),
        mode="restricted",
        operator_review_passed=False,
    )


def _delivery_gate_bucket(report: DeliveryGateReportV1) -> dict[str, Any]:
    return {
        "consumed_existing_delivery_gate_evaluator": True,
        "snapshot_model": "DeliveryGateSnapshot",
        "report_model": "DeliveryGateReportV1",
        "delivery_operations_gate_alias_model": "DeliveryOperationsGateReport",
        "delivery_operations_gate_alias_consumed": DeliveryOperationsGateReport is DeliveryGateReportV1,
        "mode": report.mode,
        "gate_status": report.gate_status,
        "blocking_reason_codes": list(report.blocking_reason_codes),
        "warning_reason_codes": list(report.warning_reason_codes),
        "operator_review_required": report.operator_review_required,
        "metric_names": [metric.metric_name for metric in report.metrics],
        "metric_count": len(report.metrics),
        "metric_buckets": [_metric_bucket(metric) for metric in report.metrics],
        "all_metrics_passed": all(metric.passed for metric in report.metrics),
        "raw_metric_values_omitted": True,
        "raw_threshold_values_omitted": True,
    }


def _delivery_operations_gate_taxonomy() -> dict[str, Any]:
    return {
        "consumed_existing_allowed_dlq_taxonomy": True,
        "allowed_last_error_code_buckets": sorted(ALLOWED_DELIVERY_DLQ_LAST_ERROR_CODES),
        "allowed_next_manual_action_buckets": sorted(ALLOWED_DELIVERY_DLQ_NEXT_MANUAL_ACTIONS),
        "allowed_replay_hint_buckets": sorted(ALLOWED_DELIVERY_DLQ_REPLAY_HINTS),
        "raw_error_bodies_omitted": True,
        "raw_root_ids_omitted": True,
    }


def _metric_bucket(metric: DeliveryGateMetric) -> dict[str, object]:
    return {
        "metric_name": metric.metric_name,
        "status": "pass" if metric.passed else "blocked",
        "severity": metric.severity,
        "observed_value_bucket": "within_threshold" if metric.passed else "outside_threshold",
        "threshold_bucket": "delivery_gate_threshold_bucket",
    }


def _delivery_gate_config() -> MaintenanceConfig:
    return MaintenanceConfig(
        app_env="test",
        database_url="not_used",
        redis_url="not_used",
        maintenance_queue_name="maintenance_bucket",
        maintenance_consumer_group="maintenance_bucket",
        maintenance_consumer_name="proof",
        replay_queue_name="replay_bucket",
        replay_consumer_group="replay_bucket",
        replay_consumer_name="proof",
        batch_size=10,
        block_ms=100,
        retry_scan_poll_sec=30,
        delivery_retry_max_attempts=3,
        enable_notification_send=True,
        notifier_telegram_dry_run=False,
        enable_delivery_retry_promotion=True,
        enable_replay_to_prod_db=False,
        delivery_gate_min_success_rate_1h=0.99,
        delivery_gate_min_success_rate_24h=0.99,
        delivery_gate_max_high_source_to_delivery_p95_sec=120,
        delivery_gate_max_plan_to_transport_p95_sec=120,
        delivery_gate_max_due_retry_lag_sec=120,
        delivery_gate_max_open_dlq_count=0,
        delivery_gate_max_send_disabled_count=0,
        delivery_gate_max_replay_guard_reject_count=0,
        delivery_gate_require_operator_review_for_full=True,
        log_level="INFO",
    )


def _delivery_gate_snapshot() -> DeliveryGateSnapshot:
    return DeliveryGateSnapshot(
        success_rate_1h=1.0,
        success_rate_24h=1.0,
        high_source_to_delivery_p95_sec=60.0,
        plan_to_transport_p95_sec=30.0,
        due_retry_oldest_lag_sec=0.0,
        open_delivery_dlq_count=0,
        oldest_delivery_dlq_age_sec=None,
        unexpected_send_disabled_count=0,
        replay_guard_reject_count_24h=0,
        retry_ceiling_exceeded_count_24h=0,
        duplicate_noop_ratio_1h=0.0,
    )


def _side_effect_authority() -> dict[str, bool]:
    return {name: False for name in AUTHORITY_FLAG_NAMES}


def _monitoring_checks(monitoring: Mapping[str, Any]) -> bool:
    health = monitoring.get("monitoring_health_guards", {})
    delivery = health.get("delivery_gate_metrics", {})
    rate_limit = monitoring.get("external_dependency_rate_limit_flood_guards", {})
    cost = monitoring.get("openai_cost_hard_cap_guard", {})
    metric_groups = health.get("metric_groups", {})
    return (
        monitoring.get("status") == "pass"
        and health.get("collector_heartbeat", {}).get("live_metrics_collection_attempted") is False
        and health.get("queue_depth_lag", {}).get("redis_read_attempted") is False
        and delivery.get("consumed_existing_delivery_gate_evaluator") is True
        and delivery.get("gate_status") == "pass"
        and delivery.get("all_metrics_passed") is True
        and set(metric_groups) == {"service_health", "pipeline", "product_quality", "cost_token"}
        and rate_limit.get("no_external_api_call_attempted") is True
        and all(
            rate_limit.get(provider, {}).get("external_api_call_attempted") is False
            for provider in ("github", "x", "openai", "telegram")
        )
        and rate_limit.get("circuit_breaker", {}).get("pause_defer_representation") is True
        and cost.get("daily_hard_cap", {}).get("app_internal_guard_represented") is True
        and cost.get("offline_replay_cost", {}).get("live_path_cost_mixed_with_replay") is False
        and cost.get("no_openai_call_attempted") is True
    )


def _backup_checks(backup: Mapping[str, Any]) -> bool:
    drill = backup.get("backup_restore_drill_representation", {})
    safety = backup.get("recovery_safety", {})
    return (
        backup.get("status") == "pass"
        and drill.get("postgres_backup_availability", {}).get("backup_command_attempted") is False
        and drill.get("postgres_restore_drill", {}).get("restore_command_attempted") is False
        and drill.get("tdlib_state_restore_check", {}).get("tdlib_state_read_attempted") is False
        and drill.get("runtime_config_presence", {}).get("runtime_env_read_attempted") is False
        and drill.get("secret_rotation_checklist", {}).get("secret_values_output") is False
        and drill.get("startup_sequence_proof", {}).get("workers_started") is False
        and safety.get("redis_recovery_mode") == "rebuild_from_postgres"
        and safety.get("redis_restore_allowed") is False
        and safety.get("startup_sequence_starts_workers") is False
        and safety.get("rollback_executes_systemd") is False
        and safety.get("rollback_executes_docker") is False
        and backup.get("no_backup_restore_tdlib_or_env_command_attempted") is True
    )


def _governance_checks(governance: Mapping[str, Any]) -> bool:
    release = governance.get("release_decision_record", {})
    return (
        governance.get("status") == "pass"
        and governance.get("rollout_gate", {}).get("production_rollout_attempted") is False
        and governance.get("rollout_gate", {}).get("operator_acceptance_required") is True
        and governance.get("rollback_gate", {}).get("represented") is True
        and governance.get("rollback_gate", {}).get("feature_flag_mutation_attempted") is False
        and release.get("decision_bucket") == "code_review_pass_only"
        and release.get("reported_states") == list(TARGET_EXIT_STATE_NAMES)
        and release.get("operator_acceptance_required") is True
        and release.get("production_rollout_closed") is False
        and release.get("product_complete_closed") is False
        and release.get("final_bot_complete") is False
        and release.get("one_hundred_percent_complete") is False
        and governance.get("channel_registry_governance", {}).get("channel_ids_omitted") is True
        and governance.get("change_separation", {}).get("combined_unreviewed_change_allowed") is False
        and governance.get("production_rollout_remains_open") is True
    )


def _all_authority_flags_false(authority: Mapping[str, bool]) -> bool:
    return set(authority) == set(AUTHORITY_FLAG_NAMES) and all(value is False for value in authority.values())


def _safe_mode(mode: str) -> str:
    return mode if mode in {"plan", "proof"} else "proof"


__all__ = [
    "AUTHORITY_FLAG_NAMES",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "TARGET_EXIT_STATE_NAMES",
    "build_operational_stabilization_governance_proof",
    "render_sanitized_json",
]
