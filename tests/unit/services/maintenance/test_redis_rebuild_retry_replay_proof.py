from __future__ import annotations

import json

from services.maintenance.redis_rebuild_retry_replay_proof import (
    build_redis_rebuild_retry_replay_proof,
    render_sanitized_json,
)


RAW_VALUES = (
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
    "33333333-3333-4333-8333-333333333333",
    "44444444-4444-4444-8444-444444444444",
    "55555555-5555-4555-8555-555555555555",
    "66666666-6666-4666-8666-666666666666",
    "notify:retry-intent:",
    "notify:replay-intent:",
    "12345",
    "https://private.example.invalid",
    "postgresql://",
    "postgresql+psycopg://",
    "redis://",
    "runtime.env",
    "sentinel_secret",
)


def test_redis_rebuild_plan_is_postgres_derived_representation_only() -> None:
    report = build_redis_rebuild_retry_replay_proof()
    rebuild = report["redis_rebuild"]

    assert rebuild["status"] == "pass"
    assert rebuild["redis_treated_as_lost_transient"] is True
    assert rebuild["postgres_durable_sources_are_rebuild_authority"] is True
    assert rebuild["plan_readback_representation_only"] is True
    assert rebuild["actual_redis_rebuild_execution_attempted"] is False
    assert set(rebuild["required_categories_present"]) == {
        "pending_event_outbox_work",
        "due_failed_retryable_notification_plans",
        "open_delivery_replay_requests",
        "stale_running_job_attempts",
    }
    assert rebuild["queue_bucket_labels"] == ["maintenance", "replay", "notification_delivery"]
    assert rebuild["raw_stream_ids_omitted"] is True
    assert rebuild["redis_group_pending_readback"]["represented_as_expected_shape_only"] is True
    assert rebuild["redis_group_pending_readback"]["redis_read_attempted"] is False


def test_retry_due_path_uses_existing_retry_decision_semantics() -> None:
    report = build_redis_rebuild_retry_replay_proof()
    due = report["retry_dlq_replay"]["due_retry_below_ceiling"]

    assert due["decision_action"] == "emit_retry_intent"
    assert due["reason_code"] == "due_retry_promotion"
    assert due["retry_intent_payload_shape_ok"] is True
    assert due["existing_dedupe_key_function_used"] is True
    assert due["dedupe_key_stable"] is True
    assert due["raw_retry_payload_omitted"] is True
    assert due["raw_dedupe_key_omitted"] is True


def test_retry_ceiling_path_produces_dlq_representation() -> None:
    report = build_redis_rebuild_retry_replay_proof()
    ceiling = report["retry_dlq_replay"]["retry_ceiling"]

    assert ceiling["decision_action"] == "dead_letter_retry_ceiling"
    assert ceiling["reason_code"] == "max_notification_retry_attempts_exceeded"
    assert ceiling["retry_intent_emitted"] is False
    assert ceiling["dlq_representation"] == {
        "stage_bucket": "maintenance_delivery_retry",
        "queue_bucket": "maintenance",
        "root_object_bucket": "notification_plan",
        "next_manual_action": "request_delivery_replay_after_operator_fix",
        "replay_hint": "delivery_replay_from_notification_plan",
        "raw_root_id_omitted": True,
    }


def test_terminal_and_send_disabled_paths_do_not_auto_retry() -> None:
    report = build_redis_rebuild_retry_replay_proof()
    terminal = report["retry_dlq_replay"]["terminal_delivery_failure"]
    send_disabled = report["retry_dlq_replay"]["send_disabled_suppressed"]

    assert terminal["retry_decision_action"] == "noop"
    assert terminal["retry_reason_code"] == "delivery_status_not_retryable"
    assert terminal["delivery_result_outcome"] == "failed_terminal"
    assert terminal["should_write_dlq"] is True
    assert terminal["should_emit_retry_intent"] is False
    assert terminal["explicit_replay_candidate"] is True
    assert terminal["dlq_representation"]["replay_hint"] == "delivery_replay_from_notification_plan"

    assert send_disabled["retry_decision_action"] == "noop"
    assert send_disabled["retry_reason_code"] == "delivery_status_not_retryable"
    assert send_disabled["replay_recovery_mode"] == "explicit_delivery_replay_only"
    assert send_disabled["auto_retry_allowed"] is False
    assert send_disabled["retry_intent_allowed"] is False
    assert send_disabled["replay_dispatch_allowed_without_request"] is False


def test_delivery_replay_is_notification_plan_only_and_does_not_recompute_upstream() -> None:
    report = build_redis_rebuild_retry_replay_proof()
    replay = report["retry_dlq_replay"]["delivery_replay"]

    assert replay["accepted_root_object_bucket"] == "notification_plan"
    assert replay["decision_action"] == "emit_replay_intent"
    assert replay["reason_code"] == "explicit_delivery_replay"
    assert replay["replay_intent_payload_shape_ok"] is True
    assert replay["notification_plan_based_only"] is True
    assert replay["dedupe_key_stable"] is True
    assert replay["unsupported_upstream_roots_rejected"] == {
        "analysis": "unsupported_replay_root",
        "judge_output": "unsupported_replay_root",
        "evidence_bundle": "unsupported_replay_root",
        "candidate_group": "unsupported_replay_root",
        "source_message": "unsupported_replay_root",
    }
    assert replay["upstream_recompute_attempts"] == {
        "analysis": 0,
        "judge_output": 0,
        "evidence_bundle": 0,
        "candidate_group": 0,
        "source_message": 0,
    }
    assert report["side_effect_authority"]["replay_recomputed_upstream"] is False


def test_stale_running_job_is_abandoned_retryable_representation_without_db_mutation() -> None:
    report = build_redis_rebuild_retry_replay_proof()
    abandoned = report["abandoned_job_handling"]

    assert abandoned["status"] == "pass"
    assert abandoned["durable_source"] == "job_attempts"
    assert abandoned["classification"] == "abandoned_retryable_representation"
    assert abandoned["planned_transition_bucket"] == {
        "from": "stale_running",
        "to": "abandoned_then_retryable_representation",
    }
    assert abandoned["db_mutation_attempted"] is False
    assert abandoned["raw_job_attempt_id_omitted"] is True


def test_authority_booleans_and_completion_claims_remain_bounded() -> None:
    report = build_redis_rebuild_retry_replay_proof()

    assert report["ok"] is True
    assert all(value is False for value in report["side_effect_authority"].values())
    assert report["completion_claims"]["redis_rebuild_code_proof_ready"] is True
    assert report["completion_claims"]["retry_dlq_replay_code_proof_ready"] is True
    assert report["completion_claims"]["actual_redis_rebuild_complete"] is False
    assert report["completion_claims"]["actual_production_replay_complete"] is False
    assert report["completion_claims"]["production_rollout_complete"] is False
    assert report["completion_claims"]["final_bot_complete"] is False
    assert report["completion_claims"]["one_hundred_percent_complete"] is False
    assert report["open_gates"] == {
        "AUTHORITY_OPEN": True,
        "ROLLOUT_OPEN": True,
        "PRODUCTION_ROLLOUT_OPEN": True,
        "FULL_ALWAYS_ON_COLLECTOR_WORKER_OPEN": True,
    }


def test_rendered_json_is_sanitized_and_compact() -> None:
    output = render_sanitized_json(build_redis_rebuild_retry_replay_proof())
    parsed = json.loads(output)

    assert output.endswith("\n")
    assert "\n" not in output[:-1]
    assert parsed["schema_version"] == "redis_rebuild_retry_replay_proof_v1"
    assert parsed["raw_values_printed"] is False
    for raw in RAW_VALUES:
        assert raw not in output
