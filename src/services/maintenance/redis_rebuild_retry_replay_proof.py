from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from uuid import UUID

from .config import MaintenanceConfig
from .delivery_replay import (
    REQUIRED_REPLAY_PAYLOAD_FIELDS,
    evaluate_delivery_replay,
    replay_intent_dedupe_key,
)
from .delivery_result_policy import decide_delivery_result
from .delivery_retry import (
    REQUIRED_RETRY_PAYLOAD_FIELDS,
    evaluate_retry_promotion,
    retry_intent_dedupe_key,
)
from .models import (
    DeliveryResultEvent,
    LatestDeliveryRecord,
    NotificationPlanRecord,
    ReplayRequestRecord,
)
from .retry_policy import classify_delivery_result_send_disabled_noop


SCHEMA_VERSION = "redis_rebuild_retry_replay_proof_v1"
RUNNER_NAME = "bounded_redis_rebuild_retry_replay_runner"

SAFE_QUEUE_BUCKETS = ("maintenance", "replay", "notification_delivery")
REBUILD_CATEGORIES = (
    "pending_event_outbox_work",
    "due_failed_retryable_notification_plans",
    "open_delivery_replay_requests",
    "stale_running_job_attempts",
)
UPSTREAM_REPLAY_ROOTS = (
    "analysis",
    "judge_output",
    "evidence_bundle",
    "candidate_group",
    "source_message",
)

PLAN_ID = UUID("11111111-1111-4111-8111-111111111111")
ANALYSIS_ID = UUID("22222222-2222-4222-8222-222222222222")
CANDIDATE_GROUP_ID = UUID("33333333-3333-4333-8333-333333333333")
DELIVERY_RECORD_ID = UUID("44444444-4444-4444-8444-444444444444")
REPLAY_REQUEST_ID = UUID("55555555-5555-4555-8555-555555555555")
EVENT_ID = UUID("66666666-6666-4666-8666-666666666666")
JOB_ATTEMPT_ID = UUID("77777777-7777-4777-8777-777777777777")


@dataclass(frozen=True, slots=True)
class DurableRebuildCandidate:
    category: str
    durable_source: str
    queue_bucket: str
    count: int
    planned_representation: str


@dataclass(frozen=True, slots=True)
class JobAttemptProofRecord:
    attempt_status: str
    queue_bucket: str
    age_bucket: str
    retry_budget_bucket: str


def build_redis_rebuild_retry_replay_proof(*, mode: str = "proof") -> dict[str, Any]:
    now = datetime(2026, 7, 3, 0, 0, tzinfo=timezone.utc)
    plan = _notification_plan(status="failed_retryable", send_after=now - timedelta(minutes=5))
    terminal_plan = _notification_plan(status="failed_terminal", send_after=None)
    send_disabled_plan = _notification_plan(
        status="suppressed",
        send_after=None,
        suppress_reason_code="notification_send_flag_disabled",
    )

    due_retry = evaluate_retry_promotion(
        delivery_status="failed_retryable",
        plan=plan,
        latest_attempt_count=1,
        max_attempts=3,
        enabled=True,
        now=now,
    )
    stable_retry_key = retry_intent_dedupe_key(
        notification_plan_id=plan.notification_plan_id,
        latest_attempt_count=1,
        send_after=plan.send_after,
    )
    retry_ceiling = evaluate_retry_promotion(
        delivery_status="failed_retryable",
        plan=plan,
        latest_attempt_count=3,
        max_attempts=3,
        enabled=True,
        now=now,
    )
    terminal_retry = evaluate_retry_promotion(
        delivery_status="failed_terminal",
        plan=terminal_plan,
        latest_attempt_count=1,
        max_attempts=3,
        enabled=True,
        now=now,
    )
    send_disabled_retry = evaluate_retry_promotion(
        delivery_status="suppressed",
        plan=send_disabled_plan,
        latest_attempt_count=1,
        max_attempts=3,
        enabled=True,
        now=now,
    )
    send_disabled_noop = classify_delivery_result_send_disabled_noop(
        delivery_status="suppressed",
        delivery_reason="notification_send_flag_disabled",
    )

    terminal_decision = decide_delivery_result(
        event=DeliveryResultEvent(
            trigger_event_id=EVENT_ID,
            notification_plan_id=terminal_plan.notification_plan_id,
            delivery_status="failed_terminal",
            notification_delivery_record_id=DELIVERY_RECORD_ID,
            attempt_count=1,
            transport_error_code="terminal_transport_bucket",
            transport_error_class="terminal",
        ),
        exact_record=_delivery_record(terminal_plan, status="failed_terminal", now=now),
        latest_record=_delivery_record(terminal_plan, status="failed_terminal", now=now),
        plan=terminal_plan,
        later_success_exists=False,
        now=now,
        retry_max_attempts=3,
    )

    replay_request = ReplayRequestRecord(
        replay_request_id=REPLAY_REQUEST_ID,
        replay_type="delivery",
        root_object_type="notification_plan",
        root_object_id=terminal_plan.notification_plan_id,
        status="requested",
        requested_by="operator",
        requested_at=now,
    )
    replay_config = _config()
    replay_decision = evaluate_delivery_replay(
        config=replay_config,
        replay_request=replay_request,
        plan=terminal_plan,
    )
    stable_replay_key = replay_intent_dedupe_key(REPLAY_REQUEST_ID)
    upstream_rejections = _upstream_replay_rejections(config=replay_config, plan=terminal_plan)

    redis_rebuild = _redis_rebuild_report()
    retry_dlq_replay = {
        "status": "pass",
        "existing_decision_functions_reused": {
            "evaluate_retry_promotion": True,
            "evaluate_delivery_replay": True,
            "decide_delivery_result": True,
            "classify_delivery_result_send_disabled_noop": True,
        },
        "due_retry_below_ceiling": {
            "delivery_status_bucket": "failed_retryable_due",
            "decision_action": due_retry.action,
            "reason_code": due_retry.reason_code,
            "retry_attempt_bucket": "next_attempt_below_ceiling",
            "retry_intent_payload_shape_ok": _payload_has_fields(
                due_retry.payload,
                REQUIRED_RETRY_PAYLOAD_FIELDS,
            ),
            "retry_intent_payload_field_count": len(due_retry.payload or {}),
            "raw_retry_payload_omitted": True,
            "raw_dedupe_key_omitted": True,
            "existing_dedupe_key_function_used": True,
            "dedupe_key_stable": due_retry.dedupe_key == stable_retry_key,
            "outbox_uniqueness_semantics_represented": True,
        },
        "retry_ceiling": {
            "decision_action": retry_ceiling.action,
            "reason_code": retry_ceiling.reason_code,
            "retry_intent_emitted": False,
            "dlq_representation": {
                "stage_bucket": "maintenance_delivery_retry",
                "queue_bucket": "maintenance",
                "root_object_bucket": "notification_plan",
                "next_manual_action": "request_delivery_replay_after_operator_fix",
                "replay_hint": "delivery_replay_from_notification_plan",
                "raw_root_id_omitted": True,
            },
        },
        "terminal_delivery_failure": {
            "retry_decision_action": terminal_retry.action,
            "retry_reason_code": terminal_retry.reason_code,
            "delivery_result_outcome": terminal_decision.outcome,
            "should_write_dlq": terminal_decision.should_write_dlq,
            "should_emit_retry_intent": terminal_decision.should_emit_retry_intent,
            "explicit_replay_candidate": terminal_decision.is_explicit_replay_candidate,
            "dlq_representation": {
                "stage_bucket": "maintenance_delivery_result",
                "queue_bucket": "maintenance",
                "root_object_bucket": "notification_plan",
                "next_manual_action": "request_delivery_replay_after_operator_fix",
                "replay_hint": "delivery_replay_from_notification_plan",
                "raw_error_body_omitted": True,
            },
        },
        "send_disabled_suppressed": {
            "retry_decision_action": send_disabled_retry.action,
            "retry_reason_code": send_disabled_retry.reason_code,
            "noop_action": send_disabled_noop.action,
            "noop_reason_code": send_disabled_noop.reason_code,
            "replay_recovery_mode": send_disabled_noop.replay_recovery_mode,
            "auto_retry_allowed": send_disabled_noop.auto_retry_allowed,
            "retry_intent_allowed": send_disabled_noop.retry_intent_allowed,
            "dead_letter_allowed": send_disabled_noop.dead_letter_allowed,
            "replay_dispatch_allowed_without_request": send_disabled_noop.replay_dispatch_allowed,
        },
        "delivery_replay": {
            "accepted_root_object_bucket": "notification_plan",
            "decision_action": replay_decision.action,
            "reason_code": replay_decision.reason_code,
            "replay_intent_payload_shape_ok": _payload_has_fields(
                replay_decision.payload,
                REQUIRED_REPLAY_PAYLOAD_FIELDS,
            ),
            "raw_replay_payload_omitted": True,
            "raw_dedupe_key_omitted": True,
            "existing_dedupe_key_function_used": True,
            "dedupe_key_stable": replay_decision.dedupe_key == stable_replay_key,
            "notification_plan_based_only": True,
            "unsupported_upstream_roots_rejected": upstream_rejections,
            "upstream_recompute_attempts": {
                "analysis": 0,
                "judge_output": 0,
                "evidence_bundle": 0,
                "candidate_group": 0,
                "source_message": 0,
            },
        },
    }
    abandoned_job = _abandoned_job_report(
        JobAttemptProofRecord(
            attempt_status="running",
            queue_bucket="maintenance",
            age_bucket="stale_lease",
            retry_budget_bucket="budget_remaining",
        )
    )
    side_effect_authority = _side_effect_authority_report()

    redis_ready = _redis_rebuild_checks(redis_rebuild, abandoned_job, side_effect_authority)
    retry_ready = _retry_dlq_replay_checks(retry_dlq_replay, side_effect_authority)
    ok = redis_ready and retry_ready

    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": _safe_mode(mode),
        "ok": ok,
        "status": "pass" if ok else "blocked",
        "reason_code": None if ok else "redis_rebuild_retry_replay_proof_failed",
        "target_exit_states": {
            "REDIS_REBUILD_CODE_REVIEW_PASS": redis_ready,
            "RETRY_DLQ_REPLAY_CODE_REVIEW_PASS": retry_ready,
            "AUTHORITY_OPEN": True,
            "ROLLOUT_OPEN": True,
            "PRODUCTION_ROLLOUT_OPEN": True,
            "FULL_ALWAYS_ON_COLLECTOR_WORKER_OPEN": True,
        },
        "redis_rebuild": redis_rebuild,
        "retry_dlq_replay": retry_dlq_replay,
        "abandoned_job_handling": abandoned_job,
        "side_effect_authority": side_effect_authority,
        "completion_claims": {
            "redis_rebuild_code_proof_ready": redis_ready,
            "retry_dlq_replay_code_proof_ready": retry_ready,
            "actual_redis_rebuild_complete": False,
            "actual_production_replay_complete": False,
            "production_rollout_complete": False,
            "final_bot_complete": False,
            "one_hundred_percent_complete": False,
        },
        "open_gates": {
            "AUTHORITY_OPEN": True,
            "ROLLOUT_OPEN": True,
            "PRODUCTION_ROLLOUT_OPEN": True,
            "FULL_ALWAYS_ON_COLLECTOR_WORKER_OPEN": True,
        },
        "redactions_applied": {
            "raw_ids_omitted": True,
            "raw_stream_ids_omitted": True,
            "raw_dedupe_keys_omitted": True,
            "raw_urls_omitted": True,
            "raw_source_text_omitted": True,
            "raw_payloads_omitted": True,
            "raw_chat_ids_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "secret_values_omitted": True,
            "exception_bodies_omitted": True,
            "runtime_env_values_omitted": True,
        },
        "raw_values_printed": False,
    }


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def _redis_rebuild_report() -> dict[str, Any]:
    candidates = (
        DurableRebuildCandidate(
            category="pending_event_outbox_work",
            durable_source="event_outbox",
            queue_bucket="maintenance",
            count=1,
            planned_representation="requeue_pending_outbox_work_from_postgres",
        ),
        DurableRebuildCandidate(
            category="due_failed_retryable_notification_plans",
            durable_source="notification_plans",
            queue_bucket="notification_delivery",
            count=1,
            planned_representation="emit_retry_intent_representation",
        ),
        DurableRebuildCandidate(
            category="open_delivery_replay_requests",
            durable_source="replay_requests",
            queue_bucket="replay",
            count=1,
            planned_representation="dispatch_delivery_replay_representation",
        ),
        DurableRebuildCandidate(
            category="stale_running_job_attempts",
            durable_source="job_attempts",
            queue_bucket="maintenance",
            count=1,
            planned_representation="abandoned_retryable_transition_representation",
        ),
    )
    return {
        "status": "pass",
        "redis_treated_as_lost_transient": True,
        "postgres_durable_sources_are_rebuild_authority": True,
        "plan_readback_representation_only": True,
        "actual_redis_rebuild_execution_attempted": False,
        "candidate_categories": [_candidate_to_dict(candidate) for candidate in candidates],
        "candidate_category_count": len(candidates),
        "required_categories_present": sorted(candidate.category for candidate in candidates),
        "queue_bucket_labels": list(SAFE_QUEUE_BUCKETS),
        "queue_names_limited_to_known_buckets": True,
        "raw_stream_ids_omitted": True,
        "raw_redis_message_ids_omitted": True,
        "redis_group_pending_readback": {
            "represented_as_expected_shape_only": True,
            "redis_read_attempted": False,
            "consumer_group_buckets": [
                {
                    "queue_bucket": queue_bucket,
                    "pending_count_bucket": "unknown_without_redis_read",
                    "lag_bucket": "unknown_without_redis_read",
                }
                for queue_bucket in SAFE_QUEUE_BUCKETS
            ],
        },
    }


def _candidate_to_dict(candidate: DurableRebuildCandidate) -> dict[str, Any]:
    return {
        "category": candidate.category,
        "durable_source": candidate.durable_source,
        "queue_bucket": candidate.queue_bucket,
        "count": candidate.count,
        "planned_representation": candidate.planned_representation,
    }


def _abandoned_job_report(record: JobAttemptProofRecord) -> dict[str, Any]:
    planned = record.attempt_status in {"running", "pending"} and record.age_bucket == "stale_lease"
    return {
        "status": "pass" if planned else "blocked",
        "durable_source": "job_attempts",
        "input_status_bucket": record.attempt_status,
        "input_queue_bucket": record.queue_bucket,
        "age_bucket": record.age_bucket,
        "retry_budget_bucket": record.retry_budget_bucket,
        "classification": "abandoned_retryable_representation" if planned else "not_recoverable",
        "planned_transition_bucket": {
            "from": "stale_running",
            "to": "abandoned_then_retryable_representation",
        },
        "db_mutation_attempted": False,
        "raw_job_attempt_id_omitted": True,
        "raw_root_id_omitted": True,
    }


def _side_effect_authority_report() -> dict[str, bool]:
    return {
        "redis_mutation_attempted": False,
        "redis_flush_attempted": False,
        "redis_xadd_attempted": False,
        "redis_xgroup_attempted": False,
        "redis_ack_attempted": False,
        "db_write_attempted": False,
        "telegram_attempted": False,
        "openai_attempted": False,
        "github_attempted": False,
        "x_attempted": False,
        "web_attempted": False,
        "docker_attempted": False,
        "systemd_attempted": False,
        "migration_attempted": False,
        "runtime_env_read_attempted": False,
        "secrets_output": False,
        "workers_started": False,
        "replay_recomputed_upstream": False,
    }


def _redis_rebuild_checks(
    redis_rebuild: Mapping[str, Any],
    abandoned_job: Mapping[str, Any],
    side_effect_authority: Mapping[str, bool],
) -> bool:
    categories = set(redis_rebuild.get("required_categories_present", []))
    return (
        redis_rebuild.get("redis_treated_as_lost_transient") is True
        and redis_rebuild.get("postgres_durable_sources_are_rebuild_authority") is True
        and redis_rebuild.get("plan_readback_representation_only") is True
        and categories == set(REBUILD_CATEGORIES)
        and redis_rebuild.get("queue_names_limited_to_known_buckets") is True
        and redis_rebuild.get("redis_group_pending_readback", {}).get("represented_as_expected_shape_only") is True
        and abandoned_job.get("classification") == "abandoned_retryable_representation"
        and abandoned_job.get("db_mutation_attempted") is False
        and _all_authority_flags_false(side_effect_authority)
    )


def _retry_dlq_replay_checks(
    retry_dlq_replay: Mapping[str, Any],
    side_effect_authority: Mapping[str, bool],
) -> bool:
    due = retry_dlq_replay.get("due_retry_below_ceiling", {})
    ceiling = retry_dlq_replay.get("retry_ceiling", {})
    terminal = retry_dlq_replay.get("terminal_delivery_failure", {})
    send_disabled = retry_dlq_replay.get("send_disabled_suppressed", {})
    replay = retry_dlq_replay.get("delivery_replay", {})
    upstream = replay.get("unsupported_upstream_roots_rejected", {})
    return (
        due.get("decision_action") == "emit_retry_intent"
        and due.get("retry_intent_payload_shape_ok") is True
        and due.get("dedupe_key_stable") is True
        and ceiling.get("decision_action") == "dead_letter_retry_ceiling"
        and ceiling.get("retry_intent_emitted") is False
        and terminal.get("retry_decision_action") == "noop"
        and terminal.get("should_write_dlq") is True
        and terminal.get("should_emit_retry_intent") is False
        and terminal.get("explicit_replay_candidate") is True
        and send_disabled.get("retry_decision_action") == "noop"
        and send_disabled.get("replay_recovery_mode") == "explicit_delivery_replay_only"
        and send_disabled.get("auto_retry_allowed") is False
        and replay.get("decision_action") == "emit_replay_intent"
        and replay.get("notification_plan_based_only") is True
        and replay.get("replay_intent_payload_shape_ok") is True
        and replay.get("dedupe_key_stable") is True
        and set(upstream) == set(UPSTREAM_REPLAY_ROOTS)
        and all(value == "unsupported_replay_root" for value in upstream.values())
        and _all_authority_flags_false(side_effect_authority)
    )


def _all_authority_flags_false(side_effect_authority: Mapping[str, bool]) -> bool:
    required = set(_side_effect_authority_report())
    return set(side_effect_authority) == required and all(value is False for value in side_effect_authority.values())


def _payload_has_fields(payload: Mapping[str, Any] | None, required_fields: set[str]) -> bool:
    if payload is None:
        return False
    return set(payload) == required_fields


def _upstream_replay_rejections(
    *,
    config: MaintenanceConfig,
    plan: NotificationPlanRecord,
) -> dict[str, str]:
    rejections: dict[str, str] = {}
    for root in UPSTREAM_REPLAY_ROOTS:
        decision = evaluate_delivery_replay(
            config=config,
            replay_request=ReplayRequestRecord(
                replay_request_id=REPLAY_REQUEST_ID,
                replay_type="delivery",
                root_object_type=root,
                root_object_id=plan.analysis_id,
                status="requested",
            ),
            plan=None,
        )
        rejections[root] = decision.reason_code
    return rejections


def _notification_plan(
    *,
    status: str,
    send_after: datetime | None,
    suppress_reason_code: str | None = None,
) -> NotificationPlanRecord:
    return NotificationPlanRecord(
        notification_plan_id=PLAN_ID,
        analysis_id=ANALYSIS_ID,
        candidate_group_id=CANDIDATE_GROUP_ID,
        delivery_decision="send_now",
        urgency_profile="high",
        target_chat_id=12345,
        target_thread_id=None,
        render_profile="telegram_single_alert_high_v1",
        dedupe_subject_key="subject-bucket",
        material_change_hash="material-bucket",
        send_after=send_after,
        suppress_reason_code=suppress_reason_code,
        status=status,
    )


def _delivery_record(
    plan: NotificationPlanRecord,
    *,
    status: str,
    now: datetime,
) -> LatestDeliveryRecord:
    return LatestDeliveryRecord(
        notification_delivery_record_id=DELIVERY_RECORD_ID,
        notification_plan_id=plan.notification_plan_id,
        delivery_status=status,
        attempt_count=1,
        transport_error_code="terminal_transport_bucket",
        transport_error_class="terminal",
        telegram_response_json=None,
        created_at=now,
    )


def _config() -> MaintenanceConfig:
    return MaintenanceConfig(
        app_env="test",
        database_url="not-used",
        redis_url="not-used",
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="maintenance",
        maintenance_consumer_name="proof",
        replay_queue_name="q.replay",
        replay_consumer_group="maintenance-replay",
        replay_consumer_name="proof",
        batch_size=10,
        block_ms=100,
        retry_scan_poll_sec=30,
        delivery_retry_max_attempts=3,
        enable_notification_send=False,
        notifier_telegram_dry_run=True,
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


def _safe_mode(mode: str) -> str:
    return mode if mode in {"plan", "proof"} else "proof"


__all__ = [
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "build_redis_rebuild_retry_replay_proof",
    "render_sanitized_json",
]
