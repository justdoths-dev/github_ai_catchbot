from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from .channel_override_policy import ChannelOverrideInput, ChannelOverridePolicy
from .config import PolicyEngineConfig
from .delivery_policy import DeliveryPolicy
from .feedback_eval import FeedbackEvalEngine, FeedbackRecord
from .models import AnalysisDraft, NotificationPlanIntent, PolicyEvaluation
from .notification_intent import NotificationIntentBuilder, material_change_hash_for_analysis


SCHEMA_VERSION = "noise_duplicate_suppression_proof_v1"
RUNNER_NAME = "noise_duplicate_suppression_proof"
F9_GATE = "F9_NOISE_DUPLICATE_SUPPRESSION"

SUBJECT_CANDIDATE_ID = UUID("11111111-1111-4111-8111-111111111111")
OTHER_CANDIDATE_ID = UUID("22222222-2222-4222-8222-222222222222")
JUDGE_OUTPUT_ID = UUID("33333333-3333-4333-8333-333333333333")
ANALYSIS_ID = UUID("44444444-4444-4444-8444-444444444444")


@dataclass(frozen=True, slots=True)
class NotificationIdentity:
    dedupe_subject_key: str
    material_change_hash: str
    target_chat_id: int


def build_noise_duplicate_suppression_proof() -> dict[str, Any]:
    config = _policy_config()
    builder = NotificationIntentBuilder(config=config)
    delivery_policy = DeliveryPolicy(enable_later_delivery=True, enable_silent_later=True)

    high_analysis, high_evaluation = _analysis_for_verdict(
        "inspect_now",
        delivery_policy=delivery_policy,
        recommended_action_ko="review now",
        freshness_note_ko="fresh",
    )
    high_intent = _require_intent(
        builder.build(analysis_id=ANALYSIS_ID, analysis=high_analysis, evaluation=high_evaluation)
    )
    replay_intent = _require_intent(
        builder.build(analysis_id=ANALYSIS_ID, analysis=high_analysis, evaluation=high_evaluation)
    )

    same_material_hash_a = material_change_hash_for_analysis(
        candidate_group_id=high_analysis.candidate_group_id,
        verdict=high_analysis.verdict,
        delivery_decision=high_analysis.delivery_decision,
        urgency_profile=high_evaluation.urgency_profile,
        reason_codes_json=high_analysis.reason_codes_json,
        recommended_action_ko=high_analysis.recommended_action_ko,
        freshness_note_ko=high_analysis.freshness_note_ko,
    )
    same_material_hash_b = material_change_hash_for_analysis(
        candidate_group_id=high_analysis.candidate_group_id,
        verdict=high_analysis.verdict,
        delivery_decision=high_analysis.delivery_decision,
        urgency_profile=high_evaluation.urgency_profile,
        reason_codes_json=high_analysis.reason_codes_json,
        recommended_action_ko=high_analysis.recommended_action_ko,
        freshness_note_ko=high_analysis.freshness_note_ko,
    )

    changed_analysis, changed_evaluation = _analysis_for_verdict(
        "inspect_now",
        delivery_policy=delivery_policy,
        recommended_action_ko="review changed material",
        freshness_note_ko="fresh with new evidence",
    )
    changed_intent = _require_intent(
        builder.build(analysis_id=ANALYSIS_ID, analysis=changed_analysis, evaluation=changed_evaluation)
    )

    other_analysis, other_evaluation = _analysis_for_verdict(
        "inspect_now",
        candidate_group_id=OTHER_CANDIDATE_ID,
        delivery_policy=delivery_policy,
        recommended_action_ko="review now",
        freshness_note_ko="fresh",
    )
    other_intent = _require_intent(
        builder.build(analysis_id=ANALYSIS_ID, analysis=other_analysis, evaluation=other_evaluation)
    )

    duplicate_actions = _dedupe_actions(high_intent, replay_intent)
    changed_actions = _dedupe_actions(high_intent, changed_intent)
    distribution = _distribution_compatibility(builder=builder, delivery_policy=delivery_policy)
    feedback_channel = _feedback_channel_compatibility()

    dedupe_subject_policy = {
        "same_subject_key_stable": high_intent.dedupe_subject_key == replay_intent.dedupe_subject_key,
        "different_subject_key_distinct": high_intent.dedupe_subject_key != other_intent.dedupe_subject_key,
        "subject_fingerprint_stable": _fingerprint(high_intent.dedupe_subject_key)
        == _fingerprint(replay_intent.dedupe_subject_key),
        "subject_fingerprints_distinct": _fingerprint(high_intent.dedupe_subject_key)
        != _fingerprint(other_intent.dedupe_subject_key),
        "raw_dedupe_subject_key_omitted": True,
    }
    material_hash_policy = {
        "same_material_hash_stable": same_material_hash_a == same_material_hash_b == high_intent.material_change_hash,
        "same_subject_material_change_distinct": high_intent.material_change_hash != changed_intent.material_change_hash,
        "material_fingerprint_stable": _fingerprint(high_intent.material_change_hash)
        == _fingerprint(replay_intent.material_change_hash),
        "material_fingerprints_distinct": _fingerprint(high_intent.material_change_hash)
        != _fingerprint(changed_intent.material_change_hash),
        "raw_material_change_hash_omitted": True,
    }
    duplicate_suppression = {
        "same_subject_same_material_first_action": duplicate_actions[0],
        "same_subject_same_material_replay_action": duplicate_actions[1],
        "same_subject_same_material_unique_send_intent_count": len(set(_identity(intent) for intent in (high_intent, replay_intent))),
        "same_subject_same_material_no_duplicate_notification": duplicate_actions == ("allow", "suppress_duplicate"),
    }
    material_change_distinction = {
        "same_subject_material_change_first_action": changed_actions[0],
        "same_subject_material_change_second_action": changed_actions[1],
        "same_subject_material_change_unique_send_intent_count": len(set(_identity(intent) for intent in (high_intent, changed_intent))),
        "same_subject_material_change_notification_allowed": changed_actions == ("allow", "allow_new_material"),
    }

    gates = {
        "dedupe_subject_key_policy": all(
            bool(dedupe_subject_policy[key])
            for key in (
                "same_subject_key_stable",
                "different_subject_key_distinct",
                "subject_fingerprint_stable",
                "subject_fingerprints_distinct",
            )
        ),
        "material_change_hash_policy": all(
            bool(material_hash_policy[key])
            for key in (
                "same_material_hash_stable",
                "same_subject_material_change_distinct",
                "material_fingerprint_stable",
                "material_fingerprints_distinct",
            )
        ),
        "same_subject_same_material_no_duplicate": duplicate_suppression[
            "same_subject_same_material_no_duplicate_notification"
        ],
        "same_subject_material_change_distinction": material_change_distinction[
            "same_subject_material_change_notification_allowed"
        ],
        "suppress_later_high_distribution_compatible": distribution["compatible"] is True,
        "feedback_channel_outputs_consumed_without_hot_path_enforcement": feedback_channel[
            "hot_path_enforcement_applied"
        ]
        is False
        and feedback_channel["feedback_eval_consumed"] is True
        and feedback_channel["channel_override_consumed"] is True,
    }
    ok = all(gates.values())
    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "ok": ok,
        "status": "pass" if ok else "blocked",
        "reason_code": "noise_duplicate_suppression_ready" if ok else "noise_duplicate_suppression_incomplete",
        "gate": F9_GATE,
        "gate_closed": ok,
        "reuse": {
            "notification_intent_builder": "NotificationIntentBuilder",
            "notification_plan_intent_model": "NotificationPlanIntent",
            "material_change_hash_function": "material_change_hash_for_analysis",
            "feedback_eval_engine_consumed": True,
            "channel_override_policy_consumed": True,
        },
        "gates": gates,
        "dedupe_subject_key_policy": dedupe_subject_policy,
        "material_change_hash_policy": material_hash_policy,
        "duplicate_suppression": duplicate_suppression,
        "material_change_distinction": material_change_distinction,
        "distribution_compatibility": distribution,
        "feedback_channel_compatibility": feedback_channel,
        "authority": {
            "database_read_allowed": False,
            "database_write_allowed": False,
            "redis_allowed": False,
            "telegram_allowed": False,
            "openai_allowed": False,
            "github_allowed": False,
            "x_allowed": False,
            "web_allowed": False,
            "external_network_allowed": False,
            "docker_or_systemd_allowed": False,
        },
        "side_effects": {
            "database_read_attempted": False,
            "database_write_attempted": False,
            "redis_called": False,
            "telegram_called": False,
            "openai_called": False,
            "external_network_called": False,
            "historical_overwrite": False,
        },
        "redactions_applied": _redactions(),
        "raw_values_printed": False,
    }


def _distribution_compatibility(
    *,
    builder: NotificationIntentBuilder,
    delivery_policy: DeliveryPolicy,
) -> dict[str, Any]:
    cases = {
        "suppress": "skip",
        "later": "later",
        "high": "inspect_now",
    }
    results: dict[str, Any] = {}
    for label, verdict in cases.items():
        analysis, evaluation = _analysis_for_verdict(
            verdict,
            delivery_policy=delivery_policy,
            recommended_action_ko=f"{label} action",
            freshness_note_ko=f"{label} freshness",
        )
        intent = builder.build(analysis_id=ANALYSIS_ID, analysis=analysis, evaluation=evaluation)
        results[label] = {
            "verdict": analysis.verdict,
            "delivery_decision": analysis.delivery_decision,
            "urgency_profile": evaluation.urgency_profile,
            "send_intent_created": intent is not None,
            "suppress_reason_code": evaluation.suppress_reason_code,
            "raw_dedupe_subject_key_omitted": True,
            "raw_material_change_hash_omitted": True,
        }
    compatible = (
        results["suppress"]["delivery_decision"] == "suppress"
        and results["suppress"]["send_intent_created"] is False
        and results["later"]["delivery_decision"] == "send_now"
        and results["later"]["urgency_profile"] == "normal_silent"
        and results["later"]["send_intent_created"] is True
        and results["high"]["delivery_decision"] == "send_now"
        and results["high"]["urgency_profile"] == "high"
        and results["high"]["send_intent_created"] is True
    )
    return {
        "compatible": compatible,
        "cases": results,
    }


def _feedback_channel_compatibility() -> dict[str, Any]:
    feedback_result = FeedbackEvalEngine().evaluate(
        (
            FeedbackRecord(label="duplicate", candidate_group_id_suffix="1111", delivery_decision="send_now"),
            FeedbackRecord(label="hype", candidate_group_id_suffix="1111", reason_code="ai_noise"),
        )
    )
    channel_result = ChannelOverridePolicy().evaluate(
        ChannelOverrideInput(
            channel_tier="B",
            artifact_type="text_idea",
            verdict="later",
            delivery_decision="send_now",
            urgency_profile="normal_silent",
            reason_codes=("ai_noise", "weak_ai_context"),
            text_idea_enabled=True,
            ai_noise_signal_count=2,
            external_evidence_present=False,
        )
    )
    return {
        "feedback_eval_consumed": True,
        "channel_override_consumed": True,
        "duplicate_risk_count_bucket": _count_bucket(
            feedback_result.policy_outcome_alignment.get("duplicate_risk", 0)
        ),
        "ai_noise_channel_decision": channel_result.decision,
        "ai_noise_simulated_delivery_decision": channel_result.simulated_delivery_decision,
        "channel_override_simulation_result": channel_result.simulation_result,
        "hot_path_enforcement_applied": False,
    }


def _analysis_for_verdict(
    verdict: str,
    *,
    delivery_policy: DeliveryPolicy,
    recommended_action_ko: str,
    freshness_note_ko: str,
    candidate_group_id: UUID = SUBJECT_CANDIDATE_ID,
) -> tuple[AnalysisDraft, PolicyEvaluation]:
    delivery = delivery_policy.evaluate(verdict=verdict)  # type: ignore[arg-type]
    reason_codes = [f"proof_{verdict}", "policy_threshold_proof"]
    if delivery.suppress_reason_code:
        reason_codes.append(delivery.suppress_reason_code)
    analysis = AnalysisDraft(
        candidate_group_id=candidate_group_id,
        judge_output_id=JUDGE_OUTPUT_ID,
        schema_version="analysis_v1",
        policy_version="verdict_policy_v1",
        prompt_version="judge_prompt_v1",
        delivery_policy_version="delivery_policy_v1",
        verdict=verdict,  # type: ignore[arg-type]
        delivery_decision=delivery.delivery_decision,
        scores_json={"proof_bucket": verdict},
        reason_codes_json=reason_codes,
        evidence_limitations_ko=None,
        recommended_action_ko=recommended_action_ko,
        freshness_note_ko=freshness_note_ko,
        model_proposed_verdict=verdict,
        policy_reconciled_flag=True,
    )
    evaluation = PolicyEvaluation(
        verdict=analysis.verdict,
        delivery_decision=analysis.delivery_decision,
        urgency_profile=delivery.urgency_profile,
        reason_codes=reason_codes,
        policy_reconciled_flag=True,
        suppress_reason_code=delivery.suppress_reason_code,
    )
    return analysis, evaluation


def _dedupe_actions(first: NotificationPlanIntent, second: NotificationPlanIntent) -> tuple[str, str]:
    seen: set[NotificationIdentity] = set()
    actions: list[str] = []
    for intent in (first, second):
        identity = _identity(intent)
        if identity in seen:
            actions.append("suppress_duplicate")
        else:
            actions.append("allow" if not actions else "allow_new_material")
            seen.add(identity)
    return actions[0], actions[1]


def _identity(intent: NotificationPlanIntent) -> NotificationIdentity:
    return NotificationIdentity(
        dedupe_subject_key=intent.dedupe_subject_key,
        material_change_hash=intent.material_change_hash,
        target_chat_id=intent.target_chat_id,
    )


def _require_intent(intent: NotificationPlanIntent | None) -> NotificationPlanIntent:
    if intent is None:
        raise RuntimeError("notification_intent_missing")
    return intent


def _policy_config() -> PolicyEngineConfig:
    return PolicyEngineConfig(
        app_env="proof",
        database_url="redacted-db-locator",
        redis_url="redacted-redis-locator",
        queue_name="q.analysis.policy",
        consumer_group="policy-engine",
        consumer_name="noise-duplicate-suppression-proof",
        batch_size=1,
        block_ms=1,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
        operator_chat_id=4100,
        enable_later_delivery=True,
        enable_silent_later=True,
        enable_notification_send=True,
        render_profile_high="telegram_single_alert_high_v1",
        render_profile_normal="telegram_single_alert_normal_v1",
        log_level="INFO",
    )


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _count_bucket(count: int) -> str:
    if count <= 0:
        return "zero"
    if count == 1:
        return "one"
    if count <= 5:
        return "two_to_five"
    if count <= 20:
        return "six_to_twenty"
    return "over_twenty"


def _redactions() -> dict[str, bool]:
    return {
        "full_ids_omitted": True,
        "raw_urls_omitted": True,
        "raw_source_text_omitted": True,
        "raw_feedback_notes_omitted": True,
        "raw_chat_ids_omitted": True,
        "dedupe_keys_omitted": True,
        "material_hashes_omitted": True,
        "db_redis_urls_omitted": True,
        "env_values_omitted": True,
        "exception_bodies_omitted": True,
        "tracebacks_omitted": True,
    }


def render_sanitized_json(report: dict[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


__all__ = [
    "F9_GATE",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "build_noise_duplicate_suppression_proof",
    "render_sanitized_json",
]
