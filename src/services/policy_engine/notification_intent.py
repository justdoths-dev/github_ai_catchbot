from __future__ import annotations

import hashlib
import json
from uuid import UUID, uuid4

from .config import PolicyEngineConfig
from .models import AnalysisDraft, NotificationPlanIntent, PolicyEvaluation


class NotificationIntentBuilder:
    def __init__(self, *, config: PolicyEngineConfig) -> None:
        self._config = config

    def build(
        self,
        *,
        analysis_id: UUID,
        analysis: AnalysisDraft,
        evaluation: PolicyEvaluation,
        dedupe_subject_key: str | None = None,
    ) -> NotificationPlanIntent | None:
        if analysis.delivery_decision == "suppress":
            return None
        if not self._config.enable_notification_send:
            return None

        render_profile = (
            self._config.render_profile_high
            if evaluation.urgency_profile == "high"
            else self._config.render_profile_normal
        )
        normalized_subject = (dedupe_subject_key or "").strip()
        notification_subject = normalized_subject or str(analysis.candidate_group_id)
        material_change_hash = material_change_hash_for_analysis(
            candidate_group_id=analysis.candidate_group_id,
            dedupe_subject_key=normalized_subject or None,
            verdict=analysis.verdict,
            delivery_decision=analysis.delivery_decision,
            urgency_profile=evaluation.urgency_profile,
            reason_codes_json=analysis.reason_codes_json,
            recommended_action_ko=analysis.recommended_action_ko,
            freshness_note_ko=analysis.freshness_note_ko,
        )
        return NotificationPlanIntent(
            notification_plan_id=uuid4(),
            analysis_id=analysis_id,
            candidate_group_id=analysis.candidate_group_id,
            delivery_decision=analysis.delivery_decision,
            urgency_profile=evaluation.urgency_profile,
            target_chat_id=self._config.operator_chat_id,
            target_thread_id=None,
            render_profile=render_profile,
            dedupe_subject_key=notification_subject,
            material_change_hash=material_change_hash,
            send_after=None,
            suppress_reason_code=evaluation.suppress_reason_code,
        )


def material_change_hash_for_analysis(
    *,
    candidate_group_id: UUID | str,
    dedupe_subject_key: str | None = None,
    verdict: str,
    delivery_decision: str,
    urgency_profile: str,
    reason_codes_json: list[str],
    recommended_action_ko: str | None,
    freshness_note_ko: str | None,
) -> str:
    payload = {
        "verdict": verdict,
        "delivery_decision": delivery_decision,
        "urgency_profile": urgency_profile,
        "reason_codes_json": reason_codes_json,
        "recommended_action_ko": recommended_action_ko,
        "freshness_note_ko": freshness_note_ko,
    }
    if dedupe_subject_key:
        payload["dedupe_subject_key"] = dedupe_subject_key
    else:
        payload["candidate_group_id"] = str(candidate_group_id)
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
