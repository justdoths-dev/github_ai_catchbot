from __future__ import annotations

import json

from .models import BundleJudgeContext, PreparedModelContext
from .preflight import ModelContextPreflight


class JudgeContextBuilder:
    def __init__(self, *, preflight: ModelContextPreflight) -> None:
        self._preflight = preflight

    def build(self, *, developer_prompt: str, bundle: BundleJudgeContext) -> PreparedModelContext:
        user_context_payload = {
            "candidate_group_id": str(bundle.candidate_group_id),
            "bundle_id": str(bundle.bundle_id),
            "current_primary_artifact_id": str(bundle.current_primary_artifact_id),
            "primary_summary": bundle.primary_summary,
            "supporting_summaries": bundle.supporting_summaries_json,
            "discovered_links_summary": bundle.discovered_links_summary_json,
            "evidence_limitations": bundle.evidence_limitations,
            "token_budget_profile": bundle.token_budget_profile,
            "reroot_count": bundle.reroot_count,
        }
        user_context = json.dumps(user_context_payload, ensure_ascii=False, sort_keys=True, indent=2)
        preflight_result = self._preflight.apply(
            developer_prompt=developer_prompt,
            user_context=user_context,
        )
        return PreparedModelContext(
            developer_prompt=preflight_result.developer_prompt,
            user_context=preflight_result.user_context,
            preflight_notes=preflight_result.notes,
            preflight_flags=preflight_result.flags,
        )
