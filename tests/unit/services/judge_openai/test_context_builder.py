from __future__ import annotations

import json
from uuid import uuid4

from services.judge_openai.context_builder import JudgeContextBuilder
from services.judge_openai.models import BundleJudgeContext
from services.judge_openai.preflight import NoopModelContextPreflight


def test_context_builder_uses_only_bundle_fields() -> None:
    bundle = BundleJudgeContext(
        bundle_id=uuid4(),
        candidate_group_id=uuid4(),
        current_primary_artifact_id=uuid4(),
        primary_summary={"title": "repo", "body": "useful"},
        supporting_summaries_json=[{"kind": "x_post"}],
        discovered_links_summary_json=[{"url": "https://example.com"}],
        evidence_limitations=["no stars snapshot"],
        token_budget_profile="small",
        reroot_count=1,
    )

    prepared = JudgeContextBuilder(preflight=NoopModelContextPreflight()).build(
        developer_prompt="developer",
        bundle=bundle,
    )
    payload = json.loads(prepared.user_context)

    assert payload == {
        "bundle_id": str(bundle.bundle_id),
        "candidate_group_id": str(bundle.candidate_group_id),
        "current_primary_artifact_id": str(bundle.current_primary_artifact_id),
        "primary_summary": {"body": "useful", "title": "repo"},
        "supporting_summaries": [{"kind": "x_post"}],
        "discovered_links_summary": [{"url": "https://example.com"}],
        "evidence_limitations": ["no stars snapshot"],
        "token_budget_profile": "small",
        "reroot_count": 1,
    }
    assert "source_message" not in prepared.user_context
    assert "artifact_snapshot" not in prepared.user_context
