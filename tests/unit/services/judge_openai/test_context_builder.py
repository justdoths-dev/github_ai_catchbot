from __future__ import annotations

import json
from pathlib import Path
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
        discovered_links_summary_json=[{"link_bucket": "one"}],
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
        "supporting_summaries_json": [{"kind": "x_post"}],
        "discovered_links_summary_json": [{"link_bucket": "one"}],
        "evidence_limitations": ["no stars snapshot"],
        "token_budget_profile": "small",
        "reroot_count": 1,
    }
    assert "source_message" not in prepared.user_context
    assert "artifact_snapshot" not in prepared.user_context
    assert "telegram raw update" not in prepared.user_context
    assert "OPENAI_API_KEY" not in prepared.user_context


def test_judge_openai_source_has_no_adjacent_service_references() -> None:
    source_root = Path(__file__).resolve().parents[4] / "src" / "services" / "judge_openai"
    source_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(source_root.glob("*.py"))
        if path.name != "__init__.py"
    )

    forbidden_references = [
        "services.notifier_telegram",
        "notifier_telegram",
        "services.policy_engine",
        "policy_engine",
        "services.analysis_validator",
        "analysis_validator",
        "services.collector_telegram",
        "collector_telegram",
        "services.gh_enricher",
        "gh_enricher",
        "services.x_enricher",
        "x_enricher",
        "services.web_enricher",
        "web_enricher",
    ]
    for forbidden in forbidden_references:
        assert forbidden not in source_text
