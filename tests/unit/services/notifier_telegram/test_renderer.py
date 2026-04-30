from __future__ import annotations

from uuid import uuid4

from services.notifier_telegram.models import AnalysisRenderContext, CandidateRenderContext, JudgeOutputRenderContext
from services.notifier_telegram.renderer import NotificationRenderer


def _analysis() -> AnalysisRenderContext:
    return AnalysisRenderContext(
        analysis_id=uuid4(),
        candidate_group_id=uuid4(),
        judge_output_id=uuid4(),
        verdict="inspect_now",
        delivery_decision="send_now",
        reason_codes_json=["repo_has_clear_scope"],
        evidence_limitations_ko=None,
        recommended_action_ko=None,
        freshness_note_ko=None,
    )


def test_renderer_includes_final_verdict_and_explicit_omissions() -> None:
    analysis = _analysis()
    render = NotificationRenderer().render(
        notification_plan_id=uuid4(),
        payload=__import__("services.notifier_telegram.renderer", fromlist=["RenderInput"]).RenderInput(
            analysis=analysis,
            judge_output=JudgeOutputRenderContext(judge_output_id=analysis.judge_output_id, payload_json={}),
            candidate=CandidateRenderContext(
                candidate_group_id=analysis.candidate_group_id,
                source_message_id=None,
                current_primary_artifact_id=None,
                primary_artifact_type=None,
                primary_canonical_url=None,
                primary_canonical_id=None,
                source_message_link=None,
                source_text_surface=None,
            ),
            urgency_profile="high",
        ),
    )

    assert "Verdict: inspect_now" in render.message_text
    assert "Delivery: send_now / high" in render.message_text
    assert "Omitted: summary unavailable." in render.message_text
    assert "Omitted: recommended action unavailable." in render.message_text


def test_render_hash_is_stable() -> None:
    analysis = _analysis()
    renderer = NotificationRenderer()
    render_input = __import__("services.notifier_telegram.renderer", fromlist=["RenderInput"]).RenderInput(
        analysis=analysis,
        judge_output=JudgeOutputRenderContext(
            judge_output_id=analysis.judge_output_id,
            payload_json={"headline": "Useful repo", "summary_one_line_ko": "summary"},
        ),
        candidate=None,
        urgency_profile="normal_silent",
    )

    first = renderer.render(notification_plan_id=uuid4(), payload=render_input)
    second = renderer.render(notification_plan_id=uuid4(), payload=render_input)

    assert first.render_hash == second.render_hash
    assert first.disable_notification is True
