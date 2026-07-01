from __future__ import annotations

from uuid import uuid4

from services.notifier_telegram.models import AnalysisRenderContext, CandidateRenderContext, JudgeOutputRenderContext
from services.notifier_telegram.renderer import NotificationRenderer, RenderInput


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
        payload=RenderInput(
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

    assert "판정: inspect_now" in render.message_text
    assert "전달: send_now / high" in render.message_text
    assert "누락: 요약 없음." in render.message_text
    assert "누락: 추천 행동 없음." in render.message_text


def test_render_hash_is_stable() -> None:
    analysis = _analysis()
    renderer = NotificationRenderer()
    render_input = RenderInput(
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


def test_renderer_normal_silent_uses_korean_labels_and_button_only_primary_url() -> None:
    analysis = AnalysisRenderContext(
        analysis_id=uuid4(),
        candidate_group_id=uuid4(),
        judge_output_id=uuid4(),
        verdict="later",
        delivery_decision="send_now",
        reason_codes_json=["operator_review_recommended"],
        evidence_limitations_ko="저장소 활동 증거가 제한적입니다.",
        recommended_action_ko="저장소 README와 최근 커밋을 확인하세요.",
        freshness_note_ko="최근 공개 신호 기준입니다.",
    )
    primary_url = "https://github.com/example/repo"
    raw_uuid = str(uuid4())
    render = NotificationRenderer().render(
        notification_plan_id=uuid4(),
        payload=RenderInput(
            analysis=analysis,
            judge_output=JudgeOutputRenderContext(
                judge_output_id=analysis.judge_output_id,
                payload_json={
                    "summary_one_line_ko": "Claude CLI로 HTML 디자인을 만드는 로컬 MCP 래퍼입니다.",
                    "skeptical_take_ko": f"실사용 유지보수 신호는 아직 더 확인해야 합니다. {raw_uuid}",
                    "why_it_might_matter_ko": "디자인 초안 반복 작업을 줄일 수 있습니다.",
                },
            ),
            candidate=CandidateRenderContext(
                candidate_group_id=analysis.candidate_group_id,
                source_message_id=uuid4(),
                current_primary_artifact_id=uuid4(),
                primary_artifact_type="github_repo",
                primary_canonical_url=primary_url,
                primary_canonical_id=raw_uuid,
                source_message_link=None,
                source_text_surface=None,
            ),
            urgency_profile="normal_silent",
        ),
    )

    nonempty_lines = [line for line in render.message_text.splitlines() if line.strip()]
    assert any(line.startswith("판정: later") for line in nonempty_lines[:3])
    assert any("normal_silent" in line for line in nonempty_lines[:3])
    assert "한줄 요약:" in render.message_text
    assert "냉정 평가:" in render.message_text
    assert "왜 볼만한가:" in render.message_text
    assert "증거 한계:" in render.message_text
    assert "추천 행동:" in render.message_text
    assert render.disable_notification is True
    assert render.link_preview_options_json == {"is_disabled": True}
    assert render.protect_content is False
    assert len(render.message_text) < 4096
    assert raw_uuid not in render.message_text
    assert "[redacted-id]" in render.message_text
    for forbidden in ("secret", "runtime.env", "DATABASE_URL", "REDIS_URL", "OPENAI_API_KEY"):
        assert forbidden not in render.message_text
    assert primary_url not in render.message_text
    assert render.reply_markup_json == {
        "inline_keyboard": [[{"text": "GitHub 열기", "url": primary_url}]]
    }


def test_renderer_send_worthy_message_redacts_raw_payload_runtime_secret_and_exception_markers() -> None:
    analysis = AnalysisRenderContext(
        analysis_id=uuid4(),
        candidate_group_id=uuid4(),
        judge_output_id=uuid4(),
        verdict="inspect_now",
        delivery_decision="send_now",
        reason_codes_json=["developer_signal_present"],
        evidence_limitations_ko="공개 증거만 확인했습니다.",
        recommended_action_ko="README와 최근 커밋을 확인하세요.",
        freshness_note_ko=None,
    )
    raw_uuid = str(uuid4())
    raw_http_url = "https://example.com/private/path?tok" + "en=do-not-print"
    raw_db_url = "postgresql+psycopg" + "://user:pass" + "word@db.internal/catchbot"
    raw_redis_url = "redis" + "://:pass" + "word@redis.internal/0"
    raw_source_text = "raw source text from Telegram must stay private"
    render = NotificationRenderer(max_message_chars=900).render(
        notification_plan_id=uuid4(),
        payload=RenderInput(
            analysis=analysis,
            judge_output=JudgeOutputRenderContext(
                judge_output_id=analysis.judge_output_id,
                payload_json={
                    "headline": "Useful repo",
                    "summary_one_line_ko": f"개발자 워크플로우 후보입니다. {raw_http_url}",
                    "skeptical_take_ko": f"Traceback OperationalError {raw_db_url}",
                    "why_it_might_matter_ko": f"반복 검토를 줄일 수 있습니다. {raw_redis_url} {raw_uuid}",
                },
            ),
            candidate=CandidateRenderContext(
                candidate_group_id=analysis.candidate_group_id,
                source_message_id=uuid4(),
                current_primary_artifact_id=uuid4(),
                primary_artifact_type="github_repo",
                primary_canonical_url="https://github.com/example/repo",
                primary_canonical_id="github.com/example/repo",
                source_message_link=None,
                source_text_surface=raw_source_text,
            ),
            urgency_profile="high",
        ),
    )

    assert "판정: inspect_now" in render.message_text
    assert "전달: send_now / high" in render.message_text
    assert "냉정 평가:" in render.message_text
    assert "왜 볼만한가:" in render.message_text
    assert "증거 한계:" in render.message_text
    assert "추천 행동:" in render.message_text
    assert len(render.message_text) <= 900
    for raw in (
        raw_uuid,
        raw_http_url,
        raw_db_url,
        raw_redis_url,
        raw_source_text,
        "payload_json",
        "DATABASE_URL",
        "REDIS_URL",
        "TELEGRAM_BOT_TOKEN",
        "Traceback",
        "OperationalError",
        "pass" + "word",
        "tok" + "en",
    ):
        assert raw not in render.message_text
