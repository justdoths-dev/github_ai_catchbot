from __future__ import annotations

import pytest

from services.router_normalizer.canonicalizer import canonicalize_resolved_urls
from services.router_normalizer.models import ResolvedUrl, TextSurfaces
from services.router_normalizer.trigger_rules import evaluate_triggers


KOREAN_LLM_WORKFLOW_TEXT = (
    "회사에서 llm 사용 권한 받은김에 이것저것 작업 중인데.. "
    "머 보안때문에 되는게 없네요. cli는 쓸수도 없고.. 자동화를 할수가 없네"
)


def _surfaces(text: str) -> TextSurfaces:
    return TextSurfaces(
        raw_text_surface=text,
        keyword_scan_surface=text.casefold(),
        hash_surface=text.casefold(),
        display_surface=text,
    )


def _resolved(url: str) -> ResolvedUrl:
    return ResolvedUrl(observed_url=url, normalized_url=url, resolved_url=None, source_kind="entity")


def test_github_link_is_strong_candidate() -> None:
    artifacts = canonicalize_resolved_urls([_resolved("https://github.com/openai/openai-python")])

    evaluation = evaluate_triggers(_surfaces("check this"), artifacts)

    assert evaluation.signal_detected is True
    assert evaluation.candidate_eligible is True
    assert evaluation.trigger_strength == "strong"


def test_x_link_is_strong_candidate() -> None:
    artifacts = canonicalize_resolved_urls([_resolved("https://x.com/someone/status/1234567890")])

    evaluation = evaluate_triggers(_surfaces("check this"), artifacts)

    assert evaluation.signal_detected is True
    assert evaluation.candidate_eligible is True
    assert evaluation.trigger_strength == "strong"


def test_vibe_coding_keyword_is_strong_candidate() -> None:
    evaluation = evaluate_triggers(_surfaces("vibe-coding workflow idea"), [])

    assert evaluation.signal_detected is True
    assert evaluation.candidate_eligible is True
    assert evaluation.trigger_strength == "strong"


def test_github_keyword_needs_dev_context_for_candidate() -> None:
    weak = evaluate_triggers(_surfaces("github"), [])
    strong = evaluate_triggers(_surfaces("github repo workflow"), [])

    assert weak.signal_detected is True
    assert weak.candidate_eligible is False
    assert weak.reason_codes == ["github_without_dev_context"]
    assert strong.candidate_eligible is True
    assert strong.trigger_strength == "strong"


def test_ai_alone_signals_but_is_not_candidate_eligible() -> None:
    evaluation = evaluate_triggers(_surfaces("interesting AI"), [])

    assert evaluation.signal_detected is True
    assert evaluation.candidate_eligible is False
    assert evaluation.trigger_strength == "weak"
    assert evaluation.reason_codes == ["ai_without_dev_context"]


def test_ai_with_dev_context_is_candidate_eligible() -> None:
    evaluation = evaluate_triggers(_surfaces("AI python SDK for developers"), [])

    assert evaluation.signal_detected is True
    assert evaluation.candidate_eligible is True
    assert evaluation.trigger_strength == "medium"


def test_korean_llm_cli_automation_security_constraint_is_candidate_eligible() -> None:
    evaluation = evaluate_triggers(_surfaces(KOREAN_LLM_WORKFLOW_TEXT), [])

    assert evaluation.signal_detected is True
    assert evaluation.candidate_eligible is True
    assert evaluation.trigger_strength == "medium"
    assert evaluation.reason_codes == ["developer_workflow_constraint_signal"]


@pytest.mark.parametrize("text", ["llm", "AI", "요즘 AI 좋네"])
def test_weak_generic_ai_or_llm_messages_are_not_candidate_eligible(text: str) -> None:
    evaluation = evaluate_triggers(_surfaces(text), [])

    assert evaluation.signal_detected is True
    assert evaluation.candidate_eligible is False
    assert evaluation.trigger_strength == "weak"


def test_mixed_english_korean_dev_workflow_constraint_is_deterministic_candidate() -> None:
    first = evaluate_triggers(_surfaces("llm cli 자동화 blocked by 보안 policy"), [])
    second = evaluate_triggers(_surfaces("llm cli 자동화 blocked by 보안 policy"), [])

    assert first == second
    assert first.signal_detected is True
    assert first.candidate_eligible is True
    assert first.trigger_strength == "medium"


def test_weak_signal_reports_suppression_reason() -> None:
    evaluation = evaluate_triggers(_surfaces("automation tool"), [])

    assert evaluation.signal_detected is True
    assert evaluation.candidate_eligible is False
    assert evaluation.reason_codes == ["weak_keyword_only"]
