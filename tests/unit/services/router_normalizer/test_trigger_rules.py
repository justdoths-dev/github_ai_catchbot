from __future__ import annotations

from services.router_normalizer.canonicalizer import canonicalize_resolved_urls
from services.router_normalizer.models import ResolvedUrl, TextSurfaces
from services.router_normalizer.trigger_rules import evaluate_triggers


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


def test_weak_signal_reports_suppression_reason() -> None:
    evaluation = evaluate_triggers(_surfaces("automation tool"), [])

    assert evaluation.signal_detected is True
    assert evaluation.candidate_eligible is False
    assert evaluation.reason_codes == ["weak_keyword_only"]
