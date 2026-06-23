from __future__ import annotations

import re

from .models import CanonicalArtifact, TextSurfaces, TriggerEvaluation


_AI_RE = re.compile(r"(?<![a-z0-9])ai(?![a-z0-9])")
_LLM_RE = re.compile(r"(?<![a-z0-9])llm(?![a-z0-9])")
_STRONG_TYPES = {"github_repo", "github_subpath", "github_repo_page", "github_gist", "x_post"}
_MEDIUM_TYPES = {"web_article"}
_DOMAIN_SIGNAL_TERMS = {
    "language model",
    "model",
    "모델",
    "대형언어모델",
    "생성형",
    "agent",
    "에이전트",
    "prompt",
    "프롬프트",
    "cli",
    "sdk",
    "api",
    "terminal",
    "터미널",
}
_DEV_CONTEXT_TERMS = {
    "repo",
    "repository",
    "code",
    "코드",
    "coding",
    "개발",
    "developer",
    "dev",
    "python",
    "javascript",
    "typescript",
    "library",
    "framework",
    "open source",
    "opensource",
    "package",
    "test",
    "deploy",
    "automation",
    "자동화",
    "workflow",
    "파이프라인",
    "script",
    "스크립트",
    "tool",
    "도구",
    "작업",
}
_ACTIONABILITY_TERMS = {
    "permission",
    "security",
    "blocked",
    "constraint",
    "restricted",
    "restriction",
    "install",
    "execute",
    "implement",
    "guide",
    "how to",
    "workaround",
    "cannot",
    "can't",
    "unable",
    "권한",
    "보안",
    "막힘",
    "제약",
    "설치",
    "실행",
    "구현",
    "가이드",
    "방법",
    "우회",
    "자동화할 수",
    "할 수가 없",
    "할수가 없",
    "쓸 수",
    "쓸수",
    "되는게 없",
    "안되",
}
_VIBE_TERMS = ("vibe coding", "vibe-coding")


def evaluate_triggers(surfaces: TextSurfaces, artifacts: list[CanonicalArtifact]) -> TriggerEvaluation:
    artifact_types = {artifact.artifact_type for artifact in artifacts}
    if artifact_types & _STRONG_TYPES:
        return TriggerEvaluation(
            signal_detected=True,
            candidate_eligible=True,
            trigger_strength="strong",
            reason_codes=["strong_artifact_link"],
            notes={"artifact_types": sorted(artifact_types)},
        )
    if artifact_types & _MEDIUM_TYPES:
        return TriggerEvaluation(
            signal_detected=True,
            candidate_eligible=True,
            trigger_strength="medium",
            reason_codes=["medium_external_link"],
            notes={"artifact_types": sorted(artifact_types)},
        )

    text = surfaces.keyword_scan_surface
    has_ai = bool(_AI_RE.search(text))
    has_domain_signal = bool(_LLM_RE.search(text)) or _contains_any_term(text, _DOMAIN_SIGNAL_TERMS)
    has_vibe = any(term in text for term in _VIBE_TERMS)
    has_github_keyword = "github" in text
    has_dev_context = _contains_any_term(text, _DEV_CONTEXT_TERMS)
    has_actionability = _contains_any_term(text, _ACTIONABILITY_TERMS)
    if has_vibe:
        return TriggerEvaluation(
            signal_detected=True,
            candidate_eligible=True,
            trigger_strength="strong",
            reason_codes=["vibe_coding_signal"],
            notes={"has_vibe": True},
        )
    if has_github_keyword and has_dev_context:
        return TriggerEvaluation(
            signal_detected=True,
            candidate_eligible=True,
            trigger_strength="strong",
            reason_codes=["github_with_dev_context"],
            notes={"has_github_keyword": True, "has_dev_context": True},
        )
    if has_domain_signal and has_dev_context and has_actionability:
        return TriggerEvaluation(
            signal_detected=True,
            candidate_eligible=True,
            trigger_strength="medium",
            reason_codes=["developer_workflow_constraint_signal"],
            notes={
                "has_domain_signal": True,
                "has_dev_context": True,
                "has_actionability": True,
            },
        )
    if has_ai and has_dev_context:
        return TriggerEvaluation(
            signal_detected=True,
            candidate_eligible=True,
            trigger_strength="medium",
            reason_codes=["ai_with_dev_context"],
            notes={"has_ai": True, "has_dev_context": True},
        )
    if has_ai:
        return TriggerEvaluation(
            signal_detected=True,
            candidate_eligible=False,
            trigger_strength="weak",
            reason_codes=["ai_without_dev_context"],
            notes={"has_ai": True, "has_dev_context": False},
        )
    if has_github_keyword:
        return TriggerEvaluation(
            signal_detected=True,
            candidate_eligible=False,
            trigger_strength="weak",
            reason_codes=["github_without_dev_context"],
            notes={"has_github_keyword": True, "has_dev_context": False},
        )
    if has_domain_signal:
        return TriggerEvaluation(
            signal_detected=True,
            candidate_eligible=False,
            trigger_strength="weak",
            reason_codes=["domain_signal_without_candidate_context"],
            notes={
                "has_domain_signal": True,
                "has_dev_context": has_dev_context,
                "has_actionability": has_actionability,
            },
        )
    if has_dev_context or has_actionability:
        return TriggerEvaluation(
            signal_detected=True,
            candidate_eligible=False,
            trigger_strength="weak",
            reason_codes=["weak_keyword_only"],
        )
    return TriggerEvaluation(
        signal_detected=False,
        candidate_eligible=False,
        trigger_strength=None,
        reason_codes=["no_signal"],
    )


def _contains_any_term(text: str, terms: set[str]) -> bool:
    return any(_contains_term(text, term) for term in terms)


def _contains_term(text: str, term: str) -> bool:
    if _ascii_token(term):
        return re.search(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", text) is not None
    return term in text


def _ascii_token(term: str) -> bool:
    return bool(re.fullmatch(r"[a-z0-9][a-z0-9_-]*", term))
