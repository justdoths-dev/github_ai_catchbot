from __future__ import annotations

import re

from .models import CanonicalArtifact, TextSurfaces, TriggerEvaluation


_AI_RE = re.compile(r"(?<![a-z0-9])ai(?![a-z0-9])")
_STRONG_TYPES = {"github_repo", "github_subpath", "github_repo_page", "github_gist", "x_post"}
_MEDIUM_TYPES = {"web_article"}
_DEV_CONTEXT_TERMS = {
    "repo",
    "repository",
    "code",
    "developer",
    "dev",
    "cli",
    "api",
    "sdk",
    "python",
    "javascript",
    "typescript",
    "library",
    "framework",
    "open source",
    "opensource",
    "package",
    "model",
    "agent",
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
    has_vibe = any(term in text for term in _VIBE_TERMS)
    has_github_keyword = "github" in text
    has_dev_context = any(term in text for term in _DEV_CONTEXT_TERMS)
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
    if any(term in text for term in ("tool", "automation", "agent")):
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
