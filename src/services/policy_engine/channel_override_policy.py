from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


CHANNEL_TIERS = frozenset({"A", "B", "C"})
ARTIFACT_TYPES = frozenset(
    {
        "github_repo",
        "github_subpath",
        "github_repo_page",
        "github_gist",
        "x_post",
        "web_article",
        "text_idea",
        "unknown",
    }
)
VERDICTS = frozenset({"inspect_now", "later", "skip"})
DELIVERY_DECISIONS = frozenset({"send_now", "send_digest", "suppress"})
URGENCY_PROFILES = frozenset({"high", "normal_silent", "digest", "suppressed"})


@dataclass(frozen=True, slots=True)
class ChannelOverrideInput:
    channel_tier: str
    artifact_type: str
    verdict: str
    delivery_decision: str
    urgency_profile: str
    reason_codes: tuple[str, ...] = ()
    text_idea_enabled: bool = True
    ai_noise_signal_count: int = 0
    external_evidence_present: bool = False


@dataclass(frozen=True, slots=True)
class ChannelOverrideResult:
    decision: str
    reason_codes: tuple[str, ...]
    original_delivery_decision: str
    simulated_delivery_decision: str
    live_delivery_decision: str
    text_idea_enabled_after: bool
    simulation_result: str

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "reason_codes": list(self.reason_codes),
            "original_delivery_decision": self.original_delivery_decision,
            "simulated_delivery_decision": self.simulated_delivery_decision,
            "live_delivery_decision": self.live_delivery_decision,
            "text_idea_enabled_after": self.text_idea_enabled_after,
            "simulation_result": self.simulation_result,
        }


class ChannelOverridePolicy:
    def evaluate(self, policy_input: ChannelOverrideInput) -> ChannelOverrideResult:
        channel_tier = _channel_tier(policy_input.channel_tier)
        artifact_type = _artifact_type(policy_input.artifact_type)
        verdict = _verdict(policy_input.verdict)
        delivery_decision = _delivery_decision(policy_input.delivery_decision)
        urgency_profile = _urgency_profile(policy_input.urgency_profile)
        reason_codes = _safe_reason_codes(policy_input.reason_codes)
        input_value = ChannelOverrideInput(
            channel_tier=channel_tier,
            artifact_type=artifact_type,
            verdict=verdict,
            delivery_decision=delivery_decision,
            urgency_profile=urgency_profile,
            reason_codes=reason_codes,
            text_idea_enabled=policy_input.text_idea_enabled,
            ai_noise_signal_count=max(0, int(policy_input.ai_noise_signal_count)),
            external_evidence_present=bool(policy_input.external_evidence_present),
        )

        if input_value.verdict == "skip":
            return _finalize(
                input_value,
                decision="keep",
                reason_codes=("channel_policy_skip_not_upgraded",),
            )
        if input_value.artifact_type == "text_idea" and not input_value.text_idea_enabled:
            return _finalize(
                input_value,
                decision="disable_text_idea",
                reason_codes=("channel_policy_text_idea_already_disabled",),
            )

        if channel_tier == "A":
            return self._evaluate_tier_a(input_value)
        if channel_tier == "B":
            return self._evaluate_tier_b(input_value)
        return self._evaluate_tier_c(input_value)

    def _evaluate_tier_a(self, policy_input: ChannelOverrideInput) -> ChannelOverrideResult:
        if policy_input.verdict == "inspect_now":
            return _finalize(policy_input, decision="keep", reason_codes=("channel_policy_tier_a_keep_inspect_now",))
        if _has_quality_risk(policy_input.reason_codes):
            return _finalize(policy_input, decision="suppress", reason_codes=("channel_policy_tier_a_later_quality_risk",))
        return _finalize(policy_input, decision="keep", reason_codes=("channel_policy_tier_a_keep_later",))

    def _evaluate_tier_b(self, policy_input: ChannelOverrideInput) -> ChannelOverrideResult:
        if (
            policy_input.artifact_type == "text_idea"
            and not policy_input.external_evidence_present
            and _has_high_ai_noise(policy_input)
        ):
            return _finalize(
                policy_input,
                decision="suppress",
                reason_codes=("channel_policy_tier_b_suppress_weak_text_idea_ai_noise",),
            )
        if policy_input.verdict == "inspect_now":
            return _finalize(policy_input, decision="keep", reason_codes=("channel_policy_tier_b_keep_inspect_now",))
        return _finalize(policy_input, decision="keep", reason_codes=("channel_policy_tier_b_later_may_stay_silent",))

    def _evaluate_tier_c(self, policy_input: ChannelOverrideInput) -> ChannelOverrideResult:
        if policy_input.artifact_type == "text_idea" and not _has_strong_dev_context(policy_input.reason_codes):
            return _finalize(
                policy_input,
                decision="disable_text_idea",
                reason_codes=("channel_policy_tier_c_disable_weak_text_idea",),
            )
        if _has_high_ai_noise(policy_input):
            return _finalize(policy_input, decision="suppress", reason_codes=("channel_policy_tier_c_suppress_ai_noise",))
        if policy_input.verdict == "inspect_now":
            if policy_input.external_evidence_present and not _has_quality_risk(policy_input.reason_codes):
                return _finalize(
                    policy_input,
                    decision="keep",
                    reason_codes=("channel_policy_tier_c_keep_strong_inspect_now",),
                )
            return _finalize(
                policy_input,
                decision="downgrade_to_later",
                reason_codes=("channel_policy_tier_c_downgrade_weak_inspect_now",),
            )
        return _finalize(policy_input, decision="suppress", reason_codes=("channel_policy_tier_c_suppress_non_strong",))


def _finalize(
    policy_input: ChannelOverrideInput,
    *,
    decision: str,
    reason_codes: tuple[str, ...],
) -> ChannelOverrideResult:
    if decision == "disable_text_idea":
        simulated_delivery_decision = "suppress"
        text_idea_enabled_after = False
    elif decision == "suppress":
        simulated_delivery_decision = "suppress"
        text_idea_enabled_after = policy_input.text_idea_enabled
    elif decision == "downgrade_to_later":
        simulated_delivery_decision = "send_digest"
        text_idea_enabled_after = policy_input.text_idea_enabled
    else:
        simulated_delivery_decision = policy_input.delivery_decision
        text_idea_enabled_after = policy_input.text_idea_enabled

    if policy_input.delivery_decision == "suppress":
        live_delivery_decision = "suppress"
        simulation_result = "would_keep_suppressed" if decision == "keep" else "would_remain_suppressed"
    else:
        live_delivery_decision = simulated_delivery_decision
        simulation_result = "simulation_only_no_hot_path_change"

    return ChannelOverrideResult(
        decision=decision,
        reason_codes=reason_codes,
        original_delivery_decision=policy_input.delivery_decision,
        simulated_delivery_decision=simulated_delivery_decision,
        live_delivery_decision=live_delivery_decision,
        text_idea_enabled_after=text_idea_enabled_after,
        simulation_result=simulation_result,
    )


def _channel_tier(value: str) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if normalized in CHANNEL_TIERS else "B"


def _artifact_type(value: str) -> str:
    normalized = str(value or "").strip()
    return normalized if normalized in ARTIFACT_TYPES else "unknown"


def _verdict(value: str) -> str:
    normalized = str(value or "").strip()
    return normalized if normalized in VERDICTS else "skip"


def _delivery_decision(value: str) -> str:
    normalized = str(value or "").strip()
    return normalized if normalized in DELIVERY_DECISIONS else "suppress"


def _urgency_profile(value: str) -> str:
    normalized = str(value or "").strip()
    return normalized if normalized in URGENCY_PROFILES else "suppressed"


def _safe_reason_codes(values: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for value in values:
        code = str(value or "").strip().lower()
        if code and all(char.isalnum() or char in {"_", "-", ":"} for char in code):
            normalized.append(code[:80])
    return tuple(normalized)


def _has_quality_risk(reason_codes: tuple[str, ...]) -> bool:
    risk_terms = ("duplicate", "hype", "insufficient_evidence", "weak_evidence", "low_evidence", "evidence_risk")
    return any(any(term in reason_code for term in risk_terms) for reason_code in reason_codes)


def _has_high_ai_noise(policy_input: ChannelOverrideInput) -> bool:
    if policy_input.ai_noise_signal_count >= 2:
        return True
    noise_terms = ("ai_noise", "ai_only", "generic_ai", "weak_ai", "bad_channel_fit")
    return any(any(term in reason_code for term in noise_terms) for reason_code in policy_input.reason_codes)


def _has_strong_dev_context(reason_codes: tuple[str, ...]) -> bool:
    strong_terms = (
        "strong_dev_context",
        "dev_context_present",
        "workflow_specific",
        "tool_signal",
        "code_signal",
        "github_supporting_present",
    )
    return any(any(term in reason_code for term in strong_terms) for reason_code in reason_codes)
