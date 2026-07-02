from __future__ import annotations

from src.services.policy_engine.channel_override_policy import ChannelOverrideInput, ChannelOverridePolicy


def test_tier_a_keeps_strong_inspect_now() -> None:
    result = ChannelOverridePolicy().evaluate(
        ChannelOverrideInput(
            channel_tier="A",
            artifact_type="github_repo",
            verdict="inspect_now",
            delivery_decision="send_now",
            urgency_profile="high",
            reason_codes=("policy_threshold_inspect_now",),
            external_evidence_present=True,
        )
    )

    assert result.decision == "keep"
    assert result.live_delivery_decision == "send_now"


def test_tier_b_suppresses_weak_text_idea_with_high_ai_noise() -> None:
    result = ChannelOverridePolicy().evaluate(
        ChannelOverrideInput(
            channel_tier="B",
            artifact_type="text_idea",
            verdict="later",
            delivery_decision="send_now",
            urgency_profile="normal_silent",
            reason_codes=("weak_ai_context",),
            ai_noise_signal_count=3,
            external_evidence_present=False,
        )
    )

    assert result.decision == "suppress"
    assert result.simulated_delivery_decision == "suppress"


def test_tier_c_disables_weak_text_idea_by_default() -> None:
    result = ChannelOverridePolicy().evaluate(
        ChannelOverrideInput(
            channel_tier="C",
            artifact_type="text_idea",
            verdict="later",
            delivery_decision="send_now",
            urgency_profile="normal_silent",
            reason_codes=("policy_threshold_later",),
            external_evidence_present=False,
        )
    )

    assert result.decision == "disable_text_idea"
    assert result.text_idea_enabled_after is False


def test_policy_never_upgrades_skip() -> None:
    result = ChannelOverridePolicy().evaluate(
        ChannelOverrideInput(
            channel_tier="A",
            artifact_type="github_repo",
            verdict="skip",
            delivery_decision="suppress",
            urgency_profile="suppressed",
            reason_codes=("policy_threshold_skip",),
            external_evidence_present=True,
        )
    )

    assert result.decision == "keep"
    assert "channel_policy_skip_not_upgraded" in result.reason_codes
    assert result.live_delivery_decision == "suppress"


def test_policy_never_changes_suppress_into_send_in_live_terms() -> None:
    result = ChannelOverridePolicy().evaluate(
        ChannelOverrideInput(
            channel_tier="A",
            artifact_type="github_repo",
            verdict="inspect_now",
            delivery_decision="suppress",
            urgency_profile="suppressed",
            reason_codes=("policy_threshold_inspect_now",),
            external_evidence_present=True,
        )
    )

    assert result.decision == "keep"
    assert result.simulation_result == "would_keep_suppressed"
    assert result.live_delivery_decision == "suppress"
