from __future__ import annotations

from services.policy_engine.verdict_policy import reconcile_model_verdict


def test_same_model_and_policy_verdict_is_reconciled() -> None:
    reconciled, reason_codes = reconcile_model_verdict(
        model_proposed_verdict="inspect_now",
        final_verdict="inspect_now",
        reason_codes=["policy_threshold_inspect_now"],
    )

    assert reconciled is True
    assert reason_codes == ["policy_threshold_inspect_now"]


def test_different_model_and_policy_verdict_is_not_reconciled_and_appends_reason() -> None:
    reconciled, reason_codes = reconcile_model_verdict(
        model_proposed_verdict="inspect_now",
        final_verdict="later",
        reason_codes=["policy_threshold_later"],
    )

    assert reconciled is False
    assert reason_codes == ["policy_threshold_later", "policy_overrode_model_verdict"]


def test_missing_model_verdict_is_not_reconciled_and_appends_reason() -> None:
    reconciled, reason_codes = reconcile_model_verdict(
        model_proposed_verdict=None,
        final_verdict="later",
        reason_codes=["policy_threshold_later"],
    )

    assert reconciled is False
    assert reason_codes == ["policy_threshold_later", "policy_no_model_verdict"]
