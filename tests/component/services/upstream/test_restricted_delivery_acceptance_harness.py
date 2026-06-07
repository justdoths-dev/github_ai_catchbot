from __future__ import annotations

import pytest

from tests.support.upstream_hot_path_acceptance import (
    event_type_sequence,
    install_runtime_tripwires,
    rerun_same_fixture_chain,
    run_upstream_hot_path_acceptance,
    terminal_counts,
)


@pytest.mark.asyncio
async def test_happy_path_reaches_restricted_delivery_acceptance_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    install_runtime_tripwires(monkeypatch)

    acceptance = await run_upstream_hot_path_acceptance(outcome="send_now")

    assert acceptance.result == {
        "schema_version": "upstream_hot_path_acceptance_v1",
        "status": "pass",
        "source_message_created": True,
        "artifact_created": True,
        "candidate_group_created": True,
        "evidence_bundle_ready": True,
        "judge_output_ready": True,
        "analysis_created": True,
        "notification_plan_created": True,
        "delivery_decision": "send_now",
        "notifier_boundary_reached": True,
        "live_telegram_called": False,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "target_chat_id_present": True,
        "fake_judge_client_calls": 1,
        "event_sequence": [
            "analysis.requested.v1",
            "judge.call.requested.v1",
            "judge.output.ready.v1",
            "analysis.policy.apply.v1",
            "notification.plan.created.v1",
            "notification.delivery.result.v1",
        ],
    }
    assert acceptance.telegram_client.send_calls == 0
    assert acceptance.telegram_client.edit_calls == 0
    assert acceptance.ledger.notification_delivery_records[0]["transport_error_code"] == "notification_send_flag_disabled"
    assert acceptance.ledger.notification_delivery_records[0]["telegram_response_json"]["transport_skipped"] is True


@pytest.mark.asyncio
async def test_suppress_path_stops_after_policy_without_notifier_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    install_runtime_tripwires(monkeypatch)

    acceptance = await run_upstream_hot_path_acceptance(outcome="suppress")

    assert acceptance.result["status"] == "pass"
    assert acceptance.result["delivery_decision"] == "suppress"
    assert acceptance.result["notification_plan_created"] is False
    assert acceptance.result["notifier_boundary_reached"] is False
    assert acceptance.result["live_telegram_called"] is False
    assert acceptance.result["openai_called"] is False
    assert event_type_sequence(acceptance.ledger) == [
        "analysis.requested.v1",
        "judge.call.requested.v1",
        "judge.output.ready.v1",
        "analysis.policy.apply.v1",
    ]
    analysis = next(iter(acceptance.ledger.analyses.values()))
    assert analysis.verdict == "skip"
    assert analysis.delivery_decision == "suppress"
    assert "verdict_skip" in analysis.reason_codes_json
    assert acceptance.ledger.notification_plans == {}
    assert acceptance.ledger.notification_delivery_records == []


@pytest.mark.asyncio
async def test_repeated_same_fixture_invocation_does_not_duplicate_terminal_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_runtime_tripwires(monkeypatch)
    acceptance = await run_upstream_hot_path_acceptance(outcome="send_now")
    counts = terminal_counts(acceptance.ledger)

    await rerun_same_fixture_chain(acceptance)

    assert terminal_counts(acceptance.ledger) == counts
    assert acceptance.telegram_client.send_calls == 0
    assert acceptance.telegram_client.edit_calls == 0
    assert acceptance.ledger.state_transitions[-1]["reason_code"] == "notification_duplicate_terminal_noop"


@pytest.mark.asyncio
async def test_layer_boundaries_remain_policy_owned_and_notifier_render_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_runtime_tripwires(monkeypatch)

    acceptance = await run_upstream_hot_path_acceptance(outcome="send_now")

    judge_output = next(iter(acceptance.ledger.judge_outputs.values()))
    analysis = next(iter(acceptance.ledger.analyses.values()))
    notification_plan = next(iter(acceptance.ledger.notification_plans.values()))

    assert judge_output.payload_json["judge_schema_version"] == "judge_output_v1"
    assert judge_output.model_proposed_verdict == "later"
    assert analysis.verdict == "inspect_now"
    assert analysis.delivery_decision == "send_now"
    assert analysis.policy_reconciled_flag is False
    assert "policy_overrode_model_verdict" in analysis.reason_codes_json
    assert notification_plan.delivery_decision == analysis.delivery_decision
    assert notification_plan.target_chat_id == 12345
    assert acceptance.ledger.notification_delivery_records[0]["result_status"] == "suppressed"
    assert acceptance.ledger.notification_delivery_records[0]["attempt_count"] == 0
