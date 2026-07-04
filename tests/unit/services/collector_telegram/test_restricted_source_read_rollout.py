from __future__ import annotations

import json

import pytest

from src.services.collector_telegram.restricted_source_read_rollout import (
    FAKE_CHAT_ID,
    FAKE_CONFIG_VALUE,
    FAKE_MESSAGE_ID,
    FAKE_MESSAGE_TEXT,
    PASS_REASON_CODE,
    RestrictedLiveCollectorOneChannelSourceReadProofRequest,
    build_restricted_live_collector_one_channel_source_read_rollout_packet,
)


def _packet(**overrides):
    values = {
        "source_value": "@trendingrepo",
        "requested_max_messages": 1,
    }
    values.update(overrides)
    return build_restricted_live_collector_one_channel_source_read_rollout_packet(
        RestrictedLiveCollectorOneChannelSourceReadProofRequest(**values)
    )


def test_one_channel_source_read_rollout_packet_uses_fake_collector_path_without_live_authority() -> None:
    report = _packet()
    rendered = json.dumps(report, sort_keys=True)

    assert report["schema_version"] == "restricted_live_collector_one_channel_source_read_rollout_v1"
    assert report["status"] == "pass"
    assert report["reason_code"] == PASS_REASON_CODE
    assert report["target_scope"]["exact_single_channel_required"] is True
    assert report["target_scope"]["target_count"] == 1
    assert report["target_scope"]["broad_registry_scan_allowed"] is False
    assert report["bounded_read"]["requested_max_messages"] == 1
    assert report["bounded_read"]["unbounded_history_allowed"] is False
    assert report["authority"]["live_telegram_read_attempted"] is False
    assert report["authority"]["live_telegram_send_attempted"] is False
    assert report["authority"]["openai_attempted"] is False
    assert report["authority"]["github_attempted"] is False
    assert report["authority"]["x_attempted"] is False
    assert report["authority"]["web_attempted"] is False
    assert report["authority"]["redis_mutation_attempted"] is False
    assert report["authority"]["database_write_attempted"] is False
    assert report["runtime_authority_opened_in_this_run"]["live_telegram_read"] is False
    assert report["runtime_authority_opened_in_this_run"]["production_database_write"] is False
    assert report["runtime_authority_opened_in_this_run"]["redis_mutation"] is False
    assert report["actual_attempted_operations"]["collector_bounded_runner_invoked"] is True
    assert report["actual_attempted_operations"]["fake_telegram_history_read_attempted"] is True
    assert report["actual_attempted_operations"]["fake_telegram_history_read_calls"] == 1
    assert report["actual_attempted_operations"]["fake_repository_write_attempted"] is True
    assert report["actual_attempted_operations"]["redis_publish_attempted"] is False
    assert report["readback"]["fake_source_read_messages_observed"] == 1
    assert report["readback"]["fake_source_messages_created_or_reused"] == 1
    assert report["readback"]["fake_source_versions_created_or_reused"] == 1
    assert report["readback"]["fake_source_outbox_events_created_or_reused"] == 1
    assert report["readback"]["duplicate_guard_preserved"] is True
    assert report["readback"]["source_message_fingerprints"]
    assert report["readback"]["source_outbox_event_fingerprints"]
    assert report["completion_claims"]["RESTRICTED_LIVE_COLLECTOR_ONE_CHANNEL_SOURCE_READ_CODE_READY"] is True
    assert report["completion_claims"]["ONE_CHANNEL_SOURCE_READ_ROLLOUT_PROOF_PACKET_READY"] is True
    assert report["completion_claims"]["LIVE_TELEGRAM_READ_AUTHORITY_REMAINS_CLOSED_IN_THIS_TASK"] is True
    assert report["open_gates"]["AUTHORITY_OPEN"] is True
    assert report["open_gates"]["ROLLOUT_OPEN"] is True
    assert report["open_gates"]["FUNCTION_COMPLETE_OPEN"] is True
    assert report["open_gates"]["PRODUCTION_ROLLOUT_OPEN"] is True
    assert report["open_gates"]["PRODUCT_COMPLETE_CLOSED"] is False
    assert report["open_gates"]["PRODUCTION_ROLLOUT_CLOSED"] is False

    for raw in (
        "trendingrepo",
        str(FAKE_CHAT_ID),
        str(FAKE_MESSAGE_ID),
        FAKE_MESSAGE_TEXT,
        FAKE_CONFIG_VALUE,
        "not-used-by-fake-proof",
    ):
        assert raw not in rendered


@pytest.mark.parametrize(
    ("overrides", "reason_code", "target_count"),
    [
        ({"source_value": None}, "target_count_must_equal_one", 0),
        ({"source_value": "   "}, "exact_source_value_required", 1),
        ({"source_value": "*"}, "broad_target_not_allowed", 1),
        ({"source_value": "all channels"}, "broad_target_not_allowed", 1),
        ({"source_value": "123456789"}, "direct_chat_id_target_not_allowed", 1),
        (
            {"source_value": "11111111-1111-1111-1111-111111111111"},
            "direct_registry_id_target_not_allowed",
            1,
        ),
        (
            {"source_value": None, "source_values": ("alpha_tools", "beta_tools")},
            "target_count_must_equal_one",
            2,
        ),
    ],
)
def test_source_read_rollout_packet_rejects_non_exact_targets_before_fake_read(
    overrides: dict[str, object],
    reason_code: str,
    target_count: int,
) -> None:
    report = _packet(**overrides)

    assert report["status"] == "blocked"
    assert report["reason_code"] == reason_code
    assert report["target_scope"]["target_count"] == target_count
    assert report["actual_attempted_operations"]["collector_bounded_runner_invoked"] is False
    assert report["actual_attempted_operations"]["fake_telegram_history_read_attempted"] is False
    assert report["readback"]["fake_source_read_messages_observed"] == 0
    assert report["authority"]["live_telegram_read_attempted"] is False
    assert report["runtime_authority_opened_in_this_run"]["live_telegram_read"] is False


@pytest.mark.parametrize(
    ("requested_max_messages", "reason_code"),
    [
        (None, "requested_max_messages_required"),
        (0, "requested_max_messages_out_of_bounds"),
        (31, "requested_max_messages_out_of_bounds"),
    ],
)
def test_source_read_rollout_packet_requires_explicit_bounded_message_cap(
    requested_max_messages: int | None,
    reason_code: str,
) -> None:
    report = _packet(requested_max_messages=requested_max_messages)

    assert report["status"] == "blocked"
    assert report["reason_code"] == reason_code
    assert report["bounded_read"]["hard_max_messages"] == 30
    assert report["bounded_read"]["unbounded_history_allowed"] is False
    assert report["actual_attempted_operations"]["collector_bounded_runner_invoked"] is False
    assert report["actual_attempted_operations"]["fake_telegram_history_read_attempted"] is False
    assert report["authority"]["live_telegram_read_attempted"] is False
