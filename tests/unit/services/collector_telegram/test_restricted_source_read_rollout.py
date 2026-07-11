from __future__ import annotations

import json

import pytest

from src.services.collector_telegram.bounded_history_ingest_runner import SEARCH_CONFIRM_TOKEN
from src.services.collector_telegram.restricted_source_read_rollout import (
    BOUNDED_RUNNER_PATH,
    COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS,
    ENV_OVERLAY_RUNNER_PATH,
    FAKE_CHAT_ID,
    FAKE_CONFIG_VALUE,
    FAKE_MESSAGE_ID,
    FAKE_MESSAGE_TEXT,
    PASS_REASON_CODE,
    PREFLIGHT_PASS_REASON_CODE,
    RUNTIME_ENV_FILE_PLACEHOLDER,
    SEARCH_CONFIRM_TOKEN_PLACEHOLDER,
    SEARCH_PREFLIGHT_PASS_REASON_CODE,
    SEARCH_PREFLIGHT_SCHEMA_VERSION,
    SOURCE_VALUE_PLACEHOLDER,
    TARGET_LOCATOR_PATH_PLACEHOLDER,
    RestrictedLiveCollectorOneChannelSourceReadProofRequest,
    build_restricted_live_collector_github_url_search_preflight_packet,
    build_restricted_live_collector_one_channel_source_read_preflight_packet,
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


def _preflight_packet(**overrides):
    values = {
        "source_value": "@trendingrepo",
        "requested_max_messages": 1,
    }
    values.update(overrides)
    return build_restricted_live_collector_one_channel_source_read_preflight_packet(
        RestrictedLiveCollectorOneChannelSourceReadProofRequest(**values)
    )


def _search_preflight_packet(**overrides):
    values = {
        "source_value": "@trendingrepo",
        "requested_max_messages": 30,
    }
    values.update(overrides)
    return build_restricted_live_collector_github_url_search_preflight_packet(
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


def test_live_preflight_command_packet_reuses_bounded_runner_without_live_authority() -> None:
    report = _preflight_packet()
    rendered = json.dumps(report, sort_keys=True)
    command = report["future_execution_command"]
    command_tokens = command["command_tokens"]
    child_command_tokens = command["child_command_tokens"]
    runtime_env = command["runtime_env"]
    safe_loader = runtime_env["safe_loader_pattern"]

    assert report["schema_version"] == "restricted_live_collector_one_channel_source_read_preflight_v1"
    assert report["status"] == "pass"
    assert report["reason_code"] == PREFLIGHT_PASS_REASON_CODE
    assert report["target_scope"]["exact_single_channel_required"] is True
    assert report["target_scope"]["target_count"] == 1
    assert report["target_scope"]["target_fingerprint"].startswith("sha256:")
    assert report["bounded_read"]["requested_max_messages"] == 1
    assert report["bounded_read"]["hard_max_messages"] == 30
    assert report["bounded_read"]["unbounded_history_allowed"] is False
    assert report["actual_attempted_operations"]["collector_bounded_runner_invoked"] is False
    assert report["actual_attempted_operations"]["fake_telegram_history_read_attempted"] is False
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
    assert command["runner_path"] == ENV_OVERLAY_RUNNER_PATH
    assert command["wrapper_runner_path"] == ENV_OVERLAY_RUNNER_PATH
    assert command["child_runner_path"] == BOUNDED_RUNNER_PATH
    assert command["operator_command_tokens"] == command_tokens
    assert command["max_messages_required"] is True
    assert command["max_messages_argument"] == "--max-messages"
    assert command["max_messages_hard_limit"] == 30
    assert command["exact_confirm_required"] is True
    assert command["confirm_token_value"] == "LIVE_COLLECTOR_1_CHANNEL_SOURCE_LAST_EXECUTE"
    assert command["send_disabled"] is True
    assert command["redis_publish_disabled"] is True
    assert command["source_outbox_publish_disabled"] is True
    assert runtime_env["placeholder"] == RUNTIME_ENV_FILE_PLACEHOLDER
    assert runtime_env["runtime_env_file_placeholder"] == RUNTIME_ENV_FILE_PLACEHOLDER
    assert runtime_env["source_runtime_env_allows_extra_keys"] is True
    assert runtime_env["source_unknown_keys_ignored"] is True
    assert runtime_env["source_forbidden_keys_ignored"] is True
    assert runtime_env["child_overlay_only"] is True
    assert runtime_env["child_overlay_allowed_keys"] == list(COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS)
    assert runtime_env["child_overlay_rejects_unknown_keys"] is True
    assert runtime_env["child_overlay_rejects_forbidden_keys"] is True
    assert runtime_env["uses_sys_executable_for_child"] is True
    assert runtime_env["child_runner_path"] == BOUNDED_RUNNER_PATH
    assert runtime_env["wrapper_runner_path"] == ENV_OVERLAY_RUNNER_PATH
    assert runtime_env["command_token_included"] is True
    assert runtime_env["exact_runtime_env_file_placeholder_required"] is True
    assert runtime_env["values_printed"] is False
    assert runtime_env["path_printed"] is False
    assert runtime_env["runtime_env_file_contents_printed"] is False
    assert runtime_env["runtime_env_values_redacted"] is True
    assert runtime_env["runtime_env_loaded"] is False
    assert runtime_env["actual_runtime_env_file_read_in_this_task"] is False
    assert runtime_env["safe_loader_pattern_available"] is True
    assert safe_loader["exact_runtime_env_file_placeholder_required"] is True
    assert safe_loader["runtime_env_file_placeholder"] == RUNTIME_ENV_FILE_PLACEHOLDER
    assert safe_loader["runtime_env_file_path_printed"] is False
    assert safe_loader["runtime_env_values_printed"] is False
    assert safe_loader["runtime_env_values_redacted"] is True
    assert safe_loader["runtime_env_loaded"] is False
    assert safe_loader["actual_runtime_env_file_read_in_this_task"] is False
    assert safe_loader["allowed_env_keys"] == list(COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS)
    assert safe_loader["allowed_env_keys"]
    assert set(safe_loader["allowed_env_keys"]) == set(COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS)
    assert safe_loader["source_runtime_env_allows_extra_keys"] is True
    assert safe_loader["source_unknown_keys_ignored"] is True
    assert safe_loader["source_forbidden_keys_ignored"] is True
    assert safe_loader["reject_unknown_env_keys"] is False
    assert safe_loader["source_reject_unknown_env_keys"] is False
    assert safe_loader["child_overlay_allowed_keys"] == list(COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS)
    assert safe_loader["child_overlay_only"] is True
    assert safe_loader["reject_unknown_child_overlay_keys"] is True
    assert safe_loader["reject_forbidden_child_overlay_keys"] is True
    assert safe_loader["child_overlay_rejects_unknown_keys"] is True
    assert safe_loader["child_overlay_rejects_forbidden_keys"] is True
    assert safe_loader["load_values_into_child_env_overlay_only"] is True
    assert safe_loader["uses_sys_executable_for_child"] is True
    assert safe_loader["entrypoint_uses_sys_executable"] is True
    assert safe_loader["wrapper_runner_path"] == ENV_OVERLAY_RUNNER_PATH
    assert safe_loader["operator_command_uses_wrapper_runner"] is True
    assert safe_loader["operator_command_tokens"] == command_tokens
    assert safe_loader["child_command_uses_existing_runner"] is True
    assert safe_loader["child_runner_path"] == BOUNDED_RUNNER_PATH
    assert safe_loader["child_command_runner_path"] == BOUNDED_RUNNER_PATH
    assert safe_loader["child_command_tokens"] == child_command_tokens
    assert safe_loader["operator_command_includes_runtime_env_file_token"] is True
    assert safe_loader["operator_command_uses_runtime_env_placeholder_only"] is True
    assert safe_loader["child_command_omits_runtime_env_file_token"] is True
    assert safe_loader["child_command_omits_source_outbox_publish"] is True
    assert safe_loader["child_command_omits_redis_publish"] is True
    assert safe_loader["child_command_omits_send_edit"] is True
    assert safe_loader["child_command_omits_chat_id"] is True
    assert safe_loader["child_command_omits_registry_id"] is True
    assert safe_loader["child_command_omits_docker_systemd_alembic"] is True
    assert command_tokens[:2] == ["venv/bin/python", ENV_OVERLAY_RUNNER_PATH]
    assert child_command_tokens[:2] == ["sys.executable", BOUNDED_RUNNER_PATH]
    for required_token in (
        "--mode",
        "execute",
        "--runtime-env-file",
        RUNTIME_ENV_FILE_PLACEHOLDER,
        "--source-value",
        SOURCE_VALUE_PLACEHOLDER,
        "--max-messages",
        "1",
        "--operator-approved",
        "--confirm-token",
        "LIVE_COLLECTOR_1_CHANNEL_SOURCE_LAST_EXECUTE",
    ):
        assert required_token in command_tokens
    for required_token in (
        "--mode",
        "execute",
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-telegram-read",
        "--allow-database-write",
        "--allow-source-message-write",
        "--allow-source-version-write",
        "--allow-source-outbox-write",
        "--source-kind",
        "public_username",
        "--source-value",
        SOURCE_VALUE_PLACEHOLDER,
        "--max-messages",
        "1",
        "--confirm-token",
        "LIVE_COLLECTOR_1_CHANNEL_SOURCE_LAST_EXECUTE",
    ):
        assert required_token in child_command_tokens
    assert "--allow-source-outbox-publish" not in command_tokens
    assert "--allow-redis-publish" not in command_tokens
    assert "--allow-send" not in command_tokens
    assert "--chat-id" not in command_tokens
    assert "--registry-id" not in command_tokens
    assert "--allow-source-outbox-publish" not in child_command_tokens
    assert "--allow-redis-publish" not in child_command_tokens
    assert "--allow-send" not in child_command_tokens
    assert "--chat-id" not in child_command_tokens
    assert "--registry-id" not in child_command_tokens
    assert "--runtime-env-file" not in child_command_tokens
    assert "--runtime-env-path" not in child_command_tokens
    assert "--env-file" not in child_command_tokens
    assert "docker" not in command_tokens
    assert "systemctl" not in command_tokens
    assert "alembic" not in command_tokens
    assert "docker" not in child_command_tokens
    assert "systemctl" not in child_command_tokens
    assert "alembic" not in child_command_tokens
    assert report["future_readback_plan"]["source_messages"]["expected_count_field"] == (
        "source_current_found_count"
    )
    assert report["future_readback_plan"]["source_message_versions"]["expected_count_field"] == (
        "source_version_rows_count"
    )
    assert report["future_readback_plan"]["source_outbox_events"]["publish_expected"] is False
    assert report["future_readback_plan"]["duplicate_noop_proof"]["expected_count_field"] == (
        "duplicate_noop_proof_count"
    )
    assert report["future_readback_plan"]["authority_transition"][
        "live_telegram_read_attempted_true_only_in_future_execution"
    ] is True
    assert report["redaction_audit"]["command_uses_target_placeholder"] is True
    assert report["redaction_audit"]["runtime_env_path_printed"] is False
    assert report["redaction_audit"]["runtime_env_values_redacted"] is True
    assert report["redaction_audit"]["actual_runtime_env_file_read_in_this_task"] is False
    assert report["completion_claims"]["F1_LIVE_ONE_CHANNEL_SOURCE_READ_PREFLIGHT_PACKET_READY"] is True
    assert report["completion_claims"]["F1_LIVE_ONE_CHANNEL_EXACT_COMMAND_PACKET_READY"] is True
    assert report["completion_claims"]["F1_COLLECTOR_ONLY_RUNTIME_ENV_OVERLAY_PREFLIGHT_READY"] is True
    assert report["completion_claims"]["LIVE_TELEGRAM_READ_AUTHORITY_REMAINS_CLOSED_IN_THIS_TASK"] is True
    assert report["completion_claims"]["LIVE_COLLECTOR_1_CHANNEL_CLOSED"] is False
    assert report["completion_claims"]["production_complete"] is False
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
        "runtime.env",
        "SENTINEL_DATABASE_URL_VALUE",
        "SENTINEL_REDIS_URL_VALUE",
        "SENTINEL_TELEGRAM_API_HASH_VALUE",
        "SENTINEL_TELEGRAM_PHONE_NUMBER_VALUE",
        "SENTINEL_TDLIB_STATE_PATH_VALUE",
        "Traceback",
        "private stderr",
    ):
        assert raw not in rendered


def test_live_preflight_accepts_private_locator_presence_without_reading_or_exposing_path() -> None:
    private_path = "/tmp/sentinel-private-preflight-locator-name.json"
    report = _preflight_packet(source_value=None, target_locator_path=private_path)
    rendered = json.dumps(report, sort_keys=True)
    command = report["future_execution_command"]

    assert report["status"] == "pass"
    assert report["target_locator_present"] is True
    assert report["target_locator_consumption_supported"] is True
    assert report["target_scope"]["target_count"] == 1
    assert report["target_scope"]["target_fingerprint"] is None
    assert "--target-locator-path" in command["operator_command_tokens"]
    assert "--target-locator-path" in command["child_command_tokens"]
    assert TARGET_LOCATOR_PATH_PLACEHOLDER in command["operator_command_tokens"]
    assert TARGET_LOCATOR_PATH_PLACEHOLDER in command["child_command_tokens"]
    assert SOURCE_VALUE_PLACEHOLDER not in command["operator_command_tokens"]
    assert private_path not in rendered
    assert "sentinel-private-preflight-locator-name.json" not in rendered
    assert report["actual_attempted_operations"]["collector_bounded_runner_invoked"] is False


def test_live_preflight_rejects_locator_direct_target_ambiguity_without_command() -> None:
    report = _preflight_packet(target_locator_path="/tmp/private-locator.json")

    assert report["status"] == "blocked"
    assert report["reason_code"] == "target_locator_direct_target_ambiguity"
    assert report["future_execution_command"]["command_tokens"] == []
    assert report["actual_attempted_operations"]["collector_bounded_runner_invoked"] is False


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
    ("overrides", "reason_code", "target_count"),
    [
        ({"source_value": None}, "target_count_must_equal_one", 0),
        ({"source_value": "*"}, "broad_target_not_allowed", 1),
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
def test_live_preflight_command_packet_rejects_non_exact_targets_before_command(
    overrides: dict[str, object],
    reason_code: str,
    target_count: int,
) -> None:
    report = _preflight_packet(**overrides)

    assert report["schema_version"] == "restricted_live_collector_one_channel_source_read_preflight_v1"
    assert report["status"] == "blocked"
    assert report["reason_code"] == reason_code
    assert report["target_scope"]["target_count"] == target_count
    assert report["future_execution_command"]["command_tokens"] == []
    assert report["future_execution_command"]["runtime_env"]["safe_loader_pattern_available"] is False
    assert report["future_execution_command"]["runtime_env"]["safe_loader_pattern"] is None
    assert report["actual_attempted_operations"]["collector_bounded_runner_invoked"] is False
    assert report["actual_attempted_operations"]["fake_telegram_history_read_attempted"] is False
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


@pytest.mark.parametrize(
    ("requested_max_messages", "reason_code"),
    [
        (None, "requested_max_messages_required"),
        (0, "requested_max_messages_out_of_bounds"),
        (31, "requested_max_messages_out_of_bounds"),
    ],
)
def test_live_preflight_command_packet_requires_explicit_bounded_message_cap(
    requested_max_messages: int | None,
    reason_code: str,
) -> None:
    report = _preflight_packet(requested_max_messages=requested_max_messages)

    assert report["schema_version"] == "restricted_live_collector_one_channel_source_read_preflight_v1"
    assert report["status"] == "blocked"
    assert report["reason_code"] == reason_code
    assert report["bounded_read"]["hard_max_messages"] == 30
    assert report["future_execution_command"]["command_tokens"] == []
    assert report["future_execution_command"]["runtime_env"]["safe_loader_pattern_available"] is False
    assert report["future_execution_command"]["runtime_env"]["safe_loader_pattern"] is None
    assert report["completion_claims"]["F1_LIVE_ONE_CHANNEL_SOURCE_READ_PREFLIGHT_PACKET_READY"] is False
    assert report["completion_claims"]["F1_LIVE_ONE_CHANNEL_EXACT_COMMAND_PACKET_READY"] is False
    assert report["authority"]["live_telegram_read_attempted"] is False


def test_search_preflight_reuses_wrapper_with_placeholder_only_read_authority() -> None:
    report = _search_preflight_packet()
    rendered = json.dumps(report, sort_keys=True)
    command = report["future_execution_command"]
    operator_tokens = command["operator_command_tokens"]
    child_tokens = command["child_command_tokens"]

    assert report["schema_version"] == SEARCH_PREFLIGHT_SCHEMA_VERSION
    assert report["status"] == "pass"
    assert report["reason_code"] == SEARCH_PREFLIGHT_PASS_REASON_CODE
    assert report["mode"] == "search"
    assert report["target_scope"]["target_count"] == 1
    assert report["target_scope"]["target_fingerprint"].startswith("sha256:")
    assert report["bounded_read"]["requested_max_messages"] == 30
    assert report["bounded_read"]["history_request_maximum"] == 1
    assert operator_tokens == [
        "venv/bin/python",
        ENV_OVERLAY_RUNNER_PATH,
        "--mode",
        "search",
        "--runtime-env-file",
        RUNTIME_ENV_FILE_PLACEHOLDER,
        "--source-value",
        SOURCE_VALUE_PLACEHOLDER,
        "--max-messages",
        "30",
        "--operator-approved",
        "--confirm-token",
        SEARCH_CONFIRM_TOKEN_PLACEHOLDER,
    ]
    assert child_tokens == [
        "sys.executable",
        BOUNDED_RUNNER_PATH,
        "--mode",
        "search",
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-telegram-read",
        "--source-kind",
        "public_username",
        "--source-value",
        SOURCE_VALUE_PLACEHOLDER,
        "--max-messages",
        "30",
        "--confirm-token",
        SEARCH_CONFIRM_TOKEN_PLACEHOLDER,
    ]
    for forbidden in command["forbidden_flags_absent"]:
        assert forbidden not in operator_tokens
        assert forbidden not in child_tokens
    assert command["database_write_disabled"] is True
    assert command["source_truth_write_disabled"] is True
    assert command["redis_disabled"] is True
    assert command["provider_openai_notifier_disabled"] is True
    assert command["confirm_token_value_printed"] is False
    assert report["actual_attempted_operations"]["runtime_env_read_attempted"] is False
    assert report["actual_attempted_operations"]["child_runner_invoked"] is False
    assert report["completion_claims"]["BOUNDED_GITHUB_LIVE_SEARCH_PREFLIGHT_READY"] is True
    assert SEARCH_CONFIRM_TOKEN not in rendered
    assert "trendingrepo" not in rendered
    assert "runtime.env" not in rendered


def test_search_preflight_passes_private_locator_write_request_with_placeholder_only() -> None:
    private_path = "/tmp/sentinel-private-search-locator-name.json"
    report = _search_preflight_packet(
        target_locator_output_path=private_path,
        allow_target_locator_write=True,
    )
    rendered = json.dumps(report, sort_keys=True)
    command = report["future_execution_command"]

    assert report["status"] == "pass"
    assert report["target_locator_requested"] is True
    assert report["target_locator_consumption_supported"] is True
    for tokens in (command["operator_command_tokens"], command["child_command_tokens"]):
        assert "--target-locator-output-path" in tokens
        assert "--allow-target-locator-write" in tokens
        assert TARGET_LOCATOR_PATH_PLACEHOLDER in tokens
    assert private_path not in rendered
    assert "sentinel-private-search-locator-name.json" not in rendered
    assert report["redaction_audit"]["target_locator_path_printed"] is False
    assert report["actual_attempted_operations"]["child_runner_invoked"] is False


@pytest.mark.parametrize(
    ("overrides", "reason_code"),
    [
        (
            {"target_locator_output_path": "/tmp/private-locator.json"},
            "target_locator_write_authority_missing",
        ),
        (
            {"allow_target_locator_write": True},
            "target_locator_output_path_required",
        ),
        (
            {"target_locator_path": "/tmp/private-locator.json"},
            "target_locator_input_not_allowed_in_search",
        ),
    ],
)
def test_search_preflight_locator_gates_fail_closed_without_command(
    overrides: dict[str, object],
    reason_code: str,
) -> None:
    report = _search_preflight_packet(**overrides)

    assert report["status"] == "blocked"
    assert report["reason_code"] == reason_code
    assert report["future_execution_command"]["operator_command_tokens"] == []
    assert report["future_execution_command"]["child_command_tokens"] == []


@pytest.mark.parametrize(
    ("overrides", "reason_code", "target_count"),
    [
        ({"source_value": None}, "search_requires_exactly_one_target", 0),
        (
            {"source_value": None, "source_values": ("alpha_tools", "beta_tools", "gamma_tools")},
            "search_requires_exactly_one_target",
            3,
        ),
        ({"requested_max_messages": None}, "search_max_messages_required", 1),
        ({"requested_max_messages": 0}, "search_max_messages_out_of_bounds", 1),
        ({"requested_max_messages": 31}, "search_max_messages_out_of_bounds", 1),
    ],
)
def test_search_preflight_rejects_invalid_target_or_cap_without_command(
    overrides: dict[str, object],
    reason_code: str,
    target_count: int,
) -> None:
    report = _search_preflight_packet(**overrides)

    assert report["schema_version"] == SEARCH_PREFLIGHT_SCHEMA_VERSION
    assert report["status"] == "blocked"
    assert report["reason_code"] == reason_code
    assert report["target_scope"]["target_count"] == target_count
    assert report["future_execution_command"]["operator_command_tokens"] == []
    assert report["future_execution_command"]["child_command_tokens"] == []
    assert report["actual_attempted_operations"]["runtime_env_read_attempted"] is False
    assert report["actual_attempted_operations"]["child_runner_invoked"] is False
    assert report["completion_claims"]["BOUNDED_GITHUB_LIVE_SEARCH_PREFLIGHT_READY"] is False
