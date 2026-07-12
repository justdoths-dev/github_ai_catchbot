from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable
from uuid import UUID


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.collector_telegram.bounded_history_ingest_runner import (
    EXECUTE_CONFIRM_TOKEN,
    MAX_MESSAGES_HARD_LIMIT,
    SEARCH_CONFIRM_TOKEN,
    SEARCH_SCHEMA_VERSION,
    SOURCE_KIND_PUBLIC_USERNAME,
    TARGET_LOCATOR_SCHEMA_VERSION,
    THREE_CHANNEL_EXECUTE_CONFIRM_TOKEN,
    THREE_CHANNEL_TARGET_COUNT,
    render_sanitized_json,
)
from src.services.collector_telegram.runtime_env_overlay import (
    COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS,
    build_collector_runtime_env_overlay,
)


CHILD_RUNNER_PATH = "tools/bounded_collector_history_ingest_runner.py"
WRAPPER_RUNNER_PATH = "tools/restricted_live_collector_one_channel_source_read_env_overlay_runner.py"
SOURCE_VALUE_PLACEHOLDER = "<PUBLIC_USERNAME_SOURCE_VALUE>"
RUNTIME_ENV_FILE_PLACEHOLDER = "<RUNTIME_ENV_FILE>"
SCHEMA_VERSION = "restricted_live_collector_one_channel_source_read_env_overlay_runner_v1"
SEARCH_WRAPPER_SCHEMA_VERSION = "restricted_live_collector_github_url_search_env_overlay_runner_v1"
SEARCH_CONFIRM_TOKEN_PLACEHOLDER = "<SEARCH_CONFIRM_TOKEN>"
TARGET_LOCATOR_PATH_PLACEHOLDER = "<PRIVATE_TARGET_LOCATOR_PATH>"

SubprocessRunner = Callable[..., subprocess.CompletedProcess[str]]

_SAFE_STRING_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-")
_AUTHORITY_PROJECTION_KEYS = (
    "live_telegram_read_attempted",
    "telegram_send_attempted",
    "openai_attempted",
    "github_attempted",
    "x_attempted",
    "web_attempted",
    "redis_consume_or_ack",
    "broad_registry_ingest",
    "docker_or_systemd_called",
    "alembic_or_ddl_ran",
)
_GATE_PROJECTION_KEYS = (
    "operator_approved",
    "confirm_token_valid",
    "runtime_config_allowed",
    "database_read_allowed",
    "telegram_read_allowed",
    "database_write_allowed",
    "source_message_write_allowed",
    "source_version_write_allowed",
    "source_outbox_write_allowed",
    "source_outbox_publish_allowed",
    "redis_publish_allowed",
    "target_locator_present",
    "target_locator_consumption_supported",
)
_ATTEMPT_PROJECTION_KEYS = (
    "telegram_read_attempted",
    "telegram_read_called",
    "database_read_attempted",
    "database_write_attempted",
    "source_message_write_attempted",
    "source_version_write_attempted",
    "source_outbox_write_attempted",
    "source_outbox_publish_attempted",
    "redis_publish_attempted",
)
_BOUNDED_COUNT_PROJECTION_KEYS = (
    "registry_targets",
    "source_messages_created",
    "source_versions_created",
    "source_created_events",
    "source_normalize_handoffs",
    "duplicate_noops",
)
_READBACK_PROJECTION_KEYS = (
    "source_current_found_count",
    "source_version_rows_count",
    "source_created_events_count",
    "source_outbox_events_count",
)
_RAW_VALUES_PRINTED_KEYS = (
    "source_text",
    "source_ref",
    "url",
    "raw_id",
    "target_locator_path",
    "target_locator_basename",
    "target_locator_content",
    "tdlib_payload",
    "database_url",
    "redis_url",
    "secret",
    "runtime_value",
    "stderr",
    "traceback",
    "exception_body",
)
_REDACTIONS_APPLIED_KEYS = (
    "full_chat_id_omitted",
    "full_registry_id_omitted",
    "source_ref_omitted",
    "full_source_message_id_omitted",
    "full_event_id_omitted",
    "full_redis_message_id_omitted",
    "raw_message_json_omitted",
    "message_text_omitted",
    "entities_json_omitted",
    "url_surface_json_omitted",
    "database_url_omitted",
    "redis_url_omitted",
    "telegram_credentials_omitted",
    "tdlib_session_paths_omitted",
    "target_locator_path_omitted",
    "target_locator_basename_omitted",
    "target_locator_content_omitted",
    "target_locator_raw_target_values_omitted",
    "exception_detail_omitted",
    "traceback_omitted",
    "stderr_omitted",
)
_SIDE_EFFECT_PROJECTION_KEYS = (
    "telegram_send_called",
    "telegram_edit_called",
)
_SEARCH_SELECTED_MATCH_SOURCE_KEYS = ("entity", "preview", "regex")
_SEARCH_TDLIB_KEYS = (
    "auth_ready_checked",
    "auth_ready",
    "parameters_submitted",
    "log_suppression_attempted",
    "log_suppression_confirmed",
)
_SEARCH_GATE_KEYS = (
    "operator_approved",
    "confirm_token_present",
    "confirm_token_valid",
    "runtime_config_allowed",
    "database_read_allowed",
    "telegram_read_allowed",
    "exact_single_public_username_required",
    "max_messages_explicit",
    "write_authority_absent",
    "publish_authority_absent",
    "target_locator_write_allowed",
    "target_locator_output_path_present",
    "target_locator_consumption_supported",
)
_SEARCH_AUTHORITY_KEYS = (
    "database_read_allowed",
    "telegram_read_allowed",
    "database_write_allowed",
    "source_truth_write_allowed",
    "cursor_write_allowed",
    "redis_allowed",
    "provider_calls_allowed",
    "openai_allowed",
    "notifier_allowed",
)
_SEARCH_SIDE_EFFECT_KEYS = (
    "database_write_attempted",
    "source_message_write_attempted",
    "source_version_write_attempted",
    "source_outbox_write_attempted",
    "channel_cursor_write_attempted",
    "source_outbox_publish_attempted",
    "redis_publish_attempted",
    "history_ingest_processor_instantiated",
    "read_exact_message_called",
    "provider_or_openai_called",
    "telegram_send_or_edit_called",
    "notifier_called",
    "target_locator_write_attempted",
)
_SEARCH_REDACTION_KEYS = (
    "public_username_omitted",
    "channel_title_omitted",
    "full_chat_id_omitted",
    "full_registry_id_omitted",
    "full_message_id_omitted",
    "raw_message_json_omitted",
    "message_text_omitted",
    "caption_text_omitted",
    "raw_url_omitted",
    "hostname_omitted",
    "repo_owner_name_omitted",
    "entities_json_omitted",
    "url_surface_json_omitted",
    "tdlib_payload_omitted",
    "message_link_omitted",
    "runtime_env_path_omitted",
    "runtime_values_omitted",
    "database_url_omitted",
    "redis_url_omitted",
    "telegram_credentials_omitted",
    "confirm_token_omitted",
    "target_locator_path_omitted",
    "target_locator_basename_omitted",
    "target_locator_content_omitted",
    "target_locator_raw_target_values_omitted",
    "exception_detail_omitted",
    "traceback_omitted",
    "stderr_omitted",
)
_SEARCH_RAW_VALUE_KEYS = (
    "public_username",
    "channel_title",
    "chat_id",
    "registry_id",
    "message_id",
    "source_text",
    "caption_text",
    "url",
    "hostname",
    "repo_owner_name",
    "entities",
    "tdlib_payload",
    "message_link",
    "runtime_env_path",
    "runtime_value",
    "database_url",
    "redis_url",
    "credential",
    "confirm_token",
    "target_locator_path",
    "target_locator_basename",
    "target_locator_content",
    "target_locator_source_value",
    "target_locator_message_id",
    "exception_body",
    "traceback",
    "stderr",
)
_SEARCH_ROLLBACK_CLOSE_KEYS = (
    "close_attempted",
    "close_succeeded",
    "rollback_requested",
    "commit_requested",
    "commit_called",
)
_SEARCH_COUNT_KEYS = (
    "requested_max_messages",
    "messages_returned",
    "messages_examined",
    "messages_with_text_surface_count",
    "messages_with_entity_surface_count",
    "messages_with_url_surface_count",
    "messages_with_entity_url_source_count",
    "messages_with_preview_url_source_count",
    "messages_with_regex_url_source_count",
    "github_matching_message_count",
    "history_request_count",
)
_SEARCH_REASON_CODES = frozenset(
    {
        "operator_approval_missing",
        "search_rollout_scope_not_allowed",
        "source_kind_unsupported",
        "direct_chat_or_registry_id_target_not_allowed",
        "direct_chat_id_target_not_allowed",
        "direct_registry_id_target_not_allowed",
        "search_requires_exactly_one_target",
        "search_max_messages_required",
        "search_max_messages_out_of_bounds",
        "search_target_message_id_not_allowed",
        "search_registry_suffix_not_allowed",
        "search_max_targets_not_allowed",
        "search_confirm_token_missing",
        "search_confirm_token_invalid",
        "search_write_authority_not_allowed",
        "search_publish_authority_not_allowed",
        "runtime_config_not_allowed",
        "database_read_not_allowed",
        "telegram_read_not_allowed",
        "runtime_config_failed",
        "search_redis_runtime_not_allowed",
        "search_runtime_commit_not_allowed",
        "target_locator_input_not_allowed_in_search",
        "target_locator_write_authority_missing",
        "target_locator_output_path_required",
        "target_locator_output_path_relative",
        "target_locator_output_path_outside_allowed_roots",
        "target_locator_output_path_traversal_not_allowed",
        "target_locator_output_root_not_real",
        "target_locator_output_parent_missing",
        "target_locator_output_parent_symlink_not_allowed",
        "target_locator_output_parent_not_directory",
        "target_locator_output_parent_invalid",
        "target_locator_output_target_symlink_not_allowed",
        "target_locator_output_target_exists",
        "target_locator_output_create_failed",
        "target_locator_output_write_failed",
        "target_locator_output_readback_failed",
        "target_locator_output_close_failed",
        "target_locator_output_cleanup_failed",
        "target_locator_output_cleanup_unconfirmed",
        "target_locator_output_private_mode_unconfirmed",
        "target_locator_payload_too_large",
        "target_locator_payload_invalid",
        "search_history_request_count_exceeded",
        "tdlib_log_suppression_unconfirmed",
        "tdlib_initialize_failed",
        "tdlib_parameters_required",
        "tdlib_not_authorized",
        "tdlib_auth_state_invalid",
        "tdlib_auth_ready_timeout",
        "telegram_history_read_failed",
        "telegram_history_response_invalid",
        "telegram_history_read_timeout",
        "github_url_live_search_history_empty",
        "github_url_live_target_not_found_in_approved_window",
        "github_url_live_target_found",
        "history_result_exceeds_requested_limit",
        "github_url_live_search_projection_failed",
        "github_url_live_search_selected_message_id_invalid",
        "github_url_live_search_failed",
        "runtime_rollback_failed",
        "source_value_missing",
        "registry_target_missing",
        "registry_target_multiple",
        "registry_id_invalid",
        "registry_id_suffix_mismatch",
        "registry_target_not_active",
        "registry_target_not_joined",
        "registry_target_chat_id_missing",
        "registry_target_chat_id_invalid",
        "non_target_channel_history_message",
    }
)
_SEARCH_FAILURE_BUCKETS = frozenset(
    {
        "request_construction_repair",
        "bounded_window_adjustment",
        "runtime_tdlib_access_issue",
        "target_unavailable",
    }
)
_EXACT_MESSAGE_READ_FAILURE_BUCKETS = frozenset(
    {
        "not_found",
        "tdlib_error",
        "unsupported_response",
        "timeout",
    }
)


class CliArgumentError(ValueError):
    pass


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Build a collector-only runtime env overlay before one-channel live source-read execution.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=("plan", "execute", "search"), default="plan")
    parser.add_argument("--runtime-env-file", default=None)
    parser.add_argument("--source-value", action="append", dest="source_values")
    parser.add_argument("--target-locator-path", default=None)
    parser.add_argument("--target-locator-output-path", default=None)
    parser.add_argument("--allow-target-locator-write", action="store_true")
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--confirm-token", default=None)
    return parser


def run(
    args: argparse.Namespace,
    *,
    subprocess_runner: SubprocessRunner | None = None,
) -> RunnerResult:
    mode = str(args.mode or "plan")
    if mode == "search":
        return _run_search(args, subprocess_runner=subprocess_runner)

    source_values = tuple(args.source_values or ())
    target_locator_present = args.target_locator_path is not None
    if args.allow_target_locator_write or args.target_locator_output_path is not None:
        report = _base_report(
            status="blocked",
            reason_code="target_locator_write_search_mode_required",
            mode=mode,
            requested_max_messages=args.max_messages,
            target_fingerprint=None,
            target_locator_present=target_locator_present,
        )
        return RunnerResult(exit_code=1, report=report)
    if target_locator_present and source_values:
        report = _base_report(
            status="blocked",
            reason_code="target_locator_direct_target_ambiguity",
            mode=mode,
            requested_max_messages=args.max_messages,
            target_fingerprint=None,
            target_locator_present=True,
        )
        return RunnerResult(exit_code=1, report=report)
    normalized_sources, target_error = (
        ((), None)
        if target_locator_present
        else _normalize_public_username_targets(source_values)
    )
    max_messages = args.max_messages
    max_messages_error = _max_messages_error(max_messages)
    target_fingerprints = tuple(
        fingerprint
        for fingerprint in (_fingerprint("source_value", source) for source in normalized_sources)
        if fingerprint is not None
    )
    target_fingerprint = target_fingerprints[0] if len(target_fingerprints) == 1 else None

    if target_error is not None:
        report = _base_report(
            status="blocked",
            reason_code=target_error,
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
            target_fingerprints=target_fingerprints,
            target_locator_present=target_locator_present,
        )
        return RunnerResult(exit_code=1, report=report)
    if max_messages_error is not None:
        report = _base_report(
            status="blocked",
            reason_code=max_messages_error,
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
            target_fingerprints=target_fingerprints,
            target_locator_present=target_locator_present,
        )
        return RunnerResult(exit_code=1, report=report)
    if mode == "execute" and not bool(args.operator_approved):
        report = _base_report(
            status="blocked",
            reason_code="operator_approval_missing",
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
            target_fingerprints=target_fingerprints,
            target_locator_present=target_locator_present,
        )
        return RunnerResult(exit_code=1, report=report)
    expected_confirm_token = (
        THREE_CHANNEL_EXECUTE_CONFIRM_TOKEN
        if len(normalized_sources) == THREE_CHANNEL_TARGET_COUNT
        else EXECUTE_CONFIRM_TOKEN
    )
    if mode == "execute" and str(args.confirm_token or "").strip() != expected_confirm_token:
        report = _base_report(
            status="blocked",
            reason_code="confirm_token_missing" if not str(args.confirm_token or "").strip() else "confirm_token_invalid",
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
            target_fingerprints=target_fingerprints,
            target_locator_present=target_locator_present,
        )
        return RunnerResult(exit_code=1, report=report)
    if not args.runtime_env_file:
        report = _base_report(
            status="blocked",
            reason_code="runtime_env_file_required",
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
            target_fingerprints=target_fingerprints,
            target_locator_present=target_locator_present,
        )
        return RunnerResult(exit_code=1, report=report)

    assert normalized_sources or target_locator_present
    assert isinstance(max_messages, int)
    overlay_result = build_collector_runtime_env_overlay(str(args.runtime_env_file))
    if not overlay_result.ok:
        report = _base_report(
            status="blocked",
            reason_code=str(overlay_result.reason_code),
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
            target_fingerprints=target_fingerprints,
            target_locator_present=target_locator_present,
            runtime_env_read_attempted=True,
            overlay_report=overlay_result.to_sanitized_dict(),
        )
        return RunnerResult(exit_code=1, report=report)

    child_command = _child_command_tokens(
        source_values=normalized_sources,
        target_locator_path=args.target_locator_path,
        max_messages=max_messages,
    )
    if mode == "plan":
        report = _base_report(
            status="pass",
            reason_code="collector_runtime_env_overlay_plan_ready",
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
            target_fingerprints=target_fingerprints,
            target_locator_present=target_locator_present,
            runtime_env_read_attempted=True,
            overlay_report=overlay_result.to_sanitized_dict(),
            redacted_child_command_tokens=_redacted_child_plan_command_tokens(
                target_count=len(normalized_sources),
                max_messages=max_messages,
                target_locator_present=target_locator_present,
            ),
        )
        return RunnerResult(exit_code=0, report=report)

    runner = subprocess_runner or subprocess.run
    completed = runner(
        list(child_command),
        cwd=str(REPO_ROOT),
        env=dict(overlay_result.child_overlay),
        text=True,
        capture_output=True,
        check=False,
    )
    child_report = _parse_child_report(getattr(completed, "stdout", ""))
    child_returncode = int(getattr(completed, "returncode", 1))
    status = "pass" if child_returncode == 0 else "failed"
    reason_code = "child_bounded_runner_passed" if child_returncode == 0 else "child_bounded_runner_failed"
    report = _base_report(
        status=status,
        reason_code=reason_code,
        mode=mode,
        requested_max_messages=max_messages,
        target_fingerprint=target_fingerprint,
        target_fingerprints=target_fingerprints,
        target_locator_present=target_locator_present,
        runtime_env_read_attempted=True,
        overlay_report=overlay_result.to_sanitized_dict(),
        redacted_child_command_tokens=_redacted_child_execute_command_tokens(
            target_count=len(normalized_sources),
            max_messages=max_messages,
            target_locator_present=target_locator_present,
        ),
        child_runner_invoked=True,
        child_runner_returncode=child_returncode,
        child_runner_report=child_report,
    )
    return RunnerResult(exit_code=0 if child_returncode == 0 else 1, report=report)


def _run_search(
    args: argparse.Namespace,
    *,
    subprocess_runner: SubprocessRunner | None,
) -> RunnerResult:
    source_values = tuple(args.source_values or ())
    max_messages = args.max_messages
    target_locator_output_path_present = args.target_locator_output_path is not None
    target_locator_write_allowed = bool(args.allow_target_locator_write)
    target_locator_requested = bool(
        target_locator_write_allowed or target_locator_output_path_present
    )
    normalized_source, target_error = _normalize_search_public_username_target(source_values)
    target_fingerprint = _fingerprint("source_value", normalized_source)

    def blocked(reason_code: str) -> RunnerResult:
        return RunnerResult(
            exit_code=1,
            report=_base_search_report(
                status="blocked",
                reason_code=reason_code,
                requested_max_messages=max_messages,
                target_count=len(source_values),
                target_fingerprint=target_fingerprint,
                target_locator_requested=target_locator_requested,
                target_locator_output_path_present=target_locator_output_path_present,
                target_locator_write_allowed=target_locator_write_allowed,
            ),
        )

    if not bool(args.operator_approved):
        return blocked("operator_approval_missing")
    if args.target_locator_path is not None:
        return blocked("target_locator_input_not_allowed_in_search")
    if target_error is not None:
        return blocked(target_error)
    max_messages_error = _search_max_messages_error(max_messages)
    if max_messages_error is not None:
        return blocked(max_messages_error)
    confirm_token = str(args.confirm_token or "").strip()
    if not confirm_token:
        return blocked("search_confirm_token_missing")
    if confirm_token != SEARCH_CONFIRM_TOKEN:
        return blocked("search_confirm_token_invalid")
    if not args.runtime_env_file:
        return blocked("runtime_env_file_required")

    assert normalized_source is not None
    assert isinstance(max_messages, int)
    overlay_result = build_collector_runtime_env_overlay(str(args.runtime_env_file))
    if not overlay_result.ok:
        return RunnerResult(
            exit_code=1,
            report=_base_search_report(
                status="blocked",
                reason_code=str(overlay_result.reason_code),
                requested_max_messages=max_messages,
                target_count=1,
                target_fingerprint=target_fingerprint,
                target_locator_requested=target_locator_requested,
                target_locator_output_path_present=target_locator_output_path_present,
                target_locator_write_allowed=target_locator_write_allowed,
                runtime_env_read_attempted=True,
                overlay_report=overlay_result.to_sanitized_dict(),
            ),
        )

    child_command = _search_child_command_tokens(
        source_value=normalized_source,
        max_messages=max_messages,
        target_locator_output_path=args.target_locator_output_path,
        allow_target_locator_write=bool(args.allow_target_locator_write),
    )
    runner = subprocess_runner or subprocess.run
    try:
        completed = runner(
            list(child_command),
            cwd=str(REPO_ROOT),
            env=dict(overlay_result.child_overlay),
            text=True,
            capture_output=True,
            check=False,
        )
    except Exception:
        report = _base_search_report(
            status="failed",
            reason_code="child_bounded_search_invocation_failed",
            requested_max_messages=max_messages,
            target_count=1,
            target_fingerprint=target_fingerprint,
            target_locator_requested=target_locator_requested,
            target_locator_output_path_present=target_locator_output_path_present,
            target_locator_write_allowed=target_locator_write_allowed,
            runtime_env_read_attempted=True,
            overlay_report=overlay_result.to_sanitized_dict(),
            redacted_child_command_tokens=_redacted_search_child_command_tokens(
                max_messages=max_messages,
                target_locator_output_path_present=args.target_locator_output_path is not None,
                allow_target_locator_write=bool(args.allow_target_locator_write),
            ),
        )
        return RunnerResult(exit_code=1, report=report)
    child_report = _parse_child_report(getattr(completed, "stdout", ""))
    child_returncode = int(getattr(completed, "returncode", 1))
    projection = _project_search_child_report(child_report)
    if child_returncode == 0 and projection.get("status") == "pass":
        status = "pass"
        reason_code = "child_bounded_search_passed"
    elif child_returncode != 0 and projection.get("status") == "blocked":
        status = "blocked"
        reason_code = "child_bounded_search_blocked"
    else:
        status = "failed"
        reason_code = "child_bounded_search_failed"
    report = _base_search_report(
        status=status,
        reason_code=reason_code,
        requested_max_messages=max_messages,
        target_count=1,
        target_fingerprint=target_fingerprint,
        target_locator_requested=target_locator_requested,
        target_locator_output_path_present=target_locator_output_path_present,
        target_locator_write_allowed=target_locator_write_allowed,
        runtime_env_read_attempted=True,
        overlay_report=overlay_result.to_sanitized_dict(),
        redacted_child_command_tokens=_redacted_search_child_command_tokens(
            max_messages=max_messages,
            target_locator_output_path_present=args.target_locator_output_path is not None,
            allow_target_locator_write=bool(args.allow_target_locator_write),
        ),
        child_runner_invoked=True,
        child_runner_returncode=child_returncode,
        child_runner_report=child_report,
    )
    if status in {"pass", "blocked"} and report["search_contract_projection"]["reviewable"] is not True:
        status = "failed"
        report["status"] = status
        report["reason_code"] = "child_bounded_search_contract_invalid"
    return RunnerResult(exit_code=0 if status == "pass" else 1, report=report)


def restricted_env_overlay_argument_error_report(error_code: str) -> dict[str, Any]:
    return _base_report(
        status="blocked",
        reason_code=error_code,
        mode="unknown",
        requested_max_messages=None,
        target_fingerprint=None,
    )


def restricted_search_env_overlay_argument_error_report(error_code: str) -> dict[str, Any]:
    return _base_search_report(
        status="blocked",
        reason_code=error_code,
        requested_max_messages=None,
        target_count=0,
        target_fingerprint=None,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    subprocess_runner: SubprocessRunner | None = None,
) -> int:
    effective_argv = list(sys.argv[1:] if argv is None else argv)
    try:
        args = build_parser().parse_args(effective_argv)
    except CliArgumentError as exc:
        error_report = (
            restricted_search_env_overlay_argument_error_report(str(exc))
            if _argv_requests_search(effective_argv)
            else restricted_env_overlay_argument_error_report(str(exc))
        )
        sys.stdout.write(render_sanitized_json(error_report))
        return 1
    result = run(args, subprocess_runner=subprocess_runner)
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


def _argv_requests_search(argv: Sequence[str]) -> bool:
    for index, token in enumerate(argv):
        if token == "--mode" and index + 1 < len(argv):
            return argv[index + 1].strip().lower() == "search"
        if token.startswith("--mode="):
            return token.partition("=")[2].strip().lower() == "search"
    return False


def _base_report(
    *,
    status: str,
    reason_code: str,
    mode: str,
    requested_max_messages: int | None,
    target_fingerprint: str | None,
    target_fingerprints: Sequence[str] = (),
    target_locator_present: bool = False,
    runtime_env_read_attempted: bool = False,
    overlay_report: Mapping[str, Any] | None = None,
    redacted_child_command_tokens: Sequence[str] = (),
    child_runner_invoked: bool = False,
    child_runner_returncode: int | None = None,
    child_runner_report: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    report_target_fingerprints = tuple(target_fingerprints or ((target_fingerprint,) if target_fingerprint else ()))
    child_report = _compact_child_report(child_runner_report)
    child_report_projection = _project_child_report(child_runner_report)
    redaction_audit = {
        "runtime_env_values_printed": False,
        "runtime_env_file_contents_printed": False,
        "runtime_env_file_path_printed": False,
        "raw_source_value_printed": False,
        "target_locator_path_printed": False,
        "target_locator_basename_printed": False,
        "target_locator_content_printed": False,
        "target_locator_raw_target_values_printed": False,
        "child_stdout_printed": False,
        "child_stderr_printed": False,
        "token_or_secret_printed": False,
    }
    f1_live_readback_closure = _build_f1_live_readback_closure(
        child_report_projection,
        child_report=child_report,
        child_runner_returncode=child_runner_returncode,
        redaction_audit=redaction_audit,
        wrapper_reason_code=reason_code,
        wrapper_status=status,
    )
    source_truth_readback_closure = _build_source_truth_readback_closure(
        child_report_projection,
        child_report=child_report,
        child_runner_returncode=child_runner_returncode,
        redaction_audit=redaction_audit,
        wrapper_reason_code=reason_code,
        wrapper_status=status,
    )
    f1_duplicate_noop_readback_closure = _build_f1_duplicate_noop_readback_closure(
        child_report_projection,
        source_truth_readback_closure=source_truth_readback_closure,
    )
    f1_fresh_write_readback_closure = _build_f1_fresh_write_readback_closure(
        child_report_projection,
        source_truth_readback_closure=source_truth_readback_closure,
    )
    f1_exact_live_readback_review_closure = {
        "duplicate_noop_readback_closed": f1_duplicate_noop_readback_closure["closed"],
        "fresh_write_readback_closed": f1_fresh_write_readback_closure["closed"],
        "closed": (
            f1_duplicate_noop_readback_closure["closed"] is True
            or f1_fresh_write_readback_closure["closed"] is True
        ),
    }
    f2_three_channel_readback_closure = _build_f2_three_channel_readback_closure(
        child_report_projection,
        child_report=child_report,
        child_runner_returncode=child_runner_returncode,
        redaction_audit=redaction_audit,
        wrapper_reason_code=reason_code,
        wrapper_status=status,
    )
    three_channel_target_requested = len(report_target_fingerprints) == THREE_CHANNEL_TARGET_COUNT
    overlay_plan_ready = (
        status == "pass"
        and mode == "plan"
        and runtime_env_read_attempted
        and (overlay_report or {}).get("status") == "pass"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "reason_code": reason_code,
        "mode": mode,
        "target_scope": {
            "exact_single_public_username_supported": True,
            "exact_three_public_usernames_supported": True,
            "exact_single_public_username_required": target_locator_present
            or len(report_target_fingerprints) <= 1,
            "exact_three_public_usernames_required": three_channel_target_requested,
            "target_count": 1 if target_locator_present else len(report_target_fingerprints),
            "target_fingerprint": target_fingerprint,
            "target_fingerprints": list(report_target_fingerprints),
            "raw_source_value_printed": False,
            "direct_chat_id_allowed": False,
            "direct_registry_id_allowed": False,
            "broad_target_allowed": False,
        },
        "bounded_read": {
            "requested_max_messages": requested_max_messages,
            "hard_max_messages": MAX_MESSAGES_HARD_LIMIT,
            "unbounded_history_allowed": False,
        },
        "target_locator_present": target_locator_present,
        "target_locator_consumption_supported": True,
        "target_locator_path_omitted": True,
        "target_locator_basename_omitted": True,
        "target_locator_content_omitted": True,
        "target_locator_raw_target_values_omitted": True,
        "runtime_env_overlay": overlay_report
        or {
            "schema_version": "collector_runtime_env_overlay_v1",
            "status": "not_attempted",
            "reason_code": "runtime_env_overlay_not_attempted",
            "source_runtime_env_allows_extra_keys": True,
            "source_unknown_keys_ignored": True,
            "source_forbidden_keys_ignored": True,
            "child_overlay_only": True,
            "child_overlay_allowed_keys": list(COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS),
            "child_overlay_keys": [],
            "child_overlay_rejects_unknown_keys": True,
            "child_overlay_rejects_forbidden_keys": True,
            "runtime_env_values_printed": False,
            "runtime_env_file_contents_printed": False,
            "runtime_env_file_path_printed": False,
        },
        "child_command": {
            "uses_sys_executable_for_child": True,
            "child_runner_path": CHILD_RUNNER_PATH,
            "wrapper_runner_path": WRAPPER_RUNNER_PATH,
            "command_tokens": list(redacted_child_command_tokens),
            "redacted_command_tokens": True,
            "source_value_placeholder": SOURCE_VALUE_PLACEHOLDER,
            "target_locator_path_placeholder": TARGET_LOCATOR_PATH_PLACEHOLDER,
            "runtime_env_file_placeholder": RUNTIME_ENV_FILE_PLACEHOLDER,
            "forbidden_flags_absent": [
                "--allow-source-outbox-publish",
                "--allow-redis-publish",
                "--allow-send",
                "--chat-id",
                "--registry-id",
                "--all-channels",
                "--docker",
                "--systemd",
            ],
        },
        "actual_attempted_operations": {
            "runtime_env_read_attempted": runtime_env_read_attempted,
            "child_runner_invoked": child_runner_invoked,
            "child_runner_returncode": child_runner_returncode,
            "live_telegram_read_attempted_by_wrapper": False,
            "telegram_send_or_edit_attempted": False,
            "openai_attempted": False,
            "github_attempted": False,
            "x_attempted": False,
            "web_attempted": False,
            "redis_publish_attempted_by_wrapper": False,
            "docker_or_systemd_called": False,
            "alembic_called": False,
        },
        "child_report": child_report,
        "child_report_projection": child_report_projection,
        "f1_live_readback_closure": f1_live_readback_closure,
        "source_truth_readback_closure": source_truth_readback_closure,
        "f1_duplicate_noop_readback_closure": f1_duplicate_noop_readback_closure,
        "f1_fresh_write_readback_closure": f1_fresh_write_readback_closure,
        "f1_exact_live_readback_review_closure": f1_exact_live_readback_review_closure,
        "f2_three_channel_readback_closure": f2_three_channel_readback_closure,
        "redaction_audit": redaction_audit,
        "completion_claims": {
            "F1_COLLECTOR_ONLY_RUNTIME_ENV_OVERLAY_PREFLIGHT_READY": status == "pass",
            "F1_CHILD_READBACK_PROJECTION_READY": child_report_projection["stdout_parsed_as_json"] is True,
            "F1_LIVE_EXECUTION_REVIEWABILITY_REPAIRED": f1_exact_live_readback_review_closure["closed"],
            "F1_SOURCE_TRUTH_DURABLE_READBACK_REVIEWABLE": source_truth_readback_closure[
                "durable_readback_present"
            ],
            "F1_DUPLICATE_NOOP_READBACK_REVIEWABLE": f1_duplicate_noop_readback_closure["closed"],
            "F1_EXACT_LIVE_READBACK_REVIEWABLE": f1_exact_live_readback_review_closure["closed"],
            "F2_THREE_CHANNEL_ENV_OVERLAY_PREFLIGHT_READY": (
                three_channel_target_requested and (overlay_plan_ready or status == "pass")
            ),
            "F2_THREE_CHANNEL_LIVE_SOURCE_READ_PROOF_READY": f2_three_channel_readback_closure["closed"],
            "LIVE_TELEGRAM_READ_AUTHORITY_REMAINS_CLOSED_IN_THIS_TASK": not child_runner_invoked,
            "LIVE_COLLECTOR_1_CHANNEL_CLOSED": False,
            "LIVE_COLLECTOR_3_CHANNEL_CLOSED": False,
            "LIVE_COLLECTOR_FULL_REGISTRY_CLOSED": False,
            "FUNCTION_COMPLETE_CLOSED": False,
            "PRODUCTION_ROLLOUT_CLOSED": False,
            "PRODUCT_COMPLETE_CLOSED": False,
            "production_complete": False,
            "production_rollout_complete": False,
        },
    }


def _base_search_report(
    *,
    status: str,
    reason_code: str,
    requested_max_messages: int | None,
    target_count: int,
    target_fingerprint: str | None,
    target_locator_requested: bool = False,
    target_locator_output_path_present: bool = False,
    target_locator_write_allowed: bool = False,
    runtime_env_read_attempted: bool = False,
    overlay_report: Mapping[str, Any] | None = None,
    redacted_child_command_tokens: Sequence[str] = (),
    child_runner_invoked: bool = False,
    child_runner_returncode: int | None = None,
    child_runner_report: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    projection = _project_search_child_report(child_runner_report)
    projected_side_effects = _mapping_child(projection, "side_effects")
    projected_gates = _mapping_child(projection, "gates")
    projected_authority = _mapping_child(projection, "authority")
    projected_redactions = _mapping_child(projection, "redactions_applied")
    projected_raw_values = _mapping_child(projection, "raw_values_printed")
    projected_rollback_close = _mapping_child(projection, "rollback_close_readback")
    history_request_count = projection.get("history_request_count")
    projection_ready = (
        projection.get("stdout_parsed_as_json") is True
        and projection.get("schema_version") == SEARCH_SCHEMA_VERSION
        and projection.get("mode") == "search"
        and projection.get("status") in {"pass", "blocked"}
        and projection.get("reason_code") is not None
    )
    terminal_read_reason_codes = {
        "github_url_live_target_found",
        "github_url_live_target_not_found_in_approved_window",
        "github_url_live_search_history_empty",
    }
    terminal_read_contract_satisfied = (
        projection.get("reason_code") not in terminal_read_reason_codes
        or (
            history_request_count == 1
            and projection.get("telegram_read_attempted") is True
            and projection.get("telegram_read_called") is True
            and projection.get("telegram_read_succeeded") is True
            and projected_rollback_close.get("close_attempted") is True
            and projected_rollback_close.get("close_succeeded") is True
            and projected_rollback_close.get("rollback_requested") is True
            and projected_rollback_close.get("commit_requested") is False
            and projected_rollback_close.get("commit_called") is False
        )
    )
    gate_contract_satisfied = all(
        projected_gates.get(key) is True
        for key in _SEARCH_GATE_KEYS
        if key not in {"target_locator_write_allowed", "target_locator_output_path_present"}
    )
    authority_contract_satisfied = (
        projected_authority.get("database_read_allowed") is True
        and projected_authority.get("telegram_read_allowed") is True
        and all(
            projected_authority.get(key) is False
            for key in _SEARCH_AUTHORITY_KEYS
            if key not in {"database_read_allowed", "telegram_read_allowed"}
        )
    )
    projected_locator_requested = projection.get("target_locator_requested")
    projected_locator_written = projection.get("target_locator_written")
    projected_locator_private_mode_confirmed = projection.get(
        "target_locator_private_mode_confirmed"
    )
    projected_locator_write_attempted = projected_side_effects.get(
        "target_locator_write_attempted"
    )
    locator_contract_satisfied = (
        projected_locator_requested is target_locator_requested
        and projected_gates.get("target_locator_output_path_present")
        is target_locator_output_path_present
        and projected_gates.get("target_locator_write_allowed")
        is target_locator_write_allowed
        and isinstance(projected_locator_written, bool)
        and isinstance(projected_locator_private_mode_confirmed, bool)
        and isinstance(projected_locator_write_attempted, bool)
        and projection.get("target_locator_schema_version") == TARGET_LOCATOR_SCHEMA_VERSION
        and projection.get("target_locator_consumption_supported") is True
        and projected_locator_private_mode_confirmed is projected_locator_written
        and (not projected_locator_written or projected_locator_requested is True)
        and (not projected_locator_written or projected_locator_write_attempted is True)
        and (target_locator_requested or projected_locator_write_attempted is False)
        and (
            projection.get("reason_code") != "github_url_live_target_found"
            or projected_locator_requested is not True
            or (
                projected_locator_written is True
                and projected_locator_write_attempted is True
            )
        )
    )
    search_contract_projection = {
        "child_report_available": projection.get("stdout_parsed_as_json") is True,
        "child_schema_valid": projection.get("schema_version") == SEARCH_SCHEMA_VERSION,
        "child_mode_is_search": projection.get("mode") == "search",
        "child_returncode_consistent": (
            (child_runner_returncode == 0 and projection.get("status") == "pass")
            or (child_runner_returncode not in {None, 0} and projection.get("status") == "blocked")
        ),
        "history_request_count_within_bound": (
            isinstance(history_request_count, int)
            and not isinstance(history_request_count, bool)
            and 0 <= history_request_count <= 1
        ),
        "terminal_read_contract_satisfied": terminal_read_contract_satisfied,
        "gate_contract_satisfied": gate_contract_satisfied,
        "authority_contract_satisfied": authority_contract_satisfied,
        "target_locator_contract_satisfied": locator_contract_satisfied,
        "write_publish_side_effects_absent": all(
            projected_side_effects.get(key) is False
            for key in _SEARCH_SIDE_EFFECT_KEYS
            if key != "target_locator_write_attempted"
        ),
        "redactions_complete": all(projected_redactions.get(key) is True for key in _SEARCH_REDACTION_KEYS),
        "raw_values_not_printed": all(projected_raw_values.get(key) is False for key in _SEARCH_RAW_VALUE_KEYS),
    }
    search_contract_projection["reviewable"] = projection_ready and all(search_contract_projection.values())
    redaction_audit = {
        "runtime_env_values_printed": False,
        "runtime_env_file_contents_printed": False,
        "runtime_env_file_path_printed": False,
        "raw_source_value_printed": False,
        "confirm_token_value_printed": False,
        "child_stdout_printed": False,
        "child_stderr_printed": False,
        "target_locator_path_printed": False,
        "target_locator_basename_printed": False,
        "target_locator_content_printed": False,
        "target_locator_raw_target_values_printed": False,
    }
    return {
        "schema_version": SEARCH_WRAPPER_SCHEMA_VERSION,
        "status": status,
        "reason_code": reason_code,
        "mode": "search",
        "target_scope": {
            "exact_single_public_username_required": True,
            "target_count": target_count,
            "target_fingerprint": target_fingerprint,
            "raw_source_value_printed": False,
            "direct_chat_id_allowed": False,
            "direct_registry_id_allowed": False,
            "broad_target_allowed": False,
        },
        "bounded_read": {
            "requested_max_messages": requested_max_messages,
            "hard_max_messages": MAX_MESSAGES_HARD_LIMIT,
            "history_request_maximum": 1,
            "unbounded_history_allowed": False,
        },
        "target_locator_requested": target_locator_requested,
        "target_locator_written": projected_locator_written is True,
        "target_locator_schema_version": TARGET_LOCATOR_SCHEMA_VERSION,
        "target_locator_private_mode_confirmed": projected_locator_private_mode_confirmed is True,
        "target_locator_consumption_supported": True,
        "target_locator_path_omitted": True,
        "target_locator_basename_omitted": True,
        "target_locator_content_omitted": True,
        "target_locator_raw_target_values_omitted": True,
        "runtime_env_overlay": overlay_report
        or {
            "schema_version": "collector_runtime_env_overlay_v1",
            "status": "not_attempted",
            "reason_code": "runtime_env_overlay_not_attempted",
            "source_runtime_env_allows_extra_keys": True,
            "source_unknown_keys_ignored": True,
            "source_forbidden_keys_ignored": True,
            "child_overlay_only": True,
            "child_overlay_allowed_keys": list(COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS),
            "child_overlay_keys": [],
            "child_overlay_rejects_unknown_keys": True,
            "child_overlay_rejects_forbidden_keys": True,
            "runtime_env_values_printed": False,
            "runtime_env_file_contents_printed": False,
            "runtime_env_file_path_printed": False,
        },
        "child_command": {
            "uses_sys_executable_for_child": True,
            "child_runner_path": CHILD_RUNNER_PATH,
            "wrapper_runner_path": WRAPPER_RUNNER_PATH,
            "command_tokens": list(redacted_child_command_tokens),
            "redacted_command_tokens": True,
            "source_value_placeholder": SOURCE_VALUE_PLACEHOLDER,
            "confirm_token_placeholder": SEARCH_CONFIRM_TOKEN_PLACEHOLDER,
            "target_locator_path_placeholder": TARGET_LOCATOR_PATH_PLACEHOLDER,
            "confirm_token_value_printed": False,
            "forbidden_flags_absent": [
                "--allow-database-write",
                "--allow-source-message-write",
                "--allow-source-version-write",
                "--allow-source-outbox-write",
                "--allow-source-outbox-publish",
                "--allow-redis-publish",
                "--target-message-id",
                "--registry-id-suffix",
                "--max-targets",
            ],
        },
        "actual_attempted_operations": {
            "runtime_env_read_attempted": runtime_env_read_attempted,
            "child_runner_invoked": child_runner_invoked,
            "child_runner_returncode": child_runner_returncode,
            "live_telegram_read_attempted_by_wrapper": False,
            "database_write_attempted_by_wrapper": False,
            "redis_attempted_by_wrapper": False,
            "provider_or_openai_attempted_by_wrapper": False,
            "telegram_send_or_edit_attempted": False,
        },
        "child_report": {
            "stdout_parsed_as_json": projection.get("stdout_parsed_as_json") is True,
            "status": projection.get("status"),
            "reason_code": projection.get("reason_code"),
            "stdout_printed": False,
            "stderr_printed": False,
        },
        "search_child_report_projection": projection,
        "search_contract_projection": search_contract_projection,
        "redaction_audit": redaction_audit,
        "completion_claims": {
            "BOUNDED_GITHUB_LIVE_SEARCH_PROJECTION_READY": projection_ready,
            "BOUNDED_GITHUB_LIVE_SEARCH_REVIEWABLE": search_contract_projection["reviewable"],
            "LIVE_COLLECTOR_1_CHANNEL_CLOSED": False,
            "LIVE_COLLECTOR_3_CHANNEL_CLOSED": False,
            "PRODUCTION_ROLLOUT_CLOSED": False,
            "PRODUCT_COMPLETE_CLOSED": False,
        },
    }


def _project_search_child_report(child_report: Mapping[str, Any] | None) -> dict[str, Any]:
    parsed = child_report is not None
    report = child_report if isinstance(child_report, Mapping) else {}
    projection: dict[str, Any] = {
        "stdout_parsed_as_json": parsed,
        "schema_version": SEARCH_SCHEMA_VERSION if report.get("schema_version") == SEARCH_SCHEMA_VERSION else None,
        "status": _safe_search_status(report.get("status")),
        "reason_code": _safe_search_reason_code(report.get("reason_code")),
        "mode": "search" if report.get("mode") == "search" else None,
        "target_fingerprint": _safe_fingerprint(report.get("target_fingerprint")),
        "registry_target_fingerprint": _safe_fingerprint(report.get("registry_target_fingerprint")),
        "selected_message_fingerprint": _safe_fingerprint(report.get("selected_message_fingerprint")),
        "target_locator_requested": _safe_bool(report.get("target_locator_requested")),
        "target_locator_written": _safe_bool(report.get("target_locator_written")),
        "target_locator_schema_version": (
            TARGET_LOCATOR_SCHEMA_VERSION
            if report.get("target_locator_schema_version") == TARGET_LOCATOR_SCHEMA_VERSION
            else None
        ),
        "target_locator_private_mode_confirmed": _safe_bool(
            report.get("target_locator_private_mode_confirmed")
        ),
        "target_locator_consumption_supported": _safe_bool(
            report.get("target_locator_consumption_supported")
        ),
        "github_url_present": _safe_bool(report.get("github_url_present")),
        "selected_match_source_buckets": _project_bool_mapping(
            _mapping_child(report, "selected_match_source_buckets"),
            _SEARCH_SELECTED_MATCH_SOURCE_KEYS,
        ),
        "telegram_read_attempted": _safe_bool(report.get("telegram_read_attempted")),
        "telegram_read_called": _safe_bool(report.get("telegram_read_called")),
        "telegram_read_succeeded": _safe_bool(report.get("telegram_read_succeeded")),
        "tdlib": _project_bool_mapping(_mapping_child(report, "tdlib"), _SEARCH_TDLIB_KEYS),
        "safe_failure_bucket": _safe_search_failure_bucket(report.get("safe_failure_bucket")),
        "gates": _project_bool_mapping(_mapping_child(report, "gates"), _SEARCH_GATE_KEYS),
        "authority": _project_bool_mapping(_mapping_child(report, "authority"), _SEARCH_AUTHORITY_KEYS),
        "side_effects": _project_bool_mapping(_mapping_child(report, "side_effects"), _SEARCH_SIDE_EFFECT_KEYS),
        "redactions_applied": _project_bool_mapping(
            _mapping_child(report, "redactions_applied"),
            _SEARCH_REDACTION_KEYS,
        ),
        "raw_values_printed": _project_bool_mapping(
            _mapping_child(report, "raw_values_printed"),
            _SEARCH_RAW_VALUE_KEYS,
        ),
        "rollback_close_readback": _project_bool_mapping(
            _mapping_child(report, "rollback_close_readback"),
            _SEARCH_ROLLBACK_CLOSE_KEYS,
        ),
    }
    for key in _SEARCH_COUNT_KEYS:
        projection[key] = _safe_nonnegative_int(report.get(key))
    return projection


def _compact_child_report(child_runner_report: Mapping[str, Any] | None) -> dict[str, Any]:
    return {
        "stdout_parsed_as_json": child_runner_report is not None,
        "status": None if child_runner_report is None else _safe_report_string(child_runner_report.get("status")),
        "reason_code": None if child_runner_report is None else _safe_report_string(child_runner_report.get("reason_code")),
        "stdout_printed": False,
        "stderr_printed": False,
    }


def _project_child_report(child_report: Mapping[str, Any] | None) -> dict[str, Any]:
    parsed = child_report is not None
    report = child_report if isinstance(child_report, Mapping) else {}
    duplicate_noop_proof = _mapping_child(report, "duplicate_noop_proof")
    projection: dict[str, Any] = {
        "stdout_parsed_as_json": parsed,
        "status": _safe_report_string(report.get("status")),
        "reason_code": _safe_report_string(report.get("reason_code")),
        "schema_version": _safe_report_string(report.get("schema_version")),
        "runner_name": _safe_report_string(report.get("runner_name")),
        "mode": _safe_report_string(report.get("mode")),
        "rollout_scope": _safe_report_string(report.get("rollout_scope")),
        "target_count": _safe_nonnegative_int(report.get("target_count")),
        "max_targets": _safe_nonnegative_int(report.get("max_targets")),
        "target_locator_present": _safe_bool(report.get("target_locator_present")),
        "target_locator_consumption_supported": _safe_bool(
            report.get("target_locator_consumption_supported")
        ),
        "history_window_attempts": _safe_nonnegative_int(report.get("history_window_attempts")),
        "history_read_failure_cause_bucket": _safe_search_failure_bucket(
            report.get("history_read_failure_cause_bucket")
        ),
        "exact_message_read_attempted": _safe_bool(report.get("exact_message_read_attempted")),
        "exact_message_read_succeeded": _safe_bool(report.get("exact_message_read_succeeded")),
        "exact_message_read_failure_cause_bucket": _safe_exact_message_read_failure_bucket(
            report.get("exact_message_read_failure_cause_bucket")
        ),
        "telegram_read_succeeded": _safe_bool(report.get("telegram_read_succeeded")),
        "authority": _project_bool_mapping(_mapping_child(report, "authority"), _AUTHORITY_PROJECTION_KEYS),
        "gates": _project_bool_mapping(_mapping_child(report, "gates"), _GATE_PROJECTION_KEYS),
        "bounded_counts": _project_count_mapping(_mapping_child(report, "bounded_counts"), _BOUNDED_COUNT_PROJECTION_KEYS),
        "readback": _project_count_mapping(_mapping_child(report, "readback"), _READBACK_PROJECTION_KEYS),
        "duplicate_noop_proof": {
            "proved_count": _safe_nonnegative_int(duplicate_noop_proof.get("proved_count")),
            "without_second_telegram_read": _safe_bool(duplicate_noop_proof.get("without_second_telegram_read")),
        },
        "target_fingerprints": _safe_fingerprint_list(report.get("target_fingerprints")),
        "per_channel_results": _project_per_channel_results(report.get("per_channel_results")),
        "source_message_fingerprints": _safe_fingerprint_list(report.get("source_message_fingerprints")),
        "source_outbox_event_fingerprints": _safe_fingerprint_list(report.get("source_outbox_event_fingerprints")),
        "exact_channel_target_fingerprint": _safe_fingerprint(report.get("exact_channel_target_fingerprint")),
        "registry_target_fingerprint": _safe_fingerprint(report.get("registry_target_fingerprint")),
        "raw_values_printed": _project_bool_mapping(_mapping_child(report, "raw_values_printed"), _RAW_VALUES_PRINTED_KEYS),
        "redactions_applied": _project_bool_mapping(_mapping_child(report, "redactions_applied"), _REDACTIONS_APPLIED_KEYS),
        "rollback_stop_readback": {
            "exact_runner_completed": _safe_bool(_mapping_child(report, "rollback_stop_readback").get("exact_runner_completed")),
        },
        "side_effects": _project_bool_mapping(_mapping_child(report, "side_effects"), _SIDE_EFFECT_PROJECTION_KEYS),
        "ok": _safe_bool(report.get("ok")),
        "error_code": _safe_report_string(report.get("error_code")),
        "error_class": _safe_report_string(report.get("error_class")),
        "duplicate_noop_proof_count": _safe_nonnegative_int(report.get("duplicate_noop_proof_count")),
    }
    for key in _ATTEMPT_PROJECTION_KEYS:
        projection[key] = _safe_bool(report.get(key))
    projection["messages_requested"] = _safe_nonnegative_int(report.get("messages_requested"))
    projection["messages_seen"] = _safe_nonnegative_int(report.get("messages_seen"))
    return projection


def _build_f1_live_readback_closure(
    projection: Mapping[str, Any],
    *,
    child_report: Mapping[str, Any],
    child_runner_returncode: int | None,
    redaction_audit: Mapping[str, Any],
    wrapper_reason_code: str,
    wrapper_status: str,
) -> dict[str, bool]:
    closure = _build_readback_closure_evidence(
        projection,
        child_report=child_report,
        child_runner_returncode=child_runner_returncode,
        redaction_audit=redaction_audit,
        wrapper_reason_code=wrapper_reason_code,
        wrapper_status=wrapper_status,
    )
    legacy_keys = (
        "child_report_available",
        "child_runner_returncode_zero",
        "wrapper_child_execution_passed",
        "exact_child_runner_passed",
        "live_telegram_read_attempted",
        "telegram_read_called",
        "database_write_attempted",
        "source_message_write_attempted",
        "source_version_write_attempted",
        "source_outbox_write_attempted",
        "source_outbox_publish_disabled",
        "redis_publish_disabled",
        "telegram_send_disabled",
        "provider_calls_disabled",
        "docker_systemd_alembic_disabled",
        "source_current_readback_present",
        "source_version_readback_present",
        "source_outbox_readback_present",
        "duplicate_noop_proof_present",
        "duplicate_noop_without_second_telegram_read",
        "raw_values_not_printed",
        "runtime_values_not_printed",
    )
    legacy_closure = {key: closure[key] for key in legacy_keys}
    legacy_closure["safe_to_review_for_f1_live_read_closure"] = all(legacy_closure.values())
    return legacy_closure


def _build_source_truth_readback_closure(
    projection: Mapping[str, Any],
    *,
    child_report: Mapping[str, Any],
    child_runner_returncode: int | None,
    redaction_audit: Mapping[str, Any],
    wrapper_reason_code: str,
    wrapper_status: str,
) -> dict[str, bool]:
    evidence = _build_readback_closure_evidence(
        projection,
        child_report=child_report,
        child_runner_returncode=child_runner_returncode,
        redaction_audit=redaction_audit,
        wrapper_reason_code=wrapper_reason_code,
        wrapper_status=wrapper_status,
    )
    closure = {
        "child_report_available": evidence["child_report_available"],
        "wrapper_child_execution_passed": evidence["wrapper_child_execution_passed"],
        "exact_child_runner_passed": evidence["exact_child_runner_passed"],
        "live_telegram_read_attempted": evidence["live_telegram_read_attempted"],
        "telegram_read_called": evidence["telegram_read_called"],
        "messages_seen_present": evidence["messages_seen_present"],
        "source_current_readback_present": evidence["source_current_readback_present"],
        "source_version_readback_present": evidence["source_version_readback_present"],
        "source_created_events_readback_present": evidence["source_created_events_readback_present"],
        "source_outbox_events_readback_present": evidence["source_outbox_events_readback_present"],
        "source_outbox_publish_disabled": evidence["source_outbox_publish_disabled"],
        "redis_publish_disabled": evidence["redis_publish_disabled"],
        "telegram_send_disabled": evidence["telegram_send_disabled"],
        "provider_calls_disabled": evidence["provider_calls_disabled"],
        "docker_systemd_alembic_disabled": evidence["docker_systemd_alembic_disabled"],
        "raw_values_not_printed": evidence["raw_values_not_printed"],
        "runtime_values_not_printed": evidence["runtime_values_not_printed"],
    }
    closure["durable_readback_present"] = all(closure.values())
    return closure


def _build_f1_duplicate_noop_readback_closure(
    projection: Mapping[str, Any],
    *,
    source_truth_readback_closure: Mapping[str, Any],
) -> dict[str, bool]:
    duplicate_noop_proof = _mapping_child(projection, "duplicate_noop_proof")
    duplicate_noop_count = duplicate_noop_proof.get("proved_count")
    if duplicate_noop_count is None:
        duplicate_noop_count = projection.get("duplicate_noop_proof_count")
    closure = {
        "one_channel_or_legacy_child_report": _one_channel_or_legacy_child_report(projection),
        "source_truth_durable_readback_present": source_truth_readback_closure.get("durable_readback_present") is True,
        "duplicate_noop_proof_present": _count_at_least(duplicate_noop_count, 1),
        "duplicate_noop_without_second_telegram_read": (
            duplicate_noop_proof.get("without_second_telegram_read") is True
        ),
    }
    closure["closed"] = all(closure.values())
    return closure


def _build_f1_fresh_write_readback_closure(
    projection: Mapping[str, Any],
    *,
    source_truth_readback_closure: Mapping[str, Any],
) -> dict[str, bool]:
    closure = {
        "one_channel_or_legacy_child_report": _one_channel_or_legacy_child_report(projection),
        "source_truth_durable_readback_present": source_truth_readback_closure.get("durable_readback_present") is True,
        "database_write_attempted": projection.get("database_write_attempted") is True,
        "source_message_write_attempted": projection.get("source_message_write_attempted") is True,
        "source_version_write_attempted": projection.get("source_version_write_attempted") is True,
        "source_outbox_write_attempted": projection.get("source_outbox_write_attempted") is True,
    }
    closure["closed"] = all(closure.values())
    return closure


def _build_f2_three_channel_readback_closure(
    projection: Mapping[str, Any],
    *,
    child_report: Mapping[str, Any],
    child_runner_returncode: int | None,
    redaction_audit: Mapping[str, Any],
    wrapper_reason_code: str,
    wrapper_status: str,
) -> dict[str, bool]:
    evidence = _build_readback_closure_evidence(
        projection,
        child_report=child_report,
        child_runner_returncode=child_runner_returncode,
        redaction_audit=redaction_audit,
        wrapper_reason_code=wrapper_reason_code,
        wrapper_status=wrapper_status,
    )
    readback = _mapping_child(projection, "readback")
    bounded_counts = _mapping_child(projection, "bounded_counts")
    duplicate_noop_proof = _mapping_child(projection, "duplicate_noop_proof")
    per_channel_results = projection.get("per_channel_results")
    safe_per_channel_results = per_channel_results if isinstance(per_channel_results, Sequence) else ()
    safe_per_channel_results = (
        ()
        if isinstance(safe_per_channel_results, (str, bytes, bytearray))
        else tuple(item for item in safe_per_channel_results if isinstance(item, Mapping))
    )
    per_channel_readbacks_present = all(
        _per_channel_source_truth_present(item) for item in safe_per_channel_results
    )
    aggregate_duplicate_noop_sufficient = (
        _count_at_least(duplicate_noop_proof.get("proved_count"), THREE_CHANNEL_TARGET_COUNT)
        and duplicate_noop_proof.get("without_second_telegram_read") is True
    )
    aggregate_fresh_write_sufficient = (
        evidence["database_write_attempted"]
        and evidence["source_message_write_attempted"]
        and evidence["source_version_write_attempted"]
        and evidence["source_outbox_write_attempted"]
        and _count_at_least(bounded_counts.get("source_messages_created"), THREE_CHANNEL_TARGET_COUNT)
        and _count_at_least(bounded_counts.get("source_versions_created"), THREE_CHANNEL_TARGET_COUNT)
        and _count_at_least(bounded_counts.get("source_created_events"), THREE_CHANNEL_TARGET_COUNT)
    )
    closure = {
        "child_report_available": evidence["child_report_available"],
        "wrapper_child_execution_passed": evidence["wrapper_child_execution_passed"],
        "exact_child_runner_passed": evidence["exact_child_runner_passed"],
        "target_count_is_three": projection.get("target_count") == THREE_CHANNEL_TARGET_COUNT,
        "target_fingerprint_count_is_three": len(projection.get("target_fingerprints") or ()) == THREE_CHANNEL_TARGET_COUNT,
        "per_channel_result_count_is_three": len(safe_per_channel_results) == THREE_CHANNEL_TARGET_COUNT,
        "per_channel_status_passed": all(item.get("status") == "pass" for item in safe_per_channel_results),
        "per_channel_messages_seen_present": all(
            _count_at_least(item.get("messages_seen"), 1) for item in safe_per_channel_results
        ),
        "per_channel_readbacks_present": per_channel_readbacks_present,
        "aggregate_source_current_readback_present": _count_at_least(
            readback.get("source_current_found_count"), THREE_CHANNEL_TARGET_COUNT
        ),
        "aggregate_source_version_readback_present": _count_at_least(
            readback.get("source_version_rows_count"), THREE_CHANNEL_TARGET_COUNT
        ),
        "aggregate_source_created_events_readback_present": _count_at_least(
            readback.get("source_created_events_count"), THREE_CHANNEL_TARGET_COUNT
        ),
        "aggregate_source_outbox_events_readback_present": _count_at_least(
            readback.get("source_outbox_events_count"), THREE_CHANNEL_TARGET_COUNT
        ),
        "aggregate_duplicate_noop_or_fresh_write_sufficient": (
            aggregate_duplicate_noop_sufficient or aggregate_fresh_write_sufficient
        ),
        "source_outbox_publish_disabled": evidence["source_outbox_publish_disabled"],
        "redis_publish_disabled": evidence["redis_publish_disabled"],
        "telegram_send_disabled": evidence["telegram_send_disabled"],
        "provider_calls_disabled": evidence["provider_calls_disabled"],
        "docker_systemd_alembic_disabled": evidence["docker_systemd_alembic_disabled"],
        "raw_values_not_printed": evidence["raw_values_not_printed"],
        "runtime_values_not_printed": evidence["runtime_values_not_printed"],
    }
    closure["closed"] = all(closure.values())
    return closure


def _build_readback_closure_evidence(
    projection: Mapping[str, Any],
    *,
    child_report: Mapping[str, Any],
    child_runner_returncode: int | None,
    redaction_audit: Mapping[str, Any],
    wrapper_reason_code: str,
    wrapper_status: str,
) -> dict[str, bool]:
    authority = _mapping_child(projection, "authority")
    gates = _mapping_child(projection, "gates")
    readback = _mapping_child(projection, "readback")
    duplicate_noop_proof = _mapping_child(projection, "duplicate_noop_proof")
    raw_values_printed = _mapping_child(projection, "raw_values_printed")
    side_effects = _mapping_child(projection, "side_effects")

    child_report_available = projection.get("stdout_parsed_as_json") is True
    child_runner_returncode_zero = child_runner_returncode == 0
    wrapper_child_execution_passed = (
        wrapper_status == "pass"
        and wrapper_reason_code == "child_bounded_runner_passed"
        and child_runner_returncode_zero
    )
    exact_child_runner_passed = (
        wrapper_child_execution_passed
        and projection.get("status") == "pass"
        and projection.get("reason_code") == "ok"
        and projection.get("ok") is True
    )
    live_telegram_read_attempted = authority.get("live_telegram_read_attempted") is True
    telegram_read_called = projection.get("telegram_read_called") is True
    database_write_attempted = projection.get("database_write_attempted") is True
    source_message_write_attempted = projection.get("source_message_write_attempted") is True
    source_version_write_attempted = projection.get("source_version_write_attempted") is True
    source_outbox_write_attempted = projection.get("source_outbox_write_attempted") is True
    source_outbox_publish_disabled = (
        projection.get("source_outbox_publish_attempted") is False
        and gates.get("source_outbox_publish_allowed") is False
    )
    redis_publish_disabled = (
        projection.get("redis_publish_attempted") is False and gates.get("redis_publish_allowed") is False
    )
    telegram_send_disabled = (
        authority.get("telegram_send_attempted") is False
        and side_effects.get("telegram_send_called") is False
        and side_effects.get("telegram_edit_called") is False
    )
    provider_calls_disabled = all(
        authority.get(key) is False for key in ("openai_attempted", "github_attempted", "x_attempted", "web_attempted")
    )
    docker_systemd_alembic_disabled = (
        authority.get("docker_or_systemd_called") is False and authority.get("alembic_or_ddl_ran") is False
    )
    source_current_readback_present = _count_at_least(readback.get("source_current_found_count"), 1)
    source_version_readback_present = _count_at_least(readback.get("source_version_rows_count"), 1)
    source_created_events_readback_present = _count_at_least(readback.get("source_created_events_count"), 1)
    source_outbox_events_readback_present = _count_at_least(readback.get("source_outbox_events_count"), 1)
    source_outbox_readback_present = _count_at_least(
        readback.get("source_outbox_events_count"), 1
    ) and _count_at_least(readback.get("source_created_events_count"), 1)
    duplicate_noop_count = duplicate_noop_proof.get("proved_count")
    if duplicate_noop_count is None:
        duplicate_noop_count = projection.get("duplicate_noop_proof_count")
    duplicate_noop_proof_present = _count_at_least(duplicate_noop_count, 1)
    duplicate_noop_without_second_telegram_read = duplicate_noop_proof.get("without_second_telegram_read") is True
    raw_values_not_printed = (
        _all_required_false(raw_values_printed, _RAW_VALUES_PRINTED_KEYS)
        and child_report.get("stdout_printed") is False
        and child_report.get("stderr_printed") is False
        and redaction_audit.get("child_stdout_printed") is False
        and redaction_audit.get("child_stderr_printed") is False
    )
    runtime_values_not_printed = (
        raw_values_printed.get("runtime_value") is False
        and redaction_audit.get("runtime_env_values_printed") is False
        and redaction_audit.get("runtime_env_file_contents_printed") is False
        and redaction_audit.get("runtime_env_file_path_printed") is False
    )

    return {
        "child_report_available": child_report_available,
        "child_runner_returncode_zero": child_runner_returncode_zero,
        "wrapper_child_execution_passed": wrapper_child_execution_passed,
        "exact_child_runner_passed": exact_child_runner_passed,
        "live_telegram_read_attempted": live_telegram_read_attempted,
        "telegram_read_called": telegram_read_called,
        "messages_seen_present": _count_at_least(projection.get("messages_seen"), 1),
        "database_write_attempted": database_write_attempted,
        "source_message_write_attempted": source_message_write_attempted,
        "source_version_write_attempted": source_version_write_attempted,
        "source_outbox_write_attempted": source_outbox_write_attempted,
        "source_outbox_publish_disabled": source_outbox_publish_disabled,
        "redis_publish_disabled": redis_publish_disabled,
        "telegram_send_disabled": telegram_send_disabled,
        "provider_calls_disabled": provider_calls_disabled,
        "docker_systemd_alembic_disabled": docker_systemd_alembic_disabled,
        "source_current_readback_present": source_current_readback_present,
        "source_version_readback_present": source_version_readback_present,
        "source_created_events_readback_present": source_created_events_readback_present,
        "source_outbox_events_readback_present": source_outbox_events_readback_present,
        "source_outbox_readback_present": source_outbox_readback_present,
        "duplicate_noop_proof_present": duplicate_noop_proof_present,
        "duplicate_noop_without_second_telegram_read": duplicate_noop_without_second_telegram_read,
        "raw_values_not_printed": raw_values_not_printed,
        "runtime_values_not_printed": runtime_values_not_printed,
    }


def _child_command_tokens(
    *,
    source_values: Sequence[str],
    target_locator_path: object | None,
    max_messages: int,
) -> tuple[str, ...]:
    confirm_token = (
        THREE_CHANNEL_EXECUTE_CONFIRM_TOKEN
        if len(source_values) == THREE_CHANNEL_TARGET_COUNT
        else EXECUTE_CONFIRM_TOKEN
    )
    source_tokens: list[str] = []
    if target_locator_path is not None:
        source_tokens.extend(("--target-locator-path", str(target_locator_path)))
    else:
        for source_value in source_values:
            source_tokens.extend(("--source-value", source_value))
    return (
        sys.executable,
        CHILD_RUNNER_PATH,
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
        SOURCE_KIND_PUBLIC_USERNAME,
        *source_tokens,
        "--max-messages",
        str(max_messages),
        "--confirm-token",
        confirm_token,
    )


def _search_child_command_tokens(
    *,
    source_value: str,
    max_messages: int,
    target_locator_output_path: object | None,
    allow_target_locator_write: bool,
) -> tuple[str, ...]:
    locator_tokens: list[str] = []
    if target_locator_output_path is not None:
        locator_tokens.extend(("--target-locator-output-path", str(target_locator_output_path)))
    if allow_target_locator_write:
        locator_tokens.append("--allow-target-locator-write")
    return (
        sys.executable,
        CHILD_RUNNER_PATH,
        "--mode",
        "search",
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-telegram-read",
        "--source-kind",
        SOURCE_KIND_PUBLIC_USERNAME,
        "--source-value",
        source_value,
        *locator_tokens,
        "--max-messages",
        str(max_messages),
        "--confirm-token",
        SEARCH_CONFIRM_TOKEN,
    )


def _redacted_child_plan_command_tokens(
    *,
    target_count: int,
    max_messages: int,
    target_locator_present: bool,
) -> tuple[str, ...]:
    source_tokens: list[str] = []
    if target_locator_present:
        source_tokens.extend(("--target-locator-path", TARGET_LOCATOR_PATH_PLACEHOLDER))
    else:
        for _ in range(target_count):
            source_tokens.extend(("--source-value", SOURCE_VALUE_PLACEHOLDER))
    return (
        "sys.executable",
        CHILD_RUNNER_PATH,
        "--mode",
        "plan",
        "--allow-runtime-config",
        "--allow-database-read",
        "--source-kind",
        SOURCE_KIND_PUBLIC_USERNAME,
        *source_tokens,
        "--max-messages",
        str(max_messages),
    )


def _redacted_child_execute_command_tokens(
    *,
    target_count: int,
    max_messages: int,
    target_locator_present: bool,
) -> tuple[str, ...]:
    confirm_token_placeholder = (
        "THREE_CHANNEL_EXECUTE_CONFIRM_TOKEN"
        if target_count == THREE_CHANNEL_TARGET_COUNT
        else "EXECUTE_CONFIRM_TOKEN"
    )
    source_tokens: list[str] = []
    if target_locator_present:
        source_tokens.extend(("--target-locator-path", TARGET_LOCATOR_PATH_PLACEHOLDER))
    else:
        for _ in range(target_count):
            source_tokens.extend(("--source-value", SOURCE_VALUE_PLACEHOLDER))
    return (
        "sys.executable",
        CHILD_RUNNER_PATH,
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
        SOURCE_KIND_PUBLIC_USERNAME,
        *source_tokens,
        "--max-messages",
        str(max_messages),
        "--confirm-token",
        confirm_token_placeholder,
    )


def _redacted_search_child_command_tokens(
    *,
    max_messages: int,
    target_locator_output_path_present: bool,
    allow_target_locator_write: bool,
) -> tuple[str, ...]:
    locator_tokens: list[str] = []
    if target_locator_output_path_present:
        locator_tokens.extend(
            ("--target-locator-output-path", TARGET_LOCATOR_PATH_PLACEHOLDER)
        )
    if allow_target_locator_write:
        locator_tokens.append("--allow-target-locator-write")
    return (
        "sys.executable",
        CHILD_RUNNER_PATH,
        "--mode",
        "search",
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-telegram-read",
        "--source-kind",
        SOURCE_KIND_PUBLIC_USERNAME,
        "--source-value",
        SOURCE_VALUE_PLACEHOLDER,
        *locator_tokens,
        "--max-messages",
        str(max_messages),
        "--confirm-token",
        SEARCH_CONFIRM_TOKEN_PLACEHOLDER,
    )


def _normalize_search_public_username_target(
    values: Sequence[object | None],
) -> tuple[str | None, str | None]:
    if len(values) != 1:
        return None, "search_requires_exactly_one_target"
    normalized, error = _normalize_public_username(values[0])
    if normalized is None:
        return None, "search_requires_exactly_one_target"
    return normalized, error


def _search_max_messages_error(value: object | None) -> str | None:
    if value is None:
        return "search_max_messages_required"
    if not isinstance(value, int) or isinstance(value, bool):
        return "search_max_messages_out_of_bounds"
    if value < 1 or value > MAX_MESSAGES_HARD_LIMIT:
        return "search_max_messages_out_of_bounds"
    return None


def _normalize_public_username_targets(values: Sequence[object | None]) -> tuple[tuple[str, ...], str | None]:
    if not values:
        return (), "exact_source_value_required"
    normalized_values: list[str] = []
    for value in values:
        normalized, error = _normalize_public_username(value)
        if normalized is not None:
            normalized_values.append(normalized)
        if error is not None:
            return tuple(normalized_values), error
    if len(normalized_values) not in {1, THREE_CHANNEL_TARGET_COUNT}:
        return tuple(normalized_values), "target_count_must_equal_three"
    if len(set(normalized_values)) != len(normalized_values):
        return tuple(normalized_values), "target_duplicate"
    return tuple(normalized_values), None


def _normalize_public_username(value: object | None) -> tuple[str | None, str | None]:
    if not isinstance(value, str):
        return None, "exact_source_value_required"
    normalized = value.strip().lstrip("@").strip().lower()
    if not normalized:
        return None, "exact_source_value_required"
    if normalized in {"*", "all", "all_channels", "all channels"}:
        return normalized, "broad_target_not_allowed"
    if _looks_like_direct_chat_id(normalized):
        return normalized, "direct_chat_id_target_not_allowed"
    if _looks_like_registry_id(normalized):
        return normalized, "direct_registry_id_target_not_allowed"
    if not _valid_public_username(normalized):
        return normalized, "exact_public_username_required"
    return normalized, None


def _max_messages_error(value: object | None) -> str | None:
    if value is None:
        return "requested_max_messages_required"
    if not isinstance(value, int) or isinstance(value, bool):
        return "requested_max_messages_out_of_bounds"
    if value < 1 or value > MAX_MESSAGES_HARD_LIMIT:
        return "requested_max_messages_out_of_bounds"
    return None


def _valid_public_username(value: str) -> bool:
    if len(value) < 5 or len(value) > 32:
        return False
    first = value[0]
    return first.isalpha() and all(ch.isalnum() or ch == "_" for ch in value)


def _looks_like_direct_chat_id(value: str) -> bool:
    candidate = value.removeprefix("+").removeprefix("-")
    return bool(candidate) and candidate.isdigit()


def _looks_like_registry_id(value: str) -> bool:
    try:
        UUID(value)
    except ValueError:
        return False
    return True


def _fingerprint(kind: str, value: object | None) -> str | None:
    if value is None:
        return None
    digest = sha256(f"{kind}:{value}".encode("utf-8")).hexdigest()
    return f"sha256:{digest[:16]}"


def _parse_child_report(stdout: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(stdout)
    except (TypeError, ValueError):
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def _mapping_child(mapping: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = mapping.get(key)
    if isinstance(value, Mapping):
        return value
    return {}


def _project_bool_mapping(mapping: Mapping[str, Any], keys: Sequence[str]) -> dict[str, bool | None]:
    return {key: _safe_bool(mapping.get(key)) for key in keys}


def _project_count_mapping(mapping: Mapping[str, Any], keys: Sequence[str]) -> dict[str, int | None]:
    return {key: _safe_nonnegative_int(mapping.get(key)) for key in keys}


def _project_per_channel_results(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    projected: list[dict[str, Any]] = []
    for item in value[:THREE_CHANNEL_TARGET_COUNT]:
        if not isinstance(item, Mapping):
            continue
        duplicate_noop_proof = _mapping_child(item, "duplicate_noop_proof")
        projected.append(
            {
                "target_fingerprint": _safe_fingerprint(item.get("target_fingerprint")),
                "registry_target_fingerprint": _safe_fingerprint(item.get("registry_target_fingerprint")),
                "status": _safe_report_string(item.get("status")),
                "reason_code": _safe_report_string(item.get("reason_code")),
                "messages_requested": _safe_nonnegative_int(item.get("messages_requested")),
                "messages_seen": _safe_nonnegative_int(item.get("messages_seen")),
                "history_window_attempts": _safe_nonnegative_int(item.get("history_window_attempts")),
                "history_read_failure_cause_bucket": _safe_search_failure_bucket(
                    item.get("history_read_failure_cause_bucket")
                ),
                "exact_message_read_attempted": _safe_bool(item.get("exact_message_read_attempted")),
                "exact_message_read_succeeded": _safe_bool(item.get("exact_message_read_succeeded")),
                "exact_message_read_failure_cause_bucket": _safe_exact_message_read_failure_bucket(
                    item.get("exact_message_read_failure_cause_bucket")
                ),
                "bounded_counts": _project_count_mapping(
                    _mapping_child(item, "bounded_counts"),
                    _BOUNDED_COUNT_PROJECTION_KEYS,
                ),
                "readback": _project_count_mapping(_mapping_child(item, "readback"), _READBACK_PROJECTION_KEYS),
                "source_message_fingerprints": _safe_fingerprint_list(item.get("source_message_fingerprints")),
                "source_outbox_event_fingerprints": _safe_fingerprint_list(
                    item.get("source_outbox_event_fingerprints")
                ),
                "duplicate_noop_proof": {
                    "proved_count": _safe_nonnegative_int(duplicate_noop_proof.get("proved_count")),
                    "without_second_telegram_read": _safe_bool(
                        duplicate_noop_proof.get("without_second_telegram_read")
                    ),
                },
                "source_commit_durable": _safe_bool(item.get("source_commit_durable")),
            }
        )
    return projected


def _one_channel_or_legacy_child_report(projection: Mapping[str, Any]) -> bool:
    target_count = projection.get("target_count")
    return target_count is None or target_count == 1


def _per_channel_source_truth_present(channel_result: Mapping[str, Any]) -> bool:
    readback = _mapping_child(channel_result, "readback")
    return (
        channel_result.get("status") == "pass"
        and _count_at_least(channel_result.get("messages_seen"), 1)
        and _count_at_least(readback.get("source_current_found_count"), 1)
        and _count_at_least(readback.get("source_version_rows_count"), 1)
        and _count_at_least(readback.get("source_created_events_count"), 1)
        and _count_at_least(readback.get("source_outbox_events_count"), 1)
    )


def _safe_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _safe_nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    if value < 0:
        return None
    return value


def _safe_report_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped or len(stripped) > 128:
        return None
    if not all(char in _SAFE_STRING_CHARS for char in stripped):
        return "unsafe_string_redacted"
    return stripped


def _safe_search_status(value: object) -> str | None:
    if value in {"pass", "blocked"}:
        return str(value)
    return None


def _safe_search_reason_code(value: object) -> str | None:
    if isinstance(value, str) and value in _SEARCH_REASON_CODES:
        return value
    return None


def _safe_search_failure_bucket(value: object) -> str | None:
    if isinstance(value, str) and value in _SEARCH_FAILURE_BUCKETS:
        return value
    return None


def _safe_exact_message_read_failure_bucket(value: object) -> str | None:
    if isinstance(value, str) and value in _EXACT_MESSAGE_READ_FAILURE_BUCKETS:
        return value
    return None


def _safe_fingerprint(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped.startswith("sha256:"):
        return None
    digest = stripped.removeprefix("sha256:")
    if len(digest) < 8 or len(digest) > 64:
        return None
    if not all(char in "0123456789abcdef" for char in digest):
        return None
    return stripped


def _safe_fingerprint_list(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    projected = []
    for item in value:
        fingerprint = _safe_fingerprint(item)
        if fingerprint is not None:
            projected.append(fingerprint)
    return projected


def _count_at_least(value: object, minimum: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def _all_required_false(mapping: Mapping[str, Any], keys: Sequence[str]) -> bool:
    return all(mapping.get(key) is False for key in keys)


__all__ = [
    "CHILD_RUNNER_PATH",
    "CliArgumentError",
    "RUNTIME_ENV_FILE_PLACEHOLDER",
    "RunnerResult",
    "SCHEMA_VERSION",
    "SEARCH_CONFIRM_TOKEN_PLACEHOLDER",
    "SEARCH_WRAPPER_SCHEMA_VERSION",
    "SOURCE_VALUE_PLACEHOLDER",
    "TARGET_LOCATOR_PATH_PLACEHOLDER",
    "TARGET_LOCATOR_SCHEMA_VERSION",
    "WRAPPER_RUNNER_PATH",
    "build_parser",
    "main",
    "restricted_env_overlay_argument_error_report",
    "restricted_search_env_overlay_argument_error_report",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
