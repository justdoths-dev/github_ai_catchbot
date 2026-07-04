from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Sequence
from uuid import UUID


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.collector_telegram.bounded_history_ingest_runner import (
    EXECUTE_CONFIRM_TOKEN,
    MAX_MESSAGES_HARD_LIMIT,
    SOURCE_KIND_PUBLIC_USERNAME,
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

SubprocessRunner = Callable[..., subprocess.CompletedProcess[str]]


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
    parser.add_argument("--mode", choices=("plan", "execute"), default="plan")
    parser.add_argument("--runtime-env-file", default=None)
    parser.add_argument("--source-value", default=None)
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
    normalized_source, target_error = _normalize_public_username(args.source_value)
    max_messages = args.max_messages
    max_messages_error = _max_messages_error(max_messages)
    target_fingerprint = _fingerprint("source_value", normalized_source) if normalized_source else None

    if target_error is not None:
        report = _base_report(
            status="blocked",
            reason_code=target_error,
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
        )
        return RunnerResult(exit_code=1, report=report)
    if max_messages_error is not None:
        report = _base_report(
            status="blocked",
            reason_code=max_messages_error,
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
        )
        return RunnerResult(exit_code=1, report=report)
    if mode == "execute" and not bool(args.operator_approved):
        report = _base_report(
            status="blocked",
            reason_code="operator_approval_missing",
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
        )
        return RunnerResult(exit_code=1, report=report)
    if mode == "execute" and str(args.confirm_token or "").strip() != EXECUTE_CONFIRM_TOKEN:
        report = _base_report(
            status="blocked",
            reason_code="confirm_token_missing" if not str(args.confirm_token or "").strip() else "confirm_token_invalid",
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
        )
        return RunnerResult(exit_code=1, report=report)
    if not args.runtime_env_file:
        report = _base_report(
            status="blocked",
            reason_code="runtime_env_file_required",
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
        )
        return RunnerResult(exit_code=1, report=report)

    assert normalized_source is not None
    assert isinstance(max_messages, int)
    overlay_result = build_collector_runtime_env_overlay(str(args.runtime_env_file))
    if not overlay_result.ok:
        report = _base_report(
            status="blocked",
            reason_code=str(overlay_result.reason_code),
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
            runtime_env_read_attempted=True,
            overlay_report=overlay_result.to_sanitized_dict(),
        )
        return RunnerResult(exit_code=1, report=report)

    child_command = _child_command_tokens(source_value=normalized_source, max_messages=max_messages)
    if mode == "plan":
        report = _base_report(
            status="pass",
            reason_code="collector_runtime_env_overlay_plan_ready",
            mode=mode,
            requested_max_messages=max_messages,
            target_fingerprint=target_fingerprint,
            runtime_env_read_attempted=True,
            overlay_report=overlay_result.to_sanitized_dict(),
            redacted_child_command_tokens=_redacted_child_command_tokens(max_messages=max_messages),
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
        runtime_env_read_attempted=True,
        overlay_report=overlay_result.to_sanitized_dict(),
        redacted_child_command_tokens=_redacted_child_command_tokens(max_messages=max_messages),
        child_runner_invoked=True,
        child_runner_returncode=child_returncode,
        child_runner_report=child_report,
    )
    return RunnerResult(exit_code=0 if child_returncode == 0 else 1, report=report)


def restricted_env_overlay_argument_error_report(error_code: str) -> dict[str, Any]:
    return _base_report(
        status="blocked",
        reason_code=error_code,
        mode="unknown",
        requested_max_messages=None,
        target_fingerprint=None,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    subprocess_runner: SubprocessRunner | None = None,
) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(restricted_env_overlay_argument_error_report(str(exc))))
        return 1
    result = run(args, subprocess_runner=subprocess_runner)
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


def _base_report(
    *,
    status: str,
    reason_code: str,
    mode: str,
    requested_max_messages: int | None,
    target_fingerprint: str | None,
    runtime_env_read_attempted: bool = False,
    overlay_report: Mapping[str, Any] | None = None,
    redacted_child_command_tokens: Sequence[str] = (),
    child_runner_invoked: bool = False,
    child_runner_returncode: int | None = None,
    child_runner_report: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "reason_code": reason_code,
        "mode": mode,
        "target_scope": {
            "exact_single_public_username_required": True,
            "target_fingerprint": target_fingerprint,
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
        "child_report": {
            "stdout_parsed_as_json": bool(child_runner_report),
            "status": None if child_runner_report is None else child_runner_report.get("status"),
            "reason_code": None if child_runner_report is None else child_runner_report.get("reason_code"),
            "stdout_printed": False,
            "stderr_printed": False,
        },
        "redaction_audit": {
            "runtime_env_values_printed": False,
            "runtime_env_file_contents_printed": False,
            "runtime_env_file_path_printed": False,
            "raw_source_value_printed": False,
            "child_stdout_printed": False,
            "child_stderr_printed": False,
            "token_or_secret_printed": False,
        },
        "completion_claims": {
            "F1_COLLECTOR_ONLY_RUNTIME_ENV_OVERLAY_PREFLIGHT_READY": status == "pass",
            "LIVE_TELEGRAM_READ_AUTHORITY_REMAINS_CLOSED_IN_THIS_TASK": not child_runner_invoked,
            "LIVE_COLLECTOR_1_CHANNEL_CLOSED": False,
            "production_complete": False,
            "production_rollout_complete": False,
        },
    }


def _child_command_tokens(*, source_value: str, max_messages: int) -> tuple[str, ...]:
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
        "--source-value",
        source_value,
        "--max-messages",
        str(max_messages),
        "--confirm-token",
        EXECUTE_CONFIRM_TOKEN,
    )


def _redacted_child_command_tokens(*, max_messages: int) -> tuple[str, ...]:
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
        "--source-value",
        SOURCE_VALUE_PLACEHOLDER,
        "--max-messages",
        str(max_messages),
        "--confirm-token",
        "EXECUTE_CONFIRM_TOKEN",
    )


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


__all__ = [
    "CHILD_RUNNER_PATH",
    "CliArgumentError",
    "RUNTIME_ENV_FILE_PLACEHOLDER",
    "RunnerResult",
    "SCHEMA_VERSION",
    "SOURCE_VALUE_PLACEHOLDER",
    "WRAPPER_RUNNER_PATH",
    "build_parser",
    "main",
    "restricted_env_overlay_argument_error_report",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
