from __future__ import annotations

import argparse
import asyncio
import ctypes.util
import json
import math
import re
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


SCHEMA_VERSION = "dedicated_vps_tdlib_runtime_response_behavior_diagnostic_v1"
SCRIPT_NAME = "dedicated_vps_tdlib_runtime_response_behavior_diagnostic"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_MAX_OBSERVATIONS = 20
DEFAULT_RECEIVE_TIMEOUT_SEC = 1.0
DEFAULT_OVERALL_TIMEOUT_SEC = 30.0

ALLOWED_REQUEST_TYPES = frozenset({"getAuthorizationState", "setTdlibParameters"})
TERMINAL_TRANSITION_STATES = frozenset(
    {"authorizationStateWaitEncryptionKey", "authorizationStateReady"}
)

SIDE_EFFECT_FLAG_NAMES = (
    "tdlib_initialized",
    "tdlib_closed",
    "tdlib_send_called",
    "tdlib_receive_called",
    "tdlib_auth_attempted",
    "public_username_resolve_called",
    "join_called",
    "history_fetch_called",
    "database_mutation_performed",
    "redis_mutation_performed",
    "live_collector_started",
    "collector_runtime_started",
    "notifier_transport_enabled",
    "outbox_relay_started",
    "router_normalizer_started",
    "source_messages_written",
    "source_message_versions_written",
    "event_outbox_written",
    "telegram_channel_registry_mutation_performed",
)

_SAFE_TYPE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,79}$")
_SAFE_ERROR_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,80}$")


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
CollectorConfigFactory = Callable[[Mapping[str, str]], Any]
TdjsonAvailabilityChecker = Callable[[Mapping[str, str]], Mapping[str, Any] | bool]
TransportFactory = Callable[[Mapping[str, str]], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ops import (  # noqa: E402
    dedicated_vps_tdlib_session_reuse_collector_readiness_preflight as session_preflight,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Observe bounded TDLib response behavior around setTdlibParameters "
            "without resolving public usernames, joining chats, fetching history, "
            "writing DB/Redis, or starting the live collector."
        )
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--repo-root", default=None)
    parser.add_argument("--max-observations", type=int, default=DEFAULT_MAX_OBSERVATIONS)
    parser.add_argument(
        "--receive-timeout-sec",
        type=float,
        default=DEFAULT_RECEIVE_TIMEOUT_SEC,
    )
    parser.add_argument(
        "--overall-timeout-sec",
        type=float,
        default=DEFAULT_OVERALL_TIMEOUT_SEC,
    )
    parser.add_argument(
        "--approved-tdlib-runtime-response-diagnostic",
        action="store_true",
    )
    return parser


def default_repo_root() -> Path:
    return ROOT


def _ensure_repo_on_path(repo_root: Path) -> None:
    root = str(repo_root)
    if root not in sys.path:
        sys.path.insert(0, root)


def _side_effect_defaults() -> dict[str, bool]:
    return {flag: False for flag in SIDE_EFFECT_FLAG_NAMES}


def _base_report() -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "contract_status": "blocked_runtime_env_unreadable",
        "checks_failed": [],
        "runtime_env_read": False,
        "collector_config_built": False,
        "tdjson_available": False,
        "approved_runtime_probe": False,
        "request_types_sent": [],
        "request_count_bucket": "zero",
        "receive_attempt_count_bucket": "zero",
        "observation_count_bucket": "zero",
        "update_types_seen": [],
        "authorization_states_seen": [],
        "final_authorization_state": None,
        "set_parameters_request_sent": False,
        "set_parameters_request_extra_present": False,
        "set_parameters_response_seen": False,
        "set_parameters_response_type": None,
        "set_parameters_response_extra_matched": False,
        "set_parameters_error_code": None,
        "set_parameters_error_class": None,
        "transition_after_set_parameters": None,
        "timed_out_waiting_for_set_parameters_response": False,
        "transport_error_class": None,
        "operator_next_action": (
            "Review the dry-run plan and rerun only on the approved operator host "
            "with the explicit runtime-response diagnostic approval flag."
        ),
    }
    report.update(_side_effect_defaults())
    return report


def _set_check(report: dict[str, Any], check: str) -> None:
    if check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _bucket_count(count: int | None) -> str:
    if count is None:
        return "unknown"
    if count <= 0:
        return "zero"
    if count == 1:
        return "one"
    if count <= 5:
        return "two_to_five"
    if count <= 10:
        return "six_to_ten"
    if count <= 20:
        return "eleven_to_twenty"
    if count <= 50:
        return "twenty_one_to_fifty"
    return "more_than_fifty"


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _safe_tdlib_type(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    if _SAFE_TYPE_RE.fullmatch(value):
        return value
    return "unrecognized"


def _safe_error_code(value: Any) -> int | str | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        if isinstance(value, str) and _SAFE_ERROR_CODE_RE.fullmatch(value):
            return value
    return None


def _authorization_state_type_from_payload(payload: Mapping[str, Any]) -> str | None:
    payload_type = payload.get("@type")
    if payload_type == "updateAuthorizationState":
        state = payload.get("authorization_state")
        if isinstance(state, Mapping):
            state_type = state.get("@type")
            return state_type if isinstance(state_type, str) else None
    if isinstance(payload_type, str) and payload_type.startswith("authorizationState"):
        return payload_type
    return None


def _validate_probe_limits(
    report: dict[str, Any],
    *,
    max_observations: int,
    receive_timeout_sec: float,
    overall_timeout_sec: float,
) -> bool:
    valid = True
    if isinstance(max_observations, bool) or max_observations <= 0:
        _set_check(report, "max_observations.invalid")
        valid = False
    for name, value in (
        ("receive_timeout_sec", receive_timeout_sec),
        ("overall_timeout_sec", overall_timeout_sec),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, (float, int))
            or not math.isfinite(float(value))
            or float(value) < 0
        ):
            _set_check(report, f"{name}.invalid")
            valid = False
    if not valid:
        report["contract_status"] = "blocked_probe_limits_invalid"
        report["operator_next_action"] = (
            "Use finite non-negative timeout values and a positive max-observations "
            "limit before attempting the runtime-response diagnostic."
        )
    return valid


def _build_collector_config_default(repo_root: Path, values: Mapping[str, str]) -> Any:
    _ensure_repo_on_path(repo_root)
    from src.services.collector_telegram.config import CollectorTelegramConfig

    return CollectorTelegramConfig.from_env(values)


def _runtime_env_tdjson_library_path(values: Mapping[str, str]) -> str | None:
    candidate = values.get("TDJSON_LIBRARY_PATH")
    if not isinstance(candidate, str):
        return None
    stripped = candidate.strip()
    return stripped or None


def _inspect_tdjson_availability(values: Mapping[str, str]) -> dict[str, bool]:
    explicit_path = _runtime_env_tdjson_library_path(values)
    explicit_present = False
    if explicit_path is not None:
        try:
            explicit_present = Path(explicit_path).is_file()
        except OSError:
            explicit_present = False

    default_present = False
    for candidate in ("/opt/github-ai-catchbot/tdlib/lib/libtdjson.so",):
        try:
            if Path(candidate).is_file():
                default_present = True
                break
        except OSError:
            continue

    return {
        "tdjson_available": bool(
            explicit_present or default_present or ctypes.util.find_library("tdjson")
        )
    }


def _tdjson_available(
    values: Mapping[str, str],
    checker: TdjsonAvailabilityChecker | None,
    transport_factory: TransportFactory | None,
) -> bool:
    if checker is None and transport_factory is not None:
        return True
    try:
        result = checker(values) if checker is not None else _inspect_tdjson_availability(values)
    except Exception:
        return False
    if isinstance(result, bool):
        return result
    return bool(result.get("tdjson_available"))


def _build_default_transport(values: Mapping[str, str]) -> Any:
    from src.services.collector_telegram.tdlib_client import TDJsonTransport

    transport = TDJsonTransport(library_path=_runtime_env_tdjson_library_path(values))
    transport.assert_available()
    return transport


def _set_count_buckets(
    report: dict[str, Any],
    *,
    request_count: int,
    receive_attempt_count: int,
    observation_count: int,
) -> None:
    report["request_count_bucket"] = _bucket_count(request_count)
    report["receive_attempt_count_bucket"] = _bucket_count(receive_attempt_count)
    report["observation_count_bucket"] = _bucket_count(observation_count)


def _make_extra(sequence: int, request_type: str) -> str:
    return f"{SCRIPT_NAME}:{sequence}:{request_type}"


def _apply_observation(
    report: dict[str, Any],
    payload: Mapping[str, Any],
    *,
    set_parameters_extra: str | None,
    set_parameters_request_sent: bool,
) -> tuple[bool, bool]:
    payload_type = _safe_tdlib_type(payload.get("@type"))
    if payload_type is not None and payload_type.startswith("update"):
        _append_unique(report["update_types_seen"], payload_type)

    matched_set_parameters_response = False
    stop_after_observation = False
    raw_extra = payload.get("@extra")
    if (
        set_parameters_extra is not None
        and isinstance(raw_extra, str)
        and raw_extra == set_parameters_extra
    ):
        matched_set_parameters_response = True
        report["set_parameters_response_seen"] = True
        report["set_parameters_response_extra_matched"] = True
        report["set_parameters_response_type"] = payload_type
        if payload_type == "error":
            report["set_parameters_error_code"] = _safe_error_code(payload.get("code"))
            report["set_parameters_error_class"] = "tdlib_error"
            stop_after_observation = True

    state_type = _authorization_state_type_from_payload(payload)
    if state_type is not None:
        _append_unique(report["authorization_states_seen"], state_type)
        report["final_authorization_state"] = state_type
        if set_parameters_request_sent:
            report["transition_after_set_parameters"] = state_type
            if state_type in TERMINAL_TRANSITION_STATES:
                stop_after_observation = True

    return matched_set_parameters_response, stop_after_observation


async def _run_runtime_probe(
    *,
    repo_root: Path,
    values: Mapping[str, str],
    config: Any,
    transport_factory: TransportFactory | None,
    max_observations: int,
    receive_timeout_sec: float,
    overall_timeout_sec: float,
    report: dict[str, Any],
) -> None:
    _ensure_repo_on_path(repo_root)
    from src.services.collector_telegram.tdlib_client import (
        TDLibClient,
        build_set_tdlib_parameters_payload,
    )

    transport = (transport_factory or _build_default_transport)(values)
    client = TDLibClient(config, transport=transport)

    request_count = 0
    receive_attempt_count = 0
    observation_count = 0
    request_sequence = 0
    set_parameters_extra: str | None = None

    def request_with_extra(request: Mapping[str, Any]) -> dict[str, Any]:
        nonlocal request_sequence
        request_sequence += 1
        request_type = request.get("@type")
        if request_type not in ALLOWED_REQUEST_TYPES:
            raise ValueError("forbidden_request_type")
        payload = dict(request)
        payload["@extra"] = _make_extra(request_sequence, str(request_type))
        return payload

    async def send_request(request: Mapping[str, Any]) -> dict[str, Any]:
        nonlocal request_count
        payload = request_with_extra(request)
        request_type = str(payload["@type"])
        await client.send(payload)
        request_count += 1
        report["tdlib_send_called"] = True
        _append_unique(report["request_types_sent"], request_type)
        return payload

    try:
        await client.initialize()
        report["tdlib_initialized"] = True
    except Exception as exc:
        report["transport_error_class"] = type(exc).__name__
        report["contract_status"] = "blocked_tdlib_initialize_failed"
        _set_check(report, "tdlib.initialize_failed")
        return

    try:
        await send_request({"@type": "getAuthorizationState"})
        started_at = time.monotonic()

        while receive_attempt_count < max_observations:
            if overall_timeout_sec >= 0 and time.monotonic() - started_at > overall_timeout_sec:
                break
            try:
                receive_attempt_count += 1
                report["tdlib_receive_called"] = True
                payload = await client.receive(float(receive_timeout_sec))
            except Exception as exc:
                report["transport_error_class"] = type(exc).__name__
                report["contract_status"] = "blocked_unexpected_error"
                _set_check(report, "tdlib.receive_failed")
                return

            if not isinstance(payload, Mapping):
                continue

            observation_count += 1
            _matched_response, stop = _apply_observation(
                report,
                payload,
                set_parameters_extra=set_parameters_extra,
                set_parameters_request_sent=report["set_parameters_request_sent"],
            )

            if (
                report["final_authorization_state"] == "authorizationStateWaitTdlibParameters"
                and not report["set_parameters_request_sent"]
            ):
                set_request = build_set_tdlib_parameters_payload(config)
                sent_payload = await send_request(set_request)
                set_parameters_extra = sent_payload.get("@extra")
                report["set_parameters_request_sent"] = True
                report["set_parameters_request_extra_present"] = isinstance(
                    set_parameters_extra,
                    str,
                )

            if stop:
                break
    finally:
        _set_count_buckets(
            report,
            request_count=request_count,
            receive_attempt_count=receive_attempt_count,
            observation_count=observation_count,
        )
        try:
            await client.close()
            report["tdlib_closed"] = True
        except Exception as exc:
            if report["transport_error_class"] is None:
                report["transport_error_class"] = type(exc).__name__


def _apply_final_probe_status(report: dict[str, Any]) -> None:
    if report["contract_status"] in {
        "blocked_tdlib_initialize_failed",
        "blocked_unexpected_error",
    }:
        report["operator_next_action"] = (
            "Inspect TDLib transport initialization/receive behavior at class level "
            "only; do not paste private stderr or runtime secrets."
        )
        return

    if not report["set_parameters_request_sent"]:
        report["contract_status"] = "blocked_set_parameters_not_sent"
        _set_check(report, "set_parameters.not_sent")
        report["operator_next_action"] = (
            "TDLib did not reach authorizationStateWaitTdlibParameters inside the "
            "bounded observation window. Re-check transport receive behavior before "
            "retrying any broader Telegram operation."
        )
        return

    if report["set_parameters_response_type"] == "error":
        report["contract_status"] = "blocked_set_parameters_error_observed"
        _set_check(report, "set_parameters.error_response")
        report["operator_next_action"] = (
            "Use the sanitized TDLib error code/class to pick the next "
            "setTdlibParameters-specific fix. Do not send encryption-key, login, "
            "public-chat, join, or history requests in this slice."
        )
        return

    if not report["set_parameters_response_seen"]:
        report["contract_status"] = "blocked_set_parameters_response_timeout"
        report["timed_out_waiting_for_set_parameters_response"] = True
        _set_check(report, "set_parameters.response_timeout")
        report["operator_next_action"] = (
            "The setTdlibParameters request was sent, but no correlated function "
            "response was observed. Compare update noise and receive-loop behavior "
            "before changing parameter shape or moving to encryption-key handling."
        )
        return

    report["contract_status"] = "tdlib_runtime_response_diagnostic_completed"
    report["operator_next_action"] = (
        "The diagnostic observed a correlated setTdlibParameters response. Use the "
        "response type and authorization-state transition fields to choose the next "
        "bounded fix."
    )


def generate_report(
    *,
    repo_root: str | Path | None = None,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_observations: int = DEFAULT_MAX_OBSERVATIONS,
    receive_timeout_sec: float = DEFAULT_RECEIVE_TIMEOUT_SEC,
    overall_timeout_sec: float = DEFAULT_OVERALL_TIMEOUT_SEC,
    approved_tdlib_runtime_response_diagnostic: bool = False,
    runtime_env_reader: RuntimeEnvReader | None = None,
    collector_config_factory: CollectorConfigFactory | None = None,
    tdjson_availability_checker: TdjsonAvailabilityChecker | None = None,
    transport_factory: TransportFactory | None = None,
) -> ScriptResult:
    resolved_repo_root = (
        Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    )
    report = _base_report()
    report["approved_runtime_probe"] = approved_tdlib_runtime_response_diagnostic

    if not _validate_probe_limits(
        report,
        max_observations=max_observations,
        receive_timeout_sec=receive_timeout_sec,
        overall_timeout_sec=overall_timeout_sec,
    ):
        return ScriptResult(exit_code=1, report=report)

    reader = runtime_env_reader or session_preflight.parse_runtime_env_file
    try:
        values = reader(runtime_env_path)
    except Exception:
        report["contract_status"] = "blocked_runtime_env_unreadable"
        _set_check(report, "runtime_env.unreadable")
        return ScriptResult(exit_code=1, report=report)
    report["runtime_env_read"] = True

    try:
        if collector_config_factory is None:
            config = _build_collector_config_default(resolved_repo_root, values)
        else:
            config = collector_config_factory(values)
    except Exception as exc:
        report["transport_error_class"] = type(exc).__name__
        report["contract_status"] = "blocked_collector_config_invalid"
        _set_check(report, "collector_config.invalid")
        report["operator_next_action"] = (
            "Fix runtime env/config on the operator host without sharing raw "
            "runtime.env or secret values."
        )
        return ScriptResult(exit_code=1, report=report)
    report["collector_config_built"] = True

    report["tdjson_available"] = _tdjson_available(
        values,
        tdjson_availability_checker,
        transport_factory,
    )

    if not approved_tdlib_runtime_response_diagnostic:
        report["contract_status"] = "dry_run_runtime_probe_requires_approval"
        _set_check(report, "approval.required")
        report["operator_next_action"] = (
            "Dry-run complete. Re-run only in the approved operator environment "
            "with --approved-tdlib-runtime-response-diagnostic to initialize TDLib "
            "and observe bounded response behavior."
        )
        return ScriptResult(exit_code=1, report=report)

    if not report["tdjson_available"]:
        report["contract_status"] = "blocked_tdjson_unavailable"
        _set_check(report, "tdjson.unavailable")
        report["operator_next_action"] = (
            "Install or point TDJSON_LIBRARY_PATH at tdjson before attempting the "
            "runtime-response diagnostic."
        )
        return ScriptResult(exit_code=1, report=report)

    asyncio.run(
        _run_runtime_probe(
            repo_root=resolved_repo_root,
            values=values,
            config=config,
            transport_factory=transport_factory,
            max_observations=max_observations,
            receive_timeout_sec=receive_timeout_sec,
            overall_timeout_sec=overall_timeout_sec,
            report=report,
        )
    )
    _apply_final_probe_status(report)
    return ScriptResult(
        exit_code=0
        if report["contract_status"] == "tdlib_runtime_response_diagnostic_completed"
        else 1,
        report=report,
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, allow_nan=False, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        repo_root=args.repo_root,
        runtime_env_path=args.runtime_env_path,
        max_observations=args.max_observations,
        receive_timeout_sec=args.receive_timeout_sec,
        overall_timeout_sec=args.overall_timeout_sec,
        approved_tdlib_runtime_response_diagnostic=(
            args.approved_tdlib_runtime_response_diagnostic
        ),
    )
    sys.stdout.write(render_json(result.report))
    sys.stdout.write("\n")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
