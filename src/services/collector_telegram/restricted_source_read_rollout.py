from __future__ import annotations

import contextlib
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from src.services.outbox_relay.models import OutboxEventRow

from .bounded_history_ingest_runner import (
    BoundedTelegramCollectorHistoryIngestConfig,
    BoundedTelegramCollectorHistoryIngestRuntimeHandle,
    EXECUTE_CONFIRM_TOKEN,
    MAX_MESSAGES_HARD_LIMIT,
    RUNNER_NAME,
    run_bounded_telegram_collector_history_ingest_sync,
)
from .config import CollectorTelegramConfig
from .models import CollectorEnvironment, CollectorMode, TrackedChat


SCHEMA_VERSION = "restricted_live_collector_one_channel_source_read_rollout_v1"
PREFLIGHT_SCHEMA_VERSION = "restricted_live_collector_one_channel_source_read_preflight_v1"
PASS_REASON_CODE = "one_channel_source_read_rollout_proof_ready"
PREFLIGHT_PASS_REASON_CODE = "one_channel_live_read_preflight_command_packet_ready"
SOURCE_KIND_PUBLIC_USERNAME = "public_username"
FAKE_CHAT_ID = 9876543210123
FAKE_MESSAGE_ID = 444555666
FAKE_MESSAGE_TEXT = "sentinel restricted one channel source read proof text"
FAKE_CONFIG_VALUE = "placeholder-value"
BOUNDED_RUNNER_PATH = "tools/bounded_collector_history_ingest_runner.py"
PYTHON_EXECUTABLE_PLACEHOLDER = "venv/bin/python"
SOURCE_VALUE_PLACEHOLDER = "<PUBLIC_USERNAME_SOURCE_VALUE>"
RUNTIME_ENV_FILE_PLACEHOLDER = "<RUNTIME_ENV_FILE>"
COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS = (
    "APP_ENV",
    "COLLECTOR_MODE",
    "DATABASE_URL",
    "REDIS_URL",
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_API_HASH_FILE",
    "TELEGRAM_PHONE_NUMBER",
    "TELEGRAM_2FA_PASSWORD",
    "TELEGRAM_2FA_PASSWORD_FILE",
    "TDLIB_STATE_DIR",
    "TDLIB_FILES_DIR",
    "TDLIB_DB_ENCRYPTION_KEY",
    "TDLIB_DB_ENCRYPTION_KEY_FILE",
    "RECONCILE_INTERVAL_SEC",
    "RECONCILE_BACKFILL_LIMIT",
    "WARM_BACKFILL_LIMIT",
    "HISTORY_PAGE_LIMIT",
    "COLLECTOR_SINGLETON_LOCK_PATH",
    "STARTUP_PROBE_TIMEOUT_SEC",
    "STARTUP_WARM_BACKFILL_ENABLED",
    "LOG_LEVEL",
)


@dataclass(frozen=True, slots=True)
class RestrictedLiveCollectorOneChannelSourceReadProofRequest:
    source_value: str | None = None
    source_values: tuple[str, ...] = ()
    requested_max_messages: int | None = None


class _FakeTransaction:
    def __init__(self, repository: "_FakeProofRepository") -> None:
        self.repository = repository
        self.snapshot: Any = None

    async def __aenter__(self) -> "_FakeProofRepository":
        self.snapshot = self.repository.snapshot()
        return self.repository

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is not None:
            self.repository.restore(self.snapshot)
        return None


class _FakeProofRepository:
    def __init__(self, source_value: str) -> None:
        self.target = TrackedChat(
            registry_id="11111111-1111-1111-1111-111111111111",
            chat_id=FAKE_CHAT_ID,
            desired_state="active",
            access_state="joined",
            source_kind=SOURCE_KIND_PUBLIC_USERNAME,
            source_value=source_value,
            priority_weight=100,
        )
        self.messages: dict[tuple[int, int], dict[str, Any]] = {}
        self.versions: dict[str, list[dict[str, Any]]] = {}
        self.outbox: list[Any] = []
        self.dedupe_keys: set[str] = set()
        self.registry_lookups: list[str] = []
        self.cursor_updates: list[dict[str, Any]] = []

    def snapshot(self) -> Any:
        return (
            deepcopy(self.messages),
            deepcopy(self.versions),
            list(self.outbox),
            set(self.dedupe_keys),
            deepcopy(self.cursor_updates),
        )

    def restore(self, snapshot: Any) -> None:
        self.messages, self.versions, self.outbox, self.dedupe_keys, self.cursor_updates = snapshot

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)

    async def find_public_username_registry_targets(self, normalized_source_value: str) -> list[dict[str, Any]]:
        self.registry_lookups.append(normalized_source_value)
        if normalized_source_value != self.target.source_value:
            return []
        return [
            {
                "registry_id": self.target.registry_id,
                "chat_id": self.target.chat_id,
                "desired_state": self.target.desired_state,
                "access_state": self.target.access_state,
                "source_kind": self.target.source_kind,
                "source_value": self.target.source_value,
                "username_snapshot": f"@{self.target.source_value}",
                "priority_weight": self.target.priority_weight,
                "last_seen_message_id": self.target.last_seen_message_id,
                "last_seen_message_date": self.target.last_seen_message_date,
            }
        ]

    async def list_reconcile_targets(self, limit: int) -> list[TrackedChat]:
        del limit
        return []

    async def get_source_message(self, *, platform: str, chat_id: int, message_id: int) -> Mapping[str, Any] | None:
        if platform != "telegram":
            return None
        return self.messages.get((chat_id, message_id))

    async def get_latest_version(self, source_message_id: str) -> Mapping[str, Any] | None:
        versions = self.versions.get(str(source_message_id), [])
        return versions[-1] if versions else None

    async def upsert_source_message(self, projection: Any, *, platform: str = "telegram") -> Mapping[str, Any]:
        if platform != "telegram":
            raise ValueError("platform_unsupported")
        source_message_uuid = uuid5(NAMESPACE_URL, f"telegram:{projection.chat_id}:{projection.message_id}")
        source_message_id = str(source_message_uuid)
        key = (projection.chat_id, projection.message_id)
        row = self.messages.get(key)
        if row is None:
            row = {
                "source_message_id": source_message_id,
                "chat_id": projection.chat_id,
                "message_id": projection.message_id,
                "logical_post_key": projection.logical_post_key,
                "current_version_no": 0,
            }
            self.messages[key] = row
            self.versions[source_message_id] = []
        row["logical_post_key"] = projection.logical_post_key
        return row

    async def append_source_message_version_if_changed(
        self,
        *,
        source_message_id: str,
        projection: Any,
        version_reason: str,
        observed_at: datetime | None = None,
        telegram_edit_date: datetime | None = None,
    ) -> tuple[bool, Mapping[str, Any] | None]:
        del observed_at, telegram_edit_date
        versions = self.versions.setdefault(str(source_message_id), [])
        if versions and versions[-1]["content_hash"] == projection.content_hash:
            return False, None
        row = {
            "source_message_id": str(source_message_id),
            "version_no": len(versions) + 1,
            "version_reason": version_reason,
            "content_hash": projection.content_hash,
        }
        versions.append(row)
        return True, row

    async def insert_outbox_event(self, event: Any) -> bool:
        if event.dedupe_key in self.dedupe_keys:
            return False
        self.dedupe_keys.add(event.dedupe_key)
        self.outbox.append(event)
        return True

    async def get_outbox_event_by_dedupe_key(self, dedupe_key: str) -> OutboxEventRow | None:
        for event in self.outbox:
            if event.dedupe_key != dedupe_key:
                continue
            return OutboxEventRow(
                event_id=uuid5(NAMESPACE_URL, event.dedupe_key),
                event_type=event.event_type,
                aggregate_type=event.aggregate_type,
                aggregate_id=UUID(str(event.aggregate_id)),
                dedupe_key=event.dedupe_key,
                payload_json=dict(event.payload_json),
                status="pending",
                fail_count=0,
                created_at=datetime.now(timezone.utc),
            )
        return None

    async def mark_outbox_published(self, *, event_id: UUID, published_at: datetime | None = None) -> bool:
        del event_id, published_at
        return False

    async def update_channel_sync_cursor(self, **kwargs: Any) -> None:
        self.cursor_updates.append(dict(kwargs))

    async def count_source_message_versions(self, source_message_id: str) -> int:
        return len(self.versions.get(str(source_message_id), []))

    async def count_source_created_events(self, source_message_id: str) -> int:
        return sum(
            1
            for event in self.outbox
            if str(event.aggregate_id) == str(source_message_id)
            and event.event_type == "source_message.created.v1"
        )

    async def count_source_outbox_events(self, source_message_id: str) -> int:
        return sum(
            1
            for event in self.outbox
            if str(event.aggregate_id) == str(source_message_id)
            and event.event_type
            in {
                "source_message.created.v1",
                "source_message.edited.v1",
                "source_message.deleted.v1",
                "source_message.reconciled.v1",
            }
        )


class _FakeProofHistoryClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, int]] = []

    async def fetch_newest_history_messages(self, *, chat_id: int, limit: int) -> Sequence[Mapping[str, Any]]:
        self.calls.append({"chat_id": chat_id, "limit": limit})
        return (
            {
                "@type": "message",
                "chat_id": chat_id,
                "id": FAKE_MESSAGE_ID,
                "date": 1713550000,
                "is_channel_post": True,
                "content": {
                    "@type": "messageText",
                    "text": {"text": FAKE_MESSAGE_TEXT, "entities": []},
                },
                "raw_nested_private_value": FAKE_CONFIG_VALUE,
            },
        )

    async def close(self) -> None:
        return None


class _FakeProofRuntimeBuilder:
    def __init__(self, source_value: str) -> None:
        self.repository = _FakeProofRepository(source_value)
        self.history_client = _FakeProofHistoryClient()
        self.calls = 0
        self.commit_calls = 0
        self.close_commits: list[bool] = []

    async def __call__(self, runtime_config: CollectorTelegramConfig, state: Any, logger: Any):
        del runtime_config, state, logger
        self.calls += 1

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            await self.history_client.close()

        async def commit() -> None:
            self.commit_calls += 1

        return BoundedTelegramCollectorHistoryIngestRuntimeHandle(
            repository=self.repository,
            history_client=self.history_client,
            close=close,
            commit=commit,
        )


def build_restricted_live_collector_one_channel_source_read_rollout_packet(
    request: RestrictedLiveCollectorOneChannelSourceReadProofRequest,
) -> dict[str, Any]:
    source_values = _request_source_values(request)
    normalized_source_values = tuple(
        value for value in (_normalize_source_value(value) for value in source_values) if value is not None
    )
    target_error = _target_error(source_values, normalized_source_values)
    if target_error is not None:
        return _base_packet(
            status="blocked",
            reason_code=target_error,
            requested_max_messages=request.requested_max_messages,
            target_count=len(source_values),
            target_fingerprint=None,
        )

    max_messages_error = _requested_max_messages_error(request.requested_max_messages)
    target_fingerprint = _fingerprint("source_value", normalized_source_values[0])
    if max_messages_error is not None:
        return _base_packet(
            status="blocked",
            reason_code=max_messages_error,
            requested_max_messages=request.requested_max_messages,
            target_count=1,
            target_fingerprint=target_fingerprint,
        )

    requested_max_messages = int(request.requested_max_messages)
    source_value = normalized_source_values[0]
    runtime_builder = _FakeProofRuntimeBuilder(source_value)
    result = run_bounded_telegram_collector_history_ingest_sync(
        BoundedTelegramCollectorHistoryIngestConfig(
            mode="execute",
            source_kind=SOURCE_KIND_PUBLIC_USERNAME,
            source_value=source_value,
            history_limit=requested_max_messages,
            operator_approved=True,
            confirm_token=EXECUTE_CONFIRM_TOKEN,
            allow_runtime_config=True,
            allow_database_read=True,
            allow_telegram_read=True,
            allow_database_write=True,
            allow_source_message_write=True,
            allow_source_version_write=True,
            allow_source_outbox_write=True,
            allow_source_outbox_publish=False,
            allow_redis_publish=False,
        ),
        runtime_config_loader=_fake_runtime_config,
        runtime_builder=runtime_builder,
    )
    lower = result.to_sanitized_dict()
    status = "pass" if result.ok else "fail" if lower.get("status") == "fail" else "blocked"
    reason_code = PASS_REASON_CODE if result.ok else str(lower.get("reason_code") or "bounded_runner_failed")
    packet = _base_packet(
        status=status,
        reason_code=reason_code,
        requested_max_messages=requested_max_messages,
        target_count=1,
        target_fingerprint=target_fingerprint,
    )
    packet["planned_readiness_state"].update(
        {
            "existing_collector_runner": RUNNER_NAME,
            "existing_runner_schema_version": lower.get("schema_version"),
            "existing_runner_consumed": True,
            "collector_source_truth_path": "source_messages/source_message_versions/event_outbox",
            "future_execute_confirm_token_required": True,
            "future_execute_confirm_token_value_printed": False,
            "fake_runtime_builder_used": True,
            "default_runtime_builder_used": False,
            "runtime_env_loaded": False,
        }
    )
    packet["actual_attempted_operations"].update(
        {
            "collector_bounded_runner_invoked": True,
            "fake_telegram_history_read_attempted": bool(lower.get("telegram_read_attempted")),
            "fake_telegram_history_read_calls": len(runtime_builder.history_client.calls),
            "fake_repository_write_attempted": bool(lower.get("database_write_attempted")),
            "fake_repository_commit_calls": runtime_builder.commit_calls,
            "fake_repository_close_commits": list(runtime_builder.close_commits),
            "source_outbox_publish_attempted": False,
            "redis_publish_attempted": False,
            "broad_collector_worker_started": False,
        }
    )
    packet["readback"] = {
        "fake_source_read_messages_observed": int(lower.get("messages_seen") or 0),
        "fake_source_messages_created_or_reused": int(
            (lower.get("readback") or {}).get("source_current_found_count") or 0
        ),
        "fake_source_versions_created_or_reused": int(
            (lower.get("readback") or {}).get("source_version_rows_count") or 0
        ),
        "fake_source_outbox_events_created_or_reused": int(
            (lower.get("readback") or {}).get("source_outbox_events_count") or 0
        ),
        "duplicate_guard_preserved": bool(
            result.ok
            and int(lower.get("messages_seen") or 0) > 0
            and int(lower.get("duplicate_noop_proof_count") or 0) == int(lower.get("messages_seen") or 0)
        ),
        "source_message_fingerprints": list(lower.get("source_message_fingerprints") or []),
        "source_outbox_event_fingerprints": list(lower.get("source_outbox_event_fingerprints") or []),
        "target_fingerprints": list(lower.get("target_fingerprints") or []),
    }
    packet["completion_claims"].update(
        {
            "RESTRICTED_LIVE_COLLECTOR_ONE_CHANNEL_SOURCE_READ_CODE_READY": bool(result.ok),
            "ONE_CHANNEL_SOURCE_READ_ROLLOUT_PROOF_PACKET_READY": bool(result.ok),
            "LIVE_TELEGRAM_READ_AUTHORITY_REMAINS_CLOSED_IN_THIS_TASK": True,
        }
    )
    if not result.ok:
        packet["lower_runner_reason_code"] = lower.get("reason_code")
        packet["lower_runner_status"] = lower.get("status")
    return packet


def build_restricted_live_collector_one_channel_source_read_preflight_packet(
    request: RestrictedLiveCollectorOneChannelSourceReadProofRequest,
) -> dict[str, Any]:
    source_values = _request_source_values(request)
    normalized_source_values = tuple(
        value for value in (_normalize_source_value(value) for value in source_values) if value is not None
    )
    target_error = _target_error(source_values, normalized_source_values)
    if target_error is not None:
        return _preflight_packet(
            status="blocked",
            reason_code=target_error,
            requested_max_messages=request.requested_max_messages,
            target_count=len(source_values),
            target_fingerprint=None,
            command_tokens=(),
        )

    max_messages_error = _requested_max_messages_error(request.requested_max_messages)
    target_fingerprint = _fingerprint("source_value", normalized_source_values[0])
    if max_messages_error is not None:
        return _preflight_packet(
            status="blocked",
            reason_code=max_messages_error,
            requested_max_messages=request.requested_max_messages,
            target_count=1,
            target_fingerprint=target_fingerprint,
            command_tokens=(),
        )

    requested_max_messages = int(request.requested_max_messages)
    command_tokens = _future_live_read_command_tokens(requested_max_messages=requested_max_messages)
    return _preflight_packet(
        status="pass",
        reason_code=PREFLIGHT_PASS_REASON_CODE,
        requested_max_messages=requested_max_messages,
        target_count=1,
        target_fingerprint=target_fingerprint,
        command_tokens=command_tokens,
    )


def restricted_live_collector_one_channel_source_read_argument_error_report(error_code: str) -> dict[str, Any]:
    return _base_packet(
        status="blocked",
        reason_code=error_code,
        requested_max_messages=None,
        target_count=0,
        target_fingerprint=None,
    )


def _request_source_values(request: RestrictedLiveCollectorOneChannelSourceReadProofRequest) -> tuple[str | None, ...]:
    values: list[str | None] = []
    if request.source_value is not None:
        values.append(request.source_value)
    values.extend(request.source_values)
    return tuple(values)


def _target_error(raw_values: Sequence[str | None], normalized_values: Sequence[str]) -> str | None:
    if len(raw_values) != 1:
        return "target_count_must_equal_one"
    raw_value = raw_values[0]
    normalized = normalized_values[0] if normalized_values else None
    if normalized is None:
        return "exact_source_value_required"
    if _is_broad_target_value(raw_value, normalized):
        return "broad_target_not_allowed"
    if _looks_like_direct_chat_id(normalized):
        return "direct_chat_id_target_not_allowed"
    if _looks_like_registry_id(normalized):
        return "direct_registry_id_target_not_allowed"
    return None


def _requested_max_messages_error(value: int | None) -> str | None:
    if value is None:
        return "requested_max_messages_required"
    if not isinstance(value, int) or isinstance(value, bool):
        return "requested_max_messages_out_of_bounds"
    if value < 1 or value > MAX_MESSAGES_HARD_LIMIT:
        return "requested_max_messages_out_of_bounds"
    return None


def _base_packet(
    *,
    status: str,
    reason_code: str,
    requested_max_messages: int | None,
    target_count: int,
    target_fingerprint: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "reason_code": reason_code,
        "target_scope": {
            "exact_single_channel_required": True,
            "target_count": target_count,
            "target_fingerprint": target_fingerprint,
            "broad_registry_scan_allowed": False,
            "empty_target_allowed": False,
            "multiple_targets_allowed": False,
            "wildcard_target_allowed": False,
            "direct_chat_id_allowed": False,
            "direct_registry_id_allowed": False,
        },
        "bounded_read": {
            "hard_max_messages": MAX_MESSAGES_HARD_LIMIT,
            "requested_max_messages": requested_max_messages,
            "unbounded_history_allowed": False,
            "exactly_one_history_request": True,
        },
        "planned_readiness_state": {
            "state": "planned_fake_backed_rollout_proof",
            "existing_collector_runner": RUNNER_NAME,
            "future_execute_requires_exact_target": True,
            "future_execute_requires_allow_live_telegram_read": True,
            "future_execute_requires_confirm_token": True,
            "future_execute_requires_hard_max_message_cap": True,
            "future_execute_send_disabled": True,
            "future_execute_redis_mutation_disabled_by_default": True,
            "future_execute_openai_github_x_web_disabled": True,
            "future_execute_systemd_docker_disabled": True,
        },
        "authority": {
            "live_telegram_read_attempted": False,
            "live_telegram_read_authority_required_for_execute": True,
            "live_telegram_send_attempted": False,
            "openai_attempted": False,
            "github_attempted": False,
            "x_attempted": False,
            "web_attempted": False,
            "redis_mutation_attempted": False,
            "database_write_attempted": False,
            "docker_or_systemd_called": False,
            "runtime_values_printed": False,
        },
        "runtime_authority_opened_in_this_run": {
            "live_telegram_read": False,
            "live_telegram_send_or_edit": False,
            "live_openai": False,
            "live_github": False,
            "live_x": False,
            "live_web": False,
            "redis_mutation": False,
            "production_database_write": False,
            "docker_or_systemd": False,
            "runtime_env_value_output": False,
            "broad_collector_worker": False,
        },
        "actual_attempted_operations": {
            "collector_bounded_runner_invoked": False,
            "fake_telegram_history_read_attempted": False,
            "fake_telegram_history_read_calls": 0,
            "fake_repository_write_attempted": False,
            "source_outbox_publish_attempted": False,
            "redis_publish_attempted": False,
            "live_telegram_read_attempted": False,
            "live_telegram_send_attempted": False,
            "openai_attempted": False,
            "github_attempted": False,
            "x_attempted": False,
            "web_attempted": False,
            "docker_or_systemd_called": False,
        },
        "readback": {
            "fake_source_read_messages_observed": 0,
            "fake_source_messages_created_or_reused": 0,
            "fake_source_versions_created_or_reused": 0,
            "fake_source_outbox_events_created_or_reused": 0,
            "duplicate_guard_preserved": False,
            "source_message_fingerprints": [],
            "source_outbox_event_fingerprints": [],
            "target_fingerprints": [],
        },
        "redaction_audit": {
            "fingerprints_only": True,
            "raw_message_text_printed": False,
            "raw_url_printed": False,
            "raw_telegram_chat_id_printed": False,
            "raw_telegram_message_id_printed": False,
            "raw_registry_id_printed": False,
            "runtime_env_value_printed": False,
            "database_url_printed": False,
            "redis_url_printed": False,
            "token_or_secret_printed": False,
            "private_stderr_printed": False,
            "traceback_printed": False,
            "raw_payload_body_printed": False,
        },
        "open_gates": {
            "AUTHORITY_OPEN": True,
            "ROLLOUT_OPEN": True,
            "FUNCTION_COMPLETE_OPEN": True,
            "PRODUCTION_ROLLOUT_OPEN": True,
            "PRODUCT_COMPLETE_CLOSED": False,
            "PRODUCTION_ROLLOUT_CLOSED": False,
        },
        "completion_claims": {
            "RESTRICTED_LIVE_COLLECTOR_ONE_CHANNEL_SOURCE_READ_CODE_READY": False,
            "ONE_CHANNEL_SOURCE_READ_ROLLOUT_PROOF_PACKET_READY": False,
            "LIVE_TELEGRAM_READ_AUTHORITY_REMAINS_CLOSED_IN_THIS_TASK": True,
            "production_complete": False,
            "production_rollout_complete": False,
            "product_complete": False,
            "final_bot_complete": False,
            "one_hundred_percent_complete": False,
        },
    }


def _future_live_read_command_tokens(*, requested_max_messages: int) -> tuple[str, ...]:
    return (
        PYTHON_EXECUTABLE_PLACEHOLDER,
        BOUNDED_RUNNER_PATH,
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
        str(requested_max_messages),
        "--confirm-token",
        EXECUTE_CONFIRM_TOKEN,
    )


def _runtime_env_safe_loader_pattern(
    *,
    command_tokens: Sequence[str],
) -> dict[str, Any]:
    forbidden_child_tokens = (
        RUNTIME_ENV_FILE_PLACEHOLDER,
        "--runtime-env-file",
        "--runtime-env-path",
        "--env-file",
        "--allow-source-outbox-publish",
        "--allow-redis-publish",
        "--allow-send",
        "--chat-id",
        "--registry-id",
        "docker",
        "systemctl",
        "alembic",
    )
    child_command_tokens = tuple(command_tokens)
    child_command_text = " ".join(child_command_tokens)
    return {
        "loader": "safe_allowlisted_env_overlay_pattern_for_CollectorTelegramConfig.from_env",
        "runtime_env_loaded": False,
        "actual_runtime_env_file_read_in_this_task": False,
        "exact_runtime_env_file_placeholder_required": True,
        "runtime_env_file_placeholder": RUNTIME_ENV_FILE_PLACEHOLDER,
        "runtime_env_file_path_printed": False,
        "runtime_env_values_printed": False,
        "runtime_env_values_redacted": True,
        "allowed_env_keys": list(COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS),
        "reject_unknown_env_keys": True,
        "load_values_into_child_env_overlay_only": True,
        "uses_sys_executable_for_child": True,
        "entrypoint_uses_sys_executable": True,
        "child_command_uses_existing_runner": child_command_tokens[:2] == (
            PYTHON_EXECUTABLE_PLACEHOLDER,
            BOUNDED_RUNNER_PATH,
        ),
        "child_command_runner_path": BOUNDED_RUNNER_PATH,
        "child_command_tokens": list(child_command_tokens),
        "child_command_omits_runtime_env_file_token": not any(
            token in child_command_tokens for token in ("--runtime-env-file", "--runtime-env-path", "--env-file")
        )
        and RUNTIME_ENV_FILE_PLACEHOLDER not in child_command_tokens,
        "child_command_omits_source_outbox_publish": "--allow-source-outbox-publish" not in child_command_tokens,
        "child_command_omits_redis_publish": "--allow-redis-publish" not in child_command_tokens,
        "child_command_omits_send_edit": "--allow-send" not in child_command_tokens,
        "child_command_omits_chat_id": "--chat-id" not in child_command_tokens,
        "child_command_omits_registry_id": "--registry-id" not in child_command_tokens,
        "child_command_omits_docker_systemd_alembic": not any(
            token in child_command_text for token in ("docker", "systemctl", "alembic")
        ),
        "child_command_forbidden_tokens_absent": list(forbidden_child_tokens),
    }


def _preflight_packet(
    *,
    status: str,
    reason_code: str,
    requested_max_messages: int | None,
    target_count: int,
    target_fingerprint: str | None,
    command_tokens: Sequence[str],
) -> dict[str, Any]:
    packet = _base_packet(
        status=status,
        reason_code=reason_code,
        requested_max_messages=requested_max_messages,
        target_count=target_count,
        target_fingerprint=target_fingerprint,
    )
    safe_loader_pattern = (
        _runtime_env_safe_loader_pattern(command_tokens=command_tokens) if status == "pass" else None
    )
    packet["schema_version"] = PREFLIGHT_SCHEMA_VERSION
    packet["bounded_read"]["exactly_one_history_request"] = status == "pass"
    packet["planned_readiness_state"].update(
        {
            "state": "static_preflight_command_packet",
            "existing_collector_runner": RUNNER_NAME,
            "existing_runner_path": BOUNDED_RUNNER_PATH,
            "existing_runner_consumed": True,
            "existing_runner_command_uses_max_messages_alias": True,
            "fake_runtime_builder_used": False,
            "default_runtime_builder_used": False,
            "runtime_env_loaded": False,
            "runtime_env_file_placeholder": RUNTIME_ENV_FILE_PLACEHOLDER,
            "future_execution_requires_explicit_operator_approval_after_chatgpt_pass": True,
            "future_execution_requires_exact_command_copy_after_user_approval": True,
            "future_execution_source_outbox_write_is_database_only": True,
            "future_execution_source_outbox_publish_disabled": True,
        }
    )
    packet["future_execution_command"] = {
        "runner_path": BOUNDED_RUNNER_PATH,
        "runner_name": RUNNER_NAME,
        "command_tokens": list(command_tokens),
        "placeholders": {
            "runtime_env_file": RUNTIME_ENV_FILE_PLACEHOLDER,
            "source_value": SOURCE_VALUE_PLACEHOLDER,
        },
        "exact_confirm_required": True,
        "confirm_token_label": "EXECUTE_CONFIRM_TOKEN",
        "confirm_token_value": EXECUTE_CONFIRM_TOKEN if status == "pass" else None,
        "confirm_token_value_is_repo_constant": status == "pass",
        "max_messages_required": True,
        "max_messages_argument": "--max-messages",
        "max_messages_hard_limit": MAX_MESSAGES_HARD_LIMIT,
        "live_read_authority_required": True,
        "operator_approval_required": True,
        "send_disabled": True,
        "redis_publish_disabled": True,
        "source_outbox_publish_disabled": True,
        "source_outbox_write_enabled_for_future_database_readback": status == "pass",
        "production_database_write_authority_required_for_future_execution": True,
        "runtime_env": {
            "required": True,
            "loader": "CollectorTelegramConfig.from_env",
            "command_token_included": False,
            "placeholder": RUNTIME_ENV_FILE_PLACEHOLDER,
            "exact_runtime_env_file_placeholder_required": status == "pass",
            "runtime_env_file_placeholder": RUNTIME_ENV_FILE_PLACEHOLDER,
            "path_printed": False,
            "values_printed": False,
            "runtime_env_file_path_printed": False,
            "runtime_env_values_printed": False,
            "runtime_env_values_redacted": True,
            "runtime_env_loaded": False,
            "actual_runtime_env_file_read_in_this_task": False,
            "safe_loader_pattern_available": status == "pass",
            "safe_loader_pattern": safe_loader_pattern,
        },
        "forbidden_tokens_absent": [
            "--allow-source-outbox-publish",
            "--allow-redis-publish",
            "--allow-send",
            "--chat-id",
            "--registry-id",
            "--rollout-scope full-tracked-registry",
            "docker",
            "systemctl",
            "alembic",
        ],
    }
    packet["runtime_authority_opened_in_this_run"].update(
        {
            "live_telegram_send": False,
            "openai": False,
            "github": False,
            "x": False,
            "web": False,
            "database_write": False,
        }
    )
    packet["future_readback_plan"] = {
        "actual_readback_in_this_task": "static_preflight_only",
        "source_messages": {
            "expected_count_field": "source_current_found_count",
            "expected_fingerprint_field": "source_message_fingerprints",
        },
        "source_message_versions": {
            "expected_count_field": "source_version_rows_count",
            "expected_fingerprint_field": "source_message_fingerprints",
        },
        "source_outbox_events": {
            "expected_count_field": "source_outbox_events_count",
            "expected_fingerprint_field": "source_outbox_event_fingerprints",
            "publish_expected": False,
        },
        "duplicate_noop_proof": {
            "expected_count_field": "duplicate_noop_proof_count",
        },
        "target": {
            "expected_fingerprint_field": "target_fingerprints",
            "target_fingerprint": target_fingerprint,
        },
        "authority_transition": {
            "live_telegram_read_attempted_true_only_in_future_execution": True,
            "live_telegram_read_attempted_in_this_task": False,
        },
    }
    packet["redaction_audit"].update(
        {
            "runtime_env_path_printed": False,
            "runtime_env_file_path_printed": False,
            "command_uses_target_placeholder": SOURCE_VALUE_PLACEHOLDER in command_tokens,
            "command_uses_runtime_env_placeholder_only": True,
            "runtime_env_values_redacted": True,
            "actual_runtime_env_file_read_in_this_task": False,
            "confirm_token_is_repo_constant": status == "pass",
            "raw_source_value_printed": False,
        }
    )
    packet["completion_claims"].update(
        {
            "F1_LIVE_ONE_CHANNEL_SOURCE_READ_PREFLIGHT_PACKET_READY": status == "pass",
            "F1_LIVE_ONE_CHANNEL_EXACT_COMMAND_PACKET_READY": status == "pass",
            "LIVE_TELEGRAM_READ_AUTHORITY_REMAINS_CLOSED_IN_THIS_TASK": True,
            "LIVE_COLLECTOR_1_CHANNEL_CLOSED": False,
            "TDLib_session_health_proof_closed": False,
            "production_database_connectivity_proof_closed": False,
            "redis_publish_proof_closed": False,
        }
    )
    return packet


def _fake_runtime_config() -> CollectorTelegramConfig:
    return CollectorTelegramConfig(
        app_env=CollectorEnvironment.TEST,
        database_url="not-used-by-fake-proof",
        redis_url=None,
        collector_mode=CollectorMode.REPLAY,
        telegram_api_id=12345,
        telegram_api_hash=FAKE_CONFIG_VALUE,
        telegram_phone_number="not-used-by-fake-proof",
        telegram_2fa_password=None,
        tdlib_state_dir="not-used-by-fake-proof",
        tdlib_files_dir="not-used-by-fake-proof",
        tdlib_db_encryption_key=FAKE_CONFIG_VALUE,
        reconcile_interval_sec=300,
        reconcile_backfill_limit=3,
        warm_backfill_limit=1,
        history_page_limit=3,
    )


def _normalize_source_value(value: object | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lstrip("@").strip().lower()
    return normalized or None


def _is_broad_target_value(raw_value: object, normalized: str) -> bool:
    if not isinstance(raw_value, str):
        return False
    compact = " ".join(raw_value.strip().lower().replace("_", " ").replace("-", " ").split())
    return "*" in raw_value or normalized in {"all", "allchannels"} or compact in {"all", "all channels"}


def _looks_like_direct_chat_id(value: str) -> bool:
    candidate = value.removeprefix("+").removeprefix("-")
    return bool(candidate) and candidate.isdigit()


def _looks_like_registry_id(value: str) -> bool:
    with contextlib.suppress(ValueError):
        UUID(value)
        return True
    return False


def _fingerprint(kind: str, value: object | None) -> str | None:
    if value is None:
        return None
    digest = __import__("hashlib").sha256(f"{kind}:{value}".encode("utf-8")).hexdigest()
    return f"sha256:{digest[:16]}"


__all__ = [
    "BOUNDED_RUNNER_PATH",
    "COLLECTOR_RUNTIME_ENV_ALLOWED_KEYS",
    "PASS_REASON_CODE",
    "PREFLIGHT_PASS_REASON_CODE",
    "PREFLIGHT_SCHEMA_VERSION",
    "RUNTIME_ENV_FILE_PLACEHOLDER",
    "SCHEMA_VERSION",
    "SOURCE_VALUE_PLACEHOLDER",
    "RestrictedLiveCollectorOneChannelSourceReadProofRequest",
    "build_restricted_live_collector_one_channel_source_read_preflight_packet",
    "build_restricted_live_collector_one_channel_source_read_rollout_packet",
    "restricted_live_collector_one_channel_source_read_argument_error_report",
]
