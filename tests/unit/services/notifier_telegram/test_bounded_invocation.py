from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.notifier_telegram.bounded_invocation import (
    EXPECTED_EVENT_TYPE,
    BoundedNotifierDryRunInvocationConfig,
    BoundedNotifierRuntime,
    EventOutboxRecord,
    NotifierInvocationOutcome,
    run_bounded_notifier_dry_run_invocation,
)
from src.services.notifier_telegram.config import NotifierTelegramConfig
from src.services.notifier_telegram.models import DeliveryResult


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/notifier_telegram/bounded_invocation.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://sentinel_redis_url"
BOT_TOKEN = "123456:sentinel_bot_token"
RAW_REQUEST = "sentinel raw request body"
RAW_RESPONSE = "sentinel raw telegram response"
RENDERED_MESSAGE = "sentinel rendered message text"
EXCEPTION_DETAIL = "sentinel private exception detail"
SOURCE_RAW_TEXT = "sentinel source raw text"


def _config(
    *,
    dry_run: bool = False,
    enable_notification_send: bool = True,
    allow_edits: bool = True,
) -> NotifierTelegramConfig:
    return NotifierTelegramConfig(
        app_env="prod",
        database_url=DB_URL,
        redis_url=REDIS_URL,
        telegram_bot_token=BOT_TOKEN,
        queue_name="q.notification.send",
        consumer_group="notifier-telegram",
        consumer_name="unit",
        batch_size=20,
        block_ms=5000,
        dry_run=dry_run,
        allow_edits=allow_edits,
        enable_notification_send=enable_notification_send,
        enable_digest_runtime=True,
        max_message_chars=3800,
        edit_window_minutes=180,
        telegram_api_base_url="https://api.telegram.org",
        request_timeout_sec=10,
        log_level="INFO",
    )


def _loader() -> NotifierTelegramConfig:
    return _config()


def _raising_loader() -> NotifierTelegramConfig:
    raise AssertionError("config loader must not be called")


class FakeRuntimeBuilder:
    def __init__(
        self,
        *,
        event_type: str | None = EXPECTED_EVENT_TYPE,
        delivery_result: DeliveryResult | None = None,
        invoke_error: BaseException | None = None,
    ) -> None:
        self.event_type = event_type
        self.delivery_result = delivery_result or DeliveryResult(
            delivery_status="suppressed",
            telegram_chat_id=12345,
            telegram_message_id=None,
            attempt_count=0,
            transport_error_code="dry_run_skip_transport",
            transport_error_class=None,
            telegram_response_json={"raw": RAW_RESPONSE},
        )
        self.invoke_error = invoke_error
        self.calls = 0
        self.configs: list[NotifierTelegramConfig] = []
        self.loaded_event_ids: list[UUID] = []
        self.invoked_event_ids: list[UUID] = []
        self.close_commits: list[bool] = []

    async def __call__(self, notifier_config, state, logger) -> BoundedNotifierRuntime:
        del logger
        self.calls += 1
        self.configs.append(notifier_config)
        state.database_session_opened = True

        async def load_event(event_id: UUID):
            self.loaded_event_ids.append(event_id)
            if self.event_type is None:
                return None
            return EventOutboxRecord(event_id=event_id, event_type=self.event_type)

        async def invoke(event_id: UUID):
            self.invoked_event_ids.append(event_id)
            if self.invoke_error is not None:
                raise self.invoke_error
            return NotifierInvocationOutcome(
                delivery_result=self.delivery_result,
                notifier_owned_write_counts={
                    "notification_plans_insert_calls": 1,
                    "notification_renders_insert_calls": 1,
                    "notification_delivery_records_insert_calls": 1,
                    "notification_plans_status_update_calls": 2,
                    "state_transitions_insert_calls": 2,
                    "event_outbox_delivery_result_insert_calls": 1,
                },
            )

        async def close(commit: bool):
            self.close_commits.append(commit)

        return BoundedNotifierRuntime(
            notifier_config=notifier_config,
            load_event_outbox=load_event,
            invoke_notifier=invoke,
            close=close,
        )


@pytest.mark.asyncio
async def test_no_flags_fail_closed_before_config_db_or_action() -> None:
    runtime_builder = FakeRuntimeBuilder()

    result = await run_bounded_notifier_dry_run_invocation(
        BoundedNotifierDryRunInvocationConfig(trigger_event_id=None),
        notifier_config_loader=_raising_loader,
        runtime_builder=runtime_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["ok"] is False
    assert report["error_code"] == "operator_approval_missing"
    assert report["network_attempted"] is False
    assert report["transport_attempted"] is False
    assert report["processed_event_count"] == 0
    assert report["side_effects"]["database_session_opened"] is False
    assert report["side_effects"]["notifier_invocation_attempted"] is False
    assert runtime_builder.calls == 0


@pytest.mark.asyncio
async def test_missing_trigger_event_id_fails_closed_before_runtime() -> None:
    runtime_builder = FakeRuntimeBuilder()

    result = await run_bounded_notifier_dry_run_invocation(
        BoundedNotifierDryRunInvocationConfig(
            trigger_event_id=None,
            operator_approved=True,
            allow_database_write=True,
        ),
        notifier_config_loader=_raising_loader,
        runtime_builder=runtime_builder,
    )

    assert result.error_code == "trigger_event_id_missing"
    assert result.processed_event_count == 0
    assert runtime_builder.calls == 0


@pytest.mark.asyncio
async def test_missing_database_write_allowance_fails_before_runtime() -> None:
    runtime_builder = FakeRuntimeBuilder()

    result = await run_bounded_notifier_dry_run_invocation(
        BoundedNotifierDryRunInvocationConfig(
            trigger_event_id=str(uuid4()),
            operator_approved=True,
            allow_database_write=False,
        ),
        notifier_config_loader=_raising_loader,
        runtime_builder=runtime_builder,
    )

    assert result.status == "blocked"
    assert result.error_code == "database_write_not_allowed"
    assert result.processed_event_count == 0
    assert runtime_builder.calls == 0


@pytest.mark.asyncio
async def test_unsupported_event_type_rejected_before_notifier_invocation() -> None:
    trigger_event_id = uuid4()
    runtime_builder = FakeRuntimeBuilder(event_type="analysis.completed.v1")

    result = await run_bounded_notifier_dry_run_invocation(
        BoundedNotifierDryRunInvocationConfig(
            trigger_event_id=str(trigger_event_id),
            operator_approved=True,
            allow_database_write=True,
        ),
        notifier_config_loader=_loader,
        runtime_builder=runtime_builder,
    )

    assert result.status == "blocked"
    assert result.error_code == "unsupported_event_type"
    assert result.processed_event_count == 0
    assert runtime_builder.loaded_event_ids == [trigger_event_id]
    assert runtime_builder.invoked_event_ids == []
    assert runtime_builder.close_commits == [False]


@pytest.mark.asyncio
async def test_valid_event_invokes_notifier_once_with_forced_dry_run_config() -> None:
    trigger_event_id = uuid4()
    runtime_builder = FakeRuntimeBuilder()

    result = await run_bounded_notifier_dry_run_invocation(
        BoundedNotifierDryRunInvocationConfig(
            trigger_event_id=str(trigger_event_id),
            operator_approved=True,
            allow_database_write=True,
        ),
        notifier_config_loader=_loader,
        runtime_builder=runtime_builder,
    )
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert result.processed_event_count == 1
    assert runtime_builder.loaded_event_ids == [trigger_event_id]
    assert runtime_builder.invoked_event_ids == [trigger_event_id]
    assert runtime_builder.close_commits == [True]
    forced = runtime_builder.configs[0]
    assert forced.dry_run is True
    assert forced.enable_notification_send is False
    assert forced.allow_edits is False
    assert forced.telegram_bot_token == ""
    assert forced.batch_size == 1
    assert report["send_enabled"] is False
    assert report["dry_run"] is True
    assert report["edits_allowed"] is False
    assert report["network_attempted"] is False
    assert report["transport_attempted"] is False
    assert report["notifier_owned_write_counts"]["notification_delivery_records_insert_calls"] == 1
    assert report["delivery_result_summary"]["delivery_status"] == "suppressed"
    assert report["delivery_result_summary"]["transport_error_code"] == "dry_run_skip_transport"


@pytest.mark.asyncio
async def test_event_missing_fails_without_notifier_invocation() -> None:
    runtime_builder = FakeRuntimeBuilder(event_type=None)

    result = await run_bounded_notifier_dry_run_invocation(
        BoundedNotifierDryRunInvocationConfig(
            trigger_event_id=str(uuid4()),
            operator_approved=True,
            allow_database_write=True,
        ),
        notifier_config_loader=_loader,
        runtime_builder=runtime_builder,
    )

    assert result.status == "blocked"
    assert result.error_code == "event_outbox_event_missing"
    assert result.processed_event_count == 0
    assert runtime_builder.invoked_event_ids == []


@pytest.mark.asyncio
async def test_raw_exception_and_secret_values_are_not_rendered() -> None:
    runtime_builder = FakeRuntimeBuilder(
        invoke_error=RuntimeError(
            f"{BOT_TOKEN} {DB_URL} {REDIS_URL} {RAW_REQUEST} {RAW_RESPONSE} {RENDERED_MESSAGE} "
            f"{EXCEPTION_DETAIL} {SOURCE_RAW_TEXT}"
        )
    )

    result = await run_bounded_notifier_dry_run_invocation(
        BoundedNotifierDryRunInvocationConfig(
            trigger_event_id=str(uuid4()),
            operator_approved=True,
            allow_database_write=True,
        ),
        notifier_config_loader=_loader,
        runtime_builder=runtime_builder,
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.status == "failed"
    assert result.error_code == "notifier_invocation_failed"
    for raw in (BOT_TOKEN, DB_URL, REDIS_URL, RAW_REQUEST, RAW_RESPONSE, RENDERED_MESSAGE, EXCEPTION_DETAIL, SOURCE_RAW_TEXT):
        assert raw not in rendered


def test_source_does_not_start_workers_read_redis_or_import_external_clients() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules = set()
    imported_roots = set()
    called_names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)
            imported_roots.add(node.module.split(".", 1)[0])
        elif isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute):
                called_names.add(func.attr)
            elif isinstance(func, ast.Name):
                called_names.add(func.id)

    assert {"redis", "openai", "requests", "httpx", "aiohttp", "telegram"}.isdisjoint(imported_roots)
    assert not any(".worker" in module or ".redis_streams" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".analysis_validator" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert "run_forever" not in called_names
    assert "read_batch" not in called_names
    assert "send_message" not in called_names
    assert "edit_message_text" not in called_names
    assert "delivery_decision =" not in source
    assert "verdict =" not in source
