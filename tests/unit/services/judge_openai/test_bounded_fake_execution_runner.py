from __future__ import annotations

import ast
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from src.services.judge_openai.bounded_fake_execution_runner import (
    BoundedJudgeOpenAIFakeExecutionConfig,
    BoundedJudgeOpenAIFakeExecutionError,
    BoundedJudgeOpenAIFakeExecutionRuntimeConfig,
    BoundedJudgeOpenAIFakeExecutionRedisPublisherHandle,
    BoundedJudgeOpenAIFakeExecutionRepositoryHandle,
    BoundedJudgeOpenAIRedisReaderHandle,
    DeterministicFakeOpenAIClient,
    ExistingJudgeOutputLookup,
    JudgeOutputRecord,
    build_deterministic_fake_judge_output_payload,
    run_bounded_judge_openai_fake_execution_sync,
)
from src.services.judge_openai.bounded_request_envelope_runner import (
    BundleEnvelopeRecord,
    JudgeCallOutboxRecord,
    RedisStreamMessage,
)
from src.services.judge_openai.models import BundleJudgeContext, JudgeRunRecord
from src.services.outbox_relay.models import OutboxEventRow, QueueRoute
from src.services.outbox_relay.routing import OutboxRouteResolver


ROOT = Path(__file__).resolve().parents[4]
RUNNER_PATH = ROOT / "src/services/judge_openai/bounded_fake_execution_runner.py"
TOOL_PATH = ROOT / "tools/bounded_judge_openai_fake_execution_runner.py"

TRIGGER_EVENT_ID = UUID("00000000-0000-4000-8000-0000a1c22bcb")
JUDGE_RUN_ID = UUID("00000000-0000-4000-8000-00007a111d13")
BUNDLE_ID = UUID("00000000-0000-4000-8000-0000c51bd89e")
CANDIDATE_GROUP_ID = UUID("00000000-0000-4000-8000-000042c0d691")
ARTIFACT_ID = UUID("00000000-0000-4000-8000-000012345678")
JUDGE_OUTPUT_ID = UUID("00000000-0000-4000-8000-0000aaabbb01")
READY_EVENT_ID = UUID("00000000-0000-4000-8000-0000dddeee02")
REDIS_MESSAGE_ID = "1700000356724-0"
VALIDATE_REDIS_MESSAGE_ID = "1700000456789-0"
RAW_PROMPT_CACHE_KEY = "judge:text_idea_primary:private-cache-key"
RAW_PRIMARY_SENTINEL = "sentinel private primary summary should not print"
DB_LOCATOR = "database-url-sentinel"
REDIS_LOCATOR = "redis-url-sentinel"


class FakeRedisReader:
    def __init__(self, messages: list[RedisStreamMessage]) -> None:
        self.messages = messages
        self.calls: list[dict[str, Any]] = []

    async def read_candidate_messages(self, *, queue_name, config):
        self.calls.append({"queue_name": queue_name, "scan_limit": config.scan_limit})
        return list(self.messages)


class FakeRedisReaderBuilder:
    def __init__(self, reader: FakeRedisReader) -> None:
        self.reader = reader
        self.calls = 0
        self.closed = False

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.redis_reader_created = True

        async def close() -> None:
            self.closed = True

        return BoundedJudgeOpenAIRedisReaderHandle(reader=self.reader, close=close)


class FakeRedisPublisher:
    def __init__(self, *, message_id: str = VALIDATE_REDIS_MESSAGE_ID) -> None:
        self.message_id = message_id
        self.calls: list[tuple[QueueRoute, object]] = []

    async def publish(self, route, message) -> str:
        self.calls.append((route, message))
        return self.message_id


class FakeRedisPublisherBuilder:
    def __init__(self, publisher: FakeRedisPublisher) -> None:
        self.publisher = publisher
        self.calls = 0
        self.closed = False

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.redis_publisher_created = True

        async def close() -> None:
            self.closed = True

        return BoundedJudgeOpenAIFakeExecutionRedisPublisherHandle(
            publisher=self.publisher,
            close=close,
        )


class RecordingFakeClientFactory:
    def __init__(self) -> None:
        self.clients: list[DeterministicFakeOpenAIClient] = []

    def __call__(self, payload):
        client = DeterministicFakeOpenAIClient(payload)
        self.clients.append(client)
        return client


class FakeRepository:
    def __init__(
        self,
        *,
        event: JudgeCallOutboxRecord | None,
        judge_run: JudgeRunRecord | None,
        bundle: BundleEnvelopeRecord | None,
        existing_outputs: list[JudgeOutputRecord] | None = None,
        ready_outbox: OutboxEventRow | None = None,
    ) -> None:
        self.event = event
        self.judge_run = judge_run
        self.bundle = bundle
        self.outputs = list(existing_outputs or [])
        self.ready_outbox = ready_outbox
        self.load_event_calls: list[UUID] = []
        self.load_judge_run_calls: list[UUID] = []
        self.load_bundle_calls: list[UUID] = []
        self.insert_output_calls: list[dict[str, Any]] = []
        self.finish_calls: list[dict[str, Any]] = []
        self.insert_ready_calls: list[dict[str, Any]] = []
        self.mark_published_calls: list[dict[str, Any]] = []
        self.job_attempt_calls: list[dict[str, Any]] = []
        self.commit_calls = 0
        self.rollback_calls = 0

    async def load_event_outbox(self, trigger_event_id):
        self.load_event_calls.append(trigger_event_id)
        return self.event

    async def load_judge_run(self, judge_run_id):
        self.load_judge_run_calls.append(judge_run_id)
        return self.judge_run

    async def load_bundle(self, bundle_id):
        self.load_bundle_calls.append(bundle_id)
        return self.bundle

    async def load_existing_judge_output(self, judge_run_id):
        outputs = [output for output in self.outputs if output.judge_run_id == judge_run_id]
        return ExistingJudgeOutputLookup(output=outputs[0] if outputs else None, count=len(outputs))

    async def insert_judge_output(self, **kwargs):
        self.insert_output_calls.append(kwargs)
        output = JudgeOutputRecord(
            judge_output_id=JUDGE_OUTPUT_ID,
            judge_run_id=kwargs["judge_run_id"],
            candidate_group_id=kwargs["candidate_group_id"],
            judge_schema_version=kwargs["judge_schema_version"],
            payload_json=kwargs["payload_json"],
            model_proposed_verdict=kwargs["model_proposed_verdict"],
            model_confidence_band=kwargs["model_confidence_band"],
        )
        self.outputs.append(output)
        return output.judge_output_id

    async def finish_judge_run_succeeded(self, **kwargs) -> None:
        self.finish_calls.append(kwargs)
        if self.judge_run is not None:
            self.judge_run = replace(self.judge_run, status="succeeded")

    async def insert_or_load_judge_output_ready_outbox(self, **kwargs):
        self.insert_ready_calls.append(kwargs)
        if self.ready_outbox is not None:
            return self.ready_outbox, False
        self.ready_outbox = _ready_outbox(
            judge_run_id=kwargs["judge_run_id"],
            judge_output_id=kwargs["judge_output_id"],
            status="pending",
        )
        return self.ready_outbox, True

    async def mark_output_ready_outbox_published(self, **kwargs) -> None:
        self.mark_published_calls.append(kwargs)
        if self.ready_outbox is not None:
            self.ready_outbox = replace(self.ready_outbox, status="published")

    async def insert_publish_job_attempt(self, **kwargs) -> None:
        self.job_attempt_calls.append(kwargs)

    async def commit(self) -> None:
        self.commit_calls += 1

    async def rollback(self) -> None:
        self.rollback_calls += 1


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository
        self.calls = 0
        self.closed = False

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True

        async def close() -> None:
            self.closed = True

        return BoundedJudgeOpenAIFakeExecutionRepositoryHandle(
            repository=self.repository,
            close=close,
        )


class RogueRouteResolver:
    def resolve(self, row):
        del row
        return QueueRoute("q.notification.send", "notify")


def _runtime_config() -> BoundedJudgeOpenAIFakeExecutionRuntimeConfig:
    return BoundedJudgeOpenAIFakeExecutionRuntimeConfig(
        database_url=DB_LOCATOR,
        redis_url=REDIS_LOCATOR,
        input_queue_name="q.analysis.judge",
        max_output_tokens=900,
        request_timeout_sec=30.0,
    )


def _missing_runtime_config() -> BoundedJudgeOpenAIFakeExecutionRuntimeConfig:
    raise BoundedJudgeOpenAIFakeExecutionError("database_url_missing")


def _raising_runtime_config() -> BoundedJudgeOpenAIFakeExecutionRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _redis_message(**field_overrides: str) -> RedisStreamMessage:
    fields = {
        "idempotency_key": "private-dedupe-key",
        "job_id": str(TRIGGER_EVENT_ID),
        "not_before": "",
        "pipeline_run_id": "",
        "root_object_id": str(JUDGE_RUN_ID),
        "root_object_type": "judge_run",
        "stage_name": "judge",
        "trigger_event_id": str(TRIGGER_EVENT_ID),
    }
    fields.update(field_overrides)
    return RedisStreamMessage(message_id=REDIS_MESSAGE_ID, fields=fields)


def _event(**overrides: Any) -> JudgeCallOutboxRecord:
    payload = {
        "judge_run_id": str(JUDGE_RUN_ID),
        "bundle_id": str(BUNDLE_ID),
        "model": "gpt-5.4-mini",
        "reasoning_effort": "low",
        "prompt_version": "judge_text_idea_primary_v1",
        "prompt_cache_key": RAW_PROMPT_CACHE_KEY,
    }
    payload.update(overrides.pop("payload_overrides", {}))
    for key in overrides.pop("payload_remove", ()):
        payload.pop(key, None)
    return JudgeCallOutboxRecord(
        event_id=overrides.pop("event_id", TRIGGER_EVENT_ID),
        event_type=overrides.pop("event_type", "judge.call.requested.v1"),
        aggregate_type=overrides.pop("aggregate_type", "judge_run"),
        aggregate_id=overrides.pop("aggregate_id", JUDGE_RUN_ID),
        payload_json=overrides.pop("payload_json", payload),
        status=overrides.pop("status", "published"),
    )


def _judge_run(**overrides: Any) -> JudgeRunRecord:
    return JudgeRunRecord(
        judge_run_id=overrides.pop("judge_run_id", JUDGE_RUN_ID),
        bundle_id=overrides.pop("bundle_id", BUNDLE_ID),
        judge_profile=overrides.pop("judge_profile", "text_idea_primary"),
        model=overrides.pop("model", "gpt-5.4-mini"),
        reasoning_effort=overrides.pop("reasoning_effort", "low"),
        prompt_version=overrides.pop("prompt_version", "judge_text_idea_primary_v1"),
        schema_version=overrides.pop("schema_version", "judge_output_v1"),
        policy_version=overrides.pop("policy_version", "verdict_policy_v1"),
        prompt_cache_key=overrides.pop("prompt_cache_key", RAW_PROMPT_CACHE_KEY),
        status=overrides.pop("status", "pending"),
        schema_retry_count=overrides.pop("schema_retry_count", 0),
    )


def _bundle_record(**overrides: Any) -> BundleEnvelopeRecord:
    bundle = BundleJudgeContext(
        bundle_id=overrides.pop("bundle_id", BUNDLE_ID),
        candidate_group_id=overrides.pop("candidate_group_id", CANDIDATE_GROUP_ID),
        current_primary_artifact_id=overrides.pop("current_primary_artifact_id", ARTIFACT_ID),
        primary_summary=overrides.pop(
            "primary_summary",
            {"headline": RAW_PRIMARY_SENTINEL, "summary": "bounded fake execution context"},
        ),
        supporting_summaries_json=overrides.pop("supporting_summaries_json", [{"kind": "text"}]),
        discovered_links_summary_json=overrides.pop("discovered_links_summary_json", []),
        evidence_limitations=overrides.pop("evidence_limitations", ["limited text evidence"]),
        token_budget_profile=overrides.pop("token_budget_profile", "small"),
        reroot_count=overrides.pop("reroot_count", 0),
    )
    return BundleEnvelopeRecord(
        bundle=bundle,
        ready_for_analysis=overrides.pop("ready_for_analysis", True),
    )


def _judge_output(**overrides: Any) -> JudgeOutputRecord:
    payload = build_deterministic_fake_judge_output_payload(
        candidate_group_id=overrides.get("candidate_group_id", CANDIDATE_GROUP_ID),
        judge_schema_version="judge_output_v1",
    )
    return JudgeOutputRecord(
        judge_output_id=overrides.pop("judge_output_id", JUDGE_OUTPUT_ID),
        judge_run_id=overrides.pop("judge_run_id", JUDGE_RUN_ID),
        candidate_group_id=overrides.pop("candidate_group_id", CANDIDATE_GROUP_ID),
        judge_schema_version=overrides.pop("judge_schema_version", "judge_output_v1"),
        payload_json=overrides.pop("payload_json", payload),
        model_proposed_verdict=overrides.pop("model_proposed_verdict", "later"),
        model_confidence_band=overrides.pop("model_confidence_band", "low"),
    )


def _ready_outbox(*, judge_run_id: UUID, judge_output_id: UUID, status: str) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=READY_EVENT_ID,
        event_type="judge.output.ready.v1",
        aggregate_type="judge_run",
        aggregate_id=judge_run_id,
        dedupe_key=f"judge-output-ready:{judge_run_id}:{judge_output_id}",
        payload_json={
            "judge_run_id": str(judge_run_id),
            "judge_output_id": str(judge_output_id),
            "finish_reason": "fake_structured_output",
            "refusal_detected": False,
        },
        status=status,
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _approved_config(**overrides: Any) -> BoundedJudgeOpenAIFakeExecutionConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_redis_read": True,
        "allow_redis_publish": True,
        "allow_database_read": True,
        "allow_database_write": True,
        "allow_fake_openai": True,
        "redis_message_suffix": "356724-0",
        "trigger_event_suffix": "a1c22bcb",
        "scan_limit": 25,
    }
    values.update(overrides)
    return BoundedJudgeOpenAIFakeExecutionConfig(**values)


def _run(
    *,
    config: BoundedJudgeOpenAIFakeExecutionConfig | None = None,
    messages: list[RedisStreamMessage] | None = None,
    event: JudgeCallOutboxRecord | None = None,
    judge_run: JudgeRunRecord | None = None,
    bundle: BundleEnvelopeRecord | None = None,
    existing_outputs: list[JudgeOutputRecord] | None = None,
    ready_outbox: OutboxEventRow | None = None,
    runtime_loader=None,
    route_resolver=None,
):
    reader_builder = FakeRedisReaderBuilder(FakeRedisReader(messages if messages is not None else [_redis_message()]))
    repository_builder = FakeRepositoryBuilder(
        FakeRepository(
            event=_event() if event is None else event,
            judge_run=_judge_run() if judge_run is None else judge_run,
            bundle=_bundle_record() if bundle is None else bundle,
            existing_outputs=existing_outputs,
            ready_outbox=ready_outbox,
        )
    )
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())
    fake_factory = RecordingFakeClientFactory()
    result = run_bounded_judge_openai_fake_execution_sync(
        config or _approved_config(),
        runtime_config_loader=runtime_loader or _runtime_config,
        redis_reader_builder=reader_builder,
        repository_builder=repository_builder,
        redis_publisher_builder=publisher_builder,
        fake_client_factory=fake_factory,
        route_resolver=route_resolver,
    )
    return result, reader_builder, repository_builder, publisher_builder, fake_factory


def test_success_appends_output_finalizes_run_publishes_validate_and_writes_attempt() -> None:
    result, reader_builder, repository_builder, publisher_builder, fake_factory = _run()
    report = result.to_sanitized_dict()
    repository = repository_builder.repository
    publisher = publisher_builder.publisher
    rendered = json.dumps(report, ensure_ascii=False, sort_keys=True)

    assert result.ok is True
    assert report["status"] == "published"
    assert report["target_redis_message_id_suffix"] == "356724-0"
    assert report["target_trigger_event_id_suffix"] == "a1c22bcb"
    assert report["target_judge_run_id_suffix"] == "7a111d13"
    assert report["target_bundle_id_suffix"] == "c51bd89e"
    assert report["target_candidate_group_suffix"] == "42c0d691"
    assert report["target_judge_output_id_suffix"] == "aaabbb01"
    assert report["judge_output_written"] is True
    assert report["judge_run_status"] == "succeeded"
    assert report["judge_output_ready_outbox_written"] is True
    assert report["judge_output_ready_event_suffix"] == "dddeee02"
    assert report["judge_output_ready_published"] is True
    assert report["q_analysis_validate_message_id_suffix"] == "456789-0"
    assert report["model"] == "gpt-5.4-mini"
    assert report["reasoning_effort"] == "low"
    assert report["judge_profile"] == "text_idea_primary"
    assert report["prompt_version"] == "judge_text_idea_primary_v1"
    assert report["schema_version_value"] == "judge_output_v1"
    assert report["policy_version"] == "verdict_policy_v1"
    assert report["fake_openai_called"] is True
    assert report["openai_called"] is False
    assert report["redis_read_attempted"] is True
    assert report["redis_publish_attempted"] is True
    assert report["redis_ack_called"] is False
    assert report["redis_consume_called"] is False
    assert report["database_read_attempted"] is True
    assert report["database_write_attempted"] is True
    assert report["validator_called"] is False
    assert report["policy_called"] is False
    assert report["notifier_called"] is False
    assert report["telegram_send_called"] is False
    assert len(repository.outputs) == 1
    assert repository.judge_run is not None and repository.judge_run.status == "succeeded"
    assert repository.ready_outbox is not None and repository.ready_outbox.status == "published"
    assert len(repository.job_attempt_calls) == 1
    assert repository.job_attempt_calls[0]["queue_name"] == "q.analysis.validate"
    assert repository.job_attempt_calls[0]["stage_name"] == "analysis_validate"
    assert len(publisher.calls) == 1
    route, message = publisher.calls[0]
    assert route == QueueRoute("q.analysis.validate", "analysis_validate")
    assert message.as_stream_fields() == {
        "job_id": str(READY_EVENT_ID),
        "stage_name": "analysis_validate",
        "root_object_type": "judge_run",
        "root_object_id": str(JUDGE_RUN_ID),
        "idempotency_key": f"judge-output-ready:{JUDGE_RUN_ID}:{JUDGE_OUTPUT_ID}",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(READY_EVENT_ID),
    }
    assert len(fake_factory.clients) == 1
    assert len(fake_factory.clients[0].calls) == 1
    assert reader_builder.closed is True
    assert repository_builder.closed is True
    assert publisher_builder.closed is True
    assert RAW_PROMPT_CACHE_KEY not in rendered
    assert RAW_PRIMARY_SENTINEL not in rendered
    assert DB_LOCATOR not in rendered
    assert REDIS_LOCATOR not in rendered
    assert str(TRIGGER_EVENT_ID) not in rendered
    assert str(JUDGE_RUN_ID) not in rendered
    assert str(BUNDLE_ID) not in rendered
    assert str(CANDIDATE_GROUP_ID) not in rendered
    assert "Bounded fake OpenAI response" not in rendered


@pytest.mark.parametrize(
    ("config", "expected_error"),
    [
        (BoundedJudgeOpenAIFakeExecutionConfig(), "operator_approval_missing"),
        (_approved_config(allow_database_write=False), "database_write_not_allowed"),
        (_approved_config(allow_redis_publish=False), "redis_publish_not_allowed"),
        (_approved_config(allow_fake_openai=False), "fake_openai_not_allowed"),
    ],
)
def test_authority_gates_fail_closed_before_runtime_config(config, expected_error) -> None:
    result, reader_builder, repository_builder, publisher_builder, fake_factory = _run(
        config=config,
        runtime_loader=_raising_runtime_config,
    )
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["status"] == "blocked"
    assert report["error_code"] == expected_error
    assert report["redis_read_attempted"] is False
    assert report["database_read_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert report["fake_openai_called"] is False
    assert reader_builder.calls == 0
    assert repository_builder.calls == 0
    assert publisher_builder.calls == 0
    assert fake_factory.clients == []


def test_runtime_config_error_blocks_before_redis_or_database() -> None:
    result, _, repository_builder, publisher_builder, _ = _run(runtime_loader=_missing_runtime_config)
    report = result.to_sanitized_dict()

    assert report["error_code"] == "database_url_missing"
    assert report["redis_read_attempted"] is False
    assert report["database_read_attempted"] is False
    assert repository_builder.calls == 0
    assert publisher_builder.calls == 0


def test_redis_input_message_must_remain_thin_id_only() -> None:
    result, _, repository_builder, publisher_builder, _ = _run(
        messages=[_redis_message(payload_json="{private}", bundle_id=str(BUNDLE_ID))]
    )
    report = result.to_sanitized_dict()

    assert report["error_code"] == "redis_message_forbidden_business_fields"
    assert report["database_read_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert repository_builder.calls == 0
    assert publisher_builder.calls == 0


def test_rerun_with_judge_run_already_succeeded_is_noop_without_duplicates() -> None:
    existing = _judge_output()
    result, _, repository_builder, publisher_builder, fake_factory = _run(
        judge_run=_judge_run(status="succeeded"),
        existing_outputs=[existing],
        ready_outbox=_ready_outbox(
            judge_run_id=JUDGE_RUN_ID,
            judge_output_id=JUDGE_OUTPUT_ID,
            status="published",
        ),
    )
    report = result.to_sanitized_dict()
    repository = repository_builder.repository

    assert result.ok is True
    assert report["status"] == "noop"
    assert report["target_judge_output_id_suffix"] == "aaabbb01"
    assert report["judge_run_status"] == "succeeded"
    assert report["judge_output_written"] is False
    assert report["judge_output_ready_published"] is False
    assert len(repository.outputs) == 1
    assert repository.insert_output_calls == []
    assert repository.finish_calls == []
    assert repository.insert_ready_calls == []
    assert publisher_builder.calls == 0
    assert fake_factory.clients == []


def test_rerun_with_existing_output_reuses_output_without_duplicate_and_publishes_pending_ready_event() -> None:
    existing = _judge_output()
    result, _, repository_builder, publisher_builder, fake_factory = _run(
        existing_outputs=[existing],
        ready_outbox=_ready_outbox(
            judge_run_id=JUDGE_RUN_ID,
            judge_output_id=JUDGE_OUTPUT_ID,
            status="pending",
        ),
    )
    report = result.to_sanitized_dict()
    repository = repository_builder.repository

    assert result.ok is True
    assert report["status"] == "published"
    assert report["judge_output_written"] is False
    assert report["target_judge_output_id_suffix"] == "aaabbb01"
    assert len(repository.outputs) == 1
    assert repository.insert_output_calls == []
    assert len(repository.finish_calls) == 1
    assert len(repository.insert_ready_calls) == 1
    assert len(publisher_builder.publisher.calls) == 1
    assert fake_factory.clients == []


def test_rerun_with_published_output_ready_outbox_does_not_publish_duplicate_redis_message() -> None:
    existing = _judge_output()
    result, _, repository_builder, publisher_builder, fake_factory = _run(
        existing_outputs=[existing],
        ready_outbox=_ready_outbox(
            judge_run_id=JUDGE_RUN_ID,
            judge_output_id=JUDGE_OUTPUT_ID,
            status="published",
        ),
    )
    report = result.to_sanitized_dict()
    repository = repository_builder.repository

    assert result.ok is True
    assert report["status"] == "noop"
    assert report["judge_output_written"] is False
    assert report["judge_output_ready_published"] is False
    assert report["redis_publish_attempted"] is False
    assert publisher_builder.calls == 0
    assert publisher_builder.publisher.calls == []
    assert len(repository.outputs) == 1
    assert len(repository.finish_calls) == 1
    assert fake_factory.clients == []


def test_duplicate_existing_judge_outputs_block_without_more_writes() -> None:
    result, _, repository_builder, publisher_builder, fake_factory = _run(
        existing_outputs=[
            _judge_output(judge_output_id=JUDGE_OUTPUT_ID),
            _judge_output(judge_output_id=UUID("00000000-0000-4000-8000-0000aaabbb02")),
        ]
    )
    report = result.to_sanitized_dict()
    repository = repository_builder.repository

    assert result.ok is False
    assert report["error_code"] == "judge_output_count_exceeded"
    assert repository.insert_output_calls == []
    assert repository.finish_calls == []
    assert publisher_builder.calls == 0
    assert fake_factory.clients == []


def test_no_validator_policy_notifier_or_external_calls_are_reported() -> None:
    result, _, _, _, _ = _run()
    report = result.to_sanitized_dict()

    assert report["validator_called"] is False
    assert report["policy_called"] is False
    assert report["notifier_called"] is False
    assert report["telegram_send_called"] is False
    assert report["github_api_called"] is False
    assert report["x_api_called"] is False
    assert report["web_fetch_called"] is False
    assert report["openai_called"] is False


def test_output_redaction_excludes_prompt_bundle_full_ids_and_urls() -> None:
    result, _, _, _, _ = _run()
    rendered = json.dumps(result.to_sanitized_dict(), ensure_ascii=False, sort_keys=True)

    forbidden = [
        RAW_PROMPT_CACHE_KEY,
        RAW_PRIMARY_SENTINEL,
        DB_LOCATOR,
        REDIS_LOCATOR,
        str(TRIGGER_EVENT_ID),
        str(JUDGE_RUN_ID),
        str(BUNDLE_ID),
        str(CANDIDATE_GROUP_ID),
        str(JUDGE_OUTPUT_ID),
        str(READY_EVENT_ID),
        "developer_prompt",
        "user_context",
        "primary_summary",
        "Bounded fake OpenAI response",
    ]
    for value in forbidden:
        assert value not in rendered


def test_fake_payload_is_schema_valid_stable_and_marked_fake() -> None:
    left = build_deterministic_fake_judge_output_payload(
        candidate_group_id=CANDIDATE_GROUP_ID,
        judge_schema_version="judge_output_v1",
    )
    right = build_deterministic_fake_judge_output_payload(
        candidate_group_id=CANDIDATE_GROUP_ID,
        judge_schema_version="judge_output_v1",
    )

    assert left == right
    assert tuple(left) == (
        "judge_schema_version",
        "candidate_group_id",
        "headline",
        "summary_one_line_ko",
        "skeptical_take_ko",
        "why_it_might_matter_ko",
        "comparables",
        "scores",
        "reason_codes",
        "red_flags_ko",
        "evidence_limitations_ko",
        "recommended_action_ko",
        "freshness_note_ko",
        "model_proposed_verdict",
        "model_confidence_band",
    )
    assert left["judge_schema_version"] == "judge_output_v1"
    assert left["candidate_group_id"] == str(CANDIDATE_GROUP_ID)
    assert "bounded_fake_openai_execution" in left["reason_codes"]
    assert "comparison_gap" in left["reason_codes"]
    assert left["model_proposed_verdict"] == "later"
    assert left["model_confidence_band"] == "low"
    assert set(left["scores"]) == {
        "novelty",
        "practical_usefulness",
        "evidence_strength",
        "hype_penalty",
        "confidence",
        "code_quality",
        "maintenance_signal",
        "specificity",
        "reproducibility_signal",
    }


def test_output_ready_route_maps_to_analysis_validate() -> None:
    route = OutboxRouteResolver().resolve(
        _ready_outbox(
            judge_run_id=JUDGE_RUN_ID,
            judge_output_id=JUDGE_OUTPUT_ID,
            status="pending",
        )
    )

    assert route == QueueRoute("q.analysis.validate", "analysis_validate")


def test_route_drift_blocks_before_redis_publish() -> None:
    result, _, repository_builder, publisher_builder, _ = _run(route_resolver=RogueRouteResolver())
    report = result.to_sanitized_dict()

    assert report["error_code"] == "route_not_allowed"
    assert report["redis_publish_attempted"] is False
    assert repository_builder.repository.insert_output_calls == []
    assert repository_builder.repository.finish_calls == []
    assert publisher_builder.publisher.calls == []
    assert repository_builder.repository.job_attempt_calls == []


def test_ast_guard_for_bounded_fake_runner_and_tool_paths() -> None:
    for path in (RUNNER_PATH, TOOL_PATH):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported_modules: set[str] = set()
        called_attrs: set[str] = set()
        called_names: set[str] = set()
        names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported_modules.add(node.module or "")
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    called_attrs.add(node.func.attr.lower())
                elif isinstance(node.func, ast.Name):
                    called_names.add(node.func.id.lower())
            elif isinstance(node, ast.Name):
                names.add(node.id)

        forbidden_import_fragments = (
            "notifier_telegram",
            "policy_engine",
            "analysis_validator",
            "collector_telegram",
            "gh_enricher",
            "x_enricher",
            "web_enricher",
            "subprocess",
        )
        for module in imported_modules:
            assert module not in {"openai"} and not module.startswith("openai.")
            assert not any(fragment in module for fragment in forbidden_import_fragments)
        assert "AsyncOpenAI" not in names
        assert {"xreadgroup", "xack", "xgroup", "xgroup_create", "xclaim", "xautoclaim"} & called_attrs == set()
        assert {"run_forever", "subprocess", "systemd", "docker", "alembic"} & called_attrs == set()
        assert {"subprocess", "systemd", "docker", "alembic"} & called_names == set()
