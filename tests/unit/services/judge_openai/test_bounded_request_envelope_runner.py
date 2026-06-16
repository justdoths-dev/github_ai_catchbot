from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from src.services.judge_openai.bounded_request_envelope_runner import (
    BoundedJudgeOpenAIRequestEnvelopeConfig,
    BoundedJudgeOpenAIRequestEnvelopeError,
    BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig,
    BoundedJudgeOpenAIRedisReaderHandle,
    BoundedJudgeOpenAIEnvelopeRepositoryHandle,
    BundleEnvelopeRecord,
    JudgeCallOutboxRecord,
    RedisStreamMessage,
    run_bounded_judge_openai_request_envelope_sync,
)
from src.services.judge_openai.models import BundleJudgeContext, JudgeRunRecord
from src.services.judge_openai.request_shape import JudgeOpenAIRequestEnvelopeBuilder


ROOT = Path(__file__).resolve().parents[4]
RUNNER_PATH = ROOT / "src/services/judge_openai/bounded_request_envelope_runner.py"
TOOL_PATH = ROOT / "tools/bounded_judge_openai_request_envelope_runner.py"

TRIGGER_EVENT_ID = UUID("00000000-0000-4000-8000-0000a1c22bcb")
JUDGE_RUN_ID = UUID("00000000-0000-4000-8000-00007a111d13")
BUNDLE_ID = UUID("00000000-0000-4000-8000-0000c51bd89e")
CANDIDATE_GROUP_ID = UUID("00000000-0000-4000-8000-000042c0d691")
ARTIFACT_ID = UUID("00000000-0000-4000-8000-000012345678")
REDIS_MESSAGE_ID = "1700000356724-0"
RAW_PROMPT_CACHE_KEY = "judge:github_primary:private-cache-key"
RAW_PRIMARY_SENTINEL = "sentinel private primary summary should not print"
_DEFAULT = object()


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
        self.closed = False

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.redis_reader_created = True

        async def close() -> None:
            self.closed = True

        return BoundedJudgeOpenAIRedisReaderHandle(reader=self.reader, close=close)


class FakeRepository:
    def __init__(
        self,
        *,
        event: JudgeCallOutboxRecord | None,
        judge_run: JudgeRunRecord | None,
        bundle: BundleEnvelopeRecord | None,
    ) -> None:
        self.event = event
        self.judge_run = judge_run
        self.bundle = bundle
        self.event_calls: list[UUID] = []
        self.judge_run_calls: list[UUID] = []
        self.bundle_calls: list[UUID] = []

    async def load_event_outbox(self, trigger_event_id):
        self.event_calls.append(trigger_event_id)
        return self.event

    async def load_judge_run(self, judge_run_id):
        self.judge_run_calls.append(judge_run_id)
        return self.judge_run

    async def load_bundle(self, bundle_id):
        self.bundle_calls.append(bundle_id)
        return self.bundle


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository
        self.closed = False

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True

        async def close() -> None:
            self.closed = True

        return BoundedJudgeOpenAIEnvelopeRepositoryHandle(repository=self.repository, close=close)


def _runtime_config() -> BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig:
    return BoundedJudgeOpenAIRequestEnvelopeRuntimeConfig(
        database_url="db-url-sentinel",
        redis_url="redis-url-sentinel",
        queue_name="q.analysis.judge",
        request_timeout_sec=30.0,
        max_output_tokens=900,
    )


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
        "prompt_version": "judge_github_primary_v1",
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
        judge_profile=overrides.pop("judge_profile", "github_primary"),
        model=overrides.pop("model", "gpt-5.4-mini"),
        reasoning_effort=overrides.pop("reasoning_effort", "low"),
        prompt_version=overrides.pop("prompt_version", "judge_github_primary_v1"),
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
            {"headline": RAW_PRIMARY_SENTINEL, "summary": "bounded request envelope context"},
        ),
        supporting_summaries_json=overrides.pop("supporting_summaries_json", [{"kind": "repo"}]),
        discovered_links_summary_json=overrides.pop("discovered_links_summary_json", [{"kind": "link"}]),
        evidence_limitations=overrides.pop("evidence_limitations", ["limited public metadata"]),
        token_budget_profile=overrides.pop("token_budget_profile", "small"),
        reroot_count=overrides.pop("reroot_count", 0),
    )
    return BundleEnvelopeRecord(
        bundle=bundle,
        ready_for_analysis=overrides.pop("ready_for_analysis", True),
    )


def _run(
    *,
    config: BoundedJudgeOpenAIRequestEnvelopeConfig | None = None,
    messages: list[RedisStreamMessage] | None = None,
    event: object = _DEFAULT,
    judge_run: object = _DEFAULT,
    bundle: object = _DEFAULT,
    runtime_loader=None,
    envelope_builder_factory=None,
):
    reader_builder = FakeRedisReaderBuilder(FakeRedisReader(messages if messages is not None else [_redis_message()]))
    repository_builder = FakeRepositoryBuilder(
        FakeRepository(
            event=_event() if event is _DEFAULT else event,
            judge_run=_judge_run() if judge_run is _DEFAULT else judge_run,
            bundle=_bundle_record() if bundle is _DEFAULT else bundle,
        )
    )
    result = run_bounded_judge_openai_request_envelope_sync(
        config
        or BoundedJudgeOpenAIRequestEnvelopeConfig(
            operator_approved=True,
            allow_runtime_config=True,
            allow_redis_read=True,
            allow_database_read=True,
            redis_message_suffix="356724-0",
            trigger_event_suffix="a1c22bcb",
            scan_limit=25,
        ),
        runtime_config_loader=runtime_loader or _runtime_config,
        redis_reader_builder=reader_builder,
        repository_builder=repository_builder,
        envelope_builder_factory=envelope_builder_factory,
    )
    return result, reader_builder, repository_builder


def test_success_builds_sanitized_reusable_request_envelope() -> None:
    result, reader_builder, repository_builder = _run()
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, ensure_ascii=False, sort_keys=True)

    assert result.ok is True
    assert report["status"] == "request_envelope_built"
    assert report["queue_name"] == "q.analysis.judge"
    assert report["stage_name"] == "judge"
    assert report["selector_type"] == "redis_message_suffix+trigger_event_suffix"
    assert report["target_redis_message_id_suffix"] == "356724-0"
    assert report["target_trigger_event_id_suffix"] == "a1c22bcb"
    assert report["target_judge_run_id_suffix"] == "7a111d13"
    assert report["target_bundle_id_suffix"] == "c51bd89e"
    assert report["target_candidate_group_suffix"] == "42c0d691"
    assert report["request_envelope_built"] is True
    assert report["structured_output_schema_present"] is True
    assert report["model"] == "gpt-5.4-mini"
    assert report["reasoning_effort"] == "low"
    assert report["judge_profile"] == "github_primary"
    assert report["prompt_version"] == "judge_github_primary_v1"
    assert report["schema_version_value"] == "judge_output_v1"
    assert report["policy_version"] == "verdict_policy_v1"
    assert report["prompt_cache_key_present"] is True
    assert report["bundle_ready_for_analysis"] is True
    assert report["primary_summary_present"] is True
    assert report["supporting_summary_count"] == 1
    assert report["discovered_link_count"] == 1
    assert report["evidence_limitation_count"] == 1
    assert report["context_character_count"] > 0
    assert report["side_effects"] == {key: False for key in report["side_effects"]}
    assert RAW_PROMPT_CACHE_KEY not in rendered
    assert RAW_PRIMARY_SENTINEL not in rendered
    assert "private-dedupe-key" not in rendered
    assert "db-url-sentinel" not in rendered
    assert "redis-url-sentinel" not in rendered
    assert reader_builder.closed is True
    assert repository_builder.closed is True


def test_no_approval_blocks_before_runtime_config_or_reads() -> None:
    def forbidden_runtime_loader():
        raise AssertionError("runtime loader must not be called")

    result, _, _ = _run(
        config=BoundedJudgeOpenAIRequestEnvelopeConfig(),
        runtime_loader=forbidden_runtime_loader,
    )
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == "operator_approval_missing"
    assert report["redis_read_attempted"] is False
    assert report["database_read_attempted"] is False


def test_missing_runtime_config_blocks_before_redis_or_database_reads() -> None:
    def missing_runtime_loader():
        raise BoundedJudgeOpenAIRequestEnvelopeError("database_url_missing")

    result, _, _ = _run(
        config=BoundedJudgeOpenAIRequestEnvelopeConfig(
            operator_approved=True,
            allow_runtime_config=True,
            allow_redis_read=True,
            allow_database_read=True,
            redis_message_suffix="356724-0",
        ),
        runtime_loader=missing_runtime_loader,
    )
    report = result.to_sanitized_dict()

    assert report["error_code"] == "database_url_missing"
    assert report["redis_read_attempted"] is False
    assert report["database_read_attempted"] is False


@pytest.mark.parametrize(
    ("config", "expected_error"),
    [
        (BoundedJudgeOpenAIRequestEnvelopeConfig(operator_approved=True), "target_missing"),
        (
            BoundedJudgeOpenAIRequestEnvelopeConfig(
                operator_approved=True,
                redis_message_suffix="bad:suffix",
            ),
            "invalid_redis_message_suffix",
        ),
        (
            BoundedJudgeOpenAIRequestEnvelopeConfig(
                operator_approved=True,
                trigger_event_suffix="not-a-suffix",
            ),
            "invalid_trigger_event_suffix",
        ),
        (
            BoundedJudgeOpenAIRequestEnvelopeConfig(
                operator_approved=True,
                redis_message_suffix="356724-0",
                scan_limit=0,
            ),
            "invalid_scan_limit",
        ),
    ],
)
def test_selector_validation_blocks_before_runtime(config, expected_error) -> None:
    result, _, _ = _run(config=config)

    assert result.to_sanitized_dict()["error_code"] == expected_error
    assert result.to_sanitized_dict()["redis_read_attempted"] is False


@pytest.mark.parametrize(
    ("messages", "expected_error"),
    [
        ([], "redis_message_not_found"),
        (
            [
                _redis_message(),
                RedisStreamMessage(message_id="2700000356724-0", fields=_redis_message().fields),
            ],
            "redis_message_count_exceeded",
        ),
    ],
)
def test_redis_suffix_match_count_is_exactly_one(messages, expected_error) -> None:
    result, _, _ = _run(messages=messages)

    assert result.to_sanitized_dict()["error_code"] == expected_error
    assert result.to_sanitized_dict()["database_read_attempted"] is False


@pytest.mark.parametrize(
    ("message", "expected_error"),
    [
        (_redis_message(stage_name="route"), "redis_message_wrong_stage"),
        (_redis_message(root_object_type="candidate_group"), "redis_message_wrong_root_object_type"),
        (
            RedisStreamMessage(
                message_id=REDIS_MESSAGE_ID,
                fields={key: value for key, value in _redis_message().fields.items() if key != "trigger_event_id"},
            ),
            "redis_message_required_fields_missing",
        ),
        (_redis_message(trigger_event_id="not-a-uuid"), "redis_message_invalid_trigger_event_id"),
        (_redis_message(job_id=str(UUID("00000000-0000-4000-8000-0000ffffffff"))), "redis_message_job_trigger_mismatch"),
        (_redis_message(bundle_id=str(BUNDLE_ID)), "redis_message_forbidden_business_fields"),
    ],
)
def test_redis_message_shape_failures_block_before_database(message, expected_error) -> None:
    result, _, _ = _run(
        config=BoundedJudgeOpenAIRequestEnvelopeConfig(
            operator_approved=True,
            allow_runtime_config=True,
            allow_redis_read=True,
            allow_database_read=True,
            redis_message_suffix="356724-0",
            scan_limit=25,
        ),
        messages=[message],
    )

    report = result.to_sanitized_dict()
    assert report["error_code"] == expected_error
    assert report["database_read_attempted"] is False


@pytest.mark.parametrize(
    ("event", "expected_error"),
    [
        (None, "event_outbox_missing"),
        (_event(event_type="analysis.requested.v1"), "event_outbox_wrong_event_type"),
        (_event(status="pending"), "event_outbox_not_published"),
        (_event(aggregate_type="candidate_group"), "event_outbox_wrong_aggregate_type"),
        (
            _event(aggregate_id=UUID("00000000-0000-4000-8000-0000eeeeeeee")),
            "event_outbox_aggregate_mismatch",
        ),
        (_event(payload_json=[]), "event_payload_malformed"),
        (_event(payload_remove=("model",)), "event_payload_missing_required_field"),
        (
            _event(payload_overrides={"judge_run_id": str(UUID("00000000-0000-4000-8000-0000eeeeeeee"))}),
            "event_payload_judge_run_id_mismatch",
        ),
    ],
)
def test_event_outbox_rehydration_failures(event, expected_error) -> None:
    result, _, _ = _run(event=event)

    report = result.to_sanitized_dict()
    assert report["error_code"] == expected_error
    assert report["redis_read_attempted"] is True
    assert report["database_read_attempted"] is True


@pytest.mark.parametrize(
    ("judge_run", "expected_error"),
    [
        (None, "judge_run_missing"),
        (_judge_run(status="running"), "judge_run_not_pending"),
        (_judge_run(bundle_id=UUID("00000000-0000-4000-8000-0000eeeeeeee")), "judge_run_bundle_mismatch"),
        (_judge_run(model="gpt-5.4"), "judge_run_model_mismatch"),
        (_judge_run(prompt_cache_key="other-cache-key"), "judge_run_prompt_cache_key_mismatch"),
        (_judge_run(policy_version=""), "judge_run_required_field_missing"),
    ],
)
def test_judge_run_rehydration_failures(judge_run, expected_error) -> None:
    result, _, _ = _run(judge_run=judge_run)

    assert result.to_sanitized_dict()["error_code"] == expected_error


@pytest.mark.parametrize(
    ("bundle", "expected_error"),
    [
        (None, "bundle_missing"),
        (_bundle_record(ready_for_analysis=False), "bundle_not_ready"),
        (_bundle_record(candidate_group_id=None), "bundle_candidate_group_missing"),
        (_bundle_record(current_primary_artifact_id=None), "bundle_primary_artifact_missing"),
        (_bundle_record(primary_summary={}), "bundle_primary_summary_missing"),
        (_bundle_record(supporting_summaries_json={}), "bundle_supporting_summaries_invalid"),
        (_bundle_record(discovered_links_summary_json={}), "bundle_discovered_links_invalid"),
        (_bundle_record(evidence_limitations={}), "bundle_evidence_limitations_invalid"),
    ],
)
def test_bundle_rehydration_failures(bundle, expected_error) -> None:
    result, _, _ = _run(bundle=bundle)

    assert result.to_sanitized_dict()["error_code"] == expected_error


def test_request_envelope_shape_failure_is_fail_closed_without_raw_output() -> None:
    def invalid_builder_factory(runtime_config):
        del runtime_config
        return JudgeOpenAIRequestEnvelopeBuilder(max_output_tokens=0)

    result, _, _ = _run(envelope_builder_factory=invalid_builder_factory)
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, ensure_ascii=False)

    assert report["error_code"] == "request_envelope_invalid"
    assert report["request_envelope_built"] is False
    assert RAW_PROMPT_CACHE_KEY not in rendered
    assert RAW_PRIMARY_SENTINEL not in rendered


def test_ast_guard_for_bounded_runner_path() -> None:
    for path in (RUNNER_PATH, TOOL_PATH):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported_modules: set[str] = set()
        called_attrs: set[str] = set()
        called_names: set[str] = set()
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
        assert {"xreadgroup", "xack", "xadd", "run_forever"} & called_attrs == set()
        assert {"systemd", "docker", "alembic", "subprocess"} & called_names == set()
