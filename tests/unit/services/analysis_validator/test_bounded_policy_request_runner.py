from __future__ import annotations

import asyncio
import ast
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from src.services.analysis_validator.bounded_policy_request_runner import (
    BoundedAnalysisValidatorPolicyRequestConfig,
    BoundedAnalysisValidatorPolicyRequestError,
    BoundedAnalysisValidatorPolicyRequestRuntimeConfig,
    BoundedAnalysisValidatorRedisPublisherHandle,
    BoundedAnalysisValidatorRedisReaderHandle,
    BoundedAnalysisValidatorRepositoryHandle,
    BundleValidationRecord,
    JudgeOutputValidationRecord,
    JudgeRunValidationRecord,
    RedisStreamMessage,
    SqlAlchemyBoundedAnalysisValidatorPolicyRequestRepository,
    run_bounded_analysis_validator_policy_request_sync,
)
from src.services.outbox_relay.models import OutboxEventRow, QueueRoute
from src.services.outbox_relay.routing import OutboxRouteResolver


ROOT = Path(__file__).resolve().parents[4]
RUNNER_PATH = ROOT / "src/services/analysis_validator/bounded_policy_request_runner.py"
TOOL_PATH = ROOT / "tools/bounded_analysis_validator_policy_request_runner.py"

REDIS_MESSAGE_ID = "1700000508480-0"
POLICY_REDIS_MESSAGE_ID = "1700000600001-0"
TRIGGER_EVENT_ID = UUID("00000000-0000-4000-8000-00003e3b11b3")
JUDGE_RUN_ID = UUID("00000000-0000-4000-8000-00007a111d13")
JUDGE_OUTPUT_ID = UUID("00000000-0000-4000-8000-0000c7d7ef5e")
BUNDLE_ID = UUID("00000000-0000-4000-8000-0000c51bd89e")
CANDIDATE_GROUP_ID = UUID("00000000-0000-4000-8000-000042c0d691")
POLICY_APPLY_EVENT_ID = UUID("00000000-0000-4000-8000-0000aa55aa55")
DB_LOCATOR = "postgresql://operator:secret@private-db/catchbot"
REDIS_LOCATOR = "redis://:secret@private-redis/0"
RAW_PAYLOAD_SENTINEL = "private judge output payload must not print"
RAW_BUNDLE_SENTINEL = "private bundle context must not print"


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

        return BoundedAnalysisValidatorRedisReaderHandle(reader=self.reader, close=close)


class FakeRedisPublisher:
    def __init__(self, *, message_id: str = POLICY_REDIS_MESSAGE_ID) -> None:
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

        return BoundedAnalysisValidatorRedisPublisherHandle(publisher=self.publisher, close=close)


class FakeRepository:
    def __init__(
        self,
        *,
        event: OutboxEventRow | None,
        judge_run: JudgeRunValidationRecord | None,
        judge_output: JudgeOutputValidationRecord | None,
        bundle: BundleValidationRecord | None,
        policy_rows: list[OutboxEventRow] | None = None,
    ) -> None:
        self.event = event
        self.judge_run = judge_run
        self.judge_output = judge_output
        self.bundle = bundle
        self.policy_rows = list(policy_rows or [])
        self.state_transitions: list[dict[str, Any]] = []
        self.mark_published_calls: list[dict[str, Any]] = []
        self.job_attempt_calls: list[dict[str, Any]] = []
        self.commits = 0
        self.rollbacks = 0

    async def load_event_outbox(self, trigger_event_id):
        return self.event if self.event is not None and self.event.event_id == trigger_event_id else None

    async def load_judge_run(self, judge_run_id):
        return self.judge_run if self.judge_run is not None and self.judge_run.judge_run_id == judge_run_id else None

    async def load_judge_output(self, judge_output_id):
        if self.judge_output is None or self.judge_output.judge_output_id != judge_output_id:
            return None
        return self.judge_output

    async def load_bundle(self, bundle_id):
        return self.bundle if self.bundle is not None and self.bundle.bundle_id == bundle_id else None

    async def load_policy_apply_outboxes(
        self,
        *,
        judge_run_id,
        judge_output_id,
        candidate_group_id,
        bundle_id,
    ):
        del candidate_group_id, bundle_id
        dedupe_key = f"analysis-policy-apply:{judge_run_id}:{judge_output_id}"
        return [
            row
            for row in self.policy_rows
            if row.event_type == "analysis.policy.apply.v1"
            and row.aggregate_type == "judge_run"
            and row.aggregate_id == judge_run_id
            and row.dedupe_key == dedupe_key
        ][:2]

    async def insert_state_transition(self, **kwargs):
        self.state_transitions.append(kwargs)

    async def insert_or_load_policy_apply_outbox(self, **kwargs):
        dedupe_key = f"analysis-policy-apply:{kwargs['judge_run_id']}:{kwargs['judge_output_id']}"
        for row in self.policy_rows:
            if row.dedupe_key == dedupe_key:
                return row, False
        row = _policy_outbox(status="pending", **kwargs)
        self.policy_rows.append(row)
        return row, True

    async def mark_policy_apply_outbox_published(self, **kwargs) -> None:
        self.mark_published_calls.append(kwargs)
        self.policy_rows = [
            replace(row, status="published") if row.event_id == kwargs["event_id"] else row
            for row in self.policy_rows
        ]

    async def insert_publish_job_attempt(self, **kwargs) -> None:
        self.job_attempt_calls.append(kwargs)

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


class RaisingPolicyLookupRepository(FakeRepository):
    async def load_policy_apply_outboxes(
        self,
        *,
        judge_run_id,
        judge_output_id,
        candidate_group_id,
        bundle_id,
    ):
        del judge_run_id, judge_output_id, candidate_group_id, bundle_id
        raise RuntimeError(f"private sql detail {RAW_PAYLOAD_SENTINEL}")


class CapturingSqlResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class CapturingSqlSession:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), dict(params)))
        return CapturingSqlResult(self.rows)


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

        return BoundedAnalysisValidatorRepositoryHandle(repository=self.repository, close=close)


class RogueRouteResolver:
    def resolve(self, row):
        del row
        return QueueRoute("q.notification.send", "notify")


def _runtime_config() -> BoundedAnalysisValidatorPolicyRequestRuntimeConfig:
    return BoundedAnalysisValidatorPolicyRequestRuntimeConfig(
        database_url=DB_LOCATOR,
        redis_url=REDIS_LOCATOR,
        input_queue_name="q.analysis.validate",
    )


def _raising_runtime_config() -> BoundedAnalysisValidatorPolicyRequestRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _missing_runtime_config() -> BoundedAnalysisValidatorPolicyRequestRuntimeConfig:
    raise BoundedAnalysisValidatorPolicyRequestError("database_url_missing")


def _redis_message(**field_overrides: str) -> RedisStreamMessage:
    fields = {
        "idempotency_key": "private-dedupe-key",
        "job_id": str(TRIGGER_EVENT_ID),
        "not_before": "",
        "pipeline_run_id": "",
        "root_object_id": str(JUDGE_RUN_ID),
        "root_object_type": "judge_run",
        "stage_name": "analysis_validate",
        "trigger_event_id": str(TRIGGER_EVENT_ID),
    }
    fields.update(field_overrides)
    return RedisStreamMessage(message_id=REDIS_MESSAGE_ID, fields=fields)


def _event(**overrides: Any) -> OutboxEventRow:
    payload = {
        "judge_run_id": str(JUDGE_RUN_ID),
        "judge_output_id": str(JUDGE_OUTPUT_ID),
        "finish_reason": "fake_structured_output",
        "refusal_detected": False,
    }
    payload.update(overrides.pop("payload_overrides", {}))
    return OutboxEventRow(
        event_id=overrides.pop("event_id", TRIGGER_EVENT_ID),
        event_type=overrides.pop("event_type", "judge.output.ready.v1"),
        aggregate_type=overrides.pop("aggregate_type", "judge_run"),
        aggregate_id=overrides.pop("aggregate_id", JUDGE_RUN_ID),
        dedupe_key=overrides.pop("dedupe_key", f"judge-output-ready:{JUDGE_RUN_ID}:{JUDGE_OUTPUT_ID}"),
        payload_json=overrides.pop("payload_json", payload),
        status=overrides.pop("status", "published"),
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _judge_run(**overrides: Any) -> JudgeRunValidationRecord:
    return JudgeRunValidationRecord(
        judge_run_id=overrides.pop("judge_run_id", JUDGE_RUN_ID),
        bundle_id=overrides.pop("bundle_id", BUNDLE_ID),
        schema_version=overrides.pop("schema_version", "judge_output_v1"),
        status=overrides.pop("status", "succeeded"),
        finish_reason=overrides.pop("finish_reason", "fake_structured_output"),
        refusal_detected=overrides.pop("refusal_detected", False),
    )


def _judge_output(**overrides: Any) -> JudgeOutputValidationRecord:
    candidate_group_id = overrides.pop("candidate_group_id", CANDIDATE_GROUP_ID)
    payload = overrides.pop("payload_json", _payload(candidate_group_id=candidate_group_id))
    return JudgeOutputValidationRecord(
        judge_output_id=overrides.pop("judge_output_id", JUDGE_OUTPUT_ID),
        judge_run_id=overrides.pop("judge_run_id", JUDGE_RUN_ID),
        candidate_group_id=candidate_group_id,
        judge_schema_version=overrides.pop("judge_schema_version", "judge_output_v1"),
        payload_json=payload,
        model_proposed_verdict=overrides.pop("model_proposed_verdict", "later"),
        model_confidence_band=overrides.pop("model_confidence_band", "low"),
    )


def _bundle(**overrides: Any) -> BundleValidationRecord:
    return BundleValidationRecord(
        bundle_id=overrides.pop("bundle_id", BUNDLE_ID),
        candidate_group_id=overrides.pop("candidate_group_id", CANDIDATE_GROUP_ID),
        ready_for_analysis=overrides.pop("ready_for_analysis", True),
    )


def _policy_outbox(
    *,
    judge_run_id: UUID = JUDGE_RUN_ID,
    judge_output_id: UUID = JUDGE_OUTPUT_ID,
    candidate_group_id: UUID = CANDIDATE_GROUP_ID,
    bundle_id: UUID = BUNDLE_ID,
    status: str,
) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=POLICY_APPLY_EVENT_ID,
        event_type="analysis.policy.apply.v1",
        aggregate_type="judge_run",
        aggregate_id=judge_run_id,
        dedupe_key=f"analysis-policy-apply:{judge_run_id}:{judge_output_id}",
        payload_json=_policy_payload(
            judge_run_id=judge_run_id,
            judge_output_id=judge_output_id,
            candidate_group_id=candidate_group_id,
            bundle_id=bundle_id,
        ),
        status=status,
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )


def _payload(*, candidate_group_id: UUID = CANDIDATE_GROUP_ID) -> dict[str, Any]:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate_group_id),
        "headline": "Bounded fake output",
        "summary_one_line_ko": "short summary",
        "skeptical_take_ko": "needs more evidence",
        "why_it_might_matter_ko": "could improve developer workflow",
        "comparables": [],
        "scores": {
            "novelty": 50,
            "practical_usefulness": 55,
            "evidence_strength": 40,
            "hype_penalty": 10,
            "confidence": 45,
            "code_quality": None,
            "maintenance_signal": None,
            "specificity": 20,
            "reproducibility_signal": None,
        },
        "reason_codes": ["bounded_fake_openai_execution"],
        "red_flags_ko": [],
        "evidence_limitations_ko": ["limited evidence"],
        "recommended_action_ko": "review later",
        "freshness_note_ko": "freshness needs verification",
        "model_proposed_verdict": "later",
        "model_confidence_band": "low",
        "private_sentinel": RAW_PAYLOAD_SENTINEL,
    }


def _policy_payload(
    *,
    judge_run_id: UUID,
    judge_output_id: UUID,
    candidate_group_id: UUID,
    bundle_id: UUID,
) -> dict[str, str]:
    return {
        "judge_run_id": str(judge_run_id),
        "judge_output_id": str(judge_output_id),
        "candidate_group_id": str(candidate_group_id),
        "bundle_id": str(bundle_id),
    }


def _approved_config(**overrides: Any) -> BoundedAnalysisValidatorPolicyRequestConfig:
    values = {
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_redis_read": True,
        "allow_redis_publish": True,
        "allow_database_read": True,
        "allow_database_write": True,
        "allow_analysis_validator": True,
        "redis_message_suffix": "508480-0",
        "trigger_event_suffix": "3e3b11b3",
        "judge_output_suffix": "c7d7ef5e",
        "judge_run_suffix": "7a111d13",
        "scan_limit": 25,
    }
    values.update(overrides)
    return BoundedAnalysisValidatorPolicyRequestConfig(**values)


def _run(
    *,
    config: BoundedAnalysisValidatorPolicyRequestConfig | None = None,
    messages: list[RedisStreamMessage] | None = None,
    repository: FakeRepository | None = None,
    event: OutboxEventRow | None = None,
    judge_run: JudgeRunValidationRecord | None = None,
    judge_output: JudgeOutputValidationRecord | None = None,
    bundle: BundleValidationRecord | None = None,
    policy_rows: list[OutboxEventRow] | None = None,
    runtime_loader=None,
    route_resolver=None,
):
    reader_builder = FakeRedisReaderBuilder(FakeRedisReader(messages if messages is not None else [_redis_message()]))
    fake_repository = repository or FakeRepository(
        event=_event() if event is None else event,
        judge_run=_judge_run() if judge_run is None else judge_run,
        judge_output=_judge_output() if judge_output is None else judge_output,
        bundle=_bundle() if bundle is None else bundle,
        policy_rows=policy_rows,
    )
    repository_builder = FakeRepositoryBuilder(fake_repository)
    publisher_builder = FakeRedisPublisherBuilder(FakeRedisPublisher())
    result = run_bounded_analysis_validator_policy_request_sync(
        config or _approved_config(),
        runtime_config_loader=runtime_loader or _runtime_config,
        redis_reader_builder=reader_builder,
        repository_builder=repository_builder,
        redis_publisher_builder=publisher_builder,
        route_resolver=route_resolver,
    )
    return result, reader_builder, repository_builder, publisher_builder


def test_success_validates_writes_policy_apply_publishes_and_records_attempt() -> None:
    result, reader_builder, repository_builder, publisher_builder = _run()
    report = result.to_sanitized_dict()
    repository = repository_builder.repository
    publisher = publisher_builder.publisher

    assert result.ok is True
    assert report["status"] == "published"
    assert report["target_redis_message_id_suffix"] == "508480-0"
    assert report["target_trigger_event_id_suffix"] == "3e3b11b3"
    assert report["target_judge_run_id_suffix"] == "7a111d13"
    assert report["target_judge_output_id_suffix"] == "c7d7ef5e"
    assert report["target_bundle_id_suffix"] == "c51bd89e"
    assert report["target_candidate_group_suffix"] == "42c0d691"
    assert report["policy_apply_outbox_written"] is True
    assert report["policy_apply_event_suffix"] == "aa55aa55"
    assert report["policy_apply_published"] is True
    assert report["q_analysis_policy_message_id_suffix"] == "600001-0"
    assert report["validation_status"] == "passed"
    assert report["validation_error_count"] == 0
    assert report["state_transition_written"] is True
    assert report["queue_name"] == "q.analysis.policy"
    assert report["stage_name"] == "analysis_policy"
    assert report["redis_message_count"] == 1
    assert report["event_outbox_found"] is True
    assert report["judge_run_found"] is True
    assert report["judge_output_found"] is True
    assert report["bundle_found"] is True
    assert report["analysis_validator_called"] is True
    assert report["policy_called"] is False
    assert report["notifier_called"] is False
    assert report["telegram_send_called"] is False
    assert report["openai_called"] is False
    assert report["redis_ack_called"] is False
    assert report["redis_consume_called"] is False
    assert repository.state_transitions == [
        {
            "object_type": "judge_run",
            "object_id": JUDGE_RUN_ID,
            "from_state": "succeeded",
            "to_state": "analysis_validated",
            "reason_code": "validator_passed",
        }
    ]
    assert repository.policy_rows[0].status == "published"
    assert repository.mark_published_calls == [
        {
            "event_id": POLICY_APPLY_EVENT_ID,
            "judge_run_id": JUDGE_RUN_ID,
            "published_at": repository.mark_published_calls[0]["published_at"],
        }
    ]
    assert repository.job_attempt_calls == [
        {
            "stage_name": "analysis_policy",
            "queue_name": "q.analysis.policy",
            "root_object_type": "judge_run",
            "root_object_id": JUDGE_RUN_ID,
            "attempt_status": "succeeded",
            "error_code": None,
        }
    ]
    assert len(publisher.calls) == 1
    route, message = publisher.calls[0]
    assert route == QueueRoute("q.analysis.policy", "analysis_policy")
    assert message.as_stream_fields() == {
        "job_id": str(POLICY_APPLY_EVENT_ID),
        "stage_name": "analysis_policy",
        "root_object_type": "judge_run",
        "root_object_id": str(JUDGE_RUN_ID),
        "idempotency_key": f"analysis-policy-apply:{JUDGE_RUN_ID}:{JUDGE_OUTPUT_ID}",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(POLICY_APPLY_EVENT_ID),
    }
    assert reader_builder.closed is True
    assert repository_builder.closed is True
    assert publisher_builder.closed is True


def test_repository_policy_apply_lookup_uses_dedupe_key_without_json_payload_operators() -> None:
    row = {
        "event_id": POLICY_APPLY_EVENT_ID,
        "event_type": "analysis.policy.apply.v1",
        "aggregate_type": "judge_run",
        "aggregate_id": JUDGE_RUN_ID,
        "dedupe_key": f"analysis-policy-apply:{JUDGE_RUN_ID}:{JUDGE_OUTPUT_ID}",
        "payload_json": _policy_payload(
            judge_run_id=JUDGE_RUN_ID,
            judge_output_id=JUDGE_OUTPUT_ID,
            candidate_group_id=CANDIDATE_GROUP_ID,
            bundle_id=BUNDLE_ID,
        ),
        "status": "pending",
        "fail_count": 0,
        "created_at": datetime.now(timezone.utc),
    }
    session = CapturingSqlSession([row])
    repository = SqlAlchemyBoundedAnalysisValidatorPolicyRequestRepository(session)

    rows = asyncio.run(
        repository.load_policy_apply_outboxes(
            judge_run_id=JUDGE_RUN_ID,
            judge_output_id=JUDGE_OUTPUT_ID,
            candidate_group_id=CANDIDATE_GROUP_ID,
            bundle_id=BUNDLE_ID,
        )
    )

    sql, params = session.calls[0]
    assert rows[0].event_id == POLICY_APPLY_EVENT_ID
    assert "dedupe_key = :dedupe_key" in " ".join(sql.split())
    assert "payload_json->" not in sql
    assert "->>" not in sql
    assert params == {
        "judge_run_id": str(JUDGE_RUN_ID),
        "dedupe_key": f"analysis-policy-apply:{JUDGE_RUN_ID}:{JUDGE_OUTPUT_ID}",
    }


@pytest.mark.parametrize(
    ("config", "expected_error"),
    [
        (BoundedAnalysisValidatorPolicyRequestConfig(), "operator_approval_missing"),
        (_approved_config(allow_analysis_validator=False), "analysis_validator_not_allowed"),
        (_approved_config(allow_database_write=False), "database_write_not_allowed"),
        (_approved_config(allow_redis_publish=False), "redis_publish_not_allowed"),
    ],
)
def test_authority_gates_fail_closed_before_runtime_config(config, expected_error) -> None:
    result, reader_builder, repository_builder, publisher_builder = _run(
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
    assert report["analysis_validator_called"] is False
    assert reader_builder.calls == 0
    assert repository_builder.calls == 0
    assert publisher_builder.calls == 0


def test_runtime_config_error_blocks_before_redis_or_database() -> None:
    result, _, repository_builder, publisher_builder = _run(runtime_loader=_missing_runtime_config)
    report = result.to_sanitized_dict()

    assert report["error_code"] == "database_url_missing"
    assert report["redis_read_attempted"] is False
    assert report["database_read_attempted"] is False
    assert repository_builder.calls == 0
    assert publisher_builder.calls == 0


def test_redis_input_message_must_remain_thin_id_only() -> None:
    result, _, repository_builder, publisher_builder = _run(
        messages=[_redis_message(payload_json="{private}", bundle_id=str(BUNDLE_ID))]
    )
    report = result.to_sanitized_dict()

    assert report["error_code"] == "redis_message_forbidden_business_fields"
    assert report["database_read_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert repository_builder.calls == 0
    assert publisher_builder.calls == 0


def test_refusal_payload_stops_at_validator_without_policy_apply() -> None:
    payload = _payload()
    payload["output_kind"] = "refusal"
    result, _, repository_builder, publisher_builder = _run(
        event=_event(payload_overrides={"refusal_detected": True}),
        judge_output=_judge_output(payload_json=payload),
    )
    report = result.to_sanitized_dict()
    repository = repository_builder.repository

    assert result.ok is True
    assert report["status"] == "refused_stopped"
    assert report["validation_status"] == "refused"
    assert report["state_transition_written"] is True
    assert repository.state_transitions[0]["to_state"] == "analysis_refused"
    assert repository.state_transitions[0]["reason_code"] == "analysis_refused"
    assert repository.policy_rows == []
    assert publisher_builder.calls == 0
    assert report["redis_publish_attempted"] is False


def test_invalid_schema_missing_required_field_stops_without_policy_apply() -> None:
    payload = _payload()
    payload.pop("headline")
    result, _, repository_builder, publisher_builder = _run(judge_output=_judge_output(payload_json=payload))
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["status"] == "validation_failed"
    assert report["error_code"] == "validator_schema_invalid"
    assert report["validation_error_count"] == 1
    assert repository_builder.repository.policy_rows == []
    assert publisher_builder.calls == 0


@pytest.mark.parametrize(
    "required_score_field",
    [
        "novelty",
        "practical_usefulness",
        "evidence_strength",
        "hype_penalty",
        "confidence",
    ],
)
def test_null_required_common_score_stops_without_policy_apply(required_score_field) -> None:
    payload = _payload()
    payload["scores"][required_score_field] = None
    result, _, repository_builder, publisher_builder = _run(judge_output=_judge_output(payload_json=payload))
    report = result.to_sanitized_dict()

    assert report["error_code"] == "validator_score_range_invalid"
    assert repository_builder.repository.policy_rows == []
    assert publisher_builder.calls == 0


def test_null_conditional_artifact_scores_pass_policy_apply_publish() -> None:
    payload = _payload()
    payload["scores"]["code_quality"] = None
    payload["scores"]["maintenance_signal"] = None
    payload["scores"]["specificity"] = None
    payload["scores"]["reproducibility_signal"] = None
    result, _, repository_builder, publisher_builder = _run(judge_output=_judge_output(payload_json=payload))
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["status"] == "published"
    assert report["validation_status"] == "passed"
    assert repository_builder.repository.policy_rows[0].status == "published"
    assert len(publisher_builder.publisher.calls) == 1


@pytest.mark.parametrize("bad_score", [True, 101, -1, "80"])
def test_invalid_score_type_or_bounds_stops_without_policy_apply(bad_score) -> None:
    payload = _payload()
    payload["scores"]["novelty"] = bad_score
    result, _, repository_builder, publisher_builder = _run(judge_output=_judge_output(payload_json=payload))
    report = result.to_sanitized_dict()

    assert report["error_code"] == "validator_score_range_invalid"
    assert repository_builder.repository.policy_rows == []
    assert publisher_builder.calls == 0


def test_candidate_group_id_mismatch_stops_without_policy_apply() -> None:
    payload = _payload(candidate_group_id=UUID("00000000-0000-4000-8000-000099999999"))
    result, _, repository_builder, publisher_builder = _run(judge_output=_judge_output(payload_json=payload))
    report = result.to_sanitized_dict()

    assert report["error_code"] == "validator_payload_candidate_mismatch"
    assert repository_builder.repository.policy_rows == []
    assert publisher_builder.calls == 0


@pytest.mark.parametrize(
    ("kwargs", "expected_error"),
    [
        ({"judge_output": _judge_output(judge_run_id=UUID("00000000-0000-4000-8000-0000bad00001"))}, "judge_output_judge_run_mismatch"),
        ({"judge_run": _judge_run(bundle_id=UUID("00000000-0000-4000-8000-0000bad00002"))}, "bundle_missing"),
        ({"bundle": _bundle(candidate_group_id=UUID("00000000-0000-4000-8000-0000bad00003"))}, "judge_output_bundle_candidate_mismatch"),
        ({"bundle": _bundle(ready_for_analysis=False)}, "bundle_not_ready"),
    ],
)
def test_identity_mismatches_stop(kwargs, expected_error) -> None:
    result, _, repository_builder, publisher_builder = _run(**kwargs)
    report = result.to_sanitized_dict()

    assert report["error_code"] == expected_error
    assert repository_builder.repository.policy_rows == []
    assert publisher_builder.calls == 0


def test_existing_pending_policy_apply_outbox_is_reused_and_published() -> None:
    existing = _policy_outbox(status="pending")
    result, _, repository_builder, publisher_builder = _run(policy_rows=[existing])
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["status"] == "published"
    assert report["policy_apply_outbox_written"] is False
    assert report["policy_apply_published"] is True
    assert report["state_transition_written"] is False
    assert repository_builder.repository.policy_rows[0].status == "published"
    assert len(publisher_builder.publisher.calls) == 1


def test_existing_published_policy_apply_outbox_returns_noop_without_duplicate_publish() -> None:
    existing = _policy_outbox(status="published")
    result, _, repository_builder, publisher_builder = _run(policy_rows=[existing])
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["status"] == "noop"
    assert report["policy_apply_outbox_written"] is False
    assert report["policy_apply_published"] is True
    assert report["redis_publish_attempted"] is False
    assert publisher_builder.calls == 0
    assert publisher_builder.publisher.calls == []
    assert repository_builder.repository.policy_rows == [existing]


def test_existing_policy_apply_lookup_is_dedupe_based_and_payload_validation_stays_strict() -> None:
    existing = replace(
        _policy_outbox(status="pending"),
        payload_json={
            "judge_run_id": str(JUDGE_RUN_ID),
            "judge_output_id": str(JUDGE_OUTPUT_ID),
            "candidate_group_id": str(CANDIDATE_GROUP_ID),
            "bundle_id": str(UUID("00000000-0000-4000-8000-0000bad0bad0")),
        },
    )
    result, _, repository_builder, publisher_builder = _run(policy_rows=[existing])
    report = result.to_sanitized_dict()

    assert report["error_code"] == "policy_apply_outbox_payload_mismatch"
    assert report["event_outbox_found"] is True
    assert report["judge_run_found"] is True
    assert report["judge_output_found"] is True
    assert report["bundle_found"] is True
    assert report["redis_publish_attempted"] is False
    assert repository_builder.repository.state_transitions == []
    assert publisher_builder.calls == 0


def test_duplicate_policy_apply_outbox_rows_fail_closed() -> None:
    first = _policy_outbox(status="pending")
    second = replace(first, event_id=UUID("00000000-0000-4000-8000-0000bb66bb66"))
    result, _, repository_builder, publisher_builder = _run(policy_rows=[first, second])
    report = result.to_sanitized_dict()

    assert report["error_code"] == "policy_apply_outbox_count_exceeded"
    assert report["redis_publish_attempted"] is False
    assert repository_builder.repository.state_transitions == []
    assert publisher_builder.calls == 0


def test_unexpected_policy_lookup_exception_preserves_known_context_without_details() -> None:
    repository = RaisingPolicyLookupRepository(
        event=_event(),
        judge_run=_judge_run(),
        judge_output=_judge_output(),
        bundle=_bundle(),
    )
    result, _, repository_builder, publisher_builder = _run(repository=repository)
    report = result.to_sanitized_dict()
    rendered = json.dumps(report, ensure_ascii=False, sort_keys=True)

    assert report["status"] == "failed"
    assert report["error_code"] == "bounded_policy_request_failed"
    assert report["error_class"] == "RuntimeError"
    assert report["target_redis_message_id_suffix"] == "508480-0"
    assert report["target_trigger_event_id_suffix"] == "3e3b11b3"
    assert report["target_judge_run_id_suffix"] == "7a111d13"
    assert report["target_judge_output_id_suffix"] == "c7d7ef5e"
    assert report["target_bundle_id_suffix"] == "c51bd89e"
    assert report["target_candidate_group_suffix"] == "42c0d691"
    assert report["event_outbox_found"] is True
    assert report["judge_run_found"] is True
    assert report["judge_output_found"] is True
    assert report["bundle_found"] is True
    assert report["analysis_validator_called"] is True
    assert report["database_write_attempted"] is False
    assert report["redis_publish_attempted"] is False
    assert report["state_transition_written"] is False
    assert report["policy_apply_outbox_written"] is False
    assert RAW_PAYLOAD_SENTINEL not in rendered
    assert "private sql detail" not in rendered
    assert repository_builder.repository.state_transitions == []
    assert publisher_builder.calls == 0


def test_route_drift_blocks_before_redis_publish() -> None:
    result, _, repository_builder, publisher_builder = _run(route_resolver=RogueRouteResolver())
    report = result.to_sanitized_dict()

    assert report["error_code"] == "route_not_allowed"
    assert report["redis_publish_attempted"] is False
    assert repository_builder.repository.policy_rows[0].status == "pending"
    assert publisher_builder.publisher.calls == []


def test_output_redaction_excludes_full_ids_payload_bundle_and_locators() -> None:
    result, _, _, _ = _run()
    rendered = json.dumps(result.to_sanitized_dict(), ensure_ascii=False, sort_keys=True)

    forbidden = [
        str(TRIGGER_EVENT_ID),
        str(JUDGE_RUN_ID),
        str(JUDGE_OUTPUT_ID),
        str(BUNDLE_ID),
        str(CANDIDATE_GROUP_ID),
        str(POLICY_APPLY_EVENT_ID),
        DB_LOCATOR,
        REDIS_LOCATOR,
        RAW_PAYLOAD_SENTINEL,
        RAW_BUNDLE_SENTINEL,
        "private-dedupe-key",
        "bounded_fake_openai_execution",
    ]
    for value in forbidden:
        assert value not in rendered


def test_policy_apply_route_maps_to_q_analysis_policy() -> None:
    route = OutboxRouteResolver().resolve(_policy_outbox(status="pending"))

    assert route == QueueRoute("q.analysis.policy", "analysis_policy")


def test_no_openai_policy_notifier_or_live_external_imports_or_calls() -> None:
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
            "openai",
            "policy_engine",
            "notifier_telegram",
            "collector_telegram",
            "gh_enricher",
            "x_enricher",
            "web_enricher",
            "subprocess",
        )
        for module in imported_modules:
            assert not any(fragment in module for fragment in forbidden_import_fragments)
        assert "AsyncOpenAI" not in names
        assert {"xreadgroup", "xack", "xgroup", "xgroup_create", "xclaim", "xautoclaim"} & called_attrs == set()
        assert {"run_forever", "subprocess", "systemd", "docker", "alembic"} & called_attrs == set()
        assert {"subprocess", "systemd", "docker", "alembic"} & called_names == set()
