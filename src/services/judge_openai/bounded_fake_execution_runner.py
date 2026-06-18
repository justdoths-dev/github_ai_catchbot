from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - local/static validation fallback
    sa = None

from ..outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from ..outbox_relay.redis_streams import RedisStreamsPublisher
from ..outbox_relay.routing import OutboxRouteResolver, UnsupportedOutboxEventTypeError
from .bounded_request_envelope_runner import (
    BundleEnvelopeRecord,
    JudgeCallOutboxRecord,
    RedisReadOnlyStreamReader,
    RedisStreamMessage,
)
from .context_builder import JudgeContextBuilder
from .models import BundleJudgeContext, JudgeRunRecord, OpenAIJudgeResult
from .preflight import HeuristicSanitizingPreflight, NoopModelContextPreflight
from .prompt_library import UnsupportedJudgeProfileError, UnsupportedPromptVersionError
from .request_shape import (
    JudgeOpenAIRequestEnvelope,
    JudgeOpenAIRequestEnvelopeBuilder,
    JudgeOpenAIRequestEnvelopeError,
    summarize_responses_request_shape,
)
from .response_mapper import OpenAIResponseMapper


SCHEMA_VERSION = "bounded_judge_openai_fake_execution_v1"
RUNNER_NAME = "bounded_judge_openai_fake_execution_runner"
MODE = "fake_openai_exact_target_execute_and_publish"
INPUT_QUEUE_NAME = "q.analysis.judge"
INPUT_STAGE_NAME = "judge"
INPUT_EVENT_TYPE = "judge.call.requested.v1"
ROOT_OBJECT_TYPE = "judge_run"
OUTPUT_EVENT_TYPE = "judge.output.ready.v1"
OUTPUT_QUEUE_NAME = "q.analysis.validate"
OUTPUT_STAGE_NAME = "analysis_validate"
DEFAULT_SCAN_LIMIT = 25
MAX_SCAN_LIMIT = 500
DEFAULT_XADD_MAXLEN = 10000
FAKE_FINISH_REASON = "fake_structured_output"
FAKE_INPUT_TOKENS = 321
FAKE_CACHED_INPUT_TOKENS = 0
FAKE_OUTPUT_TOKENS = 123
FAKE_REASONING_TOKENS = 0

UUID_SUFFIX_RE = re.compile(r"^[0-9a-f-]{4,36}$")
REDIS_ID_SUFFIX_RE = re.compile(r"^[0-9-]{3,64}$")
REQUIRED_REDIS_FIELDS = frozenset(
    {
        "idempotency_key",
        "job_id",
        "not_before",
        "pipeline_run_id",
        "root_object_id",
        "root_object_type",
        "stage_name",
        "trigger_event_id",
    }
)
FORBIDDEN_REDIS_BUSINESS_FIELDS = frozenset(
    {
        "payload_json",
        "bundle_id",
        "candidate_group_id",
        "model",
        "reasoning_effort",
        "prompt_version",
        "prompt_cache_key",
        "prompt_material",
        "bundle_data",
        "raw_text",
        "model_output",
    }
)
REQUIRED_EVENT_PAYLOAD_FIELDS = frozenset(
    {
        "judge_run_id",
        "bundle_id",
        "model",
        "reasoning_effort",
        "prompt_version",
        "prompt_cache_key",
    }
)
REQUIRED_FAKE_OUTPUT_FIELDS = (
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
REQUIRED_SCORE_FIELDS = (
    "novelty",
    "practical_usefulness",
    "evidence_strength",
    "hype_penalty",
    "confidence",
    "code_quality",
    "maintenance_signal",
    "specificity",
    "reproducibility_signal",
)


@dataclass(frozen=True, slots=True)
class BoundedJudgeOpenAIFakeExecutionConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_read: bool = False
    allow_redis_publish: bool = False
    allow_database_read: bool = False
    allow_database_write: bool = False
    allow_fake_openai: bool = False
    redis_message_id: str | None = None
    trigger_event_id: UUID | None = None
    redis_message_suffix: str | None = None
    trigger_event_suffix: str | None = None
    scan_limit: int = DEFAULT_SCAN_LIMIT


@dataclass(frozen=True, slots=True)
class BoundedJudgeOpenAIFakeExecutionRuntimeConfig:
    database_url: str
    redis_url: str
    input_queue_name: str = INPUT_QUEUE_NAME
    xadd_maxlen: int | None = DEFAULT_XADD_MAXLEN
    request_timeout_sec: float | None = 60.0
    max_output_tokens: int | None = None
    enable_prompt_guard_preflight: bool = False


@dataclass(slots=True)
class BoundedJudgeOpenAIFakeExecutionState:
    runtime_config_loaded: bool = False
    redis_reader_created: bool = False
    redis_read_attempted: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    redis_publisher_created: bool = False
    redis_publish_attempted: bool = False
    fake_openai_called: bool = False


@dataclass(frozen=True, slots=True)
class JudgeOutputRecord:
    judge_output_id: UUID
    judge_run_id: UUID
    candidate_group_id: UUID
    judge_schema_version: str
    payload_json: dict[str, Any]
    model_proposed_verdict: str | None
    model_confidence_band: str | None


@dataclass(frozen=True, slots=True)
class ExistingJudgeOutputLookup:
    output: JudgeOutputRecord | None
    count: int


class BoundedJudgeOpenAIFakeExecutionError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class _BoundedResultReady(Exception):
    pass


class ReadOnlyRedisMessageReader(Protocol):
    async def read_candidate_messages(
        self,
        *,
        queue_name: str,
        config: BoundedJudgeOpenAIFakeExecutionConfig,
    ) -> list[RedisStreamMessage]: ...


class RedisPublisher(Protocol):
    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str: ...


class BoundedJudgeOpenAIFakeExecutionRepository(Protocol):
    async def load_event_outbox(self, trigger_event_id: UUID) -> JudgeCallOutboxRecord | None: ...
    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunRecord | None: ...
    async def load_bundle(self, bundle_id: UUID) -> BundleEnvelopeRecord | None: ...
    async def load_existing_judge_output(self, judge_run_id: UUID) -> ExistingJudgeOutputLookup: ...
    async def insert_judge_output(
        self,
        *,
        judge_run_id: UUID,
        candidate_group_id: UUID,
        judge_schema_version: str,
        payload_json: dict[str, Any],
        model_proposed_verdict: str | None,
        model_confidence_band: str | None,
    ) -> UUID: ...
    async def finish_judge_run_succeeded(
        self,
        *,
        judge_run_id: UUID,
        result: OpenAIJudgeResult | None,
        finish_reason: str,
        refusal_detected: bool,
    ) -> None: ...
    async def insert_or_load_judge_output_ready_outbox(
        self,
        *,
        judge_run_id: UUID,
        judge_output_id: UUID,
        finish_reason: str,
        refusal_detected: bool,
    ) -> tuple[OutboxEventRow, bool]: ...
    async def mark_output_ready_outbox_published(
        self,
        *,
        event_id: UUID,
        judge_run_id: UUID,
        published_at: datetime,
    ) -> None: ...
    async def insert_publish_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...


class FakeOpenAIClientProtocol(Protocol):
    async def create_structured_response(self, **kwargs: Any) -> Any: ...


@dataclass(frozen=True, slots=True)
class BoundedJudgeOpenAIRedisReaderHandle:
    reader: ReadOnlyRedisMessageReader
    close: Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedJudgeOpenAIFakeExecutionRepositoryHandle:
    repository: BoundedJudgeOpenAIFakeExecutionRepository
    close: Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedJudgeOpenAIFakeExecutionRedisPublisherHandle:
    publisher: RedisPublisher
    close: Callable[[], Awaitable[None]]


class BoundedJudgeOpenAIRedisReaderBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedJudgeOpenAIFakeExecutionRuntimeConfig,
        state: BoundedJudgeOpenAIFakeExecutionState,
        logger: logging.Logger,
    ) -> BoundedJudgeOpenAIRedisReaderHandle: ...


class BoundedJudgeOpenAIFakeExecutionRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedJudgeOpenAIFakeExecutionRuntimeConfig,
        state: BoundedJudgeOpenAIFakeExecutionState,
        logger: logging.Logger,
    ) -> BoundedJudgeOpenAIFakeExecutionRepositoryHandle: ...


class BoundedJudgeOpenAIFakeExecutionRedisPublisherBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedJudgeOpenAIFakeExecutionRuntimeConfig,
        state: BoundedJudgeOpenAIFakeExecutionState,
        logger: logging.Logger,
    ) -> BoundedJudgeOpenAIFakeExecutionRedisPublisherHandle: ...


@dataclass(frozen=True, slots=True)
class BoundedJudgeOpenAIFakeExecutionResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedJudgeOpenAIFakeExecutionConfig
    state: BoundedJudgeOpenAIFakeExecutionState = field(
        default_factory=BoundedJudgeOpenAIFakeExecutionState
    )
    target_redis_message_id_suffix: str | None = None
    target_trigger_event_id_suffix: str | None = None
    target_judge_run_id_suffix: str | None = None
    target_bundle_id_suffix: str | None = None
    target_candidate_group_suffix: str | None = None
    target_judge_output_id_suffix: str | None = None
    judge_output_written: bool = False
    judge_run_status: str | None = None
    judge_output_ready_outbox_written: bool = False
    judge_output_ready_event_suffix: str | None = None
    judge_output_ready_published: bool = False
    q_analysis_validate_message_id_suffix: str | None = None
    model: str | None = None
    reasoning_effort: str | None = None
    judge_profile: str | None = None
    prompt_version: str | None = None
    schema_version_value: str | None = None
    policy_version: str | None = None
    redis_message_count: int = 0
    event_outbox_found: bool = False
    judge_run_found: bool = False
    bundle_found: bool = False
    existing_judge_output_count: int = 0
    queue_name: str | None = None
    stage_name: str | None = None

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "target_redis_message_id_suffix": self.target_redis_message_id_suffix,
            "target_trigger_event_id_suffix": self.target_trigger_event_id_suffix,
            "target_judge_run_id_suffix": self.target_judge_run_id_suffix,
            "target_bundle_id_suffix": self.target_bundle_id_suffix,
            "target_candidate_group_suffix": self.target_candidate_group_suffix,
            "target_judge_output_id_suffix": self.target_judge_output_id_suffix,
            "judge_output_written": self.judge_output_written,
            "judge_run_status": self.judge_run_status,
            "judge_output_ready_outbox_written": self.judge_output_ready_outbox_written,
            "judge_output_ready_event_suffix": self.judge_output_ready_event_suffix,
            "judge_output_ready_published": self.judge_output_ready_published,
            "q_analysis_validate_message_id_suffix": self.q_analysis_validate_message_id_suffix,
            "model": self.model,
            "reasoning_effort": self.reasoning_effort,
            "judge_profile": self.judge_profile,
            "prompt_version": self.prompt_version,
            "schema_version_value": self.schema_version_value,
            "policy_version": self.policy_version,
            "fake_openai_called": self.state.fake_openai_called,
            "openai_called": False,
            "redis_read_attempted": self.state.redis_read_attempted,
            "redis_publish_attempted": self.state.redis_publish_attempted,
            "redis_ack_called": False,
            "redis_consume_called": False,
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": self.state.database_write_attempted,
            "validator_called": False,
            "policy_called": False,
            "notifier_called": False,
            "telegram_send_called": False,
            "github_api_called": False,
            "x_api_called": False,
            "web_fetch_called": False,
            "db_write": self.state.database_write_attempted,
            "redis_message_count": self.redis_message_count,
            "event_outbox_found": self.event_outbox_found,
            "judge_run_found": self.judge_run_found,
            "bundle_found": self.bundle_found,
            "existing_judge_output_count": self.existing_judge_output_count,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "redis_read_allowed": self.config.allow_redis_read,
                "redis_publish_allowed": self.config.allow_redis_publish,
                "database_read_allowed": self.config.allow_database_read,
                "database_write_allowed": self.config.allow_database_write,
                "fake_openai_allowed": self.config.allow_fake_openai,
                "scan_limit": self.config.scan_limit,
            },
            "redactions_applied": {
                "full_redis_message_id_omitted": True,
                "full_trigger_event_id_omitted": True,
                "full_judge_run_id_omitted": True,
                "full_bundle_id_omitted": True,
                "full_candidate_group_id_omitted": True,
                "full_judge_output_id_omitted": True,
                "full_output_ready_event_id_omitted": True,
                "idempotency_key_omitted": True,
                "prompt_cache_key_value_omitted": True,
                "prompt_text_omitted": True,
                "bundle_context_omitted": True,
                "raw_source_text_omitted": True,
                "model_output_payload_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class DeterministicFakeOpenAIClient:
    def __init__(self, payload_json: Mapping[str, Any]) -> None:
        self._payload_json = dict(payload_json)
        self.calls: list[dict[str, Any]] = []

    async def create_structured_response(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(dict(kwargs))
        return {
            "id": "fake-response-redacted",
            "status": "completed",
            "output_text": json.dumps(self._payload_json, ensure_ascii=False),
            "usage": {
                "input_tokens": FAKE_INPUT_TOKENS,
                "input_tokens_details": {"cached_tokens": FAKE_CACHED_INPUT_TOKENS},
                "output_tokens": FAKE_OUTPUT_TOKENS,
                "output_tokens_details": {"reasoning_tokens": FAKE_REASONING_TOKENS},
            },
        }


class SqlAlchemyBoundedJudgeOpenAIFakeExecutionRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def load_event_outbox(self, trigger_event_id: UUID) -> JudgeCallOutboxRecord | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id, payload_json, status
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(trigger_event_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        payload = _json_loads(row["payload_json"]) or {}
        return JudgeCallOutboxRecord(
            event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            aggregate_type=str(row["aggregate_type"]),
            aggregate_id=UUID(str(row["aggregate_id"])),
            payload_json=payload if isinstance(payload, dict) else {},
            status=str(row["status"]),
        )

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunRecord | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT judge_run_id, bundle_id, judge_profile, model, reasoning_effort,
                       prompt_version, schema_version, policy_version, prompt_cache_key,
                       status, schema_retry_count
                FROM judge_runs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": str(judge_run_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeRunRecord(
            judge_run_id=UUID(str(row["judge_run_id"])),
            bundle_id=UUID(str(row["bundle_id"])),
            judge_profile=str(row["judge_profile"]),
            model=str(row["model"]),
            reasoning_effort=str(row["reasoning_effort"]),
            prompt_version=str(row["prompt_version"]),
            schema_version=str(row["schema_version"]),
            policy_version=str(row["policy_version"]),
            prompt_cache_key=str(row["prompt_cache_key"]) if row["prompt_cache_key"] else None,
            status=str(row["status"]),
            schema_retry_count=int(row["schema_retry_count"]),
        )

    async def load_bundle(self, bundle_id: UUID) -> BundleEnvelopeRecord | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT bundle_id, candidate_group_id, current_primary_artifact_id,
                       primary_summary, supporting_summaries_json,
                       discovered_links_summary_json, evidence_limitations,
                       token_budget_profile, reroot_count, ready_for_analysis, created_at
                FROM candidate_evidence_bundles
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": str(bundle_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        bundle = BundleJudgeContext(
            bundle_id=UUID(str(row["bundle_id"])),
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            current_primary_artifact_id=UUID(str(row["current_primary_artifact_id"])),
            primary_summary=_json_loads(row["primary_summary"]) or {},
            supporting_summaries_json=_json_loads(row["supporting_summaries_json"]) or [],
            discovered_links_summary_json=_json_loads(row["discovered_links_summary_json"]) or [],
            evidence_limitations=_json_loads(row["evidence_limitations"]) or [],
            token_budget_profile=str(row["token_budget_profile"]) if row["token_budget_profile"] else None,
            reroot_count=int(row["reroot_count"]),
            created_at=row["created_at"],
        )
        return BundleEnvelopeRecord(bundle=bundle, ready_for_analysis=bool(row["ready_for_analysis"]))

    async def load_existing_judge_output(self, judge_run_id: UUID) -> ExistingJudgeOutputLookup:
        result = await self._session.execute(
            _sql(
                """
                SELECT judge_output_id, judge_run_id, candidate_group_id, judge_schema_version,
                       payload_json, model_proposed_verdict, model_confidence_band
                FROM judge_outputs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                ORDER BY created_at ASC, judge_output_id ASC
                LIMIT 2
                """
            ),
            {"judge_run_id": str(judge_run_id)},
        )
        rows = list(result.mappings().all())
        if not rows:
            return ExistingJudgeOutputLookup(output=None, count=0)
        row = rows[0]
        payload = _json_loads(row["payload_json"]) or {}
        return ExistingJudgeOutputLookup(
            output=JudgeOutputRecord(
                judge_output_id=UUID(str(row["judge_output_id"])),
                judge_run_id=UUID(str(row["judge_run_id"])),
                candidate_group_id=UUID(str(row["candidate_group_id"])),
                judge_schema_version=str(row["judge_schema_version"]),
                payload_json=payload if isinstance(payload, dict) else {},
                model_proposed_verdict=str(row["model_proposed_verdict"])
                if row["model_proposed_verdict"]
                else None,
                model_confidence_band=str(row["model_confidence_band"])
                if row["model_confidence_band"]
                else None,
            ),
            count=len(rows),
        )

    async def insert_judge_output(
        self,
        *,
        judge_run_id: UUID,
        candidate_group_id: UUID,
        judge_schema_version: str,
        payload_json: dict[str, Any],
        model_proposed_verdict: str | None,
        model_confidence_band: str | None,
    ) -> UUID:
        result = await self._session.execute(
            _sql(
                """
                INSERT INTO judge_outputs (
                    judge_run_id, candidate_group_id, judge_schema_version,
                    payload_json, model_proposed_verdict, model_confidence_band, created_at
                ) VALUES (
                    CAST(:judge_run_id AS uuid),
                    CAST(:candidate_group_id AS uuid),
                    :judge_schema_version,
                    CAST(:payload_json AS jsonb),
                    :model_proposed_verdict,
                    :model_confidence_band,
                    now()
                )
                RETURNING judge_output_id
                """
            ),
            {
                "judge_run_id": str(judge_run_id),
                "candidate_group_id": str(candidate_group_id),
                "judge_schema_version": judge_schema_version,
                "payload_json": _jsonb_dumps(payload_json),
                "model_proposed_verdict": model_proposed_verdict,
                "model_confidence_band": model_confidence_band,
            },
        )
        return UUID(str(result.scalar_one()))

    async def finish_judge_run_succeeded(
        self,
        *,
        judge_run_id: UUID,
        result: OpenAIJudgeResult | None,
        finish_reason: str,
        refusal_detected: bool,
    ) -> None:
        usage = result.usage if result is not None else None
        await self._session.execute(
            _sql(
                """
                UPDATE judge_runs
                SET status = 'succeeded',
                    input_tokens = :input_tokens,
                    cached_input_tokens = :cached_input_tokens,
                    output_tokens = :output_tokens,
                    reasoning_tokens = :reasoning_tokens,
                    latency_ms = :latency_ms,
                    finish_reason = :finish_reason,
                    refusal_detected = :refusal_detected,
                    finished_at = now()
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {
                "judge_run_id": str(judge_run_id),
                "input_tokens": usage.input_tokens if usage else None,
                "cached_input_tokens": usage.cached_input_tokens if usage else None,
                "output_tokens": usage.output_tokens if usage else None,
                "reasoning_tokens": usage.reasoning_tokens if usage else None,
                "latency_ms": usage.latency_ms if usage else None,
                "finish_reason": finish_reason,
                "refusal_detected": refusal_detected,
            },
        )

    async def insert_or_load_judge_output_ready_outbox(
        self,
        *,
        judge_run_id: UUID,
        judge_output_id: UUID,
        finish_reason: str,
        refusal_detected: bool,
    ) -> tuple[OutboxEventRow, bool]:
        payload = {
            "judge_run_id": str(judge_run_id),
            "judge_output_id": str(judge_output_id),
            "finish_reason": finish_reason,
            "refusal_detected": refusal_detected,
        }
        dedupe_key = _judge_output_ready_dedupe_key(
            judge_run_id=judge_run_id,
            judge_output_id=judge_output_id,
        )
        result = await self._session.execute(
            _sql(
                """
                WITH inserted AS (
                    INSERT INTO event_outbox (
                        event_type, aggregate_type, aggregate_id, dedupe_key,
                        payload_json, status, created_at
                    ) VALUES (
                        'judge.output.ready.v1',
                        'judge_run',
                        CAST(:judge_run_id AS uuid),
                        :dedupe_key,
                        CAST(:payload_json AS jsonb),
                        'pending'::outbox_status_enum,
                        now()
                    )
                    ON CONFLICT (dedupe_key) DO NOTHING
                    RETURNING event_id, event_type, aggregate_type, aggregate_id,
                              dedupe_key, payload_json, status, fail_count, created_at,
                              true AS inserted
                )
                SELECT * FROM inserted
                UNION ALL
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       dedupe_key, payload_json, status, fail_count, created_at,
                       false AS inserted
                FROM event_outbox
                WHERE dedupe_key = :dedupe_key
                LIMIT 1
                """
            ),
            {
                "judge_run_id": str(judge_run_id),
                "dedupe_key": dedupe_key,
                "payload_json": _jsonb_dumps(payload),
            },
        )
        row = result.mappings().first()
        if row is None:  # pragma: no cover - defensive guard around unique insert/select
            raise BoundedJudgeOpenAIFakeExecutionError("judge_output_ready_outbox_missing")
        return _outbox_row_from_mapping(row), bool(row["inserted"])

    async def mark_output_ready_outbox_published(
        self,
        *,
        event_id: UUID,
        judge_run_id: UUID,
        published_at: datetime,
    ) -> None:
        await self._session.execute(
            _sql(
                """
                UPDATE event_outbox
                SET status = 'published'::outbox_status_enum,
                    published_at = :published_at,
                    last_error = NULL
                WHERE event_id = CAST(:event_id AS uuid)
                  AND event_type = 'judge.output.ready.v1'
                  AND aggregate_type = 'judge_run'
                  AND aggregate_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {
                "event_id": str(event_id),
                "judge_run_id": str(judge_run_id),
                "published_at": published_at,
            },
        )

    async def insert_publish_job_attempt(
        self,
        *,
        stage_name: str,
        queue_name: str,
        root_object_type: str,
        root_object_id: UUID,
        attempt_status: str,
        error_code: str | None = None,
    ) -> None:
        await self._session.execute(
            _sql(
                """
                INSERT INTO job_attempts (
                    stage_name,
                    queue_name,
                    root_object_type,
                    root_object_id,
                    attempt_no,
                    lease_owner,
                    started_at,
                    finished_at,
                    attempt_status,
                    error_code,
                    retry_after_at
                ) VALUES (
                    :stage_name,
                    :queue_name,
                    :root_object_type,
                    CAST(:root_object_id AS uuid),
                    1,
                    NULL,
                    now(),
                    now(),
                    CAST(:attempt_status AS job_attempt_status_enum),
                    :error_code,
                    NULL
                )
                """
            ),
            {
                "stage_name": stage_name,
                "queue_name": queue_name,
                "root_object_type": root_object_type,
                "root_object_id": str(root_object_id),
                "attempt_status": attempt_status,
                "error_code": error_code,
            },
        )

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()


def load_bounded_judge_openai_fake_execution_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedJudgeOpenAIFakeExecutionRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedJudgeOpenAIFakeExecutionError("database_url_missing")
    if not redis_url:
        raise BoundedJudgeOpenAIFakeExecutionError("redis_url_missing")
    input_queue_name = _env_value(source, "JUDGE_OPENAI_QUEUE_NAME", INPUT_QUEUE_NAME)
    if input_queue_name != INPUT_QUEUE_NAME:
        raise BoundedJudgeOpenAIFakeExecutionError("queue_not_allowed")
    xadd_maxlen_raw = _env_value(source, "OUTBOX_RELAY_XADD_MAXLEN", str(DEFAULT_XADD_MAXLEN))
    max_output_tokens_raw = _env_value(source, "JUDGE_MAX_OUTPUT_TOKENS")
    request_timeout_raw = _env_value(source, "JUDGE_OPENAI_REQUEST_TIMEOUT_SEC", "60")
    try:
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
        max_output_tokens = int(max_output_tokens_raw) if max_output_tokens_raw else None
        request_timeout_sec = float(request_timeout_raw)
    except ValueError as exc:
        raise BoundedJudgeOpenAIFakeExecutionError("runtime_config_error") from exc
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        raise BoundedJudgeOpenAIFakeExecutionError("runtime_config_error")
    if max_output_tokens is not None and max_output_tokens <= 0:
        raise BoundedJudgeOpenAIFakeExecutionError("runtime_config_error")
    if request_timeout_sec <= 0:
        raise BoundedJudgeOpenAIFakeExecutionError("runtime_config_error")
    return BoundedJudgeOpenAIFakeExecutionRuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        input_queue_name=input_queue_name,
        xadd_maxlen=xadd_maxlen,
        request_timeout_sec=request_timeout_sec,
        max_output_tokens=max_output_tokens,
        enable_prompt_guard_preflight=_parse_bool(
            _env_value(source, "ENABLE_PROMPT_GUARD_PREFLIGHT", "false")
        ),
    )


async def build_default_bounded_judge_openai_fake_execution_redis_reader(
    runtime_config: BoundedJudgeOpenAIFakeExecutionRuntimeConfig,
    state: BoundedJudgeOpenAIFakeExecutionState,
    logger: logging.Logger,
) -> BoundedJudgeOpenAIRedisReaderHandle:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    state.redis_reader_created = True
    reader = RedisReadOnlyStreamReader(client)

    async def close() -> None:
        close_client = getattr(client, "aclose", None) or getattr(client, "close", None)
        if close_client is None:
            return
        result = close_client()
        if hasattr(result, "__await__"):
            await result

    return BoundedJudgeOpenAIRedisReaderHandle(reader=reader, close=close)


async def build_default_bounded_judge_openai_fake_execution_repository(
    runtime_config: BoundedJudgeOpenAIFakeExecutionRuntimeConfig,
    state: BoundedJudgeOpenAIFakeExecutionState,
    logger: logging.Logger,
) -> BoundedJudgeOpenAIFakeExecutionRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedJudgeOpenAIFakeExecutionRepository(session)

    async def close() -> None:
        try:
            await session.rollback()
        finally:
            await session.close()
            await engine.dispose()

    return BoundedJudgeOpenAIFakeExecutionRepositoryHandle(repository=repository, close=close)


async def build_default_bounded_judge_openai_fake_execution_redis_publisher(
    runtime_config: BoundedJudgeOpenAIFakeExecutionRuntimeConfig,
    state: BoundedJudgeOpenAIFakeExecutionState,
    logger: logging.Logger,
) -> BoundedJudgeOpenAIFakeExecutionRedisPublisherHandle:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    state.redis_publisher_created = True
    publisher = RedisStreamsPublisher(client, maxlen=runtime_config.xadd_maxlen)

    async def close() -> None:
        close_client = getattr(client, "aclose", None) or getattr(client, "close", None)
        if close_client is None:
            return
        result = close_client()
        if hasattr(result, "__await__"):
            await result

    return BoundedJudgeOpenAIFakeExecutionRedisPublisherHandle(publisher=publisher, close=close)


async def run_bounded_judge_openai_fake_execution(
    config: BoundedJudgeOpenAIFakeExecutionConfig,
    *,
    runtime_config_loader: Callable[[], BoundedJudgeOpenAIFakeExecutionRuntimeConfig] = (
        load_bounded_judge_openai_fake_execution_runtime_config
    ),
    redis_reader_builder: BoundedJudgeOpenAIRedisReaderBuilder | None = None,
    repository_builder: BoundedJudgeOpenAIFakeExecutionRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedJudgeOpenAIFakeExecutionRedisPublisherBuilder | None = None,
    envelope_builder_factory: Callable[
        [BoundedJudgeOpenAIFakeExecutionRuntimeConfig], JudgeOpenAIRequestEnvelopeBuilder
    ]
    | None = None,
    fake_client_factory: Callable[[Mapping[str, Any]], FakeOpenAIClientProtocol] | None = None,
    response_mapper: OpenAIResponseMapper | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedJudgeOpenAIFakeExecutionResult:
    state = BoundedJudgeOpenAIFakeExecutionState()
    gate_error = _authority_gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedJudgeOpenAIFakeExecutionError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception as exc:
        return _result(
            "blocked",
            "runtime_config_error",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
        )

    redis_handle: BoundedJudgeOpenAIRedisReaderHandle | None = None
    repository_handle: BoundedJudgeOpenAIFakeExecutionRepositoryHandle | None = None
    publisher_handle: BoundedJudgeOpenAIFakeExecutionRedisPublisherHandle | None = None
    result: BoundedJudgeOpenAIFakeExecutionResult | None = None
    try:
        redis_handle = await (
            redis_reader_builder or build_default_bounded_judge_openai_fake_execution_redis_reader
        )(runtime_config, state, effective_logger)
        state.redis_read_attempted = True
        candidates = await redis_handle.reader.read_candidate_messages(
            queue_name=runtime_config.input_queue_name,
            config=config,
        )
        matches = [message for message in candidates if _message_matches_selectors(message, config)]
        if not matches:
            result = _result(
                "blocked",
                "redis_message_not_found",
                config=config,
                state=state,
                redis_message_count=0,
            )
            raise _BoundedResultReady
        if len(matches) > 1:
            result = _result(
                "blocked",
                "redis_message_count_exceeded",
                config=config,
                state=state,
                redis_message_count=len(matches),
            )
            raise _BoundedResultReady

        redis_message = matches[0]
        redis_error = _validate_redis_message(redis_message)
        if redis_error is not None:
            result = _result(
                "blocked",
                redis_error,
                config=config,
                state=state,
                redis_message=redis_message,
                redis_message_count=1,
            )
            raise _BoundedResultReady
        trigger_event_id = UUID(redis_message.fields["trigger_event_id"])
        root_judge_run_id = UUID(redis_message.fields["root_object_id"])

        repository_handle = await (
            repository_builder or build_default_bounded_judge_openai_fake_execution_repository
        )(runtime_config, state, effective_logger)
        repository = repository_handle.repository
        state.database_read_attempted = True

        event = await repository.load_event_outbox(trigger_event_id)
        event_error = _validate_event(
            event,
            trigger_event_id=trigger_event_id,
            root_judge_run_id=root_judge_run_id,
        )
        if event_error is not None:
            result = _result(
                "blocked",
                event_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        assert event is not None
        payload_judge_run_id = _payload_uuid(event.payload_json, "judge_run_id")
        payload_bundle_id = _payload_uuid(event.payload_json, "bundle_id")
        assert payload_judge_run_id is not None
        assert payload_bundle_id is not None
        judge_run = await repository.load_judge_run(payload_judge_run_id)
        judge_run_error = _validate_judge_run_identity(
            judge_run,
            event=event,
            payload_bundle_id=payload_bundle_id,
        )
        if judge_run_error is not None:
            result = _result(
                "blocked",
                judge_run_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                payload_bundle_id=payload_bundle_id,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        assert judge_run is not None
        bundle_record = await repository.load_bundle(judge_run.bundle_id)
        bundle_error = _validate_bundle(bundle_record)
        if bundle_error is not None:
            result = _result(
                "blocked",
                bundle_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                bundle_record=bundle_record,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        assert bundle_record is not None
        existing_lookup = await repository.load_existing_judge_output(judge_run.judge_run_id)
        if existing_lookup.count > 1:
            result = _result(
                "blocked",
                "judge_output_count_exceeded",
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                bundle_record=bundle_record,
                existing_judge_output_count=existing_lookup.count,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        if judge_run.status != "pending":
            result = _result(
                "noop",
                None,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                bundle_record=bundle_record,
                judge_output=existing_lookup.output,
                existing_judge_output_count=existing_lookup.count,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        try:
            _resolve_output_ready_route(
                _output_ready_route_probe(judge_run_id=judge_run.judge_run_id),
                route_resolver=route_resolver,
            )
        except UnsupportedOutboxEventTypeError as exc:
            result = _result(
                "blocked",
                "route_not_allowed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                bundle_record=bundle_record,
                existing_judge_output_count=existing_lookup.count,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        fake_result: OpenAIJudgeResult | None = None
        judge_output_written = False
        if existing_lookup.output is not None:
            judge_output_id = existing_lookup.output.judge_output_id
        else:
            try:
                envelope = _build_request_envelope(
                    runtime_config=runtime_config,
                    envelope_builder_factory=envelope_builder_factory,
                    judge_run=judge_run,
                    bundle=bundle_record.bundle,
                )
            except BoundedJudgeOpenAIFakeExecutionError as exc:
                result = _result(
                    "blocked",
                    exc.error_code,
                    config=config,
                    state=state,
                    redis_message=redis_message,
                    event=event,
                    judge_run=judge_run,
                    bundle_record=bundle_record,
                    existing_judge_output_count=existing_lookup.count,
                    redis_message_count=1,
                )
                raise _BoundedResultReady
            shape_summary = summarize_responses_request_shape(envelope.to_responses_request())
            if shape_summary["request_shape_valid_bucket"] != "one":
                result = _result(
                    "blocked",
                    "request_shape_invalid",
                    config=config,
                    state=state,
                    redis_message=redis_message,
                    event=event,
                    judge_run=judge_run,
                    bundle_record=bundle_record,
                    existing_judge_output_count=existing_lookup.count,
                    redis_message_count=1,
                )
                raise _BoundedResultReady
            fake_payload = build_deterministic_fake_judge_output_payload(
                candidate_group_id=bundle_record.bundle.candidate_group_id,
                judge_schema_version=judge_run.schema_version,
            )
            fake_client = (
                fake_client_factory(fake_payload)
                if fake_client_factory is not None
                else DeterministicFakeOpenAIClient(fake_payload)
            )
            state.fake_openai_called = True
            fake_response = await fake_client.create_structured_response(
                model=envelope.model,
                reasoning_effort=envelope.reasoning_effort,
                developer_prompt=envelope.developer_prompt_text,
                user_context=envelope.user_context,
                json_schema=envelope.structured_output_schema,
                max_output_tokens=envelope.max_output_tokens,
                prompt_cache_key=envelope.prompt_cache_key,
            )
            fake_result = (response_mapper or OpenAIResponseMapper()).parse(
                fake_response,
                started_monotonic=time.monotonic(),
            )
            payload_json = fake_result.payload_json
            if not _fake_payload_valid(
                payload_json,
                candidate_group_id=bundle_record.bundle.candidate_group_id,
                judge_schema_version=judge_run.schema_version,
            ):
                result = _result(
                    "blocked",
                    "fake_openai_payload_invalid",
                    config=config,
                    state=state,
                    redis_message=redis_message,
                    event=event,
                    judge_run=judge_run,
                    bundle_record=bundle_record,
                    existing_judge_output_count=existing_lookup.count,
                    redis_message_count=1,
                )
                raise _BoundedResultReady
            assert payload_json is not None
            proposed_verdict = payload_json.get("model_proposed_verdict")
            confidence_band = payload_json.get("model_confidence_band")
            state.database_write_attempted = True
            judge_output_id = await repository.insert_judge_output(
                judge_run_id=judge_run.judge_run_id,
                candidate_group_id=bundle_record.bundle.candidate_group_id,
                judge_schema_version=judge_run.schema_version,
                payload_json=payload_json,
                model_proposed_verdict=proposed_verdict if isinstance(proposed_verdict, str) else None,
                model_confidence_band=confidence_band if isinstance(confidence_band, str) else None,
            )
            judge_output_written = True

        state.database_write_attempted = True
        await repository.finish_judge_run_succeeded(
            judge_run_id=judge_run.judge_run_id,
            result=fake_result,
            finish_reason=FAKE_FINISH_REASON if fake_result is not None else "existing_judge_output_reused",
            refusal_detected=False,
        )
        ready_outbox, ready_outbox_written = await repository.insert_or_load_judge_output_ready_outbox(
            judge_run_id=judge_run.judge_run_id,
            judge_output_id=judge_output_id,
            finish_reason=FAKE_FINISH_REASON if fake_result is not None else "existing_judge_output_reused",
            refusal_detected=False,
        )
        try:
            await repository.commit()
        except Exception as exc:
            result = _result(
                "failed",
                "database_commit_failed_before_redis_publish",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=replace(judge_run, status="succeeded"),
                bundle_record=bundle_record,
                judge_output_id=judge_output_id,
                judge_output_written=judge_output_written,
                ready_outbox=ready_outbox,
                ready_outbox_written=ready_outbox_written,
                existing_judge_output_count=existing_lookup.count,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        if ready_outbox.status == "published":
            result = _result(
                "noop",
                None,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=replace(judge_run, status="succeeded"),
                bundle_record=bundle_record,
                judge_output_id=judge_output_id,
                judge_output_written=judge_output_written,
                ready_outbox=ready_outbox,
                ready_outbox_written=ready_outbox_written,
                existing_judge_output_count=existing_lookup.count,
                redis_message_count=1,
            )
            raise _BoundedResultReady
        if ready_outbox.status != "pending":
            result = _result(
                "blocked",
                "judge_output_ready_outbox_not_pending",
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=replace(judge_run, status="succeeded"),
                bundle_record=bundle_record,
                judge_output_id=judge_output_id,
                judge_output_written=judge_output_written,
                ready_outbox=ready_outbox,
                ready_outbox_written=ready_outbox_written,
                existing_judge_output_count=existing_lookup.count,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        route = _resolve_output_ready_route(ready_outbox, route_resolver=route_resolver)
        publisher_handle = await (
            redis_publisher_builder or build_default_bounded_judge_openai_fake_execution_redis_publisher
        )(runtime_config, state, effective_logger)
        message = _build_output_ready_stream_message(ready_outbox, route)
        state.redis_publish_attempted = True
        redis_message_id = await publisher_handle.publisher.publish(route, message)
        state.database_write_attempted = True
        await repository.mark_output_ready_outbox_published(
            event_id=ready_outbox.event_id,
            judge_run_id=judge_run.judge_run_id,
            published_at=(clock or _utc_now)(),
        )
        await repository.insert_publish_job_attempt(
            stage_name=route.stage_name,
            queue_name=route.queue_name,
            root_object_type=ready_outbox.aggregate_type,
            root_object_id=ready_outbox.aggregate_id,
            attempt_status="succeeded",
            error_code=None,
        )
        try:
            await repository.commit()
        except Exception as exc:
            result = _result(
                "failed",
                "database_commit_failed_after_redis_publish",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=replace(judge_run, status="succeeded"),
                bundle_record=bundle_record,
                judge_output_id=judge_output_id,
                judge_output_written=judge_output_written,
                ready_outbox=ready_outbox,
                ready_outbox_written=ready_outbox_written,
                redis_output_message_id=redis_message_id,
                queue_name=route.queue_name,
                stage_name=route.stage_name,
                existing_judge_output_count=existing_lookup.count,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        result = _result(
            "published",
            None,
            config=config,
            state=state,
            redis_message=redis_message,
            event=event,
            judge_run=replace(judge_run, status="succeeded"),
            bundle_record=bundle_record,
            judge_output_id=judge_output_id,
            judge_output_written=judge_output_written,
            ready_outbox=replace(ready_outbox, status="published"),
            ready_outbox_written=ready_outbox_written,
            ready_outbox_published=True,
            redis_output_message_id=redis_message_id,
            queue_name=route.queue_name,
            stage_name=route.stage_name,
            existing_judge_output_count=existing_lookup.count,
            redis_message_count=1,
        )
    except _BoundedResultReady:
        pass
    except UnsupportedOutboxEventTypeError as exc:
        result = _result(
            "blocked",
            "route_not_allowed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
        )
    except Exception as exc:
        error_code = "redis_xadd_failed" if state.redis_publish_attempted else "bounded_fake_execution_failed"
        result = _result(
            "failed",
            error_code,
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
        )
    finally:
        if publisher_handle is not None:
            try:
                await publisher_handle.close()
            except Exception as exc:
                result = _close_failed_result(
                    existing=result,
                    error_code="redis_publisher_close_failed",
                    error_class=_safe_exception_class(exc),
                    config=config,
                    state=state,
                )
        if repository_handle is not None:
            try:
                await repository_handle.repository.rollback()
                await repository_handle.close()
            except Exception as exc:
                result = _close_failed_result(
                    existing=result,
                    error_code="repository_close_failed",
                    error_class=_safe_exception_class(exc),
                    config=config,
                    state=state,
                )
        if redis_handle is not None:
            try:
                await redis_handle.close()
            except Exception as exc:
                result = _close_failed_result(
                    existing=result,
                    error_code="redis_reader_close_failed",
                    error_class=_safe_exception_class(exc),
                    config=config,
                    state=state,
                )

    assert result is not None
    return result


def run_bounded_judge_openai_fake_execution_sync(
    config: BoundedJudgeOpenAIFakeExecutionConfig,
    *,
    runtime_config_loader: Callable[[], BoundedJudgeOpenAIFakeExecutionRuntimeConfig] = (
        load_bounded_judge_openai_fake_execution_runtime_config
    ),
    redis_reader_builder: BoundedJudgeOpenAIRedisReaderBuilder | None = None,
    repository_builder: BoundedJudgeOpenAIFakeExecutionRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedJudgeOpenAIFakeExecutionRedisPublisherBuilder | None = None,
    envelope_builder_factory: Callable[
        [BoundedJudgeOpenAIFakeExecutionRuntimeConfig], JudgeOpenAIRequestEnvelopeBuilder
    ]
    | None = None,
    fake_client_factory: Callable[[Mapping[str, Any]], FakeOpenAIClientProtocol] | None = None,
    response_mapper: OpenAIResponseMapper | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedJudgeOpenAIFakeExecutionResult:
    return asyncio.run(
        run_bounded_judge_openai_fake_execution(
            config,
            runtime_config_loader=runtime_config_loader,
            redis_reader_builder=redis_reader_builder,
            repository_builder=repository_builder,
            redis_publisher_builder=redis_publisher_builder,
            envelope_builder_factory=envelope_builder_factory,
            fake_client_factory=fake_client_factory,
            response_mapper=response_mapper,
            route_resolver=route_resolver,
            clock=clock,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        error_code,
        config=BoundedJudgeOpenAIFakeExecutionConfig(),
        state=BoundedJudgeOpenAIFakeExecutionState(),
    ).to_sanitized_dict()


def build_deterministic_fake_judge_output_payload(
    *,
    candidate_group_id: UUID,
    judge_schema_version: str,
) -> dict[str, Any]:
    return {
        "judge_schema_version": judge_schema_version,
        "candidate_group_id": str(candidate_group_id),
        "headline": "Bounded fake judge output",
        "summary_one_line_ko": "Bounded fake OpenAI response for judge-output pipeline validation.",
        "skeptical_take_ko": "This is fake execution only and must not be treated as live model evidence.",
        "why_it_might_matter_ko": "It validates the durable JudgeOutput to analysis-validator queue handoff.",
        "comparables": [],
        "scores": {
            "novelty": 20,
            "practical_usefulness": 30,
            "evidence_strength": 20,
            "hype_penalty": 80,
            "confidence": 25,
            "code_quality": None,
            "maintenance_signal": None,
            "specificity": 20,
            "reproducibility_signal": None,
        },
        "reason_codes": [
            "bounded_fake_openai_execution",
            "pipeline_validation_only",
            "conservative_default",
            "comparison_gap",
        ],
        "red_flags_ko": [
            "Fake OpenAI execution; no live model judgment was performed.",
        ],
        "evidence_limitations_ko": [
            "No raw bundle text is included in this deterministic fake payload.",
            "Use only to validate the judge output ready handoff.",
        ],
        "recommended_action_ko": "Continue to validator only after review approval of this bounded fake run.",
        "freshness_note_ko": "Deterministic fake payload generated for exact-target pipeline validation.",
        "model_proposed_verdict": "later",
        "model_confidence_band": "low",
    }


def _build_request_envelope(
    *,
    runtime_config: BoundedJudgeOpenAIFakeExecutionRuntimeConfig,
    envelope_builder_factory: Callable[
        [BoundedJudgeOpenAIFakeExecutionRuntimeConfig], JudgeOpenAIRequestEnvelopeBuilder
    ]
    | None,
    judge_run: JudgeRunRecord,
    bundle: BundleJudgeContext,
) -> JudgeOpenAIRequestEnvelope:
    builder = (
        envelope_builder_factory(runtime_config)
        if envelope_builder_factory is not None
        else _default_envelope_builder(runtime_config)
    )
    try:
        return builder.build(judge_run=judge_run, bundle=bundle)
    except (UnsupportedJudgeProfileError, UnsupportedPromptVersionError) as exc:
        raise BoundedJudgeOpenAIFakeExecutionError("unsupported_prompt_or_profile") from exc
    except JudgeOpenAIRequestEnvelopeError as exc:
        raise BoundedJudgeOpenAIFakeExecutionError("request_envelope_invalid") from exc


def _default_envelope_builder(
    runtime_config: BoundedJudgeOpenAIFakeExecutionRuntimeConfig,
) -> JudgeOpenAIRequestEnvelopeBuilder:
    preflight = (
        HeuristicSanitizingPreflight()
        if runtime_config.enable_prompt_guard_preflight
        else NoopModelContextPreflight()
    )
    return JudgeOpenAIRequestEnvelopeBuilder(
        context_builder=JudgeContextBuilder(preflight=preflight),
        max_output_tokens=runtime_config.max_output_tokens,
        request_timeout_sec=runtime_config.request_timeout_sec,
    )


def _authority_gate_error(config: BoundedJudgeOpenAIFakeExecutionConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    if not _valid_scan_limit(config.scan_limit):
        return "invalid_scan_limit"
    if not config.redis_message_suffix or not config.trigger_event_suffix:
        return "target_missing"
    if not REDIS_ID_SUFFIX_RE.fullmatch(config.redis_message_suffix):
        return "invalid_redis_message_suffix"
    if not UUID_SUFFIX_RE.fullmatch(config.trigger_event_suffix):
        return "invalid_trigger_event_suffix"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_redis_read:
        return "redis_read_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if not config.allow_database_write:
        return "database_write_not_allowed"
    if not config.allow_redis_publish:
        return "redis_publish_not_allowed"
    if not config.allow_fake_openai:
        return "fake_openai_not_allowed"
    return None


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedJudgeOpenAIFakeExecutionConfig,
    state: BoundedJudgeOpenAIFakeExecutionState,
    error_class: str | None = None,
    redis_message: RedisStreamMessage | None = None,
    event: JudgeCallOutboxRecord | None = None,
    judge_run: JudgeRunRecord | None = None,
    payload_bundle_id: UUID | None = None,
    bundle_record: BundleEnvelopeRecord | None = None,
    judge_output: JudgeOutputRecord | None = None,
    judge_output_id: UUID | None = None,
    judge_output_written: bool = False,
    ready_outbox: OutboxEventRow | None = None,
    ready_outbox_written: bool = False,
    ready_outbox_published: bool = False,
    redis_output_message_id: str | None = None,
    existing_judge_output_count: int = 0,
    redis_message_count: int = 0,
    queue_name: str | None = None,
    stage_name: str | None = None,
) -> BoundedJudgeOpenAIFakeExecutionResult:
    bundle = bundle_record.bundle if bundle_record is not None else None
    event_payload = event.payload_json if event is not None and isinstance(event.payload_json, Mapping) else {}
    event_payload_bundle_id = payload_bundle_id or _payload_uuid(event_payload, "bundle_id")
    resolved_judge_output_id = judge_output_id or (
        judge_output.judge_output_id if judge_output is not None else None
    )
    return BoundedJudgeOpenAIFakeExecutionResult(
        status=status,
        ok=status in {"published", "noop"} and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        target_redis_message_id_suffix=_redis_message_id_suffix(
            redis_message.message_id if redis_message is not None else None
        )
        or config.redis_message_suffix,
        target_trigger_event_id_suffix=_optional_id_suffix(
            _safe_uuid(redis_message.fields.get("trigger_event_id")) if redis_message is not None else None
        )
        or config.trigger_event_suffix,
        target_judge_run_id_suffix=_optional_id_suffix(
            judge_run.judge_run_id
            if judge_run is not None
            else _safe_uuid(redis_message.fields.get("root_object_id")) if redis_message is not None else None
        ),
        target_bundle_id_suffix=_optional_id_suffix(
            bundle.bundle_id if bundle is not None else event_payload_bundle_id
        ),
        target_candidate_group_suffix=_optional_id_suffix(bundle.candidate_group_id if bundle is not None else None),
        target_judge_output_id_suffix=_optional_id_suffix(resolved_judge_output_id),
        judge_output_written=judge_output_written,
        judge_run_status=judge_run.status if judge_run is not None else None,
        judge_output_ready_outbox_written=ready_outbox_written,
        judge_output_ready_event_suffix=_optional_id_suffix(ready_outbox.event_id if ready_outbox else None),
        judge_output_ready_published=ready_outbox_published,
        q_analysis_validate_message_id_suffix=_redis_message_id_suffix(redis_output_message_id),
        model=judge_run.model if judge_run is not None else _payload_string(event_payload, "model"),
        reasoning_effort=judge_run.reasoning_effort
        if judge_run is not None
        else _payload_string(event_payload, "reasoning_effort"),
        judge_profile=judge_run.judge_profile if judge_run is not None else None,
        prompt_version=judge_run.prompt_version if judge_run is not None else _payload_string(event_payload, "prompt_version"),
        schema_version_value=judge_run.schema_version if judge_run is not None else None,
        policy_version=judge_run.policy_version if judge_run is not None else None,
        redis_message_count=redis_message_count,
        event_outbox_found=event is not None,
        judge_run_found=judge_run is not None,
        bundle_found=bundle_record is not None,
        existing_judge_output_count=existing_judge_output_count,
        queue_name=queue_name,
        stage_name=stage_name,
    )


def _close_failed_result(
    *,
    existing: BoundedJudgeOpenAIFakeExecutionResult | None,
    error_code: str,
    error_class: str,
    config: BoundedJudgeOpenAIFakeExecutionConfig,
    state: BoundedJudgeOpenAIFakeExecutionState,
) -> BoundedJudgeOpenAIFakeExecutionResult:
    if existing is None:
        return _result(
            "failed",
            error_code,
            error_class=error_class,
            config=config,
            state=state,
        )
    return replace(existing, status="failed", ok=False, error_code=error_code, error_class=error_class)


def _validate_redis_message(message: RedisStreamMessage) -> str | None:
    if FORBIDDEN_REDIS_BUSINESS_FIELDS & set(message.fields):
        return "redis_message_forbidden_business_fields"
    if REQUIRED_REDIS_FIELDS - set(message.fields):
        return "redis_message_required_fields_missing"
    if message.fields.get("stage_name") != INPUT_STAGE_NAME:
        return "redis_message_wrong_stage"
    if message.fields.get("root_object_type") != ROOT_OBJECT_TYPE:
        return "redis_message_wrong_root_object_type"
    if _safe_uuid(message.fields.get("trigger_event_id")) is None:
        return "redis_message_invalid_trigger_event_id"
    if _safe_uuid(message.fields.get("job_id")) is None:
        return "redis_message_invalid_job_id"
    if _safe_uuid(message.fields.get("root_object_id")) is None:
        return "redis_message_invalid_root_object_id"
    if message.fields.get("job_id") != message.fields.get("trigger_event_id"):
        return "redis_message_job_trigger_mismatch"
    return None


def _validate_event(
    event: JudgeCallOutboxRecord | None,
    *,
    trigger_event_id: UUID,
    root_judge_run_id: UUID,
) -> str | None:
    if event is None:
        return "event_outbox_missing"
    if event.event_id != trigger_event_id:
        return "event_outbox_id_mismatch"
    if event.event_type != INPUT_EVENT_TYPE:
        return "event_outbox_wrong_event_type"
    if event.status != "published":
        return "event_outbox_not_published"
    if event.aggregate_type != ROOT_OBJECT_TYPE:
        return "event_outbox_wrong_aggregate_type"
    if event.aggregate_id != root_judge_run_id:
        return "event_outbox_aggregate_mismatch"
    if not isinstance(event.payload_json, dict):
        return "event_payload_malformed"
    if REQUIRED_EVENT_PAYLOAD_FIELDS - set(event.payload_json):
        return "event_payload_missing_required_field"
    payload_judge_run_id = _payload_uuid(event.payload_json, "judge_run_id")
    payload_bundle_id = _payload_uuid(event.payload_json, "bundle_id")
    if payload_judge_run_id is None or payload_bundle_id is None:
        return "event_payload_malformed"
    if payload_judge_run_id != event.aggregate_id:
        return "event_payload_judge_run_id_mismatch"
    for field_name in ("model", "reasoning_effort", "prompt_version", "prompt_cache_key"):
        if not _payload_string(event.payload_json, field_name):
            return "event_payload_missing_required_field"
    return None


def _validate_judge_run_identity(
    judge_run: JudgeRunRecord | None,
    *,
    event: JudgeCallOutboxRecord,
    payload_bundle_id: UUID,
) -> str | None:
    if judge_run is None:
        return "judge_run_missing"
    if judge_run.judge_run_id != event.aggregate_id:
        return "judge_run_id_mismatch"
    if judge_run.bundle_id != payload_bundle_id:
        return "judge_run_bundle_mismatch"
    if judge_run.model != _payload_string(event.payload_json, "model"):
        return "judge_run_model_mismatch"
    if judge_run.reasoning_effort != _payload_string(event.payload_json, "reasoning_effort"):
        return "judge_run_reasoning_effort_mismatch"
    if judge_run.prompt_version != _payload_string(event.payload_json, "prompt_version"):
        return "judge_run_prompt_version_mismatch"
    payload_prompt_cache_key = _payload_string(event.payload_json, "prompt_cache_key")
    if payload_prompt_cache_key and judge_run.prompt_cache_key != payload_prompt_cache_key:
        return "judge_run_prompt_cache_key_mismatch"
    if not all(
        [
            judge_run.model,
            judge_run.reasoning_effort,
            judge_run.prompt_version,
            judge_run.schema_version,
            judge_run.policy_version,
            judge_run.prompt_cache_key,
            judge_run.judge_profile,
        ]
    ):
        return "judge_run_required_field_missing"
    return None


def _validate_bundle(bundle_record: BundleEnvelopeRecord | None) -> str | None:
    if bundle_record is None:
        return "bundle_missing"
    bundle = bundle_record.bundle
    if not bundle_record.ready_for_analysis:
        return "bundle_not_ready"
    if bundle.candidate_group_id is None:
        return "bundle_candidate_group_missing"
    if bundle.current_primary_artifact_id is None:
        return "bundle_primary_artifact_missing"
    if not isinstance(bundle.primary_summary, dict) or not bundle.primary_summary:
        return "bundle_primary_summary_missing"
    if not isinstance(bundle.supporting_summaries_json, list):
        return "bundle_supporting_summaries_invalid"
    if not isinstance(bundle.discovered_links_summary_json, list):
        return "bundle_discovered_links_invalid"
    if not isinstance(bundle.evidence_limitations, list):
        return "bundle_evidence_limitations_invalid"
    return None


def _fake_payload_valid(
    payload: Mapping[str, Any] | None,
    *,
    candidate_group_id: UUID,
    judge_schema_version: str,
) -> bool:
    if not isinstance(payload, Mapping):
        return False
    if tuple(payload) != REQUIRED_FAKE_OUTPUT_FIELDS:
        return False
    if payload.get("judge_schema_version") != judge_schema_version:
        return False
    if payload.get("candidate_group_id") != str(candidate_group_id):
        return False
    if payload.get("model_proposed_verdict") not in {"inspect_now", "later", "skip", None}:
        return False
    if payload.get("model_confidence_band") not in {"low", "medium", "high", None}:
        return False
    scores = payload.get("scores")
    if not isinstance(scores, Mapping) or tuple(scores) != REQUIRED_SCORE_FIELDS:
        return False
    for key, value in scores.items():
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 100:
            return False
    for key in ("comparables", "reason_codes", "red_flags_ko", "evidence_limitations_ko"):
        if not isinstance(payload.get(key), list):
            return False
    return "bounded_fake_openai_execution" in payload.get("reason_codes", [])


def _message_matches_selectors(
    message: RedisStreamMessage,
    config: BoundedJudgeOpenAIFakeExecutionConfig,
) -> bool:
    trigger_event_id = message.fields.get("trigger_event_id", "")
    return bool(
        message.message_id.endswith(config.redis_message_suffix or "")
        and trigger_event_id.endswith(config.trigger_event_suffix or "")
    )


def _resolve_output_ready_route(
    row: OutboxEventRow,
    *,
    route_resolver: OutboxRouteResolver | None,
) -> QueueRoute:
    canonical_route = OutboxRouteResolver().resolve(row)
    resolved_route = route_resolver.resolve(row) if route_resolver is not None else canonical_route
    if (
        resolved_route != canonical_route
        or resolved_route.queue_name != OUTPUT_QUEUE_NAME
        or resolved_route.stage_name != OUTPUT_STAGE_NAME
    ):
        raise UnsupportedOutboxEventTypeError("judge_output_ready_route_not_allowed")
    return resolved_route


def _build_output_ready_stream_message(row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
    return RedisQueuedMessage(
        job_id=str(row.event_id),
        stage_name=route.stage_name,
        root_object_type=row.aggregate_type,
        root_object_id=str(row.aggregate_id),
        idempotency_key=row.dedupe_key,
        pipeline_run_id=_payload_string(row.payload_json, "pipeline_run_id"),
        not_before=_payload_string(row.payload_json, "not_before"),
        trigger_event_id=str(row.event_id),
    )


def _output_ready_route_probe(*, judge_run_id: UUID) -> OutboxEventRow:
    return OutboxEventRow(
        event_id=judge_run_id,
        event_type=OUTPUT_EVENT_TYPE,
        aggregate_type=ROOT_OBJECT_TYPE,
        aggregate_id=judge_run_id,
        dedupe_key="route-probe",
        payload_json={},
        status="pending",
        fail_count=0,
        created_at=_utc_now(),
    )


def _outbox_row_from_mapping(row: Mapping[str, Any]) -> OutboxEventRow:
    payload = _json_loads(row["payload_json"]) or {}
    return OutboxEventRow(
        event_id=UUID(str(row["event_id"])),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=UUID(str(row["aggregate_id"])),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload if isinstance(payload, dict) else {},
        status=str(row["status"]),
        fail_count=int(row["fail_count"]),
        created_at=row["created_at"],
    )


def _judge_output_ready_dedupe_key(*, judge_run_id: UUID, judge_output_id: UUID) -> str:
    return f"judge-output-ready:{judge_run_id}:{judge_output_id}"


def _valid_scan_limit(value: int) -> bool:
    return 1 <= value <= MAX_SCAN_LIMIT


def _payload_uuid(payload: Mapping[str, Any], field_name: str) -> UUID | None:
    return _safe_uuid(payload.get(field_name))


def _payload_string(payload: Mapping[str, Any], field_name: str) -> str | None:
    value = payload.get(field_name)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _safe_uuid(value: Any) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _optional_id_suffix(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    return str(value)[-8:]


def _redis_message_id_suffix(value: str | None) -> str | None:
    if not value:
        return None
    normalized = str(value)
    return normalized[-8:] if len(normalized) > 8 else "present"


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _jsonb_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _env_value(env: Mapping[str, str], name: str, default: str = "") -> str:
    return str(env.get(name, default)).strip()


def _parse_bool(raw_value: str) -> bool:
    return raw_value.lower() not in {"", "0", "false", "no"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)


__all__ = [
    "BoundedJudgeOpenAIFakeExecutionConfig",
    "BoundedJudgeOpenAIFakeExecutionError",
    "BoundedJudgeOpenAIFakeExecutionRedisPublisherBuilder",
    "BoundedJudgeOpenAIFakeExecutionRedisPublisherHandle",
    "BoundedJudgeOpenAIFakeExecutionRepositoryBuilder",
    "BoundedJudgeOpenAIFakeExecutionRepositoryHandle",
    "BoundedJudgeOpenAIFakeExecutionResult",
    "BoundedJudgeOpenAIFakeExecutionRuntimeConfig",
    "BoundedJudgeOpenAIFakeExecutionState",
    "BoundedJudgeOpenAIRedisReaderBuilder",
    "BoundedJudgeOpenAIRedisReaderHandle",
    "DeterministicFakeOpenAIClient",
    "ExistingJudgeOutputLookup",
    "FakeOpenAIClientProtocol",
    "JudgeOutputRecord",
    "argument_error_report",
    "build_deterministic_fake_judge_output_payload",
    "load_bounded_judge_openai_fake_execution_runtime_config",
    "render_sanitized_json",
    "run_bounded_judge_openai_fake_execution",
    "run_bounded_judge_openai_fake_execution_sync",
]
