from __future__ import annotations

import asyncio
import json
import logging
import os
import re
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


SCHEMA_VERSION = "bounded_analysis_validator_policy_request_v1"
RUNNER_NAME = "bounded_analysis_validator_policy_request_runner"
MODE = "analysis_validator_exact_target_policy_request_publish"
INPUT_QUEUE_NAME = "q.analysis.validate"
INPUT_STAGE_NAME = "analysis_validate"
INPUT_EVENT_TYPE = "judge.output.ready.v1"
OUTPUT_EVENT_TYPE = "analysis.policy.apply.v1"
OUTPUT_QUEUE_NAME = "q.analysis.policy"
OUTPUT_STAGE_NAME = "analysis_policy"
ROOT_OBJECT_TYPE = "judge_run"
DEFAULT_SCAN_LIMIT = 25
MAX_SCAN_LIMIT = 500
DEFAULT_XADD_MAXLEN = 10000

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
        "judge_output_id",
        "model",
        "reasoning_effort",
        "prompt_version",
        "prompt_cache_key",
        "raw_text",
        "bundle_data",
        "judge_output_payload",
    }
)
REQUIRED_EVENT_PAYLOAD_FIELDS = frozenset(
    {
        "judge_run_id",
        "judge_output_id",
        "finish_reason",
        "refusal_detected",
    }
)
REQUIRED_OUTPUT_FIELDS = frozenset(
    {
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
    }
)
REQUIRED_SCORE_FIELDS = frozenset(
    {
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
)
REQUIRED_INTEGER_SCORE_FIELDS = frozenset(
    {
        "novelty",
        "practical_usefulness",
        "evidence_strength",
        "hype_penalty",
        "confidence",
    }
)
OPTIONAL_NULLABLE_SCORE_FIELDS = frozenset(
    {
        "code_quality",
        "maintenance_signal",
        "specificity",
        "reproducibility_signal",
    }
)
STRING_FIELDS = frozenset(
    {
        "judge_schema_version",
        "candidate_group_id",
        "headline",
        "summary_one_line_ko",
        "skeptical_take_ko",
        "why_it_might_matter_ko",
        "recommended_action_ko",
        "freshness_note_ko",
    }
)
ARRAY_FIELDS = frozenset(
    {
        "comparables",
        "reason_codes",
        "red_flags_ko",
        "evidence_limitations_ko",
    }
)
MODEL_VERDICTS = frozenset({"inspect_now", "later", "skip", None})
CONFIDENCE_BANDS = frozenset({"low", "medium", "high", None})


@dataclass(frozen=True, slots=True)
class BoundedAnalysisValidatorPolicyRequestConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_read: bool = False
    allow_redis_publish: bool = False
    allow_database_read: bool = False
    allow_database_write: bool = False
    allow_analysis_validator: bool = False
    redis_message_suffix: str | None = None
    trigger_event_suffix: str | None = None
    judge_output_suffix: str | None = None
    judge_run_suffix: str | None = None
    scan_limit: int = DEFAULT_SCAN_LIMIT


@dataclass(frozen=True, slots=True)
class BoundedAnalysisValidatorPolicyRequestRuntimeConfig:
    database_url: str
    redis_url: str
    input_queue_name: str = INPUT_QUEUE_NAME
    xadd_maxlen: int | None = DEFAULT_XADD_MAXLEN


@dataclass(slots=True)
class BoundedAnalysisValidatorPolicyRequestState:
    runtime_config_loaded: bool = False
    redis_reader_created: bool = False
    redis_read_attempted: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    redis_publisher_created: bool = False
    redis_publish_attempted: bool = False
    analysis_validator_called: bool = False


@dataclass(frozen=True, slots=True)
class RedisStreamMessage:
    message_id: str
    fields: dict[str, str]


@dataclass(frozen=True, slots=True)
class JudgeRunValidationRecord:
    judge_run_id: UUID
    bundle_id: UUID
    schema_version: str
    status: str
    finish_reason: str | None
    refusal_detected: bool


@dataclass(frozen=True, slots=True)
class JudgeOutputValidationRecord:
    judge_output_id: UUID
    judge_run_id: UUID
    candidate_group_id: UUID
    judge_schema_version: str
    payload_json: dict[str, Any]
    model_proposed_verdict: str | None
    model_confidence_band: str | None


@dataclass(frozen=True, slots=True)
class BundleValidationRecord:
    bundle_id: UUID
    candidate_group_id: UUID
    ready_for_analysis: bool


class BoundedAnalysisValidatorPolicyRequestError(RuntimeError):
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
        config: BoundedAnalysisValidatorPolicyRequestConfig,
    ) -> list[RedisStreamMessage]: ...


class RedisPublisher(Protocol):
    async def publish(self, route: QueueRoute, message: RedisQueuedMessage) -> str: ...


class BoundedAnalysisValidatorPolicyRequestRepository(Protocol):
    async def load_event_outbox(self, trigger_event_id: UUID) -> OutboxEventRow | None: ...
    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunValidationRecord | None: ...
    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputValidationRecord | None: ...
    async def load_bundle(self, bundle_id: UUID) -> BundleValidationRecord | None: ...
    async def load_policy_apply_outboxes(
        self,
        *,
        judge_run_id: UUID,
        judge_output_id: UUID,
        candidate_group_id: UUID,
        bundle_id: UUID,
    ) -> list[OutboxEventRow]: ...
    async def insert_state_transition(
        self,
        *,
        object_type: str,
        object_id: UUID,
        from_state: str | None,
        to_state: str,
        reason_code: str,
    ) -> None: ...
    async def insert_or_load_policy_apply_outbox(
        self,
        *,
        judge_run_id: UUID,
        judge_output_id: UUID,
        candidate_group_id: UUID,
        bundle_id: UUID,
    ) -> tuple[OutboxEventRow, bool]: ...
    async def mark_policy_apply_outbox_published(
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


@dataclass(frozen=True, slots=True)
class BoundedAnalysisValidatorRedisReaderHandle:
    reader: ReadOnlyRedisMessageReader
    close: Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedAnalysisValidatorRepositoryHandle:
    repository: BoundedAnalysisValidatorPolicyRequestRepository
    close: Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BoundedAnalysisValidatorRedisPublisherHandle:
    publisher: RedisPublisher
    close: Callable[[], Awaitable[None]]


class BoundedAnalysisValidatorRedisReaderBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedAnalysisValidatorPolicyRequestRuntimeConfig,
        state: BoundedAnalysisValidatorPolicyRequestState,
        logger: logging.Logger,
    ) -> BoundedAnalysisValidatorRedisReaderHandle: ...


class BoundedAnalysisValidatorRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedAnalysisValidatorPolicyRequestRuntimeConfig,
        state: BoundedAnalysisValidatorPolicyRequestState,
        logger: logging.Logger,
    ) -> BoundedAnalysisValidatorRepositoryHandle: ...


class BoundedAnalysisValidatorRedisPublisherBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedAnalysisValidatorPolicyRequestRuntimeConfig,
        state: BoundedAnalysisValidatorPolicyRequestState,
        logger: logging.Logger,
    ) -> BoundedAnalysisValidatorRedisPublisherHandle: ...


@dataclass(frozen=True, slots=True)
class BoundedAnalysisValidatorPolicyRequestResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedAnalysisValidatorPolicyRequestConfig
    state: BoundedAnalysisValidatorPolicyRequestState = field(
        default_factory=BoundedAnalysisValidatorPolicyRequestState
    )
    target_redis_message_id_suffix: str | None = None
    target_trigger_event_id_suffix: str | None = None
    target_judge_run_id_suffix: str | None = None
    target_judge_output_id_suffix: str | None = None
    target_bundle_id_suffix: str | None = None
    target_candidate_group_suffix: str | None = None
    policy_apply_outbox_written: bool = False
    policy_apply_event_suffix: str | None = None
    policy_apply_published: bool = False
    q_analysis_policy_message_id_suffix: str | None = None
    validation_status: str | None = None
    validation_error_count: int = 0
    state_transition_written: bool = False
    queue_name: str | None = None
    stage_name: str | None = None
    redis_message_count: int = 0
    event_outbox_found: bool = False
    judge_run_found: bool = False
    judge_output_found: bool = False
    bundle_found: bool = False

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
            "target_judge_output_id_suffix": self.target_judge_output_id_suffix,
            "target_bundle_id_suffix": self.target_bundle_id_suffix,
            "target_candidate_group_suffix": self.target_candidate_group_suffix,
            "policy_apply_outbox_written": self.policy_apply_outbox_written,
            "policy_apply_event_suffix": self.policy_apply_event_suffix,
            "policy_apply_published": self.policy_apply_published,
            "q_analysis_policy_message_id_suffix": self.q_analysis_policy_message_id_suffix,
            "validation_status": self.validation_status,
            "validation_error_count": self.validation_error_count,
            "state_transition_written": self.state_transition_written,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "redis_message_count": self.redis_message_count,
            "event_outbox_found": self.event_outbox_found,
            "judge_run_found": self.judge_run_found,
            "judge_output_found": self.judge_output_found,
            "bundle_found": self.bundle_found,
            "redis_read_attempted": self.state.redis_read_attempted,
            "redis_publish_attempted": self.state.redis_publish_attempted,
            "redis_ack_called": False,
            "redis_consume_called": False,
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": self.state.database_write_attempted,
            "analysis_validator_called": self.state.analysis_validator_called,
            "policy_called": False,
            "notifier_called": False,
            "telegram_send_called": False,
            "openai_called": False,
            "github_api_called": False,
            "x_api_called": False,
            "web_fetch_called": False,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "redis_read_allowed": self.config.allow_redis_read,
                "redis_publish_allowed": self.config.allow_redis_publish,
                "database_read_allowed": self.config.allow_database_read,
                "database_write_allowed": self.config.allow_database_write,
                "analysis_validator_allowed": self.config.allow_analysis_validator,
                "scan_limit": self.config.scan_limit,
            },
            "redactions_applied": {
                "full_redis_message_id_omitted": True,
                "full_trigger_event_id_omitted": True,
                "full_judge_run_id_omitted": True,
                "full_judge_output_id_omitted": True,
                "full_bundle_id_omitted": True,
                "full_candidate_group_id_omitted": True,
                "full_policy_apply_event_id_omitted": True,
                "idempotency_key_omitted": True,
                "judge_output_payload_omitted": True,
                "bundle_context_omitted": True,
                "raw_source_text_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class RedisReadOnlyStreamReader:
    def __init__(self, client: Any) -> None:
        self._client = client

    async def read_candidate_messages(
        self,
        *,
        queue_name: str,
        config: BoundedAnalysisValidatorPolicyRequestConfig,
    ) -> list[RedisStreamMessage]:
        raw_messages = await self._client.xrevrange(queue_name, count=config.scan_limit)
        return [_normalize_redis_message(message) for message in raw_messages]


class SqlAlchemyBoundedAnalysisValidatorPolicyRequestRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def load_event_outbox(self, trigger_event_id: UUID) -> OutboxEventRow | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       dedupe_key, payload_json, status, fail_count, created_at
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(trigger_event_id)},
        )
        row = result.mappings().first()
        return _outbox_row_from_mapping(row) if row is not None else None

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunValidationRecord | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT judge_run_id, bundle_id, schema_version, status,
                       finish_reason, refusal_detected
                FROM judge_runs
                WHERE judge_run_id = CAST(:judge_run_id AS uuid)
                """
            ),
            {"judge_run_id": str(judge_run_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return JudgeRunValidationRecord(
            judge_run_id=UUID(str(row["judge_run_id"])),
            bundle_id=UUID(str(row["bundle_id"])),
            schema_version=str(row["schema_version"]),
            status=str(row["status"]),
            finish_reason=_string_or_none(row["finish_reason"]),
            refusal_detected=bool(row["refusal_detected"]),
        )

    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputValidationRecord | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT judge_output_id, judge_run_id, candidate_group_id,
                       judge_schema_version, payload_json, model_proposed_verdict,
                       model_confidence_band
                FROM judge_outputs
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                """
            ),
            {"judge_output_id": str(judge_output_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        payload = _json_loads(row["payload_json"]) or {}
        return JudgeOutputValidationRecord(
            judge_output_id=UUID(str(row["judge_output_id"])),
            judge_run_id=UUID(str(row["judge_run_id"])),
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            judge_schema_version=str(row["judge_schema_version"]),
            payload_json=payload if isinstance(payload, dict) else {},
            model_proposed_verdict=_string_or_none(row["model_proposed_verdict"]),
            model_confidence_band=_string_or_none(row["model_confidence_band"]),
        )

    async def load_bundle(self, bundle_id: UUID) -> BundleValidationRecord | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT bundle_id, candidate_group_id, ready_for_analysis
                FROM candidate_evidence_bundles
                WHERE bundle_id = CAST(:bundle_id AS uuid)
                """
            ),
            {"bundle_id": str(bundle_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        return BundleValidationRecord(
            bundle_id=UUID(str(row["bundle_id"])),
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            ready_for_analysis=bool(row["ready_for_analysis"]),
        )

    async def load_policy_apply_outboxes(
        self,
        *,
        judge_run_id: UUID,
        judge_output_id: UUID,
        candidate_group_id: UUID,
        bundle_id: UUID,
    ) -> list[OutboxEventRow]:
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       dedupe_key, payload_json, status, fail_count, created_at
                FROM event_outbox
                WHERE event_type = 'analysis.policy.apply.v1'
                  AND aggregate_type = 'judge_run'
                  AND aggregate_id = CAST(:judge_run_id AS uuid)
                  AND payload_json->>'judge_run_id' = :judge_run_id
                  AND payload_json->>'judge_output_id' = :judge_output_id
                  AND payload_json->>'candidate_group_id' = :candidate_group_id
                  AND payload_json->>'bundle_id' = :bundle_id
                ORDER BY created_at ASC, event_id ASC
                LIMIT 2
                """
            ),
            {
                "judge_run_id": str(judge_run_id),
                "judge_output_id": str(judge_output_id),
                "candidate_group_id": str(candidate_group_id),
                "bundle_id": str(bundle_id),
            },
        )
        return [_outbox_row_from_mapping(row) for row in result.mappings().all()]

    async def insert_state_transition(
        self,
        *,
        object_type: str,
        object_id: UUID,
        from_state: str | None,
        to_state: str,
        reason_code: str,
    ) -> None:
        await self._session.execute(
            _sql(
                """
                INSERT INTO state_transitions (
                    state_transition_id,
                    object_type,
                    object_id,
                    from_state,
                    to_state,
                    reason_code,
                    created_at
                ) VALUES (
                    gen_random_uuid(),
                    :object_type,
                    CAST(:object_id AS uuid),
                    :from_state,
                    :to_state,
                    :reason_code,
                    now()
                )
                """
            ),
            {
                "object_type": object_type,
                "object_id": str(object_id),
                "from_state": from_state,
                "to_state": to_state,
                "reason_code": reason_code,
            },
        )

    async def insert_or_load_policy_apply_outbox(
        self,
        *,
        judge_run_id: UUID,
        judge_output_id: UUID,
        candidate_group_id: UUID,
        bundle_id: UUID,
    ) -> tuple[OutboxEventRow, bool]:
        payload = _policy_apply_payload(
            judge_run_id=judge_run_id,
            judge_output_id=judge_output_id,
            candidate_group_id=candidate_group_id,
            bundle_id=bundle_id,
        )
        dedupe_key = _policy_apply_dedupe_key(
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
                        'analysis.policy.apply.v1',
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
        if row is None:  # pragma: no cover - defensive guard around insert/select
            raise BoundedAnalysisValidatorPolicyRequestError("policy_apply_outbox_missing")
        return _outbox_row_from_mapping(row), bool(row["inserted"])

    async def mark_policy_apply_outbox_published(
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
                  AND event_type = 'analysis.policy.apply.v1'
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


def load_bounded_analysis_validator_policy_request_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedAnalysisValidatorPolicyRequestRuntimeConfig:
    source = os.environ if env is None else env
    database_url = _env_value(source, "DATABASE_URL")
    redis_url = _env_value(source, "REDIS_URL")
    if not database_url:
        raise BoundedAnalysisValidatorPolicyRequestError("database_url_missing")
    if not redis_url:
        raise BoundedAnalysisValidatorPolicyRequestError("redis_url_missing")
    input_queue_name = _env_value(source, "ANALYSIS_VALIDATOR_QUEUE_NAME", INPUT_QUEUE_NAME)
    if input_queue_name != INPUT_QUEUE_NAME:
        raise BoundedAnalysisValidatorPolicyRequestError("queue_not_allowed")
    xadd_maxlen_raw = _env_value(source, "OUTBOX_RELAY_XADD_MAXLEN", str(DEFAULT_XADD_MAXLEN))
    try:
        xadd_maxlen = int(xadd_maxlen_raw) if xadd_maxlen_raw else None
    except ValueError as exc:
        raise BoundedAnalysisValidatorPolicyRequestError("runtime_config_error") from exc
    if xadd_maxlen is not None and xadd_maxlen <= 0:
        raise BoundedAnalysisValidatorPolicyRequestError("runtime_config_error")
    return BoundedAnalysisValidatorPolicyRequestRuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        input_queue_name=input_queue_name,
        xadd_maxlen=xadd_maxlen,
    )


async def build_default_bounded_analysis_validator_redis_reader(
    runtime_config: BoundedAnalysisValidatorPolicyRequestRuntimeConfig,
    state: BoundedAnalysisValidatorPolicyRequestState,
    logger: logging.Logger,
) -> BoundedAnalysisValidatorRedisReaderHandle:
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

    return BoundedAnalysisValidatorRedisReaderHandle(reader=reader, close=close)


async def build_default_bounded_analysis_validator_repository(
    runtime_config: BoundedAnalysisValidatorPolicyRequestRuntimeConfig,
    state: BoundedAnalysisValidatorPolicyRequestState,
    logger: logging.Logger,
) -> BoundedAnalysisValidatorRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedAnalysisValidatorPolicyRequestRepository(session)

    async def close() -> None:
        await session.close()
        await engine.dispose()

    return BoundedAnalysisValidatorRepositoryHandle(repository=repository, close=close)


async def build_default_bounded_analysis_validator_redis_publisher(
    runtime_config: BoundedAnalysisValidatorPolicyRequestRuntimeConfig,
    state: BoundedAnalysisValidatorPolicyRequestState,
    logger: logging.Logger,
) -> BoundedAnalysisValidatorRedisPublisherHandle:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    state.redis_publisher_created = True
    publisher = RedisStreamsPublisher(redis_client, maxlen=runtime_config.xadd_maxlen)

    async def close() -> None:
        close_client = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close_client is None:
            return
        result = close_client()
        if hasattr(result, "__await__"):
            await result

    return BoundedAnalysisValidatorRedisPublisherHandle(publisher=publisher, close=close)


async def run_bounded_analysis_validator_policy_request(
    config: BoundedAnalysisValidatorPolicyRequestConfig,
    *,
    runtime_config_loader: Callable[[], BoundedAnalysisValidatorPolicyRequestRuntimeConfig] = (
        load_bounded_analysis_validator_policy_request_runtime_config
    ),
    redis_reader_builder: BoundedAnalysisValidatorRedisReaderBuilder | None = None,
    repository_builder: BoundedAnalysisValidatorRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedAnalysisValidatorRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedAnalysisValidatorPolicyRequestResult:
    state = BoundedAnalysisValidatorPolicyRequestState()
    gate_error = _authority_gate_error(config)
    if gate_error is not None:
        return _result("blocked", gate_error, config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedAnalysisValidatorPolicyRequestError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception as exc:
        return _result(
            "blocked",
            "runtime_config_error",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
        )

    redis_handle: BoundedAnalysisValidatorRedisReaderHandle | None = None
    repository_handle: BoundedAnalysisValidatorRepositoryHandle | None = None
    publisher_handle: BoundedAnalysisValidatorRedisPublisherHandle | None = None
    result: BoundedAnalysisValidatorPolicyRequestResult | None = None
    try:
        redis_handle = await (
            redis_reader_builder or build_default_bounded_analysis_validator_redis_reader
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
            repository_builder or build_default_bounded_analysis_validator_repository
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
        payload_judge_output_id = _payload_uuid(event.payload_json, "judge_output_id")
        assert payload_judge_run_id is not None
        assert payload_judge_output_id is not None
        if not str(payload_judge_output_id).endswith(config.judge_output_suffix or ""):
            result = _result(
                "blocked",
                "judge_output_selector_mismatch",
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_output_id=payload_judge_output_id,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        judge_run = await repository.load_judge_run(payload_judge_run_id)
        judge_run_error = _validate_judge_run(judge_run, event=event)
        if judge_run_error is not None:
            result = _result(
                "blocked",
                judge_run_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                judge_output_id=payload_judge_output_id,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        assert judge_run is not None
        judge_output = await repository.load_judge_output(payload_judge_output_id)
        judge_output_error = _validate_judge_output_identity(
            judge_output,
            judge_run=judge_run,
        )
        if judge_output_error is not None:
            result = await _record_validation_stop(
                repository,
                status="validation_failed",
                error_code=judge_output_error,
                validation_status="failed",
                transition_to_state="analysis_failed_identity_mismatch",
                reason_code=judge_output_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                judge_output=judge_output,
                judge_output_id=payload_judge_output_id,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        assert judge_output is not None
        bundle = await repository.load_bundle(judge_run.bundle_id)
        identity_error = _validate_bundle_identity(
            bundle,
            judge_run=judge_run,
            judge_output=judge_output,
        )
        if identity_error is not None:
            result = await _record_validation_stop(
                repository,
                status="validation_failed",
                error_code=identity_error,
                validation_status="failed",
                transition_to_state="analysis_failed_identity_mismatch",
                reason_code=identity_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        assert bundle is not None
        state.analysis_validator_called = True
        if _is_refusal(event=event, judge_output=judge_output):
            result = await _record_validation_stop(
                repository,
                status="refused_stopped",
                error_code=None,
                validation_status="refused",
                transition_to_state="analysis_refused",
                reason_code="analysis_refused",
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        validation_error = _validate_judge_output_payload(
            judge_output.payload_json,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
        )
        if validation_error is not None:
            result = await _record_validation_stop(
                repository,
                status="validation_failed",
                error_code=validation_error,
                validation_status="failed",
                transition_to_state="analysis_failed_schema",
                reason_code=validation_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        policy_rows = await repository.load_policy_apply_outboxes(
            judge_run_id=judge_run.judge_run_id,
            judge_output_id=judge_output.judge_output_id,
            candidate_group_id=judge_output.candidate_group_id,
            bundle_id=bundle.bundle_id,
        )
        if len(policy_rows) > 1:
            result = _result(
                "blocked",
                "policy_apply_outbox_count_exceeded",
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                validation_status="passed",
                validation_error_count=0,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        state_transition_written = False
        policy_apply_written = False
        if policy_rows:
            policy_apply_outbox = policy_rows[0]
        else:
            state.database_write_attempted = True
            await repository.insert_state_transition(
                object_type=ROOT_OBJECT_TYPE,
                object_id=judge_run.judge_run_id,
                from_state=judge_run.status,
                to_state="analysis_validated",
                reason_code="validator_passed",
            )
            state_transition_written = True
            policy_apply_outbox, policy_apply_written = await repository.insert_or_load_policy_apply_outbox(
                judge_run_id=judge_run.judge_run_id,
                judge_output_id=judge_output.judge_output_id,
                candidate_group_id=judge_output.candidate_group_id,
                bundle_id=bundle.bundle_id,
            )
            policy_apply_error = _validate_policy_apply_outbox(
                policy_apply_outbox,
                judge_run_id=judge_run.judge_run_id,
                judge_output_id=judge_output.judge_output_id,
                candidate_group_id=judge_output.candidate_group_id,
                bundle_id=bundle.bundle_id,
            )
            if policy_apply_error is not None:
                result = _result(
                    "blocked",
                    policy_apply_error,
                    config=config,
                    state=state,
                    redis_message=redis_message,
                    event=event,
                    judge_run=judge_run,
                    judge_output=judge_output,
                    bundle=bundle,
                    policy_apply_outbox=policy_apply_outbox,
                    policy_apply_written=policy_apply_written,
                    state_transition_written=state_transition_written,
                    validation_status="passed",
                    validation_error_count=0,
                    redis_message_count=1,
                )
                raise _BoundedResultReady
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
                    judge_run=judge_run,
                    judge_output=judge_output,
                    bundle=bundle,
                    policy_apply_outbox=policy_apply_outbox,
                    policy_apply_written=policy_apply_written,
                    state_transition_written=state_transition_written,
                    validation_status="passed",
                    validation_error_count=0,
                    redis_message_count=1,
                )
                raise _BoundedResultReady

        policy_apply_error = _validate_policy_apply_outbox(
            policy_apply_outbox,
            judge_run_id=judge_run.judge_run_id,
            judge_output_id=judge_output.judge_output_id,
            candidate_group_id=judge_output.candidate_group_id,
            bundle_id=bundle.bundle_id,
        )
        if policy_apply_error is not None:
            result = _result(
                "blocked",
                policy_apply_error,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                policy_apply_outbox=policy_apply_outbox,
                policy_apply_written=policy_apply_written,
                state_transition_written=state_transition_written,
                validation_status="passed",
                validation_error_count=0,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        if policy_apply_outbox.status == "published":
            result = _result(
                "noop",
                None,
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                policy_apply_outbox=policy_apply_outbox,
                policy_apply_written=policy_apply_written,
                policy_apply_published=True,
                state_transition_written=state_transition_written,
                validation_status="passed",
                validation_error_count=0,
                redis_message_count=1,
                queue_name=OUTPUT_QUEUE_NAME,
                stage_name=OUTPUT_STAGE_NAME,
            )
            raise _BoundedResultReady
        if policy_apply_outbox.status != "pending":
            result = _result(
                "blocked",
                "policy_apply_outbox_not_pending",
                config=config,
                state=state,
                redis_message=redis_message,
                event=event,
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                policy_apply_outbox=policy_apply_outbox,
                policy_apply_written=policy_apply_written,
                state_transition_written=state_transition_written,
                validation_status="passed",
                validation_error_count=0,
                redis_message_count=1,
            )
            raise _BoundedResultReady

        route = _resolve_policy_apply_route(policy_apply_outbox, route_resolver=route_resolver)
        publisher_handle = await (
            redis_publisher_builder or build_default_bounded_analysis_validator_redis_publisher
        )(runtime_config, state, effective_logger)
        message = _build_policy_apply_stream_message(policy_apply_outbox, route)
        state.redis_publish_attempted = True
        redis_output_message_id = await publisher_handle.publisher.publish(route, message)
        state.database_write_attempted = True
        await repository.mark_policy_apply_outbox_published(
            event_id=policy_apply_outbox.event_id,
            judge_run_id=judge_run.judge_run_id,
            published_at=(clock or _utc_now)(),
        )
        await repository.insert_publish_job_attempt(
            stage_name=route.stage_name,
            queue_name=route.queue_name,
            root_object_type=policy_apply_outbox.aggregate_type,
            root_object_id=policy_apply_outbox.aggregate_id,
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
                judge_run=judge_run,
                judge_output=judge_output,
                bundle=bundle,
                policy_apply_outbox=policy_apply_outbox,
                policy_apply_written=policy_apply_written,
                state_transition_written=state_transition_written,
                redis_output_message_id=redis_output_message_id,
                validation_status="passed",
                validation_error_count=0,
                redis_message_count=1,
                queue_name=route.queue_name,
                stage_name=route.stage_name,
            )
            raise _BoundedResultReady

        result = _result(
            "published",
            None,
            config=config,
            state=state,
            redis_message=redis_message,
            event=event,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
            policy_apply_outbox=replace(policy_apply_outbox, status="published"),
            policy_apply_written=policy_apply_written,
            policy_apply_published=True,
            state_transition_written=state_transition_written,
            redis_output_message_id=redis_output_message_id,
            validation_status="passed",
            validation_error_count=0,
            redis_message_count=1,
            queue_name=route.queue_name,
            stage_name=route.stage_name,
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
        error_code = "redis_xadd_failed" if state.redis_publish_attempted else "bounded_policy_request_failed"
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


def run_bounded_analysis_validator_policy_request_sync(
    config: BoundedAnalysisValidatorPolicyRequestConfig,
    *,
    runtime_config_loader: Callable[[], BoundedAnalysisValidatorPolicyRequestRuntimeConfig] = (
        load_bounded_analysis_validator_policy_request_runtime_config
    ),
    redis_reader_builder: BoundedAnalysisValidatorRedisReaderBuilder | None = None,
    repository_builder: BoundedAnalysisValidatorRepositoryBuilder | None = None,
    redis_publisher_builder: BoundedAnalysisValidatorRedisPublisherBuilder | None = None,
    route_resolver: OutboxRouteResolver | None = None,
    clock: Callable[[], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> BoundedAnalysisValidatorPolicyRequestResult:
    return asyncio.run(
        run_bounded_analysis_validator_policy_request(
            config,
            runtime_config_loader=runtime_config_loader,
            redis_reader_builder=redis_reader_builder,
            repository_builder=repository_builder,
            redis_publisher_builder=redis_publisher_builder,
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
        config=BoundedAnalysisValidatorPolicyRequestConfig(),
        state=BoundedAnalysisValidatorPolicyRequestState(),
    ).to_sanitized_dict()


async def _record_validation_stop(
    repository: BoundedAnalysisValidatorPolicyRequestRepository,
    *,
    status: str,
    error_code: str | None,
    validation_status: str,
    transition_to_state: str,
    reason_code: str,
    config: BoundedAnalysisValidatorPolicyRequestConfig,
    state: BoundedAnalysisValidatorPolicyRequestState,
    redis_message: RedisStreamMessage,
    event: OutboxEventRow,
    judge_run: JudgeRunValidationRecord,
    judge_output: JudgeOutputValidationRecord | None = None,
    judge_output_id: UUID | None = None,
    bundle: BundleValidationRecord | None = None,
    redis_message_count: int = 1,
) -> BoundedAnalysisValidatorPolicyRequestResult:
    state.database_write_attempted = True
    await repository.insert_state_transition(
        object_type=ROOT_OBJECT_TYPE,
        object_id=judge_run.judge_run_id,
        from_state=judge_run.status,
        to_state=transition_to_state,
        reason_code=reason_code,
    )
    try:
        await repository.commit()
    except Exception as exc:
        return _result(
            "failed",
            "database_commit_failed_before_redis_publish",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            redis_message=redis_message,
            event=event,
            judge_run=judge_run,
            judge_output=judge_output,
            judge_output_id=judge_output_id,
            bundle=bundle,
            validation_status=validation_status,
            validation_error_count=0 if error_code is None else 1,
            state_transition_written=True,
            redis_message_count=redis_message_count,
        )
    return _result(
        status,
        error_code,
        config=config,
        state=state,
        redis_message=redis_message,
        event=event,
        judge_run=judge_run,
        judge_output=judge_output,
        judge_output_id=judge_output_id,
        bundle=bundle,
        validation_status=validation_status,
        validation_error_count=0 if error_code is None else 1,
        state_transition_written=True,
        redis_message_count=redis_message_count,
    )


def _authority_gate_error(config: BoundedAnalysisValidatorPolicyRequestConfig) -> str | None:
    if not config.operator_approved:
        return "operator_approval_missing"
    if not _valid_scan_limit(config.scan_limit):
        return "invalid_scan_limit"
    if not all(
        [
            config.redis_message_suffix,
            config.trigger_event_suffix,
            config.judge_output_suffix,
            config.judge_run_suffix,
        ]
    ):
        return "target_missing"
    if not REDIS_ID_SUFFIX_RE.fullmatch(config.redis_message_suffix or ""):
        return "invalid_redis_message_suffix"
    if not UUID_SUFFIX_RE.fullmatch(config.trigger_event_suffix or ""):
        return "invalid_trigger_event_suffix"
    if not UUID_SUFFIX_RE.fullmatch(config.judge_output_suffix or ""):
        return "invalid_judge_output_suffix"
    if not UUID_SUFFIX_RE.fullmatch(config.judge_run_suffix or ""):
        return "invalid_judge_run_suffix"
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
    if not config.allow_analysis_validator:
        return "analysis_validator_not_allowed"
    return None


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedAnalysisValidatorPolicyRequestConfig,
    state: BoundedAnalysisValidatorPolicyRequestState,
    error_class: str | None = None,
    redis_message: RedisStreamMessage | None = None,
    event: OutboxEventRow | None = None,
    judge_run: JudgeRunValidationRecord | None = None,
    judge_output: JudgeOutputValidationRecord | None = None,
    judge_output_id: UUID | None = None,
    bundle: BundleValidationRecord | None = None,
    policy_apply_outbox: OutboxEventRow | None = None,
    policy_apply_written: bool = False,
    policy_apply_published: bool = False,
    state_transition_written: bool = False,
    redis_output_message_id: str | None = None,
    validation_status: str | None = None,
    validation_error_count: int = 0,
    redis_message_count: int = 0,
    queue_name: str | None = None,
    stage_name: str | None = None,
) -> BoundedAnalysisValidatorPolicyRequestResult:
    event_payload = event.payload_json if event is not None and isinstance(event.payload_json, Mapping) else {}
    resolved_judge_run_id = judge_run.judge_run_id if judge_run is not None else _payload_uuid(event_payload, "judge_run_id")
    resolved_judge_output_id = (
        judge_output.judge_output_id
        if judge_output is not None
        else judge_output_id or _payload_uuid(event_payload, "judge_output_id")
    )
    return BoundedAnalysisValidatorPolicyRequestResult(
        status=status,
        ok=status in {"published", "noop", "refused_stopped"} and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        target_redis_message_id_suffix=_redis_message_id_suffix(
            redis_message.message_id if redis_message is not None else None
        )
        or config.redis_message_suffix,
        target_trigger_event_id_suffix=_optional_id_suffix(
            event.event_id if event is not None else _safe_uuid(redis_message.fields.get("trigger_event_id")) if redis_message else None
        )
        or config.trigger_event_suffix,
        target_judge_run_id_suffix=_optional_id_suffix(resolved_judge_run_id)
        or config.judge_run_suffix,
        target_judge_output_id_suffix=_optional_id_suffix(resolved_judge_output_id)
        or config.judge_output_suffix,
        target_bundle_id_suffix=_optional_id_suffix(bundle.bundle_id if bundle is not None else judge_run.bundle_id if judge_run else None),
        target_candidate_group_suffix=_optional_id_suffix(
            bundle.candidate_group_id
            if bundle is not None
            else judge_output.candidate_group_id if judge_output is not None else None
        ),
        policy_apply_outbox_written=policy_apply_written,
        policy_apply_event_suffix=_optional_id_suffix(policy_apply_outbox.event_id if policy_apply_outbox else None),
        policy_apply_published=policy_apply_published,
        q_analysis_policy_message_id_suffix=_redis_message_id_suffix(redis_output_message_id),
        validation_status=validation_status,
        validation_error_count=validation_error_count,
        state_transition_written=state_transition_written,
        queue_name=queue_name,
        stage_name=stage_name,
        redis_message_count=redis_message_count,
        event_outbox_found=event is not None,
        judge_run_found=judge_run is not None,
        judge_output_found=judge_output is not None,
        bundle_found=bundle is not None,
    )


def _close_failed_result(
    *,
    existing: BoundedAnalysisValidatorPolicyRequestResult | None,
    error_code: str,
    error_class: str,
    config: BoundedAnalysisValidatorPolicyRequestConfig,
    state: BoundedAnalysisValidatorPolicyRequestState,
) -> BoundedAnalysisValidatorPolicyRequestResult:
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
    event: OutboxEventRow | None,
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
    payload_judge_output_id = _payload_uuid(event.payload_json, "judge_output_id")
    if payload_judge_run_id is None or payload_judge_output_id is None:
        return "event_payload_malformed"
    if payload_judge_run_id != event.aggregate_id:
        return "event_payload_judge_run_id_mismatch"
    if not isinstance(event.payload_json.get("refusal_detected"), bool):
        return "event_payload_refusal_detected_invalid"
    return None


def _validate_judge_run(
    judge_run: JudgeRunValidationRecord | None,
    *,
    event: OutboxEventRow,
) -> str | None:
    if judge_run is None:
        return "judge_run_missing"
    if judge_run.judge_run_id != event.aggregate_id:
        return "judge_run_id_mismatch"
    if not judge_run.schema_version:
        return "judge_run_schema_version_missing"
    return None


def _validate_judge_output_identity(
    judge_output: JudgeOutputValidationRecord | None,
    *,
    judge_run: JudgeRunValidationRecord,
) -> str | None:
    if judge_output is None:
        return "judge_output_missing"
    if judge_output.judge_run_id != judge_run.judge_run_id:
        return "judge_output_judge_run_mismatch"
    return None


def _validate_bundle_identity(
    bundle: BundleValidationRecord | None,
    *,
    judge_run: JudgeRunValidationRecord,
    judge_output: JudgeOutputValidationRecord,
) -> str | None:
    if bundle is None:
        return "bundle_missing"
    if judge_run.bundle_id != bundle.bundle_id:
        return "judge_run_bundle_mismatch"
    if judge_output.candidate_group_id != bundle.candidate_group_id:
        return "judge_output_bundle_candidate_mismatch"
    if not bundle.ready_for_analysis:
        return "bundle_not_ready"
    return None


def _is_refusal(*, event: OutboxEventRow, judge_output: JudgeOutputValidationRecord) -> bool:
    return bool(event.payload_json.get("refusal_detected")) or judge_output.payload_json.get("output_kind") == "refusal"


def _validate_judge_output_payload(
    payload: Mapping[str, Any] | None,
    *,
    judge_run: JudgeRunValidationRecord,
    judge_output: JudgeOutputValidationRecord,
    bundle: BundleValidationRecord,
) -> str | None:
    if not isinstance(payload, Mapping):
        return "validator_schema_invalid"
    if REQUIRED_OUTPUT_FIELDS - set(payload):
        return "validator_schema_invalid"
    for field_name in STRING_FIELDS:
        if not isinstance(payload.get(field_name), str):
            return "validator_schema_invalid"
    if payload.get("judge_schema_version") != judge_run.schema_version:
        return "validator_schema_version_mismatch"
    if judge_output.judge_schema_version != judge_run.schema_version:
        return "judge_output_schema_version_mismatch"
    payload_candidate_group_id = _safe_uuid(payload.get("candidate_group_id"))
    if payload_candidate_group_id != bundle.candidate_group_id:
        return "validator_payload_candidate_mismatch"
    if payload.get("model_proposed_verdict") not in MODEL_VERDICTS:
        return "validator_schema_invalid"
    if payload.get("model_confidence_band") not in CONFIDENCE_BANDS:
        return "validator_schema_invalid"
    for field_name in ARRAY_FIELDS:
        value = payload.get(field_name)
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            return "validator_schema_invalid"
    scores = payload.get("scores")
    if not isinstance(scores, Mapping) or REQUIRED_SCORE_FIELDS - set(scores):
        return "validator_schema_invalid"
    for field_name in REQUIRED_INTEGER_SCORE_FIELDS:
        value = scores.get(field_name)
        if value is None:
            return "validator_score_range_invalid"
        if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 100:
            return "validator_score_range_invalid"
    for field_name in OPTIONAL_NULLABLE_SCORE_FIELDS:
        value = scores.get(field_name)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 100:
            return "validator_score_range_invalid"
    if not _non_empty_string(payload.get("headline")):
        return "validator_missing_headline"
    if not _non_empty_string(payload.get("summary_one_line_ko")):
        return "validator_missing_summary"
    if not _non_empty_string(payload.get("skeptical_take_ko")):
        return "validator_missing_skeptical_take"
    if not _non_empty_string_list(payload.get("evidence_limitations_ko")):
        return "validator_missing_evidence_limitations"
    if not _non_empty_string_list(payload.get("reason_codes")):
        return "validator_missing_reason_codes"
    return None


def _validate_policy_apply_outbox(
    row: OutboxEventRow,
    *,
    judge_run_id: UUID,
    judge_output_id: UUID,
    candidate_group_id: UUID,
    bundle_id: UUID,
) -> str | None:
    if row.event_type != OUTPUT_EVENT_TYPE:
        return "policy_apply_outbox_wrong_event_type"
    if row.aggregate_type != ROOT_OBJECT_TYPE or row.aggregate_id != judge_run_id:
        return "policy_apply_outbox_aggregate_mismatch"
    if row.dedupe_key != _policy_apply_dedupe_key(
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
    ):
        return "policy_apply_outbox_dedupe_mismatch"
    if row.payload_json != _policy_apply_payload(
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
    ):
        return "policy_apply_outbox_payload_mismatch"
    return None


def _resolve_policy_apply_route(
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
        raise UnsupportedOutboxEventTypeError("analysis_policy_route_not_allowed")
    return resolved_route


def _build_policy_apply_stream_message(row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
    return RedisQueuedMessage(
        job_id=str(row.event_id),
        stage_name=route.stage_name,
        root_object_type=row.aggregate_type,
        root_object_id=str(row.aggregate_id),
        idempotency_key=row.dedupe_key,
        pipeline_run_id="",
        not_before="",
        trigger_event_id=str(row.event_id),
    )


def _message_matches_selectors(
    message: RedisStreamMessage,
    config: BoundedAnalysisValidatorPolicyRequestConfig,
) -> bool:
    trigger_event_id = message.fields.get("trigger_event_id", "")
    root_object_id = message.fields.get("root_object_id", "")
    return bool(
        message.message_id.endswith(config.redis_message_suffix or "")
        and trigger_event_id.endswith(config.trigger_event_suffix or "")
        and root_object_id.endswith(config.judge_run_suffix or "")
    )


def _normalize_redis_message(raw_message: Any) -> RedisStreamMessage:
    message_id: Any
    fields: Any
    if isinstance(raw_message, (list, tuple)) and len(raw_message) == 2:
        message_id, fields = raw_message
    else:
        message_id, fields = "", {}
    return RedisStreamMessage(
        message_id=_decode_value(message_id),
        fields={_decode_value(key): _decode_value(value) for key, value in dict(fields or {}).items()},
    )


def _policy_apply_payload(
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


def _policy_apply_dedupe_key(*, judge_run_id: UUID, judge_output_id: UUID) -> str:
    return f"analysis-policy-apply:{judge_run_id}:{judge_output_id}"


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


def _payload_uuid(payload: Mapping[str, Any], field_name: str) -> UUID | None:
    return _safe_uuid(payload.get(field_name))


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


def _valid_scan_limit(value: int) -> bool:
    return 1 <= value <= MAX_SCAN_LIMIT


def _non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _non_empty_string_list(value: Any) -> bool:
    return isinstance(value, list) and bool(value) and all(_non_empty_string(item) for item in value)


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _jsonb_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _decode_value(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _env_value(env: Mapping[str, str], name: str, default: str = "") -> str:
    return str(env.get(name, default) or "").strip()


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)


__all__ = [
    "BoundedAnalysisValidatorPolicyRequestConfig",
    "BoundedAnalysisValidatorPolicyRequestError",
    "BoundedAnalysisValidatorPolicyRequestResult",
    "BoundedAnalysisValidatorPolicyRequestRuntimeConfig",
    "BoundedAnalysisValidatorPolicyRequestState",
    "BoundedAnalysisValidatorRedisPublisherBuilder",
    "BoundedAnalysisValidatorRedisPublisherHandle",
    "BoundedAnalysisValidatorRedisReaderBuilder",
    "BoundedAnalysisValidatorRedisReaderHandle",
    "BoundedAnalysisValidatorRepositoryBuilder",
    "BoundedAnalysisValidatorRepositoryHandle",
    "BundleValidationRecord",
    "JudgeOutputValidationRecord",
    "JudgeRunValidationRecord",
    "RedisStreamMessage",
    "argument_error_report",
    "build_default_bounded_analysis_validator_redis_publisher",
    "build_default_bounded_analysis_validator_redis_reader",
    "build_default_bounded_analysis_validator_repository",
    "load_bounded_analysis_validator_policy_request_runtime_config",
    "render_sanitized_json",
    "run_bounded_analysis_validator_policy_request",
    "run_bounded_analysis_validator_policy_request_sync",
]
