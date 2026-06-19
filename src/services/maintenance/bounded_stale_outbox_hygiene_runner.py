from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from json import JSONDecodeError
from typing import Any, Literal, Protocol
from uuid import UUID

from ..outbox_relay.eligibility import (
    EVENT_OUTBOX_ROOT_OBJECT_TYPE,
    JUDGE_OUTPUT_READY_EVENT_TYPE,
    MAINTENANCE_QUEUE_NAME,
    MAINTENANCE_STALE_OUTBOX_HYGIENE_STAGE,
    POLICY_APPLY_EVENT_TYPE,
    canonical_relay_eligible_sql,
    stale_resolution_proof_error_code_for_classification,
)

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - local static validation fallback
    sa = None


SCHEMA_VERSION = "bounded_stale_outbox_hygiene_v1"
RUNNER_NAME = "bounded_stale_outbox_hygiene_runner"
QUEUE_NAME = MAINTENANCE_QUEUE_NAME
STAGE_NAME = MAINTENANCE_STALE_OUTBOX_HYGIENE_STAGE
DEFAULT_SCAN_LIMIT = 100
MAX_SCAN_LIMIT = 500
UUID_SUFFIX_RE = re.compile(r"^[0-9a-f]{4,12}$")
FULL_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

Mode = Literal["inventory", "plan", "execute", "proof"]
Classification = Literal[
    "judge_output_ready_already_handed_off",
    "policy_apply_already_analyzed",
    "delivery_result_hygiene",
    "unsafe_or_unknown",
]

JUDGE_OUTPUT_READY = JUDGE_OUTPUT_READY_EVENT_TYPE
POLICY_APPLY = POLICY_APPLY_EVENT_TYPE
DELIVERY_RESULT = "notification.delivery.result.v1"
SAFE_RESOLUTION_CLASSIFICATIONS = {
    "judge_output_ready_already_handed_off",
    "policy_apply_already_analyzed",
}
ALL_CLASSIFICATIONS = SAFE_RESOLUTION_CLASSIFICATIONS | {"delivery_result_hygiene", "unsafe_or_unknown"}


@dataclass(frozen=True, slots=True)
class BoundedStaleOutboxHygieneConfig:
    mode: Mode = "inventory"
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_database_write: bool = False
    target_event_suffixes: tuple[str, ...] = ()
    classifications: tuple[str, ...] = ()
    scan_limit: int = DEFAULT_SCAN_LIMIT


@dataclass(frozen=True, slots=True)
class BoundedStaleOutboxHygieneRuntimeConfig:
    database_url: str


@dataclass(slots=True)
class BoundedStaleOutboxHygieneState:
    runtime_config_loaded: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    database_committed: bool = False
    database_rolled_back: bool = False


@dataclass(frozen=True, slots=True)
class StaleOutboxRow:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    payload_json: dict[str, Any]
    status: str
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class StaleOutboxCandidate:
    event_id: UUID
    event_type: str
    status: str
    aggregate_type: str
    aggregate_id: UUID
    classification: Classification
    next_action: str
    reason_code: str
    analysis_count: int = 0
    policy_apply_event_count: int = 0
    notification_plan_count: int = 0
    judge_output_exists: bool = False
    delivery_result_has_plan_and_record: bool = False
    delivery_result_current_published: bool = False
    already_resolution_proven: bool = False
    matching_resolution_proof_exists: bool = False
    canonical_relay_eligible: bool = False
    proof_inserted: bool = False
    deliberately_left_unchanged: bool = False
    created_at: datetime | None = field(default=None, repr=False)

    @property
    def event_suffix(self) -> str:
        return uuid_suffix(self.event_id)

    @property
    def aggregate_suffix(self) -> str:
        return uuid_suffix(self.aggregate_id)

    @property
    def safe_resolution_classification(self) -> bool:
        return self.classification in SAFE_RESOLUTION_CLASSIFICATIONS

    @property
    def resolved_by_maintenance_proof(self) -> bool:
        return self.matching_resolution_proof_exists or self.already_resolution_proven or self.proof_inserted

    @property
    def hot_path_candidate(self) -> bool:
        if self.event_type == DELIVERY_RESULT:
            return False
        return self.canonical_relay_eligible

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "event_suffix": self.event_suffix,
            "event_type": self.event_type,
            "status": self.status,
            "aggregate_type": self.aggregate_type,
            "aggregate_suffix": self.aggregate_suffix,
            "classification": self.classification,
            "next_action": self.next_action,
            "reason_code": self.reason_code,
            "analysis_count": self.analysis_count,
            "policy_apply_event_count": self.policy_apply_event_count,
            "notification_plan_count": self.notification_plan_count,
            "judge_output_exists": self.judge_output_exists,
            "delivery_result_has_plan_and_record": self.delivery_result_has_plan_and_record,
            "delivery_result_current_published": self.delivery_result_current_published,
            "already_resolution_proven": self.already_resolution_proven,
            "matching_resolution_proof_exists": self.matching_resolution_proof_exists,
            "canonical_relay_eligible": self.canonical_relay_eligible,
            "proof_inserted": self.proof_inserted,
            "deliberately_left_unchanged": self.deliberately_left_unchanged,
            "hot_path_candidate": self.hot_path_candidate,
        }


@dataclass(frozen=True, slots=True)
class BoundedStaleOutboxHygieneResult:
    status: str
    ok: bool
    error_code: str | None
    config: BoundedStaleOutboxHygieneConfig
    state: BoundedStaleOutboxHygieneState = field(default_factory=BoundedStaleOutboxHygieneState)
    error_class: str | None = None
    candidates: tuple[StaleOutboxCandidate, ...] = ()
    selected_candidates: tuple[StaleOutboxCandidate, ...] = ()
    selected_target_suffixes: tuple[str, ...] = ()
    selected_classifications: tuple[str, ...] = ()
    action_attempted: bool = False
    event_outbox_status_updated_count: int = 0
    stale_resolution_proofs_inserted_count: int = 0
    stale_resolution_proofs_already_present_count: int = 0
    scan_truncated: bool = False
    scanned_candidate_count: int = 0
    remaining_hot_path_candidate_count_is_complete: bool = True
    post_commit_readback_passed: bool | None = None

    def to_sanitized_dict(self) -> dict[str, Any]:
        candidates = self.candidates
        classification_counts = Counter(candidate.classification for candidate in candidates)
        type_status_counts = Counter(f"{candidate.event_type}|{candidate.status}" for candidate in candidates)
        suffixes_by_classification: dict[str, list[str]] = {classification: [] for classification in sorted(ALL_CLASSIFICATIONS)}
        for candidate in candidates:
            suffixes_by_classification.setdefault(candidate.classification, []).append(candidate.event_suffix)

        planned_resolution_suffixes = [
            candidate.event_suffix
            for candidate in candidates
            if candidate.safe_resolution_classification and not candidate.resolved_by_maintenance_proof
        ]
        resolved_or_proven_suffixes = [
            candidate.event_suffix for candidate in candidates if candidate.resolved_by_maintenance_proof
        ]
        deliberately_left_unchanged_suffixes = [
            candidate.event_suffix
            for candidate in candidates
            if candidate.deliberately_left_unchanged or candidate.classification == "delivery_result_hygiene"
        ]
        unsafe_suffixes = [
            candidate.event_suffix for candidate in candidates if candidate.classification == "unsafe_or_unknown"
        ]
        remaining_hot_path_candidate_count = sum(1 for candidate in candidates if candidate.hot_path_candidate)

        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": self.config.mode,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "operator_approved": self.config.operator_approved,
            "runtime_config_allowed": self.config.allow_runtime_config,
            "database_read_allowed": self.config.allow_database_read,
            "database_write_allowed": self.config.allow_database_write,
            "scan_limit": self.config.scan_limit,
            "selected_target_suffixes": list(self.selected_target_suffixes),
            "selected_classifications": list(self.selected_classifications),
            "counts_by_event_type_status": dict(sorted(type_status_counts.items())),
            "counts_by_classification": {
                classification: classification_counts.get(classification, 0)
                for classification in sorted(ALL_CLASSIFICATIONS)
            },
            "suffixes_by_classification": {
                classification: sorted(suffixes)
                for classification, suffixes in sorted(suffixes_by_classification.items())
            },
            "candidates": [candidate.to_sanitized_dict() for candidate in candidates],
            "selected_candidates": [candidate.to_sanitized_dict() for candidate in self.selected_candidates],
            "planned_resolution_suffixes": sorted(planned_resolution_suffixes),
            "resolved_or_proven_suffixes": sorted(resolved_or_proven_suffixes),
            "deliberately_left_unchanged_suffixes": sorted(deliberately_left_unchanged_suffixes),
            "unsafe_suffixes": sorted(unsafe_suffixes),
            "action_attempted": self.action_attempted,
            "event_outbox_status_updated_count": self.event_outbox_status_updated_count,
            "event_outbox_left_unchanged_count": len(candidates),
            "stale_resolution_proofs_inserted_count": self.stale_resolution_proofs_inserted_count,
            "stale_resolution_proofs_already_present_count": self.stale_resolution_proofs_already_present_count,
            "remaining_hot_path_candidate_count": remaining_hot_path_candidate_count,
            "scan_truncated": self.scan_truncated,
            "scanned_candidate_count": self.scanned_candidate_count,
            "remaining_hot_path_candidate_count_is_complete": self.remaining_hot_path_candidate_count_is_complete,
            "post_commit_readback_passed": self.post_commit_readback_passed,
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": self.state.database_write_attempted,
            "database_committed": self.state.database_committed,
            "database_rolled_back": self.state.database_rolled_back,
            "redis_publish_attempted": False,
            "redis_consume_called": False,
            "redis_ack_called": False,
            "redis_claim_called": False,
            "redis_delete_called": False,
            "redis_group_create_called": False,
            "validator_called": False,
            "policy_called": False,
            "notifier_called": False,
            "judge_called": False,
            "enricher_called": False,
            "forbidden_authority_flags": {
                "db_write": self.state.database_write_attempted,
                "redis_mutation": False,
                "redis_ack_claim_delete_group_create": False,
                "telegram_send_edit": False,
                "openai_github_x_web": False,
                "docker_systemd_alembic": False,
                "runtime_env_values_printed": False,
            },
            "redactions_applied": {
                "full_uuid_omitted": True,
                "dedupe_key_omitted": True,
                "payload_json_omitted": True,
                "raw_source_text_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class BoundedStaleOutboxHygieneError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class BoundedStaleOutboxHygieneRepository(Protocol):
    async def fetch_pending_events(self, *, limit: int) -> list[StaleOutboxRow]: ...
    async def fetch_events_by_suffix(self, *, event_suffix: str, limit: int) -> list[StaleOutboxRow]: ...
    async def fetch_events_by_suffix_for_update(self, *, event_suffix: str, limit: int) -> list[StaleOutboxRow]: ...
    async def judge_output_exists(self, judge_output_id: UUID) -> bool: ...
    async def count_analyses_for_judge_output(self, judge_output_id: UUID) -> int: ...
    async def count_policy_apply_events_for_judge_output(self, judge_output_id: UUID) -> int: ...
    async def count_notification_plans_for_candidate_group(self, candidate_group_id: UUID) -> int: ...
    async def delivery_result_has_plan_and_record(
        self,
        *,
        notification_plan_id: UUID,
        notification_delivery_record_id: UUID,
    ) -> bool: ...
    async def delivery_result_current_event_published(
        self,
        *,
        notification_plan_id: UUID,
        notification_delivery_record_id: UUID,
    ) -> bool: ...
    async def has_stale_resolution_proof(self, *, event_id: UUID, classification: str) -> bool: ...
    async def is_canonically_relay_eligible(self, *, event_id: UUID) -> bool: ...
    async def insert_stale_resolution_proof(self, *, event_id: UUID, classification: str) -> bool: ...


@dataclass(frozen=True, slots=True)
class BoundedStaleOutboxHygieneRepositoryHandle:
    repository: BoundedStaleOutboxHygieneRepository
    close: Callable[[bool], Awaitable[None]]


class BoundedStaleOutboxHygieneRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedStaleOutboxHygieneRuntimeConfig,
        state: BoundedStaleOutboxHygieneState,
        logger: logging.Logger,
    ) -> BoundedStaleOutboxHygieneRepositoryHandle: ...


class SqlAlchemyBoundedStaleOutboxHygieneRepository:
    def __init__(self, session: Any, state: BoundedStaleOutboxHygieneState) -> None:
        self._session = session
        self._state = state

    async def fetch_pending_events(self, *, limit: int) -> list[StaleOutboxRow]:
        self._state.database_read_attempted = True
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       payload_json, status, created_at
                FROM event_outbox
                WHERE status = 'pending'::outbox_status_enum
                ORDER BY created_at ASC, event_id ASC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        )
        return [_row_from_mapping(row) for row in result.mappings().all()]

    async def fetch_events_by_suffix(self, *, event_suffix: str, limit: int) -> list[StaleOutboxRow]:
        self._state.database_read_attempted = True
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       payload_json, status, created_at
                FROM event_outbox
                WHERE right(replace(event_id::text, '-', ''), length(:event_suffix)) = :event_suffix
                ORDER BY created_at ASC, event_id ASC
                LIMIT :limit
                """
            ),
            {"event_suffix": event_suffix, "limit": limit},
        )
        return [_row_from_mapping(row) for row in result.mappings().all()]

    async def fetch_events_by_suffix_for_update(self, *, event_suffix: str, limit: int) -> list[StaleOutboxRow]:
        self._state.database_read_attempted = True
        self._state.database_write_attempted = True
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       payload_json, status, created_at
                FROM event_outbox
                WHERE right(replace(event_id::text, '-', ''), length(:event_suffix)) = :event_suffix
                ORDER BY created_at ASC, event_id ASC
                LIMIT :limit
                FOR UPDATE
                """
            ),
            {"event_suffix": event_suffix, "limit": limit},
        )
        return [_row_from_mapping(row) for row in result.mappings().all()]

    async def judge_output_exists(self, judge_output_id: UUID) -> bool:
        self._state.database_read_attempted = True
        result = await self._session.execute(
            _sql(
                """
                SELECT 1
                FROM judge_outputs
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                LIMIT 1
                """
            ),
            {"judge_output_id": str(judge_output_id)},
        )
        return result.scalar_one_or_none() is not None

    async def count_analyses_for_judge_output(self, judge_output_id: UUID) -> int:
        self._state.database_read_attempted = True
        result = await self._session.execute(
            _sql(
                """
                SELECT count(*)
                FROM analyses
                WHERE judge_output_id = CAST(:judge_output_id AS uuid)
                """
            ),
            {"judge_output_id": str(judge_output_id)},
        )
        return int(result.scalar_one())

    async def count_policy_apply_events_for_judge_output(self, judge_output_id: UUID) -> int:
        self._state.database_read_attempted = True
        result = await self._session.execute(
            _sql(
                """
                SELECT count(*)
                FROM event_outbox
                WHERE event_type = 'analysis.policy.apply.v1'
                  AND payload_json ->> 'judge_output_id' = :judge_output_id
                """
            ),
            {"judge_output_id": str(judge_output_id)},
        )
        return int(result.scalar_one())

    async def count_notification_plans_for_candidate_group(self, candidate_group_id: UUID) -> int:
        self._state.database_read_attempted = True
        result = await self._session.execute(
            _sql(
                """
                SELECT count(*)
                FROM notification_plans
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": str(candidate_group_id)},
        )
        return int(result.scalar_one())

    async def delivery_result_has_plan_and_record(
        self,
        *,
        notification_plan_id: UUID,
        notification_delivery_record_id: UUID,
    ) -> bool:
        self._state.database_read_attempted = True
        result = await self._session.execute(
            _sql(
                """
                SELECT 1
                FROM notification_plans np
                JOIN notification_delivery_records ndr
                  ON ndr.notification_plan_id = np.notification_plan_id
                WHERE np.notification_plan_id = CAST(:notification_plan_id AS uuid)
                  AND ndr.notification_delivery_record_id = CAST(:notification_delivery_record_id AS uuid)
                LIMIT 1
                """
            ),
            {
                "notification_plan_id": str(notification_plan_id),
                "notification_delivery_record_id": str(notification_delivery_record_id),
            },
        )
        return result.scalar_one_or_none() is not None

    async def delivery_result_current_event_published(
        self,
        *,
        notification_plan_id: UUID,
        notification_delivery_record_id: UUID,
    ) -> bool:
        self._state.database_read_attempted = True
        result = await self._session.execute(
            _sql(
                """
                SELECT 1
                FROM event_outbox
                WHERE event_type = 'notification.delivery.result.v1'
                  AND status = 'published'::outbox_status_enum
                  AND payload_json ->> 'notification_plan_id' = :notification_plan_id
                  AND payload_json ->> 'notification_delivery_record_id' = :notification_delivery_record_id
                LIMIT 1
                """
            ),
            {
                "notification_plan_id": str(notification_plan_id),
                "notification_delivery_record_id": str(notification_delivery_record_id),
            },
        )
        return result.scalar_one_or_none() is not None

    async def has_stale_resolution_proof(self, *, event_id: UUID, classification: str) -> bool:
        self._state.database_read_attempted = True
        result = await self._session.execute(
            _sql(
                """
                SELECT 1
                FROM job_attempts
                WHERE stage_name = :stage_name
                  AND queue_name = :queue_name
                  AND root_object_type = :root_object_type
                  AND root_object_id = CAST(:event_id AS uuid)
                  AND attempt_status = 'succeeded'::job_attempt_status_enum
                  AND error_code = :error_code
                LIMIT 1
                """
            ),
            {
                "stage_name": STAGE_NAME,
                "queue_name": QUEUE_NAME,
                "root_object_type": EVENT_OUTBOX_ROOT_OBJECT_TYPE,
                "event_id": str(event_id),
                "error_code": _proof_error_code(classification),
            },
        )
        return result.scalar_one_or_none() is not None

    async def is_canonically_relay_eligible(self, *, event_id: UUID) -> bool:
        self._state.database_read_attempted = True
        result = await self._session.execute(
            _sql(
                f"""
                SELECT 1
                FROM event_outbox eo
                WHERE eo.event_id = CAST(:event_id AS uuid)
                  AND {canonical_relay_eligible_sql("eo")}
                LIMIT 1
                """
            ),
            {"event_id": str(event_id)},
        )
        return result.scalar_one_or_none() is not None

    async def insert_stale_resolution_proof(self, *, event_id: UUID, classification: str) -> bool:
        self._state.database_write_attempted = True
        result = await self._session.execute(
            _sql(
                """
                INSERT INTO job_attempts (
                    stage_name, queue_name, root_object_type, root_object_id,
                    attempt_no, lease_owner, started_at, finished_at,
                    attempt_status, error_code, retry_after_at, created_at
                )
                SELECT
                    :stage_name,
                    :queue_name,
                    :root_object_type,
                    CAST(:event_id AS uuid),
                    1,
                    NULL,
                    now(),
                    now(),
                    'succeeded'::job_attempt_status_enum,
                    :error_code,
                    NULL,
                    now()
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM job_attempts
                    WHERE stage_name = :stage_name
                      AND queue_name = :queue_name
                      AND root_object_type = :root_object_type
                      AND root_object_id = CAST(:event_id AS uuid)
                      AND attempt_status = 'succeeded'::job_attempt_status_enum
                      AND error_code = :error_code
                )
                RETURNING job_attempt_id
                """
            ),
            {
                "stage_name": STAGE_NAME,
                "queue_name": QUEUE_NAME,
                "root_object_type": EVENT_OUTBOX_ROOT_OBJECT_TYPE,
                "event_id": str(event_id),
                "error_code": _proof_error_code(classification),
            },
        )
        return result.scalar_one_or_none() is not None


def load_bounded_stale_outbox_hygiene_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedStaleOutboxHygieneRuntimeConfig:
    source = os.environ if env is None else env
    database_url = str(source.get("DATABASE_URL") or "").strip()
    if not database_url:
        raise BoundedStaleOutboxHygieneError("database_url_missing")
    return BoundedStaleOutboxHygieneRuntimeConfig(database_url=database_url)


async def build_default_bounded_stale_outbox_hygiene_repository(
    runtime_config: BoundedStaleOutboxHygieneRuntimeConfig,
    state: BoundedStaleOutboxHygieneState,
    logger: logging.Logger,
) -> BoundedStaleOutboxHygieneRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    repository = SqlAlchemyBoundedStaleOutboxHygieneRepository(session, state)

    async def close(commit: bool) -> None:
        if commit:
            await session.commit()
            state.database_committed = True
        else:
            await session.rollback()
            state.database_rolled_back = True
        await session.close()
        await engine.dispose()

    return BoundedStaleOutboxHygieneRepositoryHandle(repository=repository, close=close)


async def run_bounded_stale_outbox_hygiene(
    config: BoundedStaleOutboxHygieneConfig,
    *,
    runtime_config_loader: Callable[[], BoundedStaleOutboxHygieneRuntimeConfig] = (
        load_bounded_stale_outbox_hygiene_runtime_config
    ),
    repository_builder: BoundedStaleOutboxHygieneRepositoryBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedStaleOutboxHygieneResult:
    state = BoundedStaleOutboxHygieneState()
    selected_suffixes = tuple(filter(None, (_safe_suffix_projection(value) for value in config.target_event_suffixes)))
    selected_classifications = tuple(
        classification for classification in config.classifications if classification in ALL_CLASSIFICATIONS
    )

    gate_error = _authority_gate_error(config)
    if gate_error is not None:
        return _result(
            "blocked",
            gate_error,
            config=config,
            state=state,
            selected_suffixes=selected_suffixes,
            selected_classifications=selected_classifications,
        )

    log = logger or logging.getLogger(__name__)
    builder = repository_builder or build_default_bounded_stale_outbox_hygiene_repository
    handle: BoundedStaleOutboxHygieneRepositoryHandle | None = None
    commit = False
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
        handle = await builder(runtime_config, state, log)
        repository = handle.repository

        if config.mode == "execute":
            try:
                selected = await _load_selected_candidates(repository, config.target_event_suffixes, for_update=True)
            except BoundedStaleOutboxHygieneError as exc:
                return _result(
                    "blocked",
                    exc.error_code,
                    config=config,
                    state=state,
                    selected_suffixes=selected_suffixes,
                    selected_classifications=selected_classifications,
                )
            error_code = _selection_error(config, selected, require_safe=True)
            if error_code is not None:
                return _result(
                    "blocked",
                    error_code,
                    config=config,
                    state=state,
                    candidates=tuple(selected),
                    selected=tuple(selected),
                    selected_suffixes=selected_suffixes,
                    selected_classifications=selected_classifications,
                    post_commit_readback_passed=False,
                )
            inserted_count = 0
            already_count = 0
            updated_selected: list[StaleOutboxCandidate] = []
            for candidate in selected:
                if candidate.matching_resolution_proof_exists:
                    already_count += 1
                    updated_selected.append(candidate)
                    continue
                state.database_write_attempted = True
                inserted = await repository.insert_stale_resolution_proof(
                    event_id=candidate.event_id,
                    classification=candidate.classification,
                )
                if inserted:
                    inserted_count += 1
                    updated_selected.append(
                        replace(
                            candidate,
                            proof_inserted=True,
                            matching_resolution_proof_exists=True,
                            next_action="stale_resolution_proven",
                        )
                    )
                else:
                    already_count += 1
                    updated_selected.append(
                        replace(
                            candidate,
                            already_resolution_proven=True,
                            matching_resolution_proof_exists=True,
                            next_action="already_stale_resolution_proven",
                        )
                    )
            commit = state.database_write_attempted
            await handle.close(commit)
            handle = None
            commit = False

            readback_handle = await builder(runtime_config, state, log)
            try:
                readback_selected = await _load_selected_candidates(
                    readback_handle.repository,
                    config.target_event_suffixes,
                    for_update=False,
                )
                readback_error = _selection_error(
                    config,
                    readback_selected,
                    require_safe=True,
                    require_matching_proof=True,
                    require_canonical_excluded=True,
                )
            except BoundedStaleOutboxHygieneError as exc:
                readback_selected = updated_selected
                readback_error = exc.error_code
            finally:
                await readback_handle.close(False)

            if readback_error is not None:
                return _result(
                    "blocked",
                    readback_error,
                    config=config,
                    state=state,
                    candidates=tuple(readback_selected),
                    selected=tuple(readback_selected),
                    selected_suffixes=selected_suffixes,
                    selected_classifications=selected_classifications,
                    action_attempted=True,
                    inserted_count=inserted_count,
                    already_count=already_count,
                    post_commit_readback_passed=False,
                )

            return _result(
                "pass",
                None,
                config=config,
                state=state,
                candidates=tuple(readback_selected),
                selected=tuple(readback_selected),
                selected_suffixes=selected_suffixes,
                selected_classifications=selected_classifications,
                action_attempted=True,
                inserted_count=inserted_count,
                already_count=already_count,
                post_commit_readback_passed=True,
            )

        if config.mode in {"plan", "proof"}:
            try:
                selected = await _load_selected_candidates(repository, config.target_event_suffixes, for_update=False)
            except BoundedStaleOutboxHygieneError as exc:
                return _result(
                    "blocked",
                    exc.error_code,
                    config=config,
                    state=state,
                    selected_suffixes=selected_suffixes,
                    selected_classifications=selected_classifications,
                )
            error_code = _selection_error(
                config,
                selected,
                require_safe=config.mode == "proof",
                require_matching_proof=config.mode == "proof",
                require_canonical_excluded=config.mode == "proof",
            )
            return _result(
                "blocked" if error_code else "pass",
                error_code,
                config=config,
                state=state,
                candidates=tuple(selected),
                selected=tuple(selected),
                selected_suffixes=selected_suffixes,
                selected_classifications=selected_classifications,
                post_commit_readback_passed=None,
            )

        candidates, scan_truncated = await _load_inventory_candidates(repository, limit=config.scan_limit)
        filtered = _filter_candidates(candidates, config)
        return _result(
            "pass",
            None,
            config=config,
            state=state,
            candidates=tuple(filtered),
            selected_suffixes=selected_suffixes,
            selected_classifications=selected_classifications,
            scan_truncated=scan_truncated,
            scanned_candidate_count=len(candidates),
            remaining_hot_path_candidate_count_is_complete=not scan_truncated,
        )
    except BoundedStaleOutboxHygieneError as exc:
        return _result(
            "blocked",
            exc.error_code,
            config=config,
            state=state,
            selected_suffixes=selected_suffixes,
            selected_classifications=selected_classifications,
        )
    except Exception as exc:  # pragma: no cover - defensive redaction boundary
        return _result(
            "blocked",
            "unexpected_error",
            config=config,
            state=state,
            error_class=exc.__class__.__name__,
            selected_suffixes=selected_suffixes,
            selected_classifications=selected_classifications,
        )
    finally:
        if handle is not None:
            await handle.close(commit)


def run_bounded_stale_outbox_hygiene_sync(
    config: BoundedStaleOutboxHygieneConfig,
    *,
    runtime_config_loader: Callable[[], BoundedStaleOutboxHygieneRuntimeConfig] = (
        load_bounded_stale_outbox_hygiene_runtime_config
    ),
    repository_builder: BoundedStaleOutboxHygieneRepositoryBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedStaleOutboxHygieneResult:
    return asyncio.run(
        run_bounded_stale_outbox_hygiene(
            config,
            runtime_config_loader=runtime_config_loader,
            repository_builder=repository_builder,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        error_code,
        config=BoundedStaleOutboxHygieneConfig(),
        state=BoundedStaleOutboxHygieneState(),
    ).to_sanitized_dict()


async def _load_inventory_candidates(
    repository: BoundedStaleOutboxHygieneRepository,
    *,
    limit: int,
) -> tuple[list[StaleOutboxCandidate], bool]:
    rows = await repository.fetch_pending_events(limit=limit + 1)
    scan_truncated = len(rows) > limit
    rows = rows[:limit]
    candidates = [await _classify_row(repository, row) for row in rows]
    return (
        sorted(candidates, key=lambda candidate: (candidate.created_at or datetime.min, candidate.event_suffix)),
        scan_truncated,
    )


async def _load_selected_candidates(
    repository: BoundedStaleOutboxHygieneRepository,
    suffixes: Sequence[str],
    *,
    for_update: bool,
) -> list[StaleOutboxCandidate]:
    candidates: list[StaleOutboxCandidate] = []
    for raw_suffix in suffixes:
        suffix = _safe_suffix_projection(raw_suffix)
        if suffix is None:
            raise BoundedStaleOutboxHygieneError("target_event_suffix_invalid")
        if for_update:
            rows = await repository.fetch_events_by_suffix_for_update(event_suffix=suffix, limit=2)
        else:
            rows = await repository.fetch_events_by_suffix(event_suffix=suffix, limit=2)
        if not rows:
            raise BoundedStaleOutboxHygieneError("target_event_suffix_missing")
        if len(rows) != 1:
            raise BoundedStaleOutboxHygieneError("target_event_suffix_not_unique")
        candidates.append(await _classify_row(repository, rows[0]))
    return candidates


async def _classify_row(
    repository: BoundedStaleOutboxHygieneRepository,
    row: StaleOutboxRow,
) -> StaleOutboxCandidate:
    canonical_relay_eligible = await repository.is_canonically_relay_eligible(event_id=row.event_id)
    if row.status != "pending":
        return _candidate(
            row,
            "unsafe_or_unknown",
            "not_eligible",
            "status_not_pending",
            canonical_relay_eligible=canonical_relay_eligible,
        )

    if row.event_type == JUDGE_OUTPUT_READY:
        judge_output_id = _payload_uuid(row.payload_json, "judge_output_id")
        if judge_output_id is None:
            return _candidate(
                row,
                "unsafe_or_unknown",
                "needs_manual_review",
                "judge_output_id_missing",
                canonical_relay_eligible=canonical_relay_eligible,
            )
        exists = await repository.judge_output_exists(judge_output_id)
        analysis_count = await repository.count_analyses_for_judge_output(judge_output_id)
        policy_count = await repository.count_policy_apply_events_for_judge_output(judge_output_id)
        if exists and (analysis_count > 0 or policy_count > 0):
            proven = await repository.has_stale_resolution_proof(
                event_id=row.event_id,
                classification="judge_output_ready_already_handed_off",
            )
            return _candidate(
                row,
                "judge_output_ready_already_handed_off",
                "already_stale_resolution_proven" if proven else "eligible_for_stale_resolution",
                "analysis_or_policy_handoff_exists",
                analysis_count=analysis_count,
                policy_apply_event_count=policy_count,
                judge_output_exists=exists,
                already_resolution_proven=proven,
                matching_resolution_proof_exists=proven,
                canonical_relay_eligible=canonical_relay_eligible,
            )
        return _candidate(
            row,
            "unsafe_or_unknown",
            "needs_manual_review",
            "judge_output_ready_without_handoff_evidence",
            analysis_count=analysis_count,
            policy_apply_event_count=policy_count,
            judge_output_exists=exists,
            canonical_relay_eligible=canonical_relay_eligible,
        )

    if row.event_type == POLICY_APPLY:
        judge_output_id = _payload_uuid(row.payload_json, "judge_output_id")
        if judge_output_id is None:
            return _candidate(
                row,
                "unsafe_or_unknown",
                "needs_manual_review",
                "judge_output_id_missing",
                canonical_relay_eligible=canonical_relay_eligible,
            )
        candidate_group_id = _payload_uuid(row.payload_json, "candidate_group_id")
        analysis_count = await repository.count_analyses_for_judge_output(judge_output_id)
        notification_plan_count = (
            await repository.count_notification_plans_for_candidate_group(candidate_group_id)
            if candidate_group_id is not None
            else 0
        )
        if analysis_count > 0:
            proven = await repository.has_stale_resolution_proof(
                event_id=row.event_id,
                classification="policy_apply_already_analyzed",
            )
            return _candidate(
                row,
                "policy_apply_already_analyzed",
                "already_stale_resolution_proven" if proven else "eligible_for_stale_resolution",
                "analysis_exists_for_judge_output",
                analysis_count=analysis_count,
                notification_plan_count=notification_plan_count,
                already_resolution_proven=proven,
                matching_resolution_proof_exists=proven,
                canonical_relay_eligible=canonical_relay_eligible,
            )
        return _candidate(
            row,
            "unsafe_or_unknown",
            "needs_manual_review",
            "policy_apply_without_analysis",
            analysis_count=analysis_count,
            notification_plan_count=notification_plan_count,
            canonical_relay_eligible=canonical_relay_eligible,
        )

    if row.event_type == DELIVERY_RESULT:
        notification_plan_id = _payload_uuid(row.payload_json, "notification_plan_id")
        notification_delivery_record_id = _payload_uuid(row.payload_json, "notification_delivery_record_id")
        if notification_plan_id is None or notification_delivery_record_id is None:
            return _candidate(
                row,
                "unsafe_or_unknown",
                "needs_manual_review",
                "delivery_result_ids_missing",
                canonical_relay_eligible=canonical_relay_eligible,
            )
        valid_delivery_result = await repository.delivery_result_has_plan_and_record(
            notification_plan_id=notification_plan_id,
            notification_delivery_record_id=notification_delivery_record_id,
        )
        if not valid_delivery_result:
            return _candidate(
                row,
                "unsafe_or_unknown",
                "needs_manual_review",
                "delivery_result_plan_or_record_missing",
                canonical_relay_eligible=canonical_relay_eligible,
            )
        current_published = await repository.delivery_result_current_event_published(
            notification_plan_id=notification_plan_id,
            notification_delivery_record_id=notification_delivery_record_id,
        )
        return _candidate(
            row,
            "delivery_result_hygiene",
            "hygiene_only" if current_published else "needs_manual_review",
            "delivery_result_hygiene_only",
            delivery_result_has_plan_and_record=True,
            delivery_result_current_published=current_published,
            deliberately_left_unchanged=True,
            canonical_relay_eligible=canonical_relay_eligible,
        )

    return _candidate(
        row,
        "unsafe_or_unknown",
        "needs_manual_review",
        "event_type_not_supported",
        canonical_relay_eligible=canonical_relay_eligible,
    )


def _selection_error(
    config: BoundedStaleOutboxHygieneConfig,
    selected: Sequence[StaleOutboxCandidate],
    *,
    require_safe: bool,
    require_matching_proof: bool = False,
    require_canonical_excluded: bool = False,
) -> str | None:
    if not config.target_event_suffixes:
        return "target_event_suffix_required"
    normalized_suffixes = tuple(filter(None, (_safe_suffix_projection(value) for value in config.target_event_suffixes)))
    if len(set(normalized_suffixes)) != len(normalized_suffixes):
        return "target_event_suffix_duplicate"
    if not config.classifications:
        return "classification_required"
    requested = set(config.classifications)
    invalid_requested = requested - ALL_CLASSIFICATIONS
    if invalid_requested:
        return "classification_not_allowed"
    if len(selected) != len(config.target_event_suffixes):
        return "target_event_suffix_selection_mismatch"
    for candidate in selected:
        if candidate.status != "pending":
            return "target_event_status_changed"
        if candidate.classification not in requested:
            return "target_event_classification_changed"
        if require_safe and not candidate.safe_resolution_classification:
            return "target_event_classification_not_executable"
        if require_matching_proof and not candidate.matching_resolution_proof_exists:
            return "matching_resolution_proof_missing"
        if require_canonical_excluded and candidate.canonical_relay_eligible:
            return "canonical_relay_exclusion_missing"
    return None


def _filter_candidates(
    candidates: Sequence[StaleOutboxCandidate],
    config: BoundedStaleOutboxHygieneConfig,
) -> list[StaleOutboxCandidate]:
    selected_suffixes = set(filter(None, (_safe_suffix_projection(value) for value in config.target_event_suffixes)))
    selected_classes = set(config.classifications)
    filtered = []
    for candidate in candidates:
        if selected_suffixes and candidate.event_suffix not in selected_suffixes:
            continue
        if selected_classes and candidate.classification not in selected_classes:
            continue
        filtered.append(candidate)
    return filtered


def _merge_candidates(
    candidates: Sequence[StaleOutboxCandidate],
    selected: Sequence[StaleOutboxCandidate],
) -> list[StaleOutboxCandidate]:
    merged = list(candidates)
    seen = {candidate.event_id for candidate in merged}
    for candidate in selected:
        if candidate.event_id not in seen:
            merged.append(candidate)
            seen.add(candidate.event_id)
    return merged


def _authority_gate_error(config: BoundedStaleOutboxHygieneConfig) -> str | None:
    if config.mode not in {"inventory", "plan", "execute", "proof"}:
        return "mode_not_allowed"
    if not config.operator_approved:
        return "operator_approval_missing"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if config.mode == "execute" and not config.allow_database_write:
        return "database_write_not_allowed"
    if config.mode != "execute" and config.allow_database_write:
        return "database_write_only_allowed_for_execute"
    if config.mode in {"plan", "execute", "proof"} and not config.target_event_suffixes:
        return "target_event_suffix_required"
    if config.scan_limit < 1 or config.scan_limit > MAX_SCAN_LIMIT:
        return "scan_limit_out_of_range"
    for suffix in config.target_event_suffixes:
        if _safe_suffix_projection(suffix) is None:
            return "target_event_suffix_invalid"
    if config.mode in {"plan", "execute", "proof"} and not config.classifications:
        return "classification_required"
    for classification in config.classifications:
        if classification not in ALL_CLASSIFICATIONS:
            return "classification_not_allowed"
    return None


def _candidate(
    row: StaleOutboxRow,
    classification: Classification,
    next_action: str,
    reason_code: str,
    *,
    analysis_count: int = 0,
    policy_apply_event_count: int = 0,
    notification_plan_count: int = 0,
    judge_output_exists: bool = False,
    delivery_result_has_plan_and_record: bool = False,
    delivery_result_current_published: bool = False,
    already_resolution_proven: bool = False,
    matching_resolution_proof_exists: bool = False,
    canonical_relay_eligible: bool = False,
    deliberately_left_unchanged: bool = False,
) -> StaleOutboxCandidate:
    return StaleOutboxCandidate(
        event_id=row.event_id,
        event_type=row.event_type,
        status=row.status,
        aggregate_type=row.aggregate_type,
        aggregate_id=row.aggregate_id,
        classification=classification,
        next_action=next_action,
        reason_code=reason_code,
        analysis_count=analysis_count,
        policy_apply_event_count=policy_apply_event_count,
        notification_plan_count=notification_plan_count,
        judge_output_exists=judge_output_exists,
        delivery_result_has_plan_and_record=delivery_result_has_plan_and_record,
        delivery_result_current_published=delivery_result_current_published,
        already_resolution_proven=already_resolution_proven,
        matching_resolution_proof_exists=matching_resolution_proof_exists,
        canonical_relay_eligible=canonical_relay_eligible,
        deliberately_left_unchanged=deliberately_left_unchanged,
        created_at=row.created_at,
    )


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedStaleOutboxHygieneConfig,
    state: BoundedStaleOutboxHygieneState,
    error_class: str | None = None,
    candidates: tuple[StaleOutboxCandidate, ...] = (),
    selected: tuple[StaleOutboxCandidate, ...] = (),
    selected_suffixes: tuple[str, ...] = (),
    selected_classifications: tuple[str, ...] = (),
    action_attempted: bool = False,
    inserted_count: int = 0,
    already_count: int = 0,
    scan_truncated: bool = False,
    scanned_candidate_count: int | None = None,
    remaining_hot_path_candidate_count_is_complete: bool = True,
    post_commit_readback_passed: bool | None = None,
) -> BoundedStaleOutboxHygieneResult:
    return BoundedStaleOutboxHygieneResult(
        status=status,
        ok=status == "pass" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        candidates=candidates,
        selected_candidates=selected,
        selected_target_suffixes=selected_suffixes,
        selected_classifications=selected_classifications,
        action_attempted=action_attempted,
        stale_resolution_proofs_inserted_count=inserted_count,
        stale_resolution_proofs_already_present_count=already_count,
        scan_truncated=scan_truncated,
        scanned_candidate_count=len(candidates) if scanned_candidate_count is None else scanned_candidate_count,
        remaining_hot_path_candidate_count_is_complete=remaining_hot_path_candidate_count_is_complete,
        post_commit_readback_passed=post_commit_readback_passed,
    )


def _row_from_mapping(row: Any) -> StaleOutboxRow:
    payload = _json_loads(row["payload_json"])
    return StaleOutboxRow(
        event_id=UUID(str(row["event_id"])),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=UUID(str(row["aggregate_id"])),
        payload_json=payload if isinstance(payload, dict) else {},
        status=str(row["status"]),
        created_at=row.get("created_at") if hasattr(row, "get") else row["created_at"],
    )


def _payload_uuid(payload: Mapping[str, Any], key: str) -> UUID | None:
    value = payload.get(key)
    if value in {None, ""}:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except JSONDecodeError:
            return None
    return value


def uuid_suffix(value: UUID) -> str:
    return value.hex[-8:]


def _safe_suffix_projection(value: str | None) -> str | None:
    if value is None:
        return None
    candidate = value.strip().lower()
    if FULL_UUID_RE.fullmatch(candidate):
        return None
    if not UUID_SUFFIX_RE.fullmatch(candidate):
        return None
    return candidate


def _proof_error_code(classification: str) -> str:
    return stale_resolution_proof_error_code_for_classification(classification) or "stale_outbox_unsafe_or_unknown_logical_noop"


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)


__all__ = [
    "ALL_CLASSIFICATIONS",
    "BoundedStaleOutboxHygieneConfig",
    "BoundedStaleOutboxHygieneError",
    "BoundedStaleOutboxHygieneRepositoryHandle",
    "BoundedStaleOutboxHygieneResult",
    "BoundedStaleOutboxHygieneRuntimeConfig",
    "StaleOutboxCandidate",
    "StaleOutboxRow",
    "argument_error_report",
    "load_bounded_stale_outbox_hygiene_runtime_config",
    "render_sanitized_json",
    "run_bounded_stale_outbox_hygiene",
    "run_bounded_stale_outbox_hygiene_sync",
    "uuid_suffix",
]
