from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from typing import Any, Protocol
from uuid import UUID

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - static validation fallback
    sa = None

from ..analysis_router.repositories import (
    AnalysisRouterRepository,
    AsyncSessionLike,
    BundleRefreshOutboxRecord,
    bundle_refresh_outbox_dedupe_key,
)
from ..outbox_relay.bounded_candidate_bundle_refresh_outbox_publish_runner import (
    BoundedCandidateBundleRefreshOutboxPublishConfig,
    BoundedCandidateBundleRefreshOutboxPublishError,
    BoundedCandidateBundleRefreshOutboxPublishResult,
    BoundedCandidateBundleRefreshPublishRuntimeConfig,
    QUEUE_NAME,
    STAGE_NAME,
    load_bounded_candidate_bundle_refresh_publish_runtime_config,
    render_sanitized_json,
    run_bounded_candidate_bundle_refresh_outbox_publish,
)


SCHEMA_VERSION = "bounded_candidate_bundle_refresh_event_publish_v1"
RUNNER_NAME = "bounded_candidate_bundle_refresh_event_publish_runner"
MODE_PLAN = "plan"
MODE_EXECUTE = "execute"
CONFIRM_TOKEN = "materialize-and-publish-candidate-bundle-refresh"
EVENT_TYPE = "candidate.bundle.refresh.v1"
ROOT_OBJECT_TYPE = "candidate_group"
HARD_MATCH_LIMIT = 2
SAFE_TOKEN_RE = re.compile(r"^[a-z0-9_-]{1,80}$")


@dataclass(frozen=True, slots=True)
class BoundedCandidateBundleRefreshEventPublishConfig:
    mode: str = MODE_PLAN
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_database_read: bool = False
    allow_database_write: bool = False
    allow_redis_publish: bool = False
    candidate_group_id: UUID | None = None
    candidate_group_suffix: str | None = None
    bundle_id: UUID | None = None
    refresh_reason: str | None = None
    confirm: str | None = None


@dataclass(slots=True)
class BoundedCandidateBundleRefreshEventPublishState:
    runtime_config_loaded: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False
    database_write_attempted: bool = False
    database_commit_attempted: bool = False
    redis_publish_attempted: bool = False
    publisher_attempted: bool = False


@dataclass(frozen=True, slots=True)
class CandidateCurrentBundleState:
    candidate_group_id: UUID
    current_bundle_id: UUID | None
    current_bundle_present: bool
    current_bundle_ready_for_analysis: bool


@dataclass(frozen=True, slots=True)
class RefreshEventReference:
    event_id: UUID
    status: str
    created: bool = False


class CandidateBundleRefreshEventRepository(Protocol):
    async def find_candidate_groups_by_suffix(self, suffix: str, *, limit: int) -> list[UUID]: ...
    async def load_candidate_current_bundle(self, candidate_group_id: UUID) -> CandidateCurrentBundleState | None: ...

    async def load_matching_refresh_event(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
        refresh_reason: str,
    ) -> RefreshEventReference | None: ...

    async def insert_bundle_refresh_outbox(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
        refresh_reason: str,
    ) -> BundleRefreshOutboxRecord: ...


@dataclass(frozen=True, slots=True)
class CandidateBundleRefreshEventRepositoryHandle:
    repository: CandidateBundleRefreshEventRepository
    close: Callable[[bool], Awaitable[None]]


class CandidateBundleRefreshEventRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedCandidateBundleRefreshPublishRuntimeConfig,
        state: BoundedCandidateBundleRefreshEventPublishState,
        logger: logging.Logger,
    ) -> CandidateBundleRefreshEventRepositoryHandle: ...


class CandidateBundleRefreshPublisherRunner(Protocol):
    async def __call__(
        self,
        config: BoundedCandidateBundleRefreshOutboxPublishConfig,
        **kwargs: Any,
    ) -> BoundedCandidateBundleRefreshOutboxPublishResult: ...


@dataclass(frozen=True, slots=True)
class BoundedCandidateBundleRefreshEventPublishResult:
    status: str
    ok: bool
    reason_code: str | None
    error_class: str | None
    config: BoundedCandidateBundleRefreshEventPublishConfig
    state: BoundedCandidateBundleRefreshEventPublishState = field(
        default_factory=BoundedCandidateBundleRefreshEventPublishState
    )
    candidate_group_id: UUID | None = None
    bundle_id: UUID | None = None
    refresh_event_id: UUID | None = None
    refresh_event_created: bool = False
    refresh_event_status_bucket: str = "none"
    existing_refresh_event_status_bucket: str = "none"
    current_bundle_present: bool = False
    current_bundle_ready_for_analysis: bool = False
    publish_would_be_attempted: bool = False
    publisher_status: str | None = None
    publisher_reason_code: str | None = None
    queue_name: str | None = None
    stage_name: str | None = None
    publisher_database_write_attempted: bool = False
    event_outbox_marked_published: bool = False
    events_published_count: int = 0
    job_attempts_inserted_count: int = 0

    def to_sanitized_dict(self) -> dict[str, Any]:
        database_write_attempted = self.state.database_write_attempted or self.publisher_database_write_attempted
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": self.config.mode,
            "ok": self.ok,
            "status": self.status,
            "reason_code": self.reason_code,
            "error_class": self.error_class,
            "candidate_group_suffix": _optional_id_suffix(self.candidate_group_id),
            "bundle_suffix": _optional_id_suffix(self.bundle_id),
            "current_bundle_suffix": _optional_id_suffix(self.bundle_id),
            "refresh_event_suffix": _optional_id_suffix(self.refresh_event_id),
            "refresh_event_created": self.refresh_event_created,
            "refresh_event_status_bucket": self.refresh_event_status_bucket,
            "existing_refresh_event_status_bucket": self.existing_refresh_event_status_bucket,
            "current_bundle_present": self.current_bundle_present,
            "current_bundle_ready_for_analysis": self.current_bundle_ready_for_analysis,
            "publish_would_be_attempted": self.publish_would_be_attempted,
            "publisher_attempted": self.state.publisher_attempted,
            "publisher_status": self.publisher_status,
            "publisher_reason_code": self.publisher_reason_code,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "event_outbox_marked_published": self.event_outbox_marked_published,
            "events_published_count": self.events_published_count,
            "job_attempts_inserted_count": self.job_attempts_inserted_count,
            "database_read_attempted": self.state.database_read_attempted,
            "database_write_attempted": database_write_attempted,
            "redis_publish_attempted": self.state.redis_publish_attempted,
            "side_effects": {
                "db_read": self.state.database_read_attempted,
                "db_write": database_write_attempted,
                "db_event_materialization_write": self.state.database_write_attempted,
                "redis_publish": self.state.redis_publish_attempted,
                "redis_consume_called": False,
                "redis_ack_called": False,
                "redis_group_created": False,
                "evidence_assembler_called": False,
                "judge_called": False,
                "judge_openai_called": False,
                "analysis_validator_called": False,
                "policy_called": False,
                "notifier_called": False,
                "telegram_read_called": False,
                "telegram_send_called": False,
                "openai_called": False,
                "github_api_called": False,
                "x_api_called": False,
                "web_fetch_called": False,
                "worker_started": False,
                "run_forever_called": False,
                "subprocess_called": False,
                "docker_called": False,
                "systemd_called": False,
                "alembic_called": False,
            },
            "redactions_applied": [
                "full_candidate_group_id_omitted",
                "full_bundle_id_omitted",
                "full_refresh_event_id_omitted",
                "dedupe_key_omitted",
                "refresh_reason_omitted",
                "payload_json_omitted",
                "database_url_omitted",
                "redis_url_omitted",
                "redis_message_id_omitted",
                "exception_detail_omitted",
            ],
        }


class SqlAlchemyCandidateBundleRefreshEventRepository:
    def __init__(self, session: AsyncSessionLike) -> None:
        self._session = session
        self._analysis_router_repository = AnalysisRouterRepository(session)

    async def find_candidate_groups_by_suffix(self, suffix: str, *, limit: int) -> list[UUID]:
        result = await self._session.execute(
            _sql(
                """
                SELECT candidate_group_id
                FROM candidate_group_proposals
                WHERE lower(CAST(candidate_group_id AS text)) LIKE :suffix_pattern
                ORDER BY candidate_group_id ASC
                LIMIT :limit
                """
            ),
            {"suffix_pattern": f"%{suffix.lower()}", "limit": limit},
        )
        return [UUID(str(row["candidate_group_id"])) for row in result.mappings().all()]

    async def load_candidate_current_bundle(
        self,
        candidate_group_id: UUID,
    ) -> CandidateCurrentBundleState | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT
                    cgp.candidate_group_id,
                    cgp.current_bundle_id,
                    ceb.bundle_id AS bundle_row_id,
                    ceb.ready_for_analysis
                FROM candidate_group_proposals cgp
                LEFT JOIN candidate_evidence_bundles ceb
                  ON ceb.bundle_id = cgp.current_bundle_id
                WHERE cgp.candidate_group_id = CAST(:candidate_group_id AS uuid)
                """
            ),
            {"candidate_group_id": str(candidate_group_id)},
        )
        row = result.mappings().first()
        if row is None:
            return None
        current_bundle_id = _uuid_or_none(row["current_bundle_id"])
        bundle_row_id = _uuid_or_none(row["bundle_row_id"])
        current_bundle_present = current_bundle_id is not None and bundle_row_id == current_bundle_id
        return CandidateCurrentBundleState(
            candidate_group_id=UUID(str(row["candidate_group_id"])),
            current_bundle_id=current_bundle_id,
            current_bundle_present=current_bundle_present,
            current_bundle_ready_for_analysis=current_bundle_present and bool(row["ready_for_analysis"]),
        )

    async def load_matching_refresh_event(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
        refresh_reason: str,
    ) -> RefreshEventReference | None:
        result = await self._session.execute(
            _sql(
                """
                SELECT event_id, status
                FROM event_outbox
                WHERE event_type = :event_type
                  AND aggregate_type = :aggregate_type
                  AND aggregate_id = CAST(:candidate_group_id AS uuid)
                  AND dedupe_key = :dedupe_key
                LIMIT 1
                """
            ),
            {
                "event_type": EVENT_TYPE,
                "aggregate_type": ROOT_OBJECT_TYPE,
                "candidate_group_id": str(candidate_group_id),
                "dedupe_key": bundle_refresh_outbox_dedupe_key(
                    candidate_group_id=candidate_group_id,
                    bundle_id=bundle_id,
                    refresh_reason=refresh_reason,
                ),
            },
        )
        row = result.mappings().first()
        if row is None:
            return None
        return RefreshEventReference(
            event_id=UUID(str(row["event_id"])),
            status=str(row["status"]),
            created=False,
        )

    async def insert_bundle_refresh_outbox(
        self,
        *,
        candidate_group_id: UUID,
        bundle_id: UUID,
        refresh_reason: str,
    ) -> BundleRefreshOutboxRecord:
        return await self._analysis_router_repository.insert_bundle_refresh_outbox(
            candidate_group_id=candidate_group_id,
            bundle_id=bundle_id,
            refresh_reason=refresh_reason,
        )


async def build_default_candidate_bundle_refresh_event_repository(
    runtime_config: BoundedCandidateBundleRefreshPublishRuntimeConfig,
    state: BoundedCandidateBundleRefreshEventPublishState,
    logger: logging.Logger,
) -> CandidateBundleRefreshEventRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    repository = SqlAlchemyCandidateBundleRefreshEventRepository(session)

    async def close(commit: bool) -> None:
        try:
            if commit:
                state.database_commit_attempted = True
                await session.commit()
            else:
                await session.rollback()
        finally:
            await session.close()
            await engine.dispose()

    return CandidateBundleRefreshEventRepositoryHandle(repository=repository, close=close)


async def run_bounded_candidate_bundle_refresh_event_publish(
    config: BoundedCandidateBundleRefreshEventPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedCandidateBundleRefreshPublishRuntimeConfig] = (
        load_bounded_candidate_bundle_refresh_publish_runtime_config
    ),
    repository_builder: CandidateBundleRefreshEventRepositoryBuilder | None = None,
    publisher_runner: CandidateBundleRefreshPublisherRunner = run_bounded_candidate_bundle_refresh_outbox_publish,
    logger: logging.Logger | None = None,
) -> BoundedCandidateBundleRefreshEventPublishResult:
    state = BoundedCandidateBundleRefreshEventPublishState()
    precheck_error = _precheck_error(config)
    if precheck_error is not None:
        return _result("blocked", precheck_error, config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedCandidateBundleRefreshOutboxPublishError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception:
        return _result("blocked", "runtime_config_error", config=config, state=state)

    repository_handle: CandidateBundleRefreshEventRepositoryHandle | None = None
    commit_materialization = False
    try:
        repository_handle = await (repository_builder or build_default_candidate_bundle_refresh_event_repository)(
            runtime_config,
            state,
            effective_logger,
        )
        repository = repository_handle.repository
        try:
            selected_candidate_group_id = config.candidate_group_id
            if selected_candidate_group_id is None:
                assert config.candidate_group_suffix is not None
                state.database_read_attempted = True
                matches = await repository.find_candidate_groups_by_suffix(
                    config.candidate_group_suffix,
                    limit=HARD_MATCH_LIMIT,
                )
                if not matches:
                    return _result("blocked", "candidate_group_not_found", config=config, state=state)
                if len(matches) > 1:
                    return _result("blocked", "candidate_group_suffix_ambiguous", config=config, state=state)
                selected_candidate_group_id = matches[0]

            assert selected_candidate_group_id is not None
            state.database_read_attempted = True
            candidate = await repository.load_candidate_current_bundle(selected_candidate_group_id)
            if candidate is None:
                return _result(
                    "blocked",
                    "candidate_group_not_found",
                    config=config,
                    state=state,
                    candidate_group_id=selected_candidate_group_id,
                )
            target_bundle_id = config.bundle_id or candidate.current_bundle_id
            if target_bundle_id is None or not candidate.current_bundle_present:
                return _result(
                    "blocked",
                    "current_bundle_missing",
                    config=config,
                    state=state,
                    candidate_group_id=selected_candidate_group_id,
                    bundle_id=target_bundle_id,
                    current_bundle_present=candidate.current_bundle_present,
                    current_bundle_ready=candidate.current_bundle_ready_for_analysis,
                )
            if config.bundle_id is not None and config.bundle_id != candidate.current_bundle_id:
                return _result(
                    "blocked",
                    "stale_bundle_request",
                    config=config,
                    state=state,
                    candidate_group_id=selected_candidate_group_id,
                    bundle_id=config.bundle_id,
                    current_bundle_present=candidate.current_bundle_present,
                    current_bundle_ready=candidate.current_bundle_ready_for_analysis,
                )
            if not candidate.current_bundle_ready_for_analysis:
                return _result(
                    "blocked",
                    "bundle_not_ready",
                    config=config,
                    state=state,
                    candidate_group_id=selected_candidate_group_id,
                    bundle_id=target_bundle_id,
                    current_bundle_present=candidate.current_bundle_present,
                    current_bundle_ready=False,
                )

            assert config.refresh_reason is not None
            existing = await repository.load_matching_refresh_event(
                candidate_group_id=selected_candidate_group_id,
                bundle_id=target_bundle_id,
                refresh_reason=config.refresh_reason,
            )
            existing_bucket = _status_bucket(None if existing is None else existing.status)
            if config.mode == MODE_PLAN:
                return _result(
                    "planned",
                    None,
                    config=config,
                    state=state,
                    candidate_group_id=selected_candidate_group_id,
                    bundle_id=target_bundle_id,
                    refresh_event_id=None if existing is None else existing.event_id,
                    refresh_event_created=False,
                    refresh_event_status_bucket=existing_bucket,
                    existing_refresh_event_status_bucket=existing_bucket,
                    current_bundle_present=True,
                    current_bundle_ready=True,
                    publish_would_be_attempted=False,
                    queue_name=QUEUE_NAME,
                    stage_name=STAGE_NAME,
                )

            if existing is not None and existing.status != "pending":
                return _result(
                    "blocked",
                    "refresh_event_not_pending_for_reason",
                    config=config,
                    state=state,
                    candidate_group_id=selected_candidate_group_id,
                    bundle_id=target_bundle_id,
                    refresh_event_id=existing.event_id,
                    refresh_event_created=False,
                    refresh_event_status_bucket=existing_bucket,
                    existing_refresh_event_status_bucket=existing_bucket,
                    current_bundle_present=True,
                    current_bundle_ready=True,
                    queue_name=QUEUE_NAME,
                    stage_name=STAGE_NAME,
                )

            if existing is None:
                state.database_write_attempted = True
                inserted = await repository.insert_bundle_refresh_outbox(
                    candidate_group_id=selected_candidate_group_id,
                    bundle_id=target_bundle_id,
                    refresh_reason=config.refresh_reason,
                )
                refresh_event = RefreshEventReference(
                    event_id=inserted.event_id,
                    status=inserted.status,
                    created=inserted.created,
                )
                if refresh_event.status != "pending":
                    return _result(
                        "blocked",
                        "refresh_event_not_pending_for_reason",
                        config=config,
                        state=state,
                        candidate_group_id=selected_candidate_group_id,
                        bundle_id=target_bundle_id,
                        refresh_event_id=refresh_event.event_id,
                        refresh_event_created=refresh_event.created,
                        refresh_event_status_bucket=_status_bucket(refresh_event.status),
                        existing_refresh_event_status_bucket=existing_bucket,
                        current_bundle_present=True,
                        current_bundle_ready=True,
                        queue_name=QUEUE_NAME,
                        stage_name=STAGE_NAME,
                    )
                commit_materialization = True
            else:
                refresh_event = existing

            close_error = await _close_repository(repository_handle, commit_materialization)
            repository_handle = None
            if close_error is not None:
                return replace(
                    _result(
                        "failed",
                        "repository_commit_failed" if commit_materialization else "repository_rollback_failed",
                        config=config,
                        state=state,
                        candidate_group_id=selected_candidate_group_id,
                        bundle_id=target_bundle_id,
                        refresh_event_id=refresh_event.event_id,
                        refresh_event_created=refresh_event.created,
                        refresh_event_status_bucket="pending",
                        existing_refresh_event_status_bucket=existing_bucket,
                        current_bundle_present=True,
                        current_bundle_ready=True,
                        queue_name=QUEUE_NAME,
                        stage_name=STAGE_NAME,
                    ),
                    error_class=_safe_exception_class(close_error),
                )

            state.publisher_attempted = True
            try:
                publisher_result = await publisher_runner(
                    BoundedCandidateBundleRefreshOutboxPublishConfig(
                        operator_approved=True,
                        allow_runtime_config=True,
                        allow_redis_publish=True,
                        allow_database_write=True,
                        event_id=refresh_event.event_id,
                        max_events=1,
                    ),
                    runtime_config_loader=lambda: runtime_config,
                )
            except Exception as exc:
                return _result(
                    "failed",
                    "publisher_runner_failed",
                    config=config,
                    state=state,
                    error_class=_safe_exception_class(exc),
                    candidate_group_id=selected_candidate_group_id,
                    bundle_id=target_bundle_id,
                    refresh_event_id=refresh_event.event_id,
                    refresh_event_created=refresh_event.created,
                    refresh_event_status_bucket="pending",
                    existing_refresh_event_status_bucket=existing_bucket,
                    current_bundle_present=True,
                    current_bundle_ready=True,
                    publish_would_be_attempted=True,
                    queue_name=QUEUE_NAME,
                    stage_name=STAGE_NAME,
                )
            publisher_report = publisher_result.to_sanitized_dict()
            state.redis_publish_attempted = bool(publisher_report.get("redis_publish_attempted"))
            publisher_status = _str_or_none(publisher_report.get("status"))
            publisher_reason_code = _str_or_none(publisher_report.get("error_code"))
            return _result(
                publisher_status or "failed",
                publisher_reason_code,
                config=config,
                state=state,
                error_class=_str_or_none(publisher_report.get("error_class")),
                candidate_group_id=selected_candidate_group_id,
                bundle_id=target_bundle_id,
                refresh_event_id=refresh_event.event_id,
                refresh_event_created=refresh_event.created,
                refresh_event_status_bucket="pending",
                existing_refresh_event_status_bucket=existing_bucket,
                current_bundle_present=True,
                current_bundle_ready=True,
                publish_would_be_attempted=True,
                publisher_status=publisher_status,
                publisher_reason_code=publisher_reason_code,
                queue_name=_str_or_none(publisher_report.get("queue_name")),
                stage_name=_str_or_none(publisher_report.get("stage_name")),
                publisher_database_write_attempted=bool(publisher_report.get("database_write_attempted")),
                event_outbox_marked_published=bool(publisher_report.get("event_outbox_marked_published")),
                events_published_count=_int_value(publisher_report.get("events_published_count")),
                job_attempts_inserted_count=_int_value(publisher_report.get("job_attempts_inserted_count")),
            )
        except Exception as exc:
            return _result(
                "failed",
                "database_write_failed" if state.database_write_attempted else "database_read_failed",
                config=config,
                state=state,
                error_class=_safe_exception_class(exc),
            )
    finally:
        if repository_handle is not None:
            await _close_repository(repository_handle, False)


def run_bounded_candidate_bundle_refresh_event_publish_sync(
    config: BoundedCandidateBundleRefreshEventPublishConfig,
    *,
    runtime_config_loader: Callable[[], BoundedCandidateBundleRefreshPublishRuntimeConfig] = (
        load_bounded_candidate_bundle_refresh_publish_runtime_config
    ),
    repository_builder: CandidateBundleRefreshEventRepositoryBuilder | None = None,
    publisher_runner: CandidateBundleRefreshPublisherRunner = run_bounded_candidate_bundle_refresh_outbox_publish,
    logger: logging.Logger | None = None,
) -> BoundedCandidateBundleRefreshEventPublishResult:
    return asyncio.run(
        run_bounded_candidate_bundle_refresh_event_publish(
            config,
            runtime_config_loader=runtime_config_loader,
            repository_builder=repository_builder,
            publisher_runner=publisher_runner,
            logger=logger,
        )
    )


def argument_error_report(reason_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        reason_code,
        config=BoundedCandidateBundleRefreshEventPublishConfig(),
        state=BoundedCandidateBundleRefreshEventPublishState(),
    ).to_sanitized_dict()


def _precheck_error(config: BoundedCandidateBundleRefreshEventPublishConfig) -> str | None:
    if config.mode not in {MODE_PLAN, MODE_EXECUTE}:
        return "invalid_mode"
    if not config.operator_approved:
        return "operator_approval_missing"
    selector_count = sum(value is not None for value in (config.candidate_group_id, config.candidate_group_suffix))
    if selector_count == 0:
        return "selector_missing"
    if selector_count > 1:
        return "selector_conflict"
    if config.candidate_group_suffix is not None and not _is_valid_suffix(config.candidate_group_suffix):
        return "invalid_candidate_group_suffix"
    if config.refresh_reason is None or not SAFE_TOKEN_RE.fullmatch(config.refresh_reason):
        return "invalid_refresh_reason"
    if not config.allow_runtime_config:
        return "runtime_config_not_allowed"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    if config.mode == MODE_EXECUTE:
        if not config.allow_database_write:
            return "database_write_not_allowed"
        if not config.allow_redis_publish:
            return "redis_publish_not_allowed"
        if config.confirm != CONFIRM_TOKEN:
            return "confirm_token_missing_or_invalid"
    return None


async def _close_repository(
    repository_handle: CandidateBundleRefreshEventRepositoryHandle,
    commit: bool,
) -> BaseException | None:
    try:
        await repository_handle.close(commit)
        return None
    except Exception as exc:
        return exc


def _result(
    status: str,
    reason_code: str | None,
    *,
    config: BoundedCandidateBundleRefreshEventPublishConfig,
    state: BoundedCandidateBundleRefreshEventPublishState,
    error_class: str | None = None,
    candidate_group_id: UUID | None = None,
    bundle_id: UUID | None = None,
    refresh_event_id: UUID | None = None,
    refresh_event_created: bool = False,
    refresh_event_status_bucket: str = "none",
    existing_refresh_event_status_bucket: str = "none",
    current_bundle_present: bool = False,
    current_bundle_ready: bool = False,
    publish_would_be_attempted: bool = False,
    publisher_status: str | None = None,
    publisher_reason_code: str | None = None,
    queue_name: str | None = None,
    stage_name: str | None = None,
    publisher_database_write_attempted: bool = False,
    event_outbox_marked_published: bool = False,
    events_published_count: int = 0,
    job_attempts_inserted_count: int = 0,
) -> BoundedCandidateBundleRefreshEventPublishResult:
    return BoundedCandidateBundleRefreshEventPublishResult(
        status=status,
        ok=status in {"planned", "published"} and reason_code is None,
        reason_code=reason_code,
        error_class=error_class,
        config=config,
        state=state,
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        refresh_event_id=refresh_event_id,
        refresh_event_created=refresh_event_created,
        refresh_event_status_bucket=refresh_event_status_bucket,
        existing_refresh_event_status_bucket=existing_refresh_event_status_bucket,
        current_bundle_present=current_bundle_present,
        current_bundle_ready_for_analysis=current_bundle_ready,
        publish_would_be_attempted=publish_would_be_attempted,
        publisher_status=publisher_status,
        publisher_reason_code=publisher_reason_code,
        queue_name=queue_name,
        stage_name=stage_name,
        publisher_database_write_attempted=publisher_database_write_attempted,
        event_outbox_marked_published=event_outbox_marked_published,
        events_published_count=events_published_count,
        job_attempts_inserted_count=job_attempts_inserted_count,
    )


def _status_bucket(status: str | None) -> str:
    if status is None:
        return "none"
    if status == "pending":
        return "pending"
    if status == "published":
        return "published"
    return "other"


def _is_valid_suffix(value: str) -> bool:
    stripped = value.strip().lower()
    return 4 <= len(stripped) <= 36 and all(char in "0123456789abcdef-" for char in stripped)


def _optional_id_suffix(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    return str(value)[-8:]


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _int_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _sql(statement: str) -> Any:
    if sa is None:
        return statement
    return sa.text(statement)


__all__ = [
    "BoundedCandidateBundleRefreshEventPublishConfig",
    "BoundedCandidateBundleRefreshEventPublishResult",
    "BoundedCandidateBundleRefreshEventPublishState",
    "CandidateBundleRefreshEventRepositoryBuilder",
    "CandidateBundleRefreshEventRepositoryHandle",
    "CandidateCurrentBundleState",
    "CONFIRM_TOKEN",
    "MODE_EXECUTE",
    "MODE_PLAN",
    "QUEUE_NAME",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "STAGE_NAME",
    "SqlAlchemyCandidateBundleRefreshEventRepository",
    "argument_error_report",
    "build_default_candidate_bundle_refresh_event_repository",
    "render_sanitized_json",
    "run_bounded_candidate_bundle_refresh_event_publish",
    "run_bounded_candidate_bundle_refresh_event_publish_sync",
]
