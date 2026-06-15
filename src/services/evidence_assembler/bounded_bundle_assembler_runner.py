from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Protocol
from uuid import UUID, uuid4

from .config import EvidenceAssemblerConfig, EvidenceAssemblerConfigurationError
from .models import AssemblyResult, BundleRefreshTarget, EvidenceBundleDraft
from .repositories import EvidenceAssemblerRepository
from .service import EvidenceAssemblerService


SCHEMA_VERSION = "bounded_evidence_assembler_job_runner_v1"
RUNNER_NAME = "bounded_evidence_assembler_job_runner"
MODE = "bundle_assembly_one_shot"
QUEUE_NAME = "q.candidate.bundle"
STAGE_NAME = "bundle"
ROOT_OBJECT_TYPE = "artifact"
EVENT_TYPE = "artifact.snapshot.updated.v1"
DEFAULT_MAX_MESSAGES = 1
HARD_MAX_MESSAGES = 1
DEFAULT_SCAN_LIMIT = 25
HARD_SCAN_LIMIT = 100
DEFAULT_CANDIDATE_FANOUT_LIMIT = 25
HARD_CANDIDATE_FANOUT_LIMIT = 100
REQUIRED_STREAM_FIELDS = frozenset(
    {
        "job_id",
        "stage_name",
        "root_object_type",
        "root_object_id",
        "idempotency_key",
        "trigger_event_id",
    }
)
FORBIDDEN_STREAM_FIELDS = frozenset(
    {
        "payload_json",
        "snapshot_id",
        "snapshot_type",
        "content_anchor",
        "final_url",
        "article_text",
        "title",
        "description",
        "raw_message_json",
        "body",
        "html",
    }
)
REQUIRED_EVENT_PAYLOAD_FIELDS = frozenset(
    {
        "artifact_id",
        "snapshot_id",
        "provider",
        "status",
        "content_anchor",
    }
)


@dataclass(frozen=True, slots=True)
class BoundedBundleAssemblerConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_consume: bool = False
    allow_database_write: bool = False
    allow_redis_ack: bool = False
    trigger_event_id: UUID | None = None
    artifact_id: UUID | None = None
    redis_message_id: str | None = None
    trigger_event_suffix: str | None = None
    max_messages: int = DEFAULT_MAX_MESSAGES
    scan_limit: int = DEFAULT_SCAN_LIMIT
    candidate_fanout_limit: int = DEFAULT_CANDIDATE_FANOUT_LIMIT


@dataclass(frozen=True, slots=True)
class BoundedBundleAssemblerRuntimeConfig:
    assembler_config: EvidenceAssemblerConfig

    @property
    def database_url(self) -> str:
        return self.assembler_config.database_url

    @property
    def redis_url(self) -> str:
        return self.assembler_config.redis_url


@dataclass(slots=True)
class BoundedBundleAssemblerState:
    runtime_config_loaded: bool = False
    redis_consume_attempted: bool = False
    redis_group_created: bool = False
    redis_cleanup_attempted: bool = False
    redis_cleanup_suppressed: bool = False
    redis_ack_attempted: bool = False
    database_session_opened: bool = False
    database_write_attempted: bool = False
    event_outbox_read_attempted: bool = False
    trigger_suffix_lookup_attempted: bool = False


@dataclass(slots=True)
class BoundedBundleAssemblerCounters:
    candidate_groups_seen: int = 0
    candidate_groups_processed: int = 0
    bundles_written_count: int = 0
    bundle_members_written_count: int = 0
    current_bundle_updates_count: int = 0
    reroot_events_written_count: int = 0
    analysis_requested_outbox_count: int = 0
    existing_bundle_reused_count: int = 0
    ready_for_analysis_count: int = 0


@dataclass(frozen=True, slots=True)
class RedisBundleMessage:
    job_id: str
    stage_name: str
    root_object_type: str
    root_object_id: str
    idempotency_key: str
    trigger_event_id: str

    @classmethod
    def from_stream_fields(cls, fields: Mapping[str, Any]) -> "RedisBundleMessage":
        return cls(
            job_id=str(fields.get("job_id", "")),
            stage_name=str(fields.get("stage_name", "")),
            root_object_type=str(fields.get("root_object_type", "")),
            root_object_id=str(fields.get("root_object_id", "")),
            idempotency_key=str(fields.get("idempotency_key", "")),
            trigger_event_id=str(fields.get("trigger_event_id", "")),
        )


@dataclass(frozen=True, slots=True)
class TargetedRedisBundleMessage:
    redis_message_id: str
    fields: dict[str, Any]
    message: RedisBundleMessage


@dataclass(frozen=True, slots=True)
class TriggerEventContract:
    event_id: UUID
    event_type: str
    status: str
    aggregate_type: str
    aggregate_id: UUID
    payload_json: dict[str, Any]
    snapshot_id: UUID
    snapshot_type: str
    snapshot_status: str
    content_anchor_present: bool
    impacted_candidate_group_count: int


@dataclass(frozen=True, slots=True)
class BoundedBundleAssemblerResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedBundleAssemblerConfig
    state: BoundedBundleAssemblerState = field(default_factory=BoundedBundleAssemblerState)
    counters: BoundedBundleAssemblerCounters = field(default_factory=BoundedBundleAssemblerCounters)
    target_trigger_event_id_suffix: str | None = None
    target_artifact_id_suffix: str | None = None
    redis_message_id_suffix: str | None = None
    target_snapshot_id_suffix: str | None = None
    target_snapshot_type: str | None = None
    target_snapshot_status: str | None = None
    messages_seen: int = 0
    messages_matched: int = 0
    messages_processed_count: int = 0
    redis_acked_count: int = 0
    redis_ack_status: str = "not_attempted"
    queue_name: str = QUEUE_NAME
    stage_name: str = STAGE_NAME

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "target_trigger_event_id_suffix": self.target_trigger_event_id_suffix,
            "target_artifact_id_suffix": self.target_artifact_id_suffix,
            "redis_message_id_suffix": self.redis_message_id_suffix,
            "target_snapshot_id_suffix": self.target_snapshot_id_suffix,
            "target_snapshot_type": self.target_snapshot_type,
            "target_snapshot_status": self.target_snapshot_status,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "messages_seen": self.messages_seen,
            "messages_matched": self.messages_matched,
            "messages_processed": self.messages_processed_count,
            "messages_processed_count": self.messages_processed_count,
            "candidate_groups_seen": self.counters.candidate_groups_seen,
            "candidate_groups_processed": self.counters.candidate_groups_processed,
            "bundles_written_count": self.counters.bundles_written_count,
            "bundle_members_written_count": self.counters.bundle_members_written_count,
            "current_bundle_updates_count": self.counters.current_bundle_updates_count,
            "reroot_events_written_count": self.counters.reroot_events_written_count,
            "analysis_requested_outbox_count": self.counters.analysis_requested_outbox_count,
            "existing_bundle_reused_count": self.counters.existing_bundle_reused_count,
            "ready_for_analysis_count": self.counters.ready_for_analysis_count,
            "ready_for_analysis": {
                "count": self.counters.ready_for_analysis_count,
                "candidate_groups_seen": self.counters.candidate_groups_seen,
            },
            "redis_consume_attempted": self.state.redis_consume_attempted,
            "redis_ack_attempted": self.state.redis_ack_attempted,
            "redis_ack_status": self.redis_ack_status,
            "redis_acked_count": self.redis_acked_count,
            "database_write_attempted": self.state.database_write_attempted,
            "event_outbox_read_attempted": self.state.event_outbox_read_attempted,
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "redis_consume_allowed": self.config.allow_redis_consume,
                "database_write_allowed": self.config.allow_database_write,
                "redis_ack_allowed": self.config.allow_redis_ack,
                "max_messages": self.config.max_messages,
                "scan_limit": self.config.scan_limit,
                "candidate_fanout_limit": self.config.candidate_fanout_limit,
            },
            "side_effects": {
                "redis_consume": self.state.redis_consume_attempted,
                "redis_ack": self.state.redis_ack_attempted,
                "redis_mutation": self.state.redis_group_created or self.state.redis_ack_attempted,
                "db_write": self.state.database_write_attempted,
                "event_outbox_write": self.counters.analysis_requested_outbox_count > 0,
                "candidate_bundle_write": self.counters.bundles_written_count > 0,
                "candidate_member_write": self.counters.bundle_members_written_count > 0,
                "current_bundle_update": self.counters.current_bundle_updates_count > 0,
                "analysis_router_called": False,
                "judge_called": False,
                "policy_called": False,
                "notifier_called": False,
                "telegram_read_called": False,
                "telegram_send_called": False,
                "openai_called": False,
                "github_api_called": False,
                "x_api_called": False,
                "web_fetch_called": False,
                "normalizer_called": False,
                "enricher_called": False,
                "outbox_publish_called": False,
                "worker_started": False,
                "run_forever_called": False,
                "systemd_called": False,
                "docker_called": False,
                "alembic_called": False,
                "subprocess_called": False,
            },
            "redactions_applied": {
                "full_trigger_event_id_omitted": True,
                "full_artifact_id_omitted": True,
                "full_snapshot_id_omitted": True,
                "redis_message_id_omitted": True,
                "idempotency_key_omitted": True,
                "payload_json_omitted": True,
                "snapshot_metadata_omitted": True,
                "content_anchor_omitted": True,
                "raw_text_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class BoundedBundleAssemblerError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class _BoundedBundleAssemblerResultReady(Exception):
    pass


class RedisTargetConsumer(Protocol):
    async def find_target(
        self,
        config: BoundedBundleAssemblerConfig,
        state: BoundedBundleAssemblerState,
    ) -> tuple[TargetedRedisBundleMessage | None, int, int]: ...

    async def ack(self, message_id: str, state: BoundedBundleAssemblerState) -> int: ...


class RedisTargetConsumerClient(Protocol):
    async def xlen(self, name: str) -> int: ...

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> Any: ...

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> Any: ...

    async def xack(self, name: str, groupname: str, *ids: str) -> Any: ...

    async def xgroup_destroy(self, name: str, groupname: str) -> Any: ...


@dataclass(frozen=True, slots=True)
class BoundedBundleAssemblerRedisHandle:
    consumer: RedisTargetConsumer
    close: Callable[[], Awaitable[None]]


class BundleAssemblerDatabase(Protocol):
    async def resolve_trigger_event_suffix(
        self,
        trigger_event_suffix: str,
        state: BoundedBundleAssemblerState,
    ) -> UUID: ...

    async def validate_trigger_event(
        self,
        selected: TargetedRedisBundleMessage,
        config: BoundedBundleAssemblerConfig,
        state: BoundedBundleAssemblerState,
    ) -> TriggerEventContract: ...

    async def assemble(
        self,
        trigger_event_id: UUID,
        state: BoundedBundleAssemblerState,
    ) -> list[AssemblyResult]: ...


@dataclass(frozen=True, slots=True)
class BoundedBundleAssemblerDatabaseHandle:
    database: BundleAssemblerDatabase
    counters: BoundedBundleAssemblerCounters
    close: Callable[[bool], Awaitable[None]]


class BoundedBundleAssemblerRedisBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedBundleAssemblerRuntimeConfig,
        state: BoundedBundleAssemblerState,
        logger: logging.Logger,
    ) -> BoundedBundleAssemblerRedisHandle: ...


class BoundedBundleAssemblerDatabaseBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedBundleAssemblerRuntimeConfig,
        state: BoundedBundleAssemblerState,
        logger: logging.Logger,
        fanout_limit: int,
    ) -> BoundedBundleAssemblerDatabaseHandle: ...


class TemporaryGroupRedisTargetConsumer:
    def __init__(
        self,
        client: RedisTargetConsumerClient,
        *,
        queue_name: str,
        group_name: str | None = None,
        consumer_name: str | None = None,
    ) -> None:
        self._client = client
        self._queue_name = queue_name
        unique = uuid4().hex
        self._group_name = group_name or f"bounded-evidence-assembler-{unique}"
        self._consumer_name = consumer_name or f"bounded-bundle-{unique}"
        self._group_created = False

    async def find_target(
        self,
        config: BoundedBundleAssemblerConfig,
        state: BoundedBundleAssemblerState,
    ) -> tuple[TargetedRedisBundleMessage | None, int, int]:
        state.redis_consume_attempted = True
        available_messages = await self._client.xlen(self._queue_name)
        if available_messages <= 0:
            return None, 0, 0
        await self._client.xgroup_create(self._queue_name, self._group_name, id="0", mkstream=False)
        self._group_created = True
        state.redis_group_created = True

        messages_seen = 0
        messages_matched = 0
        selected: TargetedRedisBundleMessage | None = None
        scan_limit = min(config.scan_limit, available_messages)
        while messages_seen < scan_limit:
            count = max(1, min(scan_limit - messages_seen, scan_limit))
            raw = await self._client.xreadgroup(
                self._group_name,
                self._consumer_name,
                {self._queue_name: ">"},
                count=count,
                block=None,
            )
            entries = _flatten_stream_entries(raw)
            if not entries:
                break
            for message_id, fields in entries:
                messages_seen += 1
                decoded_fields = _decode_fields(fields)
                if _matches_target(message_id, decoded_fields, config):
                    messages_matched += 1
                    if selected is None:
                        selected = TargetedRedisBundleMessage(
                            redis_message_id=message_id,
                            fields=decoded_fields,
                            message=RedisBundleMessage.from_stream_fields(decoded_fields),
                        )
                if messages_seen >= scan_limit:
                    break
        return selected, messages_seen, messages_matched

    async def ack(self, message_id: str, state: BoundedBundleAssemblerState) -> int:
        state.redis_ack_attempted = True
        result = await self._client.xack(self._queue_name, self._group_name, message_id)
        try:
            return int(result)
        except (TypeError, ValueError):
            return 1 if result else 0

    async def cleanup(self, state: BoundedBundleAssemblerState) -> None:
        if not self._group_created:
            return
        state.redis_cleanup_attempted = True
        try:
            await self._client.xgroup_destroy(self._queue_name, self._group_name)
        except Exception:
            state.redis_cleanup_suppressed = True


class CountingEvidenceAssemblerRepository:
    def __init__(
        self,
        repository: EvidenceAssemblerRepository,
        counters: BoundedBundleAssemblerCounters,
        *,
        fanout_limit: int,
    ) -> None:
        self._repository = repository
        self._counters = counters
        self._fanout_limit = fanout_limit

    def __getattr__(self, name: str) -> Any:
        return getattr(self._repository, name)

    def transaction(self):
        return self._repository.transaction()

    async def resolve_refresh_targets(self, trigger_event_id: UUID) -> list[BundleRefreshTarget]:
        targets = await self._repository.resolve_refresh_targets(trigger_event_id)
        self._counters.candidate_groups_seen = len(targets)
        if len(targets) > self._fanout_limit:
            raise BoundedBundleAssemblerError("candidate_fanout_limit_exceeded")
        return targets

    async def append_reroot_event(self, **kwargs: Any) -> None:
        await self._repository.append_reroot_event(**kwargs)
        self._counters.reroot_events_written_count += 1

    async def append_bundle(self, *, draft: EvidenceBundleDraft, bundle_version: int) -> UUID:
        bundle_id = await self._repository.append_bundle(draft=draft, bundle_version=bundle_version)
        self._counters.bundles_written_count += 1
        self._counters.bundle_members_written_count += len(draft.members)
        return bundle_id

    async def update_current_bundle(self, **kwargs: Any) -> None:
        await self._repository.update_current_bundle(**kwargs)
        self._counters.current_bundle_updates_count += 1

    async def insert_analysis_requested_outbox(self, **kwargs: Any) -> None:
        await self._repository.insert_analysis_requested_outbox(**kwargs)
        self._counters.analysis_requested_outbox_count += 1


class NoDiscoveredPromotionEvidenceAssemblerService(EvidenceAssemblerService):
    async def _promote_discovered_github_repos(self, **kwargs: Any) -> set[UUID]:
        del kwargs
        return set()


class SqlAlchemyBoundedBundleAssemblerDatabase:
    def __init__(
        self,
        *,
        session: Any,
        assembler_config: EvidenceAssemblerConfig,
        counters: BoundedBundleAssemblerCounters,
        fanout_limit: int,
        logger: logging.Logger,
    ) -> None:
        self._session = session
        self._assembler_config = assembler_config
        self._counters = counters
        self._fanout_limit = fanout_limit
        self._logger = logger

    async def resolve_trigger_event_suffix(
        self,
        trigger_event_suffix: str,
        state: BoundedBundleAssemblerState,
    ) -> UUID:
        import sqlalchemy as sa  # type: ignore[import-not-found]

        state.trigger_suffix_lookup_attempted = True
        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id
                FROM event_outbox
                WHERE event_type = :event_type
                  AND lower(CAST(event_id AS text)) LIKE :event_suffix_pattern
                ORDER BY created_at ASC, event_id ASC
                LIMIT 2
                """
            ),
            {"event_type": EVENT_TYPE, "event_suffix_pattern": f"%{trigger_event_suffix.lower()}"},
        )
        rows = result.mappings().all()
        if not rows:
            raise BoundedBundleAssemblerError("trigger_event_suffix_not_found")
        if len(rows) > 1:
            raise BoundedBundleAssemblerError("trigger_event_suffix_not_unique")
        return UUID(str(rows[0]["event_id"]))

    async def validate_trigger_event(
        self,
        selected: TargetedRedisBundleMessage,
        config: BoundedBundleAssemblerConfig,
        state: BoundedBundleAssemblerState,
    ) -> TriggerEventContract:
        import sqlalchemy as sa  # type: ignore[import-not-found]

        state.event_outbox_read_attempted = True
        trigger_event_id = _uuid_or_none(selected.message.trigger_event_id)
        if trigger_event_id is None:
            raise BoundedBundleAssemblerError("trigger_event_id_invalid")

        result = await self._session.execute(
            sa.text(
                """
                SELECT event_id, event_type, aggregate_type, aggregate_id,
                       payload_json, status
                FROM event_outbox
                WHERE event_id = CAST(:event_id AS uuid)
                """
            ),
            {"event_id": str(trigger_event_id)},
        )
        row = result.mappings().first()
        if row is None:
            raise BoundedBundleAssemblerError("trigger_event_not_found")

        payload = _json_loads(row["payload_json"]) or {}
        if not isinstance(payload, dict):
            raise BoundedBundleAssemblerError("malformed_event_payload")
        event = TriggerEventContract(
            event_id=UUID(str(row["event_id"])),
            event_type=str(row["event_type"]),
            status=str(row["status"]),
            aggregate_type=str(row["aggregate_type"]),
            aggregate_id=UUID(str(row["aggregate_id"])),
            payload_json=payload,
            snapshot_id=_payload_uuid(payload, "snapshot_id") or UUID(int=0),
            snapshot_type=_payload_string(payload, "snapshot_type") or "",
            snapshot_status=_payload_string(payload, "status") or "",
            content_anchor_present=_payload_field_present(payload.get("content_anchor")),
            impacted_candidate_group_count=0,
        )
        _validate_trigger_contract(event=event, selected=selected, config=config)
        impacted_count = await self._count_impacted_candidate_groups(event.aggregate_id)
        if impacted_count <= 0:
            raise BoundedBundleAssemblerError("candidate_group_not_found")
        if impacted_count > self._fanout_limit:
            raise BoundedBundleAssemblerError("candidate_fanout_limit_exceeded")
        return replace(event, impacted_candidate_group_count=impacted_count)

    async def assemble(
        self,
        trigger_event_id: UUID,
        state: BoundedBundleAssemblerState,
    ) -> list[AssemblyResult]:
        state.database_write_attempted = True
        repository = CountingEvidenceAssemblerRepository(
            EvidenceAssemblerRepository(self._session),
            self._counters,
            fanout_limit=self._fanout_limit,
        )
        service = NoDiscoveredPromotionEvidenceAssemblerService(
            self._assembler_config,
            repository=repository,  # type: ignore[arg-type]
            logger=self._logger,
        )
        return await service.handle_trigger_event(trigger_event_id)

    async def _count_impacted_candidate_groups(self, artifact_id: UUID) -> int:
        import sqlalchemy as sa  # type: ignore[import-not-found]

        result = await self._session.execute(
            sa.text(
                """
                SELECT COUNT(DISTINCT candidate_group_id)
                FROM candidate_group_members
                WHERE artifact_id = CAST(:artifact_id AS uuid)
                """
            ),
            {"artifact_id": str(artifact_id)},
        )
        return int(result.scalar_one())


async def build_default_bounded_bundle_assembler_redis_consumer(
    runtime_config: BoundedBundleAssemblerRuntimeConfig,
    state: BoundedBundleAssemblerState,
    logger: logging.Logger,
) -> BoundedBundleAssemblerRedisHandle:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    consumer = TemporaryGroupRedisTargetConsumer(
        redis_client,
        queue_name=runtime_config.assembler_config.queue_name,
    )

    async def close() -> None:
        await consumer.cleanup(state)
        close_client = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close_client is None:
            return
        result = close_client()
        if hasattr(result, "__await__"):
            await result

    return BoundedBundleAssemblerRedisHandle(consumer=consumer, close=close)


async def build_default_bounded_bundle_assembler_database(
    runtime_config: BoundedBundleAssemblerRuntimeConfig,
    state: BoundedBundleAssemblerState,
    logger: logging.Logger,
    fanout_limit: int,
) -> BoundedBundleAssemblerDatabaseHandle:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    counters = BoundedBundleAssemblerCounters()
    database = SqlAlchemyBoundedBundleAssemblerDatabase(
        session=session,
        assembler_config=runtime_config.assembler_config,
        counters=counters,
        fanout_limit=fanout_limit,
        logger=logger,
    )

    async def close(commit: bool) -> None:
        try:
            if commit:
                await session.commit()
            else:
                await session.rollback()
        finally:
            try:
                await session.close()
            finally:
                await engine.dispose()

    return BoundedBundleAssemblerDatabaseHandle(database=database, counters=counters, close=close)


def load_bounded_bundle_assembler_runtime_config(
    env: Mapping[str, str] | None = None,
) -> BoundedBundleAssemblerRuntimeConfig:
    source = os.environ if env is None else env
    try:
        assembler_config = EvidenceAssemblerConfig.from_env() if env is None else _config_from_mapping(source)
    except EvidenceAssemblerConfigurationError as exc:
        text = str(exc)
        if "DATABASE_URL" in text:
            raise BoundedBundleAssemblerError("database_url_missing") from exc
        if "REDIS_URL" in text:
            raise BoundedBundleAssemblerError("redis_url_missing") from exc
        raise BoundedBundleAssemblerError("runtime_config_error") from exc
    except Exception as exc:
        raise BoundedBundleAssemblerError("runtime_config_error") from exc
    if assembler_config.queue_name != QUEUE_NAME:
        raise BoundedBundleAssemblerError("queue_not_allowed")
    if assembler_config.batch_size != 1:
        assembler_config = _replace_config_batch_size(assembler_config, 1)
    return BoundedBundleAssemblerRuntimeConfig(assembler_config=assembler_config)


async def run_bounded_bundle_assembler(
    config: BoundedBundleAssemblerConfig,
    *,
    runtime_config_loader: Callable[[], BoundedBundleAssemblerRuntimeConfig] = (
        load_bounded_bundle_assembler_runtime_config
    ),
    redis_builder: BoundedBundleAssemblerRedisBuilder | None = None,
    database_builder: BoundedBundleAssemblerDatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedBundleAssemblerResult:
    state = BoundedBundleAssemblerState()
    target_error = _target_error(config)
    if not config.operator_approved:
        return _result("blocked", "operator_approval_missing", config=config, state=state)
    if target_error is not None:
        return _result("blocked", target_error, config=config, state=state)
    if config.max_messages != HARD_MAX_MESSAGES:
        return _result("blocked", "max_messages_must_be_one", config=config, state=state)
    if config.scan_limit <= 0 or config.scan_limit > HARD_SCAN_LIMIT:
        return _result("blocked", "scan_limit_out_of_range", config=config, state=state)
    if config.candidate_fanout_limit <= 0 or config.candidate_fanout_limit > HARD_CANDIDATE_FANOUT_LIMIT:
        return _result("blocked", "candidate_fanout_limit_out_of_range", config=config, state=state)
    if not config.allow_runtime_config:
        return _result("blocked", "runtime_config_not_allowed", config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedBundleAssemblerError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception:
        return _result("blocked", "runtime_config_error", config=config, state=state)

    if not config.allow_redis_consume:
        return _result("blocked", "redis_consume_not_allowed", config=config, state=state)
    if not config.allow_database_write:
        return _result("blocked", "database_write_not_allowed", config=config, state=state)
    if not config.allow_redis_ack:
        return _result("blocked", "redis_ack_not_allowed", config=config, state=state)

    redis_handle: BoundedBundleAssemblerRedisHandle | None = None
    database_handle: BoundedBundleAssemblerDatabaseHandle | None = None
    result: BoundedBundleAssemblerResult | None = None
    selected: TargetedRedisBundleMessage | None = None
    trigger_event: TriggerEventContract | None = None
    messages_seen = 0
    messages_matched = 0

    try:
        redis_handle = await (redis_builder or build_default_bounded_bundle_assembler_redis_consumer)(
            runtime_config,
            state,
            effective_logger,
        )
        selected, messages_seen, messages_matched = await redis_handle.consumer.find_target(config, state)
        if selected is None:
            result = _result(
                "blocked",
                "target_message_not_found",
                config=config,
                state=state,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
            )
            raise _BoundedBundleAssemblerResultReady
        if messages_matched > HARD_MAX_MESSAGES:
            result = _result(
                "blocked",
                "target_message_count_exceeded",
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
            )
            raise _BoundedBundleAssemblerResultReady

        contract_error = _selected_message_contract_error(selected, config)
        if contract_error is not None:
            result = _result(
                "blocked",
                contract_error,
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
            )
            raise _BoundedBundleAssemblerResultReady

        database_handle = await (database_builder or build_default_bounded_bundle_assembler_database)(
            runtime_config,
            state,
            effective_logger,
            config.candidate_fanout_limit,
        )
        try:
            if config.trigger_event_suffix is not None:
                resolved_event_id = await database_handle.database.resolve_trigger_event_suffix(
                    config.trigger_event_suffix,
                    state,
                )
                if resolved_event_id != _uuid_or_none(selected.message.trigger_event_id):
                    raise BoundedBundleAssemblerError("trigger_event_suffix_mismatch")
            trigger_event = await database_handle.database.validate_trigger_event(selected, config, state)
            if trigger_event.impacted_candidate_group_count <= 0:
                raise BoundedBundleAssemblerError("candidate_group_not_found")
            if trigger_event.impacted_candidate_group_count > config.candidate_fanout_limit:
                raise BoundedBundleAssemblerError("candidate_fanout_limit_exceeded")
            database_handle.counters.candidate_groups_seen = trigger_event.impacted_candidate_group_count
            assembly_results = await database_handle.database.assemble(trigger_event.event_id, state)
            _apply_assembly_results(database_handle.counters, assembly_results)
            if database_handle.counters.candidate_groups_processed <= 0:
                result = _result(
                    "blocked",
                    "candidate_group_not_processed",
                    config=config,
                    state=state,
                    selected=selected,
                    trigger_event=trigger_event,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                    counters=database_handle.counters,
                )
                raise _BoundedBundleAssemblerResultReady
        except BoundedBundleAssemblerError as exc:
            result = _result(
                "blocked",
                exc.error_code,
                config=config,
                state=state,
                selected=selected,
                trigger_event=trigger_event,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=database_handle.counters,
            )
            raise _BoundedBundleAssemblerResultReady
        except Exception as exc:
            result = _result(
                "failed",
                "database_write_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected=selected,
                trigger_event=trigger_event,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=database_handle.counters,
            )
            raise _BoundedBundleAssemblerResultReady

        committed_counters = database_handle.counters
        try:
            await database_handle.close(True)
        except Exception as exc:
            failed_counters = database_handle.counters
            database_handle = None
            result = _result(
                "failed",
                "database_write_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected=selected,
                trigger_event=trigger_event,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=failed_counters,
            )
            raise _BoundedBundleAssemblerResultReady
        database_handle = None

        try:
            acked_count = await redis_handle.consumer.ack(selected.redis_message_id, state)
        except Exception as exc:
            result = _result(
                "failed",
                "redis_ack_failed",
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected=selected,
                trigger_event=trigger_event,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                messages_processed_count=1,
                redis_ack_status="failed",
                counters=committed_counters,
            )
            raise _BoundedBundleAssemblerResultReady
        if acked_count != 1:
            result = _result(
                "failed",
                "redis_ack_failed",
                config=config,
                state=state,
                selected=selected,
                trigger_event=trigger_event,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                messages_processed_count=1,
                redis_acked_count=acked_count,
                redis_ack_status="failed",
                counters=committed_counters,
            )
            raise _BoundedBundleAssemblerResultReady

        result = _result(
            "assembled",
            None,
            config=config,
            state=state,
            selected=selected,
            trigger_event=trigger_event,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
            messages_processed_count=1,
            redis_acked_count=acked_count,
            redis_ack_status="acked",
            counters=committed_counters,
        )
    except _BoundedBundleAssemblerResultReady:
        pass
    except Exception as exc:
        result = _result(
            "failed",
            "bounded_bundle_assembler_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            selected=selected,
            trigger_event=trigger_event,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
        )
    finally:
        if database_handle is not None:
            try:
                await database_handle.close(False)
            except Exception as exc:
                result = _close_failure_result(
                    result,
                    exc,
                    config=config,
                    state=state,
                    selected=selected,
                    trigger_event=trigger_event,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                )
        if redis_handle is not None:
            try:
                await redis_handle.close()
            except Exception:
                state.redis_cleanup_suppressed = True

    assert result is not None
    return result


def run_bounded_bundle_assembler_sync(
    config: BoundedBundleAssemblerConfig,
    *,
    runtime_config_loader: Callable[[], BoundedBundleAssemblerRuntimeConfig] = (
        load_bounded_bundle_assembler_runtime_config
    ),
    redis_builder: BoundedBundleAssemblerRedisBuilder | None = None,
    database_builder: BoundedBundleAssemblerDatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedBundleAssemblerResult:
    return asyncio.run(
        run_bounded_bundle_assembler(
            config,
            runtime_config_loader=runtime_config_loader,
            redis_builder=redis_builder,
            database_builder=database_builder,
            logger=logger,
        )
    )


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def argument_error_report(error_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        error_code,
        config=BoundedBundleAssemblerConfig(),
        state=BoundedBundleAssemblerState(),
    ).to_sanitized_dict()


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedBundleAssemblerConfig,
    state: BoundedBundleAssemblerState,
    error_class: str | None = None,
    selected: TargetedRedisBundleMessage | None = None,
    trigger_event: TriggerEventContract | None = None,
    messages_seen: int = 0,
    messages_matched: int = 0,
    messages_processed_count: int = 0,
    redis_acked_count: int = 0,
    redis_ack_status: str = "not_attempted",
    counters: BoundedBundleAssemblerCounters | None = None,
) -> BoundedBundleAssemblerResult:
    effective_counters = counters or BoundedBundleAssemblerCounters()
    trigger_event_id = config.trigger_event_id
    artifact_id = config.artifact_id
    redis_message_id = config.redis_message_id
    snapshot_id: UUID | None = None
    snapshot_type: str | None = None
    snapshot_status: str | None = None
    if selected is not None:
        redis_message_id = selected.redis_message_id
        trigger_event_id = _uuid_or_none(selected.message.trigger_event_id) or trigger_event_id
        artifact_id = _uuid_or_none(selected.message.root_object_id) or artifact_id
    if trigger_event is not None:
        trigger_event_id = trigger_event.event_id
        artifact_id = trigger_event.aggregate_id
        snapshot_id = trigger_event.snapshot_id
        snapshot_type = trigger_event.snapshot_type or None
        snapshot_status = trigger_event.snapshot_status or None
    return BoundedBundleAssemblerResult(
        status=status,
        ok=status == "assembled" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        counters=effective_counters,
        target_trigger_event_id_suffix=_optional_id_suffix(trigger_event_id),
        target_artifact_id_suffix=_optional_id_suffix(artifact_id),
        redis_message_id_suffix=_optional_id_suffix(redis_message_id),
        target_snapshot_id_suffix=_optional_id_suffix(snapshot_id),
        target_snapshot_type=snapshot_type,
        target_snapshot_status=snapshot_status,
        messages_seen=messages_seen,
        messages_matched=messages_matched,
        messages_processed_count=messages_processed_count,
        redis_acked_count=redis_acked_count,
        redis_ack_status=redis_ack_status,
    )


def _close_failure_result(
    result: BoundedBundleAssemblerResult | None,
    exc: Exception,
    *,
    config: BoundedBundleAssemblerConfig,
    state: BoundedBundleAssemblerState,
    selected: TargetedRedisBundleMessage | None,
    trigger_event: TriggerEventContract | None,
    messages_seen: int,
    messages_matched: int,
) -> BoundedBundleAssemblerResult:
    if result is None:
        return _result(
            "failed",
            "database_rollback_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            selected=selected,
            trigger_event=trigger_event,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
        )
    return replace(
        result,
        status="failed",
        ok=False,
        error_code="database_rollback_failed",
        error_class=_safe_exception_class(exc),
    )


def _apply_assembly_results(
    counters: BoundedBundleAssemblerCounters,
    results: list[AssemblyResult],
) -> None:
    counters.candidate_groups_processed = len(results)
    counters.existing_bundle_reused_count = sum(1 for result in results if result.reused_existing_bundle)
    counters.ready_for_analysis_count = sum(1 for result in results if result.ready_for_analysis)


def _target_error(config: BoundedBundleAssemblerConfig) -> str | None:
    selected = [
        config.trigger_event_id is not None,
        config.artifact_id is not None,
        bool(config.redis_message_id),
        bool(config.trigger_event_suffix),
    ]
    count = sum(1 for item in selected if item)
    if count == 0:
        return "target_missing"
    if count > 1:
        return "target_conflict"
    if config.trigger_event_suffix is not None and not _is_valid_trigger_event_suffix(config.trigger_event_suffix):
        return "invalid_trigger_event_suffix"
    return None


def _matches_target(
    message_id: str,
    fields: Mapping[str, Any],
    config: BoundedBundleAssemblerConfig,
) -> bool:
    if config.redis_message_id:
        return message_id == config.redis_message_id
    if config.trigger_event_id is not None:
        return str(fields.get("trigger_event_id", "")) == str(config.trigger_event_id)
    if config.artifact_id is not None:
        return str(fields.get("root_object_id", "")) == str(config.artifact_id)
    if config.trigger_event_suffix is not None:
        return str(fields.get("trigger_event_id", "")).lower().endswith(config.trigger_event_suffix.lower())
    return False


def _selected_message_contract_error(
    selected: TargetedRedisBundleMessage,
    config: BoundedBundleAssemblerConfig,
) -> str | None:
    if FORBIDDEN_STREAM_FIELDS.intersection(selected.fields):
        return "redis_message_contract_invalid"
    if any(not str(selected.fields.get(key, "")).strip() for key in REQUIRED_STREAM_FIELDS):
        return "redis_message_contract_invalid"
    if selected.message.stage_name != STAGE_NAME:
        return "stage_not_allowed"
    if selected.message.root_object_type != ROOT_OBJECT_TYPE:
        return "root_object_type_not_allowed"
    selected_trigger_event_id = _uuid_or_none(selected.message.trigger_event_id)
    if selected_trigger_event_id is None:
        return "trigger_event_id_invalid"
    selected_artifact_id = _uuid_or_none(selected.message.root_object_id)
    if selected_artifact_id is None:
        return "redis_message_contract_invalid"
    if config.trigger_event_id is not None and selected_trigger_event_id != config.trigger_event_id:
        return "target_trigger_event_mismatch"
    if config.artifact_id is not None and selected_artifact_id != config.artifact_id:
        return "target_artifact_mismatch"
    if config.trigger_event_suffix is not None and not str(selected_trigger_event_id).lower().endswith(
        config.trigger_event_suffix.lower()
    ):
        return "target_trigger_event_mismatch"
    return None


def _validate_trigger_contract(
    *,
    event: TriggerEventContract,
    selected: TargetedRedisBundleMessage,
    config: BoundedBundleAssemblerConfig,
) -> None:
    selected_trigger_event_id = _uuid_or_none(selected.message.trigger_event_id)
    selected_artifact_id = _uuid_or_none(selected.message.root_object_id)
    if event.event_id != selected_trigger_event_id:
        raise BoundedBundleAssemblerError("trigger_event_id_mismatch")
    if event.event_type != EVENT_TYPE:
        raise BoundedBundleAssemblerError("event_type_not_allowed")
    if event.status != "published":
        raise BoundedBundleAssemblerError("event_not_published")
    if event.aggregate_type != ROOT_OBJECT_TYPE:
        raise BoundedBundleAssemblerError("aggregate_type_not_allowed")
    if event.aggregate_id != selected_artifact_id:
        raise BoundedBundleAssemblerError("aggregate_root_mismatch")
    if config.artifact_id is not None and event.aggregate_id != config.artifact_id:
        raise BoundedBundleAssemblerError("target_artifact_mismatch")
    if not all(_payload_field_present(event.payload_json.get(field)) for field in REQUIRED_EVENT_PAYLOAD_FIELDS):
        raise BoundedBundleAssemblerError("malformed_event_payload")
    payload_artifact_id = _payload_uuid(event.payload_json, "artifact_id")
    if payload_artifact_id is not None and payload_artifact_id != event.aggregate_id:
        raise BoundedBundleAssemblerError("payload_artifact_id_mismatch")
    if _payload_uuid(event.payload_json, "snapshot_id") is None:
        raise BoundedBundleAssemblerError("malformed_event_payload")


def _flatten_stream_entries(raw: Any) -> list[tuple[str, Mapping[str, Any]]]:
    messages: list[tuple[str, Mapping[str, Any]]] = []
    for _stream_name, entries in raw or []:
        for message_id, fields in entries:
            messages.append((str(_decode_value(message_id)), fields))
    return messages


def _decode_fields(fields: Mapping[Any, Any]) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for key, value in fields.items():
        decoded_key = _decode_value(key)
        decoded_value = _decode_value(value)
        decoded[str(decoded_key)] = decoded_value
    return decoded


def _decode_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _uuid_or_none(value: UUID | str | None) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _payload_uuid(payload: Mapping[str, Any], field_name: str) -> UUID | None:
    return _uuid_or_none(payload.get(field_name))


def _payload_string(payload: Mapping[str, Any], field_name: str) -> str | None:
    value = payload.get(field_name)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _payload_field_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _optional_id_suffix(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text[-8:] if text else None


def _is_valid_trigger_event_suffix(value: str) -> bool:
    stripped = value.strip().lower()
    return 4 <= len(stripped) <= 36 and all(char in "0123456789abcdef-" for char in stripped)


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


def _env_value(env: Mapping[str, str], name: str, default: str = "") -> str:
    return str(env.get(name, default) or "").strip()


def _env_int(env: Mapping[str, str], name: str, default: str) -> int:
    raw = _env_value(env, name, default)
    return int(raw)


def _env_bool(env: Mapping[str, str], name: str, default: str) -> bool:
    return _env_value(env, name, default).lower() not in {"0", "false", "no"}


def _config_from_mapping(env: Mapping[str, str]) -> EvidenceAssemblerConfig:
    config = EvidenceAssemblerConfig(
        app_env=_env_value(env, "APP_ENV", "dev").lower(),
        database_url=_env_value(env, "DATABASE_URL"),
        redis_url=_env_value(env, "REDIS_URL"),
        queue_name=_env_value(env, "EVIDENCE_ASSEMBLER_QUEUE_NAME", QUEUE_NAME),
        consumer_group=_env_value(env, "EVIDENCE_ASSEMBLER_CONSUMER_GROUP", "evidence-assembler"),
        consumer_name=_env_value(env, "EVIDENCE_ASSEMBLER_CONSUMER_NAME", "evidence-assembler-1"),
        batch_size=_env_int(env, "EVIDENCE_ASSEMBLER_BATCH_SIZE", "1"),
        block_ms=_env_int(env, "EVIDENCE_ASSEMBLER_BLOCK_MS", "5000"),
        bundle_profile_version=_env_value(
            env,
            "EVIDENCE_ASSEMBLER_BUNDLE_PROFILE_VERSION",
            "bundle_profile_v1",
        ),
        enable_text_idea=_env_bool(env, "EVIDENCE_ASSEMBLER_ENABLE_TEXT_IDEA", "true"),
        enable_reroot=_env_bool(env, "EVIDENCE_ASSEMBLER_ENABLE_REROOT", "true"),
        log_level=_env_value(env, "LOG_LEVEL", "INFO").upper(),
    )
    config.validate()
    return config


def _replace_config_batch_size(config: EvidenceAssemblerConfig, batch_size: int) -> EvidenceAssemblerConfig:
    return EvidenceAssemblerConfig(
        app_env=config.app_env,
        database_url=config.database_url,
        redis_url=config.redis_url,
        queue_name=config.queue_name,
        consumer_group=config.consumer_group,
        consumer_name=config.consumer_name,
        batch_size=batch_size,
        block_ms=config.block_ms,
        bundle_profile_version=config.bundle_profile_version,
        enable_text_idea=config.enable_text_idea,
        enable_reroot=config.enable_reroot,
        log_level=config.log_level,
    )


__all__ = [
    "BoundedBundleAssemblerConfig",
    "BoundedBundleAssemblerCounters",
    "BoundedBundleAssemblerDatabaseBuilder",
    "BoundedBundleAssemblerDatabaseHandle",
    "BoundedBundleAssemblerError",
    "BoundedBundleAssemblerRedisBuilder",
    "BoundedBundleAssemblerRedisHandle",
    "BoundedBundleAssemblerResult",
    "BoundedBundleAssemblerRuntimeConfig",
    "DEFAULT_CANDIDATE_FANOUT_LIMIT",
    "DEFAULT_MAX_MESSAGES",
    "DEFAULT_SCAN_LIMIT",
    "EVENT_TYPE",
    "FORBIDDEN_STREAM_FIELDS",
    "HARD_CANDIDATE_FANOUT_LIMIT",
    "HARD_MAX_MESSAGES",
    "HARD_SCAN_LIMIT",
    "MODE",
    "QUEUE_NAME",
    "REQUIRED_EVENT_PAYLOAD_FIELDS",
    "REQUIRED_STREAM_FIELDS",
    "ROOT_OBJECT_TYPE",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "STAGE_NAME",
    "SqlAlchemyBoundedBundleAssemblerDatabase",
    "TargetedRedisBundleMessage",
    "TemporaryGroupRedisTargetConsumer",
    "TriggerEventContract",
    "argument_error_report",
    "build_default_bounded_bundle_assembler_database",
    "build_default_bounded_bundle_assembler_redis_consumer",
    "load_bounded_bundle_assembler_runtime_config",
    "render_sanitized_json",
    "run_bounded_bundle_assembler",
    "run_bounded_bundle_assembler_sync",
]
