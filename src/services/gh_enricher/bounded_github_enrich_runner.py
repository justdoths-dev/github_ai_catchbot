from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Protocol
from uuid import UUID, uuid4

from .config import GhEnricherConfig, GhEnricherConfigurationError
from .fetch_planner import GitHubFetchPlanner
from .file_sampler import GitHubFileSampler
from .github_app_auth import GitHubAppTokenProvider
from .github_client import GitHubClient
from .models import ArtifactEnrichmentJob, ArtifactRecord, EnrichmentResult
from .repositories import GhEnricherRepository
from .service import GhEnricherService
from .url_discovery import GitHubUrlDiscovery


SCHEMA_VERSION = "bounded_github_enrich_runner_v1"
RUNNER_NAME = "bounded_gh_enricher_job_runner"
MODE = "github_enrich_one_shot"
QUEUE_NAME = "q.artifact.enrich.github"
STAGE_NAME = "enrich_github"
ROOT_OBJECT_TYPE = "artifact"
GITHUB_PROVIDER_ROUTE = "github"
GITHUB_ARTIFACT_TYPE = "github_repo"
GITHUB_NORMALIZED_HOST = "github.com"
DEFAULT_MAX_MESSAGES = 1
HARD_MAX_MESSAGES = 1
DEFAULT_SCAN_LIMIT = 25
HARD_SCAN_LIMIT = 100
HARD_GITHUB_REQUEST_LIMIT = 32
HARD_REQUEST_TIMEOUT_SEC = 10.0
HARD_SAMPLE_MAX_FILES = 20
HARD_SAMPLE_EXCERPT_CHARS = 1200
HARD_MAX_FILE_BYTES = 131_072
ALLOWED_GITHUB_API_BASE_URL = "https://api.github.com"
FORBIDDEN_STREAM_FIELDS = frozenset(
    {
        "payload_json",
        "canonical_url",
        "raw_url",
        "raw_message_json",
        "source_text",
        "raw_text",
        "telegram_text",
        "repo_full_name",
        "github_response",
        "readme",
        "file_contents",
        "body",
        "html",
    }
)
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


@dataclass(frozen=True, slots=True)
class BoundedGithubEnrichConfig:
    operator_approved: bool = False
    allow_runtime_config: bool = False
    allow_redis_consume: bool = False
    allow_database_write: bool = False
    allow_github_read: bool = False
    allow_redis_ack: bool = False
    redis_message_id: str | None = None
    artifact_id: UUID | None = None
    trigger_event_id: UUID | None = None
    max_messages: int = DEFAULT_MAX_MESSAGES
    scan_limit: int = DEFAULT_SCAN_LIMIT


@dataclass(slots=True)
class BoundedGithubEnrichState:
    runtime_config_loaded: bool = False
    redis_consume_attempted: bool = False
    redis_group_created: bool = False
    redis_cleanup_attempted: bool = False
    redis_cleanup_suppressed: bool = False
    redis_ack_attempted: bool = False
    database_session_opened: bool = False
    database_write_attempted: bool = False
    github_read_attempted: bool = False
    github_request_count: int = 0


@dataclass(slots=True)
class BoundedGithubEnrichCounters:
    enrichment_runs_written_count: int = 0
    enrichment_runs_finished_count: int = 0
    snapshots_written_count: int = 0
    github_repo_rows_written_count: int = 0
    github_file_samples_written_count: int = 0
    discovered_urls_written_count: int = 0
    artifact_registry_updates_count: int = 0
    snapshot_updated_outbox_inserted_count: int = 0
    artifact_snapshot_updated_event_suffixes: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class BoundedGithubEnrichRuntimeConfig:
    gh_config: GhEnricherConfig

    @property
    def database_url(self) -> str:
        return self.gh_config.database_url

    @property
    def redis_url(self) -> str:
        return self.gh_config.redis_url


@dataclass(frozen=True, slots=True)
class TargetedRedisMessage:
    redis_message_id: str
    fields: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ArtifactStatusReadback:
    artifact_type: str | None
    normalized_host: str | None
    canonical_url_present: bool
    current_status: str | None
    current_snapshot_id: UUID | None


@dataclass(frozen=True, slots=True)
class BoundedGithubEnrichResult:
    status: str
    ok: bool
    error_code: str | None
    error_class: str | None
    config: BoundedGithubEnrichConfig
    state: BoundedGithubEnrichState = field(default_factory=BoundedGithubEnrichState)
    counters: BoundedGithubEnrichCounters = field(default_factory=BoundedGithubEnrichCounters)
    target_artifact_id_suffix: str | None = None
    target_trigger_event_id_suffix: str | None = None
    redis_message_id_suffix: str | None = None
    messages_seen: int = 0
    messages_matched: int = 0
    messages_processed_count: int = 0
    redis_acked_count: int = 0
    queue_name: str = QUEUE_NAME
    stage_name: str = STAGE_NAME
    artifact_readback: ArtifactStatusReadback | None = None
    snapshot_status: str | None = None
    snapshot_id_suffix: str | None = None
    content_anchor_prefix: str | None = None

    def to_sanitized_dict(self) -> dict[str, Any]:
        readback = self.artifact_readback
        return {
            "schema_version": SCHEMA_VERSION,
            "runner_name": RUNNER_NAME,
            "mode": MODE,
            "ok": self.ok,
            "status": self.status,
            "error_code": self.error_code,
            "error_class": self.error_class,
            "target_artifact_id_suffix": self.target_artifact_id_suffix,
            "target_trigger_event_id_suffix": self.target_trigger_event_id_suffix,
            "redis_message_id_suffix": self.redis_message_id_suffix,
            "queue_name": self.queue_name,
            "stage_name": self.stage_name,
            "root_object_type": ROOT_OBJECT_TYPE,
            "messages_seen": self.messages_seen,
            "messages_matched": self.messages_matched,
            "messages_processed_count": self.messages_processed_count,
            "redis_consume_attempted": self.state.redis_consume_attempted,
            "redis_ack_attempted": self.state.redis_ack_attempted,
            "redis_ack_status": _redis_ack_status(
                attempted=self.state.redis_ack_attempted,
                acked_count=self.redis_acked_count,
                error_code=self.error_code,
            ),
            "redis_acked_count": self.redis_acked_count,
            "database_write_attempted": self.state.database_write_attempted,
            "github_read_attempted": self.state.github_read_attempted,
            "github_request_count": self.state.github_request_count,
            "enrichment_runs_written_count": self.counters.enrichment_runs_written_count,
            "enrichment_runs_finished_count": self.counters.enrichment_runs_finished_count,
            "snapshots_written_count": self.counters.snapshots_written_count,
            "github_repo_rows_written_count": self.counters.github_repo_rows_written_count,
            "github_file_samples_written_count": self.counters.github_file_samples_written_count,
            "discovered_urls_written_count": self.counters.discovered_urls_written_count,
            "artifact_registry_updates_count": self.counters.artifact_registry_updates_count,
            "snapshot_updated_outbox_inserted_count": self.counters.snapshot_updated_outbox_inserted_count,
            "artifact_snapshot_updated_event_suffixes": list(self.counters.artifact_snapshot_updated_event_suffixes),
            "artifact_type": readback.artifact_type if readback else None,
            "normalized_host": readback.normalized_host if readback else None,
            "canonical_url_present": readback.canonical_url_present if readback else None,
            "artifact_current_status": readback.current_status if readback else None,
            "artifact_current_snapshot_id_suffix": _optional_uuid_suffix(readback.current_snapshot_id) if readback else None,
            "snapshot_status": self.snapshot_status,
            "snapshot_id_suffix": self.snapshot_id_suffix,
            "content_anchor_prefix": _content_anchor_prefix(self.content_anchor_prefix),
            "gates": {
                "operator_approved": self.config.operator_approved,
                "runtime_config_allowed": self.config.allow_runtime_config,
                "redis_consume_allowed": self.config.allow_redis_consume,
                "database_write_allowed": self.config.allow_database_write,
                "github_read_allowed": self.config.allow_github_read,
                "redis_ack_allowed": self.config.allow_redis_ack,
                "max_messages": self.config.max_messages,
                "scan_limit": self.config.scan_limit,
                "github_request_limit": HARD_GITHUB_REQUEST_LIMIT,
            },
            "side_effects": {
                "redis_consume": self.state.redis_consume_attempted,
                "redis_group_created": self.state.redis_group_created,
                "redis_ack": self.state.redis_ack_attempted,
                "db_write": self.state.database_write_attempted,
                "github_read": self.state.github_read_attempted,
                "openai_called": False,
                "telegram_read_called": False,
                "telegram_send_called": False,
                "x_called": False,
                "web_called": False,
                "notification_table_write": False,
                "policy_called": False,
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
                "redis_message_id_omitted": True,
                "idempotency_key_omitted": True,
                "canonical_url_omitted": True,
                "repo_full_name_omitted": True,
                "raw_github_response_omitted": True,
                "readme_content_omitted": True,
                "file_contents_omitted": True,
                "token_values_omitted": True,
                "database_url_omitted": True,
                "redis_url_omitted": True,
                "exception_detail_omitted": True,
            },
        }


class BoundedGithubEnrichError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class _BoundedGithubEnrichResultReady(Exception):
    pass


class RedisTargetConsumer(Protocol):
    async def find_target(
        self,
        config: BoundedGithubEnrichConfig,
        state: BoundedGithubEnrichState,
    ) -> tuple[TargetedRedisMessage | None, int, int]: ...

    async def ack(self, message_id: str, state: BoundedGithubEnrichState) -> int: ...


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
class BoundedGithubEnrichRedisHandle:
    consumer: RedisTargetConsumer
    close: Callable[[], Awaitable[None]]


class BoundedGithubEnrichDatabase(Protocol):
    async def rehydrate_job(self, trigger_event_id: str) -> ArtifactEnrichmentJob | None: ...

    async def load_artifact(self, artifact_id: UUID) -> ArtifactRecord | None: ...

    async def handle_job(self, job: ArtifactEnrichmentJob) -> EnrichmentResult: ...


@dataclass(frozen=True, slots=True)
class BoundedGithubEnrichDatabaseHandle:
    database: BoundedGithubEnrichDatabase
    counters: BoundedGithubEnrichCounters
    close: Callable[[], Awaitable[None]]


class BoundedGithubEnrichRedisBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedGithubEnrichRuntimeConfig,
        state: BoundedGithubEnrichState,
        logger: logging.Logger,
    ) -> BoundedGithubEnrichRedisHandle: ...


class BoundedGithubEnrichDatabaseBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: BoundedGithubEnrichRuntimeConfig,
        state: BoundedGithubEnrichState,
        logger: logging.Logger,
    ) -> BoundedGithubEnrichDatabaseHandle: ...


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
        self._group_name = group_name or f"bounded-gh-enricher-{unique}"
        self._consumer_name = consumer_name or f"bounded-gh-enrich-{unique}"
        self._group_created = False

    async def find_target(
        self,
        config: BoundedGithubEnrichConfig,
        state: BoundedGithubEnrichState,
    ) -> tuple[TargetedRedisMessage | None, int, int]:
        state.redis_consume_attempted = True
        if await self._client.xlen(self._queue_name) <= 0:
            return None, 0, 0
        await self._client.xgroup_create(self._queue_name, self._group_name, id="0", mkstream=False)
        self._group_created = True
        state.redis_group_created = True

        messages_seen = 0
        messages_matched = 0
        selected: TargetedRedisMessage | None = None
        while messages_seen < config.scan_limit and selected is None:
            count = max(1, min(config.scan_limit - messages_seen, config.scan_limit))
            raw = await self._client.xreadgroup(
                self._group_name,
                self._consumer_name,
                {self._queue_name: ">"},
                count=count,
                block=0,
            )
            entries = _flatten_stream_entries(raw)
            if not entries:
                break
            for message_id, fields in entries:
                messages_seen += 1
                decoded_fields = _decode_fields(fields)
                if _matches_target(message_id, decoded_fields, config):
                    messages_matched += 1
                    selected = TargetedRedisMessage(redis_message_id=message_id, fields=decoded_fields)
                    break
                if messages_seen >= config.scan_limit:
                    break
        return selected, messages_seen, messages_matched

    async def ack(self, message_id: str, state: BoundedGithubEnrichState) -> int:
        state.redis_ack_attempted = True
        result = await self._client.xack(self._queue_name, self._group_name, message_id)
        try:
            return int(result)
        except (TypeError, ValueError):
            return 1 if result else 0

    async def cleanup(self, state: BoundedGithubEnrichState) -> None:
        if not self._group_created:
            return
        state.redis_cleanup_attempted = True
        try:
            await self._client.xgroup_destroy(self._queue_name, self._group_name)
        except Exception:
            state.redis_cleanup_suppressed = True


class CountingGhEnricherRepository:
    def __init__(
        self,
        repository: GhEnricherRepository,
        counters: BoundedGithubEnrichCounters,
    ) -> None:
        self._repository = repository
        self._counters = counters

    def transaction(self):
        return self._repository.transaction()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID):
        return await self._repository.load_job_by_trigger_event_id(trigger_event_id)

    async def load_artifact(self, artifact_id: UUID):
        return await self._repository.load_artifact(artifact_id)

    async def load_current_snapshot(self, snapshot_id: UUID | None):
        return await self._repository.load_current_snapshot(snapshot_id)

    async def insert_enrichment_run_if_absent(self, **kwargs):
        result = await self._repository.insert_enrichment_run_if_absent(**kwargs)
        if result is not None:
            self._counters.enrichment_runs_written_count += 1
        return result

    async def mark_enrichment_run_started(self, run_id: UUID) -> None:
        await self._repository.mark_enrichment_run_started(run_id)

    async def claim_failed_transient_enrichment_run_for_retry(self, **kwargs):
        return await self._repository.claim_failed_transient_enrichment_run_for_retry(**kwargs)

    async def load_enrichment_run_status_by_job_idempotency_key(self, **kwargs):
        return await self._repository.load_enrichment_run_status_by_job_idempotency_key(**kwargs)

    async def load_enrichment_run_by_job_idempotency_key(self, **kwargs):
        return await self._repository.load_enrichment_run_by_job_idempotency_key(**kwargs)

    async def load_valid_orphan_provider_snapshots(self, **kwargs):
        return await self._repository.load_valid_orphan_provider_snapshots(**kwargs)

    async def mark_enrichment_run_finished(self, **kwargs) -> None:
        await self._repository.mark_enrichment_run_finished(**kwargs)
        self._counters.enrichment_runs_finished_count += 1

    async def insert_snapshot(self, **kwargs):
        result = await self._repository.insert_snapshot(**kwargs)
        self._counters.snapshots_written_count += 1
        return result

    async def insert_github_repo_child(self, **kwargs) -> None:
        await self._repository.insert_github_repo_child(**kwargs)
        self._counters.github_repo_rows_written_count += 1

    async def insert_github_file_sample(self, **kwargs) -> None:
        await self._repository.insert_github_file_sample(**kwargs)
        self._counters.github_file_samples_written_count += 1

    async def insert_discovered_url(self, **kwargs) -> None:
        await self._repository.insert_discovered_url(**kwargs)
        self._counters.discovered_urls_written_count += 1

    async def update_artifact_current_snapshot(self, **kwargs) -> None:
        await self._repository.update_artifact_current_snapshot(**kwargs)
        self._counters.artifact_registry_updates_count += 1

    async def insert_snapshot_updated_outbox(self, **kwargs):
        result = await self._repository.insert_snapshot_updated_outbox(**kwargs)
        if result is not None:
            self._counters.snapshot_updated_outbox_inserted_count += 1
            self._counters.artifact_snapshot_updated_event_suffixes.append(_optional_uuid_suffix(result) or "")
        return result


class GateTrackedGitHubClient:
    def __init__(
        self,
        github_client: GitHubClient,
        state: BoundedGithubEnrichState,
        *,
        request_limit: int,
    ) -> None:
        self._github_client = github_client
        self._state = state
        self._request_limit = request_limit

    async def get_repo(self, owner: str, repo: str, *, auth_mode: str) -> dict[str, Any]:
        self._count_request()
        return await self._github_client.get_repo(owner, repo, auth_mode=auth_mode)

    async def get_tree(self, owner: str, repo: str, ref: str, *, recursive: bool, auth_mode: str) -> dict[str, Any]:
        self._count_request()
        return await self._github_client.get_tree(owner, repo, ref, recursive=recursive, auth_mode=auth_mode)

    async def get_contents(
        self,
        owner: str,
        repo: str,
        path: str,
        *,
        ref: str | None,
        auth_mode: str,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        self._count_request()
        return await self._github_client.get_contents(owner, repo, path, ref=ref, auth_mode=auth_mode)

    async def get_releases(self, owner: str, repo: str, *, auth_mode: str) -> list[dict[str, Any]]:
        self._count_request()
        return await self._github_client.get_releases(owner, repo, auth_mode=auth_mode)

    async def get_default_branch_head(self, owner: str, repo: str, default_branch: str, *, auth_mode: str) -> dict[str, Any]:
        self._count_request()
        return await self._github_client.get_default_branch_head(owner, repo, default_branch, auth_mode=auth_mode)

    async def get_gist(self, gist_id: str, *, auth_mode: str) -> dict[str, Any]:
        self._count_request()
        return await self._github_client.get_gist(gist_id, auth_mode=auth_mode)

    def _count_request(self) -> None:
        self._state.github_read_attempted = True
        self._state.github_request_count += 1
        if self._state.github_request_count > self._request_limit:
            raise BoundedGithubEnrichError("github_request_cap_exceeded")


class SessionBackedBoundedGithubEnrichDatabase:
    def __init__(
        self,
        *,
        session_factory: Callable[[], Any],
        service_config: GhEnricherConfig,
        github_client: GateTrackedGitHubClient,
        counters: BoundedGithubEnrichCounters,
        state: BoundedGithubEnrichState,
        logger: logging.Logger,
    ) -> None:
        self._session_factory = session_factory
        self._service_config = service_config
        self._github_client = github_client
        self._counters = counters
        self._state = state
        self._logger = logger

    async def rehydrate_job(self, trigger_event_id: str) -> ArtifactEnrichmentJob | None:
        async with self._session_factory() as session:
            self._state.database_session_opened = True
            repository = CountingGhEnricherRepository(GhEnricherRepository(session), self._counters)
            service = self._build_service(repository)
            return await service.rehydrate_job(trigger_event_id)

    async def load_artifact(self, artifact_id: UUID) -> ArtifactRecord | None:
        async with self._session_factory() as session:
            self._state.database_session_opened = True
            repository = CountingGhEnricherRepository(GhEnricherRepository(session), self._counters)
            return await repository.load_artifact(artifact_id)

    async def handle_job(self, job: ArtifactEnrichmentJob) -> EnrichmentResult:
        async with self._session_factory() as session:
            self._state.database_session_opened = True
            async with session.begin():
                repository = CountingGhEnricherRepository(GhEnricherRepository(session), self._counters)
                service = self._build_service(repository)
                return await service.handle_job(job)

    def _build_service(self, repository: CountingGhEnricherRepository) -> GhEnricherService:
        return GhEnricherService(
            self._service_config,
            repository=repository,  # type: ignore[arg-type]
            github_client=self._github_client,  # type: ignore[arg-type]
            fetch_planner=GitHubFetchPlanner(),
            file_sampler=GitHubFileSampler(),
            url_discovery=GitHubUrlDiscovery(),
            logger=self._logger,
        )


async def build_default_bounded_github_enrich_redis_consumer(
    runtime_config: BoundedGithubEnrichRuntimeConfig,
    state: BoundedGithubEnrichState,
    logger: logging.Logger,
) -> BoundedGithubEnrichRedisHandle:
    del logger
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    redis_client = Redis.from_url(runtime_config.redis_url, decode_responses=True)
    consumer = TemporaryGroupRedisTargetConsumer(
        redis_client,
        queue_name=runtime_config.gh_config.queue_name,
    )

    async def close() -> None:
        await consumer.cleanup(state)
        close_client = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close_client is None:
            return
        result = close_client()
        if hasattr(result, "__await__"):
            await result

    return BoundedGithubEnrichRedisHandle(consumer=consumer, close=close)


async def build_default_bounded_github_enrich_database(
    runtime_config: BoundedGithubEnrichRuntimeConfig,
    state: BoundedGithubEnrichState,
    logger: logging.Logger,
) -> BoundedGithubEnrichDatabaseHandle:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # type: ignore[import-not-found]

    config = runtime_config.gh_config
    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    counters = BoundedGithubEnrichCounters()
    token_provider = None
    service_config = config
    if config.github_app_id and config.github_installation_id and config.github_private_key:
        token_provider = GitHubAppTokenProvider(
            app_id=config.github_app_id,
            installation_id=config.github_installation_id,
            private_key_pem=config.github_private_key,
            api_base_url=config.github_api_base_url,
            timeout_sec=config.request_timeout_sec,
        )
    else:
        service_config = replace(config, github_app_id=None, github_installation_id=None, github_private_key=None)

    github_client = GateTrackedGitHubClient(
        GitHubClient(
            api_base_url=config.github_api_base_url,
            timeout_sec=config.request_timeout_sec,
            token_provider=token_provider,
        ),
        state,
        request_limit=HARD_GITHUB_REQUEST_LIMIT,
    )
    database = SessionBackedBoundedGithubEnrichDatabase(
        session_factory=session_factory,
        service_config=service_config,
        github_client=github_client,
        counters=counters,
        state=state,
        logger=logger,
    )

    async def close() -> None:
        await engine.dispose()

    return BoundedGithubEnrichDatabaseHandle(database=database, counters=counters, close=close)


def load_bounded_github_enrich_runtime_config() -> BoundedGithubEnrichRuntimeConfig:
    try:
        gh_config = GhEnricherConfig.from_env()
        _validate_runtime_caps(gh_config)
    except GhEnricherConfigurationError as exc:
        text = str(exc)
        if "DATABASE_URL" in text:
            raise BoundedGithubEnrichError("database_url_missing") from exc
        if "REDIS_URL" in text:
            raise BoundedGithubEnrichError("redis_url_missing") from exc
        raise BoundedGithubEnrichError("runtime_config_error") from exc
    except BoundedGithubEnrichError:
        raise
    except Exception as exc:
        raise BoundedGithubEnrichError("runtime_config_error") from exc
    return BoundedGithubEnrichRuntimeConfig(gh_config=gh_config)


async def run_bounded_github_enrich(
    config: BoundedGithubEnrichConfig,
    *,
    runtime_config_loader: Callable[[], BoundedGithubEnrichRuntimeConfig] = load_bounded_github_enrich_runtime_config,
    redis_builder: BoundedGithubEnrichRedisBuilder | None = None,
    database_builder: BoundedGithubEnrichDatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedGithubEnrichResult:
    state = BoundedGithubEnrichState()
    if not config.operator_approved:
        return _result("blocked", "operator_approval_missing", config=config, state=state)
    if _target_missing(config):
        return _result("blocked", "target_missing", config=config, state=state)
    if config.max_messages != HARD_MAX_MESSAGES:
        return _result("blocked", "max_messages_must_be_one", config=config, state=state)
    if config.scan_limit <= 0 or config.scan_limit > HARD_SCAN_LIMIT:
        return _result("blocked", "scan_limit_out_of_range", config=config, state=state)
    if not config.allow_runtime_config:
        return _result("blocked", "runtime_config_not_allowed", config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    try:
        runtime_config = runtime_config_loader()
        state.runtime_config_loaded = True
    except BoundedGithubEnrichError as exc:
        return _result("blocked", exc.error_code, config=config, state=state)
    except Exception:
        return _result("blocked", "runtime_config_error", config=config, state=state)

    if not config.allow_redis_consume:
        return _result("blocked", "redis_consume_not_allowed", config=config, state=state)
    if not config.allow_database_write:
        return _result("blocked", "database_write_not_allowed", config=config, state=state)
    if not config.allow_github_read:
        return _result("blocked", "github_read_not_allowed", config=config, state=state)
    if not config.allow_redis_ack:
        return _result("blocked", "redis_ack_not_allowed", config=config, state=state)

    redis_handle: BoundedGithubEnrichRedisHandle | None = None
    database_handle: BoundedGithubEnrichDatabaseHandle | None = None
    result: BoundedGithubEnrichResult | None = None
    selected: TargetedRedisMessage | None = None
    job: ArtifactEnrichmentJob | None = None
    service_result: EnrichmentResult | None = None
    artifact_readback: ArtifactStatusReadback | None = None
    messages_seen = 0
    messages_matched = 0

    try:
        redis_handle = await (redis_builder or build_default_bounded_github_enrich_redis_consumer)(
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
            raise _BoundedGithubEnrichResultReady

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
            raise _BoundedGithubEnrichResultReady

        database_handle = await (database_builder or build_default_bounded_github_enrich_database)(
            runtime_config,
            state,
            effective_logger,
        )
        try:
            job = await database_handle.database.rehydrate_job(str(selected.fields["trigger_event_id"]))
        except Exception as exc:
            result = _result(
                "blocked",
                _rehydrate_error_code(exc),
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=database_handle.counters,
            )
            raise _BoundedGithubEnrichResultReady
        if job is None:
            result = _result(
                "blocked",
                "trigger_event_not_found_or_not_github",
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=database_handle.counters,
            )
            raise _BoundedGithubEnrichResultReady

        job_error = _job_contract_error(job, selected, config)
        if job_error is not None:
            result = _result(
                "blocked",
                job_error,
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=database_handle.counters,
                job=job,
            )
            raise _BoundedGithubEnrichResultReady

        artifact = await database_handle.database.load_artifact(job.artifact_id)
        artifact_readback = _artifact_status_readback(artifact)
        artifact_error = _artifact_contract_error(artifact, job, config)
        if artifact_error is not None:
            result = _result(
                "blocked",
                artifact_error,
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=database_handle.counters,
                job=job,
                artifact_readback=artifact_readback,
            )
            raise _BoundedGithubEnrichResultReady

        try:
            service_result = await database_handle.database.handle_job(job)
        except Exception as exc:
            if _counters_total(database_handle.counters) > 0:
                state.database_write_attempted = True
            result = _result(
                "failed",
                _service_error_code(exc, state),
                error_class=_safe_exception_class(exc),
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=database_handle.counters,
                job=job,
                artifact_readback=artifact_readback,
            )
            raise _BoundedGithubEnrichResultReady

        state.database_write_attempted = _counters_total(database_handle.counters) > 0
        artifact_after = await database_handle.database.load_artifact(job.artifact_id)
        artifact_readback = _artifact_status_readback(artifact_after)
        readback_error = _post_write_readback_error(service_result, artifact_after, database_handle.counters)
        if readback_error is not None:
            result = _result(
                "failed",
                readback_error,
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                counters=database_handle.counters,
                job=job,
                service_result=service_result,
                artifact_readback=artifact_readback,
            )
            raise _BoundedGithubEnrichResultReady

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
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                messages_processed_count=1,
                counters=database_handle.counters,
                job=job,
                service_result=service_result,
                artifact_readback=artifact_readback,
            )
            raise _BoundedGithubEnrichResultReady
        if acked_count != 1:
            result = _result(
                "failed",
                "redis_ack_failed",
                config=config,
                state=state,
                selected=selected,
                messages_seen=messages_seen,
                messages_matched=messages_matched,
                messages_processed_count=1,
                redis_acked_count=acked_count,
                counters=database_handle.counters,
                job=job,
                service_result=service_result,
                artifact_readback=artifact_readback,
            )
            raise _BoundedGithubEnrichResultReady

        result = _result(
            "processed",
            None,
            config=config,
            state=state,
            selected=selected,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
            messages_processed_count=1,
            redis_acked_count=acked_count,
            counters=database_handle.counters,
            job=job,
            service_result=service_result,
            artifact_readback=artifact_readback,
        )
    except _BoundedGithubEnrichResultReady:
        pass
    except Exception as exc:
        result = _result(
            "failed",
            "bounded_github_enrich_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            selected=selected,
            messages_seen=messages_seen,
            messages_matched=messages_matched,
            counters=database_handle.counters if database_handle is not None else None,
            job=job,
            service_result=service_result,
            artifact_readback=artifact_readback,
        )
    finally:
        if database_handle is not None:
            try:
                await database_handle.close()
            except Exception as exc:
                result = _close_failure_result(
                    result,
                    exc,
                    config=config,
                    state=state,
                    selected=selected,
                    messages_seen=messages_seen,
                    messages_matched=messages_matched,
                    counters=database_handle.counters,
                    job=job,
                    service_result=service_result,
                    artifact_readback=artifact_readback,
                )
        if redis_handle is not None:
            try:
                await redis_handle.close()
            except Exception:
                state.redis_cleanup_suppressed = True

    assert result is not None
    return result


def run_bounded_github_enrich_sync(
    config: BoundedGithubEnrichConfig,
    *,
    runtime_config_loader: Callable[[], BoundedGithubEnrichRuntimeConfig] = load_bounded_github_enrich_runtime_config,
    redis_builder: BoundedGithubEnrichRedisBuilder | None = None,
    database_builder: BoundedGithubEnrichDatabaseBuilder | None = None,
    logger: logging.Logger | None = None,
) -> BoundedGithubEnrichResult:
    return asyncio.run(
        run_bounded_github_enrich(
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
        config=BoundedGithubEnrichConfig(),
        state=BoundedGithubEnrichState(),
    ).to_sanitized_dict()


def _result(
    status: str,
    error_code: str | None,
    *,
    config: BoundedGithubEnrichConfig,
    state: BoundedGithubEnrichState,
    error_class: str | None = None,
    selected: TargetedRedisMessage | None = None,
    messages_seen: int = 0,
    messages_matched: int = 0,
    messages_processed_count: int = 0,
    redis_acked_count: int = 0,
    counters: BoundedGithubEnrichCounters | None = None,
    job: ArtifactEnrichmentJob | None = None,
    service_result: EnrichmentResult | None = None,
    artifact_readback: ArtifactStatusReadback | None = None,
) -> BoundedGithubEnrichResult:
    effective_counters = counters or BoundedGithubEnrichCounters()
    trigger_id = config.trigger_event_id
    artifact_id = config.artifact_id
    redis_message_id = config.redis_message_id
    if selected is not None:
        redis_message_id = selected.redis_message_id
        trigger_id = _uuid_or_none(selected.fields.get("trigger_event_id")) or trigger_id
        artifact_id = _uuid_or_none(selected.fields.get("root_object_id")) or artifact_id
    if job is not None:
        trigger_id = job.trigger_event_id
        artifact_id = job.artifact_id
    snapshot_id = service_result.snapshot_id if service_result is not None else None
    content_anchor = service_result.content_anchor if service_result is not None else None
    return BoundedGithubEnrichResult(
        status=status,
        ok=status == "processed" and error_code is None,
        error_code=error_code,
        error_class=error_class,
        config=config,
        state=state,
        counters=effective_counters,
        target_artifact_id_suffix=_optional_uuid_suffix(artifact_id),
        target_trigger_event_id_suffix=_optional_uuid_suffix(trigger_id),
        redis_message_id_suffix=_optional_redis_id_suffix(redis_message_id),
        messages_seen=messages_seen,
        messages_matched=messages_matched,
        messages_processed_count=messages_processed_count,
        redis_acked_count=redis_acked_count,
        artifact_readback=artifact_readback,
        snapshot_status=service_result.status if service_result is not None else None,
        snapshot_id_suffix=_optional_uuid_suffix(snapshot_id),
        content_anchor_prefix=content_anchor,
    )


def _close_failure_result(
    result: BoundedGithubEnrichResult | None,
    exc: Exception,
    *,
    config: BoundedGithubEnrichConfig,
    state: BoundedGithubEnrichState,
    selected: TargetedRedisMessage | None,
    messages_seen: int,
    messages_matched: int,
    counters: BoundedGithubEnrichCounters,
    job: ArtifactEnrichmentJob | None,
    service_result: EnrichmentResult | None,
    artifact_readback: ArtifactStatusReadback | None,
) -> BoundedGithubEnrichResult:
    if result is None or result.ok:
        return _result(
            "failed",
            "database_close_failed",
            error_class=_safe_exception_class(exc),
            config=config,
            state=state,
            selected=selected,
            messages_seen=messages_seen if result is None else result.messages_seen,
            messages_matched=messages_matched if result is None else result.messages_matched,
            messages_processed_count=0 if result is None else result.messages_processed_count,
            redis_acked_count=0 if result is None else result.redis_acked_count,
            counters=counters,
            job=job,
            service_result=service_result,
            artifact_readback=artifact_readback,
        )
    return result


def _target_missing(config: BoundedGithubEnrichConfig) -> bool:
    return not (config.redis_message_id or config.artifact_id is not None or config.trigger_event_id is not None)


def _matches_target(message_id: str, fields: Mapping[str, Any], config: BoundedGithubEnrichConfig) -> bool:
    if config.redis_message_id:
        return message_id == config.redis_message_id
    if config.trigger_event_id is not None:
        return str(fields.get("trigger_event_id", "")) == str(config.trigger_event_id)
    if config.artifact_id is not None:
        return str(fields.get("root_object_id", "")) == str(config.artifact_id)
    return False


def _selected_message_contract_error(
    selected: TargetedRedisMessage,
    config: BoundedGithubEnrichConfig,
) -> str | None:
    if FORBIDDEN_STREAM_FIELDS.intersection(selected.fields):
        return "redis_message_contract_invalid"
    if any(not str(selected.fields.get(key, "")).strip() for key in REQUIRED_STREAM_FIELDS):
        return "redis_message_contract_invalid"
    if selected.fields.get("stage_name") != STAGE_NAME:
        return "stage_not_allowed"
    if selected.fields.get("root_object_type") != ROOT_OBJECT_TYPE:
        return "root_object_type_not_allowed"
    selected_trigger_event_id = _uuid_or_none(selected.fields.get("trigger_event_id"))
    if selected_trigger_event_id is None:
        return "trigger_event_id_invalid"
    selected_artifact_id = _uuid_or_none(selected.fields.get("root_object_id"))
    if selected_artifact_id is None:
        return "redis_message_contract_invalid"
    if config.trigger_event_id is not None and selected_trigger_event_id != config.trigger_event_id:
        return "target_trigger_event_mismatch"
    if config.artifact_id is not None and selected_artifact_id != config.artifact_id:
        return "target_artifact_mismatch"
    return None


def _job_contract_error(
    job: ArtifactEnrichmentJob,
    selected: TargetedRedisMessage,
    config: BoundedGithubEnrichConfig,
) -> str | None:
    selected_trigger_event_id = _uuid_or_none(selected.fields.get("trigger_event_id"))
    selected_artifact_id = _uuid_or_none(selected.fields.get("root_object_id"))
    if selected_trigger_event_id != job.trigger_event_id:
        return "target_trigger_event_mismatch"
    if selected_artifact_id != job.artifact_id:
        return "target_artifact_mismatch"
    if config.trigger_event_id is not None and config.trigger_event_id != job.trigger_event_id:
        return "target_trigger_event_mismatch"
    if config.artifact_id is not None and config.artifact_id != job.artifact_id:
        return "target_artifact_mismatch"
    if job.event_type != "artifact.enrich.requested.v1":
        return "trigger_event_contract_invalid"
    if job.provider_route != GITHUB_PROVIDER_ROUTE:
        return "provider_route_not_allowed"
    if job.artifact_type != GITHUB_ARTIFACT_TYPE:
        return "artifact_type_not_allowed"
    return None


def _artifact_contract_error(
    artifact: ArtifactRecord | None,
    job: ArtifactEnrichmentJob,
    config: BoundedGithubEnrichConfig,
) -> str | None:
    del config
    if artifact is None:
        return "artifact_not_found"
    if artifact.artifact_id != job.artifact_id:
        return "target_artifact_mismatch"
    if artifact.artifact_type != GITHUB_ARTIFACT_TYPE:
        return "artifact_type_not_allowed"
    if artifact.normalized_host != GITHUB_NORMALIZED_HOST:
        return "normalized_host_not_allowed"
    if not artifact.canonical_url:
        return "canonical_url_missing"
    return None


def _post_write_readback_error(
    service_result: EnrichmentResult,
    artifact_after: ArtifactRecord | None,
    counters: BoundedGithubEnrichCounters,
) -> str | None:
    if artifact_after is None:
        return "artifact_readback_missing"
    durable_signal = (
        service_result.emitted_snapshot_updated
        or counters.snapshot_updated_outbox_inserted_count > 0
        or counters.enrichment_runs_finished_count > 0
    )
    if not durable_signal:
        return "durable_readback_missing"
    if service_result.snapshot_id is not None and artifact_after.current_snapshot_id != service_result.snapshot_id:
        return "artifact_readback_mismatch"
    return None


def _artifact_status_readback(artifact: ArtifactRecord | None) -> ArtifactStatusReadback | None:
    if artifact is None:
        return None
    return ArtifactStatusReadback(
        artifact_type=artifact.artifact_type,
        normalized_host=artifact.normalized_host,
        canonical_url_present=bool(artifact.canonical_url),
        current_status=artifact.current_status,
        current_snapshot_id=artifact.current_snapshot_id,
    )


def _validate_runtime_caps(config: GhEnricherConfig) -> None:
    if config.queue_name != QUEUE_NAME:
        raise BoundedGithubEnrichError("queue_name_mismatch")
    if config.github_api_base_url.rstrip("/") != ALLOWED_GITHUB_API_BASE_URL:
        raise BoundedGithubEnrichError("github_api_base_url_not_allowed")
    if config.request_timeout_sec <= 0 or config.request_timeout_sec > HARD_REQUEST_TIMEOUT_SEC:
        raise BoundedGithubEnrichError("github_timeout_cap_out_of_range")
    if config.sample_max_files <= 0 or config.sample_max_files > HARD_SAMPLE_MAX_FILES:
        raise BoundedGithubEnrichError("sample_max_files_cap_out_of_range")
    if config.sample_excerpt_chars <= 0 or config.sample_excerpt_chars > HARD_SAMPLE_EXCERPT_CHARS:
        raise BoundedGithubEnrichError("sample_excerpt_cap_out_of_range")
    if config.max_file_bytes <= 0 or config.max_file_bytes > HARD_MAX_FILE_BYTES:
        raise BoundedGithubEnrichError("max_file_bytes_cap_out_of_range")


def _rehydrate_error_code(exc: Exception) -> str:
    if isinstance(exc, ValueError):
        return "trigger_event_contract_invalid"
    if isinstance(exc, BoundedGithubEnrichError):
        return exc.error_code
    return "database_read_failed"


def _service_error_code(exc: Exception, state: BoundedGithubEnrichState) -> str:
    del state
    if isinstance(exc, BoundedGithubEnrichError):
        return exc.error_code
    if exc.__class__.__name__.startswith("GitHub"):
        return "github_read_failed"
    if isinstance(exc, ValueError):
        return "trigger_event_contract_invalid"
    return "database_write_failed"


def _counters_total(counters: BoundedGithubEnrichCounters) -> int:
    return (
        counters.enrichment_runs_written_count
        + counters.enrichment_runs_finished_count
        + counters.snapshots_written_count
        + counters.github_repo_rows_written_count
        + counters.github_file_samples_written_count
        + counters.discovered_urls_written_count
        + counters.artifact_registry_updates_count
        + counters.snapshot_updated_outbox_inserted_count
    )


def _flatten_stream_entries(raw: Any) -> list[tuple[str, Mapping[str, Any]]]:
    messages: list[tuple[str, Mapping[str, Any]]] = []
    for _stream_name, entries in raw or []:
        for message_id, fields in entries:
            messages.append((str(_decode_value(message_id)), fields))
    return messages


def _decode_fields(fields: Mapping[Any, Any]) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for key, value in fields.items():
        decoded[str(_decode_value(key))] = _decode_value(value)
    return decoded


def _decode_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _uuid_or_none(value: UUID | str | None | Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _optional_uuid_suffix(value: UUID | str | None) -> str | None:
    if value is None:
        return None
    parsed = _uuid_or_none(value)
    text = str(parsed or value)
    return text[-8:] if text else None


def _optional_redis_id_suffix(value: str | None) -> str | None:
    if not value:
        return None
    if "-" not in value:
        return value[-8:]
    timestamp, sequence = value.rsplit("-", 1)
    return f"{timestamp[-6:]}-{sequence}"


def _content_anchor_prefix(value: str | None) -> str | None:
    if not value:
        return None
    return value[:12]


def _safe_exception_class(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name.isidentifier() and len(name) <= 80:
        return name
    return "unknown"


def _redis_ack_status(*, attempted: bool, acked_count: int, error_code: str | None) -> str:
    if not attempted:
        return "not_attempted"
    if error_code == "redis_ack_failed":
        return "failed"
    if acked_count == 1:
        return "acked"
    return "unknown"
