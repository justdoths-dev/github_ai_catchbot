from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from urllib.parse import parse_qsl, quote, unquote, urlsplit, urlunsplit
from uuid import UUID


SCHEMA_VERSION = "local_db_source_candidate_replay_v1"
SUPPORTED_SCHEMES = {"postgresql", "postgresql+psycopg", "postgresql+psycopg2"}
LOCAL_HOSTS = {"", "localhost", "127.0.0.1", "::1"}
SAFE_DATABASE_MARKERS = ("test", "local", "dev")
FORBIDDEN_DATABASE_MARKERS = ("prod", "production", "live")
FORBIDDEN_DATABASE_NAMES = {
    "default",
    "github_ai_catchbot",
    "main",
    "postgres",
    "template0",
    "template1",
}
NAMESPACE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
NORMALIZER_VERSION_PREFIX = "local-db-source-candidate-replay-v1"
SOURCE_EVENT_TYPE = "source_message.created.v1"
ENRICH_EVENT_TYPE = "artifact.enrich.requested.v1"
TRUE_RESULT_KEYS = (
    "fixture_loaded",
    "database_url_guard_passed",
    "source_message_upserted",
    "source_version_upserted",
    "source_outbox_event_created",
    "normalization_run_created",
    "artifact_created",
    "artifact_observation_created",
    "candidate_group_created",
    "candidate_member_created",
    "enrich_requested_event_created",
)
FALSE_RESULT_KEYS = (
    "production_db_write",
    "live_telegram_called",
    "openai_called",
    "workers_started",
    "redis_mutation",
)


@dataclass(frozen=True, slots=True)
class ParsedDatabaseUrl:
    raw_url: str
    scheme: str
    hostname: str
    database_name: str
    username: str | None
    password_present: bool


@dataclass(frozen=True, slots=True)
class SourceFixture:
    source_message_id: UUID
    source_version_no: int
    platform: str
    chat_id: int
    message_id: int
    text_body: str | None
    caption_text: str | None
    text_surface: str | None
    entities_json: list[dict[str, Any]]
    url_surface_json: list[dict[str, Any]]
    posted_at: datetime
    raw_message_json: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReplayExecutionResult:
    source_message_upserted: bool
    source_version_upserted: bool
    source_outbox_event_created: bool
    normalization_run_created: bool
    artifact_created: bool
    artifact_observation_created: bool
    candidate_group_created: bool
    candidate_member_created: bool
    enrich_requested_event_created: bool
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


class ReplayExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        fixture: SourceFixture,
        replay_namespace: str,
    ) -> ReplayExecutionResult: ...


class SqlAlchemyLocalReplayExecutor:
    def execute(
        self,
        *,
        database_url: str,
        fixture: SourceFixture,
        replay_namespace: str,
    ) -> ReplayExecutionResult:
        return asyncio.run(
            _execute_db_replay(
                database_url=database_url,
                fixture=fixture,
                replay_namespace=replay_namespace,
            )
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replay a source-message fixture into a guarded local/test PostgreSQL "
            "database and print stable JSON only."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--replay-namespace", required=True)
    parser.add_argument("--confirm-local-test-db", action="store_true")
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    executor: ReplayExecutor | None = None,
    repo_root: Path | None = None,
) -> RunnerResult:
    effective_env = env if env is not None else os.environ
    root = repo_root or _repo_root()
    report = _base_report()
    checks_failed: list[str] = []

    if not args.confirm_local_test_db:
        checks_failed.append("confirm_local_test_db_required")

    app_env = effective_env.get("APP_ENV", "").strip().lower()
    if app_env in {"prod", "production", "live"}:
        checks_failed.append("app_env_production_rejected")

    namespace_ok, namespace_failures = validate_replay_namespace(args.replay_namespace)
    checks_failed.extend(namespace_failures)

    database_ok, database_failures, _ = validate_database_url(args.database_url)
    report["database_url_guard_passed"] = database_ok
    checks_failed.extend(database_failures)

    fixture: SourceFixture | None = None
    try:
        fixture = load_source_fixture(Path(args.fixture), repo_root=root)
        report["fixture_loaded"] = True
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("fixture_load_failed")

    if checks_failed:
        return _finish(report, checks_failed)

    if fixture is None or not namespace_ok:
        checks_failed.append("precondition_missing")
        return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyLocalReplayExecutor()
    try:
        execution = active_executor.execute(
            database_url=args.database_url,
            fixture=fixture,
            replay_namespace=args.replay_namespace,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
        checks_failed.append(_safe_failure_code(exc))
        return _finish(report, checks_failed)

    report.update(
        {
            "source_message_upserted": execution.source_message_upserted,
            "source_version_upserted": execution.source_version_upserted,
            "source_outbox_event_created": execution.source_outbox_event_created,
            "normalization_run_created": execution.normalization_run_created,
            "artifact_created": execution.artifact_created,
            "artifact_observation_created": execution.artifact_observation_created,
            "candidate_group_created": execution.candidate_group_created,
            "candidate_member_created": execution.candidate_member_created,
            "enrich_requested_event_created": execution.enrich_requested_event_created,
        }
    )
    checks_failed.extend(execution.checks_failed)
    for key in TRUE_RESULT_KEYS:
        if report.get(key) is not True:
            checks_failed.append(f"{key}:missing")
    for key in FALSE_RESULT_KEYS:
        if report.get(key) is not False:
            checks_failed.append(f"{key}:unexpected")

    return _finish(report, checks_failed)


def validate_replay_namespace(replay_namespace: str | None) -> tuple[bool, list[str]]:
    value = (replay_namespace or "").strip()
    if not value:
        return False, ["replay_namespace_required"]
    if not NAMESPACE_RE.fullmatch(value):
        return False, ["replay_namespace_unsafe"]
    return True, []


def validate_database_url(database_url: str | None) -> tuple[bool, list[str], ParsedDatabaseUrl | None]:
    if database_url is None or not database_url.strip():
        return False, ["database_url_required"], None

    try:
        parsed = parse_database_url(database_url)
    except ValueError as exc:
        return False, [str(exc)], None

    failures: list[str] = []
    if parsed.scheme not in SUPPORTED_SCHEMES:
        failures.append("database_url_unsupported_scheme")
    if parsed.hostname not in LOCAL_HOSTS:
        failures.append("database_url_remote_host_rejected")
    query_host = _query_host(database_url)
    if query_host and not _query_host_is_local(query_host):
        failures.append("database_url_remote_query_host_rejected")
    if not parsed.database_name:
        failures.append("database_url_database_name_required")

    database_name = parsed.database_name.lower()
    if database_name in FORBIDDEN_DATABASE_NAMES:
        failures.append("database_url_forbidden_database_name")
    if any(marker in database_name for marker in FORBIDDEN_DATABASE_MARKERS):
        failures.append("database_url_production_name_rejected")
    if not any(marker in database_name for marker in SAFE_DATABASE_MARKERS):
        failures.append("database_url_missing_local_test_marker")

    return not failures, failures, parsed


def parse_database_url(database_url: str) -> ParsedDatabaseUrl:
    value = database_url.strip()
    try:
        split = urlsplit(value)
        database_name = unquote(split.path.lstrip("/").split("/", 1)[0]) if split.path else ""
        username = unquote(split.username) if split.username else None
    except ValueError as exc:
        raise ValueError("database_url_parse_failed") from exc
    if not split.scheme:
        raise ValueError("database_url_parse_failed")
    return ParsedDatabaseUrl(
        raw_url=value,
        scheme=split.scheme.lower(),
        hostname=(split.hostname or "").lower(),
        database_name=database_name,
        username=username,
        password_present=split.password is not None,
    )


def redact_database_url(database_url: str | None) -> str:
    if not database_url:
        return ""
    try:
        parsed = urlsplit(database_url)
    except ValueError:
        return "<redacted-database-url>"
    if not parsed.scheme:
        return "<redacted-database-url>"

    userinfo = ""
    if parsed.username:
        userinfo = quote(unquote(parsed.username), safe="") + ":<redacted>@"
    elif "@" in parsed.netloc:
        userinfo = "<redacted>@"

    host = parsed.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = f":{parsed.port}" if parsed.port is not None else ""
    query = _redact_query(parsed.query)
    netloc = f"{userinfo}{host}{port}"
    if not parsed.netloc:
        return _urlunsplit_preserving_empty_netloc(parsed.scheme, "", parsed.path, query)
    return urlunsplit((parsed.scheme, netloc, parsed.path, query, ""))


def load_source_fixture(path: Path, *, repo_root: Path | None = None) -> SourceFixture:
    fixture_path = path if path.is_absolute() else (repo_root or _repo_root()) / path
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    platform = str(payload.get("platform", "telegram")).strip().lower()
    if platform != "telegram":
        raise ValueError("fixture_platform_unsupported")
    return SourceFixture(
        source_message_id=UUID(str(payload["source_message_id"])),
        source_version_no=int(payload["source_version_no"]),
        platform=platform,
        chat_id=int(payload["chat_id"]),
        message_id=int(payload["message_id"]),
        text_body=_optional_str(payload.get("text_body")),
        caption_text=_optional_str(payload.get("caption_text")),
        text_surface=_optional_str(payload.get("text_surface")),
        entities_json=_json_list(payload.get("entities_json")),
        url_surface_json=_json_list(payload.get("url_surface_json")),
        posted_at=_parse_datetime(str(payload["posted_at"])),
        raw_message_json=_json_dict(payload.get("raw_message_json")),
    )


def build_normalizer_version(replay_namespace: str) -> str:
    return f"{NORMALIZER_VERSION_PREFIX}:{replay_namespace}"


def build_source_event_dedupe_key(fixture: SourceFixture, replay_namespace: str) -> str:
    return (
        f"local-db-source-candidate:{replay_namespace}:"
        f"{SOURCE_EVENT_TYPE}:{fixture.source_message_id}:{fixture.source_version_no}"
    )


def build_enrich_requested_dedupe_key(
    *,
    replay_namespace: str,
    candidate_group_id: UUID,
    canonical_id: str,
    source_message_id: UUID,
    source_version_no: int,
) -> str:
    return (
        f"local-db-source-candidate:{replay_namespace}:artifact.enrich:"
        f"{candidate_group_id}:{canonical_id}:{source_message_id}:{source_version_no}"
    )


async def _execute_db_replay(
    *,
    database_url: str,
    fixture: SourceFixture,
    replay_namespace: str,
) -> ReplayExecutionResult:
    _bootstrap_repo_imports()
    import sqlalchemy as sa
    from sqlalchemy.ext.asyncio import create_async_engine

    from services.router_normalizer.config import RouterNormalizerConfig
    from services.router_normalizer.models import RedisNormalizeMessage
    from services.router_normalizer.service import RouterNormalizerService

    normalizer_version = build_normalizer_version(replay_namespace)
    engine = create_async_engine(database_url, future=True)
    try:
        async with engine.begin() as connection:
            await _upsert_source_message(connection, fixture=fixture, replay_namespace=replay_namespace)
            await _insert_source_message_version_if_absent(connection, fixture=fixture)
            trigger_event_id = await _insert_source_outbox_if_absent(
                connection,
                fixture=fixture,
                replay_namespace=replay_namespace,
            )
            repository = _NamespacedRouterNormalizerRepository(connection, replay_namespace=replay_namespace)
            service = RouterNormalizerService(
                RouterNormalizerConfig(
                    app_env="test",
                    database_url=database_url,
                    redis_url="redis://disabled-local-replay",
                    queue_name="q.source.normalize",
                    consumer_group="router-normalizer",
                    consumer_name="local-db-source-candidate-replay",
                    block_ms=100,
                    batch_size=1,
                    normalizer_version=normalizer_version,
                    short_url_allowlist=(),
                    short_url_hop_limit=1,
                    short_url_timeout_seconds=0.1,
                    log_level="INFO",
                ),
                repository=repository,
                short_url_resolver=_NoNetworkShortUrlResolver(),
            )
            result = await service.process_stream_message(
                RedisNormalizeMessage(
                    job_id=str(trigger_event_id),
                    stage_name="normalize",
                    root_object_type="source_message",
                    root_object_id=str(fixture.source_message_id),
                    idempotency_key=build_source_event_dedupe_key(fixture, replay_namespace),
                    trigger_event_id=str(trigger_event_id),
                )
            )
            verification = await _verify_durable_rows(
                connection,
                fixture=fixture,
                replay_namespace=replay_namespace,
                normalizer_version=normalizer_version,
            )
            failures = list(verification.checks_failed)
            if not result.candidate_eligible:
                failures.append("router_normalizer_candidate_not_eligible")
            if result.candidate_group_count < 1:
                failures.append("router_normalizer_candidate_group_missing")
            return ReplayExecutionResult(
                source_message_upserted=verification.source_message_upserted,
                source_version_upserted=verification.source_version_upserted,
                source_outbox_event_created=verification.source_outbox_event_created,
                normalization_run_created=verification.normalization_run_created,
                artifact_created=verification.artifact_created,
                artifact_observation_created=verification.artifact_observation_created,
                candidate_group_created=verification.candidate_group_created,
                candidate_member_created=verification.candidate_member_created,
                enrich_requested_event_created=verification.enrich_requested_event_created,
                checks_failed=tuple(dict.fromkeys(failures)),
            )
    finally:
        await engine.dispose()


class _NamespacedRouterNormalizerRepository:
    def __init__(self, session: Any, *, replay_namespace: str) -> None:
        _bootstrap_repo_imports()
        from services.router_normalizer.repositories import RouterNormalizerRepository

        self._delegate = RouterNormalizerRepository(session)
        self._session = session
        self._replay_namespace = replay_namespace

    async def get_outbox_event(self, event_id: UUID) -> Any:
        return await self._delegate.get_outbox_event(event_id)

    async def get_current_source_message(self, source_message_id: UUID) -> Any:
        return await self._delegate.get_current_source_message(source_message_id)

    async def get_source_message_version(self, *, source_message_id: UUID, version_no: int) -> Any:
        return await self._delegate.get_source_message_version(
            source_message_id=source_message_id,
            version_no=version_no,
        )

    async def upsert_normalization_run(self, **kwargs: Any) -> UUID:
        return await self._delegate.upsert_normalization_run(**kwargs)

    async def insert_suppression_trace(self, **kwargs: Any) -> None:
        await self._delegate.insert_suppression_trace(**kwargs)

    async def upsert_artifact_registry(self, artifact: Any) -> UUID:
        return await self._delegate.upsert_artifact_registry(artifact)

    async def insert_artifact_observation_if_absent(self, **kwargs: Any) -> None:
        await self._delegate.insert_artifact_observation_if_absent(**kwargs)

    async def upsert_candidate_group(self, **kwargs: Any) -> UUID:
        return await self._delegate.upsert_candidate_group(**kwargs)

    async def upsert_candidate_member(self, **kwargs: Any) -> None:
        await self._delegate.upsert_candidate_member(**kwargs)

    async def insert_enrichment_requested_outbox(
        self,
        *,
        candidate_group_id: UUID,
        artifact_id: UUID,
        artifact: Any,
        source_message_id: UUID,
        source_version_no: int,
    ) -> None:
        if artifact.provider_route is None:
            return
        import sqlalchemy as sa

        dedupe_key = build_enrich_requested_dedupe_key(
            replay_namespace=self._replay_namespace,
            candidate_group_id=candidate_group_id,
            canonical_id=artifact.canonical_id,
            source_message_id=source_message_id,
            source_version_no=source_version_no,
        )
        payload = {
            "candidate_group_id": str(candidate_group_id),
            "artifact_id": str(artifact_id),
            "artifact_type": artifact.artifact_type,
            "canonical_id": artifact.canonical_id,
            "provider_route": artifact.provider_route,
            "refresh_mode": "standard",
            "depth_budget": 1,
            "source_message_id": str(source_message_id),
            "source_version_no": source_version_no,
        }
        await self._session.execute(
            sa.text(
                """
                INSERT INTO event_outbox (
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    dedupe_key,
                    payload_json,
                    status,
                    created_at
                )
                VALUES (
                    :event_type,
                    'artifact',
                    CAST(:artifact_id AS uuid),
                    :dedupe_key,
                    CAST(:payload_json AS jsonb),
                    'pending'::outbox_status_enum,
                    now()
                )
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            {
                "event_type": ENRICH_EVENT_TYPE,
                "artifact_id": str(artifact_id),
                "dedupe_key": dedupe_key,
                "payload_json": _json_dumps(payload),
            },
        )


class _NoNetworkShortUrlResolver:
    async def resolve(self, url: Any) -> Any:
        _bootstrap_repo_imports()
        from services.router_normalizer.models import ResolvedUrl

        normalized = _strip_fragment(url.observed_url)
        return ResolvedUrl(
            observed_url=url.observed_url,
            normalized_url=normalized,
            resolved_url=None,
            source_kind=url.source_kind,
            context_path=url.context_path,
            resolution_status="not_short_url",
        )


async def _upsert_source_message(connection: Any, *, fixture: SourceFixture, replay_namespace: str) -> None:
    import sqlalchemy as sa

    await connection.execute(
        sa.text(
            """
            INSERT INTO source_messages (
                source_message_id,
                platform,
                chat_id,
                message_id,
                logical_post_key,
                is_channel_post,
                posted_at,
                current_version_no,
                content_type,
                text_body,
                caption_text,
                text_surface,
                entities_json,
                url_surface_json,
                raw_message_json,
                first_seen_at,
                last_seen_at
            )
            VALUES (
                CAST(:source_message_id AS uuid),
                :platform,
                :chat_id,
                :message_id,
                :logical_post_key,
                true,
                :posted_at,
                :current_version_no,
                'text',
                :text_body,
                :caption_text,
                :text_surface,
                CAST(:entities_json AS jsonb),
                CAST(:url_surface_json AS jsonb),
                CAST(:raw_message_json AS jsonb),
                now(),
                now()
            )
            ON CONFLICT (source_message_id)
            DO UPDATE SET
                current_version_no = EXCLUDED.current_version_no,
                logical_post_key = EXCLUDED.logical_post_key,
                text_body = EXCLUDED.text_body,
                caption_text = EXCLUDED.caption_text,
                text_surface = EXCLUDED.text_surface,
                entities_json = EXCLUDED.entities_json,
                url_surface_json = EXCLUDED.url_surface_json,
                raw_message_json = EXCLUDED.raw_message_json,
                last_seen_at = now()
            """
        ),
        {
            "source_message_id": str(fixture.source_message_id),
            "platform": fixture.platform,
            "chat_id": fixture.chat_id,
            "message_id": fixture.message_id,
            "logical_post_key": _logical_post_key(fixture, replay_namespace),
            "posted_at": fixture.posted_at,
            "current_version_no": fixture.source_version_no,
            "text_body": fixture.text_body,
            "caption_text": fixture.caption_text,
            "text_surface": fixture.text_surface,
            "entities_json": _json_dumps(fixture.entities_json),
            "url_surface_json": _json_dumps(fixture.url_surface_json),
            "raw_message_json": _json_dumps(fixture.raw_message_json),
        },
    )


async def _insert_source_message_version_if_absent(connection: Any, *, fixture: SourceFixture) -> None:
    import sqlalchemy as sa

    await connection.execute(
        sa.text(
            """
            INSERT INTO source_message_versions (
                source_message_id,
                version_no,
                version_reason,
                observed_at,
                telegram_edit_date,
                text_surface,
                entities_json,
                raw_message_json,
                content_hash
            )
            VALUES (
                CAST(:source_message_id AS uuid),
                :version_no,
                'new',
                now(),
                NULL,
                :text_surface,
                CAST(:entities_json AS jsonb),
                CAST(:raw_message_json AS jsonb),
                :content_hash
            )
            ON CONFLICT (source_message_id, version_no) DO NOTHING
            """
        ),
        {
            "source_message_id": str(fixture.source_message_id),
            "version_no": fixture.source_version_no,
            "text_surface": fixture.text_surface,
            "entities_json": _json_dumps(fixture.entities_json),
            "raw_message_json": _json_dumps(fixture.raw_message_json),
            "content_hash": _fixture_content_hash(fixture),
        },
    )


async def _insert_source_outbox_if_absent(
    connection: Any,
    *,
    fixture: SourceFixture,
    replay_namespace: str,
) -> UUID:
    import sqlalchemy as sa

    payload = {
        "source_message_id": str(fixture.source_message_id),
        "current_version_no": fixture.source_version_no,
        "logical_post_key": _logical_post_key(fixture, replay_namespace),
        "occurred_at": _json_default(fixture.posted_at),
    }
    dedupe_key = build_source_event_dedupe_key(fixture, replay_namespace)
    await connection.execute(
        sa.text(
            """
            INSERT INTO event_outbox (
                event_type,
                aggregate_type,
                aggregate_id,
                dedupe_key,
                payload_json,
                status,
                created_at
            )
            VALUES (
                :event_type,
                'source_message',
                CAST(:source_message_id AS uuid),
                :dedupe_key,
                CAST(:payload_json AS jsonb),
                'pending'::outbox_status_enum,
                now()
            )
            ON CONFLICT (dedupe_key) DO NOTHING
            """
        ),
        {
            "event_type": SOURCE_EVENT_TYPE,
            "source_message_id": str(fixture.source_message_id),
            "dedupe_key": dedupe_key,
            "payload_json": _json_dumps(payload),
        },
    )
    result = await connection.execute(
        sa.text(
            """
            SELECT event_id
            FROM event_outbox
            WHERE dedupe_key = :dedupe_key
              AND event_type = :event_type
              AND aggregate_type = 'source_message'
              AND aggregate_id = CAST(:source_message_id AS uuid)
            """
        ),
        {
            "dedupe_key": dedupe_key,
            "event_type": SOURCE_EVENT_TYPE,
            "source_message_id": str(fixture.source_message_id),
        },
    )
    event_id = result.scalar_one_or_none()
    if event_id is None:
        raise RuntimeError("source_outbox_event_missing")
    return event_id


async def _verify_durable_rows(
    connection: Any,
    *,
    fixture: SourceFixture,
    replay_namespace: str,
    normalizer_version: str,
) -> ReplayExecutionResult:
    import sqlalchemy as sa

    source_message = await _exists(
        connection,
        """
        SELECT 1
        FROM source_messages
        WHERE source_message_id = CAST(:source_message_id AS uuid)
          AND current_version_no = :source_version_no
        """,
        {
            "source_message_id": str(fixture.source_message_id),
            "source_version_no": fixture.source_version_no,
        },
    )
    source_version = await _exists(
        connection,
        """
        SELECT 1
        FROM source_message_versions
        WHERE source_message_id = CAST(:source_message_id AS uuid)
          AND version_no = :source_version_no
          AND content_hash = :content_hash
        """,
        {
            "source_message_id": str(fixture.source_message_id),
            "source_version_no": fixture.source_version_no,
            "content_hash": _fixture_content_hash(fixture),
        },
    )
    source_event = await _exists(
        connection,
        """
        SELECT 1
        FROM event_outbox
        WHERE event_type = :event_type
          AND dedupe_key = :dedupe_key
          AND aggregate_id = CAST(:source_message_id AS uuid)
        """,
        {
            "event_type": SOURCE_EVENT_TYPE,
            "dedupe_key": build_source_event_dedupe_key(fixture, replay_namespace),
            "source_message_id": str(fixture.source_message_id),
        },
    )
    normalization_run = await _exists(
        connection,
        """
        SELECT 1
        FROM normalization_runs
        WHERE source_message_id = CAST(:source_message_id AS uuid)
          AND source_version_no = :source_version_no
          AND normalizer_version = :normalizer_version
          AND candidate_eligible IS TRUE
        """,
        {
            "source_message_id": str(fixture.source_message_id),
            "source_version_no": fixture.source_version_no,
            "normalizer_version": normalizer_version,
        },
    )
    artifact = await _exists(
        connection,
        """
        SELECT 1
        FROM artifact_registry
        WHERE canonical_id = 'github:repo:example/example-tool'
        """,
        {},
    )
    observation = await _exists(
        connection,
        """
        SELECT 1
        FROM artifact_observations
        WHERE source_message_id = CAST(:source_message_id AS uuid)
          AND source_version_no = :source_version_no
        """,
        {
            "source_message_id": str(fixture.source_message_id),
            "source_version_no": fixture.source_version_no,
        },
    )
    candidate_group = await _exists(
        connection,
        """
        SELECT 1
        FROM candidate_group_proposals
        WHERE source_message_id = CAST(:source_message_id AS uuid)
          AND source_version_no = :source_version_no
          AND dedupe_subject_key = 'github:repo:example/example-tool'
        """,
        {
            "source_message_id": str(fixture.source_message_id),
            "source_version_no": fixture.source_version_no,
        },
    )
    member = await _exists(
        connection,
        """
        SELECT 1
        FROM candidate_group_members AS cgm
        JOIN candidate_group_proposals AS cgp
          ON cgp.candidate_group_id = cgm.candidate_group_id
        WHERE cgp.source_message_id = CAST(:source_message_id AS uuid)
          AND cgp.source_version_no = :source_version_no
          AND cgm.member_role = 'primary'
        """,
        {
            "source_message_id": str(fixture.source_message_id),
            "source_version_no": fixture.source_version_no,
        },
    )
    enrich_event = await _exists(
        connection,
        """
        SELECT 1
        FROM event_outbox AS eo
        JOIN artifact_registry AS ar
          ON ar.artifact_id = eo.aggregate_id
        WHERE eo.event_type = :event_type
          AND eo.dedupe_key LIKE :dedupe_prefix
          AND ar.canonical_id = 'github:repo:example/example-tool'
        """,
        {
            "event_type": ENRICH_EVENT_TYPE,
            "dedupe_prefix": f"local-db-source-candidate:{replay_namespace}:artifact.enrich:%",
        },
    )
    result = ReplayExecutionResult(
        source_message_upserted=source_message,
        source_version_upserted=source_version,
        source_outbox_event_created=source_event,
        normalization_run_created=normalization_run,
        artifact_created=artifact,
        artifact_observation_created=observation,
        candidate_group_created=candidate_group,
        candidate_member_created=member,
        enrich_requested_event_created=enrich_event,
    )
    failures = [
        key.removesuffix("_upserted").removesuffix("_created") + "_missing"
        for key in (
            "source_message_upserted",
            "source_version_upserted",
            "source_outbox_event_created",
            "normalization_run_created",
            "artifact_created",
            "artifact_observation_created",
            "candidate_group_created",
            "candidate_member_created",
            "enrich_requested_event_created",
        )
        if getattr(result, key) is not True
    ]
    return ReplayExecutionResult(
        source_message_upserted=result.source_message_upserted,
        source_version_upserted=result.source_version_upserted,
        source_outbox_event_created=result.source_outbox_event_created,
        normalization_run_created=result.normalization_run_created,
        artifact_created=result.artifact_created,
        artifact_observation_created=result.artifact_observation_created,
        candidate_group_created=result.candidate_group_created,
        candidate_member_created=result.candidate_member_created,
        enrich_requested_event_created=result.enrich_requested_event_created,
        checks_failed=tuple(failures),
    )


async def _exists(connection: Any, sql: str, params: dict[str, Any]) -> bool:
    import sqlalchemy as sa

    result = await connection.execute(sa.text(sql), params)
    return result.first() is not None


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "fixture_loaded": False,
        "database_url_guard_passed": False,
        "production_db_write": False,
        "source_message_upserted": False,
        "source_version_upserted": False,
        "source_outbox_event_created": False,
        "normalization_run_created": False,
        "artifact_created": False,
        "artifact_observation_created": False,
        "candidate_group_created": False,
        "candidate_member_created": False,
        "enrich_requested_event_created": False,
        "live_telegram_called": False,
        "openai_called": False,
        "workers_started": False,
        "redis_mutation": False,
        "checks_failed": [],
    }


def _finish(report: dict[str, Any], checks_failed: Sequence[str]) -> RunnerResult:
    normalized_failures = list(dict.fromkeys(checks_failed))
    report["checks_failed"] = normalized_failures
    report["status"] = "fail" if normalized_failures else "pass"
    return RunnerResult(exit_code=0 if report["status"] == "pass" else 1, report=report)


def _fixture_content_hash(fixture: SourceFixture) -> str:
    payload = {
        "text_surface": fixture.text_surface,
        "entities_json": fixture.entities_json,
        "raw_message_json": fixture.raw_message_json,
    }
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def _logical_post_key(fixture: SourceFixture, replay_namespace: str) -> str:
    return f"{replay_namespace}:telegram:{fixture.chat_id}:{fixture.message_id}"


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def _optional_str(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None


def _json_list(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("fixture_json_list_required")
    return [item for item in value if isinstance(item, dict)]


def _json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("fixture_json_dict_required")
    return value


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _query_host(database_url: str) -> str | None:
    query = urlsplit(database_url).query
    for key, value in parse_qsl(query, keep_blank_values=True):
        if key.lower() == "host" and value:
            return value
    return None


def _query_host_is_local(query_host: str) -> bool:
    normalized = query_host.strip().lower()
    return normalized.startswith("/") or normalized in LOCAL_HOSTS


def _redact_query(query: str) -> str:
    if not query:
        return ""
    redacted_parts: list[str] = []
    for part in query.split("&"):
        key = part.split("=", 1)[0]
        if key.lower() in {"password", "pass", "sslpassword"}:
            redacted_parts.append(f"{key}=<redacted>")
        else:
            redacted_parts.append(part)
    return "&".join(redacted_parts)


def _urlunsplit_preserving_empty_netloc(scheme: str, netloc: str, path: str, query: str) -> str:
    if netloc:
        return urlunsplit((scheme, netloc, path, query, ""))
    normalized_path = path if path.startswith("/") else f"/{path}"
    suffix = f"?{query}" if query else ""
    return f"{scheme}://{normalized_path}{suffix}"


def _strip_fragment(url: str) -> str:
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc)
    if message in {"source_outbox_event_missing"}:
        return message
    return exc.__class__.__name__


def _bootstrap_repo_imports() -> None:
    repo_root = _repo_root()
    src_root = repo_root / "src"
    for path in (repo_root, src_root):
        value = str(path)
        if value not in sys.path:
            sys.path.insert(0, value)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
