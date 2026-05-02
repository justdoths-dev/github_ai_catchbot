from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, unquote, urlsplit
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.gh_enricher.config import GhEnricherConfig
from src.services.gh_enricher.fetch_planner import GitHubFetchPlanner
from src.services.gh_enricher.file_sampler import GitHubFileSampler
from src.services.gh_enricher.redis_streams import RedisStreamConsumer
from src.services.gh_enricher.repositories import GhEnricherRepository
from src.services.gh_enricher.service import GhEnricherService
from src.services.gh_enricher.url_discovery import GitHubUrlDiscovery
from src.services.gh_enricher.worker import GhEnricherWorker
from src.services.outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from src.services.outbox_relay.redis_streams import RedisStreamsPublisher
from src.services.outbox_relay.repositories import OutboxRelayRepository
from src.services.outbox_relay.routing import OutboxRouteResolver


REPORT_TYPE = "gh_enricher_runtime_smoke_v1"
DATABASE_URL_ENV = "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL"
REDIS_URL_ENV = "REDIS_URL"
MUTATION_SAFETY = (
    "controlled smoke write only: inserts marker-scoped synthetic source, artifact, "
    "candidate, and artifact.enrich.requested.v1 rows; publishes that event through "
    "the outbox relay route to q.artifact.enrich.github; runs one gh-enricher worker "
    "pass with a deterministic fake GitHub client; and leaves controlled enrichment "
    "output rows plus one pending artifact.snapshot.updated.v1 outbox row"
)
SMOKE_MARKER_PREFIX = "ops-smoke:gh-enricher-runtime:"
EXPECTED_QUEUE_NAME = "q.artifact.enrich.github"
EXPECTED_REDIS_DB = 14
EXPECTED_STAGE_NAME = "enrich_github"
EXPECTED_ARTIFACT_TYPE = "github_repo"
EXPECTED_PROVIDER = "github"
EXPECTED_AGGREGATE_TYPE = "artifact"
EXPECTED_EVENT_TYPE = "artifact.enrich.requested.v1"
EXPECTED_DOWNSTREAM_EVENT_TYPE = "artifact.snapshot.updated.v1"
EXPECTED_OWNER = "octocat"
EXPECTED_REPO_PREFIX = "hello-world-smoke"
EXPECTED_CANONICAL_ID_PREFIX = f"github:repo:{EXPECTED_OWNER}/{EXPECTED_REPO_PREFIX}"
GH_ENRICHER_VERSION = "gh-enricher-runtime-smoke"
REQUIRED_REDIS_FIELDS = {
    "job_id",
    "stage_name",
    "root_object_type",
    "root_object_id",
    "idempotency_key",
    "pipeline_run_id",
    "not_before",
    "trigger_event_id",
}
FORBIDDEN_REDIS_FIELDS = {
    "payload_json",
    "candidate_group_id",
    "artifact_id",
    "artifact_type",
    "provider_route",
    "refresh_mode",
    "depth_budget",
    "database_url",
    "redis_url",
    "password",
    "token",
    "secret",
    "api_key",
}


@dataclass(slots=True)
class SmokeReport:
    report_type: str
    smoke_id: str
    marker: str
    checks_run: list[str]
    checks_passed: list[str]
    checks_failed: list[str]
    failures: list[dict[str, str]]
    warnings: list[str]
    database_url_redacted: bool
    redis_url_redacted: bool
    mutation_safety: str
    queue_name: str
    redis_stream_message_id: str | None
    seeded_ids: dict[str, str]
    resulting_snapshot_ids: list[str]
    downstream_outbox_ids: list[str]
    fake_github_calls: list[str]

    def failed(self) -> bool:
        return bool(self.checks_failed)


@dataclass(slots=True, frozen=True)
class SmokeSeedIds:
    source_message_id: UUID
    artifact_id: UUID
    candidate_group_id: UUID
    enrich_event_id: UUID


@dataclass(slots=True, frozen=True)
class SmokeSeedShape:
    smoke_id: str
    marker: str
    repo_name: str
    canonical_id: str
    canonical_url: str
    owner: str = EXPECTED_OWNER


class FakeGitHubClient:
    """Deterministic gh-enricher client fixture; it never performs network I/O."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.external_network_calls_attempted = False

    async def get_repo(self, owner: str, repo: str, *, auth_mode: str) -> dict[str, Any]:
        self.calls.append(f"get_repo:{owner}/{repo}:{auth_mode}")
        return {
            "full_name": f"{owner}/{repo}",
            "default_branch": "main",
            "description": "Deterministic runtime smoke repository fixture",
            "homepage": "https://example.com/gh-enricher-smoke",
            "license": {"spdx_id": "MIT"},
            "topics": ["ai", "runtime-smoke"],
            "language": "Python",
            "stargazers_count": 42,
            "subscribers_count": 7,
            "forks_count": 3,
            "open_issues_count": 1,
            "archived": False,
            "fork": False,
            "is_template": False,
            "pushed_at": "2026-05-02T00:00:00Z",
        }

    async def get_default_branch_head(self, owner: str, repo: str, default_branch: str, *, auth_mode: str) -> dict[str, Any]:
        self.calls.append(f"get_default_branch_head:{owner}/{repo}:{default_branch}:{auth_mode}")
        return {"sha": "0123456789abcdef0123456789abcdef01234567"}

    async def get_tree(self, owner: str, repo: str, ref: str, *, recursive: bool, auth_mode: str) -> dict[str, Any]:
        self.calls.append(f"get_tree:{owner}/{repo}:{ref}:{recursive}:{auth_mode}")
        return {
            "truncated": False,
            "tree": [
                {"type": "blob", "path": "README.md"},
                {"type": "blob", "path": "pyproject.toml"},
                {"type": "blob", "path": ".github/workflows/ci.yml"},
                {"type": "blob", "path": "tests/test_runtime.py"},
                {"type": "blob", "path": "docs/usage.md"},
            ],
        }

    async def get_contents(
        self,
        owner: str,
        repo: str,
        path: str,
        *,
        ref: str | None,
        auth_mode: str,
    ) -> dict[str, Any]:
        self.calls.append(f"get_contents:{owner}/{repo}:{path}:{ref}:{auth_mode}")
        text_by_path = {
            "README.md": (
                "# Runtime Smoke Fixture\n\n"
                "Deterministic README evidence for gh-enricher. "
                "See https://example.com/runtime-smoke/readme for details.\n"
            ),
            "pyproject.toml": "[project]\nname = \"gh-enricher-runtime-smoke\"\n",
            ".github/workflows/ci.yml": "name: ci\non: [push]\n",
            "tests/test_runtime.py": "def test_runtime_smoke_fixture():\n    assert True\n",
            "docs/usage.md": "Usage docs at https://docs.example.com/runtime-smoke/start\n",
        }
        text = text_by_path.get(path, f"{path}\n")
        return {
            "encoding": "base64",
            "content": base64.b64encode(text.encode("utf-8")).decode("ascii"),
            "size": len(text.encode("utf-8")),
        }

    async def get_releases(self, owner: str, repo: str, *, auth_mode: str) -> list[dict[str, Any]]:
        self.calls.append(f"get_releases:{owner}/{repo}:{auth_mode}")
        return [
            {
                "published_at": "2026-05-01T00:00:00Z",
                "assets": [{"name": "fixture.tar.gz", "download_count": 5}],
                "prerelease": False,
            }
        ]

    async def get_gist(self, gist_id: str, *, auth_mode: str) -> dict[str, Any]:
        self.calls.append(f"get_gist:{gist_id}:{auth_mode}")
        raise AssertionError("gh-enricher runtime smoke seeds a github_repo, not a gist")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run an opt-in post-Stage44 gh-enricher runtime smoke. "
            f"The database URL is read from {DATABASE_URL_ENV}; Redis uses --redis-url or {REDIS_URL_ENV}."
        )
    )
    parser.add_argument("--redis-url", default=None, help=f"Smoke Redis URL. Defaults to ${REDIS_URL_ENV}.")
    parser.add_argument("--confirm", choices=["write"], required=True)
    return parser


def _build_seed_shape(smoke_id: str | None = None) -> SmokeSeedShape:
    smoke_id = smoke_id or uuid4().hex
    short = smoke_id[:12]
    marker = f"{SMOKE_MARKER_PREFIX}{smoke_id}"
    repo_name = f"{EXPECTED_REPO_PREFIX}-{short}"
    return SmokeSeedShape(
        smoke_id=smoke_id,
        marker=marker,
        repo_name=repo_name,
        canonical_id=f"github:repo:{EXPECTED_OWNER}/{repo_name}",
        canonical_url=f"https://github.com/{EXPECTED_OWNER}/{repo_name}",
    )


def _new_report(seed_shape: SmokeSeedShape | None = None) -> SmokeReport:
    seed_shape = seed_shape or _build_seed_shape("0" * 32)
    return SmokeReport(
        report_type=REPORT_TYPE,
        smoke_id=seed_shape.smoke_id,
        marker=seed_shape.marker,
        checks_run=[],
        checks_passed=[],
        checks_failed=[],
        failures=[],
        warnings=[
            "This smoke must use a dev/test PostgreSQL database and local Redis DB 14.",
            "It uses a deterministic fake GitHub client and does not require GitHub App credentials.",
            "It does not start live Telegram collector or notifier transport.",
            "A successful run leaves controlled marker-scoped DB rows and an acked Redis Stream entry.",
        ],
        database_url_redacted=True,
        redis_url_redacted=True,
        mutation_safety=MUTATION_SAFETY,
        queue_name=EXPECTED_QUEUE_NAME,
        redis_stream_message_id=None,
        seeded_ids={},
        resulting_snapshot_ids=[],
        downstream_outbox_ids=[],
        fake_github_calls=[],
    )


def _mark_pass(report: SmokeReport, check_name: str) -> None:
    report.checks_run.append(check_name)
    report.checks_passed.append(check_name)


def _mark_fail(
    report: SmokeReport,
    check_name: str,
    message: str,
    *,
    database_url: str,
    redis_url: str,
) -> None:
    report.checks_run.append(check_name)
    report.checks_failed.append(check_name)
    report.failures.append(
        {
            "check": check_name,
            "message": _redact_sensitive_text(message, database_url=database_url, redis_url=redis_url),
        }
    )


def _url_parts_to_redact(url: str) -> set[str]:
    parts: set[str] = set()
    if not url:
        return parts
    parts.add(url)
    try:
        parsed = urlsplit(url)
    except ValueError:
        return parts
    if parsed.username:
        parts.update({parsed.username, unquote(parsed.username), quote(unquote(parsed.username))})
    if parsed.password:
        parts.update({parsed.password, unquote(parsed.password), quote(unquote(parsed.password))})
    if parsed.username and parsed.password:
        parts.update(
            {
                f"{parsed.username}:{parsed.password}@",
                f"{unquote(parsed.username)}:{unquote(parsed.password)}@",
                f"{quote(unquote(parsed.username))}:{quote(unquote(parsed.password))}@",
            }
        )
    if parsed.scheme and parsed.netloc:
        parts.add(f"{parsed.scheme}://{parsed.netloc}")
    return {part for part in parts if part}


def _redact_sensitive_text(text: str, *, database_url: str, redis_url: str) -> str:
    redacted = text
    for value in sorted(_url_parts_to_redact(database_url) | _url_parts_to_redact(redis_url), key=len, reverse=True):
        redacted = redacted.replace(value, "<redacted-url-fragment>")
    redacted = re.sub(
        r"(?i)\b(password|passwd|pwd|token|secret|api[_-]?key)\s*=\s*[^,\s;]+",
        r"\1=<redacted-credential>",
        redacted,
    )
    redacted = re.sub(
        r"([a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s:@]+:[^@\s/]+@",
        r"\1<redacted-credential>@",
        redacted,
    )
    return redacted


def _is_production_like_url(url: str) -> bool:
    lowered = url.lower()
    if any(token in lowered for token in ("prod", "production")):
        return True
    try:
        parsed = urlsplit(url)
    except ValueError:
        return True
    return parsed.scheme in {"rediss"}


def _is_expected_smoke_database_url(database_url: str) -> bool:
    try:
        parsed = urlsplit(database_url)
    except ValueError:
        return False
    if not parsed.scheme.startswith("postgresql"):
        return False
    host = (parsed.hostname or "").lower()
    if host not in {"localhost", "127.0.0.1", "::1"}:
        return False
    database_name = unquote((parsed.path or "").rsplit("/", 1)[-1]).lower()
    return any(marker in database_name for marker in ("smoke", "test", "dev"))


def _is_expected_redis_db14(redis_url: str) -> bool:
    try:
        parsed = urlsplit(redis_url)
    except ValueError:
        return False
    if parsed.scheme != "redis":
        return False
    host = (parsed.hostname or "").lower()
    if host not in {"localhost", "127.0.0.1", "::1"}:
        return False
    return (parsed.path or "").rstrip("/") == f"/{EXPECTED_REDIS_DB}"


def _is_forbidden_redis_field(field_name: str) -> bool:
    lowered = field_name.lower()
    if lowered in FORBIDDEN_REDIS_FIELDS:
        return True
    return any(token in lowered for token in ("password", "token", "secret", "api_key", "apikey"))


def validate_redis_payload(
    fields: Mapping[str, Any],
    *,
    expected_event_id: UUID,
    expected_artifact_id: UUID,
    database_url: str,
    redis_url: str,
) -> list[str]:
    failures: list[str] = []
    keys = set(fields)
    missing = sorted(REQUIRED_REDIS_FIELDS - keys)
    extra = sorted(keys - REQUIRED_REDIS_FIELDS)
    extra_forbidden = sorted(key for key in keys if _is_forbidden_redis_field(key))
    if missing:
        failures.append(f"missing required Redis fields: {', '.join(missing)}")
    if extra:
        failures.append(f"unexpected Redis fields present: {', '.join(extra)}")
    if extra_forbidden:
        failures.append(f"forbidden Redis fields present: {', '.join(extra_forbidden)}")

    expected_values = {
        "job_id": str(expected_event_id),
        "stage_name": EXPECTED_STAGE_NAME,
        "root_object_type": EXPECTED_AGGREGATE_TYPE,
        "root_object_id": str(expected_artifact_id),
        "trigger_event_id": str(expected_event_id),
    }
    for key, expected in expected_values.items():
        actual = str(fields.get(key, ""))
        if actual != expected:
            failures.append(f"Redis field {key} expected {expected!r}, got {actual!r}")

    flattened = json.dumps({str(k): str(v) for k, v in fields.items()}, sort_keys=True)
    sanitized = _redact_sensitive_text(flattened, database_url=database_url, redis_url=redis_url)
    if sanitized != flattened:
        failures.append("Redis payload contains URL or credential fragments")
    return failures


def _build_expected_redis_payload(*, event_id: UUID, artifact_id: UUID, marker: str) -> dict[str, str]:
    return {
        "job_id": str(event_id),
        "stage_name": EXPECTED_STAGE_NAME,
        "root_object_type": EXPECTED_AGGREGATE_TYPE,
        "root_object_id": str(artifact_id),
        "idempotency_key": f"{marker}:artifact-enrich-requested",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
    }


async def _select_scalar(session: AsyncSession, statement: str, params: dict[str, Any] | None = None) -> Any:
    result = await session.execute(sa.text(statement), params or {})
    return result.scalar_one_or_none()


async def _ensure_consumer_group_at_new_messages(redis_client: Any, *, queue_name: str, consumer_group: str) -> None:
    try:
        await redis_client.xgroup_create(queue_name, consumer_group, id="$", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def _insert_seed_rows(session: AsyncSession, *, seed_shape: SmokeSeedShape, seed_ids: SmokeSeedIds) -> None:
    now = datetime.now(UTC)
    chat_id = -int(str(seed_ids.source_message_id.int)[-12:])
    message_id = int(str(seed_ids.enrich_event_id.int)[-9:]) or 1
    text = f"Runtime smoke for {seed_shape.canonical_url}"
    raw_message_json = {
        "smoke_marker": seed_shape.marker,
        "source": "gh_enricher_runtime_smoke",
        "external_network_calls_allowed": False,
    }
    content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    await session.execute(
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
                text_surface,
                entities_json,
                url_surface_json,
                raw_message_json,
                first_seen_at,
                last_seen_at
            ) VALUES (
                CAST(:source_message_id AS uuid),
                'telegram',
                :chat_id,
                :message_id,
                :logical_post_key,
                true,
                :posted_at,
                1,
                'text',
                :text_body,
                :text_surface,
                CAST(:entities_json AS jsonb),
                CAST(:url_surface_json AS jsonb),
                CAST(:raw_message_json AS jsonb),
                :first_seen_at,
                :last_seen_at
            )
            """
        ),
        {
            "source_message_id": str(seed_ids.source_message_id),
            "chat_id": chat_id,
            "message_id": message_id,
            "logical_post_key": seed_shape.marker,
            "posted_at": now,
            "text_body": text,
            "text_surface": text,
            "entities_json": json.dumps([], sort_keys=True),
            "url_surface_json": json.dumps([], sort_keys=True),
            "raw_message_json": json.dumps(raw_message_json, sort_keys=True),
            "first_seen_at": now,
            "last_seen_at": now,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO source_message_versions (
                source_message_id,
                version_no,
                version_reason,
                observed_at,
                text_surface,
                entities_json,
                raw_message_json,
                content_hash
            ) VALUES (
                CAST(:source_message_id AS uuid),
                1,
                'new',
                :observed_at,
                :text_surface,
                CAST(:entities_json AS jsonb),
                CAST(:raw_message_json AS jsonb),
                :content_hash
            )
            """
        ),
        {
            "source_message_id": str(seed_ids.source_message_id),
            "observed_at": now,
            "text_surface": text,
            "entities_json": json.dumps([], sort_keys=True),
            "raw_message_json": json.dumps(raw_message_json, sort_keys=True),
            "content_hash": content_hash,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO artifact_registry (
                artifact_id,
                artifact_type,
                canonical_id,
                canonical_url,
                normalized_host,
                artifact_key_json,
                created_at,
                updated_at
            ) VALUES (
                CAST(:artifact_id AS uuid),
                CAST(:artifact_type AS artifact_type_enum),
                :canonical_id,
                :canonical_url,
                'github.com',
                CAST(:artifact_key_json AS jsonb),
                :created_at,
                :updated_at
            )
            """
        ),
        {
            "artifact_id": str(seed_ids.artifact_id),
            "artifact_type": EXPECTED_ARTIFACT_TYPE,
            "canonical_id": seed_shape.canonical_id,
            "canonical_url": seed_shape.canonical_url,
            "artifact_key_json": json.dumps(
                {"owner": seed_shape.owner, "repo": seed_shape.repo_name, "smoke_marker": seed_shape.marker},
                sort_keys=True,
            ),
            "created_at": now,
            "updated_at": now,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO candidate_group_proposals (
                candidate_group_id,
                source_message_id,
                source_version_no,
                initial_primary_artifact_id,
                current_primary_artifact_id,
                proposal_status,
                normalizer_version,
                dedupe_subject_key,
                created_at,
                updated_at
            ) VALUES (
                CAST(:candidate_group_id AS uuid),
                CAST(:source_message_id AS uuid),
                1,
                CAST(:artifact_id AS uuid),
                CAST(:artifact_id AS uuid),
                'proposed',
                :normalizer_version,
                :dedupe_subject_key,
                :created_at,
                :updated_at
            )
            """
        ),
        {
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "source_message_id": str(seed_ids.source_message_id),
            "artifact_id": str(seed_ids.artifact_id),
            "normalizer_version": GH_ENRICHER_VERSION,
            "dedupe_subject_key": seed_shape.canonical_id,
            "created_at": now,
            "updated_at": now,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO candidate_group_members (
                candidate_group_id,
                artifact_id,
                member_role,
                member_order,
                created_at
            ) VALUES (
                CAST(:candidate_group_id AS uuid),
                CAST(:artifact_id AS uuid),
                'primary',
                0,
                :created_at
            )
            """
        ),
        {
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "artifact_id": str(seed_ids.artifact_id),
            "created_at": now,
        },
    )
    payload = {
        "event_id": str(seed_ids.enrich_event_id),
        "candidate_group_id": str(seed_ids.candidate_group_id),
        "artifact_id": str(seed_ids.artifact_id),
        "artifact_type": EXPECTED_ARTIFACT_TYPE,
        "provider_route": EXPECTED_PROVIDER,
        "refresh_mode": "standard",
        "depth_budget": 1,
        "smoke_marker": seed_shape.marker,
    }
    await session.execute(
        sa.text(
            """
            INSERT INTO event_outbox (
                event_id,
                event_type,
                aggregate_type,
                aggregate_id,
                dedupe_key,
                payload_json,
                status,
                created_at
            ) VALUES (
                CAST(:event_id AS uuid),
                :event_type,
                :aggregate_type,
                CAST(:aggregate_id AS uuid),
                :dedupe_key,
                CAST(:payload_json AS jsonb),
                'pending'::outbox_status_enum,
                :created_at
            )
            """
        ),
        {
            "event_id": str(seed_ids.enrich_event_id),
            "event_type": EXPECTED_EVENT_TYPE,
            "aggregate_type": EXPECTED_AGGREGATE_TYPE,
            "aggregate_id": str(seed_ids.artifact_id),
            "dedupe_key": f"{seed_shape.marker}:artifact-enrich-requested",
            "payload_json": json.dumps(payload, sort_keys=True),
            "created_at": now,
        },
    )


async def _load_outbox_event_row(session: AsyncSession, event_id: UUID) -> OutboxEventRow:
    result = await session.execute(
        sa.text(
            """
            SELECT event_id, event_type, aggregate_type, aggregate_id, dedupe_key,
                   payload_json, status, fail_count, created_at
            FROM event_outbox
            WHERE event_id = CAST(:event_id AS uuid)
            """
        ),
        {"event_id": str(event_id)},
    )
    row = result.mappings().one()
    payload = row["payload_json"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return OutboxEventRow(
        event_id=UUID(str(row["event_id"])),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=UUID(str(row["aggregate_id"])),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload or {},
        status=str(row["status"]),
        fail_count=int(row["fail_count"]),
        created_at=row["created_at"],
    )


def _build_relay_stream_message(row: OutboxEventRow, route: QueueRoute) -> RedisQueuedMessage:
    return RedisQueuedMessage(
        job_id=str(row.event_id),
        stage_name=route.stage_name,
        root_object_type=row.aggregate_type,
        root_object_id=str(row.aggregate_id),
        idempotency_key=row.dedupe_key,
        pipeline_run_id=None,
        not_before=None,
        trigger_event_id=str(row.event_id),
    )


async def _publish_seed_event_through_outbox_route(
    session: AsyncSession,
    *,
    redis_client: Any,
    event_id: UUID,
) -> tuple[str, dict[str, str]]:
    row = await _load_outbox_event_row(session, event_id)
    route = OutboxRouteResolver().resolve(row)
    message = _build_relay_stream_message(row, route)
    publisher = RedisStreamsPublisher(redis_client)
    stream_message_id = await publisher.publish(route, message)
    repository = OutboxRelayRepository(session)
    await repository.mark_published(event_id=event_id, published_at=datetime.now(UTC))
    await repository.insert_job_attempt(
        stage_name=route.stage_name,
        queue_name=route.queue_name,
        root_object_type=row.aggregate_type,
        root_object_id=row.aggregate_id,
        attempt_status="succeeded",
        error_code=None,
    )
    return stream_message_id, message.as_stream_fields()


async def _load_stream_fields(redis_client: Any, *, queue_name: str, message_id: str) -> dict[str, str] | None:
    rows = await redis_client.xrange(queue_name, min=message_id, max=message_id, count=1)
    if not rows:
        return None
    _row_id, fields = rows[0]
    return {str(key): str(value) for key, value in fields.items()}


async def _load_outputs(session: AsyncSession, *, artifact_id: UUID) -> dict[str, Any]:
    outputs: dict[str, Any] = {}
    outputs["enrichment_run_count"] = int(
        await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM artifact_enrichment_runs
            WHERE artifact_id = CAST(:artifact_id AS uuid)
              AND provider = 'github'
            """,
            {"artifact_id": str(artifact_id)},
        )
        or 0
    )
    snapshot_result = await session.execute(
        sa.text(
            """
            SELECT snapshot_id, provider, snapshot_type, status, content_anchor
            FROM artifact_snapshots
            WHERE artifact_id = CAST(:artifact_id AS uuid)
              AND provider = 'github'
            ORDER BY fetched_at ASC
            """
        ),
        {"artifact_id": str(artifact_id)},
    )
    outputs["snapshots"] = [dict(row) for row in snapshot_result.mappings().all()]
    outputs["github_repo_count"] = int(
        await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM artifact_snapshot_github_repo gr
            JOIN artifact_snapshots s ON s.snapshot_id = gr.snapshot_id
            WHERE s.artifact_id = CAST(:artifact_id AS uuid)
            """,
            {"artifact_id": str(artifact_id)},
        )
        or 0
    )
    outputs["file_sample_count"] = int(
        await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM artifact_snapshot_github_file_samples fs
            JOIN artifact_snapshots s ON s.snapshot_id = fs.snapshot_id
            WHERE s.artifact_id = CAST(:artifact_id AS uuid)
            """,
            {"artifact_id": str(artifact_id)},
        )
        or 0
    )
    outputs["discovered_url_count"] = int(
        await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM discovered_url_observations
            WHERE parent_artifact_id = CAST(:artifact_id AS uuid)
            """,
            {"artifact_id": str(artifact_id)},
        )
        or 0
    )
    registry_result = await session.execute(
        sa.text(
            """
            SELECT current_snapshot_id, current_status
            FROM artifact_registry
            WHERE artifact_id = CAST(:artifact_id AS uuid)
            """
        ),
        {"artifact_id": str(artifact_id)},
    )
    outputs["registry"] = dict(registry_result.mappings().one())
    downstream_result = await session.execute(
        sa.text(
            """
            SELECT event_id, status, payload_json
            FROM event_outbox
            WHERE event_type = 'artifact.snapshot.updated.v1'
              AND aggregate_type = 'artifact'
              AND aggregate_id = CAST(:artifact_id AS uuid)
            ORDER BY created_at ASC
            """
        ),
        {"artifact_id": str(artifact_id)},
    )
    outputs["downstream_outbox"] = [dict(row) for row in downstream_result.mappings().all()]
    return outputs


def _quiet_logger() -> logging.Logger:
    logger = logging.getLogger("gh-enricher-runtime-smoke.worker")
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False
    return logger


async def run_smoke(*, database_url: str, redis_url: str) -> SmokeReport:
    seed_shape = _build_seed_shape()
    report = _new_report(seed_shape)
    if not database_url:
        _mark_fail(
            report,
            "safety.database_url_env_required",
            f"{DATABASE_URL_ENV} is required",
            database_url=database_url,
            redis_url=redis_url,
        )
        return report
    if not redis_url:
        _mark_fail(
            report,
            "safety.redis_url_required",
            f"{REDIS_URL_ENV} or --redis-url is required",
            database_url=database_url,
            redis_url=redis_url,
        )
        return report
    if (
        _is_production_like_url(database_url)
        or _is_production_like_url(redis_url)
        or not _is_expected_smoke_database_url(database_url)
    ):
        _mark_fail(
            report,
            "safety.production_like_url_guard",
            "refusing production-like or non-local smoke database/Redis URL",
            database_url=database_url,
            redis_url=redis_url,
        )
        return report
    _mark_pass(report, "safety.production_like_url_guard")

    if not _is_expected_redis_db14(redis_url):
        _mark_fail(
            report,
            "safety.redis_db14_guard",
            "refusing Redis URL because this smoke must use redis://localhost:6379/14",
            database_url=database_url,
            redis_url=redis_url,
        )
        return report
    _mark_pass(report, "safety.redis_db14_guard")

    seed_ids = SmokeSeedIds(
        source_message_id=uuid4(),
        artifact_id=uuid4(),
        candidate_group_id=uuid4(),
        enrich_event_id=uuid4(),
    )
    report.seeded_ids = {
        "source_message_id": str(seed_ids.source_message_id),
        "artifact_id": str(seed_ids.artifact_id),
        "candidate_group_id": str(seed_ids.candidate_group_id),
        "enrich_event_id": str(seed_ids.enrich_event_id),
        "canonical_id": seed_shape.canonical_id,
    }

    from redis.asyncio import Redis  # type: ignore[import-not-found]

    engine = create_async_engine(database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    consumer_group = f"gh-enricher-smoke-{seed_shape.smoke_id[:12]}"
    consumer_name = f"{consumer_group}-1"
    fake_client = FakeGitHubClient()
    try:
        await _ensure_consumer_group_at_new_messages(
            redis_client,
            queue_name=EXPECTED_QUEUE_NAME,
            consumer_group=consumer_group,
        )
        _mark_pass(report, "redis.consumer_group_created_at_new_messages")

        async with AsyncSession(engine, expire_on_commit=False) as seed_session:
            async with seed_session.begin():
                await _insert_seed_rows(seed_session, seed_shape=seed_shape, seed_ids=seed_ids)
        _mark_pass(report, "db.seed_rows_committed")

        async with AsyncSession(engine, expire_on_commit=False) as relay_session:
            async with relay_session.begin():
                stream_message_id, expected_fields = await _publish_seed_event_through_outbox_route(
                    relay_session,
                    redis_client=redis_client,
                    event_id=seed_ids.enrich_event_id,
                )
        report.redis_stream_message_id = stream_message_id
        _mark_pass(report, "outbox_relay.route_published_github_enrich_event")

        if expected_fields == _build_expected_redis_payload(
            event_id=seed_ids.enrich_event_id,
            artifact_id=seed_ids.artifact_id,
            marker=seed_shape.marker,
        ):
            _mark_pass(report, "outbox_relay.expected_thin_payload_shape")
        else:
            _mark_fail(
                report,
                "outbox_relay.expected_thin_payload_shape",
                "outbox relay stream fields did not match the expected gh-enricher thin payload",
                database_url=database_url,
                redis_url=redis_url,
            )
            return report

        redis_fields = await _load_stream_fields(
            redis_client,
            queue_name=EXPECTED_QUEUE_NAME,
            message_id=stream_message_id,
        )
        if redis_fields is None:
            _mark_fail(
                report,
                "redis.stream_payload_readback",
                "published Redis Stream message could not be read back by message id",
                database_url=database_url,
                redis_url=redis_url,
            )
            return report
        _mark_pass(report, "redis.stream_payload_readback")
        payload_failures = validate_redis_payload(
            redis_fields,
            expected_event_id=seed_ids.enrich_event_id,
            expected_artifact_id=seed_ids.artifact_id,
            database_url=database_url,
            redis_url=redis_url,
        )
        if payload_failures:
            _mark_fail(
                report,
                "redis.thin_payload_contract",
                "; ".join(payload_failures),
                database_url=database_url,
                redis_url=redis_url,
            )
            return report
        _mark_pass(report, "redis.thin_payload_contract")

        config = GhEnricherConfig(
            app_env="smoke",
            database_url=database_url,
            redis_url=redis_url,
            queue_name=EXPECTED_QUEUE_NAME,
            consumer_group=consumer_group,
            consumer_name=consumer_name,
            batch_size=1,
            block_ms=100,
            github_api_base_url="https://api.github.com",
            github_app_id=None,
            github_installation_id=None,
            github_private_key=None,
            request_timeout_sec=1,
            sample_max_files=20,
            sample_excerpt_chars=1200,
            max_file_bytes=131072,
            stale_after_sec=21600,
            log_level="CRITICAL",
        )
        consumer = RedisStreamConsumer(
            redis_client,
            queue_name=config.queue_name,
            consumer_group=config.consumer_group,
            consumer_name=config.consumer_name,
            block_ms=config.block_ms,
            batch_size=config.batch_size,
        )

        class SessionBackedSmokeService:
            async def rehydrate_job(self, trigger_event_id: str):
                async with session_factory() as session:
                    service = GhEnricherService(
                        config,
                        repository=GhEnricherRepository(session),
                        github_client=fake_client,  # type: ignore[arg-type]
                        fetch_planner=GitHubFetchPlanner(),
                        file_sampler=GitHubFileSampler(),
                        url_discovery=GitHubUrlDiscovery(),
                        logger=_quiet_logger(),
                    )
                    return await service.rehydrate_job(trigger_event_id)

            async def handle_job(self, job):
                async with session_factory() as session:
                    async with session.begin():
                        service = GhEnricherService(
                            config,
                            repository=GhEnricherRepository(session),
                            github_client=fake_client,  # type: ignore[arg-type]
                            fetch_planner=GitHubFetchPlanner(),
                            file_sampler=GitHubFileSampler(),
                            url_discovery=GitHubUrlDiscovery(),
                            logger=_quiet_logger(),
                        )
                        return await service.handle_job(job)

        worker = GhEnricherWorker(config, consumer=consumer, service=SessionBackedSmokeService(), logger=_quiet_logger())
        batch_result = await worker.run_once()
        if batch_result.processed == 1 and batch_result.acked == 1:
            _mark_pass(report, "gh_enricher.worker_consumed_and_acked_one_message")
        else:
            _mark_fail(
                report,
                "gh_enricher.worker_consumed_and_acked_one_message",
                f"expected processed=1 acked=1, got processed={batch_result.processed} acked={batch_result.acked}",
                database_url=database_url,
                redis_url=redis_url,
            )

        pending_summary = await redis_client.xpending(EXPECTED_QUEUE_NAME, consumer_group)
        pending_count = int((pending_summary or {}).get("pending", 0))
        if pending_count == 0:
            _mark_pass(report, "redis.message_acknowledged")
        else:
            _mark_fail(
                report,
                "redis.message_acknowledged",
                f"expected zero pending messages after ack, got {pending_count}",
                database_url=database_url,
                redis_url=redis_url,
            )

        report.fake_github_calls = list(fake_client.calls)
        if fake_client.calls and fake_client.external_network_calls_attempted is False:
            _mark_pass(report, "network.fake_github_client_used_no_external_call")
        else:
            _mark_fail(
                report,
                "network.fake_github_client_used_no_external_call",
                "fake GitHub client was not used as expected",
                database_url=database_url,
                redis_url=redis_url,
            )

        async with AsyncSession(engine, expire_on_commit=False) as verify_session:
            snapshot_rows = await verify_session.execute(
                sa.text(
                    """
                    SELECT snapshot_id
                    FROM artifact_snapshots
                    WHERE artifact_id = CAST(:artifact_id AS uuid)
                      AND provider = 'github'
                    ORDER BY fetched_at ASC
                    """
                ),
                {"artifact_id": str(seed_ids.artifact_id)},
            )
            snapshot_ids = [UUID(str(row["snapshot_id"])) for row in snapshot_rows.mappings().all()]
            report.resulting_snapshot_ids = [str(snapshot_id) for snapshot_id in snapshot_ids]
            outputs = await _load_outputs(verify_session, artifact_id=seed_ids.artifact_id)

        if outputs.get("enrichment_run_count", 0) >= 1:
            _mark_pass(report, "db.artifact_enrichment_runs_github_exists")
        else:
            _mark_fail(report, "db.artifact_enrichment_runs_github_exists", "github enrichment run was not found", database_url=database_url, redis_url=redis_url)
        snapshots = outputs.get("snapshots") or []
        if any(row["provider"] == "github" and row["snapshot_type"] == "github_repo" for row in snapshots):
            _mark_pass(report, "db.artifact_snapshots_github_repo_exists")
        else:
            _mark_fail(report, "db.artifact_snapshots_github_repo_exists", "github_repo artifact snapshot was not found", database_url=database_url, redis_url=redis_url)
        if outputs.get("github_repo_count", 0) >= 1:
            _mark_pass(report, "db.artifact_snapshot_github_repo_exists")
        else:
            _mark_fail(report, "db.artifact_snapshot_github_repo_exists", "artifact_snapshot_github_repo row was not found", database_url=database_url, redis_url=redis_url)
        if outputs.get("file_sample_count", 0) >= 1:
            _mark_pass(report, "db.artifact_snapshot_github_file_samples_exists")
        else:
            _mark_fail(report, "db.artifact_snapshot_github_file_samples_exists", "artifact_snapshot_github_file_samples row was not found", database_url=database_url, redis_url=redis_url)
        if outputs.get("discovered_url_count", 0) >= 1:
            _mark_pass(report, "db.discovered_url_observations_exists")
        else:
            _mark_fail(report, "db.discovered_url_observations_exists", "discovered_url_observations row was not found", database_url=database_url, redis_url=redis_url)

        registry = outputs.get("registry") or {}
        if registry.get("current_snapshot_id") is not None:
            _mark_pass(report, "db.artifact_registry_current_snapshot_id_set")
        else:
            _mark_fail(report, "db.artifact_registry_current_snapshot_id_set", "artifact_registry.current_snapshot_id was not set", database_url=database_url, redis_url=redis_url)
        if str(registry.get("current_status")) in {"ready", "partial_ready"}:
            _mark_pass(report, "db.artifact_registry_current_status_ready")
        else:
            _mark_fail(
                report,
                "db.artifact_registry_current_status_ready",
                f"expected current_status ready or partial_ready, got {registry.get('current_status')!r}",
                database_url=database_url,
                redis_url=redis_url,
            )

        downstream = outputs.get("downstream_outbox") or []
        report.downstream_outbox_ids = [str(row["event_id"]) for row in downstream]
        if any(row["status"] == "pending" for row in downstream):
            _mark_pass(report, "db.snapshot_updated_outbox_pending_exists")
        else:
            _mark_fail(report, "db.snapshot_updated_outbox_pending_exists", "pending artifact.snapshot.updated.v1 outbox row was not found", database_url=database_url, redis_url=redis_url)
    finally:
        close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
        if close is not None:
            maybe_awaitable = close()
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable
        await engine.dispose()

    return report


def _render_json(report: SmokeReport) -> str:
    return json.dumps(asdict(report), ensure_ascii=False, indent=2, sort_keys=False)


async def _amain(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    database_url = os.getenv(DATABASE_URL_ENV, "").strip()
    redis_url = (args.redis_url or os.getenv(REDIS_URL_ENV, "")).strip()
    try:
        report = await run_smoke(database_url=database_url, redis_url=redis_url)
    except Exception as exc:
        report = _new_report(_build_seed_shape())
        _mark_fail(
            report,
            "smoke.unexpected_failure",
            str(exc),
            database_url=database_url,
            redis_url=redis_url,
        )
    print(_render_json(report))
    return 1 if report.failed() else 0


def main() -> None:
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()
