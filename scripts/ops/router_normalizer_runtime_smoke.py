from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, unquote, urlsplit
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.router_normalizer.config import RouterNormalizerConfig
from src.services.router_normalizer.redis_streams import RedisStreamsConsumer
from src.services.router_normalizer.repositories import RouterNormalizerRepository
from src.services.router_normalizer.service import RouterNormalizerService


REPORT_TYPE = "router_normalizer_runtime_smoke_v1"
MUTATION_SAFETY = (
    "controlled smoke write only: inserts one synthetic source_messages row, one "
    "source_message_versions row, one published source event_outbox row, writes one "
    "thin Redis Stream message to q.source.normalize, runs one bounded router-normalizer "
    "consumer pass, and leaves controlled normalizer output rows including a pending "
    "artifact.enrich.requested.v1 event"
)
SMOKE_MARKER_PREFIX = "ops-smoke:router-normalizer-runtime:"
SMOKE_SOURCE_TEXT = "Check this GitHub tool: https://github.com/octocat/Hello-World"
SMOKE_EVENT_TYPE = "source_message.created.v1"
SMOKE_AGGREGATE_TYPE = "source_message"
EXPECTED_QUEUE_NAME = "q.source.normalize"
EXPECTED_REDIS_DB = 14
EXPECTED_STAGE_NAME = "normalize"
NORMALIZER_VERSION = "router-normalizer-v1"
EXPECTED_CANONICAL_ID = "github:repo:octocat/hello-world"
EXPECTED_CANONICAL_URL = "https://github.com/octocat/Hello-World"
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
    "raw_message_json",
    "raw_message_text",
    "source_message_text",
    "source_text",
    "text_body",
    "caption_text",
    "database_url",
    "redis_url",
    "password",
    "token",
    "secret",
    "api_key",
}
KNOWN_QUEUE_NAMES = (
    "q.source.normalize",
    "q.artifact.enrich.github",
    "q.artifact.enrich.x",
    "q.artifact.enrich.web",
    "q.candidate.bundle",
    "q.analysis.route",
    "q.analysis.judge",
    "q.analysis.validate",
    "q.analysis.policy",
    "q.notification.send",
    "q.replay",
    "q.maintenance",
)


@dataclass(slots=True)
class SmokeReport:
    report_type: str
    checks_run: list[str]
    checks_passed: list[str]
    checks_failed: list[str]
    failures: list[dict[str, str]]
    warnings: list[str]
    database_url_redacted: bool
    redis_url_redacted: bool
    mutation_safety: str
    queue_name: str | None
    stream_message_id: str | None
    smoke_source_message_id: str | None
    smoke_event_id: str | None
    normalization_run_id: str | None
    candidate_group_id: str | None
    primary_artifact_id: str | None
    downstream_event_id: str | None

    def failed(self) -> bool:
        return bool(self.checks_failed)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run an opt-in post-Stage44 router-normalizer runtime smoke. "
            "This writes controlled synthetic source-message rows, sends one thin Redis "
            "message to q.source.normalize, and runs one bounded consumer pass."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--redis-url", required=True)
    parser.add_argument("--confirm", choices=["write"], required=True)
    parser.add_argument("--format", choices=["json", "human"], default="json")
    return parser


def _new_report() -> SmokeReport:
    return SmokeReport(
        report_type=REPORT_TYPE,
        checks_run=[],
        checks_passed=[],
        checks_failed=[],
        failures=[],
        warnings=[
            "This smoke must use dev/test PostgreSQL and Redis DB 14 only.",
            "It does not start live workers and does not call Telegram, OpenAI, GitHub, X, or Web.",
            "A successful run may leave controlled smoke DB rows and a pending downstream enrich request.",
        ],
        database_url_redacted=True,
        redis_url_redacted=True,
        mutation_safety=MUTATION_SAFETY,
        queue_name=None,
        stream_message_id=None,
        smoke_source_message_id=None,
        smoke_event_id=None,
        normalization_run_id=None,
        candidate_group_id=None,
        primary_artifact_id=None,
        downstream_event_id=None,
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
    if parsed.scheme in {"rediss"}:
        return True
    return False


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


def validate_redis_payload(
    fields: Mapping[str, Any],
    *,
    expected_event_id: UUID,
    expected_root_object_id: UUID,
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
        "root_object_type": SMOKE_AGGREGATE_TYPE,
        "root_object_id": str(expected_root_object_id),
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


def _is_forbidden_redis_field(field_name: str) -> bool:
    lowered = field_name.lower()
    if lowered in FORBIDDEN_REDIS_FIELDS:
        return True
    return any(token in lowered for token in ("password", "token", "secret", "api_key", "apikey"))


def _build_redis_payload(*, event_id: UUID, source_message_id: UUID, marker: str) -> dict[str, str]:
    return {
        "job_id": str(event_id),
        "stage_name": EXPECTED_STAGE_NAME,
        "root_object_type": SMOKE_AGGREGATE_TYPE,
        "root_object_id": str(source_message_id),
        "idempotency_key": marker,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
    }


async def _select_scalar(session: AsyncSession, statement: str, params: dict[str, Any] | None = None) -> Any:
    result = await session.execute(sa.text(statement), params or {})
    return result.scalar_one_or_none()


async def _count_pending_rows(session: AsyncSession) -> int:
    value = await _select_scalar(
        session,
        """
        SELECT COUNT(*)
        FROM event_outbox
        WHERE status = 'pending'::outbox_status_enum
        """,
    )
    return int(value or 0)


async def _queue_lengths(redis_client: Any) -> dict[str, int]:
    lengths: dict[str, int] = {}
    for queue_name in KNOWN_QUEUE_NAMES:
        lengths[queue_name] = int(await redis_client.xlen(queue_name))
    return lengths


async def _insert_smoke_source_rows(
    session: AsyncSession,
    *,
    source_message_id: UUID,
    event_id: UUID,
    marker: str,
) -> None:
    now = datetime.now(UTC)
    chat_id = -int(str(source_message_id.int)[-12:])
    message_id = int(str(event_id.int)[-9:]) or 1
    raw_message_json = {
        "smoke_marker": marker,
        "source": "router_normalizer_runtime_smoke",
        "external_network_calls_allowed": False,
    }
    url_surface_json = [
        {
            "observed_url": "https://github.com/octocat/Hello-World",
            "source_kind": "regex",
            "context_path": "text_surface",
        }
    ]
    content_hash = hashlib.sha256(SMOKE_SOURCE_TEXT.encode("utf-8")).hexdigest()
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
            "source_message_id": str(source_message_id),
            "chat_id": chat_id,
            "message_id": message_id,
            "logical_post_key": marker,
            "posted_at": now,
            "text_body": SMOKE_SOURCE_TEXT,
            "text_surface": SMOKE_SOURCE_TEXT,
            "entities_json": json.dumps([], sort_keys=True),
            "url_surface_json": json.dumps(url_surface_json, sort_keys=True),
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
            "source_message_id": str(source_message_id),
            "observed_at": now,
            "text_surface": SMOKE_SOURCE_TEXT,
            "entities_json": json.dumps([], sort_keys=True),
            "raw_message_json": json.dumps(raw_message_json, sort_keys=True),
            "content_hash": content_hash,
        },
    )
    outbox_payload = {
        "event_id": str(event_id),
        "source_message_id": str(source_message_id),
        "current_version_no": 1,
        "logical_post_key": marker,
        "smoke_marker": marker,
        "occurred_at": now.isoformat(),
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
                published_at
            ) VALUES (
                CAST(:event_id AS uuid),
                :event_type,
                :aggregate_type,
                CAST(:aggregate_id AS uuid),
                :dedupe_key,
                CAST(:payload_json AS jsonb),
                'published'::outbox_status_enum,
                :published_at
            )
            """
        ),
        {
            "event_id": str(event_id),
            "event_type": SMOKE_EVENT_TYPE,
            "aggregate_type": SMOKE_AGGREGATE_TYPE,
            "aggregate_id": str(source_message_id),
            "dedupe_key": marker,
            "payload_json": json.dumps(outbox_payload, sort_keys=True),
            "published_at": now,
        },
    )


async def _load_outputs(
    session: AsyncSession,
    *,
    source_message_id: UUID,
    source_version_no: int,
    normalizer_version: str,
) -> dict[str, Any]:
    result = await session.execute(
        sa.text(
            """
            SELECT
                nr.normalization_run_id,
                nr.signal_detected,
                nr.candidate_eligible,
                nr.trigger_strength,
                ar.artifact_id AS primary_artifact_id,
                ar.artifact_type,
                ar.canonical_id,
                ar.canonical_url,
                cgp.candidate_group_id
            FROM normalization_runs nr
            LEFT JOIN candidate_group_proposals cgp
              ON cgp.source_message_id = nr.source_message_id
             AND cgp.source_version_no = nr.source_version_no
             AND cgp.normalizer_version = nr.normalizer_version
            LEFT JOIN artifact_registry ar
              ON ar.artifact_id = cgp.current_primary_artifact_id
            WHERE nr.source_message_id = CAST(:source_message_id AS uuid)
              AND nr.source_version_no = :source_version_no
              AND nr.normalizer_version = :normalizer_version
            ORDER BY cgp.created_at ASC NULLS LAST
            LIMIT 1
            """
        ),
        {
            "source_message_id": str(source_message_id),
            "source_version_no": source_version_no,
            "normalizer_version": normalizer_version,
        },
    )
    row = result.mappings().first()
    outputs: dict[str, Any] = dict(row) if row is not None else {}

    outputs["artifact_observation_count"] = int(
        await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM artifact_observations
            WHERE source_message_id = CAST(:source_message_id AS uuid)
              AND source_version_no = :source_version_no
            """,
            {"source_message_id": str(source_message_id), "source_version_no": source_version_no},
        )
        or 0
    )
    outputs["candidate_member_count"] = 0
    if outputs.get("candidate_group_id") is not None:
        outputs["candidate_member_count"] = int(
            await _select_scalar(
                session,
                """
                SELECT COUNT(*)
                FROM candidate_group_members
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
                """,
                {"candidate_group_id": str(outputs["candidate_group_id"])},
            )
            or 0
        )

    downstream_result = await session.execute(
        sa.text(
            """
            SELECT event_id, payload_json
            FROM event_outbox
            WHERE event_type = 'artifact.enrich.requested.v1'
              AND payload_json->>'source_message_id' = :source_message_id
              AND payload_json->>'source_version_no' = :source_version_no
              AND payload_json->>'provider_route' = 'github'
            ORDER BY created_at ASC
            LIMIT 1
            """
        ),
        {"source_message_id": str(source_message_id), "source_version_no": str(source_version_no)},
    )
    downstream = downstream_result.mappings().first()
    outputs["downstream_event_id"] = downstream["event_id"] if downstream is not None else None
    outputs["downstream_payload"] = downstream["payload_json"] if downstream is not None else None
    outputs["suppression_trace_count"] = int(
        await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM normalization_suppression_traces nst
            JOIN normalization_runs nr ON nr.normalization_run_id = nst.normalization_run_id
            WHERE nr.source_message_id = CAST(:source_message_id AS uuid)
              AND nr.source_version_no = :source_version_no
              AND nr.normalizer_version = :normalizer_version
            """,
            {
                "source_message_id": str(source_message_id),
                "source_version_no": source_version_no,
                "normalizer_version": normalizer_version,
            },
        )
        or 0
    )
    return outputs


def _quiet_logger() -> logging.Logger:
    logger = logging.getLogger("router-normalizer-runtime-smoke.consumer")
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False
    return logger


async def run_smoke(*, database_url: str, redis_url: str) -> SmokeReport:
    report = _new_report()
    report.queue_name = EXPECTED_QUEUE_NAME

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

    source_message_id = uuid4()
    event_id = uuid4()
    marker = f"{SMOKE_MARKER_PREFIX}{event_id}"
    report.smoke_source_message_id = str(source_message_id)
    report.smoke_event_id = str(event_id)

    from redis.asyncio import Redis  # type: ignore[import-not-found]

    engine = create_async_engine(database_url, future=True)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as preflight_session:
            pending_before_count = await _count_pending_rows(preflight_session)
            if pending_before_count != 0:
                _mark_fail(
                    report,
                    "safety.pending_outbox_empty_guard",
                    f"aborting because {pending_before_count} pending event_outbox row(s) exist",
                    database_url=database_url,
                    redis_url=redis_url,
                )
                return report
            _mark_pass(report, "safety.pending_outbox_empty_guard")

        queue_lengths_before = await _queue_lengths(redis_client)
        non_empty_queues = {name: size for name, size in queue_lengths_before.items() if size != 0}
        if non_empty_queues:
            _mark_fail(
                report,
                "safety.redis_known_queues_empty_guard",
                f"aborting because known Redis queues are non-empty: {non_empty_queues}",
                database_url=database_url,
                redis_url=redis_url,
            )
            return report
        _mark_pass(report, "safety.redis_known_queues_empty_guard")

        async with AsyncSession(engine, expire_on_commit=False) as insert_session:
            await _insert_smoke_source_rows(
                insert_session,
                source_message_id=source_message_id,
                event_id=event_id,
                marker=marker,
            )
            await insert_session.commit()
        _mark_pass(report, "db.synthetic_source_rows_committed")

        fields = _build_redis_payload(event_id=event_id, source_message_id=source_message_id, marker=marker)
        payload_failures = validate_redis_payload(
            fields,
            expected_event_id=event_id,
            expected_root_object_id=source_message_id,
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

        stream_message_id = str(await redis_client.xadd(EXPECTED_QUEUE_NAME, fields))
        report.stream_message_id = stream_message_id
        _mark_pass(report, "redis.stream_message_inserted")

        config = RouterNormalizerConfig(
            app_env="smoke",
            database_url=database_url,
            redis_url=redis_url,
            queue_name=EXPECTED_QUEUE_NAME,
            consumer_group="router-normalizer-smoke",
            consumer_name=f"router-normalizer-smoke-{event_id}",
            block_ms=100,
            batch_size=1,
            normalizer_version=NORMALIZER_VERSION,
            short_url_allowlist=("bit.ly", "t.co", "tinyurl.com", "ow.ly", "lnkd.in", "buff.ly", "goo.gl"),
            short_url_hop_limit=3,
            short_url_timeout_seconds=0.1,
            log_level="CRITICAL",
        )
        consumer = RedisStreamsConsumer(
            redis_client,
            queue_name=config.queue_name,
            consumer_group=config.consumer_group,
            consumer_name=config.consumer_name,
            block_ms=config.block_ms,
            batch_size=config.batch_size,
        )
        await consumer.ensure_group()
        messages = await consumer.read_batch()
        if len(messages) != 1:
            _mark_fail(
                report,
                "consumer.bounded_read_one_message",
                f"expected exactly one Redis message, got {len(messages)}",
                database_url=database_url,
                redis_url=redis_url,
            )
            return report
        message_id, message = messages[0]
        if str(message_id) != stream_message_id or message.trigger_event_id != str(event_id):
            _mark_fail(
                report,
                "consumer.read_inserted_smoke_message",
                "consumer did not read the inserted controlled smoke message",
                database_url=database_url,
                redis_url=redis_url,
            )
            return report
        _mark_pass(report, "consumer.bounded_read_one_message")
        _mark_pass(report, "consumer.read_inserted_smoke_message")

        async with AsyncSession(engine, expire_on_commit=False) as process_session:
            async with process_session.begin():
                service = RouterNormalizerService(
                    config,
                    repository=RouterNormalizerRepository(process_session),
                    logger=_quiet_logger(),
                )
                result = await service.process_stream_message(message)
        await consumer.ack(message_id)
        _mark_pass(report, "consumer.processed_one_message")
        _mark_pass(report, "consumer.message_acknowledged")

        pending_summary = await redis_client.xpending(EXPECTED_QUEUE_NAME, config.consumer_group)
        pending_count = int((pending_summary or {}).get("pending", 0))
        if pending_count == 0:
            _mark_pass(report, "redis.consumer_group_pending_empty")
        else:
            _mark_fail(
                report,
                "redis.consumer_group_pending_empty",
                f"expected zero pending messages after ack, got {pending_count}",
                database_url=database_url,
                redis_url=redis_url,
            )

        queue_lengths_after = await _queue_lengths(redis_client)
        downstream_queue_writes = {
            name: size
            for name, size in queue_lengths_after.items()
            if name != EXPECTED_QUEUE_NAME and size != 0
        }
        if not downstream_queue_writes:
            _mark_pass(report, "redis.no_downstream_queue_writes")
        else:
            _mark_fail(
                report,
                "redis.no_downstream_queue_writes",
                f"unexpected downstream Redis queue writes: {downstream_queue_writes}",
                database_url=database_url,
                redis_url=redis_url,
            )

        async with AsyncSession(engine, expire_on_commit=False) as verify_session:
            outputs = await _load_outputs(
                verify_session,
                source_message_id=source_message_id,
                source_version_no=1,
                normalizer_version=NORMALIZER_VERSION,
            )

        report.normalization_run_id = str(outputs.get("normalization_run_id") or result.normalization_run_id)
        report.candidate_group_id = str(outputs["candidate_group_id"]) if outputs.get("candidate_group_id") else None
        report.primary_artifact_id = str(outputs["primary_artifact_id"]) if outputs.get("primary_artifact_id") else None
        report.downstream_event_id = str(outputs["downstream_event_id"]) if outputs.get("downstream_event_id") else None

        if outputs.get("normalization_run_id") is not None:
            _mark_pass(report, "db.normalization_run_exists")
        else:
            _mark_fail(report, "db.normalization_run_exists", "normalization_runs row was not found", database_url=database_url, redis_url=redis_url)
        if outputs.get("signal_detected") is True:
            _mark_pass(report, "db.signal_detected_true")
        else:
            _mark_fail(report, "db.signal_detected_true", "signal_detected was not true", database_url=database_url, redis_url=redis_url)
        if outputs.get("candidate_eligible") is True:
            _mark_pass(report, "db.candidate_eligible_true")
        else:
            _mark_fail(report, "db.candidate_eligible_true", "candidate_eligible was not true", database_url=database_url, redis_url=redis_url)
        if outputs.get("trigger_strength") == "strong":
            _mark_pass(report, "db.trigger_strength_strong")
        else:
            _mark_fail(
                report,
                "db.trigger_strength_strong",
                f"expected trigger_strength strong, got {outputs.get('trigger_strength')!r}",
                database_url=database_url,
                redis_url=redis_url,
            )
        if outputs.get("artifact_type") == "github_repo" and outputs.get("canonical_id") == EXPECTED_CANONICAL_ID:
            _mark_pass(report, "db.primary_github_repo_artifact_exists")
        else:
            _mark_fail(
                report,
                "db.primary_github_repo_artifact_exists",
                f"expected primary GitHub repo artifact {EXPECTED_CANONICAL_ID}, got {outputs.get('canonical_id')!r}",
                database_url=database_url,
                redis_url=redis_url,
            )
        if int(outputs.get("artifact_observation_count") or 0) >= 1:
            _mark_pass(report, "db.artifact_observation_exists")
        else:
            _mark_fail(report, "db.artifact_observation_exists", "artifact observation was not found", database_url=database_url, redis_url=redis_url)
        if outputs.get("candidate_group_id") is not None:
            _mark_pass(report, "db.candidate_group_proposal_exists")
        else:
            _mark_fail(report, "db.candidate_group_proposal_exists", "candidate group proposal was not found", database_url=database_url, redis_url=redis_url)
        if int(outputs.get("candidate_member_count") or 0) >= 1:
            _mark_pass(report, "db.primary_candidate_group_member_exists")
        else:
            _mark_fail(report, "db.primary_candidate_group_member_exists", "candidate group member was not found", database_url=database_url, redis_url=redis_url)
        downstream_payload = outputs.get("downstream_payload") or {}
        if outputs.get("downstream_event_id") is not None and downstream_payload.get("provider_route") == "github":
            _mark_pass(report, "db.downstream_github_enrich_event_exists")
        else:
            _mark_fail(
                report,
                "db.downstream_github_enrich_event_exists",
                "artifact.enrich.requested.v1 provider_route github row was not found",
                database_url=database_url,
                redis_url=redis_url,
            )
        if int(outputs.get("suppression_trace_count") or 0) == 0:
            _mark_pass(report, "db.no_positive_path_suppression_trace")
        else:
            _mark_fail(
                report,
                "db.no_positive_path_suppression_trace",
                f"expected no suppression trace, got {outputs.get('suppression_trace_count')}",
                database_url=database_url,
                redis_url=redis_url,
            )
        _mark_pass(report, "network.no_downstream_external_service_invoked")
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


def _render_human(report: SmokeReport) -> str:
    lines = [
        report.report_type,
        f"mutation_safety: {report.mutation_safety}",
        f"database_url_redacted: {report.database_url_redacted}",
        f"redis_url_redacted: {report.redis_url_redacted}",
        f"queue_name: {report.queue_name}",
        f"stream_message_id: {report.stream_message_id}",
        f"smoke_source_message_id: {report.smoke_source_message_id}",
        f"smoke_event_id: {report.smoke_event_id}",
        f"normalization_run_id: {report.normalization_run_id}",
        f"candidate_group_id: {report.candidate_group_id}",
        f"primary_artifact_id: {report.primary_artifact_id}",
        f"downstream_event_id: {report.downstream_event_id}",
        f"checks_passed: {len(report.checks_passed)}",
        f"checks_failed: {len(report.checks_failed)}",
    ]
    for failure in report.failures:
        lines.append(f"FAIL {failure['check']}: {failure['message']}")
    for warning in report.warnings:
        lines.append(f"WARN {warning}")
    return "\n".join(lines)


async def _amain(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        report = await run_smoke(database_url=args.database_url, redis_url=args.redis_url)
    except Exception as exc:
        report = _new_report()
        _mark_fail(
            report,
            "smoke.unexpected_failure",
            str(exc),
            database_url=args.database_url,
            redis_url=args.redis_url,
        )
    output = _render_json(report) if args.format == "json" else _render_human(report)
    print(output)
    return 1 if report.failed() else 0


def main() -> None:
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()
