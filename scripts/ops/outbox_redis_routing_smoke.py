from __future__ import annotations

import argparse
import asyncio
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

from src.services.outbox_relay.config import OutboxRelayConfig
from src.services.outbox_relay.redis_streams import RedisStreamsPublisher
from src.services.outbox_relay.repositories import OutboxRelayRepository
from src.services.outbox_relay.routing import OutboxRouteResolver
from src.services.outbox_relay.service import OutboxRelayService


REPORT_TYPE = "outbox_redis_routing_smoke_v1"
MUTATION_SAFETY = (
    "controlled smoke write only: inserts one event_outbox smoke row, publishes that "
    "row through one bounded outbox-relay pass, writes one succeeded job_attempts row, "
    "and writes one Redis Stream message"
)
SMOKE_MARKER_PREFIX = "ops-smoke:outbox-redis-routing:"
SMOKE_EVENT_TYPE = "source_message.created.v1"
SMOKE_AGGREGATE_TYPE = "source_message"
EXPECTED_QUEUE_NAME = "q.source.normalize"
EXPECTED_STAGE_NAME = "normalize"
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
    smoke_event_id: str | None

    def failed(self) -> bool:
        return bool(self.checks_failed)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run an opt-in post-Stage44 Redis/outbox routing smoke. "
            "This writes one controlled event_outbox smoke row and one Redis Stream message."
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
            "This smoke must use dev/test PostgreSQL and a disposable Redis DB only.",
            "It does not start live workers and does not call Telegram, OpenAI, GitHub, X, or Web.",
        ],
        database_url_redacted=True,
        redis_url_redacted=True,
        mutation_safety=MUTATION_SAFETY,
        queue_name=None,
        stream_message_id=None,
        smoke_event_id=None,
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


async def _select_scalar(session: AsyncSession, statement: str, params: dict[str, Any] | None = None) -> Any:
    result = await session.execute(sa.text(statement), params or {})
    return result.scalar_one_or_none()


async def _insert_smoke_outbox_row(session: AsyncSession, *, event_id: UUID, aggregate_id: UUID, marker: str) -> None:
    now = datetime.now(UTC)
    payload = {
        "event_id": str(event_id),
        "source_message_id": str(aggregate_id),
        "current_version_no": 1,
        "logical_post_key": marker,
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
                status
            ) VALUES (
                CAST(:event_id AS uuid),
                :event_type,
                :aggregate_type,
                CAST(:aggregate_id AS uuid),
                :dedupe_key,
                CAST(:payload_json AS jsonb),
                'pending'::outbox_status_enum
            )
            """
        ),
        {
            "event_id": str(event_id),
            "event_type": SMOKE_EVENT_TYPE,
            "aggregate_type": SMOKE_AGGREGATE_TYPE,
            "aggregate_id": str(aggregate_id),
            "dedupe_key": marker,
            "payload_json": json.dumps(payload, sort_keys=True),
        },
    )


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


async def _load_only_pending_row(session: AsyncSession) -> Mapping[str, Any] | None:
    result = await session.execute(
        sa.text(
            """
            SELECT event_id, dedupe_key
            FROM event_outbox
            WHERE status = 'pending'::outbox_status_enum
            ORDER BY created_at ASC, event_id ASC
            """
        )
    )
    row = result.mappings().first()
    return dict(row) if row is not None else None


async def _load_smoke_outbox_state(session: AsyncSession, *, event_id: UUID) -> Mapping[str, Any] | None:
    result = await session.execute(
        sa.text(
            """
            SELECT event_id, status, published_at, dedupe_key
            FROM event_outbox
            WHERE event_id = CAST(:event_id AS uuid)
            """
        ),
        {"event_id": str(event_id)},
    )
    row = result.mappings().first()
    return dict(row) if row is not None else None


async def _count_matching_job_attempts(session: AsyncSession, *, aggregate_id: UUID) -> int:
    value = await _select_scalar(
        session,
        """
        SELECT COUNT(*)
        FROM job_attempts
        WHERE root_object_type = :root_object_type
          AND root_object_id = CAST(:root_object_id AS uuid)
          AND queue_name = :queue_name
          AND stage_name = :stage_name
          AND attempt_status = 'succeeded'::job_attempt_status_enum
        """,
        {
            "root_object_type": SMOKE_AGGREGATE_TYPE,
            "root_object_id": str(aggregate_id),
            "queue_name": EXPECTED_QUEUE_NAME,
            "stage_name": EXPECTED_STAGE_NAME,
        },
    )
    return int(value or 0)


async def _queue_lengths(redis_client: Any) -> dict[str, int]:
    lengths: dict[str, int] = {}
    for queue_name in KNOWN_QUEUE_NAMES:
        lengths[queue_name] = int(await redis_client.xlen(queue_name))
    return lengths


async def _find_smoke_stream_message(redis_client: Any, *, event_id: UUID) -> tuple[str | None, dict[str, Any] | None]:
    entries = await redis_client.xrevrange(EXPECTED_QUEUE_NAME, count=100)
    for message_id, fields in entries:
        if str(fields.get("trigger_event_id", "")) == str(event_id):
            return str(message_id), dict(fields)
    return None, None


def _quiet_logger() -> logging.Logger:
    logger = logging.getLogger("outbox-redis-routing-smoke.relay")
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False
    return logger


async def run_smoke(*, database_url: str, redis_url: str) -> SmokeReport:
    report = _new_report()

    if _is_production_like_url(database_url) or _is_production_like_url(redis_url):
        _mark_fail(
            report,
            "safety.production_like_url_guard",
            "refusing production-like database or Redis URL",
            database_url=database_url,
            redis_url=redis_url,
        )
        return report
    _mark_pass(report, "safety.production_like_url_guard")

    event_id = uuid4()
    aggregate_id = uuid4()
    marker = f"{SMOKE_MARKER_PREFIX}{event_id}"
    report.smoke_event_id = str(event_id)
    report.queue_name = EXPECTED_QUEUE_NAME

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

        async with AsyncSession(engine, expire_on_commit=False) as insert_session:
            await _insert_smoke_outbox_row(insert_session, event_id=event_id, aggregate_id=aggregate_id, marker=marker)
            await insert_session.commit()

        async with AsyncSession(engine, expire_on_commit=False) as relay_session:
            pending_before_relay_count = await _count_pending_rows(relay_session)
            only_pending_row = await _load_only_pending_row(relay_session) if pending_before_relay_count == 1 else None
            if pending_before_relay_count != 1 or str((only_pending_row or {}).get("event_id", "")) != str(event_id):
                _mark_fail(
                    report,
                    "safety.smoke_row_is_only_pending_row",
                    "pending set before relay was not limited to the committed controlled smoke row",
                    database_url=database_url,
                    redis_url=redis_url,
                )
                return report
            _mark_pass(report, "safety.smoke_row_is_only_pending_row")

            queue_lengths_before = await _queue_lengths(redis_client)
            config = OutboxRelayConfig(
                app_env="smoke",
                database_url=database_url,
                redis_url=redis_url,
                poll_interval_ms=1000,
                batch_size=1,
                xadd_maxlen=None,
                log_level="CRITICAL",
            )
            service = OutboxRelayService(
                config,
                repository=OutboxRelayRepository(relay_session),
                publisher=RedisStreamsPublisher(redis_client),
                route_resolver=OutboxRouteResolver(),
                logger=_quiet_logger(),
            )
            processed = await service.run_once()
            await relay_session.commit()
            if processed != 1:
                _mark_fail(
                    report,
                    "relay.bounded_run_once",
                    f"expected one processed row, got {processed}",
                    database_url=database_url,
                    redis_url=redis_url,
                )
            else:
                _mark_pass(report, "relay.bounded_run_once")

            queue_lengths_after = await _queue_lengths(redis_client)
            changed_queues = [
                queue_name
                for queue_name in KNOWN_QUEUE_NAMES
                if queue_lengths_after[queue_name] != queue_lengths_before[queue_name]
            ]
            if changed_queues != [EXPECTED_QUEUE_NAME]:
                _mark_fail(
                    report,
                    "redis.only_expected_stream_changed",
                    f"unexpected stream length changes: {', '.join(changed_queues) or 'none'}",
                    database_url=database_url,
                    redis_url=redis_url,
                )
            else:
                _mark_pass(report, "redis.only_expected_stream_changed")

            stream_message_id, fields = await _find_smoke_stream_message(redis_client, event_id=event_id)
            report.stream_message_id = stream_message_id
            if fields is None or stream_message_id is None:
                _mark_fail(
                    report,
                    "redis.stream_message_present",
                    "expected Redis Stream message was not found",
                    database_url=database_url,
                    redis_url=redis_url,
                )
            else:
                _mark_pass(report, "redis.stream_message_present")
                payload_failures = validate_redis_payload(
                    fields,
                    expected_event_id=event_id,
                    expected_root_object_id=aggregate_id,
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
                else:
                    _mark_pass(report, "redis.thin_payload_contract")

            async with AsyncSession(engine, expire_on_commit=False) as verify_session:
                state = await _load_smoke_outbox_state(verify_session, event_id=event_id)
                if state is None:
                    _mark_fail(
                        report,
                        "db.smoke_outbox_row_present",
                        "smoke event_outbox row is missing",
                        database_url=database_url,
                        redis_url=redis_url,
                    )
                else:
                    _mark_pass(report, "db.smoke_outbox_row_present")
                    if str(state["status"]) == "published" and state["published_at"] is not None:
                        _mark_pass(report, "db.smoke_outbox_published")
                    else:
                        _mark_fail(
                            report,
                            "db.smoke_outbox_published",
                            f"expected published status with published_at, got status={state['status']!r}",
                            database_url=database_url,
                            redis_url=redis_url,
                        )

                attempts = await _count_matching_job_attempts(verify_session, aggregate_id=aggregate_id)
                if attempts >= 1:
                    _mark_pass(report, "db.job_attempt_succeeded")
                else:
                    _mark_fail(
                        report,
                        "db.job_attempt_succeeded",
                        "matching succeeded job_attempts row was not found",
                        database_url=database_url,
                        redis_url=redis_url,
                    )
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
        f"smoke_event_id: {report.smoke_event_id}",
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
