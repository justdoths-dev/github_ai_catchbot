from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, unquote, urlsplit
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.maintenance.config import MaintenanceConfig
from src.services.maintenance.redis_streams import RedisStreamConsumer
from src.services.maintenance.repositories import MaintenanceRepository
from src.services.maintenance.service import MaintenanceService
from src.services.maintenance.worker import DueRetryPromotionWorker, MaintenanceQueueWorker
from src.services.outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from src.services.outbox_relay.redis_streams import RedisStreamsPublisher
from src.services.outbox_relay.repositories import OutboxRelayRepository
from src.services.outbox_relay.routing import OutboxRouteResolver


REPORT_TYPE = "maintenance_runtime_smoke_v1"
SELECTED_SCENARIO = "retryable_due_promotion"
DATABASE_URL_ENV = "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL"
REDIS_URL_ENV = "REDIS_URL"
MUTATION_SAFETY = (
    "controlled smoke write only: inserts marker-scoped synthetic source, artifact, "
    "candidate, judge output, analysis, one failed_retryable notification_plan, one "
    "retryable notification_delivery_records row, and one pending "
    "notification.delivery.result.v1 outbox row; publishes that event through the "
    "existing outbox relay route to q.maintenance; runs exactly one maintenance "
    "delivery-result worker pass and one due retry promoter pass; and verifies the "
    "maintenance boundary only writes job_attempts and a pending "
    "notification.plan.created.v1 retry-intent outbox row"
)
SMOKE_MARKER_PREFIX = "ops-smoke:maintenance-runtime:"
EXPECTED_EVENT_TYPE = "notification.delivery.result.v1"
EXPECTED_RETRY_EVENT_TYPE = "notification.plan.created.v1"
EXPECTED_QUEUE_NAME = "q.maintenance"
EXPECTED_STAGE_NAME = "maintenance"
EXPECTED_REDIS_DB = 14
EXPECTED_AGGREGATE_TYPE = "notification_plan"
EXPECTED_DELIVERY_STATUS = "failed_retryable"
EXPECTED_DELIVERY_DECISION = "send_now"
EXPECTED_URGENCY_PROFILE = "normal_silent"
EXPECTED_TARGET_CHAT_ID = 12345
EXPECTED_RENDER_PROFILE = "telegram_single_alert_normal_v1"
EXPECTED_TRANSPORT_ERROR_CODE = "telegram_retryable_5xx"
EXPECTED_TRANSPORT_ERROR_CLASS = "retryable_transport"
EXPECTED_RETRY_REASON = "due_retry_promotion"
EXPECTED_RETRY_ATTEMPT = 2
EXPECTED_OWNER = "octocat"
EXPECTED_REPO_PREFIX = "maintenance-smoke"
EXPECTED_ARTIFACT_TYPE = "github_repo"
EXPECTED_JUDGE_PROFILE = "github_primary"
EXPECTED_MODEL = "gpt-5.4-mini"
EXPECTED_REASONING_EFFORT = "low"
EXPECTED_PROMPT_VERSION = "judge_github_primary_v1"
EXPECTED_SCHEMA_VERSION = "judge_output_v1"
EXPECTED_POLICY_VERSION = "verdict_policy_v1"
EXPECTED_DELIVERY_POLICY_VERSION = "delivery_policy_v1"
EXPECTED_PROMPT_CACHE_KEY = (
    "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1"
)
EXPECTED_VERDICT = "later"
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
    "notification_plan_id",
    "notification_delivery_record_id",
    "delivery_status",
    "attempt_count",
    "transport_error_code",
    "transport_error_class",
    "telegram_response_json",
    "analysis_id",
    "candidate_group_id",
    "delivery_decision",
    "urgency_profile",
    "target_chat_id",
    "target_thread_id",
    "render_profile",
    "dedupe_subject_key",
    "material_change_hash",
    "send_after",
    "suppress_reason_code",
    "database_url",
    "redis_url",
    "password",
    "token",
    "secret",
    "api_key",
}
JSON_ROW_COLUMNS = {
    "artifact_key_json",
    "discovered_links_summary_json",
    "entities_json",
    "evidence_limitations",
    "fetch_anomalies",
    "normalized_projection",
    "payload_json",
    "primary_summary",
    "raw_message_json",
    "reason_codes_json",
    "scores_json",
    "supporting_summaries_json",
    "telegram_response_json",
}


@dataclass(slots=True)
class SmokeReport:
    report_type: str
    selected_scenario: str
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
    redis_message_ids: dict[str, str | None]
    seeded_ids: dict[str, str]
    db_postcondition_counts: dict[str, int]
    transport_safety: dict[str, Any]
    mutation_booleans: dict[str, bool]
    maintenance_output_outbox_ids: list[str]
    maintenance_output_payloads: list[dict[str, Any]]
    worker_result: dict[str, int]
    due_retry_result: dict[str, int]

    def failed(self) -> bool:
        return bool(self.checks_failed or self.failures)


@dataclass(slots=True, frozen=True)
class SmokeSeedIds:
    source_message_id: UUID
    artifact_id: UUID
    snapshot_id: UUID
    candidate_group_id: UUID
    bundle_id: UUID
    judge_run_id: UUID
    judge_output_id: UUID
    analysis_id: UUID
    notification_plan_id: UUID
    notification_delivery_record_id: UUID
    delivery_result_event_id: UUID


@dataclass(slots=True, frozen=True)
class SmokeSeedShape:
    smoke_id: str
    marker: str
    repo_name: str
    canonical_id: str
    canonical_url: str
    material_change_hash: str
    send_after: datetime
    owner: str = EXPECTED_OWNER


class SessionBackedMaintenanceService:
    def __init__(
        self,
        *,
        config: MaintenanceConfig,
        session_factory: async_sessionmaker[AsyncSession],
        logger: logging.Logger,
    ) -> None:
        self._config = config
        self._session_factory = session_factory
        self._logger = logger

    async def handle_maintenance_trigger_event(self, trigger_event_id: str) -> None:
        async with self._session_factory.begin() as session:
            service = MaintenanceService(
                self._config,
                repository=MaintenanceRepository(session),
                logger=self._logger,
            )
            await service.handle_maintenance_trigger_event(trigger_event_id)

    async def handle_replay_trigger_event(self, trigger_event_id: str) -> None:
        raise AssertionError("maintenance runtime smoke does not run replay")

    async def promote_due_retries_once(self, limit: int | None = None) -> int:
        async with self._session_factory.begin() as session:
            service = MaintenanceService(
                self._config,
                repository=MaintenanceRepository(session),
                logger=self._logger,
            )
            return await service.promote_due_retries_once(limit=limit)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run an opt-in post-Stage44 maintenance runtime smoke for "
            "notification.delivery.result.v1 -> q.maintenance. "
            f"The database URL uses --database-url or {DATABASE_URL_ENV}; Redis uses --redis-url or {REDIS_URL_ENV}."
        )
    )
    parser.add_argument("--database-url", default=None, help=f"Smoke database URL. Defaults to ${DATABASE_URL_ENV}.")
    parser.add_argument("--redis-url", default=None, help=f"Smoke Redis URL. Defaults to ${REDIS_URL_ENV}.")
    parser.add_argument("--confirm", choices=["write"], required=True)
    return parser


def _build_seed_shape(smoke_id: str | None = None) -> SmokeSeedShape:
    smoke_id = smoke_id or uuid4().hex
    short = smoke_id[:12]
    marker = f"{SMOKE_MARKER_PREFIX}{smoke_id}"
    repo_name = f"{EXPECTED_REPO_PREFIX}-{short}"
    material_change_hash = hashlib.sha256(f"maintenance-runtime:{smoke_id}".encode("utf-8")).hexdigest()
    return SmokeSeedShape(
        smoke_id=smoke_id,
        marker=marker,
        repo_name=repo_name,
        canonical_id=f"github:repo:{EXPECTED_OWNER}/{repo_name}",
        canonical_url=f"https://github.com/{EXPECTED_OWNER}/{repo_name}",
        material_change_hash=material_change_hash,
        send_after=datetime.now(UTC) - timedelta(minutes=5),
    )


def _new_report(seed_shape: SmokeSeedShape | None = None) -> SmokeReport:
    seed_shape = seed_shape or _build_seed_shape("0" * 32)
    return SmokeReport(
        report_type=REPORT_TYPE,
        selected_scenario=SELECTED_SCENARIO,
        smoke_id=seed_shape.smoke_id,
        marker=seed_shape.marker,
        checks_run=[],
        checks_passed=[],
        checks_failed=[],
        failures=[],
        warnings=[
            "This smoke must use a dev/test PostgreSQL database and local Redis DB 14.",
            "It sets APP_ENV=smoke and MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=true.",
            "It does not call OpenAI, GitHub, X, Telegram Bot API, or any external network.",
            "It does not start notifier-telegram, policy-engine, replay, or Telegram transport workers.",
            "A successful run leaves controlled marker-scoped DB rows and an acked Redis Stream entry.",
        ],
        database_url_redacted=True,
        redis_url_redacted=True,
        mutation_safety=MUTATION_SAFETY,
        queue_name=EXPECTED_QUEUE_NAME,
        redis_stream_message_id=None,
        redis_message_ids={},
        seeded_ids={},
        db_postcondition_counts={},
        transport_safety={
            "app_env": "smoke",
            "enable_notification_send": False,
            "notifier_telegram_dry_run": True,
            "maintenance_enable_notification_retry_promotion": True,
            "openai_key_required": False,
            "telegram_bot_token_required": False,
            "github_credentials_required": False,
            "x_credentials_required": False,
            "external_network_calls_attempted": False,
            "real_telegram_bot_api_call_attempted": False,
            "notifier_worker_started": False,
            "policy_engine_started": False,
            "replay_worker_started": False,
        },
        mutation_booleans={
            "notification_plan_mutated": False,
            "notification_delivery_record_mutated": False,
            "analysis_mutated": False,
            "judge_output_mutated": False,
            "candidate_group_mutated": False,
        },
        maintenance_output_outbox_ids=[],
        maintenance_output_payloads=[],
        worker_result={},
        due_retry_result={},
    )


def _mark_pass(report: SmokeReport, check_name: str) -> None:
    report.checks_run.append(check_name)
    report.checks_passed.append(check_name)


def _mark_fail(report: SmokeReport, check_name: str, message: str, *, database_url: str, redis_url: str) -> None:
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
    expected_notification_plan_id: UUID,
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
        "root_object_id": str(expected_notification_plan_id),
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


def _build_expected_redis_payload(*, event_id: UUID, notification_plan_id: UUID, marker: str) -> dict[str, str]:
    return {
        "job_id": str(event_id),
        "stage_name": EXPECTED_STAGE_NAME,
        "root_object_type": EXPECTED_AGGREGATE_TYPE,
        "root_object_id": str(notification_plan_id),
        "idempotency_key": f"{marker}:notification-delivery-result",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
    }


def _analysis_scores_json() -> dict[str, Any]:
    return {
        "novelty": 50,
        "practical_usefulness": 60,
        "evidence_strength": 45,
        "hype_penalty": 20,
        "confidence": 55,
        "code_quality": 60,
        "maintenance_signal": 50,
        "specificity": 65,
        "reproducibility_signal": None,
    }


def _build_judge_output_payload(*, seed_ids: SmokeSeedIds, seed_shape: SmokeSeedShape) -> dict[str, Any]:
    return {
        "judge_schema_version": EXPECTED_SCHEMA_VERSION,
        "candidate_group_id": str(seed_ids.candidate_group_id),
        "headline": f"Maintenance runtime smoke for {seed_shape.owner}/{seed_shape.repo_name}",
        "summary_one_line_ko": "Deterministic maintenance smoke summary.",
        "skeptical_take_ko": "Synthetic evidence only; not a production verdict.",
        "why_it_might_matter_ko": "Exercises delivery-result maintenance retry boundary.",
        "comparables": ["deterministic-runtime-smoke"],
        "scores": _analysis_scores_json(),
        "reason_codes": ["runtime_smoke_fixture"],
        "red_flags_ko": ["Synthetic fixture, not a live analysis."],
        "evidence_limitations_ko": ["Only marker-scoped synthetic rows were provided."],
        "recommended_action_ko": "Use only as a boundary smoke result.",
        "freshness_note_ko": "Generated by deterministic smoke fixture.",
        "model_proposed_verdict": EXPECTED_VERDICT,
        "model_confidence_band": "medium",
    }


def _build_delivery_result_payload(*, seed_ids: SmokeSeedIds, seed_shape: SmokeSeedShape) -> dict[str, Any]:
    return {
        "notification_plan_id": str(seed_ids.notification_plan_id),
        "notification_delivery_record_id": str(seed_ids.notification_delivery_record_id),
        "delivery_status": EXPECTED_DELIVERY_STATUS,
        "telegram_chat_id": EXPECTED_TARGET_CHAT_ID,
        "telegram_message_id": None,
        "attempt_count": 1,
        "transport_error_code": EXPECTED_TRANSPORT_ERROR_CODE,
        "transport_error_class": EXPECTED_TRANSPORT_ERROR_CLASS,
        "edited": False,
        "smoke_marker": seed_shape.marker,
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
    message_id = int(str(seed_ids.delivery_result_event_id.int)[-9:]) or 1
    text = f"Runtime smoke for {seed_shape.canonical_url}"
    raw_message_json = {
        "smoke_marker": seed_shape.marker,
        "source": "maintenance_runtime_smoke",
        "external_network_calls_allowed": False,
    }
    content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    await session.execute(
        sa.text(
            """
            INSERT INTO source_messages (
                source_message_id, platform, chat_id, message_id, message_link,
                logical_post_key, is_channel_post, posted_at, current_version_no,
                content_type, text_body, text_surface, entities_json, url_surface_json,
                raw_message_json, first_seen_at, last_seen_at
            ) VALUES (
                CAST(:source_message_id AS uuid), 'telegram', :chat_id, :message_id,
                :message_link, :logical_post_key, true, :posted_at, 1, 'text',
                :text_body, :text_surface, CAST(:entities_json AS jsonb),
                CAST(:url_surface_json AS jsonb), CAST(:raw_message_json AS jsonb),
                :first_seen_at, :last_seen_at
            )
            """
        ),
        {
            "source_message_id": str(seed_ids.source_message_id),
            "chat_id": chat_id,
            "message_id": message_id,
            "message_link": f"https://t.me/c/{abs(chat_id)}/{message_id}",
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
                source_message_id, version_no, version_reason, observed_at,
                text_surface, entities_json, raw_message_json, content_hash
            ) VALUES (
                CAST(:source_message_id AS uuid), 1, 'new', :observed_at, :text_surface,
                CAST(:entities_json AS jsonb), CAST(:raw_message_json AS jsonb), :content_hash
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
                artifact_id, artifact_type, canonical_id, canonical_url, normalized_host,
                artifact_key_json, created_at, updated_at
            ) VALUES (
                CAST(:artifact_id AS uuid), CAST(:artifact_type AS artifact_type_enum),
                :canonical_id, :canonical_url, 'github.com',
                CAST(:artifact_key_json AS jsonb), :created_at, :updated_at
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
            INSERT INTO artifact_snapshots (
                snapshot_id, artifact_id, provider, snapshot_type, status, fetched_at,
                content_anchor, auth_mode, normalized_projection, raw_payload_ref,
                evidence_limitations, fetch_anomalies
            ) VALUES (
                CAST(:snapshot_id AS uuid), CAST(:artifact_id AS uuid), 'github',
                'github_repo', 'ready'::snapshot_status_enum, :fetched_at,
                :content_anchor, 'runtime_smoke_fixture',
                CAST(:normalized_projection AS jsonb), NULL,
                CAST(:evidence_limitations AS jsonb), CAST(:fetch_anomalies AS jsonb)
            )
            """
        ),
        {
            "snapshot_id": str(seed_ids.snapshot_id),
            "artifact_id": str(seed_ids.artifact_id),
            "fetched_at": now,
            "content_anchor": hashlib.sha1(f"maintenance-runtime-smoke:{seed_shape.smoke_id}".encode("utf-8")).hexdigest(),
            "normalized_projection": json.dumps(
                {"summary": "Maintenance runtime smoke snapshot", "smoke_marker": seed_shape.marker},
                sort_keys=True,
            ),
            "evidence_limitations": json.dumps([], sort_keys=True),
            "fetch_anomalies": json.dumps([], sort_keys=True),
        },
    )
    await session.execute(
        sa.text(
            """
            UPDATE artifact_registry
            SET current_snapshot_id = CAST(:snapshot_id AS uuid),
                current_status = 'ready'::snapshot_status_enum,
                updated_at = :updated_at
            WHERE artifact_id = CAST(:artifact_id AS uuid)
            """
        ),
        {"artifact_id": str(seed_ids.artifact_id), "snapshot_id": str(seed_ids.snapshot_id), "updated_at": now},
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO candidate_group_proposals (
                candidate_group_id, source_message_id, source_version_no,
                initial_primary_artifact_id, current_primary_artifact_id,
                proposal_status, normalizer_version, dedupe_subject_key,
                created_at, updated_at
            ) VALUES (
                CAST(:candidate_group_id AS uuid), CAST(:source_message_id AS uuid), 1,
                CAST(:artifact_id AS uuid), CAST(:artifact_id AS uuid),
                'ready_for_analysis', :normalizer_version, :dedupe_subject_key,
                :created_at, :updated_at
            )
            """
        ),
        {
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "source_message_id": str(seed_ids.source_message_id),
            "artifact_id": str(seed_ids.artifact_id),
            "normalizer_version": "maintenance-runtime-smoke",
            "dedupe_subject_key": seed_shape.canonical_id,
            "created_at": now,
            "updated_at": now,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO candidate_group_members (
                candidate_group_id, artifact_id, member_role, member_order, created_at
            ) VALUES (
                CAST(:candidate_group_id AS uuid), CAST(:artifact_id AS uuid), 'primary', 0, :created_at
            )
            """
        ),
        {
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "artifact_id": str(seed_ids.artifact_id),
            "created_at": now,
        },
    )
    bundle_input_hash = hashlib.sha256(f"maintenance-bundle:{seed_shape.smoke_id}".encode("utf-8")).hexdigest()
    await session.execute(
        sa.text(
            """
            INSERT INTO candidate_evidence_bundles (
                bundle_id, candidate_group_id, initial_primary_artifact_id,
                current_primary_artifact_id, bundle_version, bundle_profile_version,
                bundle_input_hash, reroot_count, primary_summary,
                supporting_summaries_json, discovered_links_summary_json,
                evidence_limitations, ready_for_analysis, token_budget_profile, created_at
            ) VALUES (
                CAST(:bundle_id AS uuid), CAST(:candidate_group_id AS uuid),
                CAST(:artifact_id AS uuid), CAST(:artifact_id AS uuid), 1,
                'maintenance-runtime-smoke-bundle-v1', :bundle_input_hash, 0,
                CAST(:primary_summary AS jsonb), CAST(:supporting AS jsonb),
                CAST(:discovered AS jsonb), CAST(:limitations AS jsonb), true,
                'small', :created_at
            )
            """
        ),
        {
            "bundle_id": str(seed_ids.bundle_id),
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "artifact_id": str(seed_ids.artifact_id),
            "bundle_input_hash": bundle_input_hash,
            "primary_summary": json.dumps(
                {
                    "artifact_type": EXPECTED_ARTIFACT_TYPE,
                    "canonical_id": seed_shape.canonical_id,
                    "canonical_url": seed_shape.canonical_url,
                    "summary": "Deterministic maintenance runtime smoke evidence bundle.",
                    "smoke_marker": seed_shape.marker,
                },
                sort_keys=True,
            ),
            "supporting": json.dumps([], sort_keys=True),
            "discovered": json.dumps([], sort_keys=True),
            "limitations": json.dumps([], sort_keys=True),
            "created_at": now,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO candidate_evidence_members (
                bundle_id, artifact_id, snapshot_id, member_role, member_order
            ) VALUES (
                CAST(:bundle_id AS uuid), CAST(:artifact_id AS uuid),
                CAST(:snapshot_id AS uuid), 'primary', 0
            )
            """
        ),
        {
            "bundle_id": str(seed_ids.bundle_id),
            "artifact_id": str(seed_ids.artifact_id),
            "snapshot_id": str(seed_ids.snapshot_id),
        },
    )
    await session.execute(
        sa.text(
            """
            UPDATE candidate_group_proposals
            SET current_bundle_id = CAST(:bundle_id AS uuid),
                updated_at = :updated_at
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """
        ),
        {
            "bundle_id": str(seed_ids.bundle_id),
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "updated_at": now,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO judge_runs (
                judge_run_id, bundle_id, judge_profile, model, reasoning_effort,
                prompt_version, schema_version, policy_version, prompt_cache_key,
                status, schema_retry_count, input_tokens, cached_input_tokens,
                output_tokens, reasoning_tokens, latency_ms, finish_reason,
                refusal_detected, started_at, finished_at
            ) VALUES (
                CAST(:judge_run_id AS uuid), CAST(:bundle_id AS uuid), :judge_profile,
                :model, :reasoning_effort, :prompt_version, :schema_version,
                :policy_version, :prompt_cache_key, 'succeeded', 0, 123, 23,
                45, 7, 89, 'completed', false, :started_at, :finished_at
            )
            """
        ),
        {
            "judge_run_id": str(seed_ids.judge_run_id),
            "bundle_id": str(seed_ids.bundle_id),
            "judge_profile": EXPECTED_JUDGE_PROFILE,
            "model": EXPECTED_MODEL,
            "reasoning_effort": EXPECTED_REASONING_EFFORT,
            "prompt_version": EXPECTED_PROMPT_VERSION,
            "schema_version": EXPECTED_SCHEMA_VERSION,
            "policy_version": EXPECTED_POLICY_VERSION,
            "prompt_cache_key": EXPECTED_PROMPT_CACHE_KEY,
            "started_at": now,
            "finished_at": now,
        },
    )
    judge_output_payload = _build_judge_output_payload(seed_ids=seed_ids, seed_shape=seed_shape)
    await session.execute(
        sa.text(
            """
            INSERT INTO judge_outputs (
                judge_output_id, judge_run_id, candidate_group_id, judge_schema_version,
                payload_json, model_proposed_verdict, model_confidence_band, created_at
            ) VALUES (
                CAST(:judge_output_id AS uuid), CAST(:judge_run_id AS uuid),
                CAST(:candidate_group_id AS uuid), :judge_schema_version,
                CAST(:payload_json AS jsonb), :model_proposed_verdict,
                :model_confidence_band, :created_at
            )
            """
        ),
        {
            "judge_output_id": str(seed_ids.judge_output_id),
            "judge_run_id": str(seed_ids.judge_run_id),
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "judge_schema_version": EXPECTED_SCHEMA_VERSION,
            "payload_json": json.dumps(judge_output_payload, sort_keys=True),
            "model_proposed_verdict": EXPECTED_VERDICT,
            "model_confidence_band": "medium",
            "created_at": now,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO analyses (
                analysis_id, candidate_group_id, judge_output_id, schema_version,
                policy_version, prompt_version, delivery_policy_version, verdict,
                delivery_decision, scores_json, reason_codes_json,
                evidence_limitations_ko, recommended_action_ko, freshness_note_ko,
                model_proposed_verdict, policy_reconciled_flag, created_at
            ) VALUES (
                CAST(:analysis_id AS uuid), CAST(:candidate_group_id AS uuid),
                CAST(:judge_output_id AS uuid), 'analysis_v1', :policy_version,
                :prompt_version, :delivery_policy_version, CAST(:verdict AS verdict_enum),
                CAST(:delivery_decision AS delivery_decision_enum),
                CAST(:scores_json AS jsonb), CAST(:reason_codes_json AS jsonb),
                :evidence_limitations_ko, :recommended_action_ko, :freshness_note_ko,
                CAST(:model_proposed_verdict AS verdict_enum), false, :created_at
            )
            """
        ),
        {
            "analysis_id": str(seed_ids.analysis_id),
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "judge_output_id": str(seed_ids.judge_output_id),
            "policy_version": EXPECTED_POLICY_VERSION,
            "prompt_version": EXPECTED_PROMPT_VERSION,
            "delivery_policy_version": EXPECTED_DELIVERY_POLICY_VERSION,
            "verdict": EXPECTED_VERDICT,
            "delivery_decision": EXPECTED_DELIVERY_DECISION,
            "scores_json": json.dumps(_analysis_scores_json(), sort_keys=True),
            "reason_codes_json": json.dumps(["runtime_smoke_fixture"], sort_keys=True),
            "evidence_limitations_ko": "Synthetic fixture only.",
            "recommended_action_ko": "Use only as a boundary smoke result.",
            "freshness_note_ko": "Generated by deterministic smoke fixture.",
            "model_proposed_verdict": EXPECTED_VERDICT,
            "created_at": now,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO notification_plans (
                notification_plan_id, analysis_id, candidate_group_id, delivery_decision,
                urgency_profile, target_chat_id, target_thread_id, render_profile,
                dedupe_subject_key, material_change_hash, send_after,
                suppress_reason_code, status, created_at
            ) VALUES (
                CAST(:notification_plan_id AS uuid), CAST(:analysis_id AS uuid),
                CAST(:candidate_group_id AS uuid), CAST(:delivery_decision AS delivery_decision_enum),
                CAST(:urgency_profile AS urgency_profile_enum), :target_chat_id,
                NULL, :render_profile, :dedupe_subject_key, :material_change_hash,
                :send_after, NULL, 'failed_retryable'::notification_status_enum, :created_at
            )
            """
        ),
        {
            "notification_plan_id": str(seed_ids.notification_plan_id),
            "analysis_id": str(seed_ids.analysis_id),
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "delivery_decision": EXPECTED_DELIVERY_DECISION,
            "urgency_profile": EXPECTED_URGENCY_PROFILE,
            "target_chat_id": EXPECTED_TARGET_CHAT_ID,
            "render_profile": EXPECTED_RENDER_PROFILE,
            "dedupe_subject_key": str(seed_ids.candidate_group_id),
            "material_change_hash": seed_shape.material_change_hash,
            "send_after": seed_shape.send_after,
            "created_at": now,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO notification_delivery_records (
                notification_delivery_record_id, notification_plan_id, telegram_chat_id,
                telegram_message_id, delivery_status, attempt_count, transport_error_code,
                transport_error_class, telegram_response_json, created_at
            ) VALUES (
                CAST(:notification_delivery_record_id AS uuid),
                CAST(:notification_plan_id AS uuid), :telegram_chat_id, NULL,
                'failed_retryable'::notification_status_enum, 1,
                :transport_error_code, :transport_error_class,
                CAST(:telegram_response_json AS jsonb), :created_at
            )
            """
        ),
        {
            "notification_delivery_record_id": str(seed_ids.notification_delivery_record_id),
            "notification_plan_id": str(seed_ids.notification_plan_id),
            "telegram_chat_id": EXPECTED_TARGET_CHAT_ID,
            "transport_error_code": EXPECTED_TRANSPORT_ERROR_CODE,
            "transport_error_class": EXPECTED_TRANSPORT_ERROR_CLASS,
            "telegram_response_json": json.dumps(
                {"smoke_marker": seed_shape.marker, "retryable": True, "external_network_calls_allowed": False},
                sort_keys=True,
            ),
            "created_at": now,
        },
    )
    payload = _build_delivery_result_payload(seed_ids=seed_ids, seed_shape=seed_shape)
    await session.execute(
        sa.text(
            """
            INSERT INTO event_outbox (
                event_id, event_type, aggregate_type, aggregate_id, dedupe_key,
                payload_json, status, created_at
            ) VALUES (
                CAST(:event_id AS uuid), :event_type, :aggregate_type,
                CAST(:aggregate_id AS uuid), :dedupe_key, CAST(:payload_json AS jsonb),
                'pending'::outbox_status_enum, :created_at
            )
            """
        ),
        {
            "event_id": str(seed_ids.delivery_result_event_id),
            "event_type": EXPECTED_EVENT_TYPE,
            "aggregate_type": EXPECTED_AGGREGATE_TYPE,
            "aggregate_id": str(seed_ids.notification_plan_id),
            "dedupe_key": f"{seed_shape.marker}:notification-delivery-result",
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
    payload = _json_loads(row["payload_json"]) or {}
    return OutboxEventRow(
        event_id=UUID(str(row["event_id"])),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=UUID(str(row["aggregate_id"])),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload,
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
) -> tuple[str, dict[str, str], QueueRoute]:
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
    return stream_message_id, message.as_stream_fields(), route


async def _load_stream_fields(redis_client: Any, *, queue_name: str, message_id: str) -> dict[str, str] | None:
    rows = await redis_client.xrange(queue_name, min=message_id, max=message_id, count=1)
    if not rows:
        return None
    _row_id, fields = rows[0]
    return {str(key): str(value) for key, value in fields.items()}


async def _load_snapshot(session: AsyncSession, *, table: str, id_column: str, row_id: UUID) -> dict[str, Any]:
    result = await session.execute(
        sa.text(f"SELECT * FROM {table} WHERE {id_column} = CAST(:id AS uuid)"),
        {"id": str(row_id)},
    )
    return _stringify_row(result.mappings().one())


async def _load_outputs(session: AsyncSession, *, seed_ids: SmokeSeedIds) -> dict[str, Any]:
    outputs: dict[str, Any] = {}
    outputs["seed_event_status"] = await _select_scalar(
        session,
        "SELECT status FROM event_outbox WHERE event_id = CAST(:event_id AS uuid)",
        {"event_id": str(seed_ids.delivery_result_event_id)},
    )
    count_specs = {
        "notification_plan_count": (
            "SELECT COUNT(*) FROM notification_plans WHERE notification_plan_id = CAST(:id AS uuid)",
            {"id": str(seed_ids.notification_plan_id)},
        ),
        "notification_delivery_record_count": (
            "SELECT COUNT(*) FROM notification_delivery_records WHERE notification_delivery_record_id = CAST(:id AS uuid)",
            {"id": str(seed_ids.notification_delivery_record_id)},
        ),
        "notification_render_count": (
            "SELECT COUNT(*) FROM notification_renders WHERE notification_plan_id = CAST(:id AS uuid)",
            {"id": str(seed_ids.notification_plan_id)},
        ),
        "same_plan_delivery_record_count": (
            "SELECT COUNT(*) FROM notification_delivery_records WHERE notification_plan_id = CAST(:id AS uuid)",
            {"id": str(seed_ids.notification_plan_id)},
        ),
        "dead_letter_count": (
            "SELECT COUNT(*) FROM dead_letter_entries WHERE root_object_type = 'notification_plan' AND root_object_id = CAST(:id AS uuid)",
            {"id": str(seed_ids.notification_plan_id)},
        ),
        "job_attempt_count": (
            "SELECT COUNT(*) FROM job_attempts WHERE queue_name = 'q.maintenance' AND root_object_id = CAST(:id AS uuid)",
            {"id": str(seed_ids.notification_plan_id)},
        ),
        "pipeline_run_count": (
            "SELECT COUNT(*) FROM pipeline_runs WHERE root_object_type = 'notification_plan' AND root_object_id = CAST(:id AS uuid)",
            {"id": str(seed_ids.notification_plan_id)},
        ),
        "replay_request_count": (
            "SELECT COUNT(*) FROM replay_requests WHERE root_object_type = 'notification_plan' AND root_object_id = CAST(:id AS uuid)",
            {"id": str(seed_ids.notification_plan_id)},
        ),
    }
    for key, (statement, params) in count_specs.items():
        outputs[key] = int(await _select_scalar(session, statement, params) or 0)

    retry_result = await session.execute(
        sa.text(
            """
            SELECT event_id, dedupe_key, payload_json, status
            FROM event_outbox
            WHERE event_type = 'notification.plan.created.v1'
              AND aggregate_type = 'notification_plan'
              AND aggregate_id = CAST(:notification_plan_id AS uuid)
              AND payload_json ->> 'retry_reason' = :retry_reason
            ORDER BY created_at ASC, event_id ASC
            """
        ),
        {"notification_plan_id": str(seed_ids.notification_plan_id), "retry_reason": EXPECTED_RETRY_REASON},
    )
    outputs["retry_intent_rows"] = [_stringify_row(row) for row in retry_result.mappings().all()]

    job_result = await session.execute(
        sa.text(
            """
            SELECT stage_name, queue_name, root_object_type, root_object_id,
                   attempt_status, error_code
            FROM job_attempts
            WHERE queue_name = 'q.maintenance'
              AND root_object_id = CAST(:notification_plan_id AS uuid)
            ORDER BY created_at ASC
            """
        ),
        {"notification_plan_id": str(seed_ids.notification_plan_id)},
    )
    outputs["job_attempt_rows"] = [_stringify_row(row) for row in job_result.mappings().all()]
    return outputs


def _config(*, database_url: str, redis_url: str, consumer_group: str, consumer_name: str) -> MaintenanceConfig:
    return MaintenanceConfig(
        app_env="smoke",
        database_url=database_url,
        redis_url=redis_url,
        maintenance_queue_name=EXPECTED_QUEUE_NAME,
        maintenance_consumer_group=consumer_group,
        maintenance_consumer_name=consumer_name,
        replay_queue_name="q.replay",
        replay_consumer_group=f"{consumer_group}-replay-unused",
        replay_consumer_name=f"{consumer_name}-replay-unused",
        batch_size=1,
        block_ms=100,
        retry_scan_poll_sec=1,
        delivery_retry_max_attempts=3,
        enable_notification_send=False,
        notifier_telegram_dry_run=True,
        enable_delivery_retry_promotion=True,
        enable_replay_to_prod_db=False,
        delivery_gate_min_success_rate_1h=0.99,
        delivery_gate_min_success_rate_24h=0.99,
        delivery_gate_max_high_source_to_delivery_p95_sec=120.0,
        delivery_gate_max_plan_to_transport_p95_sec=120.0,
        delivery_gate_max_due_retry_lag_sec=120.0,
        delivery_gate_max_open_dlq_count=0,
        delivery_gate_max_send_disabled_count=0,
        delivery_gate_max_replay_guard_reject_count=0,
        delivery_gate_require_operator_review_for_full=True,
        log_level="CRITICAL",
    )


def _quiet_logger() -> logging.Logger:
    logger = logging.getLogger("maintenance-runtime-smoke.worker")
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False
    return logger


async def run_smoke(*, database_url: str, redis_url: str) -> SmokeReport:
    seed_shape = _build_seed_shape()
    report = _new_report(seed_shape)
    if not database_url:
        _mark_fail(
            report,
            "safety.database_url_required",
            f"--database-url or {DATABASE_URL_ENV} is required",
            database_url=database_url,
            redis_url=redis_url,
        )
        return report
    if not redis_url:
        _mark_fail(
            report,
            "safety.redis_url_required",
            f"--redis-url or {REDIS_URL_ENV} is required",
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
    _mark_pass(report, "safety.no_external_transport_configured")

    seed_ids = SmokeSeedIds(
        source_message_id=uuid4(),
        artifact_id=uuid4(),
        snapshot_id=uuid4(),
        candidate_group_id=uuid4(),
        bundle_id=uuid4(),
        judge_run_id=uuid4(),
        judge_output_id=uuid4(),
        analysis_id=uuid4(),
        notification_plan_id=uuid4(),
        notification_delivery_record_id=uuid4(),
        delivery_result_event_id=uuid4(),
    )
    report.seeded_ids = {
        "source_message_id": str(seed_ids.source_message_id),
        "artifact_id": str(seed_ids.artifact_id),
        "snapshot_id": str(seed_ids.snapshot_id),
        "candidate_group_id": str(seed_ids.candidate_group_id),
        "bundle_id": str(seed_ids.bundle_id),
        "judge_run_id": str(seed_ids.judge_run_id),
        "judge_output_id": str(seed_ids.judge_output_id),
        "analysis_id": str(seed_ids.analysis_id),
        "notification_plan_id": str(seed_ids.notification_plan_id),
        "notification_delivery_record_id": str(seed_ids.notification_delivery_record_id),
        "delivery_result_event_id": str(seed_ids.delivery_result_event_id),
        "canonical_id": seed_shape.canonical_id,
    }

    from redis.asyncio import Redis  # type: ignore[import-not-found]

    engine = create_async_engine(database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    consumer_group = f"maintenance-smoke-{seed_shape.smoke_id[:12]}"
    consumer_name = f"{consumer_group}-1"
    snapshots_before: dict[str, dict[str, Any]] = {}
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
                snapshots_before = {
                    "notification_plan": await _load_snapshot(
                        seed_session,
                        table="notification_plans",
                        id_column="notification_plan_id",
                        row_id=seed_ids.notification_plan_id,
                    ),
                    "notification_delivery_record": await _load_snapshot(
                        seed_session,
                        table="notification_delivery_records",
                        id_column="notification_delivery_record_id",
                        row_id=seed_ids.notification_delivery_record_id,
                    ),
                    "analysis": await _load_snapshot(
                        seed_session,
                        table="analyses",
                        id_column="analysis_id",
                        row_id=seed_ids.analysis_id,
                    ),
                    "judge_output": await _load_snapshot(
                        seed_session,
                        table="judge_outputs",
                        id_column="judge_output_id",
                        row_id=seed_ids.judge_output_id,
                    ),
                    "candidate_group": await _load_snapshot(
                        seed_session,
                        table="candidate_group_proposals",
                        id_column="candidate_group_id",
                        row_id=seed_ids.candidate_group_id,
                    ),
                }
        _mark_pass(report, "db.seed_rows_committed")

        async with AsyncSession(engine, expire_on_commit=False) as relay_session:
            async with relay_session.begin():
                stream_message_id, expected_fields, route = await _publish_seed_event_through_outbox_route(
                    relay_session,
                    redis_client=redis_client,
                    event_id=seed_ids.delivery_result_event_id,
                )
        report.redis_stream_message_id = stream_message_id
        report.redis_message_ids = {"published": stream_message_id}
        if route.queue_name == EXPECTED_QUEUE_NAME and route.stage_name == EXPECTED_STAGE_NAME:
            _mark_pass(report, "outbox_relay.route_published_delivery_result_to_maintenance")
        else:
            _mark_fail(
                report,
                "outbox_relay.route_published_delivery_result_to_maintenance",
                f"expected route {EXPECTED_QUEUE_NAME}/{EXPECTED_STAGE_NAME}, got {route.queue_name}/{route.stage_name}",
                database_url=database_url,
                redis_url=redis_url,
            )
            return report
        if expected_fields == _build_expected_redis_payload(
            event_id=seed_ids.delivery_result_event_id,
            notification_plan_id=seed_ids.notification_plan_id,
            marker=seed_shape.marker,
        ):
            _mark_pass(report, "outbox_relay.expected_thin_payload_shape")
        else:
            _mark_fail(
                report,
                "outbox_relay.expected_thin_payload_shape",
                "outbox relay stream fields did not match the expected maintenance thin payload",
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
            expected_event_id=seed_ids.delivery_result_event_id,
            expected_notification_plan_id=seed_ids.notification_plan_id,
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

        config = _config(
            database_url=database_url,
            redis_url=redis_url,
            consumer_group=consumer_group,
            consumer_name=consumer_name,
        )
        service = SessionBackedMaintenanceService(
            config=config,
            session_factory=session_factory,
            logger=_quiet_logger(),
        )
        consumer = RedisStreamConsumer(
            redis_client,
            queue_name=config.maintenance_queue_name,
            consumer_group=config.maintenance_consumer_group,
            consumer_name=config.maintenance_consumer_name,
            block_ms=config.block_ms,
            batch_size=config.batch_size,
        )
        worker = MaintenanceQueueWorker(
            config,
            consumer=consumer,
            service=service,
            logger=_quiet_logger(),
        )
        batch_result = await worker.run_once()
        report.worker_result = {"processed": batch_result.processed, "acked": batch_result.acked}
        if batch_result.processed == 1 and batch_result.acked == 1:
            _mark_pass(report, "maintenance.worker_consumed_and_acked_one_message")
        else:
            _mark_fail(
                report,
                "maintenance.worker_consumed_and_acked_one_message",
                f"expected processed=1 acked=1, got processed={batch_result.processed} acked={batch_result.acked}",
                database_url=database_url,
                redis_url=redis_url,
            )

        due_retry_worker = DueRetryPromotionWorker(config, service=service, logger=_quiet_logger())
        due_result = await due_retry_worker.run_once()
        report.due_retry_result = {"processed": due_result.processed, "acked": due_result.acked}
        _mark_pass(report, "maintenance.due_retry_promoter_pass_completed")

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

        async with AsyncSession(engine, expire_on_commit=False) as verify_session:
            outputs = await _load_outputs(verify_session, seed_ids=seed_ids)
            snapshots_after = {
                "notification_plan": await _load_snapshot(
                    verify_session,
                    table="notification_plans",
                    id_column="notification_plan_id",
                    row_id=seed_ids.notification_plan_id,
                ),
                "notification_delivery_record": await _load_snapshot(
                    verify_session,
                    table="notification_delivery_records",
                    id_column="notification_delivery_record_id",
                    row_id=seed_ids.notification_delivery_record_id,
                ),
                "analysis": await _load_snapshot(
                    verify_session,
                    table="analyses",
                    id_column="analysis_id",
                    row_id=seed_ids.analysis_id,
                ),
                "judge_output": await _load_snapshot(
                    verify_session,
                    table="judge_outputs",
                    id_column="judge_output_id",
                    row_id=seed_ids.judge_output_id,
                ),
                "candidate_group": await _load_snapshot(
                    verify_session,
                    table="candidate_group_proposals",
                    id_column="candidate_group_id",
                    row_id=seed_ids.candidate_group_id,
                ),
            }

        report.db_postcondition_counts = {
            "notification_plans": int(outputs["notification_plan_count"]),
            "notification_delivery_records": int(outputs["same_plan_delivery_record_count"]),
            "notification_renders": int(outputs["notification_render_count"]),
            "job_attempts_q_maintenance": int(outputs["job_attempt_count"]),
            "pipeline_runs": int(outputs["pipeline_run_count"]),
            "dead_letter_entries": int(outputs["dead_letter_count"]),
            "replay_requests": int(outputs["replay_request_count"]),
            "notification_plan_created_retry_intents": len(outputs.get("retry_intent_rows") or []),
        }
        retry_rows = outputs.get("retry_intent_rows") or []
        report.maintenance_output_outbox_ids = [str(row["event_id"]) for row in retry_rows]
        report.maintenance_output_payloads = [row.get("payload_json") or {} for row in retry_rows]
        _verify_outputs(report, outputs=outputs, database_url=database_url, redis_url=redis_url)

        report.mutation_booleans = {
            "notification_plan_mutated": snapshots_before["notification_plan"] != snapshots_after["notification_plan"],
            "notification_delivery_record_mutated": snapshots_before["notification_delivery_record"]
            != snapshots_after["notification_delivery_record"],
            "analysis_mutated": snapshots_before["analysis"] != snapshots_after["analysis"],
            "judge_output_mutated": snapshots_before["judge_output"] != snapshots_after["judge_output"],
            "candidate_group_mutated": snapshots_before["candidate_group"] != snapshots_after["candidate_group"],
        }
        mutation_checks = {
            "db.notification_plans_not_mutated": "notification_plan_mutated",
            "db.notification_delivery_records_not_mutated": "notification_delivery_record_mutated",
            "db.analyses_not_mutated": "analysis_mutated",
            "db.judge_outputs_not_mutated": "judge_output_mutated",
            "db.candidate_group_proposals_not_mutated": "candidate_group_mutated",
        }
        for check_name, key in mutation_checks.items():
            if not report.mutation_booleans[key]:
                _mark_pass(report, check_name)
            else:
                _mark_fail(
                    report,
                    check_name,
                    f"{key} was true after maintenance processing",
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


def _verify_outputs(report: SmokeReport, *, outputs: dict[str, Any], database_url: str, redis_url: str) -> None:
    if str(outputs.get("seed_event_status")) == "published":
        _mark_pass(report, "db.seed_notification_delivery_result_outbox_published")
    else:
        _mark_fail(
            report,
            "db.seed_notification_delivery_result_outbox_published",
            f"expected seeded notification.delivery.result.v1 status published, got {outputs.get('seed_event_status')!r}",
            database_url=database_url,
            redis_url=redis_url,
        )

    expected_counts = {
        "db.exactly_one_seed_notification_plan": ("notification_plan_count", 1),
        "db.exactly_one_seed_notification_delivery_record": ("notification_delivery_record_count", 1),
        "db.no_notification_render_created_by_maintenance": ("notification_render_count", 0),
        "db.no_second_notification_delivery_record_created_by_maintenance": ("same_plan_delivery_record_count", 1),
        "db.no_dead_letter_for_retryable_due_path": ("dead_letter_count", 0),
        "db.no_replay_request_created": ("replay_request_count", 0),
    }
    for check_name, (key, expected) in expected_counts.items():
        actual = int(outputs.get(key) or 0)
        if actual == expected:
            _mark_pass(report, check_name)
        else:
            _mark_fail(
                report,
                check_name,
                f"expected {expected} for {key}, got {actual}",
                database_url=database_url,
                redis_url=redis_url,
            )

    retry_rows = outputs.get("retry_intent_rows") or []
    if len(retry_rows) == 1 and retry_rows[0].get("status") == "pending":
        _mark_pass(report, "db.exactly_one_pending_notification_plan_created_retry_intent")
    else:
        _mark_fail(
            report,
            "db.exactly_one_pending_notification_plan_created_retry_intent",
            f"expected one pending retry intent, got {retry_rows}",
            database_url=database_url,
            redis_url=redis_url,
        )
        return

    payload = retry_rows[0].get("payload_json") or {}
    required_payload = {
        "notification_plan_id",
        "analysis_id",
        "candidate_group_id",
        "delivery_decision",
        "urgency_profile",
        "target_chat_id",
        "target_thread_id",
        "render_profile",
        "dedupe_subject_key",
        "material_change_hash",
        "send_after",
        "suppress_reason_code",
        "retry_reason",
        "retry_attempt",
    }
    if required_payload <= set(payload):
        _mark_pass(report, "db.retry_intent_payload_has_existing_contract_fields")
    else:
        _mark_fail(
            report,
            "db.retry_intent_payload_has_existing_contract_fields",
            f"missing retry intent payload fields: {sorted(required_payload - set(payload))}",
            database_url=database_url,
            redis_url=redis_url,
        )
    expected_values = {
        "delivery_decision": EXPECTED_DELIVERY_DECISION,
        "urgency_profile": EXPECTED_URGENCY_PROFILE,
        "target_chat_id": EXPECTED_TARGET_CHAT_ID,
        "render_profile": EXPECTED_RENDER_PROFILE,
        "retry_reason": EXPECTED_RETRY_REASON,
        "retry_attempt": EXPECTED_RETRY_ATTEMPT,
        "send_after": None,
        "suppress_reason_code": None,
    }
    mismatches = {
        key: {"expected": expected, "actual": payload.get(key)}
        for key, expected in expected_values.items()
        if payload.get(key) != expected
    }
    if not mismatches:
        _mark_pass(report, "db.retry_intent_payload_matches_existing_contract")
    else:
        _mark_fail(
            report,
            "db.retry_intent_payload_matches_existing_contract",
            f"retry intent payload mismatches: {mismatches}",
            database_url=database_url,
            redis_url=redis_url,
        )
    dedupe_key = str(retry_rows[0].get("dedupe_key") or "")
    if dedupe_key.startswith("notify:retry-intent:") and str(payload.get("notification_plan_id")) in dedupe_key:
        _mark_pass(report, "db.retry_intent_dedupe_key_stable_and_plan_scoped")
    else:
        _mark_fail(
            report,
            "db.retry_intent_dedupe_key_stable_and_plan_scoped",
            f"unexpected retry intent dedupe_key: {dedupe_key!r}",
            database_url=database_url,
            redis_url=redis_url,
        )


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    return str(value)


def _stringify_row(row: Mapping[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in row.items():
        key_name = str(key)
        if isinstance(value, UUID):
            output[key_name] = str(value)
        elif isinstance(value, datetime):
            output[key_name] = value.isoformat()
        elif key_name in JSON_ROW_COLUMNS:
            output[key_name] = _json_loads(value)
        else:
            output[key_name] = value
    return output


def _render_json(report: SmokeReport) -> str:
    return json.dumps(asdict(report), ensure_ascii=False, indent=2, sort_keys=False, default=_json_default)


async def _amain(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    database_url = (args.database_url or os.getenv(DATABASE_URL_ENV, "")).strip()
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
