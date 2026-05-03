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

from src.services.analysis_validator.config import AnalysisValidatorConfig
from src.services.analysis_validator.redis_streams import RedisStreamConsumer
from src.services.analysis_validator.repositories import AnalysisValidatorRepository
from src.services.analysis_validator.service import AnalysisValidatorService
from src.services.analysis_validator.worker import AnalysisValidatorWorker
from src.services.outbox_relay.models import OutboxEventRow, QueueRoute, RedisQueuedMessage
from src.services.outbox_relay.redis_streams import RedisStreamsPublisher
from src.services.outbox_relay.repositories import OutboxRelayRepository
from src.services.outbox_relay.routing import OutboxRouteResolver


REPORT_TYPE = "analysis_validator_runtime_smoke_v1"
DATABASE_URL_ENV = "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL"
REDIS_URL_ENV = "REDIS_URL"
MUTATION_SAFETY = (
    "controlled smoke write only: inserts marker-scoped synthetic source, artifact, snapshot, "
    "candidate, ready evidence bundle/member, succeeded judge_run, one valid judge_output, and "
    "pending judge.output.ready.v1 outbox row; publishes that event through the outbox relay route "
    "to q.analysis.validate; runs one analysis-validator worker pass; and leaves one pending "
    "analysis.policy.apply.v1 outbox row plus an analysis_validated state transition"
)
SMOKE_MARKER_PREFIX = "ops-smoke:analysis-validator-runtime:"
EXPECTED_QUEUE_NAME = "q.analysis.validate"
EXPECTED_STAGE_NAME = "analysis_validate"
EXPECTED_REDIS_DB = 14
EXPECTED_EVENT_TYPE = "judge.output.ready.v1"
EXPECTED_DOWNSTREAM_EVENT_TYPE = "analysis.policy.apply.v1"
EXPECTED_AGGREGATE_TYPE = "judge_run"
EXPECTED_ARTIFACT_TYPE = "github_repo"
EXPECTED_OWNER = "octocat"
EXPECTED_REPO_PREFIX = "analysis-validator-smoke"
EXPECTED_JUDGE_PROFILE = "github_primary"
EXPECTED_MODEL = "gpt-5.4-mini"
EXPECTED_REASONING_EFFORT = "low"
EXPECTED_PROMPT_VERSION = "judge_github_primary_v1"
EXPECTED_SCHEMA_VERSION = "judge_output_v1"
EXPECTED_POLICY_VERSION = "verdict_policy_v1"
EXPECTED_PROMPT_CACHE_KEY = (
    "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1"
)
EXPECTED_BUNDLE_PROFILE_VERSION = "analysis-validator-runtime-smoke-bundle-v1"
EXPECTED_TOKEN_BUDGET_PROFILE = "small"
EXPECTED_FINISH_REASON = "completed"
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
    "judge_run_id",
    "judge_output_id",
    "bundle_id",
    "candidate_group_id",
    "finish_reason",
    "refusal_detected",
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
    downstream_outbox_ids: list[str]
    redis_message_ids: dict[str, str | None]
    db_postcondition_counts: dict[str, int]
    forbidden_side_effect_counts: dict[str, int]
    judge_output_mutated: bool
    external_network_calls_attempted: bool

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
    judge_output_ready_event_id: UUID


@dataclass(slots=True, frozen=True)
class SmokeSeedShape:
    smoke_id: str
    marker: str
    repo_name: str
    canonical_id: str
    canonical_url: str
    content_anchor: str
    bundle_input_hash: str
    owner: str = EXPECTED_OWNER


class SessionBackedAnalysisValidatorService:
    def __init__(
        self,
        *,
        config: AnalysisValidatorConfig,
        session_factory: async_sessionmaker[AsyncSession],
        logger: logging.Logger,
    ) -> None:
        self._config = config
        self._session_factory = session_factory
        self._logger = logger

    async def handle_trigger_event(self, trigger_event_id: str) -> None:
        async with self._session_factory.begin() as session:
            repository = AnalysisValidatorRepository(session)
            service = AnalysisValidatorService(self._config, repository=repository, logger=self._logger)
            await service.handle_trigger_event(trigger_event_id)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run an opt-in post-Stage44 analysis-validator runtime smoke. "
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
    content_anchor = hashlib.sha1(f"analysis-validator-runtime-smoke:{smoke_id}".encode("utf-8")).hexdigest()
    bundle_input_hash = hashlib.sha256(f"analysis-validator-bundle:{smoke_id}".encode("utf-8")).hexdigest()
    return SmokeSeedShape(
        smoke_id=smoke_id,
        marker=marker,
        repo_name=repo_name,
        canonical_id=f"github:repo:{EXPECTED_OWNER}/{repo_name}",
        canonical_url=f"https://github.com/{EXPECTED_OWNER}/{repo_name}",
        content_anchor=content_anchor,
        bundle_input_hash=bundle_input_hash,
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
            "It does not call OpenAI, external network, Telegram, notifier transport, or source enrichers.",
            "A successful run leaves controlled marker-scoped DB rows and an acked Redis Stream entry.",
        ],
        database_url_redacted=True,
        redis_url_redacted=True,
        mutation_safety=MUTATION_SAFETY,
        queue_name=EXPECTED_QUEUE_NAME,
        redis_stream_message_id=None,
        seeded_ids={},
        downstream_outbox_ids=[],
        redis_message_ids={},
        db_postcondition_counts={},
        forbidden_side_effect_counts={},
        judge_output_mutated=False,
        external_network_calls_attempted=False,
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
    expected_judge_run_id: UUID,
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
        "root_object_id": str(expected_judge_run_id),
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


def _build_expected_redis_payload(*, event_id: UUID, judge_run_id: UUID, marker: str) -> dict[str, str]:
    return {
        "job_id": str(event_id),
        "stage_name": EXPECTED_STAGE_NAME,
        "root_object_type": EXPECTED_AGGREGATE_TYPE,
        "root_object_id": str(judge_run_id),
        "idempotency_key": f"{marker}:judge-output-ready",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
    }


def _build_judge_output_ready_payload(*, seed_ids: SmokeSeedIds, seed_shape: SmokeSeedShape) -> dict[str, Any]:
    return {
        "judge_run_id": str(seed_ids.judge_run_id),
        "judge_output_id": str(seed_ids.judge_output_id),
        "finish_reason": EXPECTED_FINISH_REASON,
        "refusal_detected": False,
        "smoke_marker": seed_shape.marker,
    }


def _build_valid_judge_output_payload(*, seed_ids: SmokeSeedIds, seed_shape: SmokeSeedShape) -> dict[str, Any]:
    return {
        "judge_schema_version": EXPECTED_SCHEMA_VERSION,
        "candidate_group_id": str(seed_ids.candidate_group_id),
        "headline": f"Runtime smoke validator output for {seed_shape.owner}/{seed_shape.repo_name}",
        "summary_one_line_ko": "Deterministic validator smoke summary.",
        "skeptical_take_ko": "Synthetic evidence only; no production verdict.",
        "why_it_might_matter_ko": "Exercises analysis-validator schema and policy handoff.",
        "comparables": ["deterministic-runtime-smoke"],
        "scores": {
            "novelty": 40,
            "practical_usefulness": 60,
            "evidence_strength": 70,
            "hype_penalty": 10,
            "confidence": 65,
            "code_quality": 55,
            "maintenance_signal": 50,
            "specificity": 75,
            "reproducibility_signal": None,
        },
        "reason_codes": ["runtime_smoke_fixture"],
        "red_flags_ko": ["Synthetic fixture, not a live analysis."],
        "evidence_limitations_ko": ["Only marker-scoped candidate_evidence_bundles data was provided."],
        "recommended_action_ko": "Use only as a boundary smoke result.",
        "freshness_note_ko": "Generated by deterministic smoke fixture.",
        "model_proposed_verdict": "later",
        "model_confidence_band": "medium",
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
    message_id = int(str(seed_ids.judge_output_ready_event_id.int)[-9:]) or 1
    text = f"Runtime smoke for {seed_shape.canonical_url}"
    raw_message_json = {
        "smoke_marker": seed_shape.marker,
        "source": "analysis_validator_runtime_smoke",
        "external_network_calls_allowed": False,
    }
    content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    await session.execute(
        sa.text(
            """
            INSERT INTO source_messages (
                source_message_id, platform, chat_id, message_id, logical_post_key,
                is_channel_post, posted_at, current_version_no, content_type,
                text_body, text_surface, entities_json, url_surface_json,
                raw_message_json, first_seen_at, last_seen_at
            ) VALUES (
                CAST(:source_message_id AS uuid), 'telegram', :chat_id, :message_id,
                :logical_post_key, true, :posted_at, 1, 'text', :text_body, :text_surface,
                CAST(:entities_json AS jsonb), CAST(:url_surface_json AS jsonb),
                CAST(:raw_message_json AS jsonb), :first_seen_at, :last_seen_at
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
    normalized_projection = {
        "title": f"{seed_shape.owner}/{seed_shape.repo_name}",
        "description": "Deterministic analysis-validator runtime smoke repository fixture",
        "language": "Python",
        "stars": 42,
        "topics": ["ai", "runtime-smoke"],
        "smoke_marker": seed_shape.marker,
    }
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
            "content_anchor": seed_shape.content_anchor,
            "normalized_projection": json.dumps(normalized_projection, sort_keys=True),
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
                'ready_for_enrich', :normalizer_version, :dedupe_subject_key,
                :created_at, :updated_at
            )
            """
        ),
        {
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "source_message_id": str(seed_ids.source_message_id),
            "artifact_id": str(seed_ids.artifact_id),
            "normalizer_version": "analysis-validator-runtime-smoke",
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
    primary_summary = {
        "artifact_type": EXPECTED_ARTIFACT_TYPE,
        "canonical_id": seed_shape.canonical_id,
        "canonical_url": seed_shape.canonical_url,
        "repo_full_name": f"{seed_shape.owner}/{seed_shape.repo_name}",
        "summary": "Deterministic GitHub repository summary for analysis-validator runtime smoke.",
        "source_table": "candidate_evidence_bundles",
        "smoke_marker": seed_shape.marker,
    }
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
                :bundle_profile_version, :bundle_input_hash, 0,
                CAST(:primary_summary AS jsonb), CAST(:supporting AS jsonb),
                CAST(:discovered AS jsonb), CAST(:limitations AS jsonb), true,
                :token_budget_profile, :created_at
            )
            """
        ),
        {
            "bundle_id": str(seed_ids.bundle_id),
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "artifact_id": str(seed_ids.artifact_id),
            "bundle_profile_version": EXPECTED_BUNDLE_PROFILE_VERSION,
            "bundle_input_hash": seed_shape.bundle_input_hash,
            "primary_summary": json.dumps(primary_summary, sort_keys=True),
            "supporting": json.dumps([], sort_keys=True),
            "discovered": json.dumps([], sort_keys=True),
            "limitations": json.dumps([], sort_keys=True),
            "token_budget_profile": EXPECTED_TOKEN_BUDGET_PROFILE,
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
                45, 7, 89, :finish_reason, false, :started_at, :finished_at
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
            "finish_reason": EXPECTED_FINISH_REASON,
            "started_at": now,
            "finished_at": now,
        },
    )
    judge_output_payload = _build_valid_judge_output_payload(seed_ids=seed_ids, seed_shape=seed_shape)
    await session.execute(
        sa.text(
            """
            INSERT INTO judge_outputs (
                judge_output_id, judge_run_id, candidate_group_id, judge_schema_version,
                payload_json, model_proposed_verdict, model_confidence_band, created_at
            ) VALUES (
                CAST(:judge_output_id AS uuid),
                CAST(:judge_run_id AS uuid),
                CAST(:candidate_group_id AS uuid),
                :judge_schema_version,
                CAST(:payload_json AS jsonb),
                :model_proposed_verdict,
                :model_confidence_band,
                :created_at
            )
            """
        ),
        {
            "judge_output_id": str(seed_ids.judge_output_id),
            "judge_run_id": str(seed_ids.judge_run_id),
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "judge_schema_version": EXPECTED_SCHEMA_VERSION,
            "payload_json": json.dumps(judge_output_payload, sort_keys=True),
            "model_proposed_verdict": judge_output_payload["model_proposed_verdict"],
            "model_confidence_band": judge_output_payload["model_confidence_band"],
            "created_at": now,
        },
    )
    ready_payload = _build_judge_output_ready_payload(seed_ids=seed_ids, seed_shape=seed_shape)
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
            "event_id": str(seed_ids.judge_output_ready_event_id),
            "event_type": EXPECTED_EVENT_TYPE,
            "aggregate_type": EXPECTED_AGGREGATE_TYPE,
            "aggregate_id": str(seed_ids.judge_run_id),
            "dedupe_key": f"{seed_shape.marker}:judge-output-ready",
            "payload_json": json.dumps(ready_payload, sort_keys=True),
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


async def _load_judge_output_snapshot(session: AsyncSession, *, judge_output_id: UUID) -> dict[str, Any]:
    result = await session.execute(
        sa.text(
            """
            SELECT judge_output_id, judge_run_id, candidate_group_id, judge_schema_version,
                   payload_json, model_proposed_verdict, model_confidence_band, created_at
            FROM judge_outputs
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
            """
        ),
        {"judge_output_id": str(judge_output_id)},
    )
    row = result.mappings().one()
    return _normalize_judge_output_snapshot_row(row)


def _normalize_judge_output_snapshot_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "judge_output_id": str(row["judge_output_id"]),
        "judge_run_id": str(row["judge_run_id"]),
        "candidate_group_id": str(row["candidate_group_id"]),
        "judge_schema_version": str(row["judge_schema_version"]),
        "payload_json": _json_loads(row["payload_json"]),
        "model_proposed_verdict": row["model_proposed_verdict"],
        "model_confidence_band": row["model_confidence_band"],
        "created_at": row["created_at"],
    }


async def _load_outputs(session: AsyncSession, *, seed_ids: SmokeSeedIds) -> dict[str, Any]:
    outputs: dict[str, Any] = {}
    run_result = await session.execute(
        sa.text(
            """
            SELECT judge_run_id, bundle_id, status, finish_reason, refusal_detected,
                   started_at, finished_at
            FROM judge_runs
            WHERE judge_run_id = CAST(:judge_run_id AS uuid)
            """
        ),
        {"judge_run_id": str(seed_ids.judge_run_id)},
    )
    outputs["judge_run"] = dict(run_result.mappings().one())
    outputs["judge_output"] = await _load_judge_output_snapshot(session, judge_output_id=seed_ids.judge_output_id)

    transition_result = await session.execute(
        sa.text(
            """
            SELECT to_state, reason_code
            FROM state_transitions
            WHERE object_type = 'judge_run'
              AND object_id = CAST(:judge_run_id AS uuid)
            ORDER BY created_at ASC
            """
        ),
        {"judge_run_id": str(seed_ids.judge_run_id)},
    )
    outputs["state_transitions"] = [dict(row) for row in transition_result.mappings().all()]

    downstream_result = await session.execute(
        sa.text(
            """
            SELECT event_id, status, aggregate_id, payload_json
            FROM event_outbox
            WHERE event_type = 'analysis.policy.apply.v1'
              AND aggregate_id = CAST(:judge_run_id AS uuid)
            ORDER BY created_at ASC
            """
        ),
        {"judge_run_id": str(seed_ids.judge_run_id)},
    )
    outputs["downstream_outbox"] = [dict(row) for row in downstream_result.mappings().all()]
    outputs["judge_output_ready_status"] = await _select_scalar(
        session,
        """
        SELECT status
        FROM event_outbox
        WHERE event_id = CAST(:event_id AS uuid)
        """,
        {"event_id": str(seed_ids.judge_output_ready_event_id)},
    )
    outputs["analysis_count"] = int(
        await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM analyses
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
               OR judge_output_id = CAST(:judge_output_id AS uuid)
            """,
            {
                "candidate_group_id": str(seed_ids.candidate_group_id),
                "judge_output_id": str(seed_ids.judge_output_id),
            },
        )
        or 0
    )
    outputs["notification_plan_count"] = int(
        await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM notification_plans
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """,
            {"candidate_group_id": str(seed_ids.candidate_group_id)},
        )
        or 0
    )
    outputs["notification_render_count"] = int(
        await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM notification_renders r
            JOIN notification_plans p ON p.notification_plan_id = r.notification_plan_id
            WHERE p.candidate_group_id = CAST(:candidate_group_id AS uuid)
            """,
            {"candidate_group_id": str(seed_ids.candidate_group_id)},
        )
        or 0
    )
    outputs["notification_delivery_count"] = int(
        await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM notification_delivery_records d
            JOIN notification_plans p ON p.notification_plan_id = d.notification_plan_id
            WHERE p.candidate_group_id = CAST(:candidate_group_id AS uuid)
            """,
            {"candidate_group_id": str(seed_ids.candidate_group_id)},
        )
        or 0
    )
    return outputs


def _quiet_logger() -> logging.Logger:
    logger = logging.getLogger("analysis-validator-runtime-smoke.worker")
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False
    return logger


def _config(*, database_url: str, redis_url: str, consumer_group: str, consumer_name: str) -> AnalysisValidatorConfig:
    return AnalysisValidatorConfig(
        app_env="smoke",
        database_url=database_url,
        redis_url=redis_url,
        queue_name=EXPECTED_QUEUE_NAME,
        consumer_group=consumer_group,
        consumer_name=consumer_name,
        batch_size=1,
        block_ms=100,
        max_headline_chars=200,
        max_summary_chars=1200,
        max_text_items=10,
        log_level="CRITICAL",
    )


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

    seed_ids = SmokeSeedIds(
        source_message_id=uuid4(),
        artifact_id=uuid4(),
        snapshot_id=uuid4(),
        candidate_group_id=uuid4(),
        bundle_id=uuid4(),
        judge_run_id=uuid4(),
        judge_output_id=uuid4(),
        judge_output_ready_event_id=uuid4(),
    )
    report.seeded_ids = {
        "source_message_id": str(seed_ids.source_message_id),
        "artifact_id": str(seed_ids.artifact_id),
        "snapshot_id": str(seed_ids.snapshot_id),
        "candidate_group_id": str(seed_ids.candidate_group_id),
        "bundle_id": str(seed_ids.bundle_id),
        "judge_run_id": str(seed_ids.judge_run_id),
        "judge_output_id": str(seed_ids.judge_output_id),
        "judge_output_ready_event_id": str(seed_ids.judge_output_ready_event_id),
        "canonical_id": seed_shape.canonical_id,
    }

    from redis.asyncio import Redis  # type: ignore[import-not-found]

    engine = create_async_engine(database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    consumer_group = f"analysis-validator-smoke-{seed_shape.smoke_id[:12]}"
    consumer_name = f"{consumer_group}-1"
    before_judge_output: dict[str, Any] | None = None
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
                before_judge_output = await _load_judge_output_snapshot(
                    seed_session,
                    judge_output_id=seed_ids.judge_output_id,
                )
        _mark_pass(report, "db.seed_rows_committed")

        async with AsyncSession(engine, expire_on_commit=False) as relay_session:
            async with relay_session.begin():
                stream_message_id, expected_fields, route = await _publish_seed_event_through_outbox_route(
                    relay_session,
                    redis_client=redis_client,
                    event_id=seed_ids.judge_output_ready_event_id,
                )
        report.redis_stream_message_id = stream_message_id
        report.redis_message_ids = {"published": stream_message_id}
        if route.queue_name == EXPECTED_QUEUE_NAME and route.stage_name == EXPECTED_STAGE_NAME:
            _mark_pass(report, "outbox_relay.route_published_judge_output_ready_to_analysis_validate")
        else:
            _mark_fail(
                report,
                "outbox_relay.route_published_judge_output_ready_to_analysis_validate",
                f"expected route {EXPECTED_QUEUE_NAME}/{EXPECTED_STAGE_NAME}, got {route.queue_name}/{route.stage_name}",
                database_url=database_url,
                redis_url=redis_url,
            )
            return report

        if expected_fields == _build_expected_redis_payload(
            event_id=seed_ids.judge_output_ready_event_id,
            judge_run_id=seed_ids.judge_run_id,
            marker=seed_shape.marker,
        ):
            _mark_pass(report, "outbox_relay.expected_thin_payload_shape")
        else:
            _mark_fail(
                report,
                "outbox_relay.expected_thin_payload_shape",
                "outbox relay stream fields did not match the expected analysis-validator thin payload",
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
            expected_event_id=seed_ids.judge_output_ready_event_id,
            expected_judge_run_id=seed_ids.judge_run_id,
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
        consumer = RedisStreamConsumer(
            redis_client,
            queue_name=config.queue_name,
            consumer_group=config.consumer_group,
            consumer_name=config.consumer_name,
            block_ms=config.block_ms,
            batch_size=config.batch_size,
        )
        service = SessionBackedAnalysisValidatorService(
            config=config,
            session_factory=session_factory,
            logger=_quiet_logger(),
        )
        worker = AnalysisValidatorWorker(
            config,
            consumer=consumer,
            service=service,  # type: ignore[arg-type]
            logger=_quiet_logger(),
        )
        batch_result = await worker.run_once()
        if batch_result.processed == 1 and batch_result.acked == 1:
            _mark_pass(report, "analysis_validator.worker_consumed_and_acked_one_message")
        else:
            _mark_fail(
                report,
                "analysis_validator.worker_consumed_and_acked_one_message",
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

        if report.external_network_calls_attempted is False:
            _mark_pass(report, "network.no_external_network_call")
        else:
            _mark_fail(
                report,
                "network.no_external_network_call",
                "external network calls were attempted",
                database_url=database_url,
                redis_url=redis_url,
            )

        async with AsyncSession(engine, expire_on_commit=False) as verify_session:
            outputs = await _load_outputs(verify_session, seed_ids=seed_ids)

        if str(outputs.get("judge_output_ready_status")) == "published":
            _mark_pass(report, "db.judge_output_ready_outbox_published")
        else:
            _mark_fail(
                report,
                "db.judge_output_ready_outbox_published",
                f"expected seeded judge.output.ready.v1 to be published, got {outputs.get('judge_output_ready_status')!r}",
                database_url=database_url,
                redis_url=redis_url,
            )

        judge_run = outputs["judge_run"]
        if judge_run["status"] == "succeeded" and judge_run["finish_reason"] == EXPECTED_FINISH_REASON:
            _mark_pass(report, "db.judge_run_remained_validator_accepted")
        else:
            _mark_fail(
                report,
                "db.judge_run_remained_validator_accepted",
                f"judge_run did not remain in expected validator-accepted state: {judge_run}",
                database_url=database_url,
                redis_url=redis_url,
            )

        transitions = outputs.get("state_transitions") or []
        validated_transitions = [
            row
            for row in transitions
            if row.get("to_state") == "analysis_validated" and row.get("reason_code") == "validator_passed"
        ]
        if len(validated_transitions) == 1:
            _mark_pass(report, "db.analysis_validated_state_transition_exists")
        else:
            _mark_fail(
                report,
                "db.analysis_validated_state_transition_exists",
                f"expected one analysis_validated transition, got {transitions}",
                database_url=database_url,
                redis_url=redis_url,
            )

        downstream = outputs.get("downstream_outbox") or []
        report.downstream_outbox_ids = [str(row["event_id"]) for row in downstream]
        pending_downstream = [row for row in downstream if row["status"] == "pending"]
        if len(pending_downstream) == 1:
            _mark_pass(report, "db.analysis_policy_apply_outbox_pending_exists")
        else:
            _mark_fail(
                report,
                "db.analysis_policy_apply_outbox_pending_exists",
                f"expected one pending analysis.policy.apply.v1 outbox row, got {len(pending_downstream)}",
                database_url=database_url,
                redis_url=redis_url,
            )

        policy_payload_ok = False
        if pending_downstream:
            payload = _json_loads(pending_downstream[0]["payload_json"]) or {}
            policy_payload_ok = payload == {
                "judge_run_id": str(seed_ids.judge_run_id),
                "judge_output_id": str(seed_ids.judge_output_id),
                "candidate_group_id": str(seed_ids.candidate_group_id),
                "bundle_id": str(seed_ids.bundle_id),
            }
        if policy_payload_ok:
            _mark_pass(report, "db.analysis_policy_apply_payload_contract")
        else:
            _mark_fail(
                report,
                "db.analysis_policy_apply_payload_contract",
                "analysis.policy.apply.v1 payload did not match validator repository contract",
                database_url=database_url,
                redis_url=redis_url,
            )

        after_judge_output = outputs["judge_output"]
        report.judge_output_mutated = before_judge_output != after_judge_output
        if not report.judge_output_mutated:
            _mark_pass(report, "db.judge_outputs_not_mutated")
        else:
            _mark_fail(
                report,
                "db.judge_outputs_not_mutated",
                "judge_outputs row changed during analysis-validator processing",
                database_url=database_url,
                redis_url=redis_url,
            )

        side_effect_counts = {
            "analyses": int(outputs.get("analysis_count") or 0),
            "notification_plans": int(outputs.get("notification_plan_count") or 0),
            "notification_renders": int(outputs.get("notification_render_count") or 0),
            "notification_delivery_records": int(outputs.get("notification_delivery_count") or 0),
        }
        report.forbidden_side_effect_counts = side_effect_counts
        report.db_postcondition_counts = {
            "state_transitions": len(transitions),
            "analysis_validated_transitions": len(validated_transitions),
            "analysis_policy_apply_pending": len(pending_downstream),
            **side_effect_counts,
        }
        if all(count == 0 for count in side_effect_counts.values()):
            _mark_pass(report, "db.no_forbidden_downstream_side_effects")
        else:
            _mark_fail(
                report,
                "db.no_forbidden_downstream_side_effects",
                f"unexpected downstream side-effect rows found: {side_effect_counts}",
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


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _render_json(report: SmokeReport) -> str:
    return json.dumps(asdict(report), ensure_ascii=False, indent=2, sort_keys=False)


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
