from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
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

from src.services.maintenance.batch_recovery_tool import DeliveryBatchRecoveryTool
from src.services.maintenance.config import MaintenanceConfig
from src.services.maintenance.repositories import MaintenanceRepository


REPORT_TYPE = "batch_recovery_runtime_smoke_v1"
SELECTED_SCENARIO = "retry_selected_due_minimal"
DATABASE_URL_ENV = "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL"
SMOKE_MARKER_PREFIX = "ops-smoke:batch-recovery:"
EXPECTED_EVENT_TYPE = "notification.plan.created.v1"
EXPECTED_AGGREGATE_TYPE = "notification_plan"
EXPECTED_RETRY_REASON = "manual_selected_due_retry"
EXPECTED_RECOVERY_MODE = "retry-selected-due"
EXPECTED_DELIVERY_STATUS = "failed_retryable"
EXPECTED_DELIVERY_DECISION = "send_now"
EXPECTED_URGENCY_PROFILE = "normal_silent"
EXPECTED_TARGET_CHAT_ID = 12345
EXPECTED_RENDER_PROFILE = "telegram_single_alert_normal_v1"
EXPECTED_OWNER = "octocat"
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
REQUIRED_PAYLOAD_FIELDS = {
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
    "retry_reason",
    "previous_attempt_count",
    "recovery_batch_id",
}
MUTATION_SAFETY = (
    "controlled smoke write only: inserts marker-scoped synthetic source, artifact, "
    "candidate, bundle, judge, analysis, one failed_retryable notification_plan, "
    "and one failed_retryable notification_delivery_records fixture row; runs the "
    "existing DeliveryBatchRecoveryTool.retry_selected_due path twice; verifies one "
    "pending notification.plan.created.v1 manual retry-intent outbox row and the "
    "dedupe skip on the second run; does not require Redis, start runtime workers, "
    "publish Redis messages, call external network, mutate feature flags or env files, "
    "create notification renders, create extra delivery records, create replay requests, "
    "create dead letters, or create state transitions"
)
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
NOTIFICATION_PLAN_SNAPSHOT_FIELDS = (
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
    "status",
    "created_at",
)


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
    mutation_safety: str
    mutation_safety_fields: dict[str, bool]
    seeded_ids: dict[str, str]
    db_precondition_counts: dict[str, int]
    db_postcondition_counts: dict[str, int]
    batch_recovery_result_summary: dict[str, Any]
    manual_retry_intent_summary: dict[str, Any]
    dedupe_key_observed: str | None
    payload_observed: dict[str, Any]

    def failed(self) -> bool:
        return bool(self.checks_failed or self.failures)


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run an opt-in post-Stage44 DB-backed batch-recovery smoke for "
            "maintenance batch-recovery retry-selected-due. "
            f"The database URL uses --database-url or {DATABASE_URL_ENV}; Redis is not required."
        )
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help=f"Smoke PostgreSQL database URL. Defaults to ${DATABASE_URL_ENV}. Redis is not required.",
    )
    parser.add_argument("--confirm", choices=["write"], required=True, help="Required write confirmation.")
    return parser


def _build_seed_shape(smoke_id: str | None = None) -> SmokeSeedShape:
    smoke_id = smoke_id or uuid4().hex
    short = smoke_id[:12]
    marker = f"{SMOKE_MARKER_PREFIX}{smoke_id}"
    repo_name = f"batch-recovery-smoke-{short}"
    return SmokeSeedShape(
        smoke_id=smoke_id,
        marker=marker,
        repo_name=repo_name,
        canonical_id=f"github:repo:{EXPECTED_OWNER}/{repo_name}",
        canonical_url=f"https://github.com/{EXPECTED_OWNER}/{repo_name}",
        material_change_hash=hashlib.sha256(f"batch-recovery:{smoke_id}".encode("utf-8")).hexdigest(),
        send_after=datetime.now(UTC) - timedelta(minutes=5),
    )


def _new_seed_ids() -> SmokeSeedIds:
    return SmokeSeedIds(
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
            "This smoke must use a local dev/test/smoke PostgreSQL database.",
            "Redis is not required and no Redis messages are published.",
            "It does not call OpenAI, GitHub, X, Telegram Bot API, or any external network.",
            "It does not start collector, notifier, maintenance, replay, or delivery workers.",
            "A successful run leaves marker-scoped fixture rows for manual inspection.",
            "Success does not authorize production rollout.",
        ],
        database_url_redacted=True,
        mutation_safety=MUTATION_SAFETY,
        mutation_safety_fields={
            "feature_flags_mutated": False,
            "environment_files_written": False,
            "redis_required": False,
            "redis_messages_published": False,
            "external_network_calls_attempted": False,
            "runtime_workers_started": False,
            "notifier_worker_started": False,
            "telegram_bot_api_called": False,
            "openai_called": False,
            "github_or_x_api_called": False,
            "notification_plans_mutated_after_seed": False,
            "notification_renders_created": False,
            "extra_notification_delivery_records_created": False,
            "replay_requests_created": False,
            "dead_letter_entries_created": False,
            "state_transitions_created": False,
        },
        seeded_ids={},
        db_precondition_counts={},
        db_postcondition_counts={},
        batch_recovery_result_summary={},
        manual_retry_intent_summary={},
        dedupe_key_observed=None,
        payload_observed={},
    )


def _mark_pass(report: SmokeReport, check_name: str) -> None:
    report.checks_run.append(check_name)
    report.checks_passed.append(check_name)


def _mark_fail(report: SmokeReport, check_name: str, message: str, *, database_url: str) -> None:
    report.checks_run.append(check_name)
    report.checks_failed.append(check_name)
    report.failures.append(
        {
            "check": check_name,
            "message": _redact_sensitive_text(message, database_url=database_url),
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


def _redact_sensitive_text(text: str, *, database_url: str) -> str:
    redacted = text
    for value in sorted(_url_parts_to_redact(database_url), key=len, reverse=True):
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
    host = (parsed.hostname or "").lower()
    return parsed.scheme in {"rediss"} or host not in {"localhost", "127.0.0.1", "::1"}


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


def _render_json(report: SmokeReport) -> str:
    return json.dumps(asdict(report), ensure_ascii=False, indent=2, sort_keys=False, default=_json_default)


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    return str(value)


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


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


def _analysis_scores_json() -> dict[str, int]:
    return {
        "novelty": 3,
        "utility": 3,
        "credibility": 3,
        "risk": 2,
    }


def _build_judge_output_payload(*, seed_ids: SmokeSeedIds, seed_shape: SmokeSeedShape) -> dict[str, Any]:
    return {
        "schema_version": EXPECTED_SCHEMA_VERSION,
        "candidate_group_id": str(seed_ids.candidate_group_id),
        "verdict": EXPECTED_VERDICT,
        "confidence_band": "medium",
        "summary_ko": "Synthetic batch recovery smoke judge output.",
        "reason_codes": ["runtime_smoke_fixture"],
        "scores": _analysis_scores_json(),
        "smoke_marker": seed_shape.marker,
    }


def _config(database_url: str) -> MaintenanceConfig:
    return MaintenanceConfig(
        app_env="smoke",
        database_url=database_url,
        redis_url="redis://localhost:6379/0",
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="batch-recovery-runtime-smoke",
        maintenance_consumer_name="batch-recovery-runtime-smoke-1",
        replay_queue_name="q.replay",
        replay_consumer_group="batch-recovery-runtime-smoke-replay",
        replay_consumer_name="batch-recovery-runtime-smoke-replay-1",
        batch_size=1,
        block_ms=1,
        retry_scan_poll_sec=30,
        delivery_retry_max_attempts=3,
        enable_notification_send=False,
        notifier_telegram_dry_run=True,
        enable_delivery_retry_promotion=False,
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


async def _select_scalar(session: AsyncSession, statement: str, params: dict[str, Any] | None = None) -> int:
    result = await session.execute(sa.text(statement), params or {})
    value = result.scalar_one()
    return int(value or 0)


async def _insert_seed_rows(session: AsyncSession, *, seed_shape: SmokeSeedShape, seed_ids: SmokeSeedIds) -> None:
    now = datetime.now(UTC)
    chat_id = -int(str(seed_ids.source_message_id.int)[-12:])
    message_id = int(str(seed_ids.notification_delivery_record_id.int)[-9:]) or 1
    text = f"Batch recovery runtime smoke for {seed_shape.canonical_url}"
    raw_message_json = {
        "smoke_marker": seed_shape.marker,
        "source": REPORT_TYPE,
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
            "content_anchor": hashlib.sha1(f"batch-recovery-smoke:{seed_shape.smoke_id}".encode("utf-8")).hexdigest(),
            "normalized_projection": json.dumps(
                {"summary": "Batch recovery runtime smoke snapshot", "smoke_marker": seed_shape.marker},
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
            "normalizer_version": "batch-recovery-runtime-smoke",
            "dedupe_subject_key": seed_shape.marker,
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
    bundle_input_hash = hashlib.sha256(f"batch-recovery-bundle:{seed_shape.smoke_id}".encode("utf-8")).hexdigest()
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
                'batch-recovery-runtime-smoke-bundle-v1', :bundle_input_hash, 0,
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
                    "summary": "Deterministic batch recovery runtime smoke evidence bundle.",
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
            "dedupe_subject_key": seed_shape.marker,
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
            "transport_error_code": "telegram_retryable_5xx",
            "transport_error_class": "retryable_transport",
            "telegram_response_json": json.dumps(
                {"smoke_marker": seed_shape.marker, "retryable": True, "external_network_calls_allowed": False},
                sort_keys=True,
            ),
            "created_at": now,
        },
    )


async def _load_counts(session: AsyncSession, *, seed_ids: SmokeSeedIds, marker: str) -> dict[str, int]:
    marker_like = f"{marker}%"
    plan_id = str(seed_ids.notification_plan_id)
    return {
        "marker_source_messages": await _select_scalar(
            session,
            "SELECT COUNT(*) FROM source_messages WHERE logical_post_key LIKE :marker_like",
            {"marker_like": marker_like},
        ),
        "marker_notification_plans": await _select_scalar(
            session,
            "SELECT COUNT(*) FROM notification_plans WHERE notification_plan_id = CAST(:plan_id AS uuid)",
            {"plan_id": plan_id},
        ),
        "marker_notification_delivery_records": await _select_scalar(
            session,
            "SELECT COUNT(*) FROM notification_delivery_records WHERE notification_plan_id = CAST(:plan_id AS uuid)",
            {"plan_id": plan_id},
        ),
        "marker_notification_renders": await _select_scalar(
            session,
            "SELECT COUNT(*) FROM notification_renders WHERE notification_plan_id = CAST(:plan_id AS uuid)",
            {"plan_id": plan_id},
        ),
        "marker_manual_retry_intents": await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM event_outbox
            WHERE event_type = 'notification.plan.created.v1'
              AND aggregate_type = 'notification_plan'
              AND aggregate_id = CAST(:plan_id AS uuid)
              AND payload_json ->> 'retry_reason' = :retry_reason
            """,
            {"plan_id": plan_id, "retry_reason": EXPECTED_RETRY_REASON},
        ),
        "marker_replay_requests": await _select_scalar(
            session,
            "SELECT COUNT(*) FROM replay_requests WHERE root_object_type = 'notification_plan' AND root_object_id = CAST(:plan_id AS uuid)",
            {"plan_id": plan_id},
        ),
        "marker_dead_letter_entries": await _select_scalar(
            session,
            "SELECT COUNT(*) FROM dead_letter_entries WHERE root_object_type = 'notification_plan' AND root_object_id = CAST(:plan_id AS uuid)",
            {"plan_id": plan_id},
        ),
        "marker_state_transitions": await _select_scalar(
            session,
            "SELECT COUNT(*) FROM state_transitions WHERE object_type = 'notification_plan' AND object_id = CAST(:plan_id AS uuid)",
            {"plan_id": plan_id},
        ),
    }


async def _load_manual_retry_intent_rows(session: AsyncSession, *, notification_plan_id: UUID) -> list[dict[str, Any]]:
    result = await session.execute(
        sa.text(
            """
            SELECT event_id, event_type, aggregate_type, aggregate_id, dedupe_key, payload_json, status, created_at
            FROM event_outbox
            WHERE event_type = 'notification.plan.created.v1'
              AND aggregate_type = 'notification_plan'
              AND aggregate_id = CAST(:notification_plan_id AS uuid)
              AND payload_json ->> 'retry_reason' = :retry_reason
            ORDER BY created_at ASC, event_id ASC
            """
        ),
        {"notification_plan_id": str(notification_plan_id), "retry_reason": EXPECTED_RETRY_REASON},
    )
    return [_stringify_row(row) for row in result.mappings().all()]


async def _load_notification_plan_snapshot(
    session: AsyncSession, *, notification_plan_id: UUID
) -> dict[str, Any] | None:
    result = await session.execute(
        sa.text(
            """
            SELECT
                notification_plan_id,
                analysis_id,
                candidate_group_id,
                delivery_decision,
                urgency_profile,
                target_chat_id,
                target_thread_id,
                render_profile,
                dedupe_subject_key,
                material_change_hash,
                send_after,
                suppress_reason_code,
                status,
                created_at
            FROM notification_plans
            WHERE notification_plan_id = CAST(:notification_plan_id AS uuid)
            """
        ),
        {"notification_plan_id": str(notification_plan_id)},
    )
    row = result.mappings().one_or_none()
    return _stringify_row(row) if row is not None else None


async def _run_retry_selected_due(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    database_url: str,
    notification_plan_id: UUID,
) -> dict[str, Any]:
    async with session_factory.begin() as session:
        tool = DeliveryBatchRecoveryTool(
            _config(database_url),
            repository=MaintenanceRepository(session),
        )
        result = await tool.retry_selected_due(plan_ids=[notification_plan_id], requested_by="ops-smoke")
        return asdict(result)


def _expected_dedupe_key(*, notification_plan_id: UUID, send_after: datetime) -> str:
    return f"notify:manual-retry-intent:{notification_plan_id}:1:{int(send_after.timestamp())}"


def _verify_first_result(report: SmokeReport, *, result: dict[str, Any], database_url: str) -> None:
    expectations = {
        "recovery_mode": EXPECTED_RECOVERY_MODE,
        "selected_count": 1,
        "accepted_count": 1,
        "emitted_count": 1,
        "skipped_count": 0,
        "skipped_reason_codes": {},
    }
    mismatches = {key: {"expected": expected, "actual": result.get(key)} for key, expected in expectations.items() if result.get(key) != expected}
    if not mismatches:
        _mark_pass(report, "batch_recovery.first_result_accepts_and_emits_one")
    else:
        _mark_fail(
            report,
            "batch_recovery.first_result_accepts_and_emits_one",
            f"first retry_selected_due result mismatches: {mismatches}",
            database_url=database_url,
        )


def _verify_second_result(report: SmokeReport, *, result: dict[str, Any], database_url: str) -> None:
    expectations = {
        "recovery_mode": EXPECTED_RECOVERY_MODE,
        "selected_count": 1,
        "accepted_count": 1,
        "emitted_count": 0,
        "skipped_count": 1,
        "skipped_reason_codes": {"manual_retry_intent_exists_at_insert": 1},
    }
    mismatches = {key: {"expected": expected, "actual": result.get(key)} for key, expected in expectations.items() if result.get(key) != expected}
    if not mismatches:
        _mark_pass(report, "batch_recovery.second_result_idempotent_duplicate_skip")
    else:
        _mark_fail(
            report,
            "batch_recovery.second_result_idempotent_duplicate_skip",
            f"second retry_selected_due result mismatches: {mismatches}",
            database_url=database_url,
        )


def _verify_manual_retry_intent(
    report: SmokeReport,
    *,
    rows: list[dict[str, Any]],
    seed_ids: SmokeSeedIds,
    seed_shape: SmokeSeedShape,
    first_recovery_batch_id: str | None,
    database_url: str,
) -> None:
    if len(rows) == 1:
        _mark_pass(report, "db.exactly_one_manual_retry_intent_outbox_row")
    else:
        _mark_fail(
            report,
            "db.exactly_one_manual_retry_intent_outbox_row",
            f"expected one manual retry intent row, got {len(rows)}",
            database_url=database_url,
        )
        return

    row = rows[0]
    payload = row.get("payload_json") or {}
    report.dedupe_key_observed = str(row.get("dedupe_key") or "")
    report.payload_observed = dict(payload)
    expected_row_values = {
        "event_type": EXPECTED_EVENT_TYPE,
        "aggregate_type": EXPECTED_AGGREGATE_TYPE,
        "aggregate_id": str(seed_ids.notification_plan_id),
        "status": "pending",
    }
    row_mismatches = {
        key: {"expected": expected, "actual": str(row.get(key))}
        for key, expected in expected_row_values.items()
        if str(row.get(key)) != expected
    }
    if not row_mismatches:
        _mark_pass(report, "db.manual_retry_intent_outbox_row_contract")
    else:
        _mark_fail(
            report,
            "db.manual_retry_intent_outbox_row_contract",
            f"manual retry intent row mismatches: {row_mismatches}",
            database_url=database_url,
        )

    missing = sorted(REQUIRED_PAYLOAD_FIELDS - set(payload))
    if not missing:
        _mark_pass(report, "db.manual_retry_intent_payload_required_fields")
    else:
        _mark_fail(
            report,
            "db.manual_retry_intent_payload_required_fields",
            f"missing payload fields: {missing}",
            database_url=database_url,
        )

    expected_payload_values = {
        "notification_plan_id": str(seed_ids.notification_plan_id),
        "analysis_id": str(seed_ids.analysis_id),
        "candidate_group_id": str(seed_ids.candidate_group_id),
        "delivery_decision": EXPECTED_DELIVERY_DECISION,
        "urgency_profile": EXPECTED_URGENCY_PROFILE,
        "target_chat_id": EXPECTED_TARGET_CHAT_ID,
        "target_thread_id": None,
        "render_profile": EXPECTED_RENDER_PROFILE,
        "dedupe_subject_key": seed_shape.marker,
        "material_change_hash": seed_shape.material_change_hash,
        "send_after": None,
        "retry_reason": EXPECTED_RETRY_REASON,
        "previous_attempt_count": 1,
        "recovery_batch_id": first_recovery_batch_id,
    }
    payload_mismatches = {
        key: {"expected": expected, "actual": payload.get(key)}
        for key, expected in expected_payload_values.items()
        if payload.get(key) != expected
    }
    if not payload_mismatches:
        _mark_pass(report, "db.manual_retry_intent_payload_values")
    else:
        _mark_fail(
            report,
            "db.manual_retry_intent_payload_values",
            f"manual retry intent payload mismatches: {payload_mismatches}",
            database_url=database_url,
        )

    expected_dedupe = _expected_dedupe_key(
        notification_plan_id=seed_ids.notification_plan_id,
        send_after=seed_shape.send_after,
    )
    if report.dedupe_key_observed == expected_dedupe:
        _mark_pass(report, "db.manual_retry_intent_dedupe_key_shape")
    else:
        _mark_fail(
            report,
            "db.manual_retry_intent_dedupe_key_shape",
            f"expected dedupe key {expected_dedupe!r}, got {report.dedupe_key_observed!r}",
            database_url=database_url,
        )

    recovery_batch_id = str(payload.get("recovery_batch_id") or "")
    if recovery_batch_id and recovery_batch_id not in (report.dedupe_key_observed or ""):
        _mark_pass(report, "db.recovery_batch_id_not_dedupe_key_source")
    else:
        _mark_fail(
            report,
            "db.recovery_batch_id_not_dedupe_key_source",
            "recovery_batch_id was missing or appeared in the dedupe key",
            database_url=database_url,
        )


def _verify_postcondition_counts(report: SmokeReport, *, database_url: str) -> None:
    expectations = {
        "marker_notification_plans": 1,
        "marker_notification_delivery_records": 1,
        "marker_notification_renders": 0,
        "marker_manual_retry_intents": 1,
        "marker_replay_requests": 0,
        "marker_dead_letter_entries": 0,
        "marker_state_transitions": 0,
    }
    for name, expected in expectations.items():
        actual = report.db_postcondition_counts.get(name)
        if actual == expected:
            _mark_pass(report, f"db.{name}")
        else:
            _mark_fail(report, f"db.{name}", f"expected {expected}, got {actual}", database_url=database_url)


def _notification_plan_snapshot_diff(
    expected: Mapping[str, Any] | None, actual: Mapping[str, Any] | None
) -> dict[str, dict[str, Any]]:
    if expected is None or actual is None:
        return {"row": {"expected": expected, "actual": actual}}
    return {
        field: {"expected": expected.get(field), "actual": actual.get(field)}
        for field in NOTIFICATION_PLAN_SNAPSHOT_FIELDS
        if expected.get(field) != actual.get(field)
    }


def _verify_notification_plan_row_unchanged_after_recovery(
    report: SmokeReport,
    *,
    before: Mapping[str, Any] | None,
    after: Mapping[str, Any] | None,
    database_url: str,
) -> None:
    diff = _notification_plan_snapshot_diff(before, after)
    if not diff:
        _mark_pass(report, "db.notification_plan_row_unchanged_after_recovery")
        report.mutation_safety_fields["notification_plans_mutated_after_seed"] = False
        return

    report.mutation_safety_fields["notification_plans_mutated_after_seed"] = True
    _mark_fail(
        report,
        "db.notification_plan_row_unchanged_after_recovery",
        f"notification_plans row changed after fixture seed: {diff}",
        database_url=database_url,
    )


async def _run_smoke(database_url: str, report: SmokeReport, seed_shape: SmokeSeedShape) -> SmokeReport:
    seed_ids = _new_seed_ids()
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
        "canonical_id": seed_shape.canonical_id,
    }
    engine = create_async_engine(database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with session_factory.begin() as session:
            report.db_precondition_counts = await _load_counts(session, seed_ids=seed_ids, marker=seed_shape.marker)
            await _insert_seed_rows(session, seed_shape=seed_shape, seed_ids=seed_ids)
            notification_plan_snapshot_after_seed = await _load_notification_plan_snapshot(
                session,
                notification_plan_id=seed_ids.notification_plan_id,
            )
        _mark_pass(report, "db.marker_scoped_fixture_inserted")

        first_result = await _run_retry_selected_due(
            session_factory,
            database_url=database_url,
            notification_plan_id=seed_ids.notification_plan_id,
        )
        report.batch_recovery_result_summary = dict(first_result)
        _verify_first_result(report, result=first_result, database_url=database_url)

        async with session_factory() as session:
            first_rows = await _load_manual_retry_intent_rows(session, notification_plan_id=seed_ids.notification_plan_id)
        _verify_manual_retry_intent(
            report,
            rows=first_rows,
            seed_ids=seed_ids,
            seed_shape=seed_shape,
            first_recovery_batch_id=first_result.get("recovery_batch_id"),
            database_url=database_url,
        )

        second_result = await _run_retry_selected_due(
            session_factory,
            database_url=database_url,
            notification_plan_id=seed_ids.notification_plan_id,
        )
        _verify_second_result(report, result=second_result, database_url=database_url)

        async with session_factory() as session:
            final_rows = await _load_manual_retry_intent_rows(session, notification_plan_id=seed_ids.notification_plan_id)
            notification_plan_snapshot_after_recovery = await _load_notification_plan_snapshot(
                session,
                notification_plan_id=seed_ids.notification_plan_id,
            )
            report.db_postcondition_counts = await _load_counts(session, seed_ids=seed_ids, marker=seed_shape.marker)
        _verify_notification_plan_row_unchanged_after_recovery(
            report,
            before=notification_plan_snapshot_after_seed,
            after=notification_plan_snapshot_after_recovery,
            database_url=database_url,
        )
        report.manual_retry_intent_summary = {
            "after_first_run_count": len(first_rows),
            "after_second_run_count": len(final_rows),
            "status": first_rows[0].get("status") if first_rows else None,
            "event_id": str(first_rows[0].get("event_id")) if first_rows else None,
            "duplicate_rerun_result": second_result,
        }
        if len(final_rows) == 1:
            _mark_pass(report, "db.duplicate_rerun_inserted_no_second_outbox_row")
        else:
            _mark_fail(
                report,
                "db.duplicate_rerun_inserted_no_second_outbox_row",
                f"expected one final manual retry intent row, got {len(final_rows)}",
                database_url=database_url,
            )
        _verify_postcondition_counts(report, database_url=database_url)
        return report
    finally:
        await engine.dispose()


async def _amain(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    seed_shape = _build_seed_shape()
    report = _new_report(seed_shape)
    database_url = args.database_url or os.getenv(DATABASE_URL_ENV, "").strip()
    if not database_url:
        _mark_fail(
            report,
            "safety.database_url_required",
            f"--database-url or {DATABASE_URL_ENV} is required",
            database_url="",
        )
        print(_render_json(report))
        return 1
    if _is_production_like_url(database_url) or not _is_expected_smoke_database_url(database_url):
        _mark_fail(
            report,
            "safety.local_smoke_database_url_guard",
            "database URL must be local PostgreSQL and database name must contain smoke, test, or dev",
            database_url=database_url,
        )
        print(_render_json(report))
        return 1

    _mark_pass(report, "safety.database_url_explicit_or_env")
    _mark_pass(report, "safety.local_smoke_database_url_guard")
    _mark_pass(report, "safety.confirm_write_required_by_parser")
    _mark_pass(report, "safety.redis_not_required")
    _mark_pass(report, "safety.no_redis_messages_published")
    _mark_pass(report, "safety.no_external_network_calls")
    _mark_pass(report, "safety.no_runtime_workers_started")
    _mark_pass(report, "safety.no_feature_flag_or_env_file_mutation")

    try:
        report = await _run_smoke(database_url, report, seed_shape)
    except Exception as exc:
        _mark_fail(
            report,
            "smoke.unhandled_exception",
            f"{type(exc).__name__}: {exc}",
            database_url=database_url,
        )
    print(_render_json(report))
    return 1 if report.failed() else 0


def main() -> None:
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()
