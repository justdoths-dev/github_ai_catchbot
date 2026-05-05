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
from typing import Any
from urllib.parse import quote, unquote, urlsplit
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.maintenance.config import MaintenanceConfig
from src.services.maintenance.delivery_gate_runner import DeliveryGateRunner
from src.services.maintenance.repositories import MaintenanceRepository


REPORT_TYPE = "delivery_gate_full_runtime_smoke_v1"
SELECTED_SCENARIO = "full_pass_with_operator_review"
DATABASE_URL_ENV = "GITHUB_AI_CATCHBOT_DB_SMOKE_DATABASE_URL"
SMOKE_MARKER_PREFIX = "ops-smoke:delivery-gate-full:"
EXPECTED_GATE_MODE = "full"
EXPECTED_GATE_STATUS = "pass"
EXPECTED_OPERATOR_REVIEW_WARNING = "delivery_gate_operator_review_required"
EXPECTED_RECOMMENDED_FLAG_PATCH = {
    "ENABLE_NOTIFICATION_SEND": True,
    "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION": True,
    "NOTIFIER_TELEGRAM_DRY_RUN": False,
}
FULL_METRIC_ORDER_EXPECTED = [
    "success_rate_1h",
    "high_source_to_delivery_p95_sec",
    "plan_to_transport_p95_sec",
    "due_retry_oldest_lag_sec",
    "open_delivery_dlq_count",
    "unexpected_send_disabled_count",
    "success_rate_24h",
    "replay_guard_reject_count_24h",
    "retry_ceiling_exceeded_count_24h",
    "oldest_delivery_dlq_age_sec",
    "duplicate_noop_ratio_1h",
]
MUTATION_SAFETY = (
    "controlled marker-scoped fixture seeding only: inserts synthetic source, artifact, "
    "candidate, bundle, judge, analysis, one sent notification_plan, and one sent "
    "notification_delivery_records row; then runs the existing DeliveryGateRunner twice "
    "in full mode with operator_review_passed true and false; the gate path is "
    "read/report-only after fixture seeding and does not require Redis, start runtime "
    "workers, publish Redis messages, call external network, mutate feature flags or env "
    "files, apply recommended_flag_patch, emit event_outbox rows, create notification "
    "renders, create extra delivery records, create dead letters, create replay requests, "
    "or create state transitions"
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
    gate_report_summary: dict[str, Any]
    metric_order_expected: list[str]
    metric_order_observed: list[str]
    full_metric_names_observed: list[str]
    recommended_flag_patch: dict[str, Any]

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
    target_chat_id: int
    source_posted_at: datetime
    plan_created_at: datetime
    delivered_at: datetime
    owner: str = "octocat"


@dataclass(slots=True, frozen=True)
class SmokeSeedIds:
    source_message_id: UUID
    artifact_id: UUID
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
            "Run an opt-in DB-backed delivery-gate full-mode smoke for operator review. "
            f"The database URL uses --database-url or {DATABASE_URL_ENV}; Redis is not required."
        )
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help=f"Smoke PostgreSQL database URL. Defaults to ${DATABASE_URL_ENV}. Redis is not required.",
    )
    return parser


def _build_seed_shape(smoke_id: str | None = None) -> SmokeSeedShape:
    smoke_id = smoke_id or uuid4().hex
    short = smoke_id[:12]
    marker = f"{SMOKE_MARKER_PREFIX}{smoke_id}"
    repo_name = f"delivery-gate-full-smoke-{short}"
    now = datetime.now(UTC)
    source_posted_at = now - timedelta(seconds=45)
    plan_created_at = now - timedelta(seconds=20)
    delivered_at = now - timedelta(seconds=5)
    return SmokeSeedShape(
        smoke_id=smoke_id,
        marker=marker,
        repo_name=repo_name,
        canonical_id=f"github:repo:octocat/{repo_name}",
        canonical_url=f"https://github.com/octocat/{repo_name}",
        material_change_hash=hashlib.sha256(f"delivery-gate-full:{smoke_id}".encode("utf-8")).hexdigest(),
        target_chat_id=-int(str(uuid4().int)[-11:]),
        source_posted_at=source_posted_at,
        plan_created_at=plan_created_at,
        delivered_at=delivered_at,
    )


def _new_seed_ids() -> SmokeSeedIds:
    return SmokeSeedIds(
        source_message_id=uuid4(),
        artifact_id=uuid4(),
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
            "The full-mode operator review pass and missing-review warn branches are both verified.",
            "recommended_flag_patch is output-only and is not applied.",
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
            "recommended_flag_patch_applied": False,
            "event_outbox_rows_emitted_by_gate_runner": False,
            "notification_render_rows_created_by_gate_runner": False,
            "notification_delivery_rows_created_by_gate_runner": False,
            "dead_letter_rows_created_by_gate_runner": False,
            "replay_request_rows_created_by_gate_runner": False,
            "state_transition_rows_created_by_gate_runner": False,
        },
        seeded_ids={},
        db_precondition_counts={},
        db_postcondition_counts={},
        gate_report_summary={},
        metric_order_expected=list(FULL_METRIC_ORDER_EXPECTED),
        metric_order_observed=[],
        full_metric_names_observed=[],
        recommended_flag_patch={},
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
    return json.dumps(asdict(report), ensure_ascii=False, default=str, indent=2, sort_keys=False)


def _delivery_gate_config(database_url: str) -> MaintenanceConfig:
    return MaintenanceConfig(
        app_env="smoke",
        database_url=database_url,
        redis_url="redis://localhost:6379/0",
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="delivery-gate-full-runtime-smoke",
        maintenance_consumer_name="delivery-gate-full-runtime-smoke-1",
        replay_queue_name="q.replay",
        replay_consumer_group="delivery-gate-full-runtime-smoke-replay",
        replay_consumer_name="delivery-gate-full-runtime-smoke-replay-1",
        batch_size=1,
        block_ms=1,
        retry_scan_poll_sec=30,
        delivery_retry_max_attempts=3,
        enable_notification_send=True,
        notifier_telegram_dry_run=False,
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
        log_level="WARNING",
    )


async def _select_scalar(session: AsyncSession, statement: str, params: dict[str, Any] | None = None) -> int:
    result = await session.execute(sa.text(statement), params or {})
    value = result.scalar_one()
    return int(value or 0)


async def _load_precondition_counts(session: AsyncSession) -> dict[str, int]:
    marker_like = f"{SMOKE_MARKER_PREFIX}%"
    return {
        "non_marker_recent_delivery_records_24h": await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM notification_delivery_records ndr
            JOIN notification_plans np
              ON np.notification_plan_id = ndr.notification_plan_id
            WHERE ndr.created_at >= now() - interval '24 hours'
              AND np.dedupe_subject_key NOT LIKE :marker_like
            """,
            {"marker_like": marker_like},
        ),
        "non_marker_due_retry_plans": await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM notification_plans
            WHERE status = 'failed_retryable'::notification_status_enum
              AND send_after IS NOT NULL
              AND send_after <= now()
              AND dedupe_subject_key NOT LIKE :marker_like
            """,
            {"marker_like": marker_like},
        ),
        "open_delivery_dlq_count": await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM dead_letter_entries
            WHERE root_object_type = 'notification_plan'
              AND queue_name IN ('q.notification.send', 'q.maintenance', 'q.replay')
            """,
        ),
        "replay_guard_reject_count_24h": await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM replay_requests
            WHERE replay_type = 'delivery'::replay_type_enum
              AND root_object_type = 'notification_plan'
              AND status = 'rejected_by_env_guard'
              AND requested_at >= now() - interval '24 hours'
            """,
        ),
        "retry_ceiling_exceeded_count_24h": await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM dead_letter_entries
            WHERE root_object_type = 'notification_plan'
              AND last_error_code = 'max_notification_retry_attempts_exceeded'
              AND last_failed_at >= now() - interval '24 hours'
            """,
        ),
        "unexpected_send_disabled_count": await _select_scalar(
            session,
            """
            WITH latest_delivery AS (
                SELECT DISTINCT ON (ndr.notification_plan_id)
                       ndr.delivery_status,
                       ndr.telegram_response_json,
                       np.dedupe_subject_key
                FROM notification_delivery_records ndr
                JOIN notification_plans np
                  ON np.notification_plan_id = ndr.notification_plan_id
                ORDER BY ndr.notification_plan_id, ndr.created_at DESC
            )
            SELECT COUNT(*)
            FROM latest_delivery
            WHERE delivery_status = 'suppressed'::notification_status_enum
              AND lower(telegram_response_json ->> 'send_disabled') = 'true'
              AND dedupe_subject_key NOT LIKE :marker_like
            """,
            {"marker_like": marker_like},
        ),
    }


async def _load_marker_postcondition_counts(session: AsyncSession, seed_ids: SmokeSeedIds) -> dict[str, int]:
    marker_like = f"{SMOKE_MARKER_PREFIX}%"
    plan_id = str(seed_ids.notification_plan_id)
    return {
        "marker_source_messages": await _select_scalar(
            session,
            "SELECT COUNT(*) FROM source_messages WHERE logical_post_key LIKE :marker_like",
            {"marker_like": marker_like},
        ),
        "marker_notification_plans": await _select_scalar(
            session,
            "SELECT COUNT(*) FROM notification_plans WHERE dedupe_subject_key LIKE :marker_like",
            {"marker_like": marker_like},
        ),
        "marker_notification_delivery_records": await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM notification_delivery_records
            WHERE notification_plan_id = CAST(:plan_id AS uuid)
            """,
            {"plan_id": plan_id},
        ),
        "marker_notification_renders": await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM notification_renders nr
            JOIN notification_plans np
              ON np.notification_plan_id = nr.notification_plan_id
            WHERE np.dedupe_subject_key LIKE :marker_like
            """,
            {"marker_like": marker_like},
        ),
        "marker_event_outbox": await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM event_outbox
            WHERE dedupe_key LIKE :marker_like
               OR payload_json::text LIKE :marker_text_like
            """,
            {"marker_like": marker_like, "marker_text_like": f"%{SMOKE_MARKER_PREFIX}%"},
        ),
        "marker_dead_letter_entries": await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM dead_letter_entries
            WHERE root_object_type = 'notification_plan'
              AND root_object_id = CAST(:plan_id AS uuid)
            """,
            {"plan_id": plan_id},
        ),
        "marker_replay_requests": await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM replay_requests
            WHERE root_object_type = 'notification_plan'
              AND root_object_id = CAST(:plan_id AS uuid)
            """,
            {"plan_id": plan_id},
        ),
        "marker_state_transitions": await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM state_transitions
            WHERE object_type = 'notification_plan'
              AND object_id = CAST(:plan_id AS uuid)
            """,
            {"plan_id": plan_id},
        ),
        "marker_job_attempts": await _select_scalar(
            session,
            """
            SELECT COUNT(*)
            FROM job_attempts
            WHERE root_object_type = 'notification_plan'
              AND root_object_id = CAST(:plan_id AS uuid)
            """,
            {"plan_id": plan_id},
        ),
    }


async def _insert_seed_rows(session: AsyncSession, *, seed_shape: SmokeSeedShape, seed_ids: SmokeSeedIds) -> None:
    message_id = int(str(seed_ids.notification_delivery_record_id.int)[-9:]) or 1
    text = f"Delivery gate full runtime smoke for {seed_shape.canonical_url}"
    raw_message_json = {
        "smoke_marker": seed_shape.marker,
        "source": REPORT_TYPE,
        "external_network_calls_allowed": False,
    }
    await session.execute(
        sa.text(
            """
            INSERT INTO source_messages (
                source_message_id, platform, chat_id, message_id, logical_post_key,
                is_channel_post, posted_at, current_version_no, message_link,
                content_type, text_body, text_surface, entities_json, url_surface_json,
                raw_message_json, first_seen_at, last_seen_at
            ) VALUES (
                CAST(:source_message_id AS uuid), 'telegram', :chat_id, :message_id,
                :logical_post_key, true, :posted_at, 1, :message_link, 'text',
                :text_body, :text_surface, CAST(:entities_json AS jsonb),
                CAST(:url_surface_json AS jsonb), CAST(:raw_message_json AS jsonb),
                :first_seen_at, :last_seen_at
            )
            """
        ),
        {
            "source_message_id": str(seed_ids.source_message_id),
            "chat_id": seed_shape.target_chat_id,
            "message_id": message_id,
            "logical_post_key": seed_shape.marker,
            "posted_at": seed_shape.source_posted_at,
            "message_link": f"https://t.me/c/{abs(seed_shape.target_chat_id)}/{message_id}",
            "text_body": text,
            "text_surface": text,
            "entities_json": json.dumps([], sort_keys=True),
            "url_surface_json": json.dumps([], sort_keys=True),
            "raw_message_json": json.dumps(raw_message_json, sort_keys=True),
            "first_seen_at": seed_shape.source_posted_at,
            "last_seen_at": seed_shape.source_posted_at,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO artifact_registry (
                artifact_id, artifact_type, canonical_id, canonical_url,
                normalized_host, artifact_key_json, created_at, updated_at
            ) VALUES (
                CAST(:artifact_id AS uuid), 'github_repo'::artifact_type_enum,
                :canonical_id, :canonical_url, 'github.com',
                CAST(:artifact_key_json AS jsonb), :created_at, :updated_at
            )
            """
        ),
        {
            "artifact_id": str(seed_ids.artifact_id),
            "canonical_id": seed_shape.canonical_id,
            "canonical_url": seed_shape.canonical_url,
            "artifact_key_json": json.dumps(
                {"owner": seed_shape.owner, "repo": seed_shape.repo_name, "smoke_marker": seed_shape.marker},
                sort_keys=True,
            ),
            "created_at": seed_shape.plan_created_at,
            "updated_at": seed_shape.plan_created_at,
        },
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
                'proposed', 'delivery_gate_full_runtime_smoke_v1', :dedupe_subject_key,
                :created_at, :updated_at
            )
            """
        ),
        {
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "source_message_id": str(seed_ids.source_message_id),
            "artifact_id": str(seed_ids.artifact_id),
            "dedupe_subject_key": seed_shape.marker,
            "created_at": seed_shape.plan_created_at,
            "updated_at": seed_shape.plan_created_at,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO candidate_evidence_bundles (
                bundle_id, candidate_group_id, initial_primary_artifact_id,
                current_primary_artifact_id, bundle_profile_version, bundle_input_hash,
                primary_summary, supporting_summaries_json, discovered_links_summary_json,
                evidence_limitations, ready_for_analysis, token_budget_profile, created_at
            ) VALUES (
                CAST(:bundle_id AS uuid), CAST(:candidate_group_id AS uuid),
                CAST(:artifact_id AS uuid), CAST(:artifact_id AS uuid),
                'delivery_gate_full_runtime_smoke_v1', :bundle_input_hash,
                CAST(:primary_summary AS jsonb), CAST(:supporting_summaries_json AS jsonb),
                CAST(:discovered_links_summary_json AS jsonb), CAST(:evidence_limitations AS jsonb),
                true, 'smoke', :created_at
            )
            """
        ),
        {
            "bundle_id": str(seed_ids.bundle_id),
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "artifact_id": str(seed_ids.artifact_id),
            "bundle_input_hash": seed_shape.material_change_hash,
            "primary_summary": json.dumps({"smoke_marker": seed_shape.marker}, sort_keys=True),
            "supporting_summaries_json": json.dumps([], sort_keys=True),
            "discovered_links_summary_json": json.dumps([], sort_keys=True),
            "evidence_limitations": json.dumps(["synthetic smoke fixture"], sort_keys=True),
            "created_at": seed_shape.plan_created_at,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO judge_runs (
                judge_run_id, bundle_id, judge_profile, model, reasoning_effort,
                prompt_version, schema_version, policy_version, prompt_cache_key,
                status, started_at, finished_at
            ) VALUES (
                CAST(:judge_run_id AS uuid), CAST(:bundle_id AS uuid),
                'delivery_gate_full_runtime_smoke', 'gpt-5.4-mini', 'low',
                'delivery_gate_full_runtime_smoke_v1', 'judge_output_v1', 'verdict_policy_v1',
                :prompt_cache_key, 'succeeded', :started_at, :finished_at
            )
            """
        ),
        {
            "judge_run_id": str(seed_ids.judge_run_id),
            "bundle_id": str(seed_ids.bundle_id),
            "prompt_cache_key": f"delivery-gate-full:{seed_shape.smoke_id}",
            "started_at": seed_shape.plan_created_at,
            "finished_at": seed_shape.plan_created_at,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO judge_outputs (
                judge_output_id, judge_run_id, candidate_group_id, judge_schema_version,
                payload_json, model_proposed_verdict, model_confidence_band, created_at
            ) VALUES (
                CAST(:judge_output_id AS uuid), CAST(:judge_run_id AS uuid),
                CAST(:candidate_group_id AS uuid), 'judge_output_v1',
                CAST(:payload_json AS jsonb), 'later'::verdict_enum, 'medium', :created_at
            )
            """
        ),
        {
            "judge_output_id": str(seed_ids.judge_output_id),
            "judge_run_id": str(seed_ids.judge_run_id),
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "payload_json": json.dumps({"smoke_marker": seed_shape.marker}, sort_keys=True),
            "created_at": seed_shape.plan_created_at,
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
                CAST(:judge_output_id AS uuid), 'analysis_v1', 'verdict_policy_v1',
                'delivery_gate_full_runtime_smoke_v1', 'delivery_policy_v1', 'later'::verdict_enum,
                'send_now'::delivery_decision_enum, CAST(:scores_json AS jsonb),
                CAST(:reason_codes_json AS jsonb), 'synthetic smoke fixture',
                'full gate acceptance smoke only', 'deterministic fixture',
                'later'::verdict_enum, true, :created_at
            )
            """
        ),
        {
            "analysis_id": str(seed_ids.analysis_id),
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "judge_output_id": str(seed_ids.judge_output_id),
            "scores_json": json.dumps({"confidence": 100}, sort_keys=True),
            "reason_codes_json": json.dumps([seed_shape.marker], sort_keys=True),
            "created_at": seed_shape.plan_created_at,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO notification_plans (
                notification_plan_id, analysis_id, candidate_group_id,
                delivery_decision, urgency_profile, target_chat_id, target_thread_id,
                render_profile, dedupe_subject_key, material_change_hash, send_after,
                suppress_reason_code, status, created_at
            ) VALUES (
                CAST(:notification_plan_id AS uuid), CAST(:analysis_id AS uuid),
                CAST(:candidate_group_id AS uuid), 'send_now'::delivery_decision_enum,
                'high'::urgency_profile_enum, :target_chat_id, NULL,
                'telegram_single_alert_normal_v1', :dedupe_subject_key,
                :material_change_hash, :send_after, NULL, 'sent'::notification_status_enum,
                :created_at
            )
            """
        ),
        {
            "notification_plan_id": str(seed_ids.notification_plan_id),
            "analysis_id": str(seed_ids.analysis_id),
            "candidate_group_id": str(seed_ids.candidate_group_id),
            "target_chat_id": seed_shape.target_chat_id,
            "dedupe_subject_key": seed_shape.marker,
            "material_change_hash": seed_shape.material_change_hash,
            "send_after": seed_shape.delivered_at,
            "created_at": seed_shape.plan_created_at,
        },
    )
    await session.execute(
        sa.text(
            """
            INSERT INTO notification_delivery_records (
                notification_delivery_record_id, notification_plan_id,
                telegram_chat_id, telegram_message_id, delivery_status, sent_at,
                edited_at, attempt_count, transport_error_code, transport_error_class,
                telegram_response_json, created_at
            ) VALUES (
                CAST(:notification_delivery_record_id AS uuid),
                CAST(:notification_plan_id AS uuid), :telegram_chat_id, :telegram_message_id,
                'sent'::notification_status_enum, :sent_at, NULL, 1, NULL, NULL,
                CAST(:telegram_response_json AS jsonb), :created_at
            )
            """
        ),
        {
            "notification_delivery_record_id": str(seed_ids.notification_delivery_record_id),
            "notification_plan_id": str(seed_ids.notification_plan_id),
            "telegram_chat_id": seed_shape.target_chat_id,
            "telegram_message_id": message_id,
            "sent_at": seed_shape.delivered_at,
            "telegram_response_json": json.dumps({"smoke_marker": seed_shape.marker}, sort_keys=True),
            "created_at": seed_shape.delivered_at,
        },
    )
    await session.execute(
        sa.text(
            """
            UPDATE candidate_group_proposals
            SET current_bundle_id = CAST(:bundle_id AS uuid),
                current_analysis_id = CAST(:analysis_id AS uuid),
                updated_at = :updated_at
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """
        ),
        {
            "bundle_id": str(seed_ids.bundle_id),
            "analysis_id": str(seed_ids.analysis_id),
            "updated_at": seed_shape.plan_created_at,
            "candidate_group_id": str(seed_ids.candidate_group_id),
        },
    )


def _seeded_ids(seed_ids: SmokeSeedIds) -> dict[str, str]:
    return {field: str(value) for field, value in asdict(seed_ids).items()}


def _gate_report_summary(gate_report: Any) -> dict[str, Any]:
    metrics = [
        {
            "metric_name": metric.metric_name,
            "observed_value": metric.observed_value,
            "threshold": metric.threshold,
            "comparator": metric.comparator,
            "passed": metric.passed,
            "severity": metric.severity,
        }
        for metric in gate_report.metrics
    ]
    return {
        "mode": gate_report.mode,
        "gate_status": gate_report.gate_status,
        "blocking_reason_codes": list(gate_report.blocking_reason_codes),
        "warning_reason_codes": list(gate_report.warning_reason_codes),
        "operator_review_required": gate_report.operator_review_required,
        "operator_review_passed": gate_report.operator_review_passed,
        "metrics": metrics,
        "recommended_flag_patch": dict(gate_report.recommended_flag_patch),
    }


def _check_preconditions(report: SmokeReport, *, database_url: str) -> bool:
    blocking_counts = {name: count for name, count in report.db_precondition_counts.items() if count != 0}
    if blocking_counts:
        _mark_fail(
            report,
            "db.precondition_clean_full_gate_window",
            (
                "full_pass_with_operator_review requires an otherwise clean local gate window; "
                f"interfering counts: {blocking_counts}"
            ),
            database_url=database_url,
        )
        return False
    _mark_pass(report, "db.precondition_clean_full_gate_window")
    return True


def _check_full_pass_report(report: SmokeReport, full_pass: dict[str, Any], *, database_url: str) -> None:
    if full_pass.get("mode") == EXPECTED_GATE_MODE:
        _mark_pass(report, "gate.full_pass.mode_full")
    else:
        _mark_fail(report, "gate.full_pass.mode_full", f"mode was {full_pass.get('mode')!r}", database_url=database_url)

    if full_pass.get("gate_status") == EXPECTED_GATE_STATUS:
        _mark_pass(report, "gate.full_pass.gate_status_pass")
    else:
        _mark_fail(
            report,
            "gate.full_pass.gate_status_pass",
            f"gate_status was {full_pass.get('gate_status')!r}",
            database_url=database_url,
        )

    if full_pass.get("blocking_reason_codes") == []:
        _mark_pass(report, "gate.full_pass.blocking_reason_codes_empty")
    else:
        _mark_fail(
            report,
            "gate.full_pass.blocking_reason_codes_empty",
            f"blocking_reason_codes was {full_pass.get('blocking_reason_codes')!r}",
            database_url=database_url,
        )

    if full_pass.get("warning_reason_codes") == []:
        _mark_pass(report, "gate.full_pass.warning_reason_codes_empty")
    else:
        _mark_fail(
            report,
            "gate.full_pass.warning_reason_codes_empty",
            f"warning_reason_codes was {full_pass.get('warning_reason_codes')!r}",
            database_url=database_url,
        )

    if full_pass.get("operator_review_required") is True and full_pass.get("operator_review_passed") is True:
        _mark_pass(report, "gate.full_pass.operator_review_verified")
    else:
        _mark_fail(
            report,
            "gate.full_pass.operator_review_verified",
            (
                "expected operator_review_required=True and operator_review_passed=True, got "
                f"{full_pass.get('operator_review_required')!r}/"
                f"{full_pass.get('operator_review_passed')!r}"
            ),
            database_url=database_url,
        )

    if report.metric_order_observed == FULL_METRIC_ORDER_EXPECTED:
        _mark_pass(report, "gate.full_pass.full_metric_order_observed")
    else:
        _mark_fail(
            report,
            "gate.full_pass.full_metric_order_observed",
            f"expected {FULL_METRIC_ORDER_EXPECTED!r}, got {report.metric_order_observed!r}",
            database_url=database_url,
        )

    observed_metric_names = set(report.full_metric_names_observed)
    missing_metric_names = [name for name in FULL_METRIC_ORDER_EXPECTED if name not in observed_metric_names]
    if not missing_metric_names:
        _mark_pass(report, "gate.full_pass.full_metric_names_present")
    else:
        _mark_fail(
            report,
            "gate.full_pass.full_metric_names_present",
            f"missing metric names: {missing_metric_names!r}",
            database_url=database_url,
        )

    if full_pass.get("recommended_flag_patch") == EXPECTED_RECOMMENDED_FLAG_PATCH:
        _mark_pass(report, "gate.full_pass.recommended_flag_patch_expected")
    else:
        _mark_fail(
            report,
            "gate.full_pass.recommended_flag_patch_expected",
            f"recommended_flag_patch was {full_pass.get('recommended_flag_patch')!r}",
            database_url=database_url,
        )


def _check_full_warn_report(report: SmokeReport, full_warn: dict[str, Any], *, database_url: str) -> None:
    if full_warn.get("gate_status") == "warn":
        _mark_pass(report, "gate.full_warn_without_operator_review.gate_status_warn")
    else:
        _mark_fail(
            report,
            "gate.full_warn_without_operator_review.gate_status_warn",
            f"gate_status was {full_warn.get('gate_status')!r}",
            database_url=database_url,
        )

    if full_warn.get("blocking_reason_codes") == []:
        _mark_pass(report, "gate.full_warn_without_operator_review.blocking_reason_codes_empty")
    else:
        _mark_fail(
            report,
            "gate.full_warn_without_operator_review.blocking_reason_codes_empty",
            f"blocking_reason_codes was {full_warn.get('blocking_reason_codes')!r}",
            database_url=database_url,
        )

    warning_codes = full_warn.get("warning_reason_codes") or []
    if EXPECTED_OPERATOR_REVIEW_WARNING in warning_codes:
        _mark_pass(report, "gate.full_warn_without_operator_review.operator_review_warning_present")
    else:
        _mark_fail(
            report,
            "gate.full_warn_without_operator_review.operator_review_warning_present",
            f"warning_reason_codes was {warning_codes!r}",
            database_url=database_url,
        )

    if full_warn.get("operator_review_required") is True and full_warn.get("operator_review_passed") is False:
        _mark_pass(report, "gate.full_warn_without_operator_review.operator_review_verified")
    else:
        _mark_fail(
            report,
            "gate.full_warn_without_operator_review.operator_review_verified",
            (
                "expected operator_review_required=True and operator_review_passed=False, got "
                f"{full_warn.get('operator_review_required')!r}/"
                f"{full_warn.get('operator_review_passed')!r}"
            ),
            database_url=database_url,
        )

    if full_warn.get("recommended_flag_patch") == EXPECTED_RECOMMENDED_FLAG_PATCH:
        _mark_pass(report, "gate.full_warn_without_operator_review.recommended_flag_patch_output_only")
    else:
        _mark_fail(
            report,
            "gate.full_warn_without_operator_review.recommended_flag_patch_output_only",
            f"recommended_flag_patch was {full_warn.get('recommended_flag_patch')!r}",
            database_url=database_url,
        )


def _check_postconditions(report: SmokeReport, *, database_url: str) -> None:
    expectations = {
        "marker_notification_delivery_records": 1,
        "marker_notification_renders": 0,
        "marker_event_outbox": 0,
        "marker_dead_letter_entries": 0,
        "marker_replay_requests": 0,
        "marker_state_transitions": 0,
        "marker_job_attempts": 0,
    }
    for name, expected in expectations.items():
        actual = report.db_postcondition_counts.get(name)
        if actual == expected:
            _mark_pass(report, f"db.{name}")
        else:
            _mark_fail(report, f"db.{name}", f"expected {expected}, got {actual}", database_url=database_url)


async def _run_smoke(database_url: str, report: SmokeReport, seed_shape: SmokeSeedShape) -> SmokeReport:
    seed_ids = _new_seed_ids()
    report.seeded_ids = _seeded_ids(seed_ids)
    engine = create_async_engine(database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with session_factory() as session:
            report.db_precondition_counts = await _load_precondition_counts(session)
            if not _check_preconditions(report, database_url=database_url):
                return report

        async with session_factory.begin() as session:
            await _insert_seed_rows(session, seed_shape=seed_shape, seed_ids=seed_ids)
            _mark_pass(report, "db.marker_scoped_fixture_inserted")

        async with session_factory() as session:
            runner = DeliveryGateRunner(
                _delivery_gate_config(database_url),
                repository=MaintenanceRepository(session),
            )
            full_pass_report = await runner.run(mode="full", operator_review_passed=True)
            full_warn_report = await runner.run(mode="full", operator_review_passed=False)
            full_pass = _gate_report_summary(full_pass_report)
            full_warn = _gate_report_summary(full_warn_report)
            report.gate_report_summary = {
                "full_pass": full_pass,
                "full_warn_without_operator_review": full_warn,
            }
            report.metric_order_observed = [metric.metric_name for metric in full_pass_report.metrics]
            report.full_metric_names_observed = list(report.metric_order_observed)
            report.recommended_flag_patch = dict(full_pass_report.recommended_flag_patch)
            _mark_pass(report, "gate.delivery_gate_full_reports_exist")
            _check_full_pass_report(report, full_pass, database_url=database_url)
            _check_full_warn_report(report, full_warn, database_url=database_url)

        async with session_factory() as session:
            report.db_postcondition_counts = await _load_marker_postcondition_counts(session, seed_ids)
            _check_postconditions(report, database_url=database_url)

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
    _mark_pass(report, "safety.redis_not_required")
    _mark_pass(report, "safety.no_external_credentials_required")
    _mark_pass(report, "safety.no_runtime_worker_started")
    _mark_pass(report, "safety.no_feature_flag_mutation")
    _mark_pass(report, "safety.recommended_flag_patch_output_only")

    try:
        report = await _run_smoke(database_url, report, seed_shape)
    except Exception as exc:
        _mark_fail(report, "runtime.unhandled_exception", f"{type(exc).__name__}: {exc}", database_url=database_url)

    print(_render_json(report))
    return 1 if report.failed() else 0


def main() -> None:
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()
