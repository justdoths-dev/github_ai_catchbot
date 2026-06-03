from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import sqlalchemy as sa

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.ops import dedicated_vps_analysis_validator_policy_apply_handoff_smoke as validator_base  # noqa: E402
from scripts.ops import dedicated_vps_policy_engine_analysis_handoff_smoke as policy_base  # noqa: E402
from src.services.analysis_validator.business_rules import AnalysisValidatorBusinessRules  # noqa: E402
from src.services.analysis_validator.models import (  # noqa: E402
    BundleValidationContext,
    JudgeOutputReadyJob,
    JudgeOutputRecord,
    JudgeRunValidationRecord,
)
from src.services.analysis_validator.repositories import AnalysisValidatorRepository  # noqa: E402
from src.services.analysis_validator.schema_registry import JudgeOutputSchemaRegistry  # noqa: E402
from src.services.analysis_validator.service import AnalysisValidatorService  # noqa: E402
from src.services.policy_engine.models import (  # noqa: E402
    AnalysisPolicyJob,
    BundlePolicyContext,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
)
from src.services.policy_engine.service import PolicyEngineService  # noqa: E402


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_analysis_validator_non_suppress_policy_apply_seed_smoke"
REPORT_TYPE = "analysis_validator_non_suppress_policy_apply_seed_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = validator_base.DEFAULT_RUNTIME_ENV_PATH
READY_EVENT_TYPE = validator_base.READY_EVENT_TYPE
POLICY_APPLY_EVENT_TYPE = validator_base.POLICY_APPLY_EVENT_TYPE
SEED_SUFFIX = "__replay_non_suppress_plan_intent_seed_v1"
SEED_PROMPT_VERSION = f"judge_prompt_v1{SEED_SUFFIX}"
SEED_MODEL = "ops-replay-seed"
SEED_REASONING_EFFORT = "none"
SEED_JUDGE_PROFILE = "github_primary"
SEED_POLICY_VERSION = "verdict_policy_v1"
SEED_DELIVERY_POLICY_VERSION = "delivery_policy_v1"

STATUS_DEFAULT_PASSED = "analysis_validator_non_suppress_policy_apply_seed_smoke_default_passed"
STATUS_DB_PREFLIGHT_PASSED = (
    "analysis_validator_non_suppress_policy_apply_seed_smoke_db_read_preflight_passed"
)
STATUS_DB_WRITE_PASSED = (
    "analysis_validator_non_suppress_policy_apply_seed_smoke_approved_db_write_passed"
)
STATUS_NOT_APPROVED = "blocked_analysis_validator_non_suppress_policy_apply_seed_smoke_not_approved"
STATUS_DB_READ_FAILED = "blocked_analysis_validator_non_suppress_policy_apply_seed_smoke_db_read_failed"
STATUS_EXISTING_SEED = "blocked_analysis_validator_non_suppress_policy_apply_seed_smoke_existing_seed"
STATUS_VALIDATION_FAILED = "blocked_analysis_validator_non_suppress_policy_apply_seed_smoke_validation_failed"
STATUS_WRITE_FAILED = "blocked_analysis_validator_non_suppress_policy_apply_seed_smoke_write_failed"
STATUS_RAW_VALUE_EMISSION = (
    "blocked_analysis_validator_non_suppress_policy_apply_seed_smoke_raw_value_emission"
)

SELECT_SEEDABLE_CURRENT_BUNDLE_QUERY = """
SELECT c.candidate_group_id,
       c.current_bundle_id,
       b.bundle_id,
       b.current_primary_artifact_id,
       ar.artifact_type AS current_primary_artifact_type,
       b.created_at AS bundle_created_at
FROM candidate_group_proposals c
JOIN candidate_evidence_bundles b
  ON b.bundle_id = c.current_bundle_id
 AND b.candidate_group_id = c.candidate_group_id
LEFT JOIN artifact_registry ar
  ON ar.artifact_id = b.current_primary_artifact_id
WHERE c.current_bundle_id IS NOT NULL
  AND b.ready_for_analysis IS TRUE
ORDER BY b.created_at DESC NULLS LAST,
         c.created_at DESC NULLS LAST,
         c.candidate_group_id DESC
LIMIT 1
"""
COUNT_SEED_JUDGE_RUNS_QUERY = """
SELECT COUNT(*)
FROM judge_runs
WHERE prompt_version LIKE :seed_prompt_like
   OR prompt_cache_key LIKE :seed_prompt_like
"""
COUNT_SEED_JUDGE_OUTPUTS_QUERY = """
SELECT COUNT(*)
FROM judge_outputs jo
JOIN judge_runs jr ON jr.judge_run_id = jo.judge_run_id
WHERE jr.prompt_version LIKE :seed_prompt_like
   OR jr.prompt_cache_key LIKE :seed_prompt_like
"""
COUNT_SEED_READY_OUTBOX_QUERY = """
SELECT COUNT(*)
FROM event_outbox eo
JOIN judge_runs jr ON jr.judge_run_id = eo.aggregate_id
WHERE eo.event_type = 'judge.output.ready.v1'
  AND eo.aggregate_type = 'judge_run'
  AND (jr.prompt_version LIKE :seed_prompt_like OR jr.prompt_cache_key LIKE :seed_prompt_like)
"""
COUNT_SEED_POLICY_APPLY_OUTBOX_QUERY = """
SELECT COUNT(*)
FROM event_outbox eo
JOIN judge_runs jr ON jr.judge_run_id = eo.aggregate_id
WHERE eo.event_type = 'analysis.policy.apply.v1'
  AND eo.aggregate_type = 'judge_run'
  AND (jr.prompt_version LIKE :seed_prompt_like OR jr.prompt_cache_key LIKE :seed_prompt_like)
"""
COUNT_JUDGE_RUN_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM judge_runs
WHERE judge_run_id = CAST(:judge_run_id AS uuid)
"""
COUNT_JUDGE_OUTPUT_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM judge_outputs
WHERE judge_output_id = CAST(:judge_output_id AS uuid)
"""
COUNT_READY_OUTBOX_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'judge.output.ready.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
  AND payload_json->>'judge_output_id' = :judge_output_id
"""
COUNT_POLICY_APPLY_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'analysis.policy.apply.v1'
  AND aggregate_type = 'judge_run'
  AND aggregate_id = CAST(:judge_run_id AS uuid)
  AND payload_json->>'judge_output_id' = :judge_output_id
"""
COUNT_VALIDATOR_STATE_TRANSITIONS_FOR_TARGET_QUERY = """
SELECT COUNT(*)
FROM state_transitions
WHERE object_type = 'judge_run'
  AND object_id = CAST(:judge_run_id AS uuid)
  AND to_state = 'analysis_validated'
"""
COUNT_ANALYSES_FOR_OUTPUT_QUERY = policy_base.COUNT_ANALYSES_FOR_OUTPUT_QUERY
COUNT_NOTIFICATION_PLAN_INTENT_OUTBOX_FOR_TARGET_QUERY = (
    policy_base.COUNT_NOTIFICATION_PLAN_INTENT_OUTBOX_FOR_TARGET_QUERY
)
COUNT_NOTIFICATION_PLAN_ROWS_FOR_OUTPUT_QUERY = policy_base.COUNT_NOTIFICATION_PLAN_ROWS_FOR_OUTPUT_QUERY
COUNT_NOTIFICATION_RENDER_ROWS_FOR_OUTPUT_QUERY = policy_base.COUNT_NOTIFICATION_RENDER_ROWS_FOR_OUTPUT_QUERY
COUNT_NOTIFICATION_DELIVERY_ROWS_FOR_OUTPUT_QUERY = policy_base.COUNT_NOTIFICATION_DELIVERY_ROWS_FOR_OUTPUT_QUERY
SELECT_CANDIDATE_BUNDLE_FINGERPRINT_QUERY = policy_base.SELECT_CANDIDATE_BUNDLE_FINGERPRINT_QUERY
SELECT_CURRENT_ANALYSIS_ID_QUERY = policy_base.SELECT_CURRENT_ANALYSIS_ID_QUERY
INSERT_JUDGE_RUN_QUERY = """
INSERT INTO judge_runs (
    judge_run_id,
    bundle_id,
    judge_profile,
    model,
    reasoning_effort,
    prompt_version,
    schema_version,
    policy_version,
    prompt_cache_key,
    status,
    schema_retry_count,
    finish_reason,
    refusal_detected,
    started_at,
    finished_at
) VALUES (
    gen_random_uuid(),
    CAST(:bundle_id AS uuid),
    :judge_profile,
    :model,
    :reasoning_effort,
    :prompt_version,
    :schema_version,
    :policy_version,
    :prompt_cache_key,
    'succeeded',
    0,
    'completed',
    false,
    now(),
    now()
)
RETURNING judge_run_id
"""
INSERT_JUDGE_OUTPUT_QUERY = """
INSERT INTO judge_outputs (
    judge_output_id,
    judge_run_id,
    candidate_group_id,
    judge_schema_version,
    payload_json,
    model_proposed_verdict,
    model_confidence_band,
    created_at
) VALUES (
    gen_random_uuid(),
    CAST(:judge_run_id AS uuid),
    CAST(:candidate_group_id AS uuid),
    'judge_output_v1',
    CAST(:payload_json AS jsonb),
    CAST(:model_proposed_verdict AS verdict_enum),
    :model_confidence_band,
    now()
)
RETURNING judge_output_id
"""
INSERT_READY_OUTBOX_QUERY = """
INSERT INTO event_outbox (
    event_type,
    aggregate_type,
    aggregate_id,
    dedupe_key,
    payload_json,
    status,
    created_at
) VALUES (
    'judge.output.ready.v1',
    'judge_run',
    CAST(:judge_run_id AS uuid),
    :dedupe_key,
    CAST(:payload_json AS jsonb),
    'pending'::outbox_status_enum,
    now()
)
RETURNING event_id
"""

PUBLIC_LITERAL_VALUES = validator_base.PUBLIC_LITERAL_VALUES | {
    SCRIPT_NAME,
    REPORT_TYPE,
    SEED_SUFFIX,
    SEED_PROMPT_VERSION,
    SEED_MODEL,
    SEED_REASONING_EFFORT,
    "inspect_now",
    "later",
    "send_now",
    "high",
    "normal_silent",
    "AnalysisValidatorService.handle_job",
    "judge_output.ready.seed",
}
_GITHUB_PRIMARY_TYPES = {"github_repo", "github_subpath", "github_repo_page", "github_gist"}


@dataclass(frozen=True, slots=True)
class SeedCandidate:
    candidate_group_id: UUID
    bundle_id: UUID
    current_bundle_id: UUID | None
    current_primary_artifact_id: UUID
    current_primary_artifact_type: str | None


@dataclass(frozen=True, slots=True)
class SeedExistingCounts:
    judge_runs: int
    judge_outputs: int
    ready_outbox: int
    policy_apply_outbox: int


@dataclass(frozen=True, slots=True)
class TargetCounts:
    judge_runs: int = 0
    judge_outputs: int = 0
    ready_outbox: int = 0
    policy_apply_outbox: int = 0
    validator_transitions: int = 0
    analyses: int = 0
    notification_intents: int = 0
    notification_plans: int = 0
    notification_renders: int = 0
    notification_deliveries: int = 0
    candidate_bundles: int = 0


@dataclass(frozen=True, slots=True)
class TargetSnapshots:
    candidate_current_analysis_id: Any
    candidate_bundle_fingerprint: Any


@dataclass(frozen=True, slots=True)
class SeedWriteContext:
    trigger_event_id: UUID
    judge_run_id: UUID
    judge_output_id: UUID
    candidate: SeedCandidate
    payload: dict[str, Any]


class ExpectedEffectsError(RuntimeError):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Approval-gated replay-scoped seed smoke that appends one non-suppress "
            "judge_output_v1 and runs the real analysis-validator handoff path. "
            "Default mode reads no runtime env, DB, Redis, key material, OpenAI, "
            "policy-engine, notifier, or Telegram transport."
        )
    )
    parser.add_argument("--approve-db-read", action="store_true")
    parser.add_argument("--approve-db-write", action="store_true")
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def _base_report(*, approve_db_read: bool, approve_db_write: bool) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_DEFAULT_PASSED,
        "approvals": {
            "db_read": approve_db_read,
            "db_write": approve_db_write,
        },
        "runtime_env_read": False,
        "database_configured": False,
        "database_connected": False,
        "read_only_transaction": False,
        "database_write_attempted": False,
        "seed_target_candidate_found_bucket": "zero",
        "seed_target_bundle_found_bucket": "zero",
        "current_bundle_match_bucket": "zero",
        "seed_existing_judge_run_bucket": "zero",
        "seed_existing_judge_output_bucket": "zero",
        "seed_existing_judge_output_ready_outbox_bucket": "zero",
        "seed_existing_policy_apply_outbox_bucket": "zero",
        "judge_run_written_bucket": "zero",
        "judge_output_written_bucket": "zero",
        "judge_output_ready_outbox_written_bucket": "zero",
        "validator_state_transitions_written_bucket": "zero",
        "analysis_policy_apply_outbox_written_bucket": "zero",
        "analysis_rows_written_bucket": "zero",
        "notification_plan_intent_outbox_written_bucket": "zero",
        "notification_plan_rows_written_bucket": "zero",
        "notification_render_rows_written_bucket": "zero",
        "notification_delivery_rows_written_bucket": "zero",
        "analysis_verdict_bucket": "zero",
        "analysis_delivery_decision_bucket": "zero",
        "analysis_urgency_profile_bucket": "zero",
        "analysis_policy_reconciled_bucket": "zero",
        "output_schema_valid_bucket": "zero",
        "business_rules_valid_bucket": "zero",
        "policy_engine_started": False,
        "notifier_started": False,
        "telegram_send_attempted": False,
        "redis_connected": False,
        "redis_write_attempted": False,
        "redis_ack_attempted": False,
        "redis_delete_or_trim_attempted": False,
        "openai_call_attempted": False,
        "openai_key_read_bucket": "zero",
        "candidate_bundle_mutation_attempted": False,
        "candidate_current_analysis_mutation_attempted": False,
        "service_path_reused": "AnalysisValidatorService.handle_job",
        "stops_at_event_type": POLICY_APPLY_EVENT_TYPE,
        "raw_values_emitted": False,
        "checks_failed": [],
    }


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _bucket_count(count: int) -> str:
    return validator_base._bucket_count(count)


async def _execute(
    session: validator_base.AsyncSessionLike,
    statement: str,
    params: dict[str, Any] | None = None,
) -> Any:
    return await session.execute(sa.text(statement), params or {})


def _jsonb_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


def _json_default(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"unsupported json type: {type(value)!r}")


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _raw_values(*values: Any) -> set[str]:
    raw = validator_base._raw_values(*values)
    return {value for value in raw if value not in PUBLIC_LITERAL_VALUES}


async def _count_query(
    session: validator_base.AsyncSessionLike,
    query: str,
    params: dict[str, Any] | None = None,
) -> int:
    return validator_base._safe_count(validator_base._scalar(await _execute(session, query, params or {})))


async def _read_runtime_config(
    *,
    report: dict[str, Any],
    runtime_env_path: str | Path,
    runtime_env_reader: validator_base.RuntimeEnvReader | None,
    raw_values: set[str],
) -> tuple[str, int | None] | None:
    runtime_config = await policy_base._read_runtime_config(
        report=report,
        runtime_env_path=runtime_env_path,
        runtime_env_reader=runtime_env_reader,
        raw_values=raw_values,
    )
    return runtime_config


async def _select_seed_candidate(
    *,
    report: dict[str, Any],
    session: validator_base.AsyncSessionLike,
    raw_values: set[str],
) -> SeedCandidate | None:
    row = validator_base._first_mapping(await _execute(session, SELECT_SEEDABLE_CURRENT_BUNDLE_QUERY))
    report["seed_target_candidate_found_bucket"] = "one" if row is not None else "zero"
    report["seed_target_bundle_found_bucket"] = "one" if row is not None else "zero"
    if row is None:
        _set_status(report, STATUS_DB_READ_FAILED, "seed_target.missing")
        return None
    raw_values.update(_raw_values(row))

    candidate_group_id = validator_base._coerce_uuid(row.get("candidate_group_id"))
    bundle_id = validator_base._coerce_uuid(row.get("bundle_id"))
    current_bundle_id = validator_base._coerce_uuid(row.get("current_bundle_id"))
    current_primary_artifact_id = validator_base._coerce_uuid(row.get("current_primary_artifact_id"))
    if candidate_group_id is None or bundle_id is None or current_primary_artifact_id is None:
        _set_status(report, STATUS_DB_READ_FAILED, "seed_target.identity")
        return None
    current_bundle_match = current_bundle_id == bundle_id
    report["current_bundle_match_bucket"] = "one" if current_bundle_match else "zero"
    if not current_bundle_match:
        _set_status(report, STATUS_VALIDATION_FAILED, "candidate.current_bundle")
        return None

    return SeedCandidate(
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        current_bundle_id=current_bundle_id,
        current_primary_artifact_id=current_primary_artifact_id,
        current_primary_artifact_type=_string_or_none(row.get("current_primary_artifact_type")),
    )


async def _load_existing_seed_counts(session: validator_base.AsyncSessionLike) -> SeedExistingCounts:
    params = {"seed_prompt_like": f"%{SEED_SUFFIX}"}
    return SeedExistingCounts(
        judge_runs=await _count_query(session, COUNT_SEED_JUDGE_RUNS_QUERY, params),
        judge_outputs=await _count_query(session, COUNT_SEED_JUDGE_OUTPUTS_QUERY, params),
        ready_outbox=await _count_query(session, COUNT_SEED_READY_OUTBOX_QUERY, params),
        policy_apply_outbox=await _count_query(session, COUNT_SEED_POLICY_APPLY_OUTBOX_QUERY, params),
    )


def _merge_existing_seed_report(report: dict[str, Any], counts: SeedExistingCounts) -> None:
    report["seed_existing_judge_run_bucket"] = _bucket_count(counts.judge_runs)
    report["seed_existing_judge_output_bucket"] = _bucket_count(counts.judge_outputs)
    report["seed_existing_judge_output_ready_outbox_bucket"] = _bucket_count(counts.ready_outbox)
    report["seed_existing_policy_apply_outbox_bucket"] = _bucket_count(counts.policy_apply_outbox)


def _status_for_existing_seed(counts: SeedExistingCounts) -> tuple[str, str] | None:
    if counts.judge_runs:
        return STATUS_EXISTING_SEED, "seed.judge_run_existing"
    if counts.judge_outputs:
        return STATUS_EXISTING_SEED, "seed.judge_output_existing"
    if counts.ready_outbox:
        return STATUS_EXISTING_SEED, "seed.judge_output_ready_existing"
    if counts.policy_apply_outbox:
        return STATUS_EXISTING_SEED, "seed.policy_apply_existing"
    return None


def _fixture_payload(candidate: SeedCandidate) -> dict[str, Any]:
    comparables: list[str] = []
    if candidate.current_primary_artifact_type in _GITHUB_PRIMARY_TYPES:
        comparables = ["replay smoke comparable"]
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate.candidate_group_id),
        "headline": "Replay non-suppress seed",
        "summary_one_line_ko": "운영 smoke용 non-suppress seed입니다.",
        "skeptical_take_ko": "실제 추천이 아니라 replay smoke fixture입니다.",
        "why_it_might_matter_ko": "policy-engine non-suppress handoff 검증용입니다.",
        "comparables": comparables,
        "scores": {
            "novelty": 70,
            "practical_usefulness": 76,
            "evidence_strength": 65,
            "hype_penalty": 20,
            "confidence": 70,
            "code_quality": 70,
            "maintenance_signal": 50,
            "specificity": 65,
            "reproducibility_signal": 50,
        },
        "reason_codes": ["replay_non_suppress_seed"],
        "red_flags_ko": ["실서비스 판단 결과가 아닌 smoke fixture"],
        "evidence_limitations_ko": ["replay smoke fixture"],
        "recommended_action_ko": "non-suppress policy-engine handoff smoke를 실행한다.",
        "freshness_note_ko": "replay smoke fixture",
        "model_proposed_verdict": "later",
        "model_confidence_band": "medium",
    }


def _validate_and_preview_fixture(
    *,
    report: dict[str, Any],
    candidate: SeedCandidate,
    database_url: str,
    operator_chat_id: int | None,
) -> dict[str, Any] | None:
    payload = _fixture_payload(candidate)
    validator_config = validator_base._validator_config(database_url)
    schema_decision = JudgeOutputSchemaRegistry(
        max_headline_chars=validator_config.max_headline_chars,
        max_summary_chars=validator_config.max_summary_chars,
        max_text_items=validator_config.max_text_items,
    ).validate(payload)
    report["output_schema_valid_bucket"] = "one" if schema_decision.action == "forward_policy" else "zero"
    if schema_decision.action != "forward_policy":
        _set_status(report, STATUS_VALIDATION_FAILED, "validator.schema")
        return None

    bundle = BundleValidationContext(
        bundle_id=candidate.bundle_id,
        candidate_group_id=candidate.candidate_group_id,
        current_primary_artifact_id=candidate.current_primary_artifact_id,
        current_primary_artifact_type=candidate.current_primary_artifact_type,
    )
    semantic_decision = AnalysisValidatorBusinessRules().validate_semantics(payload=payload, bundle=bundle)
    report["business_rules_valid_bucket"] = "one" if semantic_decision.action == "forward_policy" else "zero"
    if semantic_decision.action != "forward_policy":
        _set_status(report, STATUS_VALIDATION_FAILED, "validator.business_rules")
        return None

    policy_config = policy_base._policy_config(
        database_url=database_url,
        operator_chat_id=operator_chat_id,
        enable_notification_send=operator_chat_id is not None,
    )
    service = PolicyEngineService(policy_config, repository=policy_base._PreviewRepository())
    job = AnalysisPolicyJob(
        trigger_event_id=UUID("00000000-0000-0000-0000-000000000001"),
        event_type=POLICY_APPLY_EVENT_TYPE,
        judge_run_id=UUID("00000000-0000-0000-0000-000000000002"),
        judge_output_id=UUID("00000000-0000-0000-0000-000000000003"),
        candidate_group_id=candidate.candidate_group_id,
        bundle_id=candidate.bundle_id,
    )
    judge_run = JudgeRunPolicyContext(
        judge_run_id=job.judge_run_id,
        bundle_id=candidate.bundle_id,
        prompt_version=SEED_PROMPT_VERSION,
        policy_version=SEED_POLICY_VERSION,
        status="succeeded",
    )
    judge_output = JudgeOutputPolicyContext(
        judge_output_id=job.judge_output_id,
        judge_run_id=job.judge_run_id,
        candidate_group_id=candidate.candidate_group_id,
        payload_json=payload,
        model_proposed_verdict=payload["model_proposed_verdict"],
        model_confidence_band=payload["model_confidence_band"],
    )
    policy_bundle = BundlePolicyContext(
        bundle_id=candidate.bundle_id,
        candidate_group_id=candidate.candidate_group_id,
        current_primary_artifact_id=candidate.current_primary_artifact_id,
        current_primary_artifact_type=candidate.current_primary_artifact_type,
    )
    analysis, evaluation = service._build_analysis(
        job=job,
        judge_run=judge_run,
        judge_output=judge_output,
        bundle=policy_bundle,
    )
    report["analysis_verdict_bucket"] = analysis.verdict
    report["analysis_delivery_decision_bucket"] = analysis.delivery_decision
    report["analysis_urgency_profile_bucket"] = evaluation.urgency_profile
    report["analysis_policy_reconciled_bucket"] = "true" if analysis.policy_reconciled_flag else "false"
    if analysis.verdict not in {"inspect_now", "later"} or analysis.delivery_decision != "send_now":
        _set_status(report, STATUS_VALIDATION_FAILED, "policy_preview.non_suppress")
        return None
    return payload


async def _load_target_counts(
    session: validator_base.AsyncSessionLike,
    *,
    context: SeedWriteContext | None,
    candidate: SeedCandidate,
) -> TargetCounts:
    if context is None:
        return TargetCounts(
            candidate_bundles=await _count_query(
                session,
                policy_base.COUNT_CANDIDATE_BUNDLES_FOR_TARGET_QUERY,
                {"bundle_id": str(candidate.bundle_id)},
            )
        )
    params = {
        "judge_run_id": str(context.judge_run_id),
        "judge_output_id": str(context.judge_output_id),
        "bundle_id": str(candidate.bundle_id),
        "policy_version": SEED_POLICY_VERSION,
        "delivery_policy_version": SEED_DELIVERY_POLICY_VERSION,
    }
    return TargetCounts(
        judge_runs=await _count_query(session, COUNT_JUDGE_RUN_FOR_TARGET_QUERY, params),
        judge_outputs=await _count_query(session, COUNT_JUDGE_OUTPUT_FOR_TARGET_QUERY, params),
        ready_outbox=await _count_query(session, COUNT_READY_OUTBOX_FOR_TARGET_QUERY, params),
        policy_apply_outbox=await _count_query(session, COUNT_POLICY_APPLY_FOR_TARGET_QUERY, params),
        validator_transitions=await _count_query(session, COUNT_VALIDATOR_STATE_TRANSITIONS_FOR_TARGET_QUERY, params),
        analyses=await _count_query(session, COUNT_ANALYSES_FOR_OUTPUT_QUERY, params),
        notification_intents=await _count_query(session, COUNT_NOTIFICATION_PLAN_INTENT_OUTBOX_FOR_TARGET_QUERY, params),
        notification_plans=await _count_query(session, COUNT_NOTIFICATION_PLAN_ROWS_FOR_OUTPUT_QUERY, params),
        notification_renders=await _count_query(session, COUNT_NOTIFICATION_RENDER_ROWS_FOR_OUTPUT_QUERY, params),
        notification_deliveries=await _count_query(session, COUNT_NOTIFICATION_DELIVERY_ROWS_FOR_OUTPUT_QUERY, params),
        candidate_bundles=await _count_query(session, policy_base.COUNT_CANDIDATE_BUNDLES_FOR_TARGET_QUERY, params),
    )


async def _load_snapshots(session: validator_base.AsyncSessionLike, candidate: SeedCandidate) -> TargetSnapshots:
    current_analysis_id = validator_base._scalar(
        await _execute(
            session,
            SELECT_CURRENT_ANALYSIS_ID_QUERY,
            {"candidate_group_id": str(candidate.candidate_group_id)},
        )
    )
    bundle_fingerprint = validator_base._scalar(
        await _execute(
            session,
            SELECT_CANDIDATE_BUNDLE_FINGERPRINT_QUERY,
            {"bundle_id": str(candidate.bundle_id)},
        )
    )
    return TargetSnapshots(
        candidate_current_analysis_id=current_analysis_id,
        candidate_bundle_fingerprint=bundle_fingerprint,
    )


def _merge_written_count_report(
    report: dict[str, Any],
    *,
    before: TargetCounts,
    after: TargetCounts,
) -> None:
    report["judge_run_written_bucket"] = _bucket_count(after.judge_runs - before.judge_runs)
    report["judge_output_written_bucket"] = _bucket_count(after.judge_outputs - before.judge_outputs)
    report["judge_output_ready_outbox_written_bucket"] = _bucket_count(after.ready_outbox - before.ready_outbox)
    report["validator_state_transitions_written_bucket"] = _bucket_count(
        after.validator_transitions - before.validator_transitions
    )
    report["analysis_policy_apply_outbox_written_bucket"] = _bucket_count(
        after.policy_apply_outbox - before.policy_apply_outbox
    )
    report["analysis_rows_written_bucket"] = _bucket_count(after.analyses - before.analyses)
    report["notification_plan_intent_outbox_written_bucket"] = _bucket_count(
        after.notification_intents - before.notification_intents
    )
    report["notification_plan_rows_written_bucket"] = _bucket_count(
        after.notification_plans - before.notification_plans
    )
    report["notification_render_rows_written_bucket"] = _bucket_count(
        after.notification_renders - before.notification_renders
    )
    report["notification_delivery_rows_written_bucket"] = _bucket_count(
        after.notification_deliveries - before.notification_deliveries
    )
    report["candidate_bundle_mutation_attempted"] = after.candidate_bundles != before.candidate_bundles


def _merge_snapshot_mutation_report(
    report: dict[str, Any],
    *,
    before: TargetSnapshots,
    after: TargetSnapshots,
) -> None:
    report["candidate_current_analysis_mutation_attempted"] = (
        str(after.candidate_current_analysis_id) != str(before.candidate_current_analysis_id)
    )
    report["candidate_bundle_mutation_attempted"] = report["candidate_bundle_mutation_attempted"] or (
        str(after.candidate_bundle_fingerprint) != str(before.candidate_bundle_fingerprint)
    )


def _approved_execution_succeeded(report: Mapping[str, Any]) -> bool:
    return bool(
        report["judge_run_written_bucket"] == "one"
        and report["judge_output_written_bucket"] == "one"
        and report["judge_output_ready_outbox_written_bucket"] == "one"
        and report["validator_state_transitions_written_bucket"] == "one"
        and report["analysis_policy_apply_outbox_written_bucket"] == "one"
        and report["analysis_rows_written_bucket"] == "zero"
        and report["notification_plan_intent_outbox_written_bucket"] == "zero"
        and report["notification_plan_rows_written_bucket"] == "zero"
        and report["notification_render_rows_written_bucket"] == "zero"
        and report["notification_delivery_rows_written_bucket"] == "zero"
        and report["analysis_verdict_bucket"] in {"inspect_now", "later"}
        and report["analysis_delivery_decision_bucket"] == "send_now"
        and report["policy_engine_started"] is False
        and report["notifier_started"] is False
        and report["telegram_send_attempted"] is False
        and report["redis_write_attempted"] is False
        and report["redis_ack_attempted"] is False
        and report["redis_delete_or_trim_attempted"] is False
        and report["openai_call_attempted"] is False
        and report["openai_key_read_bucket"] == "zero"
        and report["candidate_bundle_mutation_attempted"] is False
        and report["candidate_current_analysis_mutation_attempted"] is False
    )


async def _insert_seed_rows(
    *,
    session: validator_base.AsyncSessionLike,
    candidate: SeedCandidate,
    payload: dict[str, Any],
) -> SeedWriteContext:
    judge_run_id = validator_base._coerce_uuid(
        validator_base._scalar(
            await _execute(
                session,
                INSERT_JUDGE_RUN_QUERY,
                {
                    "bundle_id": str(candidate.bundle_id),
                    "judge_profile": SEED_JUDGE_PROFILE,
                    "model": SEED_MODEL,
                    "reasoning_effort": SEED_REASONING_EFFORT,
                    "prompt_version": SEED_PROMPT_VERSION,
                    "schema_version": "judge_output_v1",
                    "policy_version": SEED_POLICY_VERSION,
                    "prompt_cache_key": f"ops-seed:{SEED_SUFFIX}:{candidate.bundle_id}",
                },
            )
        )
    )
    if judge_run_id is None:
        raise ExpectedEffectsError("seed judge_run insert did not return an id")

    judge_output_id = validator_base._coerce_uuid(
        validator_base._scalar(
            await _execute(
                session,
                INSERT_JUDGE_OUTPUT_QUERY,
                {
                    "judge_run_id": str(judge_run_id),
                    "candidate_group_id": str(candidate.candidate_group_id),
                    "payload_json": _jsonb_dumps(payload),
                    "model_proposed_verdict": payload["model_proposed_verdict"],
                    "model_confidence_band": payload["model_confidence_band"],
                },
            )
        )
    )
    if judge_output_id is None:
        raise ExpectedEffectsError("seed judge_output insert did not return an id")

    ready_payload = {
        "judge_run_id": str(judge_run_id),
        "judge_output_id": str(judge_output_id),
        "finish_reason": "completed",
        "refusal_detected": False,
    }
    trigger_event_id = validator_base._coerce_uuid(
        validator_base._scalar(
            await _execute(
                session,
                INSERT_READY_OUTBOX_QUERY,
                {
                    "judge_run_id": str(judge_run_id),
                    "dedupe_key": f"judge-output-ready:{candidate.bundle_id}:{SEED_SUFFIX}",
                    "payload_json": _jsonb_dumps(ready_payload),
                },
            )
        )
    )
    if trigger_event_id is None:
        raise ExpectedEffectsError("seed ready outbox insert did not return an id")

    return SeedWriteContext(
        trigger_event_id=trigger_event_id,
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
        candidate=candidate,
        payload=payload,
    )


async def _run_validator_service(
    *,
    report: dict[str, Any],
    session: validator_base.AsyncSessionLike,
    context: SeedWriteContext,
    database_url: str,
) -> None:
    repository = AnalysisValidatorRepository(session)
    service = AnalysisValidatorService(validator_base._validator_config(database_url), repository=repository)
    job = await service.rehydrate_job(context.trigger_event_id)
    if job is None:
        raise ExpectedEffectsError("validator service could not rehydrate seed ready event")
    await service.handle_job(job)


async def _run_approved_write(
    *,
    report: dict[str, Any],
    session: validator_base.AsyncSessionLike,
    candidate: SeedCandidate,
    payload: dict[str, Any],
    database_url: str,
) -> None:
    before_counts = await _load_target_counts(session, context=None, candidate=candidate)
    before_snapshots = await _load_snapshots(session, candidate)
    if session.in_transaction():
        await session.rollback()

    report["database_write_attempted"] = True
    async with session.begin():
        context = await _insert_seed_rows(session=session, candidate=candidate, payload=payload)
        await _run_validator_service(
            report=report,
            session=session,
            context=context,
            database_url=database_url,
        )
        after_counts = await _load_target_counts(session, context=context, candidate=candidate)
        after_snapshots = await _load_snapshots(session, candidate)
        _merge_written_count_report(report, before=before_counts, after=after_counts)
        _merge_snapshot_mutation_report(report, before=before_snapshots, after=after_snapshots)
        if not _approved_execution_succeeded(report):
            raise ExpectedEffectsError("analysis-validator seed smoke effects did not match contract")


async def generate_report_async(
    *,
    approve_db_read: bool = False,
    approve_db_write: bool = False,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    runtime_env_reader: validator_base.RuntimeEnvReader | None = None,
    database_session_factory: validator_base.DatabaseSessionFactory | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> validator_base.ScriptResult:
    report = _base_report(approve_db_read=approve_db_read, approve_db_write=approve_db_write)
    raw_values = _raw_values(*forbidden_raw_values)
    raw_values.update(_raw_values(runtime_env_path))

    if not approve_db_read and not approve_db_write:
        _set_status(report, STATUS_DEFAULT_PASSED)
        return _finalize(report, raw_values, exit_code=0)

    db_preflight_mode = approve_db_read and not approve_db_write
    db_write_mode = approve_db_read and approve_db_write
    if not db_preflight_mode and not db_write_mode:
        _set_status(report, STATUS_NOT_APPROVED, "approval.required_mode")
        return _finalize(report, raw_values, exit_code=1)

    session: validator_base.AsyncSessionLike | None = None
    committed = False
    try:
        runtime_config = await _read_runtime_config(
            report=report,
            runtime_env_path=runtime_env_path,
            runtime_env_reader=runtime_env_reader,
            raw_values=raw_values,
        )
        if runtime_config is None:
            return _finalize(report, raw_values, exit_code=1)
        database_url, operator_chat_id = runtime_config

        try:
            session = await validator_base._open_database_session(database_url, database_session_factory)
            if db_preflight_mode:
                await _execute(session, validator_base.SET_TRANSACTION_READ_ONLY_QUERY)
                read_only = validator_base._scalar(await _execute(session, validator_base.SHOW_TRANSACTION_READ_ONLY_QUERY))
                if not validator_base._transaction_read_only_enabled(read_only):
                    _set_status(report, STATUS_DB_READ_FAILED, "database.read_only")
                    return _finalize(report, raw_values, exit_code=1)
                report["read_only_transaction"] = True
            await _execute(session, validator_base.SELECT_ONE_QUERY)
            report["database_connected"] = True
        except Exception:
            _set_status(report, STATUS_DB_READ_FAILED, "database.connection")
            return _finalize(report, raw_values, exit_code=1)

        candidate = await _select_seed_candidate(report=report, session=session, raw_values=raw_values)
        if candidate is None:
            return _finalize(report, raw_values, exit_code=1)

        existing_counts = await _load_existing_seed_counts(session)
        _merge_existing_seed_report(report, existing_counts)
        blocked = _status_for_existing_seed(existing_counts)
        if blocked is not None:
            status, check = blocked
            _set_status(report, status, check)
            return _finalize(report, raw_values, exit_code=1)

        payload = _validate_and_preview_fixture(
            report=report,
            candidate=candidate,
            database_url=database_url,
            operator_chat_id=operator_chat_id,
        )
        if payload is None:
            return _finalize(report, raw_values, exit_code=1)

        if db_preflight_mode:
            _set_status(report, STATUS_DB_PREFLIGHT_PASSED)
            return _finalize(report, raw_values, exit_code=0)

        try:
            await _run_approved_write(
                report=report,
                session=session,
                candidate=candidate,
                payload=payload,
                database_url=database_url,
            )
            committed = True
        except ExpectedEffectsError:
            _set_status(report, STATUS_WRITE_FAILED, "db_write.expected_effects")
            return _finalize(report, raw_values, exit_code=1)
        except Exception:
            _set_status(report, STATUS_WRITE_FAILED, "service.handle_job")
            return _finalize(report, raw_values, exit_code=1)

        _set_status(report, STATUS_DB_WRITE_PASSED)
        return _finalize(report, raw_values, exit_code=0)
    except Exception:
        _set_status(report, STATUS_DB_READ_FAILED, "unexpected")
        return _finalize(report, raw_values, exit_code=1)
    finally:
        if session is not None:
            try:
                if not committed:
                    await validator_base._maybe_await(session.rollback())
            finally:
                await validator_base._maybe_await(session.close())


def _finalize(
    report: dict[str, Any],
    raw_values: set[str],
    *,
    exit_code: int,
) -> validator_base.ScriptResult:
    if validator_base._report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_RAW_VALUE_EMISSION, "report.raw_value_emission")
        exit_code = 1
    return validator_base.ScriptResult(exit_code=exit_code, report=report)


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def generate_report(
    *,
    approve_db_read: bool = False,
    approve_db_write: bool = False,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    runtime_env_reader: validator_base.RuntimeEnvReader | None = None,
    database_session_factory: validator_base.DatabaseSessionFactory | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> validator_base.ScriptResult:
    return asyncio.run(
        generate_report_async(
            approve_db_read=approve_db_read,
            approve_db_write=approve_db_write,
            runtime_env_path=runtime_env_path,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = generate_report(
        approve_db_read=args.approve_db_read,
        approve_db_write=args.approve_db_write,
        runtime_env_path=args.runtime_env_path,
    )
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
