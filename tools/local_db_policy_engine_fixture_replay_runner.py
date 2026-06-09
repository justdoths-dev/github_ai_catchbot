from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from numbers import Real
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from tools import local_db_analysis_validator_fixture_replay_runner as validator_runner


SCHEMA_VERSION = "local_db_policy_engine_fixture_replay_v1"
ANALYSIS_POLICY_APPLY_EVENT_TYPE = "analysis.policy.apply.v1"
NOTIFICATION_PLAN_CREATED_EVENT_TYPE = "notification.plan.created.v1"
ANALYSIS_SCHEMA_VERSION = "analysis_v1"
POLICY_VERSION = "verdict_policy_v1"
DELIVERY_POLICY_VERSION = "delivery_policy_v1"
POLICY_FROM_STATE = "analysis_validated"
POLICY_TO_STATE = "analysis_policy_applied"
LOCAL_TEST_TARGET_CHAT_ID = 424242001
LOCAL_TEST_TARGET_THREAD_ID = None
RENDER_PROFILE_HIGH = "telegram_single_alert_high_v1"
RENDER_PROFILE_NORMAL = "telegram_single_alert_normal_v1"
ALLOWED_VERDICTS = frozenset({"inspect_now", "later", "skip"})
ALLOWED_DELIVERY_DECISIONS = frozenset({"send_now", "send_digest", "suppress"})
GITHUB_PRIMARY_TYPES = frozenset({"github_repo", "github_subpath", "github_repo_page", "github_gist"})
TEXT_LIKE_PRIMARY_TYPES = frozenset({"x_post", "web_article", "text_idea"})
REQUIRED_POLICY_EVENT_KEYS = ("judge_run_id", "judge_output_id", "candidate_group_id", "bundle_id")
SAFE_EXCEPTION_MESSAGES = {
    "analysis_policy_apply_event_missing_or_invalid",
    "analysis_policy_apply_payload_invalid",
    "judge_run_missing",
    "judge_output_missing",
    "bundle_context_missing",
    "candidate_context_missing",
    "durable_relation_invalid",
}
TRUE_RESULT_KEYS = (
    "database_url_guard_passed",
    "source_candidate_replay_confirmed",
    "artifact_snapshot_replay_confirmed",
    "evidence_bundle_replay_confirmed",
    "analysis_router_replay_confirmed",
    "judge_output_replay_confirmed",
    "analysis_validator_replay_confirmed",
    "analysis_policy_apply_event_found",
    "judge_run_succeeded_confirmed",
    "judge_output_loaded",
    "bundle_context_confirmed",
    "analysis_validation_state_transition_found",
    "analysis_created",
    "policy_state_transition_recorded",
    "notification_plan_intent_event_created",
)
FALSE_RESULT_KEYS = (
    "judge_output_mutated",
    "bundle_mutated",
    "candidate_group_mutated",
    "notification_plan_created",
    "notification_render_created",
    "notification_delivery_created",
    "production_db_write",
    "live_github_called",
    "live_telegram_called",
    "openai_called",
    "workers_started",
    "redis_mutation",
)


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReplayExecutionResult:
    analysis_policy_apply_event_found: bool
    judge_run_succeeded_confirmed: bool
    judge_output_loaded: bool
    bundle_context_confirmed: bool
    analysis_validation_state_transition_found: bool
    analysis_created: bool
    policy_state_transition_recorded: bool
    notification_plan_intent_event_created: bool
    judge_output_mutated: bool = False
    bundle_mutated: bool = False
    candidate_group_mutated: bool = False
    notification_plan_created: bool = False
    notification_render_created: bool = False
    notification_delivery_created: bool = False
    checks_failed: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PolicyApplyEvent:
    event_id: UUID
    judge_run_id: UUID
    judge_output_id: UUID
    candidate_group_id: UUID
    bundle_id: UUID


@dataclass(frozen=True, slots=True)
class CandidateContext:
    candidate_group_id: UUID
    current_bundle_id: UUID | None
    current_analysis_id: UUID | None
    current_primary_artifact_id: UUID | None


@dataclass(frozen=True, slots=True)
class JudgeRunRecord:
    judge_run_id: UUID
    bundle_id: UUID
    prompt_version: str
    policy_version: str
    status: str


@dataclass(frozen=True, slots=True)
class JudgeOutputRecord:
    judge_output_id: UUID
    judge_run_id: UUID
    candidate_group_id: UUID
    judge_schema_version: str
    payload_json: dict[str, Any]
    model_proposed_verdict: str | None
    model_confidence_band: str | None


@dataclass(frozen=True, slots=True)
class BundleContext:
    bundle_id: UUID
    candidate_group_id: UUID
    current_primary_artifact_id: UUID
    current_primary_artifact_type: str | None
    ready_for_analysis: bool


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    verdict: str
    delivery_decision: str
    urgency_profile: str
    scores_json: dict[str, int]
    reason_codes_json: list[str]
    policy_reconciled_flag: bool
    evidence_limitations_ko: str | None
    recommended_action_ko: str | None
    freshness_note_ko: str | None
    model_proposed_verdict: str | None
    suppress_reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class NotificationPlanIntent:
    notification_plan_id: UUID
    analysis_id: UUID
    candidate_group_id: UUID
    delivery_decision: str
    urgency_profile: str
    target_chat_id: int
    target_thread_id: int | None
    render_profile: str
    dedupe_subject_key: str
    material_change_hash: str
    send_after: str | None
    suppress_reason_code: str | None


class PolicyEngineReplayExecutor(Protocol):
    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
    ) -> ReplayExecutionResult: ...


class AnalysisValidatorReplayRunner(Protocol):
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> validator_runner.RunnerResult: ...


class DefaultAnalysisValidatorReplayRunner:
    def run(
        self,
        *,
        database_url: str,
        source_fixture_path: Path,
        github_snapshot_fixture_path: Path,
        replay_namespace: str,
        env: Mapping[str, str],
        repo_root: Path,
    ) -> validator_runner.RunnerResult:
        args = argparse.Namespace(
            database_url=database_url,
            source_fixture=str(source_fixture_path),
            github_snapshot_fixture=str(github_snapshot_fixture_path),
            replay_namespace=replay_namespace,
            confirm_local_test_db=True,
        )
        predecessor_env = dict(env)
        predecessor_env["APP_ENV"] = "test"
        return validator_runner.run(args, env=predecessor_env, repo_root=repo_root)


class SqlAlchemyPolicyEngineReplayExecutor:
    def execute(
        self,
        *,
        database_url: str,
        replay_namespace: str,
    ) -> ReplayExecutionResult:
        _bootstrap_repo_imports()
        import sqlalchemy as sa

        engine = sa.create_engine(database_url, future=True)
        try:
            with engine.begin() as connection:
                return _execute_policy_engine_replay(connection, replay_namespace=replay_namespace)
        finally:
            engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replay predecessor local/test DB fixtures through analysis.policy.apply, "
            "then deterministically apply the policy-engine fixture contract and emit "
            "one namespace-scoped notification.plan.created.v1 plan-intent event."
        )
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--source-fixture", required=True)
    parser.add_argument("--github-snapshot-fixture", required=True)
    parser.add_argument("--replay-namespace", required=True)
    parser.add_argument("--confirm-local-test-db", action="store_true")
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    executor: PolicyEngineReplayExecutor | None = None,
    predecessor_runner: AnalysisValidatorReplayRunner | None = None,
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

    namespace_ok, namespace_failures = (
        validator_runner.fake_judge_runner.analysis_router_runner.evidence_bundle_runner.source_candidate_runner.validate_replay_namespace(
            args.replay_namespace
        )
    )
    checks_failed.extend(namespace_failures)

    database_ok, database_failures, _ = validate_database_url(args.database_url)
    report["database_url_guard_passed"] = database_ok
    checks_failed.extend(database_failures)

    try:
        validator_runner.fake_judge_runner.analysis_router_runner.evidence_bundle_runner.source_candidate_runner.load_source_fixture(
            Path(args.source_fixture),
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("source_fixture_load_failed")

    try:
        validator_runner.fake_judge_runner.analysis_router_runner.evidence_bundle_runner.github_snapshot_runner.load_github_snapshot_fixture(
            Path(args.github_snapshot_fixture),
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - operator output must stay sanitized.
        checks_failed.append("github_snapshot_fixture_load_failed")

    if checks_failed:
        return _finish(report, checks_failed)

    if not namespace_ok:
        checks_failed.append("precondition_missing")
        return _finish(report, checks_failed)

    active_predecessor = predecessor_runner or DefaultAnalysisValidatorReplayRunner()
    try:
        predecessor = active_predecessor.run(
            database_url=args.database_url,
            source_fixture_path=Path(args.source_fixture),
            github_snapshot_fixture_path=Path(args.github_snapshot_fixture),
            replay_namespace=args.replay_namespace,
            env=effective_env,
            repo_root=root,
        )
    except Exception:  # noqa: BLE001 - never echo DB or runtime error bodies.
        checks_failed.append("analysis_validator_replay_failed")
        return _finish(report, checks_failed)

    if not _predecessor_result_acceptable(predecessor):
        checks_failed.append("analysis_validator_replay_failed")
        return _finish(report, checks_failed)

    report["source_candidate_replay_confirmed"] = predecessor.report.get("source_candidate_replay_confirmed") is True
    report["artifact_snapshot_replay_confirmed"] = predecessor.report.get("artifact_snapshot_replay_confirmed") is True
    report["evidence_bundle_replay_confirmed"] = predecessor.report.get("evidence_bundle_replay_confirmed") is True
    report["analysis_router_replay_confirmed"] = predecessor.report.get("analysis_router_replay_confirmed") is True
    report["judge_output_replay_confirmed"] = predecessor.report.get("judge_output_replay_confirmed") is True
    report["analysis_validator_replay_confirmed"] = _predecessor_validator_confirmed(predecessor.report)
    for key in (
        "source_candidate_replay_confirmed",
        "artifact_snapshot_replay_confirmed",
        "evidence_bundle_replay_confirmed",
        "analysis_router_replay_confirmed",
        "judge_output_replay_confirmed",
        "analysis_validator_replay_confirmed",
    ):
        if report[key] is not True:
            checks_failed.append(f"{key}:missing")
    if checks_failed:
        return _finish(report, checks_failed)

    active_executor = executor or SqlAlchemyPolicyEngineReplayExecutor()
    try:
        execution = active_executor.execute(
            database_url=args.database_url,
            replay_namespace=args.replay_namespace,
        )
    except Exception as exc:  # noqa: BLE001 - never echo DB errors or URLs.
        checks_failed.append(_safe_failure_code(exc))
        return _finish(report, checks_failed)

    report.update(
        {
            "analysis_policy_apply_event_found": execution.analysis_policy_apply_event_found,
            "judge_run_succeeded_confirmed": execution.judge_run_succeeded_confirmed,
            "judge_output_loaded": execution.judge_output_loaded,
            "bundle_context_confirmed": execution.bundle_context_confirmed,
            "analysis_validation_state_transition_found": execution.analysis_validation_state_transition_found,
            "analysis_created": execution.analysis_created,
            "policy_state_transition_recorded": execution.policy_state_transition_recorded,
            "notification_plan_intent_event_created": execution.notification_plan_intent_event_created,
            "judge_output_mutated": execution.judge_output_mutated,
            "bundle_mutated": execution.bundle_mutated,
            "candidate_group_mutated": execution.candidate_group_mutated,
            "notification_plan_created": execution.notification_plan_created,
            "notification_render_created": execution.notification_render_created,
            "notification_delivery_created": execution.notification_delivery_created,
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


def validate_database_url(
    database_url: str | None,
) -> tuple[bool, list[str], validator_runner.fake_judge_runner.analysis_router_runner.evidence_bundle_runner.source_candidate_runner.ParsedDatabaseUrl | None]:
    return validator_runner.validate_database_url(database_url)


def validate_policy_apply_payload(payload: Mapping[str, Any]) -> tuple[bool, tuple[str, ...]]:
    if not isinstance(payload, Mapping):
        return False, ("payload_not_object",)
    failures: list[str] = []
    for key in REQUIRED_POLICY_EVENT_KEYS:
        if key not in payload:
            failures.append(f"missing:{key}")
            continue
        if _uuid_or_none(payload.get(key)) is None:
            failures.append(f"{key}:invalid_uuid")
    return not failures, tuple(failures)


def build_notification_plan_created_dedupe_key(
    *,
    replay_namespace: str,
    analysis_id: UUID | str,
    target_chat_id: int,
    material_change_hash: str,
) -> str:
    return (
        "local-db-policy-engine:"
        f"{replay_namespace}:notification.plan.created:{analysis_id}:{target_chat_id}:{material_change_hash}"
    )


def build_notification_plan_id(
    *,
    analysis_id: UUID | str,
    target_chat_id: int,
    material_change_hash: str,
) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        f"github_ai_catchbot:local-db-policy-engine:{analysis_id}:{target_chat_id}:{material_change_hash}",
    )


def build_material_change_hash(
    *,
    candidate_group_id: UUID | str,
    verdict: str,
    delivery_decision: str,
    urgency_profile: str,
    reason_codes_json: Sequence[str],
    recommended_action_ko: str | None,
    freshness_note_ko: str | None,
) -> str:
    payload = {
        "candidate_group_id": str(candidate_group_id),
        "verdict": verdict,
        "delivery_decision": delivery_decision,
        "urgency_profile": urgency_profile,
        "reason_codes_json": list(reason_codes_json),
        "recommended_action_ko": recommended_action_ko,
        "freshness_note_ko": freshness_note_ko,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_policy_scores(payload: Mapping[str, Any]) -> dict[str, int]:
    raw_scores = payload.get("scores") if isinstance(payload.get("scores"), Mapping) else {}
    implementation_signal = _score(raw_scores, "implementation_signal")
    novelty = _score(raw_scores, "novelty")
    return {
        "novelty": novelty,
        "practical_usefulness": _score(raw_scores, "practical_usefulness", fallback=implementation_signal),
        "evidence_strength": _score(raw_scores, "evidence_strength"),
        "hype_penalty": _score(raw_scores, "hype_penalty"),
        "confidence": _score(raw_scores, "confidence", fallback=_confidence_band_score(payload.get("model_confidence_band"))),
        "code_quality": _score(raw_scores, "code_quality", fallback=implementation_signal),
        "maintenance_signal": _score(raw_scores, "maintenance_signal", fallback=implementation_signal),
        "specificity": _score(raw_scores, "specificity", fallback=novelty),
        "reproducibility_signal": _score(raw_scores, "reproducibility_signal", fallback=implementation_signal),
        "implementation_signal": implementation_signal,
        "urgency": _score(raw_scores, "urgency"),
    }


def evaluate_verdict_policy(
    *,
    payload: Mapping[str, Any],
    current_primary_artifact_type: str | None,
) -> tuple[str, list[str], dict[str, int]]:
    scores = build_policy_scores(payload)
    practical = scores["practical_usefulness"]
    evidence = scores["evidence_strength"]
    confidence = scores["confidence"]
    hype = scores["hype_penalty"]
    code_quality = scores["code_quality"]
    specificity = scores["specificity"]

    if (
        practical >= 70
        and evidence >= 50
        and confidence >= 60
        and hype < 70
        and _primary_gate(
            artifact_type=current_primary_artifact_type,
            code_quality=code_quality,
            specificity=specificity,
        )
    ):
        return "inspect_now", ["policy_threshold_inspect_now"], scores

    if practical >= 45 and evidence >= 30 and confidence >= 35:
        return "later", ["policy_threshold_later"], scores

    return "skip", ["policy_threshold_skip"], scores


def evaluate_delivery_policy(
    *,
    verdict: str,
    enable_later_delivery: bool = True,
) -> tuple[str, str, str | None]:
    if verdict == "inspect_now":
        return "send_now", "high", None
    if verdict == "later":
        if enable_later_delivery:
            return "send_now", "normal_silent", None
        return "suppress", "suppressed", "later_delivery_disabled"
    return "suppress", "suppressed", "verdict_skip"


def build_policy_decision(
    *,
    judge_output: JudgeOutputRecord,
    bundle: BundleContext,
) -> PolicyDecision:
    payload = judge_output.payload_json if isinstance(judge_output.payload_json, Mapping) else {}
    verdict, verdict_reasons, scores = evaluate_verdict_policy(
        payload=payload,
        current_primary_artifact_type=bundle.current_primary_artifact_type,
    )
    delivery_decision, urgency_profile, suppress_reason_code = evaluate_delivery_policy(verdict=verdict)
    reason_codes = [
        *_string_list(payload.get("reason_codes")),
        *verdict_reasons,
    ]
    if suppress_reason_code:
        reason_codes.append(suppress_reason_code)

    model_proposed_verdict = judge_output.model_proposed_verdict
    policy_reconciled_flag = model_proposed_verdict == verdict
    if not model_proposed_verdict:
        reason_codes.append("policy_no_model_verdict")
    elif not policy_reconciled_flag:
        reason_codes.append("policy_overrode_model_verdict")

    return PolicyDecision(
        verdict=verdict,
        delivery_decision=delivery_decision,
        urgency_profile=urgency_profile,
        scores_json=scores,
        reason_codes_json=_dedupe_strings(reason_codes),
        policy_reconciled_flag=policy_reconciled_flag,
        evidence_limitations_ko=_text_column_value(payload.get("evidence_limitations_ko")),
        recommended_action_ko=_text_column_value(payload.get("recommended_action_ko")),
        freshness_note_ko=_text_column_value(payload.get("freshness_note_ko")),
        model_proposed_verdict=model_proposed_verdict if model_proposed_verdict in ALLOWED_VERDICTS else None,
        suppress_reason_code=suppress_reason_code,
    )


def build_notification_plan_intent(
    *,
    analysis_id: UUID,
    candidate_group_id: UUID,
    decision: PolicyDecision,
) -> NotificationPlanIntent | None:
    if decision.delivery_decision == "suppress":
        return None
    material_change_hash = build_material_change_hash(
        candidate_group_id=candidate_group_id,
        verdict=decision.verdict,
        delivery_decision=decision.delivery_decision,
        urgency_profile=decision.urgency_profile,
        reason_codes_json=decision.reason_codes_json,
        recommended_action_ko=decision.recommended_action_ko,
        freshness_note_ko=decision.freshness_note_ko,
    )
    return NotificationPlanIntent(
        notification_plan_id=build_notification_plan_id(
            analysis_id=analysis_id,
            target_chat_id=LOCAL_TEST_TARGET_CHAT_ID,
            material_change_hash=material_change_hash,
        ),
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
        delivery_decision=decision.delivery_decision,
        urgency_profile=decision.urgency_profile,
        target_chat_id=LOCAL_TEST_TARGET_CHAT_ID,
        target_thread_id=LOCAL_TEST_TARGET_THREAD_ID,
        render_profile=RENDER_PROFILE_HIGH if decision.urgency_profile == "high" else RENDER_PROFILE_NORMAL,
        dedupe_subject_key=str(candidate_group_id),
        material_change_hash=material_change_hash,
        send_after=None,
        suppress_reason_code=decision.suppress_reason_code,
    )


def notification_plan_intent_payload(intent: NotificationPlanIntent) -> dict[str, Any]:
    return {
        "notification_plan_id": str(intent.notification_plan_id),
        "analysis_id": str(intent.analysis_id),
        "candidate_group_id": str(intent.candidate_group_id),
        "delivery_decision": intent.delivery_decision,
        "urgency_profile": intent.urgency_profile,
        "target_chat_id": intent.target_chat_id,
        "target_thread_id": intent.target_thread_id,
        "render_profile": intent.render_profile,
        "dedupe_subject_key": intent.dedupe_subject_key,
        "material_change_hash": intent.material_change_hash,
        "send_after": intent.send_after,
        "suppress_reason_code": intent.suppress_reason_code,
    }


def _execute_policy_engine_replay(connection: Any, *, replay_namespace: str) -> ReplayExecutionResult:
    checks_failed: list[str] = []

    event = _load_analysis_policy_apply_event(connection, replay_namespace=replay_namespace)
    event_found = event is not None
    if event is None:
        checks_failed.append("analysis_policy_apply_event_missing_or_invalid")
        return _execution_result(analysis_policy_apply_event_found=event_found, checks_failed=checks_failed)

    candidate = _load_candidate_context(connection, event.candidate_group_id)
    judge_run = _load_judge_run(connection, event.judge_run_id)
    judge_output = _load_judge_output(connection, event.judge_output_id)
    bundle = _load_bundle_context(connection, event.bundle_id)
    validation_transition_found = _analysis_validation_transition_exists(connection, judge_run_id=event.judge_run_id)

    if candidate is None:
        checks_failed.append("candidate_context_missing")
        _insert_or_reuse_candidate_state_transition(
            connection,
            candidate_group_id=event.candidate_group_id,
            to_state="analysis_policy_failed",
            reason_code="policy_missing_candidate_context",
        )
        return _execution_result(
            analysis_policy_apply_event_found=event_found,
            analysis_validation_state_transition_found=validation_transition_found,
            checks_failed=checks_failed,
        )

    if judge_run is None:
        checks_failed.append("judge_run_missing")
        _insert_or_reuse_candidate_state_transition(
            connection,
            candidate_group_id=event.candidate_group_id,
            to_state="analysis_policy_failed",
            reason_code="policy_missing_judge_run",
        )
        return _execution_result(
            analysis_policy_apply_event_found=event_found,
            analysis_validation_state_transition_found=validation_transition_found,
            checks_failed=checks_failed,
        )

    if judge_output is None:
        checks_failed.append("judge_output_missing")
        _insert_or_reuse_candidate_state_transition(
            connection,
            candidate_group_id=event.candidate_group_id,
            to_state="analysis_policy_failed",
            reason_code="policy_missing_judge_output",
        )
        return _execution_result(
            analysis_policy_apply_event_found=event_found,
            judge_run_succeeded_confirmed=judge_run.status == "succeeded",
            analysis_validation_state_transition_found=validation_transition_found,
            checks_failed=checks_failed,
        )

    if bundle is None:
        checks_failed.append("bundle_context_missing")
        _insert_or_reuse_candidate_state_transition(
            connection,
            candidate_group_id=event.candidate_group_id,
            to_state="analysis_policy_failed",
            reason_code="policy_missing_bundle_context",
        )
        return _execution_result(
            analysis_policy_apply_event_found=event_found,
            judge_run_succeeded_confirmed=judge_run.status == "succeeded",
            judge_output_loaded=True,
            analysis_validation_state_transition_found=validation_transition_found,
            checks_failed=checks_failed,
        )

    context_failures = _policy_context_failures(
        event=event,
        candidate=candidate,
        judge_run=judge_run,
        judge_output=judge_output,
        bundle=bundle,
        validation_transition_found=validation_transition_found,
    )
    if context_failures:
        checks_failed.extend(context_failures)
        _insert_or_reuse_candidate_state_transition(
            connection,
            candidate_group_id=event.candidate_group_id,
            to_state=(
                "analysis_policy_stale_bundle"
                if "candidate_current_bundle_mismatch" in context_failures
                else "analysis_policy_failed"
            ),
            reason_code=(
                "policy_stale_bundle_request"
                if "candidate_current_bundle_mismatch" in context_failures
                else "policy_context_invalid"
            ),
        )
        return _execution_result(
            analysis_policy_apply_event_found=event_found,
            judge_run_succeeded_confirmed=judge_run.status == "succeeded",
            judge_output_loaded=True,
            bundle_context_confirmed=False,
            analysis_validation_state_transition_found=validation_transition_found,
            checks_failed=checks_failed,
        )

    judge_output_before = _load_judge_output_digest(connection, judge_output.judge_output_id)
    bundle_before = _load_bundle_digest(connection, bundle.bundle_id)
    candidate_before = _load_candidate_digest(connection, candidate.candidate_group_id)
    decision = build_policy_decision(judge_output=judge_output, bundle=bundle)

    analysis_id = _insert_or_reuse_analysis(
        connection,
        candidate_group_id=event.candidate_group_id,
        judge_output_id=event.judge_output_id,
        prompt_version=judge_run.prompt_version,
        decision=decision,
    )
    _insert_or_reuse_policy_state_transition(
        connection,
        analysis_id=analysis_id,
        verdict=decision.verdict,
        delivery_decision=decision.delivery_decision,
    )
    intent = build_notification_plan_intent(
        analysis_id=analysis_id,
        candidate_group_id=event.candidate_group_id,
        decision=decision,
    )
    if intent is not None:
        _insert_or_reuse_notification_plan_intent_event(
            connection,
            replay_namespace=replay_namespace,
            intent=intent,
        )

    verification = _verify_success(
        connection,
        replay_namespace=replay_namespace,
        event=event,
        analysis_id=analysis_id,
        decision=decision,
        intent=intent,
        judge_output_before=judge_output_before,
        bundle_before=bundle_before,
        candidate_before=candidate_before,
    )
    checks_failed.extend(verification["checks_failed"])

    return ReplayExecutionResult(
        analysis_policy_apply_event_found=event_found,
        judge_run_succeeded_confirmed=judge_run.status == "succeeded",
        judge_output_loaded=True,
        bundle_context_confirmed=True,
        analysis_validation_state_transition_found=validation_transition_found,
        analysis_created=verification["analysis_created"],
        policy_state_transition_recorded=verification["policy_state_transition_recorded"],
        notification_plan_intent_event_created=verification["notification_plan_intent_event_created"],
        judge_output_mutated=verification["judge_output_mutated"],
        bundle_mutated=verification["bundle_mutated"],
        candidate_group_mutated=verification["candidate_group_mutated"],
        notification_plan_created=verification["notification_plan_created"],
        notification_render_created=verification["notification_render_created"],
        notification_delivery_created=verification["notification_delivery_created"],
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _load_analysis_policy_apply_event(connection: Any, *, replay_namespace: str) -> PolicyApplyEvent | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT event_id, payload_json
            FROM event_outbox
            WHERE event_type = :event_type
              AND dedupe_key LIKE :dedupe_prefix
            ORDER BY created_at DESC, event_id DESC
            LIMIT 1
            """
        ),
        {
            "event_type": ANALYSIS_POLICY_APPLY_EVENT_TYPE,
            "dedupe_prefix": f"local-db-analysis-validator:{replay_namespace}:analysis.policy.apply:%",
        },
    ).mappings().first()
    if row is None:
        return None

    payload = _json_loads(row["payload_json"]) or {}
    valid, _failures = validate_policy_apply_payload(payload)
    if not valid:
        return None
    return PolicyApplyEvent(
        event_id=UUID(str(row["event_id"])),
        judge_run_id=UUID(str(payload["judge_run_id"])),
        judge_output_id=UUID(str(payload["judge_output_id"])),
        candidate_group_id=UUID(str(payload["candidate_group_id"])),
        bundle_id=UUID(str(payload["bundle_id"])),
    )


def _load_candidate_context(connection: Any, candidate_group_id: UUID) -> CandidateContext | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT candidate_group_id, current_bundle_id, current_analysis_id, current_primary_artifact_id
            FROM candidate_group_proposals
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    ).mappings().first()
    if row is None:
        return None
    return CandidateContext(
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        current_bundle_id=_uuid_or_none(row["current_bundle_id"]),
        current_analysis_id=_uuid_or_none(row["current_analysis_id"]),
        current_primary_artifact_id=_uuid_or_none(row["current_primary_artifact_id"]),
    )


def _load_judge_run(connection: Any, judge_run_id: UUID) -> JudgeRunRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT judge_run_id, bundle_id, prompt_version, policy_version, status
            FROM judge_runs
            WHERE judge_run_id = CAST(:judge_run_id AS uuid)
            """
        ),
        {"judge_run_id": str(judge_run_id)},
    ).mappings().first()
    if row is None:
        return None
    return JudgeRunRecord(
        judge_run_id=UUID(str(row["judge_run_id"])),
        bundle_id=UUID(str(row["bundle_id"])),
        prompt_version=str(row["prompt_version"]),
        policy_version=str(row["policy_version"]),
        status=str(row["status"]),
    )


def _load_judge_output(connection: Any, judge_output_id: UUID) -> JudgeOutputRecord | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT judge_output_id, judge_run_id, candidate_group_id, judge_schema_version,
                   payload_json, model_proposed_verdict, model_confidence_band
            FROM judge_outputs
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
            """
        ),
        {"judge_output_id": str(judge_output_id)},
    ).mappings().first()
    if row is None:
        return None
    return JudgeOutputRecord(
        judge_output_id=UUID(str(row["judge_output_id"])),
        judge_run_id=UUID(str(row["judge_run_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        judge_schema_version=str(row["judge_schema_version"]),
        payload_json=_json_loads(row["payload_json"]) or {},
        model_proposed_verdict=_string_or_none(row["model_proposed_verdict"]),
        model_confidence_band=_string_or_none(row["model_confidence_band"]),
    )


def _load_bundle_context(connection: Any, bundle_id: UUID) -> BundleContext | None:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT b.bundle_id, b.candidate_group_id, b.current_primary_artifact_id,
                   ar.artifact_type AS current_primary_artifact_type, b.ready_for_analysis
            FROM candidate_evidence_bundles b
            LEFT JOIN artifact_registry ar
              ON ar.artifact_id = b.current_primary_artifact_id
            WHERE b.bundle_id = CAST(:bundle_id AS uuid)
            """
        ),
        {"bundle_id": str(bundle_id)},
    ).mappings().first()
    if row is None:
        return None
    return BundleContext(
        bundle_id=UUID(str(row["bundle_id"])),
        candidate_group_id=UUID(str(row["candidate_group_id"])),
        current_primary_artifact_id=UUID(str(row["current_primary_artifact_id"])),
        current_primary_artifact_type=_string_or_none(row["current_primary_artifact_type"]),
        ready_for_analysis=bool(row["ready_for_analysis"]),
    )


def _analysis_validation_transition_exists(connection: Any, *, judge_run_id: UUID) -> bool:
    import sqlalchemy as sa

    return bool(
        connection.execute(
            sa.text(
                """
                SELECT 1
                FROM state_transitions
                WHERE object_type = 'judge_run'
                  AND object_id = CAST(:judge_run_id AS uuid)
                  AND from_state = 'succeeded'
                  AND to_state = 'analysis_validated'
                  AND reason_code = 'judge_output_validated'
                LIMIT 1
                """
            ),
            {"judge_run_id": str(judge_run_id)},
        ).scalar_one_or_none()
    )


def _policy_context_failures(
    *,
    event: PolicyApplyEvent,
    candidate: CandidateContext,
    judge_run: JudgeRunRecord,
    judge_output: JudgeOutputRecord,
    bundle: BundleContext,
    validation_transition_found: bool,
) -> list[str]:
    failures: list[str] = []
    if judge_run.status != "succeeded":
        failures.append("judge_run_status_not_succeeded")
    if judge_run.bundle_id != event.bundle_id:
        failures.append("judge_run_bundle_mismatch")
    if judge_output.judge_run_id != event.judge_run_id:
        failures.append("judge_output_run_mismatch")
    if judge_output.candidate_group_id != event.candidate_group_id:
        failures.append("judge_output_candidate_mismatch")
    if bundle.candidate_group_id != event.candidate_group_id:
        failures.append("bundle_candidate_mismatch")
    if judge_output.candidate_group_id != bundle.candidate_group_id:
        failures.append("judge_output_bundle_candidate_mismatch")
    if candidate.current_bundle_id != event.bundle_id:
        failures.append("candidate_current_bundle_mismatch")
    if not validation_transition_found:
        failures.append("analysis_validation_state_transition_missing")
    if not bundle.ready_for_analysis:
        failures.append("evidence_bundle_not_ready")
    return failures


def _insert_or_reuse_analysis(
    connection: Any,
    *,
    candidate_group_id: UUID,
    judge_output_id: UUID,
    prompt_version: str,
    decision: PolicyDecision,
) -> UUID:
    import sqlalchemy as sa

    result = connection.execute(
        sa.text(
            """
            INSERT INTO analyses (
                candidate_group_id,
                judge_output_id,
                schema_version,
                policy_version,
                prompt_version,
                delivery_policy_version,
                verdict,
                delivery_decision,
                scores_json,
                reason_codes_json,
                evidence_limitations_ko,
                recommended_action_ko,
                freshness_note_ko,
                model_proposed_verdict,
                policy_reconciled_flag,
                created_at
            ) VALUES (
                CAST(:candidate_group_id AS uuid),
                CAST(:judge_output_id AS uuid),
                :schema_version,
                :policy_version,
                :prompt_version,
                :delivery_policy_version,
                CAST(:verdict AS verdict_enum),
                CAST(:delivery_decision AS delivery_decision_enum),
                CAST(:scores_json AS jsonb),
                CAST(:reason_codes_json AS jsonb),
                :evidence_limitations_ko,
                :recommended_action_ko,
                :freshness_note_ko,
                CAST(:model_proposed_verdict AS verdict_enum),
                :policy_reconciled_flag,
                now()
            )
            ON CONFLICT ON CONSTRAINT uq_analyses_judge_output_policy_delivery_policy
            DO NOTHING
            RETURNING analysis_id
            """
        ),
        {
            "candidate_group_id": str(candidate_group_id),
            "judge_output_id": str(judge_output_id),
            "schema_version": ANALYSIS_SCHEMA_VERSION,
            "policy_version": POLICY_VERSION,
            "prompt_version": prompt_version,
            "delivery_policy_version": DELIVERY_POLICY_VERSION,
            "verdict": decision.verdict,
            "delivery_decision": decision.delivery_decision,
            "scores_json": _json_dumps(decision.scores_json),
            "reason_codes_json": _json_dumps(decision.reason_codes_json),
            "evidence_limitations_ko": decision.evidence_limitations_ko,
            "recommended_action_ko": decision.recommended_action_ko,
            "freshness_note_ko": decision.freshness_note_ko,
            "model_proposed_verdict": decision.model_proposed_verdict,
            "policy_reconciled_flag": decision.policy_reconciled_flag,
        },
    )
    analysis_id = result.scalar_one_or_none()
    if analysis_id is not None:
        return UUID(str(analysis_id))

    existing = connection.execute(
        sa.text(
            """
            SELECT analysis_id
            FROM analyses
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
              AND policy_version = :policy_version
              AND delivery_policy_version = :delivery_policy_version
            """
        ),
        {
            "judge_output_id": str(judge_output_id),
            "policy_version": POLICY_VERSION,
            "delivery_policy_version": DELIVERY_POLICY_VERSION,
        },
    ).scalar_one_or_none()
    if existing is None:
        raise RuntimeError("analysis insert conflicted but existing analysis was not found")
    return UUID(str(existing))


def _insert_or_reuse_policy_state_transition(
    connection: Any,
    *,
    analysis_id: UUID,
    verdict: str,
    delivery_decision: str,
) -> None:
    import sqlalchemy as sa

    reason_code = f"policy_applied:{verdict}:{delivery_decision}"
    existing = connection.execute(
        sa.text(
            """
            SELECT 1
            FROM state_transitions
            WHERE object_type = 'analysis'
              AND object_id = CAST(:analysis_id AS uuid)
              AND from_state = :from_state
              AND to_state = :to_state
              AND reason_code = :reason_code
            LIMIT 1
            """
        ),
        {
            "analysis_id": str(analysis_id),
            "from_state": POLICY_FROM_STATE,
            "to_state": POLICY_TO_STATE,
            "reason_code": reason_code,
        },
    ).scalar_one_or_none()
    if existing:
        return

    connection.execute(
        sa.text(
            """
            INSERT INTO state_transitions (
                state_transition_id,
                object_type,
                object_id,
                from_state,
                to_state,
                reason_code,
                created_at
            ) VALUES (
                gen_random_uuid(),
                'analysis',
                CAST(:analysis_id AS uuid),
                :from_state,
                :to_state,
                :reason_code,
                now()
            )
            """
        ),
        {
            "analysis_id": str(analysis_id),
            "from_state": POLICY_FROM_STATE,
            "to_state": POLICY_TO_STATE,
            "reason_code": reason_code,
        },
    )


def _insert_or_reuse_candidate_state_transition(
    connection: Any,
    *,
    candidate_group_id: UUID,
    to_state: str,
    reason_code: str,
) -> None:
    import sqlalchemy as sa

    existing = connection.execute(
        sa.text(
            """
            SELECT 1
            FROM state_transitions
            WHERE object_type = 'candidate_group'
              AND object_id = CAST(:candidate_group_id AS uuid)
              AND from_state = :from_state
              AND to_state = :to_state
              AND reason_code = :reason_code
            LIMIT 1
            """
        ),
        {
            "candidate_group_id": str(candidate_group_id),
            "from_state": POLICY_FROM_STATE,
            "to_state": to_state,
            "reason_code": reason_code,
        },
    ).scalar_one_or_none()
    if existing:
        return

    connection.execute(
        sa.text(
            """
            INSERT INTO state_transitions (
                state_transition_id,
                object_type,
                object_id,
                from_state,
                to_state,
                reason_code,
                created_at
            ) VALUES (
                gen_random_uuid(),
                'candidate_group',
                CAST(:candidate_group_id AS uuid),
                :from_state,
                :to_state,
                :reason_code,
                now()
            )
            """
        ),
        {
            "candidate_group_id": str(candidate_group_id),
            "from_state": POLICY_FROM_STATE,
            "to_state": to_state,
            "reason_code": reason_code,
        },
    )


def _insert_or_reuse_notification_plan_intent_event(
    connection: Any,
    *,
    replay_namespace: str,
    intent: NotificationPlanIntent,
) -> None:
    import sqlalchemy as sa

    connection.execute(
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
            ) VALUES (
                :event_type,
                'analysis',
                CAST(:analysis_id AS uuid),
                :dedupe_key,
                CAST(:payload_json AS jsonb),
                'pending'::outbox_status_enum,
                now()
            )
            ON CONFLICT ON CONSTRAINT uq_event_outbox_dedupe_key
            DO NOTHING
            """
        ),
        {
            "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
            "analysis_id": str(intent.analysis_id),
            "dedupe_key": build_notification_plan_created_dedupe_key(
                replay_namespace=replay_namespace,
                analysis_id=intent.analysis_id,
                target_chat_id=intent.target_chat_id,
                material_change_hash=intent.material_change_hash,
            ),
            "payload_json": _json_dumps(notification_plan_intent_payload(intent)),
        },
    )


def _verify_success(
    connection: Any,
    *,
    replay_namespace: str,
    event: PolicyApplyEvent,
    analysis_id: UUID,
    decision: PolicyDecision,
    intent: NotificationPlanIntent | None,
    judge_output_before: str,
    bundle_before: str,
    candidate_before: str,
) -> dict[str, Any]:
    import sqlalchemy as sa

    analysis_created = bool(
        connection.execute(
            sa.text(
                """
                SELECT 1
                FROM analyses
                WHERE analysis_id = CAST(:analysis_id AS uuid)
                  AND candidate_group_id = CAST(:candidate_group_id AS uuid)
                  AND judge_output_id = CAST(:judge_output_id AS uuid)
                  AND policy_version = :policy_version
                  AND delivery_policy_version = :delivery_policy_version
                  AND verdict = CAST(:verdict AS verdict_enum)
                  AND delivery_decision = CAST(:delivery_decision AS delivery_decision_enum)
                LIMIT 1
                """
            ),
            {
                "analysis_id": str(analysis_id),
                "candidate_group_id": str(event.candidate_group_id),
                "judge_output_id": str(event.judge_output_id),
                "policy_version": POLICY_VERSION,
                "delivery_policy_version": DELIVERY_POLICY_VERSION,
                "verdict": decision.verdict,
                "delivery_decision": decision.delivery_decision,
            },
        ).scalar_one_or_none()
    )
    policy_transition_recorded = bool(
        connection.execute(
            sa.text(
                """
                SELECT 1
                FROM state_transitions
                WHERE object_type = 'analysis'
                  AND object_id = CAST(:analysis_id AS uuid)
                  AND from_state = :from_state
                  AND to_state = :to_state
                  AND reason_code = :reason_code
                LIMIT 1
                """
            ),
            {
                "analysis_id": str(analysis_id),
                "from_state": POLICY_FROM_STATE,
                "to_state": POLICY_TO_STATE,
                "reason_code": f"policy_applied:{decision.verdict}:{decision.delivery_decision}",
            },
        ).scalar_one_or_none()
    )
    notification_event_created = False
    if intent is not None:
        dedupe_key = build_notification_plan_created_dedupe_key(
            replay_namespace=replay_namespace,
            analysis_id=intent.analysis_id,
            target_chat_id=intent.target_chat_id,
            material_change_hash=intent.material_change_hash,
        )
        row = connection.execute(
            sa.text(
                """
                SELECT payload_json
                FROM event_outbox
                WHERE event_type = :event_type
                  AND aggregate_type = 'analysis'
                  AND aggregate_id = CAST(:analysis_id AS uuid)
                  AND dedupe_key = :dedupe_key
                """
            ),
            {
                "event_type": NOTIFICATION_PLAN_CREATED_EVENT_TYPE,
                "analysis_id": str(analysis_id),
                "dedupe_key": dedupe_key,
            },
        ).mappings().first()
        notification_event_created = row is not None and _json_loads(row["payload_json"]) == notification_plan_intent_payload(intent)

    judge_output_mutated = _load_judge_output_digest(connection, event.judge_output_id) != judge_output_before
    bundle_mutated = _load_bundle_digest(connection, event.bundle_id) != bundle_before
    candidate_group_mutated = _load_candidate_digest(connection, event.candidate_group_id) != candidate_before
    forbidden = _load_forbidden_side_effect_flags(connection, candidate_group_id=event.candidate_group_id)
    checks = {
        "analysis_created": analysis_created,
        "policy_state_transition_recorded": policy_transition_recorded,
        "notification_plan_intent_event_created": notification_event_created if intent is not None else False,
        "judge_output_mutated": judge_output_mutated,
        "bundle_mutated": bundle_mutated,
        "candidate_group_mutated": candidate_group_mutated,
        **forbidden,
    }
    false_expected = {
        "judge_output_mutated",
        "bundle_mutated",
        "candidate_group_mutated",
        "notification_plan_created",
        "notification_render_created",
        "notification_delivery_created",
    }
    true_expected = {"analysis_created", "policy_state_transition_recorded"}
    if intent is not None:
        true_expected.add("notification_plan_intent_event_created")
    else:
        false_expected.add("notification_plan_intent_event_created")
    failures = []
    for key, value in checks.items():
        if key in false_expected and value is not False:
            failures.append(f"{key}:unexpected")
        if key in true_expected and value is not True:
            failures.append(f"{key}:missing")
    return {**checks, "checks_failed": failures}


def _load_forbidden_side_effect_flags(connection: Any, *, candidate_group_id: UUID) -> dict[str, bool]:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT
              EXISTS (
                SELECT 1 FROM notification_plans
                WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
              ) AS notification_plan_created,
              EXISTS (
                SELECT 1
                FROM notification_renders nr
                JOIN notification_plans np
                  ON np.notification_plan_id = nr.notification_plan_id
                WHERE np.candidate_group_id = CAST(:candidate_group_id AS uuid)
              ) AS notification_render_created,
              EXISTS (
                SELECT 1
                FROM notification_delivery_records ndr
                JOIN notification_plans np
                  ON np.notification_plan_id = ndr.notification_plan_id
                WHERE np.candidate_group_id = CAST(:candidate_group_id AS uuid)
              ) AS notification_delivery_created
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    ).mappings().one()
    return {
        "notification_plan_created": bool(row["notification_plan_created"]),
        "notification_render_created": bool(row["notification_render_created"]),
        "notification_delivery_created": bool(row["notification_delivery_created"]),
    }


def _load_judge_output_digest(connection: Any, judge_output_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT judge_output_id, judge_run_id, candidate_group_id, judge_schema_version,
                   payload_json, model_proposed_verdict, model_confidence_band
            FROM judge_outputs
            WHERE judge_output_id = CAST(:judge_output_id AS uuid)
            """
        ),
        {"judge_output_id": str(judge_output_id)},
    ).mappings().first()
    return _canonical_json(dict(row)) if row is not None else ""


def _load_bundle_digest(connection: Any, bundle_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT bundle_id, candidate_group_id, initial_primary_artifact_id,
                   current_primary_artifact_id, bundle_version, bundle_profile_version,
                   bundle_input_hash, reroot_count, primary_summary,
                   supporting_summaries_json, discovered_links_summary_json,
                   evidence_limitations, ready_for_analysis, token_budget_profile
            FROM candidate_evidence_bundles
            WHERE bundle_id = CAST(:bundle_id AS uuid)
            """
        ),
        {"bundle_id": str(bundle_id)},
    ).mappings().first()
    return _canonical_json(dict(row)) if row is not None else ""


def _load_candidate_digest(connection: Any, candidate_group_id: UUID) -> str:
    import sqlalchemy as sa

    row = connection.execute(
        sa.text(
            """
            SELECT candidate_group_id, current_bundle_id, current_analysis_id, current_primary_artifact_id
            FROM candidate_group_proposals
            WHERE candidate_group_id = CAST(:candidate_group_id AS uuid)
            """
        ),
        {"candidate_group_id": str(candidate_group_id)},
    ).mappings().first()
    return _canonical_json(dict(row)) if row is not None else ""


def _predecessor_result_acceptable(predecessor: validator_runner.RunnerResult) -> bool:
    if predecessor.report.get("status") == "pass" and predecessor.exit_code == 0:
        return True
    allowed_successor_failures = {
        "analysis_created:unexpected",
        "notification_created:unexpected",
    }
    checks_failed = set(predecessor.report.get("checks_failed") or [])
    return bool(checks_failed) and checks_failed.issubset(allowed_successor_failures)


def _predecessor_validator_confirmed(report: Mapping[str, Any]) -> bool:
    required = (
        "judge_output_ready_event_found",
        "judge_run_succeeded_confirmed",
        "judge_output_loaded",
        "bundle_context_confirmed",
        "structured_output_schema_valid",
        "semantic_validation_passed",
        "refusal_not_detected",
        "validation_state_transition_recorded",
        "analysis_policy_apply_event_created",
    )
    return all(report.get(key) is True for key in required)


def _execution_result(
    *,
    analysis_policy_apply_event_found: bool = False,
    judge_run_succeeded_confirmed: bool = False,
    judge_output_loaded: bool = False,
    bundle_context_confirmed: bool = False,
    analysis_validation_state_transition_found: bool = False,
    analysis_created: bool = False,
    policy_state_transition_recorded: bool = False,
    notification_plan_intent_event_created: bool = False,
    judge_output_mutated: bool = False,
    bundle_mutated: bool = False,
    candidate_group_mutated: bool = False,
    notification_plan_created: bool = False,
    notification_render_created: bool = False,
    notification_delivery_created: bool = False,
    checks_failed: list[str] | tuple[str, ...],
) -> ReplayExecutionResult:
    return ReplayExecutionResult(
        analysis_policy_apply_event_found=analysis_policy_apply_event_found,
        judge_run_succeeded_confirmed=judge_run_succeeded_confirmed,
        judge_output_loaded=judge_output_loaded,
        bundle_context_confirmed=bundle_context_confirmed,
        analysis_validation_state_transition_found=analysis_validation_state_transition_found,
        analysis_created=analysis_created,
        policy_state_transition_recorded=policy_state_transition_recorded,
        notification_plan_intent_event_created=notification_plan_intent_event_created,
        judge_output_mutated=judge_output_mutated,
        bundle_mutated=bundle_mutated,
        candidate_group_mutated=candidate_group_mutated,
        notification_plan_created=notification_plan_created,
        notification_render_created=notification_render_created,
        notification_delivery_created=notification_delivery_created,
        checks_failed=tuple(dict.fromkeys(checks_failed)),
    )


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "database_url_guard_passed": False,
        "source_candidate_replay_confirmed": False,
        "artifact_snapshot_replay_confirmed": False,
        "evidence_bundle_replay_confirmed": False,
        "analysis_router_replay_confirmed": False,
        "judge_output_replay_confirmed": False,
        "analysis_validator_replay_confirmed": False,
        "analysis_policy_apply_event_found": False,
        "judge_run_succeeded_confirmed": False,
        "judge_output_loaded": False,
        "bundle_context_confirmed": False,
        "analysis_validation_state_transition_found": False,
        "analysis_created": False,
        "policy_state_transition_recorded": False,
        "notification_plan_intent_event_created": False,
        "judge_output_mutated": False,
        "bundle_mutated": False,
        "candidate_group_mutated": False,
        "notification_plan_created": False,
        "notification_render_created": False,
        "notification_delivery_created": False,
        "production_db_write": False,
        "live_github_called": False,
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


def _score(scores: Mapping[str, Any], key: str, *, fallback: int = 0) -> int:
    if key not in scores:
        return fallback
    value = scores.get(key)
    if isinstance(value, bool) or not isinstance(value, Real):
        return fallback
    numeric = float(value)
    if 0 <= numeric <= 1:
        numeric *= 100
    return max(0, min(100, int(round(numeric))))


def _confidence_band_score(value: Any) -> int:
    if value == "high":
        return 75
    if value == "medium":
        return 55
    if value == "low":
        return 35
    return 0


def _primary_gate(*, artifact_type: str | None, code_quality: int, specificity: int) -> bool:
    if artifact_type in GITHUB_PRIMARY_TYPES:
        return code_quality >= 65
    if artifact_type in TEXT_LIKE_PRIMARY_TYPES:
        return specificity >= 60
    return False


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item]


def _dedupe_strings(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _text_column_value(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        lines = [item for item in value if isinstance(item, str)]
        return "\n".join(lines) if lines else None
    return None


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _json_default(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


def _canonical_json(value: Mapping[str, Any]) -> str:
    normalized = _json_loads(value) or value
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default)


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _safe_failure_code(exc: Exception) -> str:
    message = str(exc)
    if message in SAFE_EXCEPTION_MESSAGES:
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
