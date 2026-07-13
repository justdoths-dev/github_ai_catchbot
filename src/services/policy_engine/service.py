from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any, Protocol
from uuid import UUID

from .channel_override_policy import ChannelOverrideInput, ChannelOverridePolicy
from .config import PolicyEngineConfig
from .delivery_policy import DeliveryPolicy
from .feedback_eval import (
    DEFAULT_FEEDBACK_WINDOW_DAYS,
    MAX_FEEDBACK_ROWS,
    ChannelFeedbackAggregate,
    ChannelFeedbackSample,
    FeedbackEvalEngine,
)
from .models import (
    AnalysisDraft,
    AnalysisInsertResult,
    AnalysisPolicyJob,
    BundlePolicyContext,
    CandidatePolicyContext,
    ExistingAnalysisRecord,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
    NotificationPlanIntent,
    PolicyEvaluation,
)
from .notification_intent import NotificationIntentBuilder
from .repositories import PolicyEngineRepository
from .verdict_policy import VerdictPolicy, normalize_scores_for_policy, reconcile_model_verdict


class PolicyEngineRepositoryProtocol(Protocol):
    def transaction(self): ...
    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> AnalysisPolicyJob | None: ...
    async def load_candidate_context(self, candidate_group_id: UUID) -> CandidatePolicyContext | None: ...
    async def load_channel_feedback_sample(
        self,
        candidate_group_id: UUID,
        *,
        sample_limit: int,
        window_days: int,
    ) -> ChannelFeedbackSample: ...
    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunPolicyContext | None: ...
    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputPolicyContext | None: ...
    async def load_bundle_context(self, bundle_id: UUID) -> BundlePolicyContext | None: ...
    async def load_existing_analysis(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ) -> ExistingAnalysisRecord | None: ...
    async def insert_analysis(self, draft: AnalysisDraft) -> UUID: ...
    async def insert_analysis_if_absent(self, draft: AnalysisDraft) -> AnalysisInsertResult: ...
    async def insert_state_transition(self, **kwargs) -> None: ...
    async def insert_notification_plan_created_outbox(self, intent: NotificationPlanIntent) -> None: ...


class PolicyEngineService:
    def __init__(
        self,
        config: PolicyEngineConfig,
        *,
        repository: PolicyEngineRepository | PolicyEngineRepositoryProtocol,
        verdict_policy: VerdictPolicy | None = None,
        delivery_policy: DeliveryPolicy | None = None,
        feedback_eval_engine: FeedbackEvalEngine | None = None,
        channel_override_policy: ChannelOverridePolicy | None = None,
        notification_intent_builder: NotificationIntentBuilder | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._verdict_policy = verdict_policy or VerdictPolicy()
        self._delivery_policy = delivery_policy or DeliveryPolicy(
            enable_later_delivery=config.enable_later_delivery,
            enable_silent_later=config.enable_silent_later,
        )
        self._feedback_eval_engine = feedback_eval_engine or FeedbackEvalEngine()
        self._channel_override_policy = channel_override_policy or ChannelOverridePolicy()
        self._notification_intent_builder = notification_intent_builder or NotificationIntentBuilder(config=config)
        self._policy_identity = _effective_policy_identity(config)
        self._logger = logger or logging.getLogger(__name__)

    async def handle_trigger_event(self, trigger_event_id: str | UUID) -> None:
        job = await self.rehydrate_job(trigger_event_id)
        if job is None:
            return
        await self.handle_job(job)

    async def rehydrate_job(self, trigger_event_id: str | UUID) -> AnalysisPolicyJob | None:
        try:
            parsed_trigger_event_id = UUID(str(trigger_event_id))
        except (TypeError, ValueError, AttributeError):
            self._logger.warning(
                "policy_engine_invalid_trigger_event_id",
                extra={"service": "policy-engine", "event": "policy_engine_invalid_trigger_event_id"},
            )
            return None
        return await self._repository.load_job_by_trigger_event_id(parsed_trigger_event_id)

    async def handle_job(self, job: AnalysisPolicyJob) -> None:
        if job.event_type != "analysis.policy.apply.v1":
            return

        candidate = await self._repository.load_candidate_context(job.candidate_group_id)
        if candidate is None:
            await self._candidate_transition(
                job=job,
                to_state="analysis_policy_failed",
                reason_code="policy_missing_candidate_context",
            )
            return

        if candidate.current_bundle_id != job.bundle_id:
            await self._candidate_transition(
                job=job,
                to_state="analysis_policy_stale_bundle",
                reason_code="policy_stale_bundle_request",
            )
            return

        judge_run = await self._repository.load_judge_run(job.judge_run_id)
        judge_output = await self._repository.load_judge_output(job.judge_output_id)
        bundle = await self._repository.load_bundle_context(job.bundle_id)
        if judge_run is None or judge_output is None or bundle is None:
            await self._candidate_transition(
                job=job,
                to_state="analysis_policy_failed",
                reason_code="policy_missing_context",
            )
            return

        mismatch_reason = self._context_mismatch_reason(
            job=job,
            judge_run=judge_run,
            judge_output=judge_output,
            bundle=bundle,
        )
        if mismatch_reason is not None:
            await self._candidate_transition(
                job=job,
                to_state="analysis_policy_failed",
                reason_code=mismatch_reason,
            )
            return

        policy_version, delivery_policy_version = self._policy_identity
        existing = await self._repository.load_existing_analysis(
            judge_output_id=job.judge_output_id,
            policy_version=policy_version,
            delivery_policy_version=delivery_policy_version,
        )
        if existing is not None:
            return

        analysis, evaluation = self._build_analysis(job=job, judge_run=judge_run, judge_output=judge_output, bundle=bundle)
        feedback_aggregate = await self._load_feedback_aggregate(job.candidate_group_id)
        analysis, evaluation = self._apply_channel_feedback_policy(
            analysis=analysis,
            evaluation=evaluation,
            bundle=bundle,
            feedback_aggregate=feedback_aggregate,
        )

        async with self._repository.transaction():
            insert_if_absent = getattr(self._repository, "insert_analysis_if_absent", None)
            if insert_if_absent is None:
                analysis_id = await self._repository.insert_analysis(analysis)
            else:
                insert_result = await insert_if_absent(analysis)
                if not insert_result.created:
                    return
                analysis_id = insert_result.analysis_id
            await self._repository.insert_state_transition(
                object_type="analysis",
                object_id=analysis_id,
                from_state="analysis_validated",
                to_state="analysis_finalized" if analysis.delivery_decision != "suppress" else "analysis_suppressed",
                reason_code=f"policy_applied:{analysis.verdict}:{analysis.delivery_decision}",
            )
            intent = self._notification_intent_builder.build(
                analysis_id=analysis_id,
                analysis=analysis,
                evaluation=evaluation,
                dedupe_subject_key=candidate.dedupe_subject_key,
            )
            if intent is not None:
                await self._repository.insert_notification_plan_created_outbox(intent)

    async def _candidate_transition(self, *, job: AnalysisPolicyJob, to_state: str, reason_code: str) -> None:
        async with self._repository.transaction():
            await self._repository.insert_state_transition(
                object_type="candidate_group",
                object_id=job.candidate_group_id,
                from_state="analysis_validated",
                to_state=to_state,
                reason_code=reason_code,
            )

    @staticmethod
    def _context_mismatch_reason(
        *,
        job: AnalysisPolicyJob,
        judge_run: JudgeRunPolicyContext,
        judge_output: JudgeOutputPolicyContext,
        bundle: BundlePolicyContext,
    ) -> str | None:
        if judge_run.status != "succeeded":
            return "policy_judge_run_not_succeeded"
        if judge_run.bundle_id != job.bundle_id:
            return "policy_judge_run_bundle_mismatch"
        if judge_output.judge_run_id != job.judge_run_id:
            return "policy_judge_output_mismatch"
        if judge_output.candidate_group_id != job.candidate_group_id:
            return "policy_judge_output_candidate_mismatch"
        if bundle.candidate_group_id != job.candidate_group_id:
            return "policy_bundle_candidate_mismatch"
        return None

    def _build_analysis(
        self,
        *,
        job: AnalysisPolicyJob,
        judge_run: JudgeRunPolicyContext,
        judge_output: JudgeOutputPolicyContext,
        bundle: BundlePolicyContext,
    ) -> tuple[AnalysisDraft, PolicyEvaluation]:
        payload = judge_output.payload_json if isinstance(judge_output.payload_json, dict) else {}
        scores = payload.get("scores") if isinstance(payload.get("scores"), dict) else {}
        scores, score_scale_normalized = normalize_scores_for_policy(
            scores,
            model_proposed_verdict=judge_output.model_proposed_verdict,
        )

        verdict_decision = self._verdict_policy.evaluate(
            scores=scores,
            current_primary_artifact_type=bundle.current_primary_artifact_type,
        )
        delivery_decision = self._delivery_policy.evaluate(verdict=verdict_decision.verdict)
        reason_codes = [
            *_string_list(payload.get("reason_codes")),
        ]
        if score_scale_normalized:
            reason_codes.append("policy_score_scale_normalized_0_10_to_0_100")
        reason_codes.extend(verdict_decision.reason_codes)
        if delivery_decision.suppress_reason_code:
            reason_codes.append(delivery_decision.suppress_reason_code)

        model_proposed_verdict = judge_output.model_proposed_verdict
        policy_reconciled_flag, reason_codes = reconcile_model_verdict(
            model_proposed_verdict=model_proposed_verdict,
            final_verdict=verdict_decision.verdict,
            reason_codes=reason_codes,
        )

        policy_version, delivery_policy_version = self._policy_identity
        analysis = AnalysisDraft(
            candidate_group_id=job.candidate_group_id,
            judge_output_id=job.judge_output_id,
            schema_version="analysis_v1",
            policy_version=policy_version,
            prompt_version=judge_run.prompt_version,
            delivery_policy_version=delivery_policy_version,
            verdict=verdict_decision.verdict,
            delivery_decision=delivery_decision.delivery_decision,
            scores_json=scores,
            reason_codes_json=reason_codes,
            evidence_limitations_ko=_text_column_value(payload.get("evidence_limitations_ko")),
            recommended_action_ko=_text_column_value(payload.get("recommended_action_ko")),
            freshness_note_ko=_text_column_value(payload.get("freshness_note_ko")),
            model_proposed_verdict=model_proposed_verdict,
            policy_reconciled_flag=policy_reconciled_flag,
        )
        evaluation = PolicyEvaluation(
            verdict=analysis.verdict,
            delivery_decision=analysis.delivery_decision,
            urgency_profile=delivery_decision.urgency_profile,
            reason_codes=reason_codes,
            policy_reconciled_flag=policy_reconciled_flag,
            suppress_reason_code=delivery_decision.suppress_reason_code,
        )
        return analysis, evaluation

    async def _load_feedback_aggregate(self, candidate_group_id: UUID) -> ChannelFeedbackAggregate:
        load_sample = getattr(self._repository, "load_channel_feedback_sample", None)
        if load_sample is None:
            return ChannelFeedbackAggregate.neutral()
        sample = await load_sample(
            candidate_group_id,
            sample_limit=MAX_FEEDBACK_ROWS,
            window_days=DEFAULT_FEEDBACK_WINDOW_DAYS,
        )
        return self._feedback_eval_engine.aggregate_channel_sample(sample)

    def _apply_channel_feedback_policy(
        self,
        *,
        analysis: AnalysisDraft,
        evaluation: PolicyEvaluation,
        bundle: BundlePolicyContext,
        feedback_aggregate: ChannelFeedbackAggregate,
    ) -> tuple[AnalysisDraft, PolicyEvaluation]:
        if not feedback_aggregate.policy_active or feedback_aggregate.channel_tier != "C":
            return analysis, evaluation

        artifact_type = bundle.current_primary_artifact_type or "unknown"
        override = self._channel_override_policy.evaluate(
            ChannelOverrideInput(
                channel_tier=feedback_aggregate.channel_tier,
                artifact_type=artifact_type,
                verdict=analysis.verdict,
                delivery_decision=analysis.delivery_decision,
                urgency_profile=evaluation.urgency_profile,
                reason_codes=tuple(evaluation.reason_codes),
                text_idea_enabled=True,
                ai_noise_signal_count=_ai_noise_signal_count(evaluation.reason_codes),
                external_evidence_present=artifact_type not in {"unknown", "text_idea"},
            )
        )
        if override.live_delivery_decision == analysis.delivery_decision:
            return analysis, evaluation

        reason_codes = [*analysis.reason_codes_json]
        for reason_code in override.reason_codes:
            if reason_code not in reason_codes:
                reason_codes.append(reason_code)
        if override.live_delivery_decision == "suppress":
            urgency_profile = "suppressed"
            suppress_reason_code = override.reason_codes[0] if override.reason_codes else "channel_feedback_suppressed"
        elif override.live_delivery_decision == "send_digest":
            urgency_profile = "digest"
            suppress_reason_code = None
        else:
            return analysis, evaluation

        return (
            replace(
                analysis,
                delivery_decision=override.live_delivery_decision,  # type: ignore[arg-type]
                reason_codes_json=reason_codes,
            ),
            replace(
                evaluation,
                delivery_decision=override.live_delivery_decision,  # type: ignore[arg-type]
                urgency_profile=urgency_profile,  # type: ignore[arg-type]
                reason_codes=reason_codes,
                suppress_reason_code=suppress_reason_code,
            ),
        )


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _text_column_value(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        lines = [item for item in value if isinstance(item, str)]
        return "\n".join(lines) if lines else None
    return None


def _ai_noise_signal_count(reason_codes: list[str]) -> int:
    noise_terms = ("ai_noise", "ai_only", "generic_ai", "weak_ai", "bad_channel_fit")
    return sum(1 for reason_code in reason_codes if any(term in reason_code for term in noise_terms))


def _effective_policy_identity(config: PolicyEngineConfig) -> tuple[str, str]:
    return (
        config.policy_version,
        f"{config.delivery_policy_version}_feedback_aware_channel_policy_v1",
    )
