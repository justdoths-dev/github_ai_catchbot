from __future__ import annotations

import logging
from typing import Protocol
from uuid import UUID

from .business_rules import AnalysisValidatorBusinessRules
from .config import AnalysisValidatorConfig
from .models import (
    BundleValidationContext,
    JudgeOutputReadyJob,
    JudgeOutputRecord,
    JudgeRunValidationRecord,
    ValidationDecision,
)
from .repositories import AnalysisValidatorRepository
from .schema_registry import JudgeOutputSchemaRegistry


class AnalysisValidatorRepositoryProtocol(Protocol):
    def transaction(self): ...
    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> JudgeOutputReadyJob | None: ...
    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunValidationRecord | None: ...
    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputRecord | None: ...
    async def load_bundle_context(self, bundle_id: UUID) -> BundleValidationContext | None: ...
    async def update_judge_run_status(self, **kwargs) -> None: ...
    async def insert_state_transition(self, **kwargs) -> None: ...
    async def insert_analysis_policy_apply_outbox(self, **kwargs) -> None: ...


class AnalysisValidatorService:
    def __init__(
        self,
        config: AnalysisValidatorConfig,
        *,
        repository: AnalysisValidatorRepository | AnalysisValidatorRepositoryProtocol,
        schema_registry: JudgeOutputSchemaRegistry | None = None,
        business_rules: AnalysisValidatorBusinessRules | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._schema_registry = schema_registry or JudgeOutputSchemaRegistry(
            max_headline_chars=config.max_headline_chars,
            max_summary_chars=config.max_summary_chars,
            max_text_items=config.max_text_items,
        )
        self._business_rules = business_rules or AnalysisValidatorBusinessRules()
        self._logger = logger or logging.getLogger(__name__)

    async def handle_trigger_event(self, trigger_event_id: str | UUID) -> None:
        job = await self.rehydrate_job(trigger_event_id)
        if job is None:
            return
        await self.handle_job(job)

    async def rehydrate_job(self, trigger_event_id: str | UUID) -> JudgeOutputReadyJob | None:
        try:
            parsed_trigger_event_id = UUID(str(trigger_event_id))
        except (TypeError, ValueError, AttributeError):
            self._logger.warning(
                "analysis_validator_invalid_trigger_event_id",
                extra={"service": "analysis-validator", "event": "analysis_validator_invalid_trigger_event_id"},
            )
            return None
        return await self._repository.load_job_by_trigger_event_id(parsed_trigger_event_id)

    async def handle_job(self, job: JudgeOutputReadyJob) -> None:
        judge_run = await self._repository.load_judge_run(job.judge_run_id)
        if judge_run is None:
            await self._missing_judge_run(job)
            return

        judge_output = await self._repository.load_judge_output(job.judge_output_id)
        if judge_output is None:
            await self._terminal(
                judge_run=judge_run,
                transition_to_state="analysis_failed_missing_output",
                reason_code="validator_missing_judge_output",
            )
            return

        if judge_output.judge_run_id != job.judge_run_id:
            await self._terminal(
                judge_run=judge_run,
                transition_to_state="analysis_failed_identity_mismatch",
                reason_code="validator_judge_output_mismatch",
            )
            return

        bundle = await self._repository.load_bundle_context(judge_run.bundle_id)
        if bundle is None or bundle.candidate_group_id != judge_output.candidate_group_id:
            await self._terminal(
                judge_run=judge_run,
                transition_to_state="analysis_failed_identity_mismatch",
                reason_code="validator_bundle_identity_mismatch",
            )
            return

        payload = judge_output.payload_json
        control_decision = self._business_rules.evaluate_control_flow(
            payload=payload,
            finish_reason=job.finish_reason or judge_run.finish_reason,
            refusal_detected=job.refusal_detected,
        )
        if control_decision.action == "refused":
            await self._transition_only(judge_run=judge_run, decision=control_decision)
            return
        if control_decision.action == "failed_retryable":
            await self._retryable(judge_run=judge_run, decision=control_decision)
            return

        schema_decision = self._schema_registry.validate(payload)
        if schema_decision.action != "forward_policy":
            await self._terminal(judge_run=judge_run, decision=schema_decision)
            return
        if payload["candidate_group_id"] != str(judge_output.candidate_group_id):
            await self._terminal(
                judge_run=judge_run,
                transition_to_state="analysis_failed_identity_mismatch",
                reason_code="validator_payload_candidate_mismatch",
            )
            return

        semantic_decision = self._business_rules.validate_semantics(payload=payload, bundle=bundle)
        if semantic_decision.action == "failed_terminal":
            await self._terminal(judge_run=judge_run, decision=semantic_decision)
            return

        async with self._repository.transaction():
            await self._repository.insert_state_transition(
                object_type="judge_run",
                object_id=judge_run.judge_run_id,
                from_state=judge_run.status,
                to_state="analysis_validated",
                reason_code="validator_passed",
            )
            await self._repository.insert_analysis_policy_apply_outbox(
                judge_run_id=judge_run.judge_run_id,
                judge_output_id=judge_output.judge_output_id,
                candidate_group_id=judge_output.candidate_group_id,
                bundle_id=judge_run.bundle_id,
            )

    async def _missing_judge_run(self, job: JudgeOutputReadyJob) -> None:
        async with self._repository.transaction():
            await self._repository.insert_state_transition(
                object_type="judge_run",
                object_id=job.judge_run_id,
                from_state=None,
                to_state="analysis_failed_missing_run",
                reason_code="validator_missing_judge_run",
            )

    async def _transition_only(
        self,
        *,
        judge_run: JudgeRunValidationRecord,
        decision: ValidationDecision,
    ) -> None:
        async with self._repository.transaction():
            await self._repository.insert_state_transition(
                object_type="judge_run",
                object_id=judge_run.judge_run_id,
                from_state=judge_run.status,
                to_state=decision.transition_to_state or "analysis_refused",
                reason_code=decision.reason_code,
            )

    async def _retryable(
        self,
        *,
        judge_run: JudgeRunValidationRecord,
        decision: ValidationDecision,
    ) -> None:
        async with self._repository.transaction():
            await self._repository.update_judge_run_status(
                judge_run_id=judge_run.judge_run_id,
                status="failed_retryable",
                finish_reason=decision.reason_code,
            )
            await self._repository.insert_state_transition(
                object_type="judge_run",
                object_id=judge_run.judge_run_id,
                from_state=judge_run.status,
                to_state=decision.transition_to_state or "analysis_failed_truncation",
                reason_code=decision.reason_code,
            )

    async def _terminal(
        self,
        *,
        judge_run: JudgeRunValidationRecord,
        transition_to_state: str | None = None,
        reason_code: str | None = None,
        decision: ValidationDecision | None = None,
    ) -> None:
        to_state = decision.transition_to_state if decision else transition_to_state
        reason = decision.reason_code if decision else reason_code
        async with self._repository.transaction():
            await self._repository.update_judge_run_status(
                judge_run_id=judge_run.judge_run_id,
                status="failed_terminal",
                finish_reason=reason,
            )
            await self._repository.insert_state_transition(
                object_type="judge_run",
                object_id=judge_run.judge_run_id,
                from_state=judge_run.status,
                to_state=to_state or "analysis_failed_semantic",
                reason_code=reason,
            )
