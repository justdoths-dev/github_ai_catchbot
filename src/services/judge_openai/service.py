from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from .config import JudgeOpenAIConfig
from .context_builder import JudgeContextBuilder
from .models import JudgeCallJob, JudgeRunRecord, OpenAIJudgeResult, OpenAIJudgeUsage
from .openai_client import OpenAIPermanentError, OpenAIRequestShapeError, OpenAITransientError
from .preflight import HeuristicSanitizingPreflight, NoopModelContextPreflight
from .prompt_library import PromptLibrary, UnsupportedJudgeProfileError, UnsupportedPromptVersionError
from .repositories import JudgeOpenAIRepository
from .request_shape import (
    JudgeOpenAIRequestEnvelope,
    JudgeOpenAIRequestEnvelopeBuilder,
    JudgeOpenAIRequestEnvelopeError,
    build_judge_output_schema,
)
from .response_mapper import OpenAIResponseMapper


class OpenAIJudgeClientProtocol(Protocol):
    async def create_structured_response(self, **kwargs): ...


@dataclass(slots=True, frozen=True)
class _CallOutcome:
    result: OpenAIJudgeResult | None
    terminal_schema_failure: bool = False


class JudgeOpenAIService:
    def __init__(
        self,
        config: JudgeOpenAIConfig,
        *,
        repository: JudgeOpenAIRepository,
        openai_client: OpenAIJudgeClientProtocol,
        prompt_library: PromptLibrary | None = None,
        response_mapper: OpenAIResponseMapper | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._openai_client = openai_client
        self._prompt_library = prompt_library or PromptLibrary()
        self._response_mapper = response_mapper or OpenAIResponseMapper()
        self._logger = logger or logging.getLogger(__name__)
        preflight = (
            HeuristicSanitizingPreflight()
            if config.enable_prompt_guard_preflight
            else NoopModelContextPreflight()
        )
        self._request_envelope_builder = JudgeOpenAIRequestEnvelopeBuilder(
            prompt_library=self._prompt_library,
            context_builder=JudgeContextBuilder(preflight=preflight),
            max_output_tokens=config.max_output_tokens,
            request_timeout_sec=config.request_timeout_sec,
        )

    async def handle_trigger_event(self, trigger_event_id: str | UUID) -> None:
        try:
            parsed_trigger_event_id = UUID(str(trigger_event_id))
        except (TypeError, ValueError, AttributeError):
            self._logger.warning(
                "judge_openai_invalid_trigger_event_id",
                extra={"service": "judge-openai", "event": "judge_openai_invalid_trigger_event_id"},
            )
            return
        job = await self._repository.load_job_by_trigger_event_id(parsed_trigger_event_id)
        if job is None:
            return
        await self.handle_job(job)

    async def rehydrate_job(self, trigger_event_id: str | UUID) -> JudgeCallJob | None:
        try:
            parsed_trigger_event_id = UUID(str(trigger_event_id))
        except (TypeError, ValueError, AttributeError):
            return None
        return await self._repository.load_job_by_trigger_event_id(parsed_trigger_event_id)

    async def handle_job(self, job: JudgeCallJob) -> None:
        if job.event_type != "judge.call.requested.v1" or self._job_missing_required_fields(job):
            return

        judge_run = await self._repository.load_judge_run(job.judge_run_id)
        if judge_run is None or judge_run.status != "pending" or judge_run.bundle_id != job.bundle_id:
            return

        if self._run_missing_required_fields(judge_run):
            await self._finish_without_output(
                judge_run=judge_run,
                status="failed_terminal",
                finish_reason="prompt_missing",
            )
            return

        if self._job_conflicts_with_run(job, judge_run):
            return

        bundle = await self._repository.load_bundle_context(judge_run.bundle_id)
        if bundle is None:
            await self._finish_without_output(
                judge_run=judge_run,
                status="failed_terminal",
                finish_reason="bundle_missing",
            )
            return
        if not bundle.is_structurally_usable():
            await self._finish_without_output(
                judge_run=judge_run,
                status="failed_terminal",
                finish_reason="bundle_invalid",
            )
            return

        try:
            envelope = self._request_envelope_builder.build(judge_run=judge_run, bundle=bundle)
        except UnsupportedPromptVersionError:
            await self._finish_without_output(
                judge_run=judge_run,
                status="failed_terminal",
                finish_reason="unsupported_prompt_version",
            )
            return
        except UnsupportedJudgeProfileError:
            await self._finish_without_output(
                judge_run=judge_run,
                status="failed_terminal",
                finish_reason="unsupported_judge_profile",
            )
            return
        except JudgeOpenAIRequestEnvelopeError:
            await self._finish_without_output(
                judge_run=judge_run,
                status="failed_terminal",
                finish_reason="openai_request_shape_invalid",
            )
            return

        async with self._repository.transaction():
            await self._repository.mark_judge_run_running(judge_run.judge_run_id)

        try:
            outcome = await self._call_with_single_schema_retry(judge_run=judge_run, envelope=envelope)
        except OpenAIRequestShapeError:
            await self._finish_without_output(
                judge_run=judge_run,
                status="failed_terminal",
                finish_reason="openai_request_shape_invalid",
            )
            return
        except OpenAITransientError:
            await self._finish_without_output(
                judge_run=judge_run,
                status="failed_retryable",
                finish_reason="openai_transport_retryable",
            )
            return
        except OpenAIPermanentError:
            await self._finish_without_output(
                judge_run=judge_run,
                status="failed_terminal",
                finish_reason="openai_permanent_error",
            )
            return

        if outcome.terminal_schema_failure or outcome.result is None:
            usage = outcome.result.usage if outcome.result else None
            await self._finish_without_output(
                judge_run=judge_run,
                status="failed_terminal",
                finish_reason="schema_invalid_after_retry",
                usage=usage,
            )
            return

        result = outcome.result
        payload_json = result.payload_json
        if payload_json is None:
            payload_json = self._response_mapper.build_refusal_envelope(
                candidate_group_id=str(bundle.candidate_group_id),
                schema_version=judge_run.schema_version,
                refusal_text=result.refusal_text,
            )

        proposed_verdict = payload_json.get("model_proposed_verdict")
        confidence_band = payload_json.get("model_confidence_band")

        async with self._repository.transaction():
            judge_output_id = await self._repository.insert_judge_output(
                judge_run_id=judge_run.judge_run_id,
                candidate_group_id=bundle.candidate_group_id,
                judge_schema_version=judge_run.schema_version,
                payload_json=payload_json,
                model_proposed_verdict=proposed_verdict if isinstance(proposed_verdict, str) else None,
                model_confidence_band=confidence_band if isinstance(confidence_band, str) else None,
            )
            await self._repository.finish_judge_run(
                judge_run_id=judge_run.judge_run_id,
                status="succeeded",
                usage=result.usage,
                finish_reason=result.finish_reason,
                refusal_detected=result.refusal_detected,
            )
            await self._repository.insert_judge_output_ready_outbox(
                judge_run_id=judge_run.judge_run_id,
                judge_output_id=judge_output_id,
                finish_reason=result.finish_reason,
                refusal_detected=result.refusal_detected,
            )

    async def _call_with_single_schema_retry(
        self,
        *,
        judge_run: JudgeRunRecord,
        envelope: JudgeOpenAIRequestEnvelope,
    ) -> _CallOutcome:
        first = await self._call_once(judge_run=judge_run, envelope=envelope)
        if first.has_structured_payload or first.refusal_detected:
            return _CallOutcome(result=first)

        async with self._repository.transaction():
            await self._repository.increment_schema_retry_count(judge_run.judge_run_id)

        second = await self._call_once(judge_run=judge_run, envelope=envelope)
        if second.has_structured_payload or second.refusal_detected:
            return _CallOutcome(result=second)
        return _CallOutcome(result=second, terminal_schema_failure=True)

    async def _call_once(
        self,
        *,
        judge_run: JudgeRunRecord,
        envelope: JudgeOpenAIRequestEnvelope,
    ) -> OpenAIJudgeResult:
        started = time.monotonic()
        response = await self._openai_client.create_structured_response(
            model=envelope.model,
            reasoning_effort=envelope.reasoning_effort,
            developer_prompt=envelope.developer_prompt_text,
            user_context=envelope.user_context,
            json_schema=envelope.structured_output_schema,
            max_output_tokens=envelope.max_output_tokens,
            prompt_cache_key=envelope.prompt_cache_key,
        )
        return self._response_mapper.parse(response, started_monotonic=started)

    async def _finish_without_output(
        self,
        *,
        judge_run: JudgeRunRecord,
        status: str,
        finish_reason: str,
        usage: OpenAIJudgeUsage | None = None,
    ) -> None:
        async with self._repository.transaction():
            await self._repository.finish_judge_run(
                judge_run_id=judge_run.judge_run_id,
                status=status,
                usage=usage,
                finish_reason=finish_reason,
                refusal_detected=False,
            )

    @staticmethod
    def _job_conflicts_with_run(job: JudgeCallJob, judge_run: JudgeRunRecord) -> bool:
        return bool(
            job.model != judge_run.model
            or job.reasoning_effort != judge_run.reasoning_effort
            or job.prompt_version != judge_run.prompt_version
            or (
                job.prompt_cache_key is not None
                and judge_run.prompt_cache_key is not None
                and job.prompt_cache_key != judge_run.prompt_cache_key
            )
        )

    @staticmethod
    def _job_missing_required_fields(job: JudgeCallJob) -> bool:
        return not all(
            [
                job.judge_run_id,
                job.bundle_id,
                job.model,
                job.reasoning_effort,
                job.prompt_version,
            ]
        )

    @staticmethod
    def _run_missing_required_fields(judge_run: JudgeRunRecord) -> bool:
        return not all(
            [
                judge_run.model,
                judge_run.reasoning_effort,
                judge_run.prompt_version,
            ]
        )

    @staticmethod
    def judge_output_schema() -> dict:
        return build_judge_output_schema()
