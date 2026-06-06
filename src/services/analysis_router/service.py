from __future__ import annotations

import logging
from uuid import UUID

from .config import AnalysisRouterConfig
from .repositories import AnalysisRouterRepository
from .routing_policy import AnalysisRoutingPolicy


class AnalysisRouterService:
    def __init__(
        self,
        config: AnalysisRouterConfig,
        *,
        repository: AnalysisRouterRepository,
        policy: AnalysisRoutingPolicy | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._policy = policy or AnalysisRoutingPolicy(config)
        self._logger = logger or logging.getLogger(__name__)

    async def handle_trigger_event(self, trigger_event_id: str | UUID) -> None:
        try:
            parsed_trigger_event_id = UUID(str(trigger_event_id))
        except (TypeError, ValueError, AttributeError):
            self._logger.warning(
                "analysis_router_invalid_trigger_event_id",
                extra={
                    "service": "analysis-router",
                    "event": "analysis_router_invalid_trigger_event_id",
                    "trigger_event_id": str(trigger_event_id),
                },
            )
            return

        job = await self._repository.load_job_by_trigger_event_id(parsed_trigger_event_id)
        if job is None:
            return

        candidate_state = await self._repository.load_candidate_route_state(job.candidate_group_id)
        if candidate_state is None:
            return

        bundle = await self._repository.load_bundle(job.bundle_id)
        if bundle is not None and bundle.candidate_group_id != job.candidate_group_id:
            self._logger.warning(
                "analysis_router_bundle_candidate_mismatch",
                extra={
                    "service": "analysis-router",
                    "event": "analysis_router_bundle_candidate_mismatch",
                    "candidate_group_id": str(job.candidate_group_id),
                    "bundle_candidate_group_id": str(bundle.candidate_group_id),
                    "bundle_id": str(job.bundle_id),
                },
            )
            return

        shape = await self._repository.load_bundle_shape_stats(job.bundle_id) if bundle is not None else None
        decision = self._policy.decide(
            job=job,
            current_bundle_id=candidate_state.current_bundle_id,
            bundle=bundle,
            shape=shape,
        )

        if decision.action == "noop":
            self._logger.info(
                "analysis_router_noop",
                extra={
                    "service": "analysis-router",
                    "event": "analysis_router_noop",
                    "candidate_group_id": str(job.candidate_group_id),
                    "bundle_id": str(job.bundle_id),
                },
            )
            return

        if decision.action == "refresh":
            async with self._repository.transaction():
                await self._repository.insert_bundle_refresh_outbox(
                    candidate_group_id=job.candidate_group_id,
                    bundle_id=job.bundle_id,
                    refresh_reason=decision.refresh_reason or "bundle_recheck",
                )
            return

        async with self._repository.transaction():
            judge_run_id, created = await self._repository.get_or_create_judge_run(
                bundle_id=job.bundle_id,
                judge_profile=decision.judge_profile or "",
                model=decision.model or "",
                reasoning_effort=decision.reasoning_effort or "",
                prompt_version=decision.prompt_version or "",
                schema_version=decision.schema_version or "",
                policy_version=decision.policy_version or "",
                prompt_cache_key=decision.prompt_cache_key or "",
            )
            if not created:
                self._logger.info(
                    "analysis_router_existing_judge_run_reused",
                    extra={
                        "service": "analysis-router",
                        "event": "analysis_router_existing_judge_run_reused",
                        "judge_run_id": str(judge_run_id),
                        "bundle_id": str(job.bundle_id),
                    },
                )
                return

            await self._repository.insert_judge_call_requested_outbox(
                judge_run_id=judge_run_id,
                candidate_group_id=job.candidate_group_id,
                bundle_id=job.bundle_id,
                judge_profile=decision.judge_profile or "",
                model=decision.model or "",
                reasoning_effort=decision.reasoning_effort or "",
                prompt_version=decision.prompt_version or "",
                prompt_cache_key=decision.prompt_cache_key or "",
            )
