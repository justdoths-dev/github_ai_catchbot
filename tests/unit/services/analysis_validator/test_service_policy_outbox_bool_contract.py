from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest

from services.analysis_validator.config import AnalysisValidatorConfig
from services.analysis_validator.models import (
    BundleValidationContext,
    JudgeOutputReadyJob,
    JudgeOutputRecord,
    JudgeRunValidationRecord,
)
from services.analysis_validator.service import AnalysisValidatorService
from tests.unit.services.analysis_validator.test_schema_registry import valid_payload


class _Tx:
    async def __aenter__(self) -> "_Tx":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeRepository:
    def __init__(self, *, policy_insert_result: Any) -> None:
        self.policy_insert_result = policy_insert_result
        self.trigger_event_id = uuid4()
        self.candidate_group_id = uuid4()
        self.bundle_id = uuid4()
        self.judge_run_id = uuid4()
        self.judge_output_id = uuid4()
        self.calls: list[str] = []
        self.policy_insert_kwargs: list[dict[str, Any]] = []
        self.state_transitions: list[dict[str, Any]] = []
        self.payload = valid_payload()
        self.payload["candidate_group_id"] = str(self.candidate_group_id)

    def transaction(self) -> _Tx:
        return _Tx()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID) -> JudgeOutputReadyJob | None:
        if trigger_event_id != self.trigger_event_id:
            return None
        return JudgeOutputReadyJob(
            trigger_event_id=self.trigger_event_id,
            event_type="judge.output.ready.v1",
            judge_run_id=self.judge_run_id,
            judge_output_id=self.judge_output_id,
            finish_reason="completed",
            refusal_detected=False,
        )

    async def load_judge_run(self, judge_run_id: UUID) -> JudgeRunValidationRecord | None:
        if judge_run_id != self.judge_run_id:
            return None
        return JudgeRunValidationRecord(
            judge_run_id=self.judge_run_id,
            bundle_id=self.bundle_id,
            judge_profile="github_primary",
            schema_version="judge_output_v1",
            policy_version="verdict_policy_v1",
            status="succeeded",
            finish_reason="completed",
            refusal_detected=False,
        )

    async def load_judge_output(self, judge_output_id: UUID) -> JudgeOutputRecord | None:
        if judge_output_id != self.judge_output_id:
            return None
        return JudgeOutputRecord(
            judge_output_id=self.judge_output_id,
            judge_run_id=self.judge_run_id,
            candidate_group_id=self.candidate_group_id,
            judge_schema_version="judge_output_v1",
            payload_json=self.payload,
            model_proposed_verdict=self.payload["model_proposed_verdict"],
            model_confidence_band=self.payload["model_confidence_band"],
        )

    async def load_bundle_context(self, bundle_id: UUID) -> BundleValidationContext | None:
        if bundle_id != self.bundle_id:
            return None
        return BundleValidationContext(
            bundle_id=self.bundle_id,
            candidate_group_id=self.candidate_group_id,
            current_primary_artifact_id=uuid4(),
            current_primary_artifact_type="github_repo",
        )

    async def update_judge_run_status(self, **kwargs: Any) -> None:
        self.calls.append("status")

    async def insert_state_transition(self, **kwargs: Any) -> None:
        self.calls.append(f"transition:{kwargs['to_state']}")
        self.state_transitions.append(kwargs)

    async def insert_analysis_policy_apply_outbox(self, **kwargs: Any) -> Any:
        self.calls.append("policy_outbox")
        self.policy_insert_kwargs.append(kwargs)
        return self.policy_insert_result


def _config() -> AnalysisValidatorConfig:
    return AnalysisValidatorConfig(
        app_env="test",
        database_url="unused",
        redis_url="unused",
        queue_name="q.analysis.validate",
        consumer_group="analysis-validator",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        max_headline_chars=200,
        max_summary_chars=1200,
        max_text_items=10,
        log_level="INFO",
    )


async def _handle(policy_insert_result: Any) -> _FakeRepository:
    repository = _FakeRepository(policy_insert_result=policy_insert_result)
    service = AnalysisValidatorService(_config(), repository=repository)  # type: ignore[arg-type]

    await service.handle_trigger_event(repository.trigger_event_id)

    return repository


@pytest.mark.asyncio
async def test_policy_outbox_insert_true_appends_exactly_one_analysis_validated_transition() -> None:
    repository = await _handle(True)

    assert repository.calls == ["policy_outbox", "transition:analysis_validated"]
    assert len(repository.policy_insert_kwargs) == 1
    assert repository.state_transitions == [
        {
            "object_type": "judge_run",
            "object_id": repository.judge_run_id,
            "from_state": "succeeded",
            "to_state": "analysis_validated",
            "reason_code": "validator_passed",
        }
    ]


@pytest.mark.asyncio
async def test_policy_outbox_insert_false_fails_closed_without_analysis_validated_transition() -> None:
    repository = await _handle(False)

    assert repository.calls == ["policy_outbox"]
    assert len(repository.policy_insert_kwargs) == 1
    assert repository.state_transitions == []


@pytest.mark.asyncio
async def test_policy_outbox_insert_unexpected_none_fails_closed_without_analysis_validated_transition() -> None:
    repository = await _handle(None)

    assert repository.calls == ["policy_outbox"]
    assert len(repository.policy_insert_kwargs) == 1
    assert repository.state_transitions == []
