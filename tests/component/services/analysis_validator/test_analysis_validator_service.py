from __future__ import annotations

from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from services.analysis_validator.config import AnalysisValidatorConfig
from services.analysis_validator.models import (
    BundleValidationContext,
    JudgeOutputReadyJob,
    JudgeOutputRecord,
    JudgeRunValidationRecord,
    StreamMessage,
)
from services.analysis_validator.service import AnalysisValidatorService
from services.analysis_validator.worker import AnalysisValidatorWorker
from tests.unit.services.analysis_validator.test_schema_registry import valid_payload


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeRepository:
    def __init__(self) -> None:
        self.jobs: dict[UUID, JudgeOutputReadyJob] = {}
        self.runs: dict[UUID, JudgeRunValidationRecord] = {}
        self.outputs: dict[UUID, JudgeOutputRecord] = {}
        self.bundles: dict[UUID, BundleValidationContext] = {}
        self.load_job_ids: list[UUID] = []
        self.state_transitions: list[dict] = []
        self.outbox: list[dict] = []
        self._outbox_dedupe_keys: set[str] = set()

    def transaction(self):
        return Tx()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID):
        self.load_job_ids.append(trigger_event_id)
        return self.jobs.get(trigger_event_id)

    async def load_judge_run(self, judge_run_id: UUID):
        return self.runs.get(judge_run_id)

    async def load_judge_output(self, judge_output_id: UUID):
        return self.outputs.get(judge_output_id)

    async def load_bundle_context(self, bundle_id: UUID):
        return self.bundles.get(bundle_id)

    async def update_judge_run_status(self, *, judge_run_id: UUID, status: str, finish_reason: str | None) -> None:
        run = self.runs[judge_run_id]
        self.runs[judge_run_id] = replace(run, status=status, finish_reason=finish_reason)

    async def insert_state_transition(self, **kwargs) -> None:
        self.state_transitions.append(kwargs)

    async def insert_analysis_policy_apply_outbox(self, **kwargs) -> bool:
        dedupe_key = f"analysis-policy-apply:{kwargs['judge_run_id']}:{kwargs['judge_output_id']}"
        if dedupe_key in self._outbox_dedupe_keys:
            return False
        self._outbox_dedupe_keys.add(dedupe_key)
        self.outbox.append(
            {
                "event_type": "analysis.policy.apply.v1",
                "aggregate_type": "judge_run",
                "aggregate_id": kwargs["judge_run_id"],
                "dedupe_key": dedupe_key,
                "payload_json": kwargs,
                "status": "pending",
            }
        )
        return True


class FakeConsumer:
    def __init__(self, messages: list[StreamMessage]) -> None:
        self.messages = messages
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        pass

    async def read_batch(self):
        return self.messages

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


def _config() -> AnalysisValidatorConfig:
    return AnalysisValidatorConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
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


def _run_record(
    *,
    judge_run_id: UUID | None = None,
    bundle_id: UUID | None = None,
    status: str = "succeeded",
) -> JudgeRunValidationRecord:
    return JudgeRunValidationRecord(
        judge_run_id=judge_run_id or uuid4(),
        bundle_id=bundle_id or uuid4(),
        judge_profile="github_primary",
        schema_version="judge_output_v1",
        policy_version="verdict_policy_v1",
        status=status,
        finish_reason="completed",
        refusal_detected=False,
    )


def _bundle(run: JudgeRunValidationRecord, *, candidate_group_id: UUID) -> BundleValidationContext:
    return BundleValidationContext(
        bundle_id=run.bundle_id,
        candidate_group_id=candidate_group_id,
        current_primary_artifact_id=uuid4(),
        current_primary_artifact_type="github_repo",
    )


def _output(
    run: JudgeRunValidationRecord,
    *,
    judge_run_id: UUID | None = None,
    candidate_group_id: UUID | None = None,
    payload: dict | None = None,
) -> JudgeOutputRecord:
    candidate_id = candidate_group_id or uuid4()
    payload_json = payload or valid_payload()
    payload_json["candidate_group_id"] = str(candidate_id)
    return JudgeOutputRecord(
        judge_output_id=uuid4(),
        judge_run_id=judge_run_id or run.judge_run_id,
        candidate_group_id=candidate_id,
        judge_schema_version="judge_output_v1",
        payload_json=payload_json,
        model_proposed_verdict=payload_json.get("model_proposed_verdict"),
        model_confidence_band=payload_json.get("model_confidence_band"),
    )


def _live_terminal_no_comparables_payload() -> dict:
    payload = valid_payload()
    payload["comparables"] = []
    payload["scores"].update(
        {
            "novelty": 22,
            "practical_usefulness": 28,
            "evidence_strength": 1,
            "hype_penalty": 35,
            "confidence": 20,
            "code_quality": None,
            "maintenance_signal": None,
        }
    )
    payload["reason_codes"] = [
        "repo_scope_unclear",
        "low_evidence_strength",
        "limited_usage_signal",
        "maintenance_signal_unknown",
        "defer_until_more_evidence",
    ]
    payload["evidence_limitations_ko"] = ["only limited public bundle evidence was available"]
    payload["recommended_action_ko"] = "review later after more evidence"
    payload["model_proposed_verdict"] = "later"
    payload["model_confidence_band"] = "low"
    return payload


def _job(run: JudgeRunValidationRecord, output: JudgeOutputRecord, *, finish_reason: str | None = "completed") -> JudgeOutputReadyJob:
    return JudgeOutputReadyJob(
        trigger_event_id=uuid4(),
        event_type="judge.output.ready.v1",
        judge_run_id=run.judge_run_id,
        judge_output_id=output.judge_output_id,
        finish_reason=finish_reason,
        refusal_detected=False,
    )


def _repo_with_valid_case() -> tuple[FakeRepository, JudgeOutputReadyJob, JudgeRunValidationRecord, JudgeOutputRecord]:
    repository = FakeRepository()
    run = _run_record()
    output = _output(run)
    job = _job(run, output)
    repository.jobs[job.trigger_event_id] = job
    repository.runs[run.judge_run_id] = run
    repository.outputs[output.judge_output_id] = output
    repository.bundles[run.bundle_id] = _bundle(run, candidate_group_id=output.candidate_group_id)
    return repository, job, run, output


async def _handle(repository: FakeRepository, job: JudgeOutputReadyJob) -> None:
    service = AnalysisValidatorService(_config(), repository=repository)  # type: ignore[arg-type]
    await service.handle_job(job)


@pytest.mark.asyncio
async def test_worker_rehydrates_judge_output_ready_from_event_outbox_trigger_id_not_redis_payload() -> None:
    repository, job, run, output = _repo_with_valid_case()
    service = AnalysisValidatorService(_config(), repository=repository)  # type: ignore[arg-type]
    consumer = FakeConsumer(
        [
            StreamMessage(
                stream="q.analysis.validate",
                message_id="1-0",
                fields={
                    "trigger_event_id": str(job.trigger_event_id),
                    "judge_run_id": str(uuid4()),
                    "judge_output_id": str(uuid4()),
                },
            )
        ]
    )
    worker = AnalysisValidatorWorker(_config(), consumer=consumer, service=service)

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert repository.load_job_ids == [job.trigger_event_id]
    assert repository.outbox[0]["payload_json"]["judge_run_id"] == run.judge_run_id
    assert repository.outbox[0]["payload_json"]["judge_output_id"] == output.judge_output_id
    assert consumer.acked == ["1-0"]


@pytest.mark.asyncio
async def test_missing_judge_run_writes_terminal_state_transition_no_policy_outbox() -> None:
    repository, job, run, _output_record = _repo_with_valid_case()
    del repository.runs[run.judge_run_id]

    await _handle(repository, job)

    assert repository.state_transitions[0]["object_type"] == "judge_run"
    assert repository.state_transitions[0]["object_id"] == run.judge_run_id
    assert repository.state_transitions[0]["from_state"] is None
    assert repository.state_transitions[0]["to_state"] == "analysis_failed_missing_run"
    assert repository.state_transitions[0]["reason_code"] == "validator_missing_judge_run"
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_missing_judge_output_marks_run_failed_terminal_and_writes_state_transition_no_policy_outbox() -> None:
    repository, job, run, output = _repo_with_valid_case()
    del repository.outputs[output.judge_output_id]

    await _handle(repository, job)

    assert repository.runs[run.judge_run_id].status == "failed_terminal"
    assert repository.state_transitions[0]["to_state"] == "analysis_failed_missing_output"
    assert repository.state_transitions[0]["reason_code"] == "validator_missing_judge_output"
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_judge_output_run_mismatch_marks_terminal_and_writes_state_transition_no_policy_outbox() -> None:
    repository, job, run, output = _repo_with_valid_case()
    repository.outputs[output.judge_output_id] = replace(output, judge_run_id=uuid4())

    await _handle(repository, job)

    assert repository.runs[run.judge_run_id].status == "failed_terminal"
    assert repository.state_transitions[0]["to_state"] == "analysis_failed_identity_mismatch"
    assert repository.state_transitions[0]["reason_code"] == "validator_judge_output_mismatch"
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_bundle_candidate_mismatch_marks_terminal_and_writes_state_transition_no_policy_outbox() -> None:
    repository, job, run, _output_record = _repo_with_valid_case()
    repository.bundles[run.bundle_id] = _bundle(run, candidate_group_id=uuid4())

    await _handle(repository, job)

    assert repository.runs[run.judge_run_id].status == "failed_terminal"
    assert repository.state_transitions[0]["to_state"] == "analysis_failed_identity_mismatch"
    assert repository.state_transitions[0]["reason_code"] == "validator_bundle_identity_mismatch"
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_refusal_records_state_transition_only_no_policy_outbox_judge_run_remains_succeeded() -> None:
    repository, job, run, output = _repo_with_valid_case()
    refusal_payload = {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(output.candidate_group_id),
        "output_kind": "refusal",
        "refusal_text": "cannot evaluate",
    }
    repository.outputs[output.judge_output_id] = replace(output, payload_json=refusal_payload)

    await _handle(repository, job)

    assert repository.runs[run.judge_run_id].status == "succeeded"
    assert repository.state_transitions[0]["to_state"] == "analysis_refused"
    assert repository.state_transitions[0]["reason_code"] == "model_refusal"
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_truncation_marks_failed_retryable_writes_state_transition_no_policy_outbox() -> None:
    repository, job, run, _output_record = _repo_with_valid_case()
    job = replace(job, finish_reason="max_output_tokens")

    await _handle(repository, job)

    assert repository.runs[run.judge_run_id].status == "failed_retryable"
    assert repository.state_transitions[0]["to_state"] == "analysis_failed_truncation"
    assert repository.state_transitions[0]["reason_code"] == "analysis_failed_truncation"
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_valid_output_emits_exactly_one_policy_apply_event_and_analysis_validated_transition() -> None:
    repository, job, run, output = _repo_with_valid_case()

    await _handle(repository, job)

    assert repository.runs[run.judge_run_id].status == "succeeded"
    assert repository.state_transitions[0]["to_state"] == "analysis_validated"
    assert repository.state_transitions[0]["reason_code"] == "validator_passed"
    assert len(repository.outbox) == 1
    assert repository.outbox[0]["event_type"] == "analysis.policy.apply.v1"
    assert repository.outbox[0]["payload_json"] == {
        "judge_run_id": run.judge_run_id,
        "judge_output_id": output.judge_output_id,
        "candidate_group_id": output.candidate_group_id,
        "bundle_id": run.bundle_id,
    }


@pytest.mark.asyncio
async def test_conservative_github_no_comparables_with_comparison_gap_emits_policy_apply_event() -> None:
    repository, job, run, output = _repo_with_valid_case()
    payload = _live_terminal_no_comparables_payload()
    payload["reason_codes"].append("comparison_gap")
    payload["candidate_group_id"] = str(output.candidate_group_id)
    repository.outputs[output.judge_output_id] = replace(output, payload_json=payload)

    await _handle(repository, job)

    assert repository.runs[run.judge_run_id].status == "succeeded"
    assert repository.state_transitions[0]["to_state"] == "analysis_validated"
    assert repository.state_transitions[0]["reason_code"] == "validator_passed"
    assert len(repository.outbox) == 1
    assert repository.outbox[0]["event_type"] == "analysis.policy.apply.v1"
    assert repository.outbox[0]["payload_json"] == {
        "judge_run_id": run.judge_run_id,
        "judge_output_id": output.judge_output_id,
        "candidate_group_id": output.candidate_group_id,
        "bundle_id": run.bundle_id,
    }


@pytest.mark.asyncio
async def test_live_terminal_shape_without_comparison_gap_fails_before_policy_apply() -> None:
    repository, job, run, output = _repo_with_valid_case()
    payload = _live_terminal_no_comparables_payload()
    payload["candidate_group_id"] = str(output.candidate_group_id)
    repository.outputs[output.judge_output_id] = replace(output, payload_json=payload)

    await _handle(repository, job)

    assert repository.runs[run.judge_run_id].status == "failed_terminal"
    assert repository.runs[run.judge_run_id].finish_reason == "validator_missing_github_comparables"
    assert repository.state_transitions[0]["to_state"] == "analysis_failed_semantic"
    assert repository.state_transitions[0]["reason_code"] == "validator_missing_github_comparables"
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_high_action_github_no_comparables_does_not_pass_to_policy_apply() -> None:
    repository, job, run, output = _repo_with_valid_case()
    payload = _live_terminal_no_comparables_payload()
    payload["candidate_group_id"] = str(output.candidate_group_id)
    payload["reason_codes"].append("comparison_gap")
    payload["scores"].update(
        {
            "novelty": 85,
            "practical_usefulness": 90,
            "evidence_strength": 80,
            "hype_penalty": 10,
            "confidence": 85,
            "code_quality": 80,
            "maintenance_signal": 75,
        }
    )
    payload["model_proposed_verdict"] = "inspect_now"
    payload["model_confidence_band"] = "high"
    repository.outputs[output.judge_output_id] = replace(output, payload_json=payload)

    await _handle(repository, job)

    assert repository.runs[run.judge_run_id].status == "failed_terminal"
    assert repository.runs[run.judge_run_id].finish_reason == "validator_github_comparables_required_for_high_action"
    assert repository.state_transitions[0]["to_state"] == "analysis_failed_semantic"
    assert repository.state_transitions[0]["reason_code"] == "validator_github_comparables_required_for_high_action"
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_invalid_schema_marks_failed_terminal_writes_state_transition_no_policy_outbox() -> None:
    repository, job, run, output = _repo_with_valid_case()
    payload = valid_payload()
    del payload["headline"]
    repository.outputs[output.judge_output_id] = replace(output, payload_json=payload)

    await _handle(repository, job)

    assert repository.runs[run.judge_run_id].status == "failed_terminal"
    assert repository.state_transitions[0]["to_state"] == "analysis_failed_schema"
    assert repository.state_transitions[0]["reason_code"] == "validator_schema_invalid"
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_invalid_semantic_contradiction_marks_failed_terminal_writes_state_transition_no_policy_outbox() -> None:
    repository, job, run, output = _repo_with_valid_case()
    payload = valid_payload()
    payload["candidate_group_id"] = str(output.candidate_group_id)
    payload["model_proposed_verdict"] = "inspect_now"
    payload["scores"]["evidence_strength"] = 49
    repository.outputs[output.judge_output_id] = replace(output, payload_json=payload)

    await _handle(repository, job)

    assert repository.runs[run.judge_run_id].status == "failed_terminal"
    assert repository.state_transitions[0]["to_state"] == "analysis_failed_semantic"
    assert repository.state_transitions[0]["reason_code"] == "validator_inspect_now_evidence_too_low"
    assert repository.outbox == []


@pytest.mark.asyncio
async def test_duplicate_event_outbox_dedupe_does_not_create_duplicate_policy_apply_event() -> None:
    repository, job, _run, _output_record = _repo_with_valid_case()

    await _handle(repository, job)
    await _handle(repository, job)

    assert len(repository.outbox) == 1
    assert len([row for row in repository.state_transitions if row["to_state"] == "analysis_validated"]) == 1
