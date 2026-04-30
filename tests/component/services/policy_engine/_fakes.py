from __future__ import annotations

from dataclasses import replace
from uuid import UUID, uuid4

from services.policy_engine.config import PolicyEngineConfig
from services.policy_engine.models import (
    AnalysisDraft,
    AnalysisPolicyJob,
    BundlePolicyContext,
    CandidatePolicyContext,
    ExistingAnalysisRecord,
    JudgeOutputPolicyContext,
    JudgeRunPolicyContext,
    NotificationPlanIntent,
    StreamMessage,
)
from services.policy_engine.service import PolicyEngineService


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeRepository:
    def __init__(self) -> None:
        self.jobs: dict[UUID, AnalysisPolicyJob] = {}
        self.candidates: dict[UUID, CandidatePolicyContext] = {}
        self.runs: dict[UUID, JudgeRunPolicyContext] = {}
        self.outputs: dict[UUID, JudgeOutputPolicyContext] = {}
        self.bundles: dict[UUID, BundlePolicyContext] = {}
        self.existing: dict[tuple[UUID, str, str], ExistingAnalysisRecord] = {}
        self.load_job_ids: list[UUID] = []
        self.analyses: list[tuple[UUID, AnalysisDraft]] = []
        self.state_transitions: list[dict] = []
        self.notification_outbox: list[NotificationPlanIntent] = []

    def transaction(self):
        return Tx()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID):
        self.load_job_ids.append(trigger_event_id)
        return self.jobs.get(trigger_event_id)

    async def load_candidate_context(self, candidate_group_id: UUID):
        return self.candidates.get(candidate_group_id)

    async def load_judge_run(self, judge_run_id: UUID):
        return self.runs.get(judge_run_id)

    async def load_judge_output(self, judge_output_id: UUID):
        return self.outputs.get(judge_output_id)

    async def load_bundle_context(self, bundle_id: UUID):
        return self.bundles.get(bundle_id)

    async def load_existing_analysis(
        self,
        *,
        judge_output_id: UUID,
        policy_version: str,
        delivery_policy_version: str,
    ):
        return self.existing.get((judge_output_id, policy_version, delivery_policy_version))

    async def insert_analysis(self, draft: AnalysisDraft) -> UUID:
        existing = await self.load_existing_analysis(
            judge_output_id=draft.judge_output_id,
            policy_version=draft.policy_version,
            delivery_policy_version=draft.delivery_policy_version,
        )
        if existing is not None:
            return existing.analysis_id
        analysis_id = uuid4()
        self.existing[(draft.judge_output_id, draft.policy_version, draft.delivery_policy_version)] = (
            ExistingAnalysisRecord(
                analysis_id=analysis_id,
                judge_output_id=draft.judge_output_id,
                policy_version=draft.policy_version,
                delivery_policy_version=draft.delivery_policy_version,
            )
        )
        self.analyses.append((analysis_id, draft))
        return analysis_id

    async def insert_state_transition(self, **kwargs) -> None:
        self.state_transitions.append(kwargs)

    async def insert_notification_plan_created_outbox(self, intent: NotificationPlanIntent) -> None:
        self.notification_outbox.append(intent)


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


def config(*, enable_later_delivery: bool = True, enable_notification_send: bool = True) -> PolicyEngineConfig:
    return PolicyEngineConfig(
        app_env="test",
        database_url="postgresql+psycopg://example",
        redis_url="redis://example",
        queue_name="q.analysis.policy",
        consumer_group="policy-engine",
        consumer_name="test",
        batch_size=20,
        block_ms=100,
        policy_version="verdict_policy_v1",
        delivery_policy_version="delivery_policy_v1",
        operator_chat_id=12345,
        enable_later_delivery=enable_later_delivery,
        enable_silent_later=True,
        enable_notification_send=enable_notification_send,
        render_profile_high="telegram_single_alert_high_v1",
        render_profile_normal="telegram_single_alert_normal_v1",
        log_level="INFO",
    )


def valid_payload(**score_overrides: int) -> dict:
    scores = {
        "novelty": 75,
        "practical_usefulness": 76,
        "evidence_strength": 65,
        "hype_penalty": 20,
        "confidence": 72,
        "code_quality": 70,
        "maintenance_signal": 60,
        "specificity": 65,
        "reproducibility_signal": 50,
    }
    scores.update(score_overrides)
    return {
        "judge_schema_version": "judge_output_v1",
        "headline": "Useful repository",
        "scores": scores,
        "reason_codes": ["repo_has_clear_scope"],
        "evidence_limitations_ko": ["only public docs were checked"],
        "recommended_action_ko": "inspect repository",
        "freshness_note_ko": "fresh",
        "model_proposed_verdict": "inspect_now",
    }


def repo_with_valid_case(*, payload: dict | None = None) -> tuple[
    FakeRepository,
    AnalysisPolicyJob,
    JudgeRunPolicyContext,
    JudgeOutputPolicyContext,
    BundlePolicyContext,
]:
    repository = FakeRepository()
    trigger_event_id = uuid4()
    judge_run_id = uuid4()
    judge_output_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    job = AnalysisPolicyJob(
        trigger_event_id=trigger_event_id,
        event_type="analysis.policy.apply.v1",
        judge_run_id=judge_run_id,
        judge_output_id=judge_output_id,
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
    )
    run = JudgeRunPolicyContext(
        judge_run_id=judge_run_id,
        bundle_id=bundle_id,
        prompt_version="judge_prompt_v1",
        policy_version="verdict_policy_v1",
        status="succeeded",
    )
    output = JudgeOutputPolicyContext(
        judge_output_id=judge_output_id,
        judge_run_id=judge_run_id,
        candidate_group_id=candidate_group_id,
        payload_json=payload or valid_payload(),
        model_proposed_verdict=(payload or valid_payload()).get("model_proposed_verdict"),
        model_confidence_band="medium",
    )
    bundle = BundlePolicyContext(
        bundle_id=bundle_id,
        candidate_group_id=candidate_group_id,
        current_primary_artifact_id=uuid4(),
        current_primary_artifact_type="github_repo",
    )
    repository.jobs[trigger_event_id] = job
    repository.candidates[candidate_group_id] = CandidatePolicyContext(
        candidate_group_id=candidate_group_id,
        current_bundle_id=bundle_id,
        current_analysis_id=None,
    )
    repository.runs[judge_run_id] = run
    repository.outputs[judge_output_id] = output
    repository.bundles[bundle_id] = bundle
    return repository, job, run, output, bundle


def service(repository: FakeRepository, *, cfg: PolicyEngineConfig | None = None) -> PolicyEngineService:
    return PolicyEngineService(cfg or config(), repository=repository)  # type: ignore[arg-type]


def stale_candidate(repository: FakeRepository, job: AnalysisPolicyJob) -> None:
    repository.candidates[job.candidate_group_id] = replace(
        repository.candidates[job.candidate_group_id],
        current_bundle_id=uuid4(),
    )
