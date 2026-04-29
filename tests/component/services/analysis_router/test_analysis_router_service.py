from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from services.analysis_router.config import AnalysisRouterConfig
from services.analysis_router.models import AnalysisRequestedJob, BundleRouteRecord, BundleShapeStats, CandidateRouteState
from services.analysis_router.redis_streams import StreamMessage
from services.analysis_router.repositories import AnalysisRouterRepository
from services.analysis_router.service import AnalysisRouterService
from services.analysis_router.worker import AnalysisRouterWorker


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeRepository:
    def __init__(self) -> None:
        self.jobs: dict[UUID, AnalysisRequestedJob] = {}
        self.candidate_states: dict[UUID, CandidateRouteState] = {}
        self.bundles: dict[UUID, BundleRouteRecord] = {}
        self.shapes: dict[UUID, BundleShapeStats] = {}
        self.existing_judge_runs: dict[tuple[UUID, str, str, str], UUID] = {}
        self.load_job_ids: list[UUID] = []
        self.judge_runs: list[dict] = []
        self.judge_call_events: list[dict] = []
        self.refresh_events: list[dict] = []

    def transaction(self):
        return Tx()

    async def load_job_by_trigger_event_id(self, trigger_event_id: UUID):
        self.load_job_ids.append(trigger_event_id)
        return self.jobs.get(trigger_event_id)

    async def load_candidate_route_state(self, candidate_group_id: UUID):
        return self.candidate_states.get(candidate_group_id)

    async def load_bundle(self, bundle_id: UUID):
        return self.bundles.get(bundle_id)

    async def load_bundle_shape_stats(self, bundle_id: UUID):
        return self.shapes.get(bundle_id, BundleShapeStats(member_count=0, supporting_count=0))

    async def load_existing_judge_run(self, **kwargs):
        key = (
            kwargs["bundle_id"],
            kwargs["prompt_version"],
            kwargs["model"],
            kwargs["reasoning_effort"],
        )
        return self.existing_judge_runs.get(key)

    async def get_or_create_judge_run(self, **kwargs):
        key = (
            kwargs["bundle_id"],
            kwargs["prompt_version"],
            kwargs["model"],
            kwargs["reasoning_effort"],
        )
        if key in self.existing_judge_runs:
            return self.existing_judge_runs[key], False

        judge_run_id = uuid4()
        self.judge_runs.append({"judge_run_id": judge_run_id, **kwargs})
        return judge_run_id, True

    async def insert_judge_call_requested_outbox(self, **kwargs):
        self.judge_call_events.append(kwargs)

    async def insert_bundle_refresh_outbox(self, **kwargs):
        self.refresh_events.append(kwargs)


class FakeConsumer:
    def __init__(self, message: StreamMessage) -> None:
        self.message = message
        self.acked: list[str] = []

    async def ensure_group(self) -> None:
        pass

    async def read_batch(self):
        return [self.message]

    async def ack(self, message_id: str) -> None:
        self.acked.append(message_id)


class FakeMappingResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class FakeSession:
    def __init__(self, row):
        self.row = row

    def in_transaction(self) -> bool:
        return False

    def begin(self):
        return Tx()

    async def execute(self, statement, params=None):
        return FakeMappingResult(self.row)


def _config() -> AnalysisRouterConfig:
    return AnalysisRouterConfig(
        app_env="test",
        database_url="postgresql://test",
        redis_url="redis://test",
        queue_name="q.analysis.route",
        consumer_group="analysis-router",
        consumer_name="test",
        batch_size=10,
        block_ms=100,
        enable_model_escalation=False,
        default_model="gpt-5.4-mini",
        escalation_model="gpt-5.4",
        default_reasoning_effort="low",
        escalation_reasoning_effort="medium",
        github_prompt_version="judge_github_primary_v1",
        x_prompt_version="judge_x_primary_v1",
        text_idea_prompt_version="judge_text_idea_primary_v1",
        judge_schema_version="judge_output_v1",
        policy_version="verdict_policy_v1",
        log_level="INFO",
    )


def _job(*, trigger_event_id, candidate_group_id, bundle_id, profile="github_primary") -> AnalysisRequestedJob:
    return AnalysisRequestedJob(
        trigger_event_id=trigger_event_id,
        event_type="analysis.requested.v1",
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        judge_profile=profile,
        escalation_allowed=True,
    )


def _bundle(*, candidate_group_id, bundle_id, ready=True) -> BundleRouteRecord:
    return BundleRouteRecord(
        bundle_id=bundle_id,
        candidate_group_id=candidate_group_id,
        bundle_profile_version="bundle_profile_v1",
        reroot_count=0,
        ready_for_analysis=ready,
        token_budget_profile="small",
    )


def _ready_repository(*, profile="github_primary") -> tuple[FakeRepository, UUID, UUID, UUID]:
    repository = FakeRepository()
    trigger_event_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository.jobs[trigger_event_id] = _job(
        trigger_event_id=trigger_event_id,
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        profile=profile,
    )
    repository.candidate_states[candidate_group_id] = CandidateRouteState(candidate_group_id, bundle_id)
    repository.bundles[bundle_id] = _bundle(candidate_group_id=candidate_group_id, bundle_id=bundle_id)
    repository.shapes[bundle_id] = BundleShapeStats(member_count=1, supporting_count=0)
    return repository, trigger_event_id, candidate_group_id, bundle_id


async def _handle(repository: FakeRepository, trigger_event_id: UUID) -> None:
    await AnalysisRouterService(_config(), repository=repository).handle_trigger_event(trigger_event_id)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_worker_rehydrates_analysis_request_from_event_outbox_trigger_id_not_redis_business_payload() -> None:
    repository, trigger_event_id, _, _ = _ready_repository()
    redis_decoy_bundle_id = uuid4()
    consumer = FakeConsumer(
        StreamMessage(
            stream="q.analysis.route",
            message_id="1-0",
            fields={
                "trigger_event_id": str(trigger_event_id),
                "candidate_group_id": str(uuid4()),
                "bundle_id": str(redis_decoy_bundle_id),
                "judge_profile": "web_primary",
            },
        )
    )
    service = AnalysisRouterService(_config(), repository=repository)  # type: ignore[arg-type]
    worker = AnalysisRouterWorker(_config(), consumer=consumer, service=service)  # type: ignore[arg-type]

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert repository.load_job_ids == [trigger_event_id]
    assert len(repository.judge_runs) == 1
    assert repository.judge_runs[0]["bundle_id"] != redis_decoy_bundle_id
    assert consumer.acked == ["1-0"]


@pytest.mark.asyncio
async def test_worker_acks_malformed_trigger_event_id_without_rehydrating() -> None:
    repository = FakeRepository()
    consumer = FakeConsumer(
        StreamMessage(
            stream="q.analysis.route",
            message_id="1-0",
            fields={"trigger_event_id": "not-a-uuid"},
        )
    )
    service = AnalysisRouterService(_config(), repository=repository)  # type: ignore[arg-type]
    worker = AnalysisRouterWorker(_config(), consumer=consumer, service=service)  # type: ignore[arg-type]

    result = await worker.run_once()

    assert result.processed == 1
    assert result.acked == 1
    assert repository.load_job_ids == []
    assert repository.judge_runs == []
    assert repository.judge_call_events == []
    assert consumer.acked == ["1-0"]


@pytest.mark.asyncio
async def test_repository_ignores_malformed_candidate_group_id_in_outbox_payload() -> None:
    trigger_event_id = uuid4()
    repository = AnalysisRouterRepository(
        FakeSession(
            {
                "event_id": trigger_event_id,
                "event_type": "analysis.requested.v1",
                "payload_json": {
                    "candidate_group_id": "not-a-uuid",
                    "bundle_id": str(uuid4()),
                    "judge_profile": "github_primary",
                },
            }
        )
    )

    assert await repository.load_job_by_trigger_event_id(trigger_event_id) is None


@pytest.mark.asyncio
async def test_repository_ignores_malformed_bundle_id_in_outbox_payload() -> None:
    trigger_event_id = uuid4()
    repository = AnalysisRouterRepository(
        FakeSession(
            {
                "event_id": trigger_event_id,
                "event_type": "analysis.requested.v1",
                "payload_json": {
                    "candidate_group_id": str(uuid4()),
                    "bundle_id": "not-a-uuid",
                    "judge_profile": "github_primary",
                },
            }
        )
    )

    assert await repository.load_job_by_trigger_event_id(trigger_event_id) is None


@pytest.mark.asyncio
async def test_stale_request_noop() -> None:
    repository, trigger_event_id, candidate_group_id, _ = _ready_repository()
    repository.candidate_states[candidate_group_id] = CandidateRouteState(candidate_group_id, uuid4())

    await _handle(repository, trigger_event_id)

    assert repository.judge_runs == []
    assert repository.judge_call_events == []
    assert repository.refresh_events == []


@pytest.mark.asyncio
async def test_bundle_missing_emits_refresh() -> None:
    repository, trigger_event_id, _, bundle_id = _ready_repository()
    repository.bundles.pop(bundle_id)

    await _handle(repository, trigger_event_id)

    assert repository.judge_runs == []
    assert repository.judge_call_events == []
    assert repository.refresh_events[0]["refresh_reason"] == "bundle_missing"


@pytest.mark.asyncio
async def test_bundle_not_ready_emits_refresh() -> None:
    repository, trigger_event_id, candidate_group_id, bundle_id = _ready_repository()
    repository.bundles[bundle_id] = _bundle(candidate_group_id=candidate_group_id, bundle_id=bundle_id, ready=False)

    await _handle(repository, trigger_event_id)

    assert repository.judge_runs == []
    assert repository.judge_call_events == []
    assert repository.refresh_events[0]["refresh_reason"] == "bundle_not_ready"


@pytest.mark.asyncio
async def test_missing_judge_profile_refreshes_without_judge_run() -> None:
    repository, trigger_event_id, candidate_group_id, bundle_id = _ready_repository(profile=None)

    await _handle(repository, trigger_event_id)

    assert repository.judge_runs == []
    assert repository.judge_call_events == []
    assert repository.refresh_events == [
        {
            "candidate_group_id": candidate_group_id,
            "bundle_id": bundle_id,
            "refresh_reason": "bundle_profile_missing",
        }
    ]


@pytest.mark.asyncio
async def test_members_missing_emits_refresh() -> None:
    repository, trigger_event_id, _, bundle_id = _ready_repository()
    repository.shapes[bundle_id] = BundleShapeStats(member_count=0, supporting_count=0)

    await _handle(repository, trigger_event_id)

    assert repository.judge_runs == []
    assert repository.judge_call_events == []
    assert repository.refresh_events[0]["refresh_reason"] == "bundle_members_missing"


@pytest.mark.asyncio
async def test_ready_bundle_creates_one_judge_run_and_one_judge_call_requested() -> None:
    repository, trigger_event_id, _, bundle_id = _ready_repository()

    await _handle(repository, trigger_event_id)

    assert len(repository.judge_runs) == 1
    assert repository.judge_runs[0]["bundle_id"] == bundle_id
    assert repository.judge_runs[0]["judge_profile"] == "github_primary"
    assert repository.judge_runs[0]["model"] == "gpt-5.4-mini"
    assert repository.judge_runs[0]["reasoning_effort"] == "low"
    assert len(repository.judge_call_events) == 1
    assert repository.judge_call_events[0]["judge_run_id"] == repository.judge_runs[0]["judge_run_id"]


@pytest.mark.asyncio
async def test_bundle_candidate_mismatch_noops_without_judge_or_refresh() -> None:
    repository, trigger_event_id, _, bundle_id = _ready_repository()
    repository.bundles[bundle_id] = _bundle(candidate_group_id=uuid4(), bundle_id=bundle_id)

    await _handle(repository, trigger_event_id)

    assert repository.judge_runs == []
    assert repository.judge_call_events == []
    assert repository.refresh_events == []


@pytest.mark.asyncio
async def test_existing_judge_run_reuse_does_not_duplicate_run_or_event() -> None:
    repository, trigger_event_id, _, bundle_id = _ready_repository()
    existing_id = uuid4()
    repository.existing_judge_runs[
        (bundle_id, "judge_github_primary_v1", "gpt-5.4-mini", "low")
    ] = existing_id

    await _handle(repository, trigger_event_id)

    assert repository.judge_runs == []
    assert repository.judge_call_events == []
    assert repository.refresh_events == []


@pytest.mark.asyncio
async def test_allowed_text_idea_profile_works_but_idea_primary_does_not() -> None:
    text_repo, text_trigger_id, _, _ = _ready_repository(profile="text_idea_primary")
    idea_repo, idea_trigger_id, _, _ = _ready_repository(profile="idea_primary")

    await _handle(text_repo, text_trigger_id)
    await _handle(idea_repo, idea_trigger_id)

    assert len(text_repo.judge_runs) == 1
    assert text_repo.judge_runs[0]["prompt_version"] == "judge_text_idea_primary_v1"
    assert idea_repo.judge_runs == []
    assert idea_repo.judge_call_events == []
    assert idea_repo.refresh_events == []
