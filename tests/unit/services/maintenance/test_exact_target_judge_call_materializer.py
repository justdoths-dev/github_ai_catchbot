from __future__ import annotations

import ast
import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.analysis_router.config import AnalysisRouterConfig
from src.services.analysis_router.models import (
    BundleRouteRecord,
    BundleShapeStats,
    CandidateRouteState,
)
from src.services.maintenance.exact_target_judge_call_materializer import (
    ANALYSIS_REQUEST_SELECTION_CONFIRM_TOKEN,
    CONFIRM_TOKEN,
    DownstreamCounts,
    ExactTargetJudgeCallMaterializerComponents,
    ExactTargetJudgeCallMaterializerRequest,
    ExistingJudgeRunReadback,
    JudgeCallEventReadback,
    MaterializerEvent,
    RuntimeConfigBundle,
    SqlExactTargetJudgeCallMaterializerRepository,
    run_cli,
    run_exact_target_judge_call_materializer,
)
from src.services.maintenance import exact_target_judge_call_materializer as materializer


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/maintenance/exact_target_judge_call_materializer.py"
RAW_UUID = uuid4()
RAW_DATABASE_URL = (
    "postgresql+psycopg"
    + ":"
    + "/"
    + "/"
    + "user:"
    + "private"
    + "-db"
    + "-secret"
    + "@localhost:5432/prod"
)
RAW_REDIS_URL = (
    "redis"
    + ":"
    + "/"
    + "/"
    + ":"
    + "private"
    + "-redis"
    + "-secret"
    + "@localhost:6379/0"
)
RAW_PAYLOAD_TEXT = "private source payload text"
RAW_PROMPT_TEXT = "private prompt text"
RAW_EXCEPTION_BODY = "private exception body"


class FakeMaterializerRepository:
    def __init__(
        self,
        *,
        event: MaterializerEvent | None,
        candidate_state: CandidateRouteState | None,
        bundle: BundleRouteRecord | None,
        shape: BundleShapeStats | None = None,
        existing_judge_run_id: UUID | None = None,
        existing_judge_call_event_id: UUID | None = None,
        router_conflicting_judge_run_id: UUID | None = None,
        downstream_counts: DownstreamCounts | None = None,
        close_preflight_error: Exception | None = None,
        commit_error: Exception | None = None,
        require_commit_visibility: bool = False,
    ) -> None:
        self.event = event
        self.candidate_state = candidate_state
        self.bundle = bundle
        self.shape = shape or BundleShapeStats(member_count=1, supporting_count=0)
        self.judge_run_id = existing_judge_run_id
        self.judge_call_event_id = existing_judge_call_event_id
        self.pending_judge_run_id: UUID | None = None
        self.pending_judge_call_event_id: UUID | None = None
        self.router_conflicting_judge_run_id = router_conflicting_judge_run_id
        self.downstream_counts = downstream_counts or DownstreamCounts()
        self.close_preflight_error = close_preflight_error
        self.commit_error = commit_error
        self.require_commit_visibility = require_commit_visibility
        self.materialize_calls = 0
        self.close_preflight_calls = 0
        self.commit_active_calls = 0
        self.pre_commit_visible_judge_run_count: int | None = None
        self.call_order: list[str] = []
        self.load_event_calls: list[UUID] = []
        self.load_candidate_calls: list[UUID] = []
        self.load_bundle_calls: list[UUID] = []
        self.load_shape_calls: list[UUID] = []
        self.exact_judge_run_readback_counts: list[int] = []

    async def select_latest_eligible_analysis_request(
        self, router_config: AnalysisRouterConfig
    ) -> UUID | None:
        self.call_order.append("select_latest_eligible_analysis_request")
        del router_config
        if self.event is None or self.event.event_type != "analysis.requested.v1":
            return None
        candidate_group_id = UUID(str(self.event.payload_json.get("candidate_group_id")))
        bundle_id = UUID(str(self.event.payload_json.get("bundle_id")))
        if self.candidate_state is None or self.candidate_state.current_bundle_id != bundle_id:
            return None
        if self.bundle is None or self.bundle.candidate_group_id != candidate_group_id:
            return None
        if not self.bundle.ready_for_analysis or self.shape.member_count <= 0:
            return None
        if self.event.payload_json.get("judge_profile") not in {
            "github_primary",
            "x_primary",
            "text_idea_primary",
        }:
            return None
        if (
            self.judge_run_id is not None
            or self.judge_call_event_id is not None
            or self.router_conflicting_judge_run_id is not None
            or self.downstream_counts.total
        ):
            return None
        return self.event.event_id

    async def load_event_by_id(self, event_id: UUID) -> MaterializerEvent | None:
        self.call_order.append("load_event_by_id")
        self.load_event_calls.append(event_id)
        if self.event is None or self.event.event_id != event_id:
            return None
        return self.event

    async def load_candidate_route_state(
        self, candidate_group_id: UUID
    ) -> CandidateRouteState | None:
        self.call_order.append("load_candidate_route_state")
        self.load_candidate_calls.append(candidate_group_id)
        return self.candidate_state

    async def load_bundle(self, bundle_id: UUID) -> BundleRouteRecord | None:
        self.call_order.append("load_bundle")
        self.load_bundle_calls.append(bundle_id)
        return self.bundle

    async def load_bundle_shape_stats(self, bundle_id: UUID) -> BundleShapeStats:
        self.call_order.append("load_bundle_shape_stats")
        self.load_shape_calls.append(bundle_id)
        return self.shape

    async def load_exact_judge_run(self, decision, *, bundle_id: UUID) -> ExistingJudgeRunReadback:
        self.call_order.append("load_exact_judge_run")
        del decision, bundle_id
        count = 1 if self.judge_run_id else 0
        self.exact_judge_run_readback_counts.append(count)
        return ExistingJudgeRunReadback(
            count=count,
            judge_run_id=self.judge_run_id,
        )

    async def load_router_conflicting_judge_run(
        self, decision, *, bundle_id: UUID
    ) -> ExistingJudgeRunReadback:
        self.call_order.append("load_router_conflicting_judge_run")
        del decision, bundle_id
        return ExistingJudgeRunReadback(
            count=1 if self.router_conflicting_judge_run_id else 0,
            judge_run_id=self.router_conflicting_judge_run_id,
        )

    async def load_judge_call_event_for_run(self, judge_run_id: UUID) -> JudgeCallEventReadback:
        self.call_order.append("load_judge_call_event_for_run")
        if self.judge_run_id != judge_run_id or self.judge_call_event_id is None:
            return JudgeCallEventReadback(count=0)
        return JudgeCallEventReadback(count=1, event_id=self.judge_call_event_id)

    async def load_downstream_counts_for_run(self, judge_run_id: UUID) -> DownstreamCounts:
        self.call_order.append("load_downstream_counts_for_run")
        assert self.judge_run_id == judge_run_id
        return self.downstream_counts

    async def close_preflight_transaction(self) -> None:
        self.call_order.append("close_preflight_transaction")
        self.close_preflight_calls += 1
        if self.close_preflight_error is not None:
            raise self.close_preflight_error

    async def commit_active_transaction(self) -> None:
        self.call_order.append("commit_active_transaction")
        self.commit_active_calls += 1
        if self.commit_error is not None:
            raise self.commit_error
        self.pre_commit_visible_judge_run_count = 1 if self.judge_run_id else 0
        if self.pending_judge_run_id is not None:
            self.judge_run_id = self.pending_judge_run_id
            self.judge_call_event_id = self.pending_judge_call_event_id
            self.pending_judge_run_id = None
            self.pending_judge_call_event_id = None

    def materialize(self) -> None:
        self.call_order.append("materialize")
        self.materialize_calls += 1
        if self.judge_run_id is None and self.pending_judge_run_id is None:
            judge_run_id = uuid4()
            judge_call_event_id = uuid4()
            if self.require_commit_visibility:
                self.pending_judge_run_id = judge_run_id
                self.pending_judge_call_event_id = judge_call_event_id
                return
            self.judge_run_id = judge_run_id
            self.judge_call_event_id = judge_call_event_id


class FakeRouterService:
    def __init__(
        self,
        repository: FakeMaterializerRepository,
        *,
        raise_error: bool = False,
    ) -> None:
        self.repository = repository
        self.raise_error = raise_error
        self.calls: list[UUID] = []

    async def handle_trigger_event(self, trigger_event_id: str | UUID) -> None:
        self.repository.call_order.append("router_service.handle_trigger_event")
        self.calls.append(UUID(str(trigger_event_id)))
        if self.raise_error:
            raise RuntimeError(RAW_EXCEPTION_BODY)
        self.repository.materialize()


def _router_config() -> AnalysisRouterConfig:
    return AnalysisRouterConfig(
        app_env="test",
        database_url=RAW_DATABASE_URL,
        redis_url=RAW_REDIS_URL,
        queue_name="q.analysis.route",
        consumer_group="analysis-router",
        consumer_name="analysis-router-test",
        batch_size=1,
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


def _event(
    event_id: UUID,
    candidate_group_id: UUID,
    bundle_id: UUID,
    *,
    event_type: str = "analysis.requested.v1",
    judge_profile: str = "github_primary",
) -> MaterializerEvent:
    return MaterializerEvent(
        event_id=event_id,
        event_type=event_type,
        payload_json={
            "candidate_group_id": str(candidate_group_id),
            "bundle_id": str(bundle_id),
            "judge_profile": judge_profile,
            "escalation_allowed": False,
            "source_text": RAW_PAYLOAD_TEXT,
            "prompt_text": RAW_PROMPT_TEXT,
            "raw_uuid": str(RAW_UUID),
        },
    )


def _candidate_state(candidate_group_id: UUID, bundle_id: UUID | None) -> CandidateRouteState:
    return CandidateRouteState(
        candidate_group_id=candidate_group_id,
        current_bundle_id=bundle_id,
    )


def _bundle(
    candidate_group_id: UUID,
    bundle_id: UUID,
    *,
    ready: bool = True,
) -> BundleRouteRecord:
    return BundleRouteRecord(
        bundle_id=bundle_id,
        candidate_group_id=candidate_group_id,
        bundle_profile_version="bundle_profile_v1",
        reroot_count=0,
        ready_for_analysis=ready,
        token_budget_profile="small",
    )


def _success_parts(
    *,
    ready: bool = True,
    judge_profile: str = "github_primary",
    existing_judge_run_id: UUID | None = None,
    existing_judge_call_event_id: UUID | None = None,
    router_conflicting_judge_run_id: UUID | None = None,
    downstream_counts: DownstreamCounts | None = None,
    close_preflight_error: Exception | None = None,
    commit_error: Exception | None = None,
    require_commit_visibility: bool = False,
) -> tuple[UUID, FakeMaterializerRepository, FakeRouterService]:
    event_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeMaterializerRepository(
        event=_event(
            event_id,
            candidate_group_id,
            bundle_id,
            judge_profile=judge_profile,
        ),
        candidate_state=_candidate_state(candidate_group_id, bundle_id),
        bundle=_bundle(candidate_group_id, bundle_id, ready=ready),
        existing_judge_run_id=existing_judge_run_id,
        existing_judge_call_event_id=existing_judge_call_event_id,
        router_conflicting_judge_run_id=router_conflicting_judge_run_id,
        downstream_counts=downstream_counts,
        close_preflight_error=close_preflight_error,
        commit_error=commit_error,
        require_commit_visibility=require_commit_visibility,
    )
    service = FakeRouterService(repository)
    return event_id, repository, service


async def _run(
    event_id: UUID,
    repository: FakeMaterializerRepository,
    service: FakeRouterService,
    *,
    mode: str = "plan",
):
    return await run_exact_target_judge_call_materializer(
        ExactTargetJudgeCallMaterializerRequest(mode=mode, trigger_event_id=event_id),
        router_config=_router_config(),
        components=ExactTargetJudgeCallMaterializerComponents(
            materializer_repository=repository,
            router_service=service,
        ),
    )


def _runtime_bundle() -> RuntimeConfigBundle:
    return RuntimeConfigBundle(
        database_url=RAW_DATABASE_URL,
        values={"DATABASE_URL": RAW_DATABASE_URL},
        router_config=_router_config(),
    )


def _fail_runtime_loader(env_file: str) -> RuntimeConfigBundle:
    raise AssertionError(f"runtime config must not be loaded: {env_file}")


@pytest.mark.asyncio
async def test_plan_is_read_only_and_reports_ready_target() -> None:
    event_id, repository, service = _success_parts()

    report = await _run(event_id, repository, service, mode="plan")

    assert report.status == "pass"
    assert report.reason_code == "plan_ready"
    assert report.preflight_passed is True
    assert report.router_attempted is False
    assert report.judge_run_created is False
    assert report.judge_call_event_created is False
    assert service.calls == []
    assert repository.materialize_calls == 0
    assert repository.close_preflight_calls == 0
    assert repository.commit_active_calls == 0
    assert repository.load_event_calls == [event_id]
    assert report.analysis_request_fingerprint is not None
    assert len(report.analysis_request_fingerprint) == 16
    assert report.bounded_counts["existing_judge_runs"] == 0


@pytest.mark.asyncio
async def test_wrong_event_type_blocks_before_router_invocation() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeMaterializerRepository(
        event=_event(
            event_id,
            candidate_group_id,
            bundle_id,
            event_type="judge.call.requested.v1",
        ),
        candidate_state=_candidate_state(candidate_group_id, bundle_id),
        bundle=_bundle(candidate_group_id, bundle_id),
    )
    service = FakeRouterService(repository)

    report = await _run(event_id, repository, service, mode="execute")

    assert report.status == "blocked"
    assert report.reason_code == "wrong_event_type"
    assert service.calls == []
    assert repository.materialize_calls == 0


@pytest.mark.asyncio
async def test_stale_bundle_blocks() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeMaterializerRepository(
        event=_event(event_id, candidate_group_id, bundle_id),
        candidate_state=_candidate_state(candidate_group_id, uuid4()),
        bundle=_bundle(candidate_group_id, bundle_id),
    )
    service = FakeRouterService(repository)

    report = await _run(event_id, repository, service, mode="execute")

    assert report.status == "blocked"
    assert report.reason_code == "stale_bundle"
    assert service.calls == []


@pytest.mark.asyncio
async def test_not_ready_bundle_blocks() -> None:
    event_id, repository, service = _success_parts(ready=False)

    report = await _run(event_id, repository, service, mode="execute")

    assert report.status == "blocked"
    assert report.reason_code == "bundle_not_ready"
    assert service.calls == []


@pytest.mark.asyncio
async def test_unsupported_judge_profile_blocks() -> None:
    event_id, repository, service = _success_parts(judge_profile="web_primary")

    report = await _run(event_id, repository, service, mode="execute")

    assert report.status == "blocked"
    assert report.reason_code == "judge_profile_not_allowlisted"
    assert service.calls == []


@pytest.mark.asyncio
async def test_existing_judge_run_blocks_before_creating_second_run() -> None:
    event_id, repository, service = _success_parts(existing_judge_run_id=uuid4())

    report = await _run(event_id, repository, service, mode="execute")

    assert report.status == "blocked"
    assert report.reason_code == "existing_judge_run"
    assert service.calls == []
    assert repository.materialize_calls == 0
    assert report.judge_run_fingerprint is not None


@pytest.mark.asyncio
async def test_existing_judge_call_blocks_before_router_invocation() -> None:
    event_id, repository, service = _success_parts(
        existing_judge_run_id=uuid4(),
        existing_judge_call_event_id=uuid4(),
    )

    report = await _run(event_id, repository, service, mode="execute")

    assert report.status == "blocked"
    assert report.reason_code == "existing_judge_call"
    assert service.calls == []
    assert repository.materialize_calls == 0
    assert report.judge_call_event_fingerprint is not None


@pytest.mark.asyncio
async def test_router_unique_conflict_blocks_before_router_invocation() -> None:
    event_id, repository, service = _success_parts(
        router_conflicting_judge_run_id=uuid4(),
    )

    report = await _run(event_id, repository, service, mode="execute")

    assert report.status == "blocked"
    assert report.reason_code == "analysis_router_would_not_create_judge_call"
    assert service.calls == []
    assert repository.materialize_calls == 0
    assert report.judge_run_fingerprint is not None


@pytest.mark.asyncio
async def test_downstream_rows_block_existing_target() -> None:
    event_id, repository, service = _success_parts(
        existing_judge_run_id=uuid4(),
        downstream_counts=DownstreamCounts(judge_outputs=1),
    )

    report = await _run(event_id, repository, service, mode="execute")

    assert report.status == "blocked"
    assert report.reason_code == "downstream_already_exists"
    assert service.calls == []
    assert repository.materialize_calls == 0
    assert report.bounded_counts["downstream_judge_outputs"] == 1


@pytest.mark.asyncio
async def test_successful_execute_invokes_router_once_and_materializes_one_judge_call() -> None:
    event_id, repository, service = _success_parts()

    report = await _run(event_id, repository, service, mode="execute")

    assert report.status == "pass"
    assert report.reason_code == "judge_call_materialized"
    assert report.preflight_passed is True
    assert report.router_attempted is True
    assert report.judge_run_created is True
    assert report.judge_call_event_created is True
    assert report.openai_attempted is False
    assert report.redis_attempted is False
    assert report.telegram_attempted is False
    assert service.calls == [event_id]
    assert repository.materialize_calls == 1
    assert repository.commit_active_calls == 1
    assert report.judge_run_fingerprint is not None
    assert report.judge_call_event_fingerprint is not None
    assert report.bounded_counts["existing_judge_runs"] == 1
    assert report.bounded_counts["existing_judge_call_events"] == 1


@pytest.mark.asyncio
async def test_execute_commits_router_writes_before_final_durable_readback() -> None:
    event_id, repository, service = _success_parts(require_commit_visibility=True)

    report = await _run(event_id, repository, service, mode="execute")

    assert report.status == "pass"
    assert report.reason_code == "judge_call_materialized"
    assert report.judge_run_created is True
    assert report.judge_call_event_created is True
    assert service.calls == [event_id]
    assert repository.materialize_calls == 1
    assert repository.commit_active_calls == 1
    assert repository.pre_commit_visible_judge_run_count == 0
    assert repository.exact_judge_run_readback_counts == [0, 1]
    assert repository.pending_judge_run_id is None
    assert repository.pending_judge_call_event_id is None
    assert repository.call_order[6:11] == [
        "close_preflight_transaction",
        "router_service.handle_trigger_event",
        "materialize",
        "commit_active_transaction",
        "load_exact_judge_run",
    ]
    assert report.bounded_counts["existing_judge_runs"] == 1
    assert report.bounded_counts["existing_judge_call_events"] == 1


@pytest.mark.asyncio
async def test_execute_closes_preflight_transaction_before_router_invocation() -> None:
    event_id, repository, service = _success_parts()

    report = await _run(event_id, repository, service, mode="execute")

    assert report.status == "pass"
    assert service.calls == [event_id]
    assert repository.call_order[:8] == [
        "load_event_by_id",
        "load_candidate_route_state",
        "load_bundle",
        "load_bundle_shape_stats",
        "load_exact_judge_run",
        "load_router_conflicting_judge_run",
        "close_preflight_transaction",
        "router_service.handle_trigger_event",
    ]


@pytest.mark.asyncio
async def test_preflight_transaction_close_failure_blocks_router_with_sanitized_reason() -> None:
    event_id, repository, service = _success_parts(
        close_preflight_error=RuntimeError(RAW_EXCEPTION_BODY),
    )

    report = await _run(event_id, repository, service, mode="execute")
    rendered = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "failed"
    assert report.reason_code == "preflight_transaction_close_failed"
    assert report.router_attempted is False
    assert service.calls == []
    assert repository.materialize_calls == 0
    assert repository.call_order[-1] == "close_preflight_transaction"
    assert RAW_EXCEPTION_BODY not in rendered
    assert "Traceback" not in rendered


@pytest.mark.asyncio
async def test_selection_flag_without_confirm_blocks_before_env_load(tmp_path: Path) -> None:
    outputs: list[str] = []

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--env-file",
            str(tmp_path / "missing-runtime.env"),
            "--select-latest-eligible-analysis-request",
        ],
        emit_json=outputs.append,
        runtime_config_loader=_fail_runtime_loader,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "analysis_request_selection_authority_required"
    assert payload["openai_attempted"] is False
    assert payload["redis_attempted"] is False
    assert payload["telegram_attempted"] is False
    assert "missing-runtime" not in outputs[0]


@pytest.mark.asyncio
async def test_selection_confirm_only_blocks_before_env_load(tmp_path: Path) -> None:
    outputs: list[str] = []

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--env-file",
            str(tmp_path / "missing-runtime.env"),
            "--selection-confirm",
            ANALYSIS_REQUEST_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        runtime_config_loader=_fail_runtime_loader,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "analysis_request_selection_authority_required"
    assert "missing-runtime" not in outputs[0]


@pytest.mark.asyncio
async def test_wrong_selection_confirm_blocks_before_env_load(tmp_path: Path) -> None:
    outputs: list[str] = []

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--env-file",
            str(tmp_path / "missing-runtime.env"),
            "--select-latest-eligible-analysis-request",
            "--selection-confirm",
            "latest",
        ],
        emit_json=outputs.append,
        runtime_config_loader=_fail_runtime_loader,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "analysis_request_selection_authority_required"
    assert "missing-runtime" not in outputs[0]


@pytest.mark.asyncio
async def test_trigger_event_id_conflicts_with_selection_before_env_load(
    tmp_path: Path,
) -> None:
    outputs: list[str] = []
    trigger_event_id = uuid4()

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--trigger-event-id",
            str(trigger_event_id),
            "--env-file",
            str(tmp_path / "missing-runtime.env"),
            "--select-latest-eligible-analysis-request",
            "--selection-confirm",
            ANALYSIS_REQUEST_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        runtime_config_loader=_fail_runtime_loader,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["reason_code"] == "target_selection_conflict"
    assert payload["analysis_request_fingerprint"] == materializer._fingerprint(
        trigger_event_id
    )
    assert str(trigger_event_id) not in outputs[0]
    assert "missing-runtime" not in outputs[0]


@pytest.mark.asyncio
async def test_select_latest_eligible_blocks_when_no_analysis_request_target() -> None:
    outputs: list[str] = []
    repository = FakeMaterializerRepository(
        event=None,
        candidate_state=None,
        bundle=None,
    )
    service = FakeRouterService(repository)

    @asynccontextmanager
    async def builder(runtime: RuntimeConfigBundle):
        del runtime
        yield ExactTargetJudgeCallMaterializerComponents(
            materializer_repository=repository,
            router_service=service,
        )

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--env-file",
            "/tmp/materializer-runtime.env",
            "--select-latest-eligible-analysis-request",
            "--selection-confirm",
            ANALYSIS_REQUEST_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        runtime_config_loader=lambda _env_file: _runtime_bundle(),
        session_components_builder=builder,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 2
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == "eligible_analysis_request_target_missing"
    assert payload["analysis_request_fingerprint"] is None
    assert payload["router_attempted"] is False
    assert payload["openai_attempted"] is False
    assert payload["redis_attempted"] is False
    assert payload["telegram_attempted"] is False
    assert repository.call_order == ["select_latest_eligible_analysis_request"]
    assert service.calls == []


@pytest.mark.asyncio
async def test_select_latest_eligible_plan_reaches_plan_ready_with_fingerprints_only() -> None:
    outputs: list[str] = []
    event_id, repository, service = _success_parts()

    @asynccontextmanager
    async def builder(runtime: RuntimeConfigBundle):
        del runtime
        yield ExactTargetJudgeCallMaterializerComponents(
            materializer_repository=repository,
            router_service=service,
        )

    exit_code = await run_cli(
        [
            "--mode",
            "plan",
            "--env-file",
            "/tmp/materializer-runtime.env",
            "--select-latest-eligible-analysis-request",
            "--selection-confirm",
            ANALYSIS_REQUEST_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        runtime_config_loader=lambda _env_file: _runtime_bundle(),
        session_components_builder=builder,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 0
    assert payload["status"] == "pass"
    assert payload["reason_code"] == "plan_ready"
    assert payload["analysis_request_fingerprint"] == materializer._fingerprint(event_id)
    assert payload["candidate_group_fingerprint"] == materializer._fingerprint(
        repository.event.payload_json["candidate_group_id"]
    )
    assert payload["bundle_fingerprint"] == materializer._fingerprint(
        repository.event.payload_json["bundle_id"]
    )
    assert payload["router_attempted"] is False
    assert repository.call_order[0] == "select_latest_eligible_analysis_request"
    for raw_value in (
        str(event_id),
        str(repository.event.payload_json["candidate_group_id"]),
        str(repository.event.payload_json["bundle_id"]),
    ):
        assert raw_value not in outputs[0]


@pytest.mark.asyncio
async def test_select_latest_eligible_execute_reuses_materializer_path_once() -> None:
    outputs: list[str] = []
    event_id, repository, service = _success_parts()

    @asynccontextmanager
    async def builder(runtime: RuntimeConfigBundle):
        del runtime
        yield ExactTargetJudgeCallMaterializerComponents(
            materializer_repository=repository,
            router_service=service,
        )

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--env-file",
            "/tmp/materializer-runtime.env",
            "--confirm",
            CONFIRM_TOKEN,
            "--select-latest-eligible-analysis-request",
            "--selection-confirm",
            ANALYSIS_REQUEST_SELECTION_CONFIRM_TOKEN,
        ],
        emit_json=outputs.append,
        runtime_config_loader=lambda _env_file: _runtime_bundle(),
        session_components_builder=builder,
    )

    payload = json.loads(outputs[0])
    assert exit_code == 0
    assert payload["status"] == "pass"
    assert payload["reason_code"] == "judge_call_materialized"
    assert payload["analysis_request_fingerprint"] == materializer._fingerprint(event_id)
    assert payload["router_attempted"] is True
    assert payload["judge_run_created"] is True
    assert payload["judge_call_event_created"] is True
    assert payload["openai_attempted"] is False
    assert payload["redis_attempted"] is False
    assert payload["telegram_attempted"] is False
    assert service.calls == [event_id]
    assert repository.materialize_calls == 1
    assert payload["bounded_counts"]["existing_judge_call_events"] == 1
    assert str(event_id) not in outputs[0]


@pytest.mark.asyncio
async def test_consumed_downstream_or_existing_judge_call_targets_are_not_selection_eligible() -> None:
    existing_run_event_id, existing_run_repository, _ = _success_parts(
        existing_judge_run_id=uuid4(),
    )
    existing_call_event_id, existing_call_repository, _ = _success_parts(
        existing_judge_run_id=uuid4(),
        existing_judge_call_event_id=uuid4(),
    )
    downstream_event_id, downstream_repository, _ = _success_parts(
        existing_judge_run_id=uuid4(),
        downstream_counts=DownstreamCounts(judge_outputs=1),
    )

    for event_id, repository in (
        (existing_run_event_id, existing_run_repository),
        (existing_call_event_id, existing_call_repository),
        (downstream_event_id, downstream_repository),
    ):
        assert await repository.select_latest_eligible_analysis_request(_router_config()) is None
        assert event_id not in repository.load_event_calls


@pytest.mark.asyncio
async def test_commit_failure_fails_closed_without_private_exception_text() -> None:
    event_id, repository, service = _success_parts(
        commit_error=RuntimeError(RAW_EXCEPTION_BODY),
        require_commit_visibility=True,
    )

    report = await _run(event_id, repository, service, mode="execute")
    rendered = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "failed"
    assert report.reason_code == "judge_call_commit_failed"
    assert report.router_attempted is True
    assert report.judge_run_created is False
    assert report.judge_call_event_created is False
    assert service.calls == [event_id]
    assert repository.materialize_calls == 1
    assert repository.commit_active_calls == 1
    assert repository.exact_judge_run_readback_counts == [0]
    assert repository.pending_judge_run_id is not None
    assert RAW_EXCEPTION_BODY not in rendered
    assert "Traceback" not in rendered


@pytest.mark.asyncio
async def test_duplicate_execute_blocks_before_second_router_call() -> None:
    event_id, repository, service = _success_parts()

    first = await _run(event_id, repository, service, mode="execute")
    second = await _run(event_id, repository, service, mode="execute")

    assert first.status == "pass"
    assert second.status == "blocked"
    assert second.reason_code == "existing_judge_call"
    assert service.calls == [event_id]
    assert repository.materialize_calls == 1


@pytest.mark.asyncio
async def test_router_noop_or_failed_materialization_is_failed() -> None:
    event_id, repository, service = _success_parts()

    async def no_op_handle(trigger_event_id: str | UUID) -> None:
        service.calls.append(UUID(str(trigger_event_id)))

    service.handle_trigger_event = no_op_handle  # type: ignore[method-assign]

    report = await _run(event_id, repository, service, mode="execute")

    assert report.status == "failed"
    assert report.reason_code == "judge_run_cardinality_invalid"
    assert report.router_attempted is True
    assert service.calls == [event_id]
    assert repository.materialize_calls == 0


@pytest.mark.asyncio
async def test_redacted_report_excludes_raw_ids_secrets_payloads_prompts_and_exceptions() -> None:
    event_id, repository, service = _success_parts()
    service.raise_error = True

    report = await _run(event_id, repository, service, mode="execute")
    rendered = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "failed"
    assert report.reason_code == "unhandled_error"
    for forbidden in (
        str(event_id),
        str(repository.event.payload_json["candidate_group_id"]),
        str(repository.event.payload_json["bundle_id"]),
        str(RAW_UUID),
        RAW_DATABASE_URL,
        RAW_REDIS_URL,
        RAW_PAYLOAD_TEXT,
        RAW_PROMPT_TEXT,
        RAW_EXCEPTION_BODY,
        "Traceback",
    ):
        assert forbidden not in rendered
    assert report.redactions_applied is True


def test_module_does_not_import_openai_redis_telegram_or_notifier_boundaries() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    forbidden = {
        "redis",
        "redis.asyncio",
        "subprocess",
        "docker",
        "systemd",
        "src.services.judge_openai",
        "..judge_openai",
        "src.services.notifier_telegram",
        "..notifier_telegram",
        "telegram",
    }
    assert imported_modules.isdisjoint(forbidden)


@pytest.mark.asyncio
async def test_sql_repository_commits_active_preflight_transaction_before_router_write() -> None:
    class FakeSqlSession:
        def __init__(self) -> None:
            self.transaction_open = True
            self.in_transaction_calls = 0
            self.commit_calls = 0

        def in_transaction(self) -> bool:
            self.in_transaction_calls += 1
            return self.transaction_open

        async def commit(self) -> None:
            self.commit_calls += 1
            self.transaction_open = False

    session = FakeSqlSession()
    repository = SqlExactTargetJudgeCallMaterializerRepository(session)

    await repository.close_preflight_transaction()

    assert session.in_transaction_calls == 1
    assert session.commit_calls == 1
    assert session.transaction_open is False


@pytest.mark.asyncio
async def test_sql_repository_commits_active_transaction_before_final_readback() -> None:
    class FakeSqlSession:
        def __init__(self, transaction_open: bool) -> None:
            self.transaction_open = transaction_open
            self.in_transaction_calls = 0
            self.commit_calls = 0

        def in_transaction(self) -> bool:
            self.in_transaction_calls += 1
            return self.transaction_open

        async def commit(self) -> None:
            self.commit_calls += 1
            self.transaction_open = False

    active_session = FakeSqlSession(transaction_open=True)
    active_repository = SqlExactTargetJudgeCallMaterializerRepository(active_session)

    await active_repository.commit_active_transaction()

    assert active_session.in_transaction_calls == 1
    assert active_session.commit_calls == 1
    assert active_session.transaction_open is False

    idle_session = FakeSqlSession(transaction_open=False)
    idle_repository = SqlExactTargetJudgeCallMaterializerRepository(idle_session)

    await idle_repository.commit_active_transaction()

    assert idle_session.in_transaction_calls == 1
    assert idle_session.commit_calls == 0
    assert idle_session.transaction_open is False
