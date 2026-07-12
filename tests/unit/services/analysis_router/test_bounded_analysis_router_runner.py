from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.services.analysis_router.bounded_analysis_router_runner import (
    BoundedAnalysisRouteRedisConsumer,
    BoundedAnalysisRouterConfig,
    BoundedAnalysisRouterDatabaseHandle,
    BoundedAnalysisRouterError,
    BoundedAnalysisRouterRedisHandle,
    BoundedAnalysisRouterRuntimeConfig,
    AnalysisRequestOutboxEvent,
    JudgeCallHandoffEvent,
    JudgeCallHandoffReadback,
    run_bounded_analysis_router,
)
from src.services.analysis_router.config import AnalysisRouterConfig
from src.services.analysis_router.models import BundleRouteRecord, BundleShapeStats, CandidateRouteState
from src.services.outbox_relay.bounded_judge_call_requested_outbox_publish_runner import (
    BoundedJudgeCallRequestedOutboxPublishConfig,
    BoundedJudgeCallRequestedPublishRuntimeConfig,
    BoundedJudgeCallRequestedRedisPublisherHandle,
    BoundedJudgeCallRequestedRepositoryHandle,
    JudgeRunLocatorRecord,
    run_bounded_judge_call_requested_outbox_publish,
)
from src.services.outbox_relay.models import OutboxEventRow


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/analysis_router/bounded_analysis_router_runner.py"
DB_URL = "db_locator_omitted_sentinel"
REDIS_URL = "redis_locator_omitted_sentinel"
RAW_PAYLOAD = "sentinel raw business payload"
RAW_PROMPT = "sentinel prompt material"
RAW_MODEL_OUTPUT = "sentinel model output"
RAW_EXCEPTION_DETAIL = "private commit or ack failure detail"
STREAM_ID = "1710000000000-0"


class FakeRedisClient:
    def __init__(
        self,
        entries: list[tuple[str, dict[str, object]]] | None = None,
        *,
        ack_error: BaseException | None = None,
        ack_count: int | None = None,
        order: list[str] | None = None,
        group_exists: bool = True,
        group_pending: int = 0,
        pending_entries: list[dict[str, object]] | None = None,
        pending_snapshots: list[list[dict[str, object]]] | None = None,
        delivered_entries: list[tuple[str, dict[str, object]]] | None = None,
        group_lag: int | None = None,
        group_last_delivered_id: str = "0-0",
    ) -> None:
        self.entries = entries or []
        self.ack_error = ack_error
        self.ack_count = ack_count
        self.order = order
        self.group_exists = group_exists
        self.group_pending = group_pending
        self.pending_entries = pending_entries or []
        self.pending_snapshots = pending_snapshots or []
        self.delivered_entries = delivered_entries if delivered_entries is not None else self.entries
        self.group_lag = len(self.entries) if group_lag is None else group_lag
        self.group_last_delivered_id = group_last_delivered_id
        self.group_created = False
        self.group_start_id: str | None = None
        self.acked: list[str] = []
        self.cursor = 0
        self.range_calls = 0
        self.xinfo_calls = 0
        self.read_calls = 0
        self.xreadgroup_blocks: list[int | None] = []
        self.xpending_range_calls: list[dict[str, object]] = []

    async def xlen(self, name: str) -> int:
        assert name == "q.analysis.route"
        return len(self.entries)

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> None:
        assert name == "q.analysis.route"
        assert groupname == "analysis-router"
        assert id == "0"
        assert mkstream is False
        self.group_created = True
        self.group_start_id = id

    async def xrange(
        self,
        name: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, object]]]:
        assert name == "q.analysis.route"
        assert max == "+"
        self.range_calls += 1
        if min == "-":
            entries = self.entries
        elif min.startswith("("):
            entries = [
                entry
                for entry in self.entries
                if _redis_stream_id_greater(entry[0], min[1:])
            ]
        else:
            raise AssertionError(f"unexpected xrange min: {min}")
        return entries[: count or len(entries)]

    async def xinfo_groups(self, name: str) -> list[dict[str, object]]:
        assert name == "q.analysis.route"
        self.xinfo_calls += 1
        if not self.group_exists:
            return []
        return [
            {
                "name": "analysis-router",
                "pending": self.group_pending,
                "lag": self.group_lag,
                "last-delivered-id": self.group_last_delivered_id,
            }
        ]

    async def xpending_range(
        self,
        name: str,
        groupname: str,
        min: str,
        max: str,
        count: int,
    ) -> list[dict[str, object]]:
        assert name == "q.analysis.route"
        assert groupname == "analysis-router"
        assert min == "-"
        assert max == "+"
        self.xpending_range_calls.append({"count": count})
        if self.pending_snapshots:
            return self.pending_snapshots.pop(0)[:count]
        return self.pending_entries[:count]

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, object]]]]]:
        assert groupname == "analysis-router"
        assert consumername == "bounded-test"
        assert streams == {"q.analysis.route": ">"}
        assert block is None
        self.xreadgroup_blocks.append(block)
        self.read_calls += 1
        if self.cursor >= len(self.delivered_entries):
            return []
        end = min(len(self.delivered_entries), self.cursor + (count or len(self.delivered_entries)))
        batch = self.delivered_entries[self.cursor : end]
        self.cursor = end
        return [("q.analysis.route", batch)]

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        assert name == "q.analysis.route"
        assert groupname == "analysis-router"
        if self.order is not None:
            self.order.append("redis:ack")
        if self.ack_error is not None:
            raise self.ack_error
        self.acked.extend(ids)
        return self.ack_count if self.ack_count is not None else len(ids)


class FakeRedisBuilder:
    def __init__(self, client: FakeRedisClient) -> None:
        self.client = client
        self.calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        consumer = BoundedAnalysisRouteRedisConsumer(
            self.client,
            queue_name="q.analysis.route",
            consumer_group="analysis-router",
            consumer_name="bounded-test",
        )

        async def close() -> None:
            return None

        return BoundedAnalysisRouterRedisHandle(consumer=consumer, close=close)


class FakeRepository:
    def __init__(
        self,
        *,
        event: AnalysisRequestOutboxEvent | None,
        candidate_state: CandidateRouteState | None,
        bundle: BundleRouteRecord | None,
        shape: BundleShapeStats | None = None,
        existing_judge_run_id: UUID | None = None,
        fail_fetch_event: BaseException | None = None,
        fail_get_or_create: BaseException | None = None,
        fail_insert_outbox: BaseException | None = None,
        handoff_readbacks: list[JudgeCallHandoffReadback] | None = None,
        fail_handoff_readback: BaseException | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.event = event
        self.candidate_state = candidate_state
        self.bundle = bundle
        self.shape = shape or BundleShapeStats(member_count=1, supporting_count=0)
        self.existing_judge_run_id = existing_judge_run_id
        self.fail_fetch_event = fail_fetch_event
        self.fail_get_or_create = fail_get_or_create
        self.fail_insert_outbox = fail_insert_outbox
        self.handoff_readbacks = handoff_readbacks or []
        self.fail_handoff_readback = fail_handoff_readback
        self.order = order
        self.fetch_event_calls: list[str] = []
        self.candidate_state_calls: list[str] = []
        self.bundle_calls: list[str] = []
        self.shape_calls: list[str] = []
        self.get_or_create_calls: list[dict[str, object]] = []
        self.outbox_calls: list[dict[str, object]] = []
        self.created_judge_run_id = uuid4()
        self.judge_call_event_id = uuid4()
        self.handoff_readback_calls: list[UUID] = []

    async def fetch_analysis_request_event(self, trigger_event_id: str):
        self.fetch_event_calls.append(trigger_event_id)
        if self.fail_fetch_event is not None:
            raise self.fail_fetch_event
        return self.event if self.event and str(self.event.event_id) == trigger_event_id else None

    async def load_candidate_route_state(self, candidate_group_id: str):
        self.candidate_state_calls.append(candidate_group_id)
        return self.candidate_state

    async def load_bundle(self, bundle_id: str):
        self.bundle_calls.append(bundle_id)
        return self.bundle

    async def load_bundle_shape_stats(self, bundle_id: str):
        self.shape_calls.append(bundle_id)
        return self.shape

    async def load_existing_judge_run(self, **kwargs):
        return self.existing_judge_run_id

    async def get_or_create_judge_run(self, **kwargs):
        if self.order is not None:
            self.order.append("db:get_or_create")
        if self.fail_get_or_create is not None:
            raise self.fail_get_or_create
        self.get_or_create_calls.append(kwargs)
        if self.existing_judge_run_id is not None:
            return self.existing_judge_run_id, False
        return self.created_judge_run_id, True

    async def insert_judge_call_requested_outbox(self, **kwargs) -> None:
        if self.order is not None:
            self.order.append("db:outbox")
        if self.fail_insert_outbox is not None:
            raise self.fail_insert_outbox
        self.outbox_calls.append(kwargs)

    async def read_judge_call_handoff(self, *, judge_run_id: UUID) -> JudgeCallHandoffReadback:
        if self.order is not None:
            self.order.append("db:handoff_readback")
        self.handoff_readback_calls.append(judge_run_id)
        if self.fail_handoff_readback is not None:
            raise self.fail_handoff_readback
        if self.handoff_readbacks:
            return self.handoff_readbacks.pop(0)
        assert self.event is not None
        payload = self.event.payload_json
        return JudgeCallHandoffReadback(
            judge_run_id=judge_run_id,
            bundle_id=UUID(str(payload["bundle_id"])),
            events=(
                JudgeCallHandoffEvent(
                    event_id=self.judge_call_event_id,
                    event_type="judge.call.requested.v1",
                    aggregate_type="judge_run",
                    aggregate_id=judge_run_id,
                    payload_json={
                        "judge_run_id": str(judge_run_id),
                        "candidate_group_id": str(payload["candidate_group_id"]),
                        "bundle_id": str(payload["bundle_id"]),
                    },
                    status="pending",
                    dedupe_key=f"judge-call:{judge_run_id}",
                ),
            ),
        )


class FakeDatabaseBuilder:
    def __init__(
        self,
        repository: FakeRepository,
        *,
        close_error: BaseException | None = None,
        commit_error: BaseException | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.repository = repository
        self.close_error = close_error
        self.commit_error = commit_error
        self.order = order
        self.calls = 0
        self.close_commits: list[bool] = []
        self.commit_calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True

        async def commit() -> None:
            self.commit_calls += 1
            state.database_commit_attempted = True
            if self.order is not None:
                self.order.append("db:commit")
            if self.commit_error is not None:
                raise self.commit_error
            state.database_commit_succeeded = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            if self.close_error is not None:
                raise self.close_error

        return BoundedAnalysisRouterDatabaseHandle(repository=self.repository, commit=commit, close=close)


class RaisingDatabaseBuilder:
    calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, state, logger
        self.calls += 1
        raise AssertionError("database builder must not be called")


def _runtime_config(*, escalation: bool = False) -> BoundedAnalysisRouterRuntimeConfig:
    return BoundedAnalysisRouterRuntimeConfig(router_config=_router_config(escalation=escalation))


def _missing_runtime_config() -> BoundedAnalysisRouterRuntimeConfig:
    raise BoundedAnalysisRouterError("database_url_missing")


def _raising_runtime_config() -> BoundedAnalysisRouterRuntimeConfig:
    raise AssertionError("runtime config must not be loaded")


def _router_config(*, escalation: bool = False) -> AnalysisRouterConfig:
    return AnalysisRouterConfig(
        app_env="test",
        database_url=DB_URL,
        redis_url=REDIS_URL,
        queue_name="q.analysis.route",
        consumer_group="analysis-router",
        consumer_name="analysis-router-test",
        batch_size=10,
        block_ms=100,
        enable_model_escalation=escalation,
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


def _approved_config(**overrides) -> BoundedAnalysisRouterConfig:
    values = {
        "mode": "execute",
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_redis_read": True,
        "allow_database_read": True,
        "allow_redis_consume": True,
        "allow_database_write": True,
        "allow_redis_ack": True,
        "allow_unrelated_pending_preservation": False,
        "trigger_event_id": uuid4(),
        "trigger_event_suffix": None,
        "redis_message_id": None,
        "max_messages": 1,
        "scan_limit": 25,
    }
    values.update(overrides)
    return BoundedAnalysisRouterConfig(**values)


def _payload(candidate_group_id: UUID, bundle_id: UUID, *, judge_profile: str = "github_primary") -> dict[str, object]:
    return {
        "candidate_group_id": str(candidate_group_id),
        "bundle_id": str(bundle_id),
        "judge_profile": judge_profile,
        "escalation_allowed": False,
        "private_bundle_data": RAW_PAYLOAD,
        "prompt_material": RAW_PROMPT,
        "model_output": RAW_MODEL_OUTPUT,
    }


def _event(
    event_id: UUID,
    candidate_group_id: UUID,
    bundle_id: UUID,
    *,
    status: str = "published",
    event_type: str = "analysis.requested.v1",
    aggregate_type: str = "candidate_group",
    payload_json: dict[str, object] | None = None,
) -> AnalysisRequestOutboxEvent:
    return AnalysisRequestOutboxEvent(
        event_id=event_id,
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=candidate_group_id,
        payload_json=payload_json if payload_json is not None else _payload(candidate_group_id, bundle_id),
        status=status,
        dedupe_key="private-dedupe-key",
        created_at=datetime.now(timezone.utc),
    )


def _candidate_state(candidate_group_id: UUID, bundle_id: UUID | None) -> CandidateRouteState:
    return CandidateRouteState(
        candidate_group_id=str(candidate_group_id),
        current_bundle_id=str(bundle_id) if bundle_id is not None else None,
    )


def _bundle(bundle_id: UUID, candidate_group_id: UUID, *, ready: bool = True) -> BundleRouteRecord:
    return BundleRouteRecord(
        bundle_id=str(bundle_id),
        candidate_group_id=str(candidate_group_id),
        bundle_profile_version="bundle_profile_v1",
        reroot_count=0,
        ready_for_analysis=ready,
        token_budget_profile="small",
    )


def _thin_fields(event_id: UUID, candidate_group_id: UUID, **overrides: object) -> dict[str, object]:
    fields: dict[str, object] = {
        "job_id": str(event_id),
        "stage_name": "analysis_route",
        "root_object_type": "candidate_group",
        "root_object_id": str(candidate_group_id),
        "idempotency_key": "private-idempotency-key",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
    }
    fields.update(overrides)
    return fields


def _handoff(
    judge_run_id: UUID,
    candidate_group_id: UUID,
    bundle_id: UUID,
    *,
    status: str = "pending",
    aggregate_id: UUID | None = None,
    payload_candidate_group_id: UUID | None = None,
    payload_bundle_id: UUID | None = None,
    event_id: UUID | None = None,
    events: tuple[JudgeCallHandoffEvent, ...] | None = None,
) -> JudgeCallHandoffReadback:
    if events is not None:
        return JudgeCallHandoffReadback(
            judge_run_id=judge_run_id,
            bundle_id=bundle_id,
            events=events,
        )
    return JudgeCallHandoffReadback(
        judge_run_id=judge_run_id,
        bundle_id=bundle_id,
        events=(
            JudgeCallHandoffEvent(
                event_id=event_id or uuid4(),
                event_type="judge.call.requested.v1",
                aggregate_type="judge_run",
                aggregate_id=aggregate_id or judge_run_id,
                payload_json={
                    "judge_run_id": str(judge_run_id),
                    "candidate_group_id": str(payload_candidate_group_id or candidate_group_id),
                    "bundle_id": str(payload_bundle_id or bundle_id),
                },
                status=status,
                dedupe_key=f"judge-call:{judge_run_id}",
            ),
        ),
    )


def _redis_stream_id_greater(left: str, right: str) -> bool:
    left_key = tuple(int(part) for part in left.split("-", 1))
    right_key = tuple(int(part) for part in right.split("-", 1))
    return left_key > right_key


def _success_parts(*, existing_judge_run_id: UUID | None = None, order: list[str] | None = None):
    event_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeRepository(
        event=_event(event_id, candidate_group_id, bundle_id),
        candidate_state=_candidate_state(candidate_group_id, bundle_id),
        bundle=_bundle(bundle_id, candidate_group_id),
        shape=BundleShapeStats(member_count=1, supporting_count=0),
        existing_judge_run_id=existing_judge_run_id,
        order=order,
    )
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, candidate_group_id))], order=order)
    config = _approved_config(trigger_event_id=event_id)
    return event_id, candidate_group_id, bundle_id, repository, redis, config


@pytest.mark.asyncio
async def test_no_flags_block_before_runtime_config_redis_db_or_ack() -> None:
    redis_builder = FakeRedisBuilder(FakeRedisClient())
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_analysis_router(
        BoundedAnalysisRouterConfig(),
        runtime_config_loader=_raising_runtime_config,
        redis_builder=redis_builder,
        database_builder=database_builder,
    )
    report = result.to_sanitized_dict()

    assert report["status"] == "blocked"
    assert report["error_code"] == "operator_approval_missing"
    assert report["side_effects"]["redis_consume_called"] is False
    assert report["redis_ack_attempted"] is False
    assert report["database_write_attempted"] is False
    assert redis_builder.calls == 0
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_missing_runtime_config_blocks_before_redis_db_or_ack() -> None:
    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=uuid4()),
        runtime_config_loader=_missing_runtime_config,
        redis_builder=FakeRedisBuilder(FakeRedisClient()),
        database_builder=RaisingDatabaseBuilder(),
    )

    assert result.status == "blocked"
    assert result.error_code == "database_url_missing"
    assert result.state.redis_consume_attempted is False
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False


@pytest.mark.asyncio
async def test_missing_read_gates_block_before_runtime_config_redis_db_or_ack() -> None:
    for overrides, expected_error in (
        ({"allow_redis_read": False}, "redis_read_not_allowed"),
        ({"allow_database_read": False}, "database_read_not_allowed"),
    ):
        redis_builder = FakeRedisBuilder(FakeRedisClient())
        database_builder = RaisingDatabaseBuilder()

        result = await run_bounded_analysis_router(
            _approved_config(**overrides),
            runtime_config_loader=_raising_runtime_config,
            redis_builder=redis_builder,
            database_builder=database_builder,
        )

        assert result.error_code == expected_error
        assert result.state.runtime_config_loaded is False
        assert result.state.redis_read_attempted is False
        assert result.state.redis_consume_attempted is False
        assert result.state.database_read_attempted is False
        assert result.state.database_write_attempted is False
        assert result.state.redis_ack_attempted is False
        assert redis_builder.calls == 0
        assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_preview_reports_create_without_db_write_consume_or_ack() -> None:
    event_id, _candidate_group_id, _bundle_id, repository, redis, _config = _success_parts()

    result = await run_bounded_analysis_router(
        _approved_config(
            mode="preview",
            trigger_event_id=event_id,
            allow_redis_consume=False,
            allow_database_write=False,
            allow_redis_ack=False,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert result.status == "preview"
    assert report["target_message_found"] is True
    assert report["analysis_request_event_found"] is True
    assert report["request_current"] is True
    assert report["bundle_ready"] is True
    assert report["existing_judge_run_found"] is False
    assert report["planned_action"] == "create_judge_run"
    assert result.state.redis_read_attempted is True
    assert result.state.redis_consume_attempted is False
    assert result.state.database_read_attempted is True
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False
    assert repository.get_or_create_calls == []
    assert repository.outbox_calls == []
    assert redis.range_calls == 2
    assert redis.read_calls == 0
    assert redis.acked == []


@pytest.mark.asyncio
async def test_preview_reports_existing_judge_run_reuse_without_duplicate_outbox() -> None:
    existing_judge_run_id = uuid4()
    event_id, _candidate_group_id, _bundle_id, repository, redis, _config = _success_parts(
        existing_judge_run_id=existing_judge_run_id
    )

    result = await run_bounded_analysis_router(
        _approved_config(
            mode="preview",
            trigger_event_id=event_id,
            allow_redis_consume=False,
            allow_database_write=False,
            allow_redis_ack=False,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )

    assert result.ok is True
    assert result.status == "preview"
    assert result.existing_judge_run_found is True
    assert result.planned_action == "reuse_existing_judge_run"
    assert repository.get_or_create_calls == []
    assert repository.outbox_calls == []
    assert redis.acked == []


@pytest.mark.asyncio
async def test_preview_reports_stale_request_as_noop_without_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeRepository(
        event=_event(event_id, candidate_group_id, bundle_id),
        candidate_state=_candidate_state(candidate_group_id, uuid4()),
        bundle=_bundle(bundle_id, candidate_group_id),
    )
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, candidate_group_id))])

    result = await run_bounded_analysis_router(
        _approved_config(
            mode="preview",
            trigger_event_id=event_id,
            allow_redis_consume=False,
            allow_database_write=False,
            allow_redis_ack=False,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )

    assert result.ok is True
    assert result.status == "preview"
    assert result.request_current is False
    assert result.bundle_ready is True
    assert result.planned_action == "noop"
    assert result.state.database_write_attempted is False
    assert result.state.redis_consume_attempted is False
    assert result.state.redis_ack_attempted is False
    assert repository.get_or_create_calls == []
    assert redis.acked == []


@pytest.mark.asyncio
async def test_candidate_group_suffix_refines_target_selection_before_db() -> None:
    event_id = uuid4()
    wrong_candidate_group_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeRepository(
        event=_event(event_id, candidate_group_id, bundle_id),
        candidate_state=_candidate_state(candidate_group_id, bundle_id),
        bundle=_bundle(bundle_id, candidate_group_id),
    )
    redis = FakeRedisClient(
        [
            ("100-0", _thin_fields(event_id, wrong_candidate_group_id)),
            (STREAM_ID, _thin_fields(event_id, candidate_group_id)),
        ]
    )

    result = await run_bounded_analysis_router(
        _approved_config(
            mode="preview",
            trigger_event_suffix=str(event_id)[-8:],
            trigger_event_id=None,
            candidate_group_suffix=str(candidate_group_id)[-8:],
            allow_redis_consume=False,
            allow_database_write=False,
            allow_redis_ack=False,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )

    assert result.ok is True
    assert result.messages_seen == 2
    assert result.messages_matched == 1
    assert result.target_candidate_group_suffix == str(candidate_group_id)[-8:]
    assert result.planned_action == "create_judge_run"
    assert redis.acked == []


def test_cli_full_selectors_fail_closed_without_echoing_raw_values() -> None:
    from tools import bounded_analysis_router_job_runner as cli

    full_trigger_event_id = str(uuid4())
    full_redis_message_id = "1710000000000-0"
    cases = (
        (
            ["--trigger-event-id", full_trigger_event_id],
            "full_trigger_event_id_selector_not_allowed",
            full_trigger_event_id,
        ),
        (
            ["--redis-message-id", full_redis_message_id],
            "full_redis_message_id_selector_not_allowed",
            full_redis_message_id,
        ),
    )

    for argv, expected_error, forbidden_value in cases:
        result = cli.run(cli.build_parser().parse_args(argv))
        rendered = json.dumps(result.report, sort_keys=True)

        assert result.exit_code == 1
        assert result.report["error_code"] == expected_error
        assert forbidden_value not in rendered


@pytest.mark.asyncio
async def test_preview_reports_group_preflight_fields_without_consume_or_ack() -> None:
    event_id, _candidate_group_id, _bundle_id, repository, redis, _config = _success_parts()

    result = await run_bounded_analysis_router(
        _approved_config(
            mode="preview",
            trigger_event_suffix=str(event_id)[-8:],
            trigger_event_id=None,
            allow_redis_consume=False,
            allow_database_write=False,
            allow_redis_ack=False,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )
    report = result.to_sanitized_dict()

    assert report["group_name"] == "analysis-router"
    assert report["group_exists"] is True
    assert report["group_pending"] == 0
    assert report["group_lag"] == 1
    assert report["group_last_delivered_id_suffix"] == "0-0"
    assert report["target_after_group_last_delivered"] is True
    assert report["target_is_next_deliverable"] is True
    assert redis.xinfo_calls == 1
    assert redis.read_calls == 0
    assert redis.acked == []


@pytest.mark.asyncio
async def test_target_behind_non_target_unread_blocks_before_xreadgroup_db_write_or_ack() -> None:
    target_event_id = uuid4()
    target_candidate_group_id = uuid4()
    redis = FakeRedisClient(
        [
            ("100-0", _thin_fields(uuid4(), uuid4())),
            ("101-0", _thin_fields(target_event_id, target_candidate_group_id)),
        ]
    )
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=target_event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    report = result.to_sanitized_dict()

    assert result.error_code == "target_not_next_deliverable"
    assert report["target_after_group_last_delivered"] is True
    assert report["target_is_next_deliverable"] is False
    assert result.state.redis_consume_attempted is False
    assert result.state.redis_ack_attempted is False
    assert result.state.database_write_attempted is False
    assert redis.read_calls == 0
    assert redis.acked == []
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_group_missing_blocks_before_xreadgroup_db_write_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    redis = FakeRedisClient(
        [("100-0", _thin_fields(event_id, candidate_group_id))],
        group_exists=False,
    )
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.error_code == "redis_consumer_group_missing"
    assert result.to_sanitized_dict()["group_exists"] is False
    assert result.state.redis_consume_attempted is False
    assert result.state.redis_ack_attempted is False
    assert result.state.database_write_attempted is False
    assert redis.read_calls == 0
    assert redis.acked == []
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_group_pending_nonzero_blocks_before_xreadgroup_db_write_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    redis = FakeRedisClient(
        [("100-0", _thin_fields(event_id, candidate_group_id))],
        group_pending=1,
    )
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    report = result.to_sanitized_dict()

    assert result.error_code == "redis_consumer_group_pending_nonzero"
    assert report["group_pending"] == 1
    assert report["target_is_next_deliverable"] is False
    assert result.state.redis_consume_attempted is False
    assert result.state.redis_ack_attempted is False
    assert result.state.database_write_attempted is False
    assert redis.read_calls == 0
    assert redis.xpending_range_calls == []
    assert redis.acked == []
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_target_not_after_last_delivered_blocks_before_xreadgroup_db_write_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    redis = FakeRedisClient(
        [("100-0", _thin_fields(event_id, candidate_group_id))],
        group_last_delivered_id="100-0",
    )
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    report = result.to_sanitized_dict()

    assert result.error_code == "target_not_after_group_last_delivered"
    assert report["target_after_group_last_delivered"] is False
    assert report["target_is_next_deliverable"] is False
    assert result.state.redis_consume_attempted is False
    assert result.state.redis_ack_attempted is False
    assert result.state.database_write_attempted is False
    assert redis.read_calls == 0
    assert redis.acked == []
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_target_not_next_deliverable_blocks_before_xreadgroup_db_write_or_ack() -> None:
    target_event_id = uuid4()
    target_candidate_group_id = uuid4()
    redis = FakeRedisClient(
        [
            ("100-0", _thin_fields(uuid4(), uuid4())),
            ("101-0", _thin_fields(uuid4(), uuid4())),
            ("102-0", _thin_fields(target_event_id, target_candidate_group_id)),
        ],
        group_last_delivered_id="100-0",
    )
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=target_event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    report = result.to_sanitized_dict()

    assert result.error_code == "target_not_next_deliverable"
    assert report["target_after_group_last_delivered"] is True
    assert report["target_is_next_deliverable"] is False
    assert result.state.redis_consume_attempted is False
    assert result.state.redis_ack_attempted is False
    assert result.state.database_write_attempted is False
    assert redis.read_calls == 0
    assert redis.acked == []
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_job_id_must_equal_trigger_event_id_blocks_before_db_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    redis = FakeRedisClient(
        [("100-0", _thin_fields(event_id, candidate_group_id, job_id=str(uuid4())))]
    )
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.error_code == "job_id_trigger_event_id_mismatch"
    assert result.state.redis_consume_attempted is False
    assert result.state.redis_ack_attempted is False
    assert result.state.database_write_attempted is False
    assert redis.xinfo_calls == 0
    assert redis.read_calls == 0
    assert redis.acked == []
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_job_id_must_be_valid_uuid_blocks_before_db_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    redis = FakeRedisClient(
        [("100-0", _thin_fields(event_id, candidate_group_id, job_id="not-a-uuid"))]
    )
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.error_code == "job_id_invalid"
    assert result.state.redis_consume_attempted is False
    assert result.state.redis_ack_attempted is False
    assert result.state.database_write_attempted is False
    assert redis.xinfo_calls == 0
    assert redis.read_calls == 0
    assert redis.acked == []
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_redis_scan_with_fewer_entries_than_scan_limit_returns_without_hanging() -> None:
    target_event_id = uuid4()
    redis = FakeRedisClient([("100-0", _thin_fields(uuid4(), uuid4()))])
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=target_event_id, scan_limit=5),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.error_code == "target_message_not_found"
    assert result.messages_seen == 1
    assert redis.range_calls == 1
    assert redis.read_calls == 0
    assert redis.xreadgroup_blocks == []
    assert redis.acked == []
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_non_target_messages_are_not_acked() -> None:
    target_event_id = uuid4()
    redis = FakeRedisClient([("100-0", _thin_fields(uuid4(), uuid4()))])

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=target_event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=RaisingDatabaseBuilder(),
    )

    assert result.error_code == "target_message_not_found"
    assert result.messages_seen == 1
    assert result.messages_matched == 0
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []


@pytest.mark.asyncio
async def test_duplicate_matching_target_blocks_before_db_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    redis = FakeRedisClient(
        [
            ("100-0", _thin_fields(event_id, candidate_group_id)),
            ("101-0", _thin_fields(event_id, candidate_group_id)),
        ]
    )
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_suffix=str(event_id)[-8:], trigger_event_id=None),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.error_code == "duplicate_target_message"
    assert result.messages_matched == 2
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False
    assert database_builder.calls == 0
    assert redis.acked == []


@pytest.mark.asyncio
async def test_redis_message_with_business_payload_blocks_before_db_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    redis = FakeRedisClient(
        [
            (
                STREAM_ID,
                _thin_fields(
                    event_id,
                    candidate_group_id,
                    payload_json={"private": RAW_PAYLOAD},
                    bundle_id=str(uuid4()),
                    prompt_material=RAW_PROMPT,
                ),
            )
        ]
    )

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=RaisingDatabaseBuilder(),
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.error_code == "redis_message_business_payload"
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False
    assert RAW_PAYLOAD not in rendered
    assert RAW_PROMPT not in rendered
    assert str(event_id) not in rendered
    assert str(candidate_group_id) not in rendered
    assert STREAM_ID not in rendered


@pytest.mark.asyncio
async def test_wrong_stage_or_root_object_type_blocks_before_db_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    for overrides, expected_error in (
        ({"stage_name": "judge"}, "stage_not_allowed"),
        ({"root_object_type": "bundle"}, "root_object_type_not_allowed"),
    ):
        redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, candidate_group_id, **overrides))])
        database_builder = RaisingDatabaseBuilder()

        result = await run_bounded_analysis_router(
            _approved_config(trigger_event_id=event_id),
            runtime_config_loader=_runtime_config,
            redis_builder=FakeRedisBuilder(redis),
            database_builder=database_builder,
        )

        assert result.error_code == expected_error
        assert result.state.database_write_attempted is False
        assert result.state.redis_ack_attempted is False
        assert database_builder.calls == 0
        assert redis.acked == []


@pytest.mark.asyncio
async def test_event_outbox_missing_blocks_before_db_write_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    repository = FakeRepository(event=None, candidate_state=None, bundle=None)
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, candidate_group_id))])

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )

    assert result.error_code == "event_outbox_missing"
    assert repository.fetch_event_calls == [str(event_id)]
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []


@pytest.mark.asyncio
async def test_event_outbox_not_published_blocks_before_judge_run_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeRepository(
        event=_event(event_id, candidate_group_id, bundle_id, status="pending"),
        candidate_state=_candidate_state(candidate_group_id, bundle_id),
        bundle=_bundle(bundle_id, candidate_group_id),
    )
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, candidate_group_id))])

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )

    assert result.error_code == "event_outbox_not_published"
    assert repository.get_or_create_calls == []
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []


@pytest.mark.asyncio
async def test_stale_bundle_blocks_before_judge_run_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeRepository(
        event=_event(event_id, candidate_group_id, bundle_id),
        candidate_state=_candidate_state(candidate_group_id, uuid4()),
        bundle=_bundle(bundle_id, candidate_group_id),
    )
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, candidate_group_id))])

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )

    assert result.error_code == "stale_bundle_request"
    assert repository.get_or_create_calls == []
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []


@pytest.mark.asyncio
async def test_not_ready_bundle_blocks_before_judge_run_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeRepository(
        event=_event(event_id, candidate_group_id, bundle_id),
        candidate_state=_candidate_state(candidate_group_id, bundle_id),
        bundle=_bundle(bundle_id, candidate_group_id, ready=False),
    )
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, candidate_group_id))])

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )

    assert result.error_code == "bundle_not_ready"
    assert repository.get_or_create_calls == []
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []


@pytest.mark.asyncio
async def test_missing_bundle_members_blocks_before_judge_run_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeRepository(
        event=_event(event_id, candidate_group_id, bundle_id),
        candidate_state=_candidate_state(candidate_group_id, bundle_id),
        bundle=_bundle(bundle_id, candidate_group_id),
        shape=BundleShapeStats(member_count=0, supporting_count=0),
    )
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, candidate_group_id))])

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )

    assert result.error_code == "bundle_members_missing"
    assert repository.get_or_create_calls == []
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []


@pytest.mark.asyncio
async def test_successful_fake_backed_run_creates_one_judge_run_one_outbox_and_acks() -> None:
    event_id, candidate_group_id, bundle_id, repository, redis, config = _success_parts()

    result = await run_bounded_analysis_router(
        config,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert result.status == "routed"
    assert report["target_trigger_event_id_suffix"] == str(event_id)[-8:]
    assert report["target_candidate_group_suffix"] == str(candidate_group_id)[-8:]
    assert report["target_bundle_id_suffix"] == str(bundle_id)[-8:]
    assert result.counters.judge_runs_written_count == 1
    assert result.counters.existing_judge_run_reused_count == 0
    assert result.counters.judge_call_requested_outbox_count == 1
    assert len(repository.get_or_create_calls) == 1
    assert len(repository.outbox_calls) == 1
    assert redis.acked == [STREAM_ID]
    assert result.redis_ack_status == "acked"
    assert result.redis_acked_count == 1
    assert report["group_exists"] is True
    assert report["group_pending"] == 0
    assert report["target_after_group_last_delivered"] is True
    assert report["target_is_next_deliverable"] is True
    assert redis.xinfo_calls == 1
    assert redis.read_calls == 1
    assert report["judge_call_handoff_found"] is True
    assert report["judge_call_handoff_ready"] is True
    assert report["judge_call_event_status"] == "pending"
    assert report["judge_call_event_id_suffix"] == str(repository.judge_call_event_id)[-8:]
    assert report["judge_run_id_suffix"] == str(repository.created_judge_run_id)[-8:]


@pytest.mark.asyncio
async def test_string_db_contract_ids_route_successfully_and_ack_selected_message() -> None:
    order: list[str] = []
    event_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeRepository(
        event=_event(event_id, candidate_group_id, bundle_id),
        candidate_state=CandidateRouteState(
            candidate_group_id=str(candidate_group_id),
            current_bundle_id=str(bundle_id),
        ),
        bundle=BundleRouteRecord(
            bundle_id=str(bundle_id),
            candidate_group_id=str(candidate_group_id),
            bundle_profile_version="bundle_profile_v1",
            reroot_count=0,
            ready_for_analysis=True,
            token_budget_profile="small",
        ),
        shape=BundleShapeStats(member_count=1, supporting_count=0),
        order=order,
    )
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, candidate_group_id))], order=order)
    database_builder = FakeDatabaseBuilder(repository, order=order)

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.ok is True
    assert result.status == "routed"
    assert repository.fetch_event_calls == [str(event_id)]
    assert repository.candidate_state_calls == [str(candidate_group_id)]
    assert repository.bundle_calls == [str(bundle_id)]
    assert repository.shape_calls == [str(bundle_id)]
    assert repository.get_or_create_calls[0]["bundle_id"] == str(bundle_id)
    assert repository.outbox_calls[0]["candidate_group_id"] == str(candidate_group_id)
    assert repository.outbox_calls[0]["bundle_id"] == str(bundle_id)
    assert result.counters.judge_runs_written_count == 1
    assert result.counters.judge_call_requested_outbox_count == 1
    assert database_builder.close_commits == [True]
    assert order == ["db:get_or_create", "db:outbox", "db:commit", "db:handoff_readback", "redis:ack"]
    assert redis.acked == [STREAM_ID]


@pytest.mark.asyncio
async def test_successful_fake_backed_run_commits_before_ack() -> None:
    order: list[str] = []
    _event_id, _candidate_group_id, _bundle_id, repository, redis, config = _success_parts(order=order)

    result = await run_bounded_analysis_router(
        config,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository, order=order),
    )

    assert result.ok is True
    assert order == ["db:get_or_create", "db:outbox", "db:commit", "db:handoff_readback", "redis:ack"]


@pytest.mark.asyncio
async def test_existing_judge_run_reuse_does_not_duplicate_outbox_but_acks_after_commit() -> None:
    existing_judge_run_id = uuid4()
    event_id, _candidate_group_id, _bundle_id, repository, redis, config = _success_parts(
        existing_judge_run_id=existing_judge_run_id
    )

    result = await run_bounded_analysis_router(
        config,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )

    assert result.ok is True
    assert result.status == "reused"
    assert result.counters.judge_runs_written_count == 0
    assert result.counters.existing_judge_run_reused_count == 1
    assert result.counters.judge_call_requested_outbox_count == 0
    assert repository.outbox_calls == []
    assert repository.fetch_event_calls == [str(event_id)]
    assert redis.acked == [STREAM_ID]


@pytest.mark.asyncio
async def test_db_commit_failure_does_not_ack() -> None:
    _event_id, _candidate_group_id, _bundle_id, repository, redis, config = _success_parts()
    database_builder = FakeDatabaseBuilder(repository, commit_error=RuntimeError(RAW_EXCEPTION_DETAIL))

    result = await run_bounded_analysis_router(
        config,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is False
    assert result.error_code == "database_write_failed"
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []
    assert database_builder.commit_calls == 1
    assert repository.handoff_readback_calls == []
    assert RAW_EXCEPTION_DETAIL not in rendered
    assert DB_URL not in rendered
    assert REDIS_URL not in rendered


@pytest.mark.asyncio
async def test_db_read_exception_before_judge_run_returns_database_read_failed_without_ack() -> None:
    _event_id, _candidate_group_id, _bundle_id, repository, redis, config = _success_parts()
    repository.fail_fetch_event = RuntimeError(RAW_EXCEPTION_DETAIL)
    database_builder = FakeDatabaseBuilder(repository)

    result = await run_bounded_analysis_router(
        config,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.error_code == "database_read_failed"
    assert result.error_code != "bounded_analysis_router_failed"
    assert result.error_class == "RuntimeError"
    assert result.error_class != "UnboundLocalError"
    assert database_builder.commit_calls == 0
    assert repository.handoff_readback_calls == []
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []
    assert RAW_EXCEPTION_DETAIL not in rendered
    assert DB_URL not in rendered
    assert REDIS_URL not in rendered


@pytest.mark.asyncio
async def test_get_or_create_exception_returns_database_write_failed_without_ack() -> None:
    _event_id, _candidate_group_id, _bundle_id, repository, redis, config = _success_parts()
    repository.fail_get_or_create = RuntimeError(RAW_EXCEPTION_DETAIL)
    database_builder = FakeDatabaseBuilder(repository)

    result = await run_bounded_analysis_router(
        config,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.error_code == "database_write_failed"
    assert result.error_class == "RuntimeError"
    assert result.error_class != "UnboundLocalError"
    assert database_builder.commit_calls == 0
    assert repository.handoff_readback_calls == []
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []
    assert RAW_EXCEPTION_DETAIL not in rendered


@pytest.mark.asyncio
async def test_insert_outbox_exception_returns_database_write_failed_without_ack() -> None:
    _event_id, _candidate_group_id, _bundle_id, repository, redis, config = _success_parts()
    repository.fail_insert_outbox = RuntimeError(RAW_EXCEPTION_DETAIL)
    database_builder = FakeDatabaseBuilder(repository)

    result = await run_bounded_analysis_router(
        config,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.error_code == "database_write_failed"
    assert result.error_class == "RuntimeError"
    assert result.error_class != "UnboundLocalError"
    assert database_builder.commit_calls == 0
    assert repository.handoff_readback_calls == []
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []
    assert RAW_EXCEPTION_DETAIL not in rendered


@pytest.mark.asyncio
async def test_redis_ack_failure_reports_failure_after_db_commit() -> None:
    _event_id, _candidate_group_id, _bundle_id, repository, redis, config = _success_parts()
    redis.ack_error = RuntimeError(RAW_EXCEPTION_DETAIL)
    database_builder = FakeDatabaseBuilder(repository)

    result = await run_bounded_analysis_router(
        config,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.ok is False
    assert result.error_code == "redis_ack_failed"
    assert result.redis_ack_status == "failed"
    assert result.messages_processed_count == 1
    assert database_builder.close_commits == [True]
    assert redis.acked == []
    assert RAW_EXCEPTION_DETAIL not in rendered


@pytest.mark.asyncio
async def test_opt_in_preserves_unrelated_pending_and_reads_durable_handoff_before_exact_ack() -> None:
    order: list[str] = []
    event_id, candidate_group_id, _bundle_id, repository, _redis, _config = _success_parts(order=order)
    unrelated_pending_id = "1709999999999-0"
    redis = FakeRedisClient(
        [(STREAM_ID, _thin_fields(event_id, candidate_group_id))],
        group_pending=1,
        pending_entries=[{"message_id": unrelated_pending_id}],
        order=order,
    )
    database_builder = FakeDatabaseBuilder(repository, order=order)

    result = await run_bounded_analysis_router(
        _approved_config(
            trigger_event_id=event_id,
            allow_unrelated_pending_preservation=True,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )
    report = result.to_sanitized_dict()

    assert result.status == "routed"
    assert report["unrelated_pending_before_count"] == 1
    assert report["unrelated_pending_after_count"] == 1
    assert report["unrelated_pending_preserved"] is True
    assert report["target_pending_before"] is False
    assert report["target_pending_after"] is False
    assert report["pending_inspection_bounded"] is True
    assert report["pending_preflight_confirmed"] is True
    assert report["judge_call_handoff_ready"] is True
    assert report["judge_call_event_id_suffix"] == str(repository.judge_call_event_id)[-8:]
    assert redis.xpending_range_calls == [{"count": 25}, {"count": 25}, {"count": 25}]
    assert redis.read_calls == 1
    assert redis.acked == [STREAM_ID]
    assert order == ["db:get_or_create", "db:outbox", "db:commit", "db:handoff_readback", "redis:ack"]
    assert unrelated_pending_id not in json.dumps(report, sort_keys=True)


@pytest.mark.asyncio
async def test_opt_in_blocks_target_already_pending_before_consume_db_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    redis = FakeRedisClient(
        [(STREAM_ID, _thin_fields(event_id, candidate_group_id))],
        group_pending=1,
        pending_entries=[{"message_id": STREAM_ID}],
    )
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_analysis_router(
        _approved_config(
            trigger_event_id=event_id,
            allow_unrelated_pending_preservation=True,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.error_code == "target_message_already_pending"
    assert result.state.target_pending_before is True
    assert result.state.redis_consume_attempted is False
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False
    assert redis.read_calls == 0
    assert redis.acked == []
    assert database_builder.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("group_pending", "pending_entries", "pending_snapshots", "expected_error"),
    (
        (101, [], None, "redis_pending_scan_limit_exceeded"),
        (2, [{"message_id": "1709999999999-0"}], None, "redis_pending_readback_unconfirmed"),
        (1, [{"message_id": "not-a-redis-stream-id"}], None, "redis_pending_readback_unconfirmed"),
        (
            1,
            [],
            [[{"message_id": "1709999999999-0"}], [{"message_id": "1709999999998-0"}]],
            "redis_pending_readback_unconfirmed",
        ),
    ),
)
async def test_opt_in_blocks_ambiguous_pending_inventory_before_consume_db_or_ack(
    group_pending: int,
    pending_entries: list[dict[str, object]],
    pending_snapshots: list[list[dict[str, object]]] | None,
    expected_error: str,
) -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    redis = FakeRedisClient(
        [(STREAM_ID, _thin_fields(event_id, candidate_group_id))],
        group_pending=group_pending,
        pending_entries=pending_entries,
        pending_snapshots=pending_snapshots,
    )
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_analysis_router(
        _approved_config(
            trigger_event_id=event_id,
            allow_unrelated_pending_preservation=True,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.error_code == expected_error
    assert result.state.redis_consume_attempted is False
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False
    assert redis.read_calls == 0
    assert redis.acked == []
    assert database_builder.calls == 0


@pytest.mark.asyncio
async def test_delivered_message_race_blocks_without_db_write_or_ack() -> None:
    event_id, candidate_group_id, _bundle_id, repository, _redis, _config = _success_parts()
    redis = FakeRedisClient(
        [(STREAM_ID, _thin_fields(event_id, candidate_group_id))],
        delivered_entries=[("1710000000001-0", _thin_fields(uuid4(), uuid4()))],
    )
    database_builder = RaisingDatabaseBuilder()

    result = await run_bounded_analysis_router(
        _approved_config(trigger_event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.error_code == "target_message_not_consumable_exactly"
    assert redis.read_calls == 1
    assert result.state.database_write_attempted is False
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []
    assert database_builder.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    (
        "missing_judge_run",
        "missing_event",
        "wrong_aggregate",
        "wrong_candidate_group",
        "wrong_bundle",
        "duplicate_event",
        "unsupported_status",
    ),
)
async def test_durable_handoff_readback_mismatch_does_not_ack_after_commit(case: str) -> None:
    event_id, candidate_group_id, bundle_id, repository, redis, config = _success_parts()
    judge_run_id = repository.created_judge_run_id
    valid = _handoff(judge_run_id, candidate_group_id, bundle_id)
    event = valid.events[0]
    if case == "missing_judge_run":
        readback = JudgeCallHandoffReadback(judge_run_id=None, bundle_id=None, events=())
    elif case == "missing_event":
        readback = JudgeCallHandoffReadback(judge_run_id=judge_run_id, bundle_id=bundle_id, events=())
    elif case == "wrong_aggregate":
        readback = _handoff(judge_run_id, candidate_group_id, bundle_id, aggregate_id=uuid4())
    elif case == "wrong_candidate_group":
        readback = _handoff(judge_run_id, candidate_group_id, bundle_id, payload_candidate_group_id=uuid4())
    elif case == "wrong_bundle":
        readback = _handoff(judge_run_id, candidate_group_id, bundle_id, payload_bundle_id=uuid4())
    elif case == "duplicate_event":
        duplicate = JudgeCallHandoffEvent(
            event_id=uuid4(),
            event_type=event.event_type,
            aggregate_type=event.aggregate_type,
            aggregate_id=event.aggregate_id,
            payload_json=event.payload_json,
            status=event.status,
            dedupe_key=event.dedupe_key,
        )
        readback = _handoff(judge_run_id, candidate_group_id, bundle_id, events=(event, duplicate))
    else:
        readback = _handoff(judge_run_id, candidate_group_id, bundle_id, status="failed")
    repository.handoff_readbacks = [readback]
    database_builder = FakeDatabaseBuilder(repository)

    result = await run_bounded_analysis_router(
        config,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=database_builder,
    )

    assert result.error_code == "judge_call_handoff_readback_failed"
    assert result.state.database_commit_succeeded is True
    assert database_builder.commit_calls == 1
    assert result.state.redis_ack_attempted is False
    assert redis.acked == []
    assert len(repository.outbox_calls) == 1


@pytest.mark.asyncio
async def test_handoff_readback_retry_reuses_judge_run_without_duplicate_outbox() -> None:
    event_id, candidate_group_id, bundle_id, repository, redis, config = _success_parts()
    judge_run_id = repository.created_judge_run_id
    repository.handoff_readbacks = [
        JudgeCallHandoffReadback(judge_run_id=judge_run_id, bundle_id=bundle_id, events=())
    ]

    first = await run_bounded_analysis_router(
        config,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )
    repository.existing_judge_run_id = judge_run_id
    retry_redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, candidate_group_id))])
    second = await run_bounded_analysis_router(
        config,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(retry_redis),
        database_builder=FakeDatabaseBuilder(repository),
    )

    assert first.error_code == "judge_call_handoff_readback_failed"
    assert first.state.redis_ack_attempted is False
    assert second.status == "reused"
    assert second.judge_call_handoff_ready is True
    assert second.counters.existing_judge_run_reused_count == 1
    assert len(repository.get_or_create_calls) == 2
    assert len(repository.outbox_calls) == 1
    assert retry_redis.acked == [STREAM_ID]


@pytest.mark.asyncio
async def test_existing_judge_run_reuse_with_opt_in_preserves_unrelated_pending() -> None:
    existing_judge_run_id = uuid4()
    event_id, candidate_group_id, _bundle_id, repository, _redis, _config = _success_parts(
        existing_judge_run_id=existing_judge_run_id
    )
    redis = FakeRedisClient(
        [(STREAM_ID, _thin_fields(event_id, candidate_group_id))],
        group_pending=1,
        pending_entries=[{"message_id": "1709999999999-0"}],
    )

    result = await run_bounded_analysis_router(
        _approved_config(
            trigger_event_id=event_id,
            allow_unrelated_pending_preservation=True,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )

    assert result.status == "reused"
    assert result.counters.existing_judge_run_reused_count == 1
    assert result.counters.judge_runs_written_count == 0
    assert repository.outbox_calls == []
    assert result.judge_call_handoff_ready is True
    assert result.state.unrelated_pending_preserved is True
    assert redis.acked == [STREAM_ID]


@pytest.mark.asyncio
async def test_post_ack_pending_preservation_readback_failure_does_not_compensate() -> None:
    event_id, candidate_group_id, _bundle_id, repository, _redis, _config = _success_parts()
    redis = FakeRedisClient(
        [(STREAM_ID, _thin_fields(event_id, candidate_group_id))],
        group_pending=1,
        pending_snapshots=[
            [{"message_id": "1709999999999-0"}],
            [{"message_id": "1709999999999-0"}],
            [{"message_id": "1709999999998-0"}],
        ],
    )

    result = await run_bounded_analysis_router(
        _approved_config(
            trigger_event_id=event_id,
            allow_unrelated_pending_preservation=True,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )

    assert result.error_code == "unrelated_pending_preservation_readback_failed"
    assert result.redis_ack_status == "acked"
    assert result.redis_acked_count == 1
    assert result.state.unrelated_pending_preserved is False
    assert redis.acked == [STREAM_ID]


@pytest.mark.asyncio
async def test_returned_handoff_suffix_is_accepted_by_existing_judge_call_publisher_selector() -> None:
    event_id, candidate_group_id, bundle_id, repository, redis, config = _success_parts()
    router_result = await run_bounded_analysis_router(
        config,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )
    assert router_result.judge_call_event_id_suffix is not None

    judge_run_id = repository.created_judge_run_id
    outbox_event = OutboxEventRow(
        event_id=repository.judge_call_event_id,
        event_type="judge.call.requested.v1",
        aggregate_type="judge_run",
        aggregate_id=judge_run_id,
        dedupe_key=f"judge-call:{judge_run_id}",
        payload_json={
            "judge_run_id": str(judge_run_id),
            "candidate_group_id": str(candidate_group_id),
            "bundle_id": str(bundle_id),
            "model": "gpt-5.4-mini",
            "reasoning_effort": "low",
            "prompt_version": "judge_github_primary_v1",
            "prompt_cache_key": "redacted-test-cache-key",
        },
        status="pending",
        fail_count=0,
        created_at=datetime.now(timezone.utc),
    )

    class PublisherRepository:
        def __init__(self) -> None:
            self.selector_suffix: str | None = None
            self.published: list[UUID] = []
            self.attempts: list[dict[str, object]] = []

        async def fetch_target_events(self, *, trigger_event_id, trigger_event_suffix, limit):
            assert trigger_event_id is None
            assert limit == 2
            self.selector_suffix = trigger_event_suffix
            return [outbox_event] if trigger_event_suffix == router_result.judge_call_event_id_suffix else []

        async def load_judge_run(self, requested_judge_run_id):
            assert requested_judge_run_id == judge_run_id
            return JudgeRunLocatorRecord(
                judge_run_id=judge_run_id,
                bundle_id=bundle_id,
                status="pending",
            )

        async def mark_published(self, *, event_id, published_at=None):
            del published_at
            self.published.append(event_id)

        async def insert_job_attempt(self, **kwargs):
            self.attempts.append(kwargs)

    class Publisher:
        def __init__(self) -> None:
            self.routes: list[object] = []

        async def publish(self, route, message):
            self.routes.append((route, message))
            return "1700000000000-0"

    publisher_repository = PublisherRepository()
    publisher = Publisher()

    async def repository_builder(runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            assert commit is True

        return BoundedJudgeCallRequestedRepositoryHandle(repository=publisher_repository, close=close)

    async def publisher_builder(runtime_config, state, logger):
        del runtime_config, logger
        state.redis_publisher_created = True

        async def close() -> None:
            return None

        return BoundedJudgeCallRequestedRedisPublisherHandle(publisher=publisher, close=close)

    publish_result = await run_bounded_judge_call_requested_outbox_publish(
        BoundedJudgeCallRequestedOutboxPublishConfig(
            operator_approved=True,
            allow_runtime_config=True,
            allow_database_read=True,
            allow_redis_publish=True,
            allow_database_write=True,
            trigger_event_suffix=router_result.judge_call_event_id_suffix,
            max_events=1,
        ),
        runtime_config_loader=lambda: BoundedJudgeCallRequestedPublishRuntimeConfig(
            database_url=DB_URL,
            redis_url=REDIS_URL,
        ),
        repository_builder=repository_builder,
        redis_publisher_builder=publisher_builder,
    )

    assert publish_result.status == "published"
    assert publisher_repository.selector_suffix == router_result.judge_call_event_id_suffix
    assert publisher_repository.published == [repository.judge_call_event_id]
    assert len(publisher.routes) == 1
    route, _message = publisher.routes[0]
    assert route.queue_name == "q.analysis.judge"
    assert route.stage_name == "judge"


def test_source_ast_guard_no_forbidden_authority_or_broad_worker_calls() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    imported_modules = set()
    forbidden_call_names = {"system", "popen", "call", "check_call", "check_output", "run_forever"}
    forbidden_call_attrs = forbidden_call_names | {"xread", "xgroup_create", "sleep"}
    xreadgroup_calls = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                if node.func.attr == "xreadgroup":
                    xreadgroup_calls.append(node)
                assert node.func.attr not in forbidden_call_attrs
            elif isinstance(node.func, ast.Name):
                assert node.func.id not in forbidden_call_names

    assert {"subprocess", "requests", "httpx", "aiohttp", "telegram", "openai"}.isdisjoint(imported_roots)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".evidence_assembler" in module for module in imported_modules)
    assert not any(".router_normalizer" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert not any(".worker" in module for module in imported_modules)
    assert "run_forever(" not in source
    assert "xgroup_create(" not in source
    assert "docker" not in imported_roots
    assert "alembic" not in imported_roots
    for call in xreadgroup_calls:
        for keyword in call.keywords:
            assert not (
                keyword.arg == "block"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value == 0
            )
