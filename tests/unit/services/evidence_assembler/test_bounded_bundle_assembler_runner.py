from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest

from src.services.evidence_assembler import bounded_bundle_assembler_runner as runner_module
from src.services.evidence_assembler.bounded_bundle_assembler_runner import (
    BoundedBundleAssemblerConfig,
    BoundedBundleAssemblerCounters,
    BoundedBundleAssemblerDatabaseHandle,
    BoundedBundleAssemblerError,
    BoundedBundleAssemblerRedisHandle,
    BoundedBundleAssemblerRuntimeConfig,
    ExistingGroupRedisTargetConsumer,
    RedisBundleMessage,
    TargetedRedisBundleMessage,
    TriggerEventContract,
    run_bounded_bundle_assembler,
)
from src.services.evidence_assembler.config import EvidenceAssemblerConfig
from src.services.evidence_assembler.models import AssemblyResult, EvidenceBundlePreview


DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://:sentinel_redis_password@127.0.0.1:6379/0"
RAW_IDEMPOTENCY_KEY = "private-bundle-idempotency-key"
RAW_EXCEPTION_DETAIL = "sentinel private bundle failure detail"
STREAM_ID = "1710000000476-0"
STREAM_ID_SUFFIX = "476-0"


def _runtime_config() -> BoundedBundleAssemblerRuntimeConfig:
    return BoundedBundleAssemblerRuntimeConfig(
        assembler_config=EvidenceAssemblerConfig(
            app_env="test",
            database_url=DB_URL,
            redis_url=REDIS_URL,
            queue_name="q.candidate.bundle",
            consumer_group="evidence-assembler",
            consumer_name="bounded-test",
            batch_size=1,
            block_ms=100,
            bundle_profile_version="bundle_profile_v1",
            enable_text_idea=True,
            enable_reroot=True,
            log_level="INFO",
        )
    )


def _message_fields(event_id: UUID, artifact_id: UUID, **extra) -> dict[str, str]:
    fields = {
        "job_id": str(event_id),
        "stage_name": "bundle",
        "root_object_type": "artifact",
        "root_object_id": str(artifact_id),
        "idempotency_key": RAW_IDEMPOTENCY_KEY,
        "trigger_event_id": str(event_id),
    }
    fields.update(extra)
    return fields


def _candidate_refresh_message_fields(event_id: UUID, candidate_group_id: UUID, **extra) -> dict[str, str]:
    fields = {
        "job_id": str(event_id),
        "stage_name": "bundle",
        "root_object_type": "candidate_group",
        "root_object_id": str(candidate_group_id),
        "idempotency_key": RAW_IDEMPOTENCY_KEY,
        "trigger_event_id": str(event_id),
    }
    fields.update(extra)
    return fields


class FakeConsumer:
    def __init__(
        self,
        selected: TargetedRedisBundleMessage,
        *,
        ack_error: BaseException | None = None,
        messages_seen: int = 1,
        messages_matched: int = 1,
        order: list[str] | None = None,
    ) -> None:
        self.selected = selected
        self.ack_error = ack_error
        self.messages_seen = messages_seen
        self.messages_matched = messages_matched
        self.acked: list[str] = []
        self.order = order if order is not None else []

    async def find_target(self, config, state):
        state.redis_read_attempted = True
        if config.run_mode == "execute":
            state.redis_consume_attempted = True
        return self.selected, self.messages_seen, self.messages_matched

    async def ack(self, message_id: str, state) -> int:
        state.redis_ack_attempted = True
        self.order.append("ack")
        if self.ack_error is not None:
            raise self.ack_error
        self.acked.append(message_id)
        return 1


class FakeRedisBuilder:
    def __init__(self, consumer: FakeConsumer) -> None:
        self.consumer = consumer

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger

        async def close() -> None:
            pass

        return BoundedBundleAssemblerRedisHandle(consumer=self.consumer, close=close)


class FakeDatabase:
    def __init__(
        self,
        *,
        event: TriggerEventContract,
        counters: BoundedBundleAssemblerCounters,
        validate_error: BoundedBundleAssemblerError | None = None,
        suffix_error: BoundedBundleAssemblerError | None = None,
        assemble_error: BaseException | None = None,
        preview_results: list[EvidenceBundlePreview] | None = None,
        run_contract_validation: bool = False,
    ) -> None:
        self.event = event
        self.counters = counters
        self.validate_error = validate_error
        self.suffix_error = suffix_error
        self.assemble_error = assemble_error
        self.preview_results = preview_results
        self.run_contract_validation = run_contract_validation
        self.assembled = False

    async def resolve_trigger_event_suffix(self, trigger_event_suffix: str, state):
        del trigger_event_suffix
        state.trigger_suffix_lookup_attempted = True
        if self.suffix_error is not None:
            raise self.suffix_error
        return self.event.event_id

    async def validate_trigger_event(self, selected, config, state):
        state.event_outbox_read_attempted = True
        state.database_read_attempted = True
        if self.validate_error is not None:
            raise self.validate_error
        if self.run_contract_validation:
            runner_module._validate_trigger_contract(event=self.event, selected=selected, config=config)
        return self.event

    async def preview(self, trigger_event_id, selected_candidate_group_id, state):
        assert trigger_event_id == self.event.event_id
        assert selected_candidate_group_id == self.event.selected_candidate_group_id
        state.database_read_attempted = True
        return self.preview_results or []

    async def assemble(self, trigger_event_id, selected_candidate_group_id, state):
        assert trigger_event_id == self.event.event_id
        assert selected_candidate_group_id == self.event.selected_candidate_group_id
        state.database_write_attempted = True
        if self.assemble_error is not None:
            raise self.assemble_error
        self.assembled = True
        self.counters.bundles_written_count = 1
        self.counters.bundle_members_written_count = 1
        self.counters.current_bundle_updates_count = 1
        self.counters.analysis_requested_outbox_count = 1
        return [
            AssemblyResult(
                candidate_group_id=uuid4(),
                bundle_id=uuid4(),
                reused_existing_bundle=False,
                ready_for_analysis=True,
                emitted_analysis_requested=True,
                analysis_requested_event_id=uuid4(),
            )
        ]


class FakeDatabaseBuilder:
    def __init__(
        self,
        database: FakeDatabase,
        *,
        close_error: BaseException | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.database = database
        self.close_error = close_error
        self.close_commits: list[bool] = []
        self.order = order if order is not None else []

    async def __call__(self, runtime_config, state, logger, fanout_limit):
        del runtime_config, logger, fanout_limit
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            self.order.append("commit" if commit else "rollback")
            if self.close_error is not None:
                raise self.close_error

        return BoundedBundleAssemblerDatabaseHandle(
            database=self.database,
            counters=self.database.counters,
            close=close,
        )


class FakeRedisClient:
    def __init__(
        self,
        entries: list[tuple[str, dict[str, str]]],
        *,
        group_name: str = "evidence-assembler",
        group_pending: int = 0,
        group_lag: int | None = 1,
        group_last_delivered_id: str = "0-0",
        group_exists: bool = True,
        xinfo_error: BaseException | None = None,
    ) -> None:
        self.entries = entries
        self.group_name = group_name
        self.group_pending = group_pending
        self.group_lag = group_lag
        self.group_last_delivered_id = group_last_delivered_id
        self.group_exists = group_exists
        self.xinfo_error = xinfo_error
        self.offset = 0
        self.acked: list[str] = []
        self.xreadgroup_calls: list[dict[str, object]] = []
        self.xinfo_groups_calls: list[str] = []
        self.xrange_calls: list[dict[str, object]] = []

    async def xlen(self, name: str) -> int:
        assert name == "q.candidate.bundle"
        return len(self.entries)

    async def xrange(self, name: str, min: str = "-", max: str = "+", count: int | None = None):
        del min, max
        assert name == "q.candidate.bundle"
        self.xrange_calls.append({"count": count})
        return self.entries[: int(count or len(self.entries))]

    async def xinfo_groups(self, name: str):
        assert name == "q.candidate.bundle"
        self.xinfo_groups_calls.append(name)
        if self.xinfo_error is not None:
            raise self.xinfo_error
        if not self.group_exists:
            return []
        return [
            {
                "name": self.group_name,
                "pending": self.group_pending,
                "lag": self.group_lag,
                "last-delivered-id": self.group_last_delivered_id,
            }
        ]

    async def xreadgroup(self, groupname, consumername, streams, count=None, block=None):
        self.xreadgroup_calls.append(
            {
                "groupname": groupname,
                "consumername": consumername,
                "streams": streams,
                "count": count,
                "block": block,
            }
        )
        assert groupname == "evidence-assembler"
        assert consumername == "bounded-test"
        assert streams == {"q.candidate.bundle": ">"}
        batch = self.entries[self.offset : self.offset + int(count or 1)]
        self.offset += len(batch)
        if not batch and block == 0:
            await asyncio.Event().wait()
        return [("q.candidate.bundle", batch)] if batch else []

    async def xack(self, name: str, groupname: str, *ids: str):
        del groupname
        assert name == "q.candidate.bundle"
        self.acked.extend(ids)
        return len(ids)


class FakeExistingGroupRedisBuilder:
    def __init__(self, client: FakeRedisClient) -> None:
        self.client = client
        self.consumer: ExistingGroupRedisTargetConsumer | None = None

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.consumer = ExistingGroupRedisTargetConsumer(
            self.client,
            queue_name="q.candidate.bundle",
            group_name="evidence-assembler",
            consumer_name="bounded-test",
        )

        async def close() -> None:
            pass

        return BoundedBundleAssemblerRedisHandle(consumer=self.consumer, close=close)


def _selected(event_id: UUID, artifact_id: UUID, fields: dict[str, str] | None = None) -> TargetedRedisBundleMessage:
    message_fields = fields or _message_fields(event_id, artifact_id)
    return TargetedRedisBundleMessage(
        redis_message_id=STREAM_ID,
        fields=message_fields,
        message=RedisBundleMessage.from_stream_fields(message_fields),
    )


def _selected_candidate_refresh(
    event_id: UUID,
    candidate_group_id: UUID,
    fields: dict[str, str] | None = None,
) -> TargetedRedisBundleMessage:
    message_fields = fields or _candidate_refresh_message_fields(event_id, candidate_group_id)
    return TargetedRedisBundleMessage(
        redis_message_id=STREAM_ID,
        fields=message_fields,
        message=RedisBundleMessage.from_stream_fields(message_fields),
    )


def _event(
    event_id: UUID,
    artifact_id: UUID,
    *,
    impacted_count: int = 1,
    snapshot_type: str | None = "web_article",
    missing_payload_fields: set[str] | None = None,
) -> TriggerEventContract:
    snapshot_id = uuid4()
    payload = {
        "artifact_id": str(artifact_id),
        "snapshot_id": str(snapshot_id),
        "provider": "web",
        "status": "low_evidence",
        "content_anchor": "private-content-anchor",
    }
    if snapshot_type is not None:
        payload["snapshot_type"] = snapshot_type
    for field_name in missing_payload_fields or set():
        payload.pop(field_name, None)
    return TriggerEventContract(
        event_id=event_id,
        event_type="artifact.snapshot.updated.v1",
        status="published",
        aggregate_type="artifact",
        aggregate_id=artifact_id,
        payload_json=payload,
        snapshot_id=snapshot_id,
        snapshot_type=snapshot_type or "",
        snapshot_status="low_evidence",
        content_anchor_present=True,
        impacted_candidate_group_count=impacted_count,
        selected_candidate_group_id=uuid4(),
    )


def _candidate_refresh_event(
    event_id: UUID,
    candidate_group_id: UUID,
    *,
    aggregate_type: str = "candidate_group",
    payload_candidate_group_id: UUID | None = None,
    missing_payload_fields: set[str] | None = None,
) -> TriggerEventContract:
    payload = {
        "candidate_group_id": str(payload_candidate_group_id or candidate_group_id),
        "trigger_kind": "bounded_refresh",
        "trigger_object_type": "artifact",
        "trigger_object_id": str(uuid4()),
        "refresh_reason": "bounded_candidate_bundle_refresh",
    }
    for field_name in missing_payload_fields or set():
        payload.pop(field_name, None)
    return TriggerEventContract(
        event_id=event_id,
        event_type="candidate.bundle.refresh.v1",
        status="published",
        aggregate_type=aggregate_type,
        aggregate_id=candidate_group_id,
        payload_json=payload,
        snapshot_id=UUID(int=0),
        snapshot_type="",
        snapshot_status="",
        content_anchor_present=False,
        impacted_candidate_group_count=1,
        selected_candidate_group_id=candidate_group_id,
    )


def _approved_config(event_id: UUID | None = None, artifact_id: UUID | None = None, **kwargs):
    del artifact_id
    trigger_event_suffix = kwargs.pop("trigger_event_suffix", str(event_id)[-8:] if event_id is not None else None)
    redis_message_id_suffix = kwargs.pop("redis_message_id_suffix", STREAM_ID_SUFFIX)
    defaults = {
        "run_mode": "execute",
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_database_read": True,
        "allow_database_write_for_evidence_bundle_only": True,
        "allow_redis_read": True,
        "allow_redis_consume": True,
        "allow_redis_group_create": False,
        "allow_redis_group_destroy": False,
        "allow_redis_ack": True,
        "queue_name": "q.candidate.bundle",
        "redis_message_id_suffix": redis_message_id_suffix,
        "trigger_event_suffix": trigger_event_suffix,
    }
    defaults.update(kwargs)
    return BoundedBundleAssemblerConfig(**defaults)


@pytest.mark.asyncio
async def test_no_flags_blocks_before_runtime_redis_or_database() -> None:
    result = await run_bounded_bundle_assembler(BoundedBundleAssemblerConfig())

    report = result.to_sanitized_dict()
    assert result.ok is False
    assert report["status"] == "blocked"
    assert report["error_code"] == "operator_approval_missing"
    assert report["redis_consume_attempted"] is False
    assert report["database_write_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert report["redis_group_create_attempted"] is False
    assert report["redis_group_destroy_attempted"] is False


@pytest.mark.asyncio
async def test_execute_does_not_require_legacy_group_create_or_destroy_gates() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    selected = _selected(event_id, artifact_id)
    counters = BoundedBundleAssemblerCounters()
    database = FakeDatabase(event=_event(event_id, artifact_id), counters=counters)
    consumer = FakeConsumer(selected)

    result = await run_bounded_bundle_assembler(
        _approved_config(
            event_id=event_id,
            allow_redis_group_destroy=False,
            allow_redis_group_create=False,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert result.ok is True
    assert report["status"] == "assembled"
    assert report["redis_group_create_allowed"] is False
    assert report["redis_group_destroy_allowed"] is False
    assert report["redis_group_create_attempted"] is False
    assert report["redis_group_destroy_attempted"] is False
    assert report["redis_ack_attempted"] is True


@pytest.mark.asyncio
async def test_success_commits_before_ack_and_reports_assembly_counts() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    order: list[str] = []
    selected = _selected(event_id, artifact_id)
    counters = BoundedBundleAssemblerCounters()
    database = FakeDatabase(event=_event(event_id, artifact_id), counters=counters)
    consumer = FakeConsumer(selected, order=order)

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database, order=order),
    )

    report = result.to_sanitized_dict()
    assert result.ok is True
    assert report["status"] == "assembled"
    assert report["queue_name"] == "q.candidate.bundle"
    assert report["stage_name"] == "bundle"
    assert report["messages_seen"] == 1
    assert report["messages_matched"] == 1
    assert report["messages_processed_count"] == 1
    assert report["candidate_groups_seen"] == 1
    assert report["candidate_groups_processed"] == 1
    assert report["bundles_written_count"] == 1
    assert report["bundle_members_written_count"] == 1
    assert report["current_bundle_updates_count"] == 1
    assert report["analysis_requested_outbox_count"] == 1
    assert report["ready_for_analysis_count"] == 1
    assert report["redis_acked_count"] == 1
    assert order == ["commit", "ack"]
    assert consumer.acked == [STREAM_ID]


@pytest.mark.asyncio
async def test_event_payload_with_provider_without_snapshot_type_is_accepted_and_assembles() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    selected = _selected(event_id, artifact_id)
    database = FakeDatabase(
        event=_event(event_id, artifact_id, snapshot_type=None),
        counters=BoundedBundleAssemblerCounters(),
        run_contract_validation=True,
    )
    consumer = FakeConsumer(selected)

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert result.ok is True
    assert report["status"] == "assembled"
    assert report["target_snapshot_type"] is None
    assert report["bundles_written_count"] == 1
    assert report["redis_acked_count"] == 1
    assert database.assembled is True


@pytest.mark.asyncio
async def test_event_payload_with_provider_and_snapshot_type_reports_sanitized_target_snapshot_type() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    selected = _selected(event_id, artifact_id)
    database = FakeDatabase(
        event=_event(event_id, artifact_id, snapshot_type="web_article"),
        counters=BoundedBundleAssemblerCounters(),
        run_contract_validation=True,
    )
    consumer = FakeConsumer(selected)

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert result.ok is True
    assert report["status"] == "assembled"
    assert report["target_snapshot_type"] == "web_article"
    assert report["target_snapshot_id_suffix"] is not None


@pytest.mark.asyncio
async def test_candidate_bundle_refresh_validates_and_assembles_without_artifact_snapshot_payload() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    order: list[str] = []
    event = _candidate_refresh_event(event_id, candidate_group_id)
    selected = _selected_candidate_refresh(event_id, candidate_group_id)
    forbidden_artifact_fields = {"artifact_id", "snapshot_id", "provider", "status", "content_anchor"}
    assert forbidden_artifact_fields.isdisjoint(event.payload_json)
    database = FakeDatabase(
        event=event,
        counters=BoundedBundleAssemblerCounters(),
        run_contract_validation=True,
    )
    consumer = FakeConsumer(selected, order=order)

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database, order=order),
    )

    report = result.to_sanitized_dict()
    assert result.ok is True
    assert report["status"] == "assembled"
    assert report["target_artifact_id_suffix"] is None
    assert report["target_candidate_group_suffix"] == str(candidate_group_id)[-8:]
    assert report["target_snapshot_id_suffix"] is None
    assert report["target_snapshot_type"] is None
    assert report["candidate_groups_seen"] == 1
    assert report["candidate_groups_processed"] == 1
    assert report["bundles_written_count"] == 1
    assert report["redis_acked_count"] == 1
    assert order == ["commit", "ack"]
    assert consumer.acked == [STREAM_ID]
    assert database.assembled is True


@pytest.mark.asyncio
async def test_candidate_bundle_refresh_preview_reaches_selected_candidate_group_filter() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    event = _candidate_refresh_event(event_id, candidate_group_id)
    selected = _selected_candidate_refresh(event_id, candidate_group_id)
    database = FakeDatabase(
        event=event,
        counters=BoundedBundleAssemblerCounters(),
        preview_results=[
            EvidenceBundlePreview(
                candidate_group_id=candidate_group_id,
                current_bundle_present_before=False,
                bundle_input_existing=False,
                ready_for_analysis=True,
                analysis_requested_existing=False,
                analysis_requested_would_emit=True,
            )
        ],
        run_contract_validation=True,
    )
    consumer = FakeConsumer(selected)

    result = await run_bounded_bundle_assembler(
        _approved_config(
            event_id=event_id,
            run_mode="preview",
            allow_database_write_for_evidence_bundle_only=False,
            allow_redis_consume=False,
            allow_redis_ack=False,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert result.ok is True
    assert report["status"] == "previewed"
    assert report["target_candidate_group_suffix"] == str(candidate_group_id)[-8:]
    assert report["bundle_input_new_count"] == 1
    assert report["database_write_attempted"] is False
    assert report["redis_ack_attempted"] is False


@pytest.mark.asyncio
async def test_candidate_bundle_refresh_wrong_aggregate_type_blocks_before_assembly_or_ack() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    selected = _selected_candidate_refresh(event_id, candidate_group_id)
    database = FakeDatabase(
        event=_candidate_refresh_event(event_id, candidate_group_id, aggregate_type="artifact"),
        counters=BoundedBundleAssemblerCounters(),
        run_contract_validation=True,
    )
    consumer = FakeConsumer(selected)

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "blocked"
    assert report["error_code"] == "aggregate_type_not_allowed"
    assert database.assembled is False
    assert report["database_write_attempted"] is False
    assert report["redis_ack_attempted"] is False


@pytest.mark.asyncio
async def test_candidate_bundle_refresh_payload_candidate_group_mismatch_blocks() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    selected = _selected_candidate_refresh(event_id, candidate_group_id)
    database = FakeDatabase(
        event=_candidate_refresh_event(
            event_id,
            candidate_group_id,
            payload_candidate_group_id=uuid4(),
        ),
        counters=BoundedBundleAssemblerCounters(),
        run_contract_validation=True,
    )
    consumer = FakeConsumer(selected)

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "blocked"
    assert report["error_code"] == "payload_candidate_group_id_mismatch"
    assert database.assembled is False
    assert report["database_write_attempted"] is False
    assert report["redis_ack_attempted"] is False


@pytest.mark.asyncio
async def test_candidate_bundle_refresh_candidate_group_suffix_mismatch_blocks() -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    selected = _selected_candidate_refresh(event_id, candidate_group_id)
    database = FakeDatabase(
        event=_candidate_refresh_event(event_id, candidate_group_id),
        counters=BoundedBundleAssemblerCounters(),
        run_contract_validation=True,
    )
    consumer = FakeConsumer(selected)
    bad_suffix = "00000000"
    if str(candidate_group_id).endswith(bad_suffix):
        bad_suffix = "11111111"

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id, candidate_group_suffix=bad_suffix),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "blocked"
    assert report["error_code"] == "candidate_group_suffix_not_found"
    assert database.assembled is False
    assert report["database_write_attempted"] is False
    assert report["redis_ack_attempted"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing_field",
    ["candidate_group_id", "trigger_kind", "trigger_object_type", "trigger_object_id", "refresh_reason"],
)
async def test_candidate_bundle_refresh_missing_required_payload_field_blocks_with_malformed_payload(
    missing_field: str,
) -> None:
    event_id = uuid4()
    candidate_group_id = uuid4()
    selected = _selected_candidate_refresh(event_id, candidate_group_id)
    database = FakeDatabase(
        event=_candidate_refresh_event(event_id, candidate_group_id, missing_payload_fields={missing_field}),
        counters=BoundedBundleAssemblerCounters(),
        run_contract_validation=True,
    )
    consumer = FakeConsumer(selected)

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "blocked"
    assert report["error_code"] == "malformed_event_payload"
    assert database.assembled is False
    assert report["database_write_attempted"] is False
    assert report["redis_ack_attempted"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_field", ["artifact_id", "snapshot_id", "status", "content_anchor"])
async def test_required_event_payload_fields_block_before_assembly_or_ack(missing_field: str) -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    selected = _selected(event_id, artifact_id)
    database = FakeDatabase(
        event=_event(event_id, artifact_id, missing_payload_fields={missing_field}),
        counters=BoundedBundleAssemblerCounters(),
        run_contract_validation=True,
    )
    consumer = FakeConsumer(selected)

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "blocked"
    assert report["error_code"] == "malformed_event_payload"
    assert database.assembled is False
    assert report["database_write_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert consumer.acked == []


@pytest.mark.asyncio
async def test_missing_event_payload_provider_blocks_before_assembly_or_ack() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    fields = _message_fields(event_id, artifact_id, provider="redis-provider-must-not-satisfy-contract")
    selected = _selected(event_id, artifact_id, fields)
    database = FakeDatabase(
        event=_event(event_id, artifact_id, missing_payload_fields={"provider"}),
        counters=BoundedBundleAssemblerCounters(),
        run_contract_validation=True,
    )
    consumer = FakeConsumer(selected)

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "blocked"
    assert report["error_code"] == "malformed_event_payload"
    assert database.assembled is False
    assert report["database_write_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert consumer.acked == []


@pytest.mark.asyncio
async def test_business_payload_stream_fields_block_before_database_and_ack() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    fields = _message_fields(event_id, artifact_id, payload_json='{"business": true}')
    consumer = FakeConsumer(_selected(event_id, artifact_id, fields))
    database = FakeDatabase(event=_event(event_id, artifact_id), counters=BoundedBundleAssemblerCounters())

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "blocked"
    assert report["error_code"] == "redis_message_contract_invalid"
    assert database.assembled is False
    assert report["database_write_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert consumer.acked == []


@pytest.mark.asyncio
async def test_database_commit_failure_does_not_ack() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    selected = _selected(event_id, artifact_id)
    database = FakeDatabase(event=_event(event_id, artifact_id), counters=BoundedBundleAssemblerCounters())
    consumer = FakeConsumer(selected)

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(
            database,
            close_error=RuntimeError(RAW_EXCEPTION_DETAIL),
        ),
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "failed"
    assert report["error_code"] == "database_write_failed"
    assert report["error_class"] == "RuntimeError"
    assert report["redis_ack_attempted"] is False
    assert consumer.acked == []


@pytest.mark.asyncio
async def test_redis_ack_failure_occurs_after_commit_and_keeps_sanitized_counts() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    order: list[str] = []
    selected = _selected(event_id, artifact_id)
    database = FakeDatabase(event=_event(event_id, artifact_id), counters=BoundedBundleAssemblerCounters())
    consumer = FakeConsumer(selected, ack_error=RuntimeError(RAW_EXCEPTION_DETAIL), order=order)

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database, order=order),
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "failed"
    assert report["error_code"] == "redis_ack_failed"
    assert report["error_class"] == "RuntimeError"
    assert report["redis_ack_attempted"] is True
    assert report["redis_acked_count"] == 0
    assert report["bundles_written_count"] == 1
    assert order == ["commit", "ack"]


@pytest.mark.asyncio
async def test_trigger_event_suffix_must_be_unique_before_assembly_or_ack() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    database = FakeDatabase(
        event=_event(event_id, artifact_id),
        counters=BoundedBundleAssemblerCounters(),
        suffix_error=BoundedBundleAssemblerError("trigger_event_suffix_not_unique"),
    )
    consumer = FakeConsumer(_selected(event_id, artifact_id))

    result = await run_bounded_bundle_assembler(
        _approved_config(trigger_event_suffix=str(event_id)[-8:]),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "blocked"
    assert report["error_code"] == "trigger_event_suffix_not_unique"
    assert database.assembled is False
    assert report["redis_ack_attempted"] is False


@pytest.mark.asyncio
async def test_candidate_fanout_cap_blocks_before_assembly_or_ack() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    database = FakeDatabase(
        event=_event(event_id, artifact_id, impacted_count=101),
        counters=BoundedBundleAssemblerCounters(),
        validate_error=BoundedBundleAssemblerError("candidate_fanout_limit_exceeded"),
    )
    consumer = FakeConsumer(_selected(event_id, artifact_id))

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id, candidate_fanout_limit=25),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "blocked"
    assert report["error_code"] == "candidate_fanout_limit_exceeded"
    assert database.assembled is False
    assert report["redis_ack_attempted"] is False


@pytest.mark.asyncio
async def test_existing_group_scan_finds_target_with_xreadgroup_gt_without_waiting_for_future_messages() -> None:
    target_event_id = uuid4()
    target_artifact_id = uuid4()
    other_event_id = uuid4()
    other_artifact_id = uuid4()
    redis_client = FakeRedisClient(
        [
            ("1710000000001-0", _message_fields(other_event_id, other_artifact_id)),
            (STREAM_ID, _message_fields(target_event_id, target_artifact_id)),
        ]
    )
    consumer = ExistingGroupRedisTargetConsumer(
        redis_client,
        queue_name="q.candidate.bundle",
        group_name="evidence-assembler",
        consumer_name="bounded-test",
    )

    selected, seen, matched = await asyncio.wait_for(
        consumer.find_target(
            _approved_config(event_id=target_event_id, scan_limit=25),
            state=runner_module.BoundedBundleAssemblerState(),
        ),
        timeout=0.25,
    )

    assert selected is not None
    assert selected.redis_message_id == STREAM_ID
    assert seen == 2
    assert matched == 1
    assert redis_client.xreadgroup_calls == [
        {
            "groupname": "evidence-assembler",
            "consumername": "bounded-test",
            "streams": {"q.candidate.bundle": ">"},
            "count": 2,
            "block": None,
        }
    ]


@pytest.mark.asyncio
async def test_preview_does_not_require_group_gates_or_create_destroy_group() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    event = _event(event_id, artifact_id)
    redis_client = FakeRedisClient(
        [(STREAM_ID, _message_fields(event_id, artifact_id))],
        group_pending=0,
        group_lag=1,
        group_last_delivered_id="1710000000001-0",
    )
    database = FakeDatabase(
        event=event,
        counters=BoundedBundleAssemblerCounters(),
        preview_results=[
            EvidenceBundlePreview(
                candidate_group_id=event.selected_candidate_group_id,
                current_bundle_present_before=False,
                bundle_input_existing=False,
                ready_for_analysis=True,
                analysis_requested_existing=False,
                analysis_requested_would_emit=True,
            )
        ],
    )

    result = await run_bounded_bundle_assembler(
        _approved_config(
            event_id=event_id,
            run_mode="preview",
            allow_database_write_for_evidence_bundle_only=False,
            allow_redis_consume=False,
            allow_redis_group_create=False,
            allow_redis_group_destroy=False,
            allow_redis_ack=False,
        ),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeExistingGroupRedisBuilder(redis_client),
        database_builder=FakeDatabaseBuilder(database),
    )

    report = result.to_sanitized_dict()
    assert result.ok is True
    assert report["status"] == "previewed"
    assert report["redis_group_create_allowed"] is False
    assert report["redis_group_destroy_allowed"] is False
    assert report["redis_group_create_attempted"] is False
    assert report["redis_group_destroy_attempted"] is False
    assert report["redis_group_destroy_succeeded"] is False
    assert report["redis_group_cleanup_suppressed"] is False
    assert report["redis_consume_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert report["group_name"] == "evidence-assembler"
    assert report["group_exists"] is True
    assert report["group_pending"] == 0
    assert report["group_lag"] == 1
    assert report["group_last_delivered_id_suffix"] == "00000001-0"
    assert report["target_after_group_last_delivered"] is True
    assert redis_client.xreadgroup_calls == []
    assert redis_client.xrange_calls == [{"count": 1}]


@pytest.mark.asyncio
async def test_execute_blocks_if_configured_consumer_group_is_missing() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    redis_client = FakeRedisClient(
        [(STREAM_ID, _message_fields(event_id, artifact_id))],
        group_exists=False,
    )

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeExistingGroupRedisBuilder(redis_client),
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "blocked"
    assert report["error_code"] == "redis_consumer_group_not_found"
    assert report["group_name"] == "evidence-assembler"
    assert report["group_exists"] is False
    assert report["redis_consume_attempted"] is False
    assert report["database_read_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert redis_client.xreadgroup_calls == []
    assert redis_client.acked == []


@pytest.mark.asyncio
async def test_execute_blocks_if_configured_consumer_group_has_pending_messages() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    redis_client = FakeRedisClient(
        [(STREAM_ID, _message_fields(event_id, artifact_id))],
        group_pending=1,
        group_lag=1,
        group_last_delivered_id="1710000000001-0",
    )

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeExistingGroupRedisBuilder(redis_client),
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "blocked"
    assert report["error_code"] == "redis_consumer_group_pending_nonzero"
    assert report["group_exists"] is True
    assert report["group_pending"] == 1
    assert report["redis_consume_attempted"] is False
    assert report["database_read_attempted"] is False
    assert report["redis_ack_attempted"] is False
    assert redis_client.xreadgroup_calls == []
    assert redis_client.acked == []


@pytest.mark.asyncio
async def test_existing_group_scan_no_target_returns_not_found_without_ack() -> None:
    target_event_id = uuid4()
    first_event_id = uuid4()
    first_artifact_id = uuid4()
    second_event_id = uuid4()
    second_artifact_id = uuid4()
    redis_client = FakeRedisClient(
        [
            ("1710000000001-0", _message_fields(first_event_id, first_artifact_id)),
            ("1710000000002-0", _message_fields(second_event_id, second_artifact_id)),
        ]
    )

    result = await asyncio.wait_for(
        run_bounded_bundle_assembler(
            _approved_config(event_id=target_event_id, scan_limit=25),
            runtime_config_loader=_runtime_config,
            redis_builder=FakeExistingGroupRedisBuilder(redis_client),
        ),
        timeout=0.25,
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "blocked"
    assert report["error_code"] == "target_message_not_found"
    assert report["messages_seen"] == 2
    assert report["messages_matched"] == 0
    assert report["redis_ack_attempted"] is False
    assert redis_client.acked == []
    assert redis_client.xreadgroup_calls == [
        {
            "groupname": "evidence-assembler",
            "consumername": "bounded-test",
            "streams": {"q.candidate.bundle": ">"},
            "count": 2,
            "block": None,
        }
    ]


@pytest.mark.asyncio
async def test_execute_uses_existing_group_commits_then_acks_exact_target_without_group_lifecycle() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    redis_client = FakeRedisClient(
        [(STREAM_ID, _message_fields(event_id, artifact_id))],
        group_pending=0,
        group_lag=1,
        group_last_delivered_id="1710000000001-0",
    )
    database = FakeDatabase(
        event=_event(event_id, artifact_id),
        counters=BoundedBundleAssemblerCounters(),
    )
    database_builder = FakeDatabaseBuilder(database)

    result = await run_bounded_bundle_assembler(
        _approved_config(event_id=event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeExistingGroupRedisBuilder(redis_client),
        database_builder=database_builder,
    )

    report = result.to_sanitized_dict()
    assert result.ok is True
    assert report["status"] == "assembled"
    assert report["group_name"] == "evidence-assembler"
    assert report["group_exists"] is True
    assert report["group_pending"] == 0
    assert report["group_lag"] == 1
    assert report["target_after_group_last_delivered"] is True
    assert report["redis_group_create_allowed"] is False
    assert report["redis_group_destroy_allowed"] is False
    assert report["redis_group_create_attempted"] is False
    assert report["redis_group_destroy_attempted"] is False
    assert report["redis_group_destroy_succeeded"] is False
    assert report["redis_group_cleanup_suppressed"] is False
    assert database_builder.close_commits == [True]
    assert redis_client.acked == [STREAM_ID]
    assert redis_client.xreadgroup_calls == [
        {
            "groupname": "evidence-assembler",
            "consumername": "bounded-test",
            "streams": {"q.candidate.bundle": ">"},
            "count": 1,
            "block": None,
        }
    ]


@pytest.mark.asyncio
async def test_existing_group_scan_duplicate_target_blocks_without_ack() -> None:
    target_event_id = uuid4()
    target_artifact_id = uuid4()
    redis_client = FakeRedisClient(
        [
            ("1710000000001-0", _message_fields(target_event_id, target_artifact_id)),
            ("1710000000011-0", _message_fields(target_event_id, target_artifact_id)),
        ]
    )

    result = await asyncio.wait_for(
        run_bounded_bundle_assembler(
            _approved_config(event_id=target_event_id, scan_limit=25, redis_message_id_suffix="1-0"),
            runtime_config_loader=_runtime_config,
            redis_builder=FakeExistingGroupRedisBuilder(redis_client),
        ),
        timeout=0.25,
    )

    report = result.to_sanitized_dict()
    assert report["status"] == "blocked"
    assert report["error_code"] == "target_message_count_exceeded"
    assert report["messages_seen"] == 2
    assert report["messages_matched"] == 2
    assert report["redis_ack_attempted"] is False
    assert redis_client.acked == []
    assert redis_client.xreadgroup_calls == [
        {
            "groupname": "evidence-assembler",
            "consumername": "bounded-test",
            "streams": {"q.candidate.bundle": ">"},
            "count": 2,
            "block": None,
        }
    ]


@pytest.mark.asyncio
async def test_existing_group_scan_does_not_ack_non_target_messages() -> None:
    target_event_id = uuid4()
    target_artifact_id = uuid4()
    other_event_id = uuid4()
    other_artifact_id = uuid4()
    redis_client = FakeRedisClient(
        [
            ("1710000000001-0", _message_fields(other_event_id, other_artifact_id)),
            (STREAM_ID, _message_fields(target_event_id, target_artifact_id)),
        ]
    )
    consumer = ExistingGroupRedisTargetConsumer(
        redis_client,
        queue_name="q.candidate.bundle",
        group_name="evidence-assembler",
        consumer_name="bounded-test",
    )

    selected, seen, matched = await consumer.find_target(
        _approved_config(event_id=target_event_id, scan_limit=25),
        state=runner_module.BoundedBundleAssemblerState(),
    )
    assert selected is not None
    assert selected.redis_message_id == STREAM_ID
    assert seen == 2
    assert matched == 1

    state = type("State", (), {"redis_ack_attempted": False})()
    await consumer.ack(selected.redis_message_id, state)
    assert redis_client.acked == [STREAM_ID]
    assert "1710000000001-0" not in redis_client.acked
