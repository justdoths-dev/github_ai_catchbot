from __future__ import annotations

import importlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from uuid import UUID, uuid4


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke.py"
)

FAKE_DATABASE_URL = "postgresql+psycopg:unit-redacted-database"
FAKE_REDIS_URL = "redis:unit-redacted-redis"
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
FAKE_STREAM_ID = "stream-id-sentinel"
FAKE_DEDUPE_KEY = "dedupe-key-sentinel"
FAKE_SOURCE_TEXT = "source-text-sentinel"
FAKE_X_URL = "url-sentinel"
FAKE_SECRET = "secret-sentinel"
_RETURN_DEFAULT = object()


class FakeMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class FakeResult:
    def __init__(self, *, scalar: Any = None, rows: list[dict[str, Any]] | None = None) -> None:
        self._scalar = scalar
        self._rows = rows or []

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def scalar(self) -> Any:
        return self._scalar

    def mappings(self) -> FakeMappings:
        return FakeMappings(self._rows)

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class FakeSession:
    def __init__(
        self,
        *,
        events: dict[UUID, dict[str, Any]] | None = None,
        x_snapshot_pairs: set[tuple[UUID, UUID]] | None = None,
        impacted_groups: dict[UUID, list[UUID]] | None = None,
        primary_artifact_types: dict[UUID, str] | None = None,
        current_bundle_ids: dict[UUID, UUID | None] | None = None,
        candidate_evidence_bundle_counts: dict[UUID, int] | None = None,
        candidate_evidence_member_counts: dict[UUID, int] | None = None,
        current_bundle_member_counts: dict[UUID, int] | None = None,
        current_bundle_ready_for_analysis: dict[UUID, bool] | None = None,
        pending_analysis_requested_counts: dict[UUID, int] | None = None,
        pending_analysis_requested_for_bundle_counts: dict[tuple[UUID, UUID], int] | None = None,
        read_only_value: str = "on",
        missing_tables: set[str] | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.events = events or {}
        self.x_snapshot_pairs = x_snapshot_pairs or set()
        self.impacted_groups = impacted_groups or {}
        self.primary_artifact_types = primary_artifact_types or {}
        self.current_bundle_ids = current_bundle_ids or {}
        self.candidate_evidence_bundle_counts = candidate_evidence_bundle_counts or {}
        self.candidate_evidence_member_counts = candidate_evidence_member_counts or {}
        self.current_bundle_member_counts = current_bundle_member_counts or {}
        self.current_bundle_ready_for_analysis = current_bundle_ready_for_analysis or {}
        self.pending_analysis_requested_counts = pending_analysis_requested_counts or {}
        self.pending_analysis_requested_for_bundle_counts = pending_analysis_requested_for_bundle_counts or {}
        self.read_only_value = read_only_value
        self.missing_tables = missing_tables or set()
        self.order = order
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.rolled_back = False
        self.closed = False

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(str(statement))
        self.statements.append(normalized)
        self.params.append(dict(params))
        module = _module()

        if normalized == _normalize(module.SET_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult()
        if normalized == _normalize(module.SHOW_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult(scalar=self.read_only_value)
        if normalized == _normalize(module.SELECT_ONE_QUERY):
            return FakeResult(scalar=1)
        if normalized == _normalize(module.TABLE_AVAILABLE_QUERY):
            table_name = str(params["qualified_table_name"]).removeprefix("public.")
            return FakeResult(scalar=table_name not in self.missing_tables)
        if normalized == _normalize(module.SELECT_TRIGGER_EVENT_QUERY):
            event = self.events.get(UUID(str(params["event_id"])))
            return FakeResult(rows=[event] if event else [])
        if normalized == _normalize(module.COUNT_X_SNAPSHOT_QUERY):
            pair = (UUID(str(params["artifact_id"])), UUID(str(params["snapshot_id"])))
            return FakeResult(scalar=1 if pair in self.x_snapshot_pairs else 0)
        if normalized == _normalize(module.SELECT_IMPACTED_CANDIDATE_GROUPS_QUERY):
            artifact_id = UUID(str(params["artifact_id"]))
            return FakeResult(
                rows=[
                    {"candidate_group_id": candidate_group_id}
                    for candidate_group_id in self.impacted_groups.get(artifact_id, [])
                ]
            )
        if normalized == _normalize(module.SELECT_PRIMARY_ARTIFACT_TYPES_QUERY):
            ids = [UUID(str(value)) for value in params["candidate_group_ids"]]
            return FakeResult(
                rows=[
                    {
                        "candidate_group_id": candidate_group_id,
                        "artifact_type": self.primary_artifact_types.get(candidate_group_id, "x_post"),
                    }
                    for candidate_group_id in ids
                    if candidate_group_id in self.primary_artifact_types or not self.primary_artifact_types
                ]
            )
        if normalized == _normalize(module.SELECT_CURRENT_BUNDLE_QUERY):
            candidate_group_id = UUID(str(params["candidate_group_id"]))
            return FakeResult(scalar=self.current_bundle_ids.get(candidate_group_id))
        if normalized == _normalize(module.COUNT_CANDIDATE_EVIDENCE_BUNDLES_QUERY):
            candidate_group_id = UUID(str(params["candidate_group_id"]))
            return FakeResult(scalar=self.candidate_evidence_bundle_counts.get(candidate_group_id, 0))
        if normalized == _normalize(module.COUNT_CANDIDATE_EVIDENCE_MEMBERS_QUERY):
            candidate_group_id = UUID(str(params["candidate_group_id"]))
            return FakeResult(scalar=self.candidate_evidence_member_counts.get(candidate_group_id, 0))
        if normalized == _normalize(module.COUNT_PENDING_ANALYSIS_REQUESTED_QUERY):
            candidate_group_id = UUID(str(params["candidate_group_id"]))
            return FakeResult(scalar=self.pending_analysis_requested_counts.get(candidate_group_id, 0))
        if normalized == _normalize(module.COUNT_CURRENT_BUNDLE_MEMBERS_QUERY):
            bundle_id = UUID(str(params["bundle_id"]))
            return FakeResult(scalar=self.current_bundle_member_counts.get(bundle_id, 0))
        if normalized == _normalize(module.SELECT_CURRENT_BUNDLE_READY_QUERY):
            bundle_id = UUID(str(params["bundle_id"]))
            return FakeResult(scalar=self.current_bundle_ready_for_analysis.get(bundle_id))
        if normalized == _normalize(module.COUNT_PENDING_ANALYSIS_REQUESTED_FOR_BUNDLE_QUERY):
            candidate_group_id = UUID(str(params["candidate_group_id"]))
            bundle_id = UUID(str(params["bundle_id"]))
            return FakeResult(
                scalar=self.pending_analysis_requested_for_bundle_counts.get((candidate_group_id, bundle_id), 0)
            )

        raise AssertionError(f"unexpected SQL: {statement}")

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        self.rolled_back = True
        if self.order is not None:
            self.order.append("db:rollback")

    async def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(
        self,
        entries: list[tuple[str, dict[str, str]]] | None = None,
        *,
        fail_ack: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self.entries = entries or []
        self.fail_ack = fail_ack
        self.order = order
        self.ping_calls = 0
        self.xlen_calls: list[str] = []
        self.xrange_calls: list[str] = []
        self.xgroup_create_calls: list[tuple[str, str]] = []
        self.xreadgroup_calls: list[dict[str, Any]] = []
        self.xack_calls: list[tuple[str, str, tuple[str, ...]]] = []
        self.closed = False

    async def ping(self) -> bool:
        self.ping_calls += 1
        return True

    async def xlen(self, name: str) -> int:
        self.xlen_calls.append(name)
        return len(self.entries)

    async def xrange(
        self,
        name: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, str]]]:
        self.xrange_calls.append(name)
        return self.entries[: count or len(self.entries)]

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "0",
        mkstream: bool = False,
    ) -> bool:
        self.xgroup_create_calls.append((name, groupname))
        return True

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        self.xreadgroup_calls.append(
            {
                "groupname": groupname,
                "consumername": consumername,
                "streams": dict(streams),
                "count": count,
                "block": block,
            }
        )
        return [("q.candidate.bundle", self.entries[: count or len(self.entries)])]

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        if self.order is not None:
            self.order.append("redis:xack")
        self.xack_calls.append((name, groupname, ids))
        if self.fail_ack:
            raise RuntimeError(f"redis ack failed with {FAKE_SECRET}")
        return len(ids)

    async def close(self) -> None:
        self.closed = True


@dataclass(frozen=True, slots=True)
class FakeAssemblyResult:
    candidate_group_id: UUID
    bundle_id: UUID | None
    reused_existing_bundle: bool
    ready_for_analysis: bool
    emitted_analysis_requested: bool


class FakeService:
    def __init__(
        self,
        *,
        result: FakeAssemblyResult | None = None,
        return_value: Any = _RETURN_DEFAULT,
        on_call: Callable[[], None] | None = None,
        raises: Exception | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.result = result
        self.return_value = return_value
        self.on_call = on_call
        self.raises = raises
        self.order = order
        self.trigger_event_ids: list[str] = []

    async def handle_trigger_event(self, trigger_event_id: str) -> Any:
        self.trigger_event_ids.append(trigger_event_id)
        if self.order is not None:
            self.order.append("db:evidence_assembler_commit")
        if self.raises is not None:
            raise self.raises
        if self.on_call is not None:
            self.on_call()
        if self.return_value is not _RETURN_DEFAULT:
            return self.return_value
        if self.result is None:
            return []
        return [self.result]


class AnalysisRequestedOutboxFailure(RuntimeError):
    pass


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(_path: str | Path) -> dict[str, str]:
    return {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "EVIDENCE_ASSEMBLER_BUNDLE_PROFILE_VERSION": "bundle_profile_v1",
    }


def _all_approvals() -> Any:
    module = _module()
    return module.ConsumeApprovals(
        artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume=True,
        candidate_evidence_bundle_write=True,
        analysis_requested_outbox_write=True,
        redis_ack=True,
    )


def _approvals(**overrides: bool) -> Any:
    values = {
        "artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume": False,
        "candidate_evidence_bundle_write": False,
        "analysis_requested_outbox_write": False,
        "redis_ack": False,
    }
    values.update(overrides)
    return _module().ConsumeApprovals(**values)


def _event_row(
    *,
    event_id: UUID,
    artifact_id: UUID,
    snapshot_id: UUID,
    event_type: str = "artifact.snapshot.updated.v1",
    aggregate_type: str = "artifact",
    status: str = "published",
    provider: str = "x",
    provider_route: str | None = "x",
    include_artifact_id: bool = True,
    include_snapshot_id: bool = True,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "provider": provider,
        "snapshot_type": "x_post",
        "status": "ready",
        "content_anchor": "private-content-anchor",
        "source_text": FAKE_SOURCE_TEXT,
        "canonical_url": FAKE_X_URL,
    }
    if provider_route is not None:
        payload["provider_route"] = provider_route
    if include_artifact_id:
        payload["artifact_id"] = str(artifact_id)
    if include_snapshot_id:
        payload["snapshot_id"] = str(snapshot_id)
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": artifact_id,
        "payload_json": payload,
        "status": status,
    }


def _stream_fields(
    *,
    trigger_event_id: UUID,
    artifact_id: UUID,
    overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    fields = {
        "job_id": str(trigger_event_id),
        "stage_name": "bundle",
        "root_object_type": "artifact",
        "root_object_id": str(artifact_id),
        "idempotency_key": FAKE_DEDUPE_KEY,
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(trigger_event_id),
    }
    if overrides:
        fields.update(overrides)
    return fields


def _fixtures(
    *,
    event_row: dict[str, Any] | None = None,
    impacted_group: UUID | None = None,
    include_event: bool = True,
    include_snapshot: bool = True,
    include_membership: bool = True,
    redis_fields: dict[str, str] | None = None,
    order: list[str] | None = None,
) -> tuple[FakeSession, FakeRedis, UUID, UUID, UUID, UUID]:
    event_id = uuid4()
    artifact_id = uuid4()
    snapshot_id = uuid4()
    candidate_group_id = impacted_group or uuid4()
    event = event_row or _event_row(event_id=event_id, artifact_id=artifact_id, snapshot_id=snapshot_id)
    session = FakeSession(
        events={event_id: event} if include_event else {},
        x_snapshot_pairs={(artifact_id, snapshot_id)} if include_snapshot else set(),
        impacted_groups={artifact_id: [candidate_group_id]} if include_membership else {},
        primary_artifact_types={candidate_group_id: "x_post"},
        order=order,
    )
    redis = FakeRedis(
        [(FAKE_STREAM_ID, redis_fields or _stream_fields(trigger_event_id=event_id, artifact_id=artifact_id))],
        order=order,
    )
    return session, redis, event_id, artifact_id, snapshot_id, candidate_group_id


def _mark_success_postconditions(
    session: FakeSession,
    candidate_group_id: UUID,
    *,
    ready_for_analysis: bool = True,
    emit_analysis_requested: bool = True,
) -> UUID:
    bundle_id = uuid4()
    session.current_bundle_ids[candidate_group_id] = bundle_id
    session.candidate_evidence_bundle_counts[candidate_group_id] = (
        session.candidate_evidence_bundle_counts.get(candidate_group_id, 0) + 1
    )
    session.candidate_evidence_member_counts[candidate_group_id] = (
        session.candidate_evidence_member_counts.get(candidate_group_id, 0) + 1
    )
    session.current_bundle_member_counts[bundle_id] = 1
    session.current_bundle_ready_for_analysis[bundle_id] = ready_for_analysis
    if emit_analysis_requested:
        session.pending_analysis_requested_counts[candidate_group_id] = (
            session.pending_analysis_requested_counts.get(candidate_group_id, 0) + 1
        )
        session.pending_analysis_requested_for_bundle_counts[(candidate_group_id, bundle_id)] = 1
    return bundle_id


def _seed_existing_bundle(
    session: FakeSession,
    candidate_group_id: UUID,
    *,
    pending_analysis_requested: bool = True,
) -> UUID:
    bundle_id = uuid4()
    session.current_bundle_ids[candidate_group_id] = bundle_id
    session.candidate_evidence_bundle_counts[candidate_group_id] = 1
    session.candidate_evidence_member_counts[candidate_group_id] = 1
    session.current_bundle_member_counts[bundle_id] = 1
    session.current_bundle_ready_for_analysis[bundle_id] = True
    if pending_analysis_requested:
        session.pending_analysis_requested_counts[candidate_group_id] = 1
        session.pending_analysis_requested_for_bundle_counts[(candidate_group_id, bundle_id)] = 1
    return bundle_id


def _run_report(
    *,
    approvals: Any | None = None,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    service: FakeService | None = None,
    side_effect_flags: dict[str, bool] | None = None,
) -> tuple[Any, FakeSession, FakeRedis, FakeService]:
    if session is None or redis is None:
        session, redis, _event_id, _artifact_id, _snapshot_id, candidate_group_id = _fixtures()
    else:
        candidate_group_id = next(iter(session.primary_artifact_types), uuid4())
    fake_service = service or FakeService(
        result=FakeAssemblyResult(
            candidate_group_id=candidate_group_id,
            bundle_id=uuid4(),
            reused_existing_bundle=False,
            ready_for_analysis=True,
            emitted_analysis_requested=True,
        ),
        on_call=lambda: _mark_success_postconditions(session, candidate_group_id),
    )
    module = _module()
    result = module.generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        approvals=approvals,
        runtime_env_reader=_runtime_env,
        database_session_factory=lambda _url: session,
        redis_client_factory=lambda _url: redis,
        service_factory=lambda config, _repository: _assert_service_config(config, fake_service),
        repository_factory=lambda _session: object(),
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_SECRET),
    )
    return result, session, redis, fake_service


def _assert_service_config(config: Any, service: FakeService) -> FakeService:
    assert config.queue_name == "q.candidate.bundle"
    assert config.enable_text_idea is False
    assert config.enable_reroot is False
    return service


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_report_contains_required_sanitized_fields() -> None:
    result, _session, _redis, _service = _run_report()

    required_fields = {
        "contract_status",
        "runtime_env_read",
        "database_connected",
        "redis_connected",
        "read_only_transaction",
        "candidate_bundle_stream_found_bucket",
        "candidate_bundle_entry_valid_bucket",
        "trigger_event_rehydrated_bucket",
        "snapshot_updated_event_valid_bucket",
        "impacted_candidate_group_bucket",
        "current_snapshot_found_bucket",
        "evidence_bundle_write_planned_bucket",
        "candidate_evidence_member_write_planned_bucket",
        "current_bundle_update_planned_bucket",
        "analysis_requested_outbox_planned_bucket",
        "redis_ack_planned_bucket",
        "baseline_current_bundle_present_bucket",
        "baseline_candidate_evidence_bundle_count_bucket",
        "baseline_candidate_evidence_member_count_bucket",
        "baseline_analysis_requested_outbox_pending_bucket",
        "postcondition_current_bundle_present_bucket",
        "postcondition_current_bundle_changed_bucket",
        "postcondition_candidate_evidence_bundle_count_bucket",
        "postcondition_candidate_evidence_member_count_bucket",
        "postcondition_current_bundle_member_count_bucket",
        "postcondition_analysis_requested_outbox_pending_bucket",
        "evidence_bundle_write_attempted",
        "evidence_bundle_written_bucket",
        "candidate_evidence_member_write_attempted",
        "candidate_evidence_member_written_bucket",
        "current_bundle_update_attempted",
        "current_bundle_updated_bucket",
        "analysis_requested_outbox_write_attempted",
        "analysis_requested_outbox_written_bucket",
        "redis_ack_attempted",
        "redis_ack_succeeded_bucket",
        "analysis_router_started",
        "judge_openai_started",
        "policy_engine_started",
        "notifier_started",
        "source_tables_mutation_performed",
        "telegram_raw_updates_mutation_performed",
        "artifact_registry_mutation_performed",
        "artifact_snapshot_mutation_performed",
        "docker_or_systemd_changed",
        "alembic_run",
        "external_network_attempted",
        "raw_values_emitted",
        "checks_failed",
    }
    assert required_fields <= set(result.report)


def test_default_mode_is_read_only_and_does_not_ack_or_write() -> None:
    result, session, redis, service = _run_report()

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_READY
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["candidate_bundle_stream_found_bucket"] == "one"
    assert result.report["candidate_bundle_entry_valid_bucket"] == "one"
    assert result.report["trigger_event_rehydrated_bucket"] == "one"
    assert result.report["snapshot_updated_event_valid_bucket"] == "one"
    assert result.report["impacted_candidate_group_bucket"] == "one"
    assert result.report["evidence_bundle_write_planned_bucket"] == "one"
    assert result.report["analysis_requested_outbox_planned_bucket"] == "one"
    assert result.report["redis_ack_planned_bucket"] == "one"
    assert result.report["evidence_bundle_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert redis.xack_calls == []
    assert redis.xgroup_create_calls == []
    assert service.trigger_event_ids == []
    assert session.rolled_back is True


def test_partial_approvals_fail_before_db_write_or_redis_ack() -> None:
    session, redis, _event_id, _artifact_id, _snapshot_id, _candidate_group_id = _fixtures()
    service = FakeService()
    result, session, redis, service = _run_report(
        session=session,
        redis=redis,
        service=service,
        approvals=_approvals(
            artifact_snapshot_updated_candidate_bundle_evidence_assembler_consume=True,
            candidate_evidence_bundle_write=True,
            analysis_requested_outbox_write=True,
        ),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_MISSING_APPROVAL
    assert "approval.redis_ack" in result.report["checks_failed"]
    assert result.report["database_connected"] is False
    assert result.report["redis_connected"] is False
    assert session.statements == []
    assert redis.xack_calls == []
    assert service.trigger_event_ids == []


def test_thin_redis_payload_shape_is_validated_before_db_write_or_ack() -> None:
    payload_artifact_id = uuid4()
    payload_event_id = uuid4()
    session, redis, event_id, artifact_id, _snapshot_id, _candidate_group_id = _fixtures(
        redis_fields={
            **_stream_fields(trigger_event_id=uuid4(), artifact_id=uuid4()),
            "payload_json": json.dumps(
                {
                    "artifact_id": str(payload_artifact_id),
                    "trigger_event_id": str(payload_event_id),
                }
            ),
        }
    )
    result, session, redis, service = _run_report(session=session, redis=redis, approvals=_all_approvals())

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_REDIS_PAYLOAD
    assert "redis.thin_payload_shape" in result.report["checks_failed"]
    assert result.report["candidate_bundle_entry_valid_bucket"] == "zero"
    assert result.report["evidence_bundle_write_attempted"] is False
    assert redis.xack_calls == []
    assert service.trigger_event_ids == []


def test_trigger_event_is_rehydrated_from_event_outbox_not_redis_business_payload() -> None:
    session, redis, event_id, _artifact_id, _snapshot_id, _candidate_group_id = _fixtures()
    result, session, _redis, _service = _run_report(session=session, redis=redis)

    assert result.exit_code == 0
    assert result.report["trigger_event_rehydrated_bucket"] == "one"
    assert any(_normalize(_module().SELECT_TRIGGER_EVENT_QUERY) == statement for statement in session.statements)
    rendered_redis_fields = json.dumps(redis.entries[0][1], sort_keys=True)
    assert "artifact_id" not in rendered_redis_fields
    assert "snapshot_id" not in rendered_redis_fields
    assert redis.entries[0][1]["trigger_event_id"] == str(event_id)


def test_missing_trigger_event_blocks_before_writes_or_ack() -> None:
    session, redis, _event_id, _artifact_id, _snapshot_id, _candidate_group_id = _fixtures(include_event=False)
    result, _session, redis, service = _run_report(session=session, redis=redis, approvals=_all_approvals())

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_TRIGGER_REHYDRATE_FAILURE
    assert result.report["evidence_bundle_write_attempted"] is False
    assert redis.xack_calls == []
    assert service.trigger_event_ids == []


def test_invalid_event_or_missing_candidate_membership_blocks_before_writes() -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    snapshot_id = uuid4()
    invalid_event = _event_row(
        event_id=event_id,
        artifact_id=artifact_id,
        snapshot_id=snapshot_id,
        status="pending",
    )
    session, redis, _event_id, _artifact_id, _snapshot_id, _candidate_group_id = _fixtures(
        event_row=invalid_event,
        include_membership=True,
    )
    result, _session, redis, service = _run_report(session=session, redis=redis, approvals=_all_approvals())

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_EVENT
    assert "event_outbox.status" in result.report["checks_failed"]
    assert redis.xack_calls == []
    assert service.trigger_event_ids == []

    session, redis, _event_id, _artifact_id, _snapshot_id, _candidate_group_id = _fixtures(
        include_membership=False,
    )
    result, _session, redis, service = _run_report(session=session, redis=redis, approvals=_all_approvals())
    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NO_IMPACTED_CANDIDATE_GROUP
    assert "candidate_group_members.artifact_id" in result.report["checks_failed"]
    assert redis.xack_calls == []
    assert service.trigger_event_ids == []


def test_approved_mode_invokes_evidence_assembler_boundary_and_acks_after_success() -> None:
    order: list[str] = []
    session, redis, event_id, _artifact_id, _snapshot_id, candidate_group_id = _fixtures(order=order)
    service = FakeService(
        result=FakeAssemblyResult(
            candidate_group_id=candidate_group_id,
            bundle_id=uuid4(),
            reused_existing_bundle=False,
            ready_for_analysis=True,
            emitted_analysis_requested=True,
        ),
        on_call=lambda: _mark_success_postconditions(session, candidate_group_id),
        order=order,
    )
    result, _session, redis, service = _run_report(
        session=session,
        redis=redis,
        service=service,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_CONSUMED
    assert service.trigger_event_ids == [str(event_id)]
    assert result.report["baseline_current_bundle_present_bucket"] == "zero"
    assert result.report["postcondition_current_bundle_present_bucket"] == "one"
    assert result.report["postcondition_current_bundle_changed_bucket"] == "one"
    assert result.report["evidence_bundle_write_attempted"] is True
    assert result.report["evidence_bundle_written_bucket"] == "one"
    assert result.report["candidate_evidence_member_write_attempted"] is True
    assert result.report["candidate_evidence_member_written_bucket"] == "one"
    assert result.report["current_bundle_update_attempted"] is True
    assert result.report["current_bundle_updated_bucket"] == "one"
    assert result.report["analysis_requested_outbox_write_attempted"] is True
    assert result.report["analysis_requested_outbox_written_bucket"] == "one"
    assert result.report["redis_ack_attempted"] is True
    assert result.report["redis_ack_succeeded_bucket"] == "one"
    assert redis.xack_calls == [("q.candidate.bundle", "evidence-assembler", (FAKE_STREAM_ID,))]
    assert order == ["db:evidence_assembler_commit", "redis:xack"]


def test_fake_service_returning_none_is_accepted_when_db_postconditions_show_success() -> None:
    session, redis, event_id, _artifact_id, _snapshot_id, candidate_group_id = _fixtures()
    service = FakeService(
        return_value=None,
        on_call=lambda: _mark_success_postconditions(session, candidate_group_id),
    )
    result, _session, redis, service = _run_report(
        session=session,
        redis=redis,
        service=service,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_CONSUMED
    assert service.trigger_event_ids == [str(event_id)]
    assert result.report["evidence_bundle_written_bucket"] == "one"
    assert result.report["analysis_requested_outbox_written_bucket"] == "one"
    assert redis.xack_calls == [("q.candidate.bundle", "evidence-assembler", (FAKE_STREAM_ID,))]


def test_service_return_shape_is_ignored_when_db_postconditions_show_success() -> None:
    session, redis, _event_id, _artifact_id, _snapshot_id, candidate_group_id = _fixtures()
    service = FakeService(
        return_value={"unexpected": "service-return-shape"},
        on_call=lambda: _mark_success_postconditions(session, candidate_group_id),
    )
    result, _session, redis, _service = _run_report(
        session=session,
        redis=redis,
        service=service,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_CONSUMED
    assert result.report["evidence_bundle_written_bucket"] == "one"
    assert result.report["redis_ack_succeeded_bucket"] == "one"
    assert len(redis.xack_calls) == 1


def test_db_postcondition_failure_after_service_call_prevents_redis_ack() -> None:
    session, redis, event_id, _artifact_id, _snapshot_id, _candidate_group_id = _fixtures()
    service = FakeService(return_value=None)
    result, session, redis, service = _run_report(
        session=session,
        redis=redis,
        service=service,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EVIDENCE_BUNDLE_WRITE_FAILURE
    assert "postcondition.current_bundle_id" in result.report["checks_failed"]
    assert result.report["redis_ack_attempted"] is False
    assert redis.xack_calls == []
    assert service.trigger_event_ids == [str(event_id)]
    assert session.rolled_back is True


def test_db_write_failure_rolls_back_and_does_not_ack() -> None:
    order: list[str] = []
    session, redis, _event_id, _artifact_id, _snapshot_id, _candidate_group_id = _fixtures(order=order)
    service = FakeService(raises=RuntimeError(f"db failed with {FAKE_SECRET}"), order=order)
    result, session, redis, service = _run_report(
        session=session,
        redis=redis,
        service=service,
        approvals=_all_approvals(),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_EVIDENCE_BUNDLE_WRITE_FAILURE
    assert result.report["evidence_bundle_write_attempted"] is True
    assert session.rolled_back is True
    assert redis.xack_calls == []
    assert service.trigger_event_ids
    assert FAKE_SECRET not in rendered
    assert "db failed" not in rendered


def test_analysis_requested_outbox_write_failure_has_sanitized_status_and_no_ack() -> None:
    session, redis, _event_id, _artifact_id, _snapshot_id, _candidate_group_id = _fixtures()
    service = FakeService(raises=AnalysisRequestedOutboxFailure(f"outbox failed with {FAKE_SECRET}"))
    result, _session, redis, _service = _run_report(
        session=session,
        redis=redis,
        service=service,
        approvals=_all_approvals(),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_ANALYSIS_REQUESTED_OUTBOX_WRITE_FAILURE
    assert "event_outbox.analysis_requested" in result.report["checks_failed"]
    assert redis.xack_calls == []
    assert FAKE_SECRET not in rendered
    assert "outbox failed" not in rendered


def test_redis_ack_failure_reports_after_db_success_without_downstream_start() -> None:
    session, redis, _event_id, _artifact_id, _snapshot_id, candidate_group_id = _fixtures()
    redis.fail_ack = True
    service = FakeService(
        result=FakeAssemblyResult(
            candidate_group_id=candidate_group_id,
            bundle_id=uuid4(),
            reused_existing_bundle=False,
            ready_for_analysis=True,
            emitted_analysis_requested=True,
        ),
        on_call=lambda: _mark_success_postconditions(session, candidate_group_id),
    )
    result, _session, redis, _service = _run_report(
        session=session,
        redis=redis,
        service=service,
        approvals=_all_approvals(),
    )
    rendered = json.dumps(result.report, sort_keys=True)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_REDIS_ACK_FAILURE
    assert result.report["evidence_bundle_written_bucket"] == "one"
    assert result.report["redis_ack_attempted"] is True
    assert result.report["redis_ack_succeeded_bucket"] == "zero"
    assert result.report["analysis_router_started"] is False
    assert result.report["judge_openai_started"] is False
    assert result.report["policy_engine_started"] is False
    assert result.report["notifier_started"] is False
    assert FAKE_SECRET not in rendered


def test_duplicate_existing_bundle_does_not_report_duplicate_bundle_or_analysis_outbox() -> None:
    session, redis, _event_id, _artifact_id, _snapshot_id, candidate_group_id = _fixtures()
    _seed_existing_bundle(session, candidate_group_id)
    service = FakeService(
        return_value=None,
    )
    result, _session, redis, _service = _run_report(
        session=session,
        redis=redis,
        service=service,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_CONSUMED
    assert result.report["baseline_current_bundle_present_bucket"] == "one"
    assert result.report["postcondition_current_bundle_changed_bucket"] == "zero"
    assert result.report["evidence_bundle_written_bucket"] == "zero"
    assert result.report["candidate_evidence_member_written_bucket"] == "zero"
    assert result.report["analysis_requested_outbox_written_bucket"] == "zero"
    assert result.report["redis_ack_succeeded_bucket"] == "one"
    assert len(redis.xack_calls) == 1


def test_no_analysis_router_judge_policy_or_notifier_starts() -> None:
    result, _session, _redis, _service = _run_report()
    text = SCRIPT.read_text(encoding="utf-8")

    assert result.report["analysis_router_started"] is False
    assert result.report["judge_openai_started"] is False
    assert result.report["policy_engine_started"] is False
    assert result.report["notifier_started"] is False
    assert "src.services.analysis_router" not in text
    assert "src.services.judge_openai" not in text
    assert "src.services.policy_engine" not in text
    assert "src.services.notifier_telegram" not in text


def test_forbidden_side_effect_flags_block() -> None:
    result, _session, _redis, _service = _run_report(
        side_effect_flags={"analysis_router_started": True},
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_FORBIDDEN_SIDE_EFFECT
    assert "side_effect.forbidden" in result.report["checks_failed"]


def test_no_raw_values_emitted_including_ids_urls_secrets_db_redis_stream_or_exception_body() -> None:
    session, redis, _event_id, _artifact_id, _snapshot_id, _candidate_group_id = _fixtures()
    result, _session, _redis, _service = _run_report(
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )
    rendered = json.dumps(result.report, sort_keys=True)
    forbidden_values = (
        *[str(value) for event in session.events.values() for value in (event["event_id"], event["aggregate_id"])],
        *[
            str(value)
            for event in session.events.values()
            for value in (
                event["payload_json"].get("artifact_id"),
                event["payload_json"].get("snapshot_id"),
            )
        ],
        *[str(value) for groups in session.impacted_groups.values() for value in groups],
        *[str(value) for value in session.current_bundle_ids.values() if value is not None],
        FAKE_DEDUPE_KEY,
        FAKE_SOURCE_TEXT,
        FAKE_X_URL,
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_RUNTIME_PATH,
        FAKE_STREAM_ID,
        FAKE_SECRET,
        "stderr",
    )
    for value in forbidden_values:
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False


def test_no_source_telegram_artifact_snapshot_runtime_systemd_docker_or_alembic_mutation_reported() -> None:
    result, _session, _redis, _service = _run_report(approvals=_all_approvals())

    assert result.report["source_tables_mutation_performed"] is False
    assert result.report["telegram_raw_updates_mutation_performed"] is False
    assert result.report["artifact_registry_mutation_performed"] is False
    assert result.report["artifact_snapshot_mutation_performed"] is False
    assert result.report["docker_or_systemd_changed"] is False
    assert result.report["alembic_run"] is False
    assert result.report["external_network_attempted"] is False
