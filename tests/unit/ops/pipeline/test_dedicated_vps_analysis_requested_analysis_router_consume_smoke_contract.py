from __future__ import annotations

import importlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_analysis_requested_analysis_router_consume_smoke.py"
)

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-analysis-consume"
FAKE_REDIS_CREDENTIAL = "unit" + "-redis" + "-credential" + "-analysis-consume"
FAKE_DATABASE_URL = (
    "postgresql+psycopg"
    + ":"
    + "/"
    + "/"
    + "github_ai_catchbot_app"
    + ":"
    + FAKE_DATABASE_CREDENTIAL
    + "@"
    + "127.0.0.1"
    + ":5432/"
    + "github_ai_catchbot"
)
FAKE_REDIS_URL = (
    "redis"
    + ":"
    + "/"
    + "/"
    + ":"
    + FAKE_REDIS_CREDENTIAL
    + "@"
    + "127.0.0.1"
    + ":6379/0"
)
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
FAKE_STREAM_ID = "1710000000000" + "-0"
FAKE_DEDUPE_KEY = "private-analysis-route-dedupe-key"
FAKE_SOURCE_TEXT = " ".join(["private", "source", "text", "fixture"])
FAKE_URL = "https" + ":" + "/" + "/" + "example.invalid" + "/private/analysis"
FAKE_SENSITIVE_VALUE = "fake" + "-private-sensitive-value"


class FakeMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None


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
        events: dict[str, dict[str, Any]] | None = None,
        candidate_groups: dict[str, UUID | None] | None = None,
        bundles: dict[str, dict[str, Any]] | None = None,
        member_counts: dict[str, tuple[int, int]] | None = None,
        read_only_value: str = "on",
        missing_tables: set[str] | None = None,
        existing_judge_run_id: UUID | None = None,
        existing_outbox_event_id: UUID | None = None,
        raise_on_judge_run_write: bool = False,
        raise_on_outbox_write: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self.events = events or {}
        self.candidate_groups = candidate_groups or {}
        self.bundles = bundles or {}
        self.member_counts = member_counts or {}
        self.read_only_value = read_only_value
        self.missing_tables = missing_tables or set()
        self.existing_judge_run_id = existing_judge_run_id
        self.existing_outbox_event_id = existing_outbox_event_id
        self.raise_on_judge_run_write = raise_on_judge_run_write
        self.raise_on_outbox_write = raise_on_outbox_write
        self.order = order
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.written_judge_runs: list[dict[str, Any]] = []
        self.written_outbox_events: list[dict[str, Any]] = []
        self.created_judge_run_id: UUID | None = None
        self.created_outbox_event_id: UUID | None = None
        self.committed = False
        self.rollback_count = 0
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
        if normalized == _normalize(module.SELECT_ANALYSIS_REQUESTED_EVENT_QUERY):
            row = self.events.get(str(params["event_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(module.SELECT_CANDIDATE_GROUP_STATE_QUERY):
            candidate_group_id = str(params["candidate_group_id"])
            if candidate_group_id not in self.candidate_groups:
                return FakeResult(rows=[])
            return FakeResult(
                rows=[
                    {
                        "candidate_group_id": UUID(candidate_group_id),
                        "current_bundle_id": self.candidate_groups[candidate_group_id],
                    }
                ]
            )
        if normalized == _normalize(module.SELECT_BUNDLE_ROUTE_STATE_QUERY):
            row = self.bundles.get(str(params["bundle_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(module.COUNT_CANDIDATE_EVIDENCE_MEMBERS_QUERY):
            member_count, supporting_count = self.member_counts.get(
                str(params["bundle_id"]),
                (0, 0),
            )
            return FakeResult(
                rows=[
                    {
                        "member_count": member_count,
                        "supporting_count": supporting_count,
                    }
                ]
            )
        if normalized == _normalize(module.INSERT_JUDGE_RUN_QUERY):
            if self.raise_on_judge_run_write:
                raise RuntimeError("judge write failed " + FAKE_SENSITIVE_VALUE)
            if self.order is not None:
                self.order.append("db:insert_judge_run")
            if self.existing_judge_run_id is not None:
                return FakeResult(scalar=None)
            self.created_judge_run_id = uuid4()
            self.written_judge_runs.append(dict(params))
            return FakeResult(scalar=self.created_judge_run_id)
        if normalized == _normalize(module.SELECT_EXISTING_JUDGE_RUN_QUERY):
            return FakeResult(scalar=self.existing_judge_run_id)
        if normalized == _normalize(module.SELECT_JUDGE_CALL_OUTBOX_QUERY):
            return FakeResult(scalar=self.existing_outbox_event_id)
        if normalized == _normalize(module.INSERT_JUDGE_CALL_OUTBOX_QUERY):
            if self.raise_on_outbox_write:
                raise RuntimeError("outbox write failed " + FAKE_SENSITIVE_VALUE)
            if self.order is not None:
                self.order.append("db:insert_judge_call_outbox")
            if self.existing_outbox_event_id is not None:
                return FakeResult(scalar=None)
            self.created_outbox_event_id = uuid4()
            self.written_outbox_events.append(dict(params))
            return FakeResult(scalar=self.created_outbox_event_id)

        raise AssertionError(f"unexpected SQL: {statement}")

    async def commit(self) -> None:
        self.committed = True
        if self.order is not None:
            self.order.append("db:commit")

    async def rollback(self) -> None:
        self.rollback_count += 1
        if self.order is not None:
            self.order.append("db:rollback")

    async def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(
        self,
        entries: list[tuple[str, dict[str, str]]] | None = None,
        *,
        ack_return: int = 1,
        raise_on_ack: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self.entries = entries or []
        self.ack_return = ack_return
        self.raise_on_ack = raise_on_ack
        self.order = order
        self.ping_calls = 0
        self.xlen_calls: list[str] = []
        self.xrange_calls: list[str] = []
        self.xgroup_create_calls: list[tuple[str, str, str, bool]] = []
        self.xreadgroup_calls: list[tuple[str, str, dict[str, str]]] = []
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
        self.xgroup_create_calls.append((name, groupname, id, mkstream))
        if self.order is not None:
            self.order.append("redis:xgroup_create")
        return True

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        self.xreadgroup_calls.append((groupname, consumername, dict(streams)))
        if self.order is not None:
            self.order.append("redis:xreadgroup")
        return [(_module().EXPECTED_QUEUE_NAME, self.entries[: count or 1])] if self.entries else []

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        self.xack_calls.append((name, groupname, ids))
        if self.order is not None:
            self.order.append("redis:xack")
        if self.raise_on_ack:
            raise RuntimeError("ack failed " + FAKE_SENSITIVE_VALUE)
        return self.ack_return

    async def close(self) -> None:
        self.closed = True


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_analysis_requested_analysis_router_consume_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(*, escalation: bool = False):
    def read(_path: str | Path) -> dict[str, str]:
        return {
            "DATABASE_URL": FAKE_DATABASE_URL,
            "REDIS_URL": FAKE_REDIS_URL,
            "ENABLE_MODEL_ESCALATION": "true" if escalation else "false",
        }

    return read


def _all_approvals() -> Any:
    module = _module()
    return module.ConsumeApprovals(
        analysis_router_consume=True,
        judge_run_write=True,
        judge_call_requested_outbox_write=True,
        redis_ack=True,
    )


def _approvals(**overrides: bool) -> Any:
    values = {
        "analysis_router_consume": False,
        "judge_run_write": False,
        "judge_call_requested_outbox_write": False,
        "redis_ack": False,
    }
    values.update(overrides)
    return _module().ConsumeApprovals(**values)


def _stream_fields(
    *,
    event_id: UUID,
    stage_name: str = "analysis_route",
    root_object_type: str = "candidate_group",
    root_object_id: UUID | None = None,
    include_trigger_event_id: bool = True,
    trigger_event_value: str | None = None,
    extra_fields: dict[str, str] | None = None,
) -> dict[str, str]:
    fields = {
        "job_id": str(event_id),
        "stage_name": stage_name,
        "root_object_type": root_object_type,
        "root_object_id": str(root_object_id or uuid4()),
        "idempotency_key": FAKE_DEDUPE_KEY,
        "pipeline_run_id": "",
        "not_before": "",
    }
    if include_trigger_event_id:
        fields["trigger_event_id"] = trigger_event_value if trigger_event_value is not None else str(event_id)
    if extra_fields:
        fields.update(extra_fields)
    return fields


def _event_row(
    *,
    event_id: UUID,
    candidate_group_id: UUID,
    bundle_id: UUID,
    event_type: str = "analysis.requested.v1",
    aggregate_type: str = "candidate_group",
    aggregate_id: UUID | None = None,
    status: str = "published",
    judge_profile: str | None = "github_primary",
    escalation_allowed: bool = True,
    include_trigger_payload: bool = True,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "candidate_group_id": str(candidate_group_id),
        "bundle_id": str(bundle_id),
        "escalation_allowed": escalation_allowed,
        "source_text": FAKE_SOURCE_TEXT,
        "canonical_url": FAKE_URL,
    }
    if judge_profile is not None:
        payload["judge_profile"] = judge_profile
    if not include_trigger_payload:
        payload.pop("bundle_id")
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id or candidate_group_id,
        "payload_json": payload,
        "status": status,
    }


def _session_for_event(
    row: dict[str, Any],
    *,
    current_bundle_id: UUID | None | object = None,
    bundle_candidate_group_id: UUID | None = None,
    ready_for_analysis: bool = True,
    member_count: int = 1,
    supporting_count: int = 0,
    reroot_count: int = 0,
    token_budget_profile: str = "small",
    **session_kwargs: Any,
) -> FakeSession:
    payload = row["payload_json"]
    candidate_group_id = UUID(str(payload["candidate_group_id"]))
    bundle_id = UUID(str(payload["bundle_id"]))
    if current_bundle_id is None:
        current_bundle_id = bundle_id
    bundle_candidate_group_id = bundle_candidate_group_id or candidate_group_id
    return FakeSession(
        events={str(row["event_id"]): row},
        candidate_groups={str(candidate_group_id): current_bundle_id},  # type: ignore[dict-item]
        bundles={
            str(bundle_id): {
                "bundle_id": bundle_id,
                "candidate_group_id": bundle_candidate_group_id,
                "bundle_profile_version": "bundle_profile_v1",
                "reroot_count": reroot_count,
                "ready_for_analysis": ready_for_analysis,
                "token_budget_profile": token_budget_profile,
                "created_at": datetime.now(timezone.utc),
            }
        },
        member_counts={str(bundle_id): (member_count, supporting_count)},
        **session_kwargs,
    )


def _valid_fixture(**overrides: Any) -> tuple[UUID, UUID, UUID, dict[str, str], dict[str, Any]]:
    event_id = overrides.pop("event_id", uuid4())
    candidate_group_id = overrides.pop("candidate_group_id", uuid4())
    bundle_id = overrides.pop("bundle_id", uuid4())
    stream = _stream_fields(event_id=event_id, root_object_id=candidate_group_id)
    event = _event_row(
        event_id=event_id,
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        **overrides,
    )
    return event_id, candidate_group_id, bundle_id, stream, event


def _run_report(
    *,
    stream_fields: dict[str, str] | None = None,
    event_row: dict[str, Any] | None = None,
    approvals: Any | None = None,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    escalation: bool = False,
    side_effect_flags: dict[str, bool] | None = None,
) -> tuple[Any, FakeSession, FakeRedis]:
    if stream_fields is None or event_row is None:
        _event_id, _candidate_group_id, _bundle_id, stream_fields, event_row = _valid_fixture()
    if session is None:
        session = _session_for_event(event_row)
    if redis is None:
        redis = FakeRedis([(FAKE_STREAM_ID, stream_fields)])
    result = _module().generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        approvals=approvals,
        runtime_env_reader=_runtime_env(escalation=escalation),
        database_session_factory=lambda _url: session,
        redis_client_factory=lambda _url: redis,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, FAKE_SENSITIVE_VALUE),
    )
    return result, session, redis


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_default_mode_is_read_only_and_does_not_write_or_ack() -> None:
    result, session, redis = _run_report()

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_READY
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["read_only_transaction"] is True
    assert result.report["q_analysis_route_entry_found_bucket"] == "one"
    assert result.report["redis_entry_shape_valid_bucket"] == "one"
    assert result.report["judge_run_write_planned_bucket"] == "one"
    assert result.report["judge_run_write_attempted"] is False
    assert result.report["judge_call_requested_outbox_write_attempted"] is False
    assert result.report["redis_ack_attempted"] is False
    assert redis.xgroup_create_calls == []
    assert redis.xack_calls == []
    assert session.written_judge_runs == []
    assert session.written_outbox_events == []
    assert session.rollback_count >= 1


def test_partial_approvals_fail_before_db_or_redis_connections() -> None:
    result, session, redis = _run_report(
        approvals=_approvals(
            analysis_router_consume=True,
            judge_run_write=True,
            judge_call_requested_outbox_write=True,
        )
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_MISSING_APPROVAL
    assert "approval.redis_ack" in result.report["checks_failed"]
    assert result.report["database_connected"] is False
    assert result.report["redis_connected"] is False
    assert session.statements == []
    assert redis.ping_calls == 0
    assert redis.xack_calls == []


def test_malformed_redis_shape_blocks_before_write_or_ack() -> None:
    event_id, _candidate_group_id, _bundle_id, stream, event = _valid_fixture()
    stream["payload_json"] = json.dumps({"event_id": str(event_id)})
    result, session, redis = _run_report(stream_fields=stream, event_row=event)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_REDIS_ENTRY
    assert "redis.fields" in result.report["checks_failed"]
    assert session.written_judge_runs == []
    assert redis.xack_calls == []


def test_missing_trigger_event_id_blocks_before_rehydration() -> None:
    _event_id, _candidate_group_id, _bundle_id, stream, event = _valid_fixture()
    stream["trigger_event_id"] = ""
    result, session, redis = _run_report(stream_fields=stream, event_row=event)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_REDIS_ENTRY
    assert "redis.trigger_event_id" in result.report["checks_failed"]
    assert result.report["analysis_requested_event_rehydrated_bucket"] == "zero"
    assert session.written_judge_runs == []
    assert redis.xack_calls == []


def test_wrong_stage_or_root_object_blocks_before_write_or_ack() -> None:
    event_id, _candidate_group_id, _bundle_id, _stream, event = _valid_fixture()
    wrong_stage = _stream_fields(event_id=event_id, stage_name="judge")
    result_stage, _session_stage, redis_stage = _run_report(
        stream_fields=wrong_stage,
        event_row=event,
    )
    wrong_root = _stream_fields(event_id=event_id, root_object_type="artifact")
    result_root, _session_root, redis_root = _run_report(
        stream_fields=wrong_root,
        event_row=event,
    )

    assert result_stage.report["contract_status"] == _module().STATUS_INVALID_REDIS_ENTRY
    assert "redis.stage_name" in result_stage.report["checks_failed"]
    assert result_root.report["contract_status"] == _module().STATUS_INVALID_REDIS_ENTRY
    assert "redis.root_object_type" in result_root.report["checks_failed"]
    assert redis_stage.xack_calls == []
    assert redis_root.xack_calls == []


def test_analysis_requested_rehydration_uses_postgresql_trigger_event_id() -> None:
    event_id, candidate_group_id, _bundle_id, stream, event = _valid_fixture()
    stream["root_object_id"] = str(uuid4())
    result, session, _redis = _run_report(stream_fields=stream, event_row=event)

    assert result.exit_code == 0
    assert result.report["analysis_requested_event_rehydrated_bucket"] == "one"
    assert result.report["candidate_group_current_bundle_match_bucket"] == "one"
    assert any(
        params.get("event_id") == str(event_id)
        for statement, params in zip(session.statements, session.params)
        if statement == _normalize(_module().SELECT_ANALYSIS_REQUESTED_EVENT_QUERY)
    )
    assert str(candidate_group_id) not in json.dumps(result.report, sort_keys=True)


def test_stale_bundle_current_bundle_mismatch_blocks_without_write_or_ack() -> None:
    _event_id, _candidate_group_id, _bundle_id, stream, event = _valid_fixture()
    session = _session_for_event(event, current_bundle_id=uuid4())
    result, session, redis = _run_report(stream_fields=stream, event_row=event, session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_BUNDLE
    assert "candidate_group.current_bundle_id" in result.report["checks_failed"]
    assert session.written_judge_runs == []
    assert redis.xack_calls == []


def test_bundle_not_ready_blocks_without_refresh_write_or_ack() -> None:
    _event_id, _candidate_group_id, _bundle_id, stream, event = _valid_fixture()
    session = _session_for_event(event, ready_for_analysis=False)
    result, session, redis = _run_report(stream_fields=stream, event_row=event, session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_BUNDLE
    assert "bundle.ready_for_analysis" in result.report["checks_failed"]
    assert result.report["bundle_ready_for_analysis_bucket"] == "zero"
    assert session.written_judge_runs == []
    assert session.written_outbox_events == []
    assert redis.xack_calls == []


def test_missing_evidence_members_blocks_without_write_or_ack() -> None:
    _event_id, _candidate_group_id, _bundle_id, stream, event = _valid_fixture()
    session = _session_for_event(event, member_count=0)
    result, session, redis = _run_report(stream_fields=stream, event_row=event, session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_BUNDLE
    assert "bundle.candidate_evidence_members" in result.report["checks_failed"]
    assert result.report["candidate_evidence_member_found_bucket"] == "zero"
    assert session.written_judge_runs == []
    assert redis.xack_calls == []


def test_invalid_judge_profile_blocks_without_write_or_ack() -> None:
    _event_id, _candidate_group_id, _bundle_id, stream, event = _valid_fixture(
        judge_profile="experimental_private_profile"
    )
    session = _session_for_event(event)
    result, session, redis = _run_report(stream_fields=stream, event_row=event, session=session)

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_INVALID_JUDGE_PROFILE
    assert "judge_profile.allowed" in result.report["checks_failed"]
    assert result.report["judge_profile_allowed_bucket"] == "zero"
    assert session.written_judge_runs == []
    assert redis.xack_calls == []


def _decision(
    *,
    escalation: bool = False,
    escalation_allowed: bool = True,
    reroot_count: int = 0,
    supporting_count: int = 0,
    token_budget_profile: str = "small",
):
    module = _module()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    runtime_config = module.RuntimeConfig(
        database_url=FAKE_DATABASE_URL,
        redis_url=FAKE_REDIS_URL,
        consumer_group="analysis-router-consume-smoke",
        consumer_name="analysis-router-consume-smoke-1",
        enable_model_escalation=escalation,
    )
    job = module.AnalysisRequestedJob(
        trigger_event_id=uuid4(),
        event_type="analysis.requested.v1",
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        judge_profile="github_primary",
        escalation_allowed=escalation_allowed,
    )
    bundle = module.BundleRouteState(
        bundle_id=bundle_id,
        candidate_group_id=candidate_group_id,
        bundle_profile_version="bundle_profile_v1",
        reroot_count=reroot_count,
        ready_for_analysis=True,
        token_budget_profile=token_budget_profile,
        created_at=datetime.now(timezone.utc),
    )
    counts = module.EvidenceMemberCounts(
        member_count=max(1, supporting_count),
        supporting_count=supporting_count,
    )
    return module.decide_analysis_route(
        runtime_config=runtime_config,
        job=job,
        bundle=bundle,
        counts=counts,
    )


def test_default_routing_chooses_gpt_5_4_mini_low() -> None:
    decision = _decision()

    assert decision.model == "gpt-5.4-mini"
    assert decision.reasoning_effort == "low"


def test_escalation_only_happens_with_config_payload_and_complexity_rules() -> None:
    disabled = _decision(
        escalation=False,
        escalation_allowed=True,
        reroot_count=1,
        supporting_count=3,
        token_budget_profile="xlarge",
    )
    payload_disallowed = _decision(escalation=True, escalation_allowed=False, reroot_count=1)
    no_complexity = _decision(escalation=True, escalation_allowed=True)
    reroot = _decision(escalation=True, escalation_allowed=True, reroot_count=1)
    supporting = _decision(escalation=True, escalation_allowed=True, supporting_count=3)
    large = _decision(escalation=True, escalation_allowed=True, token_budget_profile="large")

    assert disabled.model == "gpt-5.4-mini"
    assert payload_disallowed.model == "gpt-5.4-mini"
    assert no_complexity.model == "gpt-5.4-mini"
    assert reroot.model == "gpt-5.4"
    assert reroot.reasoning_effort == "medium"
    assert supporting.model == "gpt-5.4"
    assert large.model == "gpt-5.4"


def test_prompt_cache_key_is_stable() -> None:
    assert (
        _decision().prompt_cache_key
        == "judge:github_primary:judge_github_primary_v1:judge_output_v1:verdict_policy_v1"
    )


def test_approved_mode_creates_judge_run_and_judge_call_outbox_then_acks() -> None:
    order: list[str] = []
    _event_id, _candidate_group_id, _bundle_id, stream, event = _valid_fixture()
    session = _session_for_event(event, order=order)
    redis = FakeRedis([(FAKE_STREAM_ID, stream)], order=order)
    result, session, redis = _run_report(
        stream_fields=stream,
        event_row=event,
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_CONSUMED
    assert result.report["judge_run_write_attempted"] is True
    assert result.report["judge_run_written_bucket"] == "one"
    assert result.report["judge_call_requested_outbox_written_bucket"] == "one"
    assert result.report["redis_ack_succeeded_bucket"] == "one"
    assert len(session.written_judge_runs) == 1
    assert len(session.written_outbox_events) == 1
    assert len(redis.xack_calls) == 1
    assert order.index("db:commit") < order.index("redis:xack")


def test_existing_judge_run_is_reused_without_duplicate_judge_run_write() -> None:
    existing_judge_run_id = uuid4()
    _event_id, _candidate_group_id, _bundle_id, stream, event = _valid_fixture()
    session = _session_for_event(event, existing_judge_run_id=existing_judge_run_id)
    result, session, redis = _run_report(
        stream_fields=stream,
        event_row=event,
        session=session,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["judge_run_written_bucket"] == "zero"
    assert result.report["judge_run_reused_bucket"] == "one"
    assert session.written_judge_runs == []
    assert result.report["judge_call_requested_outbox_written_bucket"] == "one"
    assert len(redis.xack_calls) == 1


def test_existing_judge_call_requested_outbox_is_reused_without_duplicate_write() -> None:
    _event_id, _candidate_group_id, _bundle_id, stream, event = _valid_fixture()
    session = _session_for_event(
        event,
        existing_judge_run_id=uuid4(),
        existing_outbox_event_id=uuid4(),
    )
    result, session, redis = _run_report(
        stream_fields=stream,
        event_row=event,
        session=session,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 0
    assert result.report["judge_run_reused_bucket"] == "one"
    assert result.report["judge_call_requested_outbox_written_bucket"] == "zero"
    assert result.report["judge_call_requested_outbox_reused_bucket"] == "one"
    assert session.written_outbox_events == []
    assert len(redis.xack_calls) == 1


def test_no_openai_judge_output_validator_policy_notifier_or_source_side_effects() -> None:
    result, _session, _redis = _run_report()
    text = SCRIPT.read_text(encoding="utf-8")

    assert result.report["judge_openai_started"] is False
    assert result.report["judge_output_written_bucket"] == "zero"
    assert result.report["analysis_validator_started"] is False
    assert result.report["analysis_written_bucket"] == "zero"
    assert result.report["policy_engine_started"] is False
    assert result.report["notification_plan_written_bucket"] == "zero"
    assert result.report["notifier_started"] is False
    assert result.report["telegram_send_attempted"] is False
    assert result.report["source_tables_mutation_performed"] is False
    assert result.report["candidate_mutation_performed"] is False
    assert "src.services.judge_openai" not in text
    assert "src.services.analysis_validator" not in text
    assert "src.services.policy_engine" not in text
    assert "src.services.notifier_telegram" not in text


def test_no_raw_values_emitted_including_ids_urls_credentials_stream_ids_or_payload_json() -> None:
    event_id, candidate_group_id, bundle_id, stream, event = _valid_fixture()
    result, _session, _redis = _run_report(
        stream_fields=stream,
        event_row=event,
        approvals=_all_approvals(),
    )
    rendered = json.dumps(result.report, sort_keys=True)
    forbidden_values = (
        str(event_id),
        str(candidate_group_id),
        str(bundle_id),
        FAKE_DEDUPE_KEY,
        FAKE_SOURCE_TEXT,
        FAKE_URL,
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_DATABASE_CREDENTIAL,
        FAKE_REDIS_CREDENTIAL,
        FAKE_RUNTIME_PATH,
        FAKE_STREAM_ID,
        json.dumps(event["payload_json"], sort_keys=True),
        FAKE_SENSITIVE_VALUE,
    )

    assert result.exit_code == 0
    assert result.report["raw_values_emitted"] is False
    for value in forbidden_values:
        assert value not in rendered


def test_redis_ack_failure_does_not_falsely_report_success() -> None:
    _event_id, _candidate_group_id, _bundle_id, stream, event = _valid_fixture()
    session = _session_for_event(event)
    redis = FakeRedis([(FAKE_STREAM_ID, stream)], ack_return=0)
    result, session, redis = _run_report(
        stream_fields=stream,
        event_row=event,
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_REDIS_ACK_FAILED
    assert result.report["redis_ack_attempted"] is True
    assert result.report["redis_ack_succeeded_bucket"] == "zero"
    assert session.committed is True
    assert len(redis.xack_calls) == 1


def test_db_write_failure_does_not_ack_redis() -> None:
    _event_id, _candidate_group_id, _bundle_id, stream, event = _valid_fixture()
    session = _session_for_event(event, raise_on_outbox_write=True)
    redis = FakeRedis([(FAKE_STREAM_ID, stream)])
    result, session, redis = _run_report(
        stream_fields=stream,
        event_row=event,
        session=session,
        redis=redis,
        approvals=_all_approvals(),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DB_WRITE_FAILED
    assert result.report["judge_run_write_attempted"] is True
    assert result.report["judge_call_requested_outbox_write_attempted"] is True
    assert redis.xack_calls == []
    assert session.committed is False
    assert session.rollback_count >= 1
